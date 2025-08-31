
"""
kstreams.py — Minimal Kafka-based pub/sub library that models:
  - Point-to-Point (work queue): multiple consumers share a single group/subscription
  - Publish-Subscribe: multiple independent subscriptions (consumer groups) each get all events

Features:
  - At-least-once delivery (manual commit after successful handler).
  - Optional retry/backoff before DLQ.
  - JSON value serialization by default; overrideable.
  - Topic auto-create helper via Admin API.
  - Threaded consumers with graceful shutdown.

Install deps:
    pip install kafka-python>=2.0.2
"""
from __future__ import annotations
import json
import logging
import threading
import time
from typing import Callable, Optional, Dict, Any, Iterable, List

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

log = logging.getLogger("kstreams")
if not log.handlers:
    log.setLevel(logging.INFO)
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    log.addHandler(_handler)


def _json_dumps(v: Any) -> bytes:
    def _default(o):
        try:
            import datetime, decimal
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
            if isinstance(o, decimal.Decimal):
                return float(o)
        except Exception:
            pass
        return str(o)
    return json.dumps(v, separators=(",", ":"), default=_default).encode("utf-8")


class KafkaPubSub:
    def __init__(
        self,
        bootstrap_servers: Iterable[str] | str = "localhost:9092",
        client_id: str = "kstreams",
        *,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: Optional[str] = None,
        sasl_plain_username: Optional[str] = None,
        sasl_plain_password: Optional[str] = None,
        acks: str | int = "all",
        value_serializer=_json_dumps,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b is not None else None,
    ) -> None:
        common = dict(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            security_protocol=security_protocol,
        )
        if security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            common.update(
                dict(
                    sasl_mechanism=sasl_mechanism or "PLAIN",
                    sasl_plain_username=sasl_plain_username,
                    sasl_plain_password=sasl_plain_password,
                )
            )

        self._value_deserializer = value_deserializer
        self._common_conf = common

        self.producer = KafkaProducer(
            **common,
            acks=acks,
            key_serializer=lambda k: k if k is None or isinstance(k, (bytes, bytearray)) else str(k).encode("utf-8"),
            value_serializer=value_serializer,
            linger_ms=5,
            retries=5,
            max_in_flight_requests_per_connection=5,
        )

    # ---------- Admin ----------
    def create_topic(self, name: str, num_partitions: int = 1, replication_factor: int = 1, config: Optional[Dict[str, str]] = None) -> None:
        admin = KafkaAdminClient(**self._common_conf)
        try:
            admin.create_topics([NewTopic(name=name, num_partitions=num_partitions, replication_factor=replication_factor, topic_configs=config or {})])
            log.info("Created topic %s (partitions=%s, rf=%s)", name, num_partitions, replication_factor)
        except TopicAlreadyExistsError:
            log.info("Topic %s already exists", name)
        finally:
            admin.close()

    # ---------- Publish ----------
    def publish(self, topic: str, value: Dict[str, Any], *, key: Optional[bytes] = None, headers: Optional[Iterable[tuple[str, bytes]]] = None, timeout: float = 10.0) -> dict:
        fut = self.producer.send(topic, key=key, value=value, headers=list(headers) if headers else None)
        md = fut.get(timeout=timeout)
        self.producer.flush(1.0)
        return {"topic": md.topic, "partition": md.partition, "offset": md.offset, "timestamp": int(time.time() * 1000)}

    def publish_batch(self, topic: str, values: List[Dict[str, Any]], *, timeout: float = 10.0) -> List[dict]:
        futs = [self.producer.send(topic, value=v) for v in values]
        metas = []
        for f in futs:
            md = f.get(timeout=timeout)
            metas.append({"topic": md.topic, "partition": md.partition, "offset": md.offset})
        self.producer.flush(2.0)
        return metas

    # ---------- Subscribe (core) ----------
    def _consume_loop(
        self,
        topic: str,
        group_id: str,
        handler: Callable[[dict, dict], None],
        *,
        auto_offset_reset: str = "earliest",
        dead_letter_topic: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
        poll_timeout_ms: int = 1000,
        max_poll_records: int = 50,
        max_retries: int = 0,
        backoff_ms: int = 1000,
    ) -> None:
        consumer = KafkaConsumer(
            topic,
            **self._common_conf,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            max_poll_records=max_poll_records,
            value_deserializer=self._value_deserializer,
            key_deserializer=lambda b: b,
        )
        log.info("Consumer started topic=%s group=%s", topic, group_id)
        try:
            while stop_event is None or not stop_event.is_set():
                records = consumer.poll(timeout_ms=poll_timeout_ms)
                if not records:
                    continue
                for tp, batch in records.items():
                    for msg in batch:
                        ctx = {
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp,
                            "headers": dict((k, v) for k, v in (msg.headers or [])),
                            "key": msg.key,
                        }
                        attempt = 0
                        while True:
                            try:
                                handler(msg.value, ctx)
                                consumer.commit()
                                break
                            except Exception as e:
                                attempt += 1
                                if attempt <= max_retries:
                                    log.warning("Handler error, retrying %s/%s (topic=%s offset=%s): %s", attempt, max_retries, msg.topic, msg.offset, e)
                                    time.sleep(backoff_ms / 1000.0)
                                    continue
                                log.exception("Handler failed after %s retries (topic=%s offset=%s): %s", max_retries, msg.topic, msg.offset, e)
                                if dead_letter_topic:
                                    try:
                                        self.publish(dead_letter_topic, {"error": str(e), "value": msg.value, "ctx": {k: (v if k != "key" else (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))) for k, v in ctx.items() if k != "headers"}})
                                        consumer.commit()
                                    except Exception as e2:
                                        log.exception("Failed sending to DLQ %s: %s", dead_letter_topic, e2)
                                # If no DLQ: leave uncommitted => redelivery
                                break
        finally:
            consumer.close()
            log.info("Consumer closed topic=%s group=%s", topic, group_id)

    def subscribe(
        self,
        topic: str,
        group_id: str,
        handler: Callable[[dict, dict], None],
        *,
        concurrency: int = 1,
        auto_offset_reset: str = "earliest",
        dead_letter_topic: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
        max_retries: int = 0,
        backoff_ms: int = 1000,
    ) -> list[threading.Thread]:
        threads: list[threading.Thread] = []
        for i in range(concurrency):
            t = threading.Thread(
                target=self._consume_loop,
                name=f"kstreams-{topic}-{group_id}-{i}",
                kwargs=dict(
                    topic=topic,
                    group_id=group_id,
                    handler=handler,
                    auto_offset_reset=auto_offset_reset,
                    dead_letter_topic=dead_letter_topic,
                    stop_event=stop_event,
                    max_retries=max_retries,
                    backoff_ms=backoff_ms,
                ),
                daemon=True,
            )
            t.start()
            threads.append(t)
        return threads

    def subscribe_point_to_point(self, topic: str, subscription: str, handler: Callable[[dict, dict], None], **kwargs) -> list[threading.Thread]:
        return self.subscribe(topic=topic, group_id=subscription, handler=handler, **kwargs)

    def subscribe_publish(self, topic: str, subscription: str, handler: Callable[[dict, dict], None], **kwargs) -> list[threading.Thread]:
        return self.subscribe(topic=topic, group_id=subscription, handler=handler, **kwargs)

    def close(self) -> None:
        self.producer.flush(2.0)
        self.producer.close()
