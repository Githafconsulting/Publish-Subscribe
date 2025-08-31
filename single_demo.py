# single_demo.py — self-contained script (library + demo in one file)
from __future__ import annotations
import json, threading, time, os, signal
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
def _json_dumps(v): return json.dumps(v, separators=(',', ':')).encode('utf-8')
class KafkaPubSub:
    def __init__(self, bootstrap_servers='localhost:9092', client_id='kstreams'):
        self._common=dict(bootstrap_servers=bootstrap_servers, client_id=client_id, security_protocol='PLAINTEXT')
        self.producer=KafkaProducer(**self._common, acks='all', key_serializer=lambda k: k if k is None or isinstance(k,(bytes,bytearray)) else str(k).encode(), value_serializer=_json_dumps, linger_ms=5, retries=5)
    def create_topic(self, name, num_partitions=1, replication_factor=1, config=None):
        admin=KafkaAdminClient(**self._common)
        try: admin.create_topics([NewTopic(name=name, num_partitions=num_partitions, replication_factor=replication_factor, topic_configs=config or {})])
        except TopicAlreadyExistsError: pass
        finally: admin.close()
    def publish(self, topic, value, *, key=None, headers=None, timeout=10.0):
        md=self.producer.send(topic, key=key, value=value, headers=list(headers) if headers else None).get(timeout=timeout); self.producer.flush(1.0); return {'topic':md.topic,'partition':md.partition,'offset':md.offset}
    def _consume_loop(self, topic, group_id, handler, *, stop_event=None):
        consumer=KafkaConsumer(topic, **self._common, group_id=group_id, auto_offset_reset='earliest', enable_auto_commit=False, value_deserializer=lambda b: json.loads(b.decode()) if b else None, key_deserializer=lambda b: b)
        try:
            while stop_event is None or not stop_event.is_set():
                records=consumer.poll(timeout_ms=1000)
                for tp,batch in records.items():
                    for msg in batch:
                        ctx={'topic':msg.topic,'partition':msg.partition,'offset':msg.offset,'timestamp':msg.timestamp}
                        handler(msg.value, ctx); consumer.commit()
        finally: consumer.close()
    def subscribe(self, topic, group_id, handler, *, concurrency=1, stop_event=None):
        import threading
        threads=[]; 
        for i in range(concurrency):
            t=threading.Thread(target=self._consume_loop, kwargs=dict(topic=topic, group_id=group_id, handler=handler, stop_event=stop_event), daemon=True); t.start(); threads.append(t)
        return threads
    def close(self): self.producer.flush(2.0); self.producer.close()
if __name__=='__main__':
    BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP','localhost:9092'); ps=KafkaPubSub(bootstrap_servers=BOOTSTRAP)
    ps.create_topic('topic-b',3); ps.create_topic('topic-c',3)
    stop_event=threading.Event(); signal.signal(signal.SIGINT, lambda *_: stop_event.set()); signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    def handler_b(v,c): print('[B] got',v,c); 
    def handler_y(v,c): print('[Y] got',v); 
    def handler_z(v,c): print('[Z] got',v)
    ps.subscribe('topic-b','subscription-b',handler_b,stop_event=stop_event); ps.subscribe('topic-b','subscription-b',handler_b,stop_event=stop_event)
    ps.subscribe('topic-c','subscription-yc',handler_y,stop_event=stop_event); ps.subscribe('topic-c','subscription-zc',handler_z,stop_event=stop_event)
    time.sleep(1); ps.publish('topic-b',{'message':2}); ps.publish('topic-c',{'message':3})
    print('Running... Ctrl+C to exit')
    while not stop_event.is_set(): time.sleep(0.25)
    ps.close()
