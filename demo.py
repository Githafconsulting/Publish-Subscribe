
"""
demo.py — Demonstrates both semantics against local Kafka.
"""
import os
import signal
import threading
import time
from kstreams import KafkaPubSub

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

ps = KafkaPubSub(bootstrap_servers=BOOTSTRAP, client_id="demo")

# Ensure topics exist
ps.create_topic("topic-b", num_partitions=3)
ps.create_topic("topic-c", num_partitions=3)
ps.create_topic("dead-letter", num_partitions=1)

stop_event = threading.Event()
def stop(*_): stop_event.set()
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

# Handlers
def handler_b(value, ctx):
    print(f"[B-consumer] group=subscription-b got: {value} @ {ctx['topic']}[{ctx['partition']}:{ctx['offset']}]")

def handler_y(value, ctx):
    print(f"[Y-consumer] group=subscription-yc got: {value}")

def handler_z(value, ctx):
    print(f"[Z-consumer] group=subscription-zc got: {value}")

# Point-to-Point (Topic B) — two consumers share one subscription
threads_b1 = ps.subscribe_point_to_point("topic-b", "subscription-b", handler_b, concurrency=1, stop_event=stop_event, dead_letter_topic="dead-letter", max_retries=2, backoff_ms=500)
threads_b2 = ps.subscribe_point_to_point("topic-b", "subscription-b", handler_b, concurrency=1, stop_event=stop_event, dead_letter_topic="dead-letter", max_retries=2, backoff_ms=500)

# Publish-Subscribe (Topic C) — two independent subscriptions each receive all messages
threads_y = ps.subscribe_publish("topic-c", "subscription-yc", handler_y, stop_event=stop_event)
threads_z = ps.subscribe_publish("topic-c", "subscription-zc", handler_z, stop_event=stop_event)

time.sleep(1)

# Publish demo messages
ps.publish("topic-b", {"message": 2})
ps.publish("topic-c", {"message": 3})
ps.publish_batch("topic-c", [{"message": 3, "idx": i} for i in range(3)])

print("Published demo messages. Press Ctrl+C to stop...")
try:
    while not stop_event.is_set():
        time.sleep(0.25)
finally:
    ps.close()
    for t in threads_b1 + threads_b2 + threads_y + threads_z:
        t.join(timeout=2.0)
    print("Shutdown complete.")
