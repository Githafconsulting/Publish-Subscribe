from confluent_kafka import Consumer
import json

KAFKA_BROKER = "localhost:9092"
TOPICS = ["kyc-submissions", "kyc-verification", "kyc-alerts"]
GROUP_ID = "kyc-listener-group"

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_conf)
consumer.subscribe(TOPICS)

print(f"Listening to Kafka topics: {TOPICS}...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        value = json.loads(msg.value().decode("utf-8"))
        print(f"[{msg.topic()}] Key: {key} | Value: {value}")

except KeyboardInterrupt:
    print("Exiting...")

finally:
    consumer.close()