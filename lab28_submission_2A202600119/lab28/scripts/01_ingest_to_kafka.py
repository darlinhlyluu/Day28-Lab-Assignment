import json
import os
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


TOPIC = os.environ.get("KAFKA_TOPIC", "data.raw")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def ensure_topic() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="lab28-admin")
    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
        print(f"Created Kafka topic: {TOPIC}")
    except TopicAlreadyExistsError:
        print(f"Kafka topic already exists: {TOPIC}")
    finally:
        admin.close()


def ingest_data(records: list[dict]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for record in records:
            producer.send(TOPIC, value=record)
            print(f"Sent: {record['id']}")
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    sample_data = [
        {"id": "doc_001", "text": "AI platform integration test", "timestamp": time.time()},
        {"id": "doc_002", "text": "Kafka to Delta pipeline", "timestamp": time.time()},
    ]
    ensure_topic()
    ingest_data(sample_data)
    print(f"Integration 1 OK: Data -> Kafka topic {TOPIC}")
