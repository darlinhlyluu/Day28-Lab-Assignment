import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer
from prefect import flow, task


TOPIC = os.environ.get("KAFKA_TOPIC", "data.raw")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DELTA_LAKE_PATH = Path(
    os.environ.get(
        "DELTA_LAKE_PATH",
        str(Path(__file__).resolve().parents[2] / "delta-lake" / "raw"),
    )
)


@task
def consume_and_process() -> list[dict]:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    try:
        records = [msg.value for msg in consumer]
    finally:
        consumer.close()

    print(f"Consumed {len(records)} records from Kafka topic {TOPIC}")
    return records


@task
def save_to_delta(records: list[dict]) -> str | None:
    if not records:
        print("No records to save")
        return None

    DELTA_LAKE_PATH.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)
    output_file = DELTA_LAKE_PATH / f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Saved {len(df)} records to {output_file}")
    return str(output_file)


@flow(name="Kafka to Delta Pipeline", log_prints=True)
def kafka_to_delta_flow() -> str | None:
    records = consume_and_process()
    return save_to_delta(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run or deploy the Kafka -> Delta Prefect flow.")
    parser.add_argument("--deploy", action="store_true", help="Create a Prefect deployment named kafka-to-delta.")
    args = parser.parse_args()

    if args.deploy:
        try:
            kafka_to_delta_flow.deploy(name="kafka-to-delta", work_pool_name="lab28-worker")
        except TypeError:
            kafka_to_delta_flow.deploy(name="kafka-to-delta", work_queue_name="lab28-worker")
    else:
        kafka_to_delta_flow()
