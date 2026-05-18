import json
import os
from pathlib import Path

import pandas as pd
import redis


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DELTA_LAKE_PATH = Path(os.environ.get("DELTA_LAKE_PATH", PROJECT_ROOT / "delta-lake" / "raw"))
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def load_from_delta_and_push_feast() -> int:
    files = sorted(DELTA_LAKE_PATH.glob("*.parquet"))
    if not files:
        print(f"No parquet files found in {DELTA_LAKE_PATH}")
        return 0

    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    print(f"Loaded {len(df)} records from Delta Lake")

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    for _, row in df.iterrows():
        feature_key = f"feature:{row['id']}"
        client.set(
            feature_key,
            json.dumps(
                {
                    "text": row["text"],
                    "timestamp": row.get("timestamp"),
                    "processed": True,
                }
            ),
        )

    print(f"Integration 3+4 OK: Delta Lake -> Feast (Redis) - {len(df)} features stored")
    return len(df)


if __name__ == "__main__":
    load_from_delta_and_push_feast()
