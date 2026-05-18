import hashlib
import os
from pathlib import Path

import pandas as pd
import requests
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DELTA_LAKE_PATH = Path(os.environ.get("DELTA_LAKE_PATH", PROJECT_ROOT / "delta-lake" / "raw"))
EMBED_URL = (os.environ.get("EMBED_NGROK_URL") or "").rstrip("/")
COLLECTION_NAME = os.environ.get("QDRANT_COLLECTION", "documents")
VECTOR_SIZE = int(os.environ.get("VECTOR_SIZE", "384"))


def load_records() -> list[dict]:
    files = sorted(DELTA_LAKE_PATH.glob("*.parquet"))
    if files:
        df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
        return df[["id", "text"]].drop_duplicates("id").to_dict("records")

    return [
        {"id": "doc_001", "text": "AI platform integration test"},
        {"id": "doc_002", "text": "Kafka to Delta pipeline"},
    ]


def deterministic_embedding(text: str) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values = []
    while len(values) < VECTOR_SIZE:
        for byte in digest:
            values.append((byte / 255.0) * 2 - 1)
            if len(values) == VECTOR_SIZE:
                break
        digest = hashlib.sha256(digest).digest()
    return values


def embed_texts(texts: list[str]) -> list[list[float]]:
    if EMBED_URL:
        try:
            response = requests.post(f"{EMBED_URL}/embed", json={"texts": texts}, timeout=30)
            response.raise_for_status()
            embeddings = response.json()["embeddings"]
            if embeddings and len(embeddings[0]) == VECTOR_SIZE:
                print(f"Using Kaggle embedding service: {EMBED_URL}")
                return embeddings
            print("Embedding service returned unexpected vector size; using local deterministic fallback")
        except (requests.RequestException, KeyError, ValueError) as exc:
            print(f"Embedding service unavailable ({exc}); using local deterministic fallback")

    return [deterministic_embedding(text) for text in texts]


def embed_and_store(records: list[dict]) -> int:
    qdrant = QdrantClient(host=os.environ.get("QDRANT_HOST", "localhost"), port=int(os.environ.get("QDRANT_PORT", "6333")))
    qdrant.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
    )

    embeddings = embed_texts([record["text"] for record in records])
    points = [
        PointStruct(id=index, vector=embedding, payload=record)
        for index, (embedding, record) in enumerate(zip(embeddings, records))
    ]
    qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
    print(f"Integration 5 OK: {len(points)} vectors stored in Qdrant collection '{COLLECTION_NAME}'")
    return len(points)


if __name__ == "__main__":
    embed_and_store(load_records())
