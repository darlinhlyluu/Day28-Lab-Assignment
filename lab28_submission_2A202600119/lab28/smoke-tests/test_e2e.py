import json
import time

import requests


BASE_URL = "http://localhost:8000"


def test_1_happy_path_chat_and_health():
    health = requests.get(f"{BASE_URL}/health", timeout=5)
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    response = requests.post(
        f"{BASE_URL}/api/v1/chat",
        json={"query": "What is platform engineering?", "embedding": [0.1] * 384},
        timeout=30,
    )
    assert response.status_code == 200
    data = response.json()
    assert "answer" in data
    assert len(data["answer"]) > 10
    assert data["latency_ms"] < 30000


def test_2_data_ingestion_journey():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    producer.send("data.raw", {"id": "smoke_001", "text": "smoke test document", "timestamp": time.time()})
    producer.flush()
    producer.close()

    response = requests.get("http://localhost:6333/collections/documents", timeout=10)
    assert response.status_code == 200
    assert response.json()["result"]["points_count"] > 0


def test_3_observability_journey():
    response = requests.get(
        "http://localhost:9090/api/v1/query",
        params={"query": "up{job='api-gateway'}"},
        timeout=10,
    )
    assert response.status_code == 200
    result = response.json()["data"]["result"]
    assert result
    assert result[0]["value"][1] == "1"

    grafana = requests.get("http://localhost:3000/api/health", auth=("admin", "admin"), timeout=10)
    assert grafana.status_code == 200


def test_4_failure_path_is_graceful():
    invalid = requests.post(f"{BASE_URL}/api/v1/chat", json={}, timeout=5)
    assert invalid.status_code == 422

    try:
        requests.post(
            f"{BASE_URL}/api/v1/chat",
            json={"query": "timeout test", "embedding": [0.1] * 384},
            timeout=0.001,
        )
    except requests.exceptions.Timeout:
        pass

    health = requests.get(f"{BASE_URL}/health", timeout=5)
    assert health.status_code == 200


def test_5_feature_store_journey():
    import redis

    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    keys = client.keys("feature:*")
    assert keys, "Run python scripts/03_delta_to_feast.py before smoke tests"
