import subprocess

import redis
import requests


results = {}


def check(name, fn):
    try:
        fn()
        results[name] = "PASS"
        print(f"  [PASS] {name}")
    except Exception as exc:
        results[name] = f"FAIL: {exc}"
        print(f"  [FAIL] {name}: {exc}")


def get(url: str, **kwargs):
    response = requests.get(url, timeout=10, **kwargs)
    response.raise_for_status()
    return response


def post(url: str, **kwargs):
    response = requests.post(url, timeout=15, **kwargs)
    response.raise_for_status()
    return response


def check_unauthorized():
    response = requests.get("http://localhost:8000/admin", timeout=10)
    assert response.status_code in [401, 403, 404]


def check_collection_exists():
    response = get("http://localhost:6333/collections/documents")
    assert response.json()["result"]["status"] in {"green", "yellow"}


def check_feature_store_has_data():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client.ping()
    assert client.keys("feature:*"), "Run python scripts/03_delta_to_feast.py first"


def check_kafka_topics():
    container_id = subprocess.run(
        ["docker", "compose", "ps", "-q", "kafka"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert container_id, "Kafka container is not running"

    result = subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            "kafka-topics",
            "--list",
            "--bootstrap-server",
            "localhost:9092",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "data.raw" in result.stdout, "Run python scripts/01_ingest_to_kafka.py first"


print("\n=== RELIABILITY ===")
check("Health check endpoint", lambda: get("http://localhost:8000/health"))
check("API Gateway docs respond", lambda: get("http://localhost:8000/docs"))
check(
    "Chat endpoint responds",
    lambda: post(
        "http://localhost:8000/api/v1/chat",
        json={"query": "production readiness check", "embedding": [0.1] * 384},
    ),
)

print("\n=== OBSERVABILITY ===")
check("Prometheus up", lambda: get("http://localhost:9090/-/healthy"))
check("Grafana up", lambda: get("http://localhost:3000/api/health"))
check("Metrics endpoint exposed", lambda: get("http://localhost:8000/metrics"))
check(
    "Prometheus scrapes API Gateway",
    lambda: get("http://localhost:9090/api/v1/query", params={"query": "up{job='api-gateway'}"}),
)

print("\n=== SECURITY ===")
check("Unauthorized request rejected", check_unauthorized)

print("\n=== VECTOR STORE ===")
check("Qdrant healthy", lambda: get("http://localhost:6333/healthz"))
check("Collection exists", check_collection_exists)

print("\n=== FEATURE STORE ===")
check("Redis reachable and populated", check_feature_store_has_data)

print("\n=== KAFKA ===")
check("Kafka topics exist", check_kafka_topics)

passed = sum(1 for value in results.values() if value == "PASS")
total = len(results)
score = (passed / total) * 100
print(f"\n{'=' * 40}")
print(f"Production Readiness Score: {passed}/{total} = {score:.0f}%")
print(f"Target: >80% - Status: {'READY' if score >= 80 else 'NOT READY'}")
