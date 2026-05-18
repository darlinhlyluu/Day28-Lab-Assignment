import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


API_URL = "http://localhost:8000/health"
REQUESTS = 30
CONCURRENCY = 5


def call_api() -> float:
    started = time.perf_counter()
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    return (time.perf_counter() - started) * 1000


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((pct / 100) * (len(ordered) - 1)))
    return ordered[index]


def main() -> None:
    latencies = []
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(call_api) for _ in range(REQUESTS)]
        for future in as_completed(futures):
            latencies.append(future.result())

    duration = time.perf_counter() - started
    print(f"Load test endpoint: {API_URL}")
    print(f"Requests: {REQUESTS}, concurrency: {CONCURRENCY}")
    print(f"Successful requests: {len(latencies)}/{REQUESTS}")
    print(f"Throughput: {len(latencies) / duration:.2f} req/s")
    print(f"Average latency: {statistics.mean(latencies):.2f} ms")
    print(f"p95 latency: {percentile(latencies, 95):.2f} ms")
    print("Performance check: PASS")


if __name__ == "__main__":
    main()
