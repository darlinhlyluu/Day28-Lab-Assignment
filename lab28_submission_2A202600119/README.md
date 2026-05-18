# Lab 28 Submission - 2A202600119

## Overview

This repository contains a full AI infrastructure platform integration demo using a hybrid Local + Colab GPU architecture.

Local Docker services:
- Kafka and Zookeeper for event ingestion
- Prefect for orchestration
- Delta Lake parquet files for persisted batches
- Redis as the feature store backend
- Qdrant as the vector store
- FastAPI API Gateway for chat and retrieval
- Prometheus and Grafana for observability

Colab GPU services:
- vLLM-compatible chat endpoint exposed through ngrok
- Embedding endpoint with deterministic fallback
- LangSmith trace verification

## How To Run

1. Start the local platform:

```powershell
docker compose up -d
docker compose ps
```

2. Activate Python 3.11 environment and install dependencies:

```powershell
.\.venv311\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r prefect\flows\requirements.txt
```

3. Configure `.env` from `.env.example`:

```text
VLLM_NGROK_URL=<colab_or_kaggle_ngrok_url>
EMBED_NGROK_URL=<colab_or_kaggle_ngrok_url>
MODEL_NAME=Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4
LANGCHAIN_PROJECT=lab28
LANGCHAIN_API_KEY=<langsmith_api_key>
ALLOW_LOCAL_FALLBACK=true
```

4. Run the data pipeline:

```powershell
python scripts\01_ingest_to_kafka.py

$env:PREFECT_HOME="$PWD\.prefect-local"
$env:PREFECT_API_URL="http://localhost:4200/api"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:DELTA_LAKE_PATH="$PWD\delta-lake\raw"
python prefect\flows\kafka_to_delta.py

python scripts\03_delta_to_feast.py
python scripts\05_embed_to_qdrant.py
```

5. Run verification:

```powershell
python -m pytest smoke-tests -v
python scripts\08_load_test_api.py
python scripts\09_verify_observability.py
python scripts\production_readiness_check.py
```

## Dashboards

- API Gateway: http://localhost:8000/health
- Prefect UI: http://localhost:4200
- Grafana: http://localhost:3000/d/lab28-platform/lab-28-platform-observability
- Prometheus: http://localhost:9090
- Qdrant: http://localhost:6333/dashboard
- LangSmith project: `lab28`

## Submission Evidence

Screenshots are included for:
- Prefect UI flow run status
- API Gateway health check
- Grafana dashboard
- Smoke test result
- Load test result
- Observability check
- LangSmith trace

## Performance Evidence

The included load test sends 30 concurrent health-check requests with concurrency 5. The latest local run completed 30/30 successful requests with approximately 172 req/s throughput and p95 latency around 111 ms.

## Alerting And Rollback

Prometheus loads `monitoring/alert_rules.yml`, which includes alerts for API Gateway downtime and high p95 latency. Rollback is handled by keeping each integration isolated behind Docker Compose services and environment variables: if the remote GPU tunnel fails, `ALLOW_LOCAL_FALLBACK=true` keeps the API available; if a local service fails, Docker Compose can restart that service independently and Prefect flow runs can be rerun after recovery.

## Required Questions

### 1. Architecture Trade-offs

The platform separates data ingestion, orchestration, vector search, model serving, and observability into independent services. Kafka decouples producers from downstream processing, Prefect makes batch movement visible and retryable, and FastAPI keeps user-facing inference isolated from infrastructure concerns. The main performance trade-off is the hybrid GPU tunnel: Colab/vLLM gives lower-cost GPU inference, but network latency and tunnel availability are less reliable than a fully local or managed GPU deployment. To balance this, the API Gateway includes graceful local fallback so the platform remains usable even if the GPU endpoint is unavailable.

### 2. Hybrid Local + GPU Failure Handling

The local stack does not hard-fail when the remote GPU endpoint disconnects. The API Gateway checks whether `VLLM_NGROK_URL` is configured and catches vLLM request failures. If the remote endpoint is missing or unreachable, it returns a local fallback response while still using Qdrant context when available. Embedding also has a deterministic fallback, so Qdrant ingestion can continue even when the remote embedding service is offline.

### 3. Kafka Event-driven Decoupling

Kafka acts as the boundary between data ingestion and downstream processing. The ingest script only publishes records to the `data.raw` topic; it does not need to know whether Prefect, Delta Lake, Redis, or Qdrant are currently running. The Prefect flow consumes from Kafka independently and writes batches to Delta. This reduces coupling, makes retries easier, and lets each integration point be verified separately.

### 4. Observability Implementation

The API Gateway exposes Prometheus metrics through `/metrics`, including request counts and latency histograms. Prometheus scrapes the API Gateway, and Grafana provisions a dashboard showing request rate, p95 latency, service health, memory, and request totals. Prefect UI provides orchestration-level visibility for flow runs and task runs. LangSmith is used to verify tracing by creating and listing a run in the configured project.

### 5. Service Crash and Graceful Degradation

If Qdrant is unavailable, the API Gateway catches vector search errors and continues without retrieved context. If vLLM is unavailable, the gateway returns a local fallback response instead of crashing. If Kafka is temporarily down, producers and consumers fail visibly, but the rest of the stack can remain available. Docker Compose restarts individual services independently, and Prefect makes failed pipeline stages visible for rerun after the dependency is restored.
