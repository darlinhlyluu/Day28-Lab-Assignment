import os
import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field


app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)

MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4")
VLLM_URL = (os.environ.get("VLLM_URL") or os.environ.get("VLLM_NGROK_URL") or "").rstrip("/")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333").rstrip("/")
ALLOW_LOCAL_FALLBACK = os.environ.get("ALLOW_LOCAL_FALLBACK", "true").lower() in {"1", "true", "yes"}
VECTOR_SIZE = int(os.environ.get("VECTOR_SIZE", "384"))


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1)
    embedding: list[float] | None = None


def normalize_embedding(embedding: list[float] | None) -> list[float]:
    if not embedding:
        return [0.0] * VECTOR_SIZE
    if len(embedding) == VECTOR_SIZE:
        return embedding
    if len(embedding) > VECTOR_SIZE:
        return embedding[:VECTOR_SIZE]
    return embedding + [0.0] * (VECTOR_SIZE - len(embedding))


async def search_qdrant(embedding: list[float]) -> list[dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(
                f"{QDRANT_URL}/collections/documents/points/search",
                json={"vector": embedding, "limit": 3, "with_payload": True},
            )
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json().get("result", [])
    except (httpx.HTTPError, ValueError):
        return []


def fallback_answer(query: str, context: list[dict[str, Any]], reason: str) -> dict[str, Any]:
    if not ALLOW_LOCAL_FALLBACK:
        raise HTTPException(status_code=503, detail=reason)

    context_count = len(context)
    return {
        "answer": (
            "Local fallback response: the gateway is healthy, vector search returned "
            f"{context_count} context item(s), and Kaggle vLLM is not available yet. "
            f"Query received: {query}"
        ),
        "model": "local-fallback",
        "fallback_reason": reason,
    }


async def call_vllm(query: str, context: list[dict[str, Any]]) -> dict[str, Any]:
    if not VLLM_URL:
        return fallback_answer(query, context, "VLLM_URL is not configured")

    prompt = f"Context: {context}\n\nQuery: {query}"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{VLLM_URL}/v1/chat/completions",
                json={
                    "model": MODEL_NAME,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        response.raise_for_status()
        result = response.json()
        return {
            "answer": result["choices"][0]["message"]["content"],
            "model": result.get("model", MODEL_NAME),
        }
    except (httpx.HTTPError, KeyError, IndexError, ValueError) as exc:
        return fallback_answer(query, context, f"vLLM request failed: {exc}")


@app.post("/api/v1/chat")
async def chat(request: ChatRequest):
    start = time.time()
    embedding = normalize_embedding(request.embedding)
    context = await search_qdrant(embedding)
    result = await call_vllm(request.query, context)
    latency = (time.time() - start) * 1000

    return {
        "answer": result["answer"],
        "latency_ms": round(latency, 2),
        "model": result["model"],
        "context_count": len(context),
        **({"fallback_reason": result["fallback_reason"]} if "fallback_reason" in result else {}),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model": MODEL_NAME,
        "vllm_configured": bool(VLLM_URL),
        "qdrant_url": QDRANT_URL,
    }
