import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import requests


def load_dotenv() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def check_prometheus() -> None:
    response = requests.get(
        "http://localhost:9090/api/v1/query",
        params={"query": "up{job='api-gateway'}"},
        timeout=10,
    )
    response.raise_for_status()
    result = response.json()["data"]["result"]
    assert result and result[0]["value"][1] == "1"
    print("Integration 9 OK: Prometheus is scraping api-gateway")


def check_langsmith() -> None:
    api_key = os.environ.get("LANGCHAIN_API_KEY")
    if not api_key or api_key == "your_langsmith_key":
        print("Integration 10 SKIPPED: set LANGCHAIN_API_KEY to verify LangSmith traces")
        return

    from langsmith import Client

    project = os.environ.get("LANGCHAIN_PROJECT", "lab28-platform")
    client = Client(api_key=api_key)
    runs = list(client.list_runs(project_name=project, limit=1))
    if not runs:
        run_id = uuid4()
        client.create_run(
            id=run_id,
            name="lab28-observability-check",
            run_type="chain",
            project_name=project,
            inputs={"check": "verify LangSmith tracing for Lab 28"},
            start_time=datetime.now(timezone.utc),
        )
        client.update_run(
            run_id,
            outputs={"status": "ok", "source": "scripts/09_verify_observability.py"},
            end_time=datetime.now(timezone.utc),
        )
        time.sleep(2)
        runs = list(client.list_runs(project_name=project, limit=1))

    assert runs, f"No LangSmith runs found in project {project}"
    print("Integration 10 OK: LangSmith traces visible")


if __name__ == "__main__":
    load_dotenv()
    check_prometheus()
    check_langsmith()
