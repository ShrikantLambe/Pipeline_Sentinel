"""
FastAPI webhook server — receives Airflow on_failure_callback POSTs and
triggers SentinelOrchestrator immediately (zero polling delay).

Start alongside the watcher:
    uvicorn watcher.webhook_server:app --host 0.0.0.0 --port 8765

Configure each Airflow DAG to call on failure:
    # dags/your_dag.py
    import requests

    def sentinel_callback(context):
        try:
            requests.post(
                "http://sentinel-host:8765/webhook/failure",
                json={
                    "dag_id": context["dag"].dag_id,
                    "run_id": context["run_id"],
                },
                timeout=5,
            )
        except Exception:
            pass   # never block the Airflow worker

    dag = DAG(
        "your_dag",
        on_failure_callback=sentinel_callback,
        ...
    )

Endpoints:
    GET  /health             — liveness probe
    POST /webhook/failure    — Airflow on_failure_callback payload
    POST /webhook/airflow    — Airflow native callback format (dag_id in body)
"""

import logging
import os
import threading

from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("sentinel.webhook")

from fastapi import BackgroundTasks, FastAPI  # noqa: E402
from pydantic import BaseModel                # noqa: E402

from simulator.database import init_db        # noqa: E402
from watcher.sentinel_watcher import SentinelWatcher  # noqa: E402

USE_AIRFLOW = os.getenv("USE_AIRFLOW", "false").lower() == "true"

# Shared watcher instance — reuses _is_already_processed and _process_run logic
_watcher = SentinelWatcher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    log.info("Sentinel webhook server ready (USE_AIRFLOW=%s).", USE_AIRFLOW)
    yield


app = FastAPI(title="Pipeline Sentinel Webhook", version="1.0.0", lifespan=lifespan)


# ── Request models ──────────────────────────────────────────────────────────

class FailureEvent(BaseModel):
    dag_id: str
    run_id: str


class AirflowCallbackEvent(BaseModel):
    """
    Matches the body Airflow sends when you configure an HTTP callback via
    the Airflow Notifier or a custom on_failure_callback using requests.
    """
    dag_id:     str
    run_id:     str
    task_id:    str | None = None
    state:      str | None = None
    try_number: int | None = None


# ── Endpoints ───────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Liveness probe — always returns 200 if the server is up."""
    return {"status": "ok"}


@app.post("/webhook/failure")
def receive_failure(event: FailureEvent, background_tasks: BackgroundTasks):
    """
    Primary endpoint — called by Airflow on_failure_callback.
    Accepts: {"dag_id": "...", "run_id": "..."}
    Syncs from Airflow (if USE_AIRFLOW=true), then fires Sentinel asynchronously.
    """
    dag_id = event.dag_id
    run_id = event.run_id
    log.info("Webhook /failure: dag_id=%s run_id=%s", dag_id, run_id)

    background_tasks.add_task(_handle_failure, dag_id, run_id)
    return {"status": "accepted", "dag_id": dag_id, "run_id": run_id}


@app.post("/webhook/airflow")
def receive_airflow_callback(event: AirflowCallbackEvent, background_tasks: BackgroundTasks):
    """
    Alternative endpoint for Airflow's native HTTP notifier format.
    Accepts the same payload as /webhook/failure with optional extra fields.
    """
    log.info(
        "Webhook /airflow: dag_id=%s run_id=%s task_id=%s state=%s",
        event.dag_id, event.run_id, event.task_id, event.state,
    )
    background_tasks.add_task(_handle_failure, event.dag_id, event.run_id)
    return {"status": "accepted", "dag_id": event.dag_id, "run_id": event.run_id}


# ── Background handler ──────────────────────────────────────────────────────

def _handle_failure(dag_id: str, run_id: str):
    """
    Sync from Airflow if enabled, then hand off to SentinelWatcher._process_run().
    Runs in FastAPI's background task thread pool — errors are logged, not raised.
    """
    try:
        if USE_AIRFLOW:
            from simulator.airflow_connector import AirflowConnector
            af = AirflowConnector()
            run_id = af.sync_run_to_db(dag_id, run_id)

        _watcher._process_run(run_id, source="webhook")

    except Exception as exc:
        log.error(
            "Webhook handler failed for dag_id=%s run_id=%s: %s",
            dag_id, run_id, exc, exc_info=True,
        )


# ── Run directly ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "watcher.webhook_server:app",
        host="0.0.0.0",
        port=int(os.getenv("WEBHOOK_PORT", "8765")),
        reload=False,
    )
