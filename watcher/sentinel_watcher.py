"""
Sentinel Watcher — autonomous pipeline monitoring process.

Runs two loops in parallel:
  1. Reactive  — polls Airflow every WATCHER_POLL_INTERVAL_SECONDS for new
                 failed DAG runs and fires SentinelOrchestrator immediately.
  2. Proactive — every WATCHER_PROACTIVE_INTERVAL_SECONDS runs freshness and
                 row-count health checks regardless of Airflow failure events.

Start:
    python -m watcher.sentinel_watcher

Environment variables (see .env.example):
    WATCHER_DAG_IDS                  comma-separated list of DAG IDs to watch
    WATCHER_POLL_INTERVAL_SECONDS    default 300 (5 min)
    WATCHER_PROACTIVE_INTERVAL_SECONDS  default 900 (15 min)
    USE_AIRFLOW                      set true to enable Airflow polling
    SLACK_WEBHOOK_URL                set to enable Slack alerts
"""

import logging
import os
import signal
import sys
import threading
import time

from dotenv import load_dotenv

load_dotenv()

# ── Logging setup (must happen before any sentinel import) ──────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sentinel.watcher")

# ── Sentinel imports (after logging) ───────────────────────────────────────
from simulator.database import get_connection, init_db          # noqa: E402
from orchestrator.sentinel import SentinelOrchestrator          # noqa: E402
from watcher.alerting import send_slack_alert                   # noqa: E402
from watcher.proactive_monitor import (                         # noqa: E402
    check_pipeline_freshness,
    check_row_count_baselines,
)

USE_AIRFLOW         = os.getenv("USE_AIRFLOW", "false").lower() == "true"
POLL_INTERVAL       = int(os.getenv("WATCHER_POLL_INTERVAL_SECONDS", "300"))
PROACTIVE_INTERVAL  = int(os.getenv("WATCHER_PROACTIVE_INTERVAL_SECONDS", "900"))
DAG_IDS_ENV         = os.getenv("WATCHER_DAG_IDS", "")


class SentinelWatcher:
    """
    Long-running process that autonomously monitors pipelines and triggers
    SentinelOrchestrator for every new issue it discovers.
    """

    def __init__(self, dag_ids: list[str] = None):
        self.dag_ids           = dag_ids or [d.strip() for d in DAG_IDS_ENV.split(",") if d.strip()]
        self._stop             = threading.Event()
        self._last_proactive   = 0.0          # epoch timestamp of last proactive run
        self._processing_lock  = threading.Lock()  # prevent concurrent orchestrator runs

        if USE_AIRFLOW:
            from simulator.airflow_connector import AirflowConnector
            self.af = AirflowConnector()
        else:
            self.af = None

    # ── Public entry point ──────────────────────────────────────────────────

    def start(self):
        """Block and run the watcher loop until SIGINT/SIGTERM."""
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        init_db()
        log.info(
            "Sentinel Watcher started | DAGs: %s | poll: %ds | proactive: %ds | airflow: %s",
            self.dag_ids or "proactive-only",
            POLL_INTERVAL,
            PROACTIVE_INTERVAL,
            USE_AIRFLOW,
        )

        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:
                log.error("Watcher tick error: %s", exc, exc_info=True)

            # Sleep in short increments so SIGINT is responsive
            for _ in range(POLL_INTERVAL * 2):
                if self._stop.is_set():
                    break
                time.sleep(0.5)

        log.info("Sentinel Watcher stopped.")

    def _handle_signal(self, signum, _frame):
        log.info("Signal %s received — shutting down.", signum)
        self._stop.set()

    # ── One watcher cycle ───────────────────────────────────────────────────

    def _tick(self):
        # ── Reactive: poll Airflow for new failed runs ──────────────────────
        if USE_AIRFLOW and self.af and self.dag_ids:
            try:
                new_failures = self.af.poll_failed_runs(self.dag_ids)
                for dag_id, run_id in new_failures:
                    log.info("Airflow failure detected: %s / %s", dag_id, run_id)
                    try:
                        synced_id = self.af.sync_run_to_db(dag_id, run_id)
                        self._process_run(synced_id, source="airflow")
                    except Exception as exc:
                        log.error("Failed to sync Airflow run %s: %s", run_id, exc)
            except Exception as exc:
                log.error("Airflow poll failed: %s", exc)

        # ── Proactive: freshness + row count checks ─────────────────────────
        if time.time() - self._last_proactive >= PROACTIVE_INTERVAL:
            self._last_proactive = time.time()
            self._run_proactive_checks()

    def _run_proactive_checks(self):
        if not self.dag_ids:
            log.debug("No DAG IDs configured — skipping proactive checks.")
            return

        log.info("Running proactive checks for: %s", self.dag_ids)

        for run_id in check_pipeline_freshness(self.dag_ids):
            self._process_run(run_id, source="proactive_freshness")

        for run_id in check_row_count_baselines(self.dag_ids):
            self._process_run(run_id, source="proactive_rowcount")

    # ── Orchestrator runner ─────────────────────────────────────────────────

    def _process_run(self, run_id: str, source: str = "unknown"):
        """
        Run the full Sentinel agent chain for run_id, then send a Slack alert.
        Skips runs that have already been processed (idempotent).
        Runs are processed one at a time (processing_lock) to avoid overloading
        the Anthropic API with concurrent calls.
        """
        if self._is_already_processed(run_id):
            log.debug("run_id %s already processed — skipping.", run_id)
            return

        with self._processing_lock:
            # Re-check inside lock in case a concurrent call just processed it
            if self._is_already_processed(run_id):
                return

            log.info("[%s] Starting Sentinel for run_id=%s", source, run_id)

            def thought_cb(t):
                log.debug(
                    "[%s][%s] %s",
                    source,
                    t.get("agent", ""),
                    str(t.get("content", ""))[:120],
                )

            try:
                stop_event = threading.Event()
                orch   = SentinelOrchestrator(
                    thought_callback=thought_cb,
                    stop_event=stop_event,
                )
                result = orch.run(run_id)

                resolution  = result.get("resolution_status", "unknown")
                incident_id = result.get("incident_id")
                blast_radius = result.get("blast_radius")
                explanation = result.get("explanation", {})

                log.info(
                    "[%s] run_id=%s → %s (incident #%s, blast_radius=%s)",
                    source, run_id, resolution.upper(), incident_id, blast_radius,
                )

                send_slack_alert(
                    explanation=explanation,
                    run_id=run_id,
                    resolution_status=resolution,
                    blast_radius=blast_radius,
                    incident_id=incident_id,
                )

            except Exception as exc:
                log.error(
                    "[%s] Orchestrator failed for run_id=%s: %s",
                    source, run_id, exc, exc_info=True,
                )

    def _is_already_processed(self, run_id: str) -> bool:
        """True if an incident row already exists for this run_id."""
        conn = get_connection()
        c    = conn.cursor()
        c.execute("SELECT id FROM incidents WHERE run_id = ? LIMIT 1", (run_id,))
        found = c.fetchone() is not None
        conn.close()
        return found


# ── CLI entry point ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    watcher = SentinelWatcher()
    watcher.start()
