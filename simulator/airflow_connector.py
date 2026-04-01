"""
Airflow REST API connector for Pipeline Sentinel.

Reads real DAG run state from a local Airflow instance and syncs it into
our SQLite schema so the agent pipeline can work against real data unchanged.

Airflow REST API docs: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
Default local URL: http://localhost:8080  (admin / admin)
"""

import os
import json
import requests
from datetime import datetime
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

AIRFLOW_URL      = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")


# ── Airflow state → our schema ─────────────────────────────────────────────

_TASK_STATE_MAP = {
    "success":         "success",
    "failed":          "failed",
    "running":         "running",
    "queued":          "queued",
    "upstream_failed": "upstream_failed",
    "skipped":         "skipped",
    "deferred":        "running",
    "scheduled":       "queued",
    "restarting":      "running",
    "removed":         "skipped",
    "sensing":         "running",
    "none":            "queued",
}

_RUN_STATE_MAP = {
    "success": "success",
    "failed":  "failed",
    "running": "running",
    "queued":  "running",
}


class AirflowConnector:
    """
    Thin wrapper around the Airflow REST API (v1).
    All methods return plain dicts / lists and never raise — errors are
    surfaced as {"error": "..."} so callers can decide how to handle them.
    """

    def __init__(self):
        self.base    = AIRFLOW_URL.rstrip("/") + "/api/v1"
        self.session = requests.Session()
        self.session.auth    = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        self.session.headers.update({"Content-Type": "application/json"})

    # ── Low-level helpers ─────────────────────────────────────────────────

    def _get(self, path: str, params: dict = None) -> dict:
        resp = self.session.get(f"{self.base}{path}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict = None) -> dict:
        resp = self.session.post(f"{self.base}{path}", json=body or {}, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, body: dict = None) -> dict:
        resp = self.session.patch(f"{self.base}{path}", json=body or {}, timeout=10)
        resp.raise_for_status()
        return resp.json()

    # ── Connectivity ──────────────────────────────────────────────────────

    def ping(self) -> dict:
        """
        Test connection to Airflow.
        Returns {"ok": True, "version": "2.x.x"} or {"ok": False, "error": "..."}.
        """
        try:
            data = self._get("/health")
            # /health returns {"metadatabase": {...}, "scheduler": {...}}
            scheduler_ok = data.get("scheduler", {}).get("status") == "healthy"
            return {"ok": scheduler_ok, "health": data}
        except requests.ConnectionError:
            return {"ok": False, "error": f"Cannot reach Airflow at {AIRFLOW_URL}. Is it running?"}
        except requests.HTTPError as e:
            return {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── DAG discovery ─────────────────────────────────────────────────────

    def list_dags(self) -> list[str]:
        """Return list of active DAG IDs from this Airflow instance."""
        try:
            data = self._get("/dags", params={"only_active": True, "limit": 100})
            return [d["dag_id"] for d in data.get("dags", [])]
        except Exception as e:
            return []

    # ── DAG runs ──────────────────────────────────────────────────────────

    def get_dag_runs(self, dag_id: str, limit: int = 10,
                     states: list = None) -> list[dict]:
        """
        Fetch recent DAG runs for a DAG, newest first.
        states: filter list e.g. ["failed", "running"]
        """
        params = {"limit": limit, "order_by": "-start_date"}
        if states:
            # Airflow API accepts repeated `state` params
            params["state"] = states
        try:
            data = self._get(f"/dags/{dag_id}/dagRuns", params=params)
            return data.get("dag_runs", [])
        except Exception as e:
            return []

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        """Fetch a single DAG run by ID."""
        try:
            return self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        except Exception as e:
            return {"error": str(e)}

    # ── Task instances ────────────────────────────────────────────────────

    def get_task_instances(self, dag_id: str, dag_run_id: str) -> list[dict]:
        """Fetch all task instances for a DAG run."""
        try:
            data = self._get(
                f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
            )
            return data.get("task_instances", [])
        except Exception as e:
            return []

    def get_task_log(self, dag_id: str, dag_run_id: str,
                     task_id: str, try_number: int = 1) -> str:
        """
        Fetch the last 60 lines of a task's log.
        Returns empty string on any error (logs are best-effort).
        """
        try:
            resp = self.session.get(
                f"{self.base}/dags/{dag_id}/dagRuns/{dag_run_id}"
                f"/taskInstances/{task_id}/logs/{try_number}",
                headers={"Accept": "text/plain"},
                timeout=15,
            )
            if resp.status_code == 200:
                lines = resp.text.splitlines()
                return "\n".join(lines[-60:])
        except Exception:
            pass
        return ""

    def get_xcom(self, dag_id: str, dag_run_id: str,
                 task_id: str, key: str) -> any:
        """
        Read a single XCom value pushed by a task.
        Returns None if not found or on error.
        """
        try:
            data = self._get(
                f"/dags/{dag_id}/dagRuns/{dag_run_id}"
                f"/taskInstances/{task_id}/xcomEntries/{key}"
            )
            return data.get("value")
        except Exception:
            return None

    # ── Remediation actions ───────────────────────────────────────────────

    def clear_task_instance(self, dag_id: str, dag_run_id: str,
                             task_id: str) -> dict:
        """
        Clear a task instance so Airflow schedules it for re-execution.
        Equivalent to clicking "Clear" in the Airflow UI.
        """
        try:
            result = self._post(
                f"/dags/{dag_id}/dagRuns/{dag_run_id}"
                f"/taskInstances/{task_id}/clear",
                body={"dry_run": False, "reset_dag_runs": False},
            )
            return {"success": True, "result": result}
        except requests.HTTPError as e:
            return {"success": False,
                    "error": f"HTTP {e.response.status_code}: {e.response.text[:300]}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def trigger_dag_run(self, dag_id: str, conf: dict = None) -> dict:
        """
        Trigger a new DAG run, optionally passing a conf dict
        (e.g. {"full_refresh": true, "extend_window_hours": 3}).
        """
        try:
            body   = {"conf": conf or {}}
            result = self._post(f"/dags/{dag_id}/dagRuns", body=body)
            return {"success": True,
                    "dag_run_id": result.get("dag_run_id"),
                    "state": result.get("state")}
        except requests.HTTPError as e:
            return {"success": False,
                    "error": f"HTTP {e.response.status_code}: {e.response.text[:300]}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── DB sync ───────────────────────────────────────────────────────────

    def sync_run_to_db(self, dag_id: str, dag_run_id: str) -> str:
        """
        Pull a DAG run + its task instances from Airflow and write them into
        our SQLite schema.  Returns the local run_id (== dag_run_id).

        Safe to call multiple times — uses DELETE+INSERT for task_states
        and INSERT OR REPLACE for pipeline_runs.
        """
        from simulator.database import get_connection

        run_data = self.get_dag_run(dag_id, dag_run_id)
        if "error" in run_data:
            raise RuntimeError(f"Cannot fetch run {dag_run_id}: {run_data['error']}")

        task_instances = self.get_task_instances(dag_id, dag_run_id)

        # ── Map run-level state ──────────────────────────────────────────
        run_status   = _RUN_STATE_MAP.get(run_data.get("state", ""), "running")
        started_at   = _parse_ts(run_data.get("start_date"))
        completed_at = _parse_ts(run_data.get("end_date"))

        # ── Detect failure type ──────────────────────────────────────────
        failed_tasks = [t for t in task_instances if t.get("state") == "failed"]
        failure_type, failure_detail = self._detect_failure_type(
            dag_id, dag_run_id, failed_tasks
        )

        # ── Row counts from XCom (best-effort) ──────────────────────────
        expected_rows, actual_rows = None, None
        for t in task_instances:
            tid = t.get("task_id", "")
            if any(k in tid for k in ("load", "staging", "ingest")):
                val = self.get_xcom(dag_id, dag_run_id, tid, "row_count")
                if val is not None:
                    try:
                        actual_rows = int(val)
                    except Exception:
                        pass
                exp_val = self.get_xcom(dag_id, dag_run_id, tid, "expected_row_count")
                if exp_val is not None:
                    try:
                        expected_rows = int(exp_val)
                    except Exception:
                        pass

        run_id = dag_run_id  # use Airflow's ID directly as our primary key

        conn = get_connection()
        c    = conn.cursor()

        # Upsert pipeline_run
        c.execute("""
            INSERT INTO pipeline_runs
                (dag_id, run_id, status, started_at, completed_at,
                 expected_row_count, actual_row_count, failure_type, failure_detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                status           = excluded.status,
                completed_at     = excluded.completed_at,
                actual_row_count = COALESCE(excluded.actual_row_count, actual_row_count),
                failure_type     = excluded.failure_type,
                failure_detail   = excluded.failure_detail
        """, (dag_id, run_id, run_status, started_at, completed_at,
              expected_rows, actual_rows, failure_type, failure_detail))

        # Replace task states entirely (clean sync)
        c.execute("DELETE FROM task_states WHERE run_id = ?", (run_id,))
        for t in task_instances:
            task_id  = t.get("task_id", "")
            state    = _TASK_STATE_MAP.get(t.get("state") or "queued", "queued")
            duration = t.get("duration")  # float seconds or None

            # Grab error snippet from log for failed tasks
            error_msg = None
            if state == "failed":
                log_tail  = self.get_task_log(dag_id, dag_run_id, task_id,
                                              try_number=t.get("try_number", 1))
                error_msg = _extract_error_from_log(log_tail)

            c.execute("""
                INSERT INTO task_states (run_id, task_id, status, duration_seconds, error_message)
                VALUES (?, ?, ?, ?, ?)
            """, (run_id, task_id, state, duration, error_msg))

        conn.commit()
        conn.close()
        return run_id

    def _detect_failure_type(self, dag_id: str, dag_run_id: str,
                              failed_tasks: list) -> tuple:
        """
        Map Airflow task failure → our failure taxonomy using log heuristics.
        Returns (failure_type, failure_detail).
        """
        if not failed_tasks:
            return None, None

        first_failed = failed_tasks[0]
        task_id  = first_failed.get("task_id", "").lower()
        log      = self.get_task_log(dag_id, dag_run_id,
                                      first_failed.get("task_id", ""),
                                      try_number=first_failed.get("try_number", 1))
        log_l    = log.lower()
        detail   = _extract_error_from_log(log) or f"Task {task_id} failed"

        # Auth / credential errors
        if any(k in log_l for k in (
            "permissiondenied", "permission denied", "access denied",
            "credentials expired", "unauthorized", "403", "invalid credentials",
        )):
            return "auth_failure", detail

        # Timeout / connectivity
        if any(k in log_l for k in (
            "timeout", "timed out", "connection dropped",
            "connectionerror", "connection refused", "etimedout",
        )):
            return "upstream_timeout", detail

        # Schema issues
        if any(k in log_l for k in (
            "schemavalidationerror", "schema mismatch", "missing column",
            "column not found", "incompatible schema", "unexpected field",
        )):
            return "schema_drift", detail

        # Duplicate keys / unique constraints
        if any(k in log_l for k in (
            "uniquetestfailure", "duplicate key", "unique constraint",
            "integrity error", "duplicate record",
        )):
            return "duplicate_keys", detail

        # dbt failures
        if any(k in log_l for k in (
            "dbt", "not_null test", "unique test",
            "model failed", "compilation error",
        )):
            if "staging" in task_id or "stg" in task_id:
                return "duplicate_keys", detail
            return "dbt_model_failure", detail

        # Row count / late data
        if any(k in log_l for k in (
            "row count", "rowcount", "below threshold",
            "0 rows", "no rows loaded", "late arrival",
        )):
            return "late_arrival", detail

        # Partial load
        if any(k in log_l for k in ("partial", "shard", "partition failed")):
            return "partial_load", detail

        # Fall back to task-name heuristics
        if any(k in task_id for k in ("extract", "source", "ingest")):
            return "upstream_timeout", detail
        if any(k in task_id for k in ("schema", "validate")):
            return "schema_drift", detail
        if any(k in task_id for k in ("load", "staging")):
            return "late_arrival", detail
        if any(k in task_id for k in ("dbt", "mart", "model", "test")):
            return "dbt_model_failure", detail

        return "upstream_timeout", detail

    # ── Polling ───────────────────────────────────────────────────────────

    def poll_failed_runs(self, dag_ids: list) -> list[tuple]:
        """
        Check Airflow for recently failed runs that aren't in our DB yet.
        Returns list of (dag_id, dag_run_id) tuples ready to be synced.
        """
        from simulator.database import get_connection

        conn = get_connection()
        c    = conn.cursor()
        c.execute("SELECT run_id FROM pipeline_runs")
        known = {row[0] for row in c.fetchall()}
        conn.close()

        new_failures = []
        for dag_id in dag_ids:
            for run in self.get_dag_runs(dag_id, limit=10, states=["failed"]):
                dag_run_id = run.get("dag_run_id", "")
                if dag_run_id and dag_run_id not in known:
                    new_failures.append((dag_id, dag_run_id))

        return new_failures


# ── Helpers ────────────────────────────────────────────────────────────────

def _parse_ts(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _extract_error_from_log(log: str) -> str | None:
    """
    Pull the most relevant error line from a task log tail.
    Prefers lines containing exception/error keywords, last one wins.
    """
    if not log:
        return None
    keywords = ("error", "exception", "failed", "traceback", "critical")
    error_lines = [
        line.strip() for line in log.splitlines()
        if any(k in line.lower() for k in keywords)
        and len(line.strip()) > 10
    ]
    if error_lines:
        return error_lines[-1][:500]
    # Return last non-empty log line as fallback
    non_empty = [l.strip() for l in log.splitlines() if l.strip()]
    return non_empty[-1][:500] if non_empty else None
