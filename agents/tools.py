import json
import os
import sqlite3
from datetime import datetime, timedelta
from simulator.database import get_connection
from simulator.pipeline import EXPECTED_ROW_COUNTS

_USE_AIRFLOW = os.getenv("USE_AIRFLOW", "false").lower() == "true"


def _airflow_dag_id(run_id: str) -> str | None:
    """Look up the dag_id for a given run_id — needed to call the Airflow API."""
    conn = get_connection()
    c    = conn.cursor()
    c.execute("SELECT dag_id FROM pipeline_runs WHERE run_id = ?", (run_id,))
    row  = c.fetchone()
    conn.close()
    return row[0] if row else None

# Baseline task durations in seconds (p50 from a healthy run)
TASK_DURATION_BASELINES = {
    "extract_source_data": 60,
    "validate_raw_schema": 10,
    "load_to_staging": 120,
    "run_dbt_staging_models": 180,
    "run_dbt_mart_models": 240,
    "run_dbt_tests": 90,
    "update_snowflake_aggregates": 60,
    "refresh_bi_cache": 30,
}

# ── Monitor Tools ──────────────────────────────────────────────────────────

def get_pipeline_status(run_id: str) -> dict:
    """Fetch current state of a pipeline run including all task statuses."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,))
    row = c.fetchone()
    if not row:
        return {"error": f"Run {run_id} not found"}
    cols = [d[0] for d in c.description]
    run = dict(zip(cols, row))

    c.execute("""
        SELECT task_id, status, duration_seconds, error_message
        FROM task_states WHERE run_id = ? ORDER BY id
    """, (run_id,))
    run["tasks"] = [dict(zip(["task_id","status","duration","error"], r))
                    for r in c.fetchall()]
    conn.close()
    return run


def check_row_count_anomaly(run_id: str,
                             threshold_pct: float = 0.15) -> dict:
    """
    Check if actual row count deviates from expected by more than threshold.
    Returns anomaly details or clean status.
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT expected_row_count, actual_row_count, dag_id
        FROM pipeline_runs WHERE run_id = ?
    """, (run_id,))
    row = c.fetchone()
    conn.close()

    if not row:
        return {"error": "Run not found"}

    expected, actual, dag_id = row
    if expected is None or actual is None:
        return {"status": "no_data", "message": "Row counts not yet available"}

    if expected == 0:
        # No historical baseline yet — flag as anomaly if any rows arrived,
        # and as a warning if zero (no data to validate against)
        return {
            "run_id": run_id,
            "dag_id": dag_id,
            "expected_rows": 0,
            "actual_rows": actual,
            "deviation_pct": None,
            "threshold_pct": threshold_pct * 100,
            "is_anomaly": actual > 0,
            "direction": "no_baseline",
            "note": "No expected row count baseline set for this run.",
        }

    deviation = abs(actual - expected) / expected
    is_anomaly = deviation > threshold_pct

    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "expected_rows": expected,
        "actual_rows": actual,
        "deviation_pct": round(deviation * 100, 2),
        "threshold_pct": threshold_pct * 100,
        "is_anomaly": is_anomaly,
        "direction": "under" if actual < expected else "over",
    }


def get_failed_tasks(run_id: str) -> list:
    """Return all failed tasks for a given run with their error messages."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT task_id, error_message, duration_seconds
        FROM task_states
        WHERE run_id = ? AND status = 'failed'
    """, (run_id,))
    cols = ["task_id", "error_message", "duration_seconds"]
    result = [dict(zip(cols, row)) for row in c.fetchall()]
    conn.close()
    return result


# ── Remediation Tools ──────────────────────────────────────────────────────

def retry_task(run_id: str, task_id: str,
               backoff_seconds: int = 0) -> dict:
    """
    Retry a failed task.
    - Airflow mode: clears the task instance via REST API so Airflow re-schedules it,
      then re-syncs run state from Airflow into our DB.
    - Simulator mode: probabilistic success (70%).
    """
    import time

    if backoff_seconds > 0:
        time.sleep(min(backoff_seconds, 2))  # cap for demo speed

    if _USE_AIRFLOW:
        from simulator.airflow_connector import AirflowConnector
        dag_id = _airflow_dag_id(run_id)
        if not dag_id:
            return {"task_id": task_id, "retry_success": False,
                    "error": f"run_id {run_id} not found in DB"}

        af     = AirflowConnector()
        result = af.clear_task_instance(dag_id, run_id, task_id)

        if result["success"]:
            # Mark task as running in our DB; full sync will happen on next poll
            conn = get_connection()
            c    = conn.cursor()
            c.execute("""
                UPDATE task_states
                SET status = 'running', error_message = NULL, updated_at = ?
                WHERE run_id = ? AND task_id = ?
            """, (datetime.now(), run_id, task_id))
            conn.commit()
            conn.close()

        return {
            "task_id":      task_id,
            "retry_success": result["success"],
            "new_status":   "running" if result["success"] else "failed",
            "airflow_response": result.get("result") or result.get("error"),
            "timestamp":    datetime.now().isoformat(),
        }

    # ── Simulator path ────────────────────────────────────────────────────
    import random
    success    = random.random() < 0.70
    conn       = get_connection()
    c          = conn.cursor()
    new_status = "success" if success else "failed"
    c.execute("""
        UPDATE task_states
        SET status = ?, updated_at = ?, error_message = ?
        WHERE run_id = ? AND task_id = ?
    """, (new_status, datetime.now(),
          None if success else "Retry failed — persisting error",
          run_id, task_id))
    if success:
        c.execute("""
            UPDATE pipeline_runs
            SET actual_row_count = CAST(expected_row_count * 0.98 AS INTEGER),
                status = 'remediated', failure_type = NULL
            WHERE run_id = ?
        """, (run_id,))
    conn.commit()
    conn.close()
    return {
        "task_id":      task_id,
        "retry_success": success,
        "new_status":   new_status,
        "timestamp":    datetime.now().isoformat(),
    }


def apply_dedup(run_id: str) -> dict:
    """Simulate applying deduplication logic to fix duplicate key failures."""
    import random
    success = random.random() < 0.85  # dedup usually works
    conn = get_connection()
    c = conn.cursor()
    if success:
        c.execute("""
            UPDATE pipeline_runs
            SET actual_row_count = expected_row_count,
                status = 'remediated', failure_type = NULL
            WHERE run_id = ?
        """, (run_id,))
    conn.commit()
    conn.close()
    return {
        "action": "dedup_applied",
        "success": success,
        "rows_removed": random.randint(800, 900) if success else 0,
    }


def reload_schema(run_id: str) -> dict:
    """Simulate reloading schema definition from source."""
    import random
    success = random.random() < 0.80
    conn = get_connection()
    c = conn.cursor()
    if success:
        c.execute("""
            UPDATE pipeline_runs
            SET status = 'remediated', failure_type = NULL
            WHERE run_id = ?
        """, (run_id,))
        c.execute("""
            UPDATE task_states SET status = 'success', error_message = NULL
            WHERE run_id = ? AND status = 'failed'
        """, (run_id,))
    conn.commit()
    conn.close()
    return {
        "action": "schema_reloaded",
        "success": success,
        "columns_synced": 24 if success else None,
    }


def extend_ingestion_window(run_id: str,
                             extra_hours: int = 3) -> dict:
    """Simulate extending the data collection window for late arrivals."""
    import random
    success = random.random() < 0.75
    conn = get_connection()
    c = conn.cursor()
    if success:
        c.execute("""
            UPDATE pipeline_runs
            SET actual_row_count = CAST(expected_row_count * 0.99 AS INTEGER),
                status = 'remediated', failure_type = NULL
            WHERE run_id = ?
        """, (run_id,))
    conn.commit()
    conn.close()
    return {
        "action": "window_extended",
        "extra_hours": extra_hours,
        "success": success,
        "new_row_count_estimate": "~15,000" if success else "still insufficient",
    }


def run_dbt_full_refresh(run_id: str) -> dict:
    """
    Re-run dbt models with --full-refresh.
    - Airflow mode: triggers a new DAG run with conf={"full_refresh": true}.
    - Simulator mode: probabilistic success (80%).
    """
    if _USE_AIRFLOW:
        from simulator.airflow_connector import AirflowConnector
        dag_id = _airflow_dag_id(run_id)
        if not dag_id:
            return {"action": "dbt_full_refresh", "success": False,
                    "error": f"run_id {run_id} not found in DB"}

        af     = AirflowConnector()
        result = af.trigger_dag_run(dag_id, conf={"full_refresh": True})
        return {
            "action":         "dbt_full_refresh",
            "success":        result["success"],
            "new_dag_run_id": result.get("dag_run_id"),
            "note":           "New DAG run triggered with full_refresh=True" if result["success"]
                              else result.get("error"),
        }

    # ── Simulator path ────────────────────────────────────────────────────
    import random
    success = random.random() < 0.80
    conn = get_connection()
    c    = conn.cursor()
    if success:
        c.execute("""
            UPDATE pipeline_runs
            SET actual_row_count = expected_row_count,
                status = 'remediated', failure_type = NULL
            WHERE run_id = ?
        """, (run_id,))
        c.execute("""
            UPDATE task_states SET status = 'success', error_message = NULL
            WHERE run_id = ? AND status = 'failed'
        """, (run_id,))
    conn.commit()
    conn.close()
    return {
        "action":         "dbt_full_refresh",
        "success":        success,
        "models_rebuilt": ["stg_orders", "fct_orders"] if success else [],
    }


# ── Detection Tools ────────────────────────────────────────────────────────

def check_task_duration_anomaly(run_id: str) -> dict:
    """
    Check if any tasks ran >3x their expected baseline duration.
    Catches SLA breaches even when tasks eventually succeed.
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT task_id, status, duration_seconds
        FROM task_states WHERE run_id = ?
    """, (run_id,))
    tasks = c.fetchall()
    conn.close()

    anomalies = []
    for task_id, status, duration in tasks:
        if status == "queued" or duration is None:
            continue
        baseline = TASK_DURATION_BASELINES.get(task_id)
        if baseline and duration > baseline * 3:
            anomalies.append({
                "task_id": task_id,
                "duration_seconds": round(duration, 1),
                "baseline_seconds": baseline,
                "ratio": round(duration / baseline, 1),
                "status": status,
            })

    return {
        "run_id": run_id,
        "duration_anomalies": anomalies,
        "has_anomaly": len(anomalies) > 0,
    }


def check_zombie_run(run_id: str, stale_minutes: int = 30) -> dict:
    """
    Detect tasks stuck in 'running' state past their expected duration.
    A zombie task means the process died without updating the DB.
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT task_id, status, updated_at
        FROM task_states
        WHERE run_id = ? AND status = 'running'
    """, (run_id,))
    running_tasks = c.fetchall()
    conn.close()

    now = datetime.now()
    zombies = []
    for task_id, status, updated_at_str in running_tasks:
        try:
            updated_at = datetime.fromisoformat(str(updated_at_str))
            elapsed_minutes = (now - updated_at).total_seconds() / 60
            if elapsed_minutes > stale_minutes:
                zombies.append({
                    "task_id": task_id,
                    "stale_minutes": round(elapsed_minutes, 1),
                    "last_updated": updated_at_str,
                })
        except Exception:
            pass

    return {
        "run_id": run_id,
        "zombie_tasks": zombies,
        "has_zombie": len(zombies) > 0,
    }
