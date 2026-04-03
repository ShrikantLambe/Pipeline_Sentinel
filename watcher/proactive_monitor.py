"""
Proactive health checks — run on a schedule independent of Airflow failure events.

Two check types:
  1. Pipeline freshness  — has each DAG produced a successful run recently?
  2. Row count baseline  — is the latest actual row count within threshold of expected?

When an anomaly is detected, a synthetic pipeline_runs row is written so that
SentinelOrchestrator.run(run_id) can process it through the full agent chain
without any changes to the orchestrator itself.
"""

import logging
import os
import uuid
from datetime import datetime, timedelta

from simulator.database import get_connection

log = logging.getLogger("sentinel.proactive")

FRESHNESS_MAX_AGE_HOURS  = float(os.getenv("FRESHNESS_MAX_AGE_HOURS", "6"))
ROW_COUNT_THRESHOLD_PCT  = float(os.getenv("ROW_COUNT_THRESHOLD_PCT", "0.20"))


# ── Freshness check ─────────────────────────────────────────────────────────

def check_pipeline_freshness(dag_ids: list[str]) -> list[str]:
    """
    For each dag_id, verify a successful run completed within FRESHNESS_MAX_AGE_HOURS.

    If stale (or no successful run exists), a synthetic 'failed' pipeline_runs row
    is inserted with failure_type='late_arrival' so Sentinel can investigate.

    Returns list of newly created run_ids.
    """
    conn    = get_connection()
    c       = conn.cursor()
    cutoff  = datetime.now() - timedelta(hours=FRESHNESS_MAX_AGE_HOURS)
    new_ids = []

    for dag_id in dag_ids:
        c.execute(
            """
            SELECT MAX(completed_at) FROM pipeline_runs
            WHERE dag_id = ? AND status = 'success'
            """,
            (dag_id,),
        )
        row         = c.fetchone()
        last_success_raw = row[0] if row and row[0] else None

        # Parse string → datetime (MAX() aggregate bypasses SQLite type converters)
        last_success = None
        if last_success_raw:
            try:
                last_success = (
                    last_success_raw if isinstance(last_success_raw, datetime)
                    else datetime.fromisoformat(str(last_success_raw))
                )
            except Exception:
                last_success = None

        # Already fresh — skip
        if last_success and last_success >= cutoff:
            log.debug("Freshness OK for %s (last success: %s)", dag_id, last_success)
            continue

        # Stale or never run — create synthetic failure
        if last_success:
            age_h  = (datetime.now() - last_success).total_seconds() / 3600
            detail = (
                f"Pipeline {dag_id} has not produced a successful run in "
                f"{age_h:.1f}h (threshold: {FRESHNESS_MAX_AGE_HOURS}h)"
            )
        else:
            detail = f"Pipeline {dag_id} has no recorded successful run."

        run_id = f"proactive_freshness_{dag_id}_{uuid.uuid4().hex[:8]}"
        c.execute(
            """
            INSERT INTO pipeline_runs
                (dag_id, run_id, status, started_at, completed_at,
                 failure_type, failure_detail)
            VALUES (?, ?, 'failed', ?, ?, 'late_arrival', ?)
            """,
            (dag_id, run_id, datetime.now(), datetime.now(), detail),
        )
        log.warning("Freshness anomaly → created synthetic run %s for %s", run_id, dag_id)
        new_ids.append(run_id)

    conn.commit()
    conn.close()
    return new_ids


# ── Row count baseline check ─────────────────────────────────────────────────

def check_row_count_baselines(dag_ids: list[str]) -> list[str]:
    """
    Compare the most recent actual_row_count for each dag_id against its
    expected_row_count. If deviation exceeds ROW_COUNT_THRESHOLD_PCT, create
    a synthetic failed run with failure_type='partial_load'.

    Returns list of newly created run_ids.
    """
    conn    = get_connection()
    c       = conn.cursor()
    new_ids = []

    for dag_id in dag_ids:
        c.execute(
            """
            SELECT actual_row_count, expected_row_count, run_id
            FROM pipeline_runs
            WHERE dag_id = ?
              AND actual_row_count IS NOT NULL
              AND expected_row_count IS NOT NULL
              AND expected_row_count > 0
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            (dag_id,),
        )
        row = c.fetchone()
        if not row:
            continue

        actual, expected, source_run_id = row
        deviation = abs(actual - expected) / expected

        if deviation <= ROW_COUNT_THRESHOLD_PCT:
            log.debug(
                "Row count OK for %s: actual=%d expected=%d (%.1f%%)",
                dag_id, actual, expected, deviation * 100,
            )
            continue

        detail = (
            f"Row count anomaly in {dag_id}: expected ~{expected:,}, "
            f"got {actual:,} ({deviation * 100:.1f}% deviation, "
            f"threshold {ROW_COUNT_THRESHOLD_PCT * 100:.0f}%)"
        )
        run_id = f"proactive_rowcount_{dag_id}_{uuid.uuid4().hex[:8]}"
        c.execute(
            """
            INSERT INTO pipeline_runs
                (dag_id, run_id, status, started_at, completed_at,
                 expected_row_count, actual_row_count,
                 failure_type, failure_detail)
            VALUES (?, ?, 'failed', ?, ?, ?, ?, 'partial_load', ?)
            """,
            (
                dag_id, run_id,
                datetime.now(), datetime.now(),
                expected, actual,
                detail,
            ),
        )
        log.warning(
            "Row count anomaly → created synthetic run %s for %s (%.1f%% deviation)",
            run_id, dag_id, deviation * 100,
        )
        new_ids.append(run_id)

    conn.commit()
    conn.close()
    return new_ids
