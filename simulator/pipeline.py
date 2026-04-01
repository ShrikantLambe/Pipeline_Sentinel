import sqlite3
import uuid
import json
from datetime import datetime, timedelta
import random
from .database import get_connection

# Define the DAG structure (simulates a typical dbt + Snowflake pipeline)
DAG_TASKS = [
    "extract_source_data",
    "validate_raw_schema",
    "load_to_staging",
    "run_dbt_staging_models",
    "run_dbt_mart_models",
    "run_dbt_tests",
    "update_snowflake_aggregates",
    "refresh_bi_cache",
]

EXPECTED_ROW_COUNTS = {
    "fct_sales": 15000,
    "fct_orders": 8500,
    "dim_customers": 4200,
}


class PipelineSimulator:
    def __init__(self):
        self.conn = get_connection()

    def start_run(self, dag_id: str = "retail_pipeline") -> str:
        run_id = f"run_{dag_id}_{uuid.uuid4().hex[:8]}"
        table = list(EXPECTED_ROW_COUNTS.keys())[0]
        expected = EXPECTED_ROW_COUNTS[table]

        c = self.conn.cursor()
        c.execute("""
            INSERT INTO pipeline_runs
            (dag_id, run_id, status, started_at, expected_row_count)
            VALUES (?, ?, 'running', ?, ?)
        """, (dag_id, run_id, datetime.now(), expected))

        # Insert task states
        for task in DAG_TASKS:
            c.execute("""
                INSERT INTO task_states (run_id, task_id, status)
                VALUES (?, ?, 'queued')
            """, (run_id, task))

        self.conn.commit()
        return run_id

    def run_task(self, run_id: str, task_id: str,
                 success: bool = True, error: str = None,
                 duration: float = None):
        duration = duration or random.uniform(2.0, 45.0)
        status = "success" if success else "failed"

        c = self.conn.cursor()
        c.execute("""
            UPDATE task_states
            SET status = ?, duration_seconds = ?, error_message = ?,
                updated_at = ?
            WHERE run_id = ? AND task_id = ?
        """, (status, duration, error, datetime.now(), run_id, task_id))
        self.conn.commit()

    def complete_run(self, run_id: str, actual_rows: int,
                     failure_type: str = None, failure_detail: str = None):
        status = "failed" if failure_type else "success"
        c = self.conn.cursor()
        c.execute("""
            UPDATE pipeline_runs
            SET status = ?, completed_at = ?, actual_row_count = ?,
                failure_type = ?, failure_detail = ?
            WHERE run_id = ?
        """, (status, datetime.now(), actual_rows,
              failure_type, failure_detail, run_id))
        self.conn.commit()

    def get_run_state(self, run_id: str) -> dict:
        c = self.conn.cursor()
        c.execute("SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,))
        row = c.fetchone()
        if not row:
            return None
        cols = [d[0] for d in c.description]
        run = dict(zip(cols, row))

        c.execute("SELECT * FROM task_states WHERE run_id = ?", (run_id,))
        tasks = [dict(zip([d[0] for d in c.description], r))
                 for r in c.fetchall()]
        run["tasks"] = tasks
        return run

    def get_recent_runs(self, limit: int = 10) -> list:
        c = self.conn.cursor()
        c.execute("""
            SELECT * FROM pipeline_runs
            ORDER BY created_at DESC LIMIT ?
        """, (limit,))
        cols = [d[0] for d in c.description]
        return [dict(zip(cols, row)) for row in c.fetchall()]
