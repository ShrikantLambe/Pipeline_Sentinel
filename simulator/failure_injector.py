import json
import random
from datetime import datetime
from .pipeline import PipelineSimulator, DAG_TASKS
from .database import get_connection

FAILURE_MODES = {
    "schema_drift": {
        "description": "Column added/removed in source — schema mismatch on load",
        "affected_task": "validate_raw_schema",
        "error": "SchemaValidationError: Expected 24 columns, found 21. Missing: ['promo_code', 'region_id', 'channel_tag']",
        "row_count_impact": 0,        # rows not loaded at all
        "remediation_hint": "Reload schema definition and retry validation",
    },
    "late_arrival": {
        "description": "Source data arrived 3 hours late — row count well below threshold",
        "affected_task": "load_to_staging",
        "error": "RowCountWarning: 3,240 rows loaded vs 15,000 expected (78% below threshold)",
        "row_count_impact": 0.22,     # only 22% of expected rows
        "remediation_hint": "Extend ingestion window and re-poll source",
    },
    "duplicate_keys": {
        "description": "Upstream system sent duplicate records — unique key constraint violated",
        "affected_task": "run_dbt_staging_models",
        "error": "UniqueTestFailure: dbt test unique_fct_sales_order_id failed. 847 duplicate keys detected.",
        "row_count_impact": 1.12,     # 12% more rows than expected (dupes)
        "remediation_hint": "Apply QUALIFY ROW_NUMBER dedup and rerun dbt models",
    },
    "upstream_timeout": {
        "description": "Source system query exceeded SLA — connection timed out",
        "affected_task": "extract_source_data",
        "error": "SourceTimeoutError: Query to orders_db exceeded 300s SLA. Connection dropped.",
        "row_count_impact": 0,        # no rows loaded
        "remediation_hint": "Retry with exponential backoff — transient lock likely",
    },
    "dbt_model_failure": {
        "description": "dbt model assertion failed — unexpected nulls in non-null column",
        "affected_task": "run_dbt_mart_models",
        "error": "dbtTestFailure: not_null_fct_orders_customer_id failed. 312 null values in customer_id.",
        "row_count_impact": 0.97,     # rows loaded but model broken
        "remediation_hint": "Re-run dbt with --full-refresh flag and check upstream ref()",
    },
    "partial_load": {
        "description": "Only 6 of 8 source shards loaded — row count within threshold but data is incomplete",
        "affected_task": "load_to_staging",
        "error": "PartialLoadWarning: 6/8 shards completed. 2 shards timed out (shard_06, shard_07). Rows loaded: 12,900 vs 15,000 expected.",
        "row_count_impact": 0.86,     # 14% below — just within the 15% threshold so row count check alone won't catch it
        "remediation_hint": "Re-trigger missing shards (shard_06, shard_07) and merge results into staging",
    },
    "auth_failure": {
        "description": "Service account credentials expired — PermissionDenied on source extract",
        "affected_task": "extract_source_data",
        "error": "PermissionDeniedError: Service account svc-airflow@prod lacks SELECT on orders_db.raw_transactions. Credentials expired 2h ago.",
        "row_count_impact": 0,        # nothing loaded
        "remediation_hint": "Escalate to platform team — credential rotation required, cannot be auto-remediated",
    },
}


class FailureInjector:
    def __init__(self, simulator: PipelineSimulator):
        self.sim = simulator

    def inject(self, run_id: str,
               failure_type: str = None) -> dict:
        """
        Inject a specific failure (or random if not specified).
        Runs all tasks up to the failure point as success,
        then fails the affected task.
        Returns the failure config dict.
        """
        if failure_type is None:
            failure_type = random.choice(list(FAILURE_MODES.keys()))

        failure = FAILURE_MODES[failure_type]
        affected_task = failure["affected_task"]

        # Run tasks before the failure as success; fail the affected task;
        # mark all downstream tasks as upstream_failed.
        past_failure = False
        for task in DAG_TASKS:
            if task == affected_task:
                self.sim.run_task(run_id, task,
                                  success=False,
                                  error=failure["error"],
                                  duration=random.uniform(60, 300))
                past_failure = True
            elif past_failure:
                # Downstream tasks never ran — mark as upstream_failed
                c = self.sim.conn.cursor()
                c.execute("""
                    UPDATE task_states
                    SET status = 'upstream_failed',
                        error_message = 'Skipped: upstream task failed',
                        updated_at = ?
                    WHERE run_id = ? AND task_id = ?
                """, (datetime.now(), run_id, task))
                self.sim.conn.commit()
            else:
                self.sim.run_task(run_id, task,
                                  success=True,
                                  duration=random.uniform(2, 30))

        # Calculate actual rows based on impact multiplier
        from .pipeline import EXPECTED_ROW_COUNTS
        base = list(EXPECTED_ROW_COUNTS.values())[0]
        actual = int(base * failure["row_count_impact"])

        self.sim.complete_run(
            run_id,
            actual_rows=actual,
            failure_type=failure_type,
            failure_detail=failure["error"]
        )

        # For schema_drift: write a "current" snapshot showing the 3 missing columns
        if failure_type == "schema_drift":
            self._inject_schema_drift_snapshot()

        return {
            "failure_type": failure_type,
            "affected_task": affected_task,
            **failure,
        }

    def _inject_schema_drift_snapshot(self) -> None:
        """
        Write an actual_columns snapshot to schema_registry for raw_orders
        that is missing the 3 columns referenced in the schema_drift error message.
        The check_schema_drift tool compares this against the expected baseline.
        """
        MISSING_COLS = {"promo_code", "region_id", "channel_tag"}
        conn = get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT expected_columns FROM schema_registry "
            "WHERE table_name = 'raw_orders' AND actual_columns IS NULL "
            "ORDER BY id LIMIT 1"
        )
        row = c.fetchone()
        if row:
            baseline = json.loads(row[0])
            actual = [col for col in baseline if col not in MISSING_COLS]
            # Upsert: remove any previous drift snapshot, insert fresh one
            c.execute(
                "DELETE FROM schema_registry WHERE table_name = 'raw_orders' AND actual_columns IS NOT NULL"
            )
            c.execute(
                "INSERT INTO schema_registry (table_name, expected_columns, actual_columns) VALUES (?, ?, ?)",
                ("raw_orders", json.dumps(baseline), json.dumps(actual)),
            )
            conn.commit()
        conn.close()

    def simulate_healthy_run(self, run_id: str):
        """Run all tasks successfully."""
        from .pipeline import EXPECTED_ROW_COUNTS
        for task in DAG_TASKS:
            self.sim.run_task(run_id, task, success=True)
        base = list(EXPECTED_ROW_COUNTS.values())[0]
        actual = int(base * random.uniform(0.97, 1.03))
        self.sim.complete_run(run_id, actual_rows=actual)
