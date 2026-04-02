import json
import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("PIPELINE_DB_PATH", "data/sentinel.db")

# Suppress Python 3.12 deprecation warnings for datetime ↔ SQLite conversion
sqlite3.register_adapter(datetime, lambda d: d.isoformat())
sqlite3.register_converter("TIMESTAMP", lambda b: datetime.fromisoformat(b.decode()))

# Baseline schemas for schema drift detection (24 columns each)
_BASELINE_SCHEMAS = {
    "raw_orders": [
        "order_id", "customer_id", "order_date", "total_amount", "status",
        "promo_code", "region_id", "channel_tag", "payment_method",
        "shipping_address", "billing_address", "product_count", "discount_pct",
        "tax_amount", "shipping_cost", "currency", "store_id", "sales_rep_id",
        "return_flag", "notes", "created_at", "updated_at", "source_system", "batch_id",
    ],
    "raw_customers": [
        "customer_id", "email", "first_name", "last_name", "phone",
        "signup_date", "tier", "country_code", "region_id", "preferred_channel",
        "is_active", "lifetime_value", "last_purchase_date", "marketing_opt_in",
        "source_system", "created_at", "updated_at", "batch_id",
    ],
}


def get_connection():
    # Read env var dynamically so tests can override PIPELINE_DB_PATH per-fixture
    # without needing to reload the module.
    db_path = os.getenv("PIPELINE_DB_PATH", DB_PATH)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )


def init_db():
    conn = get_connection()
    c = conn.cursor()

    # Pipeline runs table
    c.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dag_id TEXT NOT NULL,
            run_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,           -- running, success, failed, remediated
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            expected_row_count INTEGER,
            actual_row_count INTEGER,
            failure_type TEXT,              -- null if no failure
            failure_detail TEXT,
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Task states table (simulates Airflow task instances)
    c.execute("""
        CREATE TABLE IF NOT EXISTS task_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            status TEXT NOT NULL,           -- queued, running, success, failed, skipped
            duration_seconds REAL,
            error_message TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Schema registry (for schema drift detection)
    c.execute("""
        CREATE TABLE IF NOT EXISTS schema_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            expected_columns TEXT NOT NULL,  -- JSON array
            actual_columns TEXT,             -- JSON array (null = no snapshot yet)
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Incident log
    c.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            dag_id TEXT NOT NULL,
            failure_type TEXT NOT NULL,
            detected_at TIMESTAMP,
            resolved_at TIMESTAMP,
            resolution_status TEXT,         -- resolved, escalated
            retry_attempts INTEGER DEFAULT 0,
            root_cause TEXT,
            remediation_steps TEXT,         -- JSON array of steps taken
            reflection_notes TEXT,          -- agent's self-assessment
            explanation TEXT,               -- plain English narrative
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Agent audit log — one row per agent transition per incident (Prompt 1)
    c.execute("""
        CREATE TABLE IF NOT EXISTS agent_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id INTEGER,            -- FK to incidents.id (populated after incident write)
            run_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            input_summary TEXT,             -- JSON
            decision TEXT,
            confidence TEXT,
            output_summary TEXT,            -- JSON
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Incident outcomes — structured metrics per incident (Prompt 3)
    c.execute("""
        CREATE TABLE IF NOT EXISTS incident_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id INTEGER,
            run_id TEXT NOT NULL,
            dag_id TEXT,
            detected_at TIMESTAMP,
            resolved_at TIMESTAMP,
            resolution_type TEXT,           -- AUTO_RESOLVED | ESCALATED | FAILED
            root_cause_category TEXT,
            agent_confidence_score REAL,    -- 0.0–1.0
            mttr_seconds REAL,
            blast_radius TEXT,              -- LOW | MEDIUM | HIGH
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Incident pattern memory — rolling stats per (root_cause, pipeline, fix) (Prompt 5)
    c.execute("""
        CREATE TABLE IF NOT EXISTS incident_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            root_cause_category TEXT NOT NULL,
            pipeline_name TEXT NOT NULL,
            fix_action_taken TEXT NOT NULL,
            success_rate REAL DEFAULT 1.0,
            occurrence_count INTEGER DEFAULT 1,
            last_seen_at TIMESTAMP,
            UNIQUE(root_cause_category, pipeline_name, fix_action_taken)
        )
    """)

    conn.commit()

    # ── Migrations for tables that may already exist ───────────────────────
    for col_sql in [
        "ALTER TABLE incidents ADD COLUMN thought_log TEXT",
        "ALTER TABLE incidents ADD COLUMN blast_radius TEXT",
        "ALTER TABLE incidents ADD COLUMN patterns_consulted TEXT",
    ]:
        try:
            c.execute(col_sql)
            conn.commit()
        except Exception:
            pass  # Column already exists

    # ── Seed baseline schema snapshots (idempotent) ────────────────────────
    for table_name, columns in _BASELINE_SCHEMAS.items():
        c.execute(
            "SELECT id FROM schema_registry WHERE table_name = ? AND actual_columns IS NULL",
            (table_name,),
        )
        if not c.fetchone():
            c.execute(
                "INSERT INTO schema_registry (table_name, expected_columns) VALUES (?, ?)",
                (table_name, json.dumps(columns)),
            )
    conn.commit()

    conn.close()
    print(f"Database initialized at {DB_PATH}")
