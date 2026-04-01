import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("PIPELINE_DB_PATH", "data/sentinel.db")

# Suppress Python 3.12 deprecation warnings for datetime ↔ SQLite conversion
sqlite3.register_adapter(datetime, lambda d: d.isoformat())
sqlite3.register_converter("TIMESTAMP", lambda b: datetime.fromisoformat(b.decode()))


def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(
        DB_PATH,
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
            actual_columns TEXT,             -- JSON array
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

    conn.commit()

    # Migration: add thought_log column to existing incidents tables
    try:
        c.execute("ALTER TABLE incidents ADD COLUMN thought_log TEXT")
        conn.commit()
    except Exception:
        pass  # Column already exists

    conn.close()
    print(f"Database initialized at {DB_PATH}")
