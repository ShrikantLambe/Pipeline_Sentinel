import json
import os
import sqlite3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("PIPELINE_DB_PATH", "data/sentinel.db")

# SQLite datetime adapters — only registered when not using PostgreSQL
if not os.getenv("DATABASE_URL"):
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


# ── DB adapter layer ──────────────────────────────────────────────────────────
# Normalises SQLite and psycopg2 differences so every other module can use
# the same SQL (? placeholders, .lastrowid) regardless of the backend.

class _CursorWrapper:
    def __init__(self, cursor, is_postgres: bool):
        self._cur = cursor
        self._is_postgres = is_postgres

    def execute(self, sql: str, params=()):
        if self._is_postgres:
            sql = sql.replace("?", "%s")
        self._cur.execute(sql, params)
        return self

    def executemany(self, sql: str, params_seq):
        if self._is_postgres:
            sql = sql.replace("?", "%s")
        self._cur.executemany(sql, params_seq)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def lastrowid(self):
        # psycopg2 doesn't set lastrowid; use lastval() which returns the last
        # sequence value generated in the current session.
        if self._is_postgres:
            self._cur.execute("SELECT lastval()")
            return self._cur.fetchone()[0]
        return self._cur.lastrowid

    @property
    def rowcount(self):
        return self._cur.rowcount


class _ConnectionWrapper:
    def __init__(self, conn, is_postgres: bool):
        self._conn = conn
        self._is_postgres = is_postgres

    def cursor(self) -> _CursorWrapper:
        return _CursorWrapper(self._conn.cursor(), self._is_postgres)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def get_connection() -> _ConnectionWrapper:
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        import psycopg2
        conn = psycopg2.connect(db_url)
        return _ConnectionWrapper(conn, is_postgres=True)
    db_path = os.getenv("PIPELINE_DB_PATH", DB_PATH)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    return _ConnectionWrapper(conn, is_postgres=False)


def _adapt_ddl(sql: str) -> str:
    """Translate SQLite DDL to PostgreSQL when DATABASE_URL is set."""
    if os.getenv("DATABASE_URL"):
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    return sql


def init_db():
    conn = get_connection()
    c = conn.cursor()
    is_postgres = bool(os.getenv("DATABASE_URL"))

    # Pipeline runs table
    c.execute(_adapt_ddl("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dag_id TEXT NOT NULL,
            run_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            expected_row_count INTEGER,
            actual_row_count INTEGER,
            failure_type TEXT,
            failure_detail TEXT,
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))

    # Task states table
    c.execute(_adapt_ddl("""
        CREATE TABLE IF NOT EXISTS task_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            status TEXT NOT NULL,
            duration_seconds REAL,
            error_message TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))

    # Schema registry
    c.execute(_adapt_ddl("""
        CREATE TABLE IF NOT EXISTS schema_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            expected_columns TEXT NOT NULL,
            actual_columns TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))

    # Incident log
    c.execute(_adapt_ddl("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            dag_id TEXT NOT NULL,
            failure_type TEXT NOT NULL,
            detected_at TIMESTAMP,
            resolved_at TIMESTAMP,
            resolution_status TEXT,
            retry_attempts INTEGER DEFAULT 0,
            root_cause TEXT,
            remediation_steps TEXT,
            reflection_notes TEXT,
            explanation TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            thought_log TEXT,
            blast_radius TEXT,
            patterns_consulted TEXT
        )
    """))

    # Agent audit log
    c.execute(_adapt_ddl("""
        CREATE TABLE IF NOT EXISTS agent_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id INTEGER,
            run_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            input_summary TEXT,
            decision TEXT,
            confidence TEXT,
            output_summary TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))

    # Incident outcomes
    c.execute(_adapt_ddl("""
        CREATE TABLE IF NOT EXISTS incident_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id INTEGER,
            run_id TEXT NOT NULL,
            dag_id TEXT,
            detected_at TIMESTAMP,
            resolved_at TIMESTAMP,
            resolution_type TEXT,
            root_cause_category TEXT,
            agent_confidence_score REAL,
            mttr_seconds REAL,
            blast_radius TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))

    # Incident pattern memory
    c.execute(_adapt_ddl("""
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
    """))

    conn.commit()

    # ── Migrations for tables that may already exist ───────────────────────
    # PostgreSQL supports IF NOT EXISTS; SQLite uses try/except.
    migration_cols = [
        ("incidents", "thought_log", "TEXT"),
        ("incidents", "blast_radius", "TEXT"),
        ("incidents", "patterns_consulted", "TEXT"),
    ]
    for table, col, col_type in migration_cols:
        if is_postgres:
            c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type}")
            conn.commit()
        else:
            try:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                conn.commit()
            except Exception:
                pass  # column already exists

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
    db_label = os.getenv("DATABASE_URL", DB_PATH)
    print(f"Database initialized at {db_label}")
