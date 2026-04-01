"""Tests for agent tool functions (no LLM calls)."""
import pytest
import os

from simulator.database import init_db
from simulator.pipeline import PipelineSimulator
from simulator.failure_injector import FailureInjector
from agents.tools import (
    get_pipeline_status,
    check_row_count_anomaly,
    get_failed_tasks,
    apply_dedup,
    reload_schema,
    extend_ingestion_window,
    run_dbt_full_refresh,
)


@pytest.fixture(autouse=True)
def setup_db(tmp_path):
    db_path = str(tmp_path / "test_agents.db")
    os.environ["PIPELINE_DB_PATH"] = db_path
    init_db()
    yield


@pytest.fixture
def failed_run():
    sim = PipelineSimulator()
    injector = FailureInjector(sim)
    run_id = sim.start_run()
    injector.inject(run_id, failure_type="duplicate_keys")
    return run_id


def test_get_pipeline_status(failed_run):
    result = get_pipeline_status(failed_run)
    assert "status" in result
    assert "tasks" in result


def test_get_pipeline_status_not_found():
    result = get_pipeline_status("nonexistent_run")
    assert "error" in result


def test_check_row_count_anomaly_detects_anomaly(failed_run):
    result = check_row_count_anomaly(failed_run)
    assert "is_anomaly" in result
    assert "deviation_pct" in result


def test_get_failed_tasks(failed_run):
    result = get_failed_tasks(failed_run)
    assert isinstance(result, list)
    assert len(result) > 0
    assert "task_id" in result[0]


def test_apply_dedup(failed_run):
    result = apply_dedup(failed_run)
    assert "action" in result
    assert result["action"] == "dedup_applied"
    assert "success" in result


def test_reload_schema(failed_run):
    result = reload_schema(failed_run)
    assert result["action"] == "schema_reloaded"


def test_extend_ingestion_window(failed_run):
    result = extend_ingestion_window(failed_run)
    assert result["action"] == "window_extended"


def test_run_dbt_full_refresh(failed_run):
    result = run_dbt_full_refresh(failed_run)
    assert result["action"] == "dbt_full_refresh"
