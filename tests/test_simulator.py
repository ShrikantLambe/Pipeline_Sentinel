"""Tests for the pipeline simulator."""
import pytest
import os
import tempfile

# Use a temp DB for tests
os.environ["PIPELINE_DB_PATH"] = ":memory:"

from simulator.database import init_db, get_connection
from simulator.pipeline import PipelineSimulator, DAG_TASKS, EXPECTED_ROW_COUNTS
from simulator.failure_injector import FailureInjector, FAILURE_MODES


@pytest.fixture(autouse=True)
def setup_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    os.environ["PIPELINE_DB_PATH"] = db_path
    init_db()
    yield
    os.environ.pop("PIPELINE_DB_PATH", None)


def test_init_db_creates_tables(tmp_path):
    db_path = str(tmp_path / "init_test.db")
    os.environ["PIPELINE_DB_PATH"] = db_path
    init_db()
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in c.fetchall()}
    conn.close()
    assert "pipeline_runs" in tables
    assert "task_states" in tables
    assert "incidents" in tables


def test_start_run_creates_record():
    sim = PipelineSimulator()
    run_id = sim.start_run()
    assert run_id.startswith("run_retail_pipeline_")
    state = sim.get_run_state(run_id)
    assert state["status"] == "running"
    assert len(state["tasks"]) == len(DAG_TASKS)


def test_healthy_run():
    sim = PipelineSimulator()
    injector = FailureInjector(sim)
    run_id = sim.start_run()
    injector.simulate_healthy_run(run_id)
    state = sim.get_run_state(run_id)
    assert state["status"] == "success"
    expected = list(EXPECTED_ROW_COUNTS.values())[0]
    assert state["actual_row_count"] >= int(expected * 0.95)


def test_failure_injection():
    for failure_type in FAILURE_MODES.keys():
        sim = PipelineSimulator()
        injector = FailureInjector(sim)
        run_id = sim.start_run()
        result = injector.inject(run_id, failure_type=failure_type)
        assert result["failure_type"] == failure_type
        state = sim.get_run_state(run_id)
        assert state["status"] == "failed"
        assert state["failure_type"] == failure_type


def test_get_recent_runs():
    sim = PipelineSimulator()
    injector = FailureInjector(sim)
    for _ in range(3):
        run_id = sim.start_run()
        injector.simulate_healthy_run(run_id)
    runs = sim.get_recent_runs(limit=10)
    assert len(runs) >= 3
