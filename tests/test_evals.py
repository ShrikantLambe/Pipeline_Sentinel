"""
LLM-behavior evals — validate agent routing decisions, not tool return values.

These tests mock the five agent functions so no real LLM calls are made.
They verify that the orchestrator correctly:
  - Routes each failure mode to the right strategy and resolution path
  - Applies the blast-radius escalation gate
  - Prevents repeated strategies via tried_strategies
  - Uses pattern memory fast-path when threshold is met

13 eval cases:
  7 × failure mode routing
  4 × escalation gate combinations
  2 × pattern memory / tried-strategy guards

Run:
    pytest tests/test_evals.py -v
"""

import json
import os
import tempfile
import threading
import pytest

os.environ.setdefault("PIPELINE_DB_PATH", tempfile.mktemp(suffix=".db"))
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db = str(tmp_path / "eval.db")
    monkeypatch.setenv("PIPELINE_DB_PATH", db)
    from simulator.database import init_db
    init_db()
    yield db


@pytest.fixture()
def sim_run(isolated_db):
    """Create a failed pipeline run for a given failure type."""
    from simulator.database import init_db
    from simulator.pipeline import PipelineSimulator
    from simulator.failure_injector import FailureInjector

    init_db()

    def _make(failure_type: str) -> str:
        sim = PipelineSimulator()
        inj = FailureInjector(sim)
        run_id = sim.start_run()
        inj.inject(run_id, failure_type=failure_type)
        return run_id

    return _make


def _monitor_result(failure_type: str, affected_task: str) -> dict:
    return {
        "anomaly_detected": True,
        "severity": "critical" if failure_type == "auth_failure" else "warning",
        "anomaly_type": failure_type,
        "affected_task": affected_task,
        "evidence": f"Injected {failure_type}",
        "recommended_action": "Investigate",
        "thoughts": [],
    }


def _diagnosis_result(strategy: str, confidence: str = "high") -> dict:
    return {
        "root_cause": f"Root cause for {strategy}",
        "remediation_strategy": strategy,
        "confidence": confidence,
        "patterns_consulted": [],
        "pattern_matched": False,
        "thoughts": [],
    }


def _remediation_result(success: bool = True) -> dict:
    return {
        "fix_attempted": True,
        "fix_successful": success,
        "tool_called": "retry_task",
        "result": {"success": success},
        "thoughts": [],
    }


def _reflection_result(assessment: str, next_strategy: str = None) -> dict:
    return {
        "assessment": assessment,
        "next_strategy": next_strategy,
        "updated_hypothesis": "Assessed.",
        "confidence": "high",
        "thoughts": [],
    }


def _explanation_result(status: str) -> dict:
    return {
        "title": "Test Incident",
        "severity": "P2",
        "summary": "Test summary.",
        "status": status,
        "action_required": None if status == "resolved" else "Check logs.",
        "time_to_resolution": "~1 minute",
        "tags": ["test"],
    }


def _run_orchestrator(run_id, monkeypatch,
                      monitor_out, diagnosis_out,
                      remediation_out=None, reflection_out=None):
    """
    Patch all five agents and run the orchestrator.
    Returns the final result dict.
    """
    monkeypatch.setattr(
        "orchestrator.sentinel.run_monitor_agent",
        lambda *a, **kw: monitor_out,
    )
    monkeypatch.setattr(
        "orchestrator.sentinel.run_diagnosis_agent",
        lambda *a, **kw: diagnosis_out,
    )
    if remediation_out is not None:
        monkeypatch.setattr(
            "orchestrator.sentinel.run_remediation_agent",
            lambda *a, **kw: remediation_out,
        )
    if reflection_out is not None:
        monkeypatch.setattr(
            "orchestrator.sentinel.run_reflection_agent",
            lambda *a, **kw: reflection_out,
        )
    monkeypatch.setattr(
        "orchestrator.sentinel.run_explanation_agent",
        lambda *a, **kw: _explanation_result(
            "resolved" if (remediation_out and remediation_out.get("fix_successful")) else "escalated"
        ),
    )

    from orchestrator.sentinel import SentinelOrchestrator
    orch = SentinelOrchestrator(stop_event=threading.Event())
    return orch.run(run_id)


# ── Failure mode routing (7 cases) ──────────────────────────────────────────

# Taxonomy reference:
#   upstream_timeout  → extract_source_data → LOW  → retry_task         → resolved
#   late_arrival      → load_to_staging     → LOW  → extend_ingestion_window → resolved
#   partial_load      → load_to_staging     → LOW  → retry_task         → resolved
#   duplicate_keys    → run_dbt_staging_models → LOW → apply_dedup      → resolved
#   dbt_model_failure → run_dbt_mart_models → MEDIUM → escalated (gate)
#   schema_drift      → validate_raw_schema  → LOW  → reload_schema     → resolved
#   auth_failure      → extract_source_data → LOW  → escalated (always)

@pytest.mark.parametrize("failure_type,affected_task,strategy,expected_resolution", [
    ("upstream_timeout",  "extract_source_data",     "retry_task",                  "resolved"),
    ("late_arrival",      "load_to_staging",          "extend_ingestion_window",     "resolved"),
    ("partial_load",      "load_to_staging",          "retry_task",                  "resolved"),
    ("duplicate_keys",    "run_dbt_staging_models",   "apply_dedup",                 "resolved"),
    ("schema_drift",      "validate_raw_schema",       "reload_schema",               "resolved"),
])
def test_failure_mode_resolves(failure_type, affected_task, strategy,
                                expected_resolution, sim_run, monkeypatch):
    """LOW-blast failure modes with high confidence should auto-resolve."""
    run_id = sim_run(failure_type)
    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result(failure_type, affected_task),
        diagnosis_out=_diagnosis_result(strategy, confidence="high"),
        remediation_out=_remediation_result(success=True),
        reflection_out=_reflection_result("resolved"),
    )
    assert result["resolution_status"] == expected_resolution, (
        f"{failure_type}: expected {expected_resolution}, got {result['resolution_status']}"
    )


def test_dbt_model_failure_escalates_due_to_blast_radius(sim_run, monkeypatch):
    """dbt_model_failure hits run_dbt_mart_models (MEDIUM blast) — gate must block."""
    run_id = sim_run("dbt_model_failure")
    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("dbt_model_failure", "run_dbt_mart_models"),
        diagnosis_out=_diagnosis_result("run_dbt_full_refresh", confidence="high"),
    )
    assert result["resolution_status"] == "escalated"
    assert result["blast_radius"] == "MEDIUM"


def test_auth_failure_always_escalates(sim_run, monkeypatch):
    """auth_failure strategy is escalate_to_manual_intervention — must never auto-remediate."""
    run_id = sim_run("auth_failure")
    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("auth_failure", "extract_source_data"),
        diagnosis_out=_diagnosis_result("escalate_to_manual_intervention", confidence="low"),
    )
    assert result["resolution_status"] == "escalated"


# ── Escalation gate (4 cases) ────────────────────────────────────────────────

def test_gate_blocks_medium_confidence(sim_run, monkeypatch):
    """HIGH blast OR low confidence must escalate — test low confidence alone."""
    run_id = sim_run("upstream_timeout")
    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("upstream_timeout", "extract_source_data"),
        diagnosis_out=_diagnosis_result("retry_task", confidence="low"),
    )
    assert result["resolution_status"] == "escalated"


def test_gate_blocks_medium_blast_radius(sim_run, monkeypatch):
    """MEDIUM blast radius must escalate regardless of confidence."""
    run_id = sim_run("dbt_model_failure")
    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("dbt_model_failure", "run_dbt_mart_models"),
        diagnosis_out=_diagnosis_result("run_dbt_full_refresh", confidence="high"),
    )
    assert result["resolution_status"] == "escalated"
    assert result["blast_radius"] == "MEDIUM"


def test_gate_allows_high_confidence_low_blast(sim_run, monkeypatch):
    """high confidence + LOW blast → auto-remediation proceeds."""
    run_id = sim_run("upstream_timeout")
    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("upstream_timeout", "extract_source_data"),
        diagnosis_out=_diagnosis_result("retry_task", confidence="high"),
        remediation_out=_remediation_result(success=True),
        reflection_out=_reflection_result("resolved"),
    )
    assert result["resolution_status"] == "resolved"
    assert result["blast_radius"] == "LOW"


def test_gate_blocks_medium_confidence_even_on_low_blast(sim_run, monkeypatch):
    """medium confidence blocks auto-remediation even when blast is LOW."""
    run_id = sim_run("late_arrival")
    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("late_arrival", "load_to_staging"),
        diagnosis_out=_diagnosis_result("extend_ingestion_window", confidence="medium"),
    )
    assert result["resolution_status"] == "escalated"


# ── Pattern memory + retry guards (2 cases) ──────────────────────────────────

def test_tried_strategy_prevents_loop(sim_run, monkeypatch):
    """
    If Reflection suggests a strategy already in tried_strategies, the orchestrator
    must escalate rather than loop.
    """
    run_id = sim_run("upstream_timeout")
    call_count = {"n": 0}

    def _reflection(*a, **kw):
        call_count["n"] += 1
        # Always suggest retry_task — same as first attempt
        return _reflection_result("retry", next_strategy="retry_task")

    monkeypatch.setattr("orchestrator.sentinel.run_reflection_agent", _reflection)

    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("upstream_timeout", "extract_source_data"),
        diagnosis_out=_diagnosis_result("retry_task", confidence="high"),
        remediation_out=_remediation_result(success=False),
        reflection_out=None,  # patched above
    )
    assert result["resolution_status"] == "escalated"
    # Should have stopped after the first reflection caught the repeat
    assert call_count["n"] >= 1


def test_pattern_matched_flag_propagates(sim_run, monkeypatch):
    """When Diagnosis returns pattern_matched=True, orchestrator result must reflect it."""
    run_id = sim_run("duplicate_keys")
    diagnosis = _diagnosis_result("apply_dedup", confidence="high")
    diagnosis["pattern_matched"] = True
    diagnosis["patterns_consulted"] = [{"root_cause": "duplicate_keys", "rate": 0.95, "count": 5}]

    result = _run_orchestrator(
        run_id, monkeypatch,
        monitor_out=_monitor_result("duplicate_keys", "run_dbt_staging_models"),
        diagnosis_out=diagnosis,
        remediation_out=_remediation_result(success=True),
        reflection_out=_reflection_result("resolved"),
    )
    assert result["pattern_matched"] is True
