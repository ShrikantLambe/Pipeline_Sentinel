"""Tests for the reflection loop and orchestration logic."""
import pytest
import os
from unittest.mock import patch, MagicMock

from simulator.database import init_db
from simulator.pipeline import PipelineSimulator
from simulator.failure_injector import FailureInjector


@pytest.fixture(autouse=True)
def setup_db(tmp_path):
    db_path = str(tmp_path / "test_reflection.db")
    os.environ["PIPELINE_DB_PATH"] = db_path
    init_db()
    yield


@pytest.fixture
def run_with_failure():
    sim = PipelineSimulator()
    injector = FailureInjector(sim)
    run_id = sim.start_run()
    injector.inject(run_id, failure_type="upstream_timeout")
    return run_id


def test_orchestrator_healthy_run():
    """Orchestrator returns healthy status for a clean run."""
    sim = PipelineSimulator()
    injector = FailureInjector(sim)
    run_id = sim.start_run()
    injector.simulate_healthy_run(run_id)

    mock_monitor_result = {
        "anomaly_detected": False,
        "severity": "none",
        "thoughts": []
    }

    with patch("orchestrator.sentinel.run_monitor_agent",
               return_value=mock_monitor_result):
        from orchestrator.sentinel import SentinelOrchestrator
        thoughts = []
        orch = SentinelOrchestrator(thought_callback=lambda t: thoughts.append(t))
        result = orch.run(run_id)

    assert result["status"] == "healthy"


def test_orchestrator_resolves_failure(run_with_failure):
    """Orchestrator resolves a failure within retry budget."""
    mock_monitor = {
        "anomaly_detected": True,
        "severity": "critical",
        "anomaly_type": "upstream_timeout",
        "affected_task": "extract_source_data",
        "evidence": "Task failed with timeout",
        "recommended_action": "retry_with_backoff",
        "thoughts": []
    }
    mock_diagnosis = {
        "root_cause": "Transient source lock",
        "confidence": "high",
        "remediation_strategy": "retry_with_backoff",
        "strategy_rationale": "Timeout suggests transient lock",
        "estimated_fix_time": "2-3 minutes",
        "escalation_risk": "low",
        "thoughts": []
    }
    mock_remediation = {
        "action_taken": "Retried extract_source_data with 30s backoff",
        "tool_used": "retry_task",
        "tool_result": {"retry_success": True},
        "fix_successful": True,
        "verification_evidence": "Task now shows success",
        "thoughts": []
    }
    mock_reflection = {
        "assessment": "resolved",
        "confidence": "high",
        "what_worked": "Backoff retry cleared the lock",
        "what_failed": None,
        "updated_hypothesis": "Transient lock resolved",
        "next_strategy": None,
        "escalation_reason": None,
        "thoughts": []
    }
    mock_explanation = {
        "title": "Upstream Timeout — retail_pipeline",
        "severity": "P2",
        "summary": "Extract task timed out due to transient lock. Resolved after 1 retry.",
        "status": "resolved",
        "action_required": None,
        "time_to_resolution": "~2 minutes",
        "tags": ["upstream_timeout", "retail_pipeline", "retry_with_backoff"]
    }

    with patch("orchestrator.sentinel.run_monitor_agent", return_value=mock_monitor), \
         patch("orchestrator.sentinel.run_diagnosis_agent", return_value=mock_diagnosis), \
         patch("orchestrator.sentinel.run_remediation_agent", return_value=mock_remediation), \
         patch("orchestrator.sentinel.run_reflection_agent", return_value=mock_reflection), \
         patch("orchestrator.sentinel.run_explanation_agent", return_value=mock_explanation):
        from orchestrator.sentinel import SentinelOrchestrator
        orch = SentinelOrchestrator()
        result = orch.run(run_with_failure)

    assert result["resolution_status"] == "resolved"
    assert result["retry_count"] == 1
    assert "incident_id" in result


def test_orchestrator_escalates_on_exhaustion(run_with_failure):
    """Orchestrator escalates when reflection says escalate."""
    mock_monitor = {
        "anomaly_detected": True,
        "severity": "critical",
        "anomaly_type": "upstream_timeout",
        "affected_task": "extract_source_data",
        "evidence": "Repeated failures",
        "recommended_action": "escalate",
        "thoughts": []
    }
    mock_diagnosis = {
        "root_cause": "Persistent source outage",
        "confidence": "high",
        "remediation_strategy": "retry_with_backoff",
        "strategy_rationale": "Only option",
        "estimated_fix_time": "unknown",
        "escalation_risk": "high",
        "thoughts": []
    }
    mock_remediation = {
        "fix_successful": False,
        "action_taken": "Retry attempted",
        "tool_used": "retry_task",
        "tool_result": {"retry_success": False},
        "verification_evidence": "Still failing",
        "thoughts": []
    }
    mock_reflection = {
        "assessment": "escalate",
        "confidence": "high",
        "what_worked": "Nothing",
        "what_failed": "Source is down",
        "updated_hypothesis": "Persistent outage",
        "next_strategy": None,
        "escalation_reason": "Source system unresponsive after 3 retries",
        "thoughts": []
    }
    mock_explanation = {
        "title": "Upstream Timeout — ESCALATED",
        "severity": "P1",
        "summary": "Source system outage. All retries exhausted.",
        "status": "escalated",
        "action_required": "Check orders_db connectivity",
        "time_to_resolution": "unresolved",
        "tags": ["upstream_timeout", "retail_pipeline"]
    }

    with patch("orchestrator.sentinel.run_monitor_agent", return_value=mock_monitor), \
         patch("orchestrator.sentinel.run_diagnosis_agent", return_value=mock_diagnosis), \
         patch("orchestrator.sentinel.run_remediation_agent", return_value=mock_remediation), \
         patch("orchestrator.sentinel.run_reflection_agent", return_value=mock_reflection), \
         patch("orchestrator.sentinel.run_explanation_agent", return_value=mock_explanation):
        from orchestrator.sentinel import SentinelOrchestrator
        orch = SentinelOrchestrator()
        result = orch.run(run_with_failure)

    assert result["resolution_status"] == "escalated"
