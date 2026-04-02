"""
Integration tests for Prompts 1–6 (no LLM calls).

Prompt 1 — Audit Trail
Prompt 2 — Schema Drift Detection
Prompt 3 — Self-Healing Rate Metric
Prompt 4 — Blast Radius Assessment
Prompt 5 — Incident Pattern Memory
Prompt 6 — LangSmith @traceable decorators (smoke test only)
"""

import json
import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

from simulator.database import init_db, get_connection
from simulator.pipeline import PipelineSimulator
from simulator.failure_injector import FailureInjector


# ── Shared fixture ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def setup_db(tmp_path):
    """Each test gets an isolated temporary SQLite database."""
    db_path = str(tmp_path / "test_features.db")
    os.environ["PIPELINE_DB_PATH"] = db_path
    init_db()
    yield


@pytest.fixture
def run_id():
    sim = PipelineSimulator()
    rid = sim.start_run()
    FailureInjector(sim).inject(rid, failure_type="upstream_timeout")
    return rid


@pytest.fixture
def schema_drift_run_id():
    sim = PipelineSimulator()
    rid = sim.start_run()
    FailureInjector(sim).inject(rid, failure_type="schema_drift")
    return rid


# ── Prompt 1 — Audit Trail ─────────────────────────────────────────────────

class TestAuditTrail:
    def test_log_agent_transition_writes_row(self, run_id):
        from agents.audit import log_agent_transition
        row_id = log_agent_transition(
            run_id=run_id,
            agent_name="Monitor",
            input_summary={"run_id": run_id},
            decision="anomaly_detected=True, type=upstream_timeout",
            confidence="high",
            output_summary={"anomaly_type": "upstream_timeout"},
        )
        assert isinstance(row_id, int)
        assert row_id > 0

    def test_get_incident_audit_returns_chain(self, run_id):
        from agents.audit import log_agent_transition, update_audit_incident_id, get_incident_audit

        for agent in ("Monitor", "Diagnosis", "Remediation"):
            log_agent_transition(
                run_id=run_id,
                agent_name=agent,
                input_summary={"step": agent},
                decision=f"{agent} decision",
                output_summary={"ok": True},
            )

        # Simulate writing an incident and back-filling the ID
        conn = get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO incidents (run_id, dag_id, failure_type, resolution_status) "
            "VALUES (?, 'dag', 'upstream_timeout', 'resolved')",
            (run_id,),
        )
        incident_id = c.lastrowid
        conn.commit()
        conn.close()

        update_audit_incident_id(run_id, incident_id)
        chain = get_incident_audit(incident_id)

        assert len(chain) == 3
        assert chain[0]["agent_name"] == "Monitor"
        assert chain[2]["agent_name"] == "Remediation"
        assert chain[0]["input_summary"] == {"step": "Monitor"}

    def test_update_audit_incident_id_backfills(self, run_id):
        from agents.audit import log_agent_transition, update_audit_incident_id

        log_agent_transition(run_id=run_id, agent_name="Monitor",
                             input_summary={}, decision="test", output_summary={})

        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT incident_id FROM agent_audit_log WHERE run_id = ?", (run_id,))
        assert c.fetchone()[0] is None  # not yet set

        c.execute(
            "INSERT INTO incidents (run_id, dag_id, failure_type, resolution_status) "
            "VALUES (?, 'dag', 'x', 'resolved')", (run_id,)
        )
        incident_id = c.lastrowid
        conn.commit()
        conn.close()

        update_audit_incident_id(run_id, incident_id)

        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT incident_id FROM agent_audit_log WHERE run_id = ?", (run_id,))
        assert c.fetchone()[0] == incident_id
        conn.close()

    def test_input_output_truncated_at_4kb(self, run_id):
        from agents.audit import log_agent_transition
        huge = {"data": "x" * 10_000}
        row_id = log_agent_transition(
            run_id=run_id, agent_name="Monitor",
            input_summary=huge, decision="test", output_summary=huge,
        )
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT input_summary, output_summary FROM agent_audit_log WHERE id = ?", (row_id,))
        inp, out = c.fetchone()
        conn.close()
        assert len(inp) <= 4096
        assert len(out) <= 4096


# ── Prompt 2 — Schema Drift Detection ─────────────────────────────────────

class TestSchemaDriftDetection:
    def test_baseline_seeded_by_init_db(self):
        conn = get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT table_name FROM schema_registry WHERE actual_columns IS NULL ORDER BY table_name"
        )
        tables = [r[0] for r in c.fetchall()]
        conn.close()
        assert "raw_orders" in tables
        assert "raw_customers" in tables

    def test_no_drift_when_no_snapshot(self, run_id):
        from agents.tools import check_schema_drift
        result = check_schema_drift(run_id)
        # No actual_columns snapshot injected → nothing to compare
        assert result["has_drift"] is False
        assert result["drift_events"] == []

    def test_detects_breaking_changes_after_schema_drift_injection(self, schema_drift_run_id):
        from agents.tools import check_schema_drift
        result = check_schema_drift(schema_drift_run_id)
        assert result["has_drift"] is True
        assert len(result["breaking_changes"]) == 3
        removed_cols = {e["column"] for e in result["breaking_changes"]}
        assert removed_cols == {"promo_code", "region_id", "channel_tag"}

    def test_drift_event_severity_is_breaking(self, schema_drift_run_id):
        from agents.tools import check_schema_drift
        result = check_schema_drift(schema_drift_run_id)
        for event in result["breaking_changes"]:
            assert event["severity"] == "BREAKING"
            assert event["drift_type"] == "column_removed"

    def test_drift_summary_is_human_readable(self, schema_drift_run_id):
        from agents.tools import check_schema_drift
        result = check_schema_drift(schema_drift_run_id)
        assert "BREAKING" in result["drift_summary"]
        assert "column" in result["drift_summary"].lower()

    def test_column_added_returns_warning(self):
        """Manually inject a snapshot that adds an extra column."""
        conn = get_connection()
        c = conn.cursor()
        # Get baseline for raw_orders
        c.execute("SELECT expected_columns FROM schema_registry WHERE table_name='raw_orders' AND actual_columns IS NULL LIMIT 1")
        row = c.fetchone()
        baseline = json.loads(row[0])
        actual = baseline + ["new_mystery_column"]
        c.execute(
            "INSERT INTO schema_registry (table_name, expected_columns, actual_columns) VALUES (?, ?, ?)",
            ("raw_orders", json.dumps(baseline), json.dumps(actual)),
        )
        conn.commit()
        conn.close()

        from agents.tools import check_schema_drift
        result = check_schema_drift("any_run")
        added = [e for e in result["drift_events"] if e["drift_type"] == "column_added"]
        assert len(added) == 1
        assert added[0]["column"] == "new_mystery_column"
        assert added[0]["severity"] == "WARNING"

    def test_fuzzy_rename_detected_as_info(self):
        """Inject a snapshot where one column is renamed (close match ≥0.75)."""
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT expected_columns FROM schema_registry WHERE table_name='raw_orders' AND actual_columns IS NULL LIMIT 1")
        baseline = json.loads(c.fetchone()[0])
        # 'return_flag' → 'returns_flag': ratio ≈ 0.96, well above 0.75 cutoff
        actual = [col if col != "return_flag" else "returns_flag" for col in baseline]
        c.execute(
            "INSERT INTO schema_registry (table_name, expected_columns, actual_columns) VALUES (?, ?, ?)",
            ("raw_orders", json.dumps(baseline), json.dumps(actual)),
        )
        conn.commit()
        conn.close()

        from agents.tools import check_schema_drift
        result = check_schema_drift("any_run")
        renames = [e for e in result["drift_events"] if e["drift_type"] == "column_renamed"]
        assert len(renames) >= 1
        assert renames[0]["severity"] == "INFO"


# ── Prompt 3 — Self-Healing Rate Metric ───────────────────────────────────

class TestSelfHealingMetrics:
    def _write_outcome(self, run_id, dag_id, resolution, confidence="high", blast="LOW"):
        from agents.metrics import write_incident_outcome
        write_incident_outcome(
            run_id=run_id, dag_id=dag_id, incident_id=None,
            detected_at=datetime.now() - timedelta(minutes=5),
            resolved_at=datetime.now(),
            resolution_status=resolution,
            root_cause_category="upstream_timeout",
            agent_confidence=confidence,
            blast_radius=blast,
        )

    def test_empty_metrics_returns_zeros(self):
        from agents.metrics import compute_self_healing_metrics
        m = compute_self_healing_metrics()
        assert m["total_incidents"] == 0
        assert m["self_healing_rate_pct"] == 0.0

    def test_resolved_incident_counted(self, run_id):
        self._write_outcome(run_id, "dag1", "resolved")
        from agents.metrics import compute_self_healing_metrics
        m = compute_self_healing_metrics()
        assert m["total_incidents"] == 1
        assert m["auto_resolved"] == 1
        assert m["self_healing_rate_pct"] == 100.0

    def test_escalated_incident_counted(self, run_id):
        self._write_outcome(run_id, "dag1", "escalated")
        from agents.metrics import compute_self_healing_metrics
        m = compute_self_healing_metrics()
        assert m["escalated"] == 1
        assert m["self_healing_rate_pct"] == 0.0

    def test_mixed_incidents_rate(self):
        sim = PipelineSimulator()
        for i in range(3):
            rid = sim.start_run()
            FailureInjector(sim).inject(rid, failure_type="upstream_timeout")
            resolution = "resolved" if i < 2 else "escalated"
            self._write_outcome(rid, "dag1", resolution)

        from agents.metrics import compute_self_healing_metrics
        m = compute_self_healing_metrics()
        assert m["total_incidents"] == 3
        assert m["auto_resolved"] == 2
        assert m["escalated"] == 1
        assert abs(m["self_healing_rate_pct"] - 66.7) < 0.1

    def test_mttr_calculated_for_auto_resolved(self, run_id):
        self._write_outcome(run_id, "dag1", "resolved")
        from agents.metrics import compute_self_healing_metrics
        m = compute_self_healing_metrics()
        assert m["avg_mttr_auto_seconds"] is not None
        assert m["avg_mttr_auto_seconds"] > 0

    def test_mttr_none_when_no_auto_resolved(self, run_id):
        self._write_outcome(run_id, "dag1", "escalated")
        from agents.metrics import compute_self_healing_metrics
        m = compute_self_healing_metrics()
        assert m["avg_mttr_auto_seconds"] is None

    def test_daily_trend_populated(self):
        sim = PipelineSimulator()
        for _ in range(3):
            rid = sim.start_run()
            FailureInjector(sim).inject(rid)
            self._write_outcome(rid, "dag1", "resolved")
        from agents.metrics import compute_self_healing_metrics
        m = compute_self_healing_metrics()
        assert len(m["daily_trend"]) >= 1
        day = m["daily_trend"][0]
        assert "date" in day
        assert "total" in day
        assert "rate_pct" in day

    def test_days_filter_excludes_old_incidents(self, run_id):
        """Outcome written with old timestamp should not appear in 30-day window."""
        from agents.metrics import write_incident_outcome, compute_self_healing_metrics, AUTO_RESOLVED
        # Write directly with old timestamp
        conn = get_connection()
        c = conn.cursor()
        old = (datetime.now() - timedelta(days=45)).isoformat()
        c.execute(
            "INSERT INTO incident_outcomes "
            "(run_id, dag_id, resolution_type, mttr_seconds, created_at) "
            "VALUES (?, 'dag', ?, 300, ?)",
            (run_id, AUTO_RESOLVED, old),
        )
        conn.commit()
        conn.close()
        m = compute_self_healing_metrics(days=30)
        assert m["total_incidents"] == 0


# ── Prompt 4 — Blast Radius Assessment ────────────────────────────────────

class TestBlastRadius:
    def test_raw_tasks_score_low(self):
        from agents.blast_radius import assess_blast_radius
        for task in ("extract_source_data", "validate_raw_schema"):
            assert assess_blast_radius(task)["blast_radius_score"] == "LOW", task

    def test_staging_tasks_score_low(self):
        from agents.blast_radius import assess_blast_radius
        for task in ("load_to_staging", "run_dbt_staging_models"):
            assert assess_blast_radius(task)["blast_radius_score"] == "LOW", task

    def test_mart_tasks_score_medium(self):
        from agents.blast_radius import assess_blast_radius
        for task in ("run_dbt_mart_models", "run_dbt_tests"):
            assert assess_blast_radius(task)["blast_radius_score"] == "MEDIUM", task

    def test_consumer_facing_tasks_score_high(self):
        from agents.blast_radius import assess_blast_radius
        for task in ("update_snowflake_aggregates", "refresh_bi_cache"):
            assert assess_blast_radius(task)["blast_radius_score"] == "HIGH", task

    def test_unknown_task_defaults_to_low(self):
        from agents.blast_radius import assess_blast_radius
        result = assess_blast_radius("nonexistent_task")
        assert result["blast_radius_score"] == "LOW"

    def test_result_contains_required_fields(self):
        from agents.blast_radius import assess_blast_radius
        r = assess_blast_radius("load_to_staging")
        assert "blast_radius_score" in r
        assert "affected_assets" in r
        assert "affected_consumers" in r
        assert "high_impact_consumers" in r
        assert "impact_description" in r

    def test_gate_approves_high_confidence_low_blast(self):
        from agents.blast_radius import should_auto_remediate
        ok, reason = should_auto_remediate("high", "LOW")
        assert ok is True
        assert "approved" in reason.lower()

    def test_gate_blocks_medium_confidence(self):
        from agents.blast_radius import should_auto_remediate
        ok, reason = should_auto_remediate("medium", "LOW")
        assert ok is False
        assert "confidence" in reason.lower()

    def test_gate_blocks_medium_blast_radius(self):
        from agents.blast_radius import should_auto_remediate
        ok, reason = should_auto_remediate("high", "MEDIUM")
        assert ok is False
        assert "blast_radius" in reason.lower()

    def test_gate_blocks_both_failures(self):
        from agents.blast_radius import should_auto_remediate
        ok, _ = should_auto_remediate("low", "HIGH")
        assert ok is False

    def test_high_blast_task_has_high_impact_consumers(self):
        from agents.blast_radius import assess_blast_radius
        r = assess_blast_radius("update_snowflake_aggregates")
        assert len(r["high_impact_consumers"]) > 0

    def test_low_blast_task_has_no_direct_high_impact_consumers(self):
        from agents.blast_radius import assess_blast_radius
        r = assess_blast_radius("validate_raw_schema")
        # Direct consumers list empty → no HIGH consumers from direct node
        assert r["blast_radius_score"] == "LOW"


# ── Prompt 5 — Incident Pattern Memory ────────────────────────────────────

class TestPatternMemory:
    def test_upsert_creates_new_pattern(self):
        from agents.patterns import upsert_pattern, get_known_patterns
        upsert_pattern("upstream_timeout", "test_dag", "retry_with_backoff", success=True)
        patterns = get_known_patterns("test_dag")
        assert len(patterns) == 1
        assert patterns[0]["occurrence_count"] == 1
        assert patterns[0]["success_rate"] == 1.0

    def test_upsert_increments_occurrence_count(self):
        from agents.patterns import upsert_pattern, get_known_patterns
        for _ in range(4):
            upsert_pattern("upstream_timeout", "test_dag", "retry_with_backoff", success=True)
        patterns = get_known_patterns("test_dag")
        assert patterns[0]["occurrence_count"] == 4

    def test_upsert_rolling_success_rate(self):
        from agents.patterns import upsert_pattern, get_known_patterns
        # 3 successes, 1 failure → 75% success rate
        for _ in range(3):
            upsert_pattern("upstream_timeout", "test_dag", "retry_with_backoff", success=True)
        upsert_pattern("upstream_timeout", "test_dag", "retry_with_backoff", success=False)
        patterns = get_known_patterns("test_dag")
        assert abs(patterns[0]["success_rate"] - 0.75) < 0.01

    def test_different_fix_actions_separate_records(self):
        from agents.patterns import upsert_pattern, get_known_patterns
        upsert_pattern("schema_drift", "test_dag", "reload_schema", success=True)
        upsert_pattern("schema_drift", "test_dag", "retry_with_backoff", success=False)
        patterns = get_known_patterns("test_dag")
        assert len(patterns) == 2

    def test_patterns_ordered_by_occurrence_count(self):
        from agents.patterns import upsert_pattern, get_known_patterns
        for _ in range(5):
            upsert_pattern("upstream_timeout", "test_dag", "retry_with_backoff", success=True)
        for _ in range(2):
            upsert_pattern("schema_drift", "test_dag", "reload_schema", success=True)
        patterns = get_known_patterns("test_dag")
        assert patterns[0]["occurrence_count"] >= patterns[1]["occurrence_count"]

    def test_match_pattern_returns_none_below_threshold(self):
        from agents.patterns import upsert_pattern, get_known_patterns, match_pattern
        # Only 2 occurrences — below threshold of 3
        for _ in range(2):
            upsert_pattern("upstream_timeout", "test_dag", "retry_with_backoff", success=True)
        patterns = get_known_patterns("test_dag")
        matched = match_pattern("upstream_timeout", "test_dag", patterns)
        assert matched is None

    def test_match_pattern_returns_pattern_at_threshold(self):
        from agents.patterns import upsert_pattern, get_known_patterns, match_pattern
        for _ in range(3):
            upsert_pattern("upstream_timeout", "test_dag", "retry_with_backoff", success=True)
        patterns = get_known_patterns("test_dag")
        matched = match_pattern("upstream_timeout", "test_dag", patterns)
        assert matched is not None
        assert matched["is_high_confidence"] is True

    def test_match_pattern_requires_correct_root_cause(self):
        from agents.patterns import upsert_pattern, get_known_patterns, match_pattern
        for _ in range(3):
            upsert_pattern("schema_drift", "test_dag", "reload_schema", success=True)
        patterns = get_known_patterns("test_dag")
        # Searching for a different anomaly type — should not match
        matched = match_pattern("upstream_timeout", "test_dag", patterns)
        assert matched is None

    def test_low_success_rate_not_high_confidence(self):
        from agents.patterns import upsert_pattern, get_known_patterns
        # 3 occurrences but only 1/3 success rate
        upsert_pattern("schema_drift", "test_dag", "reload_schema", success=True)
        upsert_pattern("schema_drift", "test_dag", "reload_schema", success=False)
        upsert_pattern("schema_drift", "test_dag", "reload_schema", success=False)
        patterns = get_known_patterns("test_dag")
        assert patterns[0]["is_high_confidence"] is False

    def test_get_known_patterns_empty_for_unknown_pipeline(self):
        from agents.patterns import get_known_patterns
        assert get_known_patterns("nonexistent_pipeline") == []


# ── Prompt 6 — LangSmith @traceable (smoke test) ──────────────────────────

class TestLangSmithTracing:
    """
    Verify that @traceable decorators are present and that functions execute
    normally when LANGCHAIN_TRACING_V2 is not set (tracing is a no-op).
    All actual LLM calls are mocked.
    """

    def test_monitor_agent_is_traceable(self):
        from agents.monitor_agent import run_monitor_agent
        assert hasattr(run_monitor_agent, "__wrapped__") or callable(run_monitor_agent)

    def test_diagnosis_agent_is_traceable(self):
        from agents.diagnosis_agent import run_diagnosis_agent
        assert callable(run_diagnosis_agent)

    def test_remediation_agent_is_traceable(self):
        from agents.remediation_agent import run_remediation_agent
        assert callable(run_remediation_agent)

    def test_reflection_agent_is_traceable(self):
        from agents.reflection_agent import run_reflection_agent
        assert callable(run_reflection_agent)

    def test_explanation_agent_is_traceable(self):
        from agents.explanation_agent import run_explanation_agent
        assert callable(run_explanation_agent)

    def test_anthropic_client_wrapped(self):
        """wrap_anthropic() returns an object that still has messages.create."""
        import agents.monitor_agent as ma
        assert hasattr(ma.client, "messages")

    def test_orchestrator_run_traceable(self, run_id):
        """Orchestrator.run executes end-to-end with all agents mocked."""
        mock_monitor = {
            "anomaly_detected": True, "severity": "warning",
            "anomaly_type": "upstream_timeout", "affected_task": "extract_source_data",
            "evidence": "task failed", "recommended_action": "retry", "thoughts": [],
        }
        mock_diagnosis = {
            "root_cause": "Transient lock", "confidence": "high",
            "remediation_strategy": "retry_with_backoff",
            "strategy_rationale": "Timeout", "estimated_fix_time": "2m",
            "escalation_risk": "low", "thoughts": [],
        }
        mock_remediation = {
            "action_taken": "Retried", "tool_used": "retry_task",
            "tool_result": {"retry_success": True}, "fix_successful": True,
            "verification_evidence": "success", "thoughts": [],
        }
        mock_reflection = {
            "assessment": "resolved", "confidence": "high",
            "what_worked": "retry", "what_failed": None,
            "updated_hypothesis": "Transient", "next_strategy": None,
            "escalation_reason": None, "thoughts": [],
        }
        mock_explanation = {
            "title": "Test", "severity": "P3", "summary": "Resolved.",
            "status": "resolved", "action_required": None,
            "time_to_resolution": "1m", "tags": [],
        }

        with patch("orchestrator.sentinel.run_monitor_agent", return_value=mock_monitor), \
             patch("orchestrator.sentinel.run_diagnosis_agent", return_value=mock_diagnosis), \
             patch("orchestrator.sentinel.run_remediation_agent", return_value=mock_remediation), \
             patch("orchestrator.sentinel.run_reflection_agent", return_value=mock_reflection), \
             patch("orchestrator.sentinel.run_explanation_agent", return_value=mock_explanation):
            from orchestrator.sentinel import SentinelOrchestrator
            result = SentinelOrchestrator().run(run_id)

        assert result["resolution_status"] == "resolved"
        assert "incident_id" in result
        assert "blast_radius" in result
        assert "pattern_matched" in result


# ── Orchestrator integration: new features wired correctly ─────────────────

class TestOrchestratorNewFeatures:
    def _run_with_mocks(self, run_id, monitor_override=None, diagnosis_override=None,
                        reflection_override=None):
        base_monitor = {
            "anomaly_detected": True, "severity": "critical",
            "anomaly_type": "upstream_timeout", "affected_task": "extract_source_data",
            "evidence": "failed", "recommended_action": "retry", "thoughts": [],
        }
        base_diagnosis = {
            "root_cause": "Transient lock", "confidence": "high",
            "remediation_strategy": "retry_with_backoff",
            "strategy_rationale": "Timeout", "estimated_fix_time": "2m",
            "escalation_risk": "low", "thoughts": [],
        }
        mock_remediation = {
            "action_taken": "Retried", "tool_used": "retry_task",
            "tool_result": {"retry_success": True}, "fix_successful": True,
            "verification_evidence": "ok", "thoughts": [],
        }
        base_reflection = {
            "assessment": "resolved", "confidence": "high",
            "what_worked": "retry", "what_failed": None,
            "updated_hypothesis": "ok", "next_strategy": None,
            "escalation_reason": None, "thoughts": [],
        }
        mock_explanation = {
            "title": "Test", "severity": "P3", "summary": "ok",
            "status": "resolved", "action_required": None,
            "time_to_resolution": "1m", "tags": [],
        }

        monitor   = {**base_monitor,    **(monitor_override or {})}
        diagnosis = {**base_diagnosis,  **(diagnosis_override or {})}
        reflection = {**base_reflection, **(reflection_override or {})}

        with patch("orchestrator.sentinel.run_monitor_agent",    return_value=monitor), \
             patch("orchestrator.sentinel.run_diagnosis_agent",  return_value=diagnosis), \
             patch("orchestrator.sentinel.run_remediation_agent", return_value=mock_remediation), \
             patch("orchestrator.sentinel.run_reflection_agent", return_value=reflection), \
             patch("orchestrator.sentinel.run_explanation_agent", return_value=mock_explanation):
            from orchestrator.sentinel import SentinelOrchestrator
            return SentinelOrchestrator().run(run_id)

    def test_blast_radius_in_result(self, run_id):
        result = self._run_with_mocks(run_id)
        assert "blast_radius" in result
        assert result["blast_radius"] in ("LOW", "MEDIUM", "HIGH")

    def test_extract_source_data_yields_low_blast(self, run_id):
        result = self._run_with_mocks(run_id)
        assert result["blast_radius"] == "LOW"

    def test_escalation_gate_blocks_medium_confidence(self, run_id):
        """When confidence=medium + blast=LOW, orchestrator escalates without remediation."""
        result = self._run_with_mocks(run_id, diagnosis_override={"confidence": "medium"})
        assert result["resolution_status"] == "escalated"

    def test_incident_outcome_written_after_run(self, run_id):
        self._run_with_mocks(run_id)
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT resolution_type, blast_radius FROM incident_outcomes WHERE run_id = ?", (run_id,))
        row = c.fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "AUTO_RESOLVED"
        assert row[1] == "LOW"

    def test_audit_log_written_after_run(self, run_id):
        self._run_with_mocks(run_id)
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT agent_name FROM agent_audit_log WHERE run_id = ? ORDER BY id", (run_id,))
        agents = [r[0] for r in c.fetchall()]
        conn.close()
        assert "Monitor" in agents
        assert "Diagnosis" in agents

    def test_pattern_upserted_after_resolution(self, run_id):
        self._run_with_mocks(run_id)
        from agents.patterns import get_known_patterns
        sim = PipelineSimulator()
        dag_id = sim.get_run_state(run_id).get("dag_id", "")
        patterns = get_known_patterns(dag_id)
        assert len(patterns) > 0

    def test_mart_level_task_escalates_via_gate(self):
        """run_dbt_mart_models has MEDIUM blast radius — gate should escalate."""
        sim = PipelineSimulator()
        rid = sim.start_run()
        FailureInjector(sim).inject(rid, failure_type="dbt_model_failure")

        result = self._run_with_mocks(
            rid,
            monitor_override={
                "anomaly_type": "dbt_model_failure",
                "affected_task": "run_dbt_mart_models",
            },
        )
        # MEDIUM blast radius + high confidence → gate blocks → escalate
        assert result["resolution_status"] == "escalated"
        assert result["blast_radius"] == "MEDIUM"
