"""
Prompt 1 — Audit Trail
Structured per-agent transition logging to the `agent_audit_log` SQLite table.

Usage (from orchestrator):
    from agents.audit import log_agent_transition, get_incident_audit, update_audit_incident_id

    log_agent_transition(
        run_id=run_id,
        agent_name="Monitor",
        input_summary={"run_id": run_id},
        decision="anomaly_detected: schema_drift",
        confidence="high",
        output_summary=monitor_result,
    )

    # After write_incident_to_db() returns the incident_id:
    update_audit_incident_id(run_id, incident_id)

    # Query full decision chain for a given incident:
    chain = get_incident_audit(incident_id)
"""

import json
from datetime import datetime
from simulator.database import get_connection


def log_agent_transition(
    run_id: str,
    agent_name: str,
    input_summary: dict,
    decision: str,
    output_summary: dict,
    confidence: str = None,
    incident_id: int = None,
) -> int:
    """
    Write one audit record for an agent transition. Returns the row id.

    Parameters
    ----------
    run_id:         Pipeline run ID (always available).
    agent_name:     "Monitor" | "Diagnosis" | "Remediation" | "Reflection" | "Explanation"
    input_summary:  Key fields passed into the agent (serialised to JSON).
    decision:       Human-readable one-liner of the agent's conclusion.
    output_summary: Key fields the agent returned (serialised to JSON, truncated to 4 KB).
    confidence:     "high" | "medium" | "low" | None if not applicable.
    incident_id:    FK to incidents.id — populated later via update_audit_incident_id().
    """
    def _safe(obj, limit=4096):
        s = json.dumps(obj, default=str)
        return s[:limit] if len(s) > limit else s

    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO agent_audit_log
            (incident_id, run_id, agent_name, input_summary,
             decision, confidence, output_summary, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            incident_id,
            run_id,
            agent_name,
            _safe(input_summary),
            decision,
            confidence,
            _safe(output_summary),
            datetime.now(),
        ),
    )
    row_id = c.lastrowid
    conn.commit()
    conn.close()
    return row_id


def update_audit_incident_id(run_id: str, incident_id: int) -> None:
    """
    Back-fill incident_id on all audit log rows for a run once the incident
    record has been written and its ID is known.
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "UPDATE agent_audit_log SET incident_id = ? WHERE run_id = ? AND incident_id IS NULL",
        (incident_id, run_id),
    )
    conn.commit()
    conn.close()


def get_incident_audit(incident_id: int) -> list[dict]:
    """
    Return the full agent decision chain for one incident, ordered chronologically.

    Each record contains:
        agent_name, recorded_at, input_summary (dict), decision,
        confidence, output_summary (dict)
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT agent_name, recorded_at, input_summary,
               decision, confidence, output_summary
        FROM agent_audit_log
        WHERE incident_id = ?
        ORDER BY id
        """,
        (incident_id,),
    )
    cols = ["agent_name", "recorded_at", "input_summary", "decision", "confidence", "output_summary"]
    rows = [dict(zip(cols, row)) for row in c.fetchall()]
    conn.close()

    for row in rows:
        for field in ("input_summary", "output_summary"):
            try:
                row[field] = json.loads(row[field])
            except Exception:
                pass
    return rows
