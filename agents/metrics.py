"""
Prompt 3 — Self-Healing Rate Metric
Reads from `incident_outcomes` to compute self-healing KPIs and daily trend data.
"""

from datetime import datetime, timedelta
from simulator.database import get_connection


# ── Resolution type constants ───────────────────────────────────────────────

AUTO_RESOLVED = "AUTO_RESOLVED"
ESCALATED     = "ESCALATED"
FAILED        = "FAILED"

CONFIDENCE_SCORE = {"high": 0.9, "medium": 0.7, "low": 0.4}


def write_incident_outcome(
    run_id: str,
    dag_id: str,
    incident_id: int,
    detected_at: datetime,
    resolved_at: datetime,
    resolution_status: str,       # "resolved" | "escalated"
    root_cause_category: str,
    agent_confidence: str,        # "high" | "medium" | "low"
    blast_radius: str = None,     # "LOW" | "MEDIUM" | "HIGH" | None
) -> None:
    """
    Write one row to `incident_outcomes` after each incident is closed.
    Called from the orchestrator right after write_incident_to_db().
    """
    resolution_type = AUTO_RESOLVED if resolution_status == "resolved" else ESCALATED
    confidence_score = CONFIDENCE_SCORE.get(agent_confidence, 0.0)

    mttr = None
    if detected_at and resolved_at:
        try:
            d = detected_at if isinstance(detected_at, datetime) else datetime.fromisoformat(str(detected_at))
            r = resolved_at if isinstance(resolved_at, datetime) else datetime.fromisoformat(str(resolved_at))
            mttr = (r - d).total_seconds()
        except Exception:
            pass

    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO incident_outcomes
            (incident_id, run_id, dag_id, detected_at, resolved_at,
             resolution_type, root_cause_category, agent_confidence_score,
             mttr_seconds, blast_radius)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            incident_id, run_id, dag_id,
            detected_at, resolved_at,
            resolution_type, root_cause_category,
            confidence_score, mttr, blast_radius,
        ),
    )
    conn.commit()
    conn.close()


def compute_self_healing_metrics(days: int = 30) -> dict:
    """
    Compute self-healing KPIs over the last `days` days.

    Returns
    -------
    {
        "total_incidents": int,
        "auto_resolved": int,
        "escalated": int,
        "self_healing_rate_pct": float,
        "avg_mttr_auto_seconds": float | None,
        "avg_mttr_escalated_seconds": float | None,
        "daily_trend": [{"date": "YYYY-MM-DD", "total": int, "auto_resolved": int, "rate_pct": float}, ...]
    }
    """
    since = datetime.now() - timedelta(days=days)

    conn = get_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT resolution_type, mttr_seconds, date(created_at) as day
        FROM incident_outcomes
        WHERE created_at >= ?
        ORDER BY created_at
        """,
        (since.isoformat(),),
    )
    rows = c.fetchall()
    conn.close()

    total       = len(rows)
    auto_rows   = [r for r in rows if r[0] == AUTO_RESOLVED]
    esc_rows    = [r for r in rows if r[0] == ESCALATED]
    auto_count  = len(auto_rows)
    esc_count   = len(esc_rows)

    rate = (auto_count / total * 100) if total > 0 else 0.0

    def _avg_mttr(group):
        vals = [r[1] for r in group if r[1] is not None]
        return sum(vals) / len(vals) if vals else None

    # Daily trend
    daily: dict[str, dict] = {}
    for res_type, _, day in rows:
        if day not in daily:
            daily[day] = {"date": day, "total": 0, "auto_resolved": 0}
        daily[day]["total"] += 1
        if res_type == AUTO_RESOLVED:
            daily[day]["auto_resolved"] += 1

    trend = []
    for day in sorted(daily):
        d = daily[day]
        d["rate_pct"] = round(d["auto_resolved"] / d["total"] * 100, 1) if d["total"] > 0 else 0.0
        trend.append(d)

    return {
        "total_incidents":          total,
        "auto_resolved":            auto_count,
        "escalated":                esc_count,
        "self_healing_rate_pct":   round(rate, 1),
        "avg_mttr_auto_seconds":   _avg_mttr(auto_rows),
        "avg_mttr_escalated_seconds": _avg_mttr(esc_rows),
        "daily_trend":             trend,
    }
