"""
Slack alerting for Sentinel incidents.

Sends a formatted Slack message for every completed incident — resolved ones
for visibility, escalated ones with explicit action_required callout.

Set SLACK_WEBHOOK_URL in .env to enable. If unset, send_slack_alert() is a no-op.
"""

import json
import logging
import os
from datetime import datetime

import requests

log = logging.getLogger("sentinel.alerting")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

_SEVERITY_EMOJI = {"P1": "🔴", "P2": "🟡", "P3": "🟢"}
_BLAST_COLOR = {"HIGH": "#e74c3c", "MEDIUM": "#f39c12", "LOW": "#2ecc71"}
_STATUS_EMOJI = {"escalated": "🚨", "resolved": "✅"}


def send_slack_alert(
    explanation: dict,
    run_id: str,
    resolution_status: str,
    blast_radius: str = None,
    incident_id: int = None,
) -> bool:
    """
    Post a Slack attachment for a completed Sentinel incident.
    Returns True if the message was delivered, False otherwise (including no-op).
    """
    if not SLACK_WEBHOOK_URL:
        log.debug("SLACK_WEBHOOK_URL not set — skipping alert.")
        return False

    severity      = explanation.get("severity", "P2")
    title         = explanation.get("title", "Pipeline Incident")
    summary       = explanation.get("summary", "")
    action        = explanation.get("action_required")
    ttr           = explanation.get("time_to_resolution", "unknown")
    tags          = explanation.get("tags", [])

    sev_emoji     = _SEVERITY_EMOJI.get(severity, "🟡")
    status_emoji  = _STATUS_EMOJI.get(resolution_status, "⚠️")
    attach_color  = _BLAST_COLOR.get(blast_radius, "#888888")

    fields = [
        {"title": "Run ID",       "value": f"`{run_id}`",                            "short": True},
        {"title": "Severity",     "value": f"{sev_emoji} {severity}",                "short": True},
        {"title": "Resolution",   "value": f"{status_emoji} {resolution_status.upper()}", "short": True},
        {"title": "Time to Res.", "value": ttr,                                       "short": True},
    ]
    if blast_radius:
        fields.append({"title": "Blast Radius", "value": blast_radius, "short": True})
    if incident_id is not None:
        fields.append({"title": "Incident #", "value": str(incident_id), "short": True})
    if action:
        fields.append({"title": "⚠ Action Required", "value": action, "short": False})

    payload = {
        "attachments": [{
            "color":    attach_color,
            "title":    f"{status_emoji} {title}",
            "text":     summary,
            "fields":   fields,
            "footer":   f"Pipeline Sentinel  •  {', '.join(tags)}",
            "ts":       int(datetime.now().timestamp()),
            "mrkdwn_in": ["text", "fields"],
        }]
    }

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            log.info("Slack alert sent for incident #%s (%s)", incident_id, resolution_status)
            return True
        log.warning("Slack returned HTTP %s: %s", resp.status_code, resp.text[:200])
        return False
    except Exception as exc:
        log.error("Slack alert failed: %s", exc)
        return False
