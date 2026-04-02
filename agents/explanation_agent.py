import anthropic
import json
import os
from datetime import datetime
from simulator.database import get_connection
from .utils import extract_json
from langsmith import traceable
from langsmith.wrappers import wrap_anthropic

client = wrap_anthropic(anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"), timeout=60.0))

EXPLANATION_SYSTEM_PROMPT = """
You are the Explanation Agent for Pipeline Sentinel.

Your job is to write a clear, human-readable incident summary for a data engineer
or on-call analyst receiving a Slack alert. Write as if briefing a colleague
who just woke up at 3am.

The summary should include:
- What failed (1 sentence)
- What the agent tried (chronological, numbered list)
- Whether it was resolved or needs human action
- If escalated: exactly what the human needs to do

Tone: Direct, factual, no jargon that isn't necessary.
Length: 150-250 words max.

Respond ONLY with JSON:
{
  "title": "<short incident title e.g. 'Duplicate Key Failure — fct_sales'>",
  "severity": "P1" | "P2" | "P3",
  "summary": "<the plain-English narrative>",
  "status": "resolved" | "escalated",
  "action_required": "<null if resolved, or specific human action if escalated>",
  "time_to_resolution": "<e.g. '~4 minutes (2 retry attempts)' or 'unresolved'>",
  "tags": ["<failure_type>", "<dag_id>", "<strategy_used>"]
}
"""


@traceable(name="Explanation Agent", run_type="chain",
           tags=["pipeline-sentinel", "explanation"])
def run_explanation_agent(run_id: str,
                          incident_context: dict,
                          thought_callback=None) -> dict:
    """Generate plain-English incident explanation from full incident context."""

    response = client.messages.create(
        model=os.getenv("REFLECTION_MODEL", "claude-sonnet-4-5"),
        max_tokens=1000,
        system=EXPLANATION_SYSTEM_PROMPT,
        timeout=60,
        messages=[{
            "role": "user",
            "content": f"""
Generate an incident summary for run {run_id}.

Full incident context:
{json.dumps(incident_context, indent=2, default=str)}
"""
        }]
    )

    if thought_callback:
        thought_callback({"agent": "Explanation", "type": "thought",
                          "content": "Generating plain-English incident summary..."})

    final_text = ""
    for block in response.content:
        if hasattr(block, "text"):
            final_text = block.text

    result = extract_json(final_text)
    if not result:
        result = {"title": "Incident", "summary": final_text, "status": "unknown"}

    return result


def write_incident_to_db(run_id: str, dag_id: str,
                         failure_type: str,
                         resolution_status: str,
                         retry_attempts: int,
                         root_cause: str,
                         remediation_steps: list,
                         reflection_notes: str,
                         explanation: dict,
                         thought_log: list = None,
                         blast_radius: str = None,
                         patterns_consulted: list = None) -> int:
    """Write completed incident to the incidents table. Returns incident ID."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT INTO incidents
        (run_id, dag_id, failure_type, detected_at, resolved_at,
         resolution_status, retry_attempts, root_cause,
         remediation_steps, reflection_notes, explanation, thought_log,
         blast_radius, patterns_consulted)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        run_id, dag_id, failure_type,
        datetime.now(), datetime.now(),
        resolution_status, retry_attempts, root_cause,
        json.dumps(remediation_steps),
        reflection_notes,
        json.dumps(explanation),
        json.dumps(thought_log or []),
        blast_radius,
        json.dumps(patterns_consulted or []),
    ))
    incident_id = c.lastrowid
    conn.commit()
    conn.close()
    return incident_id
