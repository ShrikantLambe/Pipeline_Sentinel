# Explanation Agent

**File:** `agents/explanation_agent.py`
**Role:** Single LLM call (no tools) that produces a human-readable incident summary for on-call engineers.

## Owns
- Writing the plain-English incident narrative
- Assigning `severity`: `"P1"` | `"P2"` | `"P3"`
- Setting `action_required` to a specific human instruction (or null if resolved)
- Generating `tags` for routing / filtering

## Must NOT do
- Call any tools
- Change the `resolution_status` — that is set by Orchestrator
- Write to the database — `write_incident_to_db()` is called by Orchestrator after this agent returns

## Tool budget
Zero tools. Single `messages.create()` call.

## Severity guidelines
| Severity | Condition |
|---|---|
| P1 | `blast_radius == "HIGH"` OR `auth_failure` type |
| P2 | `blast_radius == "MEDIUM"` OR escalated for any reason |
| P3 | `blast_radius == "LOW"` AND `resolution_status == "resolved"` |

## Input
Full `incident_context` dict including monitor alert, diagnosis, blast_radius, remediation attempts, reflection notes, and `resolution_status`.

## Output contract
```json
{
  "title": "<short incident title e.g. 'Duplicate Key Failure — fct_sales'>",
  "severity": "P1" | "P2" | "P3",
  "summary": "<150-250 word plain-English narrative>",
  "status": "resolved" | "escalated",
  "action_required": "<null if resolved, or specific human instruction>",
  "time_to_resolution": "<e.g. '~4 minutes (2 retry attempts)' or 'unresolved'>",
  "tags": ["<failure_type>", "<dag_id>", "<strategy_used>"]
}
```
