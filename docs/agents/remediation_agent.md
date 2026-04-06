# Remediation Agent

**File:** `agents/remediation_agent.py`
**Role:** Executes the strategy chosen by Diagnosis Agent. Only runs after the escalation gate approves.

## Owns
- Calling exactly one fix tool per attempt
- Verifying success via `get_pipeline_status` after the fix
- Returning `fix_successful: true/false`

## Must NOT do
- Choose a different strategy than what Diagnosis Agent specified
- Call more than one fix tool in a single attempt
- Write incident records (that is Explanation Agent's responsibility)

## Escalation gate pre-condition
**This agent only runs when:**
- `confidence == "high"` AND `blast_radius == "LOW"`

If the gate fails, the orchestrator escalates immediately and this agent is never called.

## Tool budget
Max 3 tool calls per run: 1 fix tool + 1–2 verification calls.

| Tool | Purpose |
|---|---|
| `retry_task` | Re-run a failed task (with optional backoff) |
| `apply_dedup` | Remove duplicate rows from staging |
| `reload_schema` | Refresh schema definition from source |
| `extend_ingestion_window` | Widen the data fetch window for late arrivals |
| `run_dbt_full_refresh` | Re-run dbt models with --full-refresh |
| `get_pipeline_status` | Verify state after fix |

## Input
Diagnosis result: `{root_cause, remediation_strategy, confidence}`

## Output contract
```json
{
  "fix_attempted": true,
  "fix_successful": true | false,
  "tool_called": "<tool name>",
  "result": { ... }
}
```
