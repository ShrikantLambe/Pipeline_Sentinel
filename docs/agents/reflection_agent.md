# Reflection Agent

**File:** `agents/reflection_agent.py`
**Role:** Self-assessment after each remediation attempt. Decides whether to resolve, retry with a new strategy, or escalate.

## Owns
- `assessment`: `"resolved"` | `"retry"` | `"escalate"`
- `next_strategy`: only when `assessment == "retry"` — must differ from all `tried_strategies`
- `updated_hypothesis`: one-sentence explanation of what changed or what was learned

## Must NOT do
- Suggest a strategy that was already tried (orchestrator enforces `tried_strategies`)
- Mark `assessment: "resolved"` if `fix_successful: false`
- Exceed retry cap — orchestrator enforces `MAX_RETRY_ATTEMPTS`

## Tool budget
Max 2 tool calls per run.

| Tool | When to call |
|---|---|
| `get_pipeline_status` | Check current run state after remediation |
| `check_row_count_anomaly` | Validate data integrity after fix |

## Input
```python
{
    "remediation_result": { fix_successful, tool_called, result },
    "attempt_number": int,
    "max_retries": int,
    "remediation_history": [{ attempt, strategy, result }, ...]
}
```

## Output contract
```json
{
  "assessment": "resolved" | "retry" | "escalate",
  "next_strategy": "<canonical strategy or null>",
  "updated_hypothesis": "<one sentence>",
  "confidence": "high" | "medium" | "low"
}
```

## Decision rules
- `fix_successful: true` → `assessment: "resolved"` (unless row count still anomalous)
- `fix_successful: false` AND attempts < max → `assessment: "retry"` with different strategy
- `fix_successful: false` AND attempts >= max → `assessment: "escalate"`
- All strategies exhausted → `assessment: "escalate"`
