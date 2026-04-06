# Diagnosis Agent

**File:** `agents/diagnosis_agent.py`
**Role:** Root cause analysis and strategy selection. Consults pattern memory before calling the LLM.

## Owns
- Selecting `remediation_strategy` from the canonical set (see error_taxonomy.md)
- Setting `confidence`: `"high"` | `"medium"` | `"low"`
- Declaring `root_cause` in one sentence
- Reporting `patterns_consulted` and `pattern_matched`

## Must NOT do
- Execute any remediation action
- Write to the database
- Select a strategy outside the canonical set without flagging `confidence: "low"`

## Fast-path (pattern memory)
Before calling the LLM, checks `get_known_patterns(pipeline_name)`.
If a pattern matches with `occurrence_count >= 3` AND `success_rate >= 0.90`:
- Returns immediately with the cached strategy
- Sets `pattern_matched: True`
- Skips the ReAct tool loop entirely

## Tool budget
Max 4 tool calls per run. Typical: 2 calls.

| Tool | When to call |
|---|---|
| `get_pipeline_status` | Verify current task states |
| `get_failed_tasks` | Read error messages for root cause clues |
| `check_row_count_anomaly` | When `late_arrival` or `partial_load` is suspected |

## Input
Monitor alert dict: `{anomaly_type, affected_task, evidence, recommended_action}`

## Output contract
```json
{
  "root_cause": "<one sentence>",
  "remediation_strategy": "<canonical strategy name>",
  "confidence": "high" | "medium" | "low",
  "patterns_consulted": [...],
  "pattern_matched": true | false
}
```

## Canonical strategies
`retry_task` · `apply_dedup` · `reload_schema` · `extend_ingestion_window` ·
`run_dbt_full_refresh` · `escalate_to_manual_intervention`
