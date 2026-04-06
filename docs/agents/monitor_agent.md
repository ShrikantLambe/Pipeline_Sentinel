# Monitor Agent

**File:** `agents/monitor_agent.py`
**Role:** Detection only — reads state, calls tools, returns an anomaly assessment. Never writes to the DB.

## Owns
- Deciding whether an anomaly exists and what type it is
- Selecting which detection tools to call and in what order
- Populating `affected_task` for blast radius assessment

## Must NOT do
- Write to any database table
- Suggest or attempt remediation
- Call tools outside its declared tool set (see below)

## Tool budget
Max 6 tool calls per run (one per available tool). Typical: 2–3 calls.

| Tool | When to call |
|---|---|
| `get_pipeline_status` | Always first — establishes baseline state |
| `get_failed_tasks` | When status shows failed tasks |
| `check_row_count_anomaly` | When rows loaded or row count is relevant |
| `check_schema_drift` | When `validate_raw_schema` failed or schema_drift suspected |
| `check_task_duration_anomaly` | When tasks succeeded but took unusually long |
| `check_zombie_run` | When tasks show `running` state past expected duration |

## Input
```
"Assess pipeline run: {run_id}. Check for any anomalies."
```

## Output contract
```json
{
  "anomaly_detected": true,
  "severity": "critical" | "warning" | "none",
  "anomaly_type": "failed_task | row_count_anomaly | duration_anomaly | schema_drift | zombie_task | auth_failure",
  "affected_task": "<task_id>",
  "evidence": "<one sentence>",
  "recommended_action": "<what Diagnosis Agent should focus on>"
}
```

## Confidence note
Monitor does not produce a confidence score — that is Diagnosis Agent's responsibility.
