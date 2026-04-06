# Orchestrator State Schema

Documents the exact shape of the state dict at each phase of `SentinelOrchestrator.run()`.
All keys are present after their respective phase completes. Keys marked `?` may be absent.

## Phase 0 — Run start

```python
run_id:        str    # pipeline run identifier (Airflow dag_run_id or simulator UUID)
dag_id:        str    # DAG name, e.g. "retail_pipeline"
pipeline_name: str    # same as dag_id, used as key in pattern memory
detected_at:   datetime
```

## Phase 1 — After Monitor Agent

```python
monitor_result = {
    "anomaly_detected": bool,
    "severity":         "critical" | "warning" | "none",
    "anomaly_type":     str | None,   # one of 7 canonical types or None
    "affected_task":    str | None,   # task_id that failed
    "evidence":         str,
    "recommended_action": str,
    "thoughts":         list[dict],   # internal ReAct trace
}
```

If `anomaly_detected == False` → orchestrator returns `{"status": "healthy", "run_id": run_id}` immediately.

## Phase 2 — After Blast Radius

```python
blast_info = {
    "blast_radius_score":    "LOW" | "MEDIUM" | "HIGH",
    "affected_level":        "raw" | "staging" | "mart" | "consumer",
    "affected_assets":       list[str],
    "affected_consumers":    list[str],
    "high_impact_consumers": list[str],
    "impact_description":    str,
}
blast_radius: str   # shorthand for blast_info["blast_radius_score"]
```

## Phase 3 — After Diagnosis Agent

```python
diagnosis = {
    "root_cause":            str,
    "remediation_strategy":  str,    # one of 6 canonical strategies
    "confidence":            "high" | "medium" | "low",
    "patterns_consulted":    list[dict],
    "pattern_matched":       bool,
    "thoughts":              list[dict],
}
confidence_str: str   # shorthand for diagnosis["confidence"]
```

## Phase 4 — After Escalation Gate

```python
can_remediate: bool
gate_reason:   str

# If can_remediate == False:
resolution_status  = "escalated"
remediation_steps  = []
reflection_notes   = gate_reason
retry_count        = 0
# → skips to Phase 6 (Explanation)
```

## Phase 5 — During Remediation + Reflection loop

Accumulates across retries (up to `MAX_RETRY_ATTEMPTS`):

```python
retry_count:        int             # current attempt number (1-based)
tried_strategies:   set[str]        # prevents repeating failed strategies
resolution_status:  str             # updated to "resolved" on success

remediation_steps = [
    {
        "attempt":  int,
        "strategy": str,
        "result": {
            "fix_attempted":  bool,
            "fix_successful": bool,
            "tool_called":    str,
            "result":         dict,
        }
    },
    ...
]

reflection_notes: str   # updated_hypothesis from last Reflection Agent call
```

### Reflection assessment routing

| `assessment` | `next_strategy` | Action |
|---|---|---|
| `"resolved"` | — | Exit loop, `resolution_status = "resolved"` |
| `"escalate"` | — | Exit loop, `resolution_status = "escalated"` |
| `"retry"` | new strategy | Continue loop if attempts < MAX_RETRIES |
| `"retry"` | already tried | Force escalate |
| `"retry"` | None | Force escalate |

## Phase 6 — After Explanation Agent

```python
explanation = {
    "title":               str,
    "severity":            "P1" | "P2" | "P3",
    "summary":             str,
    "status":              "resolved" | "escalated",
    "action_required":     str | None,
    "time_to_resolution":  str,
    "tags":                list[str],
}
```

## Phase 7–11 — Write / Audit / Outcome / Pattern

```python
incident_id: int    # incidents.id, written by write_incident_to_db()
# audit rows back-filled with incident_id via update_audit_incident_id()
# incident_outcomes row written via write_incident_outcome()
# incident_patterns upserted via upsert_pattern()
```

## Final return value

```python
{
    "incident_id":        int,
    "run_id":             str,
    "resolution_status":  "resolved" | "escalated" | "cancelled",
    "retry_count":        int,
    "explanation":        dict,    # full explanation dict from Phase 6
    "blast_radius":       str,
    "pattern_matched":    bool,
}
```
