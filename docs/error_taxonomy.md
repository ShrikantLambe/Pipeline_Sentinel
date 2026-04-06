# Error Taxonomy

Canonical mapping of failure modes to healing strategies, confidence floors, and escalation rules.
This is the single source of truth referenced by Diagnosis Agent, evals, and the orchestrator escalation gate.

## Failure modes

| `failure_type` | Affected task | Blast radius | Canonical strategy | Confidence floor | Auto-remediable? |
|---|---|---|---|---|---|
| `upstream_timeout` | `extract_source_data` | LOW | `retry_task` | high | ✅ Yes |
| `late_arrival` | `load_to_staging` | LOW | `extend_ingestion_window` | high | ✅ Yes |
| `partial_load` | `load_to_staging` | LOW | `retry_task` | medium | ⚠️ Medium conf only |
| `duplicate_keys` | `run_dbt_staging_models` | LOW | `apply_dedup` | high | ✅ Yes |
| `dbt_model_failure` | `run_dbt_mart_models` | MEDIUM | `run_dbt_full_refresh` | high | ❌ No (MEDIUM blast) |
| `schema_drift` | `validate_raw_schema` | LOW | `reload_schema` | high | ✅ Yes |
| `auth_failure` | `extract_source_data` | LOW | `escalate_to_manual_intervention` | low | ❌ No (always escalate) |

## Escalation rules

Auto-remediation is approved **only** when ALL of the following are true:
1. `confidence == "high"` (numeric equivalent ≥ 0.85)
2. `blast_radius == "LOW"`

Any other combination escalates immediately without attempting remediation.

**Special cases that always escalate regardless of blast radius or confidence:**
- `auth_failure` — credential rotation requires human action; the remediation hint explicitly says so
- Any run where the strategy has already been tried and failed (Reflection Agent enforces via `tried_strategies`)

## Confidence floors by failure type

| Failure type | Expected confidence | Why |
|---|---|---|
| `upstream_timeout` | high | Transient lock — retry is almost always the right call |
| `late_arrival` | high | Extending window is safe and reversible |
| `partial_load` | medium | Root cause (which shards) may need manual verification |
| `duplicate_keys` | high | Dedup is deterministic and safe |
| `dbt_model_failure` | high | Full refresh is safe but slow |
| `schema_drift` | high | Schema reload is idempotent |
| `auth_failure` | low | Human must rotate credentials |

## Healing strategies

| Strategy | Tool called | Reversible? | Side effects |
|---|---|---|---|
| `retry_task` | `retry_task` | Yes | May duplicate work if idempotency not guaranteed |
| `extend_ingestion_window` | `extend_ingestion_window` | Yes | Loads more data than originally expected |
| `apply_dedup` | `apply_dedup` | Yes | Removes rows; audit trail preserved |
| `reload_schema` | `reload_schema` | Yes | None |
| `run_dbt_full_refresh` | `run_dbt_full_refresh` | Yes | Expensive; clears and rebuilds all mart tables |
| `escalate_to_manual_intervention` | _(none)_ | N/A | Pages on-call engineer |
