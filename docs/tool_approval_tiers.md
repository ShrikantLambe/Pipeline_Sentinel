# Tool Approval Tiers

Defines which tools the Remediation Agent may call autonomously vs which require human approval.
The escalation gate in `orchestrator/sentinel.py` enforces Tier 1 vs Tier 2+ at the run level.
This document defines the per-tool rules that apply *within* an approved auto-remediation run.

---

## Tier 1 — Auto-approved (no human in the loop)

Requirements: `confidence == "high"` AND `blast_radius == "LOW"`

| Tool | What it does | Why safe |
|---|---|---|
| `retry_task` | Clears and re-runs a failed task | Idempotent in simulator; Airflow clear is reversible |
| `apply_dedup` | Removes duplicate rows from staging via `QUALIFY ROW_NUMBER()` | Deterministic; original data preserved in source |
| `reload_schema` | Refreshes schema definition from registry | Read-only against source; writes only to schema_registry |
| `extend_ingestion_window` | Widens the source data fetch window | Loads more data; does not delete or modify existing rows |

---

## Tier 2 — Auto-approved only with explicit high confidence + LOW blast (already gated)

These tools are technically callable in Tier 1 conditions but carry higher operational cost.
The orchestrator gate already blocks them for MEDIUM/HIGH blast. Listed separately for clarity.

| Tool | What it does | Risk |
|---|---|---|
| `run_dbt_full_refresh` | Drops and rebuilds all dbt mart tables | Expensive (minutes); downstream views may be temporarily missing |

**Note:** `run_dbt_full_refresh` is the canonical strategy for `dbt_model_failure`, which affects
`run_dbt_mart_models` (MEDIUM blast). Therefore this tool is **never auto-approved in practice**
under the current blast radius rules — `dbt_model_failure` always escalates. If the DAG topology
changes so that `run_dbt_mart_models` has LOW blast, this tier should be revisited.

---

## Tier 3 — Never automated (human approval required)

| Tool / action | Reason |
|---|---|
| `escalate_to_manual_intervention` | Not a tool call — signals human required; Explanation Agent writes action_required |
| Credential rotation / auth fix | Cannot be scripted safely; security team owns credential lifecycle |
| Deleting or truncating tables | Irreversible data loss; not in the approved tool set |
| Force-pushing to Airflow main DAG | Affects all future runs; requires code review |
| Modifying `schema_registry.expected_columns` | Changes the baseline — must be a deliberate, reviewed act |

---

## Monitor and Diagnosis tools (read-only — always approved)

These tools are used by Monitor and Diagnosis Agents and carry no write risk.

| Tool | Agent |
|---|---|
| `get_pipeline_status` | Monitor, Diagnosis, Remediation (verification), Reflection |
| `check_row_count_anomaly` | Monitor, Diagnosis, Reflection |
| `get_failed_tasks` | Monitor, Diagnosis |
| `check_task_duration_anomaly` | Monitor |
| `check_zombie_run` | Monitor |
| `check_schema_drift` | Monitor |

---

## Enforcement

The approval tier is enforced by the escalation gate in `orchestrator/sentinel.py`:
```python
can_remediate, gate_reason = should_auto_remediate(confidence_str, blast_radius)
if not can_remediate:
    resolution_status = "escalated"
    # Remediation Agent is never called
```

There is no per-tool enforcement inside `agents/tools.py` — the gate is the single control point.
If a new tool is added, this document must be updated and the tool classified into a tier before
it can be wired into any agent.
