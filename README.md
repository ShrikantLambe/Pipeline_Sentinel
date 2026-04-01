# Pipeline Sentinel 🛡️

A **multi-agent self-healing data pipeline monitor** powered by Claude and the Anthropic API. Five specialized AI agents coordinate in a ReAct loop to detect failures, diagnose root causes, attempt automated remediation, reflect on outcomes, and write plain-English incident reports — with or without a real Airflow instance.

---

## How it works

```
Pipeline Run (Airflow or Simulator)
          │
          ▼
   ┌─────────────┐
   │   Monitor   │  Detects anomalies: task failures, row count
   │    Agent    │  drift, duration SLA breaches, zombie tasks,
   └──────┬──────┘  auth failures
          │ anomaly detected
          ▼
   ┌─────────────┐
   │  Diagnosis  │  Maps anomaly → root cause + selects
   │    Agent    │  remediation strategy
   └──────┬──────┘
          │
          ▼
   ┌─────────────────────────────┐
   │  Remediation + Reflection   │  Executes fix, then self-assesses.
   │     Loop  (max 3 retries)   │  Tries alternative strategies if
   └──────────────┬──────────────┘  the first attempt fails.
                  │
          resolved │ escalated
          ─────────┴──────────
                  │
                  ▼
   ┌─────────────┐
   │ Explanation │  Writes plain-English incident summary
   │    Agent    │  (title, severity, root cause, actions taken)
   └──────┬──────┘
          │
          ▼
   Incident Log (SQLite) + Streamlit UI
```

Each agent follows the **ReAct pattern** (Reason → Act → Observe → Repeat): it receives a system prompt, calls tools to gather evidence, reasons about the results, and loops until it has enough information to return a structured JSON decision.

---

## Agent details

### Monitor Agent
Polls the pipeline state using five tools and returns a structured anomaly report.

| Tool | What it checks |
|---|---|
| `get_pipeline_status` | Full run state and all task statuses |
| `check_row_count_anomaly` | Actual vs expected rows (default ±15% threshold) |
| `get_failed_tasks` | Tasks in `failed` state with error messages |
| `check_task_duration_anomaly` | Tasks that ran >3× their baseline duration |
| `check_zombie_run` | Tasks stuck in `running` for >30 minutes |

**Output:** `anomaly_detected`, `severity`, `anomaly_type`, `affected_task`, `evidence`, `recommended_action`

### Diagnosis Agent
Receives the monitor alert and maps it to a root cause and remediation strategy.

**Strategies it can recommend:**
- `retry_with_backoff` — transient errors, source timeouts
- `apply_dedup` — duplicate key violations
- `reload_schema` — column mismatches, schema drift
- `extend_ingestion_window` — late-arriving data, low row counts
- `run_dbt_full_refresh` — broken dbt models, incremental staleness
- `escalate_to_manual_intervention` — billing issues, auth failures it cannot fix

**Output:** `root_cause`, `confidence`, `remediation_strategy`, `strategy_rationale`, `escalation_risk`

### Remediation Agent
Executes the recommended fix using pipeline action tools, then verifies success.

| Tool | What it does | Airflow mode |
|---|---|---|
| `retry_task` | Re-queues a failed task | Clears task instance via REST API |
| `apply_dedup` | Removes duplicate rows | Simulator only |
| `reload_schema` | Re-syncs schema definition | Simulator only |
| `extend_ingestion_window` | Extends data collection window | Simulator only |
| `run_dbt_full_refresh` | Rebuilds dbt models from scratch | Triggers new DAG run |

**Output:** `action_taken`, `tool_used`, `tool_result`, `fix_successful`, `verification_evidence`

### Reflection Agent
Self-assesses the remediation outcome using objective evidence (not just the tool's return value). Decides whether to mark the incident resolved, retry with a different strategy, or escalate to humans.

- Tracks all previously attempted strategies — will never recommend the same failed approach twice
- Checks actual pipeline state (row counts, task status) rather than trusting the remediation tool result
- Returns `assessment`: `resolved` | `retry` | `escalate`

### Explanation Agent
Generates a concise plain-English incident narrative from the full incident context. Single API call, no tools.

**Output:** `title`, `severity` (P1/P2/P3), `summary`, `status`, `action_required`, `time_to_resolution`, `tags`

---

## Failure modes

| Type | Affected task | Row count impact | Primary fix |
|---|---|---|---|
| `schema_drift` | validate_raw_schema | 0% (load blocked) | reload_schema |
| `late_arrival` | load_to_staging | 22% of expected | extend_ingestion_window |
| `duplicate_keys` | run_dbt_staging_models | 112% of expected | apply_dedup |
| `upstream_timeout` | extract_source_data | 0% | retry_with_backoff |
| `dbt_model_failure` | run_dbt_mart_models | 97% (loaded, model broken) | run_dbt_full_refresh |
| `partial_load` | load_to_staging | 86% (borderline threshold) | retry shards |
| `auth_failure` | extract_source_data | 0% | escalate (cannot auto-fix) |

---

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env           # then add your ANTHROPIC_API_KEY
python -c "from simulator.database import init_db; init_db()"
streamlit run app/streamlit_app.py
```

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | **required** | Claude API key |
| `PIPELINE_DB_PATH` | `data/sentinel.db` | SQLite file location |
| `REFLECTION_MODEL` | `claude-sonnet-4-5` | Model used by all agents |
| `MAX_RETRY_ATTEMPTS` | `3` | Remediation retries before escalating |
| `USE_AIRFLOW` | `false` | Set `true` to connect a real Airflow instance |
| `AIRFLOW_URL` | `http://localhost:8080` | Airflow webserver URL |
| `AIRFLOW_USERNAME` | `airflow` | Airflow credentials |
| `AIRFLOW_PASSWORD` | `airflow` | Airflow credentials |

---

## Airflow integration

Pipeline Sentinel can monitor and remediate a real Airflow instance instead of the built-in simulator.

**Requirements:**
- Airflow 2.7+ with the REST API enabled
- Basic auth enabled:
  ```yaml
  # In your Airflow docker-compose or config
  AIRFLOW__API__AUTH_BACKENDS: "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
  ```

**How it works:**
1. Switch to **Live Airflow** mode in the UI sidebar
2. Click **Poll for new failures** — Sentinel queries Airflow for recently failed DAG runs not yet in its DB
3. Select a failed run and click **Run Sentinel**
4. Sentinel syncs the run's state and task instances from Airflow into its local DB, then runs the full agent pipeline
5. In Airflow mode, `retry_task` calls `POST /dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/clear` and `run_dbt_full_refresh` triggers a new DAG run with `conf={"full_refresh": true}`

---

## Testing

```bash
# Run all tests (no LLM calls — tools and orchestration are mocked)
pytest tests/ -v

# Single test file
pytest tests/test_simulator.py -v

# Single test
pytest tests/test_agents.py::test_apply_dedup -v
```

Tests use isolated temporary SQLite databases — no shared state between runs.

---

## Project structure

```
agents/
  monitor_agent.py       # Anomaly detection (5 tools, ReAct loop)
  diagnosis_agent.py     # Root cause analysis + strategy selection
  remediation_agent.py   # Fix execution + verification (6 tools)
  reflection_agent.py    # Outcome self-assessment, retry logic
  explanation_agent.py   # Plain-English incident narrative + DB write
  tools.py               # All tool functions (SQLite + Airflow REST)
  utils.py               # JSON extraction utility

orchestrator/
  sentinel.py            # Coordinates all 5 agents, manages retry loop

simulator/
  pipeline.py            # Synthetic DAG state machine (8-task pipeline)
  failure_injector.py    # Injects 7 failure modes into pipeline state
  database.py            # SQLite schema + connection helper
  airflow_connector.py   # Airflow REST API client + DB sync

app/
  streamlit_app.py       # Live monitoring UI with agent thought stream

tests/
  test_agents.py         # Tool function tests
  test_reflection.py     # Orchestration logic tests (mocked agents)
  test_simulator.py      # Pipeline simulation tests
```
