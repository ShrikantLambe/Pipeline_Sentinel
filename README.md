# Pipeline Sentinel 🛡️

A **multi-agent self-healing data pipeline monitor** powered by Claude and the Anthropic API. Specialized AI agents coordinate in a ReAct loop to detect failures, assess blast radius, diagnose root causes, attempt automated remediation, reflect on outcomes, and write plain-English incident reports — autonomously, without a human clicking a button.

---

## How it works

```
Pipeline Run (Airflow or Simulator)
          │
          ▼
   ┌─────────────┐
   │   Monitor   │  6 detection tools: task failures, row count drift,
   │    Agent    │  duration SLA breaches, zombie tasks, schema drift,
   └──────┬──────┘  auth failures
          │ anomaly detected
          ▼
   ┌─────────────┐
   │   Blast     │  Scores downstream impact LOW / MEDIUM / HIGH
   │   Radius    │  via BFS over the dependency graph
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │  Diagnosis  │  Checks pattern memory first (fast-path if ≥3 prior
   │    Agent    │  successes). Otherwise: full ReAct loop → root cause
   └──────┬──────┘  + remediation strategy + confidence score
          │
          ▼
   ┌──────────────────────┐
   │   Escalation Gate    │  Auto-remediate ONLY if confidence == "high"
   │                      │  AND blast_radius == "LOW". Otherwise escalate.
   └──────────┬───────────┘
     approved │ blocked → ESCALATED
              ▼
   ┌─────────────────────────────┐
   │  Remediation + Reflection   │  Executes fix, self-assesses, retries
   │     Loop  (max 3 retries)   │  with different strategy if needed.
   └──────────────┬──────────────┘  Tracks tried_strategies to avoid loops.
                  │
          resolved│escalated
                  ▼
   ┌─────────────┐
   │ Explanation │  Single LLM call → plain-English incident summary
   │    Agent    │  (title, severity P1/P2/P3, action_required)
   └──────┬──────┘
          │
          ▼
   SQLite DB (7 tables) + Streamlit UI + Slack alert
```

Each agent follows the **ReAct pattern** (Reason → Act → Observe → Repeat): calls tools to gather evidence, reasons about the results, and loops until it returns a structured JSON decision. Every thought, tool call, and observation streams live to the UI.

---

## Agents

### Monitor Agent
Six tools, ReAct loop, returns a structured anomaly report. Never writes to the DB.

| Tool | What it checks |
|---|---|
| `get_pipeline_status` | Full run state and all task statuses |
| `check_row_count_anomaly` | Actual vs expected rows (default ±15% threshold) |
| `get_failed_tasks` | Tasks in `failed` state with error messages |
| `check_task_duration_anomaly` | Tasks that ran >3× their baseline duration |
| `check_zombie_run` | Tasks stuck in `running` for >30 minutes |
| `check_schema_drift` | Column mismatch vs registered baseline (BREAKING / WARNING / INFO) |

### Diagnosis Agent
Checks pattern memory before calling the LLM. If a pattern with `occurrence_count ≥ 3` and `success_rate ≥ 0.90` exists, returns the cached strategy immediately (shown as ⚡ in the UI). Otherwise runs the full ReAct loop.

**Canonical strategies:** `retry_task` · `apply_dedup` · `reload_schema` · `extend_ingestion_window` · `run_dbt_full_refresh` · `escalate_to_manual_intervention`

### Blast Radius Assessment
Not an LLM agent — a deterministic BFS over `DEPENDENCY_GRAPH` in `agents/blast_radius.py`. Scores each failing task's impact based on its DAG layer and direct downstream consumers.

| Score | Condition |
|---|---|
| LOW | Raw or staging layer; no high-impact consumers directly downstream |
| MEDIUM | dbt mart models; reporting tables may be stale |
| HIGH | Executive dashboards, ML models, or SLA-bound reports affected |

### Escalation Gate
`should_auto_remediate(confidence, blast_radius)` — enforces the rule: **auto-remediate only when `confidence == "high"` AND `blast_radius == "LOW"`**. Any other combination escalates immediately and Remediation Agent is never called. See [DECISIONS.md](DECISIONS.md) ADR-006.

### Remediation Agent
Executes exactly one fix tool per attempt, then verifies via `get_pipeline_status`.

| Tool | What it does | Airflow mode |
|---|---|---|
| `retry_task` | Re-queues a failed task | Clears task instance via REST API |
| `apply_dedup` | Removes duplicate rows via `QUALIFY ROW_NUMBER()` | Simulator only |
| `reload_schema` | Re-syncs schema definition from registry | Simulator only |
| `extend_ingestion_window` | Widens the source data fetch window | Simulator only |
| `run_dbt_full_refresh` | Drops and rebuilds dbt mart tables | Triggers new DAG run with `full_refresh=true` |

### Reflection Agent
Self-assesses the remediation outcome using objective evidence. Decides `resolved` / `retry` (with a different strategy) / `escalate`. Tracks `tried_strategies` — will never suggest a previously failed approach.

### Explanation Agent
Single LLM call, no tools. Writes a 150–250 word plain-English incident narrative for on-call engineers. Sets severity (P1/P2/P3) and `action_required`.

---

## Failure modes

| Type | Affected task | Blast radius | Canonical fix | Auto-remediable |
|---|---|---|---|---|
| `upstream_timeout` | `extract_source_data` | LOW | `retry_task` | ✅ |
| `late_arrival` | `load_to_staging` | LOW | `extend_ingestion_window` | ✅ |
| `partial_load` | `load_to_staging` | LOW | `retry_task` | ✅ |
| `duplicate_keys` | `run_dbt_staging_models` | LOW | `apply_dedup` | ✅ |
| `schema_drift` | `validate_raw_schema` | LOW | `reload_schema` | ✅ |
| `dbt_model_failure` | `run_dbt_mart_models` | MEDIUM | `run_dbt_full_refresh` | ❌ (MEDIUM blast) |
| `auth_failure` | `extract_source_data` | LOW | escalate | ❌ (always escalate) |

---

## Autonomous operation

Sentinel runs without human intervention via the `watcher/` package.

### Three trigger paths

```
1. Airflow on_failure_callback  →  POST /webhook/failure  →  SentinelOrchestrator.run()
2. Watcher poll (every 5 min)  →  poll_failed_runs()      →  SentinelOrchestrator.run()
3. Proactive check (every 15 min):
     - check_pipeline_freshness()  →  stale DAG  →  synthetic run  →  SentinelOrchestrator.run()
     - check_row_count_baselines() →  count anomaly → synthetic run → SentinelOrchestrator.run()
```

All three paths write to the same DB and appear in the same Streamlit dashboard.

### Start the watcher
```bash
# Autonomous polling + proactive checks
python -m watcher.sentinel_watcher

# Webhook server (for Airflow on_failure_callback)
uvicorn watcher.webhook_server:app --host 0.0.0.0 --port 8765
```

### Configure Airflow to call the webhook
```python
def sentinel_callback(context):
    import requests
    requests.post("http://sentinel-host:8765/webhook/failure",
                  json={"dag_id": context["dag"].dag_id, "run_id": context["run_id"]},
                  timeout=5)

dag = DAG("your_dag", on_failure_callback=sentinel_callback, ...)
```

### Slack alerts
Set `SLACK_WEBHOOK_URL` in `.env`. Every completed incident (resolved and escalated) posts a formatted Slack attachment with severity, blast radius, MTTR, and `action_required` if escalation is needed.

---

## Dashboard

```bash
streamlit run app/streamlit_app.py
```

- **KPI row** — total incidents (30d), auto-resolved count + %, escalated count, MTTR for both resolution types, daily trend chart
- **Live thought stream** — every agent reasoning step, tool call, and observation streamed in real time
- **Agent phase bar** — shows Monitor → Diagnosis → Remediation → Reflection → Explanation progress
- **Verdict card** — resolution status, root cause, remediation attempts, blast radius badge (color-coded), ⚡ pattern-matched badge
- **Autonomous Watcher panel** — shows watcher-triggered runs in progress (polls every 0.5s) and completed incidents with thought log replay
- **Incident history** — last 20 incidents with expandable reasoning replay

---

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env           # add ANTHROPIC_API_KEY at minimum
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
| `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` | `airflow` | Airflow credentials |
| `WATCHER_DAG_IDS` | — | Comma-separated DAG IDs for the autonomous watcher |
| `WATCHER_POLL_INTERVAL_SECONDS` | `300` | Airflow poll frequency |
| `WATCHER_PROACTIVE_INTERVAL_SECONDS` | `900` | Proactive check frequency |
| `FRESHNESS_MAX_AGE_HOURS` | `6` | Max age of last successful run before freshness alert |
| `ROW_COUNT_THRESHOLD_PCT` | `0.20` | Row count deviation that triggers a proactive anomaly |
| `WEBHOOK_PORT` | `8765` | Port for the FastAPI webhook server |
| `SLACK_WEBHOOK_URL` | — | Slack Incoming Webhook URL for incident alerts |
| `LANGCHAIN_TRACING_V2` | `false` | Set `true` to enable LangSmith distributed tracing |
| `LANGCHAIN_API_KEY` | — | LangSmith API key |
| `LANGCHAIN_PROJECT` | `pipeline-sentinel` | LangSmith project name |

---

## Airflow integration

Requirements: Airflow 2.7+ with the REST API and basic auth enabled.

```yaml
# airflow.cfg or environment
AIRFLOW__API__AUTH_BACKENDS: "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
```

**Manual mode (UI):** Switch to *Live Airflow* in the sidebar, poll for failed runs, select one, click *Run Sentinel*.

**Autonomous mode:** Set `USE_AIRFLOW=true` and `WATCHER_DAG_IDS`, start `python -m watcher.sentinel_watcher`. Sentinel picks up new failures automatically.

In Airflow mode, `retry_task` calls `POST .../taskInstances/{id}/clear` and `run_dbt_full_refresh` triggers a new DAG run with `conf={"full_refresh": true}`.

---

## Testing

```bash
# All tests — no LLM calls, isolated SQLite DBs per test
pytest tests/ -v

# Specific suites
pytest tests/test_agents.py -v          # tool function tests
pytest tests/test_simulator.py -v       # pipeline simulation
pytest tests/test_reflection.py -v      # orchestration logic (mocked agents)
pytest tests/test_new_features.py -v    # audit trail, schema drift, blast radius, pattern memory, metrics
pytest tests/test_evals.py -v           # LLM-behavior evals: 13 cases, 7 failure modes + 6 gate/pattern

# Token budget baseline (uses count_tokens API — $0 cost, no generation)
python scripts/token_budget.py
```

---

## Data layer

SQLite via `simulator/database.py`. Seven tables:

| Table | Purpose |
|---|---|
| `pipeline_runs` | One row per DAG run — status, row counts, failure type, timestamps |
| `task_states` | One row per task per run — status, duration, error message |
| `schema_registry` | Baseline schema snapshots vs current snapshots; seeded by `init_db()` |
| `incidents` | One row per anomaly — root cause, remediation steps, thought log, blast_radius |
| `agent_audit_log` | One row per agent transition — input/output summaries, decision, confidence |
| `incident_outcomes` | Structured metrics per incident — resolution type, MTTR, blast_radius, confidence score |
| `incident_patterns` | Rolling pattern memory — success_rate and occurrence_count per (root_cause, pipeline, fix) |

`data/` is gitignored. Created automatically on first `get_connection()` call.

---

## Project structure

```
agents/
  monitor_agent.py       # Anomaly detection (6 tools, ReAct loop)
  diagnosis_agent.py     # Root cause + strategy (pattern memory fast-path)
  remediation_agent.py   # Fix execution + verification (5 tools)
  reflection_agent.py    # Outcome self-assessment, retry logic
  explanation_agent.py   # Plain-English incident narrative
  audit.py               # Agent transition audit log (log/get/backfill)
  blast_radius.py        # Dependency graph, BFS scoring, escalation gate
  metrics.py             # write_incident_outcome(), compute_self_healing_metrics()
  patterns.py            # Pattern memory: get/match/upsert
  tools.py               # All tool functions (SQLite + Airflow REST)
  utils.py               # JSON extraction utility

orchestrator/
  sentinel.py            # Coordinates all agents + 11-step pipeline

simulator/
  pipeline.py            # Synthetic 8-task DAG state machine
  failure_injector.py    # Injects 7 failure modes
  database.py            # SQLite schema (7 tables) + connection helper
  airflow_connector.py   # Airflow REST API client + DB sync + failure type detection

watcher/
  sentinel_watcher.py    # Autonomous loop: Airflow polling + proactive checks
  proactive_monitor.py   # Freshness + row count checks → synthetic runs
  webhook_server.py      # FastAPI: POST /webhook/failure for Airflow callbacks
  alerting.py            # Slack incident alerts

app/
  streamlit_app.py       # Dashboard: live thought stream, KPI row, watcher panel

scripts/
  token_budget.py        # Measures static prompt token weight per agent

docs/
  error_taxonomy.md      # 7 failure modes → strategy, blast radius, confidence floor
  state_schema.md        # Orchestrator state dict contract at each phase
  tool_approval_tiers.md # Auto-approved vs escalation-required tools
  agents/                # Per-agent identity, tool budget, output contract

tests/
  test_agents.py         # Tool function tests
  test_reflection.py     # Orchestration logic (mocked agents)
  test_simulator.py      # Pipeline simulation
  test_new_features.py   # Audit trail, schema drift, blast radius, metrics, patterns
  test_evals.py          # LLM-behavior evals (13 cases, zero LLM calls)

DECISIONS.md             # 9 architectural decision records (ADRs)
```

---

## LangSmith tracing

When `LANGCHAIN_TRACING_V2=true`, every agent function emits a span to LangSmith. Individual `messages.create()` calls appear as nested LLM spans via `wrap_anthropic()`.

```bash
LANGCHAIN_TRACING_V2=true LANGCHAIN_API_KEY=ls__... python -c "
from simulator.database import init_db
from simulator.pipeline import PipelineSimulator
from simulator.failure_injector import FailureInjector
from orchestrator.sentinel import SentinelOrchestrator
init_db()
sim = PipelineSimulator(); inj = FailureInjector(sim)
run_id = sim.start_run(); inj.inject(run_id, failure_type='upstream_timeout')
SentinelOrchestrator().run(run_id)
print('Check https://smith.langchain.com for the trace')
"
```
