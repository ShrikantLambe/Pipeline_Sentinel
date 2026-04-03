# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then add ANTHROPIC_API_KEY

# Initialize database (creates all tables + seeds schema_registry baselines)
python -c "from simulator.database import init_db; init_db()"

# Run all tests (no LLM calls — agents and orchestration are mocked)
pytest tests/ -v

# Single test file / single test
pytest tests/test_simulator.py -v
pytest tests/test_agents.py::test_apply_dedup -v

# Launch UI
streamlit run app/streamlit_app.py

# CLI smoke test — runs all 7 failure modes, prints resolution status
python -c "
from simulator.database import init_db
from simulator.pipeline import PipelineSimulator
from simulator.failure_injector import FailureInjector, FAILURE_MODES
from orchestrator.sentinel import SentinelOrchestrator
init_db()
for ft in FAILURE_MODES:
    sim = PipelineSimulator(); inj = FailureInjector(sim)
    run_id = sim.start_run(); inj.inject(run_id, failure_type=ft)
    orch = SentinelOrchestrator(thought_callback=lambda t: print(f'  [{t[\"agent\"]}] {t[\"content\"][:80]}'))
    r = orch.run(run_id); print(f'{ft}: {r.get(\"resolution_status\", r.get(\"status\"))}')
"

# Query audit trail for a completed incident
python -c "
from agents.audit import get_incident_audit
for step in get_incident_audit(1):
    print(f'[{step[\"agent_name\"]}] {step[\"decision\"]}')
"

# Check self-healing metrics
python -c "
from agents.metrics import compute_self_healing_metrics
import json; print(json.dumps(compute_self_healing_metrics(), indent=2))
"

# Start the autonomous watcher (polls Airflow + runs proactive checks)
python -m watcher.sentinel_watcher

# Start the webhook server (receives Airflow on_failure_callback POSTs)
uvicorn watcher.webhook_server:app --host 0.0.0.0 --port 8765

# Test the webhook manually (simulates an Airflow failure event)
curl -X POST http://localhost:8765/webhook/failure \
     -H "Content-Type: application/json" \
     -d '{"dag_id": "retail_pipeline", "run_id": "your_run_id"}'
```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | **required** | Claude API key |
| `PIPELINE_DB_PATH` | `data/sentinel.db` | SQLite file path |
| `REFLECTION_MODEL` | `claude-sonnet-4-5` | Model used by all agents |
| `MAX_RETRY_ATTEMPTS` | `3` | Remediation retry cap |
| `USE_AIRFLOW` | `false` | Set `true` to use real Airflow |
| `AIRFLOW_URL` | `http://localhost:8080` | Airflow base URL |
| `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` | `airflow` | Airflow credentials |
| `LANGCHAIN_TRACING_V2` | `false` | Set `true` to enable LangSmith tracing |
| `LANGCHAIN_API_KEY` | — | LangSmith API key (from smith.langchain.com) |
| `LANGCHAIN_PROJECT` | `pipeline-sentinel` | LangSmith project name |
| `WATCHER_DAG_IDS` | — | Comma-separated DAG IDs for the autonomous watcher |
| `WATCHER_POLL_INTERVAL_SECONDS` | `300` | How often to poll Airflow for new failures |
| `WATCHER_PROACTIVE_INTERVAL_SECONDS` | `900` | How often to run freshness + row count checks |
| `FRESHNESS_MAX_AGE_HOURS` | `6` | Max age of last successful run before freshness alert |
| `ROW_COUNT_THRESHOLD_PCT` | `0.20` | Row count deviation % that triggers a proactive anomaly |
| `WEBHOOK_PORT` | `8765` | Port the FastAPI webhook server listens on |
| `SLACK_WEBHOOK_URL` | — | Slack Incoming Webhook URL for incident alerts |

Tests override `PIPELINE_DB_PATH` with a temp path — each test run is fully isolated.

## Architecture

### Agent pipeline

`SentinelOrchestrator.run()` in [orchestrator/sentinel.py](orchestrator/sentinel.py) coordinates five agents:

1. **Monitor** → calls 6 detection tools (including `check_schema_drift`), returns `anomaly_detected` + `anomaly_type`
2. **Blast radius** → `assess_blast_radius(affected_task)` scores impact: LOW / MEDIUM / HIGH
3. **Diagnosis** → checks pattern memory first (`get_known_patterns`); if no high-confidence match, runs full LLM ReAct loop to return `root_cause` + `remediation_strategy`
4. **Escalation gate** → `should_auto_remediate(confidence, blast_radius)`: auto-remediate only if `confidence == "high"` AND `blast_radius == "LOW"`; otherwise escalate immediately
5. **Remediation** → executes fix tool, verifies via `get_pipeline_status`
6. **Reflection** → returns `assessment`: `resolved | retry | escalate`; loops back to Remediation up to `MAX_RETRY_ATTEMPTS`, tracking `tried_strategies` to prevent repeating failed approaches
7. **Explanation** → single LLM call (no tools), returns plain-English incident summary
8. **Write** → `write_incident_to_db()` stores the full incident + thought log + blast_radius + patterns_consulted
9. **Audit** → `update_audit_incident_id()` back-fills incident_id on all audit log rows for the run
10. **Outcome** → `write_incident_outcome()` writes one row to `incident_outcomes` for metrics
11. **Pattern upsert** → `upsert_pattern()` updates rolling success_rate + occurrence_count

The orchestrator passes a `stop_event: threading.Event` through to each agent so the UI cancel button terminates the run within one LLM call cycle (≤60s).

### Agent implementation pattern

Every agent in `agents/` follows the same structure:
- Signature: `run_<name>_agent(run_id, ..., thought_callback=None, stop_event=None) -> dict`
- Decorated with `@traceable(name=..., run_type="chain")` for LangSmith tracing
- Anthropic client wrapped with `wrap_anthropic()` so individual LLM calls appear as nested spans
- `while True` loop with guard: `max 10 iterations`, `stop_event.is_set()` check, `timeout=60` on every `client.messages.create()` call
- Tool-use: `stop_reason == "tool_use"` → execute tools → append `tool_result` messages → loop
- `stop_reason == "end_turn"` → extract JSON with `agents/utils.py:extract_json()` → return
- `thought_callback({"agent", "type", "content"})` is called for every thought, tool call, and observation — streams to the UI in real time

### New feature modules (Prompts 1–6)

| Module | Purpose |
|---|---|
| [agents/audit.py](agents/audit.py) | `log_agent_transition()`, `get_incident_audit(incident_id)`, `update_audit_incident_id()` |
| [agents/blast_radius.py](agents/blast_radius.py) | `DEPENDENCY_GRAPH`, `assess_blast_radius(task)`, `should_auto_remediate(confidence, blast)` |
| [agents/metrics.py](agents/metrics.py) | `write_incident_outcome()`, `compute_self_healing_metrics(days=30)` |
| [agents/patterns.py](agents/patterns.py) | `get_known_patterns(pipeline)`, `match_pattern()`, `upsert_pattern()` |

### Autonomous watcher

The `watcher/` package makes Sentinel fully autonomous — no button clicks required.

| Module | Purpose |
|---|---|
| [watcher/sentinel_watcher.py](watcher/sentinel_watcher.py) | `SentinelWatcher` — main loop: polls Airflow for new failures + runs proactive checks on a schedule; fires `SentinelOrchestrator.run()` for each new issue |
| [watcher/proactive_monitor.py](watcher/proactive_monitor.py) | `check_pipeline_freshness()` + `check_row_count_baselines()` — create synthetic `pipeline_runs` rows when anomalies are detected without an Airflow failure |
| [watcher/webhook_server.py](watcher/webhook_server.py) | FastAPI server with `POST /webhook/failure` — called by Airflow `on_failure_callback`; triggers Sentinel immediately (zero polling delay) |
| [watcher/alerting.py](watcher/alerting.py) | `send_slack_alert()` — posts formatted Slack attachment after every incident |

**Trigger flow:**
```
Airflow failure  →  on_failure_callback  →  POST /webhook/failure  →  SentinelOrchestrator.run()  →  Slack alert
         or
Watcher poll (every 5 min)  →  poll_failed_runs()  →  sync_run_to_db()  →  SentinelOrchestrator.run()  →  Slack alert
         or
Proactive check (every 15 min)  →  freshness / row-count anomaly  →  synthetic run_id  →  SentinelOrchestrator.run()  →  Slack alert
```

`SentinelOrchestrator`, all agents, and the data layer are unchanged — the watcher is purely an additional trigger layer. `_is_already_processed()` prevents duplicate processing by checking whether an `incidents` row already exists for the `run_id`.

**Configure Airflow DAG to call the webhook:**
```python
def sentinel_callback(context):
    import requests
    requests.post("http://sentinel-host:8765/webhook/failure",
                  json={"dag_id": context["dag"].dag_id, "run_id": context["run_id"]},
                  timeout=5)

dag = DAG("your_dag", on_failure_callback=sentinel_callback, ...)
```

### Tools

All tool functions live in [agents/tools.py](agents/tools.py). They read/write SQLite directly. The Monitor agent now includes a 6th tool `check_schema_drift` that compares the latest schema snapshot in `schema_registry` against the expected baseline (seeded by `init_db()`). When `schema_drift` failure type is injected, `failure_injector.py` writes a drift snapshot showing 3 missing columns.

In simulator mode, remediation tools have probabilistic success rates (70–85%) to exercise the retry logic. In Airflow mode (`USE_AIRFLOW=true`), `retry_task` and `run_dbt_full_refresh` call the Airflow REST API; other tools fall back to simulator logic.

### Data layer

SQLite via [simulator/database.py](simulator/database.py). Seven tables:
- `pipeline_runs` — one row per DAG run (status, row counts, failure type, timestamps)
- `task_states` — one row per task per run (status, duration, error message)
- `schema_registry` — baseline schema snapshots (expected_columns) and current snapshots (actual_columns) per table; seeded by `init_db()`
- `incidents` — one row per anomaly (root cause, remediation steps, thought log, blast_radius, patterns_consulted)
- `agent_audit_log` — one row per agent transition per incident (input/output summaries, decision, confidence)
- `incident_outcomes` — structured metrics per incident (resolution_type, MTTR, blast_radius, confidence_score)
- `incident_patterns` — rolling pattern memory per (root_cause, pipeline, fix_action): success_rate, occurrence_count

`data/` is gitignored. The directory is created by `get_connection()` on first access.

### Blast radius + escalation gate

`DEPENDENCY_GRAPH` in [agents/blast_radius.py](agents/blast_radius.py) maps each pipeline task to its output assets, affected level (raw/staging/mart/consumer), and downstream consumers. `assess_blast_radius()` does a BFS to find all reachable consumers and scores the impact.

Gate rule: auto-remediate **only** when `confidence == "high" AND blast_radius == "LOW"`. Failures in mart-level tasks (`run_dbt_mart_models`, `update_snowflake_aggregates`) always escalate because they reach executive dashboards and ML models.

### Pattern memory

After each incident, `upsert_pattern()` updates the rolling `success_rate` and `occurrence_count` for the `(root_cause_category, pipeline_name, fix_action_taken)` triple. On the next identical failure, the Diagnosis agent calls `get_known_patterns()` **before** its LLM loop. If a pattern has `occurrence_count >= 3` and `success_rate >= 0.90`, the agent skips LLM reasoning entirely and returns the cached strategy — flagged as `pattern_matched: True` in state and shown as ⚡ in the UI.

### LangSmith tracing

When `LANGCHAIN_TRACING_V2=true` and `LANGCHAIN_API_KEY` are set, every `@traceable`-decorated function emits a span to LangSmith. Individual Anthropic `messages.create()` calls appear as nested LLM spans because each agent wraps its client with `wrap_anthropic()`. Trace the orchestrator run with the `run_id` as the LangSmith run name for easy lookup.

To verify tracing:
```bash
LANGCHAIN_TRACING_V2=true LANGCHAIN_API_KEY=ls__... python -c "
from simulator.database import init_db
from simulator.pipeline import PipelineSimulator
from simulator.failure_injector import FailureInjector
init_db()
sim = PipelineSimulator(); inj = FailureInjector(sim)
run_id = sim.start_run(); inj.inject(run_id, failure_type='upstream_timeout')
from orchestrator.sentinel import SentinelOrchestrator
SentinelOrchestrator().run(run_id)
print('Check https://smith.langchain.com for the trace')
"
```

### Failure modes

[simulator/failure_injector.py](simulator/failure_injector.py) defines 7 failure modes in `FAILURE_MODES`. `inject()` runs all tasks up to the failure point as success, fails the affected task, and marks all downstream tasks as `upstream_failed`. For `schema_drift`, it also writes an `actual_columns` snapshot to `schema_registry` (missing 3 columns) so `check_schema_drift` returns a real drift result.

### Streamlit UI

[app/streamlit_app.py](app/streamlit_app.py) runs the agent pipeline in a background `threading.Thread` and re-renders every 0.5s via `st.rerun()` while the thread is active.

Key session state: `thought_stream` (live feed), `agent_phase` (current agent), `phases_done` (completed agents), `verdict` (loaded from DB after run completes, includes `blast_radius` and `pattern_matched`), `running` (thread alive flag).

The dashboard top shows a 5-column KPI row (via `compute_self_healing_metrics()`) with total incidents, auto-resolved count + %, escalated count, and MTTR for both resolution types, plus an expandable daily trend chart.

The verdict section shows blast radius badge (color-coded LOW/MEDIUM/HIGH) and ⚡ Pattern-matched badge when pattern memory was used.
