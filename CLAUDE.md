# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then add ANTHROPIC_API_KEY

# Initialize database
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

Tests override `PIPELINE_DB_PATH` with a temp path — each test run is fully isolated.

## Architecture

### Agent pipeline

`SentinelOrchestrator.run()` in [orchestrator/sentinel.py](orchestrator/sentinel.py) coordinates five agents:

1. **Monitor** → calls 5 detection tools, returns `anomaly_detected` + `anomaly_type`
2. **Diagnosis** → maps anomaly to `root_cause` + `remediation_strategy`
3. **Remediation** → executes fix tool, verifies via `get_pipeline_status`
4. **Reflection** → returns `assessment`: `resolved | retry | escalate`; loops back to Remediation up to `MAX_RETRY_ATTEMPTS`, tracking `tried_strategies` to prevent repeating failed approaches
5. **Explanation** → single LLM call (no tools), returns plain-English incident summary
6. `write_incident_to_db()` stores the full incident + thought log for replay

The orchestrator passes a `stop_event: threading.Event` through to each agent so the UI cancel button terminates the run within one LLM call cycle (≤60s).

### Agent implementation pattern

Every agent in `agents/` follows the same structure:
- Signature: `run_<name>_agent(run_id, ..., thought_callback=None, stop_event=None) -> dict`
- `while True` loop with guard: `max 10 iterations`, `stop_event.is_set()` check, `timeout=60` on every `client.messages.create()` call
- Tool-use: `stop_reason == "tool_use"` → execute tools → append `tool_result` messages → loop
- `stop_reason == "end_turn"` → extract JSON with `agents/utils.py:extract_json()` → return
- `thought_callback({"agent", "type", "content"})` is called for every thought, tool call, and observation — streams to the UI in real time

### Tools

All tool functions live in [agents/tools.py](agents/tools.py). They read/write SQLite directly. Tool schemas (for the Anthropic API) and the Python dispatch dict (`TOOL_FUNCTIONS`) are both defined in each agent file.

In simulator mode, remediation tools have probabilistic success rates (70–85%) to exercise the retry logic. In Airflow mode (`USE_AIRFLOW=true`), `retry_task` and `run_dbt_full_refresh` call the Airflow REST API; other tools fall back to simulator logic.

### Data layer

SQLite via [simulator/database.py](simulator/database.py). Three tables:
- `pipeline_runs` — one row per DAG run (status, row counts, failure type, timestamps)
- `task_states` — one row per task per run (status, duration, error message)
- `incidents` — audit log per anomaly (root cause, remediation steps, thought log JSON)

`data/` is gitignored. The directory is created by `get_connection()` on first access.

### Failure modes

[simulator/failure_injector.py](simulator/failure_injector.py) defines 7 failure modes in `FAILURE_MODES`. `inject()` runs all tasks up to the failure point as success, fails the affected task, and marks all downstream tasks as `upstream_failed`.

### Airflow integration

[simulator/airflow_connector.py](simulator/airflow_connector.py) wraps the Airflow REST API v1. `sync_run_to_db()` pulls a DAG run + task instances into our SQLite schema. `poll_failed_runs()` finds failed runs not yet in the local DB. Failure type detection uses log keyword heuristics.

### Streamlit UI

[app/streamlit_app.py](app/streamlit_app.py) runs the agent pipeline in a background `threading.Thread` and re-renders every 0.5s via `st.rerun()` while the thread is active. Key session state: `thought_stream` (live feed), `agent_phase` (current agent), `phases_done` (completed agents), `verdict` (loaded from DB after run completes), `running` (thread alive flag).
