# Architecture Decision Records

Significant design choices made during Pipeline Sentinel development.
Status: `accepted` | `open` | `superseded`

---

## ADR-001 — Raw Anthropic API vs LangGraph

**Status:** accepted
**Decision:** Use the Anthropic Python SDK directly with a hand-written ReAct loop per agent, not LangGraph or another agent framework.
**Rationale:** LangGraph adds an abstraction layer that obscures tool-call routing and makes it harder to inspect state at each step. The hand-written loop (10 lines: check `stop_reason`, dispatch tools, append messages, repeat) is fully transparent and fits exactly in one screen. The `thought_callback` streaming pattern is also simpler to implement without a framework.
**Trade-off:** More boilerplate per agent file; no built-in graph visualization.

---

## ADR-002 — SQLite vs managed database

**Status:** accepted
**Decision:** Store all state (runs, incidents, audit log, pattern memory) in a local SQLite file.
**Rationale:** Zero infrastructure to run locally or in CI. Tests create a fresh DB in `tmp_path` per fixture. The entire data layer is in one file (`simulator/database.py`) with no migrations beyond `ALTER TABLE` guards. If the project moves to production, `get_connection()` is the only change point.
**Trade-off:** No concurrent writes (watcher uses a `threading.Lock`); Tableau/BI tools need a CSV export or Postgres migration. Documented in earlier design discussion.

---

## ADR-003 — Five-agent split

**Status:** accepted
**Decision:** Split the pipeline into Monitor → Diagnosis → Remediation → Reflection → Explanation rather than a single large agent or a two-phase detect/fix split.
**Rationale:** Each agent has a single responsibility with a declared input/output contract (see `docs/agents/`). This makes each agent independently testable and replaceable. The escalation gate sits between Diagnosis and Remediation, which is the natural decision boundary.
**Trade-off:** More orchestrator code; each agent call adds latency. Acceptable because each agent has a `timeout=60` hard cap.

---

## ADR-004 — Blast radius scoring uses direct consumers only (not BFS-transitive)

**Status:** accepted
**Decision:** `assess_blast_radius()` scores based on the failing task's own layer level and its **direct** `consumers` set, not the full BFS-reachable consumer set.
**Rationale:** If transitive consumers drove the score, every raw-layer task (`extract_source_data`) would score HIGH because it eventually feeds executive dashboards. This would make the escalation gate fire for every run, defeating the purpose. Direct consumers reflect actual blast radius more accurately.
**Trade-off:** The `affected_consumers` field still lists all transitive consumers for informational display, but they do not affect the score.

---

## ADR-005 — Pattern memory threshold: ≥ 3 occurrences AND ≥ 90% success rate

**Status:** accepted
**Decision:** The Diagnosis Agent fast-path activates only when `occurrence_count >= 3` AND `success_rate >= 0.90`.
**Rationale:** Three occurrences provides enough signal that the pattern is real, not a coincidence. 90% success rate ensures the cached strategy works reliably before skipping LLM reasoning. Rolling average formula: `new_rate = (old_rate * old_count + result) / (old_count + 1)`.
**Trade-off:** First two occurrences of a failure mode always run the full LLM loop. Accepted cost for reliability.

---

## ADR-006 — Escalation gate: high confidence AND LOW blast only

**Status:** accepted
**Decision:** Auto-remediation proceeds **only** when `confidence == "high"` (≥ 0.85 numeric) AND `blast_radius == "LOW"`. Any other combination escalates immediately.
**Rationale:** MEDIUM and HIGH blast radius failures affect dbt mart tables, executive dashboards, or ML models. An incorrect automated fix to those layers can corrupt downstream consumers before anyone notices. The conservative gate means false negatives (escalating something that could have been auto-fixed) are preferred over false positives (auto-fixing something that cascades damage).
**Trade-off:** `dbt_model_failure` always escalates even when confidence is high, because `run_dbt_mart_models` is a mart-level task. Accepted.

---

## ADR-007 — Schema drift detection via polling (not Airflow event)

**Status:** accepted
**Decision:** Schema drift is detected by the Monitor Agent calling `check_schema_drift()` as one of its 6 tools, comparing `schema_registry.actual_columns` against `expected_columns`. The snapshot is written by `failure_injector.py` (simulator) or can be written by a separate schema crawler (production).
**Rationale:** Airflow has no native schema-change event. Polling the registry at monitor time integrates naturally into the existing ReAct loop without new infrastructure.
**Open question:** In production, what writes `actual_columns` to `schema_registry`? Options: (a) a dbt source freshness job, (b) a nightly schema crawler, (c) an Airflow sensor. Not yet decided.

---

## ADR-008 — Blast radius graph is hardcoded in blast_radius.py

**Status:** open
**Decision (current):** `DEPENDENCY_GRAPH` in `agents/blast_radius.py` is a static Python dict mapping each task to its downstream consumers.
**Rationale:** Zero infrastructure to maintain. Sufficient for a fixed DAG topology.
**Open question:** In production, this should be driven by a real dbt manifest (`manifest.json`) or a lineage catalog (OpenLineage, Atlan, etc.). The replacement is a single function swap in `assess_blast_radius()`. No decision made yet on the source of truth.

---

## ADR-009 — Autonomous watcher uses polling + webhook, not Airflow sensor

**Status:** accepted
**Decision:** The `SentinelWatcher` polls Airflow's REST API every N minutes AND receives `on_failure_callback` webhooks. There is no dedicated Airflow sensor DAG.
**Rationale:** An Airflow sensor DAG would create a circular dependency (Sentinel DAG watches other DAGs, including potentially itself). The polling + webhook combination is decoupled from Airflow's scheduler entirely.
**Trade-off:** Polling has up to N-minute latency for failures that don't have a webhook configured. Webhook is zero-latency but requires per-DAG callback configuration.
