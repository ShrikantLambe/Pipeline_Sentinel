"""
Prompt 4 — Blast Radius Assessment
Traverses a simulated dependency graph to score how broadly a pipeline failure
would impact downstream assets, dashboards, and ML models.

Blast radius levels
-------------------
LOW    — affects only raw / staging tables; no impact beyond the pipeline itself.
MEDIUM — affects dbt mart models or operational reporting tables.
HIGH   — affects executive dashboards, SLA-bound reports, or ML feature pipelines.

Escalation gate (applied in orchestrator/sentinel.py)
------------------------------------------------------
Auto-remediate ONLY when:  confidence == "high"  AND  blast_radius == "LOW"
Escalate in all other combinations.
"""

# ── Simulated dependency graph ─────────────────────────────────────────────
# Maps each pipeline task to its direct output assets and the next-hop
# consumers of those assets.  Wire in real dbt manifest / Snowflake lineage later.

DEPENDENCY_GRAPH: dict[str, dict] = {
    "extract_source_data": {
        "assets": ["raw.orders", "raw.customers", "raw.products"],
        "level":  "raw",
        "downstream_tasks": ["validate_raw_schema", "load_to_staging"],
    },
    "validate_raw_schema": {
        "assets": ["raw.orders"],
        "level":  "raw",
        "downstream_tasks": ["load_to_staging"],
    },
    "load_to_staging": {
        "assets": ["stg.orders", "stg.customers"],
        "level":  "staging",
        "downstream_tasks": ["run_dbt_staging_models"],
    },
    "run_dbt_staging_models": {
        "assets": ["stg_orders", "stg_customers", "stg_products"],
        "level":  "staging",
        "downstream_tasks": ["run_dbt_mart_models", "run_dbt_tests"],
    },
    "run_dbt_mart_models": {
        "assets": ["fct_orders", "fct_sales", "dim_customers"],
        "level":  "mart",
        "downstream_tasks": ["run_dbt_tests", "update_snowflake_aggregates"],
    },
    "run_dbt_tests": {
        "assets": ["fct_orders", "fct_sales"],
        "level":  "mart",
        "downstream_tasks": ["update_snowflake_aggregates"],
    },
    "update_snowflake_aggregates": {
        "assets": ["agg_daily_sales", "agg_customer_ltv"],
        "level":  "mart",
        "downstream_tasks": ["refresh_bi_cache"],
        "consumers": ["executive_dashboard", "ml_revenue_forecast", "sla_weekly_sales"],
    },
    "refresh_bi_cache": {
        "assets": ["bi_cache"],
        "level":  "consumer",
        "consumers": ["executive_dashboard", "stakeholder_reports", "ml_churn_model"],
    },
}

# Consumers that make a failure HIGH impact
_HIGH_IMPACT_CONSUMERS = {
    "executive_dashboard",
    "ml_churn_model",
    "ml_revenue_forecast",
    "sla_weekly_sales",
}


def _all_downstream_consumers(task_id: str) -> set[str]:
    """BFS over DEPENDENCY_GRAPH to collect every consumer reachable from task_id."""
    visited_tasks: set[str] = set()
    consumers: set[str]     = set()
    queue = [task_id]

    while queue:
        current = queue.pop()
        if current in visited_tasks:
            continue
        visited_tasks.add(current)
        node = DEPENDENCY_GRAPH.get(current, {})
        consumers.update(node.get("consumers", []))
        for next_task in node.get("downstream_tasks", []):
            if next_task not in visited_tasks:
                queue.append(next_task)

    return consumers


def assess_blast_radius(affected_task: str) -> dict:
    """
    Score the blast radius of a failure in `affected_task`.

    Returns
    -------
    {
        "blast_radius_score": "LOW" | "MEDIUM" | "HIGH",
        "affected_level":    "raw" | "staging" | "mart" | "consumer",
        "affected_assets":   [...],         # direct output assets of the task
        "affected_consumers": [...],        # all transitively reachable consumers
        "high_impact_consumers": [...],     # subset in _HIGH_IMPACT_CONSUMERS
        "impact_description": str,          # plain-English sentence
    }
    """
    node  = DEPENDENCY_GRAPH.get(affected_task, {})
    level = node.get("level", "raw")

    direct_assets    = node.get("assets", [])
    direct_consumers = set(node.get("consumers", []))
    all_consumers    = _all_downstream_consumers(affected_task) | direct_consumers

    # Scoring is based on the task's own level and its DIRECT consumers only.
    # Transitive consumers are listed in affected_consumers for informational
    # purposes but do not drive the score — otherwise every raw task would be
    # HIGH because it eventually feeds everything downstream.
    direct_high = direct_consumers & _HIGH_IMPACT_CONSUMERS
    high_impact  = all_consumers & _HIGH_IMPACT_CONSUMERS

    if level == "consumer" or direct_high:
        score = "HIGH"
    elif level == "mart":
        score = "MEDIUM"
    else:
        score = "LOW"

    # Plain-English description
    desc_map = {
        "LOW":    f"Failure in `{affected_task}` affects only {level}-layer assets. "
                  "No downstream dashboards or ML models are at risk.",
        "MEDIUM": f"Failure in `{affected_task}` affects dbt mart models. "
                  "Reporting tables may be stale until resolved.",
        "HIGH":   f"Failure in `{affected_task}` cascades to high-impact consumers: "
                  f"{', '.join(sorted(high_impact))}. Escalation required.",
    }

    return {
        "blast_radius_score":    score,
        "affected_level":        level,
        "affected_assets":       sorted(direct_assets),
        "affected_consumers":    sorted(all_consumers),
        "high_impact_consumers": sorted(high_impact),
        "impact_description":    desc_map[score],
    }


# ── Escalation gate helper ─────────────────────────────────────────────────

_CONFIDENCE_NUMERIC = {"high": 0.9, "medium": 0.7, "low": 0.4}


def should_auto_remediate(confidence: str, blast_radius_score: str) -> tuple[bool, str]:
    """
    Return (can_auto_remediate: bool, reason: str).

    Gate rule: auto-remediate ONLY when confidence >= 0.85 AND blast_radius == LOW.
    """
    conf_num = _CONFIDENCE_NUMERIC.get(confidence, 0.0)
    reasons  = []

    if conf_num < 0.85:
        reasons.append(f"confidence={confidence!r} (need 'high' ≥ 0.85)")
    if blast_radius_score != "LOW":
        reasons.append(f"blast_radius={blast_radius_score!r} (need 'LOW')")

    if reasons:
        return False, "Escalation gate triggered — " + ", ".join(reasons)
    return True, "Auto-remediation approved (high confidence + LOW blast radius)"
