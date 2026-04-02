import anthropic
import json
import os
from .tools import get_pipeline_status, get_failed_tasks, check_row_count_anomaly
from .utils import extract_json
from .patterns import get_known_patterns, match_pattern, PATTERN_MIN_OCCURRENCES, PATTERN_MIN_SUCCESS_RATE
from langsmith import traceable
from langsmith.wrappers import wrap_anthropic

client = wrap_anthropic(anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"), timeout=60.0))

DIAGNOSIS_SYSTEM_PROMPT = """
You are the Diagnosis Agent for Pipeline Sentinel.

Given a monitor alert, your job is to:
1. Use tools to gather deeper evidence
2. Identify the precise root cause
3. Select the best remediation strategy from the available options

Available remediation strategies:
- "retry_with_backoff": For transient errors (timeouts, locks)
- "apply_dedup": For duplicate key violations
- "reload_schema": For schema drift / column mismatch
- "extend_ingestion_window": For late-arriving data (row count too low)
- "run_dbt_full_refresh": For dbt model/test failures

Respond ONLY with JSON:
{
  "root_cause": "<specific technical root cause>",
  "confidence": "high" | "medium" | "low",
  "remediation_strategy": "<strategy_name>",
  "strategy_rationale": "<why this strategy>",
  "estimated_fix_time": "<e.g. 2-5 minutes>",
  "escalation_risk": "low" | "medium" | "high"
}
"""

# Root cause → remediation strategy mapping (used for pattern-matched fast path)
_STRATEGY_MAP = {
    "schema_drift":      "reload_schema",
    "late_arrival":      "extend_ingestion_window",
    "duplicate_keys":    "apply_dedup",
    "upstream_timeout":  "retry_with_backoff",
    "dbt_model_failure": "run_dbt_full_refresh",
    "partial_load":      "retry_with_backoff",
    "auth_failure":      "escalate_to_manual_intervention",
}

DIAGNOSIS_TOOLS = [
    {
        "name": "get_pipeline_status",
        "description": "Get full pipeline run state including task errors.",
        "input_schema": {"type": "object",
                         "properties": {"run_id": {"type": "string"}},
                         "required": ["run_id"]}
    },
    {
        "name": "get_failed_tasks",
        "description": "Get failed tasks with error details.",
        "input_schema": {"type": "object",
                         "properties": {"run_id": {"type": "string"}},
                         "required": ["run_id"]}
    },
    {
        "name": "check_row_count_anomaly",
        "description": "Check row count deviation details.",
        "input_schema": {"type": "object",
                         "properties": {
                             "run_id": {"type": "string"},
                             "threshold_pct": {"type": "number"}
                         },
                         "required": ["run_id"]}
    },
]

TOOL_FUNCTIONS = {
    "get_pipeline_status": get_pipeline_status,
    "get_failed_tasks": get_failed_tasks,
    "check_row_count_anomaly": check_row_count_anomaly,
}


@traceable(name="Diagnosis Agent", run_type="chain",
           tags=["pipeline-sentinel", "diagnosis"])
def run_diagnosis_agent(run_id: str, monitor_alert: dict,
                        thought_callback=None,
                        stop_event=None,
                        pipeline_name: str = "retail_sales_pipeline") -> dict:
    thoughts = []

    def _thought(content: str, kind: str = "thought"):
        t = {"agent": "Diagnosis", "type": kind, "content": content}
        thoughts.append(t)
        if thought_callback:
            thought_callback(t)

    # ── Prompt 5: consult pattern memory before running LLM reasoning ──────
    known_patterns = get_known_patterns(pipeline_name)
    anomaly_type   = monitor_alert.get("anomaly_type", "")
    matched        = match_pattern(anomaly_type, pipeline_name, known_patterns)
    patterns_consulted = [
        {"root_cause": p["root_cause_category"], "fix": p["fix_action_taken"],
         "rate": p["success_rate"], "count": p["occurrence_count"]}
        for p in known_patterns
    ]

    if matched:
        _thought(
            f"Pattern memory hit: '{anomaly_type}' has been seen {matched['occurrence_count']}x "
            f"with {matched['success_rate']*100:.0f}% success using '{matched['fix_action_taken']}'. "
            f"Skipping full LLM reasoning — using pattern-matched strategy directly."
        )
        strategy = matched["fix_action_taken"] or _STRATEGY_MAP.get(anomaly_type, "retry_with_backoff")
        return {
            "root_cause": f"Pattern-matched: {matched['root_cause_category']}",
            "confidence": "high",
            "remediation_strategy": strategy,
            "strategy_rationale": (
                f"Pattern memory: this fix has worked {matched['success_rate']*100:.0f}% of the time "
                f"over {matched['occurrence_count']} previous incidents."
            ),
            "estimated_fix_time": "~2 minutes (pattern-matched)",
            "escalation_risk": "low",
            "patterns_consulted": patterns_consulted,
            "pattern_matched": True,
            "thoughts": thoughts,
        }

    if known_patterns:
        _thought(
            f"Pattern memory checked ({len(known_patterns)} pattern(s) for '{pipeline_name}') — "
            f"no high-confidence match (need ≥{PATTERN_MIN_OCCURRENCES} occurrences "
            f"and ≥{PATTERN_MIN_SUCCESS_RATE*100:.0f}% success rate). Running full LLM diagnosis."
        )

    # ── Full LLM ReAct loop ────────────────────────────────────────────────
    messages = [
        {"role": "user",
         "content": f"""
Monitor alert received for run {run_id}:
{json.dumps(monitor_alert, indent=2)}

Diagnose the root cause and recommend a remediation strategy.
"""}
    ]

    _iter = 0

    while True:
        _iter += 1
        if _iter > 10:
            return {"error": "max_iterations_exceeded", "remediation_strategy": "escalate_to_manual_intervention", "thoughts": thoughts}
        if stop_event and stop_event.is_set():
            return {"error": "cancelled", "remediation_strategy": "escalate_to_manual_intervention", "thoughts": thoughts}

        response = client.messages.create(
            model=os.getenv("REFLECTION_MODEL", "claude-sonnet-4-5"),
            max_tokens=1000,
            system=DIAGNOSIS_SYSTEM_PROMPT,
            tools=DIAGNOSIS_TOOLS,
            messages=messages,
            timeout=60,
        )

        for block in response.content:
            if hasattr(block, "text"):
                thought = {"agent": "Diagnosis", "type": "thought",
                           "content": block.text}
                thoughts.append(thought)
                if thought_callback:
                    thought_callback(thought)

        if response.stop_reason == "end_turn":
            final_text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    final_text = block.text
            result = extract_json(final_text)
            if not result:
                result = {"error": "parse_failed",
                          "remediation_strategy": "retry_with_backoff"}
            result["patterns_consulted"] = patterns_consulted
            result["pattern_matched"]    = False
            result["thoughts"]           = thoughts
            return result

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    thought = {"agent": "Diagnosis", "type": "tool_call",
                               "content": f"Calling {block.name}({json.dumps(block.input)})"}
                    thoughts.append(thought)
                    if thought_callback:
                        thought_callback(thought)

                    fn = TOOL_FUNCTIONS.get(block.name)
                    result = fn(**block.input) if fn else {"error": "unknown"}

                    thought = {"agent": "Diagnosis", "type": "observation",
                               "content": f"Result: {json.dumps(result, default=str)[:300]}"}
                    thoughts.append(thought)
                    if thought_callback:
                        thought_callback(thought)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, default=str),
                    })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
