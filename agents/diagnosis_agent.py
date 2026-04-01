import anthropic
import json
import os
from .tools import get_pipeline_status, get_failed_tasks, check_row_count_anomaly
from .utils import extract_json

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

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


def run_diagnosis_agent(run_id: str, monitor_alert: dict,
                        thought_callback=None,
                        stop_event=None) -> dict:
    messages = [
        {"role": "user",
         "content": f"""
Monitor alert received for run {run_id}:
{json.dumps(monitor_alert, indent=2)}

Diagnose the root cause and recommend a remediation strategy.
"""}
    ]

    thoughts = []
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
            result["thoughts"] = thoughts
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
