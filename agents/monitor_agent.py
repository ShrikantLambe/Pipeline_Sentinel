import anthropic
import json
import os
from .tools import get_pipeline_status, check_row_count_anomaly, get_failed_tasks, check_task_duration_anomaly, check_zombie_run
from .utils import extract_json

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

MONITOR_TOOLS = [
    {
        "name": "get_pipeline_status",
        "description": "Fetch the full current state of a pipeline run including task-level statuses and errors.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "The pipeline run ID to check"}
            },
            "required": ["run_id"]
        }
    },
    {
        "name": "check_row_count_anomaly",
        "description": "Check if the actual row count deviates significantly from expected. Returns anomaly details.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "threshold_pct": {"type": "number", "description": "Deviation threshold 0.0-1.0, default 0.15"}
            },
            "required": ["run_id"]
        }
    },
    {
        "name": "get_failed_tasks",
        "description": "Return all failed tasks for a run with their error messages.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"}
            },
            "required": ["run_id"]
        }
    },
    {
        "name": "check_task_duration_anomaly",
        "description": "Check if any tasks ran >3x their expected baseline duration — indicates a hung or slow task even if it eventually succeeded.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"}
            },
            "required": ["run_id"]
        }
    },
    {
        "name": "check_zombie_run",
        "description": "Detect tasks stuck in 'running' state past their expected duration (zombie/hung tasks).",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "stale_minutes": {"type": "integer", "description": "Minutes before a running task is considered stale, default 30"}
            },
            "required": ["run_id"]
        }
    },
]

TOOL_FUNCTIONS = {
    "get_pipeline_status": get_pipeline_status,
    "check_row_count_anomaly": check_row_count_anomaly,
    "get_failed_tasks": get_failed_tasks,
    "check_task_duration_anomaly": check_task_duration_anomaly,
    "check_zombie_run": check_zombie_run,
}

MONITOR_SYSTEM_PROMPT = """
You are the Monitor Agent for Pipeline Sentinel, a self-healing data pipeline system.

Your job is to:
1. Check the pipeline run status using available tools
2. Identify any anomalies: failed tasks, row count deviations, schema issues, duration spikes, zombie tasks
3. Return a structured JSON assessment

Anomaly types to detect:
- failed_task: One or more tasks explicitly failed with an error
- row_count_anomaly: Actual rows deviate >15% from expected (flag even if expected=0 and actual>0)
- duration_anomaly: A task ran >3x its normal baseline duration (SLA breach even if succeeded)
- schema_drift: Column mismatch detected in validate_raw_schema task
- zombie_task: A task stuck in 'running' state >30 minutes
- auth_failure: PermissionDenied / credential error — always mark severity=critical

ALWAYS respond with a final JSON object in this exact format:
{
  "anomaly_detected": true/false,
  "severity": "critical" | "warning" | "none",
  "anomaly_type": "<one of the types above, or null>",
  "affected_task": "<task_id or null>",
  "evidence": "<brief description of what you found>",
  "recommended_action": "<what the next agent should do>"
}

Be concise. Use tools to gather evidence before making your assessment.
"""


def run_monitor_agent(run_id: str,
                      thought_callback=None,
                      stop_event=None) -> dict:
    """
    Run the monitor agent against a pipeline run.
    thought_callback: optional function(step_dict) for streaming thoughts to UI.
    """
    messages = [
        {"role": "user",
         "content": f"Assess pipeline run: {run_id}. Check for any anomalies."}
    ]

    thoughts = []
    _iter = 0

    while True:
        _iter += 1
        if _iter > 10:
            return {"anomaly_detected": False, "error": "max_iterations_exceeded", "thoughts": thoughts}
        if stop_event and stop_event.is_set():
            return {"anomaly_detected": False, "error": "cancelled", "thoughts": thoughts}

        response = client.messages.create(
            model=os.getenv("REFLECTION_MODEL", "claude-sonnet-4-5"),
            max_tokens=1000,
            system=MONITOR_SYSTEM_PROMPT,
            tools=MONITOR_TOOLS,
            messages=messages,
            timeout=60,
        )

        # Collect text thoughts
        for block in response.content:
            if hasattr(block, "text"):
                thought = {"agent": "Monitor", "type": "thought",
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
                result = {"anomaly_detected": False, "error": "parse_failed"}

            result["thoughts"] = thoughts
            return result

        # Handle tool calls
        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_name = block.name
                    tool_input = block.input

                    thought = {"agent": "Monitor", "type": "tool_call",
                               "content": f"Calling {tool_name}({json.dumps(tool_input)})"}
                    thoughts.append(thought)
                    if thought_callback:
                        thought_callback(thought)

                    fn = TOOL_FUNCTIONS.get(tool_name)
                    result = fn(**tool_input) if fn else {"error": "unknown tool"}

                    thought = {"agent": "Monitor", "type": "observation",
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
