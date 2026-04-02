import anthropic
import json
import os
from .tools import (retry_task, apply_dedup, reload_schema,
                    extend_ingestion_window, run_dbt_full_refresh,
                    get_pipeline_status)
from .utils import extract_json
from langsmith import traceable
from langsmith.wrappers import wrap_anthropic

client = wrap_anthropic(anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"), timeout=60.0))

REMEDIATION_TOOLS = [
    {"name": "retry_task",
     "description": "Retry a failed task, optionally with backoff.",
     "input_schema": {"type": "object",
                      "properties": {
                          "run_id": {"type": "string"},
                          "task_id": {"type": "string"},
                          "backoff_seconds": {"type": "integer"}
                      }, "required": ["run_id", "task_id"]}},
    {"name": "apply_dedup",
     "description": "Apply deduplication to fix duplicate key violations.",
     "input_schema": {"type": "object",
                      "properties": {"run_id": {"type": "string"}},
                      "required": ["run_id"]}},
    {"name": "reload_schema",
     "description": "Reload schema definition from source to fix schema drift.",
     "input_schema": {"type": "object",
                      "properties": {"run_id": {"type": "string"}},
                      "required": ["run_id"]}},
    {"name": "extend_ingestion_window",
     "description": "Extend data collection window for late-arriving data.",
     "input_schema": {"type": "object",
                      "properties": {
                          "run_id": {"type": "string"},
                          "extra_hours": {"type": "integer"}
                      }, "required": ["run_id"]}},
    {"name": "run_dbt_full_refresh",
     "description": "Re-run dbt models with --full-refresh flag.",
     "input_schema": {"type": "object",
                      "properties": {"run_id": {"type": "string"}},
                      "required": ["run_id"]}},
    {"name": "get_pipeline_status",
     "description": "Check current pipeline state after applying a fix.",
     "input_schema": {"type": "object",
                      "properties": {"run_id": {"type": "string"}},
                      "required": ["run_id"]}},
]

TOOL_FUNCTIONS = {
    "retry_task": retry_task,
    "apply_dedup": apply_dedup,
    "reload_schema": reload_schema,
    "extend_ingestion_window": extend_ingestion_window,
    "run_dbt_full_refresh": run_dbt_full_refresh,
    "get_pipeline_status": get_pipeline_status,
}

REMEDIATION_SYSTEM_PROMPT = """
You are the Remediation Agent for Pipeline Sentinel.

You have been given a diagnosis and must execute the recommended fix using available tools.
After applying the fix, verify success using get_pipeline_status.

Respond ONLY with JSON:
{
  "action_taken": "<what you did>",
  "tool_used": "<tool name>",
  "tool_result": <the raw tool result>,
  "fix_successful": true/false,
  "verification_evidence": "<what you checked to confirm success/failure>",
  "next_recommended_action": "<if failed, what to try next>"
}
"""


@traceable(name="Remediation Agent", run_type="chain",
           tags=["pipeline-sentinel", "remediation"])
def run_remediation_agent(run_id: str, diagnosis: dict,
                          thought_callback=None,
                          stop_event=None) -> dict:
    messages = [
        {"role": "user",
         "content": f"""
Execute remediation for run {run_id}.

Diagnosis:
{json.dumps(diagnosis, indent=2)}

Apply the recommended strategy and verify the result.
"""}
    ]

    thoughts = []
    _iter = 0

    while True:
        _iter += 1
        if _iter > 10:
            return {"action_taken": "max iterations exceeded", "fix_successful": False, "thoughts": thoughts}
        if stop_event and stop_event.is_set():
            return {"action_taken": "cancelled", "fix_successful": False, "thoughts": thoughts}

        response = client.messages.create(
            model=os.getenv("REFLECTION_MODEL", "claude-sonnet-4-5"),
            max_tokens=1000,
            system=REMEDIATION_SYSTEM_PROMPT,
            tools=REMEDIATION_TOOLS,
            messages=messages,
            timeout=60,
        )

        for block in response.content:
            if hasattr(block, "text"):
                thought = {"agent": "Remediation", "type": "thought",
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
                result = {"fix_successful": False, "error": "parse_failed"}
            result["thoughts"] = thoughts
            return result

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    thought = {"agent": "Remediation", "type": "tool_call",
                               "content": f"Calling {block.name}({json.dumps(block.input)})"}
                    thoughts.append(thought)
                    if thought_callback:
                        thought_callback(thought)

                    fn = TOOL_FUNCTIONS.get(block.name)
                    result = fn(**block.input) if fn else {"error": "unknown"}

                    thought = {"agent": "Remediation", "type": "observation",
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
