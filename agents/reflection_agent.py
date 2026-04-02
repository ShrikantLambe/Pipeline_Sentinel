import anthropic
import json
import os
from .tools import get_pipeline_status, check_row_count_anomaly
from .utils import extract_json
from langsmith import traceable
from langsmith.wrappers import wrap_anthropic

client = wrap_anthropic(anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"), timeout=60.0))

REFLECTION_SYSTEM_PROMPT = """
You are the Reflection Agent for Pipeline Sentinel — the self-assessment layer
of the self-healing system.

Your role is to answer three questions after each remediation attempt:
1. Did the fix actually work? (check objective evidence)
2. If not, why did it fail? (update the diagnosis)
3. What should we try next? (or should we escalate?)

Rules:
- If fix_successful = true AND pipeline status = success/remediated → RESOLVED
- If retry_count >= max_retries → ESCALATE (do not retry forever)
- If the same strategy failed twice → try a different approach
- Always check objective data (row counts, task status) not just the tool result

Respond ONLY with JSON:
{
  "assessment": "resolved" | "retry" | "escalate",
  "confidence": "high" | "medium" | "low",
  "what_worked": "<what the fix achieved, even if partial>",
  "what_failed": "<why it didn't fully resolve, if applicable>",
  "updated_hypothesis": "<revised root cause understanding>",
  "next_strategy": "<if retry: which strategy to try next>",
  "escalation_reason": "<if escalate: why human intervention is needed>"
}
"""

REFLECTION_TOOLS = [
    {"name": "get_pipeline_status",
     "description": "Check current pipeline state to verify if fix was effective.",
     "input_schema": {"type": "object",
                      "properties": {"run_id": {"type": "string"}},
                      "required": ["run_id"]}},
    {"name": "check_row_count_anomaly",
     "description": "Check if row count anomaly was resolved.",
     "input_schema": {"type": "object",
                      "properties": {"run_id": {"type": "string"},
                                     "threshold_pct": {"type": "number"}},
                      "required": ["run_id"]}},
]

TOOL_FUNCTIONS = {
    "get_pipeline_status": get_pipeline_status,
    "check_row_count_anomaly": check_row_count_anomaly,
}


@traceable(name="Reflection Agent", run_type="chain",
           tags=["pipeline-sentinel", "reflection"])
def run_reflection_agent(run_id: str,
                         remediation_result: dict,
                         retry_count: int,
                         max_retries: int,
                         remediation_history: list = None,
                         thought_callback=None,
                         stop_event=None) -> dict:
    tried_strategies = [
        step.get("strategy") for step in (remediation_history or [])
        if step.get("strategy")
    ]
    history_summary = ""
    if remediation_history:
        history_summary = "\n\nFull remediation history (all attempts so far):\n"
        for step in remediation_history:
            fix_ok = step.get("result", {}).get("fix_successful", False)
            history_summary += (
                f"  Attempt {step['attempt']}: strategy={step['strategy']}, "
                f"fix_successful={fix_ok}\n"
            )
        history_summary += f"\nStrategies already tried: {tried_strategies}"

    messages = [
        {"role": "user",
         "content": f"""
Assess remediation attempt #{retry_count} for run {run_id}.
Max retries allowed: {max_retries}
Retries used so far: {retry_count}

Latest remediation result:
{json.dumps(remediation_result, indent=2)}
{history_summary}

IMPORTANT: Do NOT recommend a strategy that has already been tried and failed.
Check the current pipeline state and determine: resolved, retry, or escalate?
"""}
    ]

    thoughts = []
    _iter = 0

    while True:
        _iter += 1
        if _iter > 10:
            return {"assessment": "escalate", "escalation_reason": "max iterations exceeded", "thoughts": thoughts}
        if stop_event and stop_event.is_set():
            return {"assessment": "escalate", "escalation_reason": "cancelled", "thoughts": thoughts}

        response = client.messages.create(
            model=os.getenv("REFLECTION_MODEL", "claude-sonnet-4-5"),
            max_tokens=1000,
            system=REFLECTION_SYSTEM_PROMPT,
            tools=REFLECTION_TOOLS,
            messages=messages,
            timeout=60,
        )

        for block in response.content:
            if hasattr(block, "text"):
                thought = {"agent": "Reflection", "type": "thought",
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
                result = {"assessment": "escalate",
                          "escalation_reason": "reflection parse failed"}
            result["thoughts"] = thoughts
            return result

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    thought = {"agent": "Reflection", "type": "tool_call",
                               "content": f"Calling {block.name}({json.dumps(block.input)})"}
                    thoughts.append(thought)
                    if thought_callback:
                        thought_callback(thought)

                    fn = TOOL_FUNCTIONS.get(block.name)
                    result = fn(**block.input) if fn else {"error": "unknown"}

                    thought = {"agent": "Reflection", "type": "observation",
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
