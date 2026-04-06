"""
Token budget baseline — measures the static context weight of each agent's
system prompt + tool schemas before any user messages or tool results.

Usage:
    python scripts/token_budget.py

Prints a table of token counts per agent and a total.
Uses the Anthropic count_tokens API (no LLM call, billed at $0).
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import anthropic

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
MODEL  = os.getenv("REFLECTION_MODEL", "claude-sonnet-4-5")


def count(system: str, tools: list = None, extra_messages: list = None) -> int:
    kwargs = {
        "model":    MODEL,
        "messages": extra_messages or [{"role": "user", "content": "ping"}],
        "system":   system,
    }
    if tools:
        kwargs["tools"] = tools
    resp = client.messages.count_tokens(**kwargs)
    return resp.input_tokens


def main():
    from agents.monitor_agent     import MONITOR_SYSTEM_PROMPT, MONITOR_TOOLS
    from agents.diagnosis_agent   import DIAGNOSIS_SYSTEM_PROMPT
    from agents.remediation_agent import REMEDIATION_SYSTEM_PROMPT, REMEDIATION_TOOLS
    from agents.reflection_agent  import REFLECTION_SYSTEM_PROMPT
    from agents.explanation_agent import EXPLANATION_SYSTEM_PROMPT

    # Diagnosis tools (defined inline in the module)
    from agents.tools import (
        get_pipeline_status, get_failed_tasks, check_row_count_anomaly,
    )
    DIAGNOSIS_TOOLS = [
        {"name": "get_pipeline_status",
         "description": "Fetch the full current state of a pipeline run.",
         "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]}},
        {"name": "get_failed_tasks",
         "description": "Return all failed tasks for a run with their error messages.",
         "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]}},
        {"name": "check_row_count_anomaly",
         "description": "Check if the actual row count deviates significantly from expected.",
         "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]}},
    ]
    REFLECTION_TOOLS = [
        {"name": "get_pipeline_status",
         "description": "Fetch the full current state of a pipeline run.",
         "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]}},
        {"name": "check_row_count_anomaly",
         "description": "Check if the actual row count deviates significantly from expected.",
         "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]}},
    ]

    rows = [
        ("Monitor",     MONITOR_SYSTEM_PROMPT,     MONITOR_TOOLS),
        ("Diagnosis",   DIAGNOSIS_SYSTEM_PROMPT,   DIAGNOSIS_TOOLS),
        ("Remediation", REMEDIATION_SYSTEM_PROMPT, REMEDIATION_TOOLS),
        ("Reflection",  REFLECTION_SYSTEM_PROMPT,  REFLECTION_TOOLS),
        ("Explanation", EXPLANATION_SYSTEM_PROMPT, None),
    ]

    print(f"\n{'Agent':<14} {'Tokens':>8}  Notes")
    print("-" * 50)
    total = 0
    for name, system, tools in rows:
        n = count(system, tools)
        total += n
        note = f"{len(tools)} tools" if tools else "no tools"
        print(f"{name:<14} {n:>8,}  ({note})")

    print("-" * 50)
    print(f"{'TOTAL':<14} {total:>8,}")
    budget = 200_000
    pct    = total / budget * 100
    print(f"\nStatic context uses {pct:.1f}% of {budget:,}-token context window.")
    if pct > 10:
        print("⚠  Over 10% of context window consumed by static prompts alone.")
    else:
        print("✓  Well within budget.")


if __name__ == "__main__":
    main()
