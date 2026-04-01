import json


def extract_json(text: str) -> dict:
    """
    Robustly extract the first valid JSON object from an LLM response.

    Handles cases where Claude wraps JSON in markdown code fences, adds
    explanatory text before/after, or produces nested objects.
    Falls back to {} if no valid JSON is found.
    """
    if not text:
        return {}

    text = text.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(
            line for line in lines
            if not line.strip().startswith("```")
        ).strip()

    # Try parsing the whole response as JSON first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Walk character by character to find the first complete JSON object.
    # This correctly handles nested objects unlike a greedy regex.
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start is not None:
                candidate = text[start:i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    # Not valid JSON — reset and keep scanning
                    start = None

    return {}
