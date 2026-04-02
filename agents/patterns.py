"""
Prompt 5 — Incident Pattern Memory
Stores resolved incident patterns so the Diagnosis agent can skip full LLM
reasoning when a high-confidence known fix exists.

Pattern matching rule
---------------------
A pattern is used directly (bypassing LLM reasoning) when:
  - occurrence_count >= 3
  - success_rate    >= 0.90

The Diagnosis agent calls get_known_patterns() before its ReAct loop and
attaches a `patterns_consulted` field to its output so the orchestrator and
Explanation agent know whether pattern memory was used.
"""

import json
from datetime import datetime
from simulator.database import get_connection

# Confidence threshold for a pattern to be used directly
PATTERN_MIN_OCCURRENCES = 3
PATTERN_MIN_SUCCESS_RATE = 0.90


def get_known_patterns(pipeline_name: str) -> list[dict]:
    """
    Return historical patterns for a pipeline, ranked by occurrence_count desc.

    Each record:
        root_cause_category, fix_action_taken, success_rate,
        occurrence_count, last_seen_at, is_high_confidence (bool)
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT root_cause_category, fix_action_taken,
               success_rate, occurrence_count, last_seen_at
        FROM incident_patterns
        WHERE pipeline_name = ?
        ORDER BY occurrence_count DESC, success_rate DESC
        """,
        (pipeline_name,),
    )
    cols = ["root_cause_category", "fix_action_taken", "success_rate",
            "occurrence_count", "last_seen_at"]
    rows = [dict(zip(cols, r)) for r in c.fetchall()]
    conn.close()

    for row in rows:
        row["is_high_confidence"] = (
            row["occurrence_count"] >= PATTERN_MIN_OCCURRENCES
            and row["success_rate"] >= PATTERN_MIN_SUCCESS_RATE
        )
    return rows


def match_pattern(
    root_cause_category: str,
    pipeline_name: str,
    known_patterns: list[dict],
) -> dict | None:
    """
    Return the best matching high-confidence pattern, or None if no match.
    The Diagnosis agent uses this to skip the LLM ReAct loop.
    """
    for pattern in known_patterns:
        if (
            pattern["is_high_confidence"]
            and pattern["root_cause_category"] == root_cause_category
        ):
            return pattern
    return None


def upsert_pattern(
    root_cause_category: str,
    pipeline_name: str,
    fix_action_taken: str,
    success: bool,
) -> None:
    """
    Insert a new pattern record or update the rolling success_rate and
    occurrence_count of an existing one.

    Called from the orchestrator after each resolved or escalated incident.

    Rolling average formula:
        new_rate = (old_rate * old_count + new_result) / (old_count + 1)
    """
    conn = get_connection()
    c = conn.cursor()

    c.execute(
        """
        SELECT id, success_rate, occurrence_count
        FROM incident_patterns
        WHERE root_cause_category = ?
          AND pipeline_name = ?
          AND fix_action_taken = ?
        """,
        (root_cause_category, pipeline_name, fix_action_taken),
    )
    row = c.fetchone()
    now = datetime.now()

    if row:
        pid, old_rate, old_count = row
        new_count = old_count + 1
        new_rate  = (old_rate * old_count + (1.0 if success else 0.0)) / new_count
        c.execute(
            """
            UPDATE incident_patterns
            SET success_rate = ?, occurrence_count = ?, last_seen_at = ?
            WHERE id = ?
            """,
            (round(new_rate, 4), new_count, now, pid),
        )
    else:
        c.execute(
            """
            INSERT INTO incident_patterns
                (root_cause_category, pipeline_name, fix_action_taken,
                 success_rate, occurrence_count, last_seen_at)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            (root_cause_category, pipeline_name, fix_action_taken,
             1.0 if success else 0.0, now),
        )

    conn.commit()
    conn.close()
