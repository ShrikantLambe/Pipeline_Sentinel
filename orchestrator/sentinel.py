import json
import os
import threading
from datetime import datetime
from agents.monitor_agent import run_monitor_agent
from agents.diagnosis_agent import run_diagnosis_agent
from agents.remediation_agent import run_remediation_agent
from agents.reflection_agent import run_reflection_agent
from agents.explanation_agent import run_explanation_agent, write_incident_to_db
from agents.audit import log_agent_transition, update_audit_incident_id
from agents.blast_radius import assess_blast_radius, should_auto_remediate
from agents.metrics import write_incident_outcome
from agents.patterns import upsert_pattern
from simulator.pipeline import PipelineSimulator
from langsmith import traceable

MAX_RETRIES = int(os.getenv("MAX_RETRY_ATTEMPTS", 3))


class SentinelOrchestrator:
    def __init__(self, thought_callback=None, stop_event: threading.Event = None):
        self.thought_callback = thought_callback
        self.stop_event = stop_event
        self.sim = PipelineSimulator()

    def _thought(self, agent: str, content: str, type_: str = "thought"):
        if self.thought_callback:
            self.thought_callback({
                "agent": agent, "type": type_, "content": content
            })

    @traceable(name="Sentinel Orchestrator", run_type="chain",
               tags=["pipeline-sentinel", "orchestrator"])
    def run(self, run_id: str) -> dict:
        self._thought("Sentinel", f"Starting assessment of run: {run_id}")

        run_state = self.sim.get_run_state(run_id)
        dag_id       = run_state.get("dag_id", "unknown")
        pipeline_name = dag_id  # used as pipeline_name in pattern memory

        # Accumulate all agent thoughts for the incident log
        all_thoughts = []

        def collecting_callback(thought):
            all_thoughts.append(thought)
            if self.thought_callback:
                self.thought_callback(thought)

        def _cancelled():
            return self.stop_event and self.stop_event.is_set()

        detected_at = datetime.now()

        # ── Step 1: Monitor ────────────────────────────────────────────────
        self._thought("Sentinel", "Invoking Monitor Agent...")
        monitor_result = run_monitor_agent(run_id, collecting_callback,
                                           stop_event=self.stop_event)

        log_agent_transition(
            run_id=run_id,
            agent_name="Monitor",
            input_summary={"run_id": run_id},
            decision=(
                f"anomaly_detected={monitor_result.get('anomaly_detected')}, "
                f"type={monitor_result.get('anomaly_type')}"
            ),
            output_summary={k: v for k, v in monitor_result.items() if k != "thoughts"},
        )

        if _cancelled():
            return {"status": "cancelled", "run_id": run_id}

        if not monitor_result.get("anomaly_detected"):
            self._thought("Sentinel", "✅ No anomaly detected. Run is healthy.")
            return {"status": "healthy", "run_id": run_id}

        self._thought("Sentinel",
                      f"⚠ Anomaly detected: {monitor_result.get('anomaly_type')}")

        # ── Step 2: Blast Radius ───────────────────────────────────────────
        affected_task = monitor_result.get("affected_task", "")
        blast_info    = assess_blast_radius(affected_task)
        blast_radius  = blast_info["blast_radius_score"]
        self._thought(
            "Sentinel",
            f"Blast radius assessment: {blast_radius} "
            f"(level={blast_info['affected_level']}, "
            f"consumers={blast_info['affected_consumers'][:3]}{'…' if len(blast_info['affected_consumers']) > 3 else ''})"
        )

        # ── Step 3: Diagnose ───────────────────────────────────────────────
        self._thought("Sentinel", "Invoking Diagnosis Agent...")
        diagnosis = run_diagnosis_agent(
            run_id, monitor_result,
            collecting_callback,
            stop_event=self.stop_event,
            pipeline_name=pipeline_name,
        )
        if _cancelled():
            return {"status": "cancelled", "run_id": run_id}

        log_agent_transition(
            run_id=run_id,
            agent_name="Diagnosis",
            input_summary={"anomaly_type": monitor_result.get("anomaly_type")},
            decision=(
                f"root_cause={diagnosis.get('root_cause', '')[:80]}, "
                f"strategy={diagnosis.get('remediation_strategy')}"
            ),
            confidence=diagnosis.get("confidence"),
            output_summary={k: v for k, v in diagnosis.items() if k != "thoughts"},
        )

        # ── Step 4: Escalation gate (blast radius × confidence) ────────────
        confidence_str = diagnosis.get("confidence", "low")
        can_remediate, gate_reason = should_auto_remediate(confidence_str, blast_radius)

        if not can_remediate:
            self._thought("Sentinel", f"🚨 {gate_reason} — escalating without auto-remediation.")
            resolution_status  = "escalated"
            remediation_steps  = []
            reflection_notes   = gate_reason
            retry_count        = 0
        else:
            self._thought("Sentinel", f"✅ {gate_reason} — proceeding with auto-remediation.")

            # ── Step 5: Remediation + Reflection loop ──────────────────────
            retry_count        = 0
            remediation_steps  = []
            current_diagnosis  = diagnosis
            reflection_notes   = ""
            resolution_status  = "escalated"
            tried_strategies   = set()

            while retry_count < MAX_RETRIES:
                retry_count += 1
                current_strategy = current_diagnosis.get("remediation_strategy")
                self._thought("Sentinel",
                              f"Attempt {retry_count}/{MAX_RETRIES}: "
                              f"Invoking Remediation Agent (strategy: {current_strategy})...")

                if _cancelled():
                    break
                remediation = run_remediation_agent(run_id, current_diagnosis,
                                                    collecting_callback,
                                                    stop_event=self.stop_event)
                tried_strategies.add(current_strategy)
                remediation_steps.append({
                    "attempt": retry_count,
                    "strategy": current_strategy,
                    "result": remediation,
                })

                log_agent_transition(
                    run_id=run_id,
                    agent_name="Remediation",
                    input_summary={"strategy": current_strategy, "attempt": retry_count},
                    decision=f"fix_successful={remediation.get('fix_successful')}",
                    output_summary={k: v for k, v in remediation.items() if k != "thoughts"},
                )

                if _cancelled():
                    break
                self._thought("Sentinel", "Invoking Reflection Agent...")
                reflection = run_reflection_agent(
                    run_id, remediation,
                    retry_count, MAX_RETRIES,
                    remediation_history=remediation_steps,
                    thought_callback=collecting_callback,
                    stop_event=self.stop_event,
                )
                reflection_notes = reflection.get("updated_hypothesis", "")

                log_agent_transition(
                    run_id=run_id,
                    agent_name="Reflection",
                    input_summary={"attempt": retry_count, "fix_successful": remediation.get("fix_successful")},
                    decision=f"assessment={reflection.get('assessment')}",
                    confidence=reflection.get("confidence"),
                    output_summary={k: v for k, v in reflection.items() if k != "thoughts"},
                )

                assessment = reflection.get("assessment")
                self._thought("Sentinel",
                              f"Reflection assessment: {assessment.upper()}")

                if assessment == "resolved":
                    resolution_status = "resolved"
                    self._thought("Sentinel", "✅ Pipeline recovered successfully!")
                    break
                elif assessment == "escalate":
                    self._thought("Sentinel",
                                  "🚨 Escalating — human intervention required.")
                    break
                else:
                    next_strategy = reflection.get("next_strategy")
                    if next_strategy and next_strategy in tried_strategies:
                        self._thought("Sentinel",
                                      f"⚠ Reflection suggested '{next_strategy}' but it was already tried. Escalating.")
                        break
                    if next_strategy:
                        current_diagnosis = {
                            **current_diagnosis,
                            "remediation_strategy": next_strategy,
                        }
                        self._thought("Sentinel",
                                      f"Retrying with new strategy: {next_strategy}")
                    else:
                        self._thought("Sentinel",
                                      "No alternative strategy available. Escalating.")
                        break

        # ── Step 6: Explain ────────────────────────────────────────────────
        self._thought("Sentinel", "Invoking Explanation Agent...")
        collecting_callback({"agent": "Sentinel", "type": "thought",
                             "content": "Invoking Explanation Agent..."})

        incident_context = {
            "run_id": run_id,
            "dag_id": dag_id,
            "monitor_alert": monitor_result,
            "diagnosis": diagnosis,
            "blast_radius": blast_info,
            "remediation_attempts": remediation_steps,
            "reflection_notes": reflection_notes,
            "resolution_status": resolution_status,
            "retry_count": retry_count,
            "schema_drift_summary": monitor_result.get("schema_drift_summary"),
            "patterns_consulted": diagnosis.get("patterns_consulted", []),
            "pattern_matched": diagnosis.get("pattern_matched", False),
        }
        explanation = run_explanation_agent(run_id, incident_context,
                                            collecting_callback)

        log_agent_transition(
            run_id=run_id,
            agent_name="Explanation",
            input_summary={"resolution_status": resolution_status},
            decision=f"title={explanation.get('title', '')[:80]}",
            output_summary={k: v for k, v in explanation.items()},
        )

        # ── Step 7: Write incident log ─────────────────────────────────────
        incident_id = write_incident_to_db(
            run_id=run_id,
            dag_id=dag_id,
            failure_type=monitor_result.get("anomaly_type", "unknown"),
            resolution_status=resolution_status,
            retry_attempts=retry_count,
            root_cause=diagnosis.get("root_cause", ""),
            remediation_steps=remediation_steps,
            reflection_notes=reflection_notes,
            explanation=explanation,
            thought_log=all_thoughts,
            blast_radius=blast_radius,
            patterns_consulted=diagnosis.get("patterns_consulted", []),
        )

        # Back-fill incident_id on all audit log rows for this run
        update_audit_incident_id(run_id, incident_id)

        # ── Step 8: Write outcome metrics ──────────────────────────────────
        write_incident_outcome(
            run_id=run_id,
            dag_id=dag_id,
            incident_id=incident_id,
            detected_at=detected_at,
            resolved_at=datetime.now(),
            resolution_status=resolution_status,
            root_cause_category=monitor_result.get("anomaly_type", "unknown"),
            agent_confidence=diagnosis.get("confidence", "low"),
            blast_radius=blast_radius,
        )

        # ── Step 9: Upsert pattern memory ─────────────────────────────────
        strategy_used = (
            remediation_steps[-1]["strategy"] if remediation_steps
            else diagnosis.get("remediation_strategy", "")
        )
        if strategy_used and strategy_used != "escalate_to_manual_intervention":
            upsert_pattern(
                root_cause_category=monitor_result.get("anomaly_type", "unknown"),
                pipeline_name=pipeline_name,
                fix_action_taken=strategy_used,
                success=(resolution_status == "resolved"),
            )

        self._thought("Sentinel",
                      f"Incident #{incident_id} logged. "
                      f"Status: {resolution_status.upper()}")

        return {
            "incident_id": incident_id,
            "run_id": run_id,
            "resolution_status": resolution_status,
            "retry_count": retry_count,
            "explanation": explanation,
            "blast_radius": blast_radius,
            "pattern_matched": diagnosis.get("pattern_matched", False),
        }
