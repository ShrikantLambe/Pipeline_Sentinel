import json
import os
import threading
from datetime import datetime
from agents.monitor_agent import run_monitor_agent
from agents.diagnosis_agent import run_diagnosis_agent
from agents.remediation_agent import run_remediation_agent
from agents.reflection_agent import run_reflection_agent
from agents.explanation_agent import run_explanation_agent, write_incident_to_db
from simulator.pipeline import PipelineSimulator

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

    def run(self, run_id: str) -> dict:
        self._thought("Sentinel", f"Starting assessment of run: {run_id}")

        run_state = self.sim.get_run_state(run_id)
        dag_id = run_state.get("dag_id", "unknown")

        # Accumulate all agent thoughts for the incident log
        all_thoughts = []

        def collecting_callback(thought):
            all_thoughts.append(thought)
            if self.thought_callback:
                self.thought_callback(thought)

        def _cancelled():
            return self.stop_event and self.stop_event.is_set()

        # Step 1: Monitor
        self._thought("Sentinel", "Invoking Monitor Agent...")
        monitor_result = run_monitor_agent(run_id, collecting_callback,
                                           stop_event=self.stop_event)

        if _cancelled():
            return {"status": "cancelled", "run_id": run_id}

        if not monitor_result.get("anomaly_detected"):
            self._thought("Sentinel", "✅ No anomaly detected. Run is healthy.")
            return {"status": "healthy", "run_id": run_id}

        self._thought("Sentinel",
                      f"⚠ Anomaly detected: {monitor_result.get('anomaly_type')}")

        # Step 2: Diagnose
        self._thought("Sentinel", "Invoking Diagnosis Agent...")
        diagnosis = run_diagnosis_agent(run_id, monitor_result,
                                        collecting_callback,
                                        stop_event=self.stop_event)
        if _cancelled():
            return {"status": "cancelled", "run_id": run_id}

        # Step 3: Remediation + Reflection loop
        retry_count = 0
        remediation_steps = []
        current_diagnosis = diagnosis
        reflection_notes = ""
        resolution_status = "escalated"
        tried_strategies = set()

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

        # Step 4: Explain
        self._thought("Sentinel", "Invoking Explanation Agent...")
        collecting_callback({"agent": "Sentinel", "type": "thought",
                             "content": "Invoking Explanation Agent..."})
        incident_context = {
            "run_id": run_id,
            "dag_id": dag_id,
            "monitor_alert": monitor_result,
            "diagnosis": diagnosis,
            "remediation_attempts": remediation_steps,
            "reflection_notes": reflection_notes,
            "resolution_status": resolution_status,
            "retry_count": retry_count,
        }
        explanation = run_explanation_agent(run_id, incident_context,
                                            collecting_callback)

        # Step 5: Write incident log (include full thought stream for replay)
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
        }
