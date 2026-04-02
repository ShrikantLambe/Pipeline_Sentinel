import streamlit as st
import json
import time
import os
import sys
import threading
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator.database import init_db, get_connection
from simulator.pipeline import PipelineSimulator
from simulator.failure_injector import FailureInjector, FAILURE_MODES
from simulator.airflow_connector import AirflowConnector
from orchestrator.sentinel import SentinelOrchestrator
from agents.metrics import compute_self_healing_metrics
from agents.audit import get_incident_audit

_STOP_EVENT = threading.Event()

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")

st.set_page_config(
    page_title="Pipeline Sentinel",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* ── Run header ── */
  .run-header {
    display: flex; align-items: center; flex-wrap: wrap; gap: 12px;
    padding: 12px 16px; border-radius: 8px; margin-bottom: 16px;
    border: 1px solid rgba(255,255,255,0.08);
    background: rgba(255,255,255,0.03);
  }
  .run-dag   { font-size: 18px; font-weight: 700; color: inherit; }
  .run-meta  { font-size: 12px; opacity: 0.6; line-height: 1.8; }
  .run-badge {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-size: 12px; font-weight: 700; letter-spacing: 0.05em; text-transform: uppercase;
  }
  .badge-failed     { background: rgba(231,76,60,0.2);   color: #e74c3c; border: 1px solid rgba(231,76,60,0.5); }
  .badge-success    { background: rgba(46,204,113,0.2);  color: #2ecc71; border: 1px solid rgba(46,204,113,0.5); }
  .badge-remediated { background: rgba(52,152,219,0.2);  color: #3498db; border: 1px solid rgba(52,152,219,0.5); }
  .badge-running    { background: rgba(241,196,15,0.2);  color: #f1c40f; border: 1px solid rgba(241,196,15,0.5); }
  .badge-escalated  { background: rgba(155,89,182,0.2);  color: #9b59b6; border: 1px solid rgba(155,89,182,0.5); }
  .badge-unknown    { background: rgba(150,150,150,0.2); color: #aaa;    border: 1px solid rgba(150,150,150,0.4); }

  /* ── Task flow ── */
  .task-flow { display: flex; flex-direction: column; gap: 2px; }
  .task-item {
    display: grid; grid-template-columns: 90px 1fr auto;
    align-items: start; gap: 8px;
    padding: 6px 8px; border-radius: 6px;
    font-size: 13px; color: inherit;
    border-left: 3px solid transparent;
  }
  .ti-success    { border-left-color: #2ecc71; background: rgba(46,204,113,0.05); }
  .ti-failed     { border-left-color: #e74c3c; background: rgba(231,76,60,0.07); }
  .ti-upstream   { border-left-color: #9b59b6; background: rgba(155,89,182,0.05); }
  .ti-running    { border-left-color: #f1c40f; background: rgba(241,196,15,0.05); }
  .ti-queued     { border-left-color: rgba(150,150,150,0.3); background: transparent; }
  .ti-skipped    { border-left-color: rgba(150,150,150,0.2); background: transparent; opacity: 0.5; }

  .task-status-label {
    font-size: 10px; font-weight: 700; letter-spacing: 0.06em;
    text-transform: uppercase; padding: 2px 6px; border-radius: 4px;
    white-space: nowrap;
  }
  .tsl-success  { color: #2ecc71; background: rgba(46,204,113,0.15); }
  .tsl-failed   { color: #e74c3c; background: rgba(231,76,60,0.15); }
  .tsl-upstream { color: #9b59b6; background: rgba(155,89,182,0.15); }
  .tsl-running  { color: #f1c40f; background: rgba(241,196,15,0.15); }
  .tsl-queued   { color: #888;    background: rgba(150,150,150,0.12); }
  .tsl-skipped  { color: #888;    background: rgba(150,150,150,0.10); }

  .task-name  { font-family: monospace; font-size: 13px; }
  .task-dur   { font-size: 11px; opacity: 0.5; white-space: nowrap; }
  .task-err   { font-size: 11px; opacity: 0.6; grid-column: 2 / -1; padding-top: 2px; }

  /* ── Analysis panel ── */
  .analysis-header {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 14px; border-radius: 8px; margin-bottom: 10px;
  }
  .analysis-waiting {
    background: rgba(241,196,15,0.1); border: 1px solid rgba(241,196,15,0.3);
  }
  .analysis-done-ok {
    background: rgba(46,204,113,0.1); border: 1px solid rgba(46,204,113,0.3);
  }
  .analysis-done-bad {
    background: rgba(155,89,182,0.1); border: 1px solid rgba(155,89,182,0.3);
  }
  .analysis-idle {
    background: rgba(150,150,150,0.08); border: 1px solid rgba(150,150,150,0.2);
  }
  .analysis-title { font-size: 14px; font-weight: 700; color: inherit; }
  .analysis-sub   { font-size: 12px; opacity: 0.65; }

  /* ── Phase stepper ── */
  .phase-bar { display: flex; gap: 3px; margin: 8px 0; }
  .phase-chip {
    flex: 1; text-align: center; padding: 5px 4px;
    border-radius: 5px; font-size: 10px; font-weight: 700;
    letter-spacing: 0.04em; border: 1px solid transparent;
    color: inherit; opacity: 0.25; transition: opacity 0.2s;
  }
  .phase-chip.active  { opacity: 1; border-color: currentColor; }
  .phase-chip.done    { opacity: 0.6; }
  .phase-Monitor     { color: #4a90d9; }
  .phase-Diagnosis   { color: #9b59b6; }
  .phase-Remediation { color: #e67e22; }
  .phase-Reflection  { color: #e74c3c; }
  .phase-Explanation { color: #2ecc71; }

  /* ── Thought stream ── */
  .thought-block {
    border-left: 3px solid #4a90d9;
    background: rgba(74,144,217,0.07);
    padding: 6px 10px; margin: 2px 0;
    border-radius: 0 5px 5px 0;
    font-size: 12px; line-height: 1.5;
    color: inherit; word-break: break-word;
  }
  .thought-block.tool { border-left-color: #e6a817; background: rgba(230,168,23,0.07); }
  .thought-block.obs  { border-left-color: #2ecc71; background: rgba(46,204,113,0.07); }
  .thought-block.wait { border-left-color: #f1c40f; background: rgba(241,196,15,0.07); opacity: 0.75; }
  .agent-label { font-weight: 700; font-size: 11px; letter-spacing: 0.03em; }

  /* ── Verdict card ── */
  .verdict-card {
    padding: 14px 18px; border-radius: 8px; margin-top: 4px;
  }
  .verdict-resolved  { background: rgba(46,204,113,0.1);  border: 1px solid rgba(46,204,113,0.35); }
  .verdict-escalated { background: rgba(155,89,182,0.1);  border: 1px solid rgba(155,89,182,0.35); }
  .verdict-cancelled { background: rgba(150,150,150,0.1); border: 1px solid rgba(150,150,150,0.3); }
  .verdict-label { font-size: 13px; font-weight: 700; margin-bottom: 6px; }
  .verdict-body  { font-size: 13px; line-height: 1.6; }

  a.af-link {
    font-size: 11px; opacity: 0.6; text-decoration: none;
    padding: 2px 6px; border-radius: 4px;
    border: 1px solid currentColor;
  }
  a.af-link:hover { opacity: 1; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────
init_db()

_DEFAULTS = {
    "thought_stream":   [],
    "current_run_id":   None,
    "running":          False,
    "run_error":        None,
    "agent_phase":      None,
    "phases_done":      [],
    "run_start_time":   None,
    "last_thought_ts":  None,   # time.time() when last thought arrived
    "verdict":          None,   # final resolution summary from incident DB
    "mode":             "Simulator",
    "af_dags":          [],
    "af_failed_runs":   [],
    "af_selected_run":  None,
    "af_ping_ok":       None,
    "af_ping_error":    "",
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

MAX_STREAM_DISPLAY = 60
MAX_STREAM_STORED  = 500
AGENT_PHASES = ["Monitor", "Diagnosis", "Remediation", "Reflection", "Explanation"]
AGENT_COLOR  = {
    "Monitor": "#4a90d9", "Diagnosis": "#9b59b6", "Remediation": "#e67e22",
    "Reflection": "#e74c3c", "Explanation": "#2ecc71", "Sentinel": "#95a5a6",
}

# ── Helpers ───────────────────────────────────────────────────────────────

def _fmt_dt(ts_str) -> str:
    """Format a timestamp string as HH:MM:SS UTC."""
    if not ts_str:
        return "—"
    try:
        dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts_str)[:19]


def _duration(start_str, end_str) -> str:
    if not start_str or not end_str:
        return "—"
    try:
        s = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
        e = datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))
        secs = int((e - s).total_seconds())
        if secs < 60:
            return f"{secs}s"
        return f"{secs // 60}m {secs % 60}s"
    except Exception:
        return "—"


def _parse_scheduled_time(run_id: str) -> str:
    """Extract scheduled time from run_id like scheduled__2026-03-28T06:40:00+00:00"""
    if "scheduled__" in run_id:
        ts = run_id.replace("scheduled__", "")
        try:
            dt = datetime.fromisoformat(ts)
            return dt.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass
    return run_id


def _airflow_run_url(dag_id: str, run_id: str) -> str:
    return f"{AIRFLOW_URL}/dags/{dag_id}/grid?dag_run_id={run_id}&tab=details"


def _task_css(status: str) -> str:
    s = status or "queued"
    if s == "upstream_failed":
        return "upstream"
    return s if s in ("success", "failed", "running", "queued", "skipped") else "queued"


def thought_html(thought: dict) -> str:
    agent   = thought.get("agent", "")
    kind    = thought.get("type", "thought")
    content = thought.get("content", "")
    color   = AGENT_COLOR.get(agent, "#aaa")
    css     = {"tool_call": "tool", "observation": "obs"}.get(kind, "")
    prefix  = {"tool_call": "🔧 ", "observation": "👁 ", "thought": "💭 "}.get(kind, "")
    safe    = content[:350].replace("<", "&lt;").replace(">", "&gt;")
    return (
        f'<div class="thought-block {css}">'
        f'<span class="agent-label" style="color:{color}">[{agent}]</span> '
        f'{prefix}{safe}</div>'
    )


# ── Background thread ─────────────────────────────────────────────────────

def _start_sentinel_thread(run_id: str):
    _STOP_EVENT.clear()
    st.session_state.running        = True
    st.session_state.thought_stream = []
    st.session_state.run_error      = None
    st.session_state.current_run_id = run_id
    st.session_state.agent_phase    = "Monitor"
    st.session_state.phases_done    = []
    st.session_state.run_start_time = time.time()
    st.session_state.last_thought_ts = time.time()
    st.session_state.verdict        = None

    def worker():
        def thought_cb(thought):
            agent = thought.get("agent", "")
            if agent in AGENT_PHASES:
                if st.session_state.agent_phase != agent and st.session_state.agent_phase is not None:
                    prev = st.session_state.agent_phase
                    if prev not in st.session_state.phases_done:
                        st.session_state.phases_done.append(prev)
                st.session_state.agent_phase = agent
            if len(st.session_state.thought_stream) < MAX_STREAM_STORED:
                st.session_state.thought_stream.append(thought)
            st.session_state.last_thought_ts = time.time()

        try:
            orch = SentinelOrchestrator(thought_callback=thought_cb, stop_event=_STOP_EVENT)
            orch.run(run_id)
        except Exception as e:
            st.session_state.run_error = str(e)
        finally:
            if st.session_state.agent_phase and st.session_state.agent_phase not in st.session_state.phases_done:
                st.session_state.phases_done.append(st.session_state.agent_phase)
            st.session_state.agent_phase = None
            st.session_state.running     = False

    threading.Thread(target=worker, daemon=True).start()


# ── Sidebar ───────────────────────────────────────────────────────────────
run_btn = af_run_btn = False
selected_failure = "random"
healthy_run = False

with st.sidebar:
    st.title("🛡️ Pipeline Sentinel")
    st.caption("Self-Healing Data Pipeline Agent")
    st.divider()

    mode = st.radio(
        "Data source", ["Simulator", "Live Airflow"],
        index=0 if st.session_state.mode == "Simulator" else 1,
        horizontal=True, disabled=st.session_state.running,
    )
    if mode != st.session_state.mode:
        st.session_state.af_ping_ok     = None
        st.session_state.af_failed_runs = []
    st.session_state.mode = mode
    st.divider()

    # ── Simulator ─────────────────────────────────────────────────────────
    if mode == "Simulator":
        selected_failure = st.selectbox(
            "Inject failure type",
            ["random"] + list(FAILURE_MODES.keys()),
            format_func=lambda x: x.replace("_", " ").title(),
            disabled=st.session_state.running,
        )
        healthy_run = st.checkbox("Simulate healthy run", value=False,
                                  disabled=st.session_state.running)
        st.divider()

        if st.session_state.running:
            elapsed = int(time.time() - (st.session_state.run_start_time or time.time()))
            st.info(f"⏳ Sentinel running… {elapsed}s", icon="🤖")
            if st.button("⏹ Cancel run", use_container_width=True):
                _STOP_EVENT.set()
        else:
            run_btn = st.button("▶  Run Pipeline", type="primary", use_container_width=True)

        st.divider()
        with st.expander("Failure mode reference", expanded=False):
            for name, cfg in FAILURE_MODES.items():
                st.markdown(f"**{name.replace('_',' ').title()}**")
                st.caption(f"{cfg['description']} — task: `{cfg['affected_task']}`")

    # ── Live Airflow ───────────────────────────────────────────────────────
    else:
        c1, c2 = st.columns([4, 1])
        with c2:
            if st.button("↺", help="Re-check", disabled=st.session_state.running):
                af = AirflowConnector()
                ping = af.ping()
                st.session_state.af_ping_ok    = ping.get("ok", False)
                st.session_state.af_ping_error = ping.get("error", "")
                if ping.get("ok"):
                    st.session_state.af_dags = af.list_dags()

        if st.session_state.af_ping_ok is None:
            af = AirflowConnector()
            ping = af.ping()
            st.session_state.af_ping_ok    = ping.get("ok", False)
            st.session_state.af_ping_error = ping.get("error", "")
            if ping.get("ok"):
                st.session_state.af_dags = af.list_dags()

        with c1:
            if st.session_state.af_ping_ok:
                st.success("Airflow connected", icon="✅")
            else:
                st.error("Unreachable", icon="❌")
                st.caption(f"`{AIRFLOW_URL}`\n\n{st.session_state.af_ping_error}")

        st.divider()

        selected_dags = []
        if st.session_state.af_dags:
            selected_dags = st.multiselect(
                "Watch DAGs", st.session_state.af_dags,
                default=st.session_state.af_dags[:1],
                disabled=st.session_state.running,
            )
        elif st.session_state.af_ping_ok:
            st.caption("No active DAGs found.")

        if st.button("🔍 Poll for new failures", use_container_width=True,
                     disabled=st.session_state.running or not selected_dags or not st.session_state.af_ping_ok):
            af = AirflowConnector()
            with st.spinner("Querying Airflow…"):
                st.session_state.af_failed_runs = af.poll_failed_runs(selected_dags)

        failed_runs = st.session_state.af_failed_runs
        if failed_runs:
            labels = [f"{dag}  ·  {rid.replace('scheduled__','')[:19]}" for dag, rid in failed_runs]
            chosen = st.radio(f"Failed runs ({len(failed_runs)})", labels,
                              disabled=st.session_state.running)
            st.session_state.af_selected_run = failed_runs[labels.index(chosen)]
        elif st.session_state.af_ping_ok and selected_dags:
            st.caption("No new failed runs. Click **Poll** to check.")

        st.divider()
        if st.session_state.running:
            elapsed = int(time.time() - (st.session_state.run_start_time or time.time()))
            st.info(f"⏳ Sentinel running… {elapsed}s", icon="🤖")
            if st.button("⏹ Cancel run", use_container_width=True, key="af_cancel"):
                _STOP_EVENT.set()
        else:
            af_run_btn = st.button(
                "▶  Run Sentinel on selected", type="primary", use_container_width=True,
                disabled=(st.session_state.af_selected_run is None or not st.session_state.af_ping_ok),
            )


# ── Main content ──────────────────────────────────────────────────────────
st.title("🛡️ Pipeline Sentinel")

# ── Self-healing metrics summary (Prompt 3) ───────────────────────────────
def _render_metrics_row():
    """Compact KPI row at the top of the dashboard."""
    m = compute_self_healing_metrics(days=30)
    total = m["total_incidents"]
    if total == 0:
        st.caption("No incidents in the last 30 days — run a simulation to see metrics.")
        return

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Incidents (30d)", total)
    c2.metric("Auto-resolved", m["auto_resolved"],
              delta=f"{m['self_healing_rate_pct']}%")
    c3.metric("Escalated", m["escalated"])

    def _fmt_mttr(secs):
        if secs is None:
            return "—"
        if secs < 60:
            return f"{secs:.0f}s"
        return f"{secs/60:.1f}m"

    c4.metric("MTTR (auto)", _fmt_mttr(m["avg_mttr_auto_seconds"]))
    c5.metric("MTTR (escalated)", _fmt_mttr(m["avg_mttr_escalated_seconds"]))

    if m["daily_trend"] and len(m["daily_trend"]) > 1:
        import streamlit as _st
        dates = [d["date"] for d in m["daily_trend"]]
        rates = [d["rate_pct"] for d in m["daily_trend"]]
        with st.expander("📈 Daily self-healing rate trend", expanded=False):
            st.line_chart(
                {"Self-healing rate (%)": dict(zip(dates, rates))},
                height=140,
            )

_render_metrics_row()
st.divider()

if st.session_state.run_error:
    err = st.session_state.run_error
    if err == "Cancelled by user." or "Cancelled" in err:
        st.warning(f"Run cancelled.", icon="⏹")
    else:
        st.error(f"Agent error: {err}", icon="❌")

# ── Run header (full width) ───────────────────────────────────────────────
header_placeholder = st.empty()


def render_run_header(run_state: dict):
    with header_placeholder.container():
        if not run_state:
            return

        dag_id  = run_state.get("dag_id", "—")
        run_id  = run_state.get("run_id", "—")
        status  = run_state.get("status", "unknown")
        started = run_state.get("started_at")
        ended   = run_state.get("completed_at")
        sched   = _parse_scheduled_time(run_id)
        dur     = _duration(started, ended)
        badge   = f'<span class="run-badge badge-{status}">{status}</span>'

        af_href = _airflow_run_url(dag_id, run_id) if mode == "Live Airflow" else None
        af_link = f'<a class="af-link" href="{af_href}" target="_blank">↗ Airflow</a>' if af_href else ""

        html = f"""
        <div class="run-header">
          <div>
            <div class="run-dag">{dag_id} &nbsp; {badge} &nbsp; {af_link}</div>
            <div class="run-meta">
              Scheduled: {sched} &nbsp;·&nbsp;
              Started: {_fmt_dt(started)} &nbsp;·&nbsp;
              Ended: {_fmt_dt(ended)} &nbsp;·&nbsp;
              Duration: {dur}
            </div>
            <div class="run-meta" style="opacity:0.4;font-size:11px">{run_id}</div>
          </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)


# ── Two columns ───────────────────────────────────────────────────────────
col_tasks, col_agent = st.columns([1, 1], gap="large")

# ── Left: Pipeline Tasks ──────────────────────────────────────────────────
with col_tasks:
    st.subheader("📋 Pipeline Tasks")
    tasks_placeholder = st.empty()


def render_tasks(run_state: dict):
    with tasks_placeholder.container():
        if not run_state:
            if st.session_state.mode == "Simulator":
                st.info("Choose a failure type and click **▶ Run Pipeline** to start.")
            else:
                st.info("Select a failed run and click **▶ Run Sentinel** to start.")
            return

        tasks = run_state.get("tasks", [])
        if not tasks:
            st.caption("No task data available.")
            return

        # Row counts
        expected = run_state.get("expected_row_count") or 0
        actual   = run_state.get("actual_row_count") or 0
        if expected > 0:
            pct = actual / expected * 100
            delta = f"{pct:.1f}% of expected"
            dc = "normal" if abs(pct - 100) < 15 else "inverse"
            c1, c2 = st.columns(2)
            c1.metric("Expected rows", f"{expected:,}")
            c2.metric("Actual rows", f"{actual:,}", delta=delta, delta_color=dc)

        # Task list
        rows_html = '<div class="task-flow">'
        for task in tasks:
            s     = task.get("status", "queued")
            css   = _task_css(s)
            name  = task.get("task_id", "")
            dur   = task.get("duration_seconds")
            err   = task.get("error_message") or task.get("error") or ""

            dur_str = f'<span class="task-dur">{dur:.1f}s</span>' if dur else '<span class="task-dur">—</span>'
            label_css = f"tsl-{css}"
            label_txt = "upstream" if css == "upstream" else s
            safe_name = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("{", "&#123;").replace("}", "&#125;")

            err_html = ""
            if err and s == "failed":
                short = str(err)[:120].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("{", "&#123;").replace("}", "&#125;")
                err_html = f'<div class="task-err">⚠ {short}</div>'

            rows_html += f"""
            <div class="task-item ti-{css}">
              <span class="task-status-label {label_css}">{label_txt}</span>
              <span class="task-name">{safe_name}</span>
              {dur_str}
              {err_html}
            </div>"""

        rows_html += "</div>"
        st.markdown(rows_html, unsafe_allow_html=True)


# ── Right: Agent Analysis ─────────────────────────────────────────────────
with col_agent:
    st.subheader("🤖 Sentinel Analysis")
    analysis_placeholder = st.empty()


def render_analysis():
    with analysis_placeholder.container():
        running   = st.session_state.running
        phase     = st.session_state.agent_phase
        done      = set(st.session_state.phases_done)
        thoughts  = st.session_state.thought_stream
        started   = st.session_state.run_start_time
        last_ts   = st.session_state.last_thought_ts

        # ── Phase progress bar ─────────────────────────────────────────────
        if running or done:
            chips = []
            for p in AGENT_PHASES:
                if p in done and p != phase:
                    state = "done"
                    icon  = "✓ "
                elif p == phase:
                    state = "active"
                    icon  = "⟳ " if running else "✓ "
                else:
                    state = "pending"
                    icon  = ""
                chips.append(f'<div class="phase-chip phase-{p} {state}">{icon}{p}</div>')
            st.markdown(f'<div class="phase-bar">{"".join(chips)}</div>', unsafe_allow_html=True)

        # ── Status header ──────────────────────────────────────────────────
        if running:
            elapsed     = int(time.time() - (started or time.time()))
            silence     = int(time.time() - (last_ts or time.time()))
            phase_color = AGENT_COLOR.get(phase, "#aaa")

            if silence >= 5:
                hdr_css  = "analysis-waiting"
                hdr_icon = "⟳"
                hdr_main = f'<span style="color:{phase_color}">{phase}</span> agent — waiting for Claude API…'
                hdr_sub  = f"No new output for {silence}s. API call in progress (60s timeout)."
            else:
                hdr_css  = "analysis-waiting"
                hdr_icon = "⟳"
                hdr_main = f'<span style="color:{phase_color}">{phase}</span> agent is reasoning…'
                hdr_sub  = f"Elapsed: {elapsed}s"

            st.markdown(
                f'<div class="analysis-header {hdr_css}">'
                f'<span style="font-size:20px">{hdr_icon}</span>'
                f'<div><div class="analysis-title">{hdr_main}</div>'
                f'<div class="analysis-sub">{hdr_sub}</div></div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        elif done:
            # Completed — show outcome
            verdict = st.session_state.verdict
            if verdict:
                status = verdict.get("resolution_status", "unknown")
                icon   = "✅" if status == "resolved" else ("🟣" if status == "escalated" else "⏹")
                label  = status.upper()
                css    = f"analysis-done-ok" if status == "resolved" else "analysis-done-bad"
                st.markdown(
                    f'<div class="analysis-header {css}">'
                    f'<span style="font-size:20px">{icon}</span>'
                    f'<div><div class="analysis-title">Sentinel: {label}</div>'
                    f'<div class="analysis-sub">See verdict below</div></div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div class="analysis-header analysis-done-ok">'
                    '<span style="font-size:20px">✅</span>'
                    '<div><div class="analysis-title">Analysis complete</div></div>'
                    '</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                '<div class="analysis-header analysis-idle">'
                '<span style="font-size:20px">💤</span>'
                '<div><div class="analysis-title">Sentinel idle</div>'
                '<div class="analysis-sub">Trigger a run to start analysis</div></div>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Thought stream ─────────────────────────────────────────────────
        if not thoughts and not running:
            return

        shown = thoughts[-MAX_STREAM_DISPLAY:]
        if len(thoughts) > MAX_STREAM_DISPLAY:
            st.caption(f"Showing last {MAX_STREAM_DISPLAY} of {len(thoughts)} steps")

        with st.container(height=380, border=False):
            html = "".join(thought_html(t) for t in shown)
            if running and phase:
                silence = int(time.time() - (last_ts or time.time()))
                wait_msg = f"⟳ waiting for Claude API response… ({silence}s)" if silence >= 3 else "⟳ reasoning…"
                color = AGENT_COLOR.get(phase, "#aaa")
                html += (
                    f'<div class="thought-block wait">'
                    f'<span class="agent-label" style="color:{color}">[{phase}]</span> '
                    f'{wait_msg}</div>'
                )
            st.markdown(html, unsafe_allow_html=True)


# ── Verdict section (full width, shown after completion) ──────────────────
st.divider()
verdict_placeholder = st.empty()


def render_verdict():
    verdict = st.session_state.verdict
    if not verdict:
        return

    status  = verdict.get("resolution_status", "unknown")
    summary = verdict.get("summary", "")
    action  = verdict.get("action_required", "")
    root    = verdict.get("root_cause", "")
    steps   = verdict.get("remediation_steps", [])
    ttr     = verdict.get("ttr", "")

    css  = "verdict-resolved" if status == "resolved" else (
           "verdict-escalated" if status == "escalated" else "verdict-cancelled")
    icon = "✅" if status == "resolved" else ("🟣" if status == "escalated" else "⏹")

    with verdict_placeholder.container():
        st.subheader(f"Sentinel Verdict")
        col_v1, col_v2 = st.columns([3, 2])
        with col_v1:
            st.markdown(
                f'<div class="verdict-card {css}">'
                f'<div class="verdict-label">{icon} {status.upper()}</div>'
                f'<div class="verdict-body">{summary}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if action:
                st.warning(f"**Action required:** {action}", icon="⚠️")
        with col_v2:
            if root:
                st.markdown("**Root cause**")
                st.caption(root)
            if steps:
                st.markdown("**Remediation attempts**")
                for s in steps:
                    ok  = s.get("result", {}).get("fix_successful", False)
                    ico = "✓" if ok else "✗"
                    st.caption(f"{ico} Attempt {s['attempt']}: `{s['strategy']}`")
            if ttr:
                st.caption(f"Time to resolution: {ttr}")

            # Blast radius & pattern memory badges (Prompts 4 & 5)
            blast  = verdict.get("blast_radius")
            pmatch = verdict.get("pattern_matched", False)
            badges = []
            if blast:
                color = {"LOW": "#2ecc71", "MEDIUM": "#e67e22", "HIGH": "#e74c3c"}.get(blast, "#aaa")
                badges.append(f'<span style="background:rgba(150,150,150,0.1);border:1px solid {color};'
                               f'color:{color};padding:2px 8px;border-radius:10px;font-size:11px;'
                               f'font-weight:700">Blast: {blast}</span>')
            if pmatch:
                badges.append('<span style="background:rgba(74,144,217,0.1);border:1px solid #4a90d9;'
                               'color:#4a90d9;padding:2px 8px;border-radius:10px;font-size:11px;'
                               'font-weight:700">⚡ Pattern-matched</span>')
            if badges:
                st.markdown(" &nbsp; ".join(badges), unsafe_allow_html=True)


# ── Incident History ──────────────────────────────────────────────────────
st.divider()
st.subheader("📋 Incident History")
history_placeholder = st.empty()


def _load_verdict_from_db(run_id: str):
    """Pull the latest incident for this run and populate st.session_state.verdict."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT resolution_status, root_cause, remediation_steps, explanation,
               retry_attempts, blast_radius, patterns_consulted
        FROM incidents WHERE run_id = ?
        ORDER BY created_at DESC LIMIT 1
    """, (run_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return
    status, root, steps_json, expl_json, retries, blast, patterns_json = row
    try:
        expl     = json.loads(expl_json)    if expl_json    else {}
        steps    = json.loads(steps_json)   if steps_json   else []
        patterns = json.loads(patterns_json) if patterns_json else []
    except Exception:
        expl     = {}
        steps    = []
        patterns = []

    # Infer pattern_matched: any pattern was consulted AND diagnosis was fast-pathed.
    # We detect this from whether the first pattern had occurrence_count >= 3.
    pattern_matched = any(
        p.get("count", 0) >= 3 and p.get("rate", 0) >= 0.9
        for p in patterns
    ) if patterns else False

    st.session_state.verdict = {
        "resolution_status":  status,
        "root_cause":         root or "",
        "remediation_steps":  steps if isinstance(steps, list) else [],
        "summary":            expl.get("summary", ""),
        "action_required":    expl.get("action_required", ""),
        "ttr":                expl.get("time_to_resolution", ""),
        "blast_radius":       blast,
        "pattern_matched":    pattern_matched,
    }


def render_history():
    conn = get_connection()
    c    = conn.cursor()
    c.execute("""
        SELECT run_id, dag_id, failure_type, resolution_status,
               retry_attempts, explanation, thought_log, created_at
        FROM incidents ORDER BY created_at DESC LIMIT 20
    """)
    rows = c.fetchall()
    conn.close()

    with history_placeholder.container():
        if not rows:
            st.info("No incidents yet.")
            return

        for run_id, dag, ftype, status, retries, expl_json, log_json, ts in rows:
            try:
                expl    = json.loads(expl_json) if expl_json else {}
                title   = expl.get("title") or (ftype or "unknown").replace("_", " ").title()
                summary = expl.get("summary", "")
                action  = expl.get("action_required")
                sev     = expl.get("severity", "")
            except Exception:
                title   = ftype or "unknown"
                summary = ""
                action  = None
                sev     = ""

            resolved  = status == "resolved"
            escalated = status == "escalated"
            icon = "✅" if resolved else ("🟣" if escalated else "🚨")
            label = f"{icon} {title}" + (f"  —  {sev}" if sev else "")

            with st.expander(label, expanded=False):
                cols = st.columns([2, 1, 1, 1])
                cols[0].markdown(f"**DAG:** `{dag}`")
                cols[1].markdown(f"**Status:** `{status}`")
                cols[2].markdown(f"**Retries:** {retries}")
                cols[3].markdown(f"`{str(ts)[:16]}`")

                if summary:
                    st.markdown(summary)
                if action:
                    st.warning(f"**Action required:** {action}", icon="⚠️")

                try:
                    thoughts = json.loads(log_json) if log_json else []
                except Exception:
                    thoughts = []

                if thoughts:
                    with st.expander(f"🔍 Replay reasoning ({len(thoughts)} steps)"):
                        with st.container(height=300, border=False):
                            st.markdown(
                                "".join(thought_html(t) for t in thoughts),
                                unsafe_allow_html=True,
                            )


# ── Run triggers ──────────────────────────────────────────────────────────
if run_btn and not st.session_state.running:
    sim      = PipelineSimulator()
    injector = FailureInjector(sim)
    run_id   = sim.start_run()
    if healthy_run:
        injector.simulate_healthy_run(run_id)
    else:
        ft = None if selected_failure == "random" else selected_failure
        injector.inject(run_id, failure_type=ft)
    _start_sentinel_thread(run_id)
    st.rerun()


if af_run_btn and not st.session_state.running:
    dag_id, dag_run_id = st.session_state.af_selected_run
    af = AirflowConnector()
    with st.spinner(f"Syncing `{dag_run_id}` from Airflow…"):
        try:
            run_id = af.sync_run_to_db(dag_id, dag_run_id)
            st.session_state.af_failed_runs = [
                r for r in st.session_state.af_failed_runs if r != (dag_id, dag_run_id)
            ]
            st.session_state.af_selected_run = None
        except Exception as e:
            st.session_state.run_error = f"Airflow sync failed: {e}"
            run_id = None
    if run_id:
        _start_sentinel_thread(run_id)
        st.rerun()


# ── Render ────────────────────────────────────────────────────────────────
run_state = None
if st.session_state.current_run_id:
    sim       = PipelineSimulator()
    run_state = sim.get_run_state(st.session_state.current_run_id)

# Load verdict once agent finishes (running just flipped to False and verdict not yet loaded)
if (not st.session_state.running
        and st.session_state.phases_done
        and st.session_state.verdict is None
        and st.session_state.current_run_id):
    _load_verdict_from_db(st.session_state.current_run_id)

render_run_header(run_state)
render_tasks(run_state)
render_analysis()
render_verdict()
render_history()

# Live-update poll while agent thread is active
if st.session_state.running:
    time.sleep(0.5)
    st.rerun()
