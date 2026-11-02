"""
CAI Application "OGX Agentic Integrity Monitor" (Streamlit): the pattern of the Cloudera AMP
"Agentic Oil and Gas Well Monitoring" on this lakehouse instead of a seeded SQLite file.

  - Agentic analysis: pick a facility and business date; the anomaly pre-screen runs first, then
    three agents on Cloudera AI Inference stream their reports and draft work tickets
  - Work tickets: drafts waiting for an integrity engineer, approved or rejected (append-only)
  - History & documents: past analyses and inspection report chunks, ranked for a question
  - LLM & guardrails: the endpoint, token use and the LLM guardrails that fired

Separate from the OGX Integrity Workbench (app/): same lakehouse, its own application.
Needs OGX_IMPALA_USER / OGX_IMPALA_PASSWORD; the CAII endpoint comes from config/inference.json
or OGX_CAII_BASE_URL / OGX_CAII_MODEL, the token from /tmp/jwt.
"""

import os
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from agents import history, records, runner  # noqa: E402
from agents.context import FACILITIES, P  # noqa: E402
from agents.llm import CAIIChat  # noqa: E402
from lakehouse.store import ImpalaStore  # noqa: E402

st.set_page_config(page_title="OGX Agentic Integrity Monitor - Cloudera", layout="wide")
PRIO_COLOR = {"High": "#FF550C", "Medium": "#FE8756", "Low": "#A8AFB9"}
_T = threading.local()


def store(fresh: bool = False) -> ImpalaStore:
    """One Impala connection per Streamlit script thread, replaced when Impala closed it."""
    if fresh or getattr(_T, "s", None) is None:
        _T.s = ImpalaStore()
    return _T.s


def query(sql: str) -> list[dict]:
    try:
        return store().query(sql)
    except Exception:  # noqa: BLE001  stale session: reconnect once
        return store(fresh=True).query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    return pd.DataFrame(query(sql))


def q_opt(sql: str) -> pd.DataFrame:
    """A table this app creates on its first analysis: empty until then."""
    try:
        return pd.DataFrame(query(sql))
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_resource
def llm() -> CAIIChat:
    return CAIIChat()


@st.cache_data(ttl=60, show_spinner=False)
def llm_health() -> dict:
    return llm().health() if llm().configured else {"ok": False, "error": "endpoint not configured"}


if not os.environ.get("OGX_IMPALA_USER") or not os.environ.get("OGX_IMPALA_PASSWORD"):
    st.title("OGX Agentic Integrity Monitor")
    st.error("Set OGX_IMPALA_USER and OGX_IMPALA_PASSWORD in the CAI project environment.")
    st.stop()

dates = q(f"SELECT DISTINCT business_date FROM {P}_semantic.mis_risk_worklist ORDER BY business_date DESC")
if dates.empty:
    st.title("OGX Agentic Integrity Monitor")
    st.info("No gold data yet: run the pipeline (CDE rsingh-ogx-orchestration) first.")
    st.stop()

st.title("Agentic Integrity Monitor")
st.caption("Three agents on Cloudera AI Inference read the lakehouse (certified worklist, streaming sensor windows, "
           "inspection reports, work orders) and draft work tickets. Decision support only: every ticket is a draft "
           "until an integrity engineer approves it.")

with st.sidebar:
    d = st.selectbox("Business date", [str(x)[:10] for x in dates["business_date"]])
    fac = st.selectbox("Facility", list(FACILITIES), format_func=lambda f: f"{f} - {FACILITIES[f]}")
    tags = q(f"SELECT tag FROM {P}_semantic.mis_risk_worklist WHERE business_date = DATE '{d}' "
             f"AND facility_id = '{fac}' ORDER BY risk_rank")
    asset = st.selectbox("Asset (optional)", ["All assets"] + tags.get("tag", pd.Series(dtype=str)).tolist())
    depth = st.radio("Depth", ["quick", "deep"], horizontal=True,
                     help="quick: no LLM call when the pre-screen finds nothing; deep: always run the agents")
    st.divider()
    h = llm_health()
    ep = llm()
    if h.get("ok"):
        st.success(f"CAII: {ep.model} ({h['latency_s']} s)")
    else:
        st.warning(f"CAII unavailable: rule-based fallback. {h.get('error', '')[:160]}")

tab_run, tab_tk, tab_hist, tab_llm = st.tabs(["Agentic analysis", "Work tickets", "History & documents",
                                             "LLM & guardrails"])

# ------------------------------------------------------------------ agentic analysis
with tab_run:
    go = st.button("Run analysis", type="primary")
    if go:
        scope_asset = None if asset == "All assets" else asset
        boxes: dict[str, dict] = {}
        done = None
        with st.status("Pre-screen: reading the lakehouse ...", expanded=True) as status:
            for ev in runner.run(query, fac, d, scope_asset, depth, llm()):
                t = ev["type"]
                if t == "prescreen":
                    status.update(label=f"Pre-screen: {ev['windows']} sensor windows, {ev['assets']} assets")
                    st.code(ev["block"], language=None)
                elif t == "short_circuit":
                    st.success(ev["markdown"])
                elif t == "stage_started":
                    status.update(label=f"{ev['title']} ({ev['agent']}) ...")
                    c = st.container(border=True)
                    c.markdown(f"**{ev['title']}**: {ev['agent']} "
                               f"`{'Cloudera AI Inference' if ev['provider'] == 'caii' else 'rule-based'}`")
                    boxes[ev["stage_id"]] = {"ph": c.empty(), "text": "", "t": 0.0, "c": c}
                elif t == "delta":
                    b = boxes[ev["stage_id"]]
                    b["text"] += ev["text"]
                    if time.monotonic() - b["t"] > 0.15:
                        b["ph"].markdown(b["text"] + " ▌")
                        b["t"] = time.monotonic()
                elif t == "stage_completed":
                    b = boxes[ev["stage_id"]]
                    b["ph"].markdown(ev["markdown"])
                elif t == "guardrail":
                    target = boxes.get(ev["stage_id"], {}).get("c", st)
                    target.warning(f"Guardrail `{ev['guardrail']}`: {ev['reason']}")
                elif t == "done":
                    done = ev
            status.update(label="Analysis complete", state="complete", expanded=True)
        if done:
            if done["tickets"]:
                st.subheader(f"{len(done['tickets'])} draft tickets")
                st.dataframe(pd.DataFrame([t.as_dict() for t in done["tickets"]]), hide_index=True,
                             use_container_width=True)
            if done["dropped"]:
                st.caption(f"{len(done['dropped'])} tickets dropped by the scope guardrail.")
            u = done["usage"]
            st.caption(f"Analysis {done['analysis_id']} | {done['provider']} | {u.total_tokens} tokens "
                       f"({u.prompt_tokens} prompt, {u.completion_tokens} completion) | {done['latency_s']} s")
            if done["stages"]:
                with st.spinner("Saving the analysis and the draft tickets to the lakehouse ..."):
                    n = records.publish(done, store())
                st.success(f"Saved: {n.get('agent_ticket', 0)} tickets to {P}_ref.agent_ticket, "
                           f"{n.get('guardrail_event', 0)} guardrail events; review them under Work tickets.")
    else:
        st.info("Pick a facility and a business date, then run the analysis. The planted faults show up here: "
                "the stuck vibration sensor VI-112B and the psi-to-bar switch of PI-105A (REF1, 10-05 / 10-06), "
                "the wall-loss growth on PL-03-SEG-027 (PL03).")

# ------------------------------------------------------------------ work tickets
with tab_tk:
    status_pick = st.radio("Status", ["DRAFT", "APPROVED", "REJECTED", "All"], horizontal=True)
    where = "TRUE" if status_pick == "All" else f"COALESCE(r.decision, 'DRAFT') = '{status_pick}'"
    tk = q_opt(records.tickets_sql(P, where))
    if tk.empty:
        st.info("No tickets yet: run an analysis first.")
    else:
        st.dataframe(tk.drop(columns=["analysis_id"]).style.map(lambda p: f"color: {PRIO_COLOR.get(p, '')}",
                                                                subset=["priority"]),
                     hide_index=True, use_container_width=True, height=380)
        drafts = tk[tk.status == "DRAFT"]
        if not drafts.empty:
            st.subheader("Review a draft")
            pick = st.selectbox("Ticket", drafts.ticket_id.tolist(),
                                format_func=lambda i: f"{i}  {drafts.set_index('ticket_id').loc[i, 'tag']}  "
                                                      f"{drafts.set_index('ticket_id').loc[i, 'ticket_type']}")
            row = drafts.set_index("ticket_id").loc[pick]
            st.markdown(f"**{row.tag}**, {row.ticket_type}, priority {row.priority}, due {str(row.due_date)[:10]}\n\n"
                        f"{row.description}\n\n_Evidence:_ {row.evidence}")
            if row.guardrail_flags:
                st.warning(f"Guardrail: {row.guardrail_flags}")
            c1, c2, c3 = st.columns([2, 3, 2])
            reviewer = c1.text_input("Reviewer", os.environ.get("OGX_REVIEWER", ""))
            note = c2.text_input("Note")
            decision = c3.radio("Decision", ["APPROVED", "REJECTED"], horizontal=True)
            if st.button("Save decision", type="primary", disabled=not reviewer.strip()):
                records.review(pick, decision, reviewer.strip(), note, store())
                q.clear()
                st.success(f"{pick} {decision.lower()} by {reviewer}. The planner raises the SAP work order; "
                           "nothing is created automatically.")

# ------------------------------------------------------------------ history
with tab_hist:
    question = st.text_input("Search past analyses and inspection reports",
                             placeholder="e.g. pitting CUI PL-03-SEG-027, stuck vibration, wall thinning tank")
    if question:
        chunks = q(f"SELECT file_name, page_no, chunk_no, text FROM {P}_silver.doc_chunk")
        past = q_opt(f"SELECT analysis_id, recorded_at, facility_id, business_date, markdown "
                     f"FROM {P}_ref.agent_analysis ORDER BY recorded_at DESC LIMIT 300")
        docs = [{"kind": "report", "ref": f"{r.file_name} p.{r.page_no}", "text": r.text} for r in chunks.itertuples()]
        docs += [{"kind": "analysis", "ref": f"{r.analysis_id} ({r.facility_id}, {str(r.business_date)[:10]})",
                  "text": r.markdown} for r in past.itertuples()] if not past.empty else []
        hits = history.Index(docs).search(question, k=10)
        if not hits:
            st.info("Nothing matches.")
        for hit in hits:
            st.markdown(f"**{hit.ref}** `{hit.kind}` score {hit.score:.2f}\n\n> {history.snippet(hit.text, question)}")

# ------------------------------------------------------------------ llm & guardrails
with tab_llm:
    c1, c2, c3 = st.columns(3)
    c1.metric("Endpoint", "up" if h.get("ok") else "down")
    c2.metric("Model", ep.model or "not configured")
    c3.metric("Round trip", f"{h.get('latency_s', 0)} s")
    st.caption(f"Base URL: {ep.base_url or 'not set'}  |  circuit {'OPEN' if ep.breaker_open else 'closed'}")
    an = q_opt(f"SELECT recorded_at, analysis_id, facility_id, business_date, depth, provider, model, total_tokens, "
               f"latency_s, n_signals, n_tickets, n_guardrail_events FROM {P}_ref.agent_analysis "
               f"ORDER BY recorded_at DESC LIMIT 100")
    if not an.empty:
        st.subheader("Analyses")
        k1, k2, k3 = st.columns(3)
        k1.metric("Analyses", len(an))
        k2.metric("Tokens", int(an.total_tokens.fillna(0).sum()))
        k3.metric("Rule-based fallbacks", int((~an.provider.str.contains("caii")).sum()))
        st.dataframe(an, hide_index=True, use_container_width=True)
    ge = q_opt(f"SELECT recorded_at, guardrail, layer, subject, value, limit_rule, reason FROM {P}_ref.guardrail_event "
               f"WHERE source = 'agentic-monitor' ORDER BY recorded_at DESC LIMIT 200")
    st.subheader("LLM guardrails")
    st.caption("Each stage's tags, numbers and report citations are checked against the context it was given; "
               "tickets may only suggest (shut in, shut down, closing work orders or changing set points are "
               "flagged); out-of-scope tickets are dropped. Policy: config/guardrails.yaml `llm`.")
    if ge.empty:
        st.info("No LLM guardrail has fired yet.")
    else:
        st.dataframe(ge.groupby("guardrail").size().rename("events").reset_index(), hide_index=True)
        st.dataframe(ge, hide_index=True, use_container_width=True)
