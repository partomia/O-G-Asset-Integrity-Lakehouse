"""
CAI Application "OGX Integrity Workbench" (Streamlit), copied from CXR's worklist app and
pointed at the lakehouse instead of local files. Everything is read from CDW Impala
(rsingh_ogx_semantic and gold), so the app shows the same numbers as the dashboards.

  - Worklist: assets ranked by integrity risk exposure for a business date, with the engineer's
    review captured (agree / downgrade / escalate) for the outcomes table
  - Asset 360: one golden asset across ERP, inspections, drawings, drone video, sensors and
    work orders, and every source name that resolved to it
  - Sensors: stuck sensors, unit changes and breaches from the streaming pipeline
  - Data quality: reconciliation per layer and the KPI consistency check

Needs OGX_IMPALA_USER / OGX_IMPALA_PASSWORD (the CAI project environment).
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

st.set_page_config(page_title="OGX Integrity Workbench - Cloudera", layout="wide")
CFG = json.loads((ROOT / "config" / "lakehouse.json").read_text())
P = os.environ.get("OGX_DB_PREFIX", CFG.get("db_prefix", "rsingh_ogx"))
SEM, GOLD, ASSET = f"{P}_semantic", f"{P}_gold", f"{P}_asset"
BAND_COLOR = {"HIGH": "#FF550C", "MEDIUM": "#FE8756", "LOW": "#A8AFB9"}


@st.cache_resource
def connection():
    from impala.dbapi import connect

    imp = CFG["impala"]
    return connect(host=os.environ.get("OGX_IMPALA_HOST", imp["host"]), port=int(imp["port"]), use_ssl=True,
                   use_http_transport=True, http_path=imp["http_path"], auth_mechanism=imp["auth_mechanism"],
                   user=os.environ["OGX_IMPALA_USER"], password=os.environ["OGX_IMPALA_PASSWORD"])


@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    cur = connection().cursor()
    try:
        cur.execute(sql)
        cols = [d[0].split(".")[-1] for d in cur.description or []]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def lit(s) -> str:
    return "'" + str(s).replace("'", "") + "'"


if not os.environ.get("OGX_IMPALA_USER") or not os.environ.get("OGX_IMPALA_PASSWORD"):
    st.title("OGX Integrity Workbench")
    st.error("Set OGX_IMPALA_USER and OGX_IMPALA_PASSWORD in the CAI project environment.")
    st.stop()

dates = q(f"SELECT DISTINCT business_date FROM {SEM}.mis_risk_by_facility ORDER BY business_date DESC")
if dates.empty:
    st.title("OGX Integrity Workbench")
    st.info("No gold data yet: run the pipeline (CDE rsingh-ogx-orchestration) first.")
    st.stop()

st.title("Asset Integrity Workbench")
st.caption("Decision support only: every asset on the worklist is reviewed by an integrity engineer. "
           "Figures come from the certified KPI views in CDW, the same ones behind the dashboards.")
d = st.sidebar.selectbox("Business date", [str(x)[:10] for x in dates["business_date"]])
on = f"business_date = DATE {lit(d)}"

tab_wl, tab_360, tab_sens, tab_dq = st.tabs(["Worklist", "Asset 360", "Sensors", "Data quality"])

# ------------------------------------------------------------------ worklist
with tab_wl:
    fac = q(f"SELECT SUM(exposure) exposure, SUM(high_risk_assets) high, SUM(abstained_assets) abstained, "
            f"SUM(assets) assets FROM {SEM}.mis_risk_by_facility WHERE {on}").iloc[0]
    cov = q(f"SELECT SUM(covered) c, SUM(received) r FROM {SEM}.mis_coverage_by_format WHERE {on}").iloc[0]
    ttr = q(f"SELECT worklist_order, avg_hours FROM {SEM}.mis_time_to_review WHERE {on}")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Integrity risk exposure", f"{float(fac.exposure or 0):.2f}")
    c2.metric("High-risk assets", int(fac.high or 0), help=f"of {int(fac.assets or 0)} active assets")
    c3.metric("Abstained (stuck sensor)", int(fac.abstained or 0))
    c4.metric("Unstructured coverage", f"{(float(cov.c or 0) / float(cov.r or 1)):.1%}")
    if len(ttr) == 2:
        h = dict(zip(ttr.worklist_order, ttr.avg_hours.astype(float)))
        c5.metric("Severe-defect hours to review", f"{h['risk']:.1f} h", f"{h['risk'] - h['calendar']:+.1f} h vs calendar",
                  delta_color="inverse")
    wl = q(f"SELECT risk_rank, tag, asset_id, asset_class, facility_id, criticality, risk_band, wall_loss_pct, "
           f"breach_windows_3d, corrective_wo_30d, days_overdue, rule_score, exposure, abstain_reason "
           f"FROM {SEM}.mis_risk_worklist WHERE {on} AND risk_rank <= 50 ORDER BY risk_rank")
    st.subheader("Risk-ranked worklist")
    st.dataframe(wl.style.map(lambda b: f"color: {BAND_COLOR.get(b, '')}", subset=["risk_band"]),
                 hide_index=True, use_container_width=True, height=420)
    left, right = st.columns([2, 3])
    with left:
        pick = st.selectbox("Review asset", wl.tag.tolist())
        verdict = st.radio("Engineer review", ["Agree", "Downgrade: no action", "Escalate: inspect now"],
                           horizontal=True, key=f"rv-{d}-{pick}")
        note = st.text_input("Note", key=f"note-{d}-{pick}")
        if st.button("Save review", type="primary"):
            row = wl[wl.tag == pick].iloc[0]
            fb = ROOT / "outputs" / "feedback" / "review_outcome.csv"
            fb.parent.mkdir(parents=True, exist_ok=True)
            new = not fb.exists()
            with open(fb, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["ts_utc", "business_date", "asset_id", "tag", "risk_rank", "risk_band", "rule_score",
                                "review", "note"])
                w.writerow([datetime.now(timezone.utc).isoformat(), d, row.asset_id, pick, int(row.risk_rank),
                            row.risk_band, float(row.rule_score), verdict, note])
            st.success("Saved: feeds fact_review_outcome and the next labelled batch")
    with right:
        st.caption("Why this rank: the latest inspection and its trend, sensor breaches over 3 days, "
                   "corrective work orders over 30 days, and how overdue the next inspection is.")
        if pick:
            st.dataframe(wl[wl.tag == pick].T.rename(columns=lambda _: "value"), use_container_width=True)

# ------------------------------------------------------------------ asset 360
with tab_360:
    tags = q(f"SELECT tag, asset_id FROM {GOLD}.dim_asset WHERE is_current ORDER BY tag")
    tag = st.selectbox("Asset", tags.tag.tolist(), index=max(0, tags.tag.tolist().index("PL-03-SEG-027"))
                       if "PL-03-SEG-027" in tags.tag.tolist() else 0)
    aid = tags[tags.tag == tag].asset_id.iloc[0]
    a = q(f"SELECT * FROM {GOLD}.dim_asset WHERE asset_id = {lit(aid)} ORDER BY version")
    cur = a[a.is_current.astype(bool)].iloc[-1]
    st.markdown(f"### {tag}  ·  {cur.asset_class}  ·  {cur.facility_id}  ·  criticality {cur.criticality}")
    st.caption(f"{cur.description}  ·  {cur.material}  ·  design {cur.design_pressure_psi} psi  ·  "
               f"installed {str(cur.install_date)[:10]}  ·  golden id {aid}  ·  version {cur.version}")
    if len(a) > 1:
        st.markdown("**History (SCD2)**")
        st.dataframe(a[["version", "valid_from", "valid_to", "status", "criticality", "last_inspection_date",
                        "attr_source"]], hide_index=True, use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Source names resolved to this asset**")
        st.dataframe(q(f"SELECT src_system, src_name, rule, score FROM {ASSET}.asset_xref WHERE asset_id = {lit(aid)} "
                       f"AND as_of_date = DATE {lit(d)} ORDER BY src_system, src_name"),
                     hide_index=True, use_container_width=True, height=260)
        st.markdown("**Inspections**")
        st.dataframe(q(f"SELECT business_date, report_no, method, wall_loss_pct, min_measured_mm, corrosion_type, cui, "
                       f"recommended_action, extract_method FROM {GOLD}.fact_inspection WHERE asset_id = {lit(aid)} "
                       f"AND business_date <= DATE {lit(d)} ORDER BY business_date DESC"),
                     hide_index=True, use_container_width=True)
    with c2:
        st.markdown("**Documents, drawings and drone video**")
        st.dataframe(q(f"SELECT business_date, source, format, file_name, resolved_by FROM {GOLD}.fact_document "
                       f"WHERE asset_id = {lit(aid)} AND business_date <= DATE {lit(d)} ORDER BY business_date DESC"),
                     hide_index=True, use_container_width=True, height=260)
        st.markdown("**Work orders**")
        st.dataframe(q(f"SELECT business_date, aufnr, auart, priority, status, short_text, cost_usd "
                       f"FROM {GOLD}.fact_work_order WHERE asset_id = {lit(aid)} AND business_date <= DATE {lit(d)} "
                       f"ORDER BY business_date DESC"), hide_index=True, use_container_width=True)
    s = q(f"SELECT business_date, sensor_tag, measurement, mean_value, max_value, breach_windows, stuck_windows, "
          f"bar_readings FROM {GOLD}.fact_sensor_daily WHERE asset_id = {lit(aid)} ORDER BY business_date")
    if not s.empty:
        st.markdown("**Sensors (daily, from the stream)**")
        st.line_chart(s.pivot_table(index="business_date", columns="sensor_tag", values="mean_value"))
        st.dataframe(s, hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ sensors
with tab_sens:
    sd = q(f"SELECT sensor_tag, measurement, tag, asset_class, facility_id, readings, mean_value, max_value, "
           f"breach_windows, stuck_windows, late_readings, bar_readings FROM {SEM}.dash_sensor_daily WHERE {on}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sensors reporting", len(sd))
    c2.metric("Readings", f"{int(sd.readings.sum()):,}")
    c3.metric("Stuck sensors", int((sd.stuck_windows > 0).sum()))
    c4.metric("Unit changes (bar)", int((sd.bar_readings > 0).sum()))
    st.markdown("**Needs attention**: stuck, unit change or breaching")
    st.dataframe(sd[(sd.stuck_windows > 0) | (sd.bar_readings > 0) | (sd.breach_windows > 0)]
                 .sort_values(["stuck_windows", "bar_readings", "breach_windows"], ascending=False),
                 hide_index=True, use_container_width=True)
    st.markdown("**All sensors**")
    st.dataframe(sd, hide_index=True, use_container_width=True, height=360)

# ------------------------------------------------------------------ data quality
with tab_dq:
    r = q(f"SELECT layer_label, entity, check_name, status, expected, actual, difference, detail "
          f"FROM {SEM}.dash_recon WHERE {on} ORDER BY layer_label, entity, check_name")
    counts = r.status.value_counts()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Checks", len(r))
    c2.metric("Matched", int(counts.get("MATCHED", 0)))
    c3.metric("Explained", int(counts.get("EXPLAINED", 0)))
    c4.metric("Late", int(counts.get("LATE", 0)))
    c5.metric("Mismatch", int(counts.get("MISMATCH", 0)))
    st.dataframe(r[r.status != "MATCHED"], hide_index=True, use_container_width=True)
    with st.expander("Every check"):
        st.dataframe(r, hide_index=True, use_container_width=True)
