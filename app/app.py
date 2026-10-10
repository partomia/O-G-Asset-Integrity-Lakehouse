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
  - Models & guardrails: AI Registry versions and the model's life (ref.model_event), the KPI
    gate, guardrail events, drift and the retraining loop (MLOps on Cloudera AI)

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


@st.cache_resource
def corrosion_model():
    """The champion the endpoint serves (models/champion, installed by job ogx-04), loaded in process."""
    if not (ROOT / "models" / "champion" / "model.joblib").exists():
        return None
    import serve.predict as p

    return p


def q_opt(sql: str) -> pd.DataFrame:
    """A query on a table the CAI jobs create on their first run: empty until then."""
    try:
        return q(sql)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


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

tab_wl, tab_360, tab_sens, tab_dq, tab_ml = st.tabs(["Worklist", "Asset 360", "Sensors", "Data quality",
                                                     "Models & guardrails"])

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
            st.dataframe(wl[wl.tag == pick].T.astype(str).rename(columns=lambda _: "value"), use_container_width=True)

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
        st.dataframe(q(f"SELECT business_date, report_no, `method`, wall_loss_pct, min_measured_mm, corrosion_type, cui, "
                       f"recommended_action, extract_method FROM {GOLD}.fact_inspection WHERE asset_id = {lit(aid)} "
                       f"AND business_date <= DATE {lit(d)} ORDER BY business_date DESC"),
                     hide_index=True, use_container_width=True)
    with c2:
        st.markdown("**Documents, drawings and drone video**")
        st.dataframe(q(f"SELECT business_date, source, `format`, file_name, resolved_by FROM {GOLD}.fact_document "
                       f"WHERE asset_id = {lit(aid)} AND business_date <= DATE {lit(d)} ORDER BY business_date DESC"),
                     hide_index=True, use_container_width=True, height=260)
        st.markdown("**Work orders**")
        st.dataframe(q(f"SELECT business_date, aufnr, auart, priority, status, short_text, cost_usd "
                       f"FROM {GOLD}.fact_work_order WHERE asset_id = {lit(aid)} AND business_date <= DATE {lit(d)} "
                       f"ORDER BY business_date DESC"), hide_index=True, use_container_width=True)
    kf = q(f"SELECT k.business_date, k.video_id, k.frame_index, k.t_seconds, k.frame_sha256 "
           f"FROM {P}_silver.video_keyframe k WHERE k.asset_hint = {lit(tag)} AND k.business_date <= DATE {lit(d)} "
           f"ORDER BY k.business_date DESC, k.video_id, k.frame_index LIMIT 12")
    if not kf.empty:
        st.markdown("**Drone keyframes, scored by the corrosion champion behind its guardrails**")
        st.caption("Input guardrails (frame quality, out of distribution) give band NA: the frame is reviewed in "
                   "calendar order, as without AI. A score near the threshold is UNCERTAIN; a criticality-A asset "
                   "never drops below P2. The model ranks and suggests; an engineer decides.")
        eng = corrosion_model()
        if eng is None:
            st.info("No corrosion champion yet: run the CAI chain ogx-01 to ogx-04.")
        cols = st.columns(4)
        for i, r in enumerate(kf.itertuples()):
            res = eng.predict({"sha256": r.frame_sha256, "criticality": cur.criticality}) if eng else {}
            path = ROOT / "assets" / "frames" / f"{r.frame_sha256[:16]}.jpg"
            with cols[i % 4]:
                if path.exists():
                    st.image(str(path), use_container_width=True)
                band = res.get("band", "")
                sev = res.get("probabilities", {}).get("severe")
                st.caption(f"{r.video_id} t={r.t_seconds:.0f}s · **{band}** {res.get('severity') or ''}"
                           + (f" (severe {sev:.2f})" if sev is not None else "")
                           + (f" · {res['band_reason']}" if res.get("band_reason") else ""))
                with st.expander("Guardrails and review"):
                    for c in res.get("guardrails", []):
                        st.write(f"{'✅' if c['passed'] else '⛔'} {c['guardrail']}: {c['value']} ({c['limit']})")
                    for e in res.get("guardrail_events", []):
                        if e["guardrail"] in ("abstain_band", "safety_floor"):
                            st.write(f"⚠️ {e['guardrail']}: {e['reason']}")
                    lab = st.radio("Engineer label", ["none", "surface", "severe"], horizontal=True,
                                   index=["none", "surface", "severe"].index(res.get("severity") or "none"),
                                   key=f"lab-{tag}-{i}-{r.video_id}-{r.frame_index}")
                    if st.button("Save label", key=f"save-{tag}-{i}-{r.video_id}-{r.frame_index}"):
                        fb = ROOT / "outputs" / "feedback" / "frame_labels.csv"
                        fb.parent.mkdir(parents=True, exist_ok=True)
                        new = not fb.exists()
                        with open(fb, "a", newline="") as f:
                            w = csv.writer(f)
                            if new:
                                w.writerow(["ts_utc", "sha256", "label", "model_severity", "model_band",
                                            "model_version", "asset_tag", "video_id", "frame_index"])
                            w.writerow([datetime.now(timezone.utc).isoformat(), r.frame_sha256, lab,
                                        res.get("severity"), band, (res.get("model") or {}).get("version"),
                                        tag, r.video_id, r.frame_index])
                        st.success("Saved: a labelled row for the next retraining (ogx-08 counts it)")
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

# ------------------------------------------------------------------ models & guardrails
with tab_ml:
    meta_path = ROOT / "models" / "champion" / "model_meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else None
    st.subheader("Champion")
    if meta:
        t = meta["metrics"]["test"]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Registry", f"{meta.get('registry_name', 'ogx-corrosion')} v{meta.get('registry_version', '?')}")
        c2.metric("TEST AUROC", f"{t['auroc']:.3f}")
        c3.metric("Sensitivity", f"{t['sensitivity']:.3f}")
        c4.metric("Specificity", f"{t['specificity']:.3f}")
        c5.metric("Brier", f"{t['brier']:.3f}")
        st.caption(f"{meta.get('model_version') or meta['estimator']} · trained {str(meta.get('trained_at'))[:16]} · "
                   f"git {meta['git_sha'][:7]} · features {meta['feature_version']} ({meta['feature_hash']}) · "
                   f"threshold {meta['threshold']:.3f} · trigger: {meta.get('trigger_reason', 'manual')} · "
                   f"{meta.get('train_rows', '?')} training rows incl. {meta.get('feedback_rows', 0)} engineer labels · "
                   f"MLflow run {meta.get('mlflow_run_id') or '-'}")
        gate_path = ROOT / "models" / "champion" / "gate_result.json"
        if gate_path.exists():
            with st.expander("KPI gate of this champion"):
                st.dataframe(pd.DataFrame(json.loads(gate_path.read_text())["checks"]), hide_index=True,
                             use_container_width=True)
    else:
        st.info("No champion yet: run the CAI chain ogx-01 to ogx-04.")

    st.subheader("Model versions and lifecycle (Cloudera AI Registry, MLflow, ref.model_event)")
    ev = q_opt(f"SELECT recorded_at, event, stage, model_version, registry_name, registry_version, test_auroc, "
               f"test_sensitivity, test_specificity, gate_passed, feedback_rows, trigger_reason, detail "
               f"FROM {P}_ref.model_event ORDER BY recorded_at DESC LIMIT 200")
    if ev.empty:
        st.info("No model events yet: the next CAI chain run (or ogx-08-retrain-trigger) writes them.")
    else:
        reg = ev[ev.event == "REGISTERED"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registered versions", int(reg.registry_version.notna().sum()))
        c2.metric("Trainings", int((ev.event == "TRAINED").sum()))
        c3.metric("Gate rejections", int((ev.event == "GATE_FAILED").sum()))
        c4.metric("Retrains triggered", int((ev.event == "RETRAIN_TRIGGERED").sum()))
        st.dataframe(ev, hide_index=True, use_container_width=True, height=300)

    st.subheader("MLOps loop")
    st.markdown(
        "`git push` → GitHub Actions `cai-mlops.yml` → CAI API v2 → **ogx-00** sync → **ogx-01** features "
        "(+ engineer labels) → **ogx-02** train (MLflow) → **ogx-03** KPI gate (absolute + non-regression vs "
        "champion) → **ogx-04** deploy + AI Registry version.  \n"
        "Without a push: **ogx-05-nightly-drift** (02:00) scores the lakehouse keyframes through the guardrails "
        "and measures PSI; **ogx-08-retrain-trigger** (02:30) starts ogx-01 on a drift alert, "
        "≥ 20 new engineer labels or a champion older than 30 days.")
    drift_path = ROOT / "outputs" / "monitoring" / "drift_report.json"
    fb_path = ROOT / "outputs" / "feedback" / "frame_labels.csv"
    c1, c2, c3 = st.columns(3)
    if drift_path.exists():
        dr = json.loads(drift_path.read_text())
        c1.metric("Last drift check", dr["status"], help=f"{dr['recorded_at'][:16]} · {dr['source']}")
        c2.metric("Frames scored / seen", f"{dr['scored']} / {dr['frames']}")
    else:
        c1.metric("Last drift check", "not run")
    c3.metric("Engineer frame labels", sum(1 for _ in open(fb_path)) - 1 if fb_path.exists() else 0)
    dm = q_opt(f"SELECT recorded_at, metric, value, status, n_reference, n_current, detail FROM {P}_ref.model_drift "
               f"ORDER BY recorded_at DESC, metric LIMIT 60")
    if not dm.empty:
        st.dataframe(dm, hide_index=True, use_container_width=True, height=220)

    st.subheader("Guardrails")
    import yaml

    gcfg = yaml.safe_load((ROOT / "config" / "guardrails.yaml").read_text())
    ge = q_opt(f"SELECT layer, guardrail, COUNT(*) events, MAX(recorded_at) last_seen FROM {P}_ref.guardrail_event "
               f"GROUP BY layer, guardrail ORDER BY layer, events DESC")
    left, right = st.columns([3, 2])
    with left:
        st.markdown("**Events by guardrail (ref.guardrail_event)**")
        if ge.empty:
            st.info("No guardrail events yet: ogx-05-nightly-drift writes them.")
        else:
            st.bar_chart(ge.set_index("guardrail")["events"])
            st.dataframe(ge, hide_index=True, use_container_width=True)
    with right:
        st.markdown("**Configured limits (config/guardrails.yaml)**")
        st.json({k: gcfg[k] for k in ("input", "output")}, expanded=False)
        st.json({"process": gcfg["process"], "retrain": gcfg["retrain"]}, expanded=False)
    recent = q_opt(f"SELECT recorded_at, layer, guardrail, subject, value, limit_rule, reason FROM "
                   f"{P}_ref.guardrail_event ORDER BY recorded_at DESC LIMIT 100")
    if not recent.empty:
        with st.expander("Latest guardrail events"):
            st.dataframe(recent, hide_index=True, use_container_width=True)
