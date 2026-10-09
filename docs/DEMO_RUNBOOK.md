# Demo runbook (about 25 minutes)

Links:

- Integrity Workbench (CAI): https://rsingh-ogx-workbench.federal-cml.federal.dp5i-5vkq.cloudera.site
  (CAI project `rsingh-og-asset-integrity` → Applications → "OGX Integrity Workbench"). When it is
  healthy the page title is **Asset Integrity Workbench** with tabs Worklist / Asset 360 / Sensors /
  Data quality; a page titled "OGX Integrity Workbench" with no tabs is the fallback (see the end).
- Dashboards (CDW Data Visualization): https://viz-indianbank-spend-analytics.dw-federal-cdp-env.dp5i-5vkq.cloudera.site/arc/apps/
  → Dashboards → **OGX Integrity KPIs**, **OGX Reconciliation & Data Quality**, **OGX Asset Master**, **OGX Sensor Health**
- CDE jobs: `rsingh-ogx-*` (vcluster in `federal-cdp-env`); streaming runs `rsingh-ogx-stream-bronze`, `rsingh-ogx-stream-agg`
- Hue / Impala: databases `rsingh_ogx_*`
- Repo: https://github.com/partomia/O-G-Asset-Integrity-Lakehouse

## 1. The problem (2 min)

An integrity engineer has ERP, inspection reports (some scanned), drawings, seismic, well logs,
drone video and live SCADA, each naming the same asset differently (`PL-03-SEG-027`,
`PL03SEG027`, a GPS point, a functional location). Today they review inspections in date order.

## 2. One golden asset (4 min): Workbench → Asset 360

- Pick **PL-03-SEG-027**: the source names that resolved to it and by which rule
  (exact functional location, normalised tag, GPS to pipeline segment), inspections with wall loss,
  drawings and drone video linked to it, work orders, and its sensors from the stream.
- History table: SCD2 versions in `gold.dim_asset`.
- Pick **TK-504** (or any asset with drone video): its keyframes scored by the corrosion
  champion (P1 / P2 / P3 with severe probability). CAI jobs `ogx-01` to `ogx-04` built,
  gated (TEST AUROC 0.862, sensitivity 0.93) and deployed it as model `ogx-integrity`.
- Dashboard **OGX Asset Master**: every source name resolved; the one conflict (segment named in
  one source, GPS 38 m from another) auto-resolved and logged in the review queue.

## 3. Risk-ranked worklist (5 min): Workbench → Worklist

- Top of the list: PL-03-SEG-027, PL-03-SEG-041 (planted degraders), then V-307, PL-03-SEG-052.
- **Abstained (stuck sensor) = 1**: VI-112B flat-lines from 10-05 12:00, so its asset is not
  scored on a sensor that is lying.
- **Severe-defect hours to review**: risk order against calendar order on the same inspections.
- Save a review (Agree / Downgrade / Escalate): it feeds the outcomes table.
- Dashboard **OGX Integrity KPIs**: the same three KPIs from the certified views (IRE, UNC, TTR).

## 4. Streaming (4 min): CDE + Sensor Health

- Kafka `ogx.sensor.telemetry` → `rsingh-ogx-stream-bronze` (exactly-once foreachBatch,
  per-minute control counts) → `rsingh-ogx-stream-agg` (1- and 15-minute windows, watermark, MERGE).
- 2,880,000 readings for 5 days in `bronze.sensor_reading`, nothing quarantined.
- Dashboard **OGX Sensor Health**: stuck VI-112B, PI-105A switching psi to bar (normalised),
  late readings past the watermark.
- Hue: `SELECT event_date, count(*) FROM rsingh_ogx_bronze.sensor_reading GROUP BY 1 ORDER BY 1;`

## 5. Trust: reconciliation and KPI consistency (5 min)

Dashboard **OGX Reconciliation & Data Quality** → "By business date":

| Date | What recon shows |
|---|---|
| 10-05 | corrupt inspection PDF quarantined (MISMATCH, with reason); seismic resend (EXPLAINED) |
| 10-06 | work-order trailer total does not match the rows (MISMATCH) |
| others | all MATCHED |

"Latest batch" → KPI consistency: every MIS view and dashboard figure reproduces the certified
view (9 of 9 MATCHED per date).

## 6. Governance (2 min)

- Atlas: `OGX_SENSITIVE_*` classifications on inspector names, GPS, subsurface values and
  extracted text; glossary **OGX Integrity KPIs** on the certified views.
- Ranger tag masking `rsingh-ogx-sensitive-*`: masked users see hashes / NULL.

## 7. Close (2 min)

Everything is code in a public repo (no secrets; gitleaks hook), one commit per phase, and every
live run is in `docs/PROJECT_LOG.md`.

## If something is off

- Workbench shows "OGX Integrity Workbench" with an error and no tabs: the app started without
  `OGX_IMPALA_USER` / `OGX_IMPALA_PASSWORD`. They are in the project environment, so restart the
  application (CAI → Applications → Restart, about 30 s) and reload.
- Workbench shows an Impala error: refresh; the app caches queries for 5 minutes.
- The first load of each tab takes a few seconds (Impala queries); later loads come from the cache.
- Model endpoint `ogx-integrity`: deployed, but calls currently return 400 (open item in
  `docs/PROJECT_LOG.md`). Do not call it live; the Workbench scores keyframes in-process with the
  same champion (`serve/predict.py`), so Asset 360 is unaffected.
- A dashboard is empty for the latest date: the CDE chain for that date has not finished; pick
  the previous date (all five dates 10-04 to 10-08 are loaded).
