O&G Asset Integrity Lakehouse — Build Plan
Oct 8, 2026 · @RAVI
Overview
Build a third sibling repo, O-G-Asset-Integrity-Lakehouse, that proves one claim: a lakehouse can store, index and use unstructured oil and gas data (PDFs, seismic, drawings, inspection video) beside structured tables and live sensor streams, on one governed Cloudera platform. It copies the General-DataLakehouse spine (synthetic sources, medallion on Iceberg, reconciliation, certified KPIs, governance as code) and the Chest-X-ray-Triage ML spine (one feature definition, KPI gate, AI Registry, silent trial, CI/CD to CAI, Streamlit app).
The story. A synthetic operator runs one onshore field (12 wells), a 40 km gathering pipeline in 60 segments, and one refinery unit with about 150 tagged items of equipment. Each day, inspection crews file PDF reports, drones film pipeline and flare-stack inspections, engineers revise P&ID drawings, and geoscience holds seismic surveys and well logs. Sensors stream pressure, temperature, vibration and corrosion-probe readings through Kafka.
The decision supported. Today the integrity team reviews inspections in calendar order (time-based inspection). The demo scores every new inspection frame and every asset's live condition, and moves likely-severe defects to the top of the engineer's worklist (risk-based inspection). As in CXR triage, this is workflow prioritisation, not an automated decision: an engineer still reviews every inspection, and no work order is opened or closed by the model.
Scale. About 220 assets, 5 business days, roughly 30 PDFs, 4 short drone videos, 40 sensor readings per second, 20 P&ID sheets, 2 small seismic surveys and 12 well logs. A full run takes minutes on the cluster and runs on a laptop for CI. All data is synthetic except the pinned public image set used to train the defect model.
End-to-end flow

end-to-end flow · sources to CDE, CAI, CDW and CDV
Files go through the batch medallion and the extraction step; sensor readings go through Kafka and two streaming jobs; both meet in gold, which feeds the ML chain and the semantic layer. Scores and engineer feedback are written back to gold, so dashboards and the next training run see them.
Requirements map
Every capability you asked for has one home in the repo, so the demo can be walked requirement by requirement.
#
Requirement
Where it is met
1
Unstructured data stored and indexed beside relational tables
Raw objects by reference in S3, one Iceberg catalog row per object (bronze.doc_object), format extractors to silver tables, docs/UNSTRUCTURED.md
2
PDFs, seismic, engineering drawings, inspection video
generators/ makes all four; extract/ parses each: PDF text and OCR, SEG-Y headers, P&ID tag reading, video keyframes; LAS well logs as a fifth
3
Kafka stream, data generated and produced
stream/producer/sensor_producer.py (seeded, rate-controlled) writes ogx.sensor.telemetry and ogx.scada.alarm with Schema Registry schemas
4
CDE Spark streaming from Kafka
cde/jobs/stream_telemetry_bronze.py (Kafka to Iceberg, checkpointed) and stream_telemetry_agg.py (event-time windows, watermark, MERGE)
5
CDE batch medallion
cde/jobs/land_sources.py to build_gold.py, Airflow DAG cde/dags/ogx_dag.py, reconciliation per layer
6
CDW
sql/semantic/ certified KPI views, sql/adhoc.sql, sql/unstructured_search.sql, sql/time_travel.sql on Impala
7
CDV
dataviz/build_dashboard.py: four dashboards as code
8
CAI Workbench
One CAI project: Sessions, Jobs ogx-00 to ogx-07, Experiments, Models, Application
9
CAI feature engineering
features/feature_logic.py (the only definition), build_feature_table.py (versioned table + feature hash)
10
AI Registry versioning
serve/registry.py: a version per candidate, silent trial, champion and promotion, tagged with commit, feature hash, threshold and KPIs
11
Guardrails for ML
guardrails/ and config/guardrails.yaml: input, output, process and (optional) LLM guardrails, docs/GUARDRAILS.md
12
Application interface
app/app.py Streamlit Integrity Workbench as a CAI Application
13
CI/CD
.github/workflows/ci.yml (offline chain) and cai-mlops.yml (GitHub to CAI API v2)
14
Governance
Atlas lineage across batch, stream and documents; classifications on sensitive fields; Ranger tag masking; scripts/governance.py
Platform mapping
Each layer runs on the service that owns it, all in the one CDP environment the sibling repos already use.
Layer
Service
What runs there
Landing zone and raw objects
Cloud storage (S3), written by a CDE job
landing/<source>/<business_date>/ + _manifest.json; raw binaries kept immutable under raw/<format>/
Stream transport
Kafka (Streams Messaging Data Hub), Schema Registry, SMM
Topics ogx.sensor.telemetry, ogx.scada.alarm, ogx.control; producer lag and throughput shown in SMM
Stream processing
CDE Spark Structured Streaming
Kafka to bronze.sensor_reading; windowed aggregates to silver.sensor_window; stuck-sensor and unit checks
Batch ingest, extraction, medallion
CDE Spark 3, Iceberg v2
bronze, silver, asset master, gold; unstructured extractors run inside the Spark jobs
Orchestration
CDE Airflow
One DAG run per business date; calls CAI scoring through the CAI API
Feature engineering, training, gate
CAI Workbench Jobs
ogx-01 to ogx-03, MLflow experiments, versioned feature tables
Model registry and serving
CAI AI Registry, CAI Models
Registered models per head; champion deployed as a model endpoint
Application
CAI Application
Streamlit Integrity Workbench
Semantic layer, ad-hoc, time travel
CDW Impala (Hue)
Certified KPI views, document search SQL, time travel
Dashboards
Cloudera Data Visualization
Four dashboards built as code
Lineage, classification, masking
SDX Atlas and Ranger
Lineage from Spark and Impala; tag-based masking
CI
GitHub Actions
pytest, the whole chain offline on synthetic data, the gate failing on cue
Names and conventions
The short code is ogx (oil and gas, unstructured), following the gdl and cxr pattern so nothing collides with the sibling demos.
	•	Databases: rsingh_ogx_{bronze,silver,asset,gold,semantic,ref}; prefix on the database only.
	•	Environment variables: OGX_ prefix (OGX_IMPALA_*, OGX_CAI_*, OGX_KAFKA_*, OGX_VIZ_API_KEY); .env git-ignored, .env.example committed.
	•	CDE: resource rsingh-ogx-pipeline; batch jobs rsingh-ogx-{land,bronze,extract,silver,asset,gold,outcomes,recon}; streaming jobs rsingh-ogx-stream-{bronze,agg}; DAG rsingh-ogx-orchestration.
	•	CAI jobs: ogx-00-sync-code, ogx-01-build-features, ogx-02-train-validate, ogx-03-kpi-gate, ogx-04-deploy-champion, ogx-05-nightly-drift, ogx-06-score-inspections, ogx-07-promote-champion, plus ogx-setup-data and ogx-stream-producer.
	•	Registered models: ogx-corrosion, ogx-frame-qc, ogx-equipment-risk.
	•	Kafka topics: ogx.sensor.telemetry, ogx.scada.alarm, ogx.control (per-minute control counts for stream reconciliation).
	•	Landing: s3a://federal-buk-574bcea0/data/IB/rsingh_ogx/landing/<source>/<business_date>/ (same bucket and path convention as GDL); raw objects .../rsingh_ogx/raw/<format>/<sha256>.<ext>.
	•	Canonical keys: asset_id (from the asset master), doc_id (sha256 of the object), sensor_tag, inspection_id, date_key.
	•	Data Visualization: dashboards, datasets and visuals named OGX ..., fixed primary keys 9500+ to stay clear of GDL's 9000+.
	•	Atlas classifications OGX_SENSITIVE_*, glossary OGX Integrity KPIs, Ranger policies rsingh-ogx-*.
Repository. partomia/O-G-Asset-Integrity-Lakehouse, public. Built locally in PyCharm with the Cursor agent, with General-DataLakehouse and Chest-X-ray-Triage open read-only beside it as the reference implementations.
Synthetic sources and the Kafka producer
Eight sources are generated from one seeded asset universe (generators/asset_universe.py), so every PDF, video frame, drawing tag and sensor reading points at an asset that really exists in the master data.
Source
Type
Format
Generator
Cadence
Asset register (EAM/SAP PM style)
structured
mysqldump-style .sql day 1, then CDC JSON
gen_erp.py
full, then daily changes
Maintenance work orders
structured
pipe-delimited CSV with trailer count
gen_erp.py
daily
Inspection reports
unstructured
PDF (text layer), some scanned PDF (image only)
gen_documents.py (reportlab)
~30 a day
Drone inspection video
unstructured
MP4, 20–30 s, with sidecar JSON (asset hint, GPS, camera)
gen_videos.py (OpenCV from frames)
~4 a day
Engineering drawings
unstructured
P&ID sheets as PDF and PNG with equipment tags drawn in
gen_drawings.py
20 sheets day 1, revisions later
Seismic surveys
unstructured, binary
SEG-Y (small 2D lines)
gen_seismic.py (segyio)
2 surveys day 1
Well logs
semi-structured
LAS 2.0
gen_well_logs.py (lasio)
12 wells day 1, re-logs later
Sensor telemetry and alarms
streaming
JSON (or Avro) on Kafka
stream/producer/sensor_producer.py
continuous, ~40 msgs/s
Each batch folder carries _manifest.json: file name, size, sha256, record or page counts, as the source reports them. Reconciliation is always against the manifest.
The producer. A seeded Python process (kafka-python or confluent-kafka) emits one reading per sensor tag every 5 seconds for about 200 tags, with event time, tag, value, unit and quality code. Degradation is planted on a few assets so the stream carries real signal: a pump's vibration drifts up, a corrosion probe's wall-loss rate climbs. It runs as CAI job ogx-stream-producer (--rate, --duration, --start-date) or from a laptop, and every minute writes a control message with the count it sent per tag.
Planted faults, so validation, quarantine and reconciliation have work to do:
	•	A corrupt PDF, and scanned PDFs with no text layer (forces the OCR fallback).
	•	A video whose file name carries the wrong asset tag (caught by the asset master).
	•	A SEG-Y survey re-sent twice (dedup by sha256).
	•	Late and out-of-order sensor events, a stuck sensor (flat line), a unit change from psi to bar mid-stream.
	•	A work-order CSV whose trailer count is off by one.
Unstructured data handling pattern
The pattern is store by reference, extract to tables, link to the asset: binaries stay whole in object storage, and Iceberg holds a catalog row per object plus everything extracted from it, so SQL can find, join and govern unstructured content like any other table.
	•	Land and fingerprint. CDE copies each file to raw/<format>/<sha256>.<ext> (immutable, deduplicated by hash) and writes one row to bronze.doc_object: doc_id, source path, format, MIME, size, sha256, captured time, asset hint from the file name or sidecar, manifest batch, ingest status. Unlike GDL, bytes are not copied into Iceberg: seismic and video run to gigabytes in real life.
	•	Validate against an object contract. contracts/objects/<format>.json sets what a valid object is: PDF opens and has pages, SEG-Y has a readable textual and binary header, LAS has ~W and ~C sections, MP4 decodes with a minimum length. Failures go to bronze.quarantine with a reason code.
	•	Extract by format (extract/, shared by CDE jobs and tests):
Format
Extractor
Silver table(s)
What becomes queryable
PDF inspection report
pdf_text.py, ocr.py fallback
doc_page, doc_chunk, inspection_finding
Text per page, wall-loss %, coating condition, inspector, recommended action
P&ID drawing
drawing_tags.py (text layer, OCR on PNG)
drawing_tag, drawing_revision
Which equipment tags each sheet shows, revision history
SEG-Y
segy_headers.py (segyio)
seismic_survey, seismic_trace_header
Survey geometry, trace counts, sample rate, line extents
LAS well log
las_curves.py (lasio)
well_log_header, well_log_curve (long: depth, mnemonic, value)
Gamma ray, resistivity, porosity by depth per well
Drone video
video_keyframes.py (OpenCV)
video_keyframe (frame time, path to JPEG, blur and exposure scores)
Frames ready for the defect model
	•	Link to the asset. The asset master resolves every hint (file name tag, sidecar, drawing tag, sensor tag) to one asset_id; an object that cannot be resolved is flagged, not dropped.
	•	Serve. Gold fact_document (one row per object per asset), Impala views for search, and the app opens the original file from its raw/ path when an engineer clicks it.
Optional extension, off by default: embed doc_chunk text with a small pinned sentence model in CAI and store vectors for semantic search in the app. Plain Impala text search (sql/unstructured_search.sql) is the baseline and needs nothing extra.
Streaming with CDE Spark
Two long-running CDE Spark Structured Streaming jobs turn the Kafka topics into Iceberg tables that the batch DAG, the features and the app all read.
	•	stream_telemetry_bronze.py reads ogx.sensor.telemetry and ogx.scada.alarm (SASL_SSL with the workload user, offsets from startingOffsets on first run), validates each message against contracts/stream/<topic>.json, and appends to bronze.sensor_reading and bronze.scada_alarm partitioned by date and hour. Bad messages go to bronze.stream_quarantine. Trigger every 60 s; checkpoint under s3a://.../rsingh_ogx/checkpoints/bronze/.
	•	stream_telemetry_agg.py reads the bronze table as a stream, normalises units (bar to psi, °C kept), sets a 10-minute watermark on event time, and builds 1-minute and 15-minute windows per tag: min, max, mean, standard deviation, slope, count, late count. It flags stuck sensors (zero variance across N windows) and threshold breaches, and writes silver.sensor_window through foreachBatch with an Iceberg MERGE, so a late event corrects its window instead of duplicating it.
	•	Batch handoff. The daily DAG reads only windows closed before the business date's cut-off, builds gold.fact_sensor_window and gold.fact_alarm, and stamps the Iceberg snapshot it read into ref.load_audit.
	•	Stream reconciliation. reconcile.py --stream compares the producer's per-minute counts on ogx.control with rows landed per tag and minute, and Kafka end offsets with committed offsets. Results land in ref.recon_results (MATCHED / LATE / MISMATCH) beside the batch results.
	•	Operations. cde/scripts/deploy_streaming.sh starts, stops and restarts both jobs; a restart resumes from the checkpoint with no gap and no duplicate. The demo shows this live: stop the job, let the producer run, restart, watch the recon go back to MATCHED.
To confirm on the target CDE version before Phase 4: long-running streaming job support and timeout settings, and whether the Kafka client jars need a CDE resource of their own.
Layers, asset master and gold model
The asset master plays the role GDL's MDM plays for customers: one asset_id behind every way a source names the same equipment.
	•	Bronze: as received, all strings plus _batch_id, _business_date, _source_system, _source_file, _record_hash; doc_object for every file; quarantine with reason codes; schema drift kept and logged.
	•	Silver: typed and standardised; CDC applied to the asset register with MERGE; extractor outputs from the unstructured section; sensor_window from the stream.
	•	Asset master (rsingh_ogx_asset): asset_candidate (every name seen: EAM functional location like REF-U1-P-101A, P&ID tag P-101A, sensor tag PI-101A.PV, file-name tag, GPS on a pipeline segment), match_pair (rule, score, decision), asset_xref (source + source name to asset_id), golden_asset (survived attributes with the source of each). Rules: exact functional location; normalised tag (strip suffixes like .PV, unify dashes); pipeline segment by GPS within a chainage band; anything else to a review queue.
	•	Gold: the integrity model below.
	•	Ref: load_audit, recon_results, dq_results, transform_log, source_mapping, kpi_definition, plus model_event and training_set from the ML side.
Kind
Tables
Conformed dimensions
dim_date, dim_facility (field, pipeline, refinery unit), dim_asset (SCD2: status, criticality, material, design pressure), dim_document_type
Inspection
fact_inspection (one per inspection event), fact_inspection_finding (from PDF extraction), fact_inspection_score (model output)
Maintenance
fact_work_order (opened, closed, type, cost)
Condition
fact_sensor_window (15-minute grain), fact_alarm
Documents
fact_document (one per object per asset: format, pages or frames, extraction status)
Subsurface
dim_well, fact_well_log_summary, dim_seismic_survey
Outcomes
fact_review_outcome (when each severe defect reached an engineer, risk-ranked vs calendar order), daily_integrity_summary
SCD2, canonical keys, source mappings (model/source_mapping.csv) and the extension method follow GDL exactly; the worked extension example is adding a new unstructured format (thermography images) end to end.
ML on CAI: features, models and registry
Three model heads share one CAI project and one job chain each, exactly as CXR runs pneumonia, film QC and pneumothorax.
Model
Predicts
Input
Data
After a passed gate
corrosion
Defect severity on a keyframe: none / surface / severe (pitting, coating loss)
Frozen image embedding of video keyframes + small head
A pinned public corrosion image set (Hugging Face or Kaggle, licence checked in Phase 0); synthetic frames in CI
champion: ranks the inspection worklist
frame_qc
Frame unfit for AI review (blur, glare, low light, occlusion)
Same embedding
Real frames plus degraded copies (features/degrade.py)
champion: an unfit frame gets band NA
equipment_risk
Probability of an integrity event within 30 days
Tabular: sensor-window features, alarm counts, work-order history, findings extracted from PDFs
Synthetic universe with planted degradation; labels from the generator truth
silent trial: scored daily, never shown, until promoted
Feature engineering follows the CXR rule: features/feature_logic.py is the only feature definition, used for training, batch scoring and the online endpoint. build_feature_table.py (job ogx-01) writes feature_store/ogx_features/v<ver>/ (parquet + manifest.json) and publishes ref.training_set. A feature_hash() fingerprints the backbone and its pinned revision, image size, window lengths, unit rules and logic version; job 02 refuses a stale table, the gate checks the hash, and serve/predict.py refuses to start on a mismatch. Data checks in job 01: asset leakage across splits, duplicate frames, empty classes, windows with too few readings.
Train and evaluate. train/train_validate.py (job ogx-02) logs to MLflow; the operating threshold is set on VAL (for example, keep severe-defect recall at or above 0.95), KPIs are measured on TEST only.
Gate and deploy. gate/kpi_gate.py (job ogx-03) checks absolute KPIs and non-regression against the champion; exit 1 stops the chain. serve/deploy_champion.py (job ogx-04) archives the current champion, deploys through CAI API v2 and rolls back on failure.
AI Registry. serve/registry.py creates a new version of ogx-corrosion, ogx-frame-qc or ogx-equipment-risk for every candidate that passes, every silent trial, every champion and every promotion. Each version is tagged at creation with stage, git commit, feature hash, threshold, TEST KPIs, training-set snapshot id from Iceberg and, on promotion, the approver and the trial evidence. That snapshot id is what ties a model version back to the exact lakehouse data it learned from.
Lakehouse scoring and promotion. lakehouse/score_inspections.py (job ogx-06, triggered by the DAG) scores one business date's keyframes and assets through Impala and writes gold.fact_inspection_score and gold.model_score. ogx-05-nightly-drift computes PSI on embeddings and sensor features. ogx-07-promote-champion (manual, OGX_MODEL, OGX_APPROVED_BY) promotes the equipment-risk trial only when models.<n>.go_live evidence is met; otherwise it records PROMOTION_REFUSED and nothing changes.
CI/CD. Same two workflows as CXR: ci.yml runs tests and the whole chain offline on synthetic data on every push; cai-mlops.yml on push to main triggers ogx-00-sync-code through CAI API v2 and follows the chain, turning the GitHub check red when the gate stops it.
Guardrails for ML
Guardrails sit at four points, all configured in config/guardrails.yaml, all logged to ref.guardrail_event, and each with a test that shows it firing.
Layer
Guardrail
Code
What happens when it fires
Input
Object and message contracts
contracts/, ingest_bronze.py
Record to quarantine, never scored
Input
Frame quality head
frame_qc model
Frame gets band NA, reviewed in calendar order as without AI
Input
Out-of-distribution check (embedding distance to training set)
guardrails/input_checks.py
Band NA with reason OOD; counted on the drift dashboard
Input
Sensor sanity: stuck sensor, unit mismatch, too few readings in window
stream_telemetry_agg.py, input_checks.py
Feature marked missing; equipment_risk abstains for that asset
Input
Asset not resolved by the asset master
input_checks.py
Not scored; shown in the asset review queue
Output
Abstain band between two thresholds
guardrails/output_policy.py
Score shown as "uncertain, engineer review" rather than a rank
Output
No automated action
output_policy.py, app
Model can rank and suggest; only an engineer opens or closes a work order
Output
Safety-critical asset floor
output_policy.py
A criticality-A asset never ranks below its calendar due date, whatever the score
Process
KPI gate and non-regression
gate/kpi_gate.py
Chain stops, champion keeps serving
Process
Feature hash lock
feature_logic.py, predict.py
Endpoint refuses to start on a mismatch
Process
Silent trial and named approver
promote_champion.py
No promotion without evidence and a person
Process
Drift threshold
monitor/
PSI above limit raises a guardrail event and a dashboard flag; TOO_FEW below 30 samples
LLM (optional)
Grounding, scope, sensitive-data redaction for report summaries
guardrails/llm_guard.py
Summary refused unless every claim cites a page of the source PDF
The last row applies only if you turn on report summaries in the app with a model on Cloudera AI Inference; the demo stands complete without it.
Application: Integrity Workbench
One Streamlit CAI Application (app/launch_app.py starts app/app.py as a child process, the CXR lesson) gives the integrity engineer four pages over the same gold tables.
	•	Worklist. Today's inspections ranked by risk band (P1 to P3, NA, uncertain), with the calendar-order position beside each so the gain is visible. Filters by facility, asset class and criticality.
	•	Asset 360. For one asset_id: the golden record, its P&ID sheet with the tag highlighted, the latest inspection PDF opened from raw/, extracted findings, keyframes with the occlusion heatmap from serve/explain.py, a sensor trend from fact_sensor_window, alarms, work orders, and for wells the log curves and seismic line it sits on.
	•	Document search. Search across all extracted text and tags (Impala baseline; semantic search if the extension is on); every hit links to the asset and the original file.
	•	Feedback. Confirm or override a score with a reason. Overrides are written to gold.review_feedback and become labelled rows for the next ogx-01 run, closing the loop as CXR does.
The app reads through lakehouse/store.py (Impala on the cluster, Spark locally), and calls the champion endpoint only for an on-demand re-score of a frame.
KPIs, CDW and CDV
Three certified KPIs, each defined once as an Impala view in rsingh_ogx_semantic with its row in ref.kpi_definition and an Atlas glossary term; every consumer selects from the view and a consistency check proves they agree.
KPI
Certified definition (proposed)
Consumers
Severe-defect time to review
Hours from a severe defect's capture to engineer review, median and P90, risk-ranked vs calendar order
Operations dashboard, ad-hoc by facility, management extract
Integrity risk exposure
Sum over assets of equipment_risk probability × criticality weight, by facility and day
Operations dashboard, Asset 360, ad-hoc trend with time travel
Unstructured coverage
Share of objects that are extracted and linked to an asset, by format
Data estate dashboard, recon report, ad-hoc list of unlinked files
CDW (Impala, Hue). sql/semantic/*.sql for the KPI views and dashboard views; sql/adhoc.sql (for example: every inspection report on line 3 with wall loss above 20% and a rising corrosion-probe slope this week, joining PDF findings to stream windows); sql/unstructured_search.sql (text and tag search over doc_page and drawing_tag); sql/time_travel.sql (an asset's risk before and after a new inspection, a drawing before its revision). One dialect shared by Impala and Spark, run by scripts/run_semantic.py --engine impala|spark.
CDV dashboards (dataviz/build_dashboard.py, built as code, imported to the existing CDW Data Visualization instance):
	•	OGX Integrity Operations: worklist bands, time to review risk vs calendar, risk exposure by facility.
	•	OGX Unstructured Data Estate: objects and volume by format, extraction and linkage coverage, quarantine reasons, OCR fallback rate.
	•	OGX Streaming Health: messages per minute, lateness, stuck sensors, stream recon status.
	•	OGX Models, Drift and Guardrails: model versions by stage, silent-trial evidence, PSI, guardrail events by type.
Repository layout
The tree merges the two siblings: GDL's lakehouse folders, CXR's ML folders, and four new ones for the new ground (generators/, extract/, stream/, guardrails/).
O-G-Asset-Integrity-Lakehouse/ ├── README.md  PLAN.md  .env.example  .gitignore ├── common.py  cdsw-build.sh  requirements.txt  requirements-ci.txt ├── .github/workflows/        ci.yml (offline chain)  cai-mlops.yml (GitHub -> CAI API v2) ├── config/                   pipeline.yaml (ML)  lakehouse.json  streaming.json  guardrails.yaml │                             kpi.json  governance.json  ci.yaml (synthetic overlay) ├── contracts/                <entity>.json   objects/<format>.json   stream/<topic>.json ├── generators/               asset_universe.py  gen_erp.py  gen_documents.py  gen_drawings.py │                             gen_seismic.py  gen_well_logs.py  gen_videos.py  faults.py ├── stream/ │   ├── producer/             sensor_producer.py  alarm_rules.py  topics.py │   └── schemas/              sensor_reading.avsc  scada_alarm.avsc  control.avsc ├── extract/                  pdf_text.py  ocr.py  drawing_tags.py  segy_headers.py │                             las_curves.py  video_keyframes.py  chunker.py ├── cde/ │   ├── jobs/                 land_sources.py  ingest_bronze.py  extract_unstructured.py │   │                         build_silver.py  build_asset_master.py  build_gold.py │   │                         build_outcomes.py  reconcile.py  ogx_common.py │   │                         stream_telemetry_bronze.py  stream_telemetry_agg.py │   ├── dags/                 ogx_dag.py │   └── scripts/              deploy_jobs.sh  deploy_dag.sh  deploy_streaming.sh  set_airflow_variables.py ├── features/                 feature_logic.py  build_feature_table.py  degrade.py ├── train/                    train_validate.py ├── evaluate/                 metrics.py ├── gate/                     kpi_gate.py ├── guardrails/               input_checks.py  output_policy.py  llm_guard.py (optional) ├── serve/                    predict.py  explain.py  deploy_champion.py  promote_champion.py  registry.py ├── monitor/                  batch_score.py  drift.py  stream_health.py ├── lakehouse/                score_inspections.py  publish.py  store.py ├── app/                      launch_app.py  app.py  pages/ ├── ci/                       sync_code.py  cai_jobs.py  create_cai_jobs.py  trigger_cai_pipeline.py  setup_cai.py ├── sql/                      semantic/*.sql  adhoc.sql  unstructured_search.sql  time_travel.sql ├── dataviz/                  build_dashboard.py ├── model/                    source_mapping.csv ├── scripts/                  run_local.py  run_semantic.py  governance.py  fetch_dataset.py  render_mapping.py ├── capabilities/             one doc per capability: what, how, how to see it working ├── docs/                     DATA_MODEL.md  UNSTRUCTURED.md  STREAMING.md  GUARDRAILS.md  GOVERNANCE.md │                             DATAVIZ.md  FAILED_BATCH_DEMO.md  DEMO_RUNBOOK.md  PRESENTER_RUNBOOK.md  PROJECT_LOG.md └── tests/                    contracts, generators, extractors, stream windows, guardrails,                               feature hash, gate, SQL portability, CAI contract, docs
Data stays out of Git: data/raw/ (image set), data/incoming/ (keyframes for the worklist), feature_store/, models/archive/, outputs/ live only in the CAI project, as in CXR.
Phases
Thirteen phases, one commit each, pytest -q green before the next; Cloudera resources verified live one at a time and recorded in docs/PROJECT_LOG.md.
	•	☒ 0. Scaffold: layout, config, contracts, requirement files, local Spark + Iceberg runner, CI skeleton; pick and pin the corrosion image set and check its licence.
	•	☒ 1. Asset universe and batch generators: register, work orders, PDFs, drawings, SEG-Y, LAS, video, manifests, planted faults; tests.
	•	☐ 2. Kafka producer: topics and schemas in Schema Registry, seeded producer, control messages, planted stream faults; run locally against a dev broker.
	•	☐ 3. Bronze and object catalog: contracts, doc_object, quarantine, load_audit, failed-batch flags; recon at bronze.
	•	☐ 4. CDE streaming: bronze and aggregation streaming jobs, watermark and MERGE, stream recon, stop and restart drill.
	•	☐ 5. Extraction and silver: the five extractors, OCR fallback, transform_log.
	•	☐ 6. Asset master: candidates, match rules, asset_xref, golden_asset, review queue.
	•	☐ 7. Gold: integrity model, SCD2, source mapping check.
	•	☐ 8. CAI ML chain: features and hash, three heads, gate, deploy, AI Registry, jobs ogx-00 to ogx-05, ci.yml and cai-mlops.yml.
	•	☐ 9. Guardrails: input, output and process guardrails with tests that make each fire; ref.guardrail_event.
	•	☐ 10. Lakehouse scoring and outcomes: DAG calls ogx-06, fact_review_outcome, silent trial, ogx-07 promotion.
	•	☐ 11. CDW, CDV and governance: semantic views, KPI consistency check, four dashboards, Atlas and Ranger as code.
	•	☐ 12. Application and docs: Integrity Workbench, README, runbooks, capabilities, thermography as the worked extension.
How we work
The repo is public, so nothing secret or customer-specific ever enters Git, and every Cloudera action is approved once, up front, as a list.
	•	Copy, then adapt. Read the sibling repo's file before writing its counterpart; reuse its connection code (Impala, CAI API v2, CDE CLI, Data Visualization, Knox) rather than rewriting it.
	•	Credentials from the siblings' local .env files, re-prefixed to OGX_* in this repo's git-ignored .env. Only .env.example with placeholders is committed. A secret scan (gitleaks pre-commit hook) and GitHub push protection guard the public repo.
	•	Public repo simplifies deploy. CDE uses a Git repository resource on the public URL, and the CAI project is created from the public URL, so the deploy key and git archive upload that CXR needed as a private repo are dropped.
	•	One approval list. At the start the agent presents every action that touches Git remotes or Cloudera (push, CAI project and jobs, CDE resources, jobs, DAG and streaming jobs, Kafka topics and schemas, first CDW write, Atlas and Ranger, Data Visualization import, dataset download). Once approved it proceeds without asking again; anything outside the list, destructive, or outside the ogx prefix still needs a separate yes.
	•	One commit per phase, pytest -q green first, no attribution trailers (GDL's commit-msg hook), and every live run recorded in docs/PROJECT_LOG.md.
	•	Never touch the siblings: no edits to their repos, databases, CDE jobs, CAI projects, Atlas tags or dashboards.
Decisions and open items
Proposed decisions, to confirm or change before Phase 0:
	•	Binaries by reference in S3, never copied into Iceberg; Iceberg holds catalog and extracted content.
	•	Kafka on a Streams Messaging Data Hub in the same environment; Schema Registry for message contracts.
	•	Producer as CAI job ogx-stream-producer, also runnable from a laptop.
	•	Two streaming jobs on CDE rather than Flink, as requested; Flink/SSB noted in the docs as the alternative for sub-second alerting.
	•	Corrosion head on frozen image embeddings (CPU-friendly); fine-tuning only if a GPU profile exists.
	•	LLM report summaries and vector search are optional extensions, off by default.
	•	Data Visualization on the existing CDW instance with its own connection and OGX dashboards, touching nothing else.
Open items:
	•	☒ Repo: O-G-Asset-Integrity-Lakehouse, public (decided 2026-10-08).
	•	☐ Is a Streams Messaging Data Hub available in the federal environment, or must one be created?
	•	☐ CDE version on the target: long-running Structured Streaming support and job timeout limits.
	•	☐ Which public corrosion image set, and does its licence allow demo use?
	•	☐ GPU resource profile in the CAI workbench: yes or no.
	•	☐ Confirm the three certified KPIs, or swap one for an inspection-backlog KPI.
	•	☐ Masked demo users for the Ranger policies (reuse federal01 and federal07?).
