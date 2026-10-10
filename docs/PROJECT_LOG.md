# Project log

What ran on the federal Cloudera environment, what differed from `PLAN.md`, and the results.
Newest last. Times are IST unless marked UTC. No secret values are ever written here.

## 2026-10-08: setup

- Read the reference files of General-DataLakehouse (GDL) and Chest-X-ray-Triage (CXR);
  reuse recorded in [REUSE_INVENTORY.md](REUSE_INVENTORY.md).
- `.env` created from the siblings' `.env` files (identical workload user, CAI host and key,
  Data Visualization key in both), renamed to `OGX_*`; `.env.example` holds placeholders only.
- Read-only discovery with the CDP CLI (`~/.cdp/credentials`): Data Hub `federal-kafka`
  (Streams Messaging Light Duty, CDP 7.3.2, AVAILABLE) with brokers on 9093 (SASL_SSL, PAM),
  Schema Registry and SMM behind its Knox `cdp-proxy-api`; `federal-flink` also exists. The
  environment root CA was fetched (`cdp environments get-root-certificate`) to a local file
  outside the repo. Brokers have public IPs but 9093 is closed from the laptop.
- Public repository, so deployment is simpler than CXR's: CDE uses a **Git repository
  resource** on `https://github.com/partomia/O-G-Asset-Integrity-Lakehouse` (GDL's pattern)
  and the CAI project is created from the same public URL. CXR's deploy key,
  `ci/bootstrap_git.py` and the `git archive` files resource are not carried over.
- Guards for the public repo: `.gitignore` (`.env`, `data/`, `feature_store/`,
  `models/archive/`, `outputs/`, `checkpoints/`, `*.jks`, `*.keytab`, `*.pem`), a gitleaks
  `pre-commit` hook and GDL's `commit-msg` hook (`scripts/install_hooks.sh`).
- Plan correction proposed: Data Visualization PKs 13000+ instead of 9500+ (GDL's visuals reach 9508).
- Answers received: rsingh is admin (add Ranger policies where access is missing), Kafka uses
  SASL PLAIN with the workload password, CDE has no job timeout, CDE executors and memory may
  be raised above the siblings'. Credentials pulled from the siblings' local `.env` files.

## 2026-10-08: phase 0, scaffold and dataset (local)

- Dataset pinned: `LibreYOLO/corrosion-bi3q3` (Roboflow 100 "Corrosion Bi3Q3"), revision
  `d20b5b1e6b21ff7de555bbf848dcf7d30ccf8ee7`, **CC BY 4.0** (allows demo use with attribution).
  1,249 images, 640 x 640, YOLO boxes for Slippage / corrosion / crack. `scripts/fetch_dataset.py`
  (job `ogx-setup-data`) downloads it with a Hugging Face read token from `.env` (anonymous
  downloads hit HTTP 429).
- **Plan change: severity rule.** The plan's "corrosion area >= 15 % = severe" gives 4 severe
  images out of 1,249 (corrosion boxes are small: 90th percentile of per-image area 7 %).
  `features/labels.py` sets severe = corrosion boxes >= 2 % of the frame **or** >= 3 boxes;
  surface = any other corrosion; none = no corrosion box. Balance: train 567 / 111 / 162,
  valid 206 / 34 / 64, test 47 / 29 / 29 (none / surface / severe).
- Drone frame library `assets/frames/` (committed, 3.8 MB): the 105 TEST images at 320 px plus
  one degraded copy each (blur, low light, glare, occlusion, noise), with `frames.csv` (sha256,
  severity) and `ATTRIBUTION.md`. TEST only, so no video frame was seen in training.
- **Plan change: drone video is Motion-JPEG AVI, not MP4**, and every generator and extractor is
  standard library only (minimal PDF / PNG / AVI / SEG-Y / LAS writers and parsers instead of
  reportlab, OpenCV, segyio, lasio), so the CDE jobs need no python-env resource. Keyframes are
  the library JPEGs byte for byte; CAI scoring finds each by sha256.
- **Plan change: OCR is template matching** against the synthetic scanner's 5x7 font
  (`extract/ocr.py`), not a general OCR engine; it reads every scanned report field exactly.
  A production pipeline would put Tesseract or a document AI service in the same slot.

## 2026-10-08 / 09: phases 1 to 7 (local), first live runs

Local (Spark 4.0.1 + Iceberg 1.10, JDBC SQLite catalog, Kafka 3.9.1 on localhost) for the five
business dates 2026-10-04 to 10-08: land, bronze, stream (2.88M readings), extract, silver,
asset master, gold, semantic and recon all pass. Recon flags only the planted faults: corrupt
PDF on 10-05 (MISMATCH), seismic resend (EXPLAINED), work-order trailer on 10-06 (MISMATCH).
The stop/restart drill writes no duplicate (each foreachBatch commit carries
`ogx.stream-batch = <query id>:<batch id>`; a replayed batch is skipped). KPI consistency: 9 of
9 MATCHED on every date.

Live, in order:

- **CAI** project `rsingh-og-asset-integrity` (id `2le3-eze7-5ih7-fj5k`) created from the
  public URL; jobs `ogx-setup-data`, `ogx-stream-producer`, `ogx-00-sync-code`.
  `ogx-setup-data` at 2 vCPU / 8 GB stayed in scheduling for 37 minutes (cluster capacity);
  resized to **1 vCPU / 4 GB** it ran at once and succeeded in 923 s (dataset downloaded,
  requirements installed). The application is also sized 1 vCPU / 4 GB for that reason.
- **Kafka** topics `ogx.sensor.telemetry`, `ogx.scada.alarm`, `ogx.control` created from CDE
  (run 533, Java AdminClient over SASL_SSL PLAIN), which proves the network path, TLS and
  SASL from the CDE vcluster (9093 is closed from the laptop). Schemas registered in Schema
  Registry through Knox, version 1 each.
- **Plan change: the producer runs as a CDE job** (`rsingh-ogx-stream-produce`) as well as a CAI
  job, because CDE reaches the brokers and its Spark Kafka writer is fast: 5-day backfill
  (2.88M readings plus alarms and per-minute control counts) in run 534, about 3.5 minutes.
- **CDE** resource `rsingh-ogx-pipeline` (Git repository on the public URL); jobs
  `rsingh-ogx-{land,bronze,extract,silver,asset,gold,recon,stages}` at driver 2 cores / 4 GB,
  executors 4 cores / 8 GB, 1 to 4 executors. Land and bronze succeeded for all five dates
  (runs 535 to 545).
- **Streaming**: `rsingh-ogx-stream-bronze` run 541 failed at start (`Error parsing '60s' to
  interval`: Spark 3.5 wants "60 seconds"); fixed and restarted as **run 569**, which reads
  Kafka at about 200k readings per micro-batch with nothing quarantined.
  `rsingh-ogx-stream-agg` started as run 573.
- **Plan change: stuck-sensor rule.** The producer samples every 30 s (30 readings per
  15-minute window) but the rule required 90, so no window was ever flagged. The minimum is
  now 24 per 15 minutes (2 per minute); gold also derives stuck windows from the window
  statistics, so the running aggregation job needs no restart to pick it up.
- **Plan change: per-tag breach thresholds.** Wellhead pressure runs about 1,800 psi, above the
  single 1,450 psi limit, so every well window breached. `threshold_overrides` in
  `config/streaming.json` sets PI-W* to 2,500 psi.
- **Plan change: TTR before the model.** Until the equipment_risk scores land, IRE uses the gold
  rule score / 100 as `p_event_30d` (`p_source = 'rule_score'`), and TTR compares the
  risk-ranked worklist against calendar order on the same inspections (severe = wall loss
  >= 20 %). Locally risk order cuts severe-defect review time from about 28 h to 24 h.

## 2026-10-09 morning: live chain, semantic layer, dashboards, app, governance

- **CDE batch chain**: extract and silver per date (runs 570 to 580), then the new
  `rsingh-ogx-stages` job (one Spark session for several stages and dates, to avoid a driver
  start per stage on a busy vcluster): run 581 = silver, asset, gold, recon for 10-04 to 10-06;
  run 587 = 10-07 (then failed on extract 10-08: executors could not import the job module,
  fixed by loading jobs unregistered, as `run_local.py` does); extract 10-08 run 589, then a
  stages run for 10-08. Several runs queued 15 to 25 minutes behind other demos' jobs.
- Live results equal the local ones: recon 10-04 32 MATCHED; 10-05 one MISMATCH (corrupt PDF)
  and one EXPLAINED (seismic resend); 10-06 one MISMATCH (work-order trailer); 10-07 29 MATCHED.
  Golden assets 222 to 251 by date, 1 conflict auto-resolved. `bronze.sensor_reading`
  2,880,000 rows.
- **CDW semantic layer** on Impala (`rsingh_ogx_semantic`): 3 certified KPI views, 4 MIS
  views, 5 dashboard views. KPI consistency 9 of 9 MATCHED on every date; IRE 9.37, 13.96,
  17.33, 21.09 for 10-04 to 10-07, 1 asset abstained (stuck VI-112B) from 10-05.
  Impala reserved words `format` and `method` are escaped or renamed (`object_format`).
- **Data Visualization**: 4 dashboards, 9 datasets, 36 visuals imported on connection
  `federal-impala-1` (export PKs 13000+; the instance assigned dataset ids 54+). `--verify`:
  36 of 36 visuals return rows through the Data API, tiles equal to Impala.
- **CAI**: `ogx-00-sync-code` (2 vCPU / 8 GB) stuck in scheduling; it and the producer job
  resized to 1 vCPU / 4 GB. The stuck run was left alone (stopping OGX CAI runs awaits
  approval), so the code was pushed into the project with the CAI files API instead of a git
  pull. Application **OGX Integrity Workbench** `nk5h-c7u1-c0sr-w54t`, subdomain
  `rsingh-ogx-workbench`, 1 vCPU / 4 GB: APPLICATION_RUNNING; smoke test (Streamlit AppTest
  against live Impala) shows no exception across the four tabs.
- **SDX**: `scripts/governance.py apply`: 4 `OGX_SENSITIVE_*` classifications on 26 columns,
  glossary "OGX Integrity KPIs" with 3 terms on 7 views, Ranger tag masking policies
  `rsingh-ogx-sensitive-{hash,location,subsurface,text}` for federal01 / federal07. `verify: OK`.
- 10-08: extract run 589 and stages run 590 succeeded. KPI consistency 9 of 9 MATCHED, IRE
  23.30 over 223 assets, 1 abstained, coverage 100 %: identical to the local run. All five
  business dates are loaded end to end on CDE, CDW, Data Visualization and the app.
- Live stream for the demo date: `rsingh-ogx-stream-produce --mode live --duration 10800`
  (run 591, 3 hours, stops by itself) feeds `ogx.sensor.telemetry` for the running bronze and
  aggregation streams.
- **CAI corrosion chain** (08:35 to 08:40): `ogx-01-build-features` → `ogx-02-train-validate`
  → `ogx-03-kpi-gate` → `ogx-04-deploy-champion`, all ENGINE_SUCCEEDED at 1 vCPU / 4 GB.
  **Plan change:** colour/texture features (`features/feature_logic.py`, numpy + Pillow) instead
  of ViT-B/16 embeddings, so the chain fits 1 vCPU and the model build is slim
  (`requirements-model.txt`, pinned by the deploy job to the training scikit-learn). Three tree
  ensembles chosen by 5-fold out-of-fold AUROC on train + valid; the threshold comes from the
  out-of-fold scores (VALID alone set it at 0.002 and failed specificity). Champion: gradient
  boosting, TEST severe AUROC 0.862, sensitivity 0.931, specificity 0.513, Brier 0.124; gate
  PASSED. CAI Model `ogx-integrity` (`786c915b-…`), build "built", deployment "deployed"
  at 1 vCPU / 2 GB. The endpoint answers "model busy" (context deadline exceeded) on every
  call; a restart or redeploy awaits approval. The Workbench scores drone keyframes in
  process with the same champion files.
- Not built before the demo: frame_qc and equipment_risk heads, guardrails, lakehouse scoring
  and outcomes. IRE uses the gold rule score until model scores exist (`p_source`).

## 2026-10-09 09:05 - CAI app and model restart (approved)
- Workbench app `nk5h-c7u1-c0sr-w54t` restarted: it had started before the Impala env vars were set, so it showed the
  "OGX Integrity Workbench" fallback page. After restart: APPLICATION_RUNNING; a local render against live Impala reaches
  all four tabs (Worklist, Asset 360, Sensors, Data quality).
- Model deployment `ae268c14` restarted: no longer "model busy", but every call (sha256 or image_b64) returns 400 from
  replica `ogx-integrity-14-32`, i.e. `predict` raises inside the build. Open item: inspect the model build/replica logs
  in the CAI UI. The app scores keyframes in-process with `serve.predict`, so the demo does not depend on the endpoint.

## 2026-10-11 01:00 - NiFi, guardrails, AI Registry versions, retraining loop

- **NiFi** (`federal-nifi`, NiFi 2.6): process group `OGX - SCADA Alarm Router` built by
  `nifi/deploy_flow.py` (a `check` step resolves every property against NiFi's component
  definitions before anything is created). Schema Registry access through Knox with basic auth
  (the service's property is `Schema Registry URL`, singular); `CdpCredentialsProviderControllerService`
  needs CDP access keys, so `PutCDPObjectStore` uses a Kerberos user service instead. Kafka via
  `Kafka3ConnectionService` (SASL_SSL PLAIN) and the existing root truststore-only SSL service
  (referenced only). Other process groups on the canvas (incl. `rsingh-workshop`) untouched.
  NiFi Registry bucket `ogx-flows` created; flow version 1 committed and exported to `nifi/flows/`
  (no sensitive values; a test checks it). First run: 84 alarms consumed, 84 valid and landed in
  `landing/nifi/scada_alarm/2026-10-10/` (2 files), 6 priority alarms published, 0 quarantined.
- Topic `ogx.alarm.priority` created through the SMM REST API (the brokers are not reachable from
  the laptop) and its schema registered; it is kept out of `streaming.json` `topics` so the bronze
  stream (which subscribes to all of them) does not ingest the routed copies.
- **Guardrails** (`guardrails/`, `config/guardrails.yaml`, `docs/GUARDRAILS.md`): frame limits tuned
  on 105 clean + 105 degraded library frames and 300 training images (2 of 105 clean frames
  rejected; glare and low light 100 %, blur 72 %); OOD limit = training q99.5 stored with the model.
  The old placeholder `guardrails.yaml` (read by no code) was replaced; its LLM section kept.
  `serve/predict.py` now returns `{"error": ...}` with the exception instead of an opaque 400.
- **Registry and MLOps**: MLflow run per training, AI Registry version on deploy
  (`serve/registry.py`, copied from CXR), Iceberg tables `rsingh_ogx_ref.model_event`,
  `guardrail_event`, `model_drift` (created on first write). New CAI jobs `ogx-05-nightly-drift`
  (02:00) and `ogx-08-retrain-trigger` (02:30), 1 vCPU. Drift alone retrains at most weekly
  (the drone frames differ from the training set by design: PSI alerts every night).
  `cai-mlops.yml` waits for GitHub secrets `CAI_URL`, `CAI_API_KEY`, `CAI_PROJECT_ID` (not set).
- Code reached the CAI project through the files API (`ci/push_files.py`), not `ogx-00`, so the
  project's git HEAD is still `b1b3ad7` and model versions carry that sha.
- Live: `ogx-05` (40 s): 161 lakehouse keyframes, 150 scored, NA 6.8 %, PSI alert, 17 input +
  23 abstain-band + 3 drift guardrail events, `DRIFT_ALERT`. `ogx-08` with `OGX_RETRAIN_FORCE=1`
  -> `RETRAIN_TRIGGERED` -> `ogx-01` .. `ogx-03`: TRAINED (MLflow run in experiment
  `ogx-integrity`), GATE_PASSED (TEST AUROC 0.862, non-regression vs champion passed).
- Incident: while checking job runs I printed a run's `environment` field to my terminal, which
  showed part of `OGX_HF_TOKEN`. Not written to any file, commit or log; token rotation requested.
