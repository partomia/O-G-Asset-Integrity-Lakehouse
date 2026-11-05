# Drone video in the Spark layer

How a drone inspection video travels from the landing zone to the gold layer using Spark alone
(Cloudera Data Engineering jobs, Iceberg tables), what that gives you, where Spark stops, and
how the Cloudera AI (CAI) layer takes the analysis further.

The Spark jobs need only PySpark and the Python standard library: there is no CDE python-env
resource (`cde/scripts/deploy_jobs.sh`). Everything in the first part of this document runs with
that constraint, which is also where its limits come from.

## What lands

Four flights a day over pipeline segments, flare stacks and tanks (`generators/gen_videos.py`,
`config/lakehouse.json` `daily.videos`). Each flight is two files in
`landing/drone/<date>/`:

| File | Content | Contract |
|---|---|---|
| `DRN_<yyyymmdd>_<asset tag>_<n>.avi` | Motion-JPEG AVI, 20-30 s at 2 fps; the camera holds each view for 3 s, so 7-10 distinct keyframes | `contracts/objects/avi.json`: RIFF AVI with JPEG frames, decodes, at least 10 s |
| `DRN_<yyyymmdd>_<asset tag>_<n>.json` | sidecar: `video_id`, `asset_hint`, `captured_at`, `gps_track` | `contracts/objects/json.json`: valid JSON with those keys |

Frames are real corrosion photographs from the committed library `assets/frames/` (CC-BY-4.0,
`assets/frames/ATTRIBUTION.md`), chosen by the asset's planted condition: a corroding segment
shows mostly severe frames.

Planted faults the pipeline must catch:

- one flight a day carries two unfit frames (blurred, glare, low light, from `features/degrade.py`);
- on 2026-10-06 the flight over PL-03-SEG-014 is named and hinted as PL-03-SEG-041; only its GPS
  track tells the truth.

## The Spark pipeline, stage by stage

The nightly Airflow DAG (`cde/dags/ogx_dag.py`) runs land, bronze, extract, silver, asset,
gold and recon for one business date. The drone video touches these stages:

```
landing/drone/<date>/*.avi + *.json
   │  ingest_bronze.py      validate, fingerprint, store by hash, catalogue
   ▼
raw/avi/<sha256>.avi  +  bronze.doc_object
   │  extract_unstructured.py   binaryFile on the executors, keyframes + GPS
   ▼
raw/frames/<sha256>.jpg  +  silver.video_keyframe   (+ silver.extract_error)
   │  build_asset_master.py     GPS track -> nearest pipeline segment
   ▼
asset.asset_candidate, asset.match_pair, asset.asset_xref, asset.review_queue
   │  build_gold.py
   ▼
gold.fact_document  (the video, resolved to its asset)
   │  reconcile.py
   ▼
ref.recon_results  (every valid video produced keyframes)
```

Databases are `rsingh_ogx_{bronze,silver,asset,gold,ref}`; every table is Iceberg.

### 1. Bronze: validate, fingerprint, store (`cde/jobs/ingest_bronze.py`)

For each file listed in the day's drone manifest:

- **Validate.** `extract/objects.py` parses the AVI with `extract/video_keyframes.py`; a file that
  isn't a RIFF AVI or has no JPEG frames is `VIDEO_UNDECODABLE`, one under 10 s is
  `VIDEO_TOO_SHORT`. The sidecar must be JSON with the four required keys. A missing file is
  `MISSING_FILE`; bytes that don't match the manifest hash are `SHA_MISMATCH`.
- **Fingerprint and deduplicate.** The doc_id is the file's sha256. Bytes seen on an earlier date
  are `DUPLICATE` and are not stored or extracted again.
- **Store.** Valid bytes are copied once to `raw/avi/<sha256>.avi`. Video bytes never go into
  Iceberg; tables hold the path.
- **Catalogue.** One `bronze.doc_object` row per file: path, format, MIME type, size, status, the
  asset hint (from the sidecar for drone files, `hint_source = SIDECAR`) and the capture time.
  The AVI takes its `captured_at` from its sidecar.
- **Quarantine.** Failures also go to `bronze.quarantine` with a reason code, so nothing
  disappears silently.

The day's partition of `doc_object` is replaced in one Iceberg commit, so a rerun never
duplicates rows; `ref.load_audit` records STARTED, COMMITTED and COMPLETED.

### 2. Extract: keyframes and GPS on the executors (`cde/jobs/extract_unstructured.py`)

- The driver reads the small sidecar JSONs and builds one metadata record per video.
- The videos themselves are read with Spark's `binaryFile` source, so the bytes stay in object
  storage and are read by the executors. They are joined to their metadata on the path and
  processed with `rdd.mapPartitions`, a few videos per partition.
- **Keyframe detection** (`extract/video_keyframes.py`, standard library only): walk the RIFF
  tree, read the main header (fps, frame count, width, height), and take a frame as a keyframe
  whenever its JPEG bytes change. The drone holds each view, so identical consecutive frames are
  one keyframe. Each keyframe is kept byte for byte, so its sha256 identifies the exact image.
- **GPS.** Keyframe *i* takes point *i* of the sidecar's `gps_track` (the last point if the track
  is shorter).
- **Sharpness proxy.** Without an image decoder, `bytes_per_kpx` (JPEG bytes per 1,000 pixels)
  stands in for sharpness: a blurred frame compresses smaller.
- **Frames stored by hash.** Each keyframe JPEG is written once to `raw/frames/<sha256>.jpg`.
- **Output.** `silver.video_keyframe`, one row per keyframe: doc_id, file name, video id, asset
  hint, frame index, time in the video, frame sha256 and path, size, `bytes_per_kpx`, lat/lon,
  capture time, fps, duration, business date. A video whose extraction fails gets a row in
  `silver.extract_error` instead of a gap.

`ref.transform_log` records the step with rows in and out.

### 3. Asset resolution: GPS beats the file name (`cde/jobs/build_asset_master.py`)

Every name a source uses for an asset becomes an `asset.asset_candidate`. For a drone video the
candidate carries the mean position of its keyframes:

- the nearest pipeline segment within 500 m (`GPS_BAND_M`) is matched by rule `GPS_SEGMENT`,
  confidence 0.90;
- when the file name names a different segment, the GPS wins: the name match is written as
  `REJECTED` in `asset.match_pair`, and `asset.review_queue` gets a `NAME_GPS_CONFLICT` row,
  `AUTO_RESOLVED`, explaining both positions. This is how the wrong-tag flight of 2026-10-06 is
  caught;
- non-pipeline targets (flare stacks, tanks) resolve on their normalised tag.

`asset.asset_xref` then maps (source system, source name) to the asset id for every later stage.

### 4. Gold and reconciliation

- `cde/jobs/build_gold.py`: `gold.fact_document` gets one row per catalogued object per asset,
  with the rule that resolved it (`resolved_by`), the raw path and the capture time. The
  Workbench's Asset 360 lists the asset's videos from here.
- `cde/jobs/reconcile.py`: check `extract:avi:drone` compares the valid AVIs of the day with the
  doc_ids that produced rows in `silver.video_keyframe`; extraction errors explain the
  difference. The result is a row in `ref.recon_results`.

### What you can ask the lakehouse with Spark (or Impala) alone

```sql
-- keyframes per flight and the frames that look blurred (smallest JPEGs for their size)
SELECT video_id, asset_hint, COUNT(*) AS keyframes, MIN(bytes_per_kpx) AS min_bytes_per_kpx
FROM rsingh_ogx_silver.video_keyframe
WHERE business_date = DATE '2026-10-06'
GROUP BY video_id, asset_hint ORDER BY min_bytes_per_kpx;

-- flights whose file name and GPS disagree
SELECT * FROM rsingh_ogx_asset.review_queue WHERE reason = 'NAME_GPS_CONFLICT';

-- drone coverage: which assets were flown, how often, last flight
SELECT asset_id, COUNT(*) AS videos, MAX(captured_at) AS last_flight
FROM rsingh_ogx_gold.fact_document
WHERE source = 'drone' AND `format` = 'avi'
GROUP BY asset_id ORDER BY last_flight;
```

### Why this works well in Spark

- **The bytes stay in object storage.** Only paths and extracted facts go into Iceberg; the
  executors read the videos where they lie.
- **Content addressing.** Videos and keyframes are named by sha256, so a resent file or a
  repeated frame is stored once and linked many times.
- **Idempotent dates.** Each stage replaces one business date's partition in one commit; reruns
  and the failure drill (`ingest_bronze.py --mode fail-during:<entity>`, then `resume`) never
  duplicate.
- **Lineage and controls.** Every step is in `ref.load_audit` and `ref.transform_log`; every
  rejection is in `bronze.quarantine` or `silver.extract_error`; reconciliation proves nothing
  was lost.
- **Joined to the rest of the estate.** The same asset master links a video to inspection
  reports, work orders, drawings and sensors, so gold has one view per asset.

## Where Spark alone stops

| Limit | Why | Effect |
|---|---|---|
| No pixel-level analysis | the jobs have no image decoder (no numpy, Pillow or OpenCV in the CDE runtime) | blur, glare, exposure and corrosion cannot be measured; `bytes_per_kpx` is a rough proxy that catches some blur and nothing else |
| No corrosion judgement | there is no model in the Spark layer | Spark knows which asset was flown and when, not whether it is corroding |
| Keyframe = changed JPEG bytes | works because the drone holds each view | a continuously moving camera would make almost every frame a keyframe; real scene-change detection needs pixels |
| Motion-JPEG only | the extractor is standard library | H.264 / MP4 from real drones needs a codec (ffmpeg / PyAV) |
| GPS by index | keyframe *i* takes track point *i* | correct for the generated flights; real tracks need interpolation on timestamps |
| Driver-side steps | bronze reads each landed file on the driver; extract collects the keyframe JPEGs to the driver to write them | fine for four flights a day, a bottleneck at hundreds of videos |
| No learning loop | Spark has no labels, training, model registry or drift monitoring | the analysis cannot improve from engineer feedback |

## Advanced analytics: what the CAI layer adds

The CAI layer reads the same lakehouse (silver keyframes through Impala, the asset master, gold
facts) and supplies what Spark lacks: an image decoder, a model, guardrails, monitoring and a
human in the loop. Each item maps to a limit above.

### Frame quality on the pixels (`guardrails/input_checks.py`)

Before any frame is scored, `frame_checks` decodes it with Pillow and measures it against
`config/guardrails.yaml` `input`:

| Check | Limit |
|---|---|
| minimum side | 64 px |
| brightness (mean HSV value) | 0.25 to 0.92 |
| contrast (std of grey level) | at least 0.03 |
| sharpness (mean absolute grey-level gradient) | at least 0.012 |
| glare (share of near-white pixels) | at most 0.50 |

A frame that fails gets band `NA` and is reviewed in calendar order, as without AI. Tuned on the
library: 1 % of clean frames rejected, glare and low light always caught, blur 72 %
(docs/GUARDRAILS.md). This replaces the `bytes_per_kpx` proxy with real measurements.

### Corrosion severity (`features/feature_logic.py`, CAI jobs `ogx-01` to `ogx-04`)

- **Features.** 49 colour and texture statistics per frame: HSV histograms, a rust mask (overall
  and the worst cell of a 4 x 4 grid, for a local patch of severe corrosion), RGB statistics,
  edge density and gradients. numpy and Pillow only, version `hc-1.0.0`, guarded by a hash so
  training and serving compute the same vector.
- **Model.** Random forest, extra trees and gradient boosting compete on 5-fold out-of-fold
  AUROC; the threshold is set at 0.90 sensitivity; a KPI gate blocks promotion on any miss. The
  current champion (gradient boosting) has test AUROC 0.862 and sensitivity 0.931.
- **Bands.** P1 (severe probability at least 0.80), P2 (above the threshold), P3, `NA` (failed an
  input guardrail or out of distribution), `UNCERTAIN` (within 0.05 of the threshold). A
  criticality-A asset never drops below P2. The model ranks and suggests; an engineer decides.
- **Out of distribution.** The feature vector's distance to the training set is checked against
  the 0.995 quantile stored with the model; a frame unlike anything the model learned is `NA`,
  not guessed.

### Serving the score where engineers look

- CAI model endpoint `ogx-integrity` (`serve/predict.py`) scores a keyframe by sha256 or as a
  base64 image, behind the same guardrails, and returns probabilities, band, the checks that ran
  and the allowed actions.
- The Workbench (`app/app.py`, Asset 360) reads the asset's keyframes from
  `silver.video_keyframe`, scores each through the champion once per frame (cached), and shows
  the frame, its band and every guardrail result next to the inspection reports and work orders
  Spark linked to the same asset.

### Monitoring and the learning loop

- `ogx-05-nightly-drift` (02:00) reads the lakehouse keyframes through Impala, scores them on the
  serving code path, and computes PSI of the severe probability and of the 49 features against
  the training table, plus the out-of-distribution and `NA` rates. Results go to
  `ref.model_drift`; each guardrail that fires is a row in `ref.guardrail_event`.
- Engineers label frames in the Workbench; `ogx-08-retrain-trigger` (02:30) retrains on a drift
  alert, 20 new labels or a 30-day-old champion. The chain `ogx-01` to `ogx-04` rebuilds features,
  trains, gates and deploys; every champion is a version of `ogx-corrosion` in the AI Registry and
  every step is a row in `ref.model_event` (docs/MLOPS.md).

### How the two layers divide the work

| Question | Answered by |
|---|---|
| Did every flight arrive, intact and once? | Spark: manifest, contract, sha256, quarantine, recon |
| Which asset was flown, even when the file name is wrong? | Spark: GPS against the asset master, review queue |
| What did the camera see, frame by frame, and where? | Spark: keyframes, GPS, `silver.video_keyframe` |
| Is the frame fit to judge? | CAI: pixel-level frame checks, out-of-distribution check |
| Is the asset corroding, and how urgently? | CAI: corrosion champion, bands, safety floor |
| Is the model still right? Does it learn from engineers? | CAI: nightly drift, feedback labels, retraining, AI Registry |

### Next steps

- **Batch scoring into gold (`ogx-06`, planned).** Score each night's keyframes once and write
  `gold.fact_keyframe_score` (score, band, guardrail result, champion version), so dashboards,
  the IRE KPI and the Agentic Integrity Monitor read stored scores instead of calling the
  endpoint. It can run in CAI, or in CDE with a python-env (numpy, Pillow, scikit-learn) and
  `mapInPandas` over the same `feature_logic.py`, which keeps Spark's scale and CAI's model.
- **Frame-quality model head (`frame_qc`, planned).** A learned classifier for unfit frames, from
  the degraded library frames, to replace the fixed thresholds.
- **Stronger features.** The ViT backbone in `config/pipeline.yaml` is the planned upgrade of the
  hand-crafted features.
- **Real drone video.** An ffmpeg / PyAV extractor for H.264, scene-change detection on pixels, and
  timestamp-interpolated GPS, in CDE with a python-env or in CAI.

One shortcut to know: the CAI endpoint and drift job look a keyframe up in the committed library
`assets/frames/` by its sha256 (the generated videos are built from it). A real deployment would
read `raw/frames/<sha256>.jpg`, which the Spark layer already writes.
