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
