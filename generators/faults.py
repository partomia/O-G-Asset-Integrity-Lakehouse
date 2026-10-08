"""
Planted faults: one schedule, so validation, quarantine, the asset master and reconciliation
always have the same work to do (docs/PROJECT_LOG.md lists what each one exercises).

Batch (generators/):
  corrupt_pdf          2026-10-05: one inspection report truncated mid-file (object contract)
  scanned_fraction     every day: ~20 % of reports are image-only (forces the OCR fallback)
  wrong_video_tag      2026-10-06: a video of PL-03-SEG-014 named and hinted as PL-03-SEG-041
                       (the sidecar GPS track resolves it; the asset master flags the conflict)
  seismic_resend       2026-10-05: survey L101 re-sent under a new file name (dedup by sha256)
  wo_trailer_off       2026-10-06: work-order trailer count off by one (record recon MISMATCH)
  new_asset            2026-10-07: PSV-868 added by CDC, drawn on PID-REF-U1-014 revision B
  decommissioned       2026-10-08: XV-817 decommissioned by CDC

Stream (stream/producer/):  see config/streaming.json producer.* (late, out-of-order, stuck
sensor, psi -> bar unit change).
"""
from __future__ import annotations

CORRUPT_PDF_DATE = "2026-10-05"
WRONG_VIDEO_TAG = {"date": "2026-10-06", "true_tag": "PL-03-SEG-014", "named_tag": "PL-03-SEG-041"}
SEISMIC_RESEND = {"date": "2026-10-05", "line": "L101"}
WO_TRAILER_OFF_DATE = "2026-10-06"
NEW_ASSET = {"date": "2026-10-07", "equnr": "10000223", "tag": "PSV-868", "sheet": "PID-REF-U1-014"}
DECOMMISSIONED = {"date": "2026-10-08", "tag": "XV-817"}
SCANNED_FRACTION = 0.2
