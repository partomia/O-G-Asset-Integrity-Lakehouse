"""
Every batch source for one business date, with the _manifest.json each source sends:
file name, size, sha256 and the record / page / frame counts as the source reports them.
Reconciliation is always against the manifest. Standard library only.

  batch(date) -> {source: [(file name, bytes)] including _manifest.json}
"""
from __future__ import annotations

import hashlib
import json
from datetime import date

from generators import asset_universe as U
from generators import gen_documents, gen_drawings, gen_erp, gen_seismic, gen_videos, gen_well_logs


def _first_day(business_date: date) -> bool:
    return business_date.isoformat() == U.CFG["business_dates"][0]


def source_files(source: str, business_date: date) -> list[tuple[str, bytes, dict]]:
    first = _first_day(business_date)
    if source == "erp":
        return gen_erp.files(business_date, first)
    if source == "inspection":
        return gen_documents.files(business_date)
    if source == "drone":
        return gen_videos.files(business_date)
    if source == "drawings":
        return gen_drawings.files(business_date, first)
    if source == "seismic":
        return gen_seismic.files(business_date, first)
    if source == "welllogs":
        return gen_well_logs.files(business_date, first)
    raise ValueError(source)


def manifest(source: str, business_date: date, files: list[tuple[str, bytes, dict]]) -> bytes:
    return json.dumps({
        "source": source, "business_date": business_date.isoformat(), "batch_id": f"B{business_date:%Y%m%d}",
        "file_count": len(files),
        "files": [{"name": n, "size": len(b), "sha256": hashlib.sha256(b).hexdigest(), **meta}
                  for n, b, meta in files],
    }, indent=1).encode()


def batch(business_date: date, sources=None) -> dict[str, list[tuple[str, bytes]]]:
    out = {}
    for source in sources or U.CFG["sources"]:
        files = source_files(source, business_date)
        if not files:
            continue
        out[source] = [(n, b) for n, b, _ in files] + [("_manifest.json", manifest(source, business_date, files))]
    return out
