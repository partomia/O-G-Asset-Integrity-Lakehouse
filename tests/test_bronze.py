"""Bronze without Spark: the parsers and object validation on generated batches, the planted
source defects bronze must catch, and the recon status rules."""
import hashlib
import json
from datetime import date

import ogx_common as C
from extract.objects import format_of, validate
from generators import faults, land


def _batch(d: str, source: str) -> dict:
    return dict(land.batch(date.fromisoformat(d), [source])[source])


def test_work_order_trailer_and_control_total():
    contract = C.contracts()["erp_work_order"]
    cols = [c["name"] for c in contract["columns"]]
    for d in ("2026-10-05", faults.WO_TRAILER_OFF_DATE):
        files = _batch(d, "erp")
        m = json.loads(files["_manifest.json"])
        wo = next(f for f in m["files"] if f["entity"] == "erp_work_order")
        text = files[wo["name"]].decode()
        rows = list(C.parse_psv(wo["name"], text, cols))
        _, _, data, trailer, total = C.psv_stats(wo["name"], text)
        assert len(rows) == data == wo["records"]
        assert round(sum(float(r[2 + cols.index("cost_usd")]) for r in rows), 2) == wo["control_total"] == total
        assert (trailer != data) == (d == faults.WO_TRAILER_OFF_DATE)


def test_objects_validate_and_corrupt_pdf_quarantined():
    contracts = C.object_contracts()
    files = _batch(faults.CORRUPT_PDF_DATE, "inspection")
    m = json.loads(files["_manifest.json"])
    bad = []
    for f in m["files"]:
        data = files[f["name"]]
        assert hashlib.sha256(data).hexdigest() == f["sha256"]
        fmt = format_of(f["name"])
        ok, reason, _ = validate(fmt, data, contracts[fmt])
        if not ok:
            bad.append((f["name"], reason))
    assert len(bad) == 1 and bad[0][1] == "PDF_TRUNCATED"


def test_seismic_resend_is_byte_identical():
    first = _batch("2026-10-04", "seismic")
    resend = _batch(faults.SEISMIC_RESEND["date"], "seismic")
    shas = {hashlib.sha256(b).hexdigest() for n, b in first.items() if n != "_manifest.json"}
    again = [n for n, b in resend.items() if n != "_manifest.json" and hashlib.sha256(b).hexdigest() in shas]
    assert again and all(faults.SEISMIC_RESEND["line"] in n for n in again)


def test_recon_status_rules():
    from reconcile import Recon

    r = Recon(None, C.Names("x"), date(2026, 10, 4))
    r.add("bronze", "e", "a", 10, 10)
    r.add("bronze", "e", "b", 10, 9, explained_by=-1)
    r.add("bronze", "e", "c", 10, 9)
    assert [x[9] for x in r.rows] == ["MATCHED", "EXPLAINED", "MISMATCH"]
