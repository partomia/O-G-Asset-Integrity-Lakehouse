"""Extractors recover the generators' truth; object contracts reject the planted bad files."""
import json
from datetime import date

import pytest

from extract import las_curves, segy_headers, video_keyframes
from extract.drawing_tags import tags_from_pdf, tags_from_png
from extract.objects import validate
from extract.ocr import ocr_page
from extract.pdf_text import PdfError, parse, report_fields
from generators import gen_documents, gen_drawings, gen_seismic, gen_videos, gen_well_logs

D = date(2026, 10, 5)
CONTRACTS = {p: json.load(open(f"contracts/objects/{p}.json")) for p in ("pdf", "png", "segy", "las", "avi", "json")}


def _text(doc):
    return "\n".join(p["text"] or (ocr_page(p["image"]) if p["image"] else "") for p in doc["pages"])


def test_text_and_scanned_reports_give_the_same_fields():
    plan = gen_documents.inspection_plan(D)
    text_p = next(p for p in plan if not p["scanned"] and not p["corrupt"])
    scan_p = next(p for p in plan if p["scanned"] and not p["corrupt"])
    for p in (text_p, scan_p):
        doc = parse(gen_documents.report_pdf(p))
        f = report_fields(_text(doc))
        assert f["report_no"] == p["report_no"]
        assert f["wall_loss_pct"] == p["wall_loss_pct"]
        assert f["coating"] == p["coating"] and f["corrosion_type"] == p["corrosion_type"]
        assert f["inspector"] == p["inspector"].upper()
    assert not parse(gen_documents.report_pdf(scan_p))["pages"][0]["text"]


def test_corrupt_pdf_is_quarantined():
    p = next(p for p in gen_documents.inspection_plan(D) if p["corrupt"])
    data = gen_documents.report_pdf(p)
    with pytest.raises(PdfError):
        parse(data)
    ok, reason, _ = validate("pdf", data, CONTRACTS["pdf"])
    assert not ok and reason == "PDF_TRUNCATED"


def test_drawing_tags_text_and_ocr_agree():
    sheet = "PID-REF-U1-014"
    truth = gen_drawings.sheet_tags(sheet, "B")
    a = tags_from_pdf(gen_drawings.drawing_pdf(sheet, "B", D))
    b = tags_from_png(gen_drawings.drawing_png(sheet, "B", D))
    assert a["tags"] == truth == b["tags"]
    assert "PSV-868" in truth and a["revision"] == b["revision"] == "B"


def test_segy_and_las():
    s = segy_headers.parse(gen_seismic.segy("L102"))
    assert s["trace_count"] == 240 and s["samples_per_trace"] == 500 and s["line_name"] == "L102"
    las = las_curves.parse(gen_well_logs.las("W-03", 1, D).decode())
    assert [c[0] for c in las["curves"]] == ["DEPT", "GR", "RT", "NPHI", "RHOB"]
    assert len(las["rows"]) == 3001
    ok, reason, _ = validate("las", b"~VERSION\n VERS. 2.0 : x\n~ASCII\n1 2\n" * 10, CONTRACTS["las"])
    assert not ok and reason == "LAS_MISSING_SECTION"


def test_video_keyframes_are_library_frames():
    v = gen_videos.video_plan(D)[0]
    parsed = video_keyframes.parse(gen_videos.video_bytes(v))
    assert [k["sha256"][:16] + ".jpg" for k in parsed["keyframes"]] == [f["frame"] for f in v["keyframes"]]
    assert parsed["duration_s"] == v["duration_s"]
    ok, _, facts = validate("avi", gen_videos.video_bytes(v), CONTRACTS["avi"])
    assert ok and facts["keyframes"] == len(v["keyframes"])
