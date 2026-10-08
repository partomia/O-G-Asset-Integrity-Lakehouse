"""
Engineering drawings: drawings/<date>/<sheet>_REV<r>.pdf and .png

20 P&ID sheets on day 1 (14 refinery, 4 pipeline, 2 field), each showing ~10 equipment
tags as symbols with tag labels (text layer in the PDF) and an equipment list block under
the title line (rendered with the scanner font in the PNG, read by OCR). Revisions later:
PID-REF-U1-003 rev B on 2026-10-06 (a tag removed from the list), PID-REF-U1-014 rev B on
2026-10-07 (the new PSV-868 drawn in). Standard library only.
"""
from __future__ import annotations

from datetime import date

from generators import asset_universe as U
from generators import faults
from generators.formats import PDF, circle, gray_canvas, line, png_gray, rect
from extract.font5x7 import CELL_W, LINE_H, draw_text

PNG_W, PNG_H, LIST_TOP = 1400, 1000, 760
REVISIONS = {"2026-10-06": [("PID-REF-U1-003", "B")], "2026-10-07": [("PID-REF-U1-014", "B")]}


def sheets() -> dict[str, list[str]]:
    """sheet -> equipment tags drawn on it (revision A)."""
    xs = U.assets()
    ref = [a.tag for a in xs if a.facility_id == "REF-U1"]
    pl = [a.tag for a in xs if a.facility_id == "PL-03"]
    fld = [a.tag for a in xs if a.facility_id == "FLD-01"]
    out = {}
    per = -(-len(ref) // 14)
    for i in range(14):
        out[f"PID-REF-U1-{i + 1:03d}"] = ref[i * per:(i + 1) * per]
    for i in range(4):
        out[f"PID-PL03-{i + 1:03d}"] = pl[i * 15:(i + 1) * 15]
    for i in range(2):
        out[f"PID-FLD01-{i + 1:03d}"] = fld[i * 6:(i + 1) * 6]
    return out


def sheet_tags(sheet: str, rev: str) -> list[str]:
    tags = list(sheets()[sheet])
    if sheet == "PID-REF-U1-003" and rev >= "B":
        tags = tags[:-1]
    if sheet == faults.NEW_ASSET["sheet"] and rev >= "B":
        tags.append(faults.NEW_ASSET["tag"])
    return tags


def _layout(tags: list[str]) -> list[tuple[int, int, str]]:
    """(x, y, tag) symbol positions on a 1400 x 760 drawing area, 5 per row."""
    return [(120 + (i % 5) * 250, 120 + (i // 5) * 200, t) for i, t in enumerate(tags)]


def drawing_pdf(sheet: str, rev: str, business_date: date) -> bytes:
    tags = sheet_tags(sheet, rev)
    pdf = PDF({"Title": f"{sheet} rev {rev}", "Producer": "OGX synthetic generator"})
    art, text = ["1 w"], []
    sx, sy = 612 / PNG_W, 792 / PNG_H
    for x, y, t in _layout(tags):
        px, py = x * sx, 792 - y * sy
        art.append(f"{px - 18:.1f} {py - 18:.1f} 36 36 re S")
        art.append(f"{px + 18:.1f} {py:.1f} m {px + 60:.1f} {py:.1f} l S")
        text.append((px - 18, py - 30, 7, t))
    art.append(f"20 {792 - LIST_TOP * sy:.1f} m 592 {792 - LIST_TOP * sy:.1f} l S")
    text += [(30, 792 - (LIST_TOP + 30) * sy, 9, f"P&ID {sheet} REV {rev}  DATE {business_date.isoformat()}"),
             (30, 792 - (LIST_TOP + 55) * sy, 8, "EQUIPMENT LIST: " + " ".join(tags))]
    pdf.text_page(text, art)
    return pdf.tobytes()


def drawing_png(sheet: str, rev: str, business_date: date) -> bytes:
    tags = sheet_tags(sheet, rev)
    px = gray_canvas(PNG_W, PNG_H)
    for x, y, t in _layout(tags):
        if t.startswith(("P-", "K-")):
            circle(px, PNG_W, x, y, 30)
        else:
            rect(px, PNG_W, x - 30, y - 30, x + 30, y + 30)
        line(px, PNG_W, x + 30, y, x + 120, y)
        draw_text(px, PNG_W, x - 30, y + 40, t)
    for x in range(20, PNG_W - 20):
        for dy in range(3):
            px[(LIST_TOP + dy) * PNG_W + x] = 0
    block = [f"P&ID {sheet} REV {rev} DATE {business_date.isoformat()}", "EQUIPMENT LIST:"]
    row = ""
    for t in tags:
        if len(row) + len(t) + 1 > (PNG_W - 120) // CELL_W:
            block.append(row)
            row = t
        else:
            row = (row + " " + t).strip()
    block.append(row)
    for i, s in enumerate(block):
        draw_text(px, PNG_W, 60, LIST_TOP + 20 + i * LINE_H, s)
    return png_gray(PNG_W, PNG_H, bytes(px))


def files(business_date: date, first_day: bool) -> list[tuple[str, bytes, dict]]:
    todo = [(s, "A") for s in sheets()] if first_day else REVISIONS.get(business_date.isoformat(), [])
    out = []
    for sheet, rev in todo:
        meta = {"sheet": sheet, "revision": rev, "tags": len(sheet_tags(sheet, rev))}
        out.append((f"{sheet}_REV{rev}.pdf", drawing_pdf(sheet, rev, business_date), meta))
        out.append((f"{sheet}_REV{rev}.png", drawing_png(sheet, rev, business_date), meta))
    return out
