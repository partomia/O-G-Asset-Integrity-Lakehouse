"""
Inspection reports: inspection/<date>/IR_<yyyymmdd>_<asset ref>_<nn>.pdf, ~30 a day.

Most have a text layer; ~20 % are image-only scans (rendered with the 5x7 scanner font, so
the extractor has to fall back to OCR); one on 2026-10-05 is truncated (corrupt). The asset
reference in the file name is spelled the way crews write it: the EAM functional location,
the bare P&ID tag, or the tag without its dash (V307), which the asset master normalises.

inspection_plan(date) is the truth (what was inspected, measured and found); the extractor
must recover it from the PDFs. Standard library only.
"""
from __future__ import annotations

import zlib
from datetime import date, datetime, timedelta, timezone

from generators import asset_universe as U
from generators import faults
from generators.formats import PDF, scan_lines

METHODS = {"PIPE_SEGMENT": "UT THICKNESS", "TANK": "UT THICKNESS", "VESSEL": "UT THICKNESS",
           "COLUMN": "UT THICKNESS", "HEAT_EXCHANGER": "UT THICKNESS", "FLARE_STACK": "VISUAL + UT",
           "PUMP": "VISUAL + VIBRATION", "COMPRESSOR": "VISUAL + VIBRATION", "WELL": "WELLHEAD VISUAL",
           "VALVE": "VISUAL", "RELIEF_VALVE": "VISUAL + BENCH TEST"}
NOMINAL_MM = {"PIPE_SEGMENT": 9.53, "TANK": 8.0, "VESSEL": 12.7, "COLUMN": 14.0, "HEAT_EXCHANGER": 6.35,
              "FLARE_STACK": 7.11, "PUMP": 10.0, "COMPRESSOR": 12.0, "WELL": 11.05, "VALVE": 10.0,
              "RELIEF_VALVE": 6.0}
FOCUS = {  # planted degradation the crews inspect on given days (day index 0..4)
    0: ["V-307", "PL-03-SEG-027"], 1: ["TK-504", "FL-701"], 2: ["PL-03-SEG-027", "PL-03-SEG-052"],
    3: ["V-307", "PL-03-SEG-041"], 4: ["FL-701", "TK-504"],
}


def finding_severity(wall_loss: float, coating: str, corrosion_type: str) -> str:
    """The rule an engineer applies to a report (also the extractor's, on the extracted values)."""
    if wall_loss >= 20.0 or (coating == "POOR" and corrosion_type == "PITTING"):
        return "severe"
    if wall_loss >= 10.0 or coating in ("FAIR", "POOR"):
        return "surface"
    return "none"


def _day_index(business_date: date) -> int:
    return (business_date - U.EPOCH.date()).days


def inspection_plan(business_date: date) -> list[dict]:
    r = U.rng("inspections", business_date)
    di = _day_index(business_date)
    n = U.CFG["daily"]["inspection_reports"]
    xs = list(U.assets())
    focus = [U.by_tag()[t] for t in FOCUS.get(di % 5, [])]
    overdue = sorted(xs, key=lambda a: a.due_date())[:60]
    chosen = list(focus)
    while len(chosen) < n:
        a = r.choice(overdue) if r.random() < 0.6 else r.choice(xs)
        if a not in chosen:
            chosen.append(a)
    out = []
    for i, a in enumerate(chosen, 1):
        captured = datetime.combine(business_date, datetime.min.time(), timezone.utc) + \
            timedelta(minutes=r.randint(6 * 60, 16 * 60))
        day = U.days_since_epoch(captured)
        wl = U.wall_loss_pct(a.tag, day, r)
        nominal = NOMINAL_MM[a.asset_class]
        sev_deg = U.severity_at(a.tag, day)
        coating = "POOR" if sev_deg > 0.6 or wl > 22 else ("FAIR" if sev_deg > 0.2 or wl > 8 or r.random() < 0.2
                                                         else "GOOD")
        ctype = "PITTING" if (sev_deg > 0.4 or wl > 18) else r.choice(["GENERAL", "NONE", "NONE"])
        if wl < 5 and coating == "GOOD":
            ctype = "NONE"
        cui = "YES" if a.asset_class in ("VESSEL", "PIPE_SEGMENT", "TANK") and (sev_deg > 0.5 or r.random() < 0.08) \
            else "NO"
        ref_style = r.choice(["func_loc", "tag", "nodash"])
        ref = {"func_loc": a.func_loc, "tag": a.tag, "nodash": a.tag.replace("-", "")}[ref_style]
        sev = finding_severity(wl, coating, ctype)
        action = {"severe": "Repair or replace within 30 days; reduce operating pressure; re-inspect in 14 days",
                  "surface": "Recoat and monitor; re-inspect in 6 months",
                  "none": "No action; next inspection per plan"}[sev]
        out.append({
            "report_no": f"IR-{business_date:%Y%m%d}-{i:03d}", "asset_tag": a.tag, "func_loc": a.func_loc,
            "file_ref": ref, "asset_class": a.asset_class, "facility_id": a.facility_id,
            "method": METHODS[a.asset_class], "inspector": r.choice(U.INSPECTORS),
            "captured_at": captured.strftime("%Y-%m-%d %H:%M"), "nominal_mm": nominal,
            "min_measured_mm": round(nominal * (1 - wl / 100.0), 2), "wall_loss_pct": wl,
            "coating": coating, "corrosion_type": ctype, "cui": cui, "severity": sev, "action": action,
            "scanned": r.random() < faults.SCANNED_FRACTION,
            "corrupt": business_date.isoformat() == faults.CORRUPT_PDF_DATE and i == 7,
            "file": f"IR_{business_date:%Y%m%d}_{ref}_{i:02d}.pdf",
            "tml": [round(nominal * (1 - max(0.0, wl - r.uniform(0, 6)) / 100.0), 2) for _ in range(8)],
        })
    return out


def report_lines(p: dict) -> list[list[str]]:
    """Pages of text lines (the same words on a text page and on a scan)."""
    findings = {
        "severe": f"Localised {p['corrosion_type'].lower()} corrosion with wall thinning to "
                  f"{p['min_measured_mm']:.2f} mm. Coating breakdown over the affected area.",
        "surface": f"Surface corrosion and coating deterioration. Minimum thickness {p['min_measured_mm']:.2f} mm.",
        "none": "No significant corrosion or mechanical damage observed.",
    }[p["severity"]]
    page1 = [
        "OGX ENERGY - ASSET INTEGRITY INSPECTION REPORT",
        "",
        f"Report No: {p['report_no']}",
        f"Inspection Date: {p['captured_at']}",
        f"Facility: {p['facility_id']}",
        f"Equipment: {p['func_loc'] if p['file_ref'] == p['func_loc'] else p['asset_tag']}",
        f"Asset Class: {p['asset_class']}",
        f"Method: {p['method']}",
        f"Inspector: {p['inspector']}",
        "",
        f"Nominal Thickness (mm): {p['nominal_mm']:.2f}",
        f"Minimum Measured (mm): {p['min_measured_mm']:.2f}",
        f"Wall Loss (%): {p['wall_loss_pct']:.1f}",
        f"Coating Condition: {p['coating']}",
        f"Corrosion Type: {p['corrosion_type']}",
        f"CUI Observed: {p['cui']}",
        "",
        f"Findings: {findings}",
        f"Recommended Action: {p['action']}",
    ]
    page2 = ["THICKNESS MEASUREMENT LOCATIONS (TML)", "", "TML  READING (MM)"] + \
            [f"{i + 1:>3}  {v:.2f}" for i, v in enumerate(p["tml"])] + ["", f"Reviewed: pending ({p['report_no']})"]
    return [page1, page2]


def _wrap(text: str, width: int) -> list[str]:
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > width:
            lines.append(cur)
            cur = w
        else:
            cur = (cur + " " + w).strip()
    return lines + ([cur] if cur else [])


def report_pdf(p: dict) -> bytes:
    pdf = PDF({"Title": f"Inspection report {p['report_no']}", "Producer": "OGX synthetic generator"})
    for pi, lines in enumerate(report_lines(p)):
        wrapped = [w for ln in lines for w in (_wrap(ln, 74) or [""])]
        if p["scanned"]:
            pdf.image_page(1100, 1424, scan_lines(wrapped, seed=zlib.crc32(f"{p['report_no']}:{pi}".encode())))
        else:
            pdf.text_page([(54, 740 - 16 * k, 13 if (k == 0 and pi == 0) else 10, t)
                           for k, t in enumerate(wrapped)])
    data = pdf.tobytes()
    return data[: len(data) // 3] if p["corrupt"] else data


def files(business_date: date) -> list[tuple[str, bytes, dict]]:
    out = []
    for p in inspection_plan(business_date):
        out.append((p["file"], report_pdf(p), {"pages": 2, "report_no": p["report_no"]}))
    return out
