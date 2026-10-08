"""
LAS 2.0 well logs, standard library only: ~V, ~W, ~C, ~P headers and the ~A data block.

  parse(text) -> {"version", "well": {mnemonic: value}, "curves": [(mnemonic, unit, desc)],
                  "rows": [[depth, v1, ...]], "null": -999.25}
"""
from __future__ import annotations

import re

HEADER_RE = re.compile(r"^\s*([^.\s]+)\s*\.(\S*)\s+(.*?)\s*:\s*(.*)$")


class LasError(ValueError):
    pass


def parse(text: str) -> dict:
    sections: dict[str, list[str]] = {}
    current = None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.startswith("~"):
            current = line[1].upper()
            sections.setdefault(current, [])
            continue
        if current:
            sections[current].append(line)
    missing = [s for s in "VWCA" if s not in sections]
    if missing:
        raise LasError(f"missing sections {['~' + s for s in missing]}")

    def header(lines):
        out = []
        for ln in lines:
            m = HEADER_RE.match(ln)
            if m:
                out.append((m.group(1).strip().upper(), m.group(2), m.group(3).strip(), m.group(4).strip()))
        return out

    version = {m: v for m, _, v, _ in header(sections["V"])}
    well = {m: v for m, _, v, _ in header(sections["W"])}
    curves = [(m, u, d) for m, u, _, d in header(sections["C"])]
    params = {m: v for m, _, v, _ in header(sections.get("P", []))}
    try:
        null = float(well.get("NULL", "-999.25"))
    except ValueError:
        null = -999.25
    rows = []
    for ln in sections["A"]:
        vals = ln.split()
        if len(vals) != len(curves):
            raise LasError(f"data row with {len(vals)} values for {len(curves)} curves")
        rows.append([None if float(v) == null else float(v) for v in vals])
    if not curves or not rows:
        raise LasError("no curves or no data")
    return {"version": version.get("VERS"), "well": well, "curves": curves, "params": params, "rows": rows,
            "null": null}
