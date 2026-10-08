"""
Equipment tags on a P&ID sheet: from the PDF text layer, or by OCR of the equipment list
block of the PNG (the rows under the sheet's full-width title line). Standard library only.

  tags_from_pdf(data)  -> {"sheet", "revision", "tags": [...], "method": "TEXT"}
  tags_from_png(data)  -> {"sheet", "revision", "tags": [...], "method": "OCR"}
"""
from __future__ import annotations

import re
import struct
import zlib

from extract.ocr import ocr_gray
from extract.pdf_text import parse

TAG_RE = re.compile(r"\b(PL-03-SEG-\d{3}|W-\d{2}|(?:PSV|XV|TK|FL|[PKVEC])-\d{3}[AB]?)\b")
SHEET_RE = re.compile(r"P&?ID\s+(PID-[A-Z0-9-]+?)\s+REV\s+([A-Z])\b")


def _parse_text(text: str) -> tuple[str | None, str | None, list[str]]:
    up = text.upper()
    m = SHEET_RE.search(up)
    i = up.find("EQUIPMENT LIST:")
    body = up[i:] if i >= 0 else up
    tags = list(dict.fromkeys(TAG_RE.findall(body)))
    return (m.group(1) if m else None), (m.group(2) if m else None), tags


def tags_from_pdf(data: bytes) -> dict:
    doc = parse(data)
    text = "\n".join(p["text"] for p in doc["pages"])
    sheet, rev, tags = _parse_text(text)
    return {"sheet": sheet, "revision": rev, "tags": tags, "method": "TEXT"}


def png_decode(data: bytes) -> tuple[int, int, bytes]:
    """8-bit grayscale PNG -> (w, h, gray). Filters 0-4 supported."""
    if not data.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("not a PNG")
    i, idat = 8, bytearray()
    w = h = 0
    while i < len(data):
        n = struct.unpack(">I", data[i:i + 4])[0]
        kind = data[i + 4:i + 8]
        body = data[i + 8:i + 8 + n]
        if kind == b"IHDR":
            w, h, depth, ctype = struct.unpack(">IIBB", body[:10])
            if depth != 8 or ctype != 0:
                raise ValueError("only 8-bit grayscale PNG")
        elif kind == b"IDAT":
            idat += body
        elif kind == b"IEND":
            break
        i += 12 + n
    raw = zlib.decompress(bytes(idat))
    out = bytearray(w * h)
    prev = bytearray(w)
    for y in range(h):
        f = raw[y * (w + 1)]
        line = bytearray(raw[y * (w + 1) + 1:(y + 1) * (w + 1)])
        for x in range(w):
            a = line[x - 1] if x else 0
            b = prev[x]
            c = prev[x - 1] if x else 0
            if f == 1:
                line[x] = (line[x] + a) & 0xFF
            elif f == 2:
                line[x] = (line[x] + b) & 0xFF
            elif f == 3:
                line[x] = (line[x] + (a + b) // 2) & 0xFF
            elif f == 4:
                p = a + b - c
                pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
                line[x] = (line[x] + (a if pa <= pb and pa <= pc else b if pb <= pc else c)) & 0xFF
        out[y * w:(y + 1) * w] = line
        prev = line
    return w, h, bytes(out)


def tags_from_png(data: bytes) -> dict:
    w, h, gray = png_decode(data)
    sep = None
    for y in range(h):
        row = gray[y * w:(y + 1) * w]
        if sum(1 for v in row if v < 128) > 0.8 * w:
            sep = y
    if sep is None:
        return {"sheet": None, "revision": None, "tags": [], "method": "OCR"}
    top = sep + 4
    text = ocr_gray(w, h - top, gray[top * w:])
    sheet, rev, tags = _parse_text(text.replace("P?ID", "P&ID"))
    return {"sheet": sheet, "revision": rev, "tags": tags, "method": "OCR"}
