"""
PDF text per page, standard library only: object parser, FlateDecode streams, the text-showing
operators (Tj, TJ, ', ") positioned by Td/Tm, and image-only pages handed back for OCR
(extract/ocr.py). Enough for the generated reports and drawings and for simple real PDFs;
a production pipeline would put pdfminer / a document AI service in the same slot.

  parse(data) -> {"page_count", "pages": [{"page_no", "text", "image": (w, h, gray) | None}], "info"}
  report_fields(text) -> the inspection-report fields (wall loss, coating, inspector, ...)
"""
from __future__ import annotations

import re
import zlib

REF_RE = re.compile(rb"(\d+)\s+0\s+R")


class PdfError(ValueError):
    pass


OBJ_START_RE = re.compile(rb"(\d+)\s+(\d+)\s+obj\b")


def _objects(data: bytes) -> dict[int, bytes]:
    """Object number -> body. Streams are skipped by their /Length, so compressed bytes that
    happen to contain 'endobj' cannot cut an object short."""
    out, pos = {}, 0
    while True:
        m = OBJ_START_RE.search(data, pos)
        if not m:
            return out
        start = m.end()
        s = data.find(b"stream", start)
        e = data.find(b"endobj", start)
        if e < 0:
            return out
        if 0 <= s < e:
            lm = re.search(rb"/Length\s+(\d+)(?!\s+\d+\s+R)", data[start:s])
            if lm:
                body_start = s + len(b"stream")
                body_start += 2 if data[body_start:body_start + 2] == b"\r\n" else 1
                e = data.find(b"endobj", body_start + int(lm.group(1)))
                if e < 0:
                    return out
        out[int(m.group(1))] = data[start:e]
        pos = e + len(b"endobj")


def _dict_part(body: bytes) -> bytes:
    i = body.find(b"stream")
    return body if i < 0 else body[:i]


def _stream(body: bytes) -> bytes:
    i = body.find(b"stream")
    if i < 0:
        return b""
    start = i + len(b"stream")
    if body[start:start + 2] == b"\r\n":
        start += 2
    elif body[start:start + 1] in (b"\n", b"\r"):
        start += 1
    lm = re.search(rb"/Length\s+(\d+)(?!\s+\d+\s+R)", body[:i])
    if lm:
        raw = body[start:start + int(lm.group(1))]
    else:
        raw = body[start:body.rfind(b"endstream")].rstrip(b"\r\n")
    if b"/FlateDecode" in _dict_part(body):
        try:
            return zlib.decompress(raw)
        except zlib.error as e:
            raise PdfError(f"bad FlateDecode stream: {e}") from e
    return raw


def _unescape(s: bytes) -> str:
    out, i = bytearray(), 0
    while i < len(s):
        c = s[i]
        if c == 0x5C and i + 1 < len(s):  # backslash
            n = s[i + 1]
            mapping = {ord("n"): 10, ord("r"): 13, ord("t"): 9, ord("b"): 8, ord("f"): 12}
            if n in mapping:
                out.append(mapping[n])
                i += 2
            elif 0x30 <= n <= 0x37:
                j = i + 1
                while j < len(s) and j < i + 4 and 0x30 <= s[j] <= 0x37:
                    j += 1
                out.append(int(s[i + 1:j], 8) & 0xFF)
                i = j
            else:
                out.append(n)
                i += 2
        else:
            out.append(c)
            i += 1
    return out.decode("latin-1")


STR_RE = re.compile(rb"\((?:\\.|[^\\)])*\)")
TOKEN_RE = re.compile(rb"\((?:\\.|[^\\)])*\)|\[(?:\((?:\\.|[^\\)])*\)|[^\]])*\]|[-+]?\d*\.?\d+|/[^\s/\[\]()<>]+|[A-Za-z'\"*]+")


def _content_text(content: bytes) -> str:
    """Text of a content stream, lines ordered top to bottom by their y position."""
    pieces: list[tuple[float, float, str]] = []
    stack: list[bytes] = []
    x = y = 0.0
    for tok in TOKEN_RE.findall(content):
        if tok in (b"Td", b"TD") and len(stack) >= 2:
            try:
                x, y = x + float(stack[-2]), y + float(stack[-1])
            except ValueError:
                pass
        elif tok == b"Tm" and len(stack) >= 6:
            try:
                x, y = float(stack[-2]), float(stack[-1])
            except ValueError:
                pass
        elif tok == b"BT":
            x = y = 0.0
        elif tok in (b"Tj", b"'", b'"') and stack and stack[-1].startswith(b"("):
            pieces.append((y, x, _unescape(stack[-1][1:-1])))
        elif tok == b"TJ" and stack and stack[-1].startswith(b"["):
            pieces.append((y, x, "".join(_unescape(s[1:-1]) for s in STR_RE.findall(stack[-1]))))
        elif tok == b"T*":
            y -= 12
        if tok[:1] in (b"(", b"[") or re.match(rb"^[-+]?\d*\.?\d+$", tok):
            stack.append(tok)
        else:
            stack = []
    lines: list[tuple[float, list[tuple[float, str]]]] = []
    for py, px, t in sorted(pieces, key=lambda p: (-p[0], p[1])):
        if lines and abs(lines[-1][0] - py) < 2.0:
            lines[-1][1].append((px, t))
        else:
            lines.append((py, [(px, t)]))
    return "\n".join(" ".join(t for _, t in sorted(parts)) for _, parts in lines)


def _num(d: bytes, key: bytes) -> int | None:
    m = re.search(rb"/" + key + rb"\s+(\d+)", d)
    return int(m.group(1)) if m else None


def parse(data: bytes) -> dict:
    if not data.startswith(b"%PDF-"):
        raise PdfError("no %PDF- header")
    if b"%%EOF" not in data[-1024:]:
        raise PdfError("truncated: no %%EOF trailer")
    objs = _objects(data)
    if not objs:
        raise PdfError("no objects")
    pages_root = next((o for o in objs.values() if re.search(rb"/Type\s*/Pages\b", _dict_part(o))), None)
    if pages_root is None:
        raise PdfError("no page tree")
    kids_m = re.search(rb"/Kids\s*\[(.*?)\]", pages_root, re.S)
    page_ids = [int(r) for r in REF_RE.findall(kids_m.group(1))] if kids_m else []
    info = {}
    for o in objs.values():
        d = _dict_part(o)
        if b"/Producer" in d or b"/Title" in d:
            for k, v in re.findall(rb"/(\w+)\s*\(((?:\\.|[^\\)])*)\)", d):
                info[k.decode()] = _unescape(v)
    pages = []
    for no, pid in enumerate(page_ids, 1):
        page = objs.get(pid)
        if page is None:
            raise PdfError(f"page object {pid} missing")
        pd = _dict_part(page)
        text, image = "", None
        cm = re.search(rb"/Contents\s+(\d+)\s+0\s+R", pd)
        if cm and int(cm.group(1)) in objs:
            text = _content_text(_stream(objs[int(cm.group(1))]))
        xm = re.search(rb"/XObject\s*<<(.*?)>>", pd, re.S)
        if xm:
            for ref in REF_RE.findall(xm.group(1)):
                body = objs.get(int(ref))
                if body is not None and b"/Image" in _dict_part(body):
                    d = _dict_part(body)
                    w, h, bpc = _num(d, b"Width"), _num(d, b"Height"), _num(d, b"BitsPerComponent")
                    if w and h and bpc == 8 and b"/DeviceGray" in d:
                        image = (w, h, _stream(body))
        pages.append({"page_no": no, "text": text, "image": image})
    if not pages:
        raise PdfError("no pages")
    return {"page_count": len(pages), "pages": pages, "info": info}


# ---------------------------------------------------------------- inspection report fields

FIELDS = {
    "report_no": (r"REPORT NO:\s*(IR-\d{8}-\d{3})", str),
    "inspection_date": (r"INSPECTION DATE:\s*(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2})?)", str),
    "facility": (r"FACILITY:\s*([A-Z0-9-]+)", str),
    "equipment": (r"EQUIPMENT:\s*([A-Z0-9-]+)", str),
    "asset_class": (r"ASSET CLASS:\s*([A-Z_]+)", str),
    "method": (r"METHOD:\s*([A-Z +]+?)\s*$", str),
    "inspector": (r"INSPECTOR:\s*([A-Z][A-Z. ]+?)\s*$", str),
    "nominal_mm": (r"NOMINAL THICKNESS \(MM\):\s*([\d.]+)", float),
    "min_measured_mm": (r"MINIMUM MEASURED \(MM\):\s*([\d.]+)", float),
    "wall_loss_pct": (r"WALL LOSS \(%\):\s*([\d.]+)", float),
    "coating": (r"COATING CONDITION:\s*(GOOD|FAIR|POOR)", str),
    "corrosion_type": (r"CORROSION TYPE:\s*(PITTING|GENERAL|NONE)", str),
    "cui": (r"CUI OBSERVED:\s*(YES|NO)", str),
    "recommended_action": (r"RECOMMENDED ACTION:\s*(.+?)(?:\n\n|\Z)", str),
    "findings": (r"FINDINGS:\s*(.+?)(?=\nRECOMMENDED ACTION:)", str),
}


def report_fields(text: str) -> dict:
    """The fields of a generated inspection report, from a text layer or OCR (case-insensitive)."""
    up = text.upper()
    out = {}
    for k, (pattern, kind) in FIELDS.items():
        m = re.search(pattern, up, re.M | re.S)
        if not m:
            out[k] = None
            continue
        v = " ".join(m.group(1).split())
        try:
            out[k] = kind(v.rstrip(".")) if kind is float else v
        except ValueError:
            out[k] = None
    return out
