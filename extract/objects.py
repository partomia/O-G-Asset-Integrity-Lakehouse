"""
Object contracts (contracts/objects/<format>.json): what a valid object of each format is.
validate() is what bronze runs on every landed file; a failure goes to bronze.quarantine with a
reason code and is never extracted or scored. Standard library only.

  format_of(file name) -> pdf | png | segy | las | avi | json | None
  validate(fmt, data, contract) -> (ok, reason_code, facts)
"""
from __future__ import annotations

import json

EXT = {"pdf": "pdf", "png": "png", "sgy": "segy", "segy": "segy", "las": "las", "avi": "avi", "json": "json"}


def format_of(name: str) -> str | None:
    return EXT.get(name.rsplit(".", 1)[-1].lower()) if "." in name else None


def validate(fmt: str, data: bytes, contract: dict) -> tuple[bool, str | None, dict]:
    if len(data) < contract.get("min_bytes", 1):
        return False, "TOO_SMALL", {}
    try:
        if fmt == "pdf":
            from extract.pdf_text import PdfError, parse

            try:
                doc = parse(data)
            except PdfError as e:
                return False, "PDF_TRUNCATED" if "truncated" in str(e) else "PDF_UNREADABLE", {"error": str(e)}
            if doc["page_count"] < contract.get("min_pages", 1):
                return False, "PDF_NO_PAGES", {}
            text_pages = sum(1 for p in doc["pages"] if p["text"].strip())
            return True, None, {"pages": doc["page_count"], "text_pages": text_pages}
        if fmt == "png":
            from extract.drawing_tags import png_decode

            w, h, _ = png_decode(data)
            return True, None, {"width": w, "height": h}
        if fmt == "segy":
            from extract.segy_headers import SegyError, parse

            try:
                s = parse(data)
            except SegyError as e:
                return False, "SEGY_BAD_HEADER", {"error": str(e)}
            if s["trace_count"] < contract.get("min_traces", 1):
                return False, "SEGY_NO_TRACES", {}
            return True, None, {"traces": s["trace_count"], "samples": s["samples_per_trace"]}
        if fmt == "las":
            from extract.las_curves import LasError, parse

            try:
                las = parse(data.decode("ascii", errors="replace"))
            except LasError as e:
                return False, "LAS_MISSING_SECTION" if "missing" in str(e) else "LAS_BAD_DATA", {"error": str(e)}
            return True, None, {"rows": len(las["rows"]), "curves": len(las["curves"])}
        if fmt == "avi":
            from extract.video_keyframes import AviError, parse

            try:
                v = parse(data)
            except AviError as e:
                return False, "VIDEO_UNDECODABLE", {"error": str(e)}
            if v["duration_s"] < contract.get("min_seconds", 0):
                return False, "VIDEO_TOO_SHORT", {"duration_s": v["duration_s"]}
            return True, None, {"frames": v["frame_count"], "keyframes": len(v["keyframes"]),
                                "duration_s": v["duration_s"]}
        if fmt == "json":
            try:
                doc = json.loads(data)
            except ValueError as e:
                return False, "JSON_MALFORMED", {"error": str(e)[:200]}
            missing = [k for k in contract.get("required_keys", []) if k not in doc]
            if missing:
                return False, "JSON_MISSING_KEYS", {"missing": missing}
            return True, None, {"keys": len(doc)}
    except Exception as e:  # any parser crash is an invalid object, never a failed batch
        return False, "UNREADABLE", {"error": f"{type(e).__name__}: {e}"[:300]}
    return False, "UNKNOWN_FORMAT", {}
