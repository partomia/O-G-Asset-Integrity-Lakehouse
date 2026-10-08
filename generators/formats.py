"""
Minimal writers for the binary formats the synthetic sources produce, standard library only
(the generators run on the CDE driver, which has no reportlab, OpenCV, segyio or lasio):

  pdf        text pages (Helvetica text layer), image-only "scanned" pages (8-bit gray image
             XObject rendered with extract/font5x7), simple vector line art for P&IDs
  png        8-bit grayscale PNG
  avi        Motion-JPEG AVI (RIFF, one '00dc' chunk per frame, idx1 index)

The SEG-Y and LAS writers live with their generators (gen_seismic.py, gen_well_logs.py).
"""
from __future__ import annotations

import random
import struct
import zlib

from extract.font5x7 import CELL_W, LINE_H, draw_text


# ---------------------------------------------------------------- PDF


def _pdf_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


class PDF:
    """Collects pages, then serialises a valid PDF 1.4 with an xref table."""

    W, H = 612, 792  # US letter, points

    def __init__(self, info: dict | None = None):
        self.objects: list[bytes] = []
        self.pages: list[int] = []
        self.info = info or {}
        self._font = self._add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")

    def _add(self, body: bytes) -> int:
        self.objects.append(body)
        return len(self.objects)

    def _stream(self, data: bytes, extra: str = "") -> int:
        z = zlib.compress(data, 6)
        return self._add(f"<< /Length {len(z)} /Filter /FlateDecode {extra}>>\nstream\n".encode() + z +
                         b"\nendstream")

    def text_page(self, lines: list[tuple[float, float, float, str]], art: list[str] | None = None) -> None:
        """lines: (x, y, font size, text) in points from the bottom-left; art: raw PDF path operators."""
        ops = list(art or [])
        for x, y, size, text in lines:
            ops.append(f"BT /F1 {size:g} Tf {x:.1f} {y:.1f} Td ({_pdf_escape(text)}) Tj ET")
        content = self._stream("\n".join(ops).encode("latin-1", "replace"))
        self.pages.append(self._add(
            f"<< /Type /Page /Parent PARENT /MediaBox [0 0 {self.W} {self.H}] "
            f"/Resources << /Font << /F1 {self._font} 0 R >> >> /Contents {content} 0 R >>".encode()))

    def image_page(self, width: int, height: int, gray: bytes) -> None:
        """One full-page 8-bit grayscale image and no text layer (a scanned page)."""
        img = self._stream(gray, f"/Type /XObject /Subtype /Image /Width {width} /Height {height} "
                                 f"/ColorSpace /DeviceGray /BitsPerComponent 8 ")
        content = self._stream(f"q {self.W} 0 0 {self.H} 0 0 cm /Im1 Do Q".encode())
        self.pages.append(self._add(
            f"<< /Type /Page /Parent PARENT /MediaBox [0 0 {self.W} {self.H}] "
            f"/Resources << /XObject << /Im1 {img} 0 R >> >> /Contents {content} 0 R >>".encode()))

    def tobytes(self) -> bytes:
        kids = " ".join(f"{p} 0 R" for p in self.pages)
        parent = self._add(f"<< /Type /Pages /Kids [{kids}] /Count {len(self.pages)} >>".encode())
        catalog = self._add(f"<< /Type /Catalog /Pages {parent} 0 R >>".encode())
        info = self._add(("<< " + " ".join(f"/{k} ({_pdf_escape(str(v))})" for k, v in self.info.items())
                          + " >>").encode("latin-1", "replace"))
        out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        offsets = []
        for i, body in enumerate(self.objects, 1):
            body = body.replace(b"PARENT", f"{parent} 0 R".encode())
            offsets.append(len(out))
            out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
        xref = len(out)
        out += f"xref\n0 {len(self.objects) + 1}\n0000000000 65535 f \n".encode()
        for off in offsets:
            out += f"{off:010d} 00000 n \n".encode()
        out += (f"trailer\n<< /Size {len(self.objects) + 1} /Root {catalog} 0 R /Info {info} 0 R >>\n"
                f"startxref\n{xref}\n%%EOF\n").encode()
        return bytes(out)


def scan_lines(lines: list[str], width: int = 1100, height: int = 1424, margin: int = 60,
               noise: float = 0.004, seed: int = 0) -> bytes:
    """An 8-bit grayscale 'scan' of monospaced text lines (white paper, speckle noise)."""
    px = bytearray([255]) * (width * height)
    for i, line in enumerate(lines):
        y = margin + i * LINE_H
        if y + LINE_H > height - margin:
            break
        draw_text(px, width, margin, y, line[: (width - 2 * margin) // CELL_W])
    r = random.Random(seed)
    for _ in range(int(width * height * noise)):
        i = r.randrange(width * height)
        px[i] = 0 if px[i] > 128 else 255
    return bytes(px)


# ---------------------------------------------------------------- PNG


def png_gray(width: int, height: int, gray: bytes) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    raw = b"".join(b"\x00" + gray[y * width:(y + 1) * width] for y in range(height))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 6)) + chunk(b"IEND", b""))


def gray_canvas(width: int, height: int) -> bytearray:
    return bytearray([255]) * (width * height)


def line(px: bytearray, width: int, x0: int, y0: int, x1: int, y1: int, ink: int = 0, thick: int = 2) -> None:
    height = len(px) // width
    n = max(abs(x1 - x0), abs(y1 - y0), 1)
    for i in range(n + 1):
        x, y = x0 + (x1 - x0) * i // n, y0 + (y1 - y0) * i // n
        for dx in range(thick):
            for dy in range(thick):
                if 0 <= x + dx < width and 0 <= y + dy < height:
                    px[(y + dy) * width + x + dx] = ink


def rect(px: bytearray, width: int, x0: int, y0: int, x1: int, y1: int, ink: int = 0) -> None:
    for a, b, c, d in ((x0, y0, x1, y0), (x1, y0, x1, y1), (x1, y1, x0, y1), (x0, y1, x0, y0)):
        line(px, width, a, b, c, d, ink)


def circle(px: bytearray, width: int, cx: int, cy: int, r: int, ink: int = 0) -> None:
    import math

    pts = [(cx + int(r * math.cos(t / 36 * 2 * math.pi)), cy + int(r * math.sin(t / 36 * 2 * math.pi)))
           for t in range(37)]
    for (a, b), (c, d) in zip(pts, pts[1:]):
        line(px, width, a, b, c, d, ink)


# ---------------------------------------------------------------- AVI (Motion JPEG)


def _chunk(fourcc: bytes, data: bytes) -> bytes:
    pad = b"\x00" if len(data) % 2 else b""
    return fourcc + struct.pack("<I", len(data)) + data + pad


def _list(kind: bytes, data: bytes) -> bytes:
    return b"LIST" + struct.pack("<I", len(data) + 4) + kind + data


def jpeg_size(data: bytes) -> tuple[int, int]:
    """(width, height) from a baseline/progressive JPEG's SOF marker."""
    i = 2
    while i < len(data):
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        if marker in (0xC0, 0xC1, 0xC2):
            h, w = struct.unpack(">HH", data[i + 5:i + 9])
            return w, h
        seg = struct.unpack(">H", data[i + 2:i + 4])[0]
        i += 2 + seg
    raise ValueError("no SOF marker")


def mjpeg_avi(frames: list[bytes], fps: int) -> bytes:
    """A playable Motion-JPEG AVI; every frame is stored verbatim as a '00dc' chunk."""
    w, h = jpeg_size(frames[0])
    n = len(frames)
    us = 1_000_000 // fps
    maxsize = max(len(f) for f in frames)
    avih = struct.pack("<IIIIIIIIII4I", us, maxsize * fps, 0, 0x10, n, 0, 1, maxsize, w, h, 0, 0, 0, 0)
    strh = struct.pack("<4s4sIHHIIIIIIIIhhhh", b"vids", b"MJPG", 0, 0, 0, 0, 1, fps, 0, n, maxsize,
                       0xFFFFFFFF, 0, 0, 0, w, h)
    strf = struct.pack("<IiiHH4sIiiII", 40, w, h, 1, 24, b"MJPG", w * h * 3, 0, 0, 0, 0)
    hdrl = _list(b"hdrl", _chunk(b"avih", avih) + _list(b"strl", _chunk(b"strh", strh) + _chunk(b"strf", strf)))
    movi_data, index, offset = bytearray(), bytearray(), 4
    for f in frames:
        c = _chunk(b"00dc", f)
        index += b"00dc" + struct.pack("<III", 0x10, offset, len(f))
        movi_data += c
        offset += len(c)
    movi = _list(b"movi", bytes(movi_data))
    body = b"AVI " + hdrl + movi + _chunk(b"idx1", bytes(index))
    return b"RIFF" + struct.pack("<I", len(body)) + body
