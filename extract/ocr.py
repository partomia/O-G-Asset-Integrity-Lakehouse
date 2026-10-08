"""
OCR fallback for image-only pages: template matching against the scanner's 5x7 font
(extract/font5x7.py). Standard library only.

  1. binarise (ink < 128), find the text block's top-left corner from row / column ink
     profiles (robust to speckle), then refine the grid offset by match quality;
  2. read each character cell at font resolution by majority vote over its SCALE x SCALE
     blocks (removes isolated speckle);
  3. pick the glyph with the smallest Hamming distance; a nearly empty cell is a space.

Scope: this reads the demo scanner's font, nothing else. A production pipeline would call a
general OCR engine in the same slot; ocr_page() is the only function the jobs use.
"""
from __future__ import annotations

from extract.font5x7 import CELL_W, COLUMNS, GLYPH_H, GLYPH_W, LINE_H, SCALE, glyph

TEMPLATES = {ch: tuple(v for row in glyph(ch) for v in row) for ch in COLUMNS}


def _ink(gray: bytes) -> bytearray:
    return bytearray(1 if v < 128 else 0 for v in gray)


def _first_strong(profile: list[int]) -> int | None:
    s = sorted(profile)
    noise = s[len(s) // 2]
    thresh = noise + max(6, int(4 * (noise ** 0.5)))
    for i, v in enumerate(profile):
        if v > thresh:
            return i
    return None


def _cell(ink: bytearray, w: int, x: int, y: int) -> tuple[int, ...]:
    out = []
    for r in range(GLYPH_H):
        for c in range(GLYPH_W):
            n = 0
            for dy in range(SCALE):
                base = (y + r * SCALE + dy) * w + x + c * SCALE
                n += sum(ink[base:base + SCALE])
            out.append(1 if n * 2 >= SCALE * SCALE else 0)
    return tuple(out)


def _match(cell: tuple[int, ...]) -> tuple[str, int]:
    if sum(cell) <= 1:
        return " ", sum(cell)
    best, dist = "?", 1 << 30
    for ch, t in TEMPLATES.items():
        d = sum(a != b for a, b in zip(cell, t))
        if d < dist:
            best, dist = ch, d
    return best, dist


def _read(ink: bytearray, w: int, h: int, x0: int, y0: int, max_lines: int | None = None):
    lines, cost = [], 0
    y = y0
    while y + GLYPH_H * SCALE <= h and (max_lines is None or len(lines) < max_lines):
        chars = []
        x = x0
        while x + GLYPH_W * SCALE <= w:
            ch, d = _match(_cell(ink, w, x, y))
            chars.append(ch)
            cost += d
            x += CELL_W
        lines.append("".join(chars).rstrip())
        y += LINE_H
    return lines, cost


def ocr_gray(w: int, h: int, gray: bytes) -> str:
    ink = _ink(gray)
    rows = [sum(ink[y * w:(y + 1) * w]) for y in range(h)]
    cols = [sum(ink[x::w]) for x in range(w)]
    y_top, x_left = _first_strong(rows), _first_strong(cols)
    if y_top is None or x_left is None:
        return ""
    best = None
    for dy in range(-SCALE, SCALE + 1):
        for dx in range(-2 * SCALE, SCALE + 1):
            x0, y0 = max(0, x_left + dx), max(0, y_top + dy)
            _, cost = _read(ink, w, h, x0, y0, max_lines=3)
            if best is None or cost < best[0]:
                best = (cost, x0, y0)
    lines, _ = _read(ink, w, h, best[1], best[2])
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


def ocr_page(image: tuple[int, int, bytes]) -> str:
    w, h, gray = image
    return ocr_gray(w, h, gray)
