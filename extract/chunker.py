"""Split page text into overlapping word windows (silver.doc_chunk), for search and the optional
embedding extension. Standard library only."""
from __future__ import annotations


def chunks(text: str, size: int = 60, overlap: int = 15) -> list[str]:
    words = text.split()
    if not words:
        return []
    out, step = [], max(1, size - overlap)
    for i in range(0, len(words), step):
        out.append(" ".join(words[i:i + size]))
        if i + size >= len(words):
            break
    return out
