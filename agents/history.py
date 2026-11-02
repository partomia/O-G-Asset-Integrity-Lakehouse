"""
Historical patterns (the AMP's vector search, here TF-IDF in process): past agent analyses and
the inspection report chunks, ranked against a question or an asset tag. The corpus is small
(hundreds of chunks), so no vector store is needed; an embedding endpoint on CAII can replace
`Index` without changing callers.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

TOKEN = r"(?u)\b[A-Za-z0-9][A-Za-z0-9\-\.]+\b"


@dataclass
class Hit:
    kind: str
    ref: str
    score: float
    text: str


class Index:
    def __init__(self, docs: list[dict]):
        """docs: {"kind", "ref", "text"}"""
        self.docs = [d for d in docs if (d.get("text") or "").strip()]
        self.vec = TfidfVectorizer(token_pattern=TOKEN, lowercase=True, sublinear_tf=True, ngram_range=(1, 2), min_df=1)
        self.m = self.vec.fit_transform([d["text"] for d in self.docs]) if self.docs else None

    def search(self, question: str, k: int = 8, kind: str | None = None) -> list[Hit]:
        if self.m is None or not question.strip():
            return []
        sims = linear_kernel(self.vec.transform([question]), self.m).ravel()
        order = sims.argsort()[::-1]
        out = []
        for i in order:
            if sims[i] <= 0 or len(out) >= k:
                break
            d = self.docs[i]
            if kind and d["kind"] != kind:
                continue
            out.append(Hit(d["kind"], d["ref"], float(sims[i]), d["text"]))
        return out


def snippet(text: str, question: str, width: int = 320) -> str:
    words = [w for w in re.findall(TOKEN, question.lower()) if len(w) > 2]
    low = text.lower()
    pos = min((low.find(w) for w in words if low.find(w) >= 0), default=0)
    start = max(0, pos - width // 3)
    return ("..." if start else "") + text[start:start + width].replace("\n", " ") + ("..." if start + width < len(text) else "")
