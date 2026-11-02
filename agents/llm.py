"""
Chat client for Cloudera AI Inference (CAII), whose LLM endpoints speak the OpenAI API. The
pattern of the AMP's inference layer, kept to what a Streamlit app needs: streaming
/chat/completions, retry on transient errors before the first token, a circuit breaker, a TTL
cache of whole completions, token usage.

Auth is the CDP JWT: /tmp/jwt inside a CAI session, job or application; OGX_CAII_TOKEN, or the
cdp CLI (generate-workload-auth-token) on a laptop.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import requests

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "inference.json").read_text())


class LLMError(RuntimeError):
    pass


class TransientLLMError(LLMError):
    pass


@dataclass
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def add(self, other: "Usage") -> None:
        self.prompt_tokens += other.prompt_tokens
        self.completion_tokens += other.completion_tokens
        self.total_tokens += other.total_tokens


@dataclass
class Completion:
    """Filled while chat_stream is consumed: the text so far, usage once the stream ends."""
    text: str = ""
    usage: Usage = field(default_factory=Usage)
    cached: bool = False


def cdp_token() -> str:
    tok = os.environ.get("OGX_CAII_TOKEN")
    if tok:
        return tok
    jwt = Path("/tmp/jwt")
    if jwt.exists():
        return json.loads(jwt.read_text())["access_token"]
    cdp = os.environ.get("OGX_CDP_CLI") or shutil.which("cdp") or next(
        iter(sorted(Path.home().glob("Library/Python/*/bin/cdp"))), None)
    if cdp:
        out = subprocess.run([str(cdp), "iam", "generate-workload-auth-token", "--workload-name", "DE"],
                             capture_output=True, text=True, timeout=60)
        if out.returncode == 0:
            return json.loads(out.stdout)["token"]
    raise LLMError("no CDP token: run inside CAI (/tmp/jwt), set OGX_CAII_TOKEN, or install the cdp CLI")


def endpoint() -> tuple[str, str]:
    ep = CFG["endpoint"]
    return (os.environ.get("OGX_CAII_BASE_URL", ep["base_url"]).rstrip("/"),
            os.environ.get("OGX_CAII_MODEL", ep["model"]))


class CAIIChat:
    name = "caii"

    def __init__(self, base_url: str | None = None, model: str | None = None, token: str | None = None):
        b, m = endpoint()
        self.base_url, self.model = (base_url or b).rstrip("/"), model or m
        self._token, self._fixed_token = token, token is not None
        self._token_at = 0.0
        c = CFG["chat"]
        self.timeout, self.retries = c["timeout_s"], c["retries"]
        self.breaker_failures, self.breaker_reset_s = c["breaker_failures"], c["breaker_reset_s"]
        self.cache_ttl_s = c["cache_ttl_s"]
        self._failures, self._open_until = 0, 0.0
        self._cache: dict[str, tuple[float, str]] = {}

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.model)

    @property
    def breaker_open(self) -> bool:
        return time.monotonic() < self._open_until

    def token(self) -> str:
        # The JWT in /tmp/jwt is refreshed by CAI; re-read it rather than keep one for hours.
        if self._fixed_token:
            return self._token
        if not self._token or time.monotonic() - self._token_at > 600:
            self._token, self._token_at = cdp_token(), time.monotonic()
        return self._token

    def _fail(self) -> None:
        self._failures += 1
        if self._failures >= self.breaker_failures:
            self._open_until = time.monotonic() + self.breaker_reset_s

    @staticmethod
    def cache_key(model: str, messages: list[dict], temperature: float, max_tokens: int) -> str:
        blob = json.dumps([model, messages, round(temperature, 3), max_tokens], sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()

    def chat_stream(self, messages: list[dict], out: Completion, temperature: float | None = None,
                    max_tokens: int | None = None, use_cache: bool = True) -> Iterator[str]:
        """Yield text deltas; `out` holds the full text and the usage when the generator ends."""
        if not self.configured:
            raise LLMError("CAII endpoint not configured (config/inference.json or OGX_CAII_BASE_URL/OGX_CAII_MODEL)")
        if self.breaker_open:
            raise LLMError(f"circuit open after {self._failures} failures; retry in "
                           f"{self._open_until - time.monotonic():.0f} s")
        temperature = CFG["chat"]["temperature"] if temperature is None else temperature
        max_tokens = max_tokens or CFG["chat"]["max_tokens"]
        key = self.cache_key(self.model, messages, temperature, max_tokens)
        hit = self._cache.get(key) if use_cache else None
        if hit and time.monotonic() - hit[0] < self.cache_ttl_s:
            out.text, out.cached = hit[1], True
            yield hit[1]
            return
        body = {"model": self.model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens,
                "stream": True, "stream_options": {"include_usage": True}}
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            started = False
            try:
                for delta in self._stream(body, out):
                    started = True
                    yield delta
                self._failures = 0
                if out.text:
                    self._cache[key] = (time.monotonic(), out.text)
                return
            except TransientLLMError as e:
                self._fail()
                last = e
                if started:
                    raise
                time.sleep(min(2 ** attempt, 8))
            except LLMError:
                self._fail()
                raise
        raise last or LLMError("no completion")

    def _stream(self, body: dict, out: Completion) -> Iterator[str]:
        headers = {"Authorization": f"Bearer {self.token()}", "Content-Type": "application/json"}
        try:
            r = requests.post(f"{self.base_url}/chat/completions", json=body, headers=headers, stream=True,
                              timeout=(10, self.timeout))
        except requests.RequestException as e:
            raise TransientLLMError(f"CAII unreachable: {type(e).__name__}: {e}") from e
        with r:
            if r.status_code != 200:
                msg = f"CAII HTTP {r.status_code}: {r.text[:300]}"
                if r.status_code in (408, 425, 429, 502, 503, 504):
                    raise TransientLLMError(msg)
                if r.status_code == 401:
                    self._token = None
                raise LLMError(msg)
            for raw in r.iter_lines(decode_unicode=True):
                if not raw or not raw.startswith("data:"):
                    continue
                data = raw[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    ev = json.loads(data)
                except json.JSONDecodeError:
                    continue
                if ev.get("usage"):
                    u = ev["usage"]
                    out.usage = Usage(u.get("prompt_tokens", 0), u.get("completion_tokens", 0),
                                      u.get("total_tokens", 0))
                for ch in ev.get("choices") or []:
                    txt = (ch.get("delta") or {}).get("content") or ""
                    if txt:
                        out.text += txt
                        yield txt

    def health(self) -> dict:
        """One-token completion: ok, latency and the error if any."""
        t0 = time.monotonic()
        try:
            out = Completion()
            for _ in self.chat_stream([{"role": "user", "content": "Reply with OK."}], out, temperature=0.0,
                                      max_tokens=3, use_cache=False):
                pass
            return {"ok": True, "latency_s": round(time.monotonic() - t0, 2), "model": self.model}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "latency_s": round(time.monotonic() - t0, 2), "model": self.model,
                    "error": f"{type(e).__name__}: {str(e)[:300]}"}
