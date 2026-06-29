"""
Shared LLM call helpers for Duck Ops lanes.

Codifies the patterns from `duck-llm-integration` skill: env loading,
provider call with retries, response parsing, append-only call logging.
Used by every LLM-backed lane (review_reply_rewriter_llm,
review_reply_scorer_llm, weekly_sale_rewriter_llm, jeepfact_rewriter_llm).

Each caller still owns:
  - its own prompt template and PROMPT_TEMPLATE constant
  - its own sanity gate (echo check, length bounds, etc.)
  - its own per-lane env var (DUCK_<LANE>_PROVIDER, DUCK_<LANE>_MODEL)
  - its own log `kind` value so the OS producer can filter by lane

This module is the wire; callers wrap it with their lane-specific logic.

See: ~/.codex/skills/duck-llm-integration/SKILL.md
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
LLM_CALL_LOG_PATH = DUCK_OPS_ROOT / "state" / "llm_call_log.jsonl"

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_TIMEOUT_SECONDS = 12.0

# LLM pricing as of 2026 (USD per 1M tokens). Surface 10's cost
# observability page reads these to estimate weekly spend without
# hitting OpenAI / Anthropic billing APIs. Update when pricing or
# model defaults change.
MODEL_PRICING_USD_PER_1M_TOKENS = {
    # OpenAI
    "gpt-4o-mini": {"prompt": 0.150, "completion": 0.600},
    "gpt-4o": {"prompt": 2.500, "completion": 10.000},
    "gpt-4.1-mini": {"prompt": 0.400, "completion": 1.600},
    "gpt-4.1": {"prompt": 2.000, "completion": 8.000},
    "gpt-3.5-turbo": {"prompt": 0.500, "completion": 1.500},  # gpt-3.5-turbo-0125
    # Anthropic — covers what duckAgent flows will surface once Scope B
    # instrumentation lands (currently no Anthropic calls in the log).
    "claude-opus-4-7": {"prompt": 15.000, "completion": 75.000},
    "claude-sonnet-4-6": {"prompt": 3.000, "completion": 15.000},
    "claude-haiku-4-5-20251001": {"prompt": 1.000, "completion": 5.000},
    # Image models — these don't have token-based pricing, so the
    # producer treats them as "fixed_cost_per_call" via a separate
    # pathway (Phase B). Listed here for discoverability.
    # "dall-e-3", "gpt-image-1" → see _PER_CALL_IMAGE_COST_USD below.
}

# Image-generation pricing per call. Operators care about images
# specifically because a single 1024x1024 gpt-image-1 call ≈ $0.04
# — comparable to ~30,000 gpt-4o-mini tokens. Without this, the cost
# page would underestimate creative-flow spend by an order of magnitude.
PER_CALL_IMAGE_COST_USD = {
    "gpt-image-1": 0.040,           # ~$0.04 per 1024x1024 standard quality
    "gpt-image-2": 0.040,           # next-gen gpt-image; same flat estimate until OpenAI publishes rates
    "dall-e-3": 0.040,              # ~$0.04 per 1024x1024 standard
    "dall-e-3-hd": 0.080,           # ~$0.08 per 1024x1024 HD
}


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def try_load_duckagent_env() -> None:
    """Load duckAgent/.env on demand if OPENAI_API_KEY isn't already present.

    Mirrors the social_performance_collector pattern. Idempotent and safe to
    call repeatedly. Never raises.
    """
    if os.getenv("OPENAI_API_KEY"):
        return
    env_path = DUCK_AGENT_ROOT / ".env"
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv is not None:
        load_dotenv(env_path, override=False)
        return
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    except OSError:
        pass


def log_llm_call(payload: dict[str, Any]) -> None:
    """Append-only write to state/llm_call_log.jsonl. Best-effort; never raises."""
    LLM_CALL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with LLM_CALL_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        pass


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_BASE_BACKOFF_SECONDS = 1.0


def call_openai(
    prompt: str,
    *,
    model: str = DEFAULT_MODEL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = 220,
    temperature: float = 0.5,
    response_format: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Single-turn OpenAI chat completion. Returns the parsed JSON response
    on HTTP success, or a dict with an `error` key on failure. Returns None
    only when OPENAI_API_KEY is missing.

    Retries up to _MAX_RETRIES times on transient HTTP errors
    (429/5xx) and request exceptions with exponential backoff. A
    successful response on retry is reported with `retry_count` so
    the call log captures how flaky the provider was. Non-retryable
    errors (4xx other than 429, JSON decode failures) return
    immediately.

    `response_format` flows through to the OpenAI request payload
    when set — typically `{"type": "json_object"}` for JSON-mode
    lanes. The caller is still responsible for parsing the response
    string and validating its schema; native JSON mode only
    guarantees the response is parseable JSON, not that it matches
    a given shape.

    Caller is responsible for checking `error` key vs successful response
    shape.
    """
    import requests

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    payload: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if response_format is not None:
        payload["response_format"] = response_format
    overall_started = time.time()
    last_error: dict[str, Any] | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=timeout,
            )
        except Exception as exc:
            last_error = {
                "error": f"request_failed:{type(exc).__name__}:{exc}",
                "elapsed_seconds": time.time() - overall_started,
                "retry_count": attempt,
            }
            if attempt < _MAX_RETRIES - 1:
                time.sleep(_BASE_BACKOFF_SECONDS * (2 ** attempt))
                continue
            return last_error
        if response.status_code >= 400:
            err = {
                "error": f"http_{response.status_code}",
                "body": response.text[:500],
                "elapsed_seconds": time.time() - overall_started,
                "retry_count": attempt,
            }
            if response.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES - 1:
                last_error = err
                time.sleep(_BASE_BACKOFF_SECONDS * (2 ** attempt))
                continue
            return err
        try:
            return {
                **response.json(),
                "elapsed_seconds": time.time() - overall_started,
                "retry_count": attempt,
            }
        except Exception as exc:
            return {
                "error": f"json_decode_failed:{type(exc).__name__}:{exc}",
                "elapsed_seconds": time.time() - overall_started,
                "retry_count": attempt,
            }
    # Should be unreachable — loop either returns success or returns
    # the last error after exhausting retries. Guard for safety.
    return last_error or {"error": "exhausted_retries", "elapsed_seconds": time.time() - overall_started}


def extract_text(api_response: dict[str, Any]) -> str:
    choices = api_response.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    return str(message.get("content") or "").strip()


def estimate_cost_usd(prompt_tokens: int | None, completion_tokens: int | None, model: str) -> float | None:
    """Estimate USD cost for a single call given token counts and model.

    Returns None when tokens are missing or the model isn't in the pricing
    table. Used by bullet 4's cost dashboard.
    """
    pricing = MODEL_PRICING_USD_PER_1M_TOKENS.get(model)
    if pricing is None:
        return None
    try:
        p = float(prompt_tokens or 0)
        c = float(completion_tokens or 0)
    except (TypeError, ValueError):
        return None
    return round((p * pricing["prompt"] + c * pricing["completion"]) / 1_000_000.0, 6)
