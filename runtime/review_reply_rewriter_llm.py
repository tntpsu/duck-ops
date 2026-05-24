"""
LLM-backed review reply rewriter.

Replaces the deterministic rule-based rewriter at runtime/review_loop.py:
build_rewrite_suggestion_text when DUCK_REVIEW_REWRITE_PROVIDER is configured.
Output is gated by sanity checks in review_reply_rewriter_sanity; failures
trigger fallback to the rule-based path.

See REVIEW_REPLY_REWRITE_LLM_PLAN.md for the full design.

Provider configuration:
  DUCK_REVIEW_REWRITE_PROVIDER=openai     # default; uses OPENAI_API_KEY
  DUCK_REVIEW_REWRITE_PROVIDER=disabled   # forces rule-based fallback
  DUCK_REVIEW_REWRITE_MODEL=gpt-4o-mini   # override the model

The .env file at duckAgent/.env carries OPENAI_API_KEY. This module loads
it the same way social_performance_collector.py does (load_dotenv on the
DUCK_AGENT_ROOT/.env path).
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from review_reply_rewriter_sanity import evaluate_sanity


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
LLM_CALL_LOG_PATH = DUCK_OPS_ROOT / "state" / "llm_call_log.jsonl"

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_PROVIDER = "openai"
DEFAULT_TIMEOUT_SECONDS = 12.0
MAX_TOKENS = 220  # ~3 sentence reply


PROMPT_TEMPLATE = """You are drafting a public reply to a customer review for myJeepDuck, a small business that 3D-prints custom rubber-duck figurines. You are NOT a generic AI assistant; you are the shop owner replying.

Customer review (verbatim):
\"\"\"{review_text}\"\"\"

Current operator draft (good, but not yet approved):
\"\"\"{draft_text}\"\"\"

Operator's feedback for the rewrite:
{hint_text}

Constraints:
- 1-3 sentences total, roughly 30-90 words.
- Echo at least one specific word or detail from the review (e.g., a recipient, a feature mentioned, an emotion expressed).
- Sound like a human shop owner — warm, specific, grounded.
- Never invent facts. Do not promise discounts, future products, refunds, or replacements unless the operator's feedback explicitly says to.
- No emojis. No URLs. No template placeholders like [NAME] or {{customer_name}}.
- Do not address the customer by name.
- Do not say "as an AI" or otherwise reveal the rewrite is automated.

Return ONLY the reply text, no preamble, no quotation marks around the reply."""


def _has_dotenv_loaded() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


def _try_load_env() -> None:
    if _has_dotenv_loaded():
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
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _provider() -> str:
    return os.environ.get("DUCK_REVIEW_REWRITE_PROVIDER", DEFAULT_PROVIDER).strip().lower()


def _model() -> str:
    return os.environ.get("DUCK_REVIEW_REWRITE_MODEL", DEFAULT_MODEL).strip()


def is_disabled() -> bool:
    return _provider() == "disabled"


def _format_hint(hint: str | None) -> str:
    cleaned = (hint or "").strip()
    if not cleaned:
        return "(No specific feedback — refine the draft to be warmer, more specific, and tied to what the customer actually said.)"
    return cleaned


def _build_prompt(review_text: str, draft_text: str, hint: str | None) -> str:
    return PROMPT_TEMPLATE.format(
        review_text=(review_text or "").strip() or "(no review text captured)",
        draft_text=(draft_text or "").strip() or "(no draft captured)",
        hint_text=_format_hint(hint),
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _log_llm_call(payload: dict[str, Any]) -> None:
    LLM_CALL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with LLM_CALL_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _call_openai(prompt: str, *, model: str, timeout: float) -> dict[str, Any] | None:
    """Return the parsed OpenAI chat-completion response, or None on failure."""
    import requests  # local import keeps this module importable in test envs without requests

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt},
        ],
        "max_tokens": MAX_TOKENS,
        "temperature": 0.5,
    }
    started = time.time()
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
        return {"error": f"request_failed:{type(exc).__name__}:{exc}", "elapsed_seconds": time.time() - started}
    elapsed = time.time() - started
    if response.status_code >= 400:
        return {"error": f"http_{response.status_code}", "body": response.text[:500], "elapsed_seconds": elapsed}
    try:
        return {**response.json(), "elapsed_seconds": elapsed}
    except Exception as exc:
        return {"error": f"json_decode_failed:{type(exc).__name__}:{exc}", "elapsed_seconds": elapsed}


def _extract_text(api_response: dict[str, Any]) -> str:
    choices = api_response.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    return str(message.get("content") or "").strip()


def _clean_output(text: str) -> str:
    """Strip outer quotes the LLM sometimes adds despite instructions."""
    cleaned = text.strip()
    if len(cleaned) >= 2 and cleaned[0] in {'"', "'"} and cleaned[-1] == cleaned[0]:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def generate_rewrite_via_llm(
    item: dict[str, Any],
    *,
    hint: str = "",
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any] | None:
    """Generate a review-reply rewrite via LLM with sanity gating.

    Returns a result dict on success:
        {
            "text": str,
            "source": "llm",
            "model": str,
            "provider": str,
            "generated_at": ISO timestamp,
            "hint": str,
            "sanity": {...},
            "usage": {...},
        }

    Returns None when the LLM is disabled, missing config, fails to respond,
    or the output fails sanity checks. Callers fall back to the rule-based
    rewriter on None.
    """
    if item.get("artifact_type") != "review_reply":
        return None
    if is_disabled():
        return None

    _try_load_env()
    if not os.environ.get("OPENAI_API_KEY"):
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "outcome": "missing_api_key",
        })
        return None

    preview = item.get("preview") or {}
    review_text = (preview.get("context_text") or "").strip()
    draft_text = (preview.get("proposed_text") or "").strip()
    if not review_text:
        return None

    provider = _provider()
    model = _model()
    prompt = _build_prompt(review_text, draft_text, hint)

    api_response: dict[str, Any] | None
    if provider == "openai":
        api_response = _call_openai(prompt, model=model, timeout=timeout_seconds)
    else:
        # Future: gemini branch. For now, only openai is implemented.
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "outcome": f"unsupported_provider:{provider}",
        })
        return None

    if api_response is None or "error" in api_response:
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "provider": provider,
            "model": model,
            "outcome": "api_failure",
            "error": (api_response or {}).get("error"),
            "body": (api_response or {}).get("body"),
            "elapsed_seconds": (api_response or {}).get("elapsed_seconds"),
        })
        return None

    text = _clean_output(_extract_text(api_response))
    sanity = evaluate_sanity(text, review_text=review_text)
    usage = api_response.get("usage") or {}

    _log_llm_call({
        "at": _now_iso(),
        "artifact_id": item.get("artifact_id"),
        "provider": provider,
        "model": model,
        "outcome": "ok" if sanity["passed"] else "sanity_failed",
        "sanity_failures": sanity["failures"],
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "elapsed_seconds": api_response.get("elapsed_seconds"),
        "hint_present": bool((hint or "").strip()),
        "output_length": len(text),
    })

    if not sanity["passed"]:
        return None

    return {
        "text": text,
        "source": "llm",
        "model": model,
        "provider": provider,
        "generated_at": _now_iso(),
        "hint": (hint or "").strip(),
        "sanity": sanity,
        "usage": usage,
    }
