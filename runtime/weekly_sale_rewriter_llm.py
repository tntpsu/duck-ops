"""
LLM rewriter for the weekly_sale plan summary.

Bullet 3 / Slice K extension. Built per the duck-llm-integration skill
patterns: shared call infrastructure, sanity gate, kill switch env var,
call logging with kind="weekly_sale_rewrite".

The existing rule-based build_weekly_sale_rewrite_text in review_loop.py
produces section-based text from the playbook. This module asks the LLM
to refine that section text into a more cohesive narrative when the
operator wants a different angle (warmer, shorter, more strategic, etc.)
— guided by the operator's free-text hint.

Kill switch:
  DUCK_WEEKLY_SALE_REWRITE_PROVIDER=disabled forces rule-based fallback.
  DUCK_WEEKLY_SALE_REWRITE_MODEL=gpt-4o-mini (default override).
"""
from __future__ import annotations

import os
import re
from typing import Any

from llm_call_helpers import (
    call_openai,
    extract_text,
    log_llm_call,
    now_iso,
    try_load_duckagent_env,
    DEFAULT_MODEL,
)


DEFAULT_PROVIDER = "openai"
DEFAULT_TIMEOUT_SECONDS = 12.0
MAX_TOKENS = 320

# Sanity bounds for the rewrite output
MIN_LENGTH = 60
MAX_LENGTH = 1400


# Mirror the test-patch aliases the review_reply modules use.
_call_openai = call_openai
_log_llm_call = log_llm_call
_now_iso = now_iso
_try_load_env = try_load_duckagent_env
_extract_text = extract_text


PROMPT_TEMPLATE = """You are summarizing this week's sale playbook for the operator of myJeepDuck (a small business that 3D-prints custom rubber-duck figurines). You are the strategist on staff — not a generic AI — writing a concise operator-facing brief.

Rule-based section summary the operator currently has:
\"\"\"{base_text}\"\"\"

Operator's feedback for the rewrite:
{hint_text}

Constraints:
- 4-10 short lines, total under 1200 characters.
- Lead with the theme / discount call.
- Keep section labels short and consistent (Theme, Market match, Momentum, Re-engagement, Clearance — drop any section that has nothing actionable).
- Echo the actual product names, themes, or discounts from the base text. Do not invent new ones.
- Stay business-direct: this is for an internal operator, not a customer.
- No emojis, no URLs, no template placeholders. No "as an AI" disclosures.
- Never invent products that aren't in the base text.

Return ONLY the rewritten brief, no preamble, no quotation marks."""


def _provider() -> str:
    return os.environ.get("DUCK_WEEKLY_SALE_REWRITE_PROVIDER", DEFAULT_PROVIDER).strip().lower()


def _model() -> str:
    return os.environ.get("DUCK_WEEKLY_SALE_REWRITE_MODEL", DEFAULT_MODEL).strip()


def is_disabled() -> bool:
    return _provider() == "disabled"


def _format_hint(hint: str | None) -> str:
    cleaned = (hint or "").strip()
    if not cleaned:
        return "(No specific feedback — keep the structure but tighten any sections that read as boilerplate.)"
    return cleaned


def _build_prompt(base_text: str, hint: str | None) -> str:
    return PROMPT_TEMPLATE.format(
        base_text=(base_text or "").strip() or "(no playbook summary captured)",
        hint_text=_format_hint(hint),
    )


def _clean_output(text: str) -> str:
    cleaned = (text or "").strip()
    if len(cleaned) >= 2 and cleaned[0] in {'"', "'"} and cleaned[-1] == cleaned[0]:
        cleaned = cleaned[1:-1].strip()
    return cleaned


_EMOJI = re.compile(
    "["
    "\U0001F300-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF\U00002600-\U000027BF"
    "]"
)
_PLACEHOLDER_BRACES = re.compile(r"\{[^}]+\}")
_PLACEHOLDER_BRACKETS = re.compile(r"\[[A-Z_ ]+\]")
_URL = re.compile(r"https?://\S+", re.IGNORECASE)
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_REFUSAL = re.compile(r"^(?:i\s+(?:can(?:no|')t|won['’]t|am\s+unable)|as\s+an\s+ai|i'?m\s+sorry)", re.IGNORECASE)


def evaluate_sanity(rewrite: str, *, base_text: str) -> dict[str, Any]:
    """Sanity gate for the weekly_sale rewrite.

    Echo check: at least one alphanumeric token of length >= 4 from the
    base text must appear in the rewrite. Stops the LLM from inventing
    product names or wholesale-replacing the rule-based playbook with
    generic strategy talk.
    """
    failures: list[str] = []
    text = (rewrite or "").strip()
    if not (MIN_LENGTH <= len(text) <= MAX_LENGTH):
        failures.append("length_in_band")
    if _PLACEHOLDER_BRACES.search(text) or _PLACEHOLDER_BRACKETS.search(text):
        failures.append("no_placeholders")
    if _URL.search(text) or _EMAIL.search(text):
        failures.append("no_external_references")
    if _EMOJI.search(text):
        failures.append("no_emoji")
    if _REFUSAL.search(text):
        failures.append("not_a_refusal")
    base_tokens = {
        tok.lower() for tok in re.findall(r"[A-Za-z0-9']+", (base_text or "")) if len(tok) >= 4
    }
    rewrite_tokens = {
        tok.lower() for tok in re.findall(r"[A-Za-z0-9']+", text) if len(tok) >= 4
    }
    if base_tokens and not (base_tokens & rewrite_tokens):
        failures.append("echo_check")
    return {"passed": not failures, "failures": failures}


def generate_weekly_sale_rewrite_via_llm(
    item: dict[str, Any],
    *,
    base_text: str,
    hint: str = "",
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any] | None:
    """Returns a structured result dict on success or None on any failure
    (disabled, missing key, API error, sanity rejection). Caller falls
    back to the rule-based summary when None.
    """
    if str(item.get("flow") or "") != "weekly_sale":
        return None
    if is_disabled():
        return None

    _try_load_env()
    if not os.environ.get("OPENAI_API_KEY"):
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "kind": "weekly_sale_rewrite",
            "outcome": "missing_api_key",
        })
        return None

    if not (base_text or "").strip():
        return None

    provider = _provider()
    model = _model()
    if provider != "openai":
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "kind": "weekly_sale_rewrite",
            "outcome": f"unsupported_provider:{provider}",
        })
        return None

    prompt = _build_prompt(base_text, hint)
    api_response = _call_openai(
        prompt,
        model=model,
        timeout=timeout_seconds,
        max_tokens=MAX_TOKENS,
        temperature=0.4,
    )

    if api_response is None or "error" in api_response:
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "kind": "weekly_sale_rewrite",
            "provider": provider,
            "model": model,
            "outcome": "api_failure",
            "error": (api_response or {}).get("error"),
            "body": (api_response or {}).get("body"),
            "elapsed_seconds": (api_response or {}).get("elapsed_seconds"),
            "prompt": prompt,
            "hint": (hint or "").strip(),
        })
        return None

    text = _clean_output(_extract_text(api_response))
    sanity = evaluate_sanity(text, base_text=base_text)
    usage = api_response.get("usage") or {}
    log_entry: dict[str, Any] = {
        "at": _now_iso(),
        "artifact_id": item.get("artifact_id"),
        "kind": "weekly_sale_rewrite",
        "provider": provider,
        "model": model,
        "outcome": "ok" if sanity["passed"] else "sanity_failed",
        "sanity_failures": sanity["failures"],
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "elapsed_seconds": api_response.get("elapsed_seconds"),
        "hint_present": bool((hint or "").strip()),
        "hint": (hint or "").strip(),
        "output_length": len(text),
        "prompt": prompt,
    }
    if sanity["passed"]:
        log_entry["output_text"] = text
    else:
        log_entry["rejected_output_text"] = text
    _log_llm_call(log_entry)

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
