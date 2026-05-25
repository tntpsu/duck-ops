"""
LLM disambiguation for catalog matches in the gray zone.

When catalog_match.find_existing_matches returns status="partial" — token
overlap exists but isn't conclusive — this module asks an LLM whether the
proposed concept is a true duplicate of one of the candidate matches.

Structured-output pattern (per duck-llm-integration skill):
  - Temperature 0.1 (deterministic-ish judgment, not creative writing)
  - JSON output validated against an enum schema
  - Strict allow-list: matched_product_id MUST be one of the inputs
  - Kill switch: DUCK_CATALOG_DEDUP_PROVIDER=disabled
  - Logged with kind="catalog_dedup"

This is the M.4 layer of the catalog-dedup stack:
  - rules (catalog_match) handle covered + gap deterministically
  - LLM only fires on partial — the gray zone
  - operator decisions (catalog_aliases) feed forward to make future
    matches deterministic — over time the LLM does less work

Don't burn tokens:
  - No partial matches → return None (rules already decided)
  - Empty theme → return None
  - Kill switch on → return None
  - Provider isn't openai → return None
"""
from __future__ import annotations

import json
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
DEFAULT_TIMEOUT_SECONDS = 10.0
MAX_TOKENS = 320
MAX_REASONING_LENGTH = 240


# Mirror the test-patch aliases the other LLM modules use.
_call_openai = call_openai
_log_llm_call = log_llm_call
_now_iso = now_iso
_try_load_env = try_load_duckagent_env
_extract_text = extract_text


PROMPT_TEMPLATE = """You judge whether a proposed new product concept duplicates an existing product in the myJeepDuck catalog (a small business that 3D-prints custom rubber-duck figurines).

The proposed concept's theme is:
\"\"\"{proposed_theme}\"\"\"

The keyword matcher found {candidate_count} existing product(s) with some token overlap:
{candidate_block}

Decide: is the proposed concept a duplicate of one of these existing products? Two products are duplicates when an operator would say "I already have one of those" — same visual concept, same audience, even if the names phrase it differently. They are NOT duplicates if they share keywords but represent meaningfully different product ideas (e.g., "summer beach duck" vs "summer wedding duck" both match "summer" but are distinct concepts).

Return ONLY a JSON object with these keys:

  is_duplicate: true | false
  matched_product_id: string — the product_id of the existing product the proposed concept duplicates. MUST be one of: {valid_product_ids}. Use "" when is_duplicate is false.
  confidence: integer 0-100 — how confident you are. 90+ for obvious matches, 60-80 for likely matches, below 60 means escalate to the operator.
  reasoning: one short sentence (max {max_reasoning_chars} chars) explaining your call. No preamble, no markdown.

Output ONLY the JSON object. No code fences, no preamble."""


def _provider() -> str:
    return os.environ.get("DUCK_CATALOG_DEDUP_PROVIDER", DEFAULT_PROVIDER).strip().lower()


def _model() -> str:
    return os.environ.get("DUCK_CATALOG_DEDUP_MODEL", DEFAULT_MODEL).strip()


def is_disabled() -> bool:
    return _provider() == "disabled"


def _format_candidate_block(candidates: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for idx, cand in enumerate(candidates, start=1):
        title = str(cand.get("title") or "").strip() or "(no title)"
        handle = str(cand.get("handle") or "").strip()
        tokens = cand.get("matched_tokens") or []
        token_str = ", ".join(str(t) for t in tokens) if tokens else "none recorded"
        pid = str(cand.get("product_id") or "").strip()
        lines.append(
            f"  {idx}. product_id={pid} | title=\"{title}\" | handle={handle} | matched_tokens=[{token_str}]"
        )
    return "\n".join(lines) if lines else "  (none)"


def _strip_code_fence(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```\s*$", "", cleaned)
    return cleaned.strip()


def _parse_verdict(raw: str, valid_product_ids: set[str]) -> dict[str, Any] | None:
    cleaned = _strip_code_fence(raw)
    if not cleaned:
        return None
    try:
        data = json.loads(cleaned)
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if not isinstance(data.get("is_duplicate"), bool):
        return None
    matched_id = str(data.get("matched_product_id") or "").strip()
    if data["is_duplicate"]:
        if not matched_id or matched_id not in valid_product_ids:
            return None
    else:
        matched_id = ""
    raw_confidence = data.get("confidence")
    try:
        confidence = int(raw_confidence)
    except (TypeError, ValueError):
        return None
    confidence = max(0, min(100, confidence))
    reasoning = str(data.get("reasoning") or "").strip()
    if len(reasoning) > MAX_REASONING_LENGTH:
        reasoning = reasoning[:MAX_REASONING_LENGTH].rstrip() + "…"
    return {
        "is_duplicate": data["is_duplicate"],
        "matched_product_id": matched_id,
        "confidence": confidence,
        "reasoning": reasoning,
    }


def disambiguate_catalog_match(
    *,
    proposed_theme: str,
    candidates: list[dict[str, Any]],
    artifact_id: str = "",
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any] | None:
    """Ask the LLM whether the proposed concept duplicates one of the
    partial-match candidates. Returns the parsed verdict dict, or None
    on disabled / missing key / API failure / unparseable output."""
    if is_disabled():
        return None
    if not str(proposed_theme or "").strip():
        return None
    if not candidates:
        return None

    _try_load_env()
    if not os.environ.get("OPENAI_API_KEY"):
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": artifact_id,
            "kind": "catalog_dedup",
            "outcome": "missing_api_key",
        })
        return None

    provider = _provider()
    model = _model()
    if provider != "openai":
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": artifact_id,
            "kind": "catalog_dedup",
            "outcome": f"unsupported_provider:{provider}",
        })
        return None

    valid_ids = {str(c.get("product_id") or "").strip() for c in candidates if c.get("product_id")}
    prompt = PROMPT_TEMPLATE.format(
        proposed_theme=str(proposed_theme).strip(),
        candidate_count=len(candidates),
        candidate_block=_format_candidate_block(candidates),
        valid_product_ids=", ".join(sorted(valid_ids)) or "(none)",
        max_reasoning_chars=MAX_REASONING_LENGTH,
    )

    api_response = _call_openai(
        prompt,
        model=model,
        timeout=timeout_seconds,
        max_tokens=MAX_TOKENS,
        temperature=0.1,
    )

    if api_response is None or "error" in api_response:
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": artifact_id,
            "kind": "catalog_dedup",
            "provider": provider,
            "model": model,
            "outcome": "api_failure",
            "error": (api_response or {}).get("error"),
            "body": (api_response or {}).get("body"),
            "elapsed_seconds": (api_response or {}).get("elapsed_seconds"),
            "prompt": prompt,
        })
        return None

    text = _extract_text(api_response)
    verdict = _parse_verdict(text, valid_ids)
    usage = api_response.get("usage") or {}
    log_entry: dict[str, Any] = {
        "at": _now_iso(),
        "artifact_id": artifact_id,
        "kind": "catalog_dedup",
        "provider": provider,
        "model": model,
        "outcome": "ok" if verdict else "sanity_failed",
        "sanity_failures": [] if verdict else ["unparseable_or_invalid_json"],
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "elapsed_seconds": api_response.get("elapsed_seconds"),
        "candidate_count": len(candidates),
        "valid_product_ids": sorted(valid_ids),
        "output_length": len(text),
        "prompt": prompt,
    }
    if verdict:
        log_entry["verdict"] = verdict
    else:
        log_entry["rejected_output_text"] = text
    _log_llm_call(log_entry)

    if not verdict:
        return None
    return {
        **verdict,
        "source": "llm",
        "model": model,
        "provider": provider,
        "generated_at": _now_iso(),
        "usage": usage,
    }
