"""
LLM gray-zone scorer for review_reply items.

The deterministic scorer in evaluate_review_reply lands word-rich
custom-build reviews in the 60-77 "needs_revision" band because
lexical_overlap is a crappy proxy for semantic alignment (see the nephew
review case in REVIEW_REPLY_INBOX_UX_PLAN.md). This module fires ONLY in
that gray zone, asks the LLM "is this draft good enough to publish?", and
overrides the rule-based decision when the LLM agrees with the operator's
likely judgment.

Design:
  - Called only when 60 <= score < 78 AND fail_closed is empty.
  - LLM verdict is "yes" or "no" + a short reason.
  - "yes" → decision flips to publish_ready, no per-item suggestion added.
  - "no" → decision stays needs_revision, LLM reason becomes the first
    improvement_suggestion (replaces my H.5 weakest-component fallback).
  - Sanity-gated: malformed output → no override (rule-based decision wins).
  - DUCK_REVIEW_SCORER_PROVIDER=disabled is the kill switch.

Logged to state/llm_call_log.jsonl with kind="review_reply_score" so the
OS producer in K.4 surfaces it alongside the rewriter health.
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


from pathlib import Path

DEFAULT_PROVIDER = "openai"
DEFAULT_TIMEOUT_SECONDS = 10.0
MAX_TOKENS = 120

GRAY_ZONE_MIN = 60
GRAY_ZONE_MAX = 77  # 78 = publish_ready threshold, so gray = [60, 77]

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
REVIEW_REPLY_FEEDBACK_PATH = DUCK_OPS_ROOT / "state" / "review_reply_feedback.jsonl"
MAX_FEW_SHOT_PER_LABEL = 2  # 2 approve examples + 2 reject examples
MAX_FEW_SHOT_REVIEW_CHARS = 300
MAX_FEW_SHOT_REPLY_CHARS = 320


PROMPT_TEMPLATE = """You are a quality reviewer for myJeepDuck, a small business that 3D-prints custom rubber-duck figurines. The owner replies to every customer review personally — your job is to decide whether a specific draft reply is good enough to publish as-is or needs more work.
{few_shot_block}
Customer review (verbatim):
\"\"\"{review_text}\"\"\"

Draft reply (verbatim):
\"\"\"{draft_text}\"\"\"

A rule-based scorer landed this in the gray zone (score {score}/100, components: {component_summary}). You're the tie-breaker.

A draft is "publish-ready" when:
  - It echoes at least one specific detail from the customer's review (a recipient, a feature mentioned, an emotion expressed). Generic "thank you for the kind review" is NOT specific enough.
  - It sounds like a human shop owner — warm, grounded, appropriately sized for the review (no overlong reply to a 5-star one-liner).
  - It has no factual errors and doesn't promise discounts, future products, or remedies the operator didn't write.

A draft "needs revision" when:
  - The reply is generic and could be sent to any review.
  - It's templated or feels robotic.
  - It overpromises or invents facts.
  - It's the wrong size for the review (overlong vs short, or vice versa).

Use the operator's past decisions above as calibration — match their bar.

OUTPUT CONTRACT — return ONLY a JSON object, no preamble, no markdown fences:
{{"verdict": "yes" or "no", "reason": "<one sentence, under 200 chars, naming the specific quality signal you used>"}}"""


def _load_feedback_calibration_examples(
    *,
    per_label: int = MAX_FEW_SHOT_PER_LABEL,
) -> dict[str, list[dict[str, str]]]:
    """Read recent operator-labeled review_reply decisions from the feedback log
    and return calibration examples grouped by label.

    Returns:
        {"approved": [{"review": ..., "draft": ...}, ...],
         "rejected": [{"review": ..., "draft": ...}, ...]}

    Approved examples teach the LLM the operator's "publish_ready" bar.
    Rejected examples (discard / needs_changes) teach what the operator
    sends back. Each label is independently capped at `per_label` to keep
    the prompt bounded.
    """
    out: dict[str, list[dict[str, str]]] = {"approved": [], "rejected": []}
    if not REVIEW_REPLY_FEEDBACK_PATH.exists():
        return out
    seen_reviews: set[str] = set()
    try:
        with REVIEW_REPLY_FEEDBACK_PATH.open("r", encoding="utf-8") as fh:
            lines = fh.readlines()
    except OSError:
        return out
    for raw in reversed(lines):
        line = raw.strip()
        if not line:
            continue
        try:
            import json
            entry = json.loads(line)
        except (ValueError, Exception):
            continue
        action = entry.get("operator_action")
        review = (entry.get("customer_review") or "").strip()
        draft = (entry.get("draft_reply") or "").strip()
        if not review or not draft:
            continue
        if len(review) > MAX_FEW_SHOT_REVIEW_CHARS or len(draft) > MAX_FEW_SHOT_REPLY_CHARS:
            continue
        review_key = review.lower()
        if review_key in seen_reviews:
            continue
        if action == "approve" and len(out["approved"]) < per_label:
            out["approved"].append({"review": review, "draft": draft})
            seen_reviews.add(review_key)
        elif action in {"discard", "needs_changes"} and len(out["rejected"]) < per_label:
            out["rejected"].append({"review": review, "draft": draft})
            seen_reviews.add(review_key)
        if len(out["approved"]) >= per_label and len(out["rejected"]) >= per_label:
            break
    return out


def _format_few_shot_block(examples: dict[str, list[dict[str, str]]]) -> str:
    approved = examples.get("approved") or []
    rejected = examples.get("rejected") or []
    if not approved and not rejected:
        return ""
    parts = ["\nOperator's recent calibration (use this to match their judgment bar):\n"]
    for ex in approved:
        parts.append(
            "Operator APPROVED this draft as publish-ready:\n"
            "Customer review:\n\"\"\"" + ex["review"].strip() + "\"\"\"\n"
            "Draft reply:\n\"\"\"" + ex["draft"].strip() + "\"\"\""
        )
    for ex in rejected:
        parts.append(
            "Operator sent THIS DRAFT BACK (discard or needs_changes):\n"
            "Customer review:\n\"\"\"" + ex["review"].strip() + "\"\"\"\n"
            "Draft reply:\n\"\"\"" + ex["draft"].strip() + "\"\"\""
        )
    parts.append("")  # trailing blank line for prompt readability
    return "\n\n".join(parts)


# Module-level aliases preserve test patch points after the refactor.
_call_openai = call_openai
_log_llm_call = log_llm_call
_now_iso = now_iso
_try_load_env = try_load_duckagent_env
_extract_text = extract_text


def _provider() -> str:
    return os.environ.get("DUCK_REVIEW_SCORER_PROVIDER", DEFAULT_PROVIDER).strip().lower()


def _model() -> str:
    return os.environ.get("DUCK_REVIEW_SCORER_MODEL", DEFAULT_MODEL).strip()


def is_disabled() -> bool:
    return _provider() == "disabled"


def is_in_gray_zone(score: int, fail_closed: list[Any] | None) -> bool:
    if fail_closed:
        return False
    return GRAY_ZONE_MIN <= score <= GRAY_ZONE_MAX


MAX_REASON_CHARS = 240


_CODE_FENCE_OPEN = re.compile(r"^```(?:json)?\s*", re.IGNORECASE)
_CODE_FENCE_CLOSE = re.compile(r"\s*```\s*$")


def _strip_code_fence(text: str) -> str:
    """Tolerate stray ```json ... ``` wrappers OpenAI sometimes adds
    even with response_format=json_object set."""
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = _CODE_FENCE_OPEN.sub("", cleaned)
        cleaned = _CODE_FENCE_CLOSE.sub("", cleaned)
    return cleaned.strip()


def _parse_json_output(text: str) -> dict[str, Any] | None:
    cleaned = _strip_code_fence(text)
    if not cleaned:
        return None
    try:
        data = json.loads(cleaned)
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _validate_json_shape(
    parsed: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Validate the scorer JSON contract.

    Returns (verdict_dict, failure_list). verdict_dict is None when any
    contract violation fired. The failure_list names exactly which
    rules tripped so the call log captures audit-quality detail."""
    failures: list[str] = []
    if parsed is None:
        return None, ["json_unparseable"]

    verdict_raw = parsed.get("verdict")
    reason_raw = parsed.get("reason")

    if not isinstance(verdict_raw, str):
        failures.append("missing_verdict")
    else:
        verdict_clean = verdict_raw.strip().lower().rstrip(".:,;!?\"'")
        # Tolerate "yes." / "Yes," / "YES" — strict JSON-mode usually
        # returns clean "yes"/"no" but enums aren't enforced by
        # response_format alone, so be lenient about whitespace and
        # trailing punctuation. Reject anything else.
        if verdict_clean not in {"yes", "no"}:
            failures.append("verdict_not_yes_or_no")

    if not isinstance(reason_raw, str):
        failures.append("missing_reason")
    elif len(reason_raw.strip()) > MAX_REASON_CHARS:
        # Same 240-char limit as the prior two-line parser. Catches
        # the model dumping paragraphs into the reason field.
        failures.append("reason_too_long")

    if failures:
        return None, failures

    return {
        "verdict": verdict_clean,
        "reason": reason_raw.strip(),
    }, []


def _parse_verdict(text: str) -> dict[str, Any] | None:
    """Compatibility shim — exercises the JSON contract end-to-end so
    callers and tests that built on the old two-line parser keep
    working. Returns None on any contract violation (same sanity-gate
    behavior as before; the failure modes are different in JSON mode
    but the caller contract is preserved)."""
    parsed = _parse_json_output(text or "")
    verdict, _ = _validate_json_shape(parsed)
    return verdict


def evaluate_gray_zone(
    *,
    review_text: str,
    draft_text: str,
    score: int,
    component_scores: dict[str, Any] | None,
    fail_closed: list[Any] | None,
    artifact_id: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any] | None:
    """Run the LLM tie-breaker for a gray-zone review_reply item.

    Returns a structured verdict dict when the LLM was consulted and produced
    a parseable answer:
        {
            "verdict": "yes" | "no",
            "reason": "...",
            "model": str,
            "provider": str,
            "generated_at": iso,
            "usage": {...},
        }

    Returns None when:
      - the score isn't in the gray zone
      - the LLM is disabled
      - OPENAI_API_KEY is missing
      - the API call fails
      - the response can't be parsed into yes/no + reason

    The caller falls back to the rule-based decision on None.
    """
    if is_disabled():
        return None
    if not is_in_gray_zone(score, fail_closed):
        return None
    if not (review_text or "").strip():
        return None

    _try_load_env()
    if not os.environ.get("OPENAI_API_KEY"):
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": artifact_id,
            "kind": "review_reply_score",
            "outcome": "missing_api_key",
        })
        return None

    component_summary = ", ".join(
        f"{k}={v}" for k, v in (component_scores or {}).items() if v is not None
    ) or "n/a"
    few_shot_block = _format_few_shot_block(_load_feedback_calibration_examples())
    prompt = PROMPT_TEMPLATE.format(
        review_text=review_text.strip(),
        draft_text=(draft_text or "").strip() or "(no draft captured)",
        score=score,
        component_summary=component_summary,
        few_shot_block=few_shot_block,
    )

    provider = _provider()
    model = _model()
    if provider != "openai":
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": artifact_id,
            "kind": "review_reply_score",
            "outcome": f"unsupported_provider:{provider}",
        })
        return None

    api_response = _call_openai(
        prompt,
        model=model,
        timeout=timeout_seconds,
        # Native JSON mode — same pattern as the rewriter. Schema
        # ({verdict, reason}) is enforced by _validate_json_shape;
        # response_format only guarantees parseability.
        response_format={"type": "json_object"},
        # Lower temperature for structured output, mirrors the
        # rewriter refactor.
        temperature=0.3,
    )
    if api_response is None or "error" in api_response:
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": artifact_id,
            "kind": "review_reply_score",
            "provider": provider,
            "model": model,
            "outcome": "api_failure",
            "error": (api_response or {}).get("error"),
            "body": (api_response or {}).get("body"),
            "elapsed_seconds": (api_response or {}).get("elapsed_seconds"),
            "prompt": prompt,
        })
        return None

    raw_text = _extract_text(api_response)
    parsed_json = _parse_json_output(raw_text)
    verdict, schema_failures = _validate_json_shape(parsed_json)
    usage = api_response.get("usage") or {}
    log_entry: dict[str, Any] = {
        "at": _now_iso(),
        "artifact_id": artifact_id,
        "kind": "review_reply_score",
        "provider": provider,
        "model": model,
        "outcome": "ok" if verdict else "sanity_failed",
        "sanity_failures": schema_failures,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "elapsed_seconds": api_response.get("elapsed_seconds"),
        "rule_based_score": score,
        "prompt": prompt,
    }
    if verdict:
        log_entry["verdict"] = verdict["verdict"]
        log_entry["reason"] = verdict["reason"]
        log_entry["output_text"] = raw_text
    else:
        log_entry["rejected_output_text"] = raw_text
    _log_llm_call(log_entry)

    if not verdict:
        return None

    return {
        "verdict": verdict["verdict"],
        "reason": verdict["reason"],
        "model": model,
        "provider": provider,
        "generated_at": _now_iso(),
        "usage": usage,
    }
