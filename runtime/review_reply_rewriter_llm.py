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
from pathlib import Path
from typing import Any

from llm_call_helpers import (
    call_openai,
    extract_text,
    log_llm_call,
    now_iso,
    try_load_duckagent_env,
    DEFAULT_MODEL,
    DEFAULT_TIMEOUT_SECONDS,
)
from review_reply_rewriter_sanity import (
    _STOPWORDS,
    _non_stopword_tokens,
    evaluate_sanity,
)
# 2026-06-06: have the rewriter check its OWN output against the
# same deterministic contract that runs downstream in
# quality_gate_pilot.build_review_reply_contract. Previously a
# generic-positive reply on a complaint-adjacent review would
# pass the rewriter's sanity gate (echo + length + no-emoji),
# get returned as "ok", and only be flagged needs_revision when
# the quality gate evaluated the artifact later. The operator
# would still see a bad draft. Now the rewriter catches contract
# failures at generation time and returns None → callers fall
# back to the rule-based public_issue_reply_lines that handles
# issue-acknowledgment correctly. Phase 4C of
# CREATIVE_QUALITY_LOOP_V2_PLAN.md + Phase 1 of
# PROMPT_CONTRACT_AUDIT_PLAN.md.
from review_reply_contract import (
    build_review_reply_contract,
    contract_failure_messages,
)


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
REVIEW_REPLY_FEEDBACK_PATH = DUCK_OPS_ROOT / "state" / "review_reply_feedback.jsonl"

DEFAULT_PROVIDER = "openai"
MAX_TOKENS = 220  # ~3 sentence reply
MAX_FEW_SHOT_EXAMPLES = 4
MAX_FEW_SHOT_REVIEW_CHARS = 300
MAX_FEW_SHOT_REPLY_CHARS = 320


PROMPT_TEMPLATE = """You are the shop owner of myJeepDuck, a small business that 3D-prints custom rubber-duck figurines. You write public replies to customer reviews. You are NOT a generic AI assistant.

CRITICAL RULE — your reply MUST reference at least one specific detail from the customer's review. NOT generic words like "thanks", "review", "kind", "great", "amazing" — those don't count. You need a noun, a recipient, a feature, a product detail, an emotion, an occasion, or a moment the customer mentioned.

Replies that are pure boilerplate (e.g., "Thanks again for the kind review! Means a lot.") will be REJECTED. The fix is to look at the review and pull out something concrete.

STEP 1: Read the review and identify a specific detail to echo. Examples of "specific":
- A recipient: "nephew", "her dad", "best friend"
- A feature: "dimples", "carved name", "Wrangler decals"
- A use: "stocking stuffer", "retirement gift"
- An emotion that's grounded: "crying laughing", "hugged it"

STEP 2: Write the reply, weaving that detail in naturally.
{few_shot_block}
Customer review (verbatim):
\"\"\"{review_text}\"\"\"

Current operator draft (good, but not yet approved):
\"\"\"{draft_text}\"\"\"

Operator's feedback for the rewrite:
{hint_text}

Constraints:
- 1-3 sentences total, roughly 30-90 words.
- Sound like a human shop owner — warm, specific, grounded. Match the voice of the approved examples above when available.
- Never invent facts. Do not promise discounts, future products, refunds, or replacements unless the operator's feedback explicitly says to.
- No emojis. No URLs. No template placeholders like [NAME] or {{customer_name}}.
- Do not address the customer by name.
- Do not say "as an AI" or otherwise reveal the rewrite is automated.
- Do NOT repeat phrases — every clause should carry new information.

REJECTED EXAMPLE (do not produce output like this):
Review: "I ordered ducks for my nephew with his facial features. Right down to his dimples."
Bad JSON: {{"specific_detail_echoed": "kind review", "reply_text": "Thanks so much for the kind review! Means a lot to me."}} ← "kind review" is a generic stopword phrase, not a real detail from the customer.
Good JSON: {{"specific_detail_echoed": "dimples", "reply_text": "Capturing the dimples meant looking at the photos again and again — so glad the resemblance hit. Hope your nephew gets a kick out of his lookalike duck."}}

OUTPUT CONTRACT — return ONLY a JSON object, no preamble, no markdown fences:
{{"specific_detail_echoed": "<2-5 word phrase that you copied or paraphrased from the customer's review above — must be content, not generic words like 'review' or 'kind'>", "reply_text": "<your 1-3 sentence reply that weaves that phrase in>"}}"""


def _load_approved_examples(*, limit: int = MAX_FEW_SHOT_EXAMPLES) -> list[dict[str, str]]:
    """Read the most recent operator-approved review_reply rewrites from the
    feedback log. Each example provides the LLM with a (review, approved reply)
    pair grounded in the operator's own voice. Returns empty list if the log
    doesn't exist yet (cold start) or no entries qualify.
    """
    if not REVIEW_REPLY_FEEDBACK_PATH.exists():
        return []
    examples: list[dict[str, str]] = []
    seen_reviews: set[str] = set()
    try:
        # Read tail of file first by reading all and reversing — file stays tiny.
        with REVIEW_REPLY_FEEDBACK_PATH.open("r", encoding="utf-8") as fh:
            lines = fh.readlines()
    except OSError:
        return []
    for raw in reversed(lines):
        line = raw.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except (ValueError, json.JSONDecodeError):
            continue
        if entry.get("operator_action") != "approve":
            continue
        review = (entry.get("customer_review") or "").strip()
        approved = (entry.get("approved_reply_text") or "").strip()
        if not review or not approved:
            continue
        if len(review) > MAX_FEW_SHOT_REVIEW_CHARS:
            continue
        if len(approved) > MAX_FEW_SHOT_REPLY_CHARS:
            continue
        # Dedupe by review text (same customer review approved multiple times → keep newest)
        review_key = review.lower()
        if review_key in seen_reviews:
            continue
        seen_reviews.add(review_key)
        examples.append({"review": review, "reply": approved})
        if len(examples) >= limit:
            break
    return examples


def _format_few_shot_block(examples: list[dict[str, str]]) -> str:
    if not examples:
        return ""
    blocks = []
    for ex in examples:
        blocks.append(
            "Customer review:\n\"\"\"" + ex["review"].strip() + "\"\"\"\n"
            "Approved reply:\n\"\"\"" + ex["reply"].strip() + "\"\"\""
        )
    return (
        "\nApproved past examples of the myJeepDuck voice (operator approved these — match this tone):\n\n"
        + "\n\n".join(blocks)
        + "\n"
    )


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
    examples = _load_approved_examples()
    few_shot_block = _format_few_shot_block(examples)
    return PROMPT_TEMPLATE.format(
        review_text=(review_text or "").strip() or "(no review text captured)",
        draft_text=(draft_text or "").strip() or "(no draft captured)",
        hint_text=_format_hint(hint),
        few_shot_block=few_shot_block,
    )


# Test/mock-compatible aliases for the shared helpers. Existing tests patch
# `_call_openai` on this module; preserve that attachment after the refactor.
_call_openai = call_openai
_log_llm_call = log_llm_call
_now_iso = now_iso
_try_load_env = try_load_duckagent_env
_extract_text = extract_text


def _clean_output(text: str) -> str:
    """Strip outer quotes the LLM sometimes adds despite instructions."""
    cleaned = text.strip()
    if len(cleaned) >= 2 and cleaned[0] in {'"', "'"} and cleaned[-1] == cleaned[0]:
        cleaned = cleaned[1:-1].strip()
    return cleaned


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
    *,
    review_text: str,
) -> tuple[str | None, list[str]]:
    """Validate the JSON contract. Returns (reply_text, failure_list).

    reply_text is None when any required field is missing or the
    specific_detail_echoed isn't a real review-content phrase. The
    failure_list names exactly which contract violations fired so the
    log captures audit-quality detail for prompt-tuning."""
    failures: list[str] = []
    if parsed is None:
        return None, ["json_unparseable"]

    detail = parsed.get("specific_detail_echoed")
    reply_raw = parsed.get("reply_text")

    if not isinstance(detail, str) or not detail.strip():
        failures.append("missing_specific_detail_echoed")
    if not isinstance(reply_raw, str) or not reply_raw.strip():
        failures.append("missing_reply_text")
    if failures:
        return None, failures

    detail_clean = detail.strip()
    detail_tokens = _non_stopword_tokens(detail_clean)
    if not detail_tokens:
        failures.append("specific_detail_too_generic")
    else:
        review_tokens = _non_stopword_tokens(review_text)
        if review_tokens and not (detail_tokens & review_tokens):
            failures.append("specific_detail_not_in_review")

    if failures:
        return None, failures

    return reply_raw.strip(), []


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
        api_response = _call_openai(
            prompt,
            model=model,
            timeout=timeout_seconds,
            # Native JSON mode: the API guarantees a parseable JSON
            # string. Schema (specific_detail_echoed + reply_text) is
            # still enforced by _validate_json_shape below — JSON mode
            # alone does not guarantee shape, only parseability.
            response_format={"type": "json_object"},
            # 2026-05-30: tightened from default 0.5 → 0.3. Structured
            # output benefits from less sampling variance; warmth in
            # the reply still has headroom at 0.3.
            temperature=0.3,
        )
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
            "kind": "review_reply_rewrite",
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

    raw_output = _extract_text(api_response)
    parsed = _parse_json_output(raw_output)
    reply_from_json, schema_failures = _validate_json_shape(parsed, review_text=review_text)
    usage = api_response.get("usage") or {}

    if reply_from_json is None:
        # JSON-mode contract failures route to sanity_failed (NOT
        # api_failure) — the provider responded successfully; the
        # model's output didn't satisfy the schema. This keeps the OS
        # health card classifier (viewer.py:24755-24759) bucketing it
        # as a prompt/model quality issue rather than a provider
        # outage.
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "kind": "review_reply_rewrite",
            "provider": provider,
            "model": model,
            "outcome": "sanity_failed",
            "sanity_failures": schema_failures,
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "elapsed_seconds": api_response.get("elapsed_seconds"),
            "hint_present": bool((hint or "").strip()),
            "hint": (hint or "").strip(),
            "rejected_output_text": raw_output,
            "output_length": len(raw_output),
            "prompt": prompt,
        })
        return None

    text = _clean_output(reply_from_json)
    sanity = evaluate_sanity(text, review_text=review_text)

    # 2026-06-06: deterministic contract check on the rewriter's
    # own output. Catches the generic-positive-on-complaint
    # failure mode at generation time instead of downstream. The
    # downstream gate in quality_gate_pilot still runs — this is
    # an EARLIER guard, not a replacement.
    contract_outcome: dict[str, Any] | None = None
    contract_failure_list: list[str] = []
    if sanity["passed"]:
        contract_outcome = build_review_reply_contract(
            review_text, text, private_mode=False,
        )
        contract_failure_list = contract_failure_messages(contract_outcome)

    contract_failed = bool(contract_failure_list)

    log_entry: dict[str, Any] = {
        "at": _now_iso(),
        "artifact_id": item.get("artifact_id"),
        "kind": "review_reply_rewrite",
        "provider": provider,
        "model": model,
        "outcome": (
            "contract_failed" if contract_failed
            else ("ok" if sanity["passed"] else "sanity_failed")
        ),
        "sanity_failures": sanity["failures"],
        "contract_failures": contract_failure_list,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "elapsed_seconds": api_response.get("elapsed_seconds"),
        "hint_present": bool((hint or "").strip()),
        "hint": (hint or "").strip(),
        "output_length": len(text),
        "specific_detail_echoed": (parsed or {}).get("specific_detail_echoed"),
        "prompt": prompt,
    }
    if contract_outcome is not None:
        log_entry["contract_classification"] = contract_outcome.get("classification")
    if sanity["passed"] and not contract_failed:
        log_entry["output_text"] = text
    else:
        # Capture failed output too so we can audit why a gate rejected it.
        log_entry["rejected_output_text"] = text
    _log_llm_call(log_entry)

    if not sanity["passed"] or contract_failed:
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
        "specific_detail_echoed": (parsed or {}).get("specific_detail_echoed"),
        "contract_classification": (
            (contract_outcome or {}).get("classification")
        ),
    }
