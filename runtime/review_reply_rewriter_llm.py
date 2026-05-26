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
from review_reply_rewriter_sanity import evaluate_sanity


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
Bad reply: "Thanks so much for the kind review! Means a lot to me." ← zero specifics from the review, will be rejected.
Good reply: "Capturing the dimples meant looking at the photos again and again — so glad the resemblance hit. Hope your nephew gets a kick out of his lookalike duck."

Return ONLY the reply text, no preamble, no quotation marks around the reply."""


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

    text = _clean_output(_extract_text(api_response))
    sanity = evaluate_sanity(text, review_text=review_text)
    usage = api_response.get("usage") or {}

    log_entry: dict[str, Any] = {
        "at": _now_iso(),
        "artifact_id": item.get("artifact_id"),
        "kind": "review_reply_rewrite",
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
        # Capture failed output too so we can audit why sanity rejected it.
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
