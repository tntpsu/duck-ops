"""
LLM hint parser for the jeepfact rewriter.

The existing rule-based build_jeepfact_rewrite_text in review_loop.py
parses the operator's hint via literal substring matches (e.g. "short" →
caption_tone=shorter, "no sports" → avoid_tags=sports). That misses
hints phrased differently ("keep it brief", "skip the football angle",
"give me something for the holidays"). This module asks the LLM to
extract structured config from free-text hints, augmenting the rule
parse with semantic understanding.

Output is a config dict mirroring the rule-based keys, not prose. The
caller merges LLM overrides on top of the rule-based defaults; the LLM
never deletes config the rules set.

Kill switch:
  DUCK_JEEPFACT_REWRITE_PROVIDER=disabled forces pure rule-based parsing.
  DUCK_JEEPFACT_REWRITE_MODEL=gpt-4o-mini (default override).
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
MAX_TOKENS = 220

# Allowed config keys + their permitted values. Anything outside this
# schema gets dropped at sanity. Keeps the LLM honest.
ALLOWED_CONFIG_SCHEMA: dict[str, set[str]] = {
    "selection_mode": {"reroll_all", "same_ducks_new_facts", "new_ducks_same_facts"},
    "hook_style": {"punchy", "curious", "funny"},
    "caption_tone": {"standard", "shorter", "warmer", "educational"},
    "template_policy": {"new_templates", "keep_templates"},
}
ALLOWED_TAG_LIST = {
    "seasonal", "summer", "spring", "fall", "winter", "holidays",
    "sports", "patriotic", "beach", "camping", "outdoors", "humor",
}


_call_openai = call_openai
_log_llm_call = log_llm_call
_now_iso = now_iso
_try_load_env = try_load_duckagent_env
_extract_text = extract_text


PROMPT_TEMPLATE = """You are parsing an operator's free-text feedback for a Jeep Fact social-post rewrite at myJeepDuck (a small business that 3D-prints custom rubber-duck figurines). Your job is to translate their feedback into a structured config the downstream pipeline can act on.

Operator's feedback:
\"\"\"{hint_text}\"\"\"

REQUIRED field — always present in your JSON output:
  acknowledged_terms: array of 1-6 short strings (each 1-5 words) naming the SPECIFIC things you understood from the operator's feedback above. NOT generic words like "feedback" or "hint" — concrete phrases from the operator like "no sports", "make it shorter", "holiday angle". This is your receipt that you actually read the feedback. If the operator's feedback is gibberish or unparseable, return acknowledged_terms with one entry naming why (e.g. ["unparseable"]). Empty arrays are never valid — if you can't acknowledge anything, the feedback is unparseable.

Optional fields — include ONLY when the feedback speaks to them:

  selection_mode: "reroll_all" | "same_ducks_new_facts" | "new_ducks_same_facts"
  hook_style: "punchy" | "curious" | "funny"
  caption_tone: "standard" | "shorter" | "warmer" | "educational"
  template_policy: "new_templates" | "keep_templates"
  prefer_tags: array of strings from this list only: seasonal, summer, spring, fall, winter, holidays, sports, patriotic, beach, camping, outdoors, humor
  avoid_tags: array of strings from the same list
  operator_note: a one-sentence paraphrase of what the operator is asking for, suitable for the downstream generator

Rules:
- Output ONLY a JSON object. No preamble, no markdown fences, no quotation marks around the whole thing.
- Use only the allowed values above. If the feedback names something outside the allowed values, drop that key.
- Don't invent feedback — if the operator didn't speak to a key, leave it out.
- acknowledged_terms is ALWAYS required. A bare {{}} response is never valid — it's indistinguishable from "the LLM lost the prompt" and the operator's hint would get silently dropped.

Example good output for hint "keep it brief and no sports angle":
  {{"acknowledged_terms": ["keep it brief", "no sports angle"], "caption_tone": "shorter", "avoid_tags": ["sports"]}}"""


def _provider() -> str:
    return os.environ.get("DUCK_JEEPFACT_REWRITE_PROVIDER", DEFAULT_PROVIDER).strip().lower()


def _model() -> str:
    return os.environ.get("DUCK_JEEPFACT_REWRITE_MODEL", DEFAULT_MODEL).strip()


def is_disabled() -> bool:
    return _provider() == "disabled"


def _build_prompt(hint: str) -> str:
    return PROMPT_TEMPLATE.format(hint_text=(hint or "").strip())


def _strip_code_fence(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```\s*$", "", cleaned)
    return cleaned.strip()


def _parse_config(raw: str) -> dict[str, Any] | None:
    cleaned = _strip_code_fence(raw)
    if not cleaned:
        return None
    try:
        data = json.loads(cleaned)
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


_MAX_ACKNOWLEDGED_TERMS = 6
_MAX_TERM_WORDS = 5


def _validate_acknowledged_terms(raw_config: dict[str, Any]) -> tuple[list[str] | None, list[str]]:
    """Validate the required acknowledged_terms field.

    Returns (cleaned_terms, failure_list). cleaned_terms is None when
    the field is missing, wrong type, or empty — those are the exact
    "LLM lost the prompt" failure modes the 2026-05-30 schema
    tightening targets. Before this gate, a bare `{}` response was
    indistinguishable from a healthy "no config overrides apply"
    response, and the operator's hint silently disappeared.

    failure_list names which specific contract violation fired so
    the call log captures audit-quality detail for prompt-tuning."""
    failures: list[str] = []
    terms = raw_config.get("acknowledged_terms")
    if terms is None:
        return None, ["missing_acknowledged_terms"]
    if not isinstance(terms, list):
        return None, ["acknowledged_terms_wrong_type"]
    cleaned: list[str] = []
    for term in terms[:_MAX_ACKNOWLEDGED_TERMS]:
        if not isinstance(term, str):
            continue
        s = term.strip()
        if not s:
            continue
        if len(s.split()) > _MAX_TERM_WORDS:
            # Cap word length per term — the prompt asks for short
            # phrases. Long blobs usually mean the model dumped
            # paraphrased prose into the field instead of pulled
            # discrete tokens from the hint.
            continue
        cleaned.append(s)
    if not cleaned:
        # Empty list (or all entries filtered) is the exact failure
        # we're guarding against — bare `{}` repackaged as
        # `{"acknowledged_terms": []}` would still leave the hint
        # invisible. Force a non-empty acknowledgment.
        failures.append("acknowledged_terms_empty")
        return None, failures
    return cleaned, []


def _validate_config(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Drop keys/values outside the allowed schema. Returns the cleaned
    config (may be empty). Never raises.

    Does NOT check acknowledged_terms — that's the schema gate, run
    upstream in generate_jeepfact_config_via_llm. This function is
    only the per-key config-value cleanup."""
    cleaned: dict[str, Any] = {}
    for key, allowed in ALLOWED_CONFIG_SCHEMA.items():
        value = raw_config.get(key)
        if isinstance(value, str) and value in allowed:
            cleaned[key] = value
    for tag_key in ("prefer_tags", "avoid_tags"):
        value = raw_config.get(tag_key)
        if isinstance(value, list):
            kept = [t for t in value if isinstance(t, str) and t.lower() in ALLOWED_TAG_LIST]
            if kept:
                cleaned[tag_key] = sorted(dict.fromkeys(t.lower() for t in kept))
    note = raw_config.get("operator_note")
    if isinstance(note, str) and 1 <= len(note.strip()) <= 240:
        cleaned["operator_note"] = note.strip()
    return cleaned


def generate_jeepfact_config_via_llm(
    item: dict[str, Any],
    *,
    hint: str = "",
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any] | None:
    """Parse an operator hint into the Jeep Fact rewrite config schema.

    Returns the validated config dict on success, or None on any failure
    (disabled, missing key, API error, unparseable JSON, all keys
    filtered out by the validator). Caller falls back to the rule-based
    hint parser when None.
    """
    if str(item.get("flow") or "") != "jeepfact":
        return None
    if is_disabled():
        return None
    if not (hint or "").strip():
        # Nothing to parse → don't burn tokens. Rule-based defaults apply.
        return None

    _try_load_env()
    if not os.environ.get("OPENAI_API_KEY"):
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "kind": "jeepfact_rewrite",
            "outcome": "missing_api_key",
        })
        return None

    provider = _provider()
    model = _model()
    if provider != "openai":
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "kind": "jeepfact_rewrite",
            "outcome": f"unsupported_provider:{provider}",
        })
        return None

    prompt = _build_prompt(hint)
    api_response = _call_openai(
        prompt,
        model=model,
        timeout=timeout_seconds,
        max_tokens=MAX_TOKENS,
        temperature=0.1,  # structured-output task — low temperature
        # Native JSON mode (same pattern as the rewriter/scorer
        # refactors shipped 2026-05-30). Schema (acknowledged_terms +
        # the optional config keys) is enforced by the validators
        # below — response_format only guarantees parseability.
        response_format={"type": "json_object"},
    )
    if api_response is None or "error" in api_response:
        _log_llm_call({
            "at": _now_iso(),
            "artifact_id": item.get("artifact_id"),
            "kind": "jeepfact_rewrite",
            "provider": provider,
            "model": model,
            "outcome": "api_failure",
            "error": (api_response or {}).get("error"),
            "body": (api_response or {}).get("body"),
            "elapsed_seconds": (api_response or {}).get("elapsed_seconds"),
            "prompt": prompt,
            "hint": hint.strip(),
        })
        return None

    text = _extract_text(api_response)
    raw_config = _parse_config(text)
    usage = api_response.get("usage") or {}

    # Two-stage validation:
    #   1. JSON must parse (raw_config is not None).
    #   2. acknowledged_terms must be present, a list, non-empty.
    # Stage 2 is the 2026-05-30 schema tightening — a bare {} response
    # used to count as a healthy "no config overrides apply" output;
    # now it routes to sanity_failed so the operator's hint isn't
    # silently dropped.
    schema_failures: list[str] = []
    if raw_config is None:
        schema_failures = ["unparseable_json"]
        acknowledged: list[str] | None = None
        config: dict[str, Any] = {}
    else:
        acknowledged, schema_failures = _validate_acknowledged_terms(raw_config)
        config = _validate_config(raw_config)

    sanity_passed = not schema_failures
    log_entry: dict[str, Any] = {
        "at": _now_iso(),
        "artifact_id": item.get("artifact_id"),
        "kind": "jeepfact_rewrite",
        "provider": provider,
        "model": model,
        "outcome": "ok" if sanity_passed else "sanity_failed",
        "sanity_failures": schema_failures,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "elapsed_seconds": api_response.get("elapsed_seconds"),
        "hint_present": True,
        "hint": hint.strip(),
        "output_length": len(text),
        "prompt": prompt,
    }
    if sanity_passed:
        log_entry["output_text"] = text
        log_entry["parsed_config"] = config
        log_entry["acknowledged_terms"] = acknowledged
    else:
        log_entry["rejected_output_text"] = text
    _log_llm_call(log_entry)

    if not sanity_passed:
        return None
    return {
        "config": config,
        "acknowledged_terms": acknowledged,
        "source": "llm",
        "model": model,
        "provider": provider,
        "generated_at": _now_iso(),
        "hint": hint.strip(),
        "usage": usage,
    }
