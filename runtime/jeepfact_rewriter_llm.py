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

Return a single JSON object with ONLY these keys (omit any key the feedback doesn't speak to):

  selection_mode: "reroll_all" | "same_ducks_new_facts" | "new_ducks_same_facts"
  hook_style: "punchy" | "curious" | "funny"
  caption_tone: "standard" | "shorter" | "warmer" | "educational"
  template_policy: "new_templates" | "keep_templates"
  prefer_tags: array of strings from this list only: seasonal, summer, spring, fall, winter, holidays, sports, patriotic, beach, camping, outdoors, humor
  avoid_tags: array of strings from the same list
  operator_note: a one-sentence paraphrase of what the operator is asking for, suitable for the downstream generator

Rules:
- Output ONLY the JSON object. No preamble, no markdown fences, no quotation marks around the whole thing.
- Use only the allowed values above. If the feedback names something outside the allowed values, drop that key.
- Don't invent feedback — if the operator didn't speak to a key, leave it out.
- Empty/whitespace feedback → return {{}}."""


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


def _validate_config(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Drop keys/values outside the allowed schema. Returns the cleaned
    config (may be empty). Never raises."""
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
    config = _validate_config(raw_config) if raw_config is not None else {}
    usage = api_response.get("usage") or {}

    # "sanity_failed" only when output was truly unparseable or all keys
    # got filtered. An empty {} on whitespace-equivalent feedback is a
    # legitimate signal that no config overrides apply.
    sanity_passed = raw_config is not None
    log_entry: dict[str, Any] = {
        "at": _now_iso(),
        "artifact_id": item.get("artifact_id"),
        "kind": "jeepfact_rewrite",
        "provider": provider,
        "model": model,
        "outcome": "ok" if sanity_passed else "sanity_failed",
        "sanity_failures": [] if sanity_passed else ["unparseable_json"],
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
    else:
        log_entry["rejected_output_text"] = text
    _log_llm_call(log_entry)

    if not sanity_passed:
        return None
    return {
        "config": config,
        "source": "llm",
        "model": model,
        "provider": provider,
        "generated_at": _now_iso(),
        "hint": hint.strip(),
        "usage": usage,
    }
