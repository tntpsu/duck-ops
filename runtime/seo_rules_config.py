"""Read the one versioned SEO rules config, from the duck-ops side.

The config itself lives at `duckAgent/config/seo_rules.json`, beside
`models.json`, for two reasons: the rules ENGINE that reads it
(`duckAgent/helpers/seo_rules.py`) is already there and is imported by the
newduck and blog flows, and the dependency only runs one way -- duck-ops
imports duckAgent (this lane's review module already re-execs into duckAgent's
venv), while duckAgent never imports duck-ops. Putting the config here would
have required inventing that second direction.

Before this module the same two fields carried three different bands:

    duckAgent/helpers/seo_rules.py   newduck Shopify title   70-80
    duck-ops/runtime/shopify_seo_review.py   generator       45-70
    duck-ops/runtime/shopify_seo_audit.py    auditor         35-70

So the newduck builder aimed at a window where 9 of its 11 possible lengths
were auditor-rejects, and 20 live titles sit pinned at exactly 70 as a result.

A missing or malformed config RAISES. It does not fall back to defaults: a
plausible default here would silently put every surface back on its own
private numbers, which is the exact failure this config exists to end
([[feedback_plausible_fallbacks_mask_failure]]).
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = DUCK_OPS_ROOT.parent / "duckAgent" / "config" / "seo_rules.json"


class SeoRulesConfigError(RuntimeError):
    """The shared SEO rules config is missing or malformed."""


@lru_cache(maxsize=1)
def load_seo_rules() -> dict[str, Any]:
    try:
        raw = CONFIG_PATH.read_text(encoding="utf-8")
    except OSError as exc:
        raise SeoRulesConfigError(
            f"cannot read the shared SEO rules config at {CONFIG_PATH}: {exc}"
        ) from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SeoRulesConfigError(f"{CONFIG_PATH} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict) or "seo_rules_version" not in data:
        raise SeoRulesConfigError(f"{CONFIG_PATH} has no seo_rules_version")
    return data


def seo_rules_version() -> int:
    return int(load_seo_rules()["seo_rules_version"])


def field_band(platform: str, field: str, which: str) -> tuple[int, int]:
    """The (min, max) character band for one platform field.

    `which` is "generate" -- what a producer aims for -- or "accept", what the
    auditor tolerates. `accept` always contains `generate`; that subset
    relation is what stops the lane re-flagging copy it just wrote, and it is
    enforced by tests/test_seo_rules_config.py in both repos.
    """
    rules = load_seo_rules()
    try:
        spec = rules["platforms"][platform]["fields"][field]
    except (KeyError, TypeError) as exc:
        raise SeoRulesConfigError(f"no band for {platform}.{field}") from exc
    try:
        low, high = spec[which]
    except (KeyError, TypeError, ValueError) as exc:
        raise SeoRulesConfigError(f"no {which!r} band for {platform}.{field}") from exc
    return int(low), int(high)


def accept_band(field: str, platform: str = "shopify") -> tuple[int, int]:
    return field_band(platform, field, "accept")


def generate_band(field: str, platform: str = "shopify") -> tuple[int, int]:
    return field_band(platform, field, "generate")


def copy_quality() -> dict[str, Any]:
    return dict(load_seo_rules().get("copy_quality") or {})


def dangling_tail_chars() -> str:
    return str(copy_quality().get("dangling_tail_chars") or "")


def trailing_stopwords() -> frozenset[str]:
    return frozenset(str(w).lower() for w in (copy_quality().get("trailing_stopwords") or ()))


def issue_codes() -> dict[str, dict[str, Any]]:
    codes = load_seo_rules().get("issue_codes") or {}
    return {str(k): dict(v or {}) for k, v in codes.items()}


def issue_severity(code: str, default: str = "medium") -> str:
    return str((issue_codes().get(code) or {}).get("severity") or default)


def blocking_issue_codes() -> frozenset[str]:
    return frozenset(c for c, spec in issue_codes().items() if spec.get("blocking"))


def advisory_issue_codes() -> frozenset[str]:
    return frozenset(c for c, spec in issue_codes().items() if not spec.get("blocking"))


def title_quality_codes() -> dict[str, dict[str, Any]]:
    """The generator's own quality vocabulary, separate from `issue_codes`.

    `issue_codes` is how the AUDITOR describes live copy; this is how the
    GENERATOR grades a proposal before it is applied.
    """
    codes = load_seo_rules().get("title_quality_codes") or {}
    return {
        str(k): dict(v or {})
        for k, v in codes.items()
        if not str(k).startswith("_") and isinstance(v, dict)
    }


def blocking_title_quality_codes() -> frozenset[str]:
    return frozenset(c for c, spec in title_quality_codes().items() if spec.get("blocking"))
