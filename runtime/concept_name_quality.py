#!/usr/bin/env python3
"""Shared concept-name quality checks for trend-to-product gates."""

from __future__ import annotations

import re
from typing import Any


GENERIC_TOKENS = {
    "duck",
    "ducks",
    "rubber",
    "jeep",
    "collectible",
    "figurine",
    "toy",
    "gift",
    "gifts",
    "decor",
    "dashboard",
    "desk",
    "car",
}

ABSTRACT_RELATION_TOKENS = {
    "child",
    "children",
    "maternal",
    "motherhood",
    "love",
    "family",
    "relationship",
    "relationships",
}


def concept_name_tokens(value: Any) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(value or "").lower())


def concept_name_quality(value: Any) -> dict[str, Any]:
    """Return a small, stable contract for whether a trend reads product-ready."""
    tokens = concept_name_tokens(value)
    meaningful = [token for token in tokens if token not in GENERIC_TOKENS]
    abstract = [token for token in meaningful if token in ABSTRACT_RELATION_TOKENS]
    concrete = [token for token in meaningful if token not in ABSTRACT_RELATION_TOKENS]
    issues: list[str] = []

    if not meaningful:
        issues.append("Theme is too generic to become a product concept.")

    if len(abstract) >= 2 and not concrete:
        issues.append("Theme is relationship/occasion wording, not a product-ready visual concept.")

    if "maternal" in abstract and "child" in abstract:
        issues.append("Theme reads like raw search language and needs a clearer product name before build review.")

    return {
        "status": "needs_reframe" if issues else "product_ready",
        "issues": issues,
        "tokens": tokens,
        "meaningful_tokens": meaningful,
        "concrete_tokens": concrete,
    }


def is_product_ready_concept_name(value: Any) -> bool:
    return concept_name_quality(value).get("status") == "product_ready"
