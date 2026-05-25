"""
Standalone catalog matcher — finds existing products whose theme tokens
overlap with a proposed concept theme.

Extracted from phase1_observer.match_catalog so the design-brief queue
enrichment path (M.3) and the duplicate-detection LLM (M.4) can call it
without dragging in the rest of phase1_observer's import surface
(Shopify clients, ETL, etc.).

Status semantics — same as phase1_observer:
  "covered" : every theme token appears in some catalog row → exact-ish match
  "partial" : at least one token overlaps (and at least min(2, len)) → gray zone
  "gap"     : no token overlap → safe to add as a new concept

The "partial" status is the LLM disambiguation zone — keyword overlap
exists but isn't conclusive. M.4 uses it as the gating condition.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"
CATALOG_ALIASES_PATH = DUCK_OPS_ROOT / "state" / "catalog_aliases.json"

# Theme stopwords. "duck" is implicit in every product so matching
# against it is noise. The rest are common English connectives that
# carry no concept signal and would false-positive against random
# catalog rows.
_THEME_STOPWORDS = {
    "duck", "the", "and", "for", "with", "from", "into", "onto",
    "this", "that", "these", "those", "their",
}


def normalize_text(value: Any) -> str:
    """Lowercase, strip punctuation to spaces, collapse whitespace."""
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _load_catalog_index(path: Path | None = None) -> dict[str, dict[str, Any]]:
    target = path or CATALOG_INDEX_PATH
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, dict):
        return {}
    return items


def _load_aliases(path: Path | None = None) -> list[dict[str, Any]]:
    target = path or CATALOG_ALIASES_PATH
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    aliases = payload.get("aliases") if isinstance(payload, dict) else None
    return [a for a in (aliases or []) if isinstance(a, dict)]


def _alias_text_by_product_key(aliases: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Group alias theme strings by both product_id and product_handle so
    products can be looked up by either key during haystack assembly."""
    by_key: dict[str, list[str]] = {}
    for record in aliases:
        text = str(record.get("theme") or "").strip()
        if not text:
            continue
        pid = str(record.get("product_id") or "").strip()
        handle = str(record.get("product_handle") or "").strip()
        if pid:
            by_key.setdefault(pid, []).append(text)
        if handle:
            by_key.setdefault(handle, []).append(text)
    return by_key


def _product_haystack(item: dict[str, Any], alias_text: list[str]) -> str:
    fields = [
        item.get("title") or "",
        item.get("handle") or "",
        item.get("category") or "",
        item.get("ai_theme_category") or "",
        item.get("tags") or "",
        item.get("core_terms") or "",
        item.get("concept_variations") or "",
        item.get("manual_aliases") or "",
        " ".join(alias_text),
    ]
    return normalize_text(" ".join(str(field) for field in fields if field))


def _theme_tokens(theme: str) -> list[str]:
    tokens = [token for token in normalize_text(theme).split() if token and token not in _THEME_STOPWORDS]
    # Filter very short tokens — they false-positive (e.g. 'a', 'of').
    return [token for token in tokens if len(token) >= 3]


def find_existing_matches(
    theme: str,
    *,
    catalog_index_path: Path | None = None,
    aliases_path: Path | None = None,
    max_matches: int = 5,
) -> dict[str, Any]:
    """Match a proposed concept theme against the existing catalog.

    Returns:
        {
          "status": "covered" | "partial" | "gap" | "unknown",
          "theme_tokens": [...],
          "matches": [
            {
              "product_id": "...",
              "title": "...",
              "handle": "...",
              "image_src": "...",
              "matched_tokens": [...],
              "match_strength": int (number of token overlaps),
            },
            ...
          ]
        }

    "unknown" is returned when no catalog data is available — the caller
    should treat this as "can't decide, surface it to the operator".
    """
    catalog = _load_catalog_index(catalog_index_path)
    if not catalog:
        return {"status": "unknown", "theme_tokens": [], "matches": []}

    tokens = _theme_tokens(theme)
    if not tokens:
        return {"status": "unknown", "theme_tokens": [], "matches": []}

    aliases = _load_aliases(aliases_path)
    alias_by_key = _alias_text_by_product_key(aliases)

    exact: list[dict[str, Any]] = []
    partial: list[dict[str, Any]] = []
    for pid, item in catalog.items():
        if not isinstance(item, dict):
            continue
        alias_text = alias_by_key.get(str(pid), []) + alias_by_key.get(str(item.get("handle") or ""), [])
        haystack = _product_haystack(item, alias_text)
        matched_tokens = [token for token in tokens if token in haystack]
        if not matched_tokens:
            continue
        match_record = {
            "product_id": str(pid),
            "title": str(item.get("title") or ""),
            "handle": str(item.get("handle") or ""),
            "image_src": str(item.get("image_src") or ""),
            "matched_tokens": matched_tokens,
            "match_strength": len(matched_tokens),
        }
        if len(matched_tokens) == len(tokens):
            exact.append(match_record)
        else:
            # Any overlap that isn't full coverage is the gray zone.
            # M.4's LLM disambiguates these. Looser than the original
            # phase1_observer threshold because the LLM downstream needs
            # to see candidate overlaps, not just the strongest ones.
            partial.append(match_record)

    if exact:
        chosen = sorted(exact, key=lambda r: -r["match_strength"])[:max_matches]
        status = "covered"
    elif partial:
        chosen = sorted(partial, key=lambda r: -r["match_strength"])[:max_matches]
        status = "partial"
    else:
        chosen = []
        status = "gap"

    return {"status": status, "theme_tokens": tokens, "matches": chosen}
