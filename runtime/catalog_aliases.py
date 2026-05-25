"""
Append-only writer for state/catalog_aliases.json — the operator-curated
theme→product map that phase1_observer's `match_catalog` consults via
the `manual_aliases` field when comparing proposed concepts against the
existing catalog.

The file is the load-bearing dedup signal: every entry teaches the
matcher one more way operators describe a product they already have
("flamingo duck" → Flamingo Duck). Today entries are added by hand or
appear as a side-effect of trend processing. This module gives the
duplicate-discard path a single, idempotent way to record an alias so
the matcher learns from every operator decision.

Schema (kept in sync with the loader in phase1_observer._build_products_index):

    {
      "aliases": [
        {
          "theme": "flamingo duck",
          "normalized_theme": "flamingo duck",
          "product_id": "8021019656375",
          "product_title": "Flamingo Duck",
          "product_handle": "flamingo-duck",
          "recorded_at": "2026-05-24T12:34:56-04:00",
          "source_artifact_id": "operator::flamingo-duck::2026-05-24"
        },
        ...
      ]
    }

Idempotency key: (normalized_theme, product_id). Re-recording the same
pair is a no-op; recording the same theme against a *different* product
is allowed (two products legitimately share a theme).
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
CATALOG_ALIASES_PATH = DUCK_OPS_ROOT / "state" / "catalog_aliases.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def normalize_theme(value: Any) -> str:
    """Lowercase + whitespace-collapsed theme. Mirrors the matcher's
    `normalize_text` behavior for the fields it cares about."""
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _load_aliases(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"aliases": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {"aliases": []}
    if not isinstance(payload, dict):
        return {"aliases": []}
    if not isinstance(payload.get("aliases"), list):
        payload["aliases"] = []
    return payload


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=str(path.parent),
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        tmp.write(serialized)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def record_catalog_alias(
    *,
    theme: str,
    product_id: str,
    product_title: str = "",
    product_handle: str = "",
    source_artifact_id: str = "",
    path: Path | None = None,
) -> dict[str, Any] | None:
    """Append a theme→product alias to catalog_aliases.json.

    Returns the appended record on success, or None when the (theme,
    product_id) pair already exists (idempotent no-op). Raises ValueError
    on missing required fields — the caller is expected to know which
    product the alias belongs to.
    """
    normalized = normalize_theme(theme)
    pid = str(product_id or "").strip()
    if not normalized:
        raise ValueError("theme must be a non-empty string")
    if not pid:
        raise ValueError("product_id is required")

    target_path = path or CATALOG_ALIASES_PATH
    payload = _load_aliases(target_path)
    aliases = payload["aliases"]

    for existing in aliases:
        if not isinstance(existing, dict):
            continue
        if normalize_theme(existing.get("normalized_theme") or existing.get("theme")) == normalized and str(existing.get("product_id") or "").strip() == pid:
            return None

    record = {
        "theme": str(theme).strip(),
        "normalized_theme": normalized,
        "product_id": pid,
        "product_title": str(product_title or "").strip(),
        "product_handle": str(product_handle or "").strip(),
        "recorded_at": _now_iso(),
        "source_artifact_id": str(source_artifact_id or f"operator::{normalized.replace(' ', '-')}::{_now_iso()[:10]}"),
    }
    aliases.append(record)
    payload["aliases"] = aliases
    _atomic_write(target_path, payload)
    return record
