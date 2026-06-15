"""Operator overrides for the theme classifier's `needs_review` flags.

When the classifier flags a product (its LLM pick disagrees with the
deterministic keyword cross-check), the operator reviews it in the
Decision Inbox and decides: adopt the suggestion (`approve`), keep the
existing category (`keep_current`), or pick any taxonomy category
(`change_to`). This module is the persistent record of those decisions —
the direct analog of `catalog_aliases.record_catalog_alias`: a single
idempotent way to capture every operator decision so the producer (here,
the theme classifier) learns from it and never re-asks.

Unlike catalog_aliases (append-only, a product can have many theme
aliases), a product has exactly ONE current category, so this store is
**upsert-by-handle**: re-deciding overwrites the prior decision.

The classifier consults `load_theme_review_decisions` and re-asserts the
operator's `chosen_category` on every (re)classification, so a
taxonomy-version bump can never re-flag a corrected product.

Schema:

    {
      "decisions": [
        {
          "handle": "safari-duck",
          "product_id": "7937966375095",
          "chosen_category": "Professions & Careers",
          "decision_type": "change_to",          # approve | keep_current | change_to
          "previous_category": "Wild West Adventure",
          "suggested_category": "Animals & Pets", # from keyword_evidence_favors (may be null)
          "flag_reason": "keyword_evidence_favors:Animals & Pets",
          "taxonomy_version_at_decision": 3,
          "recorded_at": "2026-06-15T...-04:00",
          "source_artifact_id": "operator::theme-review::safari-duck::2026-06-15"
        }
      ]
    }

Idempotency key: `handle` (latest decision wins).
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
THEME_REVIEW_DECISIONS_PATH = DUCK_OPS_ROOT / "state" / "theme_review_decisions.json"

# Frozen factory-default — never changed by monkeypatch. The source guard
# compares the LIVE THEME_REVIEW_DECISIONS_PATH against this; conftest
# patches the live value to tmp, so they differ and the write proceeds.
# See CLAUDE.md "Cross-repo state writes need both-repo isolation +
# source-level guard" + workflow_control._FROZEN_PRODUCTION_WORKFLOW_CONTROL_PATH.
_FROZEN_THEME_REVIEW_DECISIONS_PATH = THEME_REVIEW_DECISIONS_PATH.resolve()
_TEST_MODE_ENV = "DUCK_TEST_MODE"

_VALID_DECISION_TYPES = {"approve", "keep_current", "change_to"}


class TestModeRefusalError(RuntimeError):
    """Raised when record_theme_review_decision would write to the
    factory-default PRODUCTION store while DUCK_TEST_MODE=1. Loud per
    [[feedback_swallowed_errors_lie]] — a silent no-op would mask tests
    that forgot to patch the path. Path-patched tests sail through."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _refusing_test_mode_prod_write(target_path: Path) -> bool:
    if str(os.environ.get(_TEST_MODE_ENV) or "").strip() not in {"1", "true", "TRUE", "yes"}:
        return False
    return target_path.resolve() == _FROZEN_THEME_REVIEW_DECISIONS_PATH


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"decisions": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {"decisions": []}
    if not isinstance(payload, dict):
        return {"decisions": []}
    if not isinstance(payload.get("decisions"), list):
        payload["decisions"] = []
    return payload


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    with tempfile.NamedTemporaryFile(
        mode="w", dir=str(path.parent), prefix=path.name + ".",
        suffix=".tmp", delete=False, encoding="utf-8",
    ) as tmp:
        tmp.write(serialized)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def record_theme_review_decision(
    *,
    handle: str,
    product_id: str = "",
    chosen_category: str,
    decision_type: str,
    previous_category: str = "",
    suggested_category: str | None = None,
    flag_reason: str = "",
    taxonomy_version_at_decision: int | None = None,
    input_hash_at_decision: str | None = None,
    source_artifact_id: str = "",
    path: Path | None = None,
) -> dict[str, Any]:
    """Upsert an operator theme decision (keyed by handle; latest wins).

    Returns the stored record. Raises ValueError on missing/invalid
    fields and TestModeRefusalError when it would write to the frozen
    production path under DUCK_TEST_MODE=1."""
    handle = str(handle or "").strip()
    chosen_category = str(chosen_category or "").strip()
    decision_type = str(decision_type or "").strip()
    if not handle:
        raise ValueError("handle is required")
    if not chosen_category:
        raise ValueError("chosen_category is required")
    if decision_type not in _VALID_DECISION_TYPES:
        raise ValueError(f"decision_type must be one of {sorted(_VALID_DECISION_TYPES)}, got {decision_type!r}")

    target_path = path or THEME_REVIEW_DECISIONS_PATH
    if _refusing_test_mode_prod_write(target_path):
        raise TestModeRefusalError(
            "record_theme_review_decision refused: DUCK_TEST_MODE=1 and the "
            "write would land in production theme_review_decisions.json. Patch "
            "THEME_REVIEW_DECISIONS_PATH to a tmp path (see duck-ops conftest.py)."
        )

    payload = _load(target_path)
    record = {
        "handle": handle,
        "product_id": str(product_id or "").strip(),
        "chosen_category": chosen_category,
        "decision_type": decision_type,
        "previous_category": str(previous_category or "").strip(),
        "suggested_category": (str(suggested_category).strip() if suggested_category else None),
        "flag_reason": str(flag_reason or "").strip(),
        "taxonomy_version_at_decision": taxonomy_version_at_decision,
        "input_hash_at_decision": (str(input_hash_at_decision).strip() if input_hash_at_decision else None),
        "recorded_at": _now_iso(),
        "source_artifact_id": str(source_artifact_id or f"operator::theme-review::{handle}::{_now_iso()[:10]}"),
    }
    # Upsert by handle.
    decisions = [d for d in payload["decisions"] if isinstance(d, dict) and str(d.get("handle") or "").strip() != handle]
    decisions.append(record)
    payload["decisions"] = decisions
    _atomic_write(target_path, payload)
    return record


def load_theme_review_decisions(path: Path | None = None) -> dict[str, dict[str, Any]]:
    """Return {handle: decision} for the classifier to consult. Never raises."""
    target_path = path or THEME_REVIEW_DECISIONS_PATH
    payload = _load(target_path)
    out: dict[str, dict[str, Any]] = {}
    for d in payload.get("decisions") or []:
        if isinstance(d, dict):
            h = str(d.get("handle") or "").strip()
            if h:
                out[h] = d
    return out


def get_decision_for_handle(handle: str, path: Path | None = None) -> dict[str, Any] | None:
    return load_theme_review_decisions(path).get(str(handle or "").strip())


# ─── Keyword false-flag feedback (Surface 20, Phase 4) ────────────────
# When the operator clicks "Keep current" against a keyword_evidence flag,
# the deterministic keyword cross-check over-fired (the safari/spatula bug).
# Log it so the offending per-category `keywords` entries in
# theme_taxonomy.json can be tuned. Upsert-by-handle (one record per
# product). The LOGGING is automatic; the taxonomy edit stays gated.
THEME_KEYWORD_FALSE_FLAGS_PATH = DUCK_OPS_ROOT / "state" / "theme_keyword_false_flags.json"
_FROZEN_THEME_KEYWORD_FALSE_FLAGS_PATH = THEME_KEYWORD_FALSE_FLAGS_PATH.resolve()


def record_keyword_false_flag(
    *,
    handle: str,
    title: str = "",
    kept_category: str = "",
    evidence_category: str = "",
    triggering_keywords: list[str] | None = None,
    taxonomy_version: int | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    """Record that a keyword-evidence flag was rejected by the operator
    (kept the existing category). Upsert-by-handle. Raises
    TestModeRefusalError on a frozen-prod write under DUCK_TEST_MODE=1."""
    handle = str(handle or "").strip()
    if not handle:
        raise ValueError("handle is required")
    target_path = path or THEME_KEYWORD_FALSE_FLAGS_PATH
    if str(os.environ.get(_TEST_MODE_ENV) or "").strip() in {"1", "true", "TRUE", "yes"} \
            and target_path.resolve() == _FROZEN_THEME_KEYWORD_FALSE_FLAGS_PATH:
        raise TestModeRefusalError(
            "record_keyword_false_flag refused: DUCK_TEST_MODE=1 and the write "
            "would land in production theme_keyword_false_flags.json. Patch the path."
        )
    flags = load_keyword_false_flags(target_path)
    record = {
        "handle": handle,
        "title": str(title or "").strip(),
        "kept_category": str(kept_category or "").strip(),
        "evidence_category": str(evidence_category or "").strip(),
        "triggering_keywords": [str(k).strip() for k in (triggering_keywords or []) if str(k).strip()],
        "taxonomy_version": taxonomy_version,
        "recorded_at": _now_iso(),
    }
    flags = [f for f in flags if isinstance(f, dict) and str(f.get("handle") or "").strip() != handle]
    flags.append(record)
    _atomic_write(target_path, {"false_flags": flags})
    return record


def load_keyword_false_flags(path: Path | None = None) -> list[dict[str, Any]]:
    target_path = path or THEME_KEYWORD_FALSE_FLAGS_PATH
    if not target_path.exists():
        return []
    try:
        payload = json.loads(target_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    flags = payload.get("false_flags") if isinstance(payload, dict) else None
    return [f for f in flags if isinstance(f, dict)] if isinstance(flags, list) else []
