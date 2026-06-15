"""Surface 16 producer: rank "what duck should we build next?" into one
weekly decision queue, writing state/build_next_queue.json.

The score fuses four signals, each normalized 0..1 with a reasons[] trail
(same transparency contract as the occasion selector):

  score = demand x margin x catalog_gap x occasion_fit

  - demand       competitor pull (engagement_score / views+favorites),
                 max-normalized across the candidate pool
  - margin       profit_per_product margin_pct for the nearest confident
                 title match; a neutral estimate (flagged) when no match
  - catalog_gap  1 - overlap with our existing catalog (already-made ->
                 low gap -> suppressed); deterministic token overlap
                 against catalog_index core_terms/title, NO LLM
  - occasion_fit boost when the concept hits an ACTIVE occasion window
                 (read from occasion_intel.json), neutral when evergreen

Everything is deterministic and reads only EXISTING state — no new data
collection, no LLM in the scorer, no eval-gate machinery (convention #5
applies only to LLM-output surfaces). Promote is a separate, approval-
gated step that routes into product_concept_queue; this producer never
spends image/3D credits.

Producer-on-schedule + cheap-reader: weekly (Sunday 07:00, after the
06:30 competitor weekly analysis); readers parse the small state file in
~2ms.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

from governance_review_common import DUCK_AGENT_ROOT, DUCK_OPS_ROOT, load_json, now_local_iso
from workflow_control import TestModeRefusalError
# Reuse the concept-queue feedback contract so a rejected concept stays
# suppressed here too (no parallel, drifting suppression list).
from product_concept_queue import (
    PRODUCT_CONCEPT_FEEDBACK_PATH,
    SUPPRESSING_FEEDBACK_RESOLUTIONS,
    _concept_feedback_key,
)

CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"
OCCASION_INTEL_PATH = DUCK_OPS_ROOT / "state" / "occasion_intel.json"
PROFIT_PER_PRODUCT_PATH = DUCK_OPS_ROOT / "state" / "profit_per_product.json"
COMPETITOR_REPORTS_DIR = DUCK_AGENT_ROOT / "cache" / "competitor" / "reports"
BUILD_NEXT_QUEUE_PATH = DUCK_OPS_ROOT / "state" / "build_next_queue.json"

# Comparison anchor for the DUCK_TEST_MODE write guard (architectural
# convention 4): captured at import, while the guard reads the LIVE module
# constant at call time so test monkeypatching keeps working.
_FROZEN_PRODUCTION_BUILD_NEXT_QUEUE_PATH = BUILD_NEXT_QUEUE_PATH.resolve()

SURFACE_VERSION = 1
TOP_N = 12
# Token overlap (candidate vs an existing catalog item) at/above this is
# treated as "we already make this" -> suppressed as a near-duplicate.
ALREADY_MADE_OVERLAP = 0.6
# Neutral factors for soft signals so a missing input degrades the score
# instead of zeroing it. demand and catalog_gap are the hard drivers.
NEUTRAL_MARGIN = 0.6
NEUTRAL_OCCASION = 0.6

# Tokens that carry no discriminating signal for duck concepts.
_STOPWORDS = {
    "duck", "ducks", "ducky", "3d", "3dprinted", "printed", "printing", "print",
    "figure", "figurine", "toy", "gift", "gifts", "for", "the", "and", "with",
    "a", "an", "of", "to", "dashboard", "ducking", "jeep", "rubber", "collectible",
    "collectibles", "novelty", "custom", "personalized", "cute", "mini", "small",
}


def _tokens(text: Any) -> set[str]:
    raw = re.split(r"[^a-z0-9]+", str(text or "").lower())
    return {t for t in raw if len(t) > 2 and t not in _STOPWORDS}


def _overlap(a: set[str], b: set[str]) -> float:
    """Coverage of the smaller token set — robust to one title being much
    longer than the other (competitor titles are keyword-stuffed)."""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


# --------------------------------------------------------------------------
# Candidate assembly (competitor demand)
# --------------------------------------------------------------------------

_REPORT_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_competitor_report\.json$")


def _is_snapshot_only_report(report: dict[str, Any]) -> bool:
    """A daily snapshot (2026-06-12 cadence split) carries no LLM analysis —
    period='daily_snapshot', empty ducks_to_build/trending_products. Only the
    weekly analysis has build candidates; snapshot-only reports must be
    skipped or Build-Next reads an empty file and produces 0."""
    if str(report.get("period") or "").strip() == "daily_snapshot":
        return True
    ai = report.get("ai_insights")
    if isinstance(ai, dict) and ai.get("_snapshot_only"):
        return True
    return False


def latest_competitor_report(reports_dir: Path = COMPETITOR_REPORTS_DIR) -> tuple[dict[str, Any], str | None]:
    """Newest dated <YYYY-MM-DD>_competitor_report.json that actually carries
    build candidates (i.e. a WEEKLY analysis, not a daily snapshot). Returns
    ({}, None) when none exist — a missing demand source degrades the surface,
    never crashes it. Only date-prefixed filenames qualify (a dev/test report
    like test-foo_competitor_report.json must never be chosen).

    2026-06-15: the competitor cadence split (06-12) made daily runs write
    snapshot-only reports with empty ducks_to_build to the SAME filename, so
    the newest file is usually an empty snapshot. Walk newest-first and skip
    snapshot-only reports so this reads the latest real analysis."""
    try:
        files = sorted((p for p in Path(reports_dir).glob("*_competitor_report.json")
                        if _REPORT_DATE_RE.match(p.name)), reverse=True)
    except OSError:
        files = []
    for path in files:
        report = load_json(path, {})
        if not isinstance(report, dict):
            continue
        if _is_snapshot_only_report(report):
            continue
        return report, path.name
    return {}, None


def assemble_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Union ducks_to_build + trending_products, deduped by listing_id.
    ducks_to_build wins on conflict (it carries the priority field)."""
    by_id: dict[str, dict[str, Any]] = {}
    for key in ("trending_products", "ducks_to_build"):
        for row in report.get(key) or []:
            if not isinstance(row, dict):
                continue
            lid = str(row.get("listing_id") or row.get("title") or "")
            if not lid:
                continue
            merged = {**by_id.get(lid, {}), **row}
            merged["_sources"] = sorted(set(merged.get("_sources", []) + [key]))
            by_id[lid] = merged
    return list(by_id.values())


# --------------------------------------------------------------------------
# Factor scorers — each returns (value in 0..1, reason string, extras dict)
# --------------------------------------------------------------------------

def _demand_strength(row: dict[str, Any]) -> float:
    """Raw demand magnitude before pool normalization."""
    eng = row.get("engagement_score")
    if isinstance(eng, (int, float)) and not isinstance(eng, bool):
        return float(eng)
    views = row.get("views") or 0
    favorites = row.get("favorites") or 0
    try:
        return float(views) + 5.0 * float(favorites)
    except (TypeError, ValueError):
        return 0.0


def score_demand(row: dict[str, Any], pool_max: float) -> tuple[float, str]:
    strength = _demand_strength(row)
    if pool_max <= 0:
        return 0.0, "no demand signal in competitor pool"
    value = max(0.0, min(1.0, strength / pool_max))
    return value, (
        f"engagement {int(strength)} ({int(row.get('views') or 0)} views, "
        f"{int(row.get('favorites') or 0)} favs)"
    )


def _margin_index(profit: dict[str, Any]) -> tuple[list[tuple[set[str], float]], float | None]:
    """Build (title_tokens, margin_pct) pairs for confident-margin products
    plus a global median margin for the no-match fallback."""
    pairs: list[tuple[set[str], float]] = []
    margins: list[float] = []
    for p in profit.get("products") or []:
        if not isinstance(p, dict):
            continue
        margin = p.get("margin_pct")
        if not isinstance(margin, (int, float)) or isinstance(margin, bool):
            continue
        margins.append(float(margin))
        if not p.get("is_confident_margin"):
            continue
        toks: set[str] = set()
        for variant in p.get("title_variants") or [p.get("label")]:
            toks |= _tokens(variant)
        if toks:
            pairs.append((toks, float(margin)))
    return pairs, (median(margins) if margins else None)


def score_margin(cand_tokens: set[str],
                 margin_pairs: list[tuple[set[str], float]],
                 median_margin: float | None) -> tuple[float, str, bool]:
    best_margin = None
    best_ov = 0.0
    for toks, margin in margin_pairs:
        ov = _overlap(cand_tokens, toks)
        if ov > best_ov:
            best_ov, best_margin = ov, margin
    if best_margin is not None and best_ov >= ALREADY_MADE_OVERLAP:
        return max(0.0, min(1.0, best_margin / 100.0)), f"matched product margin {best_margin:.0f}%", False
    # No confident match: neutral estimate, FLAGGED (never a silent default).
    if median_margin is not None:
        return max(0.0, min(1.0, median_margin / 100.0)), \
            f"estimated from catalog median margin {median_margin:.0f}%", True
    return NEUTRAL_MARGIN, "no margin data; neutral estimate", True


def _catalog_token_sets(catalog_items: dict[str, Any]) -> list[tuple[str, set[str]]]:
    sets: list[tuple[str, set[str]]] = []
    for item in catalog_items.values():
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "") not in ("", "active"):
            continue
        toks = _tokens(item.get("title")) | _tokens(item.get("core_terms"))
        if toks:
            sets.append((str(item.get("title") or item.get("handle") or "?"), toks))
    return sets


def score_catalog_gap(cand_tokens: set[str],
                      catalog_sets: list[tuple[str, set[str]]]) -> tuple[float, str, str | None]:
    best_title, best_ov = None, 0.0
    for title, toks in catalog_sets:
        ov = _overlap(cand_tokens, toks)
        if ov > best_ov:
            best_ov, best_title = ov, title
    gap = max(0.0, 1.0 - best_ov)
    if best_ov >= ALREADY_MADE_OVERLAP:
        return gap, f"already make a similar duck ({best_ov:.0%} overlap: {best_title})", best_title
    if best_title:
        return gap, f"closest existing duck {best_ov:.0%} overlap", None
    return 1.0, "no catalog overlap (open gap)", None


def score_occasion_fit(cand_tokens: set[str],
                       active_occasions: list[dict[str, Any]]) -> tuple[float, str]:
    best = None
    for occ in active_occasions:
        kw_tokens: set[str] = set()
        for kw in occ.get("keywords") or []:
            kw_tokens |= _tokens(kw)
        if cand_tokens & kw_tokens:
            days = occ.get("days_until_peak")
            cand = (occ.get("id"), days if isinstance(days, int) else 999, occ.get("name"))
            if best is None or cand[1] < best[1]:
                best = cand
    if best is not None:
        return 1.0, f"fits ACTIVE occasion {best[2]} (peak in {best[1]}d)"
    return NEUTRAL_OCCASION, "evergreen (no active occasion match)"


# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------

def _feedback_suppressed_keys(feedback: dict[str, Any]) -> set[str]:
    concepts = feedback.get("concepts") if isinstance(feedback.get("concepts"), dict) else {}
    keys: set[str] = set()
    for key, record in concepts.items():
        if not isinstance(record, dict):
            continue
        resolution = str(record.get("latest_resolution") or record.get("resolution") or "").strip().lower()
        if resolution not in SUPPRESSING_FEEDBACK_RESOLUTIONS:
            continue
        keys.add(_concept_feedback_key(key))
        for alias in (record.get("aliases") or []) + (record.get("concept_keys") or []):
            if str(alias or "").strip():
                keys.add(_concept_feedback_key(alias))
    return keys


def build_build_next_queue(*,
                           report: dict[str, Any],
                           report_name: str | None,
                           catalog: dict[str, Any],
                           profit: dict[str, Any],
                           occasion_intel: dict[str, Any],
                           feedback: dict[str, Any],
                           top_n: int = TOP_N) -> dict[str, Any]:
    catalog_items = catalog.get("items") if isinstance(catalog.get("items"), dict) else {}
    catalog_sets = _catalog_token_sets(catalog_items)
    margin_pairs, median_margin = _margin_index(profit)
    active_occasions = [o for o in occasion_intel.get("active_occasions") or [] if isinstance(o, dict)]
    suppressed_keys = _feedback_suppressed_keys(feedback)

    candidates = assemble_candidates(report)
    pool_max = max((_demand_strength(c) for c in candidates), default=0.0)
    classifier_coverage = sum(
        1 for p in catalog_items.values()
        if isinstance(p, dict) and isinstance(p.get("theme_classification"), dict)
    )

    queue: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    for cand in candidates:
        title = str(cand.get("title") or "").strip()
        cand_tokens = _tokens(title) | _tokens(" ".join(str(t) for t in cand.get("tags") or []))
        if not cand_tokens:
            continue

        demand, demand_reason = score_demand(cand, pool_max)
        margin, margin_reason, margin_estimated = score_margin(cand_tokens, margin_pairs, median_margin)
        gap, gap_reason, already_title = score_catalog_gap(cand_tokens, catalog_sets)
        occasion, occasion_reason = score_occasion_fit(cand_tokens, active_occasions)

        entry = {
            "title": title,
            "listing_id": cand.get("listing_id"),
            "shop_name": cand.get("shop_name"),
            "sources": cand.get("_sources") or [],
            "priority": cand.get("priority"),
            "factors": {
                "demand": round(demand, 3),
                "margin": round(margin, 3),
                "catalog_gap": round(gap, 3),
                "occasion_fit": round(occasion, 3),
            },
            "margin_estimated": margin_estimated,
            "reasons": [demand_reason, margin_reason, gap_reason, occasion_reason],
        }

        fb_key = _concept_feedback_key(title)
        if fb_key in suppressed_keys:
            entry["suppressed_reason"] = "operator feedback already resolved/rejected this concept"
            suppressed.append(entry)
            continue
        if gap < (1.0 - ALREADY_MADE_OVERLAP):
            entry["suppressed_reason"] = f"already made — {gap_reason}"
            suppressed.append(entry)
            continue

        entry["score"] = round(demand * margin * gap * occasion, 4)
        queue.append(entry)

    queue.sort(key=lambda e: (-e["score"], str(e["title"])))

    return {
        "surface_version": SURFACE_VERSION,
        "generated_at": now_local_iso(),
        "sources": {
            "competitor_report": report_name,
            "competitor_candidates": len(candidates),
            "catalog_products": len(catalog_items),
            "classifier_coverage": classifier_coverage,
            "confident_margin_products": len(margin_pairs),
            "active_occasions": [o.get("id") for o in active_occasions],
        },
        "queue": queue[:top_n],
        "suppressed": suppressed,
        "queue_count": len(queue),
        "suppressed_count": len(suppressed),
    }


# --------------------------------------------------------------------------
# Write (three-layer isolation: conftest redirect + this guard + audit test)
# --------------------------------------------------------------------------

def _refusing_test_mode_prod_write() -> bool:
    if os.environ.get("DUCK_TEST_MODE") != "1":
        return False
    return Path(BUILD_NEXT_QUEUE_PATH).resolve() == _FROZEN_PRODUCTION_BUILD_NEXT_QUEUE_PATH


def write_build_next_queue(payload: dict[str, Any]) -> Path:
    if _refusing_test_mode_prod_write():
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but BUILD_NEXT_QUEUE_PATH still points at the "
            "production state file — a test is about to pollute prod. "
            "Monkeypatch build_next_engine.BUILD_NEXT_QUEUE_PATH to a tmp path."
        )
    path = Path(BUILD_NEXT_QUEUE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Rank what duck to build next")
    parser.add_argument("--dry-run", action="store_true", help="Print summary, don't write")
    args = parser.parse_args()

    report, report_name = latest_competitor_report()
    payload = build_build_next_queue(
        report=report,
        report_name=report_name,
        catalog=load_json(CATALOG_INDEX_PATH, {}),
        profit=load_json(PROFIT_PER_PRODUCT_PATH, {}),
        occasion_intel=load_json(OCCASION_INTEL_PATH, {}),
        feedback=load_json(PRODUCT_CONCEPT_FEEDBACK_PATH, {"concepts": {}}),
    )

    if not args.dry_run:
        path = write_build_next_queue(payload)
        print(f"[build-next] {payload['queue_count']} ranked, "
              f"{payload['suppressed_count']} suppressed -> {path}")
    else:
        print(f"[build-next] (dry-run) {payload['queue_count']} ranked, "
              f"{payload['suppressed_count']} suppressed")
    for entry in payload["queue"][:5]:
        print(f"  - {entry['score']:.3f}  {entry['title'][:60]}  "
              f"[{', '.join(entry['reasons'][:1])}]")
    if report_name is None:
        print("  [warn] no competitor report found — demand signal absent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
