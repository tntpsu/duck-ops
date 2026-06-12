"""Surface 13 producer: resolve active gift occasions and select the
products to promote for each, writing state/occasion_intel.json.

Occasions are DATA (config/occasion_calendar.json) — recurrence rules,
lead windows, messaging phases, keywords, recipient affinities. Nothing
occasion-specific is hardcoded in flows; Father's Day, July 4th and
Christmas are just rows in the calendar.

Product selection reads catalog_index.json's Surface-12
theme_classification payload:
  - classifier occasions hit (strongest signal, weight 3)
  - recipient affinity overlap (weight 1.5 each)
  - occasion keyword present in tags (supporting, 0.5 each, capped)
Only active products, top-N per occasion, score floor keeps weak
matches out. The selector returns [] rather than inventing matches.

Consumers: duckAgent helpers/occasion_context.py (prompt angles + Etsy
tag candidates) and the occasion OS bracket cards.

Producer-on-schedule + cheap-reader: this runs on a daily 07:25 launchd
cadence; readers parse the small state file in ~2ms.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, load_json, now_local_iso
from workflow_control import TestModeRefusalError

CALENDAR_PATH = DUCK_OPS_ROOT / "config" / "occasion_calendar.json"
CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"
OCCASION_INTEL_PATH = DUCK_OPS_ROOT / "state" / "occasion_intel.json"

# Comparison anchor for the DUCK_TEST_MODE write guard (architectural
# convention 4): captured at import, while the guard reads the LIVE
# module constant at call time so test monkeypatching keeps working.
_FROZEN_PRODUCTION_OCCASION_INTEL_PATH = OCCASION_INTEL_PATH.resolve()

OCCASION_SCORE_FLOOR = 2.0
TOP_N_PRODUCTS = 12
WEIGHT_CLASSIFIER_OCCASION = 3.0
WEIGHT_RECIPIENT = 1.5
WEIGHT_KEYWORD = 0.5
KEYWORD_HITS_CAP = 4


def resolve_next_occurrence(rule: dict[str, Any], today: date) -> date:
    rtype = rule.get("type")
    if rtype == "fixed":
        peak = date(today.year, int(rule["month"]), int(rule["day"]))
        if peak < today:
            peak = date(today.year + 1, int(rule["month"]), int(rule["day"]))
        return peak
    if rtype == "nth_weekday":
        month = int(rule["month"])
        weekday = int(rule["weekday"])  # 0=Mon .. 6=Sun
        nth = int(rule["nth"])

        def _nth_weekday(year: int) -> date:
            first = date(year, month, 1)
            offset = (weekday - first.weekday()) % 7
            return first + timedelta(days=offset + 7 * (nth - 1))

        peak = _nth_weekday(today.year)
        if peak < today:
            peak = _nth_weekday(today.year + 1)
        return peak
    if rtype == "fixed_dates":
        # Explicit per-year dates for holidays with no simple recurrence
        # rule (e.g. Easter / computus). Pick the next listed date on or
        # after today. Maintainer must extend the list before it runs out.
        dates = sorted(date.fromisoformat(d) for d in rule.get("dates") or [])
        upcoming = [d for d in dates if d >= today]
        if upcoming:
            return upcoming[0]
        if dates:
            raise ValueError(f"fixed_dates exhausted (last {dates[-1].isoformat()}); add more dates")
        raise ValueError("fixed_dates rule has no dates")
    raise ValueError(f"unknown rule type: {rtype!r}")


def pick_messaging_phase(phases: list[dict[str, Any]], days_until_peak: int) -> dict[str, Any]:
    usable = sorted(
        (p for p in phases if isinstance(p, dict) and "max_days_until_peak" in p),
        key=lambda p: int(p["max_days_until_peak"]),
    )
    for phase in usable:
        if days_until_peak <= int(phase["max_days_until_peak"]):
            return phase
    return usable[-1] if usable else {"angle": ""}


def resolve_active_occasions(calendar: dict[str, Any], today: date) -> tuple[list[dict[str, Any]], list[str]]:
    """Active = within lead_days of the next peak (peak day inclusive).
    Malformed occasion entries are skipped and reported — one bad row
    must never take the whole producer down."""
    active: list[dict[str, Any]] = []
    errors: list[str] = []
    for occ in calendar.get("occasions") or []:
        try:
            peak = resolve_next_occurrence(occ["rule"], today)
            days_until_peak = (peak - today).days
            if 0 <= days_until_peak <= int(occ.get("lead_days", 0)):
                phase = pick_messaging_phase(occ.get("messaging_phases") or [], days_until_peak)
                active.append({
                    "id": occ["id"],
                    "name": occ.get("name") or occ["id"],
                    "peak_date": peak.isoformat(),
                    "days_until_peak": days_until_peak,
                    "messaging_angle": phase.get("angle") or "",
                    "keywords": [str(k).lower() for k in occ.get("keywords") or []],
                    "recipients": [str(r) for r in occ.get("recipients") or []],
                    "etsy_tag_candidates": [str(t) for t in occ.get("etsy_tag_candidates") or []],
                })
        except Exception as exc:
            errors.append(f"{occ.get('id', '?')}: {type(exc).__name__}: {exc}")
    return active, errors


def select_products_for_occasion(occasion: dict[str, Any],
                                 catalog_items: dict[str, Any],
                                 top_n: int = TOP_N_PRODUCTS) -> list[dict[str, Any]]:
    occ_id = occasion["id"]
    occ_recipients = set(occasion.get("recipients") or [])
    keywords = occasion.get("keywords") or []
    scored: list[dict[str, Any]] = []
    for pid, product in (catalog_items or {}).items():
        if not isinstance(product, dict):
            continue
        if str(product.get("status") or "") != "active":
            continue
        tc = product.get("theme_classification") if isinstance(product.get("theme_classification"), dict) else {}
        tags = str(product.get("tags") or "").lower()
        title = str(product.get("title") or "")

        score = 0.0
        reasons: list[str] = []
        if occ_id in (tc.get("occasions") or []):
            score += WEIGHT_CLASSIFIER_OCCASION
            reasons.append("classifier_occasion")
        recipient_overlap = occ_recipients & set(tc.get("recipients") or [])
        if recipient_overlap:
            score += WEIGHT_RECIPIENT * len(recipient_overlap)
            reasons.append("recipients:" + ",".join(sorted(recipient_overlap)))
        kw_hits = [k for k in keywords if k in tags]
        if kw_hits:
            score += WEIGHT_KEYWORD * min(len(kw_hits), KEYWORD_HITS_CAP)
            reasons.append("keywords:" + ",".join(kw_hits[:KEYWORD_HITS_CAP]))

        if score >= OCCASION_SCORE_FLOOR:
            scored.append({
                "product_id": pid,
                "handle": product.get("handle"),
                "title": title,
                "score": round(score, 2),
                "reasons": reasons,
                "primary_category": tc.get("primary_category") or product.get("ai_theme_category"),
            })
    scored.sort(key=lambda item: (-item["score"], str(item["title"])))
    return scored[:top_n]


def build_occasion_intel(calendar: dict[str, Any], catalog: dict[str, Any], today: date) -> dict[str, Any]:
    items = catalog.get("items") if isinstance(catalog.get("items"), dict) else {}
    active, errors = resolve_active_occasions(calendar, today)
    for occ in active:
        occ["products"] = select_products_for_occasion(occ, items)
        occ["product_count"] = len(occ["products"])
    return {
        "generated_at": now_local_iso(),
        "today": today.isoformat(),
        "calendar_version": calendar.get("calendar_version"),
        "catalog_generated_at": catalog.get("generated_at"),
        "catalog_products_seen": len(items),
        "active_occasions": active,
        "errors": errors,
    }


def _refusing_test_mode_prod_write() -> bool:
    if os.environ.get("DUCK_TEST_MODE") != "1":
        return False
    return Path(OCCASION_INTEL_PATH).resolve() == _FROZEN_PRODUCTION_OCCASION_INTEL_PATH


def write_occasion_intel(payload: dict[str, Any]) -> Path:
    if _refusing_test_mode_prod_write():
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but OCCASION_INTEL_PATH still points at the "
            "production state file — a test is about to pollute prod. "
            "Monkeypatch occasion_engine.OCCASION_INTEL_PATH to a tmp path."
        )
    path = Path(OCCASION_INTEL_PATH)
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
    parser = argparse.ArgumentParser(description="Resolve active occasions + select products")
    parser.add_argument("--date", help="Override today (YYYY-MM-DD) for testing")
    args = parser.parse_args()
    today = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()

    calendar = load_json(Path(CALENDAR_PATH), {})
    if not calendar.get("occasions"):
        print(f"[occasion-engine] calendar missing/empty at {CALENDAR_PATH}")
        return 1
    catalog = load_json(Path(CATALOG_INDEX_PATH), {})

    payload = build_occasion_intel(calendar, catalog, today)
    path = write_occasion_intel(payload)

    print(f"[occasion-engine] {len(payload['active_occasions'])} active occasion(s) "
          f"({payload['catalog_products_seen']} products scanned) -> {path}")
    for occ in payload["active_occasions"]:
        print(f"  - {occ['name']}: peak {occ['peak_date']} (in {occ['days_until_peak']}d), "
              f"{occ['product_count']} products, angle: {occ['messaging_angle'][:60]}")
    for err in payload["errors"]:
        print(f"  [calendar-error] {err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
