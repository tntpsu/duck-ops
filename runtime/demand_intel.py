"""Demand intel producer — the "improve what I already sell" funnel.

Fuses the per-duck Etsy demand funnel (views -> engagement -> buys) and sorts
every catalog duck into ONE action bucket so the operator can decide, per duck:
put it on sale, refresh the listing, protect a winner, or watch a fader.

This is the counterpart to build_next (which is "what to make new"). It is a
CHEAP READER of already-produced state (producer-on-schedule pattern): it reads
GA4 listing performance, GSC search, profit-per-product, the Etsy transactions
snapshot, the catalog index and occasion intel — it makes no API calls.

Channel reality (verified 2026-07-03): measurable web traffic is ~100% Etsy, so
this is an ETSY funnel. Favorites and Etsy per-listing search are unavailable via
API (Shop-Manager-scrape only) and are NEVER inferred — they surface as coverage
gaps, not fake zeros. Joins across the channel-mismatched sources are by
normalized title (the established seo_demand_context token-overlap match).

Deterministic bucketing (config/demand_buckets.json, versioned) — no LLM.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

from seo_demand_context import (  # tokenizers reused; buys join is by id/SKU, not title
    load_seo_demand_context,
    _tokens,
    _strip_channel,
)

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DEMAND_INTEL_PATH = DUCK_OPS_ROOT / "state" / "demand_intel.json"
DEMAND_BUCKETS_CONFIG_PATH = DUCK_OPS_ROOT / "config" / "demand_buckets.json"
CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"
PROFIT_PER_PRODUCT_PATH = DUCK_OPS_ROOT / "state" / "profit_per_product.json"
ETSY_TX_SNAPSHOT_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "etsy_transactions_snapshot.json"
OCCASION_INTEL_PATH = DUCK_OPS_ROOT / "state" / "occasion_intel.json"

# Frozen factory-default for the source-level test-pollution guard. See
# CLAUDE.md "Cross-repo state writes" + sale_steering._FROZEN_PRODUCTION_SALE_STEERING_PATH.
_FROZEN_PRODUCTION_DEMAND_INTEL_PATH = DEMAND_INTEL_PATH.resolve()

_DEFAULT_CONFIG = {
    "version": 0,
    "traffic": {"min_views_7d_to_judge": 15, "high_traffic_views_7d": 40,
                "winner_min_buys_7d": 2, "winner_min_buys_30d": 4},
    "engagement": {"low_engagement_rate": 0.45, "healthy_engagement_rate": 0.60},
    "trend": {"up_ratio": 1.15, "down_ratio": 0.85, "prior_window_days": 21},
    "fading": {"had_buys_30d_min": 1, "stale_views_7d_max": 20},
    "low_signal_confidence_units": 3,
}


# Catalog boilerplate that carries no identity — stripped before trusting a GA4
# title match (Etsy listing_performance has no product_id, so views join by title;
# without this "Oklahoma Sooners …officially licensed…duck" cross-matches
# "Michigan Wolverines …officially licensed…duck". 2026-07-03).
_BOILERPLATE = frozenset({
    "duck", "ducks", "3d", "printed", "print", "collectible", "collectibles", "figurine",
    "gift", "gifts", "dashboard", "decor", "dog", "loyal", "playful", "officially", "licensed",
    "college", "the", "and", "with", "for", "of", "edition", "custom", "personalized", "keepsake",
    "sculpture", "toy", "mashup", "hybrid", "animal", "pet", "lover", "lovers", "themed", "desk",
    "car", "jeep", "vibe", "vibes", "inspired", "art", "home", "cute", "funny", "quirky", "novelty",
    "small", "large", "mini", "fan", "fans", "collectic", "gameday", "game", "day",
    "team", "spirit", "pride", "official", "spirited",
})


def _distinctive(title: Any) -> set[str]:
    """Identity-bearing tokens: length >= 3, non-boilerplate. (_tokens returns an
    unordered set, so anything positional would be nondeterministic.)"""
    return {t for t in _tokens(_strip_channel(title)) if len(t) >= 3 and t not in _BOILERPLATE}


def _ga4_match_is_trustworthy(duck_title: Any, matched_title: Any) -> bool:
    """A GA4-by-title match is trustworthy only when the duck and the matched
    listing SHARE their subject — at least half the smaller side's distinctive
    tokens overlap. Shared template words alone (two 'officially licensed college
    … duck' titles) are NOT enough: that's how Oklahoma inherited Michigan's
    views. If the subjects don't overlap, the views belong to a different duck;
    discard rather than attribute wrong traffic (2026-07-03)."""
    if not matched_title:
        return False
    a, b = _distinctive(duck_title), _distinctive(matched_title)
    if not a or not b:
        return False
    shared = len(a & b)
    return shared >= 1 and shared / min(len(a), len(b)) >= 0.5


class TestModeRefusalError(RuntimeError):
    """Raised when write_demand_intel would write the frozen production path
    while DUCK_TEST_MODE=1 — loud per [[feedback_swallowed_errors_lie]]."""


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).astimezone().isoformat()


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def load_config(path: Any = None) -> dict[str, Any]:
    cfg = _load_json(path or DEMAND_BUCKETS_CONFIG_PATH, None)
    return cfg if isinstance(cfg, dict) and cfg.get("version") else dict(_DEFAULT_CONFIG)


def _derive_buys_7d_by_id(tx_snapshot: dict, sku_to_pid: dict[str, str], *,
                          now_epoch: float, days: int = 7) -> dict[str, int]:
    """Aggregate Etsy transaction quantities within the last `days`, keyed by
    Shopify product_id via the SKU→id map. Title joins are NOT used: boilerplate,
    keyword-stuffed titles ("3D-Printed…Dog Duck Collectible") match each other,
    so Boxer/Doberman falsely inherited the Dachshund's sales (2026-07-03).
    The v3 shop-transactions endpoint ignores date windows, so filter here on
    created_timestamp ([[reference_etsy_transactions_ignores_date_window]])."""
    cutoff = now_epoch - days * 86400
    by_id: dict[str, int] = {}
    for it in (tx_snapshot.get("items") or []):
        if not isinstance(it, dict):
            continue
        try:
            created = float(it.get("created_timestamp") or 0)
        except (TypeError, ValueError):
            created = 0
        if created < cutoff:
            continue
        pid = sku_to_pid.get(str(it.get("sku") or "").strip().lower())
        if not pid:
            continue
        try:
            qty = int(it.get("quantity") or 0)
        except (TypeError, ValueError):
            qty = 0
        by_id[pid] = by_id.get(pid, 0) + qty
    return by_id


def _active_occasion_ids(occasion_intel: dict) -> set[str]:
    ids: set[str] = set()
    for occ in (occasion_intel.get("active_occasions") or []):
        if isinstance(occ, dict):
            for k in ("key", "slug", "id", "name", "occasion"):
                v = occ.get(k)
                if v:
                    ids.add(str(v).strip().lower())
        elif isinstance(occ, str):
            ids.add(occ.strip().lower())
    return ids


def _trend_arrow(views_7d, views_28d, cfg) -> str:
    if not views_7d or views_28d is None:
        return "unknown"
    prior = (float(views_28d) - float(views_7d)) / 3.0  # prior-7 avg over the 21d before
    if prior <= 0:
        return "up" if views_7d else "unknown"
    ratio = float(views_7d) / prior
    if ratio >= cfg["trend"]["up_ratio"]:
        return "up"
    if ratio <= cfg["trend"]["down_ratio"]:
        return "down"
    return "flat"


def classify_bucket(rec: dict, cfg: dict) -> dict:
    """Deterministic funnel-stage bucketing. Returns
    {bucket, recommended_action, action_target, why}. Order matters:
    a converting duck is protected before it can be flagged for a discount, and
    a seasonal off-season duck is held before it can be called 'fading'."""
    f = rec["funnel"]
    views = f.get("views_7d")
    eng = f.get("engagement_rate")
    buys7 = f.get("buys_7d")
    buys30 = f.get("buys_30d")
    trend = rec.get("trend_arrow")
    verdict = rec.get("ga4_verdict")
    offseason = bool(rec.get("occasion", {}).get("is_seasonal_offseason"))

    T, E = cfg["traffic"], cfg["engagement"]
    min_views = T["min_views_7d_to_judge"]
    high_views = T["high_traffic_views_7d"]

    def out(bucket, action, target, why):
        return {"bucket": bucket, "recommended_action": action,
                "action_target": target, "why": why}

    # 1. WINNER — it's converting. Protect margin; never discount. Buys-driven so
    #    it survives a GA4 title-match miss (no views but real sales).
    if (buys7 or 0) >= T["winner_min_buys_7d"] or (buys30 or 0) >= T["winner_min_buys_30d"] or verdict == "promote":
        n = buys7 if buys7 else buys30
        win = "buys/7d" if buys7 else "buys/30d"
        if trend == "up":
            return out("winner", "expand", "sale_steering.exclude",
                       f"Converting and climbing ({n} {win}) — protect margin, consider a variant.")
        return out("winner", "protect", "sale_steering.exclude",
                   f"Converting ({n} {win}) — protect margin, keep it out of the sale.")

    has_views = views is not None and views >= min_views

    # 2. Low/no traffic path. GA4 views cover only ~1/5 of the catalog and are
    #    tiny, so with no live traffic we CAN'T read the funnel. A duck that sells
    #    occasionally but has no traffic signal is steady long-tail, NOT 'fading'
    #    — 'no sale this week' is the baseline (~24 sales/wk over 259 SKUs), not a
    #    decline. Don't over-claim; surface it as low_signal.
    if not has_views:
        if (buys30 or 0) >= 1:
            return out("low_signal", "watch", None,
                       f"Sells occasionally ({buys30} buys/30d) but no live Etsy traffic signal — steady long-tail.")
        why = ("Very low Etsy traffic (<%d views/7d) and no recent sales." % min_views) if views is not None \
            else "No GA4 view match and no recent sales — nothing to read yet."
        return out("low_signal", "watch", None, why)

    # 3. Traffic present — read where the funnel leaks.
    if (buys7 or 0) == 0 and eng is not None and eng <= E["low_engagement_rate"]:
        return out("refresh", "refresh_listing", "seo_review.refresh_request",
                   f"{views} views/7d but engagement {eng:.0%} (they bounce) — fix photos/copy before discounting.")
    if (buys7 or 0) == 0 and eng is not None and eng >= E["healthy_engagement_rate"]:
        return out("sale", "put_on_sale", "sale_steering.prioritize",
                   f"{views} engaged views/7d, zero buys — a discount is the lever to convert them.")
    if (buys7 or 0) == 0 and verdict == "fix":
        return out("refresh", "refresh_listing", "seo_review.refresh_request",
                   f"{views} views/7d, GA4 flags low engagement — refresh the listing.")

    # 4. Traffic + fading despite it.
    if (buys30 or 0) >= cfg["fading"]["had_buys_30d_min"] and not buys7 and trend == "down" \
            and views <= cfg["fading"]["stale_views_7d_max"]:
        if offseason:
            return out("seasonal_dormant", "watch", None, "Seasonal duck cooling in its off-season — hold.")
        return out("fading", "watch", None, "Was selling, traffic and buys both cooling — watch.")

    # 5. No clear lever — stay honest.
    return out("low_signal", "watch", None,
               f"{views} views/7d, no clear signal (engagement/buys middling) — nothing to act on yet.")


def build_demand_intel(*, catalog: dict | None = None, ctx: Any = None,
                       profit: dict | None = None, tx_snapshot: dict | None = None,
                       occasion: dict | None = None, cfg: dict | None = None,
                       now_epoch: float | None = None) -> dict:
    """Fuse the per-duck funnel and bucket every catalog duck. Never raises;
    missing inputs degrade to coverage gaps + low_signal, not crashes."""
    cfg = cfg or load_config()
    now_epoch = now_epoch if now_epoch is not None else time.time()
    catalog = catalog if catalog is not None else _load_json(CATALOG_INDEX_PATH, {})
    profit = profit if profit is not None else _load_json(PROFIT_PER_PRODUCT_PATH, {})
    tx_snapshot = tx_snapshot if tx_snapshot is not None else _load_json(ETSY_TX_SNAPSHOT_PATH, {})
    occasion = occasion if occasion is not None else _load_json(OCCASION_INTEL_PATH, {})
    if ctx is None:
        ctx = load_seo_demand_context()

    errors: list[str] = []
    items = catalog.get("items") if isinstance(catalog, dict) else None
    if not isinstance(items, dict):
        items = {}
        errors.append("catalog_index missing/empty")

    # EXACT joins by Shopify product_id / SKU — NOT title tokens (boilerplate
    # titles mis-match; see _derive_buys_7d_by_id). profit_by_id: id→entry;
    # sku_to_pid: SKU→id so 7d transactions (which carry a SKU) attribute cleanly.
    profit_products = [p for p in (profit.get("products") or []) if isinstance(p, dict)]
    profit_by_id: dict[str, dict] = {}
    sku_to_pid: dict[str, str] = {}
    for p in profit_products:
        pid = str(p.get("sample_product_id") or "").strip()
        if not pid:
            continue
        profit_by_id.setdefault(pid, p)
        for sku in (p.get("distinct_skus") or []):
            if str(sku).strip():
                sku_to_pid[str(sku).strip().lower()] = pid
    buys7_by_id = _derive_buys_7d_by_id(tx_snapshot, sku_to_pid, now_epoch=now_epoch)
    active_occ = _active_occasion_ids(occasion)

    ducks: list[dict] = []
    for pid, item in items.items():
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        sig = ctx.listing_signal(title) if title else None
        # Discard a boilerplate cross-match (its views belong to another duck).
        if sig and not _ga4_match_is_trustworthy(title, sig.get("matched_title")):
            sig = None
        vbw = (sig or {}).get("views_by_window") or {}
        views_7d = _as_int(vbw.get("7"))
        views_28d = _as_int(vbw.get("28"))
        eng = _as_float((sig or {}).get("engagement_rate"))
        bounce = _as_float((sig or {}).get("bounce_rate"))

        pid_str = str(item.get("id") or pid)
        prof = profit_by_id.get(pid_str) or {}
        buys_7d = buys7_by_id.get(pid_str)  # int units or None (None = no id-matched sale)
        buys_30d = _as_int(prof.get("units_sold"))
        margin_pct = _as_float(prof.get("margin_pct"))
        is_conf_margin = bool(prof.get("is_confident_margin"))

        occ_tags = [str(o).strip().lower() for o in
                    ((item.get("theme_classification") or {}).get("occasions") or []) if str(o).strip()]
        is_seasonal = bool(occ_tags)
        is_active_occ = any(o in active_occ for o in occ_tags)
        offseason = is_seasonal and not is_active_occ

        rec = {
            "product_id": str(pid),
            "handle": item.get("handle"),
            "title": title,
            "image_src": item.get("image_src"),
            "on_sale": bool(item.get("on_sale")),
            "status": item.get("status"),
            "primary_channel": "etsy",  # measurable traffic is Etsy; spine is Shopify id
            "matched_ga4_title": (sig or {}).get("matched_title"),
            "funnel": {
                "views_7d": views_7d,
                "views_28d": views_28d,
                "engagement_rate": eng,
                "bounce_rate": bounce,
                "favorites_7d": None,   # Etsy favorites unavailable via API (scrape-only)
                "buys_7d": buys_7d,
                "buys_30d": buys_30d,
            },
            "margin": {"margin_pct": margin_pct, "is_confident_margin": is_conf_margin},
            "trend_arrow": _trend_arrow(views_7d, views_28d, cfg),
            "ga4_verdict": (sig or {}).get("verdict"),
            "coverage": {
                "has_ga4_views": views_7d is not None,
                "has_buys_7d": buys_7d is not None,
                "has_favorites": False,
                "etsy_click_data": False,  # HARD false — never imply Etsy click/search
            },
            "occasion": {
                "tags": occ_tags,
                "is_active": is_active_occ,
                "is_seasonal_offseason": offseason,
            },
            # v2 outcome-loop fields (persisted now; nothing reads them yet):
            "bucket_since": None,
            "acted_at": None,
            "acted_action": None,
            "metrics_at_action": None,
        }
        verdict = classify_bucket(rec, cfg)
        rec.update(verdict)
        ducks.append(rec)

    # bucket_since: carry forward from the prior snapshot if bucket unchanged.
    _carry_bucket_since(ducks, now_iso=_now_iso())

    counts: dict[str, int] = {}
    for d in ducks:
        counts[d["bucket"]] = counts.get(d["bucket"], 0) + 1
    counts["total"] = len(ducks)

    return {
        "generated_at": _now_iso(),
        "available": bool(ducks),
        "config_version": cfg.get("version"),
        "window_primary_days": 7,
        "counts": counts,
        "sources": {
            "listing_performance_generated_at": None,  # ctx-internal; freshness on the OS card
            "profit_generated_at": profit.get("generated_at"),
            "profit_window_days": profit.get("window_days"),
            "occasion_generated_at": occasion.get("generated_at"),
            "tx_max_created": tx_snapshot.get("max_created"),
            "buys_source": "etsy_transactions_7d_by_sku" if buys7_by_id else "none",
        },
        "ducks": ducks,
        "errors": errors,
    }


def _carry_bucket_since(ducks: list[dict], *, now_iso: str, path: Any = None) -> None:
    prior = {d.get("product_id"): d for d in (_load_json(path or DEMAND_INTEL_PATH, {}).get("ducks") or [])
             if isinstance(d, dict)}
    for d in ducks:
        old = prior.get(d["product_id"])
        d["bucket_since"] = old["bucket_since"] if old and old.get("bucket") == d["bucket"] and old.get("bucket_since") else now_iso
        # preserve any recorded action so the v2 outcome loop keeps its baseline
        if old and old.get("acted_at"):
            for k in ("acted_at", "acted_action", "metrics_at_action"):
                d[k] = old.get(k)


def _as_int(v):
    try:
        return int(v) if v is not None and str(v) != "" else None
    except (TypeError, ValueError):
        return None


def _as_float(v):
    try:
        return float(v) if v is not None and str(v) != "" else None
    except (TypeError, ValueError):
        return None


def write_demand_intel(payload: dict, path: Any = None) -> Path:
    out = Path(path or DEMAND_INTEL_PATH)
    if os.environ.get("DUCK_TEST_MODE") == "1" and out.resolve() == _FROZEN_PRODUCTION_DEMAND_INTEL_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but DEMAND_INTEL_PATH still points at production. "
            "Monkeypatch demand_intel.DEMAND_INTEL_PATH to a tmp path.")
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the demand_intel funnel + action buckets.")
    parser.add_argument("--dry-run", action="store_true", help="Print, don't write")
    args = parser.parse_args()
    payload = build_demand_intel()
    if args.dry_run:
        print(json.dumps(payload["counts"], indent=2))
        return 0
    path = write_demand_intel(payload)
    print(f"[demand-intel] wrote {path} — {payload['counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
