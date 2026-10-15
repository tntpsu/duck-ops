"""Surface 11 producer: aggregate per-product profit from raw order
line items and emit state/profit_per_product.json for the
/portal/intel/profit reader's "By product" drill-down.

Operator question this answers: **"which ducks make money, which lose money?"**
The existing profit_intel.json answers aggregate-level questions
(yesterday's revenue, 7-day trend, channel mix) but doesn't break
down by product, so the operator can't act on "this duck is a
margin loser, retire it" or "this duck punches above its weight,
promote it."

Architecture matches the producer-on-schedule + cheap-reader
pattern (current_learnings, weekly_strategy, system_health,
llm_cost_summary, learning inspector). Source data is the raw
order cache produced by the profit collector:
  duckAgent/cache/profit/orders/<YYYY-MM-DD>_<platform>_orders.json

Each line item already carries gross_profit + net_profit + margin_pct
computed at order-ingest time, so this aggregator does straightforward
sum/weighted-avg work.

Grouping: by product_title (most operator-meaningful). product_handle
and sku are kept as drill-down detail. Multiple SKUs that share a
product_title roll up together — variants of "Jeep Wave Duck" are
one row, not five.

Time window: last 30 days by default. Older orders aren't deleted —
they just don't influence "is this duck currently profitable" decisions.

Channel split (2026-10-15, Surface 92): every row also carries
`by_channel`, and the payload carries top-level `channels`, because
"which ducks make money" and "where do they make it" are different
questions and only the first was answerable. Never from the file name:
the collector writes Etsy orders into `<date>_shopify_orders.json` (181
of 273 rows in the 28 days to 2026-10-14), so a filename-keyed split
hands Etsy's revenue to Shopify. Where the two disagree the order's own
field wins and `data_quality.platform_filename_mismatch` counts it, so
fixing the reader here does not quietly excuse the writer.

Sales channel, not platform (Surface 94, same day): the split above read
the order's `platform` field and called it the channel, so all four
Shopify sales channels collapsed into one row named "shopify". That is
not a channel -- it is three marketplaces and a website averaged
together, and the average is what the operator was shown. In the 28 days
to 2026-10-14 it hid a website doing 27 orders at a $16.02 basket behind
eBay's 24 at $9.79 and TikTok's 35 at $13.47, and produced a blended
"site" AOV of $13.05 that belongs to no channel that exists. The split is
now keyed on the order's `channel` field (web / tiktok / ebay / other)
with Etsy as its own channel, and `platform` is kept only for the
filename canary. Older cache rows predate the field (1,055 of them, all
outside the current window) and land in `shopify_unspecified` rather than
being credited to the website.

The producer is invoked manually for now (one-shot). A launchd plist
to fire it daily alongside profit_intel.py would be a small Tier 3
followup once the operator has used the surface for ~1 week.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, now_local_iso, write_json


DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
ORDERS_CACHE_DIR = DUCK_AGENT_ROOT / "cache" / "profit" / "orders"
SUMMARY_PATH = DUCK_OPS_ROOT / "state" / "profit_per_product.json"


# A product that's sold few units gets noisy margins; this floor is
# what we recommend showing as a "stable" datapoint vs "volatile."
MIN_UNITS_FOR_CONFIDENT_MARGIN = 3


# A channel label that we could not attribute. Kept as its own channel
# rather than folded into a real one — unattributable money should be
# visible, not quietly credited to whichever channel is larger.
_CHANNEL_UNKNOWN = "unknown"

# A Shopify order from before the collector recorded which channel sold it.
# Separate from _CHANNEL_UNKNOWN because the platform IS known here; only
# the channel is missing. Crediting these to "web" would inflate exactly
# the number this surface exists to report.
_CHANNEL_SHOPIFY_UNSPECIFIED = "shopify_unspecified"


def _sales_channel(order: dict[str, Any], platform: str) -> str:
    """The channel that actually sold it, not the platform it was booked on.

    `platform` answers "whose API did this come from" (shopify / etsy);
    `channel` answers "where did the customer buy it" (web / tiktok / ebay).
    Only the second is a channel, and only the second can be acted on."""
    raw = str(order.get("channel") or "").strip().lower()
    if platform == "etsy":
        # The Etsy collector stamps channel="direct" on every order (and
        # older rows carry nothing at all), so the platform name is the
        # only meaningful label on that side.
        return "etsy"
    if platform == "shopify":
        if not raw or raw == "direct":
            return _CHANNEL_SHOPIFY_UNSPECIFIED
        return raw
    return platform or _CHANNEL_UNKNOWN


@dataclass
class _ChannelSlice:
    """One channel's share of a single product's P&L."""
    units: int = 0
    line_items: int = 0
    order_ids: set[str] = field(default_factory=set)
    revenue_total: float = 0.0
    cogs_total: float = 0.0
    net_profit_total: float = 0.0
    gross_profit_total: float = 0.0
    discount_total: float = 0.0
    order_costs_allocated: float = 0.0


@dataclass
class _ProductBucket:
    label: str  # product_title, or "<no title>" fallback
    sample_sku: str | None = None
    sample_product_id: str | None = None
    sample_product_handle: str | None = None
    units_sold: int = 0
    order_count: int = 0
    distinct_skus: set[str] = field(default_factory=set)
    # 2026-06-06 grouping fix: when the same product_id sells under
    # multiple line-item titles (Shopify variant options like "Single
    # Duck" vs "Maize and Blue"), we merge them all into one bucket
    # and remember the title set for operator transparency. Without
    # this, Michigan Wolverines Duck split into 3 rows when it's
    # really 2 physical products.
    title_variants: set[str] = field(default_factory=set)
    revenue_total: float = 0.0
    cogs_total: float = 0.0
    net_profit_total: float = 0.0
    gross_profit_total: float = 0.0
    discount_total: float = 0.0
    order_costs_allocated: float = 0.0
    channels: dict[str, _ChannelSlice] = field(default_factory=dict)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _product_key(line_item: dict[str, Any]) -> tuple[str, str]:
    """Return (group_key, display_label).

    Fallback chain (2026-06-06 fix — was title-first which split
    variant-rich products into multiple rows):
      1. product_id  — the most stable identifier; merges Shopify
         variant options ("Single Duck", "Maize and Blue") under
         one row since the operator manages one underlying product.
      2. sku (case-folded) — second-most stable; survives title
         edits and platform sync drift.
      3. product_title (case-folded) — last resort when neither id
         nor sku is present (very old cache files, manual orders).
      4. product_handle — used for label-only fallback when title is
         also missing.

    Different `product_id`s stay separate by design — operator can
    layer an alias file on top (future Surface 11.1) to merge an
    Etsy listing with its Shopify twin if they want one P&L view."""
    product_id = str(line_item.get("product_id") or "").strip()
    title = str(line_item.get("product_title") or "").strip()
    sku = str(line_item.get("sku") or "").strip()
    handle = str(line_item.get("product_handle") or "").strip()

    if product_id:
        # Display label prefers a real title; falls back to handle/sku.
        label = title or handle or (f"<sku {sku}>" if sku else f"<product_id {product_id}>")
        return (f"pid::{product_id}", label)
    if sku:
        label = title or handle or f"<sku {sku}>"
        return (f"sku::{sku.lower()}", label)
    if title:
        return (f"title::{title.lower()}", title)
    if handle:
        return (f"handle::{handle.lower()}", f"({handle})")
    return ("__unknown__", "<no product label>")


def _channel_from_filename(name: str) -> str | None:
    """`2026-10-13_shopify_orders.json` -> `shopify`.

    A HINT, not evidence. The collector saves Shopify and Etsy orders
    together into the shopify-named file, so this is only consulted for
    orders that carry no `platform` field of their own (older cache files)."""
    stem = name[:-5] if name.endswith(".json") else name
    parts = stem.split("_")
    if len(parts) >= 3 and parts[-1] == "orders":
        return "_".join(parts[1:-1]).strip().lower() or None
    return None


def _order_costs(order: dict[str, Any]) -> float:
    """The costs an order carries that its line items never see.

    Verified against the live cache (2026-10-15):
      order["net_profit"] == sum(line["gross_profit"]) - _order_costs(order)

    Shipping is netted, not added: collecting $4.99 against a $4.90 label is
    a 9c gain, and collecting $4.50 against the same label is a 40c loss.
    A negative result is therefore correct and must not be clamped — it is
    how an over-collected shipping charge shows up as profit."""
    return (
        _safe_float(order.get("platform_fees"))
        + _safe_float(order.get("payment_fees"))
        + _safe_float(order.get("ads_fees"))
        + _safe_float(order.get("shipping_label_cost"))
        - _safe_float(order.get("shipping_collected"))
    )


def _slice_to_row(sl: _ChannelSlice) -> dict[str, Any]:
    """Render one channel slice. Margin is computed from the slice's own
    revenue and net — copying the row's margin down would make a
    high-fee channel look as healthy as the product's average."""
    revenue = sl.revenue_total
    return {
        "orders": len(sl.order_ids),
        "line_items": sl.line_items,
        "units": sl.units,
        "revenue_total": round(revenue, 2),
        "cogs_total": round(sl.cogs_total, 2),
        "discount_total": round(sl.discount_total, 2),
        "net_profit_total": round(sl.net_profit_total, 2),
        "gross_profit_total": round(sl.gross_profit_total, 2),
        "margin_pct": round(sl.net_profit_total / revenue * 100, 2) if revenue > 0 else 0.0,
        "avg_net_per_unit": round(sl.net_profit_total / sl.units, 2) if sl.units else 0.0,
        # margin_pct above is gross: revenue less COGS, the number this
        # producer has always reported. The two below carry the channel's
        # own fees and shipping, which is the only basis on which two
        # channels can actually be compared.
        "order_costs_allocated": round(sl.order_costs_allocated, 2),
        "net_after_order_costs": round(sl.net_profit_total - sl.order_costs_allocated, 2),
        "margin_after_costs_pct": (
            round((sl.net_profit_total - sl.order_costs_allocated) / revenue * 100, 2)
            if revenue > 0 else 0.0),
        "avg_net_after_costs_per_unit": (
            round((sl.net_profit_total - sl.order_costs_allocated) / sl.units, 2)
            if sl.units else 0.0),
    }


def _bucket_to_row(bucket: _ProductBucket) -> dict[str, Any]:
    """Convert an internal bucket to the JSON shape the page consumes."""
    revenue = bucket.revenue_total
    margin_pct = (bucket.net_profit_total / revenue * 100) if revenue > 0 else 0.0
    title_variants_sorted = sorted(bucket.title_variants)
    by_channel = {name: _slice_to_row(sl)
                  for name, sl in sorted(bucket.channels.items())}
    # "Where does this duck belong" is a profit question. Ranking on
    # revenue would crown the channel that takes more money and keeps
    # less of it — which is exactly the comparison being made.
    primary_channel = (
        max(sorted(bucket.channels.items()),
            key=lambda kv: kv[1].net_profit_total - kv[1].order_costs_allocated)[0]
        if bucket.channels else None
    )
    distinct_orders = len({oid for sl in bucket.channels.values()
                           for oid in sl.order_ids})
    return {
        "label": bucket.label,
        "sample_sku": bucket.sample_sku,
        "sample_product_id": bucket.sample_product_id,
        "sample_product_handle": bucket.sample_product_handle,
        "units_sold": bucket.units_sold,
        # order_count has always counted LINE ITEMS, so two variants of one
        # duck in a single order read as two orders. The number is left
        # alone so no surface shifts under the operator, and the honest
        # count sits beside it under its own name rather than overloading
        # this one ([[feedback_status_fields_one_meaning]]).
        "order_count": bucket.order_count,
        "order_count_distinct": distinct_orders,
        "distinct_skus": sorted(bucket.distinct_skus),
        "distinct_sku_count": len(bucket.distinct_skus),
        # Operator transparency: when product_id grouping merges
        # multiple titles, surface them so the operator can verify
        # the merge is correct (and spot data-entry typos).
        "title_variants": title_variants_sorted,
        "title_variant_count": len(title_variants_sorted),
        "revenue_total": round(revenue, 2),
        "cogs_total": round(bucket.cogs_total, 2),
        "discount_total": round(bucket.discount_total, 2),
        "net_profit_total": round(bucket.net_profit_total, 2),
        "gross_profit_total": round(bucket.gross_profit_total, 2),
        "margin_pct": round(margin_pct, 2),
        "is_confident_margin": bucket.units_sold >= MIN_UNITS_FOR_CONFIDENT_MARGIN,
        "avg_revenue_per_unit": round(revenue / bucket.units_sold, 2) if bucket.units_sold else 0.0,
        "avg_net_per_unit": round(bucket.net_profit_total / bucket.units_sold, 2) if bucket.units_sold else 0.0,
        "order_costs_allocated": round(bucket.order_costs_allocated, 2),
        "net_after_order_costs": round(
            bucket.net_profit_total - bucket.order_costs_allocated, 2),
        "margin_after_costs_pct": (
            round((bucket.net_profit_total - bucket.order_costs_allocated) / revenue * 100, 2)
            if revenue > 0 else 0.0),
        "by_channel": by_channel,
        "channel_count": len(by_channel),
        "primary_channel": primary_channel,
    }


def _iter_cache_files(*, cache_dir: Path, since: date | None) -> list[Path]:
    """Return cache files whose date stamp is >= since. Filename
    convention: YYYY-MM-DD_<platform>_orders.json."""
    if not cache_dir.exists():
        return []
    files: list[Path] = []
    for path in sorted(cache_dir.glob("*.json")):
        try:
            date_part = path.name[:10]
            file_date = date.fromisoformat(date_part)
        except ValueError:
            continue
        if since is not None and file_date < since:
            continue
        files.append(path)
    return files


def aggregate_per_product(
    *,
    cache_dir: Path | None = None,
    window_days: int = 30,
    now_iso: str | None = None,
) -> dict[str, Any]:
    """Read order cache files, return aggregated per-product summary.

    Returns:
      {
        "generated_at": "...",
        "window_days": 30,
        "window_start": "YYYY-MM-DD",
        "cache_dir": "...",
        "file_count": N,
        "totals": {revenue, cogs, net_profit, margin_pct, units, orders, products},
        "products": [_bucket_to_row(b), ...],  # sorted by net_profit desc
        "loss_makers": [...],   # subset with net_profit_total < 0
        "low_margin": [...],    # subset with 0 < margin < 30%, confident only
        "top_performers": [...], # top 10 by net_profit, confident only
        "data_quality": {malformed_files, line_items_without_revenue,
                         line_items_without_cogs, confidence_floor_units}
      }

    Never raises — missing cache dir / malformed files / missing fields
    result in empty/zero values gracefully.
    """
    cdir = cache_dir if cache_dir is not None else ORDERS_CACHE_DIR
    if now_iso:
        try:
            now_dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
            if now_dt.tzinfo is None:
                now_dt = now_dt.astimezone()
        except ValueError:
            now_dt = datetime.now().astimezone()
    else:
        now_dt = datetime.now().astimezone()
    window_start_date = (now_dt - timedelta(days=window_days)).date()

    buckets: dict[str, _ProductBucket] = {}
    malformed_files = 0
    items_without_revenue = 0
    items_without_cogs = 0
    file_count = 0
    total_orders_seen = 0
    orders_without_platform = 0
    orders_without_channel = 0
    orders_with_assumed_fees = 0
    orders_without_fee_source = 0
    orders_with_assumed_label_cost = 0
    platform_filename_mismatch = 0
    # Channel-level order ids dedupe across products: a three-duck basket
    # is ONE Etsy order, not three.
    channel_order_ids: dict[str, set[str]] = defaultdict(set)

    files = _iter_cache_files(cache_dir=cdir, since=window_start_date)
    for path in files:
        file_count += 1
        file_channel = _channel_from_filename(path.name)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            malformed_files += 1
            continue
        # Each file is either a list of orders or {"orders": [...]}.
        orders = payload if isinstance(payload, list) else (
            payload.get("orders") if isinstance(payload, dict) else None
        )
        if not isinstance(orders, list):
            malformed_files += 1
            continue
        for order_index, order in enumerate(orders):
            if not isinstance(order, dict):
                continue
            total_orders_seen += 1
            # The order's own `platform` is the only trustworthy source:
            # the collector writes both platforms into the shopify-named
            # file. A disagreement is counted, not smoothed away.
            declared = str(order.get("platform") or "").strip().lower()
            if declared:
                platform = declared
                if file_channel and declared != file_channel:
                    platform_filename_mismatch += 1
            else:
                orders_without_platform += 1
                platform = file_channel or _CHANNEL_UNKNOWN
            # ...and the platform is NOT the channel. See _sales_channel.
            channel = _sales_channel(order, platform)
            if channel == _CHANNEL_SHOPIFY_UNSPECIFIED:
                orders_without_channel += 1
            # How much of this window's money rests on a configured
            # assumption rather than a measured cost. Counted per order so
            # the reader can caveat a number instead of discovering later
            # that the whole column was an estimate.
            fee_source = str(order.get("platform_fee_source") or "")
            if not fee_source:
                # Written before the collector recorded a fee source. NOT the
                # same as "no assumption was made" -- these rows carry
                # platform_fees of exactly $0.00 on marketplace orders that
                # really did pay a commission, which is the defect Surface 94
                # fixed in the collector. Counted separately so a zero in
                # orders_with_assumed_fees can never be read as reassurance.
                orders_without_fee_source += 1
            elif fee_source.startswith("assumed"):
                orders_with_assumed_fees += 1
            if str(order.get("label_cost_source") or "") != "manual_import":
                orders_with_assumed_label_cost += 1
            order_ref = str(order.get("order_id") or "").strip() or (
                f"__anon::{path.name}::{order_index}")
            line_items = order.get("line_items")
            if not isinstance(line_items, list):
                continue
            # Pro-rata on line revenue. A zero-revenue basket (fully
            # discounted, or a cache row with no revenue) still paid its
            # fees, so it splits evenly rather than losing the cost.
            valid_items = [li for li in line_items if isinstance(li, dict)]
            order_cost_pool = _order_costs(order)
            order_line_revenue = sum(_safe_float(li.get("revenue_ex_tax"))
                                     for li in valid_items)
            for li in line_items:
                if not isinstance(li, dict):
                    continue
                key, label = _product_key(li)
                bucket = buckets.get(key)
                if bucket is None:
                    bucket = _ProductBucket(label=label)
                    buckets[key] = bucket
                qty = _safe_int(li.get("qty"))
                revenue = _safe_float(li.get("revenue_ex_tax"))
                cogs_total = _safe_float(li.get("cogs_total")) or (
                    _safe_float(li.get("cogs_unit")) * qty
                )
                net_profit = _safe_float(li.get("net_profit"))
                gross_profit = _safe_float(li.get("gross_profit"))
                discount = _safe_float(li.get("discount_alloc"))
                if revenue <= 0:
                    items_without_revenue += 1
                if cogs_total <= 0:
                    items_without_cogs += 1
                bucket.units_sold += qty
                bucket.order_count += 1
                # Record this line item's title in the variant set.
                # When product_id-grouping merges Shopify variants
                # under one row, title_variants lets the operator
                # confirm what got merged (and spot data-entry typos
                # that should be cleaned up upstream).
                li_title = str(li.get("product_title") or "").strip()
                if li_title:
                    bucket.title_variants.add(li_title)
                sku = str(li.get("sku") or "").strip()
                if sku:
                    bucket.distinct_skus.add(sku)
                    if not bucket.sample_sku:
                        bucket.sample_sku = sku
                pid = str(li.get("product_id") or "").strip()
                if pid and not bucket.sample_product_id:
                    bucket.sample_product_id = pid
                phandle = str(li.get("product_handle") or "").strip()
                if phandle and not bucket.sample_product_handle:
                    bucket.sample_product_handle = phandle
                bucket.revenue_total += revenue
                bucket.cogs_total += cogs_total
                bucket.net_profit_total += net_profit
                bucket.gross_profit_total += gross_profit
                bucket.discount_total += discount

                if order_line_revenue > 0:
                    cost_share = revenue / order_line_revenue
                elif valid_items:
                    cost_share = 1.0 / len(valid_items)
                else:
                    cost_share = 0.0
                allocated = order_cost_pool * cost_share
                bucket.order_costs_allocated += allocated

                sl = bucket.channels.get(channel)
                if sl is None:
                    sl = _ChannelSlice()
                    bucket.channels[channel] = sl
                sl.units += qty
                sl.line_items += 1
                sl.order_ids.add(order_ref)
                sl.revenue_total += revenue
                sl.cogs_total += cogs_total
                sl.net_profit_total += net_profit
                sl.gross_profit_total += gross_profit
                sl.discount_total += discount
                sl.order_costs_allocated += allocated
                channel_order_ids[channel].add(order_ref)

    rows = sorted(
        [_bucket_to_row(b) for b in buckets.values()],
        key=lambda r: -r["net_profit_total"],
    )

    totals_revenue = sum(r["revenue_total"] for r in rows)
    totals_cogs = sum(r["cogs_total"] for r in rows)
    totals_net = sum(r["net_profit_total"] for r in rows)
    totals_units = sum(r["units_sold"] for r in rows)
    totals_order_costs = sum(r["order_costs_allocated"] for r in rows)
    totals_net_after = sum(r["net_after_order_costs"] for r in rows)
    totals_margin = (totals_net / totals_revenue * 100) if totals_revenue > 0 else 0.0

    # Summed from the rounded per-row slices rather than recomputed, so the
    # channel header and the rows beneath it can never disagree on screen.
    channel_totals: dict[str, dict[str, Any]] = {}
    for row in rows:
        for cname, c in (row.get("by_channel") or {}).items():
            agg = channel_totals.setdefault(cname, {
                "orders": 0, "line_items": 0, "units": 0, "products": 0,
                "revenue": 0.0, "cogs": 0.0, "net_profit": 0.0,
                "margin_pct": 0.0, "order_costs": 0.0,
                "net_after_order_costs": 0.0, "margin_after_costs_pct": 0.0,
            })
            agg["line_items"] += c["line_items"]
            agg["units"] += c["units"]
            agg["products"] += 1
            agg["revenue"] += c["revenue_total"]
            agg["cogs"] += c["cogs_total"]
            agg["net_profit"] += c["net_profit_total"]
            agg["order_costs"] += c["order_costs_allocated"]
            agg["net_after_order_costs"] += c["net_after_order_costs"]
    for cname, agg in channel_totals.items():
        agg["orders"] = len(channel_order_ids.get(cname, set()))
        agg["revenue"] = round(agg["revenue"], 2)
        agg["cogs"] = round(agg["cogs"], 2)
        agg["net_profit"] = round(agg["net_profit"], 2)
        agg["margin_pct"] = (round(agg["net_profit"] / agg["revenue"] * 100, 2)
                             if agg["revenue"] > 0 else 0.0)
        agg["order_costs"] = round(agg["order_costs"], 2)
        agg["net_after_order_costs"] = round(agg["net_after_order_costs"], 2)
        agg["margin_after_costs_pct"] = (
            round(agg["net_after_order_costs"] / agg["revenue"] * 100, 2)
            if agg["revenue"] > 0 else 0.0)
    channels = dict(sorted(channel_totals.items()))

    confident_rows = [r for r in rows if r["is_confident_margin"]]
    loss_makers = [r for r in rows if r["net_profit_total"] < 0]
    low_margin = [r for r in confident_rows
                  if 0 < r["margin_pct"] < 30]
    top_performers = confident_rows[:10]

    return {
        "generated_at": now_local_iso(),
        "window_days": window_days,
        "window_start": window_start_date.isoformat(),
        "cache_dir": str(cdir),
        "file_count": file_count,
        "totals": {
            "revenue": round(totals_revenue, 2),
            "cogs": round(totals_cogs, 2),
            "net_profit": round(totals_net, 2),
            "margin_pct": round(totals_margin, 2),
            "order_costs": round(totals_order_costs, 2),
            "net_after_order_costs": round(totals_net_after, 2),
            "margin_after_costs_pct": (
                round(totals_net_after / totals_revenue * 100, 2)
                if totals_revenue > 0 else 0.0),
            "units": totals_units,
            "orders_scanned": total_orders_seen,
            "products": len(rows),
        },
        "channels": channels,
        "products": rows,
        "loss_makers": loss_makers,
        "low_margin": low_margin,
        "top_performers": top_performers,
        "data_quality": {
            "malformed_files": malformed_files,
            "line_items_without_revenue": items_without_revenue,
            "line_items_without_cogs": items_without_cogs,
            "orders_without_platform": orders_without_platform,
            "orders_without_channel": orders_without_channel,
            "platform_filename_mismatch": platform_filename_mismatch,
            "platform_source_note": (
                "Channel is read from each order's `channel` field (web / "
                "tiktok / ebay / other, or etsy), never from `platform` and "
                "never from the file name. `platform` only feeds "
                "platform_filename_mismatch, which counts how often the "
                "order disagrees with the file it was written into. Shopify "
                "orders with no channel field land in "
                f"{_CHANNEL_SHOPIFY_UNSPECIFIED!r} and are counted by "
                "orders_without_channel rather than credited to the website."
            ),
            "orders_with_assumed_fees": orders_with_assumed_fees,
            "orders_without_fee_source": orders_without_fee_source,
            "orders_with_assumed_label_cost": orders_with_assumed_label_cost,
            "assumption_note": (
                "platform fees on marketplace channels and every shipping "
                "label cost without a manual import are CONFIGURED "
                "ASSUMPTIONS, not amounts any platform reported. Net profit "
                "here is only as good as those rates "
                "(duckAgent/flows/profit/profit_config.py). "
                "orders_without_fee_source counts rows cached before the "
                "collector recorded one: those carry $0.00 platform fees even "
                "on marketplace orders, so their net profit is overstated "
                "until the day is re-collected."
            ),
            "confidence_floor_units": MIN_UNITS_FOR_CONFIDENT_MARGIN,
            "confidence_floor_note": (
                f"Products with fewer than {MIN_UNITS_FOR_CONFIDENT_MARGIN} "
                f"units sold are tagged is_confident_margin=False — their "
                f"margin_pct can swing wildly on the next sale and shouldn't "
                f"drive retire/promote decisions yet."
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--window-days", type=int, default=30,
                        help="Aggregation window (default 30).")
    parser.add_argument("--print-json", action="store_true",
                        help="Emit summary on stdout as well.")
    args = parser.parse_args()
    summary = aggregate_per_product(window_days=args.window_days)
    write_json(SUMMARY_PATH, summary)
    if args.print_json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        t = summary["totals"]
        print(
            f"[profit-per-product] window={args.window_days}d "
            f"files={summary['file_count']} "
            f"products={t['products']} units={t['units']} "
            f"revenue=${t['revenue']:.2f} net=${t['net_profit']:.2f} "
            f"margin={t['margin_pct']:.1f}% "
            f"losers={len(summary['loss_makers'])} "
            f"low_margin={len(summary['low_margin'])}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
