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


@dataclass
class _ProductBucket:
    label: str  # product_title, or "<no title>" fallback
    sample_sku: str | None = None
    sample_product_id: str | None = None
    sample_product_handle: str | None = None
    units_sold: int = 0
    order_count: int = 0
    distinct_skus: set[str] = field(default_factory=set)
    revenue_total: float = 0.0
    cogs_total: float = 0.0
    net_profit_total: float = 0.0
    gross_profit_total: float = 0.0
    discount_total: float = 0.0


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
    """Return (group_key, display_label). Group by product_title when
    available; fall back to product_handle, then sku, then 'unknown'."""
    title = str(line_item.get("product_title") or "").strip()
    if title:
        return (title.lower(), title)
    handle = str(line_item.get("product_handle") or "").strip()
    if handle:
        return (f"handle::{handle.lower()}", f"({handle})")
    sku = str(line_item.get("sku") or "").strip()
    if sku:
        return (f"sku::{sku.lower()}", f"<sku {sku}>")
    return ("__unknown__", "<no product label>")


def _bucket_to_row(bucket: _ProductBucket) -> dict[str, Any]:
    """Convert an internal bucket to the JSON shape the page consumes."""
    revenue = bucket.revenue_total
    margin_pct = (bucket.net_profit_total / revenue * 100) if revenue > 0 else 0.0
    return {
        "label": bucket.label,
        "sample_sku": bucket.sample_sku,
        "sample_product_id": bucket.sample_product_id,
        "sample_product_handle": bucket.sample_product_handle,
        "units_sold": bucket.units_sold,
        "order_count": bucket.order_count,
        "distinct_skus": sorted(bucket.distinct_skus),
        "distinct_sku_count": len(bucket.distinct_skus),
        "revenue_total": round(revenue, 2),
        "cogs_total": round(bucket.cogs_total, 2),
        "discount_total": round(bucket.discount_total, 2),
        "net_profit_total": round(bucket.net_profit_total, 2),
        "gross_profit_total": round(bucket.gross_profit_total, 2),
        "margin_pct": round(margin_pct, 2),
        "is_confident_margin": bucket.units_sold >= MIN_UNITS_FOR_CONFIDENT_MARGIN,
        "avg_revenue_per_unit": round(revenue / bucket.units_sold, 2) if bucket.units_sold else 0.0,
        "avg_net_per_unit": round(bucket.net_profit_total / bucket.units_sold, 2) if bucket.units_sold else 0.0,
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

    files = _iter_cache_files(cache_dir=cdir, since=window_start_date)
    for path in files:
        file_count += 1
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
        for order in orders:
            if not isinstance(order, dict):
                continue
            total_orders_seen += 1
            line_items = order.get("line_items")
            if not isinstance(line_items, list):
                continue
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

    rows = sorted(
        [_bucket_to_row(b) for b in buckets.values()],
        key=lambda r: -r["net_profit_total"],
    )

    totals_revenue = sum(r["revenue_total"] for r in rows)
    totals_cogs = sum(r["cogs_total"] for r in rows)
    totals_net = sum(r["net_profit_total"] for r in rows)
    totals_units = sum(r["units_sold"] for r in rows)
    totals_margin = (totals_net / totals_revenue * 100) if totals_revenue > 0 else 0.0

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
            "units": totals_units,
            "orders_scanned": total_orders_seen,
            "products": len(rows),
        },
        "products": rows,
        "loss_makers": loss_makers,
        "low_margin": low_margin,
        "top_performers": top_performers,
        "data_quality": {
            "malformed_files": malformed_files,
            "line_items_without_revenue": items_without_revenue,
            "line_items_without_cogs": items_without_cogs,
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
