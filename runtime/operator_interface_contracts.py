#!/usr/bin/env python3
"""Shared compact operator surfaces for UI readers.

This keeps the Even widget, Business Desk, and any future lightweight readers
aligned on one normalized-state contract without forcing them into the same UI
shape. Adapters should stay thin and derive their payloads from this module.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SURFACE_VERSION = 1

DUCK_OPS_ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = DUCK_OPS_ROOT / "state" / "normalized"
OUTPUT_DIR = DUCK_OPS_ROOT / "output"
OPERATOR_REJECTED_PATH = DUCK_OPS_ROOT / "state" / "operator_rejected_artifacts.json"

PACKING_SUMMARY = STATE_DIR / "packing_summary.json"
CUSTOMER_CASES = STATE_DIR / "customer_cases.json"
PUBLISH_CANDIDATES = STATE_DIR / "publish_candidates.json"
CUSTOM_BUILD_TASKS = STATE_DIR / "custom_build_task_candidates.json"
TREND_CANDIDATES = STATE_DIR / "trend_candidates.json"
USPS_TRACKING = STATE_DIR / "usps_tracking_snapshot.json"
ETSY_TRANSACTIONS = STATE_DIR / "etsy_transactions_snapshot.json"
ETSY_RECEIPTS = STATE_DIR / "etsy_receipts_snapshot.json"
CATALOG_INDEX = STATE_DIR / "catalog_index.json"
SHOPIFY_OPEN_ORDERS = STATE_DIR / "shopify_open_orders_snapshot.json"
CUSTOMER_ACTION_PACKETS_DIR = OUTPUT_DIR / "customer_intelligence"


def load_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def run_id_from_state_source(state_source: str | None) -> str | None:
    if not state_source:
        return None
    parts = state_source.split("/")
    try:
        idx = parts.index("runs")
        return parts[idx + 1]
    except (ValueError, IndexError):
        return None


def _short_title(value: str | None, max_len: int = 36) -> str:
    if not value:
        return ""
    collapsed = value.strip().replace("\n", " ")
    return collapsed if len(collapsed) <= max_len else collapsed[: max_len - 3] + "..."


def _short_duck_name(title: str) -> str:
    name = title.split("|")[0].split("–")[0].split("-")[0].strip()
    if len(name) > 32:
        name = name[:30].rstrip() + "..."
    return name


def _run_id_base(state_source: str | None) -> str:
    run_id = run_id_from_state_source(state_source) or ""
    return re.sub(r"_hardened\d+$", "", run_id)


def _ducks_to_pack(packing: dict[str, Any] | None) -> int:
    counts = (packing or {}).get("counts") or {}
    return int(counts.get("non_custom_units") or 0)


def _packing_breakdown(packing: dict[str, Any] | None) -> dict[str, Any]:
    if not packing:
        return {"etsy": 0, "shopify": 0, "unique_titles": 0, "duck_names": [], "pack_items": []}

    orders = packing.get("orders_to_pack") or []
    etsy = sum(int(((order.get("by_channel") or {}).get("etsy") or 0)) for order in orders)
    shopify = sum(int(((order.get("by_channel") or {}).get("shopify") or 0)) for order in orders)

    # Aggregate qty + earliest expected ship date per title.
    qty_by_short: dict[str, int] = {}
    earliest_ship_by_short: dict[str, str] = {}
    order_for_title: dict[str, int] = {}
    for idx, order in enumerate(orders):
        title = order.get("product_title") or ""
        short = _short_duck_name(title)
        if not short:
            continue
        qty = int(order.get("total_quantity") or 0) or sum(
            int(value or 0) for value in (order.get("by_channel") or {}).values()
        )
        qty_by_short[short] = qty_by_short.get(short, 0) + qty
        order_for_title.setdefault(short, idx)
        ship_iso = order.get("earliest_expected_ship_iso") or ""
        if ship_iso:
            current = earliest_ship_by_short.get(short)
            if current is None or ship_iso < current:
                earliest_ship_by_short[short] = ship_iso

    # Sort: items with a ship date come first (earliest first), then the rest
    # by qty desc — so the operator's pack queue is naturally urgency-ordered.
    def sort_key(title: str) -> tuple[int, str, int]:
        ship = earliest_ship_by_short.get(title) or ""
        has_date = 0 if ship else 1  # has-date sorts before no-date
        return (has_date, ship, -qty_by_short.get(title, 0))

    pack_items = [
        {
            "title": title,
            "qty": qty_by_short[title],
            "shipBy": earliest_ship_by_short.get(title) or None,
        }
        for title in sorted(qty_by_short.keys(), key=sort_key)
    ]
    duck_names = [item["title"] for item in pack_items]
    return {
        "etsy": etsy,
        "shopify": shopify,
        "unique_titles": len(duck_names),
        "duck_names": duck_names,
        "pack_items": pack_items,
    }


def _customers_to_reply(cases: dict[str, Any] | None) -> int:
    items = (cases or {}).get("items") or []
    return sum(1 for item in items if (item.get("response_recommendation") or {}).get("label"))


REJECTION_TTL_DAYS = 30


def _stable_reject_key(artifact_id: str) -> str:
    """Stable key derived from the artifact_id that survives pipeline re-runs.

    artifact_id format: 'publish::<flow>::<run_id>::<slug>'. The <run_id>
    segment mutates across re-generations (e.g. adds `_hardened2` suffix),
    which used to let rejected drafts come back because the filter matched
    only exact ids. We key on <flow>::<slug> instead, which is stable for a
    given semantic draft. Returns '' for unparseable inputs."""
    if not artifact_id:
        return ""
    parts = artifact_id.split("::")
    if len(parts) < 3:
        return ""
    flow = parts[1] if parts[0] == "publish" and len(parts) > 2 else parts[0]
    slug = parts[-1]
    if not flow or not slug:
        return ""
    return f"{flow}::{slug}"


def _operator_rejected_ids() -> set[str]:
    """Returns currently-active rejected keys (BOTH exact artifactIds AND
    their stable <flow>::<slug> derivations). Entries older than
    REJECTION_TTL_DAYS are filtered out so dismissed drafts can re-surface
    later. Supports both legacy (list of strings) and current (list of
    {artifactId, rejectedAt}) shapes for backward compat.

    Including the stable key in the returned set lets the pendingApprovals
    filter reject an artifact whose id has mutated across re-generations
    (see _stable_reject_key)."""
    data = load_json_file(OPERATOR_REJECTED_PATH) or {}
    ids = data.get("rejected") if isinstance(data, dict) else None
    if not ids:
        return set()
    cutoff = datetime.now(timezone.utc).timestamp() - REJECTION_TTL_DAYS * 24 * 3600
    out: set[str] = set()

    def add_with_stable(artifact_id: str) -> None:
        out.add(artifact_id)
        stable = _stable_reject_key(artifact_id)
        if stable:
            out.add(stable)

    for value in ids:
        if isinstance(value, str):
            add_with_stable(value)  # legacy entries: never expire
            continue
        if not isinstance(value, dict):
            continue
        artifact_id = value.get("artifactId")
        if not artifact_id:
            continue
        rejected_at = value.get("rejectedAt")
        try:
            ts = datetime.fromisoformat(rejected_at.replace("Z", "+00:00")).timestamp()
        except (AttributeError, ValueError, TypeError):
            ts = cutoff + 1  # malformed → keep it (fail-open)
        if ts >= cutoff:
            add_with_stable(str(artifact_id))
    return out


def _pending_approvals(publish: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not publish:
        return []

    cutoff = datetime.now(timezone.utc).timestamp() - 7 * 24 * 3600
    rejected = _operator_rejected_ids()
    raw: list[tuple[str, dict[str, Any], float]] = []

    for item in publish.get("items") or []:
        state = ((item.get("execution_state") or {}).get("state")) or ""
        if state != "draft":
            continue
        artifact_id = str(item.get("artifact_id") or "")
        if artifact_id and artifact_id in rejected:
            continue
        # Also block if the stable <flow>::<slug> was rejected — survives
        # pipeline re-runs that mint a fresh artifact_id for the same draft.
        stable = _stable_reject_key(artifact_id)
        if stable and stable in rejected:
            continue

        summary = item.get("candidate_summary") or {}
        flow = str(item.get("flow") or "?")
        state_source = (item.get("execution_state") or {}).get("state_source")
        run_id = _run_id_base(state_source)
        publish_token = summary.get("publish_token") or None

        age_timestamp = 0.0
        dt = parse_iso(publish_token) if publish_token else None
        if not dt and run_id:
            dt = parse_iso(run_id[:10])
        if dt:
            age_timestamp = dt.timestamp()
        if age_timestamp and age_timestamp < cutoff:
            continue

        dedup_key = f"{flow}:{publish_token or run_id}"
        body_source = summary.get("body") or ""
        instagram_caption = ((summary.get("platform_variants") or {}).get("instagram") or {}).get("caption")
        preview = " ".join((instagram_caption or body_source or "").split())[:280]

        entry = {
            "artifact_id": artifact_id,
            "flow": flow,
            "title": _short_title(summary.get("title") or flow or "draft"),
            "targets": list(summary.get("platform_targets") or []),
            "publish_token": publish_token,
            "body_preview": preview,
        }
        raw.append((dedup_key, entry, age_timestamp))

    by_key: dict[str, tuple[dict[str, Any], float]] = {}
    for key, entry, ts in raw:
        prev = by_key.get(key)
        if prev is None or ts > prev[1]:
            by_key[key] = (entry, ts)

    out = [entry for entry, _ts in by_key.values()]
    out.sort(key=lambda item: item.get("publish_token") or "", reverse=True)
    return out


def _post_agent_counts(publish: dict[str, Any] | None) -> dict[str, int]:
    counts = {"draft": 0, "published": 0, "unknown": 0}
    for item in (publish or {}).get("items") or []:
        state = ((item.get("execution_state") or {}).get("state")) or "unknown"
        if state in counts:
            counts[state] += 1
        else:
            counts["unknown"] += 1
    return counts


def _custom_builds(tasks: dict[str, Any] | None) -> list[dict[str, Any]]:
    builds: list[dict[str, Any]] = []
    for item in ((tasks or {}).get("items") or [])[:6]:
        name = item.get("product_title") or item.get("name") or "Custom duck"
        quantity = int(item.get("quantity") or item.get("units") or 1)
        due_label = item.get("due_label") or item.get("due_at") or None
        entry: dict[str, Any] = {"name": name, "quantity": quantity}
        if due_label:
            entry["due_label"] = str(due_label)
        builds.append(entry)
    return builds


def _shipments_stuck(usps: dict[str, Any] | None) -> dict[str, Any]:
    if not usps:
        return {"count": 0, "samples": [], "note": "no tracking data"}
    items = usps.get("items") or {}
    if not items:
        return {"count": 0, "samples": [], "note": "no shipments tracked"}
    now = parse_iso(usps.get("generated_at")) or datetime.now(timezone.utc)
    stuck: list[dict[str, Any]] = []
    iter_items = items.items() if isinstance(items, dict) else enumerate(items)
    for key, shipment in iter_items:
        if not isinstance(shipment, dict):
            continue
        last = (
            parse_iso(shipment.get("last_event_at"))
            or parse_iso(shipment.get("last_status_at"))
            or parse_iso(shipment.get("updated_at"))
        )
        if not last:
            continue
        days = (now - last).days
        if days <= 5:
            continue
        stuck.append(
            {
                "tracking": str(key)[:10],
                "buyer": shipment.get("buyer_name") or shipment.get("recipient") or "?",
                "days_stuck": days,
            }
        )
    stuck.sort(key=lambda item: -item["days_stuck"])
    return {"count": len(stuck), "samples": stuck[:4]}


def _trend_ideas(trends: dict[str, Any] | None) -> list[dict[str, Any]]:
    seen_themes: set[str] = set()
    scored: list[dict[str, Any]] = []
    for item in ((trends or {}).get("items") or []):
        theme = (item.get("theme") or "").strip()
        if not theme:
            continue
        status = ((item.get("catalog_match") or {}).get("status")) or ""
        if status == "covered":
            continue
        key = theme.lower()
        if key in seen_themes:
            continue
        seen_themes.add(key)
        score = ((item.get("signal_summary") or {}).get("trending_score")) or 0
        scored.append({"title": theme[:40], "score": float(score), "status": status})
    scored.sort(key=lambda item: -item["score"])
    return scored[:5]


def _latest_action_packets() -> Path | None:
    if not CUSTOMER_ACTION_PACKETS_DIR.exists():
        return None
    files = sorted(CUSTOMER_ACTION_PACKETS_DIR.glob("customer_action_packets__*.json"), reverse=True)
    return files[0] if files else None


def _top_tasks() -> list[dict[str, Any]]:
    latest = _latest_action_packets()
    if not latest:
        return []
    data = load_json_file(latest) or {}
    items = data.get("items") or []
    high = [
        item
        for item in items
        if (item.get("priority") == "high") and (item.get("status") == "reply_needed")
    ]
    if len(high) < 3:
        for item in items:
            if item in high or item.get("status") != "reply_needed":
                continue
            high.append(item)
            if len(high) >= 3:
                break
    out: list[dict[str, Any]] = []
    for item in high[:3]:
        summary = (item.get("customer_summary") or "").strip()
        out.append(
            {
                "id": str(item.get("short_id") or item.get("artifact_id") or "?"),
                "action": str(item.get("next_operator_action") or item.get("packet_type") or "review"),
                "type": str(item.get("packet_type") or "reply"),
                "summary": summary[:140] if summary else "",
                "customer_name": str(item.get("customer_name") or "") or None,
            }
        )
    return out


def _weekly_insights(receipts: dict[str, Any] | None, catalog: dict[str, Any] | None) -> dict[str, Any]:
    empty: dict[str, Any] = {
        "this_week_orders": 0,
        "this_week_units": 0,
        "last_week_orders": 0,
        "last_week_units": 0,
        "week_over_week_pct": None,
        "best_seller_this_week": None,
        "avg_units_per_order_today": 0.0,
        "today_orders": 0,
        "unsold_in_window": {"count": 0, "window_days": 0, "sample": []},
    }
    if not receipts:
        return empty

    items = receipts.get("items") or []
    if not items:
        return empty

    timestamps = [int(item.get("created_timestamp") or 0) for item in items if item.get("created_timestamp")]
    if not timestamps:
        return empty

    now_ts = datetime.now().timestamp()
    oldest_ts = min(timestamps)
    window_days = max(1, int((now_ts - oldest_ts) // 86400))
    this_week_start = now_ts - 7 * 24 * 3600
    last_week_start = now_ts - 14 * 24 * 3600
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    def bucket(start_at: float, end_at: float | None = None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for receipt in items:
            created_at = int(receipt.get("created_timestamp") or 0)
            if created_at <= start_at:
                continue
            if end_at is not None and created_at > end_at:
                continue
            out.append(receipt)
        return out

    def units(receipts_list: list[dict[str, Any]]) -> int:
        return sum(
            int(tx.get("quantity") or 0)
            for receipt in receipts_list
            for tx in (receipt.get("transactions") or [])
        )

    this_week = bucket(this_week_start)
    last_week = bucket(last_week_start, this_week_start)
    today = bucket(today_start)

    this_week_units = units(this_week)
    last_week_units = units(last_week)
    today_units = units(today)

    title_counts: dict[str, int] = {}
    for receipt in this_week:
        for tx in receipt.get("transactions") or []:
            title = tx.get("title") or "Unknown"
            title_counts[title] = title_counts.get(title, 0) + int(tx.get("quantity") or 0)
    # Use _short_duck_name (not _short_title) so titles like
    # "Graduation Duck – 3D-Printed Duck Figurine - Custom Gift" collapse
    # to "Graduation Duck" — splits on |, –, - separators and takes the
    # leading product-name segment.
    top_sellers = [
        {"title": _short_duck_name(title), "units": units}
        for title, units in sorted(
            title_counts.items(), key=lambda item: item[1], reverse=True
        )[:3]
    ]
    best_seller = top_sellers[0] if top_sellers else None

    sold_listing_ids: set[int] = set()
    for receipt in items:
        for tx in receipt.get("transactions") or []:
            listing_id = tx.get("listing_id")
            if listing_id:
                try:
                    sold_listing_ids.add(int(listing_id))
                except (TypeError, ValueError):
                    pass

    catalog_items = (catalog or {}).get("items") or {}
    unsold_titles: list[str] = []
    for listing_id, info in catalog_items.items():
        if not isinstance(info, dict) or info.get("status") != "active":
            continue
        try:
            listing_id_int = int(listing_id)
        except (TypeError, ValueError):
            continue
        if listing_id_int not in sold_listing_ids:
            unsold_titles.append(info.get("title") or str(listing_id))

    avg_units_today = (today_units / len(today)) if today else 0.0
    wow_pct: int | None = None
    if last_week_units > 0:
        wow_pct = round(((this_week_units - last_week_units) / last_week_units) * 100)

    return {
        "this_week_orders": len(this_week),
        "this_week_units": this_week_units,
        "last_week_orders": len(last_week),
        "last_week_units": last_week_units,
        "week_over_week_pct": wow_pct,
        "best_seller_this_week": best_seller,
        "top_sellers_this_week": top_sellers,
        "avg_units_per_order_today": round(avg_units_today, 1),
        "today_orders": len(today),
        "unsold_in_window": {
            "count": len(unsold_titles),
            "window_days": window_days,
            "sample": [_short_title(title) for title in unsold_titles[:5]],
        },
    }


def _weekly_sales(etsy: dict[str, Any] | None, shopify: dict[str, Any] | None) -> dict[str, Any]:
    out = {
        "etsy_orders_this_week": 0,
        "etsy_units_this_week": 0,
        "shopify_open_orders": 0,
        "shopify_open_units": 0,
        "etsy_orders_today": 0,
        "etsy_units_today": 0,
        "shopify_orders_today": 0,
        "shopify_units_today": 0,
    }
    now = datetime.now().astimezone()
    today_start_local = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_ts = today_start_local.timestamp()

    if etsy:
        generated_at = parse_iso(etsy.get("generated_at")) or datetime.now(timezone.utc)
        week_cutoff = generated_at.timestamp() - 7 * 24 * 3600
        items = etsy.get("items") or []
        recent = [item for item in items if (item.get("created_timestamp") or 0) > week_cutoff]
        out["etsy_orders_this_week"] = len(recent)
        out["etsy_units_this_week"] = sum(int(item.get("quantity") or 0) for item in recent)
        today_tx = [item for item in items if (item.get("created_timestamp") or 0) >= today_start_ts]
        out["etsy_orders_today"] = len({item.get("receipt_id") for item in today_tx if item.get("receipt_id")})
        out["etsy_units_today"] = sum(int(item.get("quantity") or 0) for item in today_tx)

    if shopify:
        counts = shopify.get("counts") or {}
        out["shopify_open_orders"] = int(counts.get("orders") or 0)
        out["shopify_open_units"] = int(counts.get("units") or 0)
        for order in shopify.get("items") or []:
            created = parse_iso(order.get("created_at"))
            if not created or created.timestamp() < today_start_ts:
                continue
            out["shopify_orders_today"] += 1
            out["shopify_units_today"] += sum(
                int(item.get("quantity") or 0) for item in (order.get("line_items") or [])
            )
    return out


def _sales_trends(
    receipts: dict[str, Any] | None,
    shopify: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Reads the Etsy *receipts* snapshot (~45-day rolling history, paid+open
    # filtered to is_paid) instead of the open-orders-tied transactions
    # snapshot — which only carries timestamps for currently-open orders
    # and so collapses to ~4 days of history when fulfillment runs ahead.
    # Shopify is now joined too; before this change the card showed 0 for
    # any day where all sales went through Shopify.
    now = datetime.now().astimezone()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    weekday = today_start.weekday()
    this_week_start = today_start - timedelta(days=weekday)
    last_week_start = this_week_start - timedelta(days=7)
    last_week_cutoff = last_week_start + (now - this_week_start)
    this_month_start = today_start.replace(day=1)
    if this_month_start.month == 1:
        last_month_start = this_month_start.replace(year=this_month_start.year - 1, month=12)
    else:
        last_month_start = this_month_start.replace(month=this_month_start.month - 1)
    last_month_cutoff = last_month_start + (now - this_month_start)

    # Build a unified [(timestamp, order_id, units)] stream from both
    # marketplaces so the bucket math runs once and shopify-only days
    # don't read as zero.
    sales: list[tuple[float, str, int]] = []
    if receipts:
        for r in receipts.get("items") or []:
            if not r.get("is_paid"):
                continue
            created = float(r.get("created_timestamp") or 0)
            if created <= 0:
                continue
            order_id = f"etsy:{r.get('receipt_id')}"
            qty = sum(int(t.get("quantity") or 0) for t in (r.get("transactions") or []))
            sales.append((created, order_id, qty))
    if shopify:
        for o in shopify.get("items") or []:
            created_dt = parse_iso(o.get("created_at"))
            if not created_dt:
                continue
            order_id = f"shopify:{o.get('id') or o.get('order_number') or o.get('name')}"
            qty = sum(int(li.get("quantity") or 0) for li in (o.get("line_items") or []))
            sales.append((created_dt.timestamp(), order_id, qty))

    def collect(start_ts: float, end_ts: float | None = None) -> tuple[int, int]:
        order_ids: set[str] = set()
        units = 0
        for ts, oid, qty in sales:
            if ts < start_ts:
                continue
            if end_ts is not None and ts >= end_ts:
                continue
            order_ids.add(oid)
            units += qty
        return len(order_ids), units

    today_orders, today_units = collect(today_start.timestamp())
    yesterday_orders, yesterday_units = collect(yesterday_start.timestamp(), today_start.timestamp())
    wtd_orders, wtd_units = collect(this_week_start.timestamp())
    wtd_last_week_orders, wtd_last_week_units = collect(last_week_start.timestamp(), last_week_cutoff.timestamp())
    mtd_orders, mtd_units = collect(this_month_start.timestamp())
    mtd_last_month_orders, mtd_last_month_units = collect(last_month_start.timestamp(), last_month_cutoff.timestamp())

    if receipts and shopify:
        source = "etsy+shopify"
    elif shopify:
        source = "shopify"
    else:
        source = "etsy"
    return {
        "today_units": today_units,
        "today_orders": today_orders,
        "yesterday_units": yesterday_units,
        "yesterday_orders": yesterday_orders,
        "wtd_units": wtd_units,
        "wtd_orders": wtd_orders,
        "wtd_last_week_units": wtd_last_week_units,
        "wtd_last_week_orders": wtd_last_week_orders,
        "mtd_units": mtd_units,
        "mtd_orders": mtd_orders,
        "mtd_last_month_units": mtd_last_month_units,
        "mtd_last_month_orders": mtd_last_month_orders,
        "source": source,
    }


def load_compact_interface_sources() -> dict[str, Any]:
    return {
        "packing": load_json_file(PACKING_SUMMARY),
        "cases": load_json_file(CUSTOMER_CASES),
        "publish": load_json_file(PUBLISH_CANDIDATES),
        "tasks": load_json_file(CUSTOM_BUILD_TASKS),
        "trends": load_json_file(TREND_CANDIDATES),
        "receipts": load_json_file(ETSY_RECEIPTS),
        "catalog": load_json_file(CATALOG_INDEX),
        "usps": load_json_file(USPS_TRACKING),
        "etsy": load_json_file(ETSY_TRANSACTIONS),
        "shopify": load_json_file(SHOPIFY_OPEN_ORDERS),
    }


def build_compact_operator_surface(source_bundle: dict[str, Any] | None = None) -> dict[str, Any]:
    source_bundle = source_bundle or load_compact_interface_sources()
    packing = source_bundle.get("packing")
    cases = source_bundle.get("cases")
    publish = source_bundle.get("publish")
    tasks = source_bundle.get("tasks")
    trends = source_bundle.get("trends")
    receipts = source_bundle.get("receipts")
    catalog = source_bundle.get("catalog")
    usps = source_bundle.get("usps")
    etsy = source_bundle.get("etsy")
    shopify = source_bundle.get("shopify")

    generated_at = (
        (packing or {}).get("generated_at")
        or (cases or {}).get("generated_at")
        or datetime.now(timezone.utc).isoformat()
    )
    pending_approvals = _pending_approvals(publish)
    top_tasks = _top_tasks()
    trend_ideas = _trend_ideas(trends)
    custom_builds = _custom_builds(tasks)

    return {
        "surface_version": SURFACE_VERSION,
        "generated_at": generated_at,
        "source_label": "Duck Ops local",
        "source_paths": {
            "packing_summary": str(PACKING_SUMMARY),
            "customer_cases": str(CUSTOMER_CASES),
            "publish_candidates": str(PUBLISH_CANDIDATES),
            "custom_build_task_candidates": str(CUSTOM_BUILD_TASKS),
            "trend_candidates": str(TREND_CANDIDATES),
            "etsy_receipts": str(ETSY_RECEIPTS),
            "catalog_index": str(CATALOG_INDEX),
            "usps_tracking": str(USPS_TRACKING),
            "etsy_transactions": str(ETSY_TRANSACTIONS),
            "shopify_open_orders": str(SHOPIFY_OPEN_ORDERS),
        },
        "metrics": {
            "ducks_to_pack_today": _ducks_to_pack(packing),
            "customers_to_reply": _customers_to_reply(cases),
            "pending_approvals": len(pending_approvals),
            "trend_ideas": len(trend_ideas),
            "top_tasks": len(top_tasks),
            "custom_builds": len(custom_builds),
        },
        "post_agent": _post_agent_counts(publish),
        "custom_builds": custom_builds,
        "packing": _packing_breakdown(packing),
        "shipments_stuck": _shipments_stuck(usps),
        "trend_ideas": trend_ideas,
        "top_tasks": top_tasks,
        "weekly_insights": _weekly_insights(receipts, catalog),
        "weekly_sales": _weekly_sales(etsy, shopify),
        "sales_trends": _sales_trends(receipts, shopify),
        "pending_approvals": pending_approvals,
    }


def build_interface_contract_summary(surface: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = surface or build_compact_operator_surface()
    metrics = payload.get("metrics") or {}
    return {
        "available": True,
        "surface_version": int(payload.get("surface_version") or SURFACE_VERSION),
        "generated_at": payload.get("generated_at"),
        "source_label": payload.get("source_label") or "Duck Ops local",
        "source_paths": dict(payload.get("source_paths") or {}),
        "ducks_to_pack_today": int(metrics.get("ducks_to_pack_today") or 0),
        "customers_to_reply": int(metrics.get("customers_to_reply") or 0),
        "pending_approvals_count": int(metrics.get("pending_approvals") or len(payload.get("pending_approvals") or [])),
        "trend_ideas_count": int(metrics.get("trend_ideas") or len(payload.get("trend_ideas") or [])),
        "top_tasks_count": int(metrics.get("top_tasks") or len(payload.get("top_tasks") or [])),
        "custom_builds_count": int(metrics.get("custom_builds") or len(payload.get("custom_builds") or [])),
        "pending_approvals": list(payload.get("pending_approvals") or [])[:4],
        "top_tasks": list(payload.get("top_tasks") or [])[:3],
        "trend_ideas": list(payload.get("trend_ideas") or [])[:4],
        "post_agent": dict(payload.get("post_agent") or {}),
        "packing": dict(payload.get("packing") or {}),
        "weekly_insights": dict(payload.get("weekly_insights") or {}),
        "weekly_sales": dict(payload.get("weekly_sales") or {}),
        "sales_trends": dict(payload.get("sales_trends") or {}),
        "shipments_stuck": dict(payload.get("shipments_stuck") or {}),
    }


def build_widget_status_payload(surface: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = surface or build_compact_operator_surface()
    metrics = payload.get("metrics") or {}
    packing = payload.get("packing") or {}
    weekly_insights = payload.get("weekly_insights") or {}
    weekly_sales = payload.get("weekly_sales") or {}
    sales_trends = payload.get("sales_trends") or {}

    return {
        "surfaceVersion": int(payload.get("surface_version") or SURFACE_VERSION),
        "generatedAt": payload.get("generated_at"),
        "sourceLabel": payload.get("source_label") or "Duck Ops local",
        "ducksToPackToday": int(metrics.get("ducks_to_pack_today") or 0),
        "customersToReply": int(metrics.get("customers_to_reply") or 0),
        "postAgent": dict(payload.get("post_agent") or {}),
        "customBuilds": list(payload.get("custom_builds") or []),
        "packing": {
            "etsy": int(packing.get("etsy") or 0),
            "shopify": int(packing.get("shopify") or 0),
            "uniqueTitles": int(packing.get("unique_titles") or 0),
            "duckNames": list(packing.get("duck_names") or []),
            "packItems": [
                {
                    "title": item.get("title"),
                    "qty": int(item.get("qty") or 0),
                    "shipBy": item.get("shipBy") or item.get("ship_by"),
                }
                for item in list(packing.get("pack_items") or [])
            ],
        },
        "shipmentsStuck": dict(payload.get("shipments_stuck") or {}),
        "trendIdeas": list(payload.get("trend_ideas") or []),
        "topTasks": [
            {
                "id": item.get("id"),
                "action": item.get("action"),
                "type": item.get("type"),
                "summary": item.get("summary"),
                "customerName": item.get("customer_name"),
            }
            for item in list(payload.get("top_tasks") or [])
        ],
        "weeklyInsights": {
            "thisWeekOrders": int(weekly_insights.get("this_week_orders") or 0),
            "thisWeekUnits": int(weekly_insights.get("this_week_units") or 0),
            "lastWeekOrders": int(weekly_insights.get("last_week_orders") or 0),
            "lastWeekUnits": int(weekly_insights.get("last_week_units") or 0),
            "weekOverWeekPct": weekly_insights.get("week_over_week_pct"),
            "bestSellerThisWeek": weekly_insights.get("best_seller_this_week"),
            "topSellersThisWeek": weekly_insights.get("top_sellers_this_week") or [],
            "avgUnitsPerOrderToday": float(weekly_insights.get("avg_units_per_order_today") or 0.0),
            "todayOrders": int(weekly_insights.get("today_orders") or 0),
            "unsoldInWindow": {
                "count": int(((weekly_insights.get("unsold_in_window") or {}).get("count")) or 0),
                "windowDays": int(((weekly_insights.get("unsold_in_window") or {}).get("window_days")) or 0),
                "sample": list(((weekly_insights.get("unsold_in_window") or {}).get("sample")) or []),
            },
        },
        "weeklySales": {
            "etsyOrdersThisWeek": int(weekly_sales.get("etsy_orders_this_week") or 0),
            "etsyUnitsThisWeek": int(weekly_sales.get("etsy_units_this_week") or 0),
            "shopifyOpenOrders": int(weekly_sales.get("shopify_open_orders") or 0),
            "shopifyOpenUnits": int(weekly_sales.get("shopify_open_units") or 0),
            "etsyOrdersToday": int(weekly_sales.get("etsy_orders_today") or 0),
            "etsyUnitsToday": int(weekly_sales.get("etsy_units_today") or 0),
            "shopifyOrdersToday": int(weekly_sales.get("shopify_orders_today") or 0),
            "shopifyUnitsToday": int(weekly_sales.get("shopify_units_today") or 0),
        },
        "salesTrends": {
            "todayUnits": int(sales_trends.get("today_units") or 0),
            "todayOrders": int(sales_trends.get("today_orders") or 0),
            "yesterdayUnits": int(sales_trends.get("yesterday_units") or 0),
            "yesterdayOrders": int(sales_trends.get("yesterday_orders") or 0),
            "wtdUnits": int(sales_trends.get("wtd_units") or 0),
            "wtdOrders": int(sales_trends.get("wtd_orders") or 0),
            "wtdLastWeekUnits": int(sales_trends.get("wtd_last_week_units") or 0),
            "wtdLastWeekOrders": int(sales_trends.get("wtd_last_week_orders") or 0),
            "mtdUnits": int(sales_trends.get("mtd_units") or 0),
            "mtdOrders": int(sales_trends.get("mtd_orders") or 0),
            "mtdLastMonthUnits": int(sales_trends.get("mtd_last_month_units") or 0),
            "mtdLastMonthOrders": int(sales_trends.get("mtd_last_month_orders") or 0),
            "source": sales_trends.get("source") or "etsy",
        },
        "pendingApprovals": [
            {
                "artifactId": item.get("artifact_id"),
                "flow": item.get("flow"),
                "title": item.get("title"),
                "targets": list(item.get("targets") or []),
                "publishToken": item.get("publish_token"),
                "bodyPreview": item.get("body_preview"),
            }
            for item in list(payload.get("pending_approvals") or [])
        ],
    }
