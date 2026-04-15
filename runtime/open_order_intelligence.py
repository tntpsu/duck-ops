#!/usr/bin/env python3
"""
Open-order and packing snapshots for Duck Ops.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from customer_case_enrichment import load_recent_etsy_receipts_snapshot
from workflow_control import record_workflow_transition, write_workflow_receipt


ROOT = Path(__file__).resolve().parents[1]
NORMALIZED_DIR = ROOT / "state" / "normalized"
DUCK_AGENT_ROOT = Path("/Users/philtullai/ai-agents/duckAgent")

ETSY_OPEN_ORDERS_PATH = NORMALIZED_DIR / "etsy_open_orders_snapshot.json"
ETSY_TRANSACTIONS_SNAPSHOT_PATH = NORMALIZED_DIR / "etsy_transactions_snapshot.json"
SHOPIFY_OPEN_ORDERS_PATH = NORMALIZED_DIR / "shopify_open_orders_snapshot.json"
PACKING_SUMMARY_PATH = NORMALIZED_DIR / "packing_summary.json"
ORDER_SNAPSHOT_REFRESH_STATE_PATH = ROOT / "state" / "order_snapshot_refresh.json"

ORDER_SNAPSHOT_REFRESH_WORKFLOW_ID = "order_snapshot_refresh"
ORDER_SNAPSHOT_REFRESH_LANE = "order_snapshot_refresh"
ORDER_SNAPSHOT_REFRESH_LABEL = "Order Snapshot Refresh"

ORDER_REFRESH_ETSY_TIMEOUT_SECONDS = 120
ORDER_REFRESH_SHOPIFY_TIMEOUT_SECONDS = 120
ETSY_OPEN_ORDER_LOOKBACK_DAYS = 45
ETSY_TRANSACTION_LOOKBACK_BUFFER_DAYS = 7

CUSTOM_KEYWORDS = ("custom", "build your custom", "design your own")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone()
    except ValueError:
        return None


def _age_hours_from_payload(payload: dict[str, Any]) -> float | None:
    generated_at = _parse_iso_datetime(payload.get("generated_at"))
    if not generated_at:
        return None
    return max(0.0, (datetime.now().astimezone() - generated_at).total_seconds() / 3600.0)


def _snapshot_counts(payload: dict[str, Any]) -> dict[str, int]:
    counts = payload.get("counts") or {}
    return {
        "orders": int(counts.get("orders") or 0),
        "units": int(counts.get("units") or 0),
        "non_custom_titles": int(counts.get("non_custom_titles") or 0),
        "non_custom_units": int(counts.get("non_custom_units") or 0),
        "custom_order_lines": int(counts.get("custom_order_lines") or 0),
    }


def _load_cached_snapshot(path: Path) -> dict[str, Any]:
    payload = _load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _run_open_order_subcommand(command: str, *, timeout_seconds: int) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), command],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"{command} exited with code {proc.returncode}")
    raw = (proc.stdout or "").strip()
    if not raw:
        raise RuntimeError(f"{command} returned no JSON payload")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise RuntimeError(f"{command} returned a non-object payload")
    return payload


def _source_result_from_payload(
    *,
    source: str,
    payload: dict[str, Any],
    status: str,
    cache_path: Path,
    error: str | None = None,
    timed_out: bool = False,
) -> dict[str, Any]:
    return {
        "source": source,
        "status": status,
        "error": error,
        "timed_out": timed_out,
        "generated_at": payload.get("generated_at"),
        "age_hours": _age_hours_from_payload(payload),
        "path": str(cache_path),
        "counts": _snapshot_counts(payload),
    }


def _record_order_refresh_receipt(kind: str, payload: dict[str, Any]) -> None:
    write_workflow_receipt(
        ORDER_SNAPSHOT_REFRESH_WORKFLOW_ID,
        {
            "lane": ORDER_SNAPSHOT_REFRESH_LANE,
            "display_label": ORDER_SNAPSHOT_REFRESH_LABEL,
            "kind": kind,
            "payload": payload,
            "recorded_at": datetime.now().astimezone().isoformat(),
        },
    )


def _title_is_custom(title: str | None) -> bool:
    lowered = str(title or "").lower()
    return any(keyword in lowered for keyword in CUSTOM_KEYWORDS)


def _normalize_title(title: str | None) -> str:
    return " ".join(str(title or "").split()).strip()


def _normalize_option_value(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"default title", "default", "default option"}:
        return None
    return text


def _compose_person_name(first: Any, last: Any) -> str | None:
    parts = [str(first or "").strip(), str(last or "").strip()]
    name = " ".join(part for part in parts if part)
    return name or None


def _shopify_buyer_name(order: dict[str, Any]) -> str | None:
    customer = order.get("customer") or {}
    shipping = order.get("shipping_address") or {}
    billing = order.get("billing_address") or {}

    for source in (customer, shipping, billing):
        if not isinstance(source, dict):
            continue
        name = _compose_person_name(source.get("first_name"), source.get("last_name"))
        if name:
            return name
        explicit = str(source.get("name") or "").strip()
        if explicit:
            return explicit

    email = str(order.get("email") or customer.get("email") or "").strip()
    if email:
        local_part = email.split("@", 1)[0].replace(".", " ").replace("_", " ").replace("-", " ").strip()
        if local_part:
            return " ".join(part.capitalize() for part in local_part.split())
    return None


def _ts_to_iso(value: Any) -> str | None:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone().isoformat()


def _earlier_iso(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return left if left <= right else right


def _earlier_timestamp(left: Any, right: Any) -> Any:
    if left in {None, ""}:
        return right
    if right in {None, ""}:
        return left
    try:
        return left if int(left) <= int(right) else right
    except (TypeError, ValueError):
        return left


def _etsy_timestamp_bounds(receipts: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    created_values: list[int] = []
    for receipt in receipts:
        try:
            created = int(receipt.get("created_timestamp") or receipt.get("create_timestamp") or 0)
        except (TypeError, ValueError):
            continue
        if created > 0:
            created_values.append(created)
    if not created_values:
        return None, None
    oldest = min(created_values)
    min_created = datetime.fromtimestamp(
        max(0, oldest - (ETSY_TRANSACTION_LOOKBACK_BUFFER_DAYS * 86400)),
        tz=timezone.utc,
    ).isoformat()
    max_created = datetime.now(timezone.utc).isoformat()
    return min_created, max_created


def _packing_urgency_rank(item: dict[str, Any]) -> tuple[int, str, int, str]:
    earliest_ship = item.get("earliest_expected_ship_date")
    oldest_created = item.get("oldest_created_at")
    urgency = str(item.get("urgency_label") or "open").lower()
    urgency_rank = {
        "ship today": 0,
        "ship soon": 1,
        "aging order": 2,
        "open": 3,
    }.get(urgency, 9)
    ship_key = str(earliest_ship or "9999-12-31T23:59:59+00:00")
    created_key = str(oldest_created or "9999-12-31T23:59:59+00:00")
    return (
        urgency_rank,
        ship_key,
        -int(item.get("total_quantity") or 0),
        created_key,
    )


def _packing_urgency_fields(*, earliest_expected_ship_date: Any, oldest_created_at: str | None) -> dict[str, Any]:
    now_local = datetime.now().astimezone()
    ship_dt = None
    if earliest_expected_ship_date not in {None, ""}:
        try:
            ship_dt = datetime.fromtimestamp(int(earliest_expected_ship_date), tz=timezone.utc).astimezone()
        except (TypeError, ValueError):
            ship_dt = None

    created_dt = None
    if oldest_created_at:
        try:
            created_dt = datetime.fromisoformat(str(oldest_created_at).replace("Z", "+00:00")).astimezone()
        except ValueError:
            created_dt = None

    label = "Open"
    reason = "Open paid order without a shipping deadline yet."
    if ship_dt:
        delta_days = (ship_dt.date() - now_local.date()).days
        if delta_days <= 0:
            label = "Ship today"
            reason = "At least one order is due to ship today."
        elif delta_days == 1:
            label = "Ship soon"
            reason = "At least one order is due to ship tomorrow."
        else:
            label = f"Ship by {ship_dt.strftime('%b')} {ship_dt.day}"
            reason = f"Earliest expected ship date is {ship_dt.strftime('%b')} {ship_dt.day}."
    elif created_dt:
        age_days = (now_local - created_dt).total_seconds() / 86400.0
        if age_days >= 3:
            label = "Aging order"
            reason = f"Oldest open order is about {age_days:.1f} day(s) old."

    return {
        "urgency_label": label,
        "urgency_reason": reason,
        "oldest_created_at": oldest_created_at,
        "earliest_expected_ship_date": earliest_expected_ship_date,
        "earliest_expected_ship_iso": _ts_to_iso(earliest_expected_ship_date),
    }


def _run_duckagent_etsy_transaction_lookup(
    transaction_ids: list[str],
    max_pages: int = 20,
    *,
    min_created: str | None = None,
    max_created: str | None = None,
) -> dict[str, Any] | None:
    wanted_ids = [str(item).strip() for item in transaction_ids if str(item).strip()]
    if not wanted_ids:
        return {"generated_at": datetime.now().astimezone().isoformat(), "items": []}
    script = textwrap.dedent(
        f"""
        import json
        import os
        from datetime import datetime
        from helpers.etsy_helper import etsy_get_shop_transactions

        shop_id = os.getenv("ETSY_SHOP_ID")
        wanted_ids = set({wanted_ids!r})
        min_created = {min_created!r}
        max_created = {max_created!r}
        rows = []
        found = set()
        limit = 100
        offset = 0
        for _ in range({max_pages}):
            response = etsy_get_shop_transactions(
                shop_id,
                limit=limit,
                offset=offset,
                min_created=min_created,
                max_created=max_created,
            )
            results = response.get("results", []) or []
            if not results:
                break
            for tx in results:
                tx_id = str(tx.get("transaction_id") or "")
                if tx_id not in wanted_ids:
                    continue
                rows.append(
                    {{
                        "transaction_id": tx_id,
                        "receipt_id": str(tx.get("receipt_id") or ""),
                        "title": tx.get("title"),
                        "quantity": tx.get("quantity"),
                        "sku": tx.get("sku"),
                        "product_id": tx.get("product_id"),
                        "listing_id": tx.get("listing_id"),
                        "created_timestamp": tx.get("created_timestamp") or tx.get("create_timestamp"),
                        "variations": tx.get("variations") or [],
                        "product_data": tx.get("product_data") or [],
                    }}
                )
                found.add(tx_id)
            if found >= wanted_ids or len(results) < limit:
                break
            offset += limit
        print(json.dumps({{
            "generated_at": datetime.now().astimezone().isoformat(),
            "items": rows,
            "requested": len(wanted_ids),
            "found": len(found),
            "min_created": min_created,
            "max_created": max_created,
        }}))
        """
    )
    proc = subprocess.run(
        [str(DUCK_AGENT_ROOT / ".venv" / "bin" / "python"), "-c", script],
        cwd=str(DUCK_AGENT_ROOT),
        env={**os.environ, **_load_env_file(DUCK_AGENT_ROOT / ".env")},
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    try:
        return json.loads(proc.stdout)
    except Exception:
        return None


def load_etsy_transaction_details(
    transaction_ids: list[str],
    *,
    min_created: str | None = None,
    max_created: str | None = None,
) -> dict[str, Any]:
    fresh = _run_duckagent_etsy_transaction_lookup(
        transaction_ids,
        min_created=min_created,
        max_created=max_created,
    )
    if fresh:
        _write_json(ETSY_TRANSACTIONS_SNAPSHOT_PATH, fresh)
        return fresh
    if ETSY_TRANSACTIONS_SNAPSHOT_PATH.exists():
        cached = _load_json(ETSY_TRANSACTIONS_SNAPSHOT_PATH, {})
        cached_items = [
            item
            for item in (cached.get("items") or [])
            if str(item.get("transaction_id") or "").strip() in {str(tx).strip() for tx in transaction_ids}
        ]
        return {
            "generated_at": cached.get("generated_at"),
            "items": cached_items,
            "requested": len(transaction_ids),
            "found": len(cached_items),
            "error": "etsy_transactions_lookup_fallback",
        }
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "items": [],
        "requested": len(transaction_ids),
        "found": 0,
        "error": "etsy_transactions_unavailable",
    }


def _build_transaction_index(transaction_snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("transaction_id") or "").strip(): item
        for item in (transaction_snapshot.get("items") or [])
        if str(item.get("transaction_id") or "").strip()
    }


def _extract_custom_transaction_details(transaction: dict[str, Any] | None) -> dict[str, Any]:
    details = {
        "custom_type": None,
        "personalization": None,
        "variation_pairs": [],
    }
    if not transaction:
        return details
    variation_pairs = []
    for variation in transaction.get("variations") or []:
        name = str(variation.get("formatted_name") or "").strip()
        value = str(variation.get("formatted_value") or "").strip()
        if not name or not value:
            continue
        variation_pairs.append({"name": name, "value": value})
        lowered_name = name.lower()
        if lowered_name == "duck type" and not details["custom_type"]:
            details["custom_type"] = value
        if lowered_name == "personalization" and not details["personalization"]:
            details["personalization"] = value
    if not details["custom_type"]:
        for product_data in transaction.get("product_data") or []:
            values = product_data.get("values") or []
            if values:
                details["custom_type"] = str(values[0]).strip() or None
                break
    details["variation_pairs"] = variation_pairs
    return details


def _build_custom_design_summary(transaction: dict[str, Any] | None) -> str | None:
    details = _extract_custom_transaction_details(transaction)
    custom_type = str(details.get("custom_type") or "").strip()
    personalization = str(details.get("personalization") or "").strip()
    if custom_type and personalization:
        return f"{custom_type}: {personalization}"
    if personalization:
        return personalization
    if custom_type:
        return custom_type
    return None


def _line_item_option_labels(line_item: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    variant_title = _normalize_option_value(line_item.get("variant_title"))
    if variant_title:
        labels.append(variant_title)

    for pair in line_item.get("variation_pairs") or []:
        if not isinstance(pair, dict):
            continue
        name = str(pair.get("name") or "").strip()
        value = _normalize_option_value(pair.get("value"))
        if not value:
            continue
        label = f"{name}: {value}" if name else value
        if label not in labels:
            labels.append(label)
    return labels


def _format_option_summary(option_counts: dict[str, int]) -> str | None:
    if not option_counts:
        return None
    ranked = sorted(
        option_counts.items(),
        key=lambda item: (-int(item[1] or 0), str(item[0]).lower()),
    )
    parts: list[str] = []
    for label, count in ranked[:5]:
        if int(count or 0) > 1:
            parts.append(f"{label} x{int(count)}")
        else:
            parts.append(label)
    if len(ranked) > 5:
        parts.append(f"+{len(ranked) - 5} more")
    return ", ".join(parts)


def build_etsy_open_orders_snapshot() -> dict[str, Any]:
    receipts_payload = load_recent_etsy_receipts_snapshot(days_back=ETSY_OPEN_ORDER_LOOKBACK_DAYS, max_age_hours=0)
    open_receipts = [
        receipt
        for receipt in (receipts_payload.get("items") or [])
        if receipt.get("is_paid") and not receipt.get("is_shipped")
    ]
    min_created, max_created = _etsy_timestamp_bounds(open_receipts)
    wanted_transaction_ids = [
        str(transaction.get("transaction_id") or "").strip()
        for receipt in open_receipts
        for transaction in (receipt.get("transactions") or [])
        if str(transaction.get("transaction_id") or "").strip()
    ]
    transaction_payload = load_etsy_transaction_details(
        wanted_transaction_ids,
        min_created=min_created,
        max_created=max_created,
    )
    transaction_index = _build_transaction_index(transaction_payload)
    rows: list[dict[str, Any]] = []
    for receipt in open_receipts:
        line_items = []
        for transaction in receipt.get("transactions") or []:
            quantity = int(transaction.get("quantity") or 0)
            if quantity <= 0:
                continue
            title = _normalize_title(transaction.get("title"))
            transaction_id = str(transaction.get("transaction_id") or "").strip()
            transaction_details = transaction_index.get(transaction_id)
            custom_details = _extract_custom_transaction_details(transaction_details)
            line_items.append(
                {
                    "channel": "etsy",
                    "order_ref": str(receipt.get("receipt_id") or ""),
                    "product_title": title,
                    "product_id": str(transaction.get("listing_id") or ""),
                    "transaction_id": transaction_id,
                    "quantity": quantity,
                    "is_custom": _title_is_custom(title),
                    "variant_title": None,
                    "custom_type": custom_details.get("custom_type"),
                    "personalization": custom_details.get("personalization"),
                    "custom_design_summary": _build_custom_design_summary(transaction_details),
                    "variation_pairs": custom_details.get("variation_pairs") or [],
                }
            )
        if not line_items:
            continue
        rows.append(
            {
                "channel": "etsy",
                "order_ref": str(receipt.get("receipt_id") or ""),
                "buyer_name": receipt.get("buyer_name"),
                "created_at": _ts_to_iso(receipt.get("created_timestamp")),
                "expected_ship_date": receipt.get("expected_ship_date"),
                "financial_status": "paid" if receipt.get("is_paid") else "unknown",
                "fulfillment_status": "shipped" if receipt.get("is_shipped") else "unfulfilled",
                "line_items": line_items,
            }
        )

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "source": "etsy_live_api" if not receipts_payload.get("error") else "etsy_receipts_fallback",
        "receipt_snapshot_generated_at": receipts_payload.get("generated_at"),
        "transaction_snapshot_generated_at": transaction_payload.get("generated_at"),
        "counts": {
            "orders": len(rows),
            "units": sum(item["quantity"] for row in rows for item in row.get("line_items") or []),
        },
        "items": rows,
    }
    _write_json(ETSY_OPEN_ORDERS_PATH, payload)
    return payload


def _fetch_shopify_open_orders() -> list[dict[str, Any]]:
    env = _load_env_file(DUCK_AGENT_ROOT / ".env")
    script = textwrap.dedent(
        """
        import json
        from helpers.shopify_helper import _rest_get_paginated

        params = {
            "status": "open",
            "limit": 250,
            "fields": "id,name,created_at,financial_status,fulfillment_status,cancelled_at,line_items,email,customer,shipping_address,billing_address",
        }
        orders = []
        for page in _rest_get_paginated("orders.json", params=params):
            orders.extend(page.get("orders", []))
        print(json.dumps({"items": orders}))
        """
    )
    proc = subprocess.run(
        [str(DUCK_AGENT_ROOT / ".venv" / "bin" / "python"), "-c", script],
        cwd=str(DUCK_AGENT_ROOT),
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=180,
        check=True,
    )
    payload = json.loads(proc.stdout)
    return payload.get("items") or []


def build_shopify_open_orders_snapshot() -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for order in _fetch_shopify_open_orders():
        if order.get("cancelled_at"):
            continue
        fulfillment_status = str(order.get("fulfillment_status") or "unfulfilled").strip().lower()
        financial_status = str(order.get("financial_status") or "").strip().lower()
        if fulfillment_status == "fulfilled":
            continue
        if financial_status in {"refunded", "voided"}:
            continue
        line_items = []
        for line_item in order.get("line_items") or []:
            if not line_item.get("requires_shipping", True):
                continue
            fulfillable_quantity = line_item.get("fulfillable_quantity")
            if fulfillable_quantity is None:
                quantity = int(line_item.get("current_quantity") or line_item.get("quantity") or 0)
            else:
                quantity = int(fulfillable_quantity or 0)
            if quantity <= 0:
                continue
            title = _normalize_title(line_item.get("title"))
            line_items.append(
                {
                    "channel": "shopify",
                    "order_ref": str(order.get("name") or order.get("id") or ""),
                    "product_title": title,
                    "product_id": str(line_item.get("product_id") or ""),
                    "quantity": quantity,
                    "is_custom": _title_is_custom(title),
                    "variant_title": line_item.get("variant_title"),
                }
            )
        if not line_items:
            continue
        rows.append(
            {
                "channel": "shopify",
                "order_ref": str(order.get("name") or order.get("id") or ""),
                "buyer_name": _shopify_buyer_name(order),
                "created_at": order.get("created_at"),
                "expected_ship_date": None,
                "financial_status": order.get("financial_status"),
                "fulfillment_status": fulfillment_status or "unfulfilled",
                "line_items": line_items,
            }
        )

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "orders": len(rows),
            "units": sum(item["quantity"] for row in rows for item in row.get("line_items") or []),
        },
        "items": rows,
    }
    _write_json(SHOPIFY_OPEN_ORDERS_PATH, payload)
    return payload


def build_packing_summary(
    etsy_open_orders: dict[str, Any],
    shopify_open_orders: dict[str, Any],
) -> dict[str, Any]:
    non_custom: dict[str, dict[str, Any]] = {}
    custom_orders: list[dict[str, Any]] = []

    for source_payload in (etsy_open_orders, shopify_open_orders):
        for order in source_payload.get("items") or []:
            for line_item in order.get("line_items") or []:
                title = _normalize_title(line_item.get("product_title"))
                quantity = int(line_item.get("quantity") or 0)
                if not title or quantity <= 0:
                    continue
                if line_item.get("is_custom"):
                    custom_orders.append(
                        {
                            "channel": line_item.get("channel"),
                            "order_ref": line_item.get("order_ref"),
                            "product_title": title,
                            "quantity": quantity,
                            "product_id": line_item.get("product_id"),
                            "transaction_id": line_item.get("transaction_id"),
                            "buyer_name": order.get("buyer_name"),
                            "custom_type": line_item.get("custom_type"),
                            "personalization": line_item.get("personalization"),
                            "custom_design_summary": line_item.get("custom_design_summary"),
                            "variation_pairs": line_item.get("variation_pairs") or [],
                            "created_at": order.get("created_at"),
                        }
                    )
                    continue

                bucket = non_custom.setdefault(
                    title.lower(),
                    {
                        "product_title": title,
                        "product_id": line_item.get("product_id"),
                        "total_quantity": 0,
                        "by_channel": {"etsy": 0, "shopify": 0},
                        "by_channel_order_count": {"etsy": 0, "shopify": 0},
                        "order_refs": [],
                        "buyer_names": [],
                        "orders_with_unknown_buyer": 0,
                        "option_counts": {},
                        "oldest_created_at": None,
                        "earliest_expected_ship_date": None,
                    },
                )
                bucket["total_quantity"] += quantity
                channel = str(line_item.get("channel") or "")
                if channel in bucket["by_channel"]:
                    bucket["by_channel"][channel] += quantity
                if line_item.get("order_ref") and line_item.get("order_ref") not in bucket["order_refs"]:
                    bucket["order_refs"].append(line_item.get("order_ref"))
                    if channel in bucket["by_channel_order_count"]:
                        bucket["by_channel_order_count"][channel] += 1
                buyer_name = str(order.get("buyer_name") or "").strip()
                if buyer_name and buyer_name not in bucket["buyer_names"]:
                    bucket["buyer_names"].append(buyer_name)
                if not buyer_name:
                    bucket["orders_with_unknown_buyer"] += 1
                for option_label in _line_item_option_labels(line_item):
                    bucket["option_counts"][option_label] = int(bucket["option_counts"].get(option_label) or 0) + quantity
                bucket["oldest_created_at"] = _earlier_iso(bucket.get("oldest_created_at"), order.get("created_at"))
                bucket["earliest_expected_ship_date"] = _earlier_timestamp(
                    bucket.get("earliest_expected_ship_date"),
                    order.get("expected_ship_date"),
                )

    orders_to_pack = []
    for bucket in non_custom.values():
        bucket["order_count"] = len(bucket.get("order_refs") or [])
        bucket["buyer_count"] = len(bucket.get("buyer_names") or [])
        unknown_orders = int(bucket.get("orders_with_unknown_buyer") or 0)
        if unknown_orders <= 0:
            bucket["buyer_count_display"] = str(bucket["buyer_count"])
        elif bucket["buyer_count"] > 0:
            bucket["buyer_count_display"] = f"{bucket['buyer_count']}+ ({unknown_orders} hidden)"
        elif int((bucket.get("by_channel") or {}).get("shopify", 0) or 0) > 0 and int((bucket.get("by_channel") or {}).get("etsy", 0) or 0) == 0:
            bucket["buyer_count_display"] = "Hidden by Shopify"
        else:
            bucket["buyer_count_display"] = "Unknown"
        bucket["option_summary"] = _format_option_summary(bucket.get("option_counts") or {})
        bucket.update(
            _packing_urgency_fields(
                earliest_expected_ship_date=bucket.get("earliest_expected_ship_date"),
                oldest_created_at=bucket.get("oldest_created_at"),
            )
        )
        orders_to_pack.append(bucket)
    orders_to_pack.sort(key=_packing_urgency_rank)
    custom_orders.sort(key=lambda item: (str(item.get("channel") or ""), str(item.get("product_title") or "").lower()))

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "non_custom_titles": len(orders_to_pack),
            "non_custom_units": sum(int(item.get("total_quantity") or 0) for item in orders_to_pack),
            "custom_order_lines": len(custom_orders),
            "custom_units": sum(int(item.get("quantity") or 0) for item in custom_orders),
        },
        "orders_to_pack": orders_to_pack,
        "custom_orders_to_make": custom_orders,
    }
    _write_json(PACKING_SUMMARY_PATH, payload)
    return payload


def refresh_order_snapshots(
    *,
    etsy_timeout_seconds: int = ORDER_REFRESH_ETSY_TIMEOUT_SECONDS,
    shopify_timeout_seconds: int = ORDER_REFRESH_SHOPIFY_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    generated_at = datetime.now().astimezone().isoformat()
    source_results: dict[str, dict[str, Any]] = {}
    selected_payloads: dict[str, dict[str, Any]] = {}

    for source_name, command, cache_path, timeout_seconds in (
        ("etsy", "etsy-open-orders", ETSY_OPEN_ORDERS_PATH, etsy_timeout_seconds),
        ("shopify", "shopify-open-orders", SHOPIFY_OPEN_ORDERS_PATH, shopify_timeout_seconds),
    ):
        try:
            live_payload = _run_open_order_subcommand(command, timeout_seconds=timeout_seconds)
            result = _source_result_from_payload(
                source=source_name,
                payload=live_payload,
                status="live",
                cache_path=cache_path,
            )
            selected_payloads[source_name] = live_payload
        except subprocess.TimeoutExpired:
            cached_payload = _load_cached_snapshot(cache_path)
            if cached_payload:
                result = _source_result_from_payload(
                    source=source_name,
                    payload=cached_payload,
                    status="fallback_cached",
                    cache_path=cache_path,
                    error=f"{source_name} refresh timed out after {timeout_seconds}s",
                    timed_out=True,
                )
                selected_payloads[source_name] = cached_payload
            else:
                result = {
                    "source": source_name,
                    "status": "missing",
                    "error": f"{source_name} refresh timed out after {timeout_seconds}s and no cached snapshot was available",
                    "timed_out": True,
                    "generated_at": None,
                    "age_hours": None,
                    "path": str(cache_path),
                    "counts": {},
                }
                selected_payloads[source_name] = {"generated_at": generated_at, "items": [], "counts": {}}
        except Exception as exc:
            cached_payload = _load_cached_snapshot(cache_path)
            if cached_payload:
                result = _source_result_from_payload(
                    source=source_name,
                    payload=cached_payload,
                    status="fallback_cached",
                    cache_path=cache_path,
                    error=str(exc),
                )
                selected_payloads[source_name] = cached_payload
            else:
                result = {
                    "source": source_name,
                    "status": "missing",
                    "error": str(exc),
                    "timed_out": False,
                    "generated_at": None,
                    "age_hours": None,
                    "path": str(cache_path),
                    "counts": {},
                }
                selected_payloads[source_name] = {"generated_at": generated_at, "items": [], "counts": {}}

        source_results[source_name] = result
        _record_order_refresh_receipt(f"{source_name}_refresh", result)

    packing_summary = build_packing_summary(
        selected_payloads.get("etsy") or {"items": [], "counts": {}},
        selected_payloads.get("shopify") or {"items": [], "counts": {}},
    )

    all_live = all(result.get("status") == "live" for result in source_results.values())
    any_missing = any(result.get("status") == "missing" for result in source_results.values())
    any_fallback = any(result.get("status") == "fallback_cached" for result in source_results.values())

    if any_missing:
        state = "blocked"
        state_reason = "stale_input"
        next_action = "At least one order source could not refresh and had no cached snapshot. Fix the failing source before trusting this shopping list."
    elif any_fallback:
        state = "observed"
        state_reason = "stale_input"
        next_action = "The shopping list was rebuilt from the last good snapshot for at least one source. Retry the order refresh lane before relying on it as fully live."
    elif all_live:
        state = "verified"
        state_reason = "order_snapshots_fresh"
        next_action = "Use this refreshed packing list as the current shopping list."
    else:
        state = "observed"
        state_reason = "refresh_partial"
        next_action = "Review the source refresh receipts before treating this pack list as current."

    refresh_payload = {
        "generated_at": generated_at,
        "state": state,
        "state_reason": state_reason,
        "next_action": next_action,
        "sources": source_results,
        "packing_summary_generated_at": packing_summary.get("generated_at"),
        "counts": {
            "orders_to_pack_titles": int((packing_summary.get("counts") or {}).get("non_custom_titles") or 0),
            "orders_to_pack_units": int((packing_summary.get("counts") or {}).get("non_custom_units") or 0),
            "custom_order_lines": int((packing_summary.get("counts") or {}).get("custom_order_lines") or 0),
        },
    }
    packing_summary["snapshot_refresh"] = refresh_payload
    _write_json(PACKING_SUMMARY_PATH, packing_summary)
    _write_json(ORDER_SNAPSHOT_REFRESH_STATE_PATH, refresh_payload)

    control = record_workflow_transition(
        workflow_id=ORDER_SNAPSHOT_REFRESH_WORKFLOW_ID,
        lane=ORDER_SNAPSHOT_REFRESH_LANE,
        display_label=ORDER_SNAPSHOT_REFRESH_LABEL,
        entity_id="packing_list",
        state=state,
        state_reason=state_reason,
        requires_confirmation=False,
        input_freshness={
            "etsy_age_hours": source_results.get("etsy", {}).get("age_hours"),
            "shopify_age_hours": source_results.get("shopify", {}).get("age_hours"),
        },
        last_verification={
            "orders_to_pack_titles": refresh_payload["counts"]["orders_to_pack_titles"],
            "orders_to_pack_units": refresh_payload["counts"]["orders_to_pack_units"],
            "custom_order_lines": refresh_payload["counts"]["custom_order_lines"],
            "source_statuses": {
                name: details.get("status")
                for name, details in source_results.items()
            },
        },
        next_action=next_action,
        metadata={
            "generated_at": generated_at,
            "sources": source_results,
        },
        receipt_kind="refresh_summary",
        receipt_payload=refresh_payload,
        history_summary="order snapshots refreshed" if all_live else "order refresh used fallback data",
    )
    refresh_payload["workflow_control"] = {
        "state": control.get("state"),
        "state_reason": control.get("state_reason"),
        "updated_at": control.get("updated_at"),
        "next_action": control.get("next_action"),
    }
    _write_json(ORDER_SNAPSHOT_REFRESH_STATE_PATH, refresh_payload)
    return {
        "generated_at": generated_at,
        "etsy_open_orders": selected_payloads.get("etsy") or {},
        "shopify_open_orders": selected_payloads.get("shopify") or {},
        "packing_summary": packing_summary,
        "refresh_state": refresh_payload,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Open-order and packing snapshot helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("etsy-open-orders", help="Build the Etsy open-orders snapshot and print JSON.")
    subparsers.add_parser("shopify-open-orders", help="Build the Shopify open-orders snapshot and print JSON.")
    refresh_parser = subparsers.add_parser("refresh", help="Refresh order snapshots with stale fallback and print JSON.")
    refresh_parser.add_argument("--etsy-timeout", type=int, default=ORDER_REFRESH_ETSY_TIMEOUT_SECONDS)
    refresh_parser.add_argument("--shopify-timeout", type=int, default=ORDER_REFRESH_SHOPIFY_TIMEOUT_SECONDS)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "etsy-open-orders":
        payload = build_etsy_open_orders_snapshot()
    elif args.command == "shopify-open-orders":
        payload = build_shopify_open_orders_snapshot()
    elif args.command == "refresh":
        payload = refresh_order_snapshots(
            etsy_timeout_seconds=int(args.etsy_timeout),
            shopify_timeout_seconds=int(args.shopify_timeout),
        )
    else:
        raise SystemExit(f"Unknown command: {args.command}")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
