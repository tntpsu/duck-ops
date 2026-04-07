#!/usr/bin/env python3
"""
Open-order and packing snapshots for Duck Ops.
"""

from __future__ import annotations

import json
import os
import subprocess
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from customer_case_enrichment import load_recent_etsy_receipts_snapshot


ROOT = Path(__file__).resolve().parents[1]
NORMALIZED_DIR = ROOT / "state" / "normalized"
DUCK_AGENT_ROOT = Path("/Users/philtullai/ai-agents/duckAgent")

ETSY_OPEN_ORDERS_PATH = NORMALIZED_DIR / "etsy_open_orders_snapshot.json"
ETSY_TRANSACTIONS_SNAPSHOT_PATH = NORMALIZED_DIR / "etsy_transactions_snapshot.json"
SHOPIFY_OPEN_ORDERS_PATH = NORMALIZED_DIR / "shopify_open_orders_snapshot.json"
PACKING_SUMMARY_PATH = NORMALIZED_DIR / "packing_summary.json"

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


def _title_is_custom(title: str | None) -> bool:
    lowered = str(title or "").lower()
    return any(keyword in lowered for keyword in CUSTOM_KEYWORDS)


def _normalize_title(title: str | None) -> str:
    return " ".join(str(title or "").split()).strip()


def _ts_to_iso(value: Any) -> str | None:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone().isoformat()


def _run_duckagent_etsy_transaction_lookup(transaction_ids: list[str], max_pages: int = 20) -> dict[str, Any] | None:
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
        rows = []
        found = set()
        limit = 100
        offset = 0
        for _ in range({max_pages}):
            response = etsy_get_shop_transactions(shop_id, limit=limit, offset=offset)
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
        print(json.dumps({{"generated_at": datetime.now().astimezone().isoformat(), "items": rows, "requested": len(wanted_ids), "found": len(found)}}))
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


def load_etsy_transaction_details(transaction_ids: list[str]) -> dict[str, Any]:
    fresh = _run_duckagent_etsy_transaction_lookup(transaction_ids)
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


def build_etsy_open_orders_snapshot() -> dict[str, Any]:
    receipts_payload = load_recent_etsy_receipts_snapshot(days_back=365, max_age_hours=0)
    wanted_transaction_ids = [
        str(transaction.get("transaction_id") or "").strip()
        for receipt in (receipts_payload.get("items") or [])
        if receipt.get("is_paid") and not receipt.get("is_shipped")
        for transaction in (receipt.get("transactions") or [])
        if str(transaction.get("transaction_id") or "").strip()
    ]
    transaction_payload = load_etsy_transaction_details(wanted_transaction_ids)
    transaction_index = _build_transaction_index(transaction_payload)
    rows: list[dict[str, Any]] = []
    for receipt in receipts_payload.get("items") or []:
        if not receipt.get("is_paid") or receipt.get("is_shipped"):
            continue
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
            "fields": "id,name,created_at,financial_status,fulfillment_status,cancelled_at,line_items",
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
        line_items = []
        for line_item in order.get("line_items") or []:
            if not line_item.get("requires_shipping", True):
                continue
            quantity = int(line_item.get("fulfillable_quantity") or line_item.get("current_quantity") or line_item.get("quantity") or 0)
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
                "buyer_name": None,
                "created_at": order.get("created_at"),
                "financial_status": order.get("financial_status"),
                "fulfillment_status": order.get("fulfillment_status") or "unfulfilled",
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
                        "order_refs": [],
                    },
                )
                bucket["total_quantity"] += quantity
                channel = str(line_item.get("channel") or "")
                if channel in bucket["by_channel"]:
                    bucket["by_channel"][channel] += quantity
                if line_item.get("order_ref") and line_item.get("order_ref") not in bucket["order_refs"]:
                    bucket["order_refs"].append(line_item.get("order_ref"))

    orders_to_pack = sorted(
        non_custom.values(),
        key=lambda item: (-int(item.get("total_quantity") or 0), str(item.get("product_title") or "").lower()),
    )
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
