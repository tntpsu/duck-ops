#!/usr/bin/env python3
"""
Staged Etsy order and tracking enrichment for Duck Ops customer cases.

This module keeps the first implementation intentionally conservative:

- prefer local mailbox order evidence first
- use a cached Etsy receipt snapshot when credentials are available
- expose order and tracking context as staged enrichment only
- do not trigger customer-facing or shipping-side actions
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import re
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
NORMALIZED_DIR = STATE_DIR / "normalized"
DUCKAGENT_ROOT = Path("/Users/philtullai/ai-agents/duckAgent")
DUCKAGENT_VENV_PYTHON = DUCKAGENT_ROOT / ".venv" / "bin" / "python"

ETSY_ORDER_EMAIL_INDEX_PATH = NORMALIZED_DIR / "etsy_order_email_index.json"
ETSY_RECEIPTS_SNAPSHOT_PATH = NORMALIZED_DIR / "etsy_receipts_snapshot.json"
REVIEW_REPLY_POST_INDEX_PATH = NORMALIZED_DIR / "review_reply_post_index.json"
REVIEW_REPLY_EXECUTION_SESSIONS_PATH = STATE_DIR / "review_reply_execution_sessions.json"

ETSY_TX_FROM_PATTERN = re.compile(r"etsy transactions <transaction@etsy\.com>", re.IGNORECASE)
ORDER_NUMBER_PATTERN = re.compile(r"order\s*#(?P<order>\d+)", re.IGNORECASE)
TRANSACTION_ID_PATTERN = re.compile(r"transaction id:\s*(?P<tx>\d+)", re.IGNORECASE)
BUYER_PATTERN = re.compile(r"buyer:\s*(?P<buyer>[^\r\n]+)", re.IGNORECASE)
ITEM_PATTERN = re.compile(
    r"item:\s*(?P<item>.+?)(?:quantity:|shirt color:|personalization:|applied discounts|shipping:)",
    re.IGNORECASE | re.DOTALL,
)
SHIP_BY_PATTERN = re.compile(r"ship by (?P<ship_by>[A-Za-z]{3}\s+\d{1,2})", re.IGNORECASE)
TRACKING_CODE_PATTERN = re.compile(r"\b((?:94|93|92|95)\d{18,26})\b")


def _trim_text(value: str | None, limit: int = 240) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _parse_etsy_order_email(email_item: dict[str, Any]) -> dict[str, Any] | None:
    sender = str(email_item.get("from") or "").strip()
    if not ETSY_TX_FROM_PATTERN.search(sender):
        return None
    subject = str(email_item.get("subject") or "").strip()
    body_text = str(email_item.get("body_text") or "")
    combined = f"{subject}\n{body_text}"

    order_match = ORDER_NUMBER_PATTERN.search(combined)
    tx_match = TRANSACTION_ID_PATTERN.search(combined)
    if not order_match and not tx_match:
        return None

    buyer_match = BUYER_PATTERN.search(body_text)
    item_match = ITEM_PATTERN.search(body_text)
    ship_by_match = SHIP_BY_PATTERN.search(subject)

    item_title = ""
    if item_match:
        item_title = re.sub(r"\s+", " ", item_match.group("item")).strip()

    return {
        "source_uid": email_item.get("uid"),
        "message_id": email_item.get("message_id"),
        "registry_key": email_item.get("registry_key"),
        "order_number": order_match.group("order") if order_match else None,
        "transaction_id": tx_match.group("tx") if tx_match else None,
        "buyer_name": buyer_match.group("buyer").strip() if buyer_match else None,
        "item_title": item_title or None,
        "ship_by_label": ship_by_match.group("ship_by") if ship_by_match else None,
        "subject": subject,
    }


def build_etsy_order_email_index(mailbox_items: list[dict[str, Any]]) -> dict[str, Any]:
    by_transaction_id: dict[str, dict[str, Any]] = {}
    by_order_number: dict[str, dict[str, Any]] = {}
    items: list[dict[str, Any]] = []
    for email_item in mailbox_items:
        parsed = _parse_etsy_order_email(email_item)
        if not parsed:
            continue
        items.append(parsed)
        tx = str(parsed.get("transaction_id") or "").strip()
        order_number = str(parsed.get("order_number") or "").strip()
        if tx:
            by_transaction_id[tx] = parsed
        if order_number:
            by_order_number[order_number] = parsed
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "items": len(items),
            "by_transaction_id": len(by_transaction_id),
            "by_order_number": len(by_order_number),
        },
        "items": items,
        "by_transaction_id": by_transaction_id,
        "by_order_number": by_order_number,
    }
    ETSY_ORDER_EMAIL_INDEX_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def _run_duckagent_etsy_receipt_snapshot(days_back: int) -> dict[str, Any] | None:
    if not DUCKAGENT_VENV_PYTHON.exists():
        return None
    script = f"""
import json
import sys
from datetime import datetime, timedelta
sys.path.insert(0, {str(DUCKAGENT_ROOT)!r})
from helpers.etsy_helper import etsy_get_all_shop_receipts
import os

shop_id = os.getenv("ETSY_SHOP_ID")
end_date = datetime.now()
start_date = end_date - timedelta(days={days_back})
receipts = etsy_get_all_shop_receipts(shop_id, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
rows = []
for receipt in receipts:
    rows.append({{
        "receipt_id": str(receipt.get("receipt_id") or ""),
        "buyer_name": receipt.get("name"),
        "status": receipt.get("status"),
        "is_paid": bool(receipt.get("is_paid")),
        "is_shipped": bool(receipt.get("is_shipped")),
        "created_timestamp": receipt.get("created_timestamp") or receipt.get("create_timestamp"),
        "updated_timestamp": receipt.get("updated_timestamp") or receipt.get("update_timestamp"),
        "shipments": receipt.get("shipments") or [],
        "transactions": [
            {{
                "transaction_id": str(tx.get("transaction_id") or ""),
                "listing_id": tx.get("listing_id"),
                "title": tx.get("title"),
                "receipt_id": str(tx.get("receipt_id") or ""),
                "expected_ship_date": tx.get("expected_ship_date"),
                "shipped_timestamp": tx.get("shipped_timestamp"),
                "quantity": tx.get("quantity"),
            }}
            for tx in (receipt.get("transactions") or [])
        ],
    }})
print("__DUCKOPS_JSON_START__")
print(json.dumps({{"generated_at": datetime.now().astimezone().isoformat(), "days_back": {days_back}, "items": rows}}))
"""
    try:
        result = subprocess.run(
            [str(DUCKAGENT_VENV_PYTHON), "-c", script],
            cwd=str(DUCKAGENT_ROOT),
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    marker = "__DUCKOPS_JSON_START__"
    if marker not in result.stdout:
        return None
    raw = result.stdout.split(marker, 1)[1].strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def load_recent_etsy_receipts_snapshot(days_back: int = 30, max_age_hours: int = 6) -> dict[str, Any]:
    if ETSY_RECEIPTS_SNAPSHOT_PATH.exists():
        try:
            cached = json.loads(ETSY_RECEIPTS_SNAPSHOT_PATH.read_text())
            generated_at = datetime.fromisoformat(str(cached.get("generated_at")))
            age = datetime.now(generated_at.tzinfo) - generated_at
            if age <= timedelta(hours=max_age_hours):
                return cached
        except Exception:
            pass
    fresh = _run_duckagent_etsy_receipt_snapshot(days_back=days_back)
    if fresh:
        ETSY_RECEIPTS_SNAPSHOT_PATH.write_text(json.dumps(fresh, indent=2), encoding="utf-8")
        return fresh
    if ETSY_RECEIPTS_SNAPSHOT_PATH.exists():
        try:
            return json.loads(ETSY_RECEIPTS_SNAPSHOT_PATH.read_text())
        except Exception:
            pass
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "days_back": days_back,
        "items": [],
        "error": "etsy_receipts_unavailable",
    }


def _build_receipt_indexes(receipt_snapshot: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    by_receipt_id: dict[str, Any] = {}
    by_transaction_id: dict[str, Any] = {}
    for receipt in receipt_snapshot.get("items") or []:
        receipt_id = str(receipt.get("receipt_id") or "").strip()
        if receipt_id:
            by_receipt_id[receipt_id] = receipt
        for transaction in receipt.get("transactions") or []:
            tx_id = str(transaction.get("transaction_id") or "").strip()
            if tx_id:
                by_transaction_id[tx_id] = {"receipt": receipt, "transaction": transaction}
    return by_receipt_id, by_transaction_id


def load_review_reply_post_index() -> dict[str, Any]:
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {"posted_items": 0, "by_transaction_id": 0},
        "items": [],
        "by_transaction_id": {},
    }
    if not REVIEW_REPLY_EXECUTION_SESSIONS_PATH.exists():
        REVIEW_REPLY_POST_INDEX_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    try:
        state = json.loads(REVIEW_REPLY_EXECUTION_SESSIONS_PATH.read_text())
    except Exception:
        REVIEW_REPLY_POST_INDEX_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    by_transaction_id: dict[str, dict[str, Any]] = {}
    items: list[dict[str, Any]] = []
    for session in (state.get("sessions") or {}).values():
        for item in (session.get("items") or {}).values():
            if str(item.get("status") or "").strip() != "posted":
                continue
            tx_id = str(item.get("transaction_id") or "").strip()
            if not tx_id:
                continue
            row = {
                "transaction_id": tx_id,
                "artifact_id": item.get("artifact_id"),
                "updated_at": item.get("updated_at"),
                "attempt_outcome": item.get("attempt_outcome"),
            }
            items.append(row)
            previous = by_transaction_id.get(tx_id)
            if previous is None or str(row.get("updated_at") or "") >= str(previous.get("updated_at") or ""):
                by_transaction_id[tx_id] = row

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "posted_items": len(items),
            "by_transaction_id": len(by_transaction_id),
        },
        "items": items,
        "by_transaction_id": by_transaction_id,
    }
    REVIEW_REPLY_POST_INDEX_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def _resolution_enrichment_from_receipt(
    receipt: dict[str, Any] | None,
    transaction: dict[str, Any] | None,
    review_reply_posts: dict[str, Any],
) -> dict[str, Any]:
    signals: list[str] = []
    summary_parts: list[str] = []
    order_status = str((receipt or {}).get("status") or "").strip()
    shipment_count = len((receipt or {}).get("shipments") or [])
    tx_id = str((transaction or {}).get("transaction_id") or "").strip()

    refund_detected = "refund" in order_status.lower()
    if refund_detected:
        signals.append("refund_detected")
        summary_parts.append(f"Etsy receipt status is `{order_status}`.")

    if shipment_count > 1:
        signals.append("multiple_shipments_present")
        summary_parts.append(f"Etsy receipt has `{shipment_count}` shipments recorded.")

    reply_record = ((review_reply_posts.get("by_transaction_id") or {}).get(tx_id) or {}) if tx_id else {}
    if reply_record:
        signals.append("public_review_reply_posted")
        summary_parts.append(f"A public Etsy review reply was already posted at `{reply_record.get('updated_at')}`.")

    status = "no_resolution_history"
    if signals:
        status = "history_present"

    return {
        "status": status,
        "signals": signals,
        "refund_detected": refund_detected,
        "shipment_count": shipment_count,
        "public_review_reply_posted": bool(reply_record),
        "review_reply_artifact_id": reply_record.get("artifact_id"),
        "summary": " ".join(summary_parts).strip() or "No prior customer-resolution signals found.",
    }


def _extract_order_number_from_case(case: dict[str, Any], mailbox_by_uid: dict[int, dict[str, Any]]) -> str | None:
    for ref in case.get("source_refs") or []:
        uid = ref.get("uid")
        try:
            uid_int = int(uid)
        except (TypeError, ValueError):
            continue
        email_item = mailbox_by_uid.get(uid_int) or {}
        combined = f"{email_item.get('subject') or ''}\n{email_item.get('body_text') or ''}"
        match = ORDER_NUMBER_PATTERN.search(combined)
        if match:
            return match.group("order")
    return None


def _tracking_enrichment_from_receipt(receipt: dict[str, Any], transaction: dict[str, Any] | None = None) -> dict[str, Any]:
    shipments = receipt.get("shipments") or []
    tracking_number = None
    tracking_url = None
    carrier = None
    for shipment in shipments:
        tracking_number = (
            shipment.get("tracking_code")
            or shipment.get("tracking_number")
            or tracking_number
        )
        tracking_url = shipment.get("tracking_url") or tracking_url
        carrier = shipment.get("carrier_name") or shipment.get("carrier") or carrier
        if tracking_number:
            break

    expected_ship_date = (transaction or {}).get("expected_ship_date")
    if tracking_number:
        return {
            "status": "tracking_available",
            "carrier": carrier or "unknown",
            "tracking_number": str(tracking_number),
            "tracking_url": tracking_url,
            "source": "etsy_receipt_snapshot",
        }
    if receipt.get("is_shipped"):
        return {
            "status": "shipped_without_tracking_details",
            "carrier": carrier,
            "tracking_number": None,
            "tracking_url": tracking_url,
            "source": "etsy_receipt_snapshot",
        }
    if expected_ship_date:
        return {
            "status": "awaiting_shipment",
            "carrier": None,
            "tracking_number": None,
            "tracking_url": None,
            "expected_ship_date": expected_ship_date,
            "source": "etsy_receipt_snapshot",
        }
    return {
        "status": "no_tracking_evidence",
        "carrier": None,
        "tracking_number": None,
        "tracking_url": None,
        "source": "none",
    }


def enrich_customer_cases(customer_cases: list[dict[str, Any]], mailbox_items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    order_email_index = build_etsy_order_email_index(mailbox_items)
    receipt_snapshot = load_recent_etsy_receipts_snapshot()
    review_reply_posts = load_review_reply_post_index()
    receipts_by_id, receipts_by_transaction_id = _build_receipt_indexes(receipt_snapshot)
    mailbox_by_uid = {int(item["uid"]): item for item in mailbox_items if item.get("uid") is not None}

    enriched_cases: list[dict[str, Any]] = []
    email_hits = 0
    api_hits = 0
    resolution_history_hits = 0
    refund_history_hits = 0
    possible_reship_history_hits = 0
    public_reply_history_hits = 0

    for case in customer_cases:
        enriched = dict(case)
        source_modes: list[str] = []
        email_match = None
        receipt_match = None
        transaction_match = None

        transaction_id = str(((case.get("business_context") or {}).get("order_id") or "")).strip()
        order_number = _extract_order_number_from_case(case, mailbox_by_uid)

        if transaction_id:
            email_match = (order_email_index.get("by_transaction_id") or {}).get(transaction_id)
            receipt_match = receipts_by_transaction_id.get(transaction_id)
        if not receipt_match and order_number:
            receipt = receipts_by_id.get(order_number)
            if receipt:
                receipt_match = {"receipt": receipt, "transaction": None}
            if not email_match:
                email_match = (order_email_index.get("by_order_number") or {}).get(order_number)

        if email_match:
            source_modes.append("etsy_order_email")
            email_hits += 1
        if receipt_match:
            source_modes.append("etsy_receipt_api")
            api_hits += 1
            transaction_match = receipt_match.get("transaction")
            receipt = receipt_match.get("receipt") or {}
            if transaction_match is None:
                transactions = receipt.get("transactions") or []
                if len(transactions) == 1:
                    transaction_match = transactions[0]
        else:
            receipt = {}

        product_title = (
            ((case.get("business_context") or {}).get("product_title"))
            or (transaction_match or {}).get("title")
            or (email_match or {}).get("item_title")
        )
        buyer_name = (receipt or {}).get("buyer_name") or (email_match or {}).get("buyer_name")
        receipt_id = (receipt or {}).get("receipt_id") or (email_match or {}).get("order_number") or order_number
        ship_by_label = (email_match or {}).get("ship_by_label")
        tracking_enrichment = _tracking_enrichment_from_receipt(receipt, transaction_match)
        resolution_enrichment = _resolution_enrichment_from_receipt(
            receipt,
            transaction_match
            or {
                "transaction_id": transaction_id or (email_match or {}).get("transaction_id"),
            },
            review_reply_posts,
        )

        if tracking_enrichment.get("status") == "no_tracking_evidence":
            tracking_match = TRACKING_CODE_PATTERN.search(str(case.get("customer_summary") or ""))
            if tracking_match:
                tracking_enrichment = {
                    "status": "tracking_number_in_message",
                    "carrier": "unknown",
                    "tracking_number": tracking_match.group(1),
                    "tracking_url": None,
                    "source": "message_text",
                }

        enriched["order_enrichment"] = {
            "matched": bool(email_match or receipt_match),
            "source_modes": source_modes,
            "transaction_id": transaction_id or (email_match or {}).get("transaction_id"),
            "receipt_id": str(receipt_id or "") or None,
            "buyer_name": buyer_name,
            "product_title": product_title,
            "order_status": (receipt or {}).get("status"),
            "is_paid": (receipt or {}).get("is_paid"),
            "is_shipped": (receipt or {}).get("is_shipped"),
            "ship_by_label": ship_by_label,
            "expected_ship_date": (transaction_match or {}).get("expected_ship_date"),
            "email_registry_key": (email_match or {}).get("registry_key"),
        }
        enriched["tracking_enrichment"] = tracking_enrichment
        enriched["resolution_enrichment"] = resolution_enrichment
        if product_title and not ((enriched.get("business_context") or {}).get("product_title")):
            enriched.setdefault("business_context", {})
            enriched["business_context"]["product_title"] = product_title
        if receipt_id and "order_id" in (enriched.get("missing_context") or []):
            enriched["missing_context"] = [item for item in (enriched.get("missing_context") or []) if item != "order_id"]
            if enriched.get("context_state") == "missing_order_context":
                enriched["context_state"] = "enough_context"

        if (resolution_enrichment.get("signals") or []):
            resolution_history_hits += 1
        if resolution_enrichment.get("refund_detected"):
            refund_history_hits += 1
        if "multiple_shipments_present" in (resolution_enrichment.get("signals") or []):
            possible_reship_history_hits += 1
        if resolution_enrichment.get("public_review_reply_posted"):
            public_reply_history_hits += 1
        enriched_cases.append(enriched)

    enrichment_summary = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "customer_cases": len(customer_cases),
            "email_index_items": (order_email_index.get("counts") or {}).get("items", 0),
            "receipt_snapshot_items": len(receipt_snapshot.get("items") or []),
            "email_hits": email_hits,
            "etsy_api_hits": api_hits,
            "resolution_history_hits": resolution_history_hits,
            "refund_history_hits": refund_history_hits,
            "possible_reship_history_hits": possible_reship_history_hits,
            "public_reply_history_hits": public_reply_history_hits,
        },
        "order_email_index_path": str(ETSY_ORDER_EMAIL_INDEX_PATH),
        "receipt_snapshot_path": str(ETSY_RECEIPTS_SNAPSHOT_PATH),
        "review_reply_post_index_path": str(REVIEW_REPLY_POST_INDEX_PATH),
    }
    return enriched_cases, enrichment_summary
