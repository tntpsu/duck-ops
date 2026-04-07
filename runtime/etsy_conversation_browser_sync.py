#!/usr/bin/env python3
"""
Stage browser-review records for Etsy conversation threads.

This module does not automate Etsy inbox browsing yet. It creates one record per
conversation thread so Duck Ops can track:

- which Etsy thread needs browser review
- who the customer is
- what order it appears tied to
- which URLs are worth opening first
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


INBOX_URLS = [
    "https://www.etsy.com/your/messages",
    "https://www.etsy.com/your/account/messages",
]


def _slugify(value: str) -> str:
    lowered = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    while "--" in lowered:
        lowered = lowered.replace("--", "-")
    return lowered.strip("-") or "unknown"


def _customer_packet_index(customer_packets: dict[str, Any] | list[dict[str, Any]] | None) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    payload_items = customer_packets
    if isinstance(customer_packets, dict):
        payload_items = customer_packets.get("items") or []
    items = list(payload_items or [])
    by_source_artifact_id: dict[str, dict[str, Any]] = {}
    by_thread_key: dict[str, dict[str, Any]] = {}
    for item in items:
        source_artifact_id = str(item.get("source_artifact_id") or "").strip()
        if source_artifact_id and source_artifact_id not in by_source_artifact_id:
            by_source_artifact_id[source_artifact_id] = item
        thread_key = str(item.get("conversation_thread_key") or "").strip()
        if thread_key and thread_key not in by_thread_key:
            by_thread_key[thread_key] = item
    return by_source_artifact_id, by_thread_key


def build_etsy_conversation_browser_sync(
    queue_items: list[dict[str, Any]],
    customer_packets: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    customer_by_source, customer_by_thread = _customer_packet_index(customer_packets)
    items: list[dict[str, Any]] = []
    for queue_item in queue_items:
        if queue_item.get("item_type") != "customer_case":
            continue
        details = queue_item.get("details") or {}
        if str(details.get("channel") or "").strip() != "mailbox_email":
            continue
        thread_key = str(details.get("conversation_thread_key") or "").strip()
        if not thread_key:
            continue
        order = details.get("order_enrichment") or {}
        browser_urls: list[str] = []
        direct_browser_url = None
        for url in details.get("browser_url_candidates") or []:
            normalized = str(url).strip()
            if not normalized:
                continue
            if normalized in INBOX_URLS:
                continue
            if direct_browser_url is None:
                direct_browser_url = normalized
        if direct_browser_url:
            browser_urls.append(direct_browser_url)
        browser_urls.extend(INBOX_URLS)
        linked_customer = (
            customer_by_source.get(str(queue_item.get("source_artifact_id") or "").strip())
            or customer_by_thread.get(thread_key)
            or {}
        )

        contact = str(details.get("conversation_contact") or "").strip() or str(order.get("buyer_name") or "").strip() or "Customer"
        product_title = str(order.get("product_title") or "").strip()
        open_in_browser_hint = product_title or str(queue_item.get("summary") or "").strip() or "Open the Etsy thread and read the latest message."
        linked_short_id = str(linked_customer.get("short_id") or "").strip() or None
        items.append(
            {
                "artifact_id": f"etsy_conversation_thread::{_slugify(thread_key)}",
                "artifact_type": "etsy_conversation_thread",
                "conversation_thread_key": thread_key,
                "conversation_contact": contact,
                "grouped_message_count": int(details.get("grouped_message_count") or 1),
                "browser_review_status": "needs_browser_review",
                "latest_message_preview": details.get("latest_message_preview") or queue_item.get("summary"),
                "browser_url_candidates": browser_urls,
                "primary_browser_url": direct_browser_url or INBOX_URLS[0],
                "open_in_browser_hint": open_in_browser_hint,
                "linked_customer_short_id": linked_short_id,
                "linked_customer_status": linked_customer.get("status"),
                "open_command": f"customer open {linked_short_id}" if linked_short_id else None,
                "order_enrichment": order,
                "source_artifact_id": queue_item.get("source_artifact_id"),
                "source_artifact_ids": queue_item.get("source_artifact_ids") or [queue_item.get("source_artifact_id")],
                "source_refs": queue_item.get("source_refs") or [],
            }
        )

    items.sort(
        key=lambda item: (
            str(item.get("conversation_contact") or "").lower(),
            str((item.get("order_enrichment") or {}).get("receipt_id") or "").lower(),
        ),
    )
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "threads": len(items),
            "needs_browser_review": len(items),
        },
        "items": items,
    }


def render_etsy_conversation_browser_sync_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Etsy Conversation Browser Review",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Threads needing browser review: `{payload.get('counts', {}).get('needs_browser_review', 0)}`",
        "",
    ]
    items = payload.get("items") or []
    if not items:
        lines.append("No Etsy conversation browser-review items right now.")
        lines.append("")
        return "\n".join(lines)

    for index, item in enumerate(items, start=1):
        order = item.get("order_enrichment") or {}
        lines.extend(
            [
                f"## {index}. {item.get('conversation_contact')}",
                "",
                f"- Browser review status: `{item.get('browser_review_status')}`",
                f"- Messages in thread: `{item.get('grouped_message_count')}`",
                f"- Latest preview: {item.get('latest_message_preview') or '(none)'}",
                f"- Order: `{order.get('receipt_id') or 'n/a'}` / `{order.get('product_title') or 'unknown product'}`",
                f"- Open hint: {item.get('open_in_browser_hint')}",
            ]
        )
        if item.get("linked_customer_short_id"):
            lines.append(
                f"- Customer lane command: `customer open {item.get('linked_customer_short_id')}`"
            )
        primary_url = str(item.get("primary_browser_url") or "").strip()
        if primary_url:
            lines.append(f"- Open now: {primary_url}")
        fallback_urls = [url for url in (item.get("browser_url_candidates") or []) if str(url).strip() and str(url).strip() != primary_url]
        if fallback_urls:
            lines.append("- Fallback browser URLs:")
            for url in fallback_urls[:2]:
                lines.append(f"  - {url}")
        lines.append("")
    return "\n".join(lines)
