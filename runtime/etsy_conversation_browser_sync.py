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

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


INBOX_URLS = [
    "https://www.etsy.com/messages?ref=hdr_user_menu-messages",
]
ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
CUSTOMER_OPERATOR_STATE_PATH = STATE_DIR / "customer_operator_state.json"


def _slugify(value: str) -> str:
    lowered = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    while "--" in lowered:
        lowered = lowered.replace("--", "-")
    return lowered.strip("-") or "unknown"


def _canonical_etsy_message_url(value: str | None) -> str | None:
    url = str(value or "").strip()
    if not url:
        return None
    match = re.search(r"https://www\.etsy\.com/(?:your/account/)?messages/(?P<id>\d+)", url, re.IGNORECASE)
    if match:
        return f"https://www.etsy.com/messages/{match.group('id')}"
    lowered = url.lower().rstrip("/")
    if lowered in {
        "https://www.etsy.com/messages",
        "https://www.etsy.com/your/messages",
        "https://www.etsy.com/your/account/messages",
    }:
        return INBOX_URLS[0]
    return url


def _contact_from_source_refs(source_refs: list[dict[str, Any]] | None) -> str | None:
    for ref in source_refs or []:
        subject = str((ref or {}).get("subject") or "").strip()
        if not subject:
            continue
        match = re.search(r"etsy conversation with\s+(?P<name>.+)$", subject, re.IGNORECASE)
        if match:
            name = re.sub(r"\s+from\s+.+$", "", match.group("name").strip(), flags=re.IGNORECASE).strip()
            if name:
                return name
        match = re.search(r"^(?P<name>.+?)\s+needs help with an order they placed$", subject, re.IGNORECASE)
        if match:
            return match.group("name").strip()
    return None


def _load_customer_operator_short_ids() -> dict[str, str]:
    if not CUSTOMER_OPERATOR_STATE_PATH.exists():
        return {}
    try:
        payload = json.loads(CUSTOMER_OPERATOR_STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    mapping = payload.get("packet_short_ids") or {}
    short_ids: dict[str, str] = {}
    for packet_id, numeric_id in mapping.items():
        key = str(packet_id or "").strip()
        if not key:
            continue
        try:
            short_ids[key] = f"C{int(numeric_id)}"
        except (TypeError, ValueError):
            continue
    return short_ids


def _decorate_customer_packets_with_short_ids(
    customer_packets: dict[str, Any] | list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    payload_items = customer_packets
    if isinstance(customer_packets, dict):
        payload_items = customer_packets.get("items") or []
    items = list(payload_items or [])
    short_id_map = _load_customer_operator_short_ids()
    enriched: list[dict[str, Any]] = []
    for item in items:
        row = dict(item)
        packet_id = str(row.get("packet_id") or "").strip()
        if not str(row.get("short_id") or "").strip() and packet_id and packet_id in short_id_map:
            row["short_id"] = short_id_map[packet_id]
        enriched.append(row)
    return enriched


def _customer_packet_index(customer_packets: dict[str, Any] | list[dict[str, Any]] | None) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    items = _decorate_customer_packets_with_short_ids(customer_packets)
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


def _custom_build_index(custom_build_candidates: dict[str, Any] | list[dict[str, Any]] | None) -> dict[str, list[dict[str, Any]]]:
    payload_items = custom_build_candidates
    if isinstance(custom_build_candidates, dict):
        payload_items = custom_build_candidates.get("items") or []
    items = list(payload_items or [])
    by_order_ref: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        order_ref = str(item.get("order_ref") or "").strip()
        if not order_ref:
            continue
        by_order_ref.setdefault(order_ref, []).append(item)
    return by_order_ref


def _browser_capture_index(browser_captures: dict[str, Any] | list[dict[str, Any]] | None) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    payload_items = browser_captures
    if isinstance(browser_captures, dict):
        payload_items = browser_captures.get("items") or []
    items = list(payload_items or [])
    by_thread_key: dict[str, dict[str, Any]] = {}
    by_source_artifact_id: dict[str, dict[str, Any]] = {}
    for item in items:
        thread_key = str(item.get("conversation_thread_key") or "").strip()
        source_artifact_id = str(item.get("source_artifact_id") or "").strip()
        if thread_key and thread_key not in by_thread_key:
            by_thread_key[thread_key] = item
        if source_artifact_id and source_artifact_id not in by_source_artifact_id:
            by_source_artifact_id[source_artifact_id] = item
    return by_thread_key, by_source_artifact_id


def _capture_status(capture: dict[str, Any] | None) -> tuple[str, str | None]:
    if not capture:
        return "needs_browser_review", None
    follow_up = str(capture.get("follow_up_state") or "").strip().lower().replace(" ", "_") or None
    status = str(capture.get("browser_review_status") or "").strip()
    draft_reply = str(capture.get("draft_reply") or "").strip()
    reply_needed = capture.get("reply_needed")
    open_loop_owner = str(capture.get("open_loop_owner") or "").strip().lower().replace(" ", "_") or None
    if follow_up in {"resolved", "done"}:
        follow_up = "resolved"
    elif follow_up == "needs_reply" and draft_reply and reply_needed is not False:
        follow_up = "reply_drafted"
    elif draft_reply and not follow_up and reply_needed is not False:
        follow_up = "reply_drafted"
    if reply_needed is False:
        if follow_up in {"waiting_on_customer", "waiting_on_operator", "ready_for_task", "concept_in_progress"}:
            return "captured", follow_up
        if open_loop_owner == "customer":
            return "captured", "waiting_on_customer"
        if open_loop_owner == "operator":
            return "captured", "waiting_on_operator"
        if open_loop_owner == "closed":
            return "resolved", "resolved"
    if status:
        return status, follow_up
    if follow_up == "resolved":
        return "resolved", follow_up
    if follow_up in {"ready_for_task", "needs_reply", "reply_drafted", "waiting_on_customer", "waiting_on_operator", "concept_in_progress"}:
        return "captured", follow_up
    return "captured", follow_up


def _browser_next_action(*, status: str, follow_up: str | None, linked_builds: list[dict[str, Any]], capture: dict[str, Any], open_hint: str) -> str:
    draft_reply = str(capture.get("draft_reply") or "").strip()
    google_task_created = any(str(item.get("google_task_status") or "") == "created" for item in linked_builds)
    if status == "needs_browser_review":
        return open_hint
    if follow_up == "resolved":
        return "No action tonight unless the Etsy thread reopens."
    if follow_up == "waiting_on_customer":
        return "Waiting on the customer. No reply needed tonight unless they sent something new."
    if follow_up == "concept_in_progress":
        return "Concept work is already moving. Keep the task updated and send the next concept when ready."
    if follow_up == "ready_for_task":
        if google_task_created:
            return "Open the Google Task and start concept work from the captured Etsy brief."
        return "Create or update the Google Task, then start concept work from the captured brief."
    if follow_up == "needs_reply":
        if draft_reply:
            return "Reply on Etsy using the staged draft, then capture the next thread state."
        return "Reply on Etsy, then capture the updated brief or customer answer."
    return str(capture.get("recommended_action") or "").strip() or open_hint


def _custom_build_summary(linked_builds: list[dict[str, Any]]) -> tuple[list[str], str | None]:
    if not linked_builds:
        return [], None
    summaries: list[str] = []
    for item in linked_builds[:3]:
        summaries.append(str(item.get("custom_design_summary") or item.get("product_title") or "Custom build").strip())
    next_action = str((linked_builds[0] or {}).get("next_design_action") or "").strip() or None
    return summaries, next_action


def build_etsy_conversation_browser_sync(
    queue_items: list[dict[str, Any]],
    customer_packets: dict[str, Any] | list[dict[str, Any]] | None = None,
    custom_build_candidates: dict[str, Any] | list[dict[str, Any]] | None = None,
    browser_captures: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    customer_by_source, customer_by_thread = _customer_packet_index(customer_packets)
    custom_builds_by_order = _custom_build_index(custom_build_candidates)
    captures_by_thread, captures_by_source = _browser_capture_index(browser_captures)
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
            normalized = _canonical_etsy_message_url(url) or ""
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
        capture = (
            captures_by_thread.get(thread_key)
            or captures_by_source.get(str(queue_item.get("source_artifact_id") or "").strip())
            or {}
        )
        order_ref = str(order.get("receipt_id") or "").strip()
        linked_builds = list(custom_builds_by_order.get(order_ref) or [])
        linked_build_summaries, build_next_action = _custom_build_summary(linked_builds)
        browser_review_status, follow_up_state = _capture_status(capture)

        contact = (
            str(details.get("conversation_contact") or "").strip()
            or _contact_from_source_refs(queue_item.get("source_refs") or [])
            or str(order.get("buyer_name") or "").strip()
            or "Customer"
        )
        product_title = str(order.get("product_title") or "").strip()
        open_in_browser_hint = product_title or str(queue_item.get("summary") or "").strip() or "Open the Etsy thread and read the latest message."
        linked_short_id = (
            str(linked_customer.get("short_id") or "").strip()
            or str(capture.get("packet_short_id") or "").strip()
            or None
        )
        latest_message_preview = str(capture.get("latest_message_text") or capture.get("customer_summary") or "").strip() or details.get("latest_message_preview") or queue_item.get("summary")
        browser_thread_url = _canonical_etsy_message_url(capture.get("thread_url")) or direct_browser_url or INBOX_URLS[0]
        recommended_action = _browser_next_action(
            status=browser_review_status,
            follow_up=follow_up_state,
            linked_builds=linked_builds,
            capture=capture,
            open_hint=str(build_next_action or open_in_browser_hint),
        )
        linked_google_tasks = [item for item in linked_builds if str(item.get("google_task_status") or "") == "created"]
        items.append(
            {
                "artifact_id": f"etsy_conversation_thread::{_slugify(thread_key)}",
                "artifact_type": "etsy_conversation_thread",
                "conversation_thread_key": thread_key,
                "conversation_contact": contact,
                "grouped_message_count": int(details.get("grouped_message_count") or 1),
                "browser_review_status": browser_review_status,
                "follow_up_state": follow_up_state,
                "browser_captured_at": capture.get("captured_at"),
                "browser_unread": capture.get("unread"),
                "browser_summary": capture.get("customer_summary") or capture.get("latest_message_text"),
                "draft_reply": capture.get("draft_reply"),
                "reply_needed": capture.get("reply_needed"),
                "open_loop_owner": capture.get("open_loop_owner"),
                "last_customer_message": capture.get("last_customer_message"),
                "last_seller_message": capture.get("last_seller_message"),
                "custom_design_brief": capture.get("custom_design_brief"),
                "missing_details": capture.get("missing_details") or [],
                "task_progress_note": capture.get("task_progress_note"),
                "latest_message_preview": latest_message_preview,
                "browser_url_candidates": browser_urls,
                "primary_browser_url": browser_thread_url,
                "open_in_browser_hint": open_in_browser_hint,
                "recommended_next_action": recommended_action,
                "linked_customer_short_id": linked_short_id,
                "linked_customer_status": linked_customer.get("status"),
                "open_command": f"customer open {linked_short_id}" if linked_short_id else None,
                "linked_custom_build_count": len(linked_builds),
                "linked_custom_build_summaries": linked_build_summaries,
                "linked_google_task_count": len(linked_google_tasks),
                "linked_google_task_statuses": sorted({str(item.get("google_task_status") or "") for item in linked_builds if str(item.get("google_task_status") or "").strip()}),
                "linked_google_task_links": [
                    str(item.get("google_task_web_view_link") or "").strip()
                    for item in linked_google_tasks
                    if str(item.get("google_task_web_view_link") or "").strip()
                ],
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
            "needs_browser_review": sum(1 for item in items if item.get("browser_review_status") == "needs_browser_review"),
            "captured_threads": sum(1 for item in items if item.get("browser_review_status") == "captured"),
            "resolved_threads": sum(1 for item in items if item.get("browser_review_status") == "resolved"),
            "linked_custom_build_threads": sum(1 for item in items if int(item.get("linked_custom_build_count") or 0) > 0),
            "threads_with_staged_reply": sum(1 for item in items if str(item.get("draft_reply") or "").strip()),
            "threads_with_reply_draft": sum(1 for item in items if str(item.get("follow_up_state") or "") == "reply_drafted"),
            "threads_waiting_on_customer": sum(1 for item in items if str(item.get("follow_up_state") or "") == "waiting_on_customer"),
            "threads_ready_for_task": sum(1 for item in items if str(item.get("follow_up_state") or "") == "ready_for_task"),
            "active_followups": sum(
                1
                for item in items
                if str(item.get("follow_up_state") or "")
                in {"reply_drafted", "waiting_on_customer", "needs_reply", "ready_for_task", "concept_in_progress", "waiting_on_operator"}
            ),
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
                f"- Follow-up state: `{item.get('follow_up_state') or 'not_set'}`",
                f"- Messages in thread: `{item.get('grouped_message_count')}`",
                f"- Latest preview: {item.get('latest_message_preview') or '(none)'}",
                f"- Order: `{order.get('receipt_id') or 'n/a'}` / `{order.get('product_title') or 'unknown product'}`",
                f"- Next step: {item.get('recommended_next_action') or item.get('open_in_browser_hint')}",
            ]
        )
        if item.get("browser_summary"):
            lines.append(f"- Browser summary: {item.get('browser_summary')}")
        if item.get("last_customer_message"):
            lines.append(f"- Last customer message: {item.get('last_customer_message')}")
        if item.get("last_seller_message"):
            lines.append(f"- Last seller message: {item.get('last_seller_message')}")
        if item.get("open_loop_owner"):
            lines.append(f"- Open loop owner: `{item.get('open_loop_owner')}`")
        if item.get("reply_needed") is not None:
            lines.append(f"- Reply needed: `{str(bool(item.get('reply_needed'))).lower()}`")
        if item.get("draft_reply"):
            lines.append(f"- Draft reply: {item.get('draft_reply')}")
        if item.get("custom_design_brief"):
            lines.append(f"- Design brief: {item.get('custom_design_brief')}")
        if item.get("missing_details"):
            lines.append(f"- Missing details: {', '.join(item.get('missing_details') or [])}")
        if item.get("task_progress_note"):
            lines.append(f"- Task progress: {item.get('task_progress_note')}")
        if item.get("browser_unread") is not None:
            lines.append(f"- Unread in browser: `{str(bool(item.get('browser_unread'))).lower()}`")
        if item.get("browser_captured_at"):
            lines.append(f"- Browser captured at: `{item.get('browser_captured_at')}`")
        if item.get("linked_customer_short_id"):
            lines.append(
                f"- Customer lane command: `customer open {item.get('linked_customer_short_id')}`"
            )
        linked_builds = item.get("linked_custom_build_summaries") or []
        if linked_builds:
            lines.append("- Linked custom builds:")
            for summary in linked_builds[:2]:
                lines.append(f"  - {summary}")
        if item.get("linked_google_task_links"):
            lines.append(f"- Google Task links: {', '.join(item.get('linked_google_task_links')[:2])}")
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
