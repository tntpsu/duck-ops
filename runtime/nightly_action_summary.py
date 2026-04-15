#!/usr/bin/env python3
"""
Nightly action summary builder for Duck Ops.
"""

from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path
import re
from typing import Any

from workflow_operator_summary import build_workflow_followthrough_items


PACKET_TYPE_ORDER = {
    "replacement": 0,
    "refund": 1,
    "reply": 2,
    "wait_for_tracking": 3,
}

FOLLOW_UP_ACTION_ORDER = {
    "waiting_on_operator": 0,
    "reply_drafted": 1,
    "needs_reply": 2,
    "ready_for_task": 3,
    "concept_in_progress": 4,
}

TOP_CUSTOMER_ACTION_LIMIT = 5
SAFE_ETSY_INBOX_URL = "https://www.etsy.com/messages?ref=hdr_user_menu-messages"
MASTER_ROADMAP_PATH = Path("/Users/philtullai/ai-agents/duck-ops/output/operator/master_roadmap.md")

ORDER_TITLE_FILLER_PATTERNS = (
    r"\brubber ducks?\b",
    r"\bfigurines?\b",
    r"\bcollectibles?\b",
    r"\bnovelty\b",
    r"\bhome decor\b",
    r"\bdesk decor\b",
    r"\bdecor\b",
    r"\bornaments?\b",
    r"\bstocking stuffer\b",
    r"\bbirthday gift\b",
    r"\bchristmas gift\b",
    r"\beaster basket\b",
    r"\bgifts?\b",
    r"\bpresents?\b",
)


def _priority_rank(value: str | None) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(value or "medium").lower(), 9)


def _trim_text(value: str | None, limit: int = 180) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _clean_order_display_name(title: str | None, limit: int = 36) -> str:
    raw = " ".join(str(title or "").split()).strip() or "Duck"
    shortened = raw
    for separator in (" | ", " - ", " – ", " — ", ": "):
        if separator in shortened:
            first = shortened.split(separator, 1)[0].strip()
            if first:
                shortened = first
                break
    lowered = shortened.lower()
    for pattern in ORDER_TITLE_FILLER_PATTERNS:
        lowered = re.sub(pattern, " ", lowered, flags=re.IGNORECASE)
    lowered = re.sub(r"\bfor\b.+$", " ", lowered, flags=re.IGNORECASE)
    lowered = re.sub(r"\s+", " ", lowered).strip(" ,;:-")
    if "duck" not in lowered and "ducks" not in lowered:
        lowered = raw
    words = [word for word in lowered.split() if word]
    if len(words) > 1 and words[-1].lower() in {"for", "with", "and", "the", "a", "an"}:
        words = words[:-1]
    display = " ".join(words).strip() or raw
    display = " ".join(word.capitalize() if word.islower() else word for word in display.split())
    return _trim_text(display, limit)


def format_operator_duck_name(title: str | None, limit: int = 36) -> str:
    return _clean_order_display_name(title, limit=limit)


def load_master_roadmap_focus() -> dict[str, Any]:
    if not MASTER_ROADMAP_PATH.exists():
        return {"available": False, "path": str(MASTER_ROADMAP_PATH), "next_steps": []}

    lines = MASTER_ROADMAP_PATH.read_text(encoding="utf-8").splitlines()
    in_section = False
    current_title: str | None = None
    current_bullets: list[str] = []
    next_steps: list[dict[str, str]] = []

    for raw in lines:
        line = raw.rstrip()
        if line.startswith("## "):
            if in_section and current_title:
                next_steps.append({"title": current_title, "summary": " ".join(current_bullets).strip()})
            in_section = line == "## Recommended Next 3 Steps"
            current_title = None
            current_bullets = []
            continue
        if not in_section:
            continue
        if line.startswith("### "):
            if current_title:
                next_steps.append({"title": current_title, "summary": " ".join(current_bullets).strip()})
            current_title = line[4:].strip()
            current_bullets = []
            continue
        if current_title and line.startswith("- "):
            current_bullets.append(line[2:].strip())

    if in_section and current_title:
        next_steps.append({"title": current_title, "summary": " ".join(current_bullets).strip()})

    return {
        "available": True,
        "path": str(MASTER_ROADMAP_PATH),
        "updated_at": datetime.fromtimestamp(MASTER_ROADMAP_PATH.stat().st_mtime).astimezone().isoformat(),
        "next_steps": next_steps[:3],
    }


def _reply_recommendation(item: dict[str, Any]) -> str | None:
    return str(item.get("suggested_reply") or "").strip() or None


def _operator_recommendation(item: dict[str, Any]) -> str:
    return str(item.get("operator_guidance") or "").strip() or "Review the case and choose the next move."


def _thread_link_from_candidates(candidates: list[Any] | None) -> str | None:
    for raw in candidates or []:
        url = str(raw or "").strip()
        if not url:
            continue
        lowered = url.lower()
        match = re.search(r"https://www\.etsy\.com/(?:your/account/)?messages/(?P<id>\d+)", url, re.IGNORECASE)
        if match:
            return f"https://www.etsy.com/messages/{match.group('id')}"
    for raw in candidates or []:
        url = str(raw or "").strip()
        if not url:
            continue
        lowered = url.lower()
        if (
            "etsy.com/messages?ref=hdr_user_menu-messages" in lowered
            or "etsy.com/your/messages" in lowered
            or "etsy.com/your/account/messages" in lowered
            or lowered.rstrip("/") == "https://www.etsy.com/messages"
        ):
            return "https://www.etsy.com/messages?ref=hdr_user_menu-messages"
    return None


def _source_ref_subject(item: dict[str, Any]) -> str | None:
    for ref in item.get("source_refs") or []:
        subject = str((ref or {}).get("subject") or "").strip()
        if subject:
            return subject
    return None


def _contact_from_source_refs(item: dict[str, Any]) -> str | None:
    subject = _source_ref_subject(item)
    if not subject:
        return None
    match = re.search(r"etsy conversation with\s+(?P<name>.+)$", subject, re.IGNORECASE)
    if match:
        name = re.sub(r"\s+from\s+.+$", "", match.group("name").strip(), flags=re.IGNORECASE).strip()
        if name:
            return name
    match = re.search(r"^(?P<name>.+?)\s+needs help with an order they placed$", subject, re.IGNORECASE)
    if match:
        return match.group("name").strip()
    return None


def _is_generic_customer_contact(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"", "customer", "buyer", "etsy customer"}


def _is_generic_customer_summary(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return True
    return normalized in {
        "latest etsy conversation needs review.",
        "latest etsy conversation needs review",
    } or normalized.startswith("latest etsy conversation needs review")


def _is_direct_thread_link(url: str | None) -> bool:
    return "/messages/" in str(url or "").strip().lower()


def _customer_action_contact(item: dict[str, Any]) -> str:
    direct = str(item.get("conversation_contact") or item.get("contact") or item.get("customer_name") or "").strip()
    if direct and not _is_generic_customer_contact(direct):
        return direct
    ref_name = _contact_from_source_refs(item)
    if ref_name:
        return ref_name
    order = item.get("order_enrichment") or {}
    buyer_name = str(order.get("buyer_name") or "").strip()
    if buyer_name:
        return buyer_name
    return direct or "Customer"


def _customer_action_summary(item: dict[str, Any]) -> str:
    summary = str(item.get("latest_message_preview") or item.get("customer_summary") or item.get("summary") or "").strip()
    if summary and not _is_generic_customer_summary(summary):
        return _trim_text(summary, 320)
    subject = _source_ref_subject(item)
    if subject:
        return _trim_text(subject, 320)
    return _trim_text(summary or "Latest Etsy conversation needs review.", 320)


def _customer_action_title(item: dict[str, Any], contact: str) -> str:
    title = str(item.get("title") or "").strip()
    if title and title.lower() not in {"customer issue", "customer reply", "customer replacement", "customer refund"}:
        return title
    if contact and not _is_generic_customer_contact(contact):
        return f"Etsy conversation - {contact}"
    return title or "Etsy conversation"


def _customer_action_category(item: dict[str, Any], *, source: str) -> str:
    if source == "attention":
        packet_types = {str(value or "").strip() for value in (item.get("related_packet_types") or []) if str(value or "").strip()}
        packet_type = str(item.get("packet_type") or "").strip()
        if packet_type:
            packet_types.add(packet_type)
        summary = _customer_action_summary(item)
        if {"refund", "replacement"} <= packet_types:
            return "Recovery decision"
        if "replacement" in packet_types:
            return "Replacement"
        if "refund" in packet_types:
            return "Refund"
        if "wait_for_tracking" in packet_types:
            return "Tracking follow-up"
        if str(item.get("case_type") or "").strip() == "email_support":
            return "Email support"
        if _is_generic_customer_summary(summary):
            return "Inbox review"
        return "Reply needed"

    follow_up_state = str(item.get("follow_up_state") or "").strip()
    return {
        "waiting_on_operator": "Operator decision",
        "reply_drafted": "Reply ready to send",
        "needs_reply": "Inbox review" if _is_generic_customer_summary(item.get("latest_message_preview") or item.get("browser_summary")) else "Reply needed",
        "ready_for_task": "Build task",
        "concept_in_progress": "Concept work",
    }.get(follow_up_state, "Follow-up")


def _customer_action_open_link(item: dict[str, Any]) -> str | None:
    direct = _thread_link_from_candidates(
        [
            item.get("primary_browser_url"),
            *((item.get("browser_url_candidates") or []) if isinstance(item.get("browser_url_candidates"), list) else []),
        ]
    )
    if direct:
        return direct
    source_refs = item.get("source_refs") or []
    if source_refs:
        return SAFE_ETSY_INBOX_URL
    if str(item.get("open_command") or "").strip():
        return SAFE_ETSY_INBOX_URL
    return None


def _customer_action_sort_key(item: dict[str, Any]) -> tuple[int, int, int, int, str]:
    category = str(item.get("category") or "").strip().lower()
    source = str(item.get("source") or "").strip()
    contact = _customer_action_contact(item)
    summary = _customer_action_summary(item)
    open_link = _customer_action_open_link(item)
    short_id = str(item.get("short_id") or item.get("linked_customer_short_id") or "").strip()
    direct_link = _is_direct_thread_link(open_link)
    generic_contact = _is_generic_customer_contact(contact)
    generic_summary = _is_generic_customer_summary(summary)

    if "recovery" in category or category in {"replacement", "refund"}:
        action_rank = 0
    elif "operator" in category:
        action_rank = 1
    elif "reply ready" in category:
        action_rank = 2
    elif "build" in category or "concept" in category:
        action_rank = 3
    elif "tracking" in category:
        action_rank = 4
    elif "email support" in category:
        action_rank = 5
    elif "reply needed" in category:
        action_rank = 6
    else:
        action_rank = 7

    specificity_rank = 0 if direct_link else (1 if short_id else 2)
    generic_rank = (1 if generic_contact else 0) + (1 if generic_summary else 0)
    source_rank = 0 if source == "attention" else 1
    return (action_rank, specificity_rank, generic_rank, source_rank, contact.lower())


def _format_tracking_line(item: dict[str, Any]) -> str | None:
    tracking = item.get("tracking_enrichment") or {}
    status = str(tracking.get("status") or "").strip()
    if not status:
        return None
    tracking_number = str(tracking.get("tracking_number") or "").strip()
    carrier = str(tracking.get("carrier") or "").strip()
    parts = [status.replace("_", " ")]
    live_status = str(tracking.get("live_status_label") or "").strip()
    if live_status:
        parts.append(f"live: {live_status.replace('_', ' ')}")
    if carrier:
        parts.append(carrier)
    if tracking_number:
        parts.append(tracking_number)
    return " / ".join(parts)


def _format_custom_order_line(order: dict[str, Any]) -> str:
    buyer = str(order.get("buyer_name") or "").strip() or "Customer"
    quantity = int(order.get("quantity") or 0)
    custom_type = str(order.get("custom_type") or "").strip()
    summary = str(order.get("custom_design_summary") or "").strip()
    product_title = str(order.get("product_title") or "Custom duck").strip()
    order_ref = str(order.get("order_ref") or "").strip()

    if summary and custom_type and not summary.lower().startswith(custom_type.lower()):
        detail = f"{custom_type}: {summary}"
    else:
        detail = summary or custom_type or "custom details still need review"

    qty_text = f"{quantity}x" if quantity > 1 else "1x"
    ref_text = f" [{order_ref}]" if order_ref else ""
    return f"{buyer}{ref_text}: {qty_text} {detail} ({product_title} on {order.get('channel')})"


def _format_custom_candidate_line(candidate: dict[str, Any]) -> str:
    buyer = str(candidate.get("buyer_name") or "").strip() or "Customer"
    quantity = int(candidate.get("quantity") or 0)
    qty_text = f"{quantity}x" if quantity > 1 else "1x"
    detail = str(candidate.get("custom_design_summary") or candidate.get("product_title") or "Custom build").strip()
    stage = str(candidate.get("design_workflow_stage") or "").strip() or "brief_ready"
    google_status = str(candidate.get("google_task_status") or "").strip() or "not_created"
    next_step = str(candidate.get("next_design_action") or "").strip() or "Review the design brief."
    return (
        f"{buyer}: {qty_text} {detail}"
        f" | stage `{stage}`"
        f" | task `{google_status}`"
        f" | next {next_step}"
    )


def _format_orders_to_pack_mobile_lines(item: dict[str, Any]) -> list[str]:
    channels = item.get("by_channel") or {}
    product_title = format_operator_duck_name(item.get("product_title"), limit=44)
    order_count = int(item.get("order_count") or 0)
    total_quantity = int(item.get("total_quantity") or 0)
    lines = [
        f"- {product_title}",
        f"  Orders: {order_count} | Total units: {total_quantity}",
        f"  Channels: Etsy {int(channels.get('etsy', 0) or 0)} / Shopify {int(channels.get('shopify', 0) or 0)}",
    ]
    order_refs = [str(value).strip() for value in (item.get("order_refs") or []) if str(value).strip()]
    if order_refs:
        refs_display = ", ".join(order_refs[:3])
        if len(order_refs) > 3:
            refs_display += f", +{len(order_refs) - 3} more"
        lines.append(f"  Order refs: {refs_display}")
    option_summary = str(item.get("option_summary") or "").strip()
    if option_summary:
        lines.append(f"  Choices: {option_summary}")
    return lines


def _browser_new_threads(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in items if str(item.get("browser_review_status") or "") == "needs_browser_review"]


def _browser_action_follow_up_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_states = {"reply_drafted", "needs_reply", "ready_for_task", "concept_in_progress", "waiting_on_operator"}
    return [item for item in items if str(item.get("follow_up_state") or "") in active_states]


def _browser_waiting_on_customer_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in items if str(item.get("follow_up_state") or "") == "waiting_on_customer"]


def _attention_action_rank(item: dict[str, Any]) -> tuple[int, int, int, str]:
    order = item.get("order_enrichment") or {}
    has_order = bool(order.get("matched"))
    has_contact = bool(str(item.get("conversation_contact") or item.get("customer_name") or "").strip())
    has_open_command = bool(str(item.get("open_command") or "").strip())
    generic_mailbox = (
        str(item.get("case_type") or "").strip() == "email_support"
        and not has_order
        and not has_contact
        and not has_open_command
    )
    return (
        1 if generic_mailbox else 0,
        _priority_rank(item.get("priority")),
        PACKET_TYPE_ORDER.get(str(item.get("packet_type") or ""), 99),
        str(item.get("title") or "").lower(),
    )


def _browser_item_index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in items:
        for key in [item.get("source_artifact_id"), *(item.get("source_artifact_ids") or [])]:
            normalized = str(key or "").strip()
            if normalized and normalized not in index:
                index[normalized] = item
    return index


def _build_customer_action_queue(
    attention_items: list[dict[str, Any]],
    follow_up_items: list[dict[str, Any]],
    *,
    limit: int = TOP_CUSTOMER_ACTION_LIMIT,
) -> tuple[list[dict[str, Any]], int]:
    ranked: list[dict[str, Any]] = []
    browser_index = _browser_item_index(follow_up_items)

    for item in attention_items:
        linked_browser = {}
        for key in [item.get("source_artifact_id"), *(item.get("source_artifact_ids") or [])]:
            normalized = str(key or "").strip()
            if normalized and normalized in browser_index:
                linked_browser = browser_index[normalized]
                break
        merged_item = {**linked_browser, **item}
        contact = _customer_action_contact(merged_item)
        summary = _customer_action_summary(merged_item)
        category = _customer_action_category(merged_item, source="attention")
        open_link = _customer_action_open_link(merged_item)
        short_id = str(item.get("short_id") or item.get("linked_customer_short_id") or linked_browser.get("linked_customer_short_id") or "").strip() or None
        ranked.append(
            {
                "source": "attention",
                "title": _customer_action_title(merged_item, contact),
                "contact": contact,
                "priority": str(item.get("priority") or "medium").upper(),
                "summary": summary,
                "next_step": _operator_recommendation(item),
                "reply_text": _reply_recommendation(item),
                "order_enrichment": item.get("order_enrichment") or {},
                "tracking_line": _format_tracking_line(item),
                "category": category,
                "short_id": short_id,
                "open_command": item.get("open_command") or linked_browser.get("open_command"),
                "open_link": open_link,
                "source_refs": item.get("source_refs") or [],
            }
        )

    for item in follow_up_items:
        follow_up_state = str(item.get("follow_up_state") or "captured")
        contact = _customer_action_contact(item)
        summary = _customer_action_summary(item)
        category = _customer_action_category(item, source="follow_up")
        open_link = _customer_action_open_link(item)
        ranked.append(
            {
                "source": "follow_up",
                "title": _customer_action_title(item, contact) or _browser_thread_title(item),
                "contact": contact,
                "state": follow_up_state.replace("_", " "),
                "summary": summary,
                "next_step": item.get("recommended_next_action") or "Keep the thread moving.",
                "reply_text": item.get("draft_reply"),
                "category": category,
                "short_id": str(item.get("linked_customer_short_id") or "").strip() or None,
                "open_command": item.get("open_command"),
                "open_link": open_link,
                "source_refs": item.get("source_refs") or [],
            }
        )

    ranked.sort(key=_customer_action_sort_key)
    hidden_count = max(0, len(ranked) - limit)
    visible = ranked[:limit]
    return visible, hidden_count


def _browser_thread_title(item: dict[str, Any]) -> str:
    order = item.get("order_enrichment") or {}
    if order.get("product_title"):
        return str(order.get("product_title"))
    contact = str(item.get("conversation_contact") or "").strip()
    if contact:
        return f"Etsy conversation - {contact}"
    return "Etsy conversation"


def _format_local_clock(iso_value: Any) -> str | None:
    text = str(iso_value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone()
    except ValueError:
        return None
    return dt.strftime("%I:%M %p").lstrip("0")


def _order_snapshot_refresh_summary(payload: dict[str, Any]) -> str | None:
    refresh = payload.get("order_snapshot_refresh") or {}
    sources = refresh.get("sources") or {}
    if not sources:
        return None
    labels = {
        "live": "live",
        "fallback_cached": "cached fallback",
        "missing": "missing",
    }
    parts: list[str] = []
    for source in ("etsy", "shopify"):
        details = sources.get(source) or {}
        if not details:
            continue
        status = labels.get(str(details.get("status") or ""), str(details.get("status") or "unknown"))
        generated_at = _format_local_clock(details.get("generated_at"))
        if generated_at:
            parts.append(f"{source.title()} {status} at {generated_at}")
        else:
            parts.append(f"{source.title()} {status}")
    return " | ".join(parts) if parts else None


def _html_text(value: Any) -> str:
    if value is None:
        return ""
    return escape(str(value))


def _nightly_card(title: str, body: str) -> str:
    return (
        "<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:14px;"
        "padding:16px 18px;margin:0 0 12px 0;\">"
        f"<div style=\"font-size:16px;font-weight:700;color:#111827;margin:0 0 8px 0;\">{_html_text(title)}</div>"
        f"{body}</div>"
    )


def _nightly_stat(label: str, value: Any) -> str:
    return (
        "<div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;\">"
        f"<div style=\"font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;\">{_html_text(label)}</div>"
        f"<div style=\"font-size:22px;font-weight:700;color:#111827;margin-top:4px;\">{_html_text(value)}</div>"
        "</div>"
    )


def _merge_attention_packets(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        thread_key = str(item.get("conversation_thread_key") or "").strip()
        receipt_id = str((item.get("order_enrichment") or {}).get("receipt_id") or "").strip()
        key = thread_key or (f"receipt::{receipt_id}" if receipt_id else str(item.get("source_artifact_id") or item.get("packet_id") or ""))
        grouped.setdefault(key, []).append(item)

    merged: list[dict[str, Any]] = []
    for group in grouped.values():
        ordered_group = sorted(
            group,
            key=lambda item: (
                _priority_rank(item.get("priority")),
                PACKET_TYPE_ORDER.get(str(item.get("packet_type") or ""), 99),
            ),
        )
        packet_types = {str(item.get("packet_type") or "") for item in ordered_group}
        representative = dict(ordered_group[0])
        title = str(representative.get("title") or "").strip()
        if title in {"Customer reply", "Customer refund", "Customer replacement"}:
            representative["title"] = "Customer issue"

        if {"refund", "replacement"} <= packet_types:
            representative["operator_guidance"] = "Decide whether you want to refund or replace before replying."
            representative["suggested_reply"] = (
                "Hi, I’m sorry this arrived broken or wasn’t right. "
                "Please let me know whether you’d prefer a refund or a replacement."
            )
        elif "replacement" in packet_types:
            representative = dict(next(item for item in ordered_group if item.get("packet_type") == "replacement"))
        elif "refund" in packet_types:
            representative = dict(next(item for item in ordered_group if item.get("packet_type") == "refund"))
        elif "reply" in packet_types:
            representative = dict(next(item for item in ordered_group if item.get("packet_type") == "reply"))

        representative["related_packet_types"] = sorted(packet_types)
        merged.append(representative)

    merged.sort(key=lambda item: (_priority_rank(item.get("priority")), str(item.get("title") or "").lower()))
    return merged


def build_nightly_action_summary(
    packet_payload: dict[str, Any],
    custom_design_cases: list[dict[str, Any]],
    packing_summary: dict[str, Any],
    custom_build_task_candidates: dict[str, Any] | None = None,
    etsy_browser_sync: dict[str, Any] | None = None,
    workflow_followthrough: list[dict[str, Any]] | None = None,
    now_local: datetime | None = None,
) -> dict[str, Any]:
    now_local = now_local or datetime.now().astimezone()
    send_after = now_local.replace(hour=19, minute=0, second=0, microsecond=0)

    packets = packet_payload.get("items") or []
    attention_items = []
    replacement_label_items = []
    watch_items = []

    for packet in packets:
        packet_type = str(packet.get("packet_type") or "")
        status = str(packet.get("status") or "")
        if packet_type in {"reply", "refund", "replacement"} and status not in {
            "buy_label_now",
            "watch",
            "waiting_by_operator_decision",
            "possible_reship_already_sent",
        }:
            attention_items.append(packet)
        elif packet_type == "replacement" and packet.get("next_physical_action") == "buy_label_now":
            replacement_label_items.append(packet)
        elif packet_type == "wait_for_tracking":
            watch_items.append(packet)
        elif packet_type == "replacement" and status in {"possible_reship_already_sent", "waiting_by_operator_decision"}:
            watch_items.append(packet)

    attention_items = _merge_attention_packets(attention_items)
    replacement_label_items.sort(key=lambda item: (_priority_rank(item.get("priority")), str(item.get("title") or "").lower()))
    watch_items.sort(key=lambda item: (_priority_rank(item.get("priority")), str(item.get("title") or "").lower()))

    ready_custom_cases = []
    blocked_custom_cases = []
    for case in custom_design_cases:
        row = {
            "source": "custom_design_case",
            "customer_name": case.get("customer_name"),
            "summary": case.get("request_summary"),
            "ready_for_manual_design": bool(case.get("ready_for_manual_design")),
            "open_questions": case.get("open_questions") or [],
            "source_refs": case.get("source_refs") or [],
        }
        if row["ready_for_manual_design"]:
            ready_custom_cases.append(row)
        else:
            blocked_custom_cases.append(row)

    custom_orders = packing_summary.get("custom_orders_to_make") or []
    custom_build_items = list((custom_build_task_candidates or {}).get("items") or [])
    orders_to_pack = packing_summary.get("orders_to_pack") or []
    browser_items = list((etsy_browser_sync or {}).get("items") or [])
    workflow_items = list(
        workflow_followthrough or build_workflow_followthrough_items(limit=6, include_all_blocked=True)
    )
    actionable_workflow_items = [item for item in workflow_items if item.get("actionable", True)]
    info_workflow_items = [item for item in workflow_items if not item.get("actionable", True)]
    customer_new_threads = _browser_new_threads(browser_items)
    customer_follow_up_items = _browser_action_follow_up_items(browser_items)
    customer_waiting_items = _browser_waiting_on_customer_items(browser_items)
    customer_top_actions, hidden_customer_actions = _build_customer_action_queue(attention_items, customer_follow_up_items)
    order_snapshot_refresh = packing_summary.get("snapshot_refresh") or {}
    customer_thread_sync_generated_at = str((etsy_browser_sync or {}).get("generated_at") or "").strip() or None

    payload = {
        "generated_at": now_local.isoformat(),
        "summary_date": now_local.strftime("%Y-%m-%d"),
        "send_after": send_after.isoformat(),
        "send_window_open": now_local >= send_after,
        "strategy_focus": load_master_roadmap_focus(),
        "counts": {
            "customer_attention_items": len(attention_items),
            "customer_reply_items": len(attention_items),
            "customer_new_thread_items": len(customer_new_threads),
            "customer_follow_up_items": len(customer_follow_up_items),
            "customer_follow_up_reply_drafts": sum(1 for item in customer_follow_up_items if str(item.get("follow_up_state") or "") == "reply_drafted"),
            "customer_waiting_on_customer": len(customer_waiting_items),
            "customer_top_action_items": len(customer_top_actions),
            "customer_hidden_action_items": hidden_customer_actions,
            "customer_thread_sync_generated_at": customer_thread_sync_generated_at,
            "replacement_labels_now": len(replacement_label_items),
            "orders_to_pack_titles": len(orders_to_pack),
            "orders_to_pack_units": sum(int(item.get("total_quantity") or 0) for item in orders_to_pack),
            "custom_ready_cases": len(ready_custom_cases),
            "custom_blocked_cases": len(blocked_custom_cases),
            "custom_order_lines": len(custom_orders) if custom_orders else len(custom_build_items),
            "custom_build_candidates": len(custom_build_items),
            "custom_build_tasks_live": sum(1 for item in custom_build_items if str(item.get("google_task_status") or "") == "created"),
            "watch_items": len(watch_items),
            "workflow_followthrough_items": len(actionable_workflow_items),
            "workflow_info_items": len(info_workflow_items),
        },
        "sections": {
            "customer_top_actions": customer_top_actions,
            "customer_new_threads": customer_new_threads,
            "customer_followups_in_motion": customer_follow_up_items,
            "customer_waiting_on_customer": customer_waiting_items,
            "customer_issues_needing_attention": attention_items,
            "customer_issues_needing_reply": attention_items,
            "buy_replacement_labels_now": replacement_label_items,
            "orders_to_pack": orders_to_pack,
            "custom_novel_ducks_to_make": {
                "ready_cases": ready_custom_cases,
                "blocked_cases": blocked_custom_cases,
                "open_custom_orders": custom_orders,
                "build_candidates": custom_build_items,
            },
            "watch_list": watch_items,
            "workflow_followthrough": actionable_workflow_items,
            "workflow_notes": info_workflow_items,
        },
        "order_snapshot_refresh": order_snapshot_refresh,
        "customer_thread_sync_generated_at": customer_thread_sync_generated_at,
    }
    return payload


def render_nightly_action_summary_markdown(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") or {}
    sections = payload.get("sections") or {}
    strategy_focus = payload.get("strategy_focus") or {}

    lines = [
        "# Duck Ops Tonight",
        "",
        f"Prepared at {payload.get('generated_at')}",
        "",
        "Tonight at a glance:",
        f"- New customer threads: {counts.get('customer_new_thread_items', 0)}",
        f"- Customer actions already in motion: {counts.get('customer_follow_up_items', 0)}",
        f"- Waiting on the customer (info only): {counts.get('customer_waiting_on_customer', 0)}",
        f"- Customer issues still needing attention tonight: {counts.get('customer_attention_items', 0)}",
        f"- Replacement labels to buy now: {counts.get('replacement_labels_now', 0)}",
        f"- Non-custom ducks to pack: {counts.get('orders_to_pack_units', 0)} units across {counts.get('orders_to_pack_titles', 0)} ducks",
        f"- Custom ducks to make: {counts.get('custom_order_lines', 0)} open custom order lines",
        f"- Workflow follow-through items: {counts.get('workflow_followthrough_items', 0)}",
        "",
        "## 0. Strategic Focus",
        "",
    ]

    if not strategy_focus.get("available"):
        lines.append("Master roadmap not available.")
    else:
        lines.append(f"- Roadmap: {strategy_focus.get('path')}")
        if strategy_focus.get("updated_at"):
            lines.append(f"- Roadmap updated: {_format_local_clock(strategy_focus.get('updated_at'))}")
        next_steps = strategy_focus.get("next_steps") or []
        if next_steps:
            lines.append("- Next major steps:")
            for step in next_steps:
                lines.append(f"  - {step.get('title')}: {_trim_text(step.get('summary'), 180)}")

    lines.extend([
        "",
        "## 1. Top Customer Actions Tonight",
        "",
    ])

    top_customer_actions = sections.get("customer_top_actions") or []
    hidden_customer_actions = int(counts.get("customer_hidden_action_items") or 0)
    if not top_customer_actions:
        lines.append("No customer actions need hands-on follow-through tonight.")
    else:
        if hidden_customer_actions > 0:
            lines.append(f"_Showing the top {len(top_customer_actions)} customer actions. {hidden_customer_actions} more are queued behind these._")
            lines.append("")
        for index, item in enumerate(top_customer_actions, start=1):
            lines.append(f"### {index}. {item.get('title') or 'Customer action'}")
            lines.append("")
            lines.append(f"- Contact: {item.get('contact') or 'Customer'}")
            if item.get("category"):
                lines.append(f"- Category: {item.get('category')}")
            if item.get("source") == "attention":
                lines.append(f"- Priority: {item.get('priority') or 'MEDIUM'}")
            else:
                lines.append(f"- State: {item.get('state') or 'active'}")
            lines.append(f"- What needs action: {_trim_text(item.get('summary'), 320)}")
            lines.append(f"- Next step: {item.get('next_step') or 'Review the case and move it forward.'}")
            if item.get("short_id"):
                lines.append(f"- Thread ID: {item.get('short_id')}")
            if item.get("open_link"):
                lines.append(f"- Open link: {item.get('open_link')}")
            if item.get("open_command"):
                lines.append(f"- Open command: `{item.get('open_command')}`")
            if item.get("reply_text"):
                label = "Suggested reply" if item.get("source") == "attention" else "Draft reply"
                lines.append(f"- {label}: \"{_trim_text(item.get('reply_text'), 280)}\"")
            order = item.get("order_enrichment") or {}
            if order.get("matched"):
                lines.append(
                    f"- Order: {order.get('product_title') or 'Unknown product'}"
                    f" (receipt {order.get('receipt_id') or 'n/a'}, status {order.get('order_status') or 'n/a'})"
                )
            if item.get("tracking_line"):
                lines.append(f"- Tracking: {item.get('tracking_line')}")
            lines.append("")

    lines.extend(["", "## 2. New Customer Threads", ""])

    new_thread_items = sections.get("customer_new_threads") or []
    if not new_thread_items:
        lines.append("No brand-new Etsy threads are waiting for first review right now.")
    else:
        lines.append(
            f"{len(new_thread_items)} new Etsy thread(s) still need first review. "
            "These are intentionally collapsed so tonight's email stays focused on actions already on your side."
        )
        lines.append("Next step: use `customer threads` when you want to triage the fresh inbox.")

    lines.extend(["", "## 3. Customer Status Counts", ""])
    waiting_count = int(counts.get("customer_waiting_on_customer") or 0)
    followup_count = int(counts.get("customer_follow_up_items") or 0)
    customer_sync = _format_local_clock(payload.get("customer_thread_sync_generated_at"))
    if waiting_count or followup_count:
        lines.append(f"- Actions already in motion: {followup_count}")
        lines.append(f"- Waiting on the customer: {waiting_count}")
        if customer_sync:
            lines.append(f"- Customer thread sync: {customer_sync}")
        if waiting_count > 0:
            lines.append("- Those waiting-on-customer threads are intentionally omitted from tonight's action queue.")
    else:
        lines.append("No customer followups are currently in motion.")

    lines.extend(["", "## 4. Buy Replacement Labels Now", ""])
    replacement_label_items = sections.get("buy_replacement_labels_now") or []
    if not replacement_label_items:
        lines.append("No resend cases are at `buy_label_now` yet.")
    else:
        for item in replacement_label_items:
            order = item.get("order_enrichment") or {}
            lines.append(f"- {item.get('title')}: buy a replacement label now.")
            if order.get("matched"):
                lines.append(
                    f"  Order: {order.get('product_title') or 'Unknown product'}"
                    f" (receipt {order.get('receipt_id') or 'n/a'})"
                )
            lines.append(f"  Reply after purchase: \"A replacement is on the way and I’ll send tracking as soon as it updates.\"")

    lines.extend(["", "## 5. Orders To Pack", ""])
    orders_to_pack = sections.get("orders_to_pack") or []
    if not orders_to_pack:
        lines.append("No non-custom duck orders are currently open for packing.")
    else:
        etsy_total = sum(int((item.get("by_channel") or {}).get("etsy", 0) or 0) for item in orders_to_pack)
        shopify_total = sum(int((item.get("by_channel") or {}).get("shopify", 0) or 0) for item in orders_to_pack)
        grand_total = sum(int(item.get("total_quantity") or 0) for item in orders_to_pack)
        lines.extend(
            [
                "_Sorted by ship urgency first, then quantity._",
                "",
                f"- Totals: Etsy {etsy_total} / Shopify {shopify_total} / Total units {grand_total}",
            ]
        )
        snapshot_summary = _order_snapshot_refresh_summary(payload)
        if snapshot_summary:
            lines.append(f"- Snapshot freshness: {snapshot_summary}")
        lines.append("")
        for item in orders_to_pack:
            lines.extend(_format_orders_to_pack_mobile_lines(item))
            lines.append("")

    lines.extend(["", "## 6. Custom / Novel Ducks To Make", ""])
    custom_section = sections.get("custom_novel_ducks_to_make") or {}
    ready_cases = custom_section.get("ready_cases") or []
    blocked_cases = custom_section.get("blocked_cases") or []
    open_custom_orders = custom_section.get("open_custom_orders") or []
    if not ready_cases and not blocked_cases and not open_custom_orders:
        lines.append("No custom or novel duck design work is queued right now.")
    else:
        if ready_cases:
            lines.append("Ready briefs:")
            for case in ready_cases:
                lines.append(f"- {case.get('summary') or 'Custom design brief'}")
        if blocked_cases:
            lines.append("Waiting on clarification:")
            for case in blocked_cases:
                question_text = "; ".join(case.get("open_questions") or []) or "More design detail needed."
                lines.append(f"- {case.get('summary') or 'Custom design brief'} ({question_text})")
        if open_custom_orders:
            lines.append("Open custom orders:")
            build_candidates = custom_section.get("build_candidates") or []
            if build_candidates:
                for candidate in build_candidates:
                    lines.append(f"- {_format_custom_candidate_line(candidate)}")
            else:
                for order in open_custom_orders:
                    lines.append(f"- {_format_custom_order_line(order)}")

    lines.extend(["", "## 7. Watch List", ""])
    watch_items = sections.get("watch_list") or []
    if not watch_items:
        lines.append("Nothing is in a monitor-only state right now.")
    else:
        for item in watch_items:
            tracking = item.get("tracking_enrichment") or {}
            resolution = item.get("resolution_enrichment") or {}
            tracking_line = f"`{tracking.get('status') or 'unknown'}`"
            if tracking.get("live_status_label"):
                tracking_line += f" / live `{tracking.get('live_status_label')}`"
            if tracking.get("tracking_number"):
                tracking_line += f" ({tracking.get('tracking_number')})"
            suffix = ""
            if resolution.get("signals"):
                suffix = f" - {resolution.get('summary') or ''}"
            lines.append(f"- {item.get('title')}: {tracking_line}{suffix}")

    lines.extend(["", "## 8. Workflow Follow-Through", ""])
    workflow_items = sections.get("workflow_followthrough") or []
    workflow_notes = sections.get("workflow_notes") or []
    if not workflow_items:
        lines.append("No workflow follow-through items are staged right now.")
    else:
        for item in workflow_items:
            lines.append(
                f"- {item.get('lane')}: {item.get('title')} | {_trim_text(item.get('summary'), 160)}"
            )
            if item.get("root_cause"):
                lines.append(f"  Why: {_trim_text(item.get('root_cause'), 220)}")
            if item.get("fix_hint"):
                lines.append(f"  Fix: {_trim_text(item.get('fix_hint'), 220)}")
            if item.get("latest_receipt"):
                lines.append(f"  Last receipt: {item.get('latest_receipt')}")
            if item.get("recent_history"):
                lines.append(f"  Trail: {_trim_text(item.get('recent_history'), 160)}")
            if item.get("next_action"):
                lines.append(f"  Do: {item.get('next_action')}")
            urgent_items = item.get("urgent_items") or []
            if urgent_items:
                lines.append("  Urgent items:")
                for urgent in urgent_items:
                    urgent_summary = f"{urgent.get('title')} | {urgent.get('decision')} | {urgent.get('priority')}"
                    why = str(urgent.get("why") or "").strip()
                    if why:
                        urgent_summary += f" | {_trim_text(why, 120)}"
                    lines.append(f"    - {urgent_summary}")
            if item.get("command"):
                lines.append(f"  Run: `{item.get('command')}`")

    if workflow_notes:
        lines.extend(["", "## 9. Workflow Notes", ""])
        for item in workflow_notes:
            lines.append(f"- {item.get('lane')}: {item.get('title')} | {_trim_text(item.get('summary'), 160)}")
            if item.get("latest_receipt"):
                lines.append(f"  Last receipt: {item.get('latest_receipt')}")
            if item.get("next_action"):
                lines.append(f"  Note: {item.get('next_action')}")

    lines.append("")
    return "\n".join(lines)


def render_nightly_action_summary_html(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") or {}
    sections = payload.get("sections") or {}
    strategy_focus = payload.get("strategy_focus") or {}
    top_customer_actions = sections.get("customer_top_actions") or []
    new_thread_items = sections.get("customer_new_threads") or []
    replacement_label_items = sections.get("buy_replacement_labels_now") or []
    orders_to_pack = sections.get("orders_to_pack") or []
    custom_section = sections.get("custom_novel_ducks_to_make") or {}
    ready_cases = custom_section.get("ready_cases") or []
    blocked_cases = custom_section.get("blocked_cases") or []
    open_custom_orders = custom_section.get("open_custom_orders") or []
    build_candidates = custom_section.get("build_candidates") or []
    watch_items = sections.get("watch_list") or []
    workflow_items = sections.get("workflow_followthrough") or []
    workflow_notes = sections.get("workflow_notes") or []

    stat_grid = "".join(
        [
            _nightly_stat("New threads", counts.get("customer_new_thread_items", 0)),
            _nightly_stat("Actions live", counts.get("customer_follow_up_items", 0)),
            _nightly_stat("Needs attention", counts.get("customer_attention_items", 0)),
            _nightly_stat("Replacement labels", counts.get("replacement_labels_now", 0)),
            _nightly_stat("Pack tonight", f"{counts.get('orders_to_pack_units', 0)} units"),
            _nightly_stat("Custom ducks", counts.get("custom_order_lines", 0)),
        ]
    )

    strategy_blocks: list[str] = []
    if not strategy_focus.get("available"):
        strategy_blocks.append("<p style=\"margin:0;color:#4b5563;\">Master roadmap not available.</p>")
    else:
        strategy_blocks.append(
            f"<div style=\"margin-bottom:8px;\"><strong>Roadmap:</strong> <code>{_html_text(strategy_focus.get('path') or '')}</code></div>"
        )
        if strategy_focus.get("updated_at"):
            strategy_blocks.append(
                f"<div style=\"margin-bottom:8px;\"><strong>Roadmap updated:</strong> {_html_text(_format_local_clock(strategy_focus.get('updated_at')) or '')}</div>"
            )
        next_steps = strategy_focus.get("next_steps") or []
        if next_steps:
            strategy_blocks.append("<div style=\"margin-bottom:8px;\"><strong>Next major steps:</strong></div>")
            strategy_blocks.append("<ul style=\"margin:0;padding-left:20px;\">")
            for step in next_steps:
                strategy_blocks.append(
                    f"<li><strong>{_html_text(step.get('title') or '')}:</strong> {_html_text(_trim_text(step.get('summary'), 180))}</li>"
                )
            strategy_blocks.append("</ul>")

    top_customer_cards: list[str] = []
    hidden_customer_actions = int(counts.get("customer_hidden_action_items") or 0)
    if not top_customer_actions:
        top_customer_cards.append("<p style=\"margin:0;color:#4b5563;\">No customer actions need hands-on follow-through tonight.</p>")
    else:
        if hidden_customer_actions > 0:
            top_customer_cards.append(
                "<div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;margin:0 0 12px 0;color:#4b5563;\">"
                f"Showing the top {_html_text(len(top_customer_actions))} customer actions. "
                f"{_html_text(hidden_customer_actions)} more are queued behind these."
                "</div>"
            )
        for index, item in enumerate(top_customer_actions, start=1):
            bits = [
                f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">Contact: {_html_text(item.get('contact') or 'Customer')}</div>",
            ]
            if item.get("category"):
                bits.append(
                    f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">Category: {_html_text(item.get('category'))}</div>"
                )
            if item.get("source") == "attention":
                bits.append(
                    f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">Priority: {_html_text(item.get('priority') or 'MEDIUM')}</div>"
                )
            else:
                bits.append(
                    f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">State: {_html_text(item.get('state') or 'active')}</div>"
                )
            bits.append(
                f"<div style=\"margin-bottom:8px;\"><strong>What needs action:</strong> {_html_text(_trim_text(item.get('summary'), 320))}</div>"
            )
            bits.append(
                f"<div style=\"margin-bottom:8px;\"><strong>Next step:</strong> {_html_text(item.get('next_step') or 'Review the case and move it forward.')}</div>"
            )
            open_parts: list[str] = []
            if item.get("short_id"):
                open_parts.append(f"<strong>Thread ID:</strong> {_html_text(item.get('short_id'))}")
            if item.get("open_link"):
                href = _html_text(item.get("open_link"))
                label = "Open thread" if "/messages/" in str(item.get("open_link")) else "Open inbox"
                open_parts.append(f"<a href=\"{href}\">{_html_text(label)}</a>")
            if item.get("open_command"):
                open_parts.append(f"<strong>Open command:</strong> <code>{_html_text(item.get('open_command'))}</code>")
            if open_parts:
                bits.append(f"<div style=\"margin-bottom:8px;\">{' | '.join(open_parts)}</div>")
            if item.get("reply_text"):
                reply_label = "Suggested reply" if item.get("source") == "attention" else "Draft reply"
                bits.append(
                    "<div style=\"margin-bottom:8px;background:#f8fafc;border-radius:10px;padding:10px 12px;\">"
                    f"<strong>{_html_text(reply_label)}:</strong><br>{_html_text(_trim_text(item.get('reply_text'), 280))}</div>"
                )
            order = item.get("order_enrichment") or {}
            if order.get("matched"):
                bits.append(
                    f"<div style=\"margin-bottom:6px;\"><strong>Order:</strong> {_html_text(order.get('product_title') or 'Unknown product')} "
                    f"(receipt {_html_text(order.get('receipt_id') or 'n/a')}, status {_html_text(order.get('order_status') or 'n/a')})</div>"
                )
            if item.get("tracking_line"):
                bits.append(f"<div><strong>Tracking:</strong> {_html_text(item.get('tracking_line'))}</div>")
            top_customer_cards.append(_nightly_card(f"{index}. {_html_text(item.get('title') or 'Customer action')}", "".join(bits)))

    new_thread_cards: list[str] = []
    if not new_thread_items:
        new_thread_cards.append("<p style=\"margin:0;color:#4b5563;\">No brand-new Etsy threads are waiting for first review right now.</p>")
    else:
        new_thread_cards.append(
            "<div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;margin:0 0 12px 0;color:#4b5563;\">"
            f"{_html_text(len(new_thread_items))} new Etsy thread(s) still need first review. "
            "They are intentionally collapsed so tonight's email stays focused on actions already on your side."
            "</div>"
        )
        new_thread_cards.append(
            "<p style=\"margin:0;color:#4b5563;\">Next step: run <code>customer threads</code> when you want to triage the fresh inbox.</p>"
        )

    status_cards: list[str] = []
    waiting_count = int(counts.get("customer_waiting_on_customer") or 0)
    followup_count = int(counts.get("customer_follow_up_items") or 0)
    customer_sync = _format_local_clock(payload.get("customer_thread_sync_generated_at"))
    if not waiting_count and not followup_count:
        status_cards.append("<p style=\"margin:0;color:#4b5563;\">No customer followups are currently in motion.</p>")
    else:
        status_cards.append(
            "<div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;margin:0 0 12px 0;color:#4b5563;\">"
            f"<strong>Actions already in motion:</strong> {_html_text(followup_count)}<br>"
            f"<strong>Waiting on the customer:</strong> {_html_text(waiting_count)}"
            f"{'<br><strong>Customer thread sync:</strong> ' + _html_text(customer_sync) if customer_sync else ''}"
            "</div>"
        )
        if waiting_count > 0:
            status_cards.append(
                "<p style=\"margin:0;color:#4b5563;\">Waiting-on-customer threads are intentionally omitted from tonight's action queue.</p>"
            )

    replacement_blocks: list[str] = []
    if not replacement_label_items:
        replacement_blocks.append("<p style=\"margin:0;color:#4b5563;\">No resend cases are at buy-label-now yet.</p>")
    else:
        for item in replacement_label_items:
            order = item.get("order_enrichment") or {}
            body = [
                "<div style=\"margin-bottom:8px;\">Buy a replacement label now.</div>",
            ]
            if order.get("matched"):
                body.append(
                    f"<div style=\"margin-bottom:8px;\"><strong>Order:</strong> {_html_text(order.get('product_title') or 'Unknown product')} "
                    f"(receipt {_html_text(order.get('receipt_id') or 'n/a')})</div>"
                )
            body.append(
                "<div style=\"background:#f8fafc;border-radius:10px;padding:10px 12px;\">"
                "<strong>Reply after purchase:</strong><br>"
                f"{_html_text('A replacement is on the way and I’ll send tracking as soon as it updates.')}</div>"
            )
            replacement_blocks.append(_nightly_card(str(item.get("title") or "Replacement"), "".join(body)))

    orders_blocks: list[str] = []
    if not orders_to_pack:
        orders_blocks.append("<p style=\"margin:0;color:#4b5563;\">No non-custom duck orders are currently open for packing.</p>")
    else:
        etsy_total = sum(int((item.get("by_channel") or {}).get("etsy", 0) or 0) for item in orders_to_pack)
        shopify_total = sum(int((item.get("by_channel") or {}).get("shopify", 0) or 0) for item in orders_to_pack)
        grand_total = sum(int(item.get("total_quantity") or 0) for item in orders_to_pack)
        orders_blocks.append(
            "<div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;margin-bottom:12px;\">"
                f"<strong>Totals:</strong> Etsy {_html_text(etsy_total)} / Shopify {_html_text(shopify_total)} / Total units {_html_text(grand_total)}"
                "</div>"
        )
        snapshot_summary = _order_snapshot_refresh_summary(payload)
        if snapshot_summary:
            orders_blocks.append(
                "<div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;margin-bottom:12px;color:#4b5563;\">"
                f"<strong>Snapshot freshness:</strong> {_html_text(snapshot_summary)}"
                "</div>"
            )
        rows = []
        for item in orders_to_pack:
            channels = item.get("by_channel") or {}
            raw_title = str(item.get("product_title") or "Duck").strip() or "Duck"
            display_title = format_operator_duck_name(raw_title, limit=36)
            option_summary = str(item.get("option_summary") or "").strip()
            order_refs = [str(value).strip() for value in (item.get("order_refs") or []) if str(value).strip()]
            duck_cell = _html_text(display_title)
            if order_refs:
                refs_display = ", ".join(order_refs[:3])
                if len(order_refs) > 3:
                    refs_display += f", +{len(order_refs) - 3} more"
                duck_cell += (
                    "<div style=\"font-size:11px;color:#6b7280;margin-top:4px;\">"
                    f"Orders: {_html_text(refs_display)}</div>"
                )
            if option_summary:
                duck_cell += (
                    "<div style=\"font-size:11px;color:#6b7280;margin-top:4px;\">"
                    f"{_html_text(option_summary)}</div>"
                )
            rows.append(
                "<tr>"
                f"<td title=\"{_html_text(raw_title)}\" style=\"padding:10px 12px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{duck_cell}</td>"
                f"<td style=\"padding:10px 12px;border-bottom:1px solid #e5e7eb;text-align:right;vertical-align:top;\">{_html_text(item.get('order_count') or 0)}</td>"
                f"<td style=\"padding:10px 12px;border-bottom:1px solid #e5e7eb;text-align:right;vertical-align:top;\">{_html_text(channels.get('etsy', 0))}</td>"
                f"<td style=\"padding:10px 12px;border-bottom:1px solid #e5e7eb;text-align:right;vertical-align:top;\">{_html_text(channels.get('shopify', 0))}</td>"
                f"<td style=\"padding:10px 12px;border-bottom:1px solid #e5e7eb;text-align:right;vertical-align:top;\">{_html_text(item.get('total_quantity') or 0)}</td>"
                "</tr>"
            )
        table = (
            "<div style=\"border:1px solid #e5e7eb;border-radius:14px;overflow:hidden;background:#fff;\">"
            "<table style=\"width:100%;border-collapse:collapse;font-size:13px;\">"
            "<thead style=\"background:#f8fafc;color:#374151;\">"
            "<tr>"
            "<th style=\"text-align:left;padding:10px 12px;\">Duck</th>"
            "<th style=\"text-align:right;padding:10px 12px;\">Orders</th>"
            "<th style=\"text-align:right;padding:10px 12px;\">Etsy</th>"
            "<th style=\"text-align:right;padding:10px 12px;\">Shopify</th>"
            "<th style=\"text-align:right;padding:10px 12px;\">Units</th>"
            "</tr>"
            "</thead>"
            f"<tbody>{''.join(rows)}</tbody>"
            "</table>"
            "</div>"
        )
        orders_blocks.append(table)

    custom_blocks: list[str] = []
    if not ready_cases and not blocked_cases and not open_custom_orders:
        custom_blocks.append("<p style=\"margin:0;color:#4b5563;\">No custom or novel duck design work is queued right now.</p>")
    else:
        if ready_cases:
            items = "".join(f"<li>{_html_text(case.get('summary') or 'Custom design brief')}</li>" for case in ready_cases)
            custom_blocks.append(
                _nightly_card("Ready briefs", f"<ul style=\"margin:0;padding-left:18px;\">{items}</ul>")
            )
        if blocked_cases:
            items = "".join(
                f"<li>{_html_text(case.get('summary') or 'Custom design brief')} ({_html_text('; '.join(case.get('open_questions') or []) or 'More design detail needed.')})</li>"
                for case in blocked_cases
            )
            custom_blocks.append(
                _nightly_card("Waiting on clarification", f"<ul style=\"margin:0;padding-left:18px;\">{items}</ul>")
            )
        if open_custom_orders:
            if build_candidates:
                items = "".join(f"<li>{_html_text(_format_custom_candidate_line(candidate))}</li>" for candidate in build_candidates)
            else:
                items = "".join(f"<li>{_html_text(_format_custom_order_line(order))}</li>" for order in open_custom_orders)
            custom_blocks.append(
                _nightly_card("Open custom orders", f"<ul style=\"margin:0;padding-left:18px;\">{items}</ul>")
            )

    watch_blocks: list[str] = []
    if not watch_items:
        watch_blocks.append("<p style=\"margin:0;color:#4b5563;\">Nothing is in a monitor-only state right now.</p>")
    else:
        for item in watch_items:
            tracking = item.get("tracking_enrichment") or {}
            resolution = item.get("resolution_enrichment") or {}
            tracking_parts = [str(tracking.get("status") or "unknown")]
            if tracking.get("live_status_label"):
                tracking_parts.append(f"live {tracking.get('live_status_label')}")
            if tracking.get("tracking_number"):
                tracking_parts.append(str(tracking.get("tracking_number")))
            suffix = f" - {resolution.get('summary') or ''}" if resolution.get("signals") else ""
            body = f"<div>{_html_text(' / '.join(tracking_parts) + suffix)}</div>"
            watch_blocks.append(_nightly_card(str(item.get("title") or "Watch item"), body))

    workflow_blocks: list[str] = []
    if not workflow_items:
        workflow_blocks.append("<p style=\"margin:0;color:#4b5563;\">No workflow follow-through items are staged right now.</p>")
    else:
        for item in workflow_items:
            body = (
                f"<div style=\"margin-bottom:8px;\"><strong>Status:</strong> {_html_text(item.get('summary') or item.get('state_reason') or 'needs follow-through')}</div>"
                + (
                    f"<div style=\"margin-bottom:8px;\"><strong>Why:</strong> {_html_text(item.get('root_cause'))}</div>"
                    if item.get("root_cause")
                    else ""
                )
                + (
                    f"<div style=\"margin-bottom:8px;\"><strong>Fix:</strong> {_html_text(item.get('fix_hint'))}</div>"
                    if item.get("fix_hint")
                    else ""
                )
                + (
                    f"<div style=\"margin-bottom:8px;\"><strong>Last receipt:</strong> {_html_text(item.get('latest_receipt'))}</div>"
                    if item.get("latest_receipt")
                    else ""
                )
                + (
                    f"<div style=\"margin-bottom:8px;\"><strong>Trail:</strong> {_html_text(item.get('recent_history'))}</div>"
                    if item.get("recent_history")
                    else ""
                )
                + f"<div><strong>Do:</strong> {_html_text(item.get('next_action') or 'Review this workflow state.')}</div>"
                + (
                    "<div style=\"margin-top:8px;\"><strong>Urgent items:</strong><ul style=\"margin:6px 0 0 18px;padding:0;\">"
                    + "".join(
                        "<li>"
                        f"{_html_text(urgent.get('title') or 'Quality gate item')} | "
                        f"{_html_text(urgent.get('decision') or 'review')} | "
                        f"{_html_text(urgent.get('priority') or 'medium')}"
                        f"{' | ' + _html_text(_trim_text(urgent.get('why'), 120)) if urgent.get('why') else ''}"
                        "</li>"
                        for urgent in (item.get("urgent_items") or [])
                    )
                    + "</ul></div>"
                    if item.get("urgent_items")
                    else ""
                )
                + (
                    f"<div style=\"margin-top:8px;\"><strong>Run:</strong> <code>{_html_text(item.get('command'))}</code></div>"
                    if item.get("command")
                    else ""
                )
            )
            workflow_blocks.append(_nightly_card(f"{_html_text(item.get('lane') or 'workflow')}: {_html_text(item.get('title') or 'Workflow follow-through')}", body))

    workflow_note_blocks: list[str] = []
    if workflow_notes:
        for item in workflow_notes:
            body = (
                f"<div style=\"margin-bottom:8px;\"><strong>Status:</strong> {_html_text(item.get('summary') or item.get('state_reason') or 'note')}</div>"
                + (
                    f"<div style=\"margin-bottom:8px;\"><strong>Last receipt:</strong> {_html_text(item.get('latest_receipt'))}</div>"
                    if item.get("latest_receipt")
                    else ""
                )
                + f"<div><strong>Note:</strong> {_html_text(item.get('next_action') or 'No action tonight.')}</div>"
            )
            workflow_note_blocks.append(_nightly_card(f"{_html_text(item.get('lane') or 'workflow')}: {_html_text(item.get('title') or 'Workflow note')}", body))

    workflow_notes_html = (
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">9. Workflow Notes</div>{''.join(workflow_note_blocks)}"
        if workflow_note_blocks
        else ""
    )

    return (
        "<html><body style=\"margin:0;padding:0;background:#f3f4f6;color:#111827;\">"
        "<div style=\"max-width:860px;margin:0 auto;padding:24px 16px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;line-height:1.5;\">"
        "<div style=\"background:linear-gradient(135deg,#111827,#1f2937);color:#fff;border-radius:18px;padding:24px 24px 18px 24px;margin-bottom:18px;\">"
        "<div style=\"font-size:28px;font-weight:800;margin-bottom:6px;\">Duck Ops Tonight</div>"
        f"<div style=\"font-size:14px;color:#d1d5db;\">Prepared at {_html_text(payload.get('generated_at'))}</div>"
        "</div>"
        f"<div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px;\">{stat_grid}</div>"
        f"<div style=\"font-size:22px;font-weight:800;margin:8px 0 12px 0;\">0. Strategic Focus</div><div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:14px;padding:16px;margin:0 0 18px 0;\">{''.join(strategy_blocks)}</div>"
        f"<div style=\"font-size:22px;font-weight:800;margin:8px 0 12px 0;\">1. Top Customer Actions Tonight</div>{''.join(top_customer_cards)}"
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">2. New Customer Threads</div>{''.join(new_thread_cards)}"
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">3. Customer Status Counts</div>{''.join(status_cards)}"
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">4. Buy Replacement Labels Now</div>{''.join(replacement_blocks)}"
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">5. Orders To Pack</div>{''.join(orders_blocks)}"
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">6. Custom / Novel Ducks To Make</div>{''.join(custom_blocks)}"
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">7. Watch List</div>{''.join(watch_blocks)}"
        f"<div style=\"font-size:22px;font-weight:800;margin:18px 0 12px 0;\">8. Workflow Follow-Through</div>{''.join(workflow_blocks)}"
        f"{workflow_notes_html}"
        "<div style=\"font-size:12px;color:#6b7280;margin-top:20px;text-align:center;\">OpenClaw operator summary</div>"
        "</div></body></html>"
    )
