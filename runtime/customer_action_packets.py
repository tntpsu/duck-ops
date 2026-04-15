#!/usr/bin/env python3
"""
Staged customer action packets for Duck Ops.

These packets translate enriched customer cases into clearer operational next
steps without taking live customer or order actions automatically.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


REPLY_ACTIONS = {
    "reply_recommended",
    "reply_with_context",
    "refund_review",
    "replacement_review",
    "refund_or_replacement_review",
    "escalate",
}
TRACKING_STATUSES = {
    "tracking_available",
    "tracking_number_in_message",
    "shipped_without_tracking_details",
}
RECOVERY_REVIEW_ACTIONS = {
    "refund_review",
    "replacement_review",
    "refund_or_replacement_review",
}
SUPPRESSED_BROWSER_FOLLOW_UP_STATES = {
    "reply_drafted",
    "waiting_on_customer",
    "resolved",
}


def _slugify(value: str) -> str:
    lowered = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    while "--" in lowered:
        lowered = lowered.replace("--", "-")
    return lowered.strip("-") or "unknown"


def _trim_text(value: str | None, limit: int = 220) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _is_custom_product(order: dict[str, Any]) -> bool:
    title = str(order.get("product_title") or "").lower()
    return "custom" in title or "build your custom" in title


def _tracking_live_context(packet: dict[str, Any]) -> tuple[str | None, str | None]:
    tracking = packet.get("tracking_enrichment") or {}
    label = str(tracking.get("live_status_label") or "").strip() or None
    action = str(tracking.get("live_status_recommended_action") or "").strip() or None
    return label, action


def _suggested_customer_reply(packet: dict[str, Any]) -> str | None:
    packet_type = str(packet.get("packet_type") or "").strip()
    order = packet.get("order_enrichment") or {}
    tracking = packet.get("tracking_enrichment") or {}
    tracking_number = str(tracking.get("tracking_number") or "").strip()
    tracking_line = f" I found tracking `{tracking_number}` on the order." if tracking_number else ""
    live_status_label, live_action = _tracking_live_context(packet)
    if packet_type == "reply" and live_action == "reply_with_delivery_context":
        return (
            "Hi, thanks for reaching out."
            f"{tracking_line} USPS currently shows the package as delivered."
            " Please double-check around the mailbox, porch, side door, and with anyone at the address who may have brought it in."
            " If it still does not turn up, reply here and I’ll help with the next step."
        )
    if packet_type == "reply" and live_action == "wait_same_day":
        return (
            "Hi, thanks for reaching out."
            f"{tracking_line} USPS shows the package is out for delivery today."
            " I would give it until the end of the day, and if it still does not arrive, message me again and I’ll help right away."
        )
    if packet_type == "reply" and live_action == "reply_with_pickup_context":
        return (
            "Hi, thanks for reaching out."
            f"{tracking_line} USPS shows the package is available for pickup."
            " Please check your pickup notice or local post office, and if anything looks wrong, let me know so I can help."
        )
    if packet_type == "reply" and live_action == "wait_for_tracking":
        return (
            "Hi, thanks for reaching out."
            f"{tracking_line} USPS still shows the package moving through the network."
            " I’m keeping an eye on it, and I’d give it a little more time before we replace it."
            " If it stops moving or the delivery window passes, message me again and I’ll take the next step."
        )
    if packet_type == "reply" and live_action == "review_replacement_or_refund":
        return (
            "Hi, thanks for reaching out."
            f"{tracking_line} USPS is showing an exception on the shipment."
            " Please confirm the delivery address and whether you would prefer a replacement or refund if the package does not recover."
        )
    if packet_type == "replacement":
        return (
            "Hi, I’m sorry this arrived damaged or wasn’t right."
            f"{tracking_line} If you can, send a quick photo and confirm whether you want a replacement."
        )
    if packet_type == "refund":
        return (
            "Hi, I’m sorry this wasn’t what you expected."
            " I want to make it right. Please confirm whether you’d prefer a refund."
        )
    if packet_type == "reply" and _is_custom_product(order):
        return (
            "Hi, thanks for reaching out about your custom duck."
            " Please send any reference photos, color changes, name text, or deadline details you want me to use."
        )
    if packet_type == "reply" and tracking_number:
        return (
            "Hi, thanks for reaching out."
            f"{tracking_line} Can you tell me what went wrong or what you need help with so I can make it right?"
        )
    if packet_type == "reply":
        return (
            "Hi, thanks for reaching out. I want to help."
            " Please send your order number and a quick note about what went wrong or what you want changed."
        )
    return None


def _operator_guidance(packet: dict[str, Any]) -> str:
    packet_type = str(packet.get("packet_type") or "").strip()
    approved_recovery_action = str(packet.get("approved_recovery_action") or "").strip().lower()
    live_status_label, live_action = _tracking_live_context(packet)
    if packet_type == "replacement":
        if approved_recovery_action in {"replacement", "resend"}:
            return "Buy the replacement label and reply that a replacement is being sent."
        if live_action == "reply_with_delivery_context":
            return "Reply with delivered-context first. Do not buy a replacement label until the delivered scan has been checked with the customer."
        if live_action == "wait_same_day":
            return "USPS shows out for delivery today. Wait until tonight before replacing anything."
        if live_action == "wait_for_tracking":
            return "USPS still shows movement. Do not buy a replacement label yet."
        if live_action == "review_replacement_or_refund":
            return "USPS shows an exception. Confirm address and decide whether this should become a refund or replacement."
        return "Decide whether you want to replace or refund before replying."
    if packet_type == "refund":
        if approved_recovery_action == "refund":
            return "Process the refund, then reply that you refunded the order."
        return "Decide whether you want to refund before replying."
    if packet_type == "wait_for_tracking":
        if live_action == "reply_with_delivery_context":
            return "USPS now shows delivered. Reply with delivery context instead of keeping this as a generic watch item."
        if live_action == "wait_same_day":
            return "USPS shows out for delivery today. Wait until the end of the day before taking any recovery action."
        if live_action == "reply_with_pickup_context":
            return "Reply that USPS is holding the package for pickup and ask the customer to check the local post office."
        if live_action == "review_replacement_or_refund":
            return "Tracking shows an exception. Move this out of watch mode and review replacement or refund."
        return "Do not resend yet. Recheck tracking first, then reply only if the package stops moving."
    if packet_type == "reply" and _is_custom_product(packet.get("order_enrichment") or {}):
        return "Read the thread and answer the custom-design question. Ask for reference photos, colors, and deadline if they are missing."
    if packet_type == "reply" and live_status_label:
        if live_action == "reply_with_delivery_context":
            return "Reply with the delivered scan context and ask the customer to recheck the delivery spot before escalating."
        if live_action == "wait_same_day":
            return "Reply that USPS still has it out for delivery today and set the expectation to check back tonight."
        if live_action == "reply_with_pickup_context":
            return "Reply that USPS is holding the package for pickup and ask the customer to confirm with the post office."
        if live_action == "wait_for_tracking":
            return "Reply with current tracking context and do not escalate into resend/refund yet."
        if live_action == "review_replacement_or_refund":
            return "Reply with the USPS exception context, confirm the address, and decide whether to replace or refund."
    if packet_type == "reply":
        return "Open the thread and send a same-day reply with context or a clarifying question."
    return "Review the case and decide the next customer-facing move."


def _normalized_case(record: dict[str, Any]) -> dict[str, Any]:
    if record.get("item_type") == "customer_case":
        details = record.get("details") or {}
        return {
            "artifact_id": record.get("source_artifact_id"),
            "priority": record.get("priority"),
            "channel": details.get("channel"),
            "case_type": details.get("issue_type"),
            "issue_type": details.get("issue_type"),
            "recommended_action": record.get("recommended_action"),
            "recommended_recovery_action": details.get("recommended_recovery_action"),
            "customer_summary": record.get("summary"),
            "context_state": details.get("context_state"),
            "response_recommendation": details.get("response_recommendation") or {},
            "recovery_recommendation": details.get("recovery_recommendation") or {},
            "missing_context": details.get("missing_context") or [],
            "order_enrichment": details.get("order_enrichment") or {},
            "tracking_enrichment": details.get("tracking_enrichment") or {},
            "resolution_enrichment": details.get("resolution_enrichment") or {},
            "operator_decision": details.get("operator_decision") or {},
            "approved_recovery_action": details.get("approved_recovery_action"),
            "conversation_contact": details.get("conversation_contact"),
            "conversation_thread_key": details.get("conversation_thread_key"),
            "browser_url_candidates": details.get("browser_url_candidates") or [],
            "grouped_message_count": details.get("grouped_message_count"),
            "latest_message_preview": details.get("latest_message_preview"),
            "source_refs": record.get("source_refs") or [],
        }
    return record


def _resolution_signals(case: dict[str, Any]) -> set[str]:
    return {
        str(signal).strip()
        for signal in ((case.get("resolution_enrichment") or {}).get("signals") or [])
        if str(signal).strip()
    }


def _suppress_reply_packet(case: dict[str, Any], action: str) -> bool:
    signals = _resolution_signals(case)
    approved_recovery_action = str(case.get("approved_recovery_action") or "").strip().lower()
    if approved_recovery_action in {"replacement", "refund", "wait"}:
        return True
    if "public_review_reply_posted" in signals:
        return True
    return "refund_detected" in signals and action in RECOVERY_REVIEW_ACTIONS


def _suppress_refund_packet(case: dict[str, Any]) -> bool:
    approved_recovery_action = str(case.get("approved_recovery_action") or "").strip().lower()
    if approved_recovery_action in {"wait", "replacement"}:
        return True
    return "refund_detected" in _resolution_signals(case)


def _replacement_resolution_override(case: dict[str, Any]) -> dict[str, str] | None:
    signals = _resolution_signals(case)
    approved_recovery_action = str(case.get("approved_recovery_action") or "").strip().lower()
    if approved_recovery_action == "refund":
        return None
    if approved_recovery_action == "wait":
        return {
            "status": "waiting_by_operator_decision",
            "next_operator_action": "recheck_later_after_wait_decision",
            "next_physical_action": "do_not_buy_label_yet",
            "reason": "Operator already chose to wait, so Duck Ops should not prompt for a replacement label yet.",
        }
    if "refund_detected" in signals:
        return None
    if "multiple_shipments_present" in signals:
        return {
            "status": "possible_reship_already_sent",
            "next_operator_action": "confirm_if_reship_already_sent",
            "next_physical_action": "do_not_buy_label_yet",
            "reason": "The Etsy receipt already shows multiple shipments, so Duck Ops should confirm whether a resend already went out before buying another label.",
        }
    return {}


def _browser_capture_index(
    browser_captures: dict[str, Any] | list[dict[str, Any]] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
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


def _normalized_browser_follow_up_state(capture: dict[str, Any] | None) -> str | None:
    if not capture:
        return None
    follow_up = str(capture.get("follow_up_state") or "").strip().lower().replace(" ", "_")
    draft_reply = str(capture.get("draft_reply") or "").strip()
    if follow_up in {"resolved", "done"}:
        return "resolved"
    if follow_up == "needs_reply" and draft_reply:
        return "reply_drafted"
    if draft_reply and not follow_up:
        return "reply_drafted"
    return follow_up or None


def _browser_capture_for_case(
    case: dict[str, Any],
    by_thread_key: dict[str, dict[str, Any]],
    by_source_artifact_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    normalized = _normalized_case(case)
    thread_key = str(normalized.get("conversation_thread_key") or "").strip()
    source_artifact_id = str(normalized.get("artifact_id") or "").strip()
    return by_thread_key.get(thread_key) or by_source_artifact_id.get(source_artifact_id) or {}


def _base_packet(case: dict[str, Any], packet_type: str) -> dict[str, Any]:
    normalized = _normalized_case(case)
    order = normalized.get("order_enrichment") or {}
    tracking = normalized.get("tracking_enrichment") or {}
    product_title = order.get("product_title")
    conversation_contact = normalized.get("conversation_contact")
    customer_summary = normalized.get("customer_summary") or ""
    display_title = product_title or (
        f"Etsy conversation - {conversation_contact}"
        if conversation_contact
        else f"Customer {packet_type.replace('_', ' ')}"
    )
    return {
        "packet_id": f"{packet_type}_packet::{_slugify(normalized.get('artifact_id'))}",
        "packet_type": packet_type,
        "source_artifact_id": normalized.get("artifact_id"),
        "generated_at": datetime.now().astimezone().isoformat(),
        "priority": normalized.get("priority") or "medium",
        "channel": normalized.get("channel"),
        "case_type": normalized.get("case_type") or normalized.get("issue_type"),
        "recommended_action": normalized.get("recommended_action"),
        "recommended_recovery_action": normalized.get("recommended_recovery_action"),
        "title": display_title,
        "customer_summary": _trim_text(customer_summary),
        "latest_message_preview": _trim_text(
            normalized.get("latest_message_preview") or customer_summary,
            280,
        ),
        "conversation_contact": normalized.get("conversation_contact"),
        "conversation_thread_key": normalized.get("conversation_thread_key"),
        "browser_url_candidates": normalized.get("browser_url_candidates") or [],
        "grouped_message_count": normalized.get("grouped_message_count"),
        "context_state": normalized.get("context_state"),
        "response_recommendation": normalized.get("response_recommendation") or {},
        "recovery_recommendation": normalized.get("recovery_recommendation") or {},
        "missing_context": normalized.get("missing_context") or [],
        "order_enrichment": order,
        "tracking_enrichment": tracking,
        "resolution_enrichment": normalized.get("resolution_enrichment") or {},
        "operator_decision": normalized.get("operator_decision") or {},
        "approved_recovery_action": normalized.get("approved_recovery_action"),
        "source_refs": normalized.get("source_refs") or [],
    }


def _priority_rank(value: str | None) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(value or "medium").lower(), 9)


def _merge_thread_packets(packets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    ungrouped: list[dict[str, Any]] = []
    for packet in packets:
        thread_key = str(packet.get("conversation_thread_key") or "").strip()
        if not thread_key:
            ungrouped.append(packet)
            continue
        group_key = (str(packet.get("packet_type") or "").strip(), thread_key)
        existing = grouped.get(group_key)
        if existing is None:
            seed = dict(packet)
            seed["source_artifact_ids"] = [packet.get("source_artifact_id")] if packet.get("source_artifact_id") else []
            grouped[group_key] = seed
            continue

        source_artifact_id = packet.get("source_artifact_id")
        if source_artifact_id and source_artifact_id not in existing["source_artifact_ids"]:
            existing["source_artifact_ids"].append(source_artifact_id)
        existing["grouped_message_count"] = max(
            int(existing.get("grouped_message_count") or 1),
            int(packet.get("grouped_message_count") or 1),
        )
        for url in packet.get("browser_url_candidates") or []:
            normalized_url = str(url).strip()
            if normalized_url and normalized_url not in existing["browser_url_candidates"]:
                existing["browser_url_candidates"].append(normalized_url)
        if _priority_rank(packet.get("priority")) < _priority_rank(existing.get("priority")):
            replacement = dict(packet)
            replacement["source_artifact_ids"] = existing["source_artifact_ids"]
            grouped[group_key] = replacement

    merged = list(grouped.values()) + ungrouped
    merged.sort(
        key=lambda packet: (
            _priority_rank(packet.get("priority")),
            {"reply": 0, "replacement": 1, "refund": 2, "wait_for_tracking": 3}.get(packet.get("packet_type"), 9),
            str(packet.get("title") or "").lower(),
        )
    )
    return merged


def build_customer_action_packets(
    customer_cases: list[dict[str, Any]],
    browser_captures: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    captures_by_thread_key, captures_by_source_artifact_id = _browser_capture_index(browser_captures)
    packets: list[dict[str, Any]] = []
    for raw_case in customer_cases:
        case = _normalized_case(raw_case)
        browser_capture = _browser_capture_for_case(case, captures_by_thread_key, captures_by_source_artifact_id)
        browser_follow_up_state = _normalized_browser_follow_up_state(browser_capture)
        browser_review_status = str(browser_capture.get("browser_review_status") or "").strip() or None
        browser_draft_reply = str(browser_capture.get("draft_reply") or "").strip() or None

        if browser_follow_up_state in SUPPRESSED_BROWSER_FOLLOW_UP_STATES:
            continue

        action = str(case.get("recommended_action") or "").strip()
        approved_recovery_action = str(case.get("approved_recovery_action") or "").strip().lower()
        response_recommendation = case.get("response_recommendation") or {}
        recovery_recommendation = case.get("recovery_recommendation") or {}
        tracking_enrichment = case.get("tracking_enrichment") or {}
        issue_type = str(case.get("issue_type") or "").strip()

        if (
            action in REPLY_ACTIONS
            and response_recommendation.get("label") not in {None, "", "none"}
            and not _suppress_reply_packet(case, action)
        ):
            reply_packet = _base_packet(case, "reply")
            reply_packet.update(
                {
                    "browser_review_status": browser_review_status,
                    "follow_up_state": browser_follow_up_state,
                    "draft_reply": browser_draft_reply,
                    "browser_captured_at": browser_capture.get("captured_at"),
                }
            )
            reply_packet.update(
                {
                    "status": "reply_needed",
                    "next_operator_action": "review_customer_reply_path",
                    "next_physical_action": "none",
                    "reason": response_recommendation.get("reason"),
                }
            )
            reply_packet["operator_guidance"] = _operator_guidance(reply_packet)
            reply_packet["suggested_reply"] = _suggested_customer_reply(reply_packet)
            packets.append(reply_packet)

        if action in {"refund_review", "refund_or_replacement_review"} and not _suppress_refund_packet(case):
            refund_packet = _base_packet(case, "refund")
            refund_packet.update(
                {
                    "browser_review_status": browser_review_status,
                    "follow_up_state": browser_follow_up_state,
                    "draft_reply": browser_draft_reply,
                    "browser_captured_at": browser_capture.get("captured_at"),
                }
            )
            if approved_recovery_action == "refund":
                refund_packet.update(
                    {
                        "status": "issue_manual_refund_now",
                        "next_operator_action": "process_manual_refund_now",
                        "next_physical_action": "issue_refund_now",
                        "reason": "Operator already approved a refund, so Duck Ops should now treat this as a manual refund task rather than a fresh decision.",
                    }
                )
            else:
                refund_packet.update(
                    {
                        "status": "operator_confirmation_required",
                        "next_operator_action": "decide_refund_path",
                        "next_physical_action": "none",
                        "reason": recovery_recommendation.get("reason"),
                    }
                )
            refund_packet["operator_guidance"] = _operator_guidance(refund_packet)
            refund_packet["suggested_reply"] = _suggested_customer_reply(refund_packet)
            packets.append(refund_packet)

        if action in {"replacement_review", "refund_or_replacement_review"}:
            replacement_override = _replacement_resolution_override(case)
            if replacement_override is None:
                continue
            replacement_packet = _base_packet(case, "replacement")
            replacement_packet.update(
                {
                    "browser_review_status": browser_review_status,
                    "follow_up_state": browser_follow_up_state,
                    "draft_reply": browser_draft_reply,
                    "browser_captured_at": browser_capture.get("captured_at"),
                }
            )
            if replacement_override:
                replacement_packet.update(replacement_override)
            else:
                buy_label_now = approved_recovery_action in {"replacement", "resend"}
                replacement_packet.update(
                    {
                        "status": "buy_label_now" if buy_label_now else "operator_confirmation_required",
                        "next_operator_action": (
                            "buy_replacement_label_now" if buy_label_now else "confirm_resend_before_buying_label"
                        ),
                        "next_physical_action": "buy_label_now" if buy_label_now else "do_not_buy_label_yet",
                        "reason": recovery_recommendation.get("reason"),
                    }
                )
            replacement_packet["operator_guidance"] = _operator_guidance(replacement_packet)
            replacement_packet["suggested_reply"] = _suggested_customer_reply(replacement_packet)
            packets.append(replacement_packet)

        should_stage_wait_packet = (
            (
                issue_type == "shipping"
                and str(tracking_enrichment.get("status") or "").strip() in TRACKING_STATUSES
            )
            or approved_recovery_action == "wait"
        ) and action not in {"refund_review", "replacement_review", "refund_or_replacement_review", "escalate"}
        if should_stage_wait_packet:
            wait_packet = _base_packet(case, "wait_for_tracking")
            wait_packet.update(
                {
                    "browser_review_status": browser_review_status,
                    "follow_up_state": browser_follow_up_state,
                    "draft_reply": browser_draft_reply,
                    "browser_captured_at": browser_capture.get("captured_at"),
                }
            )
            wait_packet.update(
                {
                    "status": "watch" if approved_recovery_action != "wait" else "waiting_by_operator_decision",
                    "next_operator_action": (
                        "monitor_tracking_before_escalating"
                        if approved_recovery_action != "wait"
                        else "recheck_later_after_wait_decision"
                    ),
                    "next_physical_action": "do_not_buy_label_yet",
                    "reason": (
                        "Tracking exists, so this case should stay in a watch state until carrier progress becomes clearer."
                        if approved_recovery_action != "wait"
                        else "Operator already chose to wait, so Duck Ops should keep this case in a watch state."
                    ),
                }
            )
            wait_packet["operator_guidance"] = _operator_guidance(wait_packet)
            wait_packet["suggested_reply"] = _suggested_customer_reply(wait_packet)
            packets.append(wait_packet)

    return _merge_thread_packets(packets)


def render_customer_action_packets_markdown(packet_payload: dict[str, Any]) -> str:
    lines = [
        "# Duck Ops Customer Action Packets",
        "",
        f"- Generated at: `{packet_payload.get('generated_at')}`",
        f"- Packets: `{len(packet_payload.get('items') or [])}`",
        "",
    ]
    items = packet_payload.get("items") or []
    if not items:
        lines.append("No staged customer action packets right now.")
        lines.append("")
        return "\n".join(lines)

    for index, packet in enumerate(items, start=1):
        lines.extend(
            [
                f"## {index}. [{str(packet.get('priority') or '').upper()}] {packet.get('packet_type')} - {packet.get('title')}",
                "",
                f"- Status: `{packet.get('status')}`",
                f"- Channel: `{packet.get('channel')}`",
                f"- Next operator action: `{packet.get('next_operator_action')}`",
                f"- Next physical action: `{packet.get('next_physical_action')}`",
                f"- Reason: {packet.get('reason') or '(none)'}",
                f"- Customer summary: {packet.get('customer_summary') or '(none)'}",
            ]
        )
        order = packet.get("order_enrichment") or {}
        tracking = packet.get("tracking_enrichment") or {}
        resolution = packet.get("resolution_enrichment") or {}
        operator_decision = packet.get("operator_decision") or {}
        if order.get("matched"):
            lines.append(
                f"- Order: receipt `{order.get('receipt_id') or 'n/a'}`, product `{order.get('product_title') or 'n/a'}`, status `{order.get('order_status') or 'n/a'}`"
            )
        if tracking.get("status"):
            tracking_line = f"`{tracking.get('status')}`"
            if tracking.get("tracking_number"):
                tracking_line += f" ({tracking.get('tracking_number')})"
            lines.append(f"- Tracking: {tracking_line}")
        if resolution.get("signals"):
            lines.append(f"- Resolution history: `{resolution.get('status')}` - {resolution.get('summary') or ''}")
        if operator_decision.get("resolution"):
            lines.append(
                f"- Operator decision: `{operator_decision.get('resolution')}`"
                f" at `{operator_decision.get('recorded_at') or 'unknown'}`"
                f" - {operator_decision.get('note') or 'No note provided.'}"
            )
        lines.append("")
    return "\n".join(lines)
