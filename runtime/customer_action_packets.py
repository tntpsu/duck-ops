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


def _suggested_customer_reply(packet: dict[str, Any]) -> str | None:
    packet_type = str(packet.get("packet_type") or "").strip()
    order = packet.get("order_enrichment") or {}
    tracking = packet.get("tracking_enrichment") or {}
    tracking_number = str(tracking.get("tracking_number") or "").strip()
    tracking_line = f" I found tracking `{tracking_number}` on the order." if tracking_number else ""
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
    if packet_type == "replacement":
        if approved_recovery_action in {"replacement", "resend"}:
            return "Buy the replacement label and reply that a replacement is being sent."
        return "Decide whether you want to replace or refund before replying."
    if packet_type == "refund":
        if approved_recovery_action == "refund":
            return "Process the refund, then reply that you refunded the order."
        return "Decide whether you want to refund before replying."
    if packet_type == "wait_for_tracking":
        return "Do not resend yet. Recheck tracking first, then reply only if the package stops moving."
    if packet_type == "reply" and _is_custom_product(packet.get("order_enrichment") or {}):
        return "Read the thread and answer the custom-design question. Ask for reference photos, colors, and deadline if they are missing."
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


def build_customer_action_packets(customer_cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    packets: list[dict[str, Any]] = []
    for raw_case in customer_cases:
        case = _normalized_case(raw_case)
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

    packets.sort(
        key=lambda packet: (
            {"high": 0, "medium": 1, "low": 2}.get(str(packet.get("priority") or "medium"), 9),
            {"reply": 0, "replacement": 1, "refund": 2, "wait_for_tracking": 3}.get(packet.get("packet_type"), 9),
            str(packet.get("title") or "").lower(),
        )
    )
    return packets


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
