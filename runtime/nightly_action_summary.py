#!/usr/bin/env python3
"""
Nightly action summary builder for Duck Ops.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


PACKET_TYPE_ORDER = {
    "replacement": 0,
    "refund": 1,
    "reply": 2,
    "wait_for_tracking": 3,
}


def _priority_rank(value: str | None) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(value or "medium").lower(), 9)


def _trim_text(value: str | None, limit: int = 180) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _reply_recommendation(item: dict[str, Any]) -> str | None:
    return str(item.get("suggested_reply") or "").strip() or None


def _operator_recommendation(item: dict[str, Any]) -> str:
    return str(item.get("operator_guidance") or "").strip() or "Review the case and choose the next move."


def _format_tracking_line(item: dict[str, Any]) -> str | None:
    tracking = item.get("tracking_enrichment") or {}
    status = str(tracking.get("status") or "").strip()
    if not status:
        return None
    tracking_number = str(tracking.get("tracking_number") or "").strip()
    carrier = str(tracking.get("carrier") or "").strip()
    parts = [status.replace("_", " ")]
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

    if summary and custom_type and not summary.lower().startswith(custom_type.lower()):
        detail = f"{custom_type}: {summary}"
    else:
        detail = summary or custom_type or "custom details still need review"

    qty_text = f"{quantity}x" if quantity > 1 else "1x"
    return f"{buyer}: {qty_text} {detail} ({product_title} on {order.get('channel')})"


def _merge_attention_packets(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        key = str(item.get("source_artifact_id") or item.get("packet_id") or "")
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
    orders_to_pack = packing_summary.get("orders_to_pack") or []

    payload = {
        "generated_at": now_local.isoformat(),
        "summary_date": now_local.strftime("%Y-%m-%d"),
        "send_after": send_after.isoformat(),
        "send_window_open": now_local >= send_after,
        "counts": {
            "customer_attention_items": len(attention_items),
            "customer_reply_items": len(attention_items),
            "replacement_labels_now": len(replacement_label_items),
            "orders_to_pack_titles": len(orders_to_pack),
            "orders_to_pack_units": sum(int(item.get("total_quantity") or 0) for item in orders_to_pack),
            "custom_ready_cases": len(ready_custom_cases),
            "custom_blocked_cases": len(blocked_custom_cases),
            "custom_order_lines": len(custom_orders),
            "watch_items": len(watch_items),
        },
        "sections": {
            "customer_issues_needing_attention": attention_items,
            "customer_issues_needing_reply": attention_items,
            "buy_replacement_labels_now": replacement_label_items,
            "orders_to_pack": orders_to_pack,
            "custom_novel_ducks_to_make": {
                "ready_cases": ready_custom_cases,
                "blocked_cases": blocked_custom_cases,
                "open_custom_orders": custom_orders,
            },
            "watch_list": watch_items,
        },
    }
    return payload


def render_nightly_action_summary_markdown(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") or {}
    sections = payload.get("sections") or {}

    lines = [
        "# Duck Ops Tonight",
        "",
        f"Prepared at {payload.get('generated_at')}",
        "",
        "Tonight at a glance:",
        f"- Customer issues needing attention: {counts.get('customer_attention_items', 0)}",
        f"- Replacement labels to buy now: {counts.get('replacement_labels_now', 0)}",
        f"- Non-custom ducks to pack: {counts.get('orders_to_pack_units', 0)} units across {counts.get('orders_to_pack_titles', 0)} ducks",
        f"- Custom ducks to make: {counts.get('custom_order_lines', 0)} open custom order lines",
        "",
        "## 1. Customer Issues Needing Attention",
        "",
    ]

    attention_items = sections.get("customer_issues_needing_attention") or sections.get("customer_issues_needing_reply") or []
    if not attention_items:
        lines.append("No customer issues need attention right now.")
    else:
        for index, item in enumerate(attention_items, start=1):
            order = item.get("order_enrichment") or {}
            tracking_line = _format_tracking_line(item)
            title = item.get("title") or "Customer issue"
            summary = item.get("latest_message_preview") or item.get("customer_summary")
            lines.append(f"### {index}. {title}")
            lines.append("")
            lines.append(f"- Priority: {str(item.get('priority') or '').upper()}")
            lines.append(f"- What happened: {_trim_text(summary, 320)}")
            lines.append(f"- What to do: {_operator_recommendation(item)}")
            suggested_reply = _reply_recommendation(item)
            if suggested_reply:
                lines.append(f"- Suggested reply: \"{suggested_reply}\"")
            if order.get("matched"):
                lines.append(
                    f"- Order: {order.get('product_title') or 'Unknown product'}"
                    f" (receipt {order.get('receipt_id') or 'n/a'}, status {order.get('order_status') or 'n/a'})"
                )
            if tracking_line:
                lines.append(f"- Tracking: {tracking_line}")
            lines.append("")

    lines.extend(["", "## 2. Buy Replacement Labels Now", ""])
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

    lines.extend(["", "## 3. Orders To Pack", ""])
    orders_to_pack = sections.get("orders_to_pack") or []
    if not orders_to_pack:
        lines.append("No non-custom duck orders are currently open for packing.")
    else:
        etsy_total = sum(int((item.get("by_channel") or {}).get("etsy", 0) or 0) for item in orders_to_pack)
        shopify_total = sum(int((item.get("by_channel") or {}).get("shopify", 0) or 0) for item in orders_to_pack)
        grand_total = sum(int(item.get("total_quantity") or 0) for item in orders_to_pack)
        lines.extend(
            [
                "| Duck | Etsy | Shopify | Total |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for item in orders_to_pack:
            channels = item.get("by_channel") or {}
            lines.append(
                f"| {item.get('product_title')} | {channels.get('etsy', 0)} | {channels.get('shopify', 0)} | {item.get('total_quantity', 0)} |"
            )
        lines.append(f"| **Total** | **{etsy_total}** | **{shopify_total}** | **{grand_total}** |")

    lines.extend(["", "## 4. Custom / Novel Ducks To Make", ""])
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
            for order in open_custom_orders:
                lines.append(f"- {_format_custom_order_line(order)}")

    lines.extend(["", "## 5. Watch List", ""])
    watch_items = sections.get("watch_list") or []
    if not watch_items:
        lines.append("Nothing is in a monitor-only state right now.")
    else:
        for item in watch_items:
            tracking = item.get("tracking_enrichment") or {}
            resolution = item.get("resolution_enrichment") or {}
            tracking_line = f"`{tracking.get('status') or 'unknown'}`"
            if tracking.get("tracking_number"):
                tracking_line += f" ({tracking.get('tracking_number')})"
            suffix = ""
            if resolution.get("signals"):
                suffix = f" - {resolution.get('summary') or ''}"
            lines.append(f"- {item.get('title')}: {tracking_line}{suffix}")

    lines.append("")
    return "\n".join(lines)
