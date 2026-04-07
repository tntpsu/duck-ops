#!/usr/bin/env python3
"""
Unified operator desk for Duck Ops.

This keeps the main business lanes visible in one place without replacing the
specialized queues that already exist.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def _trim_text(value: str | None, limit: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _customer_action_items(customer_packets: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in list((customer_packets or {}).get("items") or []):
        short_id = str(item.get("short_id") or "").strip()
        items.append(
            {
                **item,
                "detail_command": f"customer show {short_id}" if short_id else "customer status",
                "open_command": f"customer open {short_id}" if short_id else None,
            }
        )
    return items


def _browser_review_items(etsy_browser_sync: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in list((etsy_browser_sync or {}).get("items") or []):
        linked_short_id = str(item.get("linked_customer_short_id") or "").strip()
        items.append(
            {
                **item,
                "detail_command": f"customer show {linked_short_id}" if linked_short_id else None,
                "open_command": f"customer open {linked_short_id}" if linked_short_id else None,
            }
        )
    return items


def _custom_build_items(custom_build_candidates: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in list((custom_build_candidates or {}).get("items") or []):
        detail = _trim_text(item.get("custom_design_summary"), 140)
        order_ref = str(item.get("order_ref") or "").strip()
        channel = str(item.get("channel") or "").strip()
        items.append(
            {
                **item,
                "next_action_summary": (
                    "Create/update Google Task in `myjeepduck`."
                    if str(item.get("google_task_status") or "") == "created"
                    else "Stage this as a Google Task once Google credentials are live."
                ),
                "detail_summary": detail,
                "operator_hint": f"{channel} order {order_ref}" if channel or order_ref else "custom build candidate",
            }
        )
    return items


def _review_queue_items(review_queue: dict[str, Any] | None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    source_items = list((review_queue or {}).get("surfaced_items") or [])
    if not source_items:
        source_items = list((review_queue or {}).get("items") or [])
    for item in source_items:
        short_id = str(item.get("short_id") or "").strip()
        decision = str(item.get("decision") or "").strip()
        if decision == "publish_ready":
            approve_command = f"approve {short_id} because ..."
        elif decision == "needs_revision":
            approve_command = f"needs changes {short_id} because ..."
        elif decision == "discard":
            approve_command = f"discard {short_id} because ..."
        else:
            approve_command = f"agree {short_id}" if short_id else None
        items.append(
            {
                **item,
                "detail_command": f"why {short_id}" if short_id else None,
                "approve_command": approve_command,
            }
        )
    return items


def _print_queue_items(print_queue_candidates: dict[str, Any] | list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    payload_items = print_queue_candidates
    if isinstance(print_queue_candidates, dict):
        payload_items = print_queue_candidates.get("items") or []
    items = list(payload_items or [])
    return sorted(
        items,
        key=lambda item: (
            {"high": 0, "medium": 1, "low": 2}.get(str(item.get("priority") or "low").lower(), 9),
            -int(item.get("recent_demand") or 0),
            str(item.get("product_title") or "").lower(),
        ),
    )


def _build_next_actions(
    *,
    customer_items: list[dict[str, Any]],
    browser_items: list[dict[str, Any]],
    build_items: list[dict[str, Any]],
    pack_items: list[dict[str, Any]],
    stock_items: list[dict[str, Any]],
    review_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if customer_items:
        first = customer_items[0]
        actions.append(
            {
                "lane": "customer",
                "title": first.get("title") or "Customer issue",
                "summary": _trim_text(first.get("customer_summary") or first.get("title"), 120),
                "command": first.get("detail_command") or "customer status",
                "secondary_command": first.get("open_command"),
            }
        )
    unresolved_browser = [item for item in browser_items if not item.get("linked_customer_short_id")]
    if unresolved_browser:
        first = unresolved_browser[0]
        actions.append(
            {
                "lane": "etsy_thread",
                "title": first.get("conversation_contact") or "Etsy thread",
                "summary": _trim_text(first.get("latest_message_preview") or first.get("open_in_browser_hint"), 120),
                "command": first.get("primary_browser_url"),
                "secondary_command": None,
            }
        )
    if build_items:
        first = build_items[0]
        actions.append(
            {
                "lane": "custom_build",
                "title": first.get("buyer_name") or "Custom build",
                "summary": _trim_text(first.get("detail_summary") or first.get("custom_design_summary"), 120),
                "command": first.get("next_action_summary"),
                "secondary_command": None,
            }
        )
    if pack_items:
        first = pack_items[0]
        channels = first.get("by_channel") or {}
        actions.append(
            {
                "lane": "packing",
                "title": first.get("product_title") or "Pack tonight",
                "summary": f"Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {first.get('total_quantity', 0)}",
                "command": "Pack this duck tonight.",
                "secondary_command": None,
            }
        )
    if stock_items:
        first = stock_items[0]
        actions.append(
            {
                "lane": "stock_print",
                "title": first.get("product_title") or "Stock print candidate",
                "summary": f"{first.get('priority', 'low')} priority | recent demand {int(first.get('recent_demand') or 0)}",
                "command": "Check live stock and queue a replenishment print.",
                "secondary_command": None,
            }
        )
    if review_items:
        first = review_items[0]
        actions.append(
            {
                "lane": "creative_review",
                "title": first.get("title") or "Creative review",
                "summary": f"{first.get('decision') or 'pending'} | {first.get('priority') or 'medium'} priority",
                "command": first.get("detail_command") or "status",
                "secondary_command": first.get("approve_command"),
            }
        )
    return actions[:8]


def build_business_operator_desk(
    *,
    customer_packets: dict[str, Any],
    nightly_summary: dict[str, Any],
    etsy_browser_sync: dict[str, Any],
    custom_build_candidates: dict[str, Any],
    print_queue_candidates: dict[str, Any] | list[dict[str, Any]] | None,
    review_queue: dict[str, Any] | None,
) -> dict[str, Any]:
    review_items = _review_queue_items(review_queue)
    customer_items = _customer_action_items(customer_packets)
    browser_items = _browser_review_items(etsy_browser_sync)
    build_items = _custom_build_items(custom_build_candidates)
    stock_items = _print_queue_items(print_queue_candidates)
    counts = (nightly_summary or {}).get("counts") or {}
    pack_items = list(((nightly_summary or {}).get("sections") or {}).get("orders_to_pack") or [])
    review_queue_backlog = int((review_queue or {}).get("pending_count_all") or len((review_queue or {}).get("items") or []))
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "customer_packets": len(customer_items),
            "customer_attention_items": int(counts.get("customer_attention_items") or 0),
            "replacement_labels_now": int(counts.get("replacement_labels_now") or 0),
            "etsy_browser_threads": len(browser_items),
            "custom_build_candidates": len(build_items),
            "orders_to_pack_units": int(counts.get("orders_to_pack_units") or 0),
            "stock_print_candidates": len(stock_items),
            "review_queue_items": len(review_items),
            "review_queue_backlog": review_queue_backlog,
        },
        "next_actions": _build_next_actions(
            customer_items=customer_items,
            browser_items=browser_items,
            build_items=build_items,
            pack_items=pack_items,
            stock_items=stock_items,
            review_items=review_items,
        ),
        "sections": {
            "customer_packets": customer_items[:6],
            "etsy_browser_threads": browser_items[:6],
            "custom_build_candidates": build_items[:6],
            "orders_to_pack": pack_items[:8],
            "stock_print_candidates": stock_items[:6],
            "review_queue": review_items[:6],
        },
    }


def render_business_operator_desk_markdown(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") or {}
    sections = payload.get("sections") or {}
    lines = [
        "# Duck Ops Business Desk",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Customer attention items: `{counts.get('customer_attention_items', 0)}`",
        f"- Replacement labels now: `{counts.get('replacement_labels_now', 0)}`",
        f"- Etsy browser-review threads: `{counts.get('etsy_browser_threads', 0)}`",
        f"- Custom build candidates: `{counts.get('custom_build_candidates', 0)}`",
        f"- Non-custom units to pack: `{counts.get('orders_to_pack_units', 0)}`",
        f"- Print-soon candidates: `{counts.get('stock_print_candidates', 0)}`",
        f"- Creative/operator review items: `{counts.get('review_queue_items', 0)}`",
        f"- Older creative/operator backlog: `{max(0, int(counts.get('review_queue_backlog', 0)) - int(counts.get('review_queue_items', 0)))}`",
        "",
        "## Do Next",
        "",
    ]
    next_actions = payload.get("next_actions") or []
    if not next_actions:
        lines.append("No urgent next actions are staged right now.")
    else:
        for item in next_actions:
            command = item.get("command")
            secondary = item.get("secondary_command")
            command_text = f"`{command}`" if command and not str(command).startswith("http") else str(command or "(none)")
            secondary_text = (
                f" | then `{secondary}`"
                if secondary and not str(secondary).startswith("http")
                else f" | then {secondary}"
                if secondary
                else ""
            )
            lines.append(f"- {item.get('lane')}: {item.get('title')} - {_trim_text(item.get('summary'), 110)}")
            lines.append(f"  Do: {command_text}{secondary_text}")

    lines.extend(["", "## Customer Queue", ""])

    customer_items = sections.get("customer_packets") or []
    if not customer_items:
        lines.append("No customer packets are staged right now.")
    else:
        for item in customer_items:
            lines.append(
                f"- {item.get('short_id') or '?'} | {item.get('status') or 'unknown'} | {item.get('title') or 'Customer item'}"
            )
            if item.get("detail_command"):
                lines.append(f"  Command: `{item.get('detail_command')}`")
            if item.get("open_command"):
                lines.append(f"  Open: `{item.get('open_command')}`")
    lines.extend(["", "## Etsy Browser Review", ""])
    browser_items = sections.get("etsy_browser_threads") or []
    if not browser_items:
        lines.append("No Etsy browser-review threads are staged right now.")
    else:
        for item in browser_items:
            lines.append(
                f"- {item.get('conversation_contact') or 'Customer'} | {item.get('grouped_message_count') or 1} messages | {_trim_text(item.get('open_in_browser_hint'))}"
            )
            if item.get("open_command"):
                lines.append(f"  Command: `{item.get('open_command')}`")
            elif item.get("primary_browser_url"):
                lines.append(f"  Open: {item.get('primary_browser_url')}")

    lines.extend(["", "## Custom Builds", ""])
    build_items = sections.get("custom_build_candidates") or []
    if not build_items:
        lines.append("No custom build candidates are ready right now.")
    else:
        for item in build_items:
            lines.append(
                f"- {item.get('buyer_name') or 'Customer'} | {item.get('quantity') or 0}x | {_trim_text(item.get('custom_design_summary'))}"
            )
            if item.get("next_action_summary"):
                lines.append(f"  Next: {item.get('next_action_summary')}")

    lines.extend(["", "## Pack Tonight", ""])
    pack_items = sections.get("orders_to_pack") or []
    if not pack_items:
        lines.append("No non-custom ducks are open for packing right now.")
    else:
        for item in pack_items:
            channels = item.get("by_channel") or {}
            lines.append(
                f"- {item.get('product_title')} | {item.get('urgency_label') or 'Open'} | Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {item.get('total_quantity', 0)}"
            )

    lines.extend(["", "## Print Soon / Stock Watch", ""])
    stock_items = sections.get("stock_print_candidates") or []
    if not stock_items:
        lines.append("No stock-print candidates are staged right now.")
    else:
        for item in stock_items:
            lines.append(
                f"- {item.get('product_title')} | {item.get('priority') or 'low'} priority | recent demand {int(item.get('recent_demand') or 0)}"
            )
            lines.append(f"  Why: {_trim_text(item.get('why_now'), 120)}")

    lines.extend(["", "## Creative Review Queue", ""])
    review_items = sections.get("review_queue") or []
    if not review_items:
        backlog_total = int(counts.get("review_queue_backlog", 0))
        if backlog_total > 0:
            lines.append("No new creative/operator review items are surfaced right now.")
            lines.append("Older backlog exists. Use `status all` if you want to inspect it directly.")
        else:
            lines.append("No creative/operator review items are pending right now.")
    else:
        for item in review_items:
            lines.append(
                f"- {item.get('short_id') or item.get('operator_id') or '?'} | {item.get('review_status') or item.get('status') or 'pending'} | {_trim_text(item.get('title') or item.get('candidate_summary') or 'Review item')}"
            )
            if item.get("detail_command"):
                lines.append(f"  Detail: `{item.get('detail_command')}`")
            if item.get("approve_command"):
                lines.append(f"  Decide: `{item.get('approve_command')}`")

    lines.append("")
    return "\n".join(lines)


def render_business_section(payload: dict[str, Any], section: str) -> str:
    section_key = section.strip().lower()
    if section_key in {"status", "all", ""}:
        return render_business_operator_desk_markdown(payload)

    aliases = {
        "customer": "customer_packets",
        "customers": "customer_packets",
        "threads": "etsy_browser_threads",
        "etsy": "etsy_browser_threads",
        "builds": "custom_build_candidates",
        "custom": "custom_build_candidates",
        "packing": "orders_to_pack",
        "pack": "orders_to_pack",
        "stock": "stock_print_candidates",
        "print": "stock_print_candidates",
        "reviews": "review_queue",
        "creative": "review_queue",
        "next": "next_actions",
    }
    normalized = aliases.get(section_key, section_key)
    if normalized == "next_actions":
        lines = ["Duck Ops business next actions", ""]
        items = payload.get("next_actions") or []
        if not items:
            lines.append("No urgent next actions are staged right now.")
        else:
            for item in items:
                lines.append(f"- {item.get('lane')}: {item.get('title')} - {_trim_text(item.get('summary'), 120)}")
                if item.get("command"):
                    lines.append(f"  Do: {item.get('command')}")
                if item.get("secondary_command"):
                    lines.append(f"  Then: {item.get('secondary_command')}")
        return "\n".join(lines)

    sections = payload.get("sections") or {}
    items = sections.get(normalized) or []
    title_map = {
        "customer_packets": "Customer Queue",
        "etsy_browser_threads": "Etsy Browser Review",
        "custom_build_candidates": "Custom Builds",
        "orders_to_pack": "Pack Tonight",
        "stock_print_candidates": "Print Soon / Stock Watch",
        "review_queue": "Creative Review Queue",
    }
    lines = [f"Duck Ops {title_map.get(normalized, normalized)}", ""]
    if not items:
        if normalized == "review_queue" and int((payload.get("counts") or {}).get("review_queue_backlog", 0)) > 0:
            lines.append("No new creative/operator review items are surfaced right now.")
            lines.append("Use `status all` if you want to inspect older backlog.")
        else:
            lines.append("Nothing is staged in this section right now.")
        return "\n".join(lines)
    for item in items:
        if normalized == "customer_packets":
            lines.append(f"- {item.get('short_id')} | {item.get('status')} | {item.get('title')}")
            lines.append(f"  Summary: {_trim_text(item.get('customer_summary'), 120)}")
            if item.get("detail_command"):
                lines.append(f"  Detail: {item.get('detail_command')}")
        elif normalized == "etsy_browser_threads":
            lines.append(f"- {item.get('conversation_contact')} | {item.get('grouped_message_count')} messages")
            lines.append(f"  Open hint: {_trim_text(item.get('open_in_browser_hint'), 120)}")
            if item.get("open_command"):
                lines.append(f"  Command: {item.get('open_command')}")
            elif item.get("primary_browser_url"):
                lines.append(f"  Open: {item.get('primary_browser_url')}")
        elif normalized == "custom_build_candidates":
            lines.append(f"- {item.get('buyer_name')} | {item.get('quantity')}x | {_trim_text(item.get('custom_design_summary'), 120)}")
            if item.get("next_action_summary"):
                lines.append(f"  Next: {item.get('next_action_summary')}")
        elif normalized == "orders_to_pack":
            channels = item.get("by_channel") or {}
            lines.append(
                f"- {item.get('product_title')} | {item.get('urgency_label')} | Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {item.get('total_quantity', 0)}"
            )
        elif normalized == "stock_print_candidates":
            lines.append(
                f"- {item.get('product_title')} | {item.get('priority')} priority | recent demand {int(item.get('recent_demand') or 0)}"
            )
            lines.append(f"  Why: {_trim_text(item.get('why_now'), 120)}")
        elif normalized == "review_queue":
            lines.append(f"- {item.get('short_id')} | {item.get('decision')} | {item.get('title')}")
            if item.get("detail_command"):
                lines.append(f"  Detail: {item.get('detail_command')}")
        else:
            lines.append(f"- {_trim_text(str(item), 120)}")
    return "\n".join(lines)
