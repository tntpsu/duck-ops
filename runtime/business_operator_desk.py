#!/usr/bin/env python3
"""
Unified operator desk for Duck Ops.

This keeps the main business lanes visible in one place without replacing the
specialized queues that already exist.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from nightly_action_summary import format_operator_duck_name, load_master_roadmap_focus
from workflow_operator_summary import build_workflow_followthrough_items


def _trim_text(value: str | None, limit: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _display_duck_name(title: str | None, limit: int = 36) -> str:
    return format_operator_duck_name(title, limit=limit)


def _customer_action_items(customer_packets: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in list((customer_packets or {}).get("items") or []):
        short_id = str(item.get("short_id") or "").strip()
        items.append(
            {
                **item,
                "detail_command": f"customer show {short_id}" if short_id else "customer status",
                "open_command": f"customer open {short_id}" if short_id else None,
                "tracking_live_label": ((item.get("tracking_enrichment") or {}).get("live_status_label")),
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
        google_task_status = str(item.get("google_task_status") or "").strip()
        google_sync_status = str(item.get("google_task_sync_status") or "").strip()
        browser_state = str(item.get("browser_follow_up_state") or item.get("browser_review_status") or "").strip()
        if google_task_status == "created":
            next_action = "Open the live Google Task and move the concept forward."
            if browser_state == "waiting_on_customer":
                next_action = "Task is live, but this one is blocked on the customer answering the Etsy thread."
            elif browser_state == "reply_needed_before_design":
                next_action = "Task is live. Reply on Etsy first so the brief is locked before more concept work."
        elif browser_state == "waiting_on_customer":
            next_action = "Waiting on the customer. No design work tonight unless new Etsy context arrives."
        elif browser_state == "needs_reply":
            next_action = "Reply on Etsy, then create or update the Google Task once the brief is firm."
        else:
            next_action = "Stage this as a Google Task and move it into concept work."
        items.append(
            {
                **item,
                "next_action_summary": next_action,
                "detail_summary": detail,
                "operator_hint": f"{channel} order {order_ref}" if channel or order_ref else "custom build candidate",
                "google_task_sync_status": google_sync_status or None,
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


def _weekly_sale_items(weekly_sale_monitor: dict[str, Any] | None) -> list[dict[str, Any]]:
    items = list((weekly_sale_monitor or {}).get("items") or [])
    return sorted(
        items,
        key=lambda item: (
            {"weak": 0, "watch": 1, "working": 2, "strong": 3}.get(str(item.get("effectiveness") or "watch").lower(), 9),
            -int(item.get("sales_7d") or 0),
            -int(item.get("sales_30d") or 0),
        ),
    )


def _build_next_actions(
    *,
    customer_items: list[dict[str, Any]],
    browser_items: list[dict[str, Any]],
    build_items: list[dict[str, Any]],
    pack_items: list[dict[str, Any]],
    stock_items: list[dict[str, Any]],
    weekly_sale_items: list[dict[str, Any]],
    review_items: list[dict[str, Any]],
    workflow_items: list[dict[str, Any]],
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
    unresolved_browser = [
        item
        for item in browser_items
        if str(item.get("follow_up_state") or "") in {"needs_reply", "ready_for_task", "concept_in_progress"}
        or not item.get("linked_customer_short_id")
    ]
    if unresolved_browser:
        first = unresolved_browser[0]
        actions.append(
            {
                "lane": "etsy_thread",
                "title": first.get("conversation_contact") or "Etsy thread",
                "summary": _trim_text(first.get("recommended_next_action") or first.get("latest_message_preview") or first.get("open_in_browser_hint"), 120),
                "command": first.get("open_command") or first.get("primary_browser_url"),
                "secondary_command": (
                    f"reply: {first.get('draft_reply')}"
                    if str(first.get("draft_reply") or "").strip()
                    else None
                ),
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
        order_count = int(first.get("order_count") or 0)
        buyer_count = str(first.get("buyer_count_display") or first.get("buyer_count") or 0)
        option_summary = str(first.get("option_summary") or "").strip()
        summary = (
            f"{first.get('urgency_label') or 'Open'} | "
            f"Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {first.get('total_quantity', 0)}"
            f" | {order_count} order(s), {buyer_count} buyer(s)"
        )
        if option_summary:
            summary += f" | choices: {option_summary}"
        actions.append(
            {
                "lane": "packing",
                "title": _display_duck_name(first.get("product_title")) or "Pack tonight",
                "summary": summary,
                "command": "Pack this duck tonight.",
                "secondary_command": None,
            }
        )
    if stock_items:
        first = stock_items[0]
        actions.append(
            {
                "lane": "stock_print",
                "title": _display_duck_name(first.get("product_title")) or "Stock print candidate",
                "summary": f"{first.get('priority', 'low')} priority | recent demand {int(first.get('recent_demand') or 0)}",
                "command": "Check live stock and queue a replenishment print.",
                "secondary_command": None,
            }
        )
    weak_sale_items = [item for item in weekly_sale_items if str(item.get("effectiveness") or "") in {"weak", "watch"}]
    if weak_sale_items:
        first = weak_sale_items[0]
        actions.append(
            {
                "lane": "weekly_sale",
                "title": _display_duck_name(first.get("product_title")) or "Weekly sale review",
                "summary": (
                    f"{first.get('effectiveness')} | {first.get('discount')} | "
                    f"7d {int(first.get('sales_7d') or 0)} | 30d {int(first.get('sales_30d') or 0)}"
                ),
                "command": "Rewrite or rotate this sale item in the next weekly sale cycle.",
                "secondary_command": first.get("marketing_recommendation"),
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
    for item in workflow_items[:3]:
        actions.append(
            {
                "lane": item.get("lane") or "workflow",
                "title": item.get("title") or "Workflow follow-through",
                "summary": _trim_text(item.get("summary"), 120),
                "command": item.get("command") or item.get("next_action"),
                "secondary_command": (item.get("next_action") if item.get("command") else None),
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
    weekly_sale_monitor: dict[str, Any] | None,
    review_queue: dict[str, Any] | None,
    workflow_followthrough: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    review_items = _review_queue_items(review_queue)
    customer_items = _customer_action_items(customer_packets)
    browser_items = _browser_review_items(etsy_browser_sync)
    build_items = _custom_build_items(custom_build_candidates)
    stock_items = _print_queue_items(print_queue_candidates)
    weekly_sale_items = _weekly_sale_items(weekly_sale_monitor)
    workflow_items = list(workflow_followthrough or build_workflow_followthrough_items(limit=6))
    counts = (nightly_summary or {}).get("counts") or {}
    pack_items = list(((nightly_summary or {}).get("sections") or {}).get("orders_to_pack") or [])
    review_queue_backlog = int((review_queue or {}).get("pending_count_all") or len((review_queue or {}).get("items") or []))
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "strategy_focus": load_master_roadmap_focus(),
        "counts": {
            "customer_packets": len(customer_items),
            "customer_attention_items": int(counts.get("customer_attention_items") or 0),
            "replacement_labels_now": int(counts.get("replacement_labels_now") or 0),
            "etsy_browser_threads": len(browser_items),
            "threads_with_staged_reply": sum(1 for item in browser_items if str(item.get("draft_reply") or "").strip()),
            "threads_waiting_on_customer": sum(1 for item in browser_items if str(item.get("follow_up_state") or "") == "waiting_on_customer"),
            "custom_build_candidates": len(build_items),
            "custom_build_tasks_live": sum(1 for item in build_items if str(item.get("google_task_status") or "") == "created"),
            "orders_to_pack_units": int(counts.get("orders_to_pack_units") or 0),
            "stock_print_candidates": len(stock_items),
            "active_weekly_sale_items": len(weekly_sale_items),
            "weak_weekly_sale_items": sum(1 for item in weekly_sale_items if str(item.get("effectiveness") or "") == "weak"),
            "review_queue_items": len(review_items),
            "review_queue_backlog": review_queue_backlog,
            "usps_live_customer_items": sum(1 for item in customer_items if str(item.get("tracking_live_label") or "").strip()),
            "workflow_followthrough_items": len(workflow_items),
        },
        "next_actions": _build_next_actions(
            customer_items=customer_items,
            browser_items=browser_items,
            build_items=build_items,
            pack_items=pack_items,
            stock_items=stock_items,
            weekly_sale_items=weekly_sale_items,
            review_items=review_items,
            workflow_items=workflow_items,
        ),
        "sections": {
            "customer_packets": customer_items[:6],
            "etsy_browser_threads": browser_items[:6],
            "custom_build_candidates": build_items[:6],
            "orders_to_pack": pack_items[:8],
            "stock_print_candidates": stock_items[:6],
            "weekly_sale_monitor": weekly_sale_items[:6],
            "review_queue": review_items[:6],
            "workflow_followthrough": workflow_items[:6],
        },
    }


def render_business_operator_desk_markdown(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") or {}
    sections = payload.get("sections") or {}
    strategy_focus = payload.get("strategy_focus") or {}
    lines = [
        "# Duck Ops Business Desk",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Customer attention items: `{counts.get('customer_attention_items', 0)}`",
        f"- Replacement labels now: `{counts.get('replacement_labels_now', 0)}`",
        f"- Etsy browser-review threads: `{counts.get('etsy_browser_threads', 0)}`",
        f"- Threads with staged reply drafts: `{counts.get('threads_with_staged_reply', 0)}`",
        f"- Threads waiting on customer: `{counts.get('threads_waiting_on_customer', 0)}`",
        f"- Custom build candidates: `{counts.get('custom_build_candidates', 0)}`",
        f"- Live Google Tasks for builds: `{counts.get('custom_build_tasks_live', 0)}`",
        f"- Non-custom units to pack: `{counts.get('orders_to_pack_units', 0)}`",
        f"- Print-soon candidates: `{counts.get('stock_print_candidates', 0)}`",
        f"- Active weekly sale items: `{counts.get('active_weekly_sale_items', 0)}`",
        f"- Weak weekly sale items: `{counts.get('weak_weekly_sale_items', 0)}`",
        f"- Creative/operator review items: `{counts.get('review_queue_items', 0)}`",
        f"- Older creative/operator backlog: `{max(0, int(counts.get('review_queue_backlog', 0)) - int(counts.get('review_queue_items', 0)))}`",
        f"- Customer cases with live USPS context: `{counts.get('usps_live_customer_items', 0)}`",
        f"- Workflow follow-through items: `{counts.get('workflow_followthrough_items', 0)}`",
        "",
        "## Strategic Focus",
        "",
    ]
    if not strategy_focus.get("available"):
        lines.append("Master roadmap not available.")
    else:
        lines.append(f"- Roadmap: `{strategy_focus.get('path')}`")
        next_steps = strategy_focus.get("next_steps") or []
        if next_steps:
            lines.append("- Next major steps:")
            for step in next_steps:
                lines.append(f"  - {step.get('title')}: {_trim_text(step.get('summary'), 160)}")
    lines.extend([
        "",
        "## Do Next",
        "",
    ])
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
            if item.get("tracking_live_label"):
                lines.append(f"  USPS live: {item.get('tracking_live_label')}")
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
            if item.get("draft_reply"):
                lines.append(f"  Draft reply: {_trim_text(item.get('draft_reply'), 140)}")
            if item.get("recommended_next_action"):
                lines.append(f"  Next: {_trim_text(item.get('recommended_next_action'), 140)}")
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
            if item.get("google_task_web_view_link"):
                lines.append(f"  Task: {item.get('google_task_web_view_link')}")

    lines.extend(["", "## Pack Tonight", ""])
    pack_items = sections.get("orders_to_pack") or []
    if not pack_items:
        lines.append("No non-custom ducks are open for packing right now.")
    else:
        for item in pack_items:
            channels = item.get("by_channel") or {}
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('urgency_label') or 'Open'} | Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {item.get('total_quantity', 0)} | Buyers {item.get('buyer_count_display') or item.get('buyer_count') or 0}"
            )
            if item.get("option_summary"):
                lines.append(f"  Choices: {_trim_text(item.get('option_summary'), 120)}")

    lines.extend(["", "## Print Soon / Stock Watch", ""])
    stock_items = sections.get("stock_print_candidates") or []
    if not stock_items:
        lines.append("No stock-print candidates are staged right now.")
    else:
        for item in stock_items:
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('priority') or 'low'} priority | recent demand {int(item.get('recent_demand') or 0)}"
            )
            lines.append(f"  Why: {_trim_text(item.get('why_now'), 120)}")

    lines.extend(["", "## Weekly Sale Monitor", ""])
    weekly_sale_items = sections.get("weekly_sale_monitor") or []
    if not weekly_sale_items:
        lines.append("No active weekly sale items are available right now.")
    else:
        for item in weekly_sale_items:
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('discount')} | {item.get('effectiveness')} | 7d {int(item.get('sales_7d') or 0)} | 30d {int(item.get('sales_30d') or 0)}"
            )
            lines.append(f"  Recommendation: {item.get('recommendation')}")
            lines.append(f"  Marketing: {_trim_text(item.get('marketing_recommendation'), 120)}")

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

    lines.extend(["", "## Workflow Follow-Through", ""])
    workflow_items = sections.get("workflow_followthrough") or []
    if not workflow_items:
        lines.append("No workflow follow-through items are staged right now.")
    else:
        for item in workflow_items:
            lines.append(
                f"- {item.get('lane')}: {item.get('title')} | {item.get('summary') or item.get('state_reason') or 'needs follow-through'}"
            )
            if item.get("root_cause"):
                lines.append(f"  Why: {_trim_text(item.get('root_cause'), 180)}")
            if item.get("fix_hint"):
                lines.append(f"  Fix: {_trim_text(item.get('fix_hint'), 180)}")
            if item.get("latest_receipt"):
                lines.append(f"  Last receipt: {item.get('latest_receipt')}")
            if item.get("recent_history"):
                lines.append(f"  Trail: {item.get('recent_history')}")
            if item.get("next_action"):
                lines.append(f"  Do: {item.get('next_action')}")
            if item.get("command"):
                lines.append(f"  Run: `{item.get('command')}`")

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
        "sale": "weekly_sale_monitor",
        "sales": "weekly_sale_monitor",
        "weekly_sales": "weekly_sale_monitor",
        "stock": "stock_print_candidates",
        "print": "stock_print_candidates",
        "reviews": "review_queue",
        "creative": "review_queue",
        "next": "next_actions",
        "workflow": "workflow_followthrough",
        "workflows": "workflow_followthrough",
        "roadmap": "strategy_focus",
        "strategy": "strategy_focus",
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
    if normalized == "strategy_focus":
        lines = ["Duck Ops Strategic Focus", ""]
        strategy_focus = payload.get("strategy_focus") or {}
        if not strategy_focus.get("available"):
            lines.append("Master roadmap not available.")
        else:
            lines.append(f"Roadmap: {strategy_focus.get('path')}")
            next_steps = strategy_focus.get("next_steps") or []
            if next_steps:
                lines.append("")
                for step in next_steps:
                    lines.append(f"- {step.get('title')}: {_trim_text(step.get('summary'), 160)}")
        return "\n".join(lines)

    sections = payload.get("sections") or {}
    items = sections.get(normalized) or []
    title_map = {
        "customer_packets": "Customer Queue",
        "etsy_browser_threads": "Etsy Browser Review",
        "custom_build_candidates": "Custom Builds",
        "orders_to_pack": "Pack Tonight",
        "stock_print_candidates": "Print Soon / Stock Watch",
        "weekly_sale_monitor": "Weekly Sale Monitor",
        "review_queue": "Creative Review Queue",
        "workflow_followthrough": "Workflow Follow-Through",
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
            if item.get("draft_reply"):
                lines.append(f"  Draft reply: {_trim_text(item.get('draft_reply'), 140)}")
            if item.get("recommended_next_action"):
                lines.append(f"  Next: {_trim_text(item.get('recommended_next_action'), 140)}")
            if item.get("open_command"):
                lines.append(f"  Command: {item.get('open_command')}")
            elif item.get("primary_browser_url"):
                lines.append(f"  Open: {item.get('primary_browser_url')}")
        elif normalized == "custom_build_candidates":
            lines.append(f"- {item.get('buyer_name')} | {item.get('quantity')}x | {_trim_text(item.get('custom_design_summary'), 120)}")
            if item.get("next_action_summary"):
                lines.append(f"  Next: {item.get('next_action_summary')}")
            if item.get("google_task_web_view_link"):
                lines.append(f"  Task: {item.get('google_task_web_view_link')}")
        elif normalized == "orders_to_pack":
            channels = item.get("by_channel") or {}
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('urgency_label')} | Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {item.get('total_quantity', 0)} | Buyers {item.get('buyer_count_display') or item.get('buyer_count') or 0}"
            )
            if item.get("option_summary"):
                lines.append(f"  Choices: {_trim_text(item.get('option_summary'), 120)}")
        elif normalized == "stock_print_candidates":
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('priority')} priority | recent demand {int(item.get('recent_demand') or 0)}"
            )
            lines.append(f"  Why: {_trim_text(item.get('why_now'), 120)}")
        elif normalized == "weekly_sale_monitor":
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('discount')} | {item.get('effectiveness')} | 7d {int(item.get('sales_7d') or 0)} | 30d {int(item.get('sales_30d') or 0)}"
            )
            lines.append(f"  Recommendation: {item.get('recommendation')}")
            lines.append(f"  Marketing: {_trim_text(item.get('marketing_recommendation'), 120)}")
        elif normalized == "review_queue":
            lines.append(f"- {item.get('short_id')} | {item.get('decision')} | {item.get('title')}")
            if item.get("detail_command"):
                lines.append(f"  Detail: {item.get('detail_command')}")
        elif normalized == "workflow_followthrough":
            lines.append(
                f"- {item.get('lane')}: {item.get('title')} | {_trim_text(item.get('summary'), 120)}"
            )
            if item.get("root_cause"):
                lines.append(f"  Why: {_trim_text(item.get('root_cause'), 180)}")
            if item.get("fix_hint"):
                lines.append(f"  Fix: {_trim_text(item.get('fix_hint'), 180)}")
            if item.get("latest_receipt"):
                lines.append(f"  Last receipt: {item.get('latest_receipt')}")
            if item.get("recent_history"):
                lines.append(f"  Trail: {item.get('recent_history')}")
            if item.get("next_action"):
                lines.append(f"  Do: {item.get('next_action')}")
            if item.get("command"):
                lines.append(f"  Run: {item.get('command')}")
        else:
            lines.append(f"- {_trim_text(str(item), 120)}")
    return "\n".join(lines)
