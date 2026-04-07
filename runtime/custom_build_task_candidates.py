#!/usr/bin/env python3
"""
Stage custom build task candidates from live open custom-order lines.

These candidates are deliberately simple:

- they turn paid, unfulfilled custom orders into one tracked work item
- they do not require Google Tasks credentials to exist yet
- they preserve enough Etsy order detail to later create a task or hand off to a browser thread review
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


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


def _detail_summary(order: dict[str, Any]) -> str:
    custom_type = str(order.get("custom_type") or "").strip()
    summary = str(order.get("custom_design_summary") or "").strip()
    personalization = str(order.get("personalization") or "").strip()
    if summary and custom_type and not summary.lower().startswith(custom_type.lower()):
        return f"{custom_type}: {summary}"
    if summary:
        return summary
    if personalization and custom_type:
        return f"{custom_type}: {personalization}"
    if personalization:
        return personalization
    if custom_type:
        return custom_type
    return "custom details still need review"


def build_custom_build_task_candidates(packing_summary: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for order in packing_summary.get("custom_orders_to_make") or []:
        buyer_name = str(order.get("buyer_name") or "").strip() or "Customer"
        channel = str(order.get("channel") or "").strip() or "unknown"
        order_ref = str(order.get("order_ref") or "").strip() or "unknown-order"
        product_title = str(order.get("product_title") or "Custom duck").strip()
        custom_type = str(order.get("custom_type") or "").strip()
        personalization = str(order.get("personalization") or "").strip()
        summary = _detail_summary(order)
        grouping_key = "::".join(
            [
                channel,
                order_ref,
                product_title.lower(),
                custom_type.lower(),
                personalization.lower(),
                summary.lower(),
            ]
        )
        bucket = grouped.setdefault(
            grouping_key,
            {
                "artifact_id": (
                    f"custom_build_task::{channel}::{order_ref}::"
                    f"{_slugify(custom_type or summary or product_title)}"
                ),
                "artifact_type": "custom_build_task_candidate",
                "buyer_name": buyer_name,
                "channel": channel,
                "order_ref": order_ref,
                "transaction_ids": [],
                "product_title": product_title,
                "quantity": 0,
                "custom_type": custom_type or None,
                "personalization": personalization or None,
                "custom_design_summary": summary,
                "created_at": order.get("created_at"),
                "ready_for_task": bool(summary and summary != "custom details still need review"),
                "google_task_status": "not_created",
                "source_refs": [],
            },
        )
        bucket["quantity"] += int(order.get("quantity") or 0)
        tx_id = str(order.get("transaction_id") or "").strip()
        if tx_id and tx_id not in bucket["transaction_ids"]:
            bucket["transaction_ids"].append(tx_id)
        source_ref = {
            "path": "state/normalized/packing_summary.json",
            "channel": channel,
            "order_ref": order_ref,
            "transaction_id": tx_id or None,
        }
        if source_ref not in bucket["source_refs"]:
            bucket["source_refs"].append(source_ref)

    items = sorted(
        grouped.values(),
        key=lambda item: (
            str(item.get("buyer_name") or "").lower(),
            str(item.get("order_ref") or "").lower(),
            str(item.get("product_title") or "").lower(),
        ),
    )
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "items": len(items),
            "ready_for_task": sum(1 for item in items if item.get("ready_for_task")),
            "units": sum(int(item.get("quantity") or 0) for item in items),
        },
        "items": items,
    }


def render_custom_build_task_candidates_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Custom Build Task Candidates",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Candidates: `{payload.get('counts', {}).get('items', 0)}`",
        f"- Units represented: `{payload.get('counts', {}).get('units', 0)}`",
        "",
    ]
    items = payload.get("items") or []
    if not items:
        lines.append("No custom build task candidates right now.")
        lines.append("")
        return "\n".join(lines)

    for index, item in enumerate(items, start=1):
        lines.extend(
            [
                f"## {index}. {item.get('buyer_name')} - {item.get('product_title')}",
                "",
                f"- Quantity: `{item.get('quantity')}`",
                f"- Order: `{item.get('channel')} {item.get('order_ref')}`",
                f"- Build details: {_trim_text(item.get('custom_design_summary'))}",
                f"- Ready for task: `{str(bool(item.get('ready_for_task'))).lower()}`",
                f"- Google Task status: `{item.get('google_task_status')}`",
                "",
            ]
        )
    return "\n".join(lines)
