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
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
ETSY_BROWSER_CAPTURES_PATH = STATE_DIR / "etsy_conversation_browser_captures.json"


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


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


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


def _task_title_preview(order: dict[str, Any], summary: str) -> str:
    buyer = str(order.get("buyer_name") or "").strip()
    quantity = int(order.get("quantity") or 0)
    qty_prefix = f"{quantity}x " if quantity > 1 else ""
    owner_prefix = f"{buyer}: " if buyer else ""
    return f"{owner_prefix}{qty_prefix}{summary}"


def _browser_capture_index() -> dict[str, dict[str, Any]]:
    payload = _load_json(ETSY_BROWSER_CAPTURES_PATH, {"items": []})
    by_order_ref: dict[str, dict[str, Any]] = {}
    for item in payload.get("items") or []:
        order_ref = str(item.get("order_ref") or "").strip()
        if order_ref and order_ref not in by_order_ref:
            by_order_ref[order_ref] = item
    return by_order_ref


def _design_stage(order: dict[str, Any], summary: str, browser_capture: dict[str, Any] | None) -> tuple[str, str]:
    if not summary or summary == "custom details still need review":
        return "needs_clarification", "Open the Etsy thread, capture the brief, and fill the missing design details."
    follow_up_state = str((browser_capture or {}).get("follow_up_state") or "").strip()
    draft_reply = str((browser_capture or {}).get("draft_reply") or "").strip()
    if follow_up_state == "resolved":
        return "resolved_in_thread", "No new design work tonight unless the customer reopens the Etsy thread."
    if follow_up_state == "waiting_on_customer":
        return "waiting_on_customer", "Wait for the customer to answer the open questions before making the next concept."
    if follow_up_state == "needs_reply":
        if draft_reply:
            return "reply_needed_before_design", "Send the staged Etsy reply, then update the task once the customer answers."
        return "reply_needed_before_design", "Reply on Etsy first so the brief is locked before design work continues."
    if follow_up_state == "concept_in_progress":
        return "concept_in_progress", "Continue the concept work already in flight and send the next draft back to the customer."
    if follow_up_state == "ready_for_task":
        return "brief_ready", "Open the Google Task and start concept work from the captured Etsy brief."
    if str(order.get("channel") or "").strip().lower() == "etsy":
        return "brief_ready_browser_followup", "Open the Etsy thread, confirm the live design brief, then stage the concept workflow."
    return "brief_ready", "Stage this custom build for concept work and manual design follow-up."


def build_custom_build_task_candidates(packing_summary: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    now_local = datetime.now().astimezone()
    browser_capture_by_order = _browser_capture_index()
    for order in packing_summary.get("custom_orders_to_make") or []:
        buyer_name = str(order.get("buyer_name") or "").strip() or "Customer"
        channel = str(order.get("channel") or "").strip() or "unknown"
        order_ref = str(order.get("order_ref") or "").strip() or "unknown-order"
        product_title = str(order.get("product_title") or "Custom duck").strip()
        custom_type = str(order.get("custom_type") or "").strip()
        personalization = str(order.get("personalization") or "").strip()
        summary = _detail_summary(order)
        created_dt = _parse_iso_datetime(order.get("created_at"))
        age_days = None
        if created_dt:
            age_days = max(0.0, (now_local - created_dt.astimezone()).total_seconds() / 86400.0)
        browser_capture = browser_capture_by_order.get(order_ref) or {}
        design_stage, next_design_action = _design_stage(order, summary, browser_capture)
        task_title_preview = _task_title_preview(order, summary)
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
        transaction_id = str(order.get("transaction_id") or "").strip()
        artifact_suffix = transaction_id or _slugify("::".join([custom_type or "", personalization or "", summary or product_title]))
        bucket = grouped.setdefault(
            grouping_key,
            {
                "artifact_id": (
                    f"custom_build_task::{channel}::{order_ref}::"
                    f"{artifact_suffix}"
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
                "task_title_preview": task_title_preview,
                "design_workflow_stage": design_stage,
                "next_design_action": next_design_action,
                "order_age_days": round(age_days or 0.0, 1) if age_days is not None else None,
                "browser_review_status": browser_capture.get("browser_review_status"),
                "browser_follow_up_state": browser_capture.get("follow_up_state"),
                "browser_capture_summary": browser_capture.get("customer_summary") or browser_capture.get("latest_message_text"),
                "browser_draft_reply": browser_capture.get("draft_reply"),
                "browser_missing_details": browser_capture.get("missing_details") or [],
                "browser_task_progress_note": browser_capture.get("task_progress_note"),
                "source_refs": [],
            },
        )
        bucket["quantity"] += int(order.get("quantity") or 0)
        tx_id = transaction_id
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
                f"- Etsy thread state: `{item.get('browser_follow_up_state') or item.get('browser_review_status') or 'not_captured'}`",
                f"- Browser summary: {_trim_text(item.get('browser_capture_summary'))}" if item.get("browser_capture_summary") else "- Browser summary: `(not captured)`",
                f"- Task title preview: {_trim_text(item.get('task_title_preview'))}",
                f"- Stage: `{item.get('design_workflow_stage')}`",
                f"- Next step: {_trim_text(item.get('next_design_action'), 180)}",
                f"- Order age: `{item.get('order_age_days')}` day(s)" if item.get("order_age_days") is not None else "- Order age: `(unknown)`",
                f"- Ready for task: `{str(bool(item.get('ready_for_task'))).lower()}`",
                f"- Google Task status: `{item.get('google_task_status')}`",
                f"- Draft reply: {_trim_text(item.get('browser_draft_reply'), 180)}" if item.get("browser_draft_reply") else "- Draft reply: `(none staged)`",
                "",
            ]
        )
    return "\n".join(lines)
