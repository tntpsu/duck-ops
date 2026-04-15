#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from workflow_control import record_workflow_transition


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
CUSTOMER_QUEUE_PATH = STATE_DIR / "customer_interaction_queue.json"
REVIEW_QUEUE_PATH = STATE_DIR / "review_queue.json"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        return parsed.astimezone()
    except ValueError:
        return None


def sync_ops_control(customer_queue: dict[str, Any], review_queue: dict[str, Any]) -> dict[str, Any]:
    customer_dt = _parse_iso(customer_queue.get("generated_at"))
    review_dt = _parse_iso(review_queue.get("generated_at"))
    timestamps = [dt for dt in (customer_dt, review_dt) if dt is not None]
    freshest = max(timestamps) if timestamps else None
    age_hours = None
    if freshest is not None:
        age_hours = round((datetime.now().astimezone() - freshest).total_seconds() / 3600.0, 2)

    customer_attention = int((customer_queue.get("counts") or {}).get("operator_queue_items") or len(customer_queue.get("items") or []))
    review_pending = int(review_queue.get("pending_count_all") or review_queue.get("pending_count") or len(review_queue.get("items") or []))

    if age_hours is not None and age_hours >= 36:
        state = "blocked"
        reason = "stale_input"
        next_action = "Refresh the customer and review queues before trusting the operator desk."
    elif customer_attention or review_pending:
        state = "observed"
        reason = "backlog_present"
        if customer_attention and review_pending:
            next_action = "Clear both customer and creative review backlog items from the operator queue."
        elif customer_attention:
            next_action = "Work the customer queue until only true followups remain."
        else:
            next_action = "Clear the creative review backlog so publishing lanes do not stall."
    else:
        state = "verified"
        reason = "desk_ready"
        next_action = "No immediate ops action is required."

    control = record_workflow_transition(
        workflow_id="ops",
        lane="ops",
        display_label="Ops Lane",
        entity_id="ops",
        state=state,
        state_reason=reason,
        input_freshness={
            "customer_queue_age_hours": age_hours,
            "review_queue_age_hours": age_hours,
            "customer_queue_path": str(CUSTOMER_QUEUE_PATH),
            "review_queue_path": str(REVIEW_QUEUE_PATH),
        },
        next_action=next_action,
        metadata={
            "customer_attention": customer_attention,
            "review_pending": review_pending,
        },
        receipt_kind="state_sync",
        receipt_payload={
            "customer_attention": customer_attention,
            "review_pending": review_pending,
        },
        history_summary=reason.replace("_", " "),
    )
    return {
        "state": state,
        "state_reason": reason,
        "age_hours": age_hours,
        "path": str((control or {}).get("latest_receipt", {}).get("path") or ""),
    }
