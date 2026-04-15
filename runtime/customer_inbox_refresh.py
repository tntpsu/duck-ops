#!/usr/bin/env python3
"""
Refresh Etsy inbox thread state for customer conversations.

This lane is intentionally read-only against Etsy:
- it reuses the trusted Etsy seller session
- verifies and persists direct thread URLs when safe
- checks whether staged replies are already visible in-thread
- rebuilds customer/operator artifacts from the refreshed browser state

It never types customer replies or clicks send/submit.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from business_operator_desk import build_business_operator_desk
from customer_action_packets import build_customer_action_packets
from customer_interaction_cases import build_customer_interaction_queue
from customer_operator import (
    _backfill_resolution_is_safe,
    _best_browser_url,
    _browser_thread_as_packet,
    _is_direct_etsy_thread_url,
    _open_in_trusted_etsy_session,
    _persist_resolved_thread_url,
    _record_browser_capture,
    _resolve_trusted_etsy_session,
    _verify_reply_sent_in_trusted_etsy_session,
    load_browser_sync,
    load_json,
    now_iso,
    write_customer_operator_outputs,
)
from etsy_conversation_browser_sync import build_etsy_conversation_browser_sync
from etsy_browser_guard import blocked_status as etsy_browser_blocked_status
from nightly_action_summary import build_nightly_action_summary
from phase1_observer import (
    BUSINESS_OPERATOR_DESK_PATH,
    CUSTOMER_ACTION_PACKETS_PATH,
    CUSTOMER_INTERACTION_QUEUE_PATH,
    CUSTOM_BUILD_TASK_CANDIDATES_PATH,
    NIGHTLY_ACTION_SUMMARY_PATH,
    NORMALIZED_DIR,
    REVIEW_QUEUE_STATE_PATH,
    WEEKLY_SALE_MONITOR_PATH,
    write_business_operator_desk_outputs,
    write_customer_action_packet_outputs,
    write_customer_interaction_queue_outputs,
    write_etsy_conversation_browser_sync_outputs,
    write_nightly_action_summary_outputs,
)
from review_reply_executor import ensure_authenticated_session
from workflow_control import record_workflow_transition


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
REFRESH_STATE_PATH = STATE_DIR / "customer_inbox_refresh.json"

ACTIVE_REFRESH_STATES = {
    "needs_reply",
    "reply_drafted",
    "ready_for_task",
    "concept_in_progress",
    "waiting_on_operator",
}


def _local_now() -> datetime:
    return datetime.now().astimezone()


def _within_allowed_window(now: datetime, *, start_hour: int, start_minute: int, end_hour: int, end_minute: int) -> bool:
    current_minutes = now.hour * 60 + now.minute
    start_minutes = start_hour * 60 + start_minute
    end_minutes = end_hour * 60 + end_minute
    return start_minutes <= current_minutes <= end_minutes


def _build_packet_lookup(browser_sync: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for item in browser_sync.get("items") or []:
        short_id = str(item.get("linked_customer_short_id") or "").strip()
        if short_id:
            lookup[short_id] = _browser_thread_as_packet(item)
    return lookup


def _candidate_rank(item: dict[str, Any], top_short_ids: set[str], *, include_waiting: bool) -> tuple[int, int, int, str]:
    short_id = str(item.get("linked_customer_short_id") or "").strip()
    follow_up_state = str(item.get("follow_up_state") or "").strip()
    review_status = str(item.get("browser_review_status") or "").strip()
    direct_url = _is_direct_etsy_thread_url(item.get("primary_browser_url"))
    active_state_rank = {
        "needs_browser_review": 0,
        "reply_drafted": 1,
        "needs_reply": 2,
        "waiting_on_operator": 3,
        "ready_for_task": 4,
        "concept_in_progress": 5,
        "waiting_on_customer": 6 if include_waiting else 9,
        "resolved": 10,
    }.get(review_status if review_status == "needs_browser_review" else follow_up_state, 8)
    return (
        0 if short_id in top_short_ids else 1,
        0 if not direct_url else 1,
        active_state_rank,
        str(item.get("conversation_contact") or "").lower(),
    )


def select_refresh_candidates(
    browser_sync: dict[str, Any],
    nightly_summary: dict[str, Any] | None,
    *,
    limit: int,
    include_waiting: bool = False,
) -> list[dict[str, Any]]:
    top_short_ids = {
        str(item.get("short_id") or "").strip()
        for item in ((nightly_summary or {}).get("top_customer_actions") or [])
        if str(item.get("short_id") or "").strip()
    }
    candidates: list[dict[str, Any]] = []
    for item in browser_sync.get("items") or []:
        follow_up_state = str(item.get("follow_up_state") or "").strip()
        review_status = str(item.get("browser_review_status") or "").strip()
        if follow_up_state == "resolved":
            continue
        if follow_up_state == "waiting_on_customer" and not include_waiting and str(item.get("linked_customer_short_id") or "").strip() not in top_short_ids:
            continue
        if review_status != "needs_browser_review" and follow_up_state not in ACTIVE_REFRESH_STATES and str(item.get("linked_customer_short_id") or "").strip() not in top_short_ids:
            continue
        candidates.append(item)
    candidates.sort(key=lambda item: _candidate_rank(item, top_short_ids, include_waiting=include_waiting))
    return candidates[: max(limit, 0)]


def _persist_refresh_metadata(packet: dict[str, Any], refresh_row: dict[str, Any]) -> None:
    captures = load_json(ROOT / "state" / "etsy_conversation_browser_captures.json", {"generated_at": now_iso(), "items": []})
    items = list(captures.get("items") or [])
    thread_key = str(packet.get("conversation_thread_key") or "").strip()
    source_artifact_id = str(packet.get("source_artifact_id") or "").strip()
    updated = False
    for item in items:
        same_thread = thread_key and str(item.get("conversation_thread_key") or "").strip() == thread_key
        same_source = source_artifact_id and str(item.get("source_artifact_id") or "").strip() == source_artifact_id
        if same_thread or same_source:
            item.update(refresh_row)
            updated = True
            break
    if updated:
        captures["generated_at"] = now_iso()
        (ROOT / "state" / "etsy_conversation_browser_captures.json").write_text(json.dumps(captures, indent=2), encoding="utf-8")


def _maybe_mark_waiting_on_customer(packet: dict[str, Any]) -> bool:
    reply_text = str(packet.get("draft_reply") or "").strip()
    if not reply_text:
        return False
    verification = _verify_reply_sent_in_trusted_etsy_session(packet, reply_text)
    if not verification.get("reply_sent_verified"):
        return False
    capture_note = (
        "state: waiting_on_customer; "
        "reply_needed: no; "
        "open_loop: customer; "
        "summary: Refresh verified that the staged reply is already visible in the Etsy thread. "
        f"last_seller_message: {reply_text}; "
        "action: Wait for the customer to respond."
    )
    _record_browser_capture(packet, capture_note)
    return True


def refresh_packet(packet: dict[str, Any]) -> dict[str, Any]:
    opened = _open_in_trusted_etsy_session(packet)
    resolved_url = str(opened.get("current_url") or opened.get("target_url") or "").strip()
    resolution = dict(opened.get("target_resolution_details") or {})
    resolution_strategy = str(opened.get("target_resolution_strategy") or "").strip()
    safe_direct_url = _is_direct_etsy_thread_url(resolved_url) and (
        resolution_strategy == "direct_url" or _backfill_resolution_is_safe(packet, resolution)
    )
    if safe_direct_url:
        _persist_resolved_thread_url(packet, resolved_url)

    verification = dict(opened.get("thread_verification") or {})
    refreshed_at = now_iso()
    refresh_row = {
        "browser_refreshed_at": refreshed_at,
        "thread_verification": verification,
        "refresh_status": "verified" if not verification.get("verification_required") else "manual_verification_required",
    }
    if safe_direct_url:
        refresh_row["thread_url"] = resolved_url
    _persist_refresh_metadata(packet, refresh_row)
    auto_waiting = _maybe_mark_waiting_on_customer(packet)
    return {
        "short_id": packet.get("short_id"),
        "title": packet.get("title"),
        "current_url": resolved_url or None,
        "persisted_direct_url": resolved_url if safe_direct_url else None,
        "resolution_strategy": opened.get("target_resolution_strategy"),
        "verification_required": bool(opened.get("target_verification_required")),
        "contact_match": bool(verification.get("contactMatch")),
        "summary_matches": verification.get("summaryMatches") or [],
        "reply_verified_waiting": auto_waiting,
        "refreshed_at": refreshed_at,
    }


def rebuild_customer_outputs() -> dict[str, Any]:
    customer_cases_payload = load_json(NORMALIZED_DIR / "customer_cases.json", {"items": []})
    custom_design_payload = load_json(NORMALIZED_DIR / "custom_design_cases.json", {"items": []})
    print_queue_payload = load_json(NORMALIZED_DIR / "print_queue_candidates.json", {"items": []})
    custom_build_candidates = load_json(CUSTOM_BUILD_TASK_CANDIDATES_PATH, {"items": [], "counts": {}})
    weekly_sale_monitor = load_json(WEEKLY_SALE_MONITOR_PATH, {"items": []})
    packing_summary = load_json(NORMALIZED_DIR / "packing_summary.json", {"orders_to_pack": [], "custom_orders_to_make": []})
    review_queue = load_json(REVIEW_QUEUE_STATE_PATH, {})
    browser_captures = load_json(ROOT / "state" / "etsy_conversation_browser_captures.json", {"generated_at": now_iso(), "items": []})

    customer_interaction_queue = build_customer_interaction_queue(
        customer_cases_payload.get("items") or [],
        custom_design_payload.get("items") or [],
        print_queue_payload.get("items") or [],
    )
    customer_issue_queue_items = [
        item for item in customer_interaction_queue.get("items") or [] if item.get("item_type") == "customer_case"
    ]
    customer_action_packets = {
        "generated_at": now_iso(),
        "counts": {},
        "items": build_customer_action_packets(customer_issue_queue_items, browser_captures=browser_captures),
    }
    customer_action_packets["counts"] = {
        "packets_total": len(customer_action_packets["items"]),
        "reply_packets": sum(1 for item in customer_action_packets["items"] if item.get("packet_type") == "reply"),
        "refund_packets": sum(1 for item in customer_action_packets["items"] if item.get("packet_type") == "refund"),
        "replacement_packets": sum(1 for item in customer_action_packets["items"] if item.get("packet_type") == "replacement"),
        "wait_for_tracking_packets": sum(1 for item in customer_action_packets["items"] if item.get("packet_type") == "wait_for_tracking"),
    }
    customer_operator_payload = write_customer_operator_outputs(customer_action_packets)
    etsy_browser_sync = build_etsy_conversation_browser_sync(
        customer_issue_queue_items,
        customer_packets=customer_operator_payload,
        custom_build_candidates=custom_build_candidates,
        browser_captures=browser_captures,
    )
    nightly_action_summary = build_nightly_action_summary(
        customer_action_packets,
        custom_design_payload.get("items") or [],
        packing_summary,
        custom_build_task_candidates=custom_build_candidates,
        etsy_browser_sync=etsy_browser_sync,
    )
    business_operator_desk = build_business_operator_desk(
        customer_packets=customer_operator_payload,
        nightly_summary=nightly_action_summary,
        etsy_browser_sync=etsy_browser_sync,
        custom_build_candidates=custom_build_candidates,
        print_queue_candidates=print_queue_payload if isinstance(print_queue_payload, dict) else {"items": print_queue_payload},
        weekly_sale_monitor=weekly_sale_monitor,
        review_queue=review_queue if isinstance(review_queue, dict) else {},
    )

    write_customer_interaction_queue_outputs(customer_interaction_queue)
    write_customer_action_packet_outputs(customer_action_packets)
    write_etsy_conversation_browser_sync_outputs(etsy_browser_sync)
    write_nightly_action_summary_outputs(nightly_action_summary)
    write_business_operator_desk_outputs(business_operator_desk)

    return {
        "customer_interaction_queue": customer_interaction_queue,
        "customer_action_packets": customer_action_packets,
        "etsy_browser_sync": etsy_browser_sync,
        "nightly_action_summary": nightly_action_summary,
        "business_operator_desk": business_operator_desk,
    }


def run_refresh(
    *,
    limit: int,
    include_waiting: bool,
    skip_outside_hours: bool,
    start_hour: int,
    start_minute: int,
    end_hour: int,
    end_minute: int,
) -> dict[str, Any]:
    now = _local_now()
    if skip_outside_hours and not _within_allowed_window(
        now,
        start_hour=start_hour,
        start_minute=start_minute,
        end_hour=end_hour,
        end_minute=end_minute,
    ):
        summary = {
            "generated_at": now_iso(),
            "status": "skipped",
            "reason": "quiet_hours",
            "window": {
                "start": f"{start_hour:02d}:{start_minute:02d}",
                "end": f"{end_hour:02d}:{end_minute:02d}",
            },
        }
        REFRESH_STATE_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        record_workflow_transition(
            workflow_id="customer_inbox_refresh",
            lane="customer_inbox_refresh",
            display_label="Customer Inbox Refresh",
            entity_id="customer_inbox_refresh",
            state="resolved",
            state_reason="quiet_hours",
            next_action="Wait for the next allowed refresh window.",
            receipt_kind="refresh_run",
            receipt_payload=summary,
            history_summary="quiet hours",
        )
        return summary

    browser_sync = load_browser_sync()
    nightly_summary = load_json(NIGHTLY_ACTION_SUMMARY_PATH, {})
    candidates = select_refresh_candidates(
        browser_sync,
        nightly_summary if isinstance(nightly_summary, dict) else {},
        limit=limit,
        include_waiting=include_waiting,
    )
    packet_lookup = _build_packet_lookup(browser_sync)
    session_name, start_url = ("", "")
    if candidates:
        session_name, start_url = _resolve_trusted_etsy_session()
        ensure_authenticated_session(session_name, start_url)

    refreshed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for thread in candidates:
        short_id = str(thread.get("linked_customer_short_id") or "").strip()
        packet = packet_lookup.get(short_id)
        if not packet:
            failed.append({"short_id": short_id, "reason": "packet_lookup_missing"})
            continue
        try:
            refreshed.append(refresh_packet(packet))
        except Exception as exc:  # noqa: BLE001
            failed.append({"short_id": short_id, "reason": str(exc)})

    rebuilt = rebuild_customer_outputs()
    summary = {
        "generated_at": now_iso(),
        "status": "ok" if not failed else ("partial" if refreshed else "failed"),
        "session_name": session_name or None,
        "attempted": len(candidates),
        "refreshed": len(refreshed),
        "failed": len(failed),
        "refreshed_items": refreshed,
        "failed_items": failed,
        "sync_generated_at": ((rebuilt.get("etsy_browser_sync") or {}).get("generated_at")),
        "sync_counts": ((rebuilt.get("etsy_browser_sync") or {}).get("counts") or {}),
    }
    REFRESH_STATE_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    record_workflow_transition(
        workflow_id="customer_inbox_refresh",
        lane="customer_inbox_refresh",
        display_label="Customer Inbox Refresh",
        entity_id="customer_inbox_refresh",
        state="verified" if refreshed and not failed else ("blocked" if failed else "observed"),
        state_reason="threads_refreshed" if refreshed and not failed else ("refresh_partial" if refreshed else "refresh_failed"),
        next_action="Open the refreshed customer queue if any follow-up state changed." if refreshed else "Retry a smaller refresh batch or inspect Etsy session state.",
        last_verification={
            "attempted": len(candidates),
            "refreshed": len(refreshed),
            "failed": len(failed),
        },
        metadata={
            "session_name": session_name or None,
            "sync_generated_at": summary.get("sync_generated_at"),
        },
        receipt_kind="refresh_run",
        receipt_payload=summary,
        history_summary="customer inbox refresh",
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Etsy customer inbox state")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--include-waiting", action="store_true")
    parser.add_argument("--skip-outside-hours", action="store_true")
    parser.add_argument("--start-hour", type=int, default=7)
    parser.add_argument("--start-minute", type=int, default=30)
    parser.add_argument("--end-hour", type=int, default=23)
    parser.add_argument("--end-minute", type=int, default=59)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    blocked = etsy_browser_blocked_status()
    if blocked.get("blocked"):
        summary = {
            "status": "blocked",
            "reason": blocked.get("block_reason"),
            "blocked_until": blocked.get("blocked_until"),
            "attempted": 0,
            "refreshed": 0,
            "failed": 0,
        }
        if args.json:
            print(json.dumps(summary, indent=2))
        else:
            print(
                f"Customer inbox refresh: blocked | reason {summary.get('reason')} | "
                f"until {summary.get('blocked_until')}"
            )
        return 0
    summary = run_refresh(
        limit=args.limit,
        include_waiting=args.include_waiting,
        skip_outside_hours=args.skip_outside_hours,
        start_hour=args.start_hour,
        start_minute=args.start_minute,
        end_hour=args.end_hour,
        end_minute=args.end_minute,
    )
    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(
            f"Customer inbox refresh: {summary.get('status')} | "
            f"attempted {summary.get('attempted', 0)} | refreshed {summary.get('refreshed', 0)} | failed {summary.get('failed', 0)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
