#!/usr/bin/env python3
"""
Customer-issue operator lane for Duck Ops.

This gives the staged customer action packets a lightweight operator surface with:

- stable short ids
- a current-item card
- status / next navigation
- explicit recovery decision recording
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from customer_recovery_decisions import append_decision, normalize_resolution
from workflow_control import record_workflow_transition, workflow_state_path


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
OUTPUT_DIR = ROOT / "output" / "operator"
PACKETS_PATH = STATE_DIR / "customer_action_packets.json"
OPERATOR_STATE_PATH = STATE_DIR / "customer_operator_state.json"
ETSY_BROWSER_CAPTURES_PATH = STATE_DIR / "etsy_conversation_browser_captures.json"
ETSY_BROWSER_SYNC_PATH = STATE_DIR / "etsy_conversation_browser_sync.json"
REVIEW_REPLY_EXECUTION_AUTH_PATH = STATE_DIR / "review_reply_execution_auth.json"
REVIEW_REPLY_DISCOVERY_SESSIONS_PATH = STATE_DIR / "review_reply_discovery_sessions.json"
CURRENT_CARD_PATH = OUTPUT_DIR / "current_customer_action.md"
QUEUE_CARD_PATH = OUTPUT_DIR / "customer_queue.md"
SAFE_ETSY_MESSAGES_URL = "https://www.etsy.com/messages"
SAFE_ETSY_INBOX_URL = "https://www.etsy.com/messages?ref=hdr_user_menu-messages"
BAD_ETSY_MESSAGES_URLS = {
    "https://www.etsy.com/your/messages",
    "https://www.etsy.com/your/account/messages",
    "https://www.etsy.com/messages",
}
GENERIC_ETSY_MESSAGES_URLS = {
    SAFE_ETSY_INBOX_URL,
    *BAD_ETSY_MESSAGES_URLS,
}

SHORT_ID_START = 301
SHORT_ID_PATTERN = re.compile(r"^c(?P<num>\d+)$", re.IGNORECASE)
RESOLUTION_COMMANDS = {
    "replacement": "replacement",
    "replace": "replacement",
    "resend": "replacement",
    "refund": "refund",
    "wait": "wait",
    "reply": "reply_only",
    "replyonly": "reply_only",
    "reply_only": "reply_only",
    "open": "open",
    "browser": "open",
}
FOLLOW_UP_STATE_COMMANDS = {
    "drafted": "reply_drafted",
    "waiting": "waiting_on_customer",
    "resolved": "resolved",
    "taskready": "ready_for_task",
    "ready": "ready_for_task",
    "needreply": "needs_reply",
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_packets() -> dict[str, Any]:
    return load_json(PACKETS_PATH, {"generated_at": now_iso(), "counts": {}, "items": []})


def load_operator_state() -> dict[str, Any]:
    return load_json(
        OPERATOR_STATE_PATH,
        {
            "next_short_id": SHORT_ID_START,
            "packet_short_ids": {},
            "current_packet_id": None,
        },
    )


def load_browser_sync() -> dict[str, Any]:
    return load_json(ETSY_BROWSER_SYNC_PATH, {"generated_at": now_iso(), "counts": {}, "items": []})


def write_operator_state(state: dict[str, Any]) -> None:
    write_json(OPERATOR_STATE_PATH, state)


def _priority_rank(value: str | None) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(value or "medium").lower(), 9)


def _status_rank(value: str | None) -> int:
    return {
        "buy_label_now": 0,
        "issue_manual_refund_now": 1,
        "operator_confirmation_required": 2,
        "reply_needed": 3,
        "waiting_by_operator_decision": 4,
        "possible_reship_already_sent": 5,
        "watch": 6,
    }.get(str(value or "").strip(), 9)


def _trim_text(value: str | None, limit: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _customer_workflow_key(packet: dict[str, Any]) -> str:
    candidates = [
        packet.get("short_id"),
        packet.get("linked_customer_short_id"),
        packet.get("packet_short_id"),
        packet.get("conversation_thread_key"),
        packet.get("source_artifact_id"),
        packet.get("packet_id"),
    ]
    for value in candidates:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return "unknown_customer_thread"


def _customer_workflow_metadata(packet: dict[str, Any]) -> dict[str, Any]:
    order = packet.get("order_enrichment") or {}
    return {
        "short_id": packet.get("short_id") or packet.get("linked_customer_short_id") or packet.get("packet_short_id"),
        "conversation_thread_key": packet.get("conversation_thread_key"),
        "source_artifact_id": packet.get("source_artifact_id"),
        "conversation_contact": packet.get("conversation_contact") or packet.get("title"),
        "buyer_name": order.get("buyer_name"),
        "receipt_id": order.get("receipt_id"),
        "transaction_id": order.get("transaction_id"),
    }


def _record_customer_workflow_capture(packet: dict[str, Any], row: dict[str, Any]) -> None:
    follow_up_state = str(row.get("follow_up_state") or "").strip()
    browser_review_status = str(row.get("browser_review_status") or "").strip()
    draft_reply = str(row.get("draft_reply") or "").strip()

    if follow_up_state == "resolved":
        state = "resolved"
        reason = "thread_resolved"
        requires_confirmation = False
    elif follow_up_state == "waiting_on_customer":
        state = "verified"
        reason = "awaiting_customer"
        requires_confirmation = False
    elif follow_up_state in {"waiting_on_operator", "ready_for_task", "concept_in_progress"}:
        state = "blocked"
        reason = "manual_intervention_required"
        requires_confirmation = False
    elif follow_up_state == "needs_reply":
        if draft_reply:
            state = "proposed"
            reason = "reply_drafted"
            requires_confirmation = True
        else:
            state = "observed"
            reason = "customer_waiting_on_us"
            requires_confirmation = False
    elif browser_review_status == "needs_browser_review":
        state = "observed"
        reason = "needs_thread_review"
        requires_confirmation = False
    else:
        state = "observed"
        reason = "thread_observed"
        requires_confirmation = False

    workflow_key = _customer_workflow_key(packet)
    record_workflow_transition(
        workflow_id=f"customer_reply::{workflow_key}",
        lane="customer_reply",
        display_label=f"Customer Reply {workflow_key}",
        entity_id=workflow_key,
        state=state,
        state_reason=reason,
        requires_confirmation=requires_confirmation,
        last_side_effect={
            "kind": "browser_capture",
            "captured_at": row.get("captured_at"),
            "thread_url": row.get("thread_url"),
            "has_draft_reply": bool(draft_reply),
        },
        last_verification={
            "browser_review_status": browser_review_status or None,
            "follow_up_state": follow_up_state or None,
            "reply_needed": row.get("reply_needed"),
            "open_loop_owner": row.get("open_loop_owner"),
            "last_customer_message": row.get("last_customer_message"),
            "last_seller_message": row.get("last_seller_message"),
        },
        next_action=str(row.get("recommended_action") or "").strip() or None,
        metadata={
            **_customer_workflow_metadata(packet),
            "follow_up_state": follow_up_state or None,
            "browser_review_status": browser_review_status or None,
        },
        receipt_kind="browser_capture",
        receipt_payload={
            "note": row.get("customer_summary"),
            "draft_reply_excerpt": _trim_text(draft_reply, 120) if draft_reply else None,
        },
        history_summary=reason.replace("_", " "),
    )


def _record_customer_preview_workflow(packet: dict[str, Any], preview: dict[str, Any], reply_text: str) -> None:
    preview_typed = bool(preview.get("preview_typed"))
    if preview_typed:
        state = "proposed"
        reason = "reply_preview_staged"
        next_action = "Confirm the typed Etsy reply, then send only after explicit approval."
    else:
        state = "blocked"
        reason = (
            "thread_verification_failed"
            if str(preview.get("preview_reason") or "").strip() == "thread_verification_failed"
            or bool(preview.get("target_verification_required"))
            else "preview_failed"
        )
        next_action = "Re-open the Etsy thread, verify the target, and restage the preview before any send."

    workflow_key = _customer_workflow_key(packet)
    record_workflow_transition(
        workflow_id=f"customer_reply::{workflow_key}",
        lane="customer_reply",
        display_label=f"Customer Reply {workflow_key}",
        entity_id=workflow_key,
        state=state,
        state_reason=reason,
        requires_confirmation=True,
        last_side_effect={
            "kind": "reply_preview",
            "preview_typed": preview_typed,
            "session_name": preview.get("session_name"),
            "current_url": preview.get("current_url"),
            "screenshot_path": preview.get("screenshot_path"),
        },
        last_verification=preview.get("thread_verification"),
        next_action=next_action,
        metadata=_customer_workflow_metadata(packet),
        receipt_kind="preview",
        receipt_payload={
            "reply_excerpt": _trim_text(reply_text, 180),
            "preview_reason": preview.get("preview_reason"),
            "target_resolution_strategy": preview.get("target_resolution_strategy"),
            "target_verification_required": preview.get("target_verification_required"),
        },
        history_summary=reason.replace("_", " "),
    )


def _record_customer_preview_failure(packet: dict[str, Any], error_text: str) -> None:
    workflow_key = _customer_workflow_key(packet)
    record_workflow_transition(
        workflow_id=f"customer_reply::{workflow_key}",
        lane="customer_reply",
        display_label=f"Customer Reply {workflow_key}",
        entity_id=workflow_key,
        state="blocked",
        state_reason="preview_failed",
        requires_confirmation=True,
        last_side_effect={"kind": "reply_preview", "preview_typed": False},
        next_action="Retry the preview only after verifying the target Etsy thread.",
        metadata=_customer_workflow_metadata(packet),
        receipt_kind="preview_error",
        receipt_payload={"error": error_text},
        history_summary="preview failed",
    )


def _record_customer_confirm_workflow(packet: dict[str, Any], confirmation: dict[str, Any], reply_text: str) -> None:
    confirmed = bool(confirmation.get("preview_confirmed"))
    if confirmed:
        state = "approved"
        reason = "reply_send_confirmed"
        next_action = "Send the verified Etsy reply in the live thread, then run `customer verify C###` to record the posted result."
    else:
        state = "blocked"
        reason = (
            "thread_verification_failed"
            if str(confirmation.get("confirmation_reason") or "").strip() == "thread_verification_failed"
            else "preview_confirmation_failed"
        )
        next_action = "Restage or recheck the reply preview before sending anything in Etsy."

    workflow_key = _customer_workflow_key(packet)
    record_workflow_transition(
        workflow_id=f"customer_reply::{workflow_key}",
        lane="customer_reply",
        display_label=f"Customer Reply {workflow_key}",
        entity_id=workflow_key,
        state=state,
        state_reason=reason,
        requires_confirmation=False,
        last_side_effect={
            "kind": "reply_preview_confirmed",
            "session_name": confirmation.get("session_name"),
            "current_url": confirmation.get("current_url"),
            "screenshot_path": confirmation.get("screenshot_path"),
        },
        last_verification=confirmation.get("preview_state"),
        next_action=next_action,
        metadata=_customer_workflow_metadata(packet),
        receipt_kind="send_confirmed",
        receipt_payload={
            "reply_excerpt": _trim_text(reply_text, 180),
            "confirmation_reason": confirmation.get("confirmation_reason"),
            "target_resolution_strategy": confirmation.get("target_resolution_strategy"),
        },
        history_summary=reason.replace("_", " "),
    )


def _record_customer_send_verification_workflow(packet: dict[str, Any], verification: dict[str, Any], reply_text: str) -> None:
    verified = bool(verification.get("reply_sent_verified"))
    if verified:
        state = "verified"
        reason = "reply_sent_verified"
        next_action = "Wait for the customer to respond unless a new Etsy message changes the thread."
    else:
        state = "blocked"
        reason = (
            "thread_verification_failed"
            if str(verification.get("verification_reason") or "").strip() == "thread_verification_failed"
            else "send_verification_failed"
        )
        next_action = "Re-open the Etsy thread and verify whether the reply actually posted before changing the follow-up state."

    workflow_key = _customer_workflow_key(packet)
    record_workflow_transition(
        workflow_id=f"customer_reply::{workflow_key}",
        lane="customer_reply",
        display_label=f"Customer Reply {workflow_key}",
        entity_id=workflow_key,
        state=state,
        state_reason=reason,
        requires_confirmation=False,
        last_side_effect={
            "kind": "reply_send_verification",
            "session_name": verification.get("session_name"),
            "current_url": verification.get("current_url"),
            "screenshot_path": verification.get("screenshot_path"),
        },
        last_verification=verification.get("posted_state"),
        next_action=next_action,
        metadata=_customer_workflow_metadata(packet),
        receipt_kind="send_verified",
        receipt_payload={
            "reply_excerpt": _trim_text(reply_text, 180),
            "verification_reason": verification.get("verification_reason"),
            "target_resolution_strategy": verification.get("target_resolution_strategy"),
        },
        history_summary=reason.replace("_", " "),
    )


def assign_short_ids(packet_payload: dict[str, Any], operator_state: dict[str, Any]) -> list[dict[str, Any]]:
    items = list(packet_payload.get("items") or [])
    mapping = dict(operator_state.get("packet_short_ids") or {})
    next_short_id = int(operator_state.get("next_short_id", SHORT_ID_START))
    for item in items:
        packet_id = str(item.get("packet_id") or "")
        if packet_id and packet_id not in mapping:
            mapping[packet_id] = next_short_id
            next_short_id += 1
    operator_state["packet_short_ids"] = mapping
    operator_state["next_short_id"] = next_short_id
    for item in items:
        item["short_id"] = f"C{mapping.get(str(item.get('packet_id') or ''), 0)}"
    items.sort(
        key=lambda item: (
            _priority_rank(item.get("priority")),
            _status_rank(item.get("status")),
            str(item.get("title") or "").lower(),
        )
    )
    return items


def sync_current_packet(items: list[dict[str, Any]], operator_state: dict[str, Any]) -> dict[str, Any] | None:
    current_packet_id = operator_state.get("current_packet_id")
    for item in items:
        if item.get("packet_id") == current_packet_id:
            return item
    current = items[0] if items else None
    operator_state["current_packet_id"] = current.get("packet_id") if current else None
    return current


def _packet_source_refs(packet: dict[str, Any]) -> str:
    refs = packet.get("source_refs") or []
    if not refs:
        return "(none)"
    values = []
    for ref in refs[:3]:
        values.append(str(ref.get("path") or ref.get("uid") or ref.get("message_id") or "ref"))
    return ", ".join(values)


def _best_browser_url(packet: dict[str, Any]) -> str | None:
    candidates = [str(url).strip() for url in (packet.get("browser_url_candidates") or []) if str(url).strip()]
    safe_direct: list[str] = []
    fallback_etsy: list[str] = []
    for url in candidates:
        lowered = url.lower()
        if "ablink.account.etsy.com" in lowered:
            continue
        if "etsy.com" not in lowered:
            continue
        match = re.search(r"https://www\.etsy\.com/(?:your/account/)?messages/(?P<id>\d+)", url, re.IGNORECASE)
        if match:
            safe_direct.append(f"https://www.etsy.com/messages/{match.group('id')}")
            continue
        if lowered.rstrip("/") in {
            "https://www.etsy.com/your/messages",
            "https://www.etsy.com/your/account/messages",
            "https://www.etsy.com/messages",
        }:
            fallback_etsy.append(SAFE_ETSY_INBOX_URL)
            continue
        fallback_etsy.append(url)
    if safe_direct:
        return safe_direct[0]
    if fallback_etsy:
        return fallback_etsy[0]
    for url in candidates:
        if "etsy.com" in url.lower():
            return SAFE_ETSY_INBOX_URL
    return candidates[0] if candidates else None


def _browser_url_requires_manual_verification(url: str | None) -> bool:
    normalized = str(url or "").strip().lower()
    if not normalized:
        return True
    if "ablink.account.etsy.com" in normalized:
        return True
    return normalized.rstrip("/") in GENERIC_ETSY_MESSAGES_URLS


def _resolve_trusted_etsy_session() -> tuple[str, str]:
    from review_reply_executor import choose_session

    session_name, start_url = choose_session()
    auth_state = load_json(REVIEW_REPLY_EXECUTION_AUTH_PATH, {})
    if str(auth_state.get("auth_status") or "").strip().lower() == "healthy":
        trusted_session = str(auth_state.get("last_session_name") or "").strip()
        if trusted_session:
            session_name = trusted_session
        trusted_url = str(auth_state.get("last_checked_url") or "").strip()
        if trusted_url and trusted_url.rstrip("/").lower() not in BAD_ETSY_MESSAGES_URLS:
            start_url = trusted_url

    session_state = load_json(REVIEW_REPLY_DISCOVERY_SESSIONS_PATH, {"sessions": {}})
    session_record = (session_state.get("sessions") or {}).get(session_name) or {}
    record_url = str(session_record.get("url") or "").strip()
    if record_url and not str(start_url or "").strip():
        start_url = record_url
    return session_name, start_url


def _verify_thread_context(session_name: str, packet: dict[str, Any]) -> dict[str, Any]:
    from review_reply_discovery import parse_eval_json, run_pw_command

    expected_contact = str(
        _contact_hint_from_source_refs(packet)
        or packet.get("conversation_contact")
        or ((packet.get("order_enrichment") or {}).get("buyer_name"))
        or packet.get("title")
        or ""
    ).strip()
    expected_summary = str(packet.get("latest_message_preview") or packet.get("customer_summary") or "").strip()
    result = run_pw_command(
        session_name,
        "eval",
        (
            "(() => { "
            f"const expectedContact = {json.dumps(_normalized_text(expected_contact))}; "
            f"const expectedTerms = {json.dumps(_summary_terms(expected_summary))}; "
            "const bodyText = (document.body.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase(); "
            "const contactMatch = expectedContact ? bodyText.includes(expectedContact) : false; "
            "const summaryMatches = expectedTerms.filter(token => bodyText.includes(token)); "
            "const textarea = document.querySelector('textarea[aria-label=\"Type your reply\"], textarea'); "
            "return JSON.stringify({ "
            "  currentUrl: window.location.href, "
            "  contactMatch, "
            "  summaryMatches, "
            "  hasReplyBox: !!textarea "
            "}); "
            "})()"
        ),
    )
    parsed = parse_eval_json(result)
    if not isinstance(parsed, dict):
        parsed = {}
    if _contact_terms(expected_contact):
        parsed["verification_required"] = not bool(parsed.get("contactMatch"))
    else:
        parsed["verification_required"] = not bool(parsed.get("summaryMatches"))
    return parsed


def _open_in_trusted_etsy_session(packet: dict[str, Any]) -> dict[str, Any]:
    from review_reply_discovery import navigate_within_session, parse_page_metadata, run_pw_command
    from review_reply_executor import ensure_authenticated_session

    browser_url = _best_browser_url(packet)
    if not browser_url:
        raise RuntimeError("No Etsy browser URL is available for this customer packet.")

    session_name, start_url = _resolve_trusted_etsy_session()
    session_meta = ensure_authenticated_session(session_name, start_url)
    resolution: dict[str, Any] | None = None
    if _browser_url_requires_manual_verification(browser_url):
        resolution = _locate_thread_via_inbox_search(session_name, packet)
        if resolution.get("ok"):
            resolved_url = str(resolution.get("landed_url") or resolution.get("target_url") or "").strip()
            if resolved_url:
                browser_url = resolved_url
                if not resolution.get("verification_required"):
                    _persist_resolved_thread_url(packet, resolved_url)
        else:
            navigate_within_session(session_name, browser_url, wait_seconds=1.5)
    else:
        landed_url, landed_title = navigate_within_session(session_name, browser_url, wait_seconds=1.5)
        resolution = {
            "ok": True,
            "strategy": "direct_url",
            "target_url": browser_url,
            "landed_url": landed_url,
            "landed_title": landed_title,
            "verification_required": False,
        }
    snapshot_output = run_pw_command(session_name, "snapshot")
    current_url, page_title = parse_page_metadata(snapshot_output)
    verification = _verify_thread_context(session_name, packet)
    return {
        "session_name": session_name,
        "target_url": browser_url,
        "current_url": current_url or (resolution or {}).get("landed_url"),
        "page_title": page_title or (resolution or {}).get("landed_title"),
        "reused_existing_session": bool(session_meta.get("reused_existing_session")),
        "target_verification_required": bool((resolution or {}).get("verification_required") or verification.get("verification_required")),
        "target_resolution_strategy": (resolution or {}).get("strategy"),
        "target_resolution_ok": bool((resolution or {}).get("ok")),
        "target_resolution_details": resolution,
        "thread_verification": verification,
    }


def _browser_thread_as_packet(thread: dict[str, Any]) -> dict[str, Any]:
    candidates: list[str] = []
    for url in [thread.get("primary_browser_url"), *(thread.get("browser_url_candidates") or [])]:
        normalized = str(url or "").strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return {
        "packet_id": f"browser_thread::{thread.get('linked_customer_short_id') or thread.get('conversation_thread_key') or 'thread'}",
        "short_id": thread.get("linked_customer_short_id"),
        "title": thread.get("conversation_contact") or thread.get("linked_customer_short_id") or "Customer thread",
        "browser_url_candidates": candidates,
        "order_enrichment": thread.get("order_enrichment") or {},
        "conversation_contact": thread.get("conversation_contact"),
        "conversation_thread_key": thread.get("conversation_thread_key"),
        "source_artifact_id": thread.get("source_artifact_id"),
        "source_refs": thread.get("source_refs") or [],
        "customer_summary": thread.get("browser_summary") or thread.get("latest_message_preview"),
        "latest_message_preview": thread.get("latest_message_preview") or thread.get("browser_summary"),
        "draft_reply": thread.get("draft_reply"),
    }


def _resolve_target_thread_packet(
    items: list[dict[str, Any]],
    operator_state: dict[str, Any],
    token: str | None,
) -> dict[str, Any] | None:
    normalized = str(token or "").strip().lower()
    if not normalized:
        return resolve_target_packet(items, operator_state, token)
    target = _resolve_exact_packet(items, token)
    if target:
        return target
    browser_sync = load_browser_sync()
    for item in browser_sync.get("items") or []:
        if str(item.get("linked_customer_short_id") or "").strip().lower() == normalized:
            return _browser_thread_as_packet(item)
    return None


def _stage_reply_preview_in_trusted_etsy_session(packet: dict[str, Any], reply_text: str) -> dict[str, Any]:
    from review_reply_discovery import parse_eval_json, parse_screenshot_path, run_pw_command

    opened = _open_in_trusted_etsy_session(packet)
    verification = dict(opened.get("thread_verification") or {})
    if verification.get("verification_required"):
        return {
            **opened,
            "preview_typed": False,
            "preview_reason": "thread_verification_failed",
            "reply_text": reply_text,
        }
    fill_output = run_pw_command(
        opened["session_name"],
        "eval",
        (
            "(() => { "
            f"const replyText = {json.dumps(reply_text)}; "
            "const textarea = document.querySelector('textarea[aria-label=\"Type your reply\"], textarea'); "
            "if (!textarea) return JSON.stringify({ok:false, reason:'textarea_missing'}); "
            "textarea.focus(); "
            "textarea.value = replyText; "
            "textarea.dispatchEvent(new Event('input', { bubbles: true })); "
            "textarea.dispatchEvent(new Event('change', { bubbles: true })); "
            "return JSON.stringify({ok: textarea.value === replyText, valueLength: textarea.value.length}); "
            "})()"
        ),
    )
    fill_result = parse_eval_json(fill_output)
    screenshot_output = run_pw_command(opened["session_name"], "screenshot")
    screenshot_path = parse_screenshot_path(screenshot_output)
    return {
        **opened,
        "preview_typed": bool(isinstance(fill_result, dict) and fill_result.get("ok")),
        "preview_reason": None,
        "reply_text": reply_text,
        "fill_result": fill_result,
        "screenshot_path": screenshot_path,
    }


def _confirm_reply_preview_in_trusted_etsy_session(packet: dict[str, Any], reply_text: str) -> dict[str, Any]:
    from review_reply_discovery import parse_eval_json, parse_screenshot_path, run_pw_command

    opened = _open_in_trusted_etsy_session(packet)
    verification = dict(opened.get("thread_verification") or {})
    if verification.get("verification_required"):
        return {
            **opened,
            "preview_confirmed": False,
            "confirmation_reason": "thread_verification_failed",
            "reply_text": reply_text,
        }
    inspect_output = run_pw_command(
        opened["session_name"],
        "eval",
        (
            "(() => { "
            f"const replyText = {json.dumps(reply_text)}; "
            "const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim().toLowerCase(); "
            "const textarea = document.querySelector('textarea[aria-label=\"Type your reply\"], textarea'); "
            "const submit = document.querySelector('button[type=\"submit\"], button[aria-label*=\"Send\"], button'); "
            "const replyValue = textarea ? normalize(textarea.value) : ''; "
            "const expectedValue = normalize(replyText); "
            "return JSON.stringify({"
            "  ok: !!textarea && !!submit && replyValue === expectedValue, "
            "  textareaVisible: !!textarea, "
            "  submitVisible: !!submit, "
            "  submitDisabled: !!(submit && submit.disabled), "
            "  valueMatches: replyValue === expectedValue, "
            "  currentUrl: window.location.href "
            "}); "
            "})()"
        ),
    )
    preview_state = parse_eval_json(inspect_output)
    screenshot_output = run_pw_command(opened["session_name"], "screenshot")
    screenshot_path = parse_screenshot_path(screenshot_output)
    confirmation_reason = None
    if not bool(isinstance(preview_state, dict) and preview_state.get("ok")):
        confirmation_reason = (
            "textarea_mismatch"
            if isinstance(preview_state, dict) and preview_state.get("textareaVisible") and not preview_state.get("valueMatches")
            else "preview_not_ready"
        )
    return {
        **opened,
        "preview_confirmed": bool(isinstance(preview_state, dict) and preview_state.get("ok")),
        "confirmation_reason": confirmation_reason,
        "reply_text": reply_text,
        "preview_state": preview_state,
        "screenshot_path": screenshot_path,
    }


def _verify_reply_sent_in_trusted_etsy_session(packet: dict[str, Any], reply_text: str) -> dict[str, Any]:
    from review_reply_discovery import parse_eval_json, parse_screenshot_path, run_pw_command

    opened = _open_in_trusted_etsy_session(packet)
    verification = dict(opened.get("thread_verification") or {})
    if verification.get("verification_required"):
        return {
            **opened,
            "reply_sent_verified": False,
            "verification_reason": "thread_verification_failed",
            "reply_text": reply_text,
        }
    inspect_output = run_pw_command(
        opened["session_name"],
        "eval",
        (
            "(() => { "
            f"const replyText = {json.dumps(reply_text)}; "
            "const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim().toLowerCase(); "
            "const normalizedReply = normalize(replyText); "
            "const bodyText = normalize(document.body ? document.body.innerText : ''); "
            "const textarea = document.querySelector('textarea[aria-label=\"Type your reply\"], textarea'); "
            "const submit = document.querySelector('button[type=\"submit\"], button[aria-label*=\"Send\"], button'); "
            "return JSON.stringify({"
            "  ok: !!normalizedReply && bodyText.includes(normalizedReply), "
            "  bodyContainsReply: !!normalizedReply && bodyText.includes(normalizedReply), "
            "  textareaVisible: !!textarea, "
            "  submitVisible: !!submit, "
            "  currentUrl: window.location.href "
            "}); "
            "})()"
        ),
    )
    posted_state = parse_eval_json(inspect_output)
    screenshot_output = run_pw_command(opened["session_name"], "screenshot")
    screenshot_path = parse_screenshot_path(screenshot_output)
    verification_reason = None
    if not bool(isinstance(posted_state, dict) and posted_state.get("ok")):
        verification_reason = "reply_not_visible_in_thread"
    return {
        **opened,
        "reply_sent_verified": bool(isinstance(posted_state, dict) and posted_state.get("ok")),
        "verification_reason": verification_reason,
        "reply_text": reply_text,
        "posted_state": posted_state,
        "screenshot_path": screenshot_path,
    }


def _parse_bool_token(value: str | None) -> bool | None:
    lowered = str(value or "").strip().lower()
    if lowered in {"1", "true", "yes", "y", "unread"}:
        return True
    if lowered in {"0", "false", "no", "n", "read"}:
        return False
    return None


def _normalize_open_loop_owner(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    if not normalized:
        return None
    aliases = {
        "buyer": "customer",
        "customer": "customer",
        "waiting_on_customer": "customer",
        "seller": "seller",
        "us": "seller",
        "reply_needed": "seller",
        "operator": "operator",
        "waiting_on_operator": "operator",
        "task": "operator",
        "design": "operator",
        "closed": "closed",
        "none": "closed",
        "done": "closed",
        "resolved": "closed",
    }
    return aliases.get(normalized, normalized)


def _normalized_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _canonical_direct_etsy_thread_url(value: str | None) -> str | None:
    url = str(value or "").strip()
    if not url:
        return None
    match = re.search(r"https://www\.etsy\.com/(?:your/account/)?messages/(?P<id>\d+)", url, re.IGNORECASE)
    if not match:
        return None
    return f"https://www.etsy.com/messages/{match.group('id')}"


def _is_direct_etsy_thread_url(value: str | None) -> bool:
    return _canonical_direct_etsy_thread_url(value) is not None


def _summary_terms(value: str | None) -> list[str]:
    text = _normalized_text(value)
    tokens = re.findall(r"[a-z0-9#']+", text)
    stop = {
        "the", "and", "for", "with", "that", "this", "they", "them", "their", "have", "has", "your",
        "from", "need", "needs", "want", "wants", "buyer", "customer", "reply", "message", "order",
        "etsy", "thread", "next", "step", "moving", "forward", "asked", "already", "waiting",
        "latest", "review", "conversation", "help", "placed",
    }
    deduped: list[str] = []
    for token in tokens:
        if len(token) < 4 or token in stop or token in deduped:
            continue
        deduped.append(token)
    return deduped[:6]


def _contact_hint_from_source_refs(packet: dict[str, Any]) -> str | None:
    for ref in packet.get("source_refs") or []:
        subject = str(ref.get("subject") or "").strip()
        match = re.search(r"etsy conversation with\s+(?P<name>.+)$", subject, re.IGNORECASE)
        if match:
            name = match.group("name").strip()
            name = re.sub(r"\s+from\s+.+$", "", name, flags=re.IGNORECASE).strip()
            return name
        match = re.search(r"^(?P<name>.+?)\s+needs help with an order they placed$", subject, re.IGNORECASE)
        if match:
            return match.group("name").strip()
    return None


def _contact_terms(value: str | None) -> list[str]:
    tokens = re.findall(r"[a-z0-9']+", _normalized_text(value))
    stop = {"from", "with", "the", "and", "shop", "etsy", "buyer", "customer", "co", "com"}
    deduped: list[str] = []
    for token in tokens:
        if len(token) < 2 or token in stop or token in deduped:
            continue
        deduped.append(token)
    return deduped


def _select_inbox_search_candidate(
    candidates: list[dict[str, Any]],
    *,
    expected_contact: str | None,
    expected_summary: str | None,
) -> dict[str, Any] | None:
    contact = _normalized_text(expected_contact)
    contact_terms = _contact_terms(expected_contact)
    summary_terms = _summary_terms(expected_summary)
    ranked: list[tuple[int, int, int, dict[str, Any]]] = []
    for candidate in candidates:
        href = str(candidate.get("href") or "").strip()
        if "/messages/" not in href:
            continue
        text = _normalized_text(candidate.get("text"))
        if not text:
            continue
        contact_score = 0
        if contact:
            if text == contact:
                contact_score = 5
            elif contact in text:
                contact_score = 4
            else:
                matched_terms = sum(1 for token in contact_terms if token in text)
                if matched_terms and matched_terms == len(contact_terms):
                    contact_score = 3
                elif matched_terms >= 2:
                    contact_score = 2
                elif matched_terms == 1:
                    contact_score = 1
        if contact_terms and contact_score == 0:
            continue
        elif not contact_terms and any(part and part in text for part in contact.split()):
            contact_score = max(contact_score, 1)
        summary_score = sum(1 for token in summary_terms if token in text)
        ranked.append((contact_score, summary_score, len(text), candidate))
    ranked.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
    if not ranked:
        return None
    best = ranked[0]
    if best[0] <= 0 and best[1] <= 0:
        return None
    return best[3]


def _persist_resolved_thread_url(packet: dict[str, Any], resolved_url: str | None) -> None:
    url = _canonical_direct_etsy_thread_url(resolved_url)
    if not url:
        return
    captures = load_json(ETSY_BROWSER_CAPTURES_PATH, {"generated_at": now_iso(), "items": []})
    items = list(captures.get("items") or [])
    thread_key = str(packet.get("conversation_thread_key") or "").strip()
    source_artifact_id = str(packet.get("source_artifact_id") or "").strip()
    updated = False
    for item in items:
        same_thread = thread_key and str(item.get("conversation_thread_key") or "").strip() == thread_key
        same_source = source_artifact_id and str(item.get("source_artifact_id") or "").strip() == source_artifact_id
        if same_thread or same_source:
            item["thread_url"] = url
            updated = True
            break
    if not updated:
        items.append(
            {
                "conversation_thread_key": thread_key or None,
                "source_artifact_id": source_artifact_id or None,
                "packet_short_id": packet.get("short_id"),
                "browser_review_status": "captured",
                "follow_up_state": packet.get("follow_up_state"),
                "latest_message_text": packet.get("latest_message_preview") or packet.get("customer_summary"),
                "customer_summary": packet.get("customer_summary") or packet.get("latest_message_preview"),
                "draft_reply": packet.get("draft_reply"),
                "recommended_action": packet.get("recommended_action"),
                "custom_design_brief": packet.get("custom_design_brief"),
                "missing_details": packet.get("missing_details") or [],
                "task_progress_note": packet.get("task_progress_note"),
                "thread_url": url,
                "captured_at": now_iso(),
                "unread": None,
                "order_ref": ((packet.get("order_enrichment") or {}).get("receipt_id")),
                "transaction_id": ((packet.get("order_enrichment") or {}).get("transaction_id")),
                "buyer_name": packet.get("conversation_contact")
                or ((packet.get("order_enrichment") or {}).get("buyer_name"))
                or packet.get("title"),
            }
        )
        updated = True
    if updated:
        captures["items"] = items
        captures["generated_at"] = now_iso()
        write_json(ETSY_BROWSER_CAPTURES_PATH, captures)
    workflow_path = workflow_state_path(f"customer_reply::{_customer_workflow_key(packet)}")
    workflow_state = load_json(workflow_path, {})
    if isinstance(workflow_state, dict) and workflow_state:
        side_effect = dict(workflow_state.get("last_side_effect") or {})
        side_effect["thread_url"] = url
        workflow_state["last_side_effect"] = side_effect
        write_json(workflow_path, workflow_state)


class _ThreadResolutionTimeout(RuntimeError):
    pass


def _timeboxed_thread_resolution(packet: dict[str, Any], session_name: str, timeout_seconds: int) -> dict[str, Any]:
    if timeout_seconds <= 0:
        return _locate_thread_via_inbox_search(session_name, packet)

    def _raise_timeout(signum: int, frame: Any) -> None:  # pragma: no cover - signal handler
        raise _ThreadResolutionTimeout(f"timed out after {timeout_seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    try:
        signal.signal(signal.SIGALRM, _raise_timeout)
        signal.alarm(timeout_seconds)
        return _locate_thread_via_inbox_search(session_name, packet)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def _backfill_resolution_is_safe(packet: dict[str, Any], resolution: dict[str, Any]) -> bool:
    if not resolution.get("ok"):
        return False
    resolved_url = _canonical_direct_etsy_thread_url(resolution.get("landed_url") or resolution.get("target_url"))
    if not resolved_url:
        return False
    if resolution.get("verification_required"):
        return False
    expected_summary = str(packet.get("latest_message_preview") or packet.get("customer_summary") or "").strip()
    summary_terms = _summary_terms(expected_summary)
    verification = resolution.get("verification") or {}
    if summary_terms and not verification.get("summaryMatches"):
        return False
    return True


def backfill_exact_thread_urls(*, limit: int | None = None, timeout_seconds: int = 30) -> dict[str, Any]:
    from review_reply_executor import ensure_authenticated_session

    browser_sync = load_browser_sync()
    candidates: list[dict[str, Any]] = []
    for thread in browser_sync.get("items") or []:
        packet = _browser_thread_as_packet(thread)
        current_url = _best_browser_url(packet)
        if _is_direct_etsy_thread_url(current_url):
            continue
        candidates.append(packet)
    if limit is not None:
        candidates = candidates[: max(int(limit), 0)]

    if not candidates:
        return {
            "attempted": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "updated_short_ids": [],
            "failed_items": [],
        }

    session_name, start_url = _resolve_trusted_etsy_session()
    ensure_authenticated_session(session_name, start_url)

    updated_short_ids: list[str] = []
    failed_items: list[dict[str, Any]] = []
    skipped = 0
    for packet in candidates:
        try:
            resolution = _timeboxed_thread_resolution(packet, session_name, timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            failed_items.append(
                {
                    "short_id": packet.get("short_id"),
                    "contact": packet.get("conversation_contact") or packet.get("title"),
                    "reason": f"exception: {exc}",
                }
            )
            continue
        if _backfill_resolution_is_safe(packet, resolution):
            resolved_url = _canonical_direct_etsy_thread_url(resolution.get("landed_url") or resolution.get("target_url"))
            _persist_resolved_thread_url(packet, resolved_url)
            updated_short_ids.append(str(packet.get("short_id") or "").strip())
            continue
        if resolution.get("ok"):
            skipped += 1
        else:
            failed_items.append(
                {
                    "short_id": packet.get("short_id"),
                    "contact": packet.get("conversation_contact") or packet.get("title"),
                    "reason": resolution.get("reason") or "thread_not_found",
                }
            )

    if updated_short_ids:
        _rerun_observer()

    return {
        "attempted": len(candidates),
        "updated": len(updated_short_ids),
        "skipped": skipped,
        "failed": len(failed_items),
        "updated_short_ids": updated_short_ids,
        "failed_items": failed_items,
        "session_name": session_name,
    }


def _locate_thread_via_inbox_search(
    session_name: str,
    packet: dict[str, Any],
    *,
    wait_seconds: float = 1.5,
) -> dict[str, Any]:
    from review_reply_discovery import navigate_within_session, parse_eval_json, run_pw_command

    def collect_candidates() -> list[dict[str, Any]]:
        candidates_output = run_pw_command(
            session_name,
            "eval",
            (
                "(() => { "
                "const anchors = Array.from(document.querySelectorAll('a[href*=\"/messages/\"]')) "
                "  .map(node => { "
                "    const scope = node.closest('li, article, section, div') || node; "
                "    return { "
                "      href: node.href || null, "
                "      text: (scope.innerText || node.innerText || '').replace(/\\s+/g, ' ').trim() "
                "    }; "
                "  }) "
                "  .filter(item => item.href && item.text); "
                "return JSON.stringify(anchors.slice(0, 120)); "
                "})()"
            ),
        )
        parsed = parse_eval_json(candidates_output)
        return parsed if isinstance(parsed, list) else []

    expected_contact = str(
        _contact_hint_from_source_refs(packet)
        or packet.get("conversation_contact")
        or ((packet.get("order_enrichment") or {}).get("buyer_name"))
        or packet.get("title")
        or ""
    ).strip()
    expected_summary = str(packet.get("latest_message_preview") or packet.get("customer_summary") or "").strip()

    navigate_within_session(session_name, SAFE_ETSY_MESSAGES_URL, wait_seconds=wait_seconds)
    search_term = expected_contact or str(packet.get("short_id") or "").strip()
    candidates = collect_candidates()
    chosen = _select_inbox_search_candidate(
        candidates,
        expected_contact=expected_contact,
        expected_summary=expected_summary,
    )
    search_attempted = False
    if not chosen:
        search_attempted = True
        direct_search_url = f"https://www.etsy.com/messages/search?query={quote(search_term)}"
        navigate_within_session(session_name, direct_search_url, wait_seconds=wait_seconds)
        time.sleep(wait_seconds)
        candidates = collect_candidates()
        chosen = _select_inbox_search_candidate(
            candidates,
            expected_contact=expected_contact,
            expected_summary=expected_summary,
        )
    if not chosen:
        run_pw_command(
            session_name,
            "eval",
            (
                "(() => { "
                f"const searchTerm = {json.dumps(search_term)}; "
                "const input = document.querySelector('input[aria-label=\"Search your messages\"], input[type=\"search\"], input'); "
                "if (!input) return JSON.stringify({ok:false, reason:'search_input_missing'}); "
                "input.focus(); "
                "input.value = searchTerm; "
                "input.dispatchEvent(new Event('input', { bubbles: true })); "
                "input.dispatchEvent(new Event('change', { bubbles: true })); "
                "input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', bubbles: true })); "
                "input.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', code: 'Enter', bubbles: true })); "
                "const form = input.closest('form'); "
                "const submit = form ? form.querySelector('button[type=\"submit\"], [aria-label*=\"search\" i], button') : null; "
                "if (submit) submit.click(); "
                "else if (form && form.requestSubmit) form.requestSubmit(); "
                "return JSON.stringify({ok:true, value: input.value, usedFallbackFormSubmit: true}); "
                "})()"
            ),
        )
        time.sleep(wait_seconds)
        candidates = collect_candidates()
        chosen = _select_inbox_search_candidate(
            candidates,
            expected_contact=expected_contact,
            expected_summary=expected_summary,
        )
    if not chosen:
        return {
            "ok": False,
            "reason": "thread_not_found_via_inbox_search",
            "search_term": search_term,
            "candidate_count": len(candidates),
            "search_attempted": search_attempted,
        }
    target_url = str(chosen.get("href") or "").strip()
    landed_url, landed_title = navigate_within_session(session_name, target_url, wait_seconds=wait_seconds)
    verify_output = run_pw_command(
        session_name,
        "eval",
        (
            "(() => { "
            f"const expectedContact = {json.dumps(_normalized_text(expected_contact))}; "
            f"const expectedTerms = {json.dumps(_summary_terms(expected_summary))}; "
            "const bodyText = (document.body.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase(); "
            "const contactMatch = expectedContact ? bodyText.includes(expectedContact) : false; "
            "const summaryMatches = expectedTerms.filter(token => bodyText.includes(token)); "
            "return JSON.stringify({ "
            "  currentUrl: window.location.href, "
            "  contactMatch, "
            "  summaryMatches, "
            "  hasReplyBox: !!document.querySelector('textarea[aria-label=\"Type your reply\"], textarea') "
            "}); "
            "})()"
        ),
    )
    verify = parse_eval_json(verify_output)
    if not isinstance(verify, dict):
        verify = {}
    if _contact_terms(expected_contact):
        verification_required = not bool(verify.get("contactMatch"))
    else:
        verification_required = not bool(verify.get("summaryMatches"))
    return {
        "ok": True,
        "strategy": "inbox_search",
        "search_term": search_term,
        "candidate_count": len(candidates),
        "chosen_text": chosen.get("text"),
        "target_url": target_url,
        "landed_url": landed_url,
        "landed_title": landed_title,
        "verification_required": verification_required,
        "verification": verify,
    }


def _split_list_field(value: str | None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    working = raw.replace("|", ",")
    return [item.strip() for item in working.split(",") if item.strip()]


def _derive_follow_up_state(
    packet: dict[str, Any],
    summary: str,
    explicit_state: str | None,
    *,
    reply_needed: bool | None = None,
    open_loop_owner: str | None = None,
) -> tuple[str, str]:
    if explicit_state:
        normalized = explicit_state.strip().lower().replace(" ", "_")
        if normalized in {
            "needs_reply",
            "reply_drafted",
            "waiting_on_customer",
            "waiting_on_operator",
            "ready_for_task",
            "concept_in_progress",
            "resolved",
        }:
            if normalized == "reply_drafted":
                normalized = "needs_reply"
            browser_review_status = "resolved" if normalized == "resolved" else "captured"
            return normalized, browser_review_status

    if reply_needed is False:
        if open_loop_owner == "customer":
            return "waiting_on_customer", "captured"
        if open_loop_owner == "operator":
            return "waiting_on_operator", "captured"
        if open_loop_owner == "closed":
            return "resolved", "resolved"

    note_lower = summary.lower()
    if any(token in note_lower for token in {"resolved", "all set", "done", "approved final"}):
        return "resolved", "resolved"
    if any(token in note_lower for token in {"waiting on customer", "awaiting customer", "customer to reply"}):
        return "waiting_on_customer", "captured"
    if any(token in note_lower for token in {"ready for task", "brief ready", "start concept", "ready to design"}):
        return "ready_for_task", "captured"
    if any(token in note_lower for token in {"concept in progress", "working on concept"}):
        return "concept_in_progress", "captured"
    if any(token in note_lower for token in {"waiting on operator", "need decision", "operator decision"}):
        return "waiting_on_operator", "captured"
    if any(token in note_lower for token in {"needs reply", "reply tonight", "customer asked"}):
        return "needs_reply", "captured"
    if str((packet.get("order_enrichment") or {}).get("product_title") or "").lower().find("custom") >= 0:
        return "ready_for_task", "captured"
    return "needs_reply", "captured"


def _parse_capture_note(packet: dict[str, Any], note: str) -> dict[str, Any]:
    raw = str(note or "").strip()
    fields: dict[str, str] = {}
    summary_parts: list[str] = []
    for chunk in [part.strip() for part in raw.split(";") if part.strip()]:
        if ":" in chunk:
            key, value = chunk.split(":", 1)
            normalized_key = key.strip().lower().replace(" ", "_")
            if normalized_key:
                fields[normalized_key] = value.strip()
                continue
        summary_parts.append(chunk)

    summary = fields.get("summary") or fields.get("latest") or fields.get("message") or " ".join(summary_parts).strip() or raw
    draft_reply = (
        fields.get("reply")
        or fields.get("draft")
        or fields.get("draft_reply")
        or ""
    ).strip()
    recommended_action = (
        fields.get("action")
        or fields.get("next")
        or fields.get("recommended")
        or ""
    ).strip()
    explicit_state = fields.get("state") or fields.get("follow_up") or fields.get("follow_up_state")
    last_customer_message = (
        fields.get("customer_latest")
        or fields.get("latest_customer")
        or fields.get("last_customer")
        or fields.get("last_customer_message")
        or ""
    ).strip()
    last_seller_message = (
        fields.get("seller_latest")
        or fields.get("latest_seller")
        or fields.get("last_seller")
        or fields.get("last_seller_message")
        or ""
    ).strip()
    reply_needed = _parse_bool_token(fields.get("reply_needed") or fields.get("needs_reply"))
    open_loop_owner = _normalize_open_loop_owner(
        fields.get("open_loop") or fields.get("open_loop_owner") or fields.get("waiting_on")
    )
    follow_up_state, browser_review_status = _derive_follow_up_state(
        packet,
        " ".join([summary, draft_reply, recommended_action]).strip(),
        explicit_state,
        reply_needed=reply_needed,
        open_loop_owner=open_loop_owner,
    )
    unread = _parse_bool_token(fields.get("unread"))
    custom_design_brief = (
        fields.get("brief")
        or fields.get("design")
        or fields.get("custom")
        or fields.get("build")
        or ""
    ).strip()
    task_progress_note = (
        fields.get("task")
        or fields.get("progress")
        or fields.get("task_progress")
        or ""
    ).strip()
    missing_details = _split_list_field(fields.get("missing") or fields.get("questions"))
    if not recommended_action:
        if follow_up_state == "ready_for_task":
            recommended_action = "Open or update the Google Task and start concept work."
        elif follow_up_state == "waiting_on_customer":
            recommended_action = "No new design work tonight. Wait for the customer to answer the open questions."
        elif follow_up_state == "concept_in_progress":
            recommended_action = "Keep the concept moving and send the next draft back to the customer when ready."
        elif follow_up_state == "resolved":
            recommended_action = "No action needed unless the Etsy thread reopens."
        else:
            recommended_action = "Reply on Etsy, then update the capture once the conversation moves forward."
    if not custom_design_brief and str((packet.get("order_enrichment") or {}).get("product_title") or "").lower().find("custom") >= 0:
        custom_design_brief = summary
    return {
        "browser_review_status": browser_review_status,
        "follow_up_state": follow_up_state,
        "latest_message_text": summary,
        "customer_summary": summary,
        "draft_reply": draft_reply or None,
        "recommended_action": recommended_action,
        "custom_design_brief": custom_design_brief or None,
        "missing_details": missing_details,
        "task_progress_note": task_progress_note or None,
        "unread": False if unread is None else unread,
        "last_customer_message": last_customer_message or None,
        "last_seller_message": last_seller_message or None,
        "open_loop_owner": open_loop_owner,
        "reply_needed": reply_needed,
    }


def _quick_capture_note(follow_up_state: str, note: str) -> str:
    normalized = str(follow_up_state or "").strip().lower().replace(" ", "_")
    text = str(note or "").strip()
    if normalized == "reply_drafted":
        reply_text = text or "Draft reply staged."
        return (
            f"state: needs_reply; "
            f"reply_needed: yes; "
            f"open_loop: seller; "
            f"summary: Reply drafted and ready to send.; "
            f"reply: {reply_text}; "
            f"action: Send the staged Etsy reply, then capture the next thread state."
        )
    if normalized == "waiting_on_customer":
        summary = text or "Waiting on the customer to answer before doing more work."
        return (
            f"state: waiting_on_customer; "
            f"reply_needed: no; "
            f"open_loop: customer; "
            f"summary: {summary}; "
            f"action: No new work tonight. Wait for the customer to respond."
        )
    if normalized == "resolved":
        summary = text or "Resolved."
        return (
            f"state: resolved; "
            f"reply_needed: no; "
            f"open_loop: closed; "
            f"summary: {summary}; "
            f"action: No action needed unless the thread reopens."
        )
    if normalized == "ready_for_task":
        brief = text or "Brief is ready for task work."
        return (
            f"state: ready_for_task; "
            f"reply_needed: no; "
            f"open_loop: operator; "
            f"summary: {brief}; "
            f"brief: {brief}; "
            f"action: Open or update the Google Task and start concept work."
        )
    if normalized == "needs_reply":
        summary = text or "Customer needs a reply."
        return (
            f"state: needs_reply; "
            f"reply_needed: yes; "
            f"open_loop: seller; "
            f"summary: {summary}; "
            f"action: Reply on Etsy, then capture the next thread state."
        )
    return text


def render_customer_card(packet: dict[str, Any] | None) -> str:
    if not packet:
        return "No customer action packets right now."
    order = packet.get("order_enrichment") or {}
    tracking = packet.get("tracking_enrichment") or {}
    resolution = packet.get("resolution_enrichment") or {}
    operator_decision = packet.get("operator_decision") or {}
    lines = [
        f"Customer action {packet.get('short_id')}: {packet.get('title')}",
        "",
        f"Status: `{packet.get('status')}`",
        f"Priority: `{packet.get('priority')}`",
        f"Next operator action: `{packet.get('next_operator_action')}`",
        f"Next physical action: `{packet.get('next_physical_action')}`",
        f"Reason: {packet.get('reason') or '(none)'}",
        f"Customer summary: {_trim_text(packet.get('customer_summary'))}",
    ]
    if order.get("receipt_id") or order.get("product_title"):
        lines.append(
            f"Order: receipt `{order.get('receipt_id') or 'n/a'}`,"
            f" transaction `{order.get('transaction_id') or 'n/a'}`,"
            f" product `{order.get('product_title') or 'n/a'}`"
        )
    if tracking.get("status"):
        tracking_line = f"`{tracking.get('status')}`"
        if tracking.get("tracking_number"):
            tracking_line += f" ({tracking.get('tracking_number')})"
        lines.append(f"Tracking: {tracking_line}")
    if resolution.get("signals"):
        lines.append(f"History: {resolution.get('summary') or '(none)'}")
    if operator_decision.get("resolution"):
        lines.append(
            f"Recorded decision: `{operator_decision.get('resolution')}`"
            f" at `{operator_decision.get('recorded_at') or 'unknown'}`"
            f" - {operator_decision.get('note') or 'No note provided.'}"
        )
    browser_url = _best_browser_url(packet)
    if browser_url:
        lines.append(f"Browser review: `customer open {packet.get('short_id')}`")
        lines.append(f"Best browser URL: {browser_url}")
    lines.extend(
        [
            f"Source refs: {_packet_source_refs(packet)}",
            "",
            "Allowed replies:",
            f"- `replacement {packet.get('short_id')} because ...`",
            f"- `refund {packet.get('short_id')} because ...`",
            f"- `wait {packet.get('short_id')} because ...`",
            f"- `reply only {packet.get('short_id')} because ...`",
            f"- `customer open {packet.get('short_id')}`",
            f"- `customer preview {packet.get('short_id')}`",
            f"- `customer drafted {packet.get('short_id')} <reply text>`",
            f"- `customer waiting {packet.get('short_id')} <what we're waiting on>`",
            f"- `customer resolved {packet.get('short_id')} <resolution note>`",
            f"- `customer taskready {packet.get('short_id')} <brief summary>`",
            "- `customer threads`",
            "- `customer followups`",
            "- `customer next`",
            "- `customer status`",
        ]
    )
    return "\n".join(lines)


def render_customer_queue(items: list[dict[str, Any]], current: dict[str, Any] | None) -> str:
    lines = [
        "# Duck Ops Customer Operator Queue",
        "",
        f"- Generated at: `{now_iso()}`",
        f"- Customer packets: `{len(items)}`",
    ]
    if current:
        lines.append(f"- Current packet: `{current.get('short_id')}`")
    lines.append("")
    if not items:
        lines.append("No customer action packets right now.")
        lines.append("")
        return "\n".join(lines)
    for item in items:
        lines.extend(
            [
                f"## {item.get('short_id')} - {item.get('title')}",
                "",
                f"- Status: `{item.get('status')}`",
                f"- Priority: `{item.get('priority')}`",
                f"- Next operator action: `{item.get('next_operator_action')}`",
                f"- Summary: {_trim_text(item.get('customer_summary'))}",
                f"- Browser command: `customer open {item.get('short_id')}`" if _best_browser_url(item) else "- Browser command: `(none)`",
                "",
            ]
        )
    return "\n".join(lines)


def write_customer_operator_outputs(packet_payload: dict[str, Any], operator_state: dict[str, Any] | None = None) -> dict[str, Any]:
    operator_state = operator_state or load_operator_state()
    items = assign_short_ids(packet_payload, operator_state)
    current = sync_current_packet(items, operator_state)
    queue_markdown = render_customer_queue(items, current)
    current_markdown = render_customer_card(current)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    QUEUE_CARD_PATH.write_text(queue_markdown, encoding="utf-8")
    CURRENT_CARD_PATH.write_text(current_markdown, encoding="utf-8")
    for item in items:
        detail_path = OUTPUT_DIR / f"customer_action__{item.get('short_id')}.md"
        detail_path.write_text(render_customer_card(item), encoding="utf-8")
    write_operator_state(operator_state)
    return {
        "generated_at": now_iso(),
        "current_short_id": current.get("short_id") if current else None,
        "items": items,
    }


def parse_customer_command(text: str) -> tuple[str, str | None, str]:
    raw = (text or "").strip()
    if not raw:
        return "status", None, ""
    lowered = raw.lower().strip()

    if lowered.startswith("customer status"):
        return "status", None, ""
    if lowered == "customer":
        return "status", None, ""
    if lowered.startswith("customer next"):
        return "next", None, ""
    if lowered.startswith("customer threads") or lowered == "customer new":
        return "threads", None, "new"
    if lowered.startswith("customer followups"):
        return "threads", None, "followups"
    if lowered.startswith("customer show"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        return "show", target, ""
    if lowered.startswith("customer open"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        return "open", target, ""
    if lowered.startswith("customer preview"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        note = " ".join(parts[3:]).strip()
        return "preview", target, note
    if lowered.startswith("customer confirm"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        note = " ".join(parts[3:]).strip()
        return "confirm", target, note
    if lowered.startswith("customer verify"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        note = " ".join(parts[3:]).strip()
        return "verify", target, note
    if lowered.startswith("customer sent"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        note = " ".join(parts[3:]).strip()
        return "verify", target, note
    if lowered.startswith("customer capture"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        note = " ".join(parts[3:]).strip()
        return "capture", target, note
    for alias, follow_up_state in FOLLOW_UP_STATE_COMMANDS.items():
        prefix = f"customer {alias}"
        if lowered.startswith(prefix):
            parts = raw.split()
            target = parts[2] if len(parts) > 2 else None
            note = " ".join(parts[3:]).strip()
            return f"state::{follow_up_state}", target, note
    if lowered.startswith("reply only"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        note = " ".join(parts[3:]).strip()
        return "reply_only", target, note
    if lowered.startswith("reply_only"):
        parts = raw.split()
        target = parts[1] if len(parts) > 1 else None
        note = " ".join(parts[2:]).strip()
        return "reply_only", target, note

    parts = raw.split()
    first = parts[0].lower()
    if first == "customer" and len(parts) > 1:
        first = parts[1].lower()
        parts = parts[1:]
    command = RESOLUTION_COMMANDS.get(first, first)
    target = parts[1] if len(parts) > 1 else None
    note = " ".join(parts[2:]).strip()
    return command, target, note


def resolve_target_packet(items: list[dict[str, Any]], operator_state: dict[str, Any], token: str | None) -> dict[str, Any] | None:
    if token:
        normalized = token.strip()
        packet_by_short = {str(item.get("short_id") or "").lower(): item for item in items}
        if normalized.lower() in packet_by_short:
            return packet_by_short[normalized.lower()]
        order_matches = [
            item
            for item in items
            if str(((item.get("order_enrichment") or {}).get("receipt_id") or "")).strip() == normalized
            or str(((item.get("order_enrichment") or {}).get("transaction_id") or "")).strip() == normalized
        ]
        if order_matches:
            return order_matches[0]
    current_packet_id = operator_state.get("current_packet_id")
    for item in items:
        if item.get("packet_id") == current_packet_id:
            return item
    return items[0] if items else None


def _resolve_exact_packet(items: list[dict[str, Any]], token: str | None) -> dict[str, Any] | None:
    if not token:
        return None
    normalized = token.strip()
    packet_by_short = {str(item.get("short_id") or "").lower(): item for item in items}
    if normalized.lower() in packet_by_short:
        return packet_by_short[normalized.lower()]
    order_matches = [
        item
        for item in items
        if str(((item.get("order_enrichment") or {}).get("receipt_id") or "")).strip() == normalized
        or str(((item.get("order_enrichment") or {}).get("transaction_id") or "")).strip() == normalized
    ]
    if order_matches:
        return order_matches[0]
    return None


def _record_packet_decision(packet: dict[str, Any], resolution: str, note: str) -> dict[str, Any]:
    order = packet.get("order_enrichment") or {}
    row = {
        "artifact_id": packet.get("source_artifact_id"),
        "packet_id": packet.get("packet_id"),
        "receipt_id": order.get("receipt_id"),
        "transaction_id": order.get("transaction_id"),
        "resolution": normalize_resolution(resolution),
        "note": note,
        "recorded_at": now_iso(),
        "source": "customer_operator",
        "operator": "whatsapp_operator",
    }
    append_decision(row)
    return row


def _record_browser_capture(packet: dict[str, Any], note: str) -> None:
    captures = load_json(ETSY_BROWSER_CAPTURES_PATH, {"generated_at": now_iso(), "items": []})
    items = list(captures.get("items") or [])
    thread_key = str(packet.get("conversation_thread_key") or "").strip()
    source_artifact_id = str(packet.get("source_artifact_id") or "").strip()
    browser_url = _best_browser_url(packet)
    parsed = _parse_capture_note(packet, note)
    order = packet.get("order_enrichment") or {}
    existing_row: dict[str, Any] | None = None
    for item in items:
        same_thread = thread_key and str(item.get("conversation_thread_key") or "").strip() == thread_key
        same_source = source_artifact_id and str(item.get("source_artifact_id") or "").strip() == source_artifact_id
        if same_thread or same_source:
            existing_row = item
            break
    existing_thread_url = str((existing_row or {}).get("thread_url") or "").strip()
    if _browser_url_requires_manual_verification(browser_url) and existing_thread_url:
        browser_url = existing_thread_url
    captured_at = now_iso()
    row = {
        "conversation_thread_key": thread_key or None,
        "source_artifact_id": source_artifact_id or None,
        "packet_short_id": packet.get("short_id"),
        "browser_review_status": parsed["browser_review_status"],
        "follow_up_state": parsed["follow_up_state"],
        "latest_message_text": parsed["latest_message_text"],
        "customer_summary": parsed["customer_summary"],
        "draft_reply": parsed["draft_reply"],
        "recommended_action": parsed["recommended_action"],
        "custom_design_brief": parsed["custom_design_brief"],
        "missing_details": parsed["missing_details"],
        "task_progress_note": parsed["task_progress_note"],
        "last_customer_message": parsed["last_customer_message"],
        "last_seller_message": parsed["last_seller_message"],
        "open_loop_owner": parsed["open_loop_owner"],
        "reply_needed": parsed["reply_needed"],
        "thread_url": browser_url,
        "captured_at": captured_at,
        "unread": parsed["unread"],
        "order_ref": order.get("receipt_id"),
        "transaction_id": order.get("transaction_id"),
        "buyer_name": order.get("buyer_name") or packet.get("conversation_contact"),
    }
    replaced = False
    for index, item in enumerate(items):
        same_thread = thread_key and str(item.get("conversation_thread_key") or "").strip() == thread_key
        same_source = source_artifact_id and str(item.get("source_artifact_id") or "").strip() == source_artifact_id
        if same_thread or same_source:
            items[index] = {**item, **row}
            replaced = True
            break
    if not replaced:
        items.append(row)
    captures["generated_at"] = now_iso()
    captures["items"] = items
    write_json(ETSY_BROWSER_CAPTURES_PATH, captures)
    _record_customer_workflow_capture(packet, row)


def _rerun_observer() -> None:
    subprocess.run([sys.executable, str(ROOT / "runtime" / "phase1_observer.py")], cwd=str(ROOT), check=False)


def render_customer_status(items: list[dict[str, Any]], current: dict[str, Any] | None) -> str:
    browser_sync = load_browser_sync()
    counts = browser_sync.get("counts") or {}
    lines = [
        "Duck Ops customer status:",
        f"- Packets: {len(items)}",
        f"- New Etsy threads: {int(counts.get('needs_browser_review') or 0)}",
        f"- Active followups: {int(counts.get('active_followups') or 0)}",
        f"- Waiting on customer: {int(counts.get('threads_waiting_on_customer') or 0)}",
        f"- Resolved threads: {int(counts.get('resolved_threads') or 0)}",
    ]
    if current:
        lines.append(f"- Current packet: {current.get('short_id')} | {current.get('title')}")
    if items:
        lines.append("")
        lines.append("Next up:")
        for item in items[:3]:
            lines.append(f"- {item.get('short_id')} | {item.get('status')} | {item.get('title')}")
    else:
        lines.append("- No customer packets right now.")
    return "\n".join(lines)


def render_browser_threads(kind: str) -> str:
    browser_sync = load_browser_sync()
    items = list(browser_sync.get("items") or [])
    if kind == "new":
        filtered = [item for item in items if str(item.get("browser_review_status") or "") == "needs_browser_review"]
        heading = "Duck Ops new customer threads:"
    else:
        filtered = [
            item
            for item in items
            if str(item.get("follow_up_state") or "") in {"reply_drafted", "waiting_on_customer", "needs_reply", "ready_for_task", "concept_in_progress", "waiting_on_operator"}
        ]
        heading = "Duck Ops customer followups in motion:"

    lines = [heading, f"- Count: {len(filtered)}"]
    if not filtered:
        lines.append("- None right now.")
        return "\n".join(lines)

    lines.append("")
    for item in filtered[:10]:
        short_id = item.get("linked_customer_short_id") or "(no packet)"
        state = str(item.get("follow_up_state") or item.get("browser_review_status") or "unknown").replace("_", " ")
        contact = item.get("conversation_contact") or "Customer"
        summary = _trim_text(item.get("latest_message_preview") or item.get("browser_summary") or item.get("recommended_next_action"), 140)
        lines.append(f"- {short_id} | {contact} | {state}")
        lines.append(f"  {summary}")
        if item.get("linked_customer_short_id"):
            lines.append(f"  open: customer open {item.get('linked_customer_short_id')}")
        lines.append("")
    return "\n".join(lines).rstrip()


def handle_customer_text(text: str) -> str:
    packet_payload = load_packets()
    operator_state = load_operator_state()
    items = assign_short_ids(packet_payload, operator_state)
    current = sync_current_packet(items, operator_state)
    command, target_token, note = parse_customer_command(text)

    if command in {"status", "customer"}:
        write_customer_operator_outputs(packet_payload, operator_state)
        return render_customer_status(items, current)

    if command == "threads":
        write_customer_operator_outputs(packet_payload, operator_state)
        return render_browser_threads("followups" if target_token == "followups" or note == "followups" else "new")

    if command == "next":
        if not items:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "No customer action packets right now."
        current_index = 0
        if current:
            for idx, item in enumerate(items):
                if item.get("packet_id") == current.get("packet_id"):
                    current_index = idx
                    break
        next_item = items[(current_index + 1) % len(items)] if items else None
        operator_state["current_packet_id"] = next_item.get("packet_id") if next_item else None
        write_customer_operator_outputs(packet_payload, operator_state)
        return render_customer_card(next_item)

    if command == "show":
        target = resolve_target_packet(items, operator_state, target_token)
        write_customer_operator_outputs(packet_payload, operator_state)
        return render_customer_card(target)

    if command == "open":
        target = _resolve_target_thread_packet(items, operator_state, target_token)
        if not target:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "I couldn't find that customer packet."
        if not _best_browser_url(target):
            write_customer_operator_outputs(packet_payload, operator_state)
            return f"{target.get('short_id') or 'That packet'} does not have a browser URL yet."
        try:
            opened = _open_in_trusted_etsy_session(target)
        except Exception as exc:  # noqa: BLE001
            write_customer_operator_outputs(packet_payload, operator_state)
            return (
                f"I couldn't open {target.get('short_id') or 'that packet'} in the trusted Etsy session yet.\n"
                f"- Error: {exc}"
            )
        operator_state["current_packet_id"] = target.get("packet_id")
        write_customer_operator_outputs(packet_payload, operator_state)
        return (
            f"Opened {target.get('short_id')} in Etsy session `{opened.get('session_name')}`.\n"
            f"- Customer: {target.get('title')}\n"
            f"- URL: {opened.get('current_url')}\n"
            f"- Reused session: {bool(opened.get('reused_existing_session'))}\n"
            f"- Resolution strategy: {opened.get('target_resolution_strategy') or 'unknown'}\n"
            f"- Verification required: {bool(opened.get('target_verification_required'))}"
        )

    if command == "preview":
        target = _resolve_target_thread_packet(items, operator_state, target_token)
        if not target:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "I couldn't find that customer thread."
        reply_text = str(note or target.get("draft_reply") or "").strip()
        if not reply_text:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "There is no staged draft for that thread yet. Add reply text after the command or stage a draft first."
        try:
            preview = _stage_reply_preview_in_trusted_etsy_session(target, reply_text)
        except Exception as exc:  # noqa: BLE001
            _record_customer_preview_failure(target, str(exc))
            write_customer_operator_outputs(packet_payload, operator_state)
            return (
                f"I couldn't stage the preview for {target.get('short_id') or 'that thread'} yet.\n"
                f"- Error: {exc}"
            )
        _record_customer_preview_workflow(target, preview, reply_text)
        operator_state["current_packet_id"] = target.get("packet_id")
        write_customer_operator_outputs(packet_payload, operator_state)
        lines = [
            f"Preview staged for {target.get('short_id')} in Etsy session `{preview.get('session_name')}`.",
            f"- Customer: {target.get('title')}",
            f"- URL: {preview.get('current_url')}",
            f"- Resolution strategy: {preview.get('target_resolution_strategy') or 'unknown'}",
            f"- Verification required: {bool(preview.get('target_verification_required'))}",
        ]
        verification = preview.get("thread_verification") or {}
        if verification:
            lines.append(f"- Contact match: {bool(verification.get('contactMatch'))}")
            lines.append(f"- Summary matches: {', '.join(verification.get('summaryMatches') or []) or '(none)'}")
        if preview.get("preview_typed"):
            lines.append("- Reply is typed on the page and not sent.")
        else:
            lines.append(f"- Reply was not typed: {preview.get('preview_reason') or 'unknown'}")
        if preview.get("screenshot_path"):
            lines.append(f"- Screenshot: {preview.get('screenshot_path')}")
        lines.append("- Confirm in the Etsy page before any send.")
        return "\n".join(lines)

    if command == "confirm":
        target = _resolve_target_thread_packet(items, operator_state, target_token)
        if not target:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "I couldn't find that customer thread."
        reply_text = str(note or target.get("draft_reply") or "").strip()
        if not reply_text:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "There is no staged draft for that thread yet. Preview a reply first or include the exact reply text after the command."
        try:
            confirmation = _confirm_reply_preview_in_trusted_etsy_session(target, reply_text)
        except Exception as exc:  # noqa: BLE001
            _record_customer_preview_failure(target, str(exc))
            write_customer_operator_outputs(packet_payload, operator_state)
            return (
                f"I couldn't confirm the preview for {target.get('short_id') or 'that thread'} yet.\n"
                f"- Error: {exc}"
            )
        _record_customer_confirm_workflow(target, confirmation, reply_text)
        operator_state["current_packet_id"] = target.get("packet_id")
        write_customer_operator_outputs(packet_payload, operator_state)
        lines = [
            f"Preview confirmation checked for {target.get('short_id')} in Etsy session `{confirmation.get('session_name')}`.",
            f"- Customer: {target.get('title')}",
            f"- URL: {confirmation.get('current_url')}",
            f"- Resolution strategy: {confirmation.get('target_resolution_strategy') or 'unknown'}",
            f"- Verification required: {bool(confirmation.get('target_verification_required'))}",
        ]
        preview_state = confirmation.get("preview_state") or {}
        if preview_state:
            lines.append(f"- Textarea visible: {bool(preview_state.get('textareaVisible'))}")
            lines.append(f"- Reply matches typed draft: {bool(preview_state.get('valueMatches'))}")
            lines.append(f"- Submit visible: {bool(preview_state.get('submitVisible'))}")
        if confirmation.get("preview_confirmed"):
            lines.append("- Preview is approved for send. Send it in Etsy, then run `customer verify C###`.")
        else:
            lines.append(f"- Preview was not approved: {confirmation.get('confirmation_reason') or 'preview mismatch'}")
        if confirmation.get("screenshot_path"):
            lines.append(f"- Screenshot: {confirmation.get('screenshot_path')}")
        return "\n".join(lines)

    if command == "verify":
        target = _resolve_target_thread_packet(items, operator_state, target_token)
        if not target:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "I couldn't find that customer thread."
        reply_text = str(note or target.get("draft_reply") or "").strip()
        if not reply_text:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "I need the exact sent reply text to verify this thread. Pass it after the command or keep the staged draft on the thread."
        try:
            verification = _verify_reply_sent_in_trusted_etsy_session(target, reply_text)
        except Exception as exc:  # noqa: BLE001
            _record_customer_preview_failure(target, str(exc))
            write_customer_operator_outputs(packet_payload, operator_state)
            return (
                f"I couldn't verify the send result for {target.get('short_id') or 'that thread'} yet.\n"
                f"- Error: {exc}"
            )
        if verification.get("reply_sent_verified"):
            capture_note = (
                "state: waiting_on_customer; "
                "reply_needed: no; "
                "open_loop: customer; "
                "summary: Sent reply in Etsy and waiting on the customer.; "
                f"last_seller_message: {reply_text}; "
                "action: Wait for the customer to respond."
            )
            _record_browser_capture(target, capture_note)
            _rerun_observer()
        _record_customer_send_verification_workflow(target, verification, reply_text)
        packet_payload = load_packets()
        write_customer_operator_outputs(packet_payload, operator_state)
        lines = [
            f"Send verification checked for {target.get('short_id')} in Etsy session `{verification.get('session_name')}`.",
            f"- Customer: {target.get('title')}",
            f"- URL: {verification.get('current_url')}",
            f"- Resolution strategy: {verification.get('target_resolution_strategy') or 'unknown'}",
            f"- Verification required: {bool(verification.get('target_verification_required'))}",
        ]
        posted_state = verification.get("posted_state") or {}
        if posted_state:
            lines.append(f"- Reply visible in thread: {bool(posted_state.get('bodyContainsReply'))}")
            lines.append(f"- Reply box still visible: {bool(posted_state.get('textareaVisible'))}")
        if verification.get("reply_sent_verified"):
            lines.append("- Verified: the reply is posted and the thread is now waiting on the customer.")
        else:
            lines.append(f"- Not yet verified: {verification.get('verification_reason') or 'reply not visible yet'}")
        if verification.get("screenshot_path"):
            lines.append(f"- Screenshot: {verification.get('screenshot_path')}")
        return "\n".join(lines)

    if command == "capture":
        target = _resolve_target_thread_packet(items, operator_state, target_token)
        if not target:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "I couldn't find that customer packet."
        if not note:
            write_customer_operator_outputs(packet_payload, operator_state)
            return (
                "Add a short browser summary. Example: "
                "`customer capture C301 summary: customer wants patriotic Afroman duck; "
                "reply: I can do that and will send the first concept tomorrow.; "
                "state: ready_for_task; brief: patriotic Afroman duck with flag hat`"
            )
        _record_browser_capture(target, note)
        _rerun_observer()
        packet_payload = load_packets()
        write_customer_operator_outputs(packet_payload, operator_state)
        return f"Recorded Etsy browser capture for {target.get('short_id')}."

    if command.startswith("state::"):
        target = _resolve_target_thread_packet(items, operator_state, target_token)
        if not target:
            write_customer_operator_outputs(packet_payload, operator_state)
            return "I couldn't find that customer packet."
        follow_up_state = command.split("::", 1)[1]
        _record_browser_capture(target, _quick_capture_note(follow_up_state, note))
        _rerun_observer()
        packet_payload = load_packets()
        write_customer_operator_outputs(packet_payload, operator_state)
        friendly = follow_up_state.replace("_", " ")
        return f"Marked {target.get('short_id')} as `{friendly}`."

    if command not in {"replacement", "refund", "wait", "reply_only"}:
        write_customer_operator_outputs(packet_payload, operator_state)
        return (
            "Customer operator commands:\n"
            "- customer status\n"
            "- customer threads\n"
            "- customer followups\n"
            "- customer next\n"
            "- customer open C301\n"
            "- customer preview C301\n"
            "- customer confirm C301\n"
            "- customer verify C301\n"
            "- customer capture C301 <what you learned in the thread>\n"
            "- customer drafted C301 <reply text>\n"
            "- customer waiting C301 <what we are waiting on>\n"
            "- customer resolved C301 <resolution note>\n"
            "- customer taskready C301 <brief summary>\n"
            "- replacement C301 because ...\n"
            "- refund C301 because ...\n"
            "- wait C301 because ...\n"
            "- reply only C301 because ..."
        )

    target = resolve_target_packet(items, operator_state, target_token)
    if not target:
        write_customer_operator_outputs(packet_payload, operator_state)
        return "I couldn't find that customer packet."
    if not note:
        write_customer_operator_outputs(packet_payload, operator_state)
        return f"Please add a short reason. Example: `{command} {target.get('short_id')} because ...`"

    _record_packet_decision(target, command, note)
    _rerun_observer()
    packet_payload = load_packets()
    items = assign_short_ids(packet_payload, operator_state)
    current = sync_current_packet(items, operator_state)
    target = resolve_target_packet(items, operator_state, target.get("short_id"))
    write_customer_operator_outputs(packet_payload, operator_state)
    response = f"Recorded customer decision: {target.get('short_id') if target else 'packet'} -> `{command}`. Note: {note}"
    if current and target and current.get("packet_id") == target.get("packet_id"):
        next_response = render_customer_card(current)
        return response + "\n\nUpdated customer packet:\n\n" + next_response
    return response


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Duck Ops customer operator lane")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("message", help="Show the current customer packet")
    sub.add_parser("status", help="Show customer packet status")
    backfill_parser = sub.add_parser("backfill-links", help="Resolve and persist direct Etsy thread URLs")
    backfill_parser.add_argument("--limit", type=int, default=None)
    backfill_parser.add_argument("--timeout-seconds", type=int, default=30)
    handle_parser = sub.add_parser("handle", help="Handle a plain-language customer operator reply")
    handle_parser.add_argument("--text", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    packet_payload = load_packets()
    operator_state = load_operator_state()
    if args.command == "message":
        payload = write_customer_operator_outputs(packet_payload, operator_state)
        items = payload.get("items") or []
        current = None
        current_short_id = payload.get("current_short_id")
        for item in items:
            if item.get("short_id") == current_short_id:
                current = item
                break
        print(render_customer_card(current))
        return 0
    if args.command == "status":
        payload = write_customer_operator_outputs(packet_payload, operator_state)
        items = payload.get("items") or []
        current = None
        current_short_id = payload.get("current_short_id")
        for item in items:
            if item.get("short_id") == current_short_id:
                current = item
                break
        print(render_customer_status(items, current))
        return 0
    if args.command == "backfill-links":
        summary = backfill_exact_thread_urls(limit=args.limit, timeout_seconds=args.timeout_seconds)
        print(json.dumps(summary, indent=2))
        return 0
    if args.command == "handle":
        print(handle_customer_text(args.text))
        return 0
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
