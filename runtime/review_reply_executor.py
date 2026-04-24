#!/usr/bin/env python3
"""
Deterministic queueing and execution helper for Etsy public review replies.

This script currently supports:
- queueing an approved review-reply artifact for execution
- auto-queueing publish-ready Etsy public replies when policy allows it
- navigating to the exact Etsy review row in a signed-in seller session
- opening the reply box if needed
- filling the exact approved reply text in a dry-run path
- submitting the exact approved reply text only after the target and text are re-verified
- draining the queued review replies in a deterministic batch
- recording auditable attempt packets for both dry-run and submit paths
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from decision_writer import ensure_parent, load_output_patterns, render_pattern, slugify, write_decision
from review_reply_discovery import (
    DEFAULT_ETSY_REVIEWS_URL,
    capture_target_review_screenshot,
    load_discovery_config,
    locate_review_block,
    navigate_within_session,
    navigate_to_reviews_surface,
    parse_eval_json,
    parse_page_metadata,
    run_pw_command,
    session_is_open,
)
from etsy_browser_guard import blocked_status as etsy_browser_blocked_status
from etsy_browser_guard import cleanup_stale_playwright_processes
from workflow_control import record_workflow_transition


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
OUTPUT_DIR = ROOT / "output"
DUCK_AGENT_ROOT = ROOT.parents[2] / "duckAgent"
CONFIG_DIR = ROOT / "config"

QUALITY_GATE_STATE_PATH = STATE_DIR / "quality_gate_state.json"
EXECUTION_QUEUE_STATE_PATH = STATE_DIR / "review_reply_execution_queue.json"
EXECUTION_SESSION_STATE_PATH = STATE_DIR / "review_reply_execution_sessions.json"
EXECUTION_AUTH_STATE_PATH = STATE_DIR / "review_reply_execution_auth.json"
AUTH_STORAGE_DIR = STATE_DIR / "review_reply_execution_auth_storage"
DISCOVERY_SESSION_STATE_PATH = STATE_DIR / "review_reply_discovery_sessions.json"
DISCOVERY_APPROVALS_PATH = STATE_DIR / "review_reply_discovery_approvals.json"
DISCOVERY_OUTPUT_DIR = OUTPUT_DIR / "discovery"
EXECUTION_POLICY_PATH = CONFIG_DIR / "review_reply_execution.json"

DEFAULT_EXECUTION_POLICY: dict[str, Any] = {
    "auto_execution_enabled": True,
    "auto_queue_publish_ready_positive": True,
    "auto_queue_requires_browser_approval": True,
    "auto_drain_enabled": True,
    "auto_drain_max_submits_per_run": 3,
    "auto_drain_close_browser_after_run": True,
    "auto_drain_send_session_summary": True,
    "stop_after_first_failure": True,
    "review_page_max_probe": 10,
    "retryable_row_not_found_enabled": True,
    "retryable_row_not_found_max_attempts": 3,
    "retryable_row_not_found_retry_delay_seconds": 3600,
    "auth_block_retry_delay_seconds": 1800,
    "auth_alert_cooldown_seconds": 21600,
    "auth_storage_state_enabled": True,
    "auth_storage_restore_on_open": True,
    "auth_storage_restore_on_auth_failure": True,
    "auth_storage_save_on_healthy": True,
}


def _review_execution_workflow_id(artifact_id: str) -> str:
    return f"review_execution::{artifact_id}"


def _review_execution_metadata(artifact_id: str, decision: dict[str, Any]) -> dict[str, Any]:
    target = decision.get("review_target") or {}
    preview = decision.get("preview") or {}
    return {
        "artifact_id": artifact_id,
        "flow": decision.get("flow"),
        "review_status": decision.get("review_status"),
        "execution_mode": decision.get("execution_mode"),
        "decision": decision.get("decision"),
        "transaction_id": target.get("transaction_id"),
        "listing_id": target.get("listing_id"),
        "context_excerpt": _reply_excerpt(str(preview.get("context_text") or ""), limit=180),
    }


def _record_review_execution_transition(
    artifact_id: str,
    decision: dict[str, Any],
    *,
    state: str,
    state_reason: str,
    requires_confirmation: bool | None = None,
    last_side_effect: dict[str, Any] | None = None,
    last_verification: dict[str, Any] | None = None,
    next_action: str | None = None,
    receipt_kind: str | None = None,
    receipt_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return record_workflow_transition(
        workflow_id=_review_execution_workflow_id(artifact_id),
        lane="review_execution",
        display_label=f"Review Execution {artifact_id}",
        entity_id=artifact_id,
        run_id=str(decision.get("run_id") or artifact_id),
        state=state,
        state_reason=state_reason,
        requires_confirmation=requires_confirmation,
        last_side_effect=last_side_effect,
        last_verification=last_verification,
        next_action=next_action,
        metadata=_review_execution_metadata(artifact_id, decision),
        receipt_kind=receipt_kind,
        receipt_payload=receipt_payload,
        history_summary=state_reason.replace("_", " "),
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_quality_gate_state() -> dict[str, Any]:
    return load_json(QUALITY_GATE_STATE_PATH, {"artifacts": {}})


def save_quality_gate_state(payload: dict[str, Any]) -> None:
    write_json(QUALITY_GATE_STATE_PATH, payload)


def load_queue_state() -> dict[str, Any]:
    return load_json(EXECUTION_QUEUE_STATE_PATH, {"generated_at": None, "items": {}})


def save_queue_state(payload: dict[str, Any]) -> None:
    payload["generated_at"] = now_iso()
    write_json(EXECUTION_QUEUE_STATE_PATH, payload)


def load_discovery_approvals() -> dict[str, Any]:
    return load_json(DISCOVERY_APPROVALS_PATH, {"approvals": {}})


def load_execution_policy() -> dict[str, Any]:
    payload = load_json(EXECUTION_POLICY_PATH, {})
    policy = dict(DEFAULT_EXECUTION_POLICY)
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in policy:
                policy[key] = value
    return policy


def load_session_state() -> dict[str, Any]:
    return load_json(
        EXECUTION_SESSION_STATE_PATH,
        {
            "generated_at": None,
            "current_session_id": None,
            "sessions": {},
        },
    )


def _default_storage_state() -> dict[str, Any]:
    return {
        "path": None,
        "exists": False,
        "saved_at": None,
        "last_restore_at": None,
        "last_restore_status": None,
        "last_restore_reason": None,
        "last_restore_error": None,
        "last_save_at": None,
        "last_save_status": None,
        "last_save_error": None,
    }


def save_session_state(payload: dict[str, Any]) -> None:
    payload["generated_at"] = now_iso()
    write_json(EXECUTION_SESSION_STATE_PATH, payload)


def load_auth_state() -> dict[str, Any]:
    payload = load_json(EXECUTION_AUTH_STATE_PATH, {})
    auth_state = {
        "generated_at": None,
        "auth_status": "unknown",
        "blocked_at": None,
        "cleared_at": None,
        "last_auth_check_at": None,
        "last_error": None,
        "last_session_name": None,
        "last_checked_url": None,
        "next_retry_after": None,
        "last_alert_sent_at": None,
        "last_alert_subject": None,
        "storage_state": _default_storage_state(),
    }
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "storage_state" and isinstance(value, dict):
                storage_state = _default_storage_state()
                storage_state.update(value)
                auth_state["storage_state"] = storage_state
            elif key in auth_state:
                auth_state[key] = value
    return auth_state


def save_auth_state(payload: dict[str, Any]) -> None:
    payload["generated_at"] = now_iso()
    write_json(EXECUTION_AUTH_STATE_PATH, payload)


def auth_storage_state_path(session_name: str) -> Path:
    return AUTH_STORAGE_DIR / f"{slugify(session_name)}.json"


def _session_local_storage_path(allowed_roots: list[Path], session_name: str) -> Path | None:
    for root in allowed_roots:
        if not root.exists():
            continue
        if root.name == ".playwright-cli":
            base = root / "review_reply_execution_auth_storage"
        else:
            base = root / ".playwright-cli" / "review_reply_execution_auth_storage"
        return base / f"{slugify(session_name)}.json"
    return None


def current_storage_state(session_name: str, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    storage_state = _default_storage_state()
    if isinstance(existing, dict):
        storage_state.update(existing)
    path = Path(str(storage_state.get("path") or auth_storage_state_path(session_name)))
    storage_state["path"] = str(path)
    storage_state["exists"] = path.exists()
    if path.exists() and not storage_state.get("saved_at"):
        storage_state["saved_at"] = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone().isoformat()
    return storage_state


def merge_storage_state(
    auth_state: dict[str, Any],
    session_name: str,
    storage_state: dict[str, Any] | None,
) -> dict[str, Any]:
    auth_state["storage_state"] = current_storage_state(
        session_name,
        storage_state if isinstance(storage_state, dict) else (auth_state.get("storage_state") or {}),
    )
    return auth_state


def _new_session_id() -> str:
    return f"session-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"


def _session_counts(session: dict[str, Any]) -> dict[str, int]:
    items = list((session.get("items") or {}).values())
    return {
        "posted": sum(1 for item in items if str(item.get("status") or "") == "posted"),
        "failed": sum(1 for item in items if str(item.get("status") or "") == "failed"),
        "skipped": sum(1 for item in items if str(item.get("status") or "") == "skipped"),
        "total": len(items),
    }


def ensure_open_session(session_state: dict[str, Any]) -> dict[str, Any]:
    sessions = session_state.setdefault("sessions", {})
    current_session_id = str(session_state.get("current_session_id") or "")
    current = sessions.get(current_session_id)
    if isinstance(current, dict) and str(current.get("status") or "") == "open":
        return current

    session_id = _new_session_id()
    current = {
        "session_id": session_id,
        "status": "open",
        "started_at": now_iso(),
        "last_activity_at": None,
        "items": {},
        "summary_sent_at": None,
        "summary_subject": None,
        "summary_artifact_paths": None,
    }
    sessions[session_id] = current
    session_state["current_session_id"] = session_id
    return current


def current_open_session(session_state: dict[str, Any]) -> dict[str, Any] | None:
    sessions = session_state.get("sessions") or {}
    session_id = str(session_state.get("current_session_id") or "")
    current = sessions.get(session_id)
    if isinstance(current, dict) and str(current.get("status") or "") == "open":
        return current
    return None


def _known_review_reply_browser_sessions() -> list[str]:
    names: list[str] = []
    session_name, _ = choose_session()
    if session_name:
        names.append(session_name)

    discovery_state = load_json(DISCOVERY_SESSION_STATE_PATH, {"sessions": {}})
    for name in (discovery_state.get("sessions") or {}).keys():
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def cleanup_review_reply_browsers(*, force_kill_temp_profiles: bool = False) -> dict[str, Any]:
    attempted: list[dict[str, Any]] = []
    closed_count = 0
    open_before_count = 0

    for session_name in _known_review_reply_browser_sessions():
        was_open = session_is_open(session_name)
        if was_open:
            open_before_count += 1
        try:
            run_pw_command(session_name, "close")
            closed = was_open or not session_is_open(session_name)
            attempted.append(
                {
                    "session_name": session_name,
                    "was_open": was_open,
                    "closed": closed,
                }
            )
            if closed and was_open:
                closed_count += 1
        except subprocess.CalledProcessError as exc:
            attempted.append(
                {
                    "session_name": session_name,
                    "was_open": was_open,
                    "closed": False,
                    "error": _pw_error_text(exc),
                }
            )

    forced_cleanup = None
    if force_kill_temp_profiles:
        forced_cleanup = cleanup_stale_playwright_processes(
            stale_after_seconds=0,
            force=True,
            reason="review_reply_browser_cleanup",
            respect_keepalive=False,
        )
    cleanup_suffix = "."
    if forced_cleanup:
        cleaned_groups = int(forced_cleanup.get("killed_group_count") or 0)
        if cleaned_groups > 0:
            cleanup_suffix = (
                f" and force-cleaned {cleaned_groups} lingering Playwright browser group"
                f"{'' if cleaned_groups == 1 else 's'}."
            )
        else:
            cleanup_suffix = " and confirmed there were no lingering Playwright browser groups to clean."

    return {
        "ok": True,
        "status": "completed",
        "message": (
            f"Closed {closed_count} review-reply automation browser session"
            f"{'' if closed_count == 1 else 's'}"
            + cleanup_suffix
        ),
        "open_before_count": open_before_count,
        "closed_count": closed_count,
        "attempted": attempted,
        "forced_cleanup": forced_cleanup,
    }


def backfill_session_from_queue(
    session_state: dict[str, Any],
    quality_state: dict[str, Any],
    queue_state: dict[str, Any],
) -> dict[str, Any] | None:
    if current_open_session(session_state):
        return current_open_session(session_state)
    if session_state.get("sessions"):
        return None

    queue_items = queue_state.get("items") or {}
    completed = []
    for artifact_id, queue_item in queue_items.items():
        status = str((queue_item or {}).get("status") or "")
        if status not in {"posted", "failed", "skipped"}:
            continue
        record = (quality_state.get("artifacts") or {}).get(artifact_id)
        decision = (record or {}).get("decision") or {}
        attempts = decision.get("execution_attempts") or []
        latest_attempt = attempts[-1] if attempts else {}
        completed.append((artifact_id, decision, queue_item, latest_attempt))
    if not completed:
        return None

    session = ensure_open_session(session_state)
    earliest = None
    latest = None
    for artifact_id, decision, queue_item, latest_attempt in completed:
        updated_at = (
            queue_item.get("last_attempt_at")
            or latest_attempt.get("finished_at")
            or latest_attempt.get("started_at")
            or now_iso()
        )
        session.setdefault("items", {})[artifact_id] = {
            "artifact_id": artifact_id,
            "status": queue_item.get("status"),
            "decision": decision.get("decision"),
            "review_status": decision.get("review_status"),
            "updated_at": updated_at,
            "attempt_id": queue_item.get("last_attempt_id") or latest_attempt.get("attempt_id"),
            "attempt_type": latest_attempt.get("attempt_type"),
            "transaction_id": (decision.get("review_target") or {}).get("transaction_id"),
            "listing_id": (decision.get("review_target") or {}).get("listing_id"),
            "customer_review": ((decision.get("preview") or {}).get("context_text")) or "",
            "approved_reply_text": decision.get("approved_reply_text") or "",
            "error": latest_attempt.get("error"),
            "attempt_outcome": queue_item.get("last_attempt_outcome") or latest_attempt.get("outcome"),
            "attempt_paths": latest_attempt.get("artifact_paths"),
        }
        if earliest is None or str(updated_at) < str(earliest):
            earliest = updated_at
        if latest is None or str(updated_at) > str(latest):
            latest = updated_at
    if earliest:
        session["started_at"] = earliest
    session["last_activity_at"] = latest
    session["notes"] = "Backfilled from existing execution queue after session batching was introduced."
    return session


def _reply_excerpt(value: str, limit: int = 120) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _page_label_from_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        parsed = urlparse(str(url))
        raw = (parse_qs(parsed.query).get("page") or [None])[0]
        if raw is None:
            return None
        return str(raw)
    except Exception:
        return None


def build_attempt_breadcrumbs(attempt: dict[str, Any]) -> dict[str, Any]:
    session = attempt.get("session") if isinstance(attempt.get("session"), dict) else {}
    navigation = attempt.get("navigation") if isinstance(attempt.get("navigation"), dict) else {}
    initial_match = attempt.get("initial_match") if isinstance(attempt.get("initial_match"), dict) else {}
    post_click_match = attempt.get("post_click_match") if isinstance(attempt.get("post_click_match"), dict) else {}
    refresh = attempt.get("surface_refresh") if isinstance(attempt.get("surface_refresh"), dict) else {}
    probes = attempt.get("review_page_probes") if isinstance(attempt.get("review_page_probes"), list) else []

    simplified_probes: list[dict[str, Any]] = []
    for probe in probes[:6]:
        if not isinstance(probe, dict):
            continue
        simplified_probes.append(
            {
                "page": _page_label_from_url(str(probe.get("url") or "")),
                "found": bool(probe.get("found")),
                "matched_transaction_id": probe.get("matched_transaction_id"),
                "matched_listing_id": probe.get("matched_listing_id"),
            }
        )

    return {
        "session_url": str(session.get("current_url") or ""),
        "auth_probe": session.get("auth_probe") if isinstance(session.get("auth_probe"), dict) else {},
        "navigation_strategy": str(navigation.get("strategy") or ""),
        "landed_url": str(navigation.get("landed_url") or ""),
        "initial_match_found": bool(initial_match.get("found")),
        "initial_candidate_count": initial_match.get("candidateCount"),
        "initial_transaction_match": initial_match.get("matchedTransactionId"),
        "initial_listing_match": initial_match.get("matchedListingId"),
        "page_probes": simplified_probes,
        "surface_refresh_attempted": bool(refresh),
        "surface_refresh_found": bool(refresh.get("found")) if refresh else False,
        "surface_refresh_url": str(refresh.get("landed_url") or ""),
        "post_click_found": bool(post_click_match.get("found")) if post_click_match else False,
        "post_click_reply_box_visible": bool(post_click_match.get("replyBoxVisible")) if post_click_match else False,
    }


def summarize_attempt_breadcrumbs(breadcrumbs: dict[str, Any]) -> str:
    parts: list[str] = []
    session_url = str(breadcrumbs.get("session_url") or "")
    if session_url:
        parts.append(f"session {session_url}")
    auth_probe = breadcrumbs.get("auth_probe") if isinstance(breadcrumbs.get("auth_probe"), dict) else {}
    if auth_probe:
        parts.append(
            "auth sign_in_visible="
            f"{bool(auth_probe.get('signInVisible'))}"
            f" seller_controls_visible={bool(auth_probe.get('sellerControlsVisible'))}"
        )
    if breadcrumbs.get("navigation_strategy"):
        parts.append(f"nav {breadcrumbs.get('navigation_strategy')}")
    if breadcrumbs.get("initial_match_found"):
        parts.append(
            "initial match"
            f" tx={breadcrumbs.get('initial_transaction_match') or 'n/a'}"
            f" listing={breadcrumbs.get('initial_listing_match') or 'n/a'}"
        )
    else:
        parts.append(f"initial match not found (candidates={breadcrumbs.get('initial_candidate_count') or 0})")
    probes = breadcrumbs.get("page_probes") if isinstance(breadcrumbs.get("page_probes"), list) else []
    if probes:
        probe_bits = []
        for probe in probes:
            if not isinstance(probe, dict):
                continue
            label = probe.get("page") or "?"
            probe_bits.append(f"p{label}:{'hit' if probe.get('found') else 'miss'}")
        if probe_bits:
            parts.append("probe pages " + ", ".join(probe_bits))
    if breadcrumbs.get("surface_refresh_attempted"):
        parts.append(
            "surface refresh "
            + ("found row" if breadcrumbs.get("surface_refresh_found") else "still missing row")
        )
    if breadcrumbs.get("post_click_found"):
        parts.append(
            "post-click "
            + ("reply box visible" if breadcrumbs.get("post_click_reply_box_visible") else "reply box missing")
        )
    return " | ".join(parts) or "No breadcrumbs captured."


def classify_attempt_failure(attempt: dict[str, Any], error_text: str) -> dict[str, Any]:
    lowered = str(error_text or "").lower()
    failure_class = "unexpected_executor_failure"
    phase = "unknown"
    retryable = False

    if is_retryable_row_not_found(error_text):
        failure_class = "review_row_not_found"
        phase = "preflight"
        retryable = True
    elif "sign in again" in lowered or "signed-out view" in lowered or "etsy auth is required" in lowered:
        failure_class = "auth_required"
        phase = "auth"
    elif "did not keep the expected transaction_id" in lowered:
        failure_class = "review_row_transaction_mismatch"
        phase = "preflight"
    elif "did not keep the expected listing_id" in lowered:
        failure_class = "review_row_listing_mismatch"
        phase = "preflight"
    elif "could not open the reply box" in lowered:
        failure_class = "reply_box_open_failed"
        phase = "preflight"
    elif "reply textarea did not appear" in lowered:
        failure_class = "reply_box_not_visible"
        phase = "preflight"
    elif "textarea fill verification failed" in lowered or "could not stage the exact approved reply text" in lowered:
        failure_class = "textarea_fill_failed"
        phase = "fill"
    elif "could not inspect the target review row before submit" in lowered:
        failure_class = "pre_submit_inspection_failed"
        phase = "pre_submit"
    elif "reply textarea disappeared before submit" in lowered:
        failure_class = "reply_box_disappeared"
        phase = "pre_submit"
    elif "textarea no longer matches" in lowered:
        failure_class = "pre_submit_text_mismatch"
        phase = "pre_submit"
    elif "submit control is not visible" in lowered or "submit control is disabled" in lowered:
        failure_class = "submit_control_unavailable"
        phase = "pre_submit"
    elif "could not click the etsy submit control" in lowered:
        failure_class = "submit_click_failed"
        phase = "submit"
    elif "did not show a clear post-submit success state" in lowered:
        failure_class = "post_submit_verification_failed"
        phase = "post_submit"

    blocked = etsy_browser_blocked_status()
    return {
        "failure_class": failure_class,
        "phase": phase,
        "retryable": retryable,
        "browser_guard_active": bool(blocked.get("blocked")),
        "browser_guard_reason": blocked.get("block_reason"),
        "browser_guard_until": blocked.get("blocked_until"),
    }


def annotate_attempt_failure(attempt: dict[str, Any], error_text: str) -> dict[str, Any]:
    breadcrumbs = build_attempt_breadcrumbs(attempt)
    failure = classify_attempt_failure(attempt, error_text)
    failure["breadcrumb_summary"] = summarize_attempt_breadcrumbs(breadcrumbs)
    attempt["breadcrumbs"] = breadcrumbs
    attempt["failure"] = failure
    return failure


def _pw_error_text(exc: subprocess.CalledProcessError) -> str:
    return re.sub(r"\s+", " ", f"{exc.stdout}\n{exc.stderr}".strip()).strip()


def _pw_cli_error(output: str) -> str | None:
    text = str(output or "").strip()
    if not text.startswith("### Error"):
        return None
    cleaned = re.sub(r"^### Error\s*", "", text, count=1).strip()
    return re.sub(r"\s+", " ", cleaned).strip() or "Unknown Playwright CLI error"


def _allowed_roots_from_error(error_text: str | None) -> list[Path]:
    if not error_text:
        return []
    match = re.search(r"Allowed roots:\s*(?P<roots>.+)$", error_text)
    if not match:
        return []
    roots: list[Path] = []
    for raw_root in match.group("roots").split(","):
        root = raw_root.strip()
        if root:
            roots.append(Path(root))
    return roots


def _run_pw_command_checked(session_name: str, *args: str) -> dict[str, Any]:
    try:
        output = run_pw_command(session_name, *args)
    except subprocess.CalledProcessError as exc:
        error_text = _pw_error_text(exc)
        return {
            "ok": False,
            "output": error_text,
            "error": error_text,
            "allowed_roots": _allowed_roots_from_error(error_text),
        }
    error_text = _pw_cli_error(output)
    return {
        "ok": error_text is None,
        "output": output,
        "error": error_text,
        "allowed_roots": _allowed_roots_from_error(error_text),
    }


def auth_retry_due(auth_state: dict[str, Any]) -> bool:
    retry_at = parse_iso(str(auth_state.get("next_retry_after") or ""))
    if retry_at is None:
        return True
    return retry_at <= datetime.now(timezone.utc).astimezone()


def auth_block_active(auth_state: dict[str, Any]) -> bool:
    return str(auth_state.get("auth_status") or "") == "blocked" and not auth_retry_due(auth_state)


def is_auth_error(error: Exception | str | None) -> bool:
    text = str(error or "").lower()
    markers = (
        "not authenticated",
        "showing a public signed-out view",
        "sign in again",
        "redirected to sign-in",
        "/signin",
        "public signed-out",
    )
    return any(marker in text for marker in markers)


def is_cooldown_error(error: Exception | str | None) -> bool:
    text = str(error or "").lower()
    markers = (
        "etsy automation is cooling down until",
        "shared pacing budget",
        "rate_limit_preemptive_cooldown",
        "cooling down because",
    )
    return any(marker in text for marker in markers)


def is_signed_out_state(current_url: str | None, auth_probe: dict[str, Any] | None) -> bool:
    return "/signin" in str(current_url or "").lower() or bool((auth_probe or {}).get("signInVisible"))


def _load_send_email():
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:  # noqa: BLE001
        load_dotenv = None

    if load_dotenv is not None:
        load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
    env_path = DUCK_AGENT_ROOT / ".env"
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    duck_agent_path = str(DUCK_AGENT_ROOT)
    if duck_agent_path not in sys.path:
        sys.path.insert(0, duck_agent_path)
    try:
        from helpers.email_helper import send_email  # type: ignore
    except ModuleNotFoundError:
        helper_path = DUCK_AGENT_ROOT / "helpers" / "email_helper.py"
        spec = importlib.util.spec_from_file_location("duckagent_email_helper", helper_path)
        if spec is None or spec.loader is None:
            raise
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        send_email = module.send_email  # type: ignore[attr-defined]

    return send_email


def choose_session() -> tuple[str, str]:
    config = load_discovery_config()
    entries = config.get("entry_points") or []
    for entry in entries:
        if entry.get("id") == "seller_dashboard":
            return (
                str(entry.get("session_name") or "esd"),
                str(entry.get("url") or DEFAULT_ETSY_REVIEWS_URL),
            )
    return ("esd", DEFAULT_ETSY_REVIEWS_URL)


def restore_auth_storage_state(
    session_name: str,
    start_url: str,
    *,
    reason: str,
    existing_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    storage_state = current_storage_state(session_name, existing_state)
    storage_state["last_restore_at"] = now_iso()
    storage_state["last_restore_reason"] = reason
    storage_state["last_restore_error"] = None
    path = Path(str(storage_state.get("path") or ""))
    if not path.exists():
        storage_state["last_restore_status"] = "missing"
        storage_state["exists"] = False
        return storage_state
    session_local_path: Path | None = None
    result = _run_pw_command_checked(session_name, "state-load", str(path))
    if not result["ok"]:
        session_local_path = _session_local_storage_path(result.get("allowed_roots") or [], session_name)
        if session_local_path is not None:
            ensure_parent(session_local_path).write_bytes(path.read_bytes())
            result = _run_pw_command_checked(session_name, "state-load", str(session_local_path))
    if not result["ok"]:
        storage_state["last_restore_status"] = "failed"
        storage_state["last_restore_error"] = result.get("error")
        storage_state["exists"] = path.exists()
        if session_local_path is not None:
            storage_state["session_local_path"] = str(session_local_path)
        return storage_state

    time.sleep(0.5)
    landed_url, landed_title = navigate_within_session(session_name, start_url, wait_seconds=1.5)
    snapshot_output = run_pw_command(session_name, "snapshot")
    current_url, page_title = parse_page_metadata(snapshot_output)
    storage_state["last_restore_status"] = "restored"
    storage_state["exists"] = True
    storage_state["saved_at"] = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone().isoformat()
    storage_state["current_url"] = current_url or landed_url
    storage_state["page_title"] = page_title or landed_title
    if session_local_path is not None:
        storage_state["session_local_path"] = str(session_local_path)
    return storage_state


def save_auth_storage_state(
    session_name: str,
    existing_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    storage_state = current_storage_state(session_name, existing_state)
    storage_state["last_save_at"] = now_iso()
    storage_state["last_save_error"] = None
    path = Path(str(storage_state.get("path") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    session_local_path: Path | None = None
    result = _run_pw_command_checked(session_name, "state-save", str(path))
    if result.get("ok") and not path.exists():
        result = {
            "ok": False,
            "error": f"Playwright state-save returned success but did not create `{path}`.",
            "allowed_roots": [],
        }
    if not result["ok"]:
        session_local_path = _session_local_storage_path(result.get("allowed_roots") or [], session_name)
        if session_local_path is not None:
            ensure_parent(session_local_path)
            retry = _run_pw_command_checked(session_name, "state-save", str(session_local_path))
            if retry.get("ok") and session_local_path.exists():
                ensure_parent(path).write_bytes(session_local_path.read_bytes())
                result = retry
            else:
                result = retry
    if result.get("ok") and path.exists():
        storage_state["last_save_status"] = "saved"
        storage_state["exists"] = True
        storage_state["saved_at"] = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone().isoformat()
        if session_local_path is not None:
            storage_state["session_local_path"] = str(session_local_path)
        return storage_state

    storage_state["last_save_status"] = "failed"
    storage_state["last_save_error"] = result.get("error")
    storage_state["exists"] = path.exists()
    if session_local_path is not None:
        storage_state["session_local_path"] = str(session_local_path)
    return storage_state


def artifact_record(state: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    record = (state.get("artifacts") or {}).get(artifact_id)
    if not isinstance(record, dict):
        raise SystemExit(f"Unknown artifact_id: {artifact_id}")
    return record


def latest_discovery_packet_for_artifact(artifact_id: str) -> dict[str, Any] | None:
    candidates = []
    for path in sorted(DISCOVERY_OUTPUT_DIR.glob("review_reply_discovery__*.json"), reverse=True):
        payload = load_json(path, None)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("artifact_id") or "") != artifact_id:
            continue
        payload["_path"] = str(path)
        candidates.append(payload)
    if not candidates:
        return None
    candidates.sort(key=lambda item: str(item.get("generated_at") or ""), reverse=True)
    return candidates[0]


def packet_is_approved(packet: dict[str, Any] | None, approvals: dict[str, Any]) -> bool:
    if not packet:
        return False
    artifact_id = str(packet.get("artifact_id") or "")
    entry = (approvals.get("approvals") or {}).get(artifact_id)
    if not isinstance(entry, dict):
        return False
    if not entry.get("approved"):
        return False
    return str(entry.get("packet_generated_at") or "") == str(packet.get("generated_at") or "")


def latest_approved_browser_path(approvals: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for artifact_id, entry in (approvals.get("approvals") or {}).items():
        if not isinstance(entry, dict) or not entry.get("approved"):
            continue
        candidates.append(
            {
                **entry,
                "artifact_id": artifact_id,
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            str(item.get("approved_at") or ""),
            str(item.get("packet_generated_at") or ""),
        ),
        reverse=True,
    )
    return candidates[0]


def resolve_execution_approval(packet: dict[str, Any] | None, approvals: dict[str, Any]) -> dict[str, Any]:
    if packet and packet_is_approved(packet, approvals):
        entry = ((approvals.get("approvals") or {}).get(str(packet.get("artifact_id") or "")) or {})
        return {
            "scope": "artifact",
            "artifact_id": packet.get("artifact_id"),
            "approved_at": entry.get("approved_at"),
            "packet_generated_at": packet.get("generated_at"),
            "packet_path": packet.get("_path"),
        }

    latest = latest_approved_browser_path(approvals)
    if latest:
        return {
            "scope": "global",
            "artifact_id": latest.get("artifact_id"),
            "approved_at": latest.get("approved_at"),
            "packet_generated_at": latest.get("packet_generated_at"),
            "packet_path": latest.get("packet_path"),
        }
    raise SystemExit("No approved browser path exists for review-reply execution yet.")


def validate_record_for_queue(
    record: dict[str, Any],
    packet: dict[str, Any] | None,
    approvals: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    decision = record.get("decision") or {}
    if decision.get("flow") != "reviews_reply_positive":
        raise SystemExit("Only Etsy public review replies are in scope for this executor.")
    if str(decision.get("artifact_type") or "") != "review_reply":
        raise SystemExit("Artifact is not a review-reply decision.")
    if str(decision.get("decision") or "") not in {"publish_ready", "needs_revision", "discard"}:
        raise SystemExit("Review decision is missing or invalid.")
    target = decision.get("review_target") or {}
    transaction_id = str(target.get("transaction_id") or "").strip()
    listing_id = str(target.get("listing_id") or "").strip()
    if not transaction_id:
        raise SystemExit("Review target is missing a stable Etsy transaction_id.")
    if not listing_id:
        raise SystemExit("Review target is missing an Etsy listing_id.")
    approved_reply_text = str(decision.get("approved_reply_text") or "").strip()
    if not approved_reply_text:
        raise SystemExit("No approved reply text is stored for this artifact.")
    if decision.get("execution_state") == "posted":
        raise SystemExit("This review reply is already marked as posted.")
    approval = resolve_execution_approval(packet, approvals)
    return decision, approval


def queue_review_reply(
    artifact_id: str,
    *,
    queued_by: str = "browser_review_execution_page",
    execution_mode_override: str | None = None,
    review_status_override: str | None = None,
    operator_resolution_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    quality_state = load_quality_gate_state()
    queue_state = load_queue_state()
    approvals = load_discovery_approvals()
    record = artifact_record(quality_state, artifact_id)
    packet = latest_discovery_packet_for_artifact(artifact_id)
    decision, approval = validate_record_for_queue(record, packet, approvals)

    if execution_mode_override is not None:
        decision["execution_mode"] = execution_mode_override
    if review_status_override is not None:
        decision["review_status"] = review_status_override
    if operator_resolution_override is not None:
        decision["operator_resolution"] = operator_resolution_override

    queue_items = queue_state.setdefault("items", {})
    queue_item = queue_items.get(artifact_id) or {}
    if queue_item.get("status") in {"queued", "running"}:
        return {
            "ok": True,
            "artifact_id": artifact_id,
            "status": queue_item.get("status"),
            "message": "Review reply was already queued.",
            "queue_item": queue_item,
        }

    queued_at = now_iso()
    queue_item = {
        "artifact_id": artifact_id,
        "flow": decision.get("flow"),
        "decision": decision.get("decision"),
        "status": "queued",
        "queued_at": queued_at,
        "queued_by": queued_by,
        "execution_mode": decision.get("execution_mode"),
        "approved_reply_text": decision.get("approved_reply_text"),
        "review_target": decision.get("review_target"),
        "approval_scope": approval.get("scope"),
        "packet_generated_at": approval.get("packet_generated_at"),
        "packet_path": approval.get("packet_path"),
        "attempt_count": len(decision.get("execution_attempts") or []),
        "last_attempt_at": None,
        "last_attempt_outcome": None,
        "last_attempt_id": None,
        "last_preflight_status": None,
    }
    queue_items[artifact_id] = queue_item

    decision["execution_state"] = "queued"
    decision.setdefault("execution_attempts", [])
    record["decision"] = decision
    record["output_paths"] = write_decision(decision)

    save_quality_gate_state(quality_state)
    save_queue_state(queue_state)
    _record_review_execution_transition(
        artifact_id,
        decision,
        state="approved",
        state_reason="queued_for_execution",
        requires_confirmation=False,
        last_side_effect={
            "kind": "queue",
            "queued_at": queued_at,
            "queued_by": queued_by,
            "execution_mode": decision.get("execution_mode"),
        },
        next_action="Run dry-run fill before any live Etsy submit.",
        receipt_kind="queue",
        receipt_payload={
            "queue_status": queue_item.get("status"),
            "approval_scope": queue_item.get("approval_scope"),
            "packet_path": queue_item.get("packet_path"),
        },
    )
    return {
        "ok": True,
        "artifact_id": artifact_id,
        "status": "queued",
        "queued_at": queued_at,
        "message": "Review reply queued for deterministic execution.",
        "queue_item": queue_item,
    }


def auto_enqueue_publish_ready(
    *,
    queued_by: str = "phase2_sidecar_auto_enqueue",
    policy_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = load_execution_policy()
    if isinstance(policy_override, dict):
        policy = {**policy, **policy_override}
    if not policy.get("auto_execution_enabled"):
        return {"ok": True, "status": "disabled", "message": "Review auto-execution is disabled by policy.", "queued": []}
    if not policy.get("auto_queue_publish_ready_positive"):
        return {"ok": True, "status": "disabled", "message": "Auto-queueing publish-ready replies is disabled by policy.", "queued": []}

    approvals = load_discovery_approvals()
    if policy.get("auto_queue_requires_browser_approval", True) and not latest_approved_browser_path(approvals):
        return {
            "ok": True,
            "status": "waiting_for_browser_approval",
            "message": "Browser path approval is still required before publish-ready review replies can auto-queue.",
            "queued": [],
        }

    quality_state = load_quality_gate_state()
    artifacts = (quality_state.get("artifacts") or {}).items()
    queued: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for artifact_id, record in sorted(artifacts, key=lambda item: str(((item[1] or {}).get("decision") or {}).get("created_at") or "")):
        decision = (record or {}).get("decision") or {}
        target = decision.get("review_target") or {}
        if decision.get("flow") != "reviews_reply_positive":
            continue
        if decision.get("decision") != "publish_ready":
            continue
        if decision.get("review_status") != "pending":
            continue
        if str(decision.get("execution_state") or "not_queued") != "not_queued":
            continue
        if not str(target.get("transaction_id") or "").strip() or not str(target.get("listing_id") or "").strip():
            skipped.append(
                {
                    "artifact_id": artifact_id,
                    "reason": "missing_target_identifiers",
                }
            )
            continue
        try:
            result = queue_review_reply(
                artifact_id,
                queued_by=queued_by,
                execution_mode_override="auto",
                review_status_override="approved",
                operator_resolution_override={
                    "action": "auto",
                    "note": "Auto-queued after a publish_ready OpenClaw decision because review auto-execution is enabled.",
                    "recorded_at": now_iso(),
                },
            )
            queued.append(
                {
                    "artifact_id": artifact_id,
                    "transaction_id": target.get("transaction_id"),
                    "listing_id": target.get("listing_id"),
                    "status": result.get("status"),
                }
            )
        except SystemExit as exc:
            skipped.append({"artifact_id": artifact_id, "reason": str(exc)})

    return {
        "ok": True,
        "status": "completed",
        "message": f"Auto-queued {len(queued)} publish-ready public review replies.",
        "queued": queued,
        "skipped": skipped,
    }


def ensure_browser_session(
    session_name: str,
    start_url: str,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = policy or load_execution_policy()
    reused = session_is_open(session_name)
    storage_state = current_storage_state(session_name)
    if not reused:
        run_pw_command(session_name, "open", "about:blank", "--headed")
        time.sleep(2)
        if policy.get("auth_storage_state_enabled") and policy.get("auth_storage_restore_on_open"):
            storage_state = restore_auth_storage_state(
                session_name,
                start_url,
                reason="session_reopen",
                existing_state=storage_state,
            )
        if not storage_state.get("current_url"):
            landed_url, landed_title = navigate_within_session(session_name, start_url, wait_seconds=1.5)
            snapshot_output = run_pw_command(session_name, "snapshot")
            current_url, page_title = parse_page_metadata(snapshot_output)
            storage_state["current_url"] = current_url or landed_url
            storage_state["page_title"] = page_title or landed_title
    else:
        snapshot_output = run_pw_command(session_name, "snapshot")
        current_url, page_title = parse_page_metadata(snapshot_output)
        storage_state["current_url"] = current_url
        storage_state["page_title"] = page_title
    return {
        "session_name": session_name,
        "reused_existing_session": reused,
        "current_url": storage_state.get("current_url"),
        "page_title": storage_state.get("page_title"),
        "storage_state": storage_state,
    }


def inspect_auth_state(session_name: str) -> dict[str, Any]:
    result = run_pw_command(
        session_name,
        "eval",
        (
            "(() => { "
            "const textFor = node => ((node.innerText || node.getAttribute('aria-label') || '').trim()); "
            "const signInVisible = Array.from(document.querySelectorAll('a,button')).some(node => /^sign in$/i.test(textFor(node))); "
            "const sellerControlsVisible = !!document.querySelector('[data-action=\"respond-to-review\"], [data-action=\"submit-response\"], textarea'); "
            "return { signInVisible, sellerControlsVisible }; "
            "})()"
        ),
    )
    parsed = parse_eval_json(result)
    return parsed if isinstance(parsed, dict) else {"signInVisible": False, "sellerControlsVisible": False}


def ensure_authenticated_session(
    session_name: str,
    start_url: str,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = policy or load_execution_policy()
    meta = ensure_browser_session(session_name, start_url, policy=policy)
    current_url = str(meta.get("current_url") or "")
    auth_probe = inspect_auth_state(session_name)
    meta["auth_probe"] = auth_probe
    if is_signed_out_state(current_url, auth_probe):
        storage_state = current_storage_state(session_name, meta.get("storage_state"))
        restored_on_open = (
            storage_state.get("last_restore_reason") == "session_reopen"
            and storage_state.get("last_restore_status") == "restored"
        )
        if (
            policy.get("auth_storage_state_enabled")
            and policy.get("auth_storage_restore_on_auth_failure")
            and not restored_on_open
        ):
            storage_state = restore_auth_storage_state(
                session_name,
                start_url,
                reason="auth_recovery",
                existing_state=storage_state,
            )
            meta["storage_state"] = storage_state
            current_url = str(storage_state.get("current_url") or current_url)
            auth_probe = inspect_auth_state(session_name)
            meta["auth_probe_after_restore"] = auth_probe
            meta["current_url"] = current_url
            meta["page_title"] = storage_state.get("page_title") or meta.get("page_title")

    if is_signed_out_state(current_url, auth_probe):
        raise RuntimeError(
            f"Etsy seller session `{session_name}` is showing a public signed-out view. Sign in again in that browser session before retrying."
        )

    storage_state = current_storage_state(session_name, meta.get("storage_state"))
    if policy.get("auth_storage_state_enabled") and policy.get("auth_storage_save_on_healthy"):
        storage_state = save_auth_storage_state(session_name, storage_state)
    meta["storage_state"] = storage_state
    return meta


def click_reply_button(session_name: str, transaction_id: str) -> dict[str, Any]:
    result = run_pw_command(
        session_name,
        "eval",
        (
            "(() => { "
            f"const tx = {json.dumps(transaction_id)}; "
            "const row = document.querySelector(`li[data-review-region=\"${tx}\"]`); "
            "if (!row) return {ok:false, reason:'review_row_not_found'}; "
            "if (row.querySelector('textarea')) return {ok:true, alreadyOpen:true}; "
            "const button = row.querySelector(`button[data-action=\"respond-to-review\"][data-transaction-id=\"${tx}\"]`) "
            "  || row.querySelector(`[data-transaction-id=\"${tx}\"][data-action=\"respond-to-review\"]`); "
            "if (!button) return {ok:false, reason:'reply_button_not_found'}; "
            "button.click(); "
            "return {ok:true, clicked:true, buttonText:(button.innerText || '').trim()}; "
            "})()"
        ),
    )
    parsed = parse_eval_json(result)
    if isinstance(parsed, dict):
        return parsed
    return {"ok": False, "reason": "unparseable_click_result", "raw": result}


def inspect_reply_row_state(session_name: str, transaction_id: str, expected_reply_text: str) -> dict[str, Any]:
    result = run_pw_command(
        session_name,
        "eval",
        (
            "(() => { "
            f"const tx = {json.dumps(transaction_id)}; "
            f"const expectedReplyText = {json.dumps(expected_reply_text)}; "
            "const row = document.querySelector(`li[data-review-region=\"${tx}\"]`); "
            "if (!row) return { ok: false, reason: 'review_row_not_found' }; "
            "const textarea = row.querySelector('textarea'); "
            "const submit = row.querySelector(`button[data-action=\"submit-response\"][data-transaction-id=\"${tx}\"]`); "
            "const rowText = (row.innerText || '').trim(); "
            "const responseSnippet = expectedReplyText.slice(0, 80); "
            "return { "
            "  ok: true, "
            "  textareaVisible: !!textarea, "
            "  textareaValue: textarea ? textarea.value : null, "
            "  textareaValueLength: textarea ? (textarea.value || '').length : 0, "
            "  valueMatches: textarea ? textarea.value === expectedReplyText : false, "
            "  submitVisible: !!submit, "
            "  submitDisabled: submit ? !!submit.disabled : null, "
            "  rowTextContainsReplySnippet: responseSnippet ? rowText.includes(responseSnippet) : false, "
            "  rowTextExcerpt: rowText.slice(0, 1800) "
            "}; "
            "})()"
        ),
    )
    parsed = parse_eval_json(result)
    if not isinstance(parsed, dict):
        raise RuntimeError("Could not inspect the current Etsy review row state.")
    return parsed


def fill_reply_text_without_submit(session_name: str, transaction_id: str, reply_text: str) -> dict[str, Any]:
    result = run_pw_command(
        session_name,
        "eval",
        (
            "(() => { "
            f"const tx = {json.dumps(transaction_id)}; "
            f"const replyText = {json.dumps(reply_text)}; "
            "const row = document.querySelector(`li[data-review-region=\"${tx}\"]`); "
            "if (!row) return { ok: false, reason: 'review_row_not_found' }; "
            "const textarea = row.querySelector('textarea'); "
            "if (!textarea) return { ok: false, reason: 'textarea_missing' }; "
            "textarea.focus(); "
            "textarea.value = replyText; "
            "textarea.dispatchEvent(new Event('input', { bubbles: true })); "
            "textarea.dispatchEvent(new Event('change', { bubbles: true })); "
            "const submit = row.querySelector(`button[data-action=\"submit-response\"][data-transaction-id=\"${tx}\"]`); "
            "return { "
            "  ok: textarea.value === replyText, "
            "  valueLength: textarea.value.length, "
            "  submitVisible: !!submit, "
            "  submitDisabled: submit ? !!submit.disabled : null, "
            "  submitPerformed: false "
            "}; "
            "})()"
        ),
    )
    parsed = parse_eval_json(result)
    if not isinstance(parsed, dict):
        raise RuntimeError("Dry-run fill did not return a structured result.")
    return parsed


def submit_reply_after_verification(session_name: str, transaction_id: str) -> dict[str, Any]:
    result = run_pw_command(
        session_name,
        "eval",
        (
            "(() => { "
            f"const tx = {json.dumps(transaction_id)}; "
            "const row = document.querySelector(`li[data-review-region=\"${tx}\"]`); "
            "if (!row) return { ok: false, reason: 'review_row_not_found' }; "
            "const submit = row.querySelector(`button[data-action=\"submit-response\"][data-transaction-id=\"${tx}\"]`); "
            "if (!submit) return { ok: false, reason: 'submit_button_missing' }; "
            "if (submit.disabled) return { ok: false, reason: 'submit_button_disabled' }; "
            "submit.click(); "
            "return { ok: true, clicked: true, buttonText: (submit.innerText || '').trim() }; "
            "})()"
        ),
    )
    parsed = parse_eval_json(result)
    if not isinstance(parsed, dict):
        raise RuntimeError("Could not parse Etsy submit-click result.")
    return parsed


def write_attempt_artifact(decision: dict[str, Any], attempt: dict[str, Any]) -> dict[str, str]:
    patterns = load_output_patterns()
    replacements = {
        "run_id": str(decision.get("run_id") or "unknown"),
        "artifact_slug": slugify(str(decision.get("artifact_slug") or decision.get("artifact_id") or "artifact")),
        "attempt_id": slugify(str(attempt.get("attempt_id") or now_iso())),
    }
    json_path = render_pattern(patterns["execution_attempt_json"], replacements)
    md_path = render_pattern(patterns["execution_attempt_md"], replacements)
    payload = {
        "generated_at": now_iso(),
        "artifact_id": decision.get("artifact_id"),
        "decision": decision.get("decision"),
        "execution_state": decision.get("execution_state"),
        "attempt": attempt,
        "review_target": decision.get("review_target"),
        "approved_reply_text": decision.get("approved_reply_text"),
    }
    write_json(json_path, payload)

    lines = [
        "# Review Reply Execution Attempt",
        "",
        f"- Artifact: `{decision.get('artifact_id')}`",
        f"- Run ID: `{decision.get('run_id')}`",
        f"- Attempt ID: `{attempt.get('attempt_id')}`",
        f"- Attempt type: `{attempt.get('attempt_type')}`",
        f"- Outcome: `{attempt.get('outcome')}`",
        f"- Started at: `{attempt.get('started_at')}`",
        f"- Finished at: `{attempt.get('finished_at')}`",
        f"- Session: `{attempt.get('session_name')}`",
        f"- Submit performed: `{attempt.get('submit_performed')}`",
    ]
    failure = attempt.get("failure") if isinstance(attempt.get("failure"), dict) else {}
    if failure:
        lines.extend(
            [
                f"- Failure class: `{failure.get('failure_class') or 'unknown'}`",
                f"- Failure phase: `{failure.get('phase') or 'unknown'}`",
                f"- Retryable: `{failure.get('retryable')}`",
                f"- Breadcrumbs: {failure.get('breadcrumb_summary') or 'n/a'}",
            ]
        )
    lines.extend(
        [
            "",
            "## Review Target",
            "",
            f"- Transaction ID: `{((decision.get('review_target') or {}).get('transaction_id'))}`",
            f"- Listing ID: `{((decision.get('review_target') or {}).get('listing_id'))}`",
            "",
            "## Approved Reply Text",
            "",
            decision.get("approved_reply_text") or "",
            "",
            "## Attempt Detail",
            "",
            "```json",
            json.dumps(attempt, indent=2),
            "```",
        ]
    )
    ensure_parent(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def write_session_artifact(session: dict[str, Any]) -> dict[str, str]:
    session_slug = slugify(str(session.get("session_id") or _new_session_id()))
    json_path = OUTPUT_DIR / "execution" / f"review_reply_execution_session__{session_slug}.json"
    md_path = OUTPUT_DIR / "execution" / f"review_reply_execution_session__{session_slug}.md"

    payload = {
        "generated_at": now_iso(),
        "session": session,
        "counts": _session_counts(session),
    }
    write_json(json_path, payload)

    items = sorted(
        (session.get("items") or {}).values(),
        key=lambda item: str(item.get("updated_at") or ""),
        reverse=True,
    )
    counts = _session_counts(session)
    lines = [
        "# Review Reply Execution Session",
        "",
        f"- Session ID: `{session.get('session_id')}`",
        f"- Status: `{session.get('status')}`",
        f"- Started at: `{session.get('started_at')}`",
        f"- Last activity: `{session.get('last_activity_at')}`",
        f"- Summary sent at: `{session.get('summary_sent_at')}`",
        f"- Posted: `{counts['posted']}`",
        f"- Failed: `{counts['failed']}`",
        f"- Skipped: `{counts['skipped']}`",
        "",
        "## Items",
        "",
    ]
    for item in items:
        lines.extend(
            [
                f"### `{item.get('artifact_id')}`",
                "",
                f"- Status: `{item.get('status')}`",
                f"- Updated at: `{item.get('updated_at')}`",
                f"- Transaction ID: `{item.get('transaction_id')}`",
                f"- Listing ID: `{item.get('listing_id')}`",
                "",
                "Customer review:",
                "",
                item.get("customer_review") or "",
                "",
                "Approved reply:",
                "",
                item.get("approved_reply_text") or "",
                "",
            ]
        )
        if item.get("error"):
            lines.extend(["Error:", "", str(item.get("error")), ""])
        if item.get("failure_class"):
            lines.extend(["Failure class:", "", str(item.get("failure_class")), ""])
        if item.get("breadcrumb_summary"):
            lines.extend(["Breadcrumbs:", "", str(item.get("breadcrumb_summary")), ""])
    ensure_parent(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def record_session_event(
    session_state: dict[str, Any],
    decision: dict[str, Any],
    attempt: dict[str, Any],
    *,
    status: str,
    artifact_paths: dict[str, str] | None = None,
) -> dict[str, Any]:
    session = ensure_open_session(session_state)
    items = session.setdefault("items", {})
    target = decision.get("review_target") or {}
    artifact_id = str(decision.get("artifact_id") or "")
    items[artifact_id] = {
        "artifact_id": artifact_id,
        "status": status,
        "decision": decision.get("decision"),
        "review_status": decision.get("review_status"),
        "updated_at": attempt.get("finished_at") or attempt.get("started_at") or now_iso(),
        "attempt_id": attempt.get("attempt_id"),
        "attempt_type": attempt.get("attempt_type"),
        "transaction_id": target.get("transaction_id"),
        "listing_id": target.get("listing_id"),
        "customer_review": ((decision.get("preview") or {}).get("context_text")) or "",
        "approved_reply_text": decision.get("approved_reply_text") or "",
        "error": attempt.get("error"),
        "attempt_outcome": attempt.get("outcome"),
        "failure_class": ((attempt.get("failure") or {}).get("failure_class") if isinstance(attempt.get("failure"), dict) else None),
        "failure_phase": ((attempt.get("failure") or {}).get("phase") if isinstance(attempt.get("failure"), dict) else None),
        "breadcrumb_summary": ((attempt.get("failure") or {}).get("breadcrumb_summary") if isinstance(attempt.get("failure"), dict) else None),
        "attempt_paths": artifact_paths,
    }
    session["last_activity_at"] = attempt.get("finished_at") or attempt.get("started_at") or now_iso()
    return session


def send_failure_alert(decision: dict[str, Any], attempt: dict[str, Any], *, artifact_paths: dict[str, str] | None = None) -> dict[str, Any]:
    send_email = _load_send_email()
    review_target = decision.get("review_target") or {}
    artifact_id = str(decision.get("artifact_id") or "")
    customer_review = ((decision.get("preview") or {}).get("context_text")) or ""
    approved_reply_text = decision.get("approved_reply_text") or ""
    error_text = str(attempt.get("error") or "Unknown execution failure")
    failure = attempt.get("failure") if isinstance(attempt.get("failure"), dict) else {}
    subject = f"OpenClaw Review Reply Failure: {artifact_id}"
    text = "\n".join(
        [
            f"Artifact: {artifact_id}",
            f"Run ID: {decision.get('run_id') or 'unknown'}",
            f"Transaction ID: {review_target.get('transaction_id') or 'n/a'}",
            f"Listing ID: {review_target.get('listing_id') or 'n/a'}",
            f"Attempt ID: {attempt.get('attempt_id') or 'n/a'}",
            f"Outcome: {attempt.get('outcome') or 'failed'}",
            "",
            "Customer review:",
            str(customer_review),
            "",
            "Approved reply text:",
            str(approved_reply_text),
            "",
            "Error:",
            error_text,
            "",
            "Failure classification:",
            f"- Class: {failure.get('failure_class') or 'n/a'}",
            f"- Phase: {failure.get('phase') or 'n/a'}",
            f"- Retryable: {failure.get('retryable')}",
            f"- Browser guard active: {failure.get('browser_guard_active')}",
            f"- Breadcrumbs: {failure.get('breadcrumb_summary') or 'n/a'}",
            "",
            "Attempt artifacts:",
            f"- JSON: {(artifact_paths or {}).get('json_path') or 'n/a'}",
            f"- Markdown: {(artifact_paths or {}).get('md_path') or 'n/a'}",
        ]
    )
    html = (
        "<div style='font-family:system-ui,Segoe UI,Roboto,Arial;line-height:1.5;'>"
        f"<h2>{artifact_id}</h2>"
        "<p><strong>Status:</strong> review reply execution failed and needs human attention.</p>"
        f"<p><strong>Transaction ID:</strong> {review_target.get('transaction_id') or 'n/a'}<br>"
        f"<strong>Listing ID:</strong> {review_target.get('listing_id') or 'n/a'}<br>"
        f"<strong>Attempt ID:</strong> {attempt.get('attempt_id') or 'n/a'}</p>"
        f"<p><strong>Customer review:</strong><br>{customer_review}</p>"
        f"<p><strong>Approved reply text:</strong><br>{approved_reply_text}</p>"
        f"<p><strong>Error:</strong><br>{error_text}</p>"
        f"<p><strong>Failure class:</strong> {failure.get('failure_class') or 'n/a'}<br>"
        f"<strong>Phase:</strong> {failure.get('phase') or 'n/a'}<br>"
        f"<strong>Retryable:</strong> {failure.get('retryable')}<br>"
        f"<strong>Browser guard active:</strong> {failure.get('browser_guard_active')}<br>"
        f"<strong>Breadcrumbs:</strong> {failure.get('breadcrumb_summary') or 'n/a'}</p>"
        f"<p><strong>Attempt JSON:</strong> {(artifact_paths or {}).get('json_path') or 'n/a'}<br>"
        f"<strong>Attempt Markdown:</strong> {(artifact_paths or {}).get('md_path') or 'n/a'}</p>"
        "</div>"
    )
    send_email(subject, html, text)
    return {"sent_at": now_iso(), "subject": subject}


def send_auth_required_alert(session_name: str, error_text: str, auth_state: dict[str, Any]) -> dict[str, Any]:
    send_email = _load_send_email()
    subject = "OpenClaw Review Reply Executor Needs Etsy Sign-In"
    text = "\n".join(
        [
            "The Etsy review-reply executor is paused because the automation browser is no longer authenticated.",
            "",
            f"Session: {session_name}",
            f"Last auth check: {auth_state.get('last_auth_check_at') or 'n/a'}",
            f"Next retry after: {auth_state.get('next_retry_after') or 'n/a'}",
            f"Current URL: {auth_state.get('last_checked_url') or 'n/a'}",
            "",
            "Error:",
            error_text,
            "",
            "What to do:",
            "- Open the Playwright Etsy seller session used by OpenClaw.",
            "- Sign back in to Etsy in that automation window.",
            "- The hourly drain will resume automatically once the auth check passes again.",
        ]
    )
    html = (
        "<div style='font-family:system-ui,Segoe UI,Roboto,Arial;line-height:1.5;'>"
        "<h2>OpenClaw review-reply executor paused</h2>"
        "<p>The Etsy automation browser is signed out, so queued public review replies will stay queued until the seller session is authenticated again.</p>"
        f"<p><strong>Session:</strong> {session_name}<br>"
        f"<strong>Last auth check:</strong> {auth_state.get('last_auth_check_at') or 'n/a'}<br>"
        f"<strong>Next retry after:</strong> {auth_state.get('next_retry_after') or 'n/a'}<br>"
        f"<strong>Current URL:</strong> {auth_state.get('last_checked_url') or 'n/a'}</p>"
        f"<p><strong>Error:</strong><br>{error_text}</p>"
        "<p><strong>Recovery:</strong><br>"
        "Sign back in within the Playwright Etsy seller session. The scheduled queue drain will retry automatically after the auth block window expires.</p>"
        "</div>"
    )
    send_email(subject, html, text)
    return {"sent_at": now_iso(), "subject": subject}


def mark_auth_healthy(auth_state: dict[str, Any], session_name: str, session_meta: dict[str, Any]) -> dict[str, Any]:
    now = now_iso()
    auth_state["auth_status"] = "healthy"
    auth_state["last_auth_check_at"] = now
    auth_state["last_session_name"] = session_name
    auth_state["last_checked_url"] = session_meta.get("current_url")
    auth_state["last_error"] = None
    auth_state["next_retry_after"] = None
    auth_state["cleared_at"] = now
    return merge_storage_state(auth_state, session_name, session_meta.get("storage_state"))


def mark_auth_blocked(
    auth_state: dict[str, Any],
    *,
    session_name: str,
    error_text: str,
    policy: dict[str, Any],
    current_url: str | None = None,
    storage_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = now_iso()
    if not auth_state.get("blocked_at"):
        auth_state["blocked_at"] = now
    auth_state["auth_status"] = "blocked"
    auth_state["last_auth_check_at"] = now
    auth_state["last_session_name"] = session_name
    auth_state["last_checked_url"] = current_url
    auth_state["last_error"] = error_text
    delay_seconds = max(60, int(policy.get("auth_block_retry_delay_seconds") or 1800))
    auth_state["next_retry_after"] = (
        datetime.now(timezone.utc).astimezone() + timedelta(seconds=delay_seconds)
    ).isoformat()
    return merge_storage_state(auth_state, session_name, storage_state)


def maybe_send_auth_alert(auth_state: dict[str, Any], session_name: str, error_text: str, policy: dict[str, Any]) -> dict[str, Any] | None:
    cooldown_seconds = max(300, int(policy.get("auth_alert_cooldown_seconds") or 21600))
    last_alert_at = parse_iso(str(auth_state.get("last_alert_sent_at") or ""))
    now = datetime.now(timezone.utc).astimezone()
    if last_alert_at is not None and last_alert_at + timedelta(seconds=cooldown_seconds) > now:
        return None
    delivery = send_auth_required_alert(session_name, error_text, auth_state)
    auth_state["last_alert_sent_at"] = delivery.get("sent_at")
    auth_state["last_alert_subject"] = delivery.get("subject")
    return delivery


def send_session_summary_email() -> dict[str, Any]:
    session_state = load_session_state()
    quality_state = load_quality_gate_state()
    queue_state = load_queue_state()
    session = current_open_session(session_state) or backfill_session_from_queue(session_state, quality_state, queue_state)
    if not session:
        return {"ok": False, "status": "no_open_session", "message": "No active review-reply execution session is open."}

    items = sorted(
        (session.get("items") or {}).values(),
        key=lambda item: str(item.get("updated_at") or ""),
        reverse=True,
    )
    if not items:
        return {"ok": False, "status": "empty_session", "message": "The current session has no submitted review replies yet."}

    counts = _session_counts(session)
    subject = (
        "OpenClaw Review Reply Session Summary "
        f"({counts['posted']} posted"
        f"{', ' + str(counts['failed']) + ' failed' if counts['failed'] else ''}"
        f"{', ' + str(counts['skipped']) + ' skipped' if counts['skipped'] else ''})"
    )

    lines = [
        f"Session ID: {session.get('session_id')}",
        f"Started: {session.get('started_at')}",
        f"Last activity: {session.get('last_activity_at')}",
        f"Posted: {counts['posted']}",
        f"Failed: {counts['failed']}",
        f"Skipped: {counts['skipped']}",
        "",
    ]
    html_parts = [
        "<div style='font-family:system-ui,Segoe UI,Roboto,Arial;line-height:1.5;'>",
        f"<h2>{subject}</h2>",
        f"<p><strong>Session ID:</strong> {session.get('session_id')}<br>"
        f"<strong>Started:</strong> {session.get('started_at')}<br>"
        f"<strong>Last activity:</strong> {session.get('last_activity_at')}<br>"
        f"<strong>Posted:</strong> {counts['posted']}<br>"
        f"<strong>Failed:</strong> {counts['failed']}<br>"
        f"<strong>Skipped:</strong> {counts['skipped']}</p>",
    ]
    for item in items:
        lines.extend(
            [
                f"{item.get('artifact_id')} [{item.get('status')}]",
                f"- Transaction ID: {item.get('transaction_id') or 'n/a'}",
                f"- Listing ID: {item.get('listing_id') or 'n/a'}",
                f"- Customer review: {_reply_excerpt(item.get('customer_review'))}",
                f"- Reply: {_reply_excerpt(item.get('approved_reply_text'))}",
            ]
        )
        if item.get("error"):
            lines.append(f"- Error: {item.get('error')}")
        if item.get("failure_class"):
            lines.append(f"- Failure class: {item.get('failure_class')}")
        if item.get("breadcrumb_summary"):
            lines.append(f"- Breadcrumbs: {item.get('breadcrumb_summary')}")
        if (item.get("attempt_paths") or {}).get("json_path"):
            lines.append(f"- Attempt JSON: {item.get('attempt_paths').get('json_path')}")
        lines.append("")

        html_parts.extend(
            [
                f"<h3>{item.get('artifact_id')} <span style='font-weight:normal;'>[{item.get('status')}]</span></h3>",
                f"<p><strong>Transaction ID:</strong> {item.get('transaction_id') or 'n/a'}<br>"
                f"<strong>Listing ID:</strong> {item.get('listing_id') or 'n/a'}<br>"
                f"<strong>Customer review:</strong> {item.get('customer_review') or ''}<br>"
                f"<strong>Reply:</strong> {item.get('approved_reply_text') or ''}</p>",
            ]
        )
        if item.get("error"):
            html_parts.append(f"<p><strong>Error:</strong> {item.get('error')}</p>")
        if item.get("failure_class"):
            html_parts.append(f"<p><strong>Failure class:</strong> {item.get('failure_class')}</p>")
        if item.get("breadcrumb_summary"):
            html_parts.append(f"<p><strong>Breadcrumbs:</strong> {item.get('breadcrumb_summary')}</p>")
    html_parts.append("</div>")

    send_email = _load_send_email()
    text = "\n".join(lines).strip() + "\n"
    html = "\n".join(html_parts)
    send_email(subject, html, text)

    session["summary_sent_at"] = now_iso()
    session["summary_subject"] = subject
    session["status"] = "emailed"
    session["summary_artifact_paths"] = write_session_artifact(session)
    session_state["current_session_id"] = None
    save_session_state(session_state)
    return {
        "ok": True,
        "status": "emailed",
        "message": "Session summary email sent.",
        "session": session,
        "counts": counts,
    }


def record_attempt(
    quality_state: dict[str, Any],
    queue_state: dict[str, Any],
    artifact_id: str,
    attempt: dict[str, Any],
    *,
    final_queue_status: str,
    final_execution_state: str,
    last_preflight_status: str | None = None,
) -> dict[str, Any]:
    record = artifact_record(quality_state, artifact_id)
    decision = record.get("decision") or {}
    attempts = list(decision.get("execution_attempts") or [])
    attempts.append(attempt)
    decision["execution_attempts"] = attempts
    decision["execution_state"] = final_execution_state
    record["decision"] = decision
    record["output_paths"] = write_decision(decision)

    queue_items = queue_state.setdefault("items", {})
    queue_item = queue_items.get(artifact_id) or {}
    queue_item["status"] = final_queue_status
    queue_item["attempt_count"] = len(attempts)
    queue_item["last_attempt_id"] = attempt.get("attempt_id")
    queue_item["last_attempt_at"] = attempt.get("finished_at") or attempt.get("started_at")
    queue_item["last_attempt_outcome"] = attempt.get("outcome")
    failure = attempt.get("failure") if isinstance(attempt.get("failure"), dict) else {}
    if failure:
        queue_item["last_failure_class"] = failure.get("failure_class")
        queue_item["last_failure_phase"] = failure.get("phase")
        queue_item["last_breadcrumb_summary"] = failure.get("breadcrumb_summary")
    else:
        queue_item.pop("last_failure_class", None)
        queue_item.pop("last_failure_phase", None)
        queue_item.pop("last_breadcrumb_summary", None)
    if last_preflight_status is not None:
        queue_item["last_preflight_status"] = last_preflight_status
    if final_queue_status != "queued":
        queue_item.pop("next_attempt_after", None)
        queue_item.pop("retry_reason", None)
    queue_items[artifact_id] = queue_item
    attempt["artifact_paths"] = write_attempt_artifact(decision, attempt)

    save_quality_gate_state(quality_state)
    save_queue_state(queue_state)
    return queue_item


def handle_auth_blocked_attempt(
    *,
    quality_state: dict[str, Any],
    queue_state: dict[str, Any],
    session_state: dict[str, Any],
    artifact_id: str,
    decision: dict[str, Any],
    attempt: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    session_name = str(attempt.get("session_name") or choose_session()[0])
    session_meta = attempt.get("session") if isinstance(attempt.get("session"), dict) else {}
    auth_state = load_auth_state()
    auth_state = mark_auth_blocked(
        auth_state,
        session_name=session_name,
        error_text=str(attempt.get("error") or "Etsy auth is required."),
        policy=policy,
        current_url=str((session_meta or {}).get("current_url") or ""),
        storage_state=(session_meta or {}).get("storage_state") if isinstance(session_meta, dict) else None,
    )
    alert = maybe_send_auth_alert(auth_state, session_name, str(attempt.get("error") or ""), policy)
    save_auth_state(auth_state)

    queue_item = record_attempt(
        quality_state,
        queue_state,
        artifact_id,
        attempt,
        final_queue_status="queued",
        final_execution_state="queued",
        last_preflight_status="waiting_for_auth",
    )
    queue_item["next_attempt_after"] = auth_state.get("next_retry_after")
    queue_item["retry_reason"] = "etsy_auth_required"
    queue_state.setdefault("items", {})[artifact_id] = queue_item
    save_queue_state(queue_state)

    session = record_session_event(
        session_state,
        decision,
        attempt,
        status="queued",
        artifact_paths=attempt.get("artifact_paths"),
    )
    session_item = (session.get("items") or {}).get(artifact_id)
    if isinstance(session_item, dict):
        session_item["auth_blocked_at"] = auth_state.get("blocked_at")
        session_item["next_attempt_after"] = auth_state.get("next_retry_after")
        if alert:
            session_item["auth_alert_sent_at"] = alert.get("sent_at")
            session_item["auth_alert_subject"] = alert.get("subject")
    save_session_state(session_state)
    _record_review_execution_transition(
        artifact_id,
        decision,
        state="blocked",
        state_reason="auth_blocked",
        requires_confirmation=False,
        last_side_effect={
            "kind": "auth_blocked",
            "attempt_id": attempt.get("attempt_id"),
            "next_retry_after": auth_state.get("next_retry_after"),
        },
        last_verification={
            "auth_status": auth_state.get("auth_status"),
            "blocked_at": auth_state.get("blocked_at"),
        },
        next_action="Reauthenticate the Etsy seller session, then retry the queued execution.",
        receipt_kind="auth_blocked",
        receipt_payload={
            "error": attempt.get("error"),
            "auth_status": auth_state.get("auth_status"),
            "next_retry_after": auth_state.get("next_retry_after"),
            "alert_sent": bool(alert),
            "failure_class": (((attempt.get("failure") or {}).get("failure_class")) if isinstance(attempt.get("failure"), dict) else None),
            "failure_phase": (((attempt.get("failure") or {}).get("phase")) if isinstance(attempt.get("failure"), dict) else None),
            "breadcrumb_summary": (((attempt.get("failure") or {}).get("breadcrumb_summary")) if isinstance(attempt.get("failure"), dict) else None),
        },
    )
    return queue_item, auth_state, alert


def prepare_auth_for_drain(policy: dict[str, Any]) -> dict[str, Any]:
    auth_state = load_auth_state()
    session_name, start_url = choose_session()

    if auth_block_active(auth_state):
        return {
            "ok": True,
            "ready": False,
            "status": "waiting_for_auth",
            "message": "The Etsy seller session is still blocked on sign-in, so the review-reply queue is paused.",
            "auth_state": auth_state,
        }

    try:
        session_meta = ensure_authenticated_session(session_name, start_url, policy=policy)
    except Exception as exc:  # noqa: BLE001
        if is_cooldown_error(exc):
            blocked = etsy_browser_blocked_status()
            blocked_until = blocked.get("blocked_until")
            block_reason = blocked.get("block_reason")
            detail = (
                f"Etsy browser automation is cooling down until {blocked_until}."
                if blocked_until
                else "Etsy browser automation is cooling down."
            )
            if block_reason:
                detail += f" Reason: {block_reason}."
            return {
                "ok": True,
                "ready": False,
                "status": "cooldown",
                "message": detail,
                "blocked_until": blocked_until,
                "block_reason": block_reason,
                "auth_state": auth_state,
            }
        if not is_auth_error(exc):
            raise
        auth_state = mark_auth_blocked(
            auth_state,
            session_name=session_name,
            error_text=str(exc),
            policy=policy,
            storage_state=auth_state.get("storage_state"),
        )
        alert = maybe_send_auth_alert(auth_state, session_name, str(exc), policy)
        save_auth_state(auth_state)
        return {
            "ok": True,
            "ready": False,
            "status": "waiting_for_auth",
            "message": str(exc),
            "auth_state": auth_state,
            "alert": alert,
        }

    auth_state = mark_auth_healthy(auth_state, session_name, session_meta)
    save_auth_state(auth_state)
    return {
        "ok": True,
        "ready": True,
        "status": "healthy",
        "message": "Etsy seller session is authenticated and ready for queue drain.",
        "auth_state": auth_state,
        "session": session_meta,
    }


def prepare_review_row_for_execution(
    session_name: str,
    decision: dict[str, Any],
    attempt: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[str, str]:
    session_meta = ensure_authenticated_session(session_name, choose_session()[1], policy=policy)
    attempt["session"] = session_meta

    navigation = navigate_to_reviews_surface(session_name)
    attempt["navigation"] = navigation

    current_url = str(navigation.get("landed_url") or session_meta.get("current_url") or "")
    if "/signin" in current_url.lower():
        raise RuntimeError("Etsy seller session redirected to sign-in before the review surface was reached.")

    review_target = decision.get("review_target") or {}
    customer_review = str(((decision.get("preview") or {}).get("context_text")) or "").strip()
    expected_listing_id = str(review_target.get("listing_id") or "").strip() or None
    expected_transaction_id = str(review_target.get("transaction_id") or "").strip() or None

    def locate_current_page() -> dict[str, Any]:
        return locate_review_block(
            session_name,
            customer_review,
            expected_listing_id=expected_listing_id,
            expected_transaction_id=expected_transaction_id,
        )

    def review_page_candidates(current_reviews_url: str | None) -> list[str]:
        if not current_reviews_url or "/reviews" not in current_reviews_url:
            return []
        policy = load_execution_policy()
        parsed = urlparse(current_reviews_url)
        current_page = int((parse_qs(parsed.query).get("page") or ["1"])[0] or "1")
        max_pages = max(5, int(policy.get("review_page_max_probe") or 5))
        candidates: list[str] = []
        for page in range(1, max_pages + 1):
            query = parse_qs(parsed.query)
            query["page"] = [str(page)]
            if "ref" not in query:
                query["ref"] = ["pagination"]
            url = urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
            if page == current_page:
                continue
            candidates.append(url)
        return candidates

    initial_block = locate_current_page()
    attempt["initial_match"] = initial_block
    if not initial_block.get("found"):
        review_page_probes: list[dict[str, Any]] = []
        for candidate_url in review_page_candidates(str(navigation.get("landed_url") or "")):
            landed_url, landed_title = navigate_within_session(session_name, candidate_url, wait_seconds=1.5)
            page_block = locate_current_page()
            review_page_probes.append(
                {
                    "url": candidate_url,
                    "landed_url": landed_url,
                    "page_title": landed_title,
                    "found": bool(page_block.get("found")),
                    "matched_transaction_id": page_block.get("matchedTransactionId"),
                    "matched_listing_id": page_block.get("matchedListingId"),
                }
            )
            if page_block.get("found"):
                initial_block = page_block
                navigation["strategy"] = "review_page_probe_search"
                navigation["landed_url"] = landed_url or candidate_url
                navigation["page_title"] = landed_title
                break
        if review_page_probes:
            attempt["review_page_probes"] = review_page_probes
        if not initial_block.get("found"):
            refresh_url = str(navigation.get("landed_url") or session_meta.get("current_url") or DEFAULT_ETSY_REVIEWS_URL)
            refresh_landed_url, refresh_title = navigate_within_session(session_name, refresh_url, wait_seconds=2.0)
            refreshed_block = locate_current_page()
            attempt["surface_refresh"] = {
                "url": refresh_url,
                "landed_url": refresh_landed_url or refresh_url,
                "page_title": refresh_title,
                "found": bool(refreshed_block.get("found")),
                "matched_transaction_id": refreshed_block.get("matchedTransactionId"),
                "matched_listing_id": refreshed_block.get("matchedListingId"),
            }
            if refreshed_block.get("found"):
                initial_block = refreshed_block
                navigation["strategy"] = "review_page_surface_refresh"
                navigation["landed_url"] = refresh_landed_url or refresh_url
                navigation["page_title"] = refresh_title
        if not initial_block.get("found"):
            raise RuntimeError("Exact review row could not be found in the signed-in Etsy session.")
    if expected_transaction_id and str(initial_block.get("matchedTransactionId") or "") != expected_transaction_id:
        raise RuntimeError("Matched Etsy review row did not keep the expected transaction_id.")
    if expected_listing_id and str(initial_block.get("matchedListingId") or "") != expected_listing_id:
        raise RuntimeError("Matched Etsy review row did not keep the expected listing_id.")

    if not initial_block.get("replyBoxVisible"):
        click_result = click_reply_button(session_name, expected_transaction_id or "")
        attempt["click_result"] = click_result
        if not click_result.get("ok"):
            raise RuntimeError(f"Could not open the reply box: {click_result.get('reason')}")
        time.sleep(1.0)

    final_block = locate_review_block(
        session_name,
        customer_review,
        expected_listing_id=expected_listing_id,
        expected_transaction_id=expected_transaction_id,
    )
    attempt["post_click_match"] = final_block
    if not final_block.get("replyBoxVisible"):
        raise RuntimeError("Reply textarea did not appear on the matched Etsy review row.")
    return expected_transaction_id or "", expected_listing_id or ""


def is_retryable_row_not_found(error: Exception | str | None) -> bool:
    text = str(error or "")
    return "Exact review row could not be found in the signed-in Etsy session." in text


def maybe_reschedule_retryable_failure(
    *,
    quality_state: dict[str, Any],
    queue_state: dict[str, Any],
    artifact_id: str,
    decision: dict[str, Any],
    attempt: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any] | None:
    if not policy.get("retryable_row_not_found_enabled"):
        return None
    if not is_retryable_row_not_found(attempt.get("error")):
        return None

    max_attempts = max(1, int(policy.get("retryable_row_not_found_max_attempts") or 3))
    retry_delay_seconds = max(60, int(policy.get("retryable_row_not_found_retry_delay_seconds") or 3600))
    existing_attempt_count = int(((queue_state.get("items") or {}).get(artifact_id) or {}).get("attempt_count") or 0)
    this_attempt_count = existing_attempt_count + 1
    if this_attempt_count >= max_attempts:
        return None

    next_attempt_after = (datetime.now(timezone.utc).astimezone() + timedelta(seconds=retry_delay_seconds)).isoformat()
    queue_item = record_attempt(
        quality_state,
        queue_state,
        artifact_id,
        attempt,
        final_queue_status="queued",
        final_execution_state="queued",
        last_preflight_status="waiting_for_review_row",
    )
    queue_item["next_attempt_after"] = next_attempt_after
    queue_item["retry_reason"] = "exact_review_row_not_found"
    queue_state.setdefault("items", {})[artifact_id] = queue_item

    record = artifact_record(quality_state, artifact_id)
    updated_decision = record.get("decision") or {}
    updated_decision["execution_state"] = "queued"
    record["decision"] = updated_decision
    save_quality_gate_state(quality_state)
    save_queue_state(queue_state)
    _record_review_execution_transition(
        artifact_id,
        updated_decision if isinstance(updated_decision, dict) else decision,
        state="blocked",
        state_reason="blocked_by_upstream",
        requires_confirmation=False,
        last_side_effect={
            "kind": "retry_scheduled",
            "attempt_id": attempt.get("attempt_id"),
            "next_attempt_after": next_attempt_after,
        },
        next_action=f"Retry after {next_attempt_after} once Etsy surfaces the review row.",
        receipt_kind="retry_scheduled",
        receipt_payload={
            "attempt_outcome": attempt.get("outcome"),
            "error": attempt.get("error"),
            "retry_reason": "exact_review_row_not_found",
            "next_attempt_after": next_attempt_after,
            "failure_class": (((attempt.get("failure") or {}).get("failure_class")) if isinstance(attempt.get("failure"), dict) else None),
            "failure_phase": (((attempt.get("failure") or {}).get("phase")) if isinstance(attempt.get("failure"), dict) else None),
            "breadcrumb_summary": (((attempt.get("failure") or {}).get("breadcrumb_summary")) if isinstance(attempt.get("failure"), dict) else None),
        },
    )
    return queue_item


def run_dry_run_fill(
    artifact_id: str,
    *,
    keep_browser_open: bool = False,
    notify_on_failure: bool = False,
) -> dict[str, Any]:
    policy = load_execution_policy()
    quality_state = load_quality_gate_state()
    queue_state = load_queue_state()
    session_state = load_session_state()
    approvals = load_discovery_approvals()
    record = artifact_record(quality_state, artifact_id)
    packet = latest_discovery_packet_for_artifact(artifact_id)
    decision, _ = validate_record_for_queue(record, packet, approvals)
    queue_items = queue_state.setdefault("items", {})
    queue_item = queue_items.get(artifact_id)
    if not isinstance(queue_item, dict) or queue_item.get("status") not in {"queued", "failed"}:
        raise SystemExit("Review reply is not queued yet. Queue it before running the dry-run fill.")

    session_name, start_url = choose_session()
    started_at = now_iso()
    attempt: dict[str, Any] = {
        "attempt_id": f"dry-run-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}",
        "attempt_type": "dry_run_fill",
        "started_at": started_at,
        "finished_at": None,
        "session_name": session_name,
        "submit_performed": False,
        "outcome": "running",
        "notes": [],
    }

    queue_item["status"] = "running"
    decision["execution_state"] = "running"
    record["decision"] = decision
    queue_items[artifact_id] = queue_item
    save_quality_gate_state(quality_state)
    save_queue_state(queue_state)

    try:
        auth_meta = ensure_authenticated_session(session_name, start_url, policy=policy)
        auth_state = mark_auth_healthy(load_auth_state(), session_name, auth_meta)
        save_auth_state(auth_state)
        expected_transaction_id, _ = prepare_review_row_for_execution(session_name, decision, attempt, policy)

        fill_result = fill_reply_text_without_submit(
            session_name,
            expected_transaction_id,
            str(decision.get("approved_reply_text") or ""),
        )
        attempt["fill_result"] = fill_result
        if not fill_result.get("ok"):
            raise RuntimeError("Textarea fill verification failed before submit.")

        destination_dir = ROOT / "output" / "execution" / "assets" / slugify(artifact_id)
        destination_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = capture_target_review_screenshot(session_name, destination_dir)
        attempt["screenshot_path"] = screenshot_path
        attempt["finished_at"] = now_iso()
        attempt["outcome"] = "dry_run_filled"
        attempt["notes"].append("Reply text was filled into the exact Etsy review row and submit was not clicked.")
        queue_item = record_attempt(
            quality_state,
            queue_state,
            artifact_id,
            attempt,
            final_queue_status="queued",
            final_execution_state="queued",
            last_preflight_status="dry_run_filled",
        )
        _record_review_execution_transition(
            artifact_id,
            decision,
            state="proposed",
            state_reason="reply_preview_staged",
            requires_confirmation=True,
            last_side_effect={
                "kind": "dry_run_fill",
                "attempt_id": attempt.get("attempt_id"),
                "screenshot_path": attempt.get("screenshot_path"),
            },
            last_verification={
                "fill_result": fill_result,
                "queue_status": queue_item.get("status"),
                "last_preflight_status": queue_item.get("last_preflight_status"),
            },
            next_action="Review the dry-run reply in Etsy, then run live submit only after explicit confirmation.",
            receipt_kind="dry_run_fill",
            receipt_payload={
                "attempt_id": attempt.get("attempt_id"),
                "screenshot_path": attempt.get("screenshot_path"),
                "queue_status": queue_item.get("status"),
            },
        )
        return {
            "ok": True,
            "artifact_id": artifact_id,
            "status": "queued",
            "message": "Dry-run fill completed. The reply text is in Etsy, but submit was not clicked.",
            "queue_item": queue_item,
            "attempt": attempt,
        }
    except Exception as exc:  # noqa: BLE001
        attempt["finished_at"] = now_iso()
        attempt["outcome"] = "failed"
        attempt["error"] = str(exc)
        failure = annotate_attempt_failure(attempt, str(exc))
        if is_auth_error(exc):
            queue_item, auth_state, alert = handle_auth_blocked_attempt(
                quality_state=quality_state,
                queue_state=queue_state,
                session_state=session_state,
                artifact_id=artifact_id,
                decision=decision,
                attempt=attempt,
                policy=policy,
            )
            return {
                "ok": False,
                "artifact_id": artifact_id,
                "status": "queued",
                "message": f"{exc} The executor is paused until the Etsy seller session is authenticated again.",
                "queue_item": queue_item,
                "attempt": attempt,
                "auth_state": auth_state,
                "auth_alert": alert,
            }
        retry_queue_item = maybe_reschedule_retryable_failure(
            quality_state=quality_state,
            queue_state=queue_state,
            artifact_id=artifact_id,
            decision=decision,
            attempt=attempt,
            policy=policy,
        )
        if retry_queue_item is not None:
            return {
                "ok": False,
                "artifact_id": artifact_id,
                "status": "queued",
                "message": f"{exc} Auto-retrying later after Etsy has more time to surface the review.",
                "queue_item": retry_queue_item,
                "attempt": attempt,
            }
        queue_item = record_attempt(
            quality_state,
            queue_state,
            artifact_id,
            attempt,
            final_queue_status="failed",
            final_execution_state="failed",
        )
        _record_review_execution_transition(
            artifact_id,
            decision,
            state="blocked",
            state_reason="execution_failed",
            requires_confirmation=False,
            last_side_effect={
                "kind": "dry_run_fill",
                "attempt_id": attempt.get("attempt_id"),
            },
            last_verification={"queue_status": queue_item.get("status")},
            next_action="Inspect the execution attempt, fix the blocker, then requeue or rerun the dry-run fill.",
            receipt_kind="execution_failed",
            receipt_payload={
                "attempt_id": attempt.get("attempt_id"),
                "error": attempt.get("error"),
                "queue_status": queue_item.get("status"),
                "failure_class": failure.get("failure_class"),
                "failure_phase": failure.get("phase"),
                "breadcrumb_summary": failure.get("breadcrumb_summary"),
            },
        )
        failure_email = None
        if notify_on_failure:
            session = record_session_event(
                session_state,
                decision,
                attempt,
                status="failed",
                artifact_paths=attempt.get("artifact_paths"),
            )
            try:
                failure_email = send_failure_alert(
                    decision,
                    attempt,
                    artifact_paths=attempt.get("artifact_paths"),
                )
                session_item = (session.get("items") or {}).get(artifact_id)
                if isinstance(session_item, dict):
                    session_item["failure_alert_sent_at"] = failure_email.get("sent_at")
                    session_item["failure_alert_subject"] = failure_email.get("subject")
            except Exception as alert_exc:  # noqa: BLE001
                session_item = (session.get("items") or {}).get(artifact_id)
                if isinstance(session_item, dict):
                    session_item["failure_alert_error"] = str(alert_exc)
            save_session_state(session_state)
        return {
            "ok": False,
            "artifact_id": artifact_id,
            "status": "failed",
            "message": str(exc),
            "queue_item": queue_item,
            "attempt": attempt,
            "failure_email": failure_email,
        }
    finally:
        if not keep_browser_open:
            try:
                run_pw_command(session_name, "close")
            except subprocess.CalledProcessError:
                pass


def run_live_submit(artifact_id: str, *, keep_browser_open: bool = False) -> dict[str, Any]:
    policy = load_execution_policy()
    quality_state = load_quality_gate_state()
    queue_state = load_queue_state()
    session_state = load_session_state()
    approvals = load_discovery_approvals()
    record = artifact_record(quality_state, artifact_id)
    packet = latest_discovery_packet_for_artifact(artifact_id)
    decision, _ = validate_record_for_queue(record, packet, approvals)
    queue_items = queue_state.setdefault("items", {})
    queue_item = queue_items.get(artifact_id)
    if not isinstance(queue_item, dict) or queue_item.get("status") not in {"queued", "failed"}:
        raise SystemExit("Review reply must be queued before it can be submitted.")
    if str(queue_item.get("last_preflight_status") or "") != "dry_run_filled":
        raise SystemExit("Run Dry-Run Fill successfully before enabling live submit.")

    session_name, start_url = choose_session()
    started_at = now_iso()
    attempt: dict[str, Any] = {
        "attempt_id": f"submit-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}",
        "attempt_type": "submit",
        "started_at": started_at,
        "finished_at": None,
        "session_name": session_name,
        "submit_performed": False,
        "outcome": "running",
        "notes": [],
    }

    queue_item["status"] = "running"
    decision["execution_state"] = "running"
    record["decision"] = decision
    queue_items[artifact_id] = queue_item
    save_quality_gate_state(quality_state)
    save_queue_state(queue_state)

    try:
        auth_meta = ensure_authenticated_session(session_name, start_url, policy=policy)
        auth_state = mark_auth_healthy(load_auth_state(), session_name, auth_meta)
        save_auth_state(auth_state)
        expected_transaction_id, _ = prepare_review_row_for_execution(session_name, decision, attempt, policy)
        approved_reply_text = str(decision.get("approved_reply_text") or "")

        fill_result = fill_reply_text_without_submit(session_name, expected_transaction_id, approved_reply_text)
        attempt["pre_submit_fill"] = fill_result
        if not fill_result.get("ok"):
            raise RuntimeError("Could not stage the exact approved reply text before submit.")

        row_state = inspect_reply_row_state(session_name, expected_transaction_id, approved_reply_text)
        attempt["pre_submit_state"] = row_state
        if not row_state.get("ok"):
            raise RuntimeError(f"Could not inspect the target review row before submit: {row_state.get('reason')}")
        if not row_state.get("textareaVisible"):
            if row_state.get("rowTextContainsReplySnippet"):
                attempt["finished_at"] = now_iso()
                attempt["outcome"] = "already_replied"
                attempt["notes"].append("The expected reply text already appears on the Etsy review row, so submit was skipped.")
                queue_item = record_attempt(
                    quality_state,
                    queue_state,
                    artifact_id,
                    attempt,
                    final_queue_status="skipped",
                    final_execution_state="skipped",
                    last_preflight_status="already_replied",
                )
                session = record_session_event(
                    session_state,
                    decision,
                    attempt,
                    status="skipped",
                    artifact_paths=attempt.get("artifact_paths"),
                )
                save_session_state(session_state)
                _record_review_execution_transition(
                    artifact_id,
                    decision,
                    state="resolved",
                    state_reason="already_replied",
                    requires_confirmation=False,
                    last_side_effect={
                        "kind": "live_submit_skipped",
                        "attempt_id": attempt.get("attempt_id"),
                    },
                    last_verification=row_state,
                    next_action="No execution needed because Etsy already shows the expected public reply.",
                    receipt_kind="already_replied",
                    receipt_payload={
                        "attempt_id": attempt.get("attempt_id"),
                        "queue_status": queue_item.get("status"),
                    },
                )
                return {
                    "ok": True,
                    "artifact_id": artifact_id,
                    "status": "skipped",
                    "message": "The review already appears to have a public reply, so nothing was submitted.",
                    "queue_item": queue_item,
                    "attempt": attempt,
                    "session": {
                        "session_id": session.get("session_id"),
                        "counts": _session_counts(session),
                    },
                }
            raise RuntimeError("The reply textarea disappeared before submit, so execution is failing closed.")
        if not row_state.get("valueMatches"):
            raise RuntimeError("The textarea no longer matches the exact approved reply text.")
        if not row_state.get("submitVisible"):
            raise RuntimeError("The submit control is not visible on the matched Etsy review row.")
        if row_state.get("submitDisabled"):
            raise RuntimeError("The submit control is disabled; execution is failing closed.")

        _record_review_execution_transition(
            artifact_id,
            decision,
            state="approved",
            state_reason="submit_confirmed",
            requires_confirmation=False,
            last_side_effect={
                "kind": "live_submit_confirmed",
                "attempt_id": attempt.get("attempt_id"),
                "submit_performed": False,
            },
            last_verification=row_state,
            next_action="Submit the verified Etsy reply and confirm the post-submit state before clearing the queue item.",
            receipt_kind="submit_confirmed",
            receipt_payload={
                "attempt_id": attempt.get("attempt_id"),
                "queue_status": queue_item.get("status"),
                "last_preflight_status": queue_item.get("last_preflight_status"),
            },
        )

        click_result = submit_reply_after_verification(session_name, expected_transaction_id)
        attempt["submit_click"] = click_result
        if not click_result.get("ok"):
            raise RuntimeError(f"Could not click the Etsy submit control: {click_result.get('reason')}")
        attempt["submit_performed"] = True
        time.sleep(3.0)

        post_submit_state = inspect_reply_row_state(session_name, expected_transaction_id, approved_reply_text)
        attempt["post_submit_state"] = post_submit_state
        destination_dir = ROOT / "output" / "execution" / "assets" / slugify(artifact_id)
        destination_dir.mkdir(parents=True, exist_ok=True)
        attempt["screenshot_path"] = capture_target_review_screenshot(session_name, destination_dir)

        success = (
            bool(post_submit_state.get("ok"))
            and (
                (not post_submit_state.get("textareaVisible") and not post_submit_state.get("submitVisible"))
                or post_submit_state.get("rowTextContainsReplySnippet")
            )
        )
        if not success:
            raise RuntimeError("Submit was clicked, but Etsy did not show a clear post-submit success state.")

        attempt["finished_at"] = now_iso()
        attempt["outcome"] = "posted"
        attempt["notes"].append("The exact approved reply text was submitted to Etsy after pre-submit verification passed.")
        if decision.get("review_status") == "pending":
            decision["review_status"] = "approved"
        queue_item = record_attempt(
            quality_state,
            queue_state,
            artifact_id,
            attempt,
            final_queue_status="posted",
            final_execution_state="posted",
            last_preflight_status="submitted",
        )
        session = record_session_event(
            session_state,
            decision,
            attempt,
            status="posted",
            artifact_paths=attempt.get("artifact_paths"),
        )
        save_session_state(session_state)
        _record_review_execution_transition(
            artifact_id,
            decision,
            state="verified",
            state_reason="reply_posted",
            requires_confirmation=False,
            last_side_effect={
                "kind": "live_submit",
                "attempt_id": attempt.get("attempt_id"),
                "submit_performed": attempt.get("submit_performed"),
                "screenshot_path": attempt.get("screenshot_path"),
            },
            last_verification=post_submit_state,
            next_action="No further action is needed unless Etsy or the customer indicates a new issue.",
            receipt_kind="live_submit",
            receipt_payload={
                "attempt_id": attempt.get("attempt_id"),
                "queue_status": queue_item.get("status"),
                "session_id": session.get("session_id"),
            },
        )
        return {
            "ok": True,
            "artifact_id": artifact_id,
            "status": "posted",
            "message": "Live submit completed and the reply should now be posted on Etsy.",
            "queue_item": queue_item,
            "attempt": attempt,
            "session": {
                "session_id": session.get("session_id"),
                "counts": _session_counts(session),
            },
        }
    except Exception as exc:  # noqa: BLE001
        attempt["finished_at"] = now_iso()
        attempt["outcome"] = "failed"
        attempt["error"] = str(exc)
        failure = annotate_attempt_failure(attempt, str(exc))
        if is_auth_error(exc):
            queue_item, auth_state, alert = handle_auth_blocked_attempt(
                quality_state=quality_state,
                queue_state=queue_state,
                session_state=session_state,
                artifact_id=artifact_id,
                decision=decision,
                attempt=attempt,
                policy=policy,
            )
            return {
                "ok": False,
                "artifact_id": artifact_id,
                "status": "queued",
                "message": f"{exc} The executor is paused until the Etsy seller session is authenticated again.",
                "queue_item": queue_item,
                "attempt": attempt,
                "session": {
                    "session_id": (current_open_session(session_state) or {}).get("session_id"),
                    "counts": _session_counts(current_open_session(session_state) or {}),
                },
                "auth_state": auth_state,
                "auth_alert": alert,
            }
        retry_queue_item = maybe_reschedule_retryable_failure(
            quality_state=quality_state,
            queue_state=queue_state,
            artifact_id=artifact_id,
            decision=decision,
            attempt=attempt,
            policy=policy,
        )
        if retry_queue_item is not None:
            session = record_session_event(
                session_state,
                decision,
                attempt,
                status="queued",
                artifact_paths=attempt.get("artifact_paths"),
            )
            save_session_state(session_state)
            return {
                "ok": False,
                "artifact_id": artifact_id,
                "status": "queued",
                "message": f"{exc} Auto-retrying later after Etsy has more time to surface the review.",
                "queue_item": retry_queue_item,
                "attempt": attempt,
                "session": {
                    "session_id": session.get("session_id"),
                    "counts": _session_counts(session),
                },
            }
        queue_item = record_attempt(
            quality_state,
            queue_state,
            artifact_id,
            attempt,
            final_queue_status="failed",
            final_execution_state="failed",
        )
        _record_review_execution_transition(
            artifact_id,
            decision,
            state="blocked",
            state_reason="execution_failed",
            requires_confirmation=False,
            last_side_effect={
                "kind": "live_submit",
                "attempt_id": attempt.get("attempt_id"),
                "submit_performed": attempt.get("submit_performed"),
            },
            last_verification=attempt.get("post_submit_state") or attempt.get("pre_submit_state"),
            next_action="Inspect the failed live submit attempt before retrying the Etsy reply.",
            receipt_kind="execution_failed",
            receipt_payload={
                "attempt_id": attempt.get("attempt_id"),
                "error": attempt.get("error"),
                "queue_status": queue_item.get("status"),
                "failure_class": failure.get("failure_class"),
                "failure_phase": failure.get("phase"),
                "breadcrumb_summary": failure.get("breadcrumb_summary"),
            },
        )
        session = record_session_event(
            session_state,
            decision,
            attempt,
            status="failed",
            artifact_paths=attempt.get("artifact_paths"),
        )
        failure_email = None
        try:
            failure_email = send_failure_alert(
                decision,
                attempt,
                artifact_paths=attempt.get("artifact_paths"),
            )
            session_item = (session.get("items") or {}).get(artifact_id)
            if isinstance(session_item, dict):
                session_item["failure_alert_sent_at"] = failure_email.get("sent_at")
                session_item["failure_alert_subject"] = failure_email.get("subject")
        except Exception as alert_exc:  # noqa: BLE001
            session_item = (session.get("items") or {}).get(artifact_id)
            if isinstance(session_item, dict):
                session_item["failure_alert_error"] = str(alert_exc)
        save_session_state(session_state)
        return {
            "ok": False,
            "artifact_id": artifact_id,
            "status": "failed",
            "message": str(exc),
            "queue_item": queue_item,
            "attempt": attempt,
            "session": {
                "session_id": session.get("session_id"),
                "counts": _session_counts(session),
            },
            "failure_email": failure_email,
        }
    finally:
        if not keep_browser_open:
            try:
                run_pw_command(session_name, "close")
            except subprocess.CalledProcessError:
                pass


def drain_queue(
    *,
    max_items: int | None = None,
    keep_browser_open: bool | None = None,
    send_summary: bool | None = None,
    policy_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = load_execution_policy()
    if isinstance(policy_override, dict):
        policy = {**policy, **policy_override}
    if not policy.get("auto_execution_enabled"):
        return {"ok": True, "status": "disabled", "message": "Review auto-execution is disabled by policy.", "results": []}
    if not policy.get("auto_drain_enabled"):
        return {"ok": True, "status": "disabled", "message": "Review auto-drain is disabled by policy.", "results": []}

    queue_state = load_queue_state()
    queue_items = queue_state.get("items") or {}
    eligible = sorted(
        [
            item
            for item in queue_items.values()
            if isinstance(item, dict)
            and str(item.get("status") or "") == "queued"
            and (
                not item.get("next_attempt_after")
                or (
                    parse_iso(str(item.get("next_attempt_after") or ""))
                    and parse_iso(str(item.get("next_attempt_after") or "")) <= datetime.now(timezone.utc).astimezone()
                )
            )
        ],
        key=lambda item: str(item.get("queued_at") or ""),
    )
    if max_items is None:
        try:
            max_items = int(policy.get("auto_drain_max_submits_per_run") or 0) or None
        except Exception:  # noqa: BLE001
            max_items = None
    if max_items is not None:
        eligible = eligible[:max_items]
    if not eligible:
        return {"ok": True, "status": "idle", "message": "No queued review replies are waiting for execution.", "results": []}

    if keep_browser_open is None:
        keep_browser_open = not bool(policy.get("auto_drain_close_browser_after_run", True))
    if send_summary is None:
        send_summary = bool(policy.get("auto_drain_send_session_summary", True))

    auth_readiness = prepare_auth_for_drain(policy)
    if not auth_readiness.get("ready"):
        return {
            "ok": True,
            "status": str(auth_readiness.get("status") or "waiting_for_auth"),
            "message": str(auth_readiness.get("message") or "The Etsy seller session needs attention before queue drain can continue."),
            "results": [],
            "auth_state": auth_readiness.get("auth_state"),
            "alert": auth_readiness.get("alert"),
            "blocked_until": auth_readiness.get("blocked_until"),
            "block_reason": auth_readiness.get("block_reason"),
        }

    results: list[dict[str, Any]] = []
    stop_after_first_failure = bool(policy.get("stop_after_first_failure", True))
    posted_count = 0
    failed_count = 0
    skipped_count = 0

    for item in eligible:
        artifact_id = str(item.get("artifact_id") or "")
        if not artifact_id:
            continue

        if str(item.get("last_preflight_status") or "") != "dry_run_filled":
            dry_run = run_dry_run_fill(
                artifact_id,
                keep_browser_open=True,
                notify_on_failure=True,
            )
            results.append(
                {
                    "artifact_id": artifact_id,
                    "step": "dry_run_fill",
                    "status": dry_run.get("status"),
                    "message": dry_run.get("message"),
                }
            )
            if not dry_run.get("ok"):
                failed_count += 1
                if stop_after_first_failure:
                    break
                continue

        submit = run_live_submit(artifact_id, keep_browser_open=True)
        results.append(
            {
                "artifact_id": artifact_id,
                "step": "submit",
                "status": submit.get("status"),
                "message": submit.get("message"),
            }
        )
        if submit.get("ok"):
            if submit.get("status") == "posted":
                posted_count += 1
            elif submit.get("status") == "skipped":
                skipped_count += 1
        else:
            failed_count += 1
            if stop_after_first_failure:
                break

    summary_result = None
    if posted_count > 0 and send_summary:
        summary_result = send_session_summary_email()

    if not keep_browser_open:
        session_name, _ = choose_session()
        try:
            run_pw_command(session_name, "close")
        except subprocess.CalledProcessError:
            pass

    status = "completed"
    if failed_count and posted_count:
        status = "completed_with_failures"
    elif failed_count and not posted_count:
        status = "failed"
    elif posted_count:
        status = "posted"
    elif skipped_count:
        status = "skipped"

    return {
        "ok": True,
        "status": status,
        "message": (
            f"Drained {posted_count + failed_count + skipped_count} queued review replies: "
            f"{posted_count} posted, {failed_count} failed, {skipped_count} skipped."
        ),
        "posted_count": posted_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "results": results,
        "summary_result": summary_result,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Queue and execute Etsy public review replies.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    queue_parser = subparsers.add_parser("queue", help="Queue an approved review reply for deterministic execution.")
    queue_parser.add_argument("--artifact-id", required=True)
    queue_parser.add_argument("--queued-by", default="browser_review_execution_page")

    dry_run_parser = subparsers.add_parser("dry-run-fill", help="Fill the exact approved reply text without submitting.")
    dry_run_parser.add_argument("--artifact-id", required=True)
    dry_run_parser.add_argument("--keep-browser-open", action="store_true")
    dry_run_parser.add_argument("--close-browser", action="store_true")

    submit_parser = subparsers.add_parser("submit", help="Submit the exact approved reply text after pre-submit verification.")
    submit_parser.add_argument("--artifact-id", required=True)
    submit_parser.add_argument("--keep-browser-open", action="store_true")
    submit_parser.add_argument("--close-browser", action="store_true")

    auto_queue_parser = subparsers.add_parser("auto-queue-publish-ready", help="Auto-queue publish-ready Etsy public review replies when policy allows it.")
    auto_queue_parser.add_argument("--queued-by", default="phase2_sidecar_auto_enqueue")

    drain_parser = subparsers.add_parser("drain-queue", help="Drain queued Etsy public review replies using dry-run fill plus live submit.")
    drain_parser.add_argument("--max-items", type=int)
    drain_parser.add_argument("--keep-browser-open", action="store_true")
    drain_parser.add_argument("--skip-session-summary", action="store_true")

    subparsers.add_parser("send-session-summary", help="Send one email summarizing the current review-reply execution session.")
    cleanup_parser = subparsers.add_parser("cleanup-browsers", help="Close any known review-reply automation browser sessions.")
    cleanup_parser.add_argument("--force-kill-temp-profiles", action="store_true")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    browser_commands = {"dry-run-fill", "submit", "drain-queue"}
    if args.command in browser_commands:
        blocked = etsy_browser_blocked_status()
        if blocked.get("blocked"):
            print(
                json.dumps(
                    {
                        "ok": True,
                        "status": "blocked",
                        "reason": blocked.get("block_reason"),
                        "blocked_until": blocked.get("blocked_until"),
                        "command": args.command,
                    },
                    indent=2,
                )
            )
            return 0

    if args.command == "queue":
        result = queue_review_reply(args.artifact_id, queued_by=args.queued_by)
    elif args.command == "dry-run-fill":
        keep_browser_open = bool(args.keep_browser_open and not args.close_browser)
        result = run_dry_run_fill(args.artifact_id, keep_browser_open=keep_browser_open)
    elif args.command == "submit":
        keep_browser_open = bool(args.keep_browser_open and not args.close_browser)
        result = run_live_submit(args.artifact_id, keep_browser_open=keep_browser_open)
    elif args.command == "auto-queue-publish-ready":
        result = auto_enqueue_publish_ready(queued_by=args.queued_by)
    elif args.command == "drain-queue":
        result = drain_queue(
            max_items=args.max_items,
            keep_browser_open=True if args.keep_browser_open else None,
            send_summary=False if args.skip_session_summary else None,
        )
    elif args.command == "send-session-summary":
        result = send_session_summary_email()
    elif args.command == "cleanup-browsers":
        result = cleanup_review_reply_browsers(force_kill_temp_profiles=args.force_kill_temp_profiles)
    else:
        raise SystemExit(f"Unknown command: {args.command}")

    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
