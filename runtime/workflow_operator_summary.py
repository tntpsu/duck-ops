#!/usr/bin/env python3
"""
Operator-facing summaries for shared workflow control state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workflow_control import list_workflow_states, load_json


ROOT = Path(__file__).resolve().parents[1]
DUCKOPS_ROOT = ROOT
DUCKAGENT_ROOT = ROOT.parent / "duckAgent"
REVIEW_EXECUTION_SESSIONS_PATH = ROOT / "state" / "review_reply_execution_sessions.json"
QUALITY_GATE_STATE_PATH = ROOT / "state" / "quality_gate_state.json"

EXCLUDED_LANES = {
    "customer_reply_control",
    "review_execution_control",
    "notifier",
    "etsy_token_auth",
}


def _parse_iso(value: str | None) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _reason_text(value: str | None) -> str:
    return str(value or "needs_follow_through").replace("_", " ").strip()


def _format_local_timestamp(value: str | None) -> str | None:
    parsed = _parse_iso(value)
    if parsed == datetime.min.replace(tzinfo=timezone.utc):
        return None
    return parsed.astimezone().strftime("%b %d, %I:%M %p").replace(" 0", " ")


def _trim_text(value: Any, limit: int = 260) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _status_rank(item: dict[str, Any]) -> tuple[int, datetime]:
    state = str(item.get("state") or "").strip().lower()
    requires_confirmation = bool(item.get("requires_confirmation"))
    if state == "blocked":
        priority = 0
    elif requires_confirmation:
        priority = 1
    elif state in {"approved", "proposed"}:
        priority = 2
    else:
        priority = 3
    return priority, _parse_iso(item.get("updated_at"))


def _display_title(item: dict[str, Any]) -> str:
    metadata = item.get("metadata") or {}
    candidates = [
        metadata.get("theme_name"),
        metadata.get("title"),
        metadata.get("blog_title"),
        metadata.get("sale_theme_name"),
        item.get("display_label"),
        item.get("lane"),
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    return "Workflow follow-through"


def _summary_text(item: dict[str, Any]) -> str:
    parts = [_reason_text(item.get("state_reason"))]
    freshness = item.get("input_freshness") or {}
    if isinstance(freshness, dict) and freshness.get("stale_sources"):
        stale = ", ".join(str(source).replace("_", " ") for source in freshness.get("stale_sources") or [])
        parts.append(f"stale: {stale}")
    verification = item.get("last_verification") or {}
    if isinstance(verification, dict):
        if verification.get("publication_lane"):
            parts.append(f"lane: {verification.get('publication_lane')}")
        elif verification.get("article_id"):
            parts.append(f"article {verification.get('article_id')}")
        elif verification.get("applied_count") is not None:
            parts.append(f"applied {verification.get('applied_count')}")
    return " | ".join(parts)


def _load_receipt_payload(item: dict[str, Any]) -> dict[str, Any]:
    receipt = item.get("latest_receipt") or {}
    path_text = str(receipt.get("path") or "").strip()
    if not path_text:
        return {}
    path = Path(path_text)
    if not path.exists():
        return {}
    payload = load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _latest_review_execution_session_item(artifact_id: str) -> dict[str, Any]:
    payload = load_json(REVIEW_EXECUTION_SESSIONS_PATH, {})
    sessions = payload.get("sessions") or {}
    latest_item: dict[str, Any] | None = None
    latest_at = datetime.min.replace(tzinfo=timezone.utc)
    for session in sessions.values():
        if not isinstance(session, dict):
            continue
        item = (session.get("items") or {}).get(artifact_id)
        if not isinstance(item, dict):
            continue
        updated_at = _parse_iso(item.get("updated_at"))
        if updated_at >= latest_at:
            latest_at = updated_at
            latest_item = item
    return latest_item or {}


def _meme_root_cause(receipt_payload: dict[str, Any]) -> str | None:
    publish_result = ((receipt_payload.get("payload") or {}).get("publish_result") or {})
    if not isinstance(publish_result, dict):
        return None
    details = [str(item).strip() for item in (publish_result.get("details") or []) if str(item).strip()]
    if details:
        condensed: list[str] = []
        for detail in details[:2]:
            text = detail.replace(
                "Please read the Graph API documentation at https://developers.facebook.com/docs/graph-api",
                "",
            ).strip()
            condensed.append(_trim_text(text, 180))
        return " | ".join(condensed)
    summary = str(publish_result.get("summary") or "").strip()
    return summary or None


def _weekly_root_cause(item: dict[str, Any]) -> str | None:
    receipt_payload = _load_receipt_payload(item)
    payload = receipt_payload.get("payload") or {}
    refresh_errors = payload.get("refresh_errors")
    if not isinstance(refresh_errors, dict) or not refresh_errors:
        refresh_errors = (item.get("input_freshness") or {}).get("refresh_errors")

    if isinstance(refresh_errors, dict) and refresh_errors:
        label_map = {
            "sale_monitor_snapshot": "sale monitor",
            "campaign_coordination_snapshot": "campaign coordination",
        }
        parts: list[str] = []
        for source, error in refresh_errors.items():
            label = label_map.get(str(source), str(source).replace("_", " "))
            parts.append(f"{label}: {_trim_text(error, 180)}")
        return "Weekly snapshot refresh failed. " + " | ".join(parts[:2])

    freshness = item.get("input_freshness") or {}
    missing_sources = [str(source).replace("_", " ") for source in list(freshness.get("missing_sources") or []) if str(source).strip()]
    if missing_sources:
        return "Required weekly inputs were missing: " + ", ".join(missing_sources[:3]) + "."
    return None


def _review_execution_root_cause(item: dict[str, Any]) -> str | None:
    metadata = item.get("metadata") or {}
    artifact_id = str(metadata.get("artifact_id") or item.get("entity_id") or "").strip()
    if not artifact_id:
        return None
    session_item = _latest_review_execution_session_item(artifact_id)
    error = str(session_item.get("error") or "").strip()
    alert_error = str(session_item.get("failure_alert_error") or "").strip()
    if error and alert_error:
        return f"{error} Secondary alert problem: {alert_error}."
    if error:
        return error
    if alert_error:
        return f"Secondary alert problem: {alert_error}."
    receipt_payload = _load_receipt_payload(item)
    payload = receipt_payload.get("payload") or {}
    execution_state = str(payload.get("execution_state") or "").strip()
    if execution_state:
        return f"Execution ended in `{execution_state}` without a more detailed error payload."
    return None


def _quality_gate_urgent_items(quality_gate_state_path: Path | None = None) -> list[dict[str, Any]]:
    state_path = quality_gate_state_path or QUALITY_GATE_STATE_PATH
    payload = load_json(state_path, {})
    if not isinstance(payload, dict):
        return []
    alerts = payload.get("alerts") or {}
    artifacts = payload.get("artifacts") or {}
    if not isinstance(alerts, dict) or not isinstance(artifacts, dict):
        return []

    items: list[dict[str, Any]] = []
    for alert_key, alert in alerts.items():
        if not isinstance(alert, dict):
            continue
        artifact_id = str(alert_key or "").rsplit("::", 1)[0].strip()
        artifact = artifacts.get(artifact_id) or {}
        decision = artifact.get("decision") or {}
        metadata = decision.get("quality_gate_metadata") or {}
        suggestions = [str(item).strip() for item in (decision.get("improvement_suggestions") or []) if str(item).strip()]
        fail_closed = [str(item).strip() for item in (metadata.get("fail_closed") or []) if str(item).strip()]
        items.append(
            {
                "artifact_id": artifact_id,
                "title": str(decision.get("title") or artifact_id).strip() or artifact_id,
                "decision": str(decision.get("decision") or "review").strip() or "review",
                "priority": str(decision.get("priority") or "medium").strip() or "medium",
                "created_at": str((alert or {}).get("created_at") or "").strip(),
                "why": fail_closed[0] if fail_closed else (suggestions[0] if suggestions else None),
                "review_status": str(decision.get("review_status") or "").strip() or None,
            }
        )
    items.sort(
        key=lambda item: (
            {"urgent": 0, "high": 1, "medium": 2, "low": 3}.get(str(item.get("priority") or "medium").lower(), 9),
            -_parse_iso(item.get("created_at")).timestamp() if _parse_iso(item.get("created_at")) != datetime.min.replace(tzinfo=timezone.utc) else float("-inf"),
        )
    )
    return items


def _fix_hint(item: dict[str, Any], root_cause: str | None) -> str | None:
    lane = str(item.get("lane") or "").strip()
    metadata = item.get("metadata") or {}
    if lane == "meme" and root_cause:
        return "Check the Facebook page/object permissions in Meta, retry after the transient Instagram error clears, then rerun Meme Monday publish."
    if lane == "review_execution" and root_cause:
        artifact_id = str(metadata.get("artifact_id") or item.get("entity_id") or "").strip()
        transaction_id = str(metadata.get("transaction_id") or "").strip()
        if "review row could not be found" in root_cause.lower():
            return (
                f"Open the signed-in Etsy reviews surface, confirm the row for transaction {transaction_id or 'this review'}, "
                f"rerun dry-run for `{artifact_id}`, then submit once the exact row is visible."
            )
    if lane == "quality_gate" and root_cause:
        return "Review the urgent gate items below, then archive or rerun the stale one so the quality gate queue matches the real operator queue."
    return None


def _latest_receipt_text(item: dict[str, Any]) -> str | None:
    receipt = item.get("latest_receipt") or {}
    if not isinstance(receipt, dict):
        return None
    receipt_id = str(receipt.get("receipt_id") or "").strip()
    recorded_at = str(receipt.get("recorded_at") or "").strip()
    if not receipt_id:
        return None
    kind = receipt_id.split("-", 1)[-1].replace("-", " ").strip() or "receipt"
    formatted_at = _format_local_timestamp(recorded_at)
    if formatted_at:
        return f"{kind} at {formatted_at}"
    if recorded_at:
        return f"{kind} at {recorded_at}"
    return kind


def _history_text(item: dict[str, Any], *, limit: int = 3) -> str | None:
    history = list(item.get("history") or [])
    if not history:
        return None
    parts: list[str] = []
    for row in history[:limit]:
        reason = _reason_text(row.get("state_reason") or row.get("state"))
        parts.append(reason)
    if not parts:
        return None
    return " -> ".join(parts)


def _command_text(item: dict[str, Any]) -> str | None:
    lane = str(item.get("lane") or "").strip()
    reason = str(item.get("state_reason") or "").strip()
    metadata = item.get("metadata") or {}

    duckagent = str(DUCKAGENT_ROOT)
    duckops = str(DUCKOPS_ROOT)

    if lane == "blog" and reason in {"stale_input", "draft_ready", "awaiting_review"}:
        return f"cd {duckagent} && python src/main_agent.py --all --flow blog --force"
    if lane == "reviews" and reason in {"stale_input", "report_ready", "awaiting_review"}:
        return f"cd {duckagent} && python src/main_agent.py --all --flow reviews --force"
    if lane == "weekly" and reason in {"stale_input", "draft_ready", "awaiting_review", "awaiting_sale_review"}:
        return f"cd {duckagent} && python src/main_agent.py --all --flow weekly --force"
    if lane == "meme" and reason == "execution_failed":
        return f"cd {duckagent} && python src/main_agent.py --only meme_publish --flow meme --type meme --force"
    if lane == "jeepfact" and reason == "execution_failed":
        return f"cd {duckagent} && python src/main_agent.py --only jeepfact_publish --flow jeepfact --type jeepfact --force"
    if lane == "quality_gate" and reason in {"alerts_pending", "execution_failed"}:
        return f"cd {duckops} && python runtime/quality_gate_pilot.py"
    if lane == "trend_ranker" and reason in {"pending_review", "execution_failed"}:
        return f"cd {duckops} && python runtime/trend_ranker.py"
    if lane == "notifier" and reason in {"alerts_pending", "review_needed"}:
        return f"cd {duckops} && python runtime/notifier.py"
    if lane == "weekly_sale_monitor" and reason in {"stale_input", "weak_items_present", "no_active_sales"}:
        if reason == "stale_input":
            return None
        return f"cd {duckagent} && python src/main_agent.py --only weekly_sale_playbook --flow weekly --force"
    if lane == "weekly_campaign_coordination" and reason in {"stale_input", "coordination_missing", "blocked_by_upstream"}:
        return f"cd {duckagent} && python src/main_agent.py --only weekly_sale_playbook --flow weekly --force"
    if lane == "gtdf" and reason == "blocked_by_upstream":
        return f"cd {duckagent} && python src/main_agent.py --all --flow gtdf --force"
    if lane == "gtdf_winner" and reason == "blocked_by_upstream":
        return f"cd {duckagent} && python src/main_agent.py --all --flow gtdf_winner --force"
    if lane == "review_execution" and reason == "execution_failed":
        artifact_id = str(metadata.get("artifact_id") or item.get("entity_id") or "").strip()
        if artifact_id:
            return f"cd {duckops} && python runtime/review_reply_executor.py dry-run-fill --artifact-id {artifact_id} --keep-browser-open"
    return None


def _root_cause(item: dict[str, Any], *, quality_gate_state_path: Path | None = None) -> str | None:
    lane = str(item.get("lane") or "").strip()
    if lane == "meme":
        return _meme_root_cause(_load_receipt_payload(item))
    if lane == "weekly" and str(item.get("state_reason") or "").strip() == "stale_input":
        return _weekly_root_cause(item)
    if lane == "review_execution":
        return _review_execution_root_cause(item)
    if lane == "quality_gate":
        urgent_items = _quality_gate_urgent_items(quality_gate_state_path)
        if urgent_items:
            top = urgent_items[0]
            count = len(urgent_items)
            noun = "item is" if count == 1 else "items are"
            why = str(top.get("why") or "").strip()
            summary = (
                f"{count} urgent quality gate {noun} still open. "
                f"Top item: {top.get('title')} ({top.get('decision')}, {top.get('priority')} priority)."
            )
            if why:
                summary += f" {why}"
            return summary
    return None


def _operator_next_action(item: dict[str, Any]) -> str:
    lane = str(item.get("lane") or "").strip()
    reason = str(item.get("state_reason") or "").strip()
    next_action = str(item.get("next_action") or "").strip()
    if lane == "weekly_sale_monitor" and reason == "stale_input":
        return (
            "No manual refresh is needed if you are waiting for the next weekly sale or campaign. "
            "That flow refreshes the sale monitor automatically before it builds the playbook."
        )
    return next_action


def _is_info_only_followthrough(item: dict[str, Any], next_action: str) -> bool:
    lane = str(item.get("lane") or "").strip()
    reason = str(item.get("state_reason") or "").strip()
    normalized_action = next_action.strip().lower()
    if lane == "weekly_sale_monitor" and reason == "stale_input":
        return True
    if reason == "blocked_by_upstream":
        return True
    if normalized_action.startswith("wait for the next"):
        return True
    if normalized_action.startswith("no manual refresh is needed"):
        return True
    return False


def build_workflow_followthrough_items(
    *,
    limit: int = 6,
    include_all_blocked: bool = False,
    state_dir=None,
    quality_gate_state_path: Path | None = None,
) -> list[dict[str, Any]]:
    latest_by_lane: dict[str, dict[str, Any]] = {}
    for item in list_workflow_states(state_dir=state_dir):
        lane = str(item.get("lane") or "").strip()
        if not lane or lane in EXCLUDED_LANES or lane.endswith("_control"):
            continue
        next_action = str(item.get("next_action") or "").strip()
        state = str(item.get("state") or "").strip().lower()
        if not next_action:
            continue
        if state in {"verified", "resolved"} and not item.get("requires_confirmation"):
            continue
        previous = latest_by_lane.get(lane)
        if previous is None or _parse_iso(item.get("updated_at")) > _parse_iso(previous.get("updated_at")):
            latest_by_lane[lane] = item

    ranked = sorted(
        latest_by_lane.values(),
        key=lambda item: (
            _status_rank(item)[0],
            -_status_rank(item)[1].timestamp() if _status_rank(item)[1] != datetime.min.replace(tzinfo=timezone.utc) else float("inf"),
        ),
    )
    selected: list[dict[str, Any]]
    if include_all_blocked:
        blocked = [item for item in ranked if str(item.get("state") or "").strip().lower() == "blocked"]
        blocked_ids = {id(item) for item in blocked}
        extras = [item for item in ranked if id(item) not in blocked_ids][:limit]
        selected = blocked + extras
    else:
        selected = ranked[:limit]

    for candidate in ranked:
        if str(candidate.get("lane") or "").strip() != "quality_gate":
            continue
        if not _quality_gate_urgent_items(quality_gate_state_path):
            continue
        if any(existing is candidate for existing in selected):
            break
        selected = [candidate, *selected]
        break

    results: list[dict[str, Any]] = []
    for item in selected:
        lane = str(item.get("lane") or "").strip()
        root_cause = _root_cause(item, quality_gate_state_path=quality_gate_state_path)
        fix_hint = _fix_hint(item, root_cause)
        urgent_items = _quality_gate_urgent_items(quality_gate_state_path) if lane == "quality_gate" else []
        next_action = _operator_next_action(item)
        results.append(
            {
                "lane": item.get("lane"),
                "title": _display_title(item),
                "summary": _summary_text(item),
                "next_action": next_action,
                "command": _command_text(item),
                "latest_receipt": _latest_receipt_text(item),
                "recent_history": _history_text(item),
                "root_cause": root_cause,
                "fix_hint": fix_hint,
                "urgent_items": urgent_items,
                "actionable": not _is_info_only_followthrough(item, next_action),
                "state": item.get("state"),
                "state_reason": item.get("state_reason"),
                "updated_at": item.get("updated_at"),
                "requires_confirmation": bool(item.get("requires_confirmation")),
            }
        )
    return results
