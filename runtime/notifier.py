#!/usr/bin/env python3
"""
Standalone notifier for passive OpenClaw outputs.

Reads only generated output artifacts and sends or previews:
- daily digest emails
- urgent alert emails
- phase-readiness alerts when present
"""

from __future__ import annotations

import argparse
import hashlib
import json
import smtplib
import subprocess
import sys
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from customer_action_packets import build_customer_action_packets
from customer_interaction_cases import build_customer_interaction_queue
from nightly_action_summary import build_nightly_action_summary, render_nightly_action_summary_markdown
from open_order_intelligence import (
    build_etsy_open_orders_snapshot,
    build_packing_summary,
    build_shopify_open_orders_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "notifier.json"
STATE_PATH = ROOT / "state" / "notifier_state.json"
OUTPUT_DIGESTS = ROOT / "output" / "digests"
DIGEST_SIGNATURE_VERSION = 2
TREND_DIGEST_SIGNATURE_VERSION = 1
QUALITY_GATE_STATE_PATH = ROOT / "state" / "quality_gate_state.json"
OPERATOR_CURRENT_PATH = ROOT / "output" / "operator" / "current_review.json"
WHATSAPP_PUSH_SENTINEL = "OPENCLAW_OPERATOR_PUSH"
CUSTOMER_ACTION_PACKETS_PATH = ROOT / "state" / "customer_action_packets.json"
CUSTOMER_CASES_PATH = ROOT / "state" / "normalized" / "customer_cases.json"
CUSTOM_DESIGN_CASES_PATH = ROOT / "state" / "normalized" / "custom_design_cases.json"
PRINT_QUEUE_CANDIDATES_PATH = ROOT / "state" / "normalized" / "print_queue_candidates.json"
NIGHTLY_ACTION_SUMMARY_STATE_PATH = ROOT / "state" / "nightly_action_summary.json"
NIGHTLY_ACTION_SUMMARY_OPERATOR_JSON_PATH = ROOT / "output" / "operator" / "nightly_action_summary.json"
NIGHTLY_ACTION_SUMMARY_OPERATOR_MD_PATH = ROOT / "output" / "operator" / "nightly_action_summary.md"
SOURCE_OBSERVER_CONFIG_PATH = ROOT / "config" / "source_observer.json"


def refresh_nightly_action_summary_sources() -> None:
    now_local = datetime.now().astimezone()
    customer_cases_payload = load_json(CUSTOMER_CASES_PATH, {"items": []})
    custom_design_payload = load_json(CUSTOM_DESIGN_CASES_PATH, {"items": []})
    print_queue_payload = load_json(PRINT_QUEUE_CANDIDATES_PATH, {"items": []})
    customer_queue = build_customer_interaction_queue(
        customer_cases_payload.get("items") or [],
        custom_design_payload.get("items") or [],
        print_queue_payload.get("items") or [],
    )
    customer_issue_items = [
        item
        for item in customer_queue.get("items") or []
        if item.get("item_type") == "customer_case"
    ]
    packet_items = build_customer_action_packets(customer_issue_items)
    packet_payload = {
        "generated_at": now_local.isoformat(),
        "counts": {
            "packets_total": len(packet_items),
            "reply_packets": sum(1 for item in packet_items if item.get("packet_type") == "reply"),
            "refund_packets": sum(1 for item in packet_items if item.get("packet_type") == "refund"),
            "replacement_packets": sum(1 for item in packet_items if item.get("packet_type") == "replacement"),
            "wait_for_tracking_packets": sum(1 for item in packet_items if item.get("packet_type") == "wait_for_tracking"),
        },
        "items": packet_items,
    }
    CUSTOMER_ACTION_PACKETS_PATH.write_text(json.dumps(packet_payload, indent=2), encoding="utf-8")
    etsy_open_orders = build_etsy_open_orders_snapshot()
    shopify_open_orders = build_shopify_open_orders_snapshot()
    packing_summary = build_packing_summary(etsy_open_orders, shopify_open_orders)
    summary_payload = build_nightly_action_summary(
        packet_payload,
        custom_design_payload.get("items") or [],
        packing_summary,
        now_local=now_local,
    )
    markdown = render_nightly_action_summary_markdown(summary_payload)
    NIGHTLY_ACTION_SUMMARY_STATE_PATH.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    NIGHTLY_ACTION_SUMMARY_OPERATOR_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    NIGHTLY_ACTION_SUMMARY_OPERATOR_JSON_PATH.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    NIGHTLY_ACTION_SUMMARY_OPERATOR_MD_PATH.write_text(markdown + "\n", encoding="utf-8")
    if summary_payload.get("send_window_open"):
        summary_date = str(summary_payload.get("summary_date") or now_local.strftime("%Y-%m-%d"))
        digest_json = OUTPUT_DIGESTS / f"nightly_action_summary__{summary_date}.json"
        digest_md = digest_json.with_suffix(".md")
        digest_json.parent.mkdir(parents=True, exist_ok=True)
        digest_json.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
        digest_md.write_text(markdown + "\n", encoding="utf-8")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def first_present(env: dict[str, str], keys: list[str]) -> str | None:
    for key in keys:
        value = env.get(key)
        if value:
            return value
    return None


def notifier_settings() -> dict[str, Any]:
    config = load_json(CONFIG_PATH, {})
    env = load_env_file(Path(config.get("env_file", "")))
    smtp_cfg = config.get("smtp", {})
    port_value = first_present(env, smtp_cfg.get("port_env_precedence", ["SMTP_PORT"]))
    try:
        port = int(port_value) if port_value else int(smtp_cfg.get("default_port", 587))
    except ValueError:
        port = 587
    return {
        "env_file": config.get("env_file"),
        "host": first_present(env, smtp_cfg.get("host_env_precedence", ["SMTP_HOST"])),
        "port": port,
        "user": first_present(env, smtp_cfg.get("user_env_precedence", ["SMTP_USER"])),
        "password": first_present(env, smtp_cfg.get("password_env_precedence", ["SMTP_PASS"])),
        "to": first_present(env, smtp_cfg.get("to_env_precedence", ["EMAIL_TO", "SMTP_USER"])),
        "use_starttls": bool(smtp_cfg.get("use_starttls", True)),
        "subjects": config.get("subjects", {}),
        "whatsapp": config.get("whatsapp", {}),
    }


def render_subject(template: str, replacements: dict[str, str]) -> str:
    subject = template
    for key, value in replacements.items():
        subject = subject.replace(f"<{key}>", value)
    return subject


def md_for_json(path: Path) -> Path:
    return path.with_suffix(".md")


def canonical_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        return parsed.astimezone()
    except ValueError:
        return None


def iso_week_token(now_local: datetime) -> str:
    iso_year, iso_week, _ = now_local.isocalendar()
    return f"{iso_year}-{iso_week:02d}"


def observer_phase_readiness_window_open(now_local: datetime) -> bool:
    config = load_json(SOURCE_OBSERVER_CONFIG_PATH, {})
    polling = config.get("polling") or {}
    target_day = str(polling.get("phase_readiness_day") or "sunday").strip().lower()
    target_hour = int(polling.get("phase_readiness_hour_local") or 18)
    weekday_map = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    desired_weekday = weekday_map.get(target_day, 6)
    if now_local.weekday() != desired_weekday:
        return False
    return now_local.hour >= target_hour


def summarize_phase_readiness(now_local: datetime) -> dict[str, Any]:
    window_start = now_local - timedelta(days=7)
    decision_rows = [
        row
        for row in load_jsonl(ROOT / "state" / "decision_history.jsonl")
        if (parse_iso_datetime(row.get("evaluated_at")) or now_local) >= window_start
    ]
    override_rows = [
        row
        for row in load_jsonl(ROOT / "state" / "overrides.jsonl")
        if (parse_iso_datetime(row.get("recorded_at")) or now_local) >= window_start
    ]
    quality_gate = load_json(QUALITY_GATE_STATE_PATH, {"artifacts": {}})
    artifacts = (quality_gate.get("artifacts") or {}).values()
    pending_items = []
    stale_high_priority = []
    for record in artifacts:
        decision = record.get("decision") or {}
        if str(decision.get("review_status") or "") != "pending":
            continue
        pending_items.append(decision)
        created_at = parse_iso_datetime(decision.get("created_at"))
        if (
            str(decision.get("priority") or "") in {"high", "urgent"}
            and created_at is not None
            and created_at <= now_local - timedelta(days=2)
        ):
            stale_high_priority.append(decision)

    urgent_alert_count = 0
    for path in OUTPUT_DIGESTS.glob("urgent__*.json"):
        payload = load_json(path, {})
        generated_at = parse_iso_datetime(payload.get("generated_at"))
        if generated_at and generated_at >= window_start:
            urgent_alert_count += 1

    total_decisions = len(decision_rows)
    override_count = len(override_rows)
    publish_ready_count = sum(1 for row in decision_rows if str(row.get("decision") or "") == "publish_ready")
    needs_revision_count = sum(1 for row in decision_rows if str(row.get("decision") or "") == "needs_revision")
    discard_count = sum(1 for row in decision_rows if str(row.get("decision") or "") == "discard")
    override_rate = (override_count / total_decisions) if total_decisions else 0.0

    evidence = [
        f"Collected {total_decisions} quality-gate decisions in the last 7 days.",
        f"Operator overrides in the same window: {override_count} ({override_rate:.0%} of decisions)." if total_decisions else "Operator overrides in the same window: 0.",
        f"Current pending quality-gate backlog: {len(pending_items)}.",
        f"Urgent alerts in the last 7 days: {urgent_alert_count}.",
        f"Decision mix: {publish_ready_count} publish-ready, {needs_revision_count} needs-revision, {discard_count} discard.",
    ]
    blockers: list[str] = []
    if stale_high_priority:
        blockers.append(f"{len(stale_high_priority)} high-priority pending items are older than 2 days.")
    if urgent_alert_count > 0:
        blockers.append("Urgent alerts are still firing in the last 7 days.")

    if blockers:
        readiness_decision = "blocked"
        confidence = 0.78
    elif total_decisions >= 20 and override_rate <= 0.20 and len(pending_items) <= 3:
        readiness_decision = "ready_to_advance"
        confidence = 0.74
    else:
        readiness_decision = "stay_in_current_phase"
        confidence = 0.66 if total_decisions >= 10 else 0.58

    return {
        "current_phase": "phase_2_pilot",
        "readiness_decision": readiness_decision,
        "confidence": round(confidence, 2),
        "evidence": evidence,
        "blockers": blockers,
        "recommended_next_phase": "phase_3" if readiness_decision == "ready_to_advance" else "phase_2_pilot",
        "generated_at": now_local.isoformat(),
        "window": {
            "started_at": window_start.isoformat(),
            "ended_at": now_local.isoformat(),
            "week": iso_week_token(now_local),
        },
        "metrics": {
            "total_decisions": total_decisions,
            "override_count": override_count,
            "override_rate": round(override_rate, 3),
            "pending_items": len(pending_items),
            "urgent_alert_count": urgent_alert_count,
            "publish_ready_count": publish_ready_count,
            "needs_revision_count": needs_revision_count,
            "discard_count": discard_count,
        },
    }


def render_phase_readiness_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# OpenClaw Phase Readiness",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Current phase: `{payload.get('current_phase')}`",
        f"- Readiness decision: `{payload.get('readiness_decision')}`",
        f"- Confidence: `{payload.get('confidence')}`",
        f"- Recommended next phase: `{payload.get('recommended_next_phase')}`",
        "",
        "## Evidence",
        "",
    ]
    for item in payload.get("evidence") or []:
        lines.append(f"- {item}")
    blockers = payload.get("blockers") or []
    lines.extend(["", "## Blockers", ""])
    if blockers:
        for item in blockers:
            lines.append(f"- {item}")
    else:
        lines.append("No active blockers were detected.")
    return "\n".join(lines)


def refresh_phase_readiness_artifact() -> None:
    now_local = datetime.now().astimezone()
    if not observer_phase_readiness_window_open(now_local):
        return
    week = iso_week_token(now_local)
    json_path = OUTPUT_DIGESTS / f"phase_readiness__{week}.json"
    md_path = OUTPUT_DIGESTS / f"phase_readiness__{week}.md"
    if json_path.exists() and md_path.exists():
        return
    payload = summarize_phase_readiness(now_local)
    markdown = render_phase_readiness_markdown(payload)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(markdown + "\n", encoding="utf-8")


def digest_signature(payload: dict[str, Any]) -> str:
    blocked_items = []
    for item in payload.get("items", []):
        if item.get("decision") == "publish_ready":
            continue
        blocked_items.append(
            {
                "artifact_id": item.get("artifact_id"),
                "decision": item.get("decision"),
                "priority": item.get("priority"),
                "review_status": item.get("review_status"),
            }
        )
    blocked_items.sort(key=lambda item: item.get("artifact_id") or "")
    material_view = {
        "active_counts": payload.get("active_counts", {}),
        "blocked_items": blocked_items,
    }
    return canonical_hash(material_view)


def trend_digest_signature(payload: dict[str, Any]) -> str:
    active_items = []
    for item in payload.get("items", []):
        active_items.append(
            {
                "artifact_id": item.get("artifact_id"),
                "decision": item.get("decision"),
                "action_frame": item.get("action_frame"),
                "priority": item.get("priority"),
            }
        )
    active_items.sort(key=lambda item: item.get("artifact_id") or "")
    material_view = {
        "active_counts": payload.get("active_counts", {}),
        "items": active_items[:15],
    }
    return canonical_hash(material_view)


def hydrate_digest_signature(state: dict[str, Any]) -> bool:
    if (
        state.get("last_digest_signature")
        and state.get("last_digest_signature_version") == DIGEST_SIGNATURE_VERSION
    ):
        return False

    sent_items = state.get("sent") or {}
    digest_entries = []
    for key, record in sent_items.items():
        if record.get("kind") != "digest":
            continue
        digest_entries.append((record.get("sent_at") or "", key))
    payload_path: Path | None = None
    sent_at: str | None = None
    if digest_entries:
        sent_at, latest_key = sorted(digest_entries)[-1]
        payload_path = Path(latest_key)
    else:
        today_path = OUTPUT_DIGESTS / f"digest__{datetime.now().strftime('%Y-%m-%d')}.json"
        if today_path.exists():
            payload_path = today_path

    if payload_path is None or not payload_path.exists():
        return False
    payload = load_json(payload_path, {})
    state["last_digest_signature"] = digest_signature(payload)
    state["last_digest_signature_version"] = DIGEST_SIGNATURE_VERSION
    if sent_at:
        state["last_digest_sent_at"] = sent_at
    return True


def hydrate_trend_digest_signature(state: dict[str, Any]) -> bool:
    if (
        state.get("last_trend_digest_signature")
        and state.get("last_trend_digest_signature_version") == TREND_DIGEST_SIGNATURE_VERSION
    ):
        return False

    sent_items = state.get("sent") or {}
    digest_entries = []
    for key, record in sent_items.items():
        if record.get("kind") != "trend_digest":
            continue
        digest_entries.append((record.get("sent_at") or "", key))
    payload_path: Path | None = None
    sent_at: str | None = None
    if digest_entries:
        sent_at, latest_key = sorted(digest_entries)[-1]
        payload_path = Path(latest_key)
    else:
        today_path = OUTPUT_DIGESTS / f"trend_digest__{datetime.now().strftime('%Y-%m-%d')}.json"
        if today_path.exists():
            payload_path = today_path

    if payload_path is None or not payload_path.exists():
        return False
    payload = load_json(payload_path, {})
    state["last_trend_digest_signature"] = trend_digest_signature(payload)
    state["last_trend_digest_signature_version"] = TREND_DIGEST_SIGNATURE_VERSION
    if sent_at:
        state["last_trend_digest_sent_at"] = sent_at
    return True


def should_send_digest(state: dict[str, Any], payload: dict[str, Any]) -> tuple[bool, str, str]:
    signature = digest_signature(payload)
    if not payload.get("items"):
        return False, "no_items", signature

    previous_signature = state.get("last_digest_signature")
    if signature != previous_signature:
        if int(payload.get("new_decision_count", 0)) > 0:
            return True, "new_decision", signature
        return True, "blocked_state_changed", signature

    return False, "no_material_change", signature


def should_send_trend_digest(state: dict[str, Any], payload: dict[str, Any]) -> tuple[bool, str, str]:
    signature = trend_digest_signature(payload)
    if not payload.get("items"):
        return False, "no_items", signature

    previous_signature = state.get("last_trend_digest_signature")
    if signature != previous_signature:
        if int(payload.get("new_decision_count", 0)) > 0:
            return True, "new_trend_decision", signature
        return True, "trend_state_changed", signature

    return False, "no_material_change", signature


def load_sendable_artifacts(state: dict[str, Any]) -> list[dict[str, Any]]:
    today = datetime.now().strftime("%Y-%m-%d")
    artifacts: list[dict[str, Any]] = []

    digest_path = OUTPUT_DIGESTS / f"digest__{today}.json"
    if digest_path.exists():
        payload = load_json(digest_path, {})
        should_send, send_reason, signature = should_send_digest(state, payload)
        if should_send:
            artifacts.append(
                {
                    "kind": "digest",
                    "key": str(digest_path),
                    "json_path": digest_path,
                    "md_path": md_for_json(digest_path),
                    "payload": payload,
                    "send_reason": send_reason,
                    "digest_signature": signature,
                }
            )

    trend_digest_path = OUTPUT_DIGESTS / f"trend_digest__{today}.json"
    if trend_digest_path.exists():
        payload = load_json(trend_digest_path, {})
        should_send, send_reason, signature = should_send_trend_digest(state, payload)
        if should_send:
            artifacts.append(
                {
                    "kind": "trend_digest",
                    "key": str(trend_digest_path),
                    "json_path": trend_digest_path,
                    "md_path": md_for_json(trend_digest_path),
                    "payload": payload,
                    "send_reason": send_reason,
                    "trend_digest_signature": signature,
                }
            )

    nightly_action_summary_path = OUTPUT_DIGESTS / f"nightly_action_summary__{today}.json"
    nightly_key = str(nightly_action_summary_path)
    if nightly_action_summary_path.exists() and not (state.get("sent") or {}).get(nightly_key):
        payload = load_json(nightly_action_summary_path, {})
        if payload.get("send_window_open"):
            artifacts.append(
                {
                    "kind": "nightly_action_summary",
                    "key": nightly_key,
                    "json_path": nightly_action_summary_path,
                    "md_path": md_for_json(nightly_action_summary_path),
                    "payload": payload,
                    "send_reason": "nightly_action_summary",
                }
            )

    for urgent_path in sorted(OUTPUT_DIGESTS.glob("urgent__*.json")):
        key = str(urgent_path)
        sent = (state.get("sent") or {}).get(key)
        if sent:
            continue
        payload = load_json(urgent_path, {})
        artifacts.append(
            {
                "kind": "urgent",
                "key": key,
                "json_path": urgent_path,
                "md_path": md_for_json(urgent_path),
                "payload": payload,
            }
        )

    for readiness_path in sorted(OUTPUT_DIGESTS.glob("phase_readiness__*.json")):
        key = str(readiness_path)
        sent = (state.get("sent") or {}).get(key)
        if sent:
            continue
        payload = load_json(readiness_path, {})
        artifacts.append(
            {
                "kind": "phase_readiness",
                "key": key,
                "json_path": readiness_path,
                "md_path": md_for_json(readiness_path),
                "payload": payload,
            }
        )

    return artifacts


def build_reviews_whatsapp_summary(state: dict[str, Any]) -> dict[str, Any] | None:
    quality_gate = load_json(QUALITY_GATE_STATE_PATH, {"artifacts": {}})
    artifacts = quality_gate.get("artifacts", {})
    review_items: list[dict[str, Any]] = []
    for record in artifacts.values():
        decision = record.get("decision") or {}
        flow = decision.get("flow") or ""
        if not flow.startswith("reviews_"):
            continue
        review_status = decision.get("review_status")
        if review_status not in {None, "pending"}:
            continue
        review_items.append(decision)

    if not review_items:
        return None

    latest_run_id = max(item.get("run_id") or "" for item in review_items)
    latest_items = [item for item in review_items if (item.get("run_id") or "") == latest_run_id]
    if not latest_items:
        return None

    latest_items.sort(key=lambda item: ((item.get("flow") or ""), -(item.get("score") or 0), item.get("artifact_id") or ""))
    signature_payload = [
        {
            "artifact_id": item.get("artifact_id"),
            "decision": item.get("decision"),
            "score": item.get("score"),
            "confidence": item.get("confidence"),
        }
        for item in latest_items
    ]
    signature = canonical_hash(signature_payload)
    if state.get("last_reviews_whatsapp_signature") == signature:
        return None

    story_items = [item for item in latest_items if item.get("flow") == "reviews_story"]
    positive_items = [item for item in latest_items if item.get("flow") == "reviews_reply_positive"]
    private_items = [item for item in latest_items if item.get("flow") == "reviews_reply_private"]

    def count_decision(items: list[dict[str, Any]], decision: str) -> int:
        return sum(1 for item in items if item.get("decision") == decision)

    lines = [
        f"OpenClaw reviews {latest_run_id}",
        "",
    ]
    if story_items:
        top_story = max(story_items, key=lambda item: item.get("score") or 0)
        lines.append(
            f"Best review story: {top_story.get('decision')} (score {top_story.get('score')}, confidence {top_story.get('confidence')})"
        )
        first_reason = (top_story.get("reasoning") or ["No reasoning captured."])[0]
        lines.append(f"Why: {first_reason}")
    else:
        lines.append("Best review story: none ready today")

    lines.extend(
        [
            "",
            f"Public replies ready: {count_decision(positive_items, 'publish_ready')}",
            f"Public replies need changes: {count_decision(positive_items, 'needs_revision')}",
            f"Private recovery replies need changes: {count_decision(private_items, 'needs_revision')}",
            f"Private recovery replies ready: {count_decision(private_items, 'publish_ready')}",
        ]
    )

    interesting = sorted(
        latest_items,
        key=lambda item: ((item.get("decision") != "publish_ready"), -(item.get("score") or 0)),
    )[:3]
    if interesting:
        lines.extend(["", "Top calls:"])
        for item in interesting:
            short_title = item.get("title") or item.get("artifact_id")
            lines.append(
                f"- {short_title}: {item.get('decision')} (score {item.get('score')}, confidence {item.get('confidence')})"
            )

    return {
        "kind": "reviews_whatsapp",
        "run_id": latest_run_id,
        "signature": signature,
        "message": "\n".join(lines),
    }


def build_reviews_whatsapp_operator_push(state: dict[str, Any]) -> dict[str, Any] | None:
    quality_gate = load_json(QUALITY_GATE_STATE_PATH, {"artifacts": {}})
    artifacts = quality_gate.get("artifacts", {})
    review_items: list[dict[str, Any]] = []
    for record in artifacts.values():
        decision = record.get("decision") or {}
        flow = decision.get("flow") or ""
        if not flow.startswith("reviews_"):
            continue
        review_status = decision.get("review_status")
        if review_status not in {None, "pending"}:
            continue
        review_items.append(decision)

    if not review_items:
        return None

    operator_current = load_json(OPERATOR_CURRENT_PATH, {})
    current = operator_current.get("current") or {}
    current_flow = current.get("flow") or ""
    current_message = (operator_current.get("message") or "").strip()

    latest_run_id = max(item.get("run_id") or "" for item in review_items)
    latest_items = [item for item in review_items if (item.get("run_id") or "") == latest_run_id]
    latest_items.sort(
        key=lambda item: (
            item.get("priority") == "urgent",
            item.get("priority") == "high",
            item.get("score") or 0,
            item.get("artifact_id") or "",
        ),
        reverse=True,
    )
    latest_selected = latest_items[0] if latest_items else None

    current_run_id = current.get("run_id") or ""
    if current_flow.startswith("reviews_") and current_run_id >= latest_run_id:
        selected = current
    else:
        selected = latest_selected or current

    artifact_id = selected.get("artifact_id")
    if not artifact_id:
        return None

    signature_payload = {
        "artifact_id": artifact_id,
        "decision": selected.get("decision"),
        "score": selected.get("score"),
        "confidence": selected.get("confidence"),
        "review_status": selected.get("review_status"),
        "preview": selected.get("preview"),
        "message": current_message if current_flow.startswith("reviews_") and current.get("artifact_id") == artifact_id else None,
    }
    signature = canonical_hash(signature_payload)
    if state.get("last_reviews_whatsapp_signature") == signature:
        return None

    if current_flow.startswith("reviews_") and current_message and current.get("artifact_id") == artifact_id:
        lines = [WHATSAPP_PUSH_SENTINEL, current_message]
    else:
        short_id = selected.get("short_id")
        title = selected.get("title") or artifact_id
        decision = selected.get("decision") or "pending"
        reasons = selected.get("reasoning") or ["No reasoning captured."]
        lines = [
            f"{WHATSAPP_PUSH_SENTINEL}",
            f"OpenClaw Review {short_id}" if short_id is not None else "OpenClaw Review",
            f"{title}",
            f"Recommendation: {decision}",
            f"Confidence: {selected.get('confidence')}",
            f"Priority: {selected.get('priority')}",
            "",
            "Why:",
        ]
        for index, reason in enumerate(reasons[:3], start=1):
            lines.append(f"{index}. {reason}")
        lines.extend(
            [
                "",
                "Reply:",
                "agree",
                "publish <short reason>",
                "hold",
                "discard <short reason>",
                "why",
            ]
        )
    return {
        "kind": "reviews_whatsapp",
        "run_id": selected.get("run_id"),
        "signature": signature,
        "message": "\n".join(lines),
    }


def build_message(settings: dict[str, Any], artifact: dict[str, Any]) -> EmailMessage:
    subjects = settings.get("subjects", {})
    kind = artifact["kind"]
    payload = artifact.get("payload", {})
    replacements = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "artifact_id": ((payload.get("decision") or {}).get("artifact_id")) or artifact["json_path"].stem,
        "week": datetime.now().strftime("%Y-%W"),
    }
    subject_template = subjects.get(kind, "[OpenClaw] Notification")
    subject = render_subject(subject_template, replacements)
    body = artifact["md_path"].read_text(encoding="utf-8") if artifact["md_path"].exists() else json.dumps(payload, indent=2)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings["user"]
    msg["To"] = settings["to"]
    msg.set_content(body)
    return msg


def send_message(settings: dict[str, Any], msg: EmailMessage) -> None:
    with smtplib.SMTP(settings["host"], settings["port"]) as server:
        if settings.get("use_starttls"):
            server.starttls()
        server.login(settings["user"], settings["password"])
        server.send_message(msg)


def send_whatsapp_message(settings: dict[str, Any], message: str) -> None:
    whatsapp_cfg = settings.get("whatsapp") or {}
    target = whatsapp_cfg.get("target")
    docker_path = whatsapp_cfg.get("docker_path") or "/usr/local/bin/docker"
    gateway_container = whatsapp_cfg.get("gateway_container") or "openclaw-openclaw-gateway-1"
    if not target:
        raise SystemExit("Notifier WhatsApp target is missing.")
    cmd = [
        docker_path,
        "exec",
        gateway_container,
        "/usr/local/bin/node",
        "/app/dist/index.js",
        "message",
        "send",
        "--channel",
        "whatsapp",
        "--target",
        str(target),
        "--message",
        message,
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send or preview OpenClaw digest and urgent alerts.")
    parser.add_argument("--dry-run", action="store_true", help="Preview notifications without sending mail.")
    args = parser.parse_args()

    settings = notifier_settings()
    state = load_json(STATE_PATH, {"sent": {}})
    state.setdefault("sent", {})
    try:
        refresh_nightly_action_summary_sources()
    except Exception as exc:
        print(f"[notifier] Warning: could not refresh nightly action summary sources: {exc}", file=sys.stderr)
    try:
        refresh_phase_readiness_artifact()
    except Exception as exc:
        print(f"[notifier] Warning: could not refresh phase readiness artifact: {exc}", file=sys.stderr)
    state_changed = hydrate_digest_signature(state)
    state_changed = hydrate_trend_digest_signature(state) or state_changed
    artifacts = load_sendable_artifacts(state)
    whatsapp_summary = build_reviews_whatsapp_operator_push(state)

    if not artifacts and not whatsapp_summary:
        if state_changed and not args.dry_run:
            write_json(STATE_PATH, state)
        return 0

    for artifact in artifacts:
        msg = build_message(settings, artifact)
        if args.dry_run:
            print(f"DRY RUN :: {artifact['kind']} :: {msg['Subject']}")
            if artifact.get("send_reason"):
                print(f"REASON :: {artifact['send_reason']}")
            print(f"TO :: {msg['To']}")
            print(f"SOURCE :: {artifact['json_path']}")
            print("---")
            print(msg.get_content()[:2000])
            print("===")
            continue

        if not all((settings.get("host"), settings.get("user"), settings.get("password"), settings.get("to"))):
            raise SystemExit("Notifier SMTP settings are incomplete.")
        send_message(settings, msg)
        state.setdefault("sent", {})[artifact["key"]] = {
            "kind": artifact["kind"],
            "sent_at": datetime.now().astimezone().isoformat(),
            "subject": str(msg["Subject"]),
        }
        if artifact["kind"] == "digest":
            state["last_digest_signature"] = artifact["digest_signature"]
            state["last_digest_signature_version"] = DIGEST_SIGNATURE_VERSION
            state["last_digest_sent_at"] = state["sent"][artifact["key"]]["sent_at"]
            state["last_digest_reason"] = artifact.get("send_reason")
        if artifact["kind"] == "trend_digest":
            state["last_trend_digest_signature"] = artifact["trend_digest_signature"]
            state["last_trend_digest_signature_version"] = TREND_DIGEST_SIGNATURE_VERSION
            state["last_trend_digest_sent_at"] = state["sent"][artifact["key"]]["sent_at"]
            state["last_trend_digest_reason"] = artifact.get("send_reason")
        state_changed = True

    if whatsapp_summary:
        if args.dry_run:
            print(f"DRY RUN :: reviews_whatsapp :: {whatsapp_summary['run_id']}")
            print("---")
            print(whatsapp_summary["message"])
            print("===")
        else:
            whatsapp_cfg = settings.get("whatsapp") or {}
            if whatsapp_cfg.get("enabled"):
                send_whatsapp_message(settings, whatsapp_summary["message"])
                state["last_reviews_whatsapp_signature"] = whatsapp_summary["signature"]
                state["last_reviews_whatsapp_run_id"] = whatsapp_summary["run_id"]
                state["last_reviews_whatsapp_sent_at"] = datetime.now().astimezone().isoformat()
                state_changed = True

    if not args.dry_run and state_changed:
        write_json(STATE_PATH, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
