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
import copy
import html
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
from etsy_conversation_browser_sync import build_etsy_conversation_browser_sync
from nightly_action_summary import (
    build_nightly_action_summary,
    render_nightly_action_summary_html,
    render_nightly_action_summary_markdown,
)
from open_order_intelligence import (
    refresh_order_snapshots,
)
from workflow_control import record_workflow_transition

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "notifier.json"
STATE_PATH = ROOT / "state" / "notifier_state.json"
OUTPUT_DIGESTS = ROOT / "output" / "digests"
PROMOTION_READINESS_DIGEST_PATH = OUTPUT_DIGESTS / "promotion_readiness.json"
LEARNING_CHANGE_DIGEST_PATH = OUTPUT_DIGESTS / "learning_change_digest.json"
DIGEST_SIGNATURE_VERSION = 2
TREND_DIGEST_SIGNATURE_VERSION = 1
PROMOTION_READINESS_SIGNATURE_VERSION = 1
LEARNING_CHANGE_SIGNATURE_VERSION = 1
QUALITY_GATE_STATE_PATH = ROOT / "state" / "quality_gate_state.json"
OPERATOR_CURRENT_PATH = ROOT / "output" / "operator" / "current_review.json"
WHATSAPP_PUSH_SENTINEL = "OPENCLAW_OPERATOR_PUSH"
BUSINESS_OPERATOR_DESK_PATH = ROOT / "output" / "operator" / "business_operator_desk.json"
CURRENT_LEARNINGS_PATH = ROOT / "state" / "current_learnings.json"
WEEKLY_STRATEGY_PACKET_PATH = ROOT / "state" / "weekly_strategy_recommendation_packet.json"
CUSTOMER_ACTION_PACKETS_PATH = ROOT / "state" / "customer_action_packets.json"
CUSTOMER_CASES_PATH = ROOT / "state" / "normalized" / "customer_cases.json"
CUSTOM_DESIGN_CASES_PATH = ROOT / "state" / "normalized" / "custom_design_cases.json"
PRINT_QUEUE_CANDIDATES_PATH = ROOT / "state" / "normalized" / "print_queue_candidates.json"
ETSY_BROWSER_CAPTURES_PATH = ROOT / "state" / "etsy_conversation_browser_captures.json"
PACKING_SUMMARY_PATH = ROOT / "state" / "normalized" / "packing_summary.json"
ORDER_SNAPSHOT_REFRESH_STATE_PATH = ROOT / "state" / "order_snapshot_refresh.json"
NIGHTLY_ACTION_SUMMARY_STATE_PATH = ROOT / "state" / "nightly_action_summary.json"
NIGHTLY_ACTION_SUMMARY_OPERATOR_JSON_PATH = ROOT / "output" / "operator" / "nightly_action_summary.json"
NIGHTLY_ACTION_SUMMARY_OPERATOR_MD_PATH = ROOT / "output" / "operator" / "nightly_action_summary.md"
SOURCE_OBSERVER_CONFIG_PATH = ROOT / "config" / "source_observer.json"
WHATSAPP_COLLAGE_DIR = ROOT / "output" / "operator" / "whatsapp_collages"
WHATSAPP_COLLAGE_HELPER = ROOT / "runtime" / "whatsapp_collage_helper.py"
WHATSAPP_COLLAGE_VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
WHATSAPP_CONTAINER_MEDIA_DIR = "/home/node/.openclaw/media/outbound"


def _run_customer_inbox_refresh_preflight(limit: int = 3, timeout_seconds: int = 150) -> dict[str, Any] | None:
    script_path = ROOT / "runtime" / "customer_inbox_refresh.py"
    if not script_path.exists():
        return None
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--limit",
                str(limit),
                "--skip-outside-hours",
                "--start-hour",
                "7",
                "--start-minute",
                "30",
                "--end-hour",
                "23",
                "--end-minute",
                "59",
                "--json",
            ],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0 or not (proc.stdout or "").strip():
        return None
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _default_packing_summary(now_local: datetime) -> dict[str, Any]:
    return {
        "generated_at": now_local.isoformat(),
        "counts": {},
        "orders_to_pack": [],
        "custom_orders_to_make": [],
    }


def _default_order_snapshot_refresh_state(now_local: datetime) -> dict[str, Any]:
    return {
        "generated_at": now_local.isoformat(),
        "state": "missing",
        "state_reason": "order_snapshot_refresh_missing",
        "next_action": "Run the standalone order refresh lane before relying on the packing summary.",
        "sources": {},
        "counts": {},
        "workflow_control": {
            "state": "missing",
            "state_reason": "order_snapshot_refresh_missing",
            "updated_at": now_local.isoformat(),
            "next_action": "Run the standalone order refresh lane before relying on the packing summary.",
        },
    }


def load_cached_order_refresh_artifacts(now_local: datetime | None = None) -> dict[str, Any]:
    now_local = now_local or datetime.now().astimezone()
    packing_summary = load_json(PACKING_SUMMARY_PATH, _default_packing_summary(now_local))
    if not isinstance(packing_summary, dict):
        packing_summary = _default_packing_summary(now_local)
    else:
        packing_summary = copy.deepcopy(packing_summary)

    refresh_state = load_json(ORDER_SNAPSHOT_REFRESH_STATE_PATH, {})
    if not isinstance(refresh_state, dict):
        refresh_state = {}
    else:
        refresh_state = copy.deepcopy(refresh_state)

    snapshot_refresh = packing_summary.get("snapshot_refresh")
    if not refresh_state and isinstance(snapshot_refresh, dict):
        refresh_state = copy.deepcopy(snapshot_refresh)

    if not refresh_state:
        refresh_state = _default_order_snapshot_refresh_state(now_local)

    packing_summary.setdefault(
        "generated_at",
        str(refresh_state.get("packing_summary_generated_at") or refresh_state.get("generated_at") or now_local.isoformat()),
    )
    if not isinstance(packing_summary.get("counts"), dict):
        packing_summary["counts"] = {}
    if not isinstance(packing_summary.get("orders_to_pack"), list):
        packing_summary["orders_to_pack"] = []
    if not isinstance(packing_summary.get("custom_orders_to_make"), list):
        packing_summary["custom_orders_to_make"] = []
    refresh_state.setdefault("packing_summary_generated_at", packing_summary.get("generated_at"))
    packing_summary["snapshot_refresh"] = refresh_state

    return {
        "packing_summary": packing_summary,
        "refresh_state": refresh_state,
    }


def refresh_nightly_action_summary_sources(
    *,
    skip_order_refresh: bool = False,
    skip_customer_refresh_preflight: bool = False,
) -> None:
    now_local = datetime.now().astimezone()
    if skip_customer_refresh_preflight:
        customer_refresh_preflight = {
            "status": "skipped",
            "reason": "disabled_by_notifier_policy",
            "generated_at": now_local.isoformat(),
        }
    else:
        customer_refresh_preflight = _run_customer_inbox_refresh_preflight()
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
    etsy_browser_captures = load_json(ETSY_BROWSER_CAPTURES_PATH, {"generated_at": now_local.isoformat(), "items": []})
    packet_items = build_customer_action_packets(customer_issue_items, browser_captures=etsy_browser_captures)
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
    if skip_order_refresh:
        cached_order_refresh = load_cached_order_refresh_artifacts(now_local=now_local)
        packing_summary = cached_order_refresh.get("packing_summary") or _default_packing_summary(now_local)
        order_refresh_state = cached_order_refresh.get("refresh_state") or _default_order_snapshot_refresh_state(now_local)
    else:
        order_refresh = refresh_order_snapshots()
        packing_summary = order_refresh.get("packing_summary") or _default_packing_summary(now_local)
        order_refresh_state = order_refresh.get("refresh_state") or _default_order_snapshot_refresh_state(now_local)
    etsy_browser_sync = build_etsy_conversation_browser_sync(
        customer_issue_items,
        customer_packets=packet_payload,
        custom_build_candidates={"items": []},
        browser_captures=etsy_browser_captures,
    )
    summary_payload = build_nightly_action_summary(
        packet_payload,
        custom_design_payload.get("items") or [],
        packing_summary,
        etsy_browser_sync=etsy_browser_sync,
        now_local=now_local,
    )
    summary_payload["order_snapshot_refresh"] = order_refresh_state
    if customer_refresh_preflight:
        summary_payload["customer_inbox_refresh_preflight"] = customer_refresh_preflight
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
        "auto_approval": config.get("auto_approval", {}),
        "customer_inbox_refresh_preflight": config.get("customer_inbox_refresh_preflight", {}),
        "whatsapp": config.get("whatsapp", {}),
    }


def render_subject(template: str, replacements: dict[str, str]) -> str:
    subject = template
    for key, value in replacements.items():
        subject = subject.replace(f"<{key}>", value)
    return subject


def _html_text(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value))


def _notifier_stat(label: str, value: Any) -> str:
    return (
        "<div style=\"background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;\">"
        f"<div style=\"font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;\">{_html_text(label)}</div>"
        f"<div style=\"font-size:22px;font-weight:700;color:#111827;margin-top:4px;\">{_html_text(value)}</div>"
        "</div>"
    )


def _notifier_card(title: str, body: str) -> str:
    return (
        "<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:16px 18px;margin:0 0 12px 0;\">"
        f"<div style=\"font-size:16px;font-weight:700;color:#111827;margin:0 0 8px 0;\">{_html_text(title)}</div>"
        f"{body}</div>"
    )


def _notifier_shell(label: str, subject: str, subtitle: str, stats_html: str, body_html: str) -> str:
    stats_block = (
        f"<div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px;\">{stats_html}</div>"
        if stats_html
        else ""
    )
    return (
        "<html><body style=\"margin:0;padding:0;background:#f3f4f6;color:#111827;\">"
        "<div style=\"max-width:860px;margin:0 auto;padding:24px 16px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;line-height:1.5;\">"
        "<div style=\"background:linear-gradient(135deg,#111827,#1f2937);color:#fff;border-radius:18px;padding:24px;margin-bottom:18px;\">"
        f"<div style=\"font-size:13px;letter-spacing:.05em;text-transform:uppercase;color:#d1d5db;\">{_html_text(label)}</div>"
        f"<div style=\"font-size:26px;font-weight:800;margin-top:6px;\">{_html_text(subject)}</div>"
        f"<div style=\"font-size:14px;color:#d1d5db;margin-top:8px;\">{_html_text(subtitle)}</div>"
        "</div>"
        f"{stats_block}"
        f"{body_html}"
        "</div></body></html>"
    )


def _render_decision_card(item: dict[str, Any]) -> str:
    preview = item.get("preview") or {}
    details = [
        f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">{_html_text(item.get('flow') or item.get('artifact_type') or 'artifact')} | {_html_text(item.get('review_status') or 'pending')}</div>",
        (
            "<div style=\"display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px;\">"
            f"<span style=\"background:#eef2ff;color:#3730a3;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">{_html_text(item.get('decision') or 'review')}</span>"
            f"<span style=\"background:#f8fafc;color:#374151;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">priority {_html_text(item.get('priority') or 'medium')}</span>"
            f"<span style=\"background:#f8fafc;color:#374151;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">score {_html_text(item.get('score') or 0)}</span>"
            f"<span style=\"background:#f8fafc;color:#374151;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">confidence {_html_text(item.get('confidence') or 0)}</span>"
            "</div>"
        ),
    ]
    context_text = str(preview.get("context_text") or "").strip()
    proposed_text = str(preview.get("proposed_text") or "").strip()
    if context_text:
        details.append(
            "<div style=\"margin-bottom:8px;\">"
            f"<strong>{_html_text(preview.get('context_label') or 'Context')}:</strong> {_html_text(context_text[:260])}"
            "</div>"
        )
    if proposed_text:
        details.append(
            "<div style=\"margin-bottom:8px;background:#f8fafc;border-radius:10px;padding:10px 12px;\">"
            f"<strong>{_html_text(preview.get('proposed_label') or 'Proposed')}:</strong><br>{_html_text(proposed_text[:280])}"
            "</div>"
        )
    suggestions = item.get("improvement_suggestions") or []
    if suggestions:
        details.append(f"<div><strong>Next improvement:</strong> {_html_text(suggestions[0])}</div>")
    return _notifier_card(str(item.get("title") or item.get("artifact_id") or "Review item"), "".join(details))


def _render_digest_html(subject: str, payload: dict[str, Any]) -> str:
    active = payload.get("active_counts") or {}
    stats = "".join(
        [
            _notifier_stat("Pending review", payload.get("pending_review_count", 0)),
            _notifier_stat("Publish ready", active.get("publish_ready", 0)),
            _notifier_stat("Needs revision", active.get("needs_revision", 0)),
            _notifier_stat("Discard", active.get("discard", 0)),
        ]
    )
    pending_items = list(payload.get("pending_items") or [])
    new_items = list(payload.get("new_items") or [])
    sections: list[str] = []
    if pending_items:
        sections.append("<div style=\"font-size:20px;font-weight:800;margin:8px 0 12px 0;\">Pending review</div>")
        sections.extend(_render_decision_card(item) for item in pending_items[:8])
    if new_items:
        sections.append("<div style=\"font-size:20px;font-weight:800;margin:18px 0 12px 0;\">New decisions</div>")
        sections.extend(_render_decision_card(item) for item in new_items[:6])
    if not sections:
        sections.append(_notifier_card("No review items", "<div style=\"color:#4b5563;\">Nothing new is waiting right now.</div>"))
    subtitle = (
        f"Generated {payload.get('generated_at')} | "
        f"{int(payload.get('pending_review_count', 0))} pending review item(s)"
    )
    return _notifier_shell("OpenClaw Digest", subject, subtitle, stats, "".join(sections))


def _render_trend_card(item: dict[str, Any]) -> str:
    metadata = item.get("trend_metadata") or {}
    matching = metadata.get("matching_products") or []
    matching_text = str((matching[0] or {}).get("title") or "").strip() if matching else ""
    body = [
        f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">{_html_text(item.get('action_frame') or 'watch')} | {_html_text(item.get('review_status') or 'pending')}</div>",
        (
            "<div style=\"display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px;\">"
            f"<span style=\"background:#ecfeff;color:#155e75;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">{_html_text(item.get('decision') or 'watch')}</span>"
            f"<span style=\"background:#f8fafc;color:#374151;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">score {_html_text(item.get('score') or 0)}</span>"
            f"<span style=\"background:#f8fafc;color:#374151;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">confidence {_html_text(item.get('confidence') or 0)}</span>"
            f"<span style=\"background:#f8fafc;color:#374151;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;\">catalog {_html_text(metadata.get('catalog_status') or 'unknown')}</span>"
            "</div>"
        ),
    ]
    reasoning = item.get("reasoning") or []
    if reasoning:
        body.append(f"<div style=\"margin-bottom:8px;\"><strong>Signal:</strong> {_html_text(reasoning[0])}</div>")
    suggestions = item.get("improvement_suggestions") or []
    if suggestions:
        body.append(f"<div style=\"margin-bottom:8px;\"><strong>Recommendation:</strong> {_html_text(suggestions[0])}</div>")
    if matching_text:
        body.append(f"<div><strong>Closest catalog match:</strong> {_html_text(matching_text)}</div>")
    return _notifier_card(str(item.get("title") or item.get("artifact_id") or "Trend"), "".join(body))


def _render_trend_digest_html(subject: str, payload: dict[str, Any]) -> str:
    active = payload.get("active_counts") or {}
    stats = "".join(
        [
            _notifier_stat("Worth acting on", active.get("worth_acting_on", 0)),
            _notifier_stat("Background watch", payload.get("background_watch_count", 0)),
            _notifier_stat("New background watch", payload.get("new_background_watch_count", 0)),
            _notifier_stat("Ignored", active.get("ignore", 0)),
        ]
    )
    action_items = list(payload.get("items") or [])
    background_items = list(payload.get("background_watch_items") or [])
    sections: list[str] = []
    if action_items:
        sections.append("<div style=\"font-size:20px;font-weight:800;margin:8px 0 12px 0;\">Worth acting on</div>")
        sections.extend(_render_trend_card(item) for item in action_items[:8])
    if background_items:
        sections.append("<div style=\"font-size:20px;font-weight:800;margin:18px 0 12px 0;\">Background watch</div>")
        sections.extend(_render_trend_card(item) for item in background_items[:8])
    if not sections:
        sections.append(_notifier_card("No active trend items", "<div style=\"color:#4b5563;\">No trend candidates need action right now.</div>"))
    subtitle = (
        f"Generated {payload.get('generated_at')} | "
        f"{int(payload.get('background_watch_count', 0))} background-watch item(s)"
    )
    return _notifier_shell("OpenClaw Trends", subject, subtitle, stats, "".join(sections))


def _render_urgent_html(subject: str, payload: dict[str, Any]) -> str:
    decision = payload.get("decision") or {}
    stats = "".join(
        [
            _notifier_stat("Priority", decision.get("priority") or "high"),
            _notifier_stat("Score", decision.get("score") or 0),
            _notifier_stat("Confidence", decision.get("confidence") or 0),
            _notifier_stat("Decision", decision.get("decision") or "review"),
        ]
    )
    body_parts = [
        f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">{_html_text(decision.get('flow') or decision.get('artifact_type') or 'artifact')} | run {_html_text(decision.get('run_id') or '')}</div>"
    ]
    reasoning = decision.get("reasoning") or []
    if reasoning:
        body_parts.append(f"<div style=\"margin-bottom:8px;\"><strong>Why this fired:</strong> {_html_text(reasoning[0])}</div>")
    suggestions = decision.get("improvement_suggestions") or []
    if suggestions:
        body_parts.append(f"<div style=\"margin-bottom:8px;\"><strong>What to fix:</strong> {_html_text(suggestions[0])}</div>")
    evidence = decision.get("evidence_refs") or []
    if evidence:
        body_parts.append(f"<div><strong>Evidence refs:</strong> {_html_text(', '.join(str(item) for item in evidence[:3]))}</div>")
    body = _notifier_card(str(decision.get("title") or decision.get("artifact_id") or "Urgent alert"), "".join(body_parts))
    subtitle = f"Generated {payload.get('generated_at')} | immediate operator attention"
    return _notifier_shell("OpenClaw Urgent", subject, subtitle, stats, body)


def _render_phase_readiness_html(subject: str, payload: dict[str, Any]) -> str:
    metrics = payload.get("metrics") or {}
    stats = "".join(
        [
            _notifier_stat("Decision", payload.get("readiness_decision") or "unknown"),
            _notifier_stat("Confidence", payload.get("confidence") or 0),
            _notifier_stat("Pending items", metrics.get("pending_items", 0)),
            _notifier_stat("Urgent alerts", metrics.get("urgent_alert_count", 0)),
        ]
    )
    evidence_html = "".join(
        f"<li style=\"margin-bottom:6px;\">{_html_text(item)}</li>" for item in (payload.get('evidence') or [])
    ) or "<li>No evidence captured.</li>"
    blockers = payload.get("blockers") or []
    blockers_html = "".join(
        f"<li style=\"margin-bottom:6px;\">{_html_text(item)}</li>" for item in blockers
    ) or "<li>No active blockers were detected.</li>"
    body = (
        _notifier_card(
            "Phase recommendation",
            f"<div style=\"margin-bottom:8px;\"><strong>Current phase:</strong> {_html_text(payload.get('current_phase'))}</div>"
            f"<div><strong>Recommended next phase:</strong> {_html_text(payload.get('recommended_next_phase'))}</div>",
        )
        + _notifier_card("Evidence", f"<ul style=\"margin:0;padding-left:18px;color:#111827;\">{evidence_html}</ul>")
        + _notifier_card("Blockers", f"<ul style=\"margin:0;padding-left:18px;color:#111827;\">{blockers_html}</ul>")
    )
    subtitle = f"Generated {payload.get('generated_at')} | weekly readiness review"
    return _notifier_shell("OpenClaw Phase Readiness", subject, subtitle, stats, body)


def _render_promotion_readiness_html(subject: str, payload: dict[str, Any]) -> str:
    items = list(payload.get("items") or [])
    stats = "".join(
        [
            _notifier_stat("Ready now", payload.get("ready_item_count") or len(items)),
            _notifier_stat("Candidates", payload.get("item_count") or len(items)),
            _notifier_stat("Source", payload.get("source") or "business_desk"),
        ]
    )
    sections: list[str] = []
    for item in items[:6]:
        evidence_html = "".join(
            f"<li style=\"margin-bottom:6px;\">{_html_text(entry)}</li>"
            for entry in list(item.get("evidence") or [])[:4]
        ) or "<li>No evidence captured.</li>"
        summary_html = ""
        if item.get("summary"):
            summary_html += f"<div style=\"margin-bottom:8px;\"><strong>Why now:</strong> {_html_text(item.get('summary'))}</div>"
        if item.get("recommended_action"):
            summary_html += f"<div style=\"margin-bottom:8px;\"><strong>Promote with:</strong> {_html_text(item.get('recommended_action'))}</div>"
        if item.get("source_path"):
            summary_html += f"<div><strong>Source:</strong> {_html_text(item.get('source_path'))}</div>"
        sections.append(
            _notifier_card(
                str(item.get("title") or item.get("promotion_id") or "Promotion candidate"),
                f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">"
                f"{_html_text(item.get('promotion_state') or 'ready')} | {_html_text(item.get('progress_label') or '')}"
                "</div>"
                f"{summary_html}"
                f"<ul style=\"margin:0;padding-left:18px;color:#111827;\">{evidence_html}</ul>",
            )
        )
    if not sections:
        sections.append(_notifier_card("No promotion candidates", "<div style=\"color:#4b5563;\">No promotion candidates are ready right now.</div>"))
    subtitle = f"Generated {payload.get('generated_at')} | business desk promotion watch"
    return _notifier_shell("Duck Ops Promotion Ready", subject, subtitle, stats, "".join(sections))


def _render_learning_change_html(subject: str, payload: dict[str, Any]) -> str:
    items = list(payload.get("items") or [])
    stats = "".join(
        [
            _notifier_stat("Material changes", payload.get("material_change_count") or len(items)),
            _notifier_stat("Attention items", payload.get("attention_change_count") or 0),
            _notifier_stat("Source", payload.get("source") or "current_learnings"),
        ]
    )
    sections: list[str] = []
    for item in items[:6]:
        meta_bits = [
            str(item.get("urgency") or "opportunity"),
            str(item.get("source") or "learning"),
            str(item.get("kind") or ""),
        ]
        body = [
            f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">{' | '.join(bit for bit in meta_bits if bit)}</div>",
            f"<div style=\"margin-bottom:8px;\"><strong>Change:</strong> {_html_text(item.get('headline') or '')}</div>",
        ]
        if item.get("detail"):
            body.append(f"<div style=\"margin-bottom:8px;\"><strong>Detail:</strong> {_html_text(item.get('detail'))}</div>")
        sections.append(_notifier_card(str(item.get("headline") or "Learning change"), "".join(body)))
    if not sections:
        sections.append(_notifier_card("No material learning changes", "<div style=\"color:#4b5563;\">No material learning changes are ready to send right now.</div>"))
    subtitle = f"Generated {payload.get('generated_at')} | current learnings + weekly strategy shifts"
    return _notifier_shell("Duck Ops Learning Changes", subject, subtitle, stats, "".join(sections))


def render_notifier_html(kind: str, subject: str, body: str, payload: dict[str, Any]) -> str:
    if kind == "nightly_action_summary":
        return render_nightly_action_summary_html(payload)
    if kind == "digest":
        return _render_digest_html(subject, payload)
    if kind == "trend_digest":
        return _render_trend_digest_html(subject, payload)
    if kind == "urgent":
        return _render_urgent_html(subject, payload)
    if kind == "phase_readiness":
        return _render_phase_readiness_html(subject, payload)
    if kind == "promotion_readiness":
        return _render_promotion_readiness_html(subject, payload)
    if kind == "learning_change_digest":
        return _render_learning_change_html(subject, payload)
    escaped_body = html.escape(body)
    label = {
        "digest": "OpenClaw Digest",
        "trend_digest": "OpenClaw Trends",
        "urgent": "OpenClaw Urgent",
        "phase_readiness": "OpenClaw Phase Readiness",
        "promotion_readiness": "Duck Ops Promotion Ready",
        "learning_change_digest": "Duck Ops Learning Changes",
    }.get(kind, "OpenClaw Notification")
    return (
        "<html><body style=\"margin:0;padding:0;background:#f3f4f6;color:#111827;\">"
        "<div style=\"max-width:860px;margin:0 auto;padding:24px 16px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;line-height:1.5;\">"
        "<div style=\"background:linear-gradient(135deg,#111827,#1f2937);color:#fff;border-radius:18px;padding:24px;margin-bottom:18px;\">"
        f"<div style=\"font-size:13px;letter-spacing:.05em;text-transform:uppercase;color:#d1d5db;\">{html.escape(label)}</div>"
        f"<div style=\"font-size:26px;font-weight:800;margin-top:6px;\">{html.escape(subject)}</div>"
        "</div>"
        "<div style=\"background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:18px;\">"
        f"<pre style=\"margin:0;white-space:pre-wrap;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px;line-height:1.55;color:#111827;\">{escaped_body}</pre>"
        "</div>"
        "</div></body></html>"
    )


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


def unique_media_urls(urls: list[str] | None) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in urls or []:
        candidate = str(value).strip()
        if not candidate or candidate in seen:
            continue
        unique.append(candidate)
        seen.add(candidate)
    return unique


def whatsapp_operator_item_allowed(item: dict[str, Any] | None) -> bool:
    if not isinstance(item, dict) or not item:
        return False
    flow = str(item.get("flow") or "").strip()
    artifact_type = str(item.get("artifact_type") or "").strip()
    return flow.startswith("reviews_") or artifact_type == "trend"


def build_whatsapp_collage(
    media_urls: list[str],
    media_title: str | None = None,
) -> Path | None:
    if len(media_urls) < 2:
        return None
    if not WHATSAPP_COLLAGE_VENV_PYTHON.exists() or not WHATSAPP_COLLAGE_HELPER.exists():
        return None
    signature = canonical_hash({"title": media_title or "", "media_urls": media_urls[:6]})[:16]
    output_path = WHATSAPP_COLLAGE_DIR / f"operator_collage_{signature}.png"
    if output_path.exists():
        return output_path
    cmd = [
        str(WHATSAPP_COLLAGE_VENV_PYTHON),
        str(WHATSAPP_COLLAGE_HELPER),
        "--output",
        str(output_path),
    ]
    if media_title:
        cmd.extend(["--title", media_title])
    for media_url in media_urls[:6]:
        cmd.extend(["--url", media_url])
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except Exception:
        return None
    return output_path if output_path.exists() else None


def prepare_whatsapp_media_urls(
    settings: dict[str, Any],
    media_urls: list[str] | None = None,
    media_title: str | None = None,
) -> list[str]:
    unique_urls = unique_media_urls(media_urls)
    whatsapp_cfg = settings.get("whatsapp") or {}
    collage_cfg = whatsapp_cfg.get("collage") or {}
    collage_enabled = collage_cfg.get("enabled", True)
    send_originals = collage_cfg.get("send_originals", False)
    if collage_enabled:
        collage_path = build_whatsapp_collage(unique_urls, media_title=media_title)
        if collage_path:
            if send_originals:
                return [str(collage_path), *unique_urls]
            return [str(collage_path)]
    return unique_urls


def stage_whatsapp_media_for_container(settings: dict[str, Any], media_url: str) -> str:
    trimmed = str(media_url).strip()
    if not trimmed or trimmed.startswith(("http://", "https://")):
        return trimmed
    docker_path = (settings.get("whatsapp") or {}).get("docker_path") or "/usr/local/bin/docker"
    gateway_container = (settings.get("whatsapp") or {}).get("gateway_container") or "openclaw-openclaw-gateway-1"
    source_path = Path(trimmed[7:]) if trimmed.startswith("file://") else Path(trimmed)
    if not source_path.exists():
        return trimmed
    container_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{source_path.name}"
    container_path = f"{WHATSAPP_CONTAINER_MEDIA_DIR}/{container_name}"
    subprocess.run(
        [docker_path, "exec", gateway_container, "mkdir", "-p", WHATSAPP_CONTAINER_MEDIA_DIR],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [docker_path, "cp", str(source_path), f"{gateway_container}:{container_path}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return container_path


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


def promotion_readiness_signature(payload: dict[str, Any]) -> str:
    items = []
    for item in payload.get("items", []):
        items.append(
            {
                "promotion_id": item.get("promotion_id"),
                "promotion_state": item.get("promotion_state"),
                "progress_label": item.get("progress_label"),
                "summary": item.get("summary"),
                "recommended_action": item.get("recommended_action"),
            }
        )
    items.sort(key=lambda item: item.get("promotion_id") or "")
    return canonical_hash({"ready_item_count": payload.get("ready_item_count", 0), "items": items})


def learning_change_signature(payload: dict[str, Any]) -> str:
    items = []
    for item in payload.get("items", []):
        items.append(
            {
                "source": item.get("source"),
                "kind": item.get("kind"),
                "urgency": item.get("urgency"),
                "headline": item.get("headline"),
                "detail": item.get("detail"),
            }
        )
    items.sort(key=lambda item: ((item.get("kind") or ""), (item.get("headline") or "")))
    return canonical_hash(
        {
            "material_change_count": payload.get("material_change_count", 0),
            "attention_change_count": payload.get("attention_change_count", 0),
            "items": items,
        }
    )


def render_promotion_readiness_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Duck Ops Promotion Ready",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Source: `{payload.get('source') or 'business_desk'}`",
        f"- Promotion candidates: `{payload.get('item_count', 0)}`",
        f"- Ready now: `{payload.get('ready_item_count', 0)}`",
    ]
    if payload.get("headline"):
        lines.append(f"- Status: `{payload.get('headline')}`")
    if payload.get("recommended_action"):
        lines.append(f"- Recommended action: `{payload.get('recommended_action')}`")
    lines.extend(["", "## Ready Candidates", ""])
    items = list(payload.get("items") or [])
    if not items:
        lines.append("No promotion candidates are ready right now.")
        return "\n".join(lines)
    for item in items:
        lines.append(
            f"- {item.get('title') or item.get('promotion_id') or 'Promotion candidate'} | `{item.get('promotion_state') or 'ready'}` | `{item.get('progress_label') or ''}`"
        )
        if item.get("summary"):
            lines.append(f"  Why: {item.get('summary')}")
        if item.get("recommended_action"):
            lines.append(f"  Next: {item.get('recommended_action')}")
        for entry in list(item.get("evidence") or [])[:4]:
            lines.append(f"  Evidence: {entry}")
    return "\n".join(lines)


def render_learning_change_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Duck Ops Learning Changes",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Source: `{payload.get('source') or 'current_learnings'}`",
        f"- Material changes: `{payload.get('material_change_count', 0)}`",
        f"- Attention changes: `{payload.get('attention_change_count', 0)}`",
    ]
    if payload.get("headline"):
        lines.append(f"- Status: `{payload.get('headline')}`")
    if payload.get("recommended_action"):
        lines.append(f"- Recommended action: `{payload.get('recommended_action')}`")
    lines.extend(["", "## Material Changes", ""])
    items = list(payload.get("items") or [])
    if not items:
        lines.append("No material learning changes are ready right now.")
        return "\n".join(lines)
    for item in items:
        lines.append(f"- `{item.get('urgency') or 'opportunity'}` · {item.get('headline') or 'Learning change'}")
        if item.get("detail"):
            lines.append(f"  Detail: {item.get('detail')}")
        if item.get("source"):
            lines.append(f"  Source: `{item.get('source')}`")
    return "\n".join(lines)


def refresh_promotion_readiness_artifact() -> None:
    payload = load_json(BUSINESS_OPERATOR_DESK_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return
    promotion_surface = payload.get("promotion_watch_surface") if isinstance(payload.get("promotion_watch_surface"), dict) else {}
    items = [item for item in list(promotion_surface.get("items") or []) if isinstance(item, dict)]
    ready_items = [
        item
        for item in items
        if item.get("promotion_state") == "ready" and not bool(item.get("already_promoted"))
    ]
    digest_payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "source": "business_desk",
        "source_path": str(BUSINESS_OPERATOR_DESK_PATH),
        "headline": promotion_surface.get("headline"),
        "recommended_action": promotion_surface.get("recommended_action"),
        "item_count": len(items),
        "ready_item_count": len(ready_items),
        "items": ready_items[:6],
    }
    markdown = render_promotion_readiness_markdown(digest_payload)
    PROMOTION_READINESS_DIGEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROMOTION_READINESS_DIGEST_PATH.write_text(json.dumps(digest_payload, indent=2), encoding="utf-8")
    md_for_json(PROMOTION_READINESS_DIGEST_PATH).write_text(markdown + "\n", encoding="utf-8")


def refresh_learning_change_artifact() -> None:
    payload = load_json(CURRENT_LEARNINGS_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return
    notifier = payload.get("change_notifier") if isinstance(payload.get("change_notifier"), dict) else {}
    items = [item for item in list(notifier.get("items") or []) if isinstance(item, dict)]
    digest_payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "source": "current_learnings",
        "source_path": str(CURRENT_LEARNINGS_PATH),
        "supporting_paths": {
            "current_learnings": str(CURRENT_LEARNINGS_PATH),
            "weekly_strategy_packet": str(WEEKLY_STRATEGY_PACKET_PATH),
        },
        "headline": notifier.get("headline"),
        "recommended_action": notifier.get("recommended_action"),
        "change_count": int(notifier.get("change_count") or 0),
        "material_change_count": int(notifier.get("material_change_count") or 0),
        "attention_change_count": int(notifier.get("attention_change_count") or 0),
        "items": items[:6],
    }
    markdown = render_learning_change_markdown(digest_payload)
    LEARNING_CHANGE_DIGEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    LEARNING_CHANGE_DIGEST_PATH.write_text(json.dumps(digest_payload, indent=2), encoding="utf-8")
    md_for_json(LEARNING_CHANGE_DIGEST_PATH).write_text(markdown + "\n", encoding="utf-8")


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


def hydrate_promotion_readiness_signature(state: dict[str, Any]) -> bool:
    if (
        state.get("last_promotion_readiness_signature")
        and state.get("last_promotion_readiness_signature_version") == PROMOTION_READINESS_SIGNATURE_VERSION
    ):
        return False

    payload_path = PROMOTION_READINESS_DIGEST_PATH
    sent_items = state.get("sent") or {}
    promotion_entries = []
    sent_at: str | None = None
    for key, record in sent_items.items():
        if record.get("kind") != "promotion_readiness":
            continue
        promotion_entries.append((record.get("sent_at") or "", key))
    if promotion_entries:
        sent_at, latest_key = sorted(promotion_entries)[-1]
        payload_path = Path(latest_key)

    if not payload_path.exists():
        return False
    payload = load_json(payload_path, {})
    state["last_promotion_readiness_signature"] = promotion_readiness_signature(payload)
    state["last_promotion_readiness_signature_version"] = PROMOTION_READINESS_SIGNATURE_VERSION
    if sent_at:
        state["last_promotion_readiness_sent_at"] = sent_at
    return True


def hydrate_learning_change_signature(state: dict[str, Any]) -> bool:
    if (
        state.get("last_learning_change_signature")
        and state.get("last_learning_change_signature_version") == LEARNING_CHANGE_SIGNATURE_VERSION
    ):
        return False

    payload_path = LEARNING_CHANGE_DIGEST_PATH
    sent_items = state.get("sent") or {}
    sent_at: str | None = None
    learning_entries = []
    for key, record in sent_items.items():
        if record.get("kind") != "learning_change_digest":
            continue
        learning_entries.append((record.get("sent_at") or "", key))
    if learning_entries:
        sent_at, latest_key = sorted(learning_entries)[-1]
        payload_path = Path(latest_key)

    if not payload_path.exists():
        return False
    payload = load_json(payload_path, {})
    state["last_learning_change_signature"] = learning_change_signature(payload)
    state["last_learning_change_signature_version"] = LEARNING_CHANGE_SIGNATURE_VERSION
    if sent_at:
        state["last_learning_change_sent_at"] = sent_at
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


def should_send_promotion_readiness(state: dict[str, Any], payload: dict[str, Any]) -> tuple[bool, str, str]:
    signature = promotion_readiness_signature(payload)
    if int(payload.get("ready_item_count") or 0) <= 0:
        return False, "no_ready_candidates", signature

    previous_signature = state.get("last_promotion_readiness_signature")
    if signature != previous_signature:
        return True, "promotion_ready", signature

    return False, "no_material_change", signature


def should_send_learning_change(state: dict[str, Any], payload: dict[str, Any]) -> tuple[bool, str, str]:
    signature = learning_change_signature(payload)
    if int(payload.get("material_change_count") or 0) <= 0:
        return False, "no_material_changes", signature

    previous_signature = state.get("last_learning_change_signature")
    if signature != previous_signature:
        if int(payload.get("attention_change_count") or 0) > 0:
            return True, "attention_learning_change", signature
        return True, "learning_change", signature

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

    if PROMOTION_READINESS_DIGEST_PATH.exists():
        payload = load_json(PROMOTION_READINESS_DIGEST_PATH, {})
        should_send, send_reason, signature = should_send_promotion_readiness(state, payload)
        if should_send:
            artifacts.append(
                {
                    "kind": "promotion_readiness",
                    "key": str(PROMOTION_READINESS_DIGEST_PATH),
                    "json_path": PROMOTION_READINESS_DIGEST_PATH,
                    "md_path": md_for_json(PROMOTION_READINESS_DIGEST_PATH),
                    "payload": payload,
                    "send_reason": send_reason,
                    "promotion_readiness_signature": signature,
                }
            )

    if LEARNING_CHANGE_DIGEST_PATH.exists():
        payload = load_json(LEARNING_CHANGE_DIGEST_PATH, {})
        should_send, send_reason, signature = should_send_learning_change(state, payload)
        if should_send:
            artifacts.append(
                {
                    "kind": "learning_change_digest",
                    "key": str(LEARNING_CHANGE_DIGEST_PATH),
                    "json_path": LEARNING_CHANGE_DIGEST_PATH,
                    "md_path": md_for_json(LEARNING_CHANGE_DIGEST_PATH),
                    "payload": payload,
                    "send_reason": send_reason,
                    "learning_change_signature": signature,
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


def _latest_file_mtime(path: Path) -> datetime | None:
    if not path.exists():
        return None
    candidates = [item for item in path.rglob("*") if item.is_file()]
    if not candidates:
        return None
    latest = max(candidates, key=lambda item: item.stat().st_mtime)
    return datetime.fromtimestamp(latest.stat().st_mtime).astimezone()


def sync_notifier_control(
    state: dict[str, Any],
    *,
    pending_artifacts: list[dict[str, Any]] | None = None,
    whatsapp_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if pending_artifacts is None:
        pending_artifacts = load_sendable_artifacts(state)

    last_delivery_dt = parse_iso_datetime(state.get("last_reviews_whatsapp_sent_at"))
    last_delivery_dt = max(
        [dt for dt in [
            last_delivery_dt,
            parse_iso_datetime(state.get("last_digest_sent_at")),
            parse_iso_datetime(state.get("last_trend_digest_sent_at")),
            parse_iso_datetime(state.get("last_learning_change_sent_at")),
            _latest_file_mtime(OUTPUT_DIGESTS),
            _latest_file_mtime(ROOT / "output" / "operator"),
        ] if dt is not None],
        default=None,
    )
    age_hours = None
    if last_delivery_dt is not None:
        age_hours = round((datetime.now().astimezone() - last_delivery_dt).total_seconds() / 3600.0, 2)

    pending_count = len(pending_artifacts or [])
    has_whatsapp = bool(state.get("last_operator_whatsapp_signature") or (whatsapp_summary or {}).get("signature"))
    has_recent_digest = bool(
        state.get("last_digest_sent_at")
        or state.get("last_trend_digest_sent_at")
        or state.get("last_learning_change_sent_at")
    )
    sent_count = len(state.get("sent") or {})

    if age_hours is not None and age_hours >= 72 and not pending_count:
        control_state = "blocked"
        reason = "stale_input"
        next_action = "Run the notifier so digests and operator pushes reflect the current live queue."
    elif pending_count:
        control_state = "observed"
        reason = "pending_delivery"
        next_action = "Send or clear the queued notifier artifacts."
    elif has_whatsapp:
        control_state = "verified"
        reason = "operator_push_sent"
        next_action = "No notifier action needed unless new review items arrive."
    elif has_recent_digest:
        control_state = "verified"
        reason = "digest_sent"
        next_action = "No notifier action needed unless new digest content appears."
    elif sent_count:
        control_state = "observed"
        reason = "artifacts_ready"
        next_action = "Review notifier outputs and send any missing summaries if needed."
    else:
        control_state = "observed"
        reason = "idle"
        next_action = "No notifier artifacts are ready right now."

    control = record_workflow_transition(
        workflow_id="notifier",
        lane="notifier",
        display_label="Notifier",
        entity_id="notifier",
        state=control_state,
        state_reason=reason,
        input_freshness={
            "source": str(STATE_PATH),
            "age_hours": age_hours,
        },
        next_action=next_action,
        metadata={
            "pending_count": pending_count,
            "sent_count": sent_count,
            "has_whatsapp": has_whatsapp,
            "has_recent_digest": has_recent_digest,
        },
        receipt_kind="state_sync",
        receipt_payload={
            "pending_kinds": [str(item.get("kind") or "") for item in pending_artifacts[:10]],
            "pending_count": pending_count,
        },
        history_summary=reason.replace("_", " "),
    )
    state["workflow_control"] = {
        "state": control_state,
        "state_reason": reason,
        "age_hours": age_hours,
        "path": str((control or {}).get("latest_receipt", {}).get("path") or ""),
    }
    return state


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
    operator_current = load_json(OPERATOR_CURRENT_PATH, {})
    current = operator_current.get("current") or {}
    current_flow = current.get("flow") or ""
    current_message = (operator_current.get("message") or "").strip()
    if (
        whatsapp_operator_item_allowed(current)
        and str(current.get("review_status") or "") in {"", "pending"}
        and current_message
    ):
        preview = current.get("preview") or {}
        media_urls = [
            str(url).strip()
            for url in ([preview.get("asset_url")] + list(preview.get("asset_urls") or []))
            if str(url or "").strip()
        ]
        signature_payload = {
            "artifact_id": current.get("artifact_id"),
            "input_hash": current.get("input_hash"),
            "review_status": current.get("review_status"),
            "message": current_message,
            "media_urls": media_urls,
        }
        signature = canonical_hash(signature_payload)
        if state.get("last_operator_whatsapp_signature") == signature or state.get("last_reviews_whatsapp_signature") == signature:
            return None
        return {
            "kind": "operator_whatsapp",
            "run_id": current.get("run_id"),
            "signature": signature,
            "message": f"{WHATSAPP_PUSH_SENTINEL}\n{current_message}",
            "media_urls": list(dict.fromkeys(media_urls)),
            "media_title": current.get("title") or current.get("artifact_id") or "OpenClaw Review",
        }

    quality_gate = load_json(QUALITY_GATE_STATE_PATH, {"artifacts": {}})
    artifacts = quality_gate.get("artifacts", {})
    review_items: list[dict[str, Any]] = []
    for record in artifacts.values():
        decision = record.get("decision") or {}
        review_status = decision.get("review_status")
        if review_status not in {None, "pending"}:
            continue
        if not whatsapp_operator_item_allowed(decision):
            continue
        review_items.append(decision)

    if not review_items:
        return None

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
    if state.get("last_operator_whatsapp_signature") == signature or state.get("last_reviews_whatsapp_signature") == signature:
        return None

    if current_flow.startswith("reviews_") and current_message and current.get("artifact_id") == artifact_id:
        lines = [WHATSAPP_PUSH_SENTINEL, current_message]
    else:
        short_id = selected.get("short_id")
        title = selected.get("title") or artifact_id
        decision = selected.get("decision") or "pending"
        reasons = selected.get("reasoning") or ["No reasoning captured."]
        flow = str(selected.get("flow") or "")
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
        lines.extend(["", "Reply:", "agree"])
        if flow == "weekly_sale":
            lines.extend(
                [
                    "approve <short reason>",
                    "needs changes <short reason>",
                    "rewrite",
                    "discard <short reason>",
                    "why",
                ]
            )
        elif flow in {"meme", "jeepfact"}:
            lines.extend(
                [
                    "approve <short reason>",
                    "needs changes <short reason>",
                    "rewrite",
                    "discard <short reason>",
                    "why",
                ]
            )
        else:
            lines.extend(
                [
                    "publish <short reason>",
                    "hold",
                    "discard <short reason>",
                    "why",
                ]
            )
    return {
        "kind": "operator_whatsapp",
        "run_id": selected.get("run_id"),
        "signature": signature,
        "message": "\n".join(lines),
        "media_urls": unique_media_urls(
            [
                str(url).strip()
                for url in ([((selected.get("preview") or {}).get("asset_url"))] + list(((selected.get("preview") or {}).get("asset_urls") or [])))
                if str(url or "").strip()
            ]
        ),
        "media_title": selected.get("title") or selected.get("artifact_id") or "OpenClaw Review",
    }


def _business_desk_whatsapp_lines(payload: dict[str, Any]) -> list[str]:
    counts = payload.get("counts") or {}
    next_actions = list(payload.get("next_actions") or [])
    lines = [
        WHATSAPP_PUSH_SENTINEL,
        "Duck Ops Business Desk",
        f"Customer actions: {int(counts.get('customer_packets') or 0)}",
        f"Etsy thread follow-ups: {int(counts.get('etsy_browser_threads') or 0)}",
        f"Custom builds: {int(counts.get('custom_build_candidates') or 0)}",
        f"Pack tonight units: {int(counts.get('orders_to_pack_units') or 0)}",
        f"Creative reviews: {int(counts.get('review_queue_items') or 0)}",
        f"Social plan ready: {int(counts.get('strategy_ready_slots') or 0)}",
        f"Workflow follow-through: {int(counts.get('workflow_followthrough_items') or 0)}",
    ]
    if next_actions:
        lines.extend(["", "Do next:"])
        for item in next_actions[:3]:
            summary = str(item.get("summary") or item.get("title") or "Next action").strip()
            command = str(item.get("command") or "").strip()
            secondary = str(item.get("secondary_command") or "").strip()
            line = f"- {item.get('lane') or 'desk'}: {summary}"
            if command:
                line += f" | {command}"
            if secondary:
                line += f" -> {secondary}"
            lines.append(line)
    lines.extend(["", "Reply:", "desk next", "desk show customer", "desk show builds"])
    return lines


def build_business_desk_whatsapp_operator_push(state: dict[str, Any]) -> dict[str, Any] | None:
    payload = load_json(BUSINESS_OPERATOR_DESK_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return None

    counts = payload.get("counts") or {}
    next_actions = list(payload.get("next_actions") or [])
    actionable_count = (
        int(counts.get("customer_packets") or 0)
        + int(counts.get("etsy_browser_threads") or 0)
        + int(counts.get("custom_build_candidates") or 0)
        + int(counts.get("orders_to_pack_units") or 0)
        + int(counts.get("review_queue_items") or 0)
        + int(counts.get("strategy_ready_slots") or 0)
        + int(counts.get("workflow_followthrough_items") or 0)
    )
    if actionable_count <= 0 and not next_actions:
        return None

    signature_payload = {
        "generated_at": payload.get("generated_at"),
        "counts": counts,
        "next_actions": next_actions[:3],
    }
    signature = canonical_hash(signature_payload)
    if state.get("last_operator_whatsapp_signature") == signature or state.get("last_reviews_whatsapp_signature") == signature:
        return None

    return {
        "kind": "operator_whatsapp",
        "run_id": payload.get("generated_at"),
        "signature": signature,
        "message": "\n".join(_business_desk_whatsapp_lines(payload)),
        "media_urls": [],
        "media_title": "Duck Ops Business Desk",
    }


def build_operator_whatsapp_summary(settings: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    whatsapp_cfg = settings.get("whatsapp") or {}
    if not whatsapp_cfg.get("enabled"):
        return None

    summary = None
    if whatsapp_cfg.get("review_operator_push_enabled", True):
        summary = build_reviews_whatsapp_operator_push(state)
    if not summary and whatsapp_cfg.get("business_desk_operator_push_enabled", True):
        summary = build_business_desk_whatsapp_operator_push(state)
    return summary


def maybe_auto_approve_weekly_sales(
    settings: dict[str, Any],
    state: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    weekly_cfg = ((settings.get("auto_approval") or {}).get("weekly_sale") or {})
    if not weekly_cfg.get("enabled"):
        return {"enabled": False, "changed": False, "results": []}

    min_score = int(weekly_cfg.get("min_score") or 82)
    min_confidence = float(weekly_cfg.get("min_confidence") or 0.72)

    from review_loop import (
        build_review_items,
        load_operator_state,
        load_state_bundle,
        maybe_handoff_duckagent_publish_after_operator_action,
        record_action,
        write_review_queue,
        write_state_source,
    )

    state_bundle = load_state_bundle()
    operator_state = load_operator_state()
    items = build_review_items(state_bundle)
    eligible = sorted(
        [
            item
            for item in items
            if str(item.get("flow") or "") == "weekly_sale"
            and str(item.get("review_status") or "") in {"", "pending"}
            and str(item.get("decision") or "") == "publish_ready"
            and int(item.get("score") or 0) >= min_score
            and float(item.get("confidence") or 0.0) >= min_confidence
        ],
        key=lambda item: (
            str(item.get("run_id") or ""),
            int(item.get("score") or 0),
            float(item.get("confidence") or 0.0),
        ),
        reverse=True,
    )

    results: list[dict[str, Any]] = []
    changed = False
    for item in eligible:
        artifact_id = str(item.get("artifact_id") or "").strip()
        if not artifact_id:
            continue
        source_name = ""
        for candidate_source, source_state in state_bundle.items():
            if artifact_id in ((source_state.get("artifacts") or {}).keys()):
                source_name = candidate_source
                break
        if not source_name:
            results.append(
                {
                    "artifact_id": artifact_id,
                    "run_id": item.get("run_id"),
                    "status": "missing_source",
                    "message": "Could not find the weekly-sale item in the review state bundle.",
                }
            )
            continue

        source_snapshot = copy.deepcopy(state_bundle[source_name])
        approval_note = (
            "Auto-approved by OpenClaw because this weekly sale was publish-ready "
            f"with score {int(item.get('score') or 0)} and confidence {float(item.get('confidence') or 0.0):.2f}."
        )
        record, resolved_source = record_action(
            state_bundle,
            artifact_id,
            "approve",
            note=approval_note,
            resolution="approve",
        )
        handoff = maybe_handoff_duckagent_publish_after_operator_action(record.get("decision") or {})
        handoff_ok = bool(handoff and handoff.get("ok"))
        if dry_run:
            state_bundle[source_name] = source_snapshot
            results.append(
                {
                    "artifact_id": artifact_id,
                    "run_id": item.get("run_id"),
                    "status": "would_auto_approve" if handoff_ok else "would_fail_closed",
                    "message": (handoff or {}).get("message") or "Dry run only.",
                    "score": item.get("score"),
                    "confidence": item.get("confidence"),
                }
            )
            continue
        if not handoff_ok:
            state_bundle[source_name] = source_snapshot
            results.append(
                {
                    "artifact_id": artifact_id,
                    "run_id": item.get("run_id"),
                    "status": "failed_closed",
                    "message": (handoff or {}).get("message") or "DuckAgent publish handoff did not succeed.",
                    "score": item.get("score"),
                    "confidence": item.get("confidence"),
                }
            )
            continue

        write_state_source(resolved_source, state_bundle[resolved_source])
        changed = True
        results.append(
            {
                "artifact_id": artifact_id,
                "run_id": item.get("run_id"),
                "status": "auto_approved",
                "message": (handoff or {}).get("message") or "DuckAgent publish was requested.",
                "score": item.get("score"),
                "confidence": item.get("confidence"),
            }
        )

    if changed and not dry_run:
        write_review_queue(state_bundle, operator_state)

    state["weekly_sale_auto_approval"] = {
        "last_run_at": datetime.now().astimezone().isoformat(),
        "enabled": True,
        "thresholds": {"min_score": min_score, "min_confidence": min_confidence},
        "results": results[-10:],
    }
    return {"enabled": True, "changed": changed, "results": results}


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
    html_body = render_notifier_html(kind, subject, body, payload)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings["user"]
    msg["To"] = settings["to"]
    msg.set_content(body)
    msg.add_alternative(html_body, subtype="html")
    return msg


def send_message(settings: dict[str, Any], msg: EmailMessage) -> None:
    with smtplib.SMTP(settings["host"], settings["port"]) as server:
        if settings.get("use_starttls"):
            server.starttls()
        server.login(settings["user"], settings["password"])
        server.send_message(msg)


def send_whatsapp_message(
    settings: dict[str, Any],
    message: str,
    media_urls: list[str] | None = None,
    media_title: str | None = None,
) -> None:
    whatsapp_cfg = settings.get("whatsapp") or {}
    target = whatsapp_cfg.get("target")
    docker_path = whatsapp_cfg.get("docker_path") or "/usr/local/bin/docker"
    gateway_container = whatsapp_cfg.get("gateway_container") or "openclaw-openclaw-gateway-1"
    if not target:
        raise SystemExit("Notifier WhatsApp target is missing.")
    media_list = prepare_whatsapp_media_urls(settings, media_urls=media_urls, media_title=media_title)
    for index, media_url in enumerate(media_list or [None]):
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
        ]
        text_payload = message if index == 0 else ""
        if text_payload:
            cmd.extend(["--message", text_payload])
        if media_url:
            cmd.extend(["--media", stage_whatsapp_media_for_container(settings, media_url)])
        subprocess.run(cmd, check=True, capture_output=True, text=True)


def preview_message_text(msg: EmailMessage, artifact: dict[str, Any]) -> str:
    preview_part = msg.get_body(preferencelist=("plain", "html"))
    if preview_part is not None:
        return str(preview_part.get_content())
    md_path = artifact.get("md_path")
    if isinstance(md_path, Path) and md_path.exists():
        return md_path.read_text(encoding="utf-8")
    return json.dumps(artifact.get("payload", {}), indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send or preview OpenClaw digest and urgent alerts.")
    parser.add_argument("--dry-run", action="store_true", help="Preview notifications without sending mail.")
    parser.add_argument(
        "--skip-order-refresh",
        action="store_true",
        help="Reuse the latest saved order snapshots and packing summary instead of refreshing them live.",
    )
    parser.add_argument(
        "--skip-customer-refresh-preflight",
        action="store_true",
        help="Skip the Etsy customer inbox browser preflight when building notifier summaries.",
    )
    args = parser.parse_args()

    settings = notifier_settings()
    state = load_json(STATE_PATH, {"sent": {}})
    state.setdefault("sent", {})
    try:
        customer_preflight_cfg = settings.get("customer_inbox_refresh_preflight") or {}
        customer_preflight_enabled = bool(customer_preflight_cfg.get("enabled", True))
        refresh_nightly_action_summary_sources(
            skip_order_refresh=args.skip_order_refresh,
            skip_customer_refresh_preflight=(not customer_preflight_enabled) or args.skip_customer_refresh_preflight,
        )
    except Exception as exc:
        print(f"[notifier] Warning: could not refresh nightly action summary sources: {exc}", file=sys.stderr)
    try:
        refresh_phase_readiness_artifact()
    except Exception as exc:
        print(f"[notifier] Warning: could not refresh phase readiness artifact: {exc}", file=sys.stderr)
    try:
        refresh_promotion_readiness_artifact()
    except Exception as exc:
        print(f"[notifier] Warning: could not refresh promotion readiness artifact: {exc}", file=sys.stderr)
    try:
        refresh_learning_change_artifact()
    except Exception as exc:
        print(f"[notifier] Warning: could not refresh learning change artifact: {exc}", file=sys.stderr)
    auto_approval_result = maybe_auto_approve_weekly_sales(settings, state, dry_run=args.dry_run)
    state_changed = hydrate_digest_signature(state)
    state_changed = hydrate_trend_digest_signature(state) or state_changed
    state_changed = hydrate_promotion_readiness_signature(state) or state_changed
    state_changed = hydrate_learning_change_signature(state) or state_changed
    state_changed = bool(auto_approval_result.get("changed")) or state_changed
    artifacts = load_sendable_artifacts(state)
    whatsapp_summary = build_operator_whatsapp_summary(settings, state)

    if args.dry_run and auto_approval_result.get("results"):
        for result in auto_approval_result.get("results") or []:
            print(
                "DRY RUN :: weekly_sale_auto_approval :: "
                f"{result.get('run_id')} :: {result.get('status')} :: {result.get('message')}"
            )

    if not artifacts and not whatsapp_summary:
        if not args.dry_run:
            state = sync_notifier_control(state, pending_artifacts=[], whatsapp_summary=None)
        if not args.dry_run and (state_changed or state.get("workflow_control")):
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
            print(preview_message_text(msg, artifact)[:2000])
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
        if artifact["kind"] == "promotion_readiness":
            state["last_promotion_readiness_signature"] = artifact["promotion_readiness_signature"]
            state["last_promotion_readiness_signature_version"] = PROMOTION_READINESS_SIGNATURE_VERSION
            state["last_promotion_readiness_sent_at"] = state["sent"][artifact["key"]]["sent_at"]
            state["last_promotion_readiness_reason"] = artifact.get("send_reason")
        if artifact["kind"] == "learning_change_digest":
            state["last_learning_change_signature"] = artifact["learning_change_signature"]
            state["last_learning_change_signature_version"] = LEARNING_CHANGE_SIGNATURE_VERSION
            state["last_learning_change_sent_at"] = state["sent"][artifact["key"]]["sent_at"]
            state["last_learning_change_reason"] = artifact.get("send_reason")
        state_changed = True

    if whatsapp_summary:
        if args.dry_run:
            print(f"DRY RUN :: operator_whatsapp :: {whatsapp_summary['run_id']}")
            print("---")
            print(whatsapp_summary["message"])
            print("===")
        else:
            whatsapp_cfg = settings.get("whatsapp") or {}
            if whatsapp_cfg.get("enabled"):
                send_whatsapp_message(
                    settings,
                    whatsapp_summary["message"],
                    media_urls=whatsapp_summary.get("media_urls"),
                    media_title=whatsapp_summary.get("media_title"),
                )
                state["last_operator_whatsapp_signature"] = whatsapp_summary["signature"]
                state["last_reviews_whatsapp_signature"] = whatsapp_summary["signature"]
                state["last_reviews_whatsapp_run_id"] = whatsapp_summary["run_id"]
                state["last_reviews_whatsapp_sent_at"] = datetime.now().astimezone().isoformat()
                state_changed = True

    if not args.dry_run:
        state = sync_notifier_control(state, pending_artifacts=load_sendable_artifacts(state), whatsapp_summary=whatsapp_summary)

    if not args.dry_run and (state_changed or state.get("workflow_control")):
        write_json(STATE_PATH, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
