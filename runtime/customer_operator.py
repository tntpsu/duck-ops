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
import subprocess
import sys
from pathlib import Path
from typing import Any

from customer_recovery_decisions import append_decision, normalize_resolution


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
OUTPUT_DIR = ROOT / "output" / "operator"
PACKETS_PATH = STATE_DIR / "customer_action_packets.json"
OPERATOR_STATE_PATH = STATE_DIR / "customer_operator_state.json"
CURRENT_CARD_PATH = OUTPUT_DIR / "current_customer_action.md"
QUEUE_CARD_PATH = OUTPUT_DIR / "customer_queue.md"

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
    lines.extend(
        [
            f"Source refs: {_packet_source_refs(packet)}",
            "",
            "Allowed replies:",
            f"- `replacement {packet.get('short_id')} because ...`",
            f"- `refund {packet.get('short_id')} because ...`",
            f"- `wait {packet.get('short_id')} because ...`",
            f"- `reply only {packet.get('short_id')} because ...`",
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
    if lowered.startswith("customer show"):
        parts = raw.split()
        target = parts[2] if len(parts) > 2 else None
        return "show", target, ""
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


def _rerun_observer() -> None:
    subprocess.run([sys.executable, str(ROOT / "runtime" / "phase1_observer.py")], cwd=str(ROOT), check=False)


def render_customer_status(items: list[dict[str, Any]], current: dict[str, Any] | None) -> str:
    lines = [
        "Duck Ops customer status:",
        f"- Packets: {len(items)}",
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


def handle_customer_text(text: str) -> str:
    packet_payload = load_packets()
    operator_state = load_operator_state()
    items = assign_short_ids(packet_payload, operator_state)
    current = sync_current_packet(items, operator_state)
    command, target_token, note = parse_customer_command(text)

    if command in {"status", "customer"}:
        write_customer_operator_outputs(packet_payload, operator_state)
        return render_customer_status(items, current)

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

    if command not in {"replacement", "refund", "wait", "reply_only"}:
        write_customer_operator_outputs(packet_payload, operator_state)
        return (
            "Customer operator commands:\n"
            "- customer status\n"
            "- customer next\n"
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
    if args.command == "handle":
        print(handle_customer_text(args.text))
        return 0
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
