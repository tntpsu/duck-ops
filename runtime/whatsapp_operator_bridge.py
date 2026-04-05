#!/usr/bin/env python3
"""
Strict WhatsApp operator bridge for Duck Phase 2 review handling.

This bridge bypasses the generic OpenClaw chat path for the self-chat review lane.
It watches the active WhatsApp DM session transcript, extracts real human replies,
routes them through the deterministic review loop, and sends the result back to WhatsApp.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
HOST_OPENCLAW_ROOT = Path("/Users/philtullai/ai-agents/openclaw")
HOST_SESSION_INDEX = HOST_OPENCLAW_ROOT / "config" / "agents" / "main" / "sessions" / "sessions.json"
HOST_SESSION_DIR = HOST_OPENCLAW_ROOT / "config" / "agents" / "main" / "sessions"
STATE_PATH = ROOT / "state" / "whatsapp_operator_bridge_state.json"
WHATSAPP_SESSION_KEY = "agent:main:whatsapp:direct:+18148124112"
PROCESSED_ID_LIMIT = 500
OUTBOUND_ECHO_HASH_LIMIT = 200

sys.path.insert(0, str(Path(__file__).resolve().parent))

from notifier import notifier_settings, send_whatsapp_message  # noqa: E402
from review_loop import (  # noqa: E402
    load_operator_state,
    load_state_bundle,
    now_iso,
    write_json,
    write_operator_state,
    write_review_queue,
    handle_operator_text,
)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def load_bridge_state() -> dict[str, Any]:
    return load_json(
        STATE_PATH,
        {
            "session_id": None,
            "processed_message_ids": [],
            "last_processed_message_id": None,
            "recent_outbound_echo_hashes": [],
            "bootstrapped_at": None,
            "last_processed_at": None,
        },
    )


def write_bridge_state(state: dict[str, Any]) -> None:
    processed = state.get("processed_message_ids") or []
    if len(processed) > PROCESSED_ID_LIMIT:
        state["processed_message_ids"] = processed[-PROCESSED_ID_LIMIT:]
    outbound_hashes = state.get("recent_outbound_echo_hashes") or []
    if len(outbound_hashes) > OUTBOUND_ECHO_HASH_LIMIT:
        state["recent_outbound_echo_hashes"] = outbound_hashes[-OUTBOUND_ECHO_HASH_LIMIT:]
    write_json(STATE_PATH, state)


def resolve_whatsapp_session() -> tuple[str | None, Path | None]:
    index = load_json(HOST_SESSION_INDEX, {})
    session = index.get(WHATSAPP_SESSION_KEY) or {}
    session_id = session.get("sessionId")
    if not session_id:
        return None, None
    session_path = HOST_SESSION_DIR / f"{session_id}.jsonl"
    if not session_path.exists():
        return str(session_id), None
    return str(session_id), session_path


def parse_sender_value(raw_text: str) -> str | None:
    match = re.search(r'"sender"\s*:\s*"([^"]+)"', raw_text)
    if match:
        return match.group(1)
    return None


def parse_simplified_self_body(raw_text: str) -> str | None:
    match = re.match(r"^\[WhatsApp [^\]]+\]\s+\(self\):\s*(.*)$", raw_text.strip(), re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def parse_body_text(raw_text: str) -> str:
    simplified = parse_simplified_self_body(raw_text)
    if simplified is not None:
        return simplified
    if "```" in raw_text:
        tail = raw_text.rsplit("```", 1)[-1].strip()
        if tail:
            return tail
    return raw_text.strip()


def normalize_echo_text(body_text: str) -> str:
    return re.sub(r"\s+", " ", (body_text or "").strip()).lower()


def echo_hash(body_text: str) -> str | None:
    normalized = normalize_echo_text(body_text)
    if not normalized:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def is_human_operator_message(raw_text: str) -> bool:
    sender_value = parse_sender_value(raw_text)
    if sender_value:
        return not sender_value.startswith("+")
    return parse_simplified_self_body(raw_text) is not None


def is_operator_echo_body(body_text: str) -> bool:
    lowered = body_text.strip().lower()
    return (
        lowered.startswith("openclaw review ")
        or lowered.startswith("openclaw detail ")
        or lowered.startswith("openclaw suggestions ")
        or lowered.startswith("openclaw operator commands:")
        or lowered.startswith("openclaw queue status:")
        or lowered.startswith("openclaw_operator_push")
        or lowered.startswith("recorded:")
        or lowered.startswith("saved:")
        or lowered.startswith("please tell me which duck you already have.")
        or lowered.startswith("i couldn't confidently match that to one of your products.")
        or lowered.startswith("that command only works on trend items.")
        or lowered.startswith("please add a short reason.")
        or lowered.startswith("that review item `")
        or lowered.startswith("i could not find that review item")
        or lowered.startswith("no more queued reviews right now.")
        or lowered.startswith("no pending reviews right now.")
    )


def is_recent_outbound_echo(body_text: str, outbound_hashes: set[str]) -> bool:
    hashed = echo_hash(body_text)
    if not hashed:
        return False
    return hashed in outbound_hashes


def extract_text_content(entry: dict[str, Any]) -> str:
    message = entry.get("message") or {}
    content = message.get("content") or []
    texts = []
    for part in content:
        if isinstance(part, dict) and part.get("type") == "text":
            texts.append(part.get("text") or "")
    return "\n".join(texts).strip()


def iter_new_human_messages(
    session_path: Path,
    processed_ids: set[str],
    outbound_hashes: set[str],
    last_processed_message_id: str | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    entries: list[dict[str, Any]] = []
    for raw_line in session_path.read_text(encoding="utf-8").splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            entry = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if entry.get("type") == "message":
            entries.append(entry)

    start_index = 0
    if last_processed_message_id:
        for index, entry in enumerate(entries):
            if entry.get("id") == last_processed_message_id:
                start_index = index + 1
                break

    messages: list[dict[str, Any]] = []
    ignored_ids: list[str] = []
    # Read a fixed snapshot of the transcript at the start of the bridge pass.
    # This prevents one run from seeing self-echo lines it triggered moments
    # earlier and turning them into another operator response.
    for entry in entries[start_index:]:
        if entry.get("type") != "message":
            continue
        if (entry.get("message") or {}).get("role") != "user":
            continue
        message_id = entry.get("id")
        if not message_id or message_id in processed_ids:
            continue
        raw_text = extract_text_content(entry)
        if not raw_text or not is_human_operator_message(raw_text):
            continue
        body_text = parse_body_text(raw_text)
        if not body_text:
            continue
        if is_operator_echo_body(body_text) or is_recent_outbound_echo(body_text, outbound_hashes):
            ignored_ids.append(message_id)
            continue
        messages.append(
            {
                "id": message_id,
                "timestamp": entry.get("timestamp"),
                "body_text": body_text,
                "raw_text": raw_text,
            }
        )
    return messages, ignored_ids


def process_operator_text(text: str, dry_run: bool = False) -> str:
    state_bundle = load_state_bundle()
    operator_state = load_operator_state()
    if not dry_run:
        response = handle_operator_text(state_bundle, operator_state, text)
        write_operator_state(operator_state)
        write_review_queue(state_bundle, operator_state)
        return response

    state_bundle = json.loads(json.dumps(state_bundle))
    operator_state = json.loads(json.dumps(operator_state))
    snapshot_paths = [
        ROOT / "state" / "quality_gate_state.json",
        ROOT / "state" / "trend_ranker_state.json",
        ROOT / "state" / "operator_state.json",
        ROOT / "state" / "review_queue.json",
    ]
    snapshot_paths.extend(sorted((ROOT / "output" / "operator").glob("*")))
    snapshots: dict[Path, bytes | None] = {}
    for path in snapshot_paths:
        snapshots[path] = path.read_bytes() if path.exists() else None
    try:
        return handle_operator_text(state_bundle, operator_state, text)
    finally:
        for path, content in snapshots.items():
            if content is None:
                if path.exists():
                    path.unlink()
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)


MAX_WHATSAPP_CHARS = 1400


def split_whatsapp_response(message: str) -> list[str]:
    if len(message) <= MAX_WHATSAPP_CHARS:
        return [message]

    if "\n\nNext review:\n\n" in message:
        ack, next_card = message.split("\n\nNext review:\n\n", 1)
        parts = []
        if ack.strip():
            parts.extend(split_whatsapp_response(ack.strip()))
        next_payload = "Next review:\n\n" + next_card.strip()
        if next_payload.strip():
            parts.extend(split_whatsapp_response(next_payload))
        return parts

    chunks: list[str] = []
    current = ""
    for block in message.split("\n\n"):
        candidate = block.strip()
        if not candidate:
            continue
        proposed = candidate if not current else f"{current}\n\n{candidate}"
        if len(proposed) <= MAX_WHATSAPP_CHARS:
            current = proposed
            continue
        if current:
            chunks.append(current)
            current = ""
        while len(candidate) > MAX_WHATSAPP_CHARS:
            split_at = candidate.rfind("\n", 0, MAX_WHATSAPP_CHARS)
            if split_at <= 0:
                split_at = MAX_WHATSAPP_CHARS
            chunks.append(candidate[:split_at].strip())
            candidate = candidate[split_at:].strip()
        current = candidate
    if current:
        chunks.append(current)
    return chunks or [message[:MAX_WHATSAPP_CHARS]]


def send_operator_response(settings: dict[str, Any], response: str) -> list[str]:
    try:
        send_whatsapp_message(settings, response)
        return [response]
    except Exception:
        parts = split_whatsapp_response(response)
        if len(parts) == 1 and parts[0] == response:
            raise

    for part in parts:
        send_whatsapp_message(settings, part)
    return parts


def remember_outbound_echoes(state: dict[str, Any], sent_messages: list[str]) -> None:
    remembered = list(state.get("recent_outbound_echo_hashes") or [])
    seen = set(remembered)
    for message in sent_messages:
        hashed = echo_hash(message)
        if not hashed or hashed in seen:
            continue
        remembered.append(hashed)
        seen.add(hashed)
    state["recent_outbound_echo_hashes"] = remembered[-OUTBOUND_ECHO_HASH_LIMIT:]


def append_processed_message_id(state: dict[str, Any], processed_id_set: set[str], message_id: str) -> None:
    if message_id in processed_id_set:
        return
    processed_id_set.add(message_id)
    processed = list(state.get("processed_message_ids") or [])
    processed.append(message_id)
    state["processed_message_ids"] = processed[-PROCESSED_ID_LIMIT:]
    state["last_processed_message_id"] = message_id
    state["last_processed_at"] = now_iso()


def bootstrap_seen_messages(state: dict[str, Any], session_id: str, session_path: Path) -> bool:
    if state.get("bootstrapped_at"):
        return False
    seen_ids: list[str] = []
    with session_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                entry = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if entry.get("type") == "message":
                message_id = entry.get("id")
                if message_id:
                    seen_ids.append(message_id)
    state["session_id"] = session_id
    state["processed_message_ids"] = seen_ids[-PROCESSED_ID_LIMIT:]
    state["last_processed_message_id"] = seen_ids[-1] if seen_ids else None
    state["bootstrapped_at"] = now_iso()
    write_bridge_state(state)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Bridge WhatsApp operator replies into the deterministic review loop.")
    parser.add_argument("--dry-run", action="store_true", help="Print responses instead of sending them to WhatsApp.")
    parser.add_argument("--simulate-text", help="Process one operator reply directly without reading session logs.")
    args = parser.parse_args()

    if args.simulate_text:
        response = process_operator_text(args.simulate_text, dry_run=args.dry_run)
        if args.dry_run:
            print(response)
            return 0
        settings = notifier_settings()
        send_operator_response(settings, response)
        return 0

    state = load_bridge_state()
    session_id, session_path = resolve_whatsapp_session()
    if not session_id or session_path is None:
        return 0
    if bootstrap_seen_messages(state, session_id, session_path):
        return 0

    processed_id_set = set(state.get("processed_message_ids") or [])
    outbound_hashes = set(state.get("recent_outbound_echo_hashes") or [])
    new_messages, ignored_ids = iter_new_human_messages(
        session_path,
        processed_id_set,
        outbound_hashes,
        state.get("last_processed_message_id"),
    )
    state["session_id"] = session_id
    for ignored_id in ignored_ids:
        append_processed_message_id(state, processed_id_set, ignored_id)
        write_bridge_state(state)
    if not new_messages:
        write_bridge_state(state)
        return 0

    settings = notifier_settings()
    for message in new_messages:
        if (
            is_operator_echo_body(message["body_text"])
            or is_recent_outbound_echo(message["body_text"], outbound_hashes)
        ):
            append_processed_message_id(state, processed_id_set, message["id"])
            write_bridge_state(state)
            continue
        response = process_operator_text(message["body_text"], dry_run=args.dry_run)
        if args.dry_run:
            print(f"INBOUND :: {message['body_text']}")
            print("---")
            print(response)
            print("===")
        else:
            sent_messages = send_operator_response(settings, response)
            remember_outbound_echoes(state, sent_messages)
            outbound_hashes = set(state.get("recent_outbound_echo_hashes") or [])
        append_processed_message_id(state, processed_id_set, message["id"])
        write_bridge_state(state)

    write_bridge_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
