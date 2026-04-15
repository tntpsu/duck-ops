#!/usr/bin/env python3
"""
Shared pacing and block-detection guard for Etsy Playwright activity.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "state" / "etsy_browser_guard.json"

BLOCK_WINDOW_MINUTES = 45
RATE_WINDOW_SECONDS = 5 * 60
MAX_COMMANDS_PER_WINDOW = 18
MAX_MUTATING_COMMANDS_PER_WINDOW = 8
MIN_GAP_SECONDS = 1.25
MIN_MUTATING_GAP_SECONDS = 3.5

BLOCK_PHRASES = (
    "bot activity",
    "unusual activity",
    "suspicious activity",
    "verify you're a human",
    "verify you are a human",
    "access denied",
    "pardon the interruption",
    "temporarily blocked",
    "captcha",
)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone()
    except ValueError:
        return None


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"generated_at": None, "blocked_until": None, "block_reason": None, "events": []}
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("generated_at", None)
    payload.setdefault("blocked_until", None)
    payload.setdefault("block_reason", None)
    payload.setdefault("events", [])
    return payload


def save_state(payload: dict[str, Any]) -> None:
    payload["generated_at"] = now_iso()
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _is_mutating_command(args: tuple[str, ...]) -> bool:
    if not args:
        return False
    command = str(args[0] or "").strip().lower()
    if command in {"click", "fill", "type", "press"}:
        return True
    if command == "eval":
        script = " ".join(str(part or "") for part in args[1:]).lower()
        return any(token in script for token in (".click(", "location.assign", "window.location", ".submit("))
    return False


def _prune_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = datetime.now().astimezone() - timedelta(seconds=RATE_WINDOW_SECONDS)
    pruned: list[dict[str, Any]] = []
    for event in events:
        parsed = _parse_iso(event.get("at"))
        if parsed and parsed >= cutoff:
            pruned.append(event)
    return pruned[-100:]


def _detect_block_reason(output: str) -> str | None:
    lowered = str(output or "").lower()
    for phrase in BLOCK_PHRASES:
        if phrase in lowered:
            return phrase
    return None


def blocked_status() -> dict[str, Any]:
    state = load_state()
    blocked_until = _parse_iso(state.get("blocked_until"))
    now = datetime.now().astimezone()
    is_blocked = bool(blocked_until and blocked_until > now)
    return {
        "blocked": is_blocked,
        "blocked_until": blocked_until.isoformat() if blocked_until else None,
        "block_reason": state.get("block_reason"),
    }


def is_blocked() -> bool:
    return bool(blocked_status().get("blocked"))


def before_command(session: str, args: tuple[str, ...]) -> None:
    state = load_state()
    status = blocked_status()
    blocked_until = _parse_iso(status.get("blocked_until"))
    now = datetime.now().astimezone()
    if blocked_until and blocked_until > now:
        raise RuntimeError(
            f"Etsy automation is cooling down until {blocked_until.isoformat()} because: {state.get('block_reason') or 'unknown'}"
        )

    events = _prune_events(list(state.get("events") or []))
    mutating = _is_mutating_command(args)
    mutating_count = sum(1 for event in events if bool(event.get("mutating")))
    if len(events) >= MAX_COMMANDS_PER_WINDOW or (mutating and mutating_count >= MAX_MUTATING_COMMANDS_PER_WINDOW):
        cooldown_until = now + timedelta(minutes=15)
        state["blocked_until"] = cooldown_until.isoformat()
        state["block_reason"] = "rate_limit_preemptive_cooldown"
        state["events"] = events
        save_state(state)
        raise RuntimeError(
            f"Etsy automation hit the shared pacing budget and is cooling down until {cooldown_until.isoformat()}."
        )

    if events:
        last_at = _parse_iso(events[-1].get("at"))
        if last_at:
            gap = (now - last_at).total_seconds()
            required_gap = MIN_MUTATING_GAP_SECONDS if mutating else MIN_GAP_SECONDS
            if gap < required_gap:
                time.sleep(required_gap - gap)


def after_command(session: str, args: tuple[str, ...], output: str) -> None:
    state = load_state()
    events = _prune_events(list(state.get("events") or []))
    now = datetime.now().astimezone()
    mutating = _is_mutating_command(args)
    events.append(
        {
            "at": now.isoformat(),
            "session": session,
            "command": str(args[0] or "") if args else None,
            "mutating": mutating,
        }
    )
    state["events"] = events[-100:]

    block_reason = _detect_block_reason(output)
    if block_reason:
        blocked_until = now + timedelta(minutes=BLOCK_WINDOW_MINUTES)
        state["blocked_until"] = blocked_until.isoformat()
        state["block_reason"] = block_reason

    save_state(state)


def clear_block(reason: str = "manual_clear") -> None:
    state = load_state()
    state["blocked_until"] = None
    state["block_reason"] = reason
    save_state(state)


def detect_block_in_output(output: str) -> str | None:
    return _detect_block_reason(output)
