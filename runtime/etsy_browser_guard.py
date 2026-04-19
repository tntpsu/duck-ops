#!/usr/bin/env python3
"""
Shared pacing and block-detection guard for Etsy Playwright activity.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "state" / "etsy_browser_guard.json"
DISCOVERY_SESSION_STATE_PATH = ROOT / "state" / "review_reply_discovery_sessions.json"

BLOCK_WINDOW_MINUTES = 45
RATE_WINDOW_SECONDS = 5 * 60
MAX_COMMANDS_PER_WINDOW = 18
MAX_MUTATING_COMMANDS_PER_WINDOW = 8
MIN_GAP_SECONDS = 1.25
MIN_MUTATING_GAP_SECONDS = 3.5
PLAYWRIGHT_STALE_AFTER_SECONDS = 2 * 60 * 60
PLAYWRIGHT_CLEANUP_MIN_INTERVAL_SECONDS = 5 * 60
PLAYWRIGHT_CLEANUP_HISTORY_LIMIT = 20

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

PROCESS_LINE_RE = re.compile(
    r"^\s*(?P<pid>\d+)\s+(?P<ppid>\d+)\s+(?P<pgid>\d+)\s+(?P<etime>\S+)\s+(?P<command>.+)$"
)
USER_DATA_DIR_RE = re.compile(r"--user-data-dir=(?P<path>\S+)")


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
        return {
            "generated_at": None,
            "blocked_until": None,
            "block_reason": None,
            "events": [],
            "last_cleanup_at": None,
            "last_cleanup_summary": None,
            "cleanup_events": [],
        }
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
    payload.setdefault("last_cleanup_at", None)
    payload.setdefault("last_cleanup_summary", None)
    payload.setdefault("cleanup_events", [])
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


def _parse_etime_seconds(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    days = 0
    if "-" in text:
        day_text, text = text.split("-", 1)
        if not day_text.isdigit():
            return None
        days = int(day_text)
    parts = text.split(":")
    if not all(part.isdigit() for part in parts):
        return None
    if len(parts) == 2:
        hours = 0
        minutes, seconds = (int(parts[0]), int(parts[1]))
    elif len(parts) == 3:
        hours, minutes, seconds = (int(parts[0]), int(parts[1]), int(parts[2]))
    else:
        return None
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _extract_user_data_dir(command: str) -> str | None:
    match = USER_DATA_DIR_RE.search(str(command or ""))
    if not match:
        return None
    return match.group("path").strip()


def _is_playwright_temp_profile(path: str | None) -> bool:
    if not path:
        return False
    return Path(path).name.startswith("playwright_chromiumdev_profile-")


def _matches_playwright_process(command: str) -> bool:
    lowered = str(command or "").lower()
    return (
        "playwright-core/lib/entry/clidaemon.js" in lowered
        or "playwright_chromiumdev_profile" in lowered
        or "--remote-debugging-pipe" in lowered
    )


def _list_playwright_processes() -> list[dict[str, Any]]:
    result = subprocess.run(
        ["ps", "-axo", "pid,ppid,pgid,etime,command"],
        capture_output=True,
        text=True,
        check=True,
    )
    rows: list[dict[str, Any]] = []
    for raw_line in (result.stdout or "").splitlines():
        match = PROCESS_LINE_RE.match(raw_line)
        if not match:
            continue
        command = match.group("command").strip()
        if not _matches_playwright_process(command):
            continue
        rows.append(
            {
                "pid": int(match.group("pid")),
                "ppid": int(match.group("ppid")),
                "pgid": int(match.group("pgid")),
                "etime": match.group("etime"),
                "age_seconds": _parse_etime_seconds(match.group("etime")) or 0,
                "command": command,
                "user_data_dir": _extract_user_data_dir(command),
            }
        )
    return rows


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _active_discovery_keepalive_groups() -> tuple[set[int], set[int]]:
    payload = _load_json(DISCOVERY_SESSION_STATE_PATH, {"sessions": {}})
    sessions = payload.get("sessions") if isinstance(payload.get("sessions"), dict) else {}
    now = datetime.now().astimezone()
    exempt_pgids: set[int] = set()
    exempt_pids: set[int] = set()
    for item in sessions.values():
        if not isinstance(item, dict):
            continue
        keepalive_until = _parse_iso(item.get("keepalive_until"))
        if keepalive_until is None or keepalive_until <= now:
            continue
        try:
            pid = int(item.get("pid") or 0)
        except Exception:
            pid = 0
        try:
            pgid = int(item.get("process_group_id") or 0)
        except Exception:
            pgid = 0
        if pid > 0:
            exempt_pids.add(pid)
        if pgid > 0:
            exempt_pgids.add(pgid)
    return exempt_pgids, exempt_pids


def _mark_discovery_sessions_cleaned(*, cleaned_pgids: set[int], cleaned_pids: set[int], reason: str) -> None:
    payload = _load_json(DISCOVERY_SESSION_STATE_PATH, {"sessions": {}})
    sessions = payload.get("sessions") if isinstance(payload.get("sessions"), dict) else {}
    changed = False
    cleaned_at = now_iso()
    for item in sessions.values():
        if not isinstance(item, dict):
            continue
        try:
            pid = int(item.get("pid") or 0)
        except Exception:
            pid = 0
        try:
            pgid = int(item.get("process_group_id") or 0)
        except Exception:
            pgid = 0
        if (pid and pid in cleaned_pids) or (pgid and pgid in cleaned_pgids):
            item["ready"] = False
            item["already_open"] = False
            item["keepalive_until"] = None
            item["cleanup_status"] = "stale_process_cleaned"
            item["cleaned_at"] = cleaned_at
            item["cleanup_reason"] = reason
            changed = True
    if changed:
        _save_json(DISCOVERY_SESSION_STATE_PATH, payload)


def cleanup_stale_playwright_processes(
    *,
    stale_after_seconds: int = PLAYWRIGHT_STALE_AFTER_SECONDS,
    force: bool = False,
    reason: str = "before_command",
    respect_keepalive: bool = True,
) -> dict[str, Any]:
    state = load_state()
    now = datetime.now().astimezone()
    last_cleanup = _parse_iso(state.get("last_cleanup_at"))
    if (
        not force
        and last_cleanup is not None
        and (now - last_cleanup).total_seconds() < PLAYWRIGHT_CLEANUP_MIN_INTERVAL_SECONDS
    ):
        return {
            "ok": True,
            "status": "skipped_recent_cleanup",
            "reason": reason,
            "stale_after_seconds": stale_after_seconds,
            "groups_seen": 0,
            "stale_group_count": 0,
            "killed_group_count": 0,
            "killed_pids": [],
            "removed_profile_paths": [],
        }

    rows = _list_playwright_processes()
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row.get("pgid") or 0), []).append(row)

    exempt_pgids: set[int] = set()
    exempt_pids: set[int] = set()
    if respect_keepalive:
        exempt_pgids, exempt_pids = _active_discovery_keepalive_groups()
    stale_groups: dict[int, list[dict[str, Any]]] = {}
    skipped_keepalive_groups: list[int] = []
    for pgid, group in grouped.items():
        if pgid <= 0:
            continue
        leader = next((item for item in group if int(item.get("pid") or 0) == pgid), group[0])
        if pgid in exempt_pgids or int(leader.get("pid") or 0) in exempt_pids:
            skipped_keepalive_groups.append(pgid)
            continue
        has_temp_profile = any(_is_playwright_temp_profile(str(item.get("user_data_dir") or "")) for item in group)
        has_daemon = any("cliDaemon.js".lower() in str(item.get("command") or "").lower() for item in group)
        oldest_age_seconds = max(int(item.get("age_seconds") or 0) for item in group)
        if oldest_age_seconds >= stale_after_seconds and (has_temp_profile or has_daemon):
            stale_groups[pgid] = group

    killed_pgids: set[int] = set()
    killed_pids: set[int] = set()
    removed_profile_paths: list[str] = []
    for pgid, group in stale_groups.items():
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            continue
        time.sleep(0.5)
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            continue
        killed_pgids.add(pgid)
        for item in group:
            killed_pids.add(int(item.get("pid") or 0))
            profile_path = str(item.get("user_data_dir") or "").strip()
            if _is_playwright_temp_profile(profile_path) and profile_path not in removed_profile_paths:
                shutil.rmtree(profile_path, ignore_errors=True)
                removed_profile_paths.append(profile_path)

    if killed_pgids or killed_pids:
        _mark_discovery_sessions_cleaned(cleaned_pgids=killed_pgids, cleaned_pids=killed_pids, reason=reason)

    summary = {
        "ok": True,
        "status": "completed",
        "reason": reason,
        "stale_after_seconds": stale_after_seconds,
        "respect_keepalive": respect_keepalive,
        "groups_seen": len(grouped),
        "skipped_keepalive_groups": skipped_keepalive_groups,
        "stale_group_count": len(stale_groups),
        "killed_group_count": len(killed_pgids),
        "killed_pids": sorted(pid for pid in killed_pids if pid > 0),
        "removed_profile_paths": removed_profile_paths,
        "observed_at": now_iso(),
    }
    state["last_cleanup_at"] = summary["observed_at"]
    state["last_cleanup_summary"] = summary
    cleanup_events = list(state.get("cleanup_events") or [])
    cleanup_events.append(summary)
    state["cleanup_events"] = cleanup_events[-PLAYWRIGHT_CLEANUP_HISTORY_LIMIT:]
    save_state(state)
    return summary


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
    cleanup_stale_playwright_processes(reason="before_command")
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
