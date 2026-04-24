from __future__ import annotations

import argparse
import json
import random
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import customer_inbox_refresh
import etsy_expired_relist
from etsy_browser_guard import blocked_status as etsy_browser_blocked_status
from governance_review_common import OUTPUT_OPERATOR_DIR, STATE_DIR, now_local_iso, write_json, write_markdown
from review_reply_executor import (
    auto_enqueue_publish_ready,
    choose_session,
    drain_queue,
    load_execution_policy,
    run_pw_command,
)
from workflow_control import record_workflow_transition


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "etsy_browser_batch.json"
SCHEDULE_STATE_PATH = STATE_DIR / "etsy_browser_schedule.json"
LATEST_STATE_PATH = STATE_DIR / "etsy_browser_batch_latest.json"
HISTORY_PATH = STATE_DIR / "etsy_browser_batch_history.jsonl"
RECOVERY_STATE_PATH = STATE_DIR / "etsy_recovery_window.json"
SCHEDULE_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "etsy_browser_schedule.json"
SCHEDULE_OPERATOR_MD_PATH = OUTPUT_OPERATOR_DIR / "etsy_browser_schedule.md"
BATCH_RUNNER_PATH = ROOT.parent / "openclaw_runtime" / "run_duck_ops_etsy_browser_batch.sh"

WORKFLOW_ID = "etsy_browser_batch"
WORKFLOW_LANE = "etsy_browser_batch"

DEFAULT_CONFIG: dict[str, Any] = {
    "enabled": True,
    "checker_interval_minutes": 15,
    "due_grace_minutes": 20,
    "session_timeout_seconds": 720,
    "windows": [
        {"slot_id": "morning", "label": "Morning", "start_hour": 9, "start_minute": 0, "end_hour": 10, "end_minute": 30},
        {"slot_id": "afternoon", "label": "Afternoon", "start_hour": 13, "start_minute": 30, "end_hour": 15, "end_minute": 30},
        {"slot_id": "evening", "label": "Evening", "start_hour": 18, "start_minute": 30, "end_hour": 20, "end_minute": 30},
    ],
    "customer_read": {"enabled": True, "max_threads_per_session": 2},
    "review_reply": {"enabled": True, "queue_publish_ready": True, "max_replies_per_session": 2},
    "relist": {"enabled": True, "max_renewals_per_day": 1, "max_per_session": 1, "force_sales_refresh": False},
}


def _local_now() -> datetime:
    return datetime.now().astimezone()


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _parse_iso(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.astimezone()
    return parsed.astimezone()


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def load_config() -> dict[str, Any]:
    payload = _load_json(CONFIG_PATH, {})
    if not isinstance(payload, dict):
        payload = {}
    return _deep_merge(DEFAULT_CONFIG, payload)


def _recovery_pause(now: datetime | None = None) -> dict[str, Any]:
    now = now or _local_now()
    payload = _load_json(RECOVERY_STATE_PATH, {})
    if not isinstance(payload, dict):
        return {"blocked": False}
    quiet_until = _parse_iso(payload.get("quiet_until"))
    status = str(payload.get("status") or "").strip()
    if quiet_until and quiet_until > now and status == "paused_for_manual_etsy_recovery":
        return {
            "blocked": True,
            "reason": "manual_recovery_window",
            "blocked_until": quiet_until.isoformat(),
            "payload": payload,
        }
    return {"blocked": False}


def _window_bounds(now: datetime, window: dict[str, Any]) -> tuple[datetime, datetime]:
    start = now.replace(
        hour=int(window.get("start_hour", 0)),
        minute=int(window.get("start_minute", 0)),
        second=0,
        microsecond=0,
    )
    end = now.replace(
        hour=int(window.get("end_hour", 0)),
        minute=int(window.get("end_minute", 0)),
        second=0,
        microsecond=0,
    )
    return start, end


def _choose_window_time(start: datetime, end: datetime, rng: random.Random) -> datetime:
    total_minutes = max(0, int((end - start).total_seconds() // 60))
    if total_minutes <= 0:
        return start
    return start + timedelta(minutes=rng.randint(0, total_minutes))


def build_daily_schedule(
    *,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    config = config or load_config()
    now = now or _local_now()
    rng = rng or random.SystemRandom()

    windows = list(config.get("windows") or [])
    slots: list[dict[str, Any]] = []
    for window in windows:
        if not isinstance(window, dict):
            continue
        start, end = _window_bounds(now, window)
        scheduled_for = _choose_window_time(start, end, rng)
        slots.append(
            {
                "slot_id": str(window.get("slot_id") or f"slot_{len(slots) + 1}"),
                "label": str(window.get("label") or str(window.get("slot_id") or "Slot")),
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "scheduled_for": scheduled_for.isoformat(),
                "status": "pending",
                "relist_slot": False,
            }
        )

    relist_slot_id = None
    relist_cfg = config.get("relist") or {}
    if bool(relist_cfg.get("enabled")) and slots:
        chosen = rng.choice(slots)
        chosen["relist_slot"] = True
        relist_slot_id = str(chosen.get("slot_id") or "")

    return {
        "generated_at": now_local_iso(),
        "date_local": now.date().isoformat(),
        "timezone": str(now.tzinfo or ""),
        "enabled": bool(config.get("enabled", True)),
        "checker_interval_minutes": int(config.get("checker_interval_minutes") or 15),
        "due_grace_minutes": int(config.get("due_grace_minutes") or 20),
        "relist_slot_id": relist_slot_id,
        "slots": slots,
    }


def _load_schedule() -> dict[str, Any]:
    payload = _load_json(SCHEDULE_STATE_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _load_latest() -> dict[str, Any]:
    payload = _load_json(LATEST_STATE_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _save_schedule(schedule: dict[str, Any]) -> None:
    write_json(SCHEDULE_STATE_PATH, schedule)
    write_json(SCHEDULE_OPERATOR_JSON_PATH, schedule)
    write_markdown(SCHEDULE_OPERATOR_MD_PATH, render_schedule_markdown(schedule, _load_latest()))


def _save_latest(payload: dict[str, Any], schedule: dict[str, Any]) -> None:
    write_json(LATEST_STATE_PATH, payload)
    _append_jsonl(HISTORY_PATH, payload)
    write_markdown(SCHEDULE_OPERATOR_MD_PATH, render_schedule_markdown(schedule, payload))


def render_schedule_markdown(schedule: dict[str, Any], latest: dict[str, Any] | None = None) -> str:
    latest = latest or {}
    lines = [
        "# Etsy Browser Schedule",
        "",
        f"- Generated at: `{schedule.get('generated_at')}`",
        f"- Date: `{schedule.get('date_local')}`",
        f"- Checker cadence: every `{schedule.get('checker_interval_minutes')}` minutes",
        f"- Due grace: `{schedule.get('due_grace_minutes')}` minutes",
        f"- Relist slot: `{schedule.get('relist_slot_id') or 'none'}`",
        "",
        "## Today's Slots",
        "",
    ]

    slots = list(schedule.get("slots") or [])
    if not slots:
        lines.append("No Etsy browser slots are planned today.")
    else:
        for slot in slots:
            relist_suffix = " | relist" if slot.get("relist_slot") else ""
            lines.append(
                f"- `{slot.get('slot_id')}` ({slot.get('label')}) | scheduled `{slot.get('scheduled_for')}` | "
                f"window `{slot.get('window_start')}` -> `{slot.get('window_end')}` | "
                f"status `{slot.get('status')}`{relist_suffix}"
            )

    if latest:
        lines.extend(
            [
                "",
                "## Latest Batch",
                "",
                f"- Started: `{latest.get('started_at')}`",
                f"- Slot: `{latest.get('slot_id')}`",
                f"- Status: `{latest.get('status')}`",
            ]
        )
        customer = latest.get("customer_read") if isinstance(latest.get("customer_read"), dict) else {}
        review = latest.get("review_reply") if isinstance(latest.get("review_reply"), dict) else {}
        relist = latest.get("relist") if isinstance(latest.get("relist"), dict) else {}
        lines.extend(
            [
                f"- Customer read: `{customer.get('status') or 'unknown'}`",
                f"- Review reply: `{review.get('status') or 'unknown'}`",
                f"- Relist: `{relist.get('status') or 'unknown'}`",
            ]
        )
    return "\n".join(lines) + "\n"


def plan_schedule(
    *,
    force: bool = False,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    config = config or load_config()
    now = now or _local_now()
    existing = _load_schedule()
    if (
        not force
        and existing
        and str(existing.get("date_local") or "") == now.date().isoformat()
        and isinstance(existing.get("slots"), list)
    ):
        _save_schedule(existing)
        return {"ok": True, "status": "reused", "schedule": existing}

    schedule = build_daily_schedule(config=config, now=now, rng=rng)
    _save_schedule(schedule)
    return {"ok": True, "status": "planned", "schedule": schedule}


def ensure_today_schedule(*, config: dict[str, Any] | None = None, now: datetime | None = None) -> dict[str, Any]:
    now = now or _local_now()
    current = _load_schedule()
    if current and str(current.get("date_local") or "") == now.date().isoformat():
        return current
    result = plan_schedule(config=config, now=now)
    return result["schedule"]


def _mark_missed_slots(schedule: dict[str, Any], now: datetime) -> bool:
    grace_minutes = int(schedule.get("due_grace_minutes") or 20)
    changed = False
    for slot in list(schedule.get("slots") or []):
        if str(slot.get("status") or "") != "pending":
            continue
        window_end = _parse_iso(slot.get("window_end"))
        if window_end is None:
            continue
        if now > window_end + timedelta(minutes=grace_minutes):
            slot["status"] = "missed"
            slot["missed_at"] = now.isoformat()
            changed = True
    return changed


def find_due_slot(schedule: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any] | None:
    now = now or _local_now()
    if _mark_missed_slots(schedule, now):
        _save_schedule(schedule)
    grace_minutes = int(schedule.get("due_grace_minutes") or 20)
    due: list[dict[str, Any]] = []
    for slot in list(schedule.get("slots") or []):
        if str(slot.get("status") or "") != "pending":
            continue
        scheduled_for = _parse_iso(slot.get("scheduled_for"))
        window_end = _parse_iso(slot.get("window_end"))
        if scheduled_for is None or window_end is None:
            continue
        if scheduled_for <= now <= window_end + timedelta(minutes=grace_minutes):
            due.append(slot)
    if not due:
        return None
    return min(due, key=lambda item: str(item.get("scheduled_for") or ""))


def check_and_run(
    *,
    batch_runner: Path | None = None,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    config = config or load_config()
    now = now or _local_now()
    if not bool(config.get("enabled", True)):
        return {"ok": True, "status": "disabled", "message": "Etsy browser batching is disabled."}

    recovery = _recovery_pause(now)
    if recovery.get("blocked"):
        return {
            "ok": True,
            "status": "blocked",
            "reason": recovery.get("reason"),
            "blocked_until": recovery.get("blocked_until"),
        }
    guard = etsy_browser_blocked_status()
    if guard.get("blocked"):
        return {
            "ok": True,
            "status": "blocked",
            "reason": guard.get("block_reason"),
            "blocked_until": guard.get("blocked_until"),
        }

    schedule = ensure_today_schedule(config=config, now=now)
    due_slot = find_due_slot(schedule, now=now)
    if not due_slot:
        return {"ok": True, "status": "idle", "message": "No Etsy browser slot is due right now."}

    runner_path = batch_runner or BATCH_RUNNER_PATH
    if not Path(runner_path).exists():
        return {"ok": False, "status": "missing_runner", "message": f"Batch runner not found: {runner_path}"}

    result = subprocess.run(
        [str(runner_path), "duck_ops_etsy_browser_batch", "--slot-id", str(due_slot.get("slot_id") or "")],
        cwd=str(ROOT),
        check=False,
        text=True,
    )
    return {
        "ok": result.returncode == 0,
        "status": "launched" if result.returncode == 0 else "runner_failed",
        "slot_id": due_slot.get("slot_id"),
        "exit_code": result.returncode,
    }


def _slot_by_id(schedule: dict[str, Any], slot_id: str) -> dict[str, Any] | None:
    for slot in list(schedule.get("slots") or []):
        if str(slot.get("slot_id") or "") == slot_id:
            return slot
    return None


def _review_reply_policy_override(config: dict[str, Any]) -> dict[str, Any]:
    review_cfg = config.get("review_reply") or {}
    policy = load_execution_policy()
    policy["auto_execution_enabled"] = True
    policy["auto_drain_enabled"] = True
    policy["auto_drain_max_submits_per_run"] = int(review_cfg.get("max_replies_per_session") or 2)
    policy["auto_queue_publish_ready_positive"] = bool(review_cfg.get("queue_publish_ready", True))
    return policy


def _run_review_reply_batch(config: dict[str, Any]) -> dict[str, Any]:
    review_cfg = config.get("review_reply") or {}
    if not bool(review_cfg.get("enabled", True)):
        return {"status": "disabled", "queued": [], "drain": {"status": "disabled", "results": []}}
    policy_override = _review_reply_policy_override(config)
    queued = auto_enqueue_publish_ready(queued_by="etsy_browser_batch", policy_override=policy_override)
    drained = drain_queue(
        max_items=int(review_cfg.get("max_replies_per_session") or 2),
        keep_browser_open=True,
        policy_override=policy_override,
    )
    return {
        "status": str(drained.get("status") or queued.get("status") or "idle"),
        "queued": queued,
        "drain": drained,
    }


def _run_relist_batch(config: dict[str, Any]) -> dict[str, Any]:
    relist_cfg = config.get("relist") or {}
    if not bool(relist_cfg.get("enabled", True)):
        return {"status": "disabled", "results": []}

    daily_limit = int(relist_cfg.get("max_renewals_per_day") or 1)
    limit = max(1, int(relist_cfg.get("max_per_session") or 1))
    payload = etsy_expired_relist.refresh_payload(
        daily_limit=daily_limit,
        force_sales_refresh=bool(relist_cfg.get("force_sales_refresh", False)),
    )
    if int(payload.get("remaining_today") or 0) <= 0:
        return {
            "status": "idle",
            "reason": "daily_limit_reached",
            "results": [],
            "remaining_today": payload.get("remaining_today"),
        }

    targets = list(payload.get("eligible_items") or [])[:limit]
    if not targets:
        return {
            "status": "idle",
            "reason": "no_eligible_items",
            "results": [],
            "remaining_today": payload.get("remaining_today"),
        }

    results: list[dict[str, Any]] = []
    renewed_count = 0
    for item in targets:
        result = etsy_expired_relist.execute_relist(item)
        if str(result.get("status") or "") == "renewed":
            renewed_count += 1
            etsy_expired_relist.append_jsonl(
                etsy_expired_relist.RELIST_HISTORY_PATH,
                {
                    "at": now_local_iso(),
                    "date_local": _local_now().date().isoformat(),
                    "listing_id": item.get("listing_id"),
                    "title": item.get("title"),
                    "sales_count": item.get("sales_count"),
                    "status": "renewed",
                },
            )
        results.append(result)

    payload = etsy_expired_relist.refresh_payload(daily_limit=daily_limit, force_sales_refresh=False)
    payload["last_run"] = {"mode": "batch_renew", "at": now_local_iso(), "results": results}
    etsy_expired_relist.write_outputs(payload)
    etsy_expired_relist.sync_control(payload, results=results)
    return {
        "status": "renewed" if renewed_count else ("failed" if results else "idle"),
        "results": results,
        "renewed_count": renewed_count,
        "remaining_today": payload.get("remaining_today"),
    }


def _close_primary_browser_session() -> dict[str, Any]:
    session_name, _ = choose_session()
    try:
        run_pw_command(session_name, "close")
        return {"session_name": session_name, "closed": True}
    except subprocess.CalledProcessError as exc:
        return {"session_name": session_name, "closed": False, "error": exc.stderr.strip() or exc.stdout.strip()}


def _overall_status(customer: dict[str, Any], review: dict[str, Any], relist: dict[str, Any]) -> str:
    statuses = [str(customer.get("status") or ""), str(review.get("status") or ""), str(relist.get("status") or "")]
    if any(status in {"failed", "error"} for status in statuses):
        return "failed"
    if any(status == "blocked" for status in statuses):
        return "blocked"
    return "completed"


def run_slot(
    *,
    slot_id: str | None = None,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    config = config or load_config()
    now = now or _local_now()
    schedule = ensure_today_schedule(config=config, now=now)

    recovery = _recovery_pause(now)
    if recovery.get("blocked"):
        return {
            "ok": True,
            "status": "blocked",
            "reason": recovery.get("reason"),
            "blocked_until": recovery.get("blocked_until"),
        }

    blocked = etsy_browser_blocked_status()
    if blocked.get("blocked"):
        return {
            "ok": True,
            "status": "blocked",
            "reason": blocked.get("block_reason"),
            "blocked_until": blocked.get("blocked_until"),
        }

    target = _slot_by_id(schedule, slot_id) if slot_id else find_due_slot(schedule, now=now)
    if target is None:
        return {"ok": True, "status": "idle", "message": "No due Etsy browser slot is available."}

    scheduled_for = _parse_iso(target.get("scheduled_for"))
    if scheduled_for is not None and now < scheduled_for:
        return {"ok": True, "status": "not_due", "slot_id": target.get("slot_id"), "scheduled_for": target.get("scheduled_for")}
    if str(target.get("status") or "") not in {"pending", "running"}:
        return {"ok": True, "status": "already_handled", "slot_id": target.get("slot_id"), "slot_status": target.get("status")}

    target["status"] = "running"
    target["started_at"] = now.isoformat()
    _save_schedule(schedule)

    customer_cfg = config.get("customer_read") or {}
    if bool(customer_cfg.get("enabled", True)):
        customer_result = customer_inbox_refresh.run_refresh(
            limit=int(customer_cfg.get("max_threads_per_session") or 2),
            include_waiting=False,
        )
    else:
        customer_result = {"status": "disabled", "attempted": 0, "refreshed": 0, "failed": 0}

    review_result = _run_review_reply_batch(config)
    if str(target.get("slot_id") or "") == str(schedule.get("relist_slot_id") or ""):
        relist_result = _run_relist_batch(config)
    else:
        relist_result = {"status": "idle", "reason": "not_relist_slot", "results": []}

    cleanup_result = _close_primary_browser_session()
    overall_status = _overall_status(customer_result, review_result, relist_result)
    finished_at = now_local_iso()

    receipt = {
        "generated_at": finished_at,
        "date_local": schedule.get("date_local"),
        "slot_id": target.get("slot_id"),
        "scheduled_for": target.get("scheduled_for"),
        "started_at": target.get("started_at"),
        "finished_at": finished_at,
        "status": overall_status,
        "customer_read": customer_result,
        "review_reply": review_result,
        "relist": relist_result,
        "cleanup": cleanup_result,
    }

    target["status"] = overall_status
    target["finished_at"] = finished_at
    target["receipt_status"] = overall_status
    _save_schedule(schedule)
    _save_latest(receipt, schedule)

    state = {
        "completed": "verified",
        "blocked": "blocked",
        "failed": "failed",
    }.get(overall_status, "observed")
    state_reason = {
        "completed": "browser_batch_completed",
        "blocked": "browser_batch_blocked",
        "failed": "browser_batch_failed",
    }.get(overall_status, "browser_batch_observed")
    record_workflow_transition(
        workflow_id=WORKFLOW_ID,
        lane=WORKFLOW_LANE,
        display_label="Etsy Browser Batch",
        entity_id=str(target.get("slot_id") or "slot"),
        state=state,
        state_reason=state_reason,
        next_action="Wait for the next planned Etsy browser window." if overall_status == "completed" else "Inspect the Etsy browser batch receipt before retrying.",
        last_verification={
            "customer_refreshed": int(customer_result.get("refreshed") or 0),
            "review_posted": int(((review_result.get("drain") or {}).get("posted_count") or 0)),
            "relisted": int(relist_result.get("renewed_count") or 0),
        },
        metadata={"slot_id": target.get("slot_id"), "scheduled_for": target.get("scheduled_for")},
        receipt_kind="etsy_browser_batch",
        receipt_payload=receipt,
        history_summary="etsy browser batch",
    )
    return {"ok": overall_status != "failed", **receipt}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plan and run the Etsy browser batch schedule.")
    sub = parser.add_subparsers(dest="command", required=True)

    plan_parser = sub.add_parser("plan", help="Plan today's three Etsy browser slots.")
    plan_parser.add_argument("--force", action="store_true")

    status_parser = sub.add_parser("status", help="Show the current Etsy browser schedule state.")
    status_parser.add_argument("--json", action="store_true")

    check_parser = sub.add_parser("check-and-run", help="Launch the Etsy batch runner when a planned slot is due.")
    check_parser.add_argument("--batch-runner", default=str(BATCH_RUNNER_PATH))

    run_parser = sub.add_parser("run-slot", help="Run the due Etsy browser slot or a specific slot id.")
    run_parser.add_argument("--slot-id")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "plan":
        result = plan_schedule(force=bool(args.force))
    elif args.command == "status":
        payload = ensure_today_schedule()
        result = {"ok": True, "status": "loaded", "schedule": payload, "latest": _load_latest()}
        if not args.json:
            print(render_schedule_markdown(payload, result["latest"]).rstrip())
            return 0
    elif args.command == "check-and-run":
        result = check_and_run(batch_runner=Path(args.batch_runner))
    elif args.command == "run-slot":
        result = run_slot(slot_id=args.slot_id)
    else:
        raise SystemExit(f"Unknown command: {args.command}")

    print(json.dumps(result, indent=2))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
