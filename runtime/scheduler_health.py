from __future__ import annotations

import argparse
import json
import os
import plistlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from governance_review_common import OUTPUT_OPERATOR_DIR, STATE_DIR, now_local_iso, write_json, write_markdown


DUCKAGENT_RUNTIME_ROOT = Path(os.getenv("DUCKAGENT_RUNTIME_ROOT", "/Users/philtullai/ai-agents/duckAgent_runtime"))
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
SCHEDULER_LOG_PATH = DUCKAGENT_RUNTIME_ROOT / "logs" / "duckagent_scheduler.log"
RECEIPT_DIR = DUCKAGENT_RUNTIME_ROOT / "state" / "scheduler_receipts"
SCHEDULER_HEALTH_STATE_PATH = STATE_DIR / "scheduler_health.json"
SCHEDULER_HEALTH_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "scheduler_health.json"
SCHEDULER_HEALTH_MD_PATH = OUTPUT_OPERATOR_DIR / "scheduler_health.md"

DEFAULT_TIMEOUT_SECONDS = 1800
DEFAULT_GRACE_SECONDS = 1800
JOB_TIMEOUT_SECONDS = {
    "competitor_daily": 5400,
    "weekly_sunday": 5400,
}
ATTENTION_STATUSES = {
    "missed_run",
    "failed",
    "timeout",
    "hung",
    "orphaned",
    "slow",
    "skipped_lock_active",
    "skipped_lock_unavailable",
    "unknown",
}
BAD_STATUSES = {"missed_run", "failed", "timeout", "hung", "orphaned"}
WARN_STATUSES = {
    "dependency_blocked_recent",
    "slow",
    "skipped_lock_active",
    "skipped_lock_unavailable",
    "running",
    "no_history",
    "unknown",
}
NON_ACTIONABLE_ATTENTION_STATUSES = {"dependency_blocked_recent"}
LOG_EVENT_RE = re.compile(
    r"^\[(?P<stamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?: [A-Z]{2,4})?\]\s+"
    r"(?P<event>START|END|TIMEOUT|BUDGET|SKIP|INTERRUPTED)\s+"
    r"(?P<job>[^\s]+)\s*(?:::)?\s*(?P<detail>.*)$"
)
LOG_STAMP_RE = re.compile(r"^\[(?P<stamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?: [A-Z]{2,4})?\]")


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


def _parse_log_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return parsed.astimezone()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _pid_alive(pid: Any) -> bool | None:
    pid_int = _safe_int(pid)
    if pid_int <= 0:
        return None
    try:
        os.kill(pid_int, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def _load_receipts(receipt_dir: Path = RECEIPT_DIR) -> dict[str, dict[str, Any]]:
    receipts: dict[str, dict[str, Any]] = {}
    if not receipt_dir.exists():
        return receipts
    for path in sorted(receipt_dir.glob("*.json")):
        payload = _load_json(path, {})
        if not isinstance(payload, dict):
            continue
        job_name = str(payload.get("job_name") or path.stem).strip()
        if not job_name:
            continue
        receipts[job_name] = {**payload, "receipt_path": str(path)}
    return receipts


def _load_scheduled_jobs(launch_agents_dir: Path = LAUNCH_AGENTS_DIR) -> dict[str, dict[str, Any]]:
    jobs: dict[str, dict[str, Any]] = {}
    if not launch_agents_dir.exists():
        return jobs
    for path in sorted(launch_agents_dir.glob("com.philtullai.duckagent.*.plist")):
        try:
            payload = plistlib.loads(path.read_bytes())
        except Exception:
            continue
        args = payload.get("ProgramArguments") if isinstance(payload, dict) else None
        if not isinstance(args, list) or len(args) < 2:
            continue
        runner = str(args[0])
        if not runner.endswith("run_scheduled_flow.sh"):
            continue
        job_name = str(args[1]).strip()
        if not job_name:
            continue
        schedule = payload.get("StartCalendarInterval") if isinstance(payload.get("StartCalendarInterval"), dict) else {}
        jobs[job_name] = {
            "job_name": job_name,
            "label": str(payload.get("Label") or path.stem),
            "plist_path": str(path),
            "schedule": dict(schedule),
            "command": " ".join(str(part) for part in args),
            "timeout_seconds": JOB_TIMEOUT_SECONDS.get(job_name, DEFAULT_TIMEOUT_SECONDS),
            "grace_seconds": DEFAULT_GRACE_SECONDS,
        }
    return jobs


def _parse_scheduler_log(log_path: Path = SCHEDULER_LOG_PATH, *, max_lines: int = 100000) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    events: list[dict[str, Any]] = []
    for line in lines[-max_lines:]:
        match = LOG_EVENT_RE.match(line.strip())
        if not match:
            continue
        parsed_at = _parse_log_timestamp(match.group("stamp"))
        if parsed_at is None:
            continue
        detail = (match.group("detail") or "").strip()
        event = match.group("event")
        exit_code = None
        timeout_seconds = None
        if event == "END":
            exit_match = re.search(r"exit=(?P<exit>-?\d+)", detail)
            if exit_match:
                exit_code = int(exit_match.group("exit"))
        elif event == "BUDGET":
            timeout_match = re.search(r"timeout=(?P<timeout>\d+)s", detail)
            if timeout_match:
                timeout_seconds = int(timeout_match.group("timeout"))
        events.append(
            {
                "at": parsed_at,
                "event": event,
                "job_name": match.group("job"),
                "detail": detail,
                "exit_code": exit_code,
                "timeout_seconds": timeout_seconds,
            }
        )
    return events


def _failure_dependency_from_log(
    *,
    log_path: Path,
    started_at: datetime | None,
    finished_at: datetime | None,
) -> dict[str, str] | None:
    if started_at is None or not log_path.exists():
        return None
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-2500:]
    except OSError:
        return None

    window_lines: list[str] = []
    for line in lines:
        parsed_at = None
        match = LOG_STAMP_RE.match(line.strip())
        if match:
            parsed_at = _parse_log_timestamp(match.group("stamp"))
        if parsed_at and parsed_at < started_at:
            continue
        if parsed_at and finished_at and parsed_at > finished_at:
            continue
        window_lines.append(line)

    text = "\n".join(window_lines).lower()
    if "photoroom" in text and ("exhausted the number of images" in text or "[402]" in text or "quota" in text):
        return {
            "dependency": "photoroom",
            "dependency_blocker": "photoroom_quota_exhausted",
            "failure_class": "dependency_blocked_recent",
            "summary": "Last run hit PhotoRoom image quota, not a scheduler failure.",
            "recommended_action": "No scheduler fix needed. Rerun the flow after the PhotoRoom quota resets or the plan is refreshed.",
        }
    return None


def _schedule_datetime(schedule: dict[str, Any], *, now: datetime, future: bool) -> datetime | None:
    if not schedule:
        return None
    hour = _safe_int(schedule.get("Hour"), 0)
    minute = _safe_int(schedule.get("Minute"), 0)
    weekday_raw = schedule.get("Weekday")
    if weekday_raw is None:
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if future:
            return candidate if candidate > now else candidate + timedelta(days=1)
        return candidate if candidate <= now else candidate - timedelta(days=1)

    weekday = _safe_int(weekday_raw, 0)
    python_weekday = 6 if weekday == 0 else max(0, min(6, weekday - 1))
    days_delta = python_weekday - now.weekday()
    candidate = (now + timedelta(days=days_delta)).replace(hour=hour, minute=minute, second=0, microsecond=0)
    if future:
        return candidate if candidate > now else candidate + timedelta(days=7)
    return candidate if candidate <= now else candidate - timedelta(days=7)


def _latest_event(events: list[dict[str, Any]], job_name: str, event_name: str) -> dict[str, Any] | None:
    matches = [item for item in events if item.get("job_name") == job_name and item.get("event") == event_name]
    if not matches:
        return None
    return max(matches, key=lambda item: item["at"])


def _events_after(events: list[dict[str, Any]], job_name: str, event_name: str, at: datetime) -> list[dict[str, Any]]:
    return [
        item
        for item in events
        if item.get("job_name") == job_name
        and item.get("event") == event_name
        and isinstance(item.get("at"), datetime)
        and item["at"] >= at
    ]


def _format_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "unknown duration"
    total = int(max(0, float(seconds)))
    minutes, sec = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def _evaluate_job(
    job: dict[str, Any],
    *,
    receipt: dict[str, Any] | None,
    events: list[dict[str, Any]],
    now: datetime,
    scheduler_log_path: Path,
) -> dict[str, Any]:
    job_name = str(job.get("job_name") or "").strip()
    timeout_seconds = _safe_int((receipt or {}).get("timeout_seconds"), _safe_int(job.get("timeout_seconds"), DEFAULT_TIMEOUT_SECONDS))
    grace_seconds = _safe_int(job.get("grace_seconds"), DEFAULT_GRACE_SECONDS)
    expected_at = _schedule_datetime(job.get("schedule") or {}, now=now, future=False)
    next_expected_at = _schedule_datetime(job.get("schedule") or {}, now=now, future=True)
    start_event = _latest_event(events, job_name, "START")
    end_event = _latest_event(events, job_name, "END")
    timeout_event = _latest_event(events, job_name, "TIMEOUT")

    receipt_started_at = _parse_iso((receipt or {}).get("started_at"))
    receipt_finished_at = _parse_iso((receipt or {}).get("finished_at"))
    started_at = receipt_started_at or (start_event or {}).get("at")
    if start_event and (started_at is None or start_event["at"] > started_at):
        started_at = start_event["at"]
    finished_at = receipt_finished_at or None
    if end_event and started_at and end_event["at"] >= started_at:
        if finished_at is None or end_event["at"] > finished_at:
            finished_at = end_event["at"]

    exit_code = (receipt or {}).get("exit_code")
    if exit_code is None and end_event and (not started_at or end_event["at"] >= started_at):
        exit_code = end_event.get("exit_code")
    exit_code = _safe_int(exit_code, 0) if exit_code is not None else None
    duration_seconds = None
    if started_at and finished_at:
        duration_seconds = round(max(0.0, (finished_at - started_at).total_seconds()), 1)

    receipt_status = str((receipt or {}).get("status") or "").strip()
    pid = (receipt or {}).get("child_pid") or (receipt or {}).get("pid")
    pid_alive = _pid_alive(pid)
    elapsed_seconds = round(max(0.0, (now - started_at).total_seconds()), 1) if started_at else None
    status = "healthy"
    severity = "ok"
    summary = "Last scheduled run finished successfully."
    recommended_action = "No action needed."

    latest_start_after_expected = bool(started_at and expected_at and started_at >= expected_at - timedelta(seconds=grace_seconds))
    expected_due = bool(expected_at and now >= expected_at + timedelta(seconds=grace_seconds))

    if receipt_status.startswith("skipped_lock"):
        status = receipt_status
        severity = "warn"
        summary = "The latest run skipped because another scheduler lock was present."
        recommended_action = "Inspect the active lock and receipt before manually rerunning this job."
    elif receipt_status == "running" and started_at and not finished_at:
        if elapsed_seconds is not None and elapsed_seconds > timeout_seconds:
            status = "hung" if pid_alive else "orphaned"
            severity = "bad"
            summary = f"Run started {_format_duration(elapsed_seconds)} ago and has not finished."
            recommended_action = "Inspect the PID, scheduler log, and flow output before rerunning."
        else:
            status = "running"
            severity = "warn"
            summary = f"Run is currently in progress for {_format_duration(elapsed_seconds)}."
            recommended_action = "Watch for the END receipt before taking action."
    elif started_at and not finished_at:
        if elapsed_seconds is not None and elapsed_seconds > timeout_seconds:
            status = "hung"
            severity = "bad"
            summary = f"Scheduler log has START but no END after {_format_duration(elapsed_seconds)}."
            recommended_action = "Inspect the process table and scheduler log; this may be a stuck or orphaned run."
        else:
            status = "running"
            severity = "warn"
            summary = f"Scheduler log has START but no END yet after {_format_duration(elapsed_seconds)}."
            recommended_action = "Wait for the configured timeout before intervening."
    elif timeout_event and started_at and timeout_event["at"] >= started_at and (not finished_at or timeout_event["at"] <= finished_at):
        status = "timeout"
        severity = "bad"
        summary = f"Run exceeded the {timeout_seconds}s scheduler budget."
        recommended_action = "Inspect the flow logs for the stalled external call before retrying."
    elif exit_code not in {None, 0}:
        dependency_failure = _failure_dependency_from_log(
            log_path=scheduler_log_path,
            started_at=started_at,
            finished_at=finished_at,
        )
        if dependency_failure:
            status = "dependency_blocked_recent"
            severity = "warn"
            summary = dependency_failure["summary"]
            recommended_action = dependency_failure["recommended_action"]
        else:
            status = "failed"
            severity = "bad"
            summary = f"Last run exited with code {exit_code}."
            recommended_action = "Open the scheduler log tail and fix the failing flow before the next scheduled window."
    elif duration_seconds is not None and duration_seconds > timeout_seconds:
        status = "slow"
        severity = "warn"
        summary = f"Last run completed, but took {_format_duration(duration_seconds)}."
        recommended_action = "Watch the next run and consider raising or splitting the timeout only if this is expected."
    elif expected_due and not latest_start_after_expected:
        status = "missed_run"
        severity = "bad"
        summary = "The expected schedule window passed without a matching START event."
        recommended_action = "Check launchd load state and run the job manually if the output is still needed."
    elif not started_at:
        status = "no_history"
        severity = "warn"
        summary = "No scheduler history has been observed yet."
        recommended_action = "Let the next scheduled window run or smoke-test the wrapper manually."

    return {
        "job_name": job_name,
        "label": job.get("label"),
        "status": status,
        "severity": severity,
        "attention_needed": status in ATTENTION_STATUSES and status not in NON_ACTIONABLE_ATTENTION_STATUSES,
        "summary": summary,
        "recommended_action": recommended_action,
        "last_start_at": started_at.isoformat() if started_at else None,
        "last_finished_at": finished_at.isoformat() if finished_at else None,
        "last_exit_code": exit_code,
        "duration_seconds": duration_seconds,
        "elapsed_seconds": elapsed_seconds if status in {"running", "hung", "orphaned"} else None,
        "timeout_seconds": timeout_seconds,
        "grace_seconds": grace_seconds,
        "expected_at": expected_at.isoformat() if expected_at else None,
        "next_expected_at": next_expected_at.isoformat() if next_expected_at else None,
        "pid": pid,
        "pid_alive": pid_alive,
        "receipt_path": (receipt or {}).get("receipt_path"),
        "log_path": str(scheduler_log_path),
        "plist_path": job.get("plist_path"),
        **(dependency_failure if "dependency_failure" in locals() and dependency_failure else {}),
    }


def _overall_status(items: list[dict[str, Any]]) -> str:
    statuses = {str(item.get("status") or "") for item in items}
    if statuses & BAD_STATUSES:
        return "bad"
    if statuses & WARN_STATUSES:
        return "warn"
    return "ok"


def _headline(status: str, counts: dict[str, int]) -> str:
    if status == "bad":
        return (
            f"Scheduler health needs attention: {counts.get('bad_count', 0)} bad job(s), "
            f"{counts.get('warn_count', 0)} warning job(s)."
        )
    if status == "warn":
        return f"Scheduler health has {counts.get('warn_count', 0)} warning job(s), but no bad jobs."
    return "Scheduler health is clean across tracked scheduled jobs."


def build_scheduler_health(
    *,
    now: datetime | None = None,
    launch_agents_dir: Path = LAUNCH_AGENTS_DIR,
    scheduler_log_path: Path = SCHEDULER_LOG_PATH,
    receipt_dir: Path = RECEIPT_DIR,
    write_outputs: bool = True,
) -> dict[str, Any]:
    current = now or datetime.now().astimezone()
    jobs = _load_scheduled_jobs(launch_agents_dir)
    receipts = _load_receipts(receipt_dir)
    events = _parse_scheduler_log(scheduler_log_path)
    for job_name in sorted({*receipts.keys(), *(event.get("job_name") for event in events if event.get("job_name"))}):
        jobs.setdefault(
            str(job_name),
            {
                "job_name": str(job_name),
                "label": str(job_name),
                "schedule": {},
                "timeout_seconds": JOB_TIMEOUT_SECONDS.get(str(job_name), DEFAULT_TIMEOUT_SECONDS),
                "grace_seconds": DEFAULT_GRACE_SECONDS,
            },
        )

    items = [
        _evaluate_job(job, receipt=receipts.get(job_name), events=events, now=current, scheduler_log_path=scheduler_log_path)
        for job_name, job in sorted(jobs.items())
    ]
    items.sort(
        key=lambda item: (
            {"bad": 0, "warn": 1, "ok": 2}.get(str(item.get("severity") or "ok"), 9),
            str(item.get("job_name") or ""),
        )
    )
    status = _overall_status(items)
    counts = {
        "tracked_jobs": len(items),
        "attention_count": sum(1 for item in items if item.get("attention_needed")),
        "bad_count": sum(1 for item in items if item.get("severity") == "bad"),
        "warn_count": sum(1 for item in items if item.get("severity") == "warn"),
        "healthy_count": sum(1 for item in items if item.get("status") == "healthy"),
        "missed_count": sum(1 for item in items if item.get("status") == "missed_run"),
        "failed_count": sum(1 for item in items if item.get("status") == "failed"),
        "timeout_count": sum(1 for item in items if item.get("status") == "timeout"),
        "hung_count": sum(1 for item in items if item.get("status") == "hung"),
        "orphaned_count": sum(1 for item in items if item.get("status") == "orphaned"),
        "slow_count": sum(1 for item in items if item.get("status") == "slow"),
        "running_count": sum(1 for item in items if item.get("status") == "running"),
    }
    payload = {
        "generated_at": current.isoformat(),
        "source": "duckagent_launchd_scheduler",
        "status": status,
        "headline": _headline(status, counts),
        "recommended_action": "Resolve the top scheduler item before trusting downstream workflow freshness." if status != "ok" else "No action needed.",
        "summary": counts,
        "items": items,
        "paths": {
            "scheduler_log": str(scheduler_log_path),
            "receipt_dir": str(receipt_dir),
            "launch_agents_dir": str(launch_agents_dir),
            "markdown": str(SCHEDULER_HEALTH_MD_PATH),
        },
    }
    if write_outputs:
        write_scheduler_health_outputs(payload)
    return payload


def render_scheduler_health_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    lines = [
        "# Scheduler Health",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Status: `{payload.get('status')}`",
        f"- Headline: {payload.get('headline')}",
        f"- Tracked jobs: `{summary.get('tracked_jobs', 0)}`",
        f"- Need attention: `{summary.get('attention_count', 0)}`",
        f"- Bad: `{summary.get('bad_count', 0)}`",
        f"- Warn: `{summary.get('warn_count', 0)}`",
        f"- Healthy: `{summary.get('healthy_count', 0)}`",
        "",
        "## Attention",
        "",
    ]
    attention_items = [item for item in list(payload.get("items") or []) if item.get("attention_needed")]
    if not attention_items:
        lines.append("No scheduler jobs need attention.")
    else:
        for item in attention_items[:10]:
            lines.append(f"- `{item.get('job_name')}` | `{item.get('status')}` | {item.get('summary')}")
            if item.get("last_start_at"):
                lines.append(f"  Last start: `{item.get('last_start_at')}`")
            if item.get("last_finished_at"):
                lines.append(f"  Last finish: `{item.get('last_finished_at')}`")
            if item.get("expected_at"):
                lines.append(f"  Expected: `{item.get('expected_at')}`")
            if item.get("recommended_action"):
                lines.append(f"  Next: {item.get('recommended_action')}")
    lines.extend(["", "## Tracked Jobs", ""])
    for item in list(payload.get("items") or []):
        lines.append(
            f"- `{item.get('job_name')}` | `{item.get('status')}` | "
            f"last start `{item.get('last_start_at') or 'never'}` | next `{item.get('next_expected_at') or 'unknown'}`"
        )
    return "\n".join(lines)


def write_scheduler_health_outputs(payload: dict[str, Any]) -> None:
    markdown = render_scheduler_health_markdown(payload)
    write_json(SCHEDULER_HEALTH_STATE_PATH, payload)
    write_json(SCHEDULER_HEALTH_OPERATOR_JSON_PATH, payload)
    write_markdown(SCHEDULER_HEALTH_MD_PATH, markdown + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build DuckAgent scheduler health.")
    parser.add_argument("--no-write", action="store_true", help="Print JSON without updating state/operator files.")
    args = parser.parse_args()
    payload = build_scheduler_health(write_outputs=not args.no_write)
    print(json.dumps(payload, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
