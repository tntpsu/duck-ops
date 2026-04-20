from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
from typing import Any

from governance_review_common import (
    DUCK_AGENT_ROOT,
    DUCK_OPS_ROOT,
    OUTPUT_OPERATOR_DIR,
    STATE_DIR,
    age_hours,
    load_json,
    now_local_iso,
    write_json,
    write_markdown,
)


REPO_CI_STATE_PATH = STATE_DIR / "repo_ci_status.json"
REPO_CI_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "repo_ci_status.json"
REPO_CI_MD_PATH = OUTPUT_OPERATOR_DIR / "repo_ci_status.md"


def _duckagent_check_command() -> list[str]:
    python_path = DUCK_AGENT_ROOT / ".venv" / "bin" / "python"
    return [
        str(python_path),
        "-m",
        "pytest",
        "-q",
        "creative_agent/runtime/tests/test_delivery.py",
        "creative_agent/runtime/tests/test_runner.py",
        "creative_agent/runtime/tests/test_viewer.py",
    ]


def _duckops_check_command() -> list[str]:
    return [
        "python3",
        "-c",
        (
            "import pathlib, py_compile; "
            "root = pathlib.Path('runtime'); "
            "files = sorted(root.rglob('*.py')); "
            "assert files, 'No runtime python files found.'; "
            "[py_compile.compile(str(path), doraise=True) for path in files]; "
            "print(f'Compiled {len(files)} Python files.')"
        ),
    ]


TRACKED_REPOS: dict[str, dict[str, Any]] = {
    "duckAgent": {
        "path": DUCK_AGENT_ROOT,
        "visibility": "private",
        "workflow_name": "DuckAgent Creative Runtime",
        "job_name": "runtime-tests",
        "check_label": "creative-runtime",
        "check_description": "Mirrors the GitHub creative runtime workflow locally for this private repo.",
        "stale_after_hours": 24.0,
        "timeout_seconds": 900,
        "command_builder": _duckagent_check_command,
    },
    "duck-ops": {
        "path": DUCK_OPS_ROOT,
        "visibility": "public",
        "workflow_name": "Duck Ops Checks",
        "job_name": "py-compile",
        "check_label": "py-compile",
        "check_description": "Mirrors the GitHub runtime compile workflow locally.",
        "stale_after_hours": 24.0,
        "timeout_seconds": 300,
        "command_builder": _duckops_check_command,
    },
}

STATUS_PRIORITY = {
    "failed": 0,
    "error": 0,
    "timeout": 0,
    "dirty": 1,
    "outdated": 2,
    "not_run": 3,
    "stale": 4,
    "passed": 5,
}

ATTENTION_STATUSES = {"failed", "error", "timeout", "dirty", "outdated", "not_run", "stale"}


def _tail_lines(value: str, *, limit: int = 10) -> list[str]:
    lines = [line.rstrip() for line in str(value or "").splitlines() if line.strip()]
    return lines[-limit:]


def _command_text(command: list[str]) -> str:
    return " ".join(str(part) for part in command)


def _git_capture(repo_path: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(repo_path),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout


def _git_snapshot(repo_path: Path) -> dict[str, Any]:
    result = subprocess.run(
        ["git", "status", "--porcelain=2", "--branch"],
        cwd=str(repo_path),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return {
            "branch": None,
            "upstream": None,
            "head_sha": None,
            "ahead": 0,
            "behind": 0,
            "modified_count": 0,
            "untracked_count": 0,
            "status_lines": [],
            "error": (result.stderr or result.stdout or "git status failed").strip(),
        }

    branch = None
    upstream = None
    head_sha = None
    ahead = 0
    behind = 0
    modified_count = 0
    untracked_count = 0
    status_lines: list[str] = []

    for raw_line in (result.stdout or "").splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        if line.startswith("# branch.head "):
            branch = line.split("# branch.head ", 1)[1].strip()
            if branch == "(detached)":
                branch = None
            continue
        if line.startswith("# branch.upstream "):
            upstream = line.split("# branch.upstream ", 1)[1].strip() or None
            continue
        if line.startswith("# branch.oid "):
            head_sha = line.split("# branch.oid ", 1)[1].strip() or None
            continue
        if line.startswith("# branch.ab "):
            parts = line.split()
            if len(parts) >= 4:
                try:
                    ahead = int(parts[2].lstrip("+"))
                    behind = abs(int(parts[3]))
                except ValueError:
                    ahead = 0
                    behind = 0
            continue
        status_lines.append(line)
        if line.startswith("? "):
            untracked_count += 1
        else:
            modified_count += 1

    return {
        "branch": branch,
        "upstream": upstream,
        "head_sha": head_sha,
        "ahead": ahead,
        "behind": behind,
        "modified_count": modified_count,
        "untracked_count": untracked_count,
        "status_lines": status_lines[:20],
        "error": None,
    }


def _check_summary(status: str, stdout_tail: list[str], stderr_tail: list[str]) -> str:
    if status == "passed":
        candidates = [line for line in stdout_tail if line.strip()]
        return candidates[-1] if candidates else "Local CI mirror passed."
    candidates = [line for line in stderr_tail if line.strip()] or [line for line in stdout_tail if line.strip()]
    return candidates[-1] if candidates else "Local CI mirror failed."


def _run_repo_check(repo_name: str, config: dict[str, Any], git_snapshot: dict[str, Any]) -> dict[str, Any]:
    command = list(config.get("command_builder", lambda: [])() or [])
    started_at = now_local_iso()
    started = time.monotonic()
    repo_path = Path(config["path"])

    if not command:
        finished_at = now_local_iso()
        return {
            "status": "error",
            "summary": "No check command is configured.",
            "command": "",
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": 0.0,
            "head_sha": git_snapshot.get("head_sha"),
            "stdout_tail": [],
            "stderr_tail": [],
            "exit_code": None,
        }

    if repo_name == "duckAgent" and not Path(command[0]).exists():
        finished_at = now_local_iso()
        return {
            "status": "error",
            "summary": "DuckAgent virtualenv python is missing, so the local CI mirror cannot run.",
            "command": _command_text(command),
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": 0.0,
            "head_sha": git_snapshot.get("head_sha"),
            "stdout_tail": [],
            "stderr_tail": [],
            "exit_code": None,
        }

    try:
        result = subprocess.run(
            command,
            cwd=str(repo_path),
            capture_output=True,
            text=True,
            check=False,
            timeout=int(config.get("timeout_seconds") or 300),
        )
        status = "passed" if result.returncode == 0 else "failed"
        stdout_tail = _tail_lines(result.stdout)
        stderr_tail = _tail_lines(result.stderr)
        summary = _check_summary(status, stdout_tail, stderr_tail)
        exit_code: int | None = result.returncode
    except subprocess.TimeoutExpired as exc:
        status = "timeout"
        stdout_tail = _tail_lines(exc.stdout or "")
        stderr_tail = _tail_lines(exc.stderr or "")
        summary = f"Local CI mirror timed out after {int(config.get('timeout_seconds') or 300)} second(s)."
        exit_code = None
    except Exception as exc:
        status = "error"
        stdout_tail = []
        stderr_tail = [str(exc)]
        summary = str(exc)
        exit_code = None

    finished_at = now_local_iso()
    return {
        "status": status,
        "summary": summary,
        "command": _command_text(command),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(max(0.0, time.monotonic() - started), 2),
        "head_sha": git_snapshot.get("head_sha"),
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "exit_code": exit_code,
    }


def _status_headline(status: str, repo_name: str) -> str:
    labels = {
        "passed": f"{repo_name} local CI mirror is green.",
        "failed": f"{repo_name} local CI mirror failed.",
        "timeout": f"{repo_name} local CI mirror timed out.",
        "error": f"{repo_name} local CI mirror errored before it finished.",
        "dirty": f"{repo_name} has uncommitted changes after the last CI mirror result.",
        "outdated": f"{repo_name} has new commits after the last CI mirror result.",
        "not_run": f"{repo_name} does not have a local CI mirror result yet.",
        "stale": f"{repo_name} local CI mirror result is stale.",
    }
    return labels.get(status, f"{repo_name} CI status is unknown.")


def _recommended_action(status: str, repo_name: str) -> str:
    rerun = f"python3 runtime/repo_ci_status.py --run-checks --repo {repo_name}"
    if status == "passed":
        return "No immediate action is needed."
    if status == "dirty":
        return f"Commit or discard the local changes, then rerun `{rerun}`."
    if status == "outdated":
        return f"Rerun `{rerun}` so the CI mirror matches the current head."
    if status == "not_run":
        return f"Run `{rerun}` to capture the first local CI mirror result."
    if status == "stale":
        return f"Refresh the CI mirror with `{rerun}` before trusting it."
    return f"Open the repo failure details and rerun `{rerun}` after the fix."


def _item_summary(status: str, repo_name: str, git_snapshot: dict[str, Any], check: dict[str, Any]) -> str:
    workspace = "clean workspace"
    modified = int(git_snapshot.get("modified_count") or 0)
    untracked = int(git_snapshot.get("untracked_count") or 0)
    if modified or untracked:
        workspace = f"dirty workspace ({modified} modified / {untracked} untracked)"

    parts = [workspace]
    if status in {"passed", "failed", "error", "timeout", "stale"}:
        parts.append(str(check.get("summary") or _status_headline(status, repo_name)))
    elif status == "outdated":
        checked_head = str(check.get("head_sha") or "")[:7]
        current_head = str(git_snapshot.get("head_sha") or "")[:7]
        if checked_head and current_head:
            parts.append(f"last check covered `{checked_head}` but current head is `{current_head}`")
        else:
            parts.append("the repo moved after the last check")
    elif status == "not_run":
        parts.append("no local CI mirror result is recorded yet")
    elif status == "dirty":
        parts.append(str(check.get("summary") or "The repo changed after the last CI mirror result."))
    return " | ".join(part for part in parts if part)


def _normalize_repo_item(
    repo_name: str,
    config: dict[str, Any],
    git_snapshot: dict[str, Any],
    previous_check: dict[str, Any] | None,
) -> dict[str, Any]:
    check = dict(previous_check or {})
    stale_after_hours = float(config.get("stale_after_hours") or 24.0)
    check_finished_at = check.get("finished_at")
    check_age_hours = age_hours(check_finished_at) if check_finished_at else None
    check_head_sha = str(check.get("head_sha") or "").strip() or None
    current_head_sha = str(git_snapshot.get("head_sha") or "").strip() or None
    matches_current_head = bool(check_head_sha and current_head_sha and check_head_sha == current_head_sha)
    modified_count = int(git_snapshot.get("modified_count") or 0)
    untracked_count = int(git_snapshot.get("untracked_count") or 0)
    dirty_workspace = bool(modified_count or untracked_count)

    check_status = str(check.get("status") or "").strip()
    if check_status in {"failed", "error", "timeout"}:
        status = check_status
    elif dirty_workspace:
        status = "dirty"
    elif not check_status:
        status = "not_run"
    elif not matches_current_head:
        status = "outdated"
    elif check_age_hours is not None and check_age_hours > stale_after_hours:
        status = "stale"
    else:
        status = "passed"

    head_sha_short = current_head_sha[:7] if current_head_sha else None
    item = {
        "repo": repo_name,
        "path": str(config["path"]),
        "visibility": str(config.get("visibility") or "unknown"),
        "workflow_name": str(config.get("workflow_name") or ""),
        "job_name": str(config.get("job_name") or ""),
        "check_label": str(config.get("check_label") or "local-ci"),
        "check_description": str(config.get("check_description") or ""),
        "status": status,
        "status_label": status.upper().replace("_", " "),
        "headline": _status_headline(status, repo_name),
        "recommended_action": _recommended_action(status, repo_name),
        "summary": _item_summary(status, repo_name, git_snapshot, check),
        "attention_needed": status in ATTENTION_STATUSES,
        "stale_after_hours": stale_after_hours,
        "git": {
            **git_snapshot,
            "head_sha_short": head_sha_short,
        },
        "check": {
            **check,
            "head_sha": check_head_sha,
            "head_sha_short": check_head_sha[:7] if check_head_sha else None,
            "matches_current_head": matches_current_head,
            "age_hours": check_age_hours,
        },
        "rerun_command": f"python3 runtime/repo_ci_status.py --run-checks --repo {repo_name}",
    }
    return item


def _headline_for_payload(items: list[dict[str, Any]]) -> tuple[str, str]:
    failing = [item for item in items if item.get("status") in {"failed", "error", "timeout"}]
    attention = [item for item in items if item.get("attention_needed")]
    outdated = [item for item in items if item.get("status") == "outdated"]
    not_run = [item for item in items if item.get("status") == "not_run"]
    dirty = [item for item in items if item.get("status") == "dirty"]

    if failing:
        headline = f"{len(failing)} repo local CI mirror result(s) need attention."
        action = "Review the failing repo CI section in the business desk and rerun the specific mirror command after the fix."
    elif dirty:
        headline = f"{len(dirty)} repo workspace(s) are dirty, so the last CI mirror result may no longer match local reality."
        action = "Commit or discard local changes before trusting the last CI mirror result, then rerun the repo-specific check."
    elif outdated:
        headline = f"{len(outdated)} repo CI mirror result(s) are behind the current commit."
        action = "Rerun the local CI mirror for the repos that moved after the last check."
    elif not_run:
        headline = f"{len(not_run)} tracked repo(s) do not have a CI mirror result yet."
        action = "Run the local CI mirror once so the business desk has a baseline result."
    elif attention:
        headline = f"{len(attention)} repo CI mirror result(s) need to be refreshed."
        action = "Refresh the stale repo CI mirror results before relying on them."
    else:
        headline = "All tracked repo CI mirrors are green for the current local heads."
        action = "No immediate CI follow-through is needed."
    return headline, action


def build_repo_ci_status(
    *,
    run_checks: bool = False,
    repo_names: list[str] | None = None,
    write_outputs: bool = True,
) -> dict[str, Any]:
    requested = set(repo_names or TRACKED_REPOS.keys())
    previous_payload = load_json(REPO_CI_STATE_PATH, {})
    previous_items = {
        str(item.get("repo") or ""): item
        for item in list(previous_payload.get("items") or [])
        if isinstance(item, dict) and str(item.get("repo") or "").strip()
    }

    items: list[dict[str, Any]] = []
    for repo_name, config in TRACKED_REPOS.items():
        if repo_name not in requested and repo_names is not None:
            previous_item = previous_items.get(repo_name)
            if isinstance(previous_item, dict):
                items.append(previous_item)
            continue

        git_snapshot = _git_snapshot(Path(config["path"]))
        previous_item = previous_items.get(repo_name) or {}
        previous_check = previous_item.get("check") if isinstance(previous_item.get("check"), dict) else {}
        check = previous_check
        if run_checks and repo_name in requested:
            check = _run_repo_check(repo_name, config, git_snapshot)
        item = _normalize_repo_item(repo_name, config, git_snapshot, check if isinstance(check, dict) else {})
        items.append(item)

    items.sort(key=lambda item: (STATUS_PRIORITY.get(str(item.get("status") or "passed"), 9), str(item.get("repo") or "")))
    headline, recommended_action = _headline_for_payload(items)
    summary = {
        "repo_count": len(items),
        "passed_count": sum(1 for item in items if item.get("status") == "passed"),
        "attention_count": sum(1 for item in items if item.get("attention_needed")),
        "failing_count": sum(1 for item in items if item.get("status") in {"failed", "error", "timeout"}),
        "dirty_count": sum(1 for item in items if item.get("status") == "dirty"),
        "outdated_count": sum(1 for item in items if item.get("status") == "outdated"),
        "not_run_count": sum(1 for item in items if item.get("status") == "not_run"),
        "stale_count": sum(1 for item in items if item.get("status") == "stale"),
    }
    payload = {
        "generated_at": now_local_iso(),
        "source": "local_repo_ci_mirror",
        "headline": headline,
        "recommended_action": recommended_action,
        "summary": summary,
        "items": items,
    }
    if write_outputs:
        write_json(REPO_CI_STATE_PATH, payload)
        write_json(REPO_CI_OPERATOR_JSON_PATH, payload)
        write_markdown(REPO_CI_MD_PATH, render_repo_ci_status_markdown(payload))
    return payload


def render_repo_ci_status_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# Repo CI Status",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Source: `{payload.get('source') or 'local_repo_ci_mirror'}`",
        f"- Repos tracked: `{summary.get('repo_count', 0)}`",
        f"- Passing: `{summary.get('passed_count', 0)}`",
        f"- Need attention: `{summary.get('attention_count', 0)}`",
        f"- Failing: `{summary.get('failing_count', 0)}`",
        f"- Dirty: `{summary.get('dirty_count', 0)}`",
        f"- Outdated: `{summary.get('outdated_count', 0)}`",
        f"- Not run: `{summary.get('not_run_count', 0)}`",
        f"- Stale: `{summary.get('stale_count', 0)}`",
        f"- Headline: {payload.get('headline') or ''}",
        f"- Recommended action: {payload.get('recommended_action') or ''}",
        "",
    ]
    for item in payload.get("items") or []:
        git = item.get("git") or {}
        check = item.get("check") or {}
        lines.extend(
            [
                f"## {item.get('repo')}",
                "",
                f"- Status: `{item.get('status_label') or 'UNKNOWN'}`",
                f"- Visibility: `{item.get('visibility') or 'unknown'}`",
                f"- Workflow mirror: `{item.get('workflow_name') or 'unknown'}` / `{item.get('job_name') or 'unknown'}`",
                f"- Repo path: `{item.get('path') or ''}`",
                f"- Branch: `{git.get('branch') or 'detached'}`",
                f"- Upstream: `{git.get('upstream') or 'none'}`",
                f"- Head: `{git.get('head_sha_short') or 'unknown'}`",
                f"- Ahead / behind: `+{int(git.get('ahead') or 0)} / -{int(git.get('behind') or 0)}`",
                f"- Workspace changes: `{int(git.get('modified_count') or 0)}` modified / `{int(git.get('untracked_count') or 0)}` untracked",
                f"- Summary: {item.get('summary') or ''}",
                f"- Check label: `{item.get('check_label') or 'local-ci'}`",
                f"- Check description: {item.get('check_description') or ''}",
                f"- Check command: `{check.get('command') or item.get('rerun_command') or ''}`",
                f"- Check finished: `{check.get('finished_at') or 'never'}`",
                f"- Check duration: `{check.get('duration_seconds')}` second(s)",
                f"- Check head: `{check.get('head_sha_short') or 'unknown'}`",
                f"- Matches current head: `{bool(check.get('matches_current_head'))}`",
                f"- Check age: `{check.get('age_hours')}` hour(s)",
                f"- Recommended action: {item.get('recommended_action') or ''}",
            ]
        )
        stdout_tail = list(check.get("stdout_tail") or [])
        stderr_tail = list(check.get("stderr_tail") or [])
        if stdout_tail:
            lines.append("- Stdout tail:")
            for line in stdout_tail:
                lines.append(f"  - {line}")
        if stderr_tail:
            lines.append("- Stderr tail:")
            for line in stderr_tail:
                lines.append(f"  - {line}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build or refresh the local repo CI mirror status.")
    parser.add_argument("--run-checks", action="store_true", help="Run the configured local CI mirror checks before writing the status artifact.")
    parser.add_argument("--repo", action="append", dest="repos", help="Restrict the run to one or more repo names.")
    args = parser.parse_args()
    payload = build_repo_ci_status(run_checks=args.run_checks, repo_names=args.repos)
    print(
        {
            "generated_at": payload.get("generated_at"),
            "headline": payload.get("headline"),
            "attention_count": ((payload.get("summary") or {}).get("attention_count") or 0),
        }
    )


if __name__ == "__main__":
    main()
