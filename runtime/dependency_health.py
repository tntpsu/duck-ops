#!/usr/bin/env python3
"""
Dependency health surface for DuckAgent and Duck Ops workflows.

This watches workflow-control state plus recent DuckAgent state files for
classified upstream blockers such as PhotoRoom quota, auth, or rate-limit issues.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from governance_review_common import (
    DUCK_AGENT_ROOT,
    OUTPUT_OPERATOR_DIR,
    STATE_DIR,
    load_json,
    now_local_iso,
    write_json,
    write_markdown,
)
from workflow_control import list_workflow_states

DEPENDENCY_HEALTH_PATH = STATE_DIR / "dependency_health.json"
DEPENDENCY_HEALTH_MD_PATH = OUTPUT_OPERATOR_DIR / "dependency_health.md"

DEPENDENCY_PREFIXES = {
    "photoroom_": "photoroom",
}

DEPENDENCY_LABELS = {
    "photoroom": "PhotoRoom",
}

BLOCKER_LABELS = {
    "photoroom_quota_exhausted": "PhotoRoom image quota is exhausted.",
    "photoroom_auth_blocked": "PhotoRoom account or API key access is blocked.",
    "photoroom_rate_limited": "PhotoRoom is rate-limiting image generation.",
    "photoroom_upstream_unavailable": "PhotoRoom is temporarily unavailable.",
    "photoroom_render_failed": "PhotoRoom render failed.",
}

BLOCKER_ACTIONS = {
    "photoroom_quota_exhausted": "Wait for the PhotoRoom quota reset or refresh the plan, then rerun only the blocked render step.",
    "photoroom_auth_blocked": "Check PHOTOROOM_API_KEY and account access before rerunning any PhotoRoom-dependent render.",
    "photoroom_rate_limited": "Wait for the rate-limit window to clear, then retry the render without regenerating unrelated copy.",
    "photoroom_upstream_unavailable": "Retry after PhotoRoom recovers; keep the prepared content unless the render keeps failing.",
    "photoroom_render_failed": "Inspect the PhotoRoom response, template ID, and image inputs before retrying the render.",
}


def _dependency_for_blocker(blocker: str) -> str | None:
    value = str(blocker or "").strip()
    for prefix, dependency in DEPENDENCY_PREFIXES.items():
        if value.startswith(prefix):
            return dependency
    return None


def _status_for_blocker(blocker: str) -> str:
    if blocker in {"photoroom_rate_limited", "photoroom_upstream_unavailable"}:
        return "warn"
    return "bad"


def _blocker_label(blocker: str) -> str:
    return BLOCKER_LABELS.get(blocker, blocker.replace("_", " ").strip().capitalize())


def _recommended_action(blocker: str) -> str:
    return BLOCKER_ACTIONS.get(blocker, "Clear the upstream dependency blocker before retrying the workflow.")


def _lane_from_state_path(path: Path) -> str:
    name = path.stem
    if name.startswith("state_"):
        return name.removeprefix("state_")
    return name


def _recent_duckagent_state_files(limit: int = 80) -> list[Path]:
    runs_root = DUCK_AGENT_ROOT / "runs"
    if not runs_root.exists():
        return []
    files = [path for path in runs_root.glob("*/state_*.json") if path.is_file()]
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return files[: max(0, int(limit))]


def _workflow_dependency_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for state in list_workflow_states():
        if not isinstance(state, dict):
            continue
        metadata = state.get("metadata") if isinstance(state.get("metadata"), dict) else {}
        blocker = str(metadata.get("render_blocker") or state.get("state_reason") or "").strip()
        dependency = _dependency_for_blocker(blocker)
        if not dependency:
            continue
        lane = str(state.get("lane") or "workflow").strip() or "workflow"
        title = str(state.get("display_label") or state.get("workflow_id") or lane).strip()
        items.append(
            {
                "source": "workflow_control",
                "dependency": dependency,
                "dependency_label": DEPENDENCY_LABELS.get(dependency, dependency),
                "blocker": blocker,
                "blocker_label": _blocker_label(blocker),
                "status": _status_for_blocker(blocker),
                "lane": lane,
                "run_id": state.get("run_id"),
                "title": title,
                "summary": f"{title} is blocked by {_blocker_label(blocker)}",
                "updated_at": state.get("updated_at"),
                "recommended_action": str(state.get("next_action") or "").strip() or _recommended_action(blocker),
                "path": state.get("_path"),
            }
        )
    return items


def _state_file_dependency_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in _recent_duckagent_state_files():
        payload = load_json(path, {})
        if not isinstance(payload, dict):
            continue
        lane = _lane_from_state_path(path)
        run_id = path.parent.name
        for key, value in payload.items():
            if not str(key).endswith("_blocker"):
                continue
            blocker = str(value or "").strip()
            dependency = _dependency_for_blocker(blocker)
            if not dependency:
                continue
            recommended_key = key.replace("_blocker", "_recommended_action")
            error_key = key.replace("_blocker", "_error")
            items.append(
                {
                    "source": "duckagent_state",
                    "dependency": dependency,
                    "dependency_label": DEPENDENCY_LABELS.get(dependency, dependency),
                    "blocker": blocker,
                    "blocker_label": _blocker_label(blocker),
                    "status": _status_for_blocker(blocker),
                    "lane": lane,
                    "run_id": run_id,
                    "title": f"{lane} {run_id}",
                    "summary": f"{lane} {run_id} is blocked by {_blocker_label(blocker)}",
                    "updated_at": None,
                    "recommended_action": str(payload.get(recommended_key) or "").strip() or _recommended_action(blocker),
                    "error": str(payload.get(error_key) or "").strip() or None,
                    "path": str(path),
                }
            )
    return items


def build_dependency_health(*, write_outputs: bool = True) -> dict[str, Any]:
    generated_at = now_local_iso()
    raw_items = _workflow_dependency_items() + _state_file_dependency_items()
    deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in raw_items:
        key = (
            str(item.get("dependency") or ""),
            str(item.get("blocker") or ""),
            str(item.get("lane") or ""),
            str(item.get("run_id") or item.get("path") or ""),
        )
        existing = deduped.get(key)
        if existing and existing.get("source") == "workflow_control":
            continue
        deduped[key] = item

    status_rank = {"bad": 0, "warn": 1, "ok": 2}
    items = list(deduped.values())
    items.sort(
        key=lambda item: (
            status_rank.get(str(item.get("status") or "warn"), 9),
            str(item.get("dependency") or ""),
            str(item.get("updated_at") or ""),
            str(item.get("title") or ""),
        )
    )
    bad_items = [item for item in items if item.get("status") == "bad"]
    warn_items = [item for item in items if item.get("status") == "warn"]
    dependencies = sorted({str(item.get("dependency") or "") for item in items if item.get("dependency")})

    if bad_items:
        headline = f"{len(bad_items)} dependency blocker(s) need attention."
        recommended_action = str(bad_items[0].get("recommended_action") or "").strip() or _recommended_action(str(bad_items[0].get("blocker") or ""))
        status = "bad"
    elif warn_items:
        headline = f"{len(warn_items)} dependency warning(s) are active."
        recommended_action = str(warn_items[0].get("recommended_action") or "").strip() or _recommended_action(str(warn_items[0].get("blocker") or ""))
        status = "warn"
    else:
        headline = "No active dependency blockers found in recent workflow state."
        recommended_action = "No action needed."
        status = "ok"

    payload = {
        "generated_at": generated_at,
        "status": status,
        "headline": headline,
        "recommended_action": recommended_action,
        "summary": {
            "item_count": len(items),
            "bad_count": len(bad_items),
            "warn_count": len(warn_items),
            "dependency_count": len(dependencies),
            "dependencies": dependencies,
        },
        "items": items[:12],
        "source": "workflow_control_and_recent_duckagent_state",
    }
    if write_outputs:
        write_json(DEPENDENCY_HEALTH_PATH, payload)
        write_markdown(DEPENDENCY_HEALTH_MD_PATH, render_dependency_health_markdown(payload))
    return payload


def render_dependency_health_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    items = [item for item in list(payload.get("items") or []) if isinstance(item, dict)]
    lines = [
        "# Dependency Health",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Status: `{payload.get('status') or 'unknown'}`",
        f"- Active blockers/warnings: `{summary.get('item_count', len(items))}`",
        f"- Bad: `{summary.get('bad_count', 0)}`",
        f"- Warn: `{summary.get('warn_count', 0)}`",
        f"- Headline: {payload.get('headline')}",
        f"- Recommended action: {payload.get('recommended_action')}",
        "",
        "## Active Items",
        "",
    ]
    if not items:
        lines.append("No active dependency blockers were found.")
    for item in items:
        lines.append(
            f"- {item.get('dependency_label') or item.get('dependency')} | `{item.get('status')}` | "
            f"{item.get('lane')} | {item.get('title')}"
        )
        lines.append(f"  Blocker: {item.get('blocker_label') or item.get('blocker')}")
        if item.get("recommended_action"):
            lines.append(f"  Next: {item.get('recommended_action')}")
        if item.get("path"):
            lines.append(f"  Source: `{item.get('path')}`")
    return "\n".join(lines) + "\n"


def main() -> None:
    build_dependency_health(write_outputs=True)


if __name__ == "__main__":
    main()
