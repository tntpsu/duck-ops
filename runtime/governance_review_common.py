from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
STATE_DIR = DUCK_OPS_ROOT / "state"
OUTPUT_OPERATOR_DIR = DUCK_OPS_ROOT / "output" / "operator"
SYSTEM_HEALTH_PATH = DUCK_AGENT_ROOT / "creative_agent" / "runtime" / "output" / "operator" / "system_health.json"
ENGINEERING_GOVERNANCE_DIGEST_PATH = STATE_DIR / "engineering_governance_digest.json"

REPOS = {
    "duckAgent": DUCK_AGENT_ROOT,
    "duck-ops": DUCK_OPS_ROOT,
}

SEVERITY_RANK = {
    "bad": 0,
    "warn": 1,
    "ok": 2,
}


def now_local_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def repo_status(repo_name: str, repo_path: Path) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        return {
            "repo": repo_name,
            "path": str(repo_path),
            "modified_count": 0,
            "untracked_count": 0,
            "status_lines": [],
            "error": str(exc),
        }

    lines = [line.rstrip() for line in (result.stdout or "").splitlines() if line.strip()]
    return {
        "repo": repo_name,
        "path": str(repo_path),
        "modified_count": sum(1 for line in lines if not line.startswith("??")),
        "untracked_count": sum(1 for line in lines if line.startswith("??")),
        "status_lines": lines[:20],
    }


def health_payload() -> dict[str, Any]:
    payload = load_json(SYSTEM_HEALTH_PATH, {})
    return payload if isinstance(payload, dict) else {}


def health_alerts(limit: int | None = None) -> list[dict[str, Any]]:
    payload = health_payload()
    flow_health = payload.get("flow_health")
    if not isinstance(flow_health, list):
        return []
    items = [item for item in flow_health if isinstance(item, dict) and item.get("status") in {"bad", "warn"}]
    items.sort(
        key=lambda item: (
            SEVERITY_RANK.get(str(item.get("status") or "ok"), 9),
            str(item.get("last_run_at") or ""),
            str(item.get("label") or ""),
        )
    )
    if limit is not None:
        items = items[:limit]
    return items


def parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed


def age_hours(value: Any, *, now: datetime | None = None) -> float | None:
    parsed = parse_iso(value)
    if parsed is None:
        return None
    current = now or datetime.now().astimezone()
    return round(max(0.0, (current - parsed.astimezone()).total_seconds() / 3600.0), 1)


def review_state_summary(path: Path, key: str) -> dict[str, Any]:
    payload = load_json(path, {})
    if not isinstance(payload, dict):
        return {"present": False, "path": str(path), "generated_at": None, "item_count": 0, "top_label": None}
    items = payload.get(key)
    if not isinstance(items, list):
        items = []
    top_label = None
    if items:
        first = items[0]
        if isinstance(first, dict):
            top_label = first.get("title") or first.get("label") or first.get("surface")
        else:
            top_label = str(first)
    return {
        "present": path.exists(),
        "path": str(path),
        "generated_at": payload.get("generated_at"),
        "item_count": len(items),
        "top_label": top_label,
    }
