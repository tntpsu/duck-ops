#!/usr/bin/env python3
"""
Google Tasks bridge for ready Duck Ops custom design cases.

This bridge stays fail-closed:

- no task is created without explicit OAuth refresh-token credentials
- task creation is scoped to ready custom-design cases only
- duplicate tasks are suppressed through local state
"""

from __future__ import annotations

from datetime import datetime
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
DUCKAGENT_ENV_PATH = Path("/Users/philtullai/ai-agents/duckAgent/.env")
DUCKOPS_ENV_PATH = ROOT / ".env"
TASKS_STATE_PATH = STATE_DIR / "google_tasks_custom_design_tasks.json"

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_TASKLISTS_URL = "https://tasks.googleapis.com/tasks/v1/users/@me/lists"
GOOGLE_TASKS_URL_TEMPLATE = "https://tasks.googleapis.com/tasks/v1/lists/{tasklist_id}/tasks"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


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
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_google_tasks_env() -> dict[str, str]:
    env = {}
    env.update(load_env_file(DUCKAGENT_ENV_PATH))
    env.update(load_env_file(DUCKOPS_ENV_PATH))
    return env


def _first_present(env: dict[str, str], keys: list[str]) -> str | None:
    for key in keys:
        value = str(env.get(key) or "").strip()
        if value:
            return value
    return None


def google_tasks_config(env: dict[str, str] | None = None) -> dict[str, Any]:
    env = env or load_google_tasks_env()
    client_id = _first_present(env, ["GOOGLE_TASKS_CLIENT_ID", "GOOGLE_CLIENT_ID"])
    client_secret = _first_present(env, ["GOOGLE_TASKS_CLIENT_SECRET", "GOOGLE_CLIENT_SECRET"])
    refresh_token = _first_present(env, ["GOOGLE_TASKS_REFRESH_TOKEN", "GOOGLE_REFRESH_TOKEN"])
    tasklist_id = _first_present(env, ["GOOGLE_TASKS_TASKLIST_ID"])
    tasklist_title = _first_present(env, ["GOOGLE_TASKS_TASKLIST_TITLE", "GOOGLE_TASKLIST_TITLE"])
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "tasklist_id": tasklist_id,
        "tasklist_title": tasklist_title,
        "credentials_ready": bool(client_id and client_secret and refresh_token),
    }


def _http_json(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    body = None
    req_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return response.getcode(), json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload_json = json.loads(raw)
        except json.JSONDecodeError:
            payload_json = {"raw": raw}
        return exc.code, payload_json


def fetch_google_access_token(config: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    data = urllib.parse.urlencode(
        {
            "client_id": config.get("client_id") or "",
            "client_secret": config.get("client_secret") or "",
            "refresh_token": config.get("refresh_token") or "",
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        GOOGLE_TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return str(payload.get("access_token") or ""), {"ok": True, "response": payload}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw}
        return None, {"ok": False, "status_code": exc.code, "response": payload}


def resolve_tasklist_id(config: dict[str, Any], access_token: str) -> tuple[str | None, dict[str, Any]]:
    if config.get("tasklist_id"):
        return str(config["tasklist_id"]), {"ok": True, "source": "env_tasklist_id"}
    title = str(config.get("tasklist_title") or "").strip()
    if not title:
        return None, {"ok": False, "reason": "tasklist_not_configured"}
    status, payload = _http_json(
        GOOGLE_TASKLISTS_URL,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if status != 200:
        return None, {"ok": False, "status_code": status, "response": payload}
    for item in payload.get("items") or []:
        if str(item.get("title") or "").strip().lower() == title.lower():
            return str(item.get("id") or ""), {"ok": True, "source": "tasklist_title", "title": title}
    return None, {"ok": False, "reason": "tasklist_title_not_found", "title": title}


def _task_notes_for_case(case: dict[str, Any]) -> str:
    lines = [
        "Duck Ops custom design case",
        "",
        f"Artifact: {case.get('artifact_id')}",
        f"Summary: {case.get('request_summary') or '(none)'}",
        "",
        "Normalized brief:",
        json.dumps(case.get("normalized_brief") or {}, indent=2),
    ]
    questions = case.get("open_questions") or []
    if questions:
        lines.extend(["", "Open questions:"])
        for question in questions:
            lines.append(f"- {question}")
    refs = case.get("source_refs") or []
    if refs:
        lines.extend(["", "Source refs:"])
        for ref in refs[:5]:
            lines.append(f"- {ref}")
    return "\n".join(lines)


def create_google_task(access_token: str, tasklist_id: str, case: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    title = str(case.get("request_summary") or "Custom duck design task").strip() or "Custom duck design task"
    payload = {
        "title": title,
        "notes": _task_notes_for_case(case),
    }
    status, response = _http_json(
        GOOGLE_TASKS_URL_TEMPLATE.format(tasklist_id=tasklist_id),
        method="POST",
        headers={"Authorization": f"Bearer {access_token}"},
        payload=payload,
    )
    if status not in {200, 201}:
        return None, {"ok": False, "status_code": status, "response": response}
    return response, {"ok": True, "status_code": status}


def sync_ready_custom_design_cases(custom_design_cases: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = google_tasks_config()
    state = load_json(
        TASKS_STATE_PATH,
        {
            "generated_at": None,
            "config_status": "not_started",
            "tasklist_id": None,
            "items": {},
        },
    )
    rows: list[dict[str, Any]] = []
    summary = {
        "generated_at": now_iso(),
        "config_status": "credentials_missing",
        "tasklist_id": state.get("tasklist_id"),
        "counts": {
            "ready_cases": sum(1 for case in custom_design_cases if case.get("ready_for_manual_design")),
            "matched_existing_tasks": 0,
            "created_tasks": 0,
        },
        "state_path": str(TASKS_STATE_PATH),
    }
    if not config.get("credentials_ready"):
        for case in custom_design_cases:
            enriched = dict(case)
            enriched["google_task_status"] = "credentials_missing"
            rows.append(enriched)
        state["generated_at"] = summary["generated_at"]
        state["config_status"] = "credentials_missing"
        write_json(TASKS_STATE_PATH, state)
        return rows, summary

    access_token, token_result = fetch_google_access_token(config)
    if not access_token:
        summary["config_status"] = "token_failed"
        summary["token_result"] = token_result
        for case in custom_design_cases:
            enriched = dict(case)
            enriched["google_task_status"] = "token_failed"
            rows.append(enriched)
        state["generated_at"] = summary["generated_at"]
        state["config_status"] = "token_failed"
        state["token_result"] = token_result
        write_json(TASKS_STATE_PATH, state)
        return rows, summary

    tasklist_id, tasklist_result = resolve_tasklist_id(config, access_token)
    if not tasklist_id:
        summary["config_status"] = "tasklist_unavailable"
        summary["tasklist_result"] = tasklist_result
        for case in custom_design_cases:
            enriched = dict(case)
            enriched["google_task_status"] = "tasklist_unavailable"
            rows.append(enriched)
        state["generated_at"] = summary["generated_at"]
        state["config_status"] = "tasklist_unavailable"
        state["tasklist_result"] = tasklist_result
        write_json(TASKS_STATE_PATH, state)
        return rows, summary

    state.setdefault("items", {})
    state["tasklist_id"] = tasklist_id
    state["config_status"] = "ready"
    summary["config_status"] = "ready"
    summary["tasklist_id"] = tasklist_id

    for case in custom_design_cases:
        enriched = dict(case)
        artifact_id = str(case.get("artifact_id") or "")
        existing = (state.get("items") or {}).get(artifact_id) or {}
        if existing.get("task_id"):
            enriched["google_task_status"] = "created"
            enriched["google_task_id"] = existing.get("task_id")
            enriched["google_task_web_view_link"] = existing.get("web_view_link")
            summary["counts"]["matched_existing_tasks"] += 1
            rows.append(enriched)
            continue

        if not case.get("ready_for_manual_design"):
            enriched["google_task_status"] = "not_ready"
            rows.append(enriched)
            continue

        task_response, task_result = create_google_task(access_token, tasklist_id, case)
        if task_response:
            task_row = {
                "task_id": task_response.get("id"),
                "title": task_response.get("title"),
                "web_view_link": task_response.get("webViewLink"),
                "updated": task_response.get("updated"),
                "created_at": now_iso(),
            }
            state["items"][artifact_id] = task_row
            enriched["google_task_status"] = "created"
            enriched["google_task_id"] = task_row.get("task_id")
            enriched["google_task_web_view_link"] = task_row.get("web_view_link")
            summary["counts"]["created_tasks"] += 1
        else:
            enriched["google_task_status"] = "create_failed"
            enriched["google_task_error"] = task_result
        rows.append(enriched)

    state["generated_at"] = summary["generated_at"]
    write_json(TASKS_STATE_PATH, state)
    return rows, summary
