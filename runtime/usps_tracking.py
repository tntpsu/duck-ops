#!/usr/bin/env python3
"""
Read-only USPS tracking enrichment for Duck Ops.

This module is intentionally conservative:

- only USPS tracking numbers already staged on customer cases are eligible
- calls remain read-only
- credentials and endpoint configuration must be present before live lookups run
"""

from __future__ import annotations

import base64
from datetime import datetime
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
NORMALIZED_DIR = STATE_DIR / "normalized"
DUCKAGENT_ENV_PATH = Path("/Users/philtullai/ai-agents/duckAgent/.env")
DUCKOPS_ENV_PATH = ROOT / ".env"
USPS_SNAPSHOT_PATH = NORMALIZED_DIR / "usps_tracking_snapshot.json"


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


def load_usps_env() -> dict[str, str]:
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


def usps_config(env: dict[str, str] | None = None) -> dict[str, Any]:
    env = env or load_usps_env()
    client_id = _first_present(env, ["USPS_CLIENT_ID", "USPS_CONSUMER_KEY"])
    client_secret = _first_present(env, ["USPS_CLIENT_SECRET", "USPS_CONSUMER_SECRET"])
    token_url = _first_present(env, ["USPS_OAUTH_TOKEN_URL"])
    tracking_url_template = _first_present(env, ["USPS_TRACKING_URL_TEMPLATE"])
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "token_url": token_url,
        "tracking_url_template": tracking_url_template,
        "credentials_ready": bool(client_id and client_secret),
        "endpoint_ready": bool(token_url and tracking_url_template),
    }


def _http_json(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, body: bytes | None = None) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return response.getcode(), json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw}
        return exc.code, payload


def fetch_usps_access_token(config: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8")
    status, payload = _http_json(
        str(config.get("token_url") or ""),
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic "
            + base64.b64encode(
                f"{config.get('client_id') or ''}:{config.get('client_secret') or ''}".encode("utf-8")
            ).decode("ascii"),
        },
        body=body,
    )
    if status not in {200, 201}:
        return None, {"ok": False, "status_code": status, "response": payload}
    return str(payload.get("access_token") or ""), {"ok": True, "response": payload}


def lookup_usps_tracking(tracking_number: str, config: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    access_token, token_result = fetch_usps_access_token(config)
    if not access_token:
        return None, {"ok": False, "stage": "token", **token_result}
    url = str(config.get("tracking_url_template") or "").format(tracking_number=tracking_number)
    status, payload = _http_json(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if status != 200:
        return None, {"ok": False, "stage": "tracking", "status_code": status, "response": payload}
    return payload, {"ok": True, "stage": "tracking"}


def enrich_cases_with_usps_tracking(customer_cases: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = usps_config()
    snapshot = {
        "generated_at": now_iso(),
        "config_status": "credentials_missing",
        "items": {},
    }
    rows: list[dict[str, Any]] = []
    tracked_cases = 0
    if not config.get("credentials_ready"):
        write_json(USPS_SNAPSHOT_PATH, snapshot)
        return customer_cases, {
            "generated_at": snapshot["generated_at"],
            "config_status": "credentials_missing",
            "counts": {"eligible_cases": 0, "lookups_succeeded": 0},
            "snapshot_path": str(USPS_SNAPSHOT_PATH),
        }
    if not config.get("endpoint_ready"):
        snapshot["config_status"] = "endpoint_not_configured"
        write_json(USPS_SNAPSHOT_PATH, snapshot)
        return customer_cases, {
            "generated_at": snapshot["generated_at"],
            "config_status": "endpoint_not_configured",
            "counts": {"eligible_cases": 0, "lookups_succeeded": 0},
            "snapshot_path": str(USPS_SNAPSHOT_PATH),
        }

    lookups_succeeded = 0
    for case in customer_cases:
        enriched = dict(case)
        tracking = dict(case.get("tracking_enrichment") or {})
        carrier = str(tracking.get("carrier") or "").strip().lower()
        tracking_number = str(tracking.get("tracking_number") or "").strip()
        if carrier != "usps" or not tracking_number:
            rows.append(enriched)
            continue
        tracked_cases += 1
        payload, result = lookup_usps_tracking(tracking_number, config)
        if payload:
            tracking["live_status"] = payload
            tracking["live_status_source"] = "usps_api"
            tracking["live_status_checked_at"] = now_iso()
            lookups_succeeded += 1
            snapshot["items"][tracking_number] = {
                "checked_at": tracking["live_status_checked_at"],
                "status": "ok",
                "payload": payload,
            }
        else:
            tracking["live_status_error"] = result
            tracking["live_status_checked_at"] = now_iso()
            snapshot["items"][tracking_number] = {
                "checked_at": tracking["live_status_checked_at"],
                "status": "error",
                "error": result,
            }
        enriched["tracking_enrichment"] = tracking
        rows.append(enriched)

    snapshot["config_status"] = "ready"
    write_json(USPS_SNAPSHOT_PATH, snapshot)
    return rows, {
        "generated_at": snapshot["generated_at"],
        "config_status": "ready",
        "counts": {
            "eligible_cases": tracked_cases,
            "lookups_succeeded": lookups_succeeded,
        },
        "snapshot_path": str(USPS_SNAPSHOT_PATH),
    }
