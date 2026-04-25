#!/usr/bin/env python3
"""LAN-facing read-only HTTP API for the Even G2 widget."""

from __future__ import annotations

import argparse
import json
import smtplib
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from operator_interface_contracts import (
    OPERATOR_REJECTED_PATH,
    PUBLISH_CANDIDATES,
    build_widget_status_payload,
    load_json_file,
    run_id_from_state_source,
)


DUCKAGENT_ENV_PATH = Path("/Users/philtullai/ai-agents/duckAgent/.env")
APPROVAL_LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "widget_api_approvals.log"

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8780


def _load_smtp_creds() -> dict[str, str]:
    if not DUCKAGENT_ENV_PATH.exists():
        return {}
    creds: dict[str, str] = {}
    for raw in DUCKAGENT_ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key in {"SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"}:
            creds[key] = value
    return creds


REJECTION_TTL_DAYS = 30


def _operator_rejected_entries() -> list[dict[str, Any]]:
    """Returns the raw rejected list, normalized to the dict shape so the
    writer can append idempotently without losing legacy entries."""
    data = load_json_file(OPERATOR_REJECTED_PATH) or {}
    raw = data.get("rejected") if isinstance(data, dict) else None
    out: list[dict[str, Any]] = []
    for value in raw or []:
        if isinstance(value, str):
            out.append({"artifactId": value, "rejectedAt": None})
        elif isinstance(value, dict) and value.get("artifactId"):
            out.append({
                "artifactId": str(value["artifactId"]),
                "rejectedAt": value.get("rejectedAt"),
            })
    return out


def _operator_rejected_ids() -> set[str]:
    cutoff = datetime.now(timezone.utc).timestamp() - REJECTION_TTL_DAYS * 24 * 3600
    out: set[str] = set()
    for entry in _operator_rejected_entries():
        rejected_at = entry.get("rejectedAt")
        if rejected_at is None:
            out.add(entry["artifactId"])  # legacy: keep
            continue
        try:
            ts = datetime.fromisoformat(rejected_at.replace("Z", "+00:00")).timestamp()
        except (AttributeError, ValueError, TypeError):
            ts = cutoff + 1
        if ts >= cutoff:
            out.add(entry["artifactId"])
    return out


def _find_candidate_by_artifact(artifact_id: str) -> dict[str, Any] | None:
    publish = load_json_file(PUBLISH_CANDIDATES) or {}
    for item in publish.get("items") or []:
        if str(item.get("artifact_id") or "") == artifact_id:
            return item
    return None


def _log_approval(line: str) -> None:
    try:
        APPROVAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with APPROVAL_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now().isoformat()} {line}\n")
    except OSError:
        pass


def reject_publish_candidate(artifact_id: str) -> dict[str, Any]:
    if not artifact_id:
        return {"ok": False, "error": "artifactId required"}
    candidate = _find_candidate_by_artifact(artifact_id)
    if not candidate:
        return {"ok": False, "error": f"artifact not found: {artifact_id}"}
    now_iso = datetime.now(timezone.utc).isoformat()
    cutoff = datetime.now(timezone.utc).timestamp() - REJECTION_TTL_DAYS * 24 * 3600

    # Re-read on every write so we don't trample concurrent edits, and prune
    # entries that aged past the TTL while we're here.
    existing = _operator_rejected_entries()
    pruned: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for entry in existing:
        if entry["artifactId"] in seen_ids:
            continue
        rejected_at = entry.get("rejectedAt")
        if rejected_at is not None:
            try:
                ts = datetime.fromisoformat(rejected_at.replace("Z", "+00:00")).timestamp()
                if ts < cutoff:
                    continue
            except (AttributeError, ValueError, TypeError):
                pass
        pruned.append(entry)
        seen_ids.add(entry["artifactId"])

    if artifact_id in seen_ids:
        return {"ok": True, "alreadyRejected": True, "artifactId": artifact_id}

    pruned.append({"artifactId": artifact_id, "rejectedAt": now_iso})
    payload = {"updated_at": now_iso, "rejected": pruned}
    try:
        OPERATOR_REJECTED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with OPERATOR_REJECTED_PATH.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
    except OSError as exc:
        return {"ok": False, "error": f"write failed: {exc}"}
    flow = str(candidate.get("flow") or "?")
    title = str((candidate.get("candidate_summary") or {}).get("title") or "(untitled)")[:80]
    _log_approval(f"REJECT artifact={artifact_id} flow={flow} title={title!r}")
    return {"ok": True, "artifactId": artifact_id, "flow": flow}


def approve_publish_candidate(artifact_id: str, dry_run: bool = False) -> dict[str, Any]:
    if not artifact_id:
        return {"ok": False, "error": "artifactId required"}

    candidate = _find_candidate_by_artifact(artifact_id)
    if not candidate:
        return {"ok": False, "error": f"artifact not found: {artifact_id}"}

    state = (candidate.get("execution_state") or {}).get("state")
    if state != "draft":
        return {"ok": False, "error": f"candidate state is {state!r}, not 'draft'"}

    summary = candidate.get("candidate_summary") or {}
    flow = str(candidate.get("flow") or "?")
    title = str(summary.get("title") or "(untitled)")[:80]

    state_source = (candidate.get("execution_state") or {}).get("state_source")
    run_id = run_id_from_state_source(state_source) or summary.get("publish_token") or "?"

    subject = f"Re: MJD: [{flow}] {title} | FLOW:{flow} | RUN:{run_id} | ACTION:publish"
    body = "publish\n\n(approved via Pulse glasses dashboard)\n"

    creds = _load_smtp_creds()
    missing = [key for key in ("SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS") if not creds.get(key)]
    if missing:
        return {"ok": False, "error": f"missing SMTP env: {missing}"}

    msg = EmailMessage()
    msg["From"] = creds["SMTP_USER"]
    msg["To"] = creds["SMTP_USER"]
    msg["Subject"] = subject
    msg.set_content(body)

    if dry_run:
        _log_approval(f"DRY artifact={artifact_id} subject={subject!r}")
        return {"ok": True, "dryRun": True, "subject": subject, "to": creds["SMTP_USER"]}

    try:
        port = int(creds["SMTP_PORT"])
        if port == 465:
            with smtplib.SMTP_SSL(creds["SMTP_HOST"], port, timeout=10) as smtp:
                smtp.login(creds["SMTP_USER"], creds["SMTP_PASS"])
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(creds["SMTP_HOST"], port, timeout=10) as smtp:
                smtp.starttls()
                smtp.login(creds["SMTP_USER"], creds["SMTP_PASS"])
                smtp.send_message(msg)
    except (smtplib.SMTPException, OSError, TimeoutError) as exc:
        _log_approval(f"FAIL artifact={artifact_id} error={exc}")
        return {"ok": False, "error": f"smtp send failed: {exc}"}

    _log_approval(f"SENT artifact={artifact_id} flow={flow} run={run_id}")
    return {"ok": True, "subject": subject, "flow": flow, "runId": run_id}


def build_widget_status() -> dict[str, Any]:
    return build_widget_status_payload()


class WidgetHandler(BaseHTTPRequestHandler):
    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
        self.send_header("Cache-Control", "no-store")

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write("[widget_api] " + (format % args) + "\n")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors()
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path not in ("/approvals/approve", "/approvals/reject"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length > 0 else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
        except (ValueError, json.JSONDecodeError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": f"bad json: {exc}"})
            return
        artifact_id = str(payload.get("artifactId") or "")
        confirm = bool(payload.get("confirm"))
        dry_run = bool(payload.get("dryRun"))
        if not dry_run and not confirm:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "must include {confirm: true} or {dryRun: true}"},
            )
            return
        if path == "/approvals/reject":
            if dry_run:
                self._send_json(HTTPStatus.OK, {"ok": True, "dryRun": True, "artifactId": artifact_id})
                return
            result = reject_publish_candidate(artifact_id)
        else:
            result = approve_publish_candidate(artifact_id, dry_run=dry_run)
        status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
        self._send_json(status, result)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path in ("/", "/widget-status.json"):
            try:
                payload = build_widget_status()
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
                return
            self._send_json(HTTPStatus.OK, payload)
            return
        if path == "/healthz":
            self._send_json(HTTPStatus.OK, {"ok": True})
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})


def main() -> int:
    parser = argparse.ArgumentParser(description="Duck Ops widget LAN API")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    try:
        server = ThreadingHTTPServer((args.host, args.port), WidgetHandler)
    except OSError as exc:
        # Fail loudly + quickly rather than produce a giant traceback — launchd
        # KeepAlive would otherwise respawn-loop us every 10s. Exit code 2
        # means "don't restart" per plist convention.
        if getattr(exc, "errno", None) == 48:
            # Clean-exit 0 so launchd's KeepAlive.SuccessfulExit=false stops
            # respawning us — another copy is already serving, nothing to do.
            sys.stderr.write(
                f"[widget_api] port {args.port} already in use. Another copy is "
                f"already running (likely launchd or a stale nohup). "
                f"Run: pkill -f widget_api.py  then launchctl kickstart -k "
                f"gui/$(id -u)/com.philtullai.duck-ops-widget\n"
            )
            return 0
        raise

    sys.stderr.write(f"[widget_api] serving http://{args.host}:{args.port}/widget-status.json\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write("[widget_api] shutting down\n")
        server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
