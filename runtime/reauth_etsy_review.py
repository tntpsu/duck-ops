#!/usr/bin/env python3
"""One-button Etsy review-reply re-auth.

Opens the headed `esd` Playwright session at the shop reviews page, waits
for the operator to log into Etsy in that window, then captures
(`state-save`) the authenticated storage state and marks the review-reply
auth healthy. Triggered by the "Re-authenticate Etsy" button on the
`etsy_review_auth` OS card (POST /api/etsy-review-auth/reauth), or run
directly. Read-only w.r.t. reviews — it never clicks reply/submit; it only
opens a login window and saves cookies. See memory etsy-review-reply-reauth.

Progress is written to state/etsy_review_reauth_status.json so the portal
can show what's happening; the OS card turns green on the next health
refresh once the save lands.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = DUCK_OPS_ROOT / "state"
AUTH_META_PATH = STATE_DIR / "review_reply_execution_auth.json"
AUTH_STORE_PATH = STATE_DIR / "review_reply_execution_auth_storage" / "esd.json"
STATUS_PATH = STATE_DIR / "etsy_review_reauth_status.json"
PWCLI_PATH = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills" / "playwright" / "scripts" / "playwright_cli.sh"
SESSION = "esd"
REVIEWS_URL = "https://www.etsy.com/shop/myJeepDuck/reviews?ref=pagination&page=1"
LOGIN_TIMEOUT_SECONDS = 300
POLL_SECONDS = 5


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _write_status(phase: str, message: str, *, success: bool | None = None) -> None:
    payload = {"phase": phase, "message": message, "success": success, "updated_at": _now()}
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    os.replace(tmp, STATUS_PATH)


def _pw(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(PWCLI_PATH), "--session", SESSION, *args],
        capture_output=True, text=True, timeout=timeout,
    )


def _logged_in() -> bool:
    """True when the reviews page shows review rows and isn't the sign-in page."""
    try:
        from review_reply_discovery import parse_eval_json  # type: ignore
    except Exception:
        parse_eval_json = None
    try:
        res = _pw("eval",
                  "JSON.stringify({rows:document.querySelectorAll('[data-review-region]').length,"
                  "signin:/signin|Sign in/i.test(location.href+document.title)})",
                  timeout=30)
    except Exception:
        return False
    raw = res.stdout or ""
    parsed = parse_eval_json(raw) if parse_eval_json else None
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except ValueError:
            parsed = None
    if isinstance(parsed, dict):
        return int(parsed.get("rows") or 0) > 0 and not parsed.get("signin")
    # Fallback: heuristic on raw output.
    return ("data-review-region" not in raw) and ('"signin":false' in raw or '\\"signin\\":false' in raw)


def mark_auth_healthy() -> None:
    """Capture the storage state + flip the executor's auth metadata to healthy."""
    AUTH_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _pw("state-save", str(AUTH_STORE_PATH), timeout=60)
    now = _now()
    try:
        meta = json.loads(AUTH_META_PATH.read_text(encoding="utf-8")) if AUTH_META_PATH.exists() else {}
    except (OSError, ValueError):
        meta = {}
    if not isinstance(meta, dict):
        meta = {}
    meta["auth_status"] = "healthy"
    meta["cleared_at"] = now
    meta["last_auth_check_at"] = now
    meta["last_error"] = None
    meta["next_retry_after"] = None
    meta["last_session_name"] = SESSION
    storage = meta.get("storage_state") if isinstance(meta.get("storage_state"), dict) else {}
    storage.update({"path": str(AUTH_STORE_PATH), "exists": True,
                    "saved_at": now, "last_save_at": now, "last_save_status": "saved", "last_save_error": None})
    meta["storage_state"] = storage
    tmp = AUTH_META_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    os.replace(tmp, AUTH_META_PATH)


def main() -> int:
    if not PWCLI_PATH.exists():
        _write_status("error", f"Playwright CLI not found at {PWCLI_PATH}", success=False)
        return 2
    _write_status("opening", "Opening the Etsy login window — log in there and I'll capture it automatically.")
    try:
        _pw("open", REVIEWS_URL, "--headed", timeout=120)
    except Exception as exc:
        _write_status("error", f"Could not open the login window: {exc}", success=False)
        return 2
    deadline = time.time() + LOGIN_TIMEOUT_SECONDS
    while time.time() < deadline:
        if _logged_in():
            _write_status("capturing", "Logged in — saving the Etsy session…")
            try:
                mark_auth_healthy()
            except Exception as exc:
                _write_status("error", f"Logged in but could not save the session: {exc}", success=False)
                return 1
            _write_status("done", "Etsy review-reply session re-authenticated. The card will go green on the next refresh.", success=True)
            return 0
        time.sleep(POLL_SECONDS)
    _write_status("timeout", "Timed out waiting for login (5 min). Press Re-authenticate again when ready.", success=False)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
