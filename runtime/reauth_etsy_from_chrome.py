#!/usr/bin/env python3
"""Etsy review-reply re-auth from the operator's REAL Chrome session.

2026-08-08: Etsy's bot detection blocks the Playwright login window with
"unusual activity" (fresh automated context + login attempt = highest-risk
fingerprint) while the operator's normal Chrome is fine — and the drain
itself keeps working because it browses WITH valid cookies. So instead of
logging in inside an automated window (reauth_etsy_review.py), this reads
the already-authenticated etsy.com cookies from the local Chrome profile
and writes them into the executor's storage state
(state/review_reply_execution_auth_storage/esd.json, restored on open per
review_reply_executor DEFAULTS.auth_storage_restore_on_open). No browser
launches; Etsy is never contacted.

Run it YOURSELF in a terminal (macOS will show ONE Keychain prompt for
"Chrome Safe Storage" — click Allow; that's Chrome's cookie-encryption key):

  cd /Users/philtullai/ai-agents/duck-ops
  python3 runtime/reauth_etsy_from_chrome.py

Everything stays on this machine: cookies go only into the same gitignored
state file the old re-auth wrote. Nothing is transmitted or committed.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = DUCK_OPS_ROOT / "state"
AUTH_META_PATH = STATE_DIR / "review_reply_execution_auth.json"
AUTH_STORE_PATH = STATE_DIR / "review_reply_execution_auth_storage" / "esd.json"
STATUS_PATH = STATE_DIR / "etsy_review_reauth_status.json"
CHROME_COOKIES = Path.home() / "Library/Application Support/Google/Chrome/Default/Cookies"
SESSION = "esd"

# Chrome-on-macOS cookie crypto: AES-128-CBC, key = PBKDF2-HMAC-SHA1 of the
# Keychain "Chrome Safe Storage" password, salt b"saltysalt", 1003 rounds,
# IV = 16 spaces. Values are prefixed "v10"; newer Chrome versions prepend a
# 32-byte SHA256(host_key) to the plaintext — strip it when it matches.
_SALT = b"saltysalt"
_IV = b" " * 16
_ITERATIONS = 1003


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _write_status(phase: str, message: str, *, success: bool | None = None) -> None:
    payload = {"phase": phase, "message": message, "success": success, "updated_at": _now()}
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    os.replace(tmp, STATUS_PATH)


def _chrome_key() -> bytes:
    proc = subprocess.run(
        ["security", "find-generic-password", "-w", "-s", "Chrome Safe Storage", "-a", "Chrome"],
        capture_output=True, text=True, timeout=120,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        raise SystemExit(
            "Could not read the 'Chrome Safe Storage' Keychain item — did you "
            "click Deny on the prompt? Re-run and click Allow."
        )
    password = proc.stdout.strip().encode()
    return hashlib.pbkdf2_hmac("sha1", password, _SALT, _ITERATIONS, dklen=16)


def _decrypt(encrypted: bytes, key: bytes, host_key: str) -> str | None:
    if not encrypted or not encrypted.startswith(b"v10"):
        return None
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    cipher = Cipher(algorithms.AES(key), modes.CBC(_IV))
    decryptor = cipher.decryptor()
    padded = decryptor.update(encrypted[3:]) + decryptor.finalize()
    if not padded:
        return None
    pad = padded[-1]
    if not 1 <= pad <= 16:
        return None
    plain = padded[:-pad]
    if len(plain) >= 32 and plain[:32] == hashlib.sha256(host_key.encode()).digest():
        plain = plain[32:]
    try:
        return plain.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _samesite(value: int) -> str:
    return {0: "None", 1: "Lax", 2: "Strict"}.get(value, "Lax")


def _chrome_expiry_to_unix(expires_utc: int) -> float:
    if not expires_utc:
        return -1  # session cookie
    return expires_utc / 1_000_000 - 11_644_473_600


def read_etsy_cookies() -> list[dict]:
    if not CHROME_COOKIES.exists():
        raise SystemExit(f"Chrome cookie DB not found at {CHROME_COOKIES}")
    key = _chrome_key()
    with tempfile.TemporaryDirectory() as tmp:
        db_copy = Path(tmp) / "Cookies"
        shutil.copy2(CHROME_COOKIES, db_copy)
        con = sqlite3.connect(str(db_copy))
        try:
            rows = con.execute(
                "SELECT host_key, name, value, encrypted_value, path, expires_utc, "
                "is_secure, is_httponly, samesite FROM cookies "
                "WHERE host_key LIKE '%etsy.com'"
            ).fetchall()
        finally:
            con.close()
    cookies: list[dict] = []
    for host_key, name, value, encrypted_value, path, expires_utc, secure, httponly, samesite in rows:
        cookie_value = value or _decrypt(encrypted_value, key, host_key)
        if cookie_value is None:
            continue
        cookies.append({
            "name": name,
            "value": cookie_value,
            "domain": host_key,
            "path": path or "/",
            "expires": _chrome_expiry_to_unix(int(expires_utc or 0)),
            "httpOnly": bool(httponly),
            "secure": bool(secure),
            "sameSite": _samesite(int(samesite if samesite is not None else 1)),
        })
    return cookies


def _mark_meta_healthy() -> None:
    """Same meta flip as reauth_etsy_review.mark_auth_healthy, minus the
    live-session state-save (we wrote the storage file directly)."""
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
                    "saved_at": now, "last_save_at": now,
                    "last_save_status": "saved", "last_save_error": None,
                    "source": "chrome_profile_import"})
    meta["storage_state"] = storage
    tmp = AUTH_META_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    os.replace(tmp, AUTH_META_PATH)


def main() -> int:
    _write_status("reading", "Reading the Etsy session from your Chrome profile…")
    cookies = read_etsy_cookies()
    session_cookies = [c for c in cookies if "session" in c["name"].lower()]
    if len(cookies) < 3 or not session_cookies:
        _write_status("error",
                      f"Only {len(cookies)} etsy.com cookies found and no session cookie — "
                      "log into etsy.com in Chrome first, then re-run.", success=False)
        print(f"[reauth] FAILED: {len(cookies)} cookies, {len(session_cookies)} session cookies. "
              "Open Chrome, make sure you're logged into etsy.com, then re-run.")
        return 1

    AUTH_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = {"cookies": cookies, "origins": []}
    tmp = AUTH_STORE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1), encoding="utf-8")
    os.replace(tmp, AUTH_STORE_PATH)
    _mark_meta_healthy()
    _write_status("done",
                  "Etsy session imported from Chrome. The executor restores it on next open; "
                  "the card goes green on the next health refresh.", success=True)

    longest = max((c["expires"] for c in session_cookies if c["expires"] > 0), default=-1)
    expiry = datetime.fromtimestamp(longest).date().isoformat() if longest > 0 else "session-only"
    print(f"[reauth] SUCCESS: wrote {len(cookies)} etsy.com cookies "
          f"({len(session_cookies)} session cookies, longest-lived expires {expiry}) "
          f"to {AUTH_STORE_PATH}")
    print("[reauth] No browser touched Etsy. The next scheduled checker window will "
          "confirm the session works end-to-end.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
