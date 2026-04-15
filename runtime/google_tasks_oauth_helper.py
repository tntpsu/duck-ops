#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import http.server
import json
import secrets
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
import webbrowser


AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPE = "https://www.googleapis.com/auth/tasks"


def redirect_uri_for_port(port: int) -> str:
    return f"http://127.0.0.1:{port}/callback"


def _urlsafe_b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _pkce_pair() -> tuple[str, str]:
    verifier = _urlsafe_b64(secrets.token_bytes(48))
    challenge = _urlsafe_b64(hashlib.sha256(verifier.encode("utf-8")).digest())
    return verifier, challenge


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    server_version = "DuckOpsGoogleTasksOAuth/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        self.server.auth_code = (query.get("code") or [""])[0]  # type: ignore[attr-defined]
        self.server.auth_error = (query.get("error") or [""])[0]  # type: ignore[attr-defined]
        body = "Google authorization received. You can return to Codex now."
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body.encode("utf-8"))))
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def _wait_for_code(port: int, timeout_seconds: int = 180) -> tuple[str, str]:
    server = http.server.HTTPServer(("127.0.0.1", port), OAuthCallbackHandler)
    server.auth_code = ""  # type: ignore[attr-defined]
    server.auth_error = ""  # type: ignore[attr-defined]

    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    started = time.time()
    while time.time() - started < timeout_seconds:
        auth_code = getattr(server, "auth_code", "")
        auth_error = getattr(server, "auth_error", "")
        if auth_code or auth_error:
            server.server_close()
            return auth_code, auth_error
        time.sleep(0.2)
    server.server_close()
    return "", "timeout"


def exchange_code_for_tokens(client_id: str, client_secret: str, code: str, code_verifier: str, redirect_uri: str) -> dict[str, str]:
    token_payload = {
        "client_id": client_id,
        "code": code,
        "code_verifier": code_verifier,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
    }
    if client_secret.strip():
        token_payload["client_secret"] = client_secret.strip()
    payload = urllib.parse.urlencode(token_payload).encode("utf-8")
    request = urllib.request.Request(
        TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw}
        payload["http_status"] = exc.code
        raise RuntimeError(json.dumps(payload))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a one-time Google Tasks OAuth flow and print the refresh token.")
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", default="")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()

    client_id = args.client_id.strip()
    redirect_uri = redirect_uri_for_port(args.port)
    verifier, challenge = _pkce_pair()
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    url = AUTH_URL + "?" + urllib.parse.urlencode(params)
    print("Open this URL if the browser does not open automatically:\n")
    print(url)
    print("")
    webbrowser.open(url)
    code, error = _wait_for_code(args.port)
    if error:
        print(json.dumps({"ok": False, "error": error}, indent=2))
        return 1
    if not code:
        print(json.dumps({"ok": False, "error": "no_code_received"}, indent=2))
        return 1

    try:
        tokens = exchange_code_for_tokens(client_id, args.client_secret, code, verifier, redirect_uri)
    except RuntimeError as exc:
        print(str(exc))
        return 1
    print(json.dumps({"ok": True, "refresh_token": tokens.get("refresh_token"), "access_token": tokens.get("access_token")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
