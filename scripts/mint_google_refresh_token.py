"""One-command Google refresh-token mint (combined scope).

The 2026-07/08 recurring token deaths: both GOOGLE_TASKS_REFRESH_TOKEN and
GSC_REFRESH_TOKEN (same OAuth client) go invalid_grant roughly weekly —
consistent with the consent screen sitting in "Testing" publishing status
(Google force-expires Testing refresh tokens after 7 days). BEFORE minting:
Google Cloud Console → APIs & Services → OAuth consent screen → Publish to
production. Then run this once and paste the printed token into
duckAgent/.env for BOTH keys.

  /Users/philtullai/ai-agents/duckAgent/.venv/bin/python \
      scripts/mint_google_refresh_token.py

Scopes minted (one token serves Tasks + GSC + GA4):
  tasks, webmasters.readonly, analytics.readonly
"""
from __future__ import annotations

import http.server
import json
import secrets
import threading
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

DUCKAGENT_ENV = Path(__file__).resolve().parents[2] / "duckAgent" / ".env"
REDIRECT_PORT = 8917
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/callback"
SCOPES = " ".join([
    "https://www.googleapis.com/auth/tasks",
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/analytics.readonly",
])


def _load_client() -> tuple[str, str]:
    from dotenv import dotenv_values
    env = dotenv_values(DUCKAGENT_ENV)
    client_id = env.get("GOOGLE_TASKS_CLIENT_ID") or env.get("GOOGLE_CLIENT_ID") or ""
    client_secret = env.get("GOOGLE_TASKS_CLIENT_SECRET") or env.get("GOOGLE_CLIENT_SECRET") or ""
    if not (client_id and client_secret):
        raise SystemExit(f"GOOGLE_TASKS_CLIENT_ID/SECRET not found in {DUCKAGENT_ENV}")
    return client_id, client_secret


def main() -> int:
    client_id, client_secret = _load_client()
    state = secrets.token_urlsafe(16)
    code_holder: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if query.get("state", [""])[0] != state:
                self.send_response(400)
                self.end_headers()
                return
            code_holder["code"] = query.get("code", [""])[0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Token captured - return to the terminal.")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("localhost", REDIRECT_PORT), Handler)
    threading.Thread(target=server.handle_request, daemon=True).start()

    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    })
    print("[mint] REMINDER: consent screen must be 'In production', or this")
    print("[mint] token dies in 7 days like the last ones.")
    print(f"[mint] Opening browser... (or visit)\n{auth_url}\n")
    webbrowser.open(auth_url)

    print("[mint] Waiting for the OAuth callback on localhost:8917 ...")
    while "code" not in code_holder:
        pass

    data = urllib.parse.urlencode({
        "code": code_holder["code"],
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }).encode()
    with urllib.request.urlopen("https://oauth2.googleapis.com/token", data=data, timeout=30) as resp:
        payload = json.loads(resp.read())
    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        print(f"[mint] FAILED — no refresh_token in response: {payload}")
        return 1
    print("\n[mint] SUCCESS. Paste this into duckAgent/.env as BOTH keys:\n")
    print(f"GOOGLE_TASKS_REFRESH_TOKEN={refresh_token}")
    print(f"GSC_REFRESH_TOKEN={refresh_token}")
    print("\n[mint] (One combined-scope token serves Tasks + GSC + GA4.)")
    print("[mint] Never commit .env. Verify with:")
    print("[mint]   cd duck-ops && python3 runtime/gsc_search_demand.py --dry-run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
