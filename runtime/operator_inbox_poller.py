"""
Poll the operator inbox for replies to Duck Ops approval emails and
feed them into DuckAgent's existing email-reply dispatcher.

DuckAgent's main_agent.handle_mail_event() already knows how to parse
button-style replies: it pulls FLOW / RUN / ACTION from the
"DuckAgent approval metadata:" footer the email templates emit, then
routes to flow-specific handlers (design_brief_queue, meme publish,
duck_concept_builder, etc.) and on to duck-ops's decision gateway
where appropriate.

The missing piece was the polling glue — nothing was reading the
inbox and invoking handle_mail_event. This module is that glue. It:

  1. Connects to Gmail via the existing phase1_observer IMAP plumbing.
  2. Finds unread emails matching: From=operator AND body contains
     "DuckAgent approval metadata:" (the unique system-template
     marker — proves the email came from a real Duck Ops template,
     not a random reply or a system notification).
  3. Serializes each email as {"subject", "body"} JSON.
  4. Subprocess-invokes `python src/main_agent.py --mail-file <tmp>`
     so the dispatcher runs in DuckAgent's own venv with its own
     imports.
  5. Marks the email Seen on success; leaves it unread on failure so
     the operator can intervene.

Trust signal: the "DuckAgent approval metadata:" footer. Plain
operator-typed emails (or system notifications without the footer)
are ignored — only emails that originated from a template's mailto
button reach the dispatcher.

Receipts:
  state/operator_inbox_receipts.jsonl logs every message we touched.

CLI:
  python runtime/operator_inbox_poller.py              # poll and process
  python runtime/operator_inbox_poller.py --dry-run    # show what would happen
"""
from __future__ import annotations

import argparse
import email
import imaplib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import Message
from pathlib import Path
from typing import Any

RUNTIME_DIR = Path(__file__).resolve().parent
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

# Reuse the existing IMAP plumbing from the source observer so we
# don't drift the credential resolution logic.
from phase1_observer import (  # type: ignore
    connect_imap,
    load_env_file,
    load_mailbox_settings,
)


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = DUCK_OPS_ROOT / "config" / "source_observer.json"
RECEIPT_PATH = DUCK_OPS_ROOT / "state" / "operator_inbox_receipts.jsonl"
POLL_FOLDER = "INBOX"

# The unique marker that proves an email originated from a Duck Ops
# template's mailto button. See
# duckAgent/helpers/email_reply_action_helper.py::reply_body.
APPROVAL_METADATA_MARKER = "DuckAgent approval metadata:"

DUCK_AGENT_ROOT = Path(os.environ.get("DUCK_AGENT_ROOT", "/Users/philtullai/ai-agents/duckAgent"))
DUCK_AGENT_VENV_PY = DUCK_AGENT_ROOT / ".venv" / "bin" / "python3"
DUCK_AGENT_MAIN = DUCK_AGENT_ROOT / "src" / "main_agent.py"
DISPATCH_TIMEOUT_SECONDS = 180.0

# Reply boundary patterns that mark where the operator's text ends
# and the quoted prior message begins. Order matters — match the
# strictest first.
_REPLY_BOUNDARY_PATTERNS = (
    re.compile(r"^On .{1,80}\bwrote:\s*$", re.MULTILINE),         # On <date> <person> wrote:
    re.compile(r"^>+\s.*$", re.MULTILINE),                          # quoted lines starting with >
    re.compile(r"^-{2,}\s*Original\s+Message\s*-{2,}", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^From:\s.+$", re.MULTILINE),                       # email client header block
    re.compile(r"^Sent from my\b", re.MULTILINE),                   # mobile signature
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _trusted_operator_email() -> str:
    """Resolve the operator email we'll trust replies from. Override
    via POLLER_OPERATOR_EMAIL; otherwise falls back to EMAIL_TO in the
    duckAgent .env (the address the system sends approval emails to —
    by definition the operator's address)."""
    explicit = os.environ.get("POLLER_OPERATOR_EMAIL", "").strip().lower()
    if explicit:
        return explicit
    env_path = Path(os.environ.get("DUCKAGENT_ENV_FILE", "/Users/philtullai/ai-agents/duckAgent/.env"))
    env = load_env_file(env_path) if env_path.exists() else {}
    return str(env.get("EMAIL_TO") or "").strip().lower()


def _load_config() -> dict[str, Any]:
    try:
        return json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _extract_plain_text(message: Message) -> str:
    """Return the plain-text body of an email, decoded best-effort.
    Prefers text/plain parts; falls back to a crude HTML strip when
    only text/html is available."""
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_type() == "text/plain":
                try:
                    payload = part.get_payload(decode=True) or b""
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                except Exception:
                    continue
        for part in message.walk():
            if part.get_content_type() == "text/html":
                try:
                    payload = part.get_payload(decode=True) or b""
                    html = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                    return re.sub(r"<[^>]+>", " ", html)
                except Exception:
                    continue
        return ""
    payload = message.get_payload(decode=True) or b""
    if isinstance(payload, bytes):
        return payload.decode(message.get_content_charset() or "utf-8", errors="replace")
    return str(payload)


def _strip_quoted_reply(body: str) -> str:
    """Return only the operator's own text — everything before the
    first quoted-reply boundary. Real-world Gmail/Apple Mail/Outlook
    replies all use one of the patterns in _REPLY_BOUNDARY_PATTERNS."""
    earliest = len(body)
    for pat in _REPLY_BOUNDARY_PATTERNS:
        m = pat.search(body)
        if m:
            earliest = min(earliest, m.start())
    return body[:earliest].strip()


def _strip_metadata_footer(body: str) -> str:
    """Trim the 'DuckAgent approval metadata:' footer so the receipt
    log's command_text field shows only the operator-visible command."""
    # Cut at the separator that precedes the metadata block, or at the
    # marker itself if the separator is missing.
    for marker in ("\n----\n", "\nDuckAgent approval metadata:"):
        idx = body.find(marker)
        if idx >= 0:
            return body[:idx].strip()
    return body.strip()


def _extract_command_text(message: Message) -> str:
    """Pull the operator's reply text out of an email message. The
    text is what handle_operator_text() will parse — typically
    'approve 240' or 'agree 205'."""
    body = _extract_plain_text(message)
    body = body.replace("\r\n", "\n").replace("\r", "\n")
    return _strip_quoted_reply(body)


def _parse_address(raw: str) -> str:
    """Return the bare 'user@host' lowercased, stripped of display name."""
    match = re.search(r"[\w.+-]+@[\w.-]+", raw or "")
    return match.group(0).lower() if match else ""


def _decode_header_value(raw: str) -> str:
    """Decode an RFC 2047 encoded-word header into a plain string.

    Gmail wraps non-ASCII subjects (and even some ASCII subjects when
    the line wraps) as `=?UTF-8?Q?...?=`. Without decoding first the
    "Re:" prefix is hidden behind the encoding."""
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return raw


def _append_receipt(receipt: dict[str, Any]) -> None:
    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with RECEIPT_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(receipt, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _dispatch_mail_event(subject: str, body: str) -> dict[str, Any]:
    """Hand the email to DuckAgent's handle_mail_event by writing a
    tmp JSON file and subprocess-invoking main_agent.py --mail-file.

    Why a subprocess and not an import: main_agent.py is in duckAgent's
    own package layout with its own venv and module assumptions. Running
    it as a script with its own Python interpreter avoids import-path
    contamination and matches the manual pattern Codex documented for
    one-off mail tests."""
    if not body or not body.strip():
        return {"ok": False, "reason": "empty_body"}
    if not DUCK_AGENT_VENV_PY.exists() or not DUCK_AGENT_MAIN.exists():
        return {
            "ok": False,
            "reason": "duckagent_unreachable",
            "venv_py": str(DUCK_AGENT_VENV_PY),
            "main_agent": str(DUCK_AGENT_MAIN),
        }
    evt = {"subject": subject, "body": body}
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="inbox-poller-evt-", delete=False, encoding="utf-8",
    )
    try:
        json.dump(evt, tmp)
        tmp.close()
        proc = subprocess.run(
            [str(DUCK_AGENT_VENV_PY), str(DUCK_AGENT_MAIN), "--mail-file", tmp.name],
            cwd=str(DUCK_AGENT_ROOT),
            capture_output=True,
            text=True,
            timeout=DISPATCH_TIMEOUT_SECONDS,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-800:],
            "stderr_tail": (proc.stderr or "")[-800:],
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": "dispatch_timeout"}
    except Exception as exc:
        return {"ok": False, "reason": f"{type(exc).__name__}", "error": str(exc)}
    finally:
        try:
            Path(tmp.name).unlink(missing_ok=True)
        except Exception:
            pass


def poll_inbox(*, dry_run: bool = False, limit: int = 25) -> list[dict[str, Any]]:
    """Connect to the operator's inbox, process up to `limit` unread
    operator replies, and return a list of receipt dicts (one per
    message touched). Each receipt is also appended to the receipt log."""
    config = _load_config()
    settings = load_mailbox_settings(config)
    if not settings.get("enabled"):
        receipt = {
            "at": _now_iso(),
            "outcome": "mailbox_disabled",
            "reason": settings.get("error") or "missing imap settings",
        }
        _append_receipt(receipt)
        return [receipt]
    trusted = _trusted_operator_email()
    if not trusted:
        receipt = {
            "at": _now_iso(),
            "outcome": "no_trusted_sender",
            "reason": "POLLER_OPERATOR_EMAIL or EMAIL_TO must be set so the poller knows whose replies to trust",
        }
        _append_receipt(receipt)
        return [receipt]

    client, _transport, error = connect_imap(settings)
    if client is None:
        receipt = {
            "at": _now_iso(),
            "outcome": "imap_connect_failed",
            "reason": str(error or "unknown"),
        }
        _append_receipt(receipt)
        return [receipt]

    receipts: list[dict[str, Any]] = []
    try:
        status, _ = client.select(POLL_FOLDER)
        if status != "OK":
            receipt = {"at": _now_iso(), "outcome": "select_failed", "folder": POLL_FOLDER}
            _append_receipt(receipt)
            return [receipt]
        # Strict search: UNSEEN AND FROM the trusted operator email.
        # The body-marker check on each fetched message is the true
        # trust signal; the SUBJECT prefix filter was wrong here
        # because Gmail's SEARCH SUBJECT doesn't reliably match
        # encoded-word subjects across providers.
        search_query = f'(UNSEEN FROM "{trusted}")'
        status, ids_payload = client.search(None, search_query)
        if status != "OK":
            receipt = {"at": _now_iso(), "outcome": "search_failed", "query": search_query}
            _append_receipt(receipt)
            return [receipt]
        uids = (ids_payload[0] or b"").split()[:limit]
        if not uids:
            receipt = {
                "at": _now_iso(),
                "outcome": "no_new_replies",
                "trusted_sender": trusted,
            }
            _append_receipt(receipt)
            return [receipt]

        for uid in uids:
            uid_str = uid.decode("ascii", errors="replace")
            try:
                # BODY.PEEK[] fetches the message without setting the
                # \Seen flag. Plain RFC822 would auto-mark every fetched
                # message as read — a dry-run that "didn't write
                # anything" would still silently consume unread state.
                fetch_status, fetch_payload = client.fetch(uid, "(BODY.PEEK[])")
                if fetch_status != "OK" or not fetch_payload:
                    receipts.append(
                        {"at": _now_iso(), "uid": uid_str, "outcome": "fetch_failed"}
                    )
                    continue
                raw = next(
                    (item[1] for item in fetch_payload if isinstance(item, tuple) and len(item) >= 2),
                    None,
                )
                if not raw:
                    receipts.append(
                        {"at": _now_iso(), "uid": uid_str, "outcome": "fetch_empty"}
                    )
                    continue
                msg = email.message_from_bytes(raw)
                from_addr = _parse_address(msg.get("From", ""))
                # Belt + suspenders: re-verify From even though we
                # asked IMAP to filter; some servers honor From loosely.
                if from_addr != trusted:
                    receipts.append({
                        "at": _now_iso(),
                        "uid": uid_str,
                        "outcome": "skipped_untrusted_sender",
                        "from_addr": from_addr,
                    })
                    continue
                subject = _decode_header_value(msg.get("Subject", "")).strip()
                body = _extract_plain_text(msg).replace("\r\n", "\n")
                # Trust signal: the email body MUST contain the marker
                # the Duck Ops mailto templates emit. This filters out
                # system notifications (which the operator's own
                # address sends to itself) and arbitrary replies.
                if APPROVAL_METADATA_MARKER not in body:
                    receipts.append({
                        "at": _now_iso(),
                        "uid": uid_str,
                        "outcome": "skipped_missing_approval_marker",
                        "subject": subject,
                    })
                    continue
                # Visible command text (above the metadata footer) for
                # the receipt log; the actual dispatch passes the full
                # body so handle_mail_event can parse FLOW/RUN/ACTION.
                command_text = _strip_quoted_reply(_strip_metadata_footer(body))
                dispatch = (
                    _dispatch_mail_event(subject, body)
                    if not dry_run
                    else {"ok": True, "response": "(dry-run)"}
                )
                receipt = {
                    "at": _now_iso(),
                    "uid": uid_str,
                    "from_addr": from_addr,
                    "subject": subject,
                    "command_text": command_text,
                    "dry_run": dry_run,
                    "outcome": "ok" if dispatch.get("ok") else "dispatch_failed",
                    "dispatch": dispatch,
                }
                receipts.append(receipt)
                # Only mark Seen on successful real dispatch. Dry runs
                # and failures leave the email unread so the operator
                # can intervene.
                if dispatch.get("ok") and not dry_run:
                    client.store(uid, "+FLAGS", "\\Seen")
            except Exception as exc:
                receipts.append({
                    "at": _now_iso(),
                    "uid": uid_str,
                    "outcome": "process_exception",
                    "error": f"{type(exc).__name__}: {exc}",
                })
    finally:
        try:
            client.close()
        except Exception:
            pass
        try:
            client.logout()
        except Exception:
            pass

    for r in receipts:
        _append_receipt(r)
    return receipts


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll the operator inbox for Duck Ops command replies.")
    parser.add_argument("--dry-run", action="store_true", help="Parse replies but do not dispatch or mark Seen.")
    parser.add_argument("--limit", type=int, default=25, help="Max messages to process per run.")
    args = parser.parse_args()
    receipts = poll_inbox(dry_run=args.dry_run, limit=args.limit)
    print(json.dumps(receipts, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
