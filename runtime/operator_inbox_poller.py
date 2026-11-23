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
import email.utils
import imaplib
import json
import os
import re
import subprocess
import sys
import time
import tempfile
from datetime import datetime, timedelta, timezone
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
# Folders polled, in order. INBOX is where a reply normally lands, but a
# reply that was archived or deleted before the poller ran is invisible
# there. On 2026-11-17 an approval sat in Trash for nine hours while the
# digest went on re-sending the same four drops; nothing in either
# direction noticed. Gmail's "[Gmail]/All Mail" deliberately EXCLUDES
# Trash and Spam -- measured, not assumed: it returned the same 2 unseen
# messages as INBOX while Trash held 174 -- so Trash has to be named.
POLL_FOLDERS = ("INBOX", '"[Gmail]/All Mail"', '"[Gmail]/Trash"')

# How far back the IMAP search looks. Unbounded is not an option once
# Trash is in scope: an oldest-first slice of 174 stale messages never
# reaches today's reply, so the window AND the newest-first ordering
# below are both load-bearing.
POLL_SINCE_DAYS = 7

# A reply older than this is REPORTED, never dispatched. The same Trash
# sweep also turned up an unread THURSDAY batch approval from five days
# earlier; widening the search without this guard would have published
# it on the next run. An approval is a statement about the moment it was
# written, and acting on a stale one is the failure that cannot be
# undone -- so this fails closed and leaves the message unread.
MAX_REPLY_AGE_HOURS = 48

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


#: A run of separator characters at the very end of the operator's text.
_SEPARATOR_TAIL = re.compile(r"\s+[-=_*~.]{2,}\s*$")


def _has_approval_marker(body: str) -> bool:
    # Gmail hard-wraps a reply's plain text near 78 columns. A long drop stem
    # pushed "DuckAgent approval metadata:" across a line break, so a real
    # approval (2026-11-22, thanksgiving_morning_drive_coffee_duck) was read as
    # an arbitrary email and never dispatched.
    return APPROVAL_METADATA_MARKER in " ".join(body.split())


def _strip_metadata_footer(body: str) -> str:
    """Trim the 'DuckAgent approval metadata:' footer so the receipt
    log's command_text field shows only the operator-visible command."""
    # Cut at the separator that precedes the metadata block, or at the
    # marker itself if the separator is missing.
    for marker in ("\n----\n", "\nDuckAgent approval metadata:"):
        idx = body.find(marker)
        if idx >= 0:
            body = body[:idx]
            break
    # Gmail rendered the digest's mailto body with the separator on the SAME
    # line as the command, so neither marker above matched and the receipt
    # log read `drop approve <stem> only ----`. The command handler now
    # tolerates that, but the receipt is what a human reads when asking
    # "what did they actually send", so it should not show furniture.
    return _SEPARATOR_TAIL.sub("", body).strip()


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


def _message_age_hours(msg: Message) -> float | None:
    """Hours since the reply was written, or None if it has no usable Date.

    None is treated as "too old to trust" by the caller rather than "fine":
    a reply we cannot date is a reply whose staleness we cannot rule out.
    """
    raw = msg.get("Date")
    if not raw:
        return None
    try:
        sent = email.utils.parsedate_to_datetime(str(raw))
    except (TypeError, ValueError):
        return None
    if sent is None:
        return None
    if sent.tzinfo is None:
        sent = sent.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - sent).total_seconds() / 3600.0


def _search_newest_first(client: Any, trusted: str) -> tuple[list[bytes], str]:
    """UIDs of candidate replies in the selected folder, newest first.

    IMAP returns ascending sequence numbers, i.e. oldest first, and the
    caller slices to a limit -- so without the reverse a backlog of stale
    messages starves the reply that actually matters.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=POLL_SINCE_DAYS)).strftime("%d-%b-%Y")
    query = f'(UNSEEN FROM "{trusted}" SINCE {since})'
    status, ids_payload = client.search(None, query)
    if status != "OK":
        return [], query
    uids = (ids_payload[0] or b"").split()
    uids.reverse()
    return uids, query


def _poll_folder(
    client: Any,
    folder: str,
    *,
    trusted: str,
    limit: int,
    dry_run: bool,
    seen_message_ids: set[str],
) -> list[dict[str, Any]]:
    """Process up to `limit` candidate replies in one folder.

    `seen_message_ids` is shared across folders and carries Message-ID
    values already handled this run. Gmail surfaces one message in several
    folders with a different UID in each, so without it an INBOX reply is
    dispatched a second time from All Mail.
    """
    receipts: list[dict[str, Any]] = []
    status, _ = client.select(folder)
    if status != "OK":
        # A missing folder is not a failure -- a non-Gmail mailbox has no
        # "[Gmail]/Trash" -- but it IS recorded, so a provider that
        # renames them does not quietly narrow the search back to INBOX.
        receipts.append({"at": _now_iso(), "outcome": "folder_unavailable", "folder": folder})
        return receipts

    uids, query = _search_newest_first(client, trusted)
    if not uids:
        receipts.append({
            "at": _now_iso(),
            "outcome": "no_new_replies",
            "folder": folder,
            "trusted_sender": trusted,
        })
        return receipts

    for uid in uids[:limit]:
        uid_str = uid.decode("ascii", errors="replace")
        try:
            # BODY.PEEK[] fetches the message without setting the
            # \Seen flag. Plain RFC822 would auto-mark every fetched
            # message as read — a dry-run that "didn't write
            # anything" would still silently consume unread state.
            fetch_status, fetch_payload = client.fetch(uid, "(BODY.PEEK[])")
            if fetch_status != "OK" or not fetch_payload:
                receipts.append(
                    {"at": _now_iso(), "uid": uid_str, "folder": folder, "outcome": "fetch_failed"}
                )
                continue
            raw = next(
                (item[1] for item in fetch_payload if isinstance(item, tuple) and len(item) >= 2),
                None,
            )
            if not raw:
                receipts.append(
                    {"at": _now_iso(), "uid": uid_str, "folder": folder, "outcome": "fetch_empty"}
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
                    "folder": folder,
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
            if not _has_approval_marker(body):
                receipts.append({
                    "at": _now_iso(),
                    "uid": uid_str,
                    "folder": folder,
                    "outcome": "skipped_missing_approval_marker",
                    "subject": subject,
                })
                continue

            message_id = str(msg.get("Message-ID", "")).strip()
            if message_id and message_id in seen_message_ids:
                receipts.append({
                    "at": _now_iso(),
                    "uid": uid_str,
                    "folder": folder,
                    "outcome": "skipped_duplicate_folder_copy",
                    "subject": subject,
                    "message_id": message_id,
                })
                continue

            age_hours = _message_age_hours(msg)
            if age_hours is None or age_hours > MAX_REPLY_AGE_HOURS:
                # Left UNREAD on purpose: an undispatched approval the
                # operator can still see beats one silently acted on a
                # week late. The drop-claim canary reads this outcome.
                receipts.append({
                    "at": _now_iso(),
                    "uid": uid_str,
                    "folder": folder,
                    "outcome": "skipped_stale_reply",
                    "subject": subject,
                    "age_hours": round(age_hours, 2) if age_hours is not None else None,
                    "max_age_hours": MAX_REPLY_AGE_HOURS,
                    "command_text": _strip_quoted_reply(_strip_metadata_footer(body)),
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
                "folder": folder,
                "from_addr": from_addr,
                "subject": subject,
                "command_text": command_text,
                "age_hours": round(age_hours, 2),
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
                if message_id:
                    seen_message_ids.add(message_id)
        except Exception as exc:
            receipts.append({
                "at": _now_iso(),
                "uid": uid_str,
                "folder": folder,
                "outcome": "process_exception",
                "error": f"{type(exc).__name__}: {exc}",
            })
    return receipts


def poll_inbox(*, dry_run: bool = False, limit: int = 25) -> list[dict[str, Any]]:
    """Connect to the operator's mailbox, process up to `limit` unread
    operator replies PER FOLDER, and return a list of receipt dicts (one
    per message touched). Each receipt is also appended to the receipt log."""
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

    client, error, attempts = _connect_with_retry(settings)
    if client is None:
        receipt = {
            "at": _now_iso(),
            "outcome": "imap_connect_failed",
            "reason": str(error or "unknown"),
            "attempts": attempts,
        }
        _append_receipt(receipt)
        return [receipt]

    receipts: list[dict[str, Any]] = []
    seen_message_ids: set[str] = set()
    try:
        for folder in POLL_FOLDERS:
            receipts.extend(_poll_folder(
                client, folder,
                trusted=trusted, limit=limit, dry_run=dry_run,
                seen_message_ids=seen_message_ids,
            ))
            try:
                client.close()
            except Exception:
                pass
    finally:
        try:
            client.logout()
        except Exception:
            pass

    for r in receipts:
        _append_receipt(r)
    # Publish the reading the OS card grades. Never fatal: a poll that
    # delivered an approval must not be reported as a failure because a
    # state file could not be written.
    try:
        import operator_reply_health  # local import: optional consumer

        operator_reply_health.write_state()
    except Exception as exc:  # noqa: BLE001
        print(f"[inbox-poller] WARNING reply-health state not written: {exc}",
              file=sys.stderr)
    return receipts

#: Gmail refuses a connection when too many are already open against one
#: account. WHY there were too many on 2026-11-20 is unknown: measured right
#: after, `lsof -iTCP:993` showed a single connection, held by our own
#: long-lived publish watcher, which closes cleanly. So the spike was
#: transient and unattributed -- which is the argument FOR a retry rather than
#: against one, and the argument for the snapshot below.
#: That refusal is TRANSIENT and it cost a full six-hour cycle on 2026-11-20:
#: one `[ALERT] Too many simultaneous connections` at 18:16 meant an approval
#: reply the operator had already sent went unread until the next run.
#:
#: Three attempts, backing off, is the whole fix. The budget is deliberately
#: small: this runs inside a sidecar with other steps behind it, and a poller
#: that hangs for minutes retrying is its own outage.
IMAP_CONNECT_ATTEMPTS = 3
IMAP_CONNECT_BACKOFF_SECONDS = (5.0, 15.0)


def imap_connection_snapshot() -> dict[str, Any]:
    """Who holds an IMAP connection right now, by process.

    Recorded ONLY when a connect fails, and only to make the NEXT occurrence
    attributable. The 2026-11-20 refusal could not be explained after the
    fact: by the time anyone looked, the spike was gone and a single
    connection was open. A count taken at the moment of failure is the one
    observation that would have settled it.

    Never raises and never blocks the retry: this is evidence gathering, and
    evidence capture must not fail the thing it documents
    ([[feedback_receipts_never_load_bearing]]).
    """
    out: dict[str, Any] = {"at": _now_iso()}
    try:
        import collections
        import subprocess

        proc = subprocess.run(
            ["lsof", "-nP", "-iTCP:993", "-sTCP:ESTABLISHED"],
            capture_output=True, text=True, timeout=10, check=False)
        lines = [ln for ln in proc.stdout.splitlines()[1:] if ln.strip()]
        by_command = collections.Counter(ln.split()[0] for ln in lines)
        out["established"] = len(lines)
        out["by_process"] = dict(by_command)
    except Exception as exc:  # noqa: BLE001
        out["detail"] = f"{type(exc).__name__}: {exc}"
    return out


def _connect_with_retry(settings: dict[str, Any], *,
                        sleep=None,
                        ) -> tuple[Any, str | None, list[dict[str, Any]]]:
    """Connect, retrying a transient refusal. Returns (client, error, log).

    Retries EVERY connect failure rather than matching on the error text. A
    missing credential costs 20 wasted seconds once every six hours, which is
    nothing; a string match that stops recognising Gmail's wording costs the
    whole fix, silently ([[feedback_pin_the_rule_not_the_reading]]).
    """
    # Resolved at CALL time, not captured as a default: a default of
    # `time.sleep` is bound when this function is defined, so patching
    # `poller.time` cannot reach it and a test that thinks it stubbed the
    # wait really waits twenty seconds (which is how this was found).
    sleep = sleep or time.sleep
    attempts: list[dict[str, Any]] = []
    error: str | None = None
    for index in range(max(1, IMAP_CONNECT_ATTEMPTS)):
        client, _transport, error = connect_imap(settings)
        attempts.append({
            "at": _now_iso(),
            "attempt": index + 1,
            "ok": client is not None,
            "error": None if client is not None else str(error or "unknown"),
        })
        if client is not None:
            if index:
                # Loud about a recovery too: a run that needed three tries is
                # healthy today and a warning about tomorrow.
                print(f"[operator-inbox-poller] connected on attempt "
                      f"{index + 1} of {IMAP_CONNECT_ATTEMPTS}", file=sys.stderr)
            return client, None, attempts
        # Taken on the FIRST failure only: by the third attempt the picture
        # has changed, and the question is what was open when it refused.
        if not index:
            attempts[-1]["connections"] = imap_connection_snapshot()
        if index < IMAP_CONNECT_ATTEMPTS - 1:
            delay = IMAP_CONNECT_BACKOFF_SECONDS[
                min(index, len(IMAP_CONNECT_BACKOFF_SECONDS) - 1)]
            print(f"[operator-inbox-poller] connect attempt {index + 1} "
                  f"failed ({error}); retrying in {delay:.0f}s",
                  file=sys.stderr)
            sleep(delay)
    return None, error, attempts


#: Outcomes that mean THE MAILBOX WAS NEVER READ. A run that ends in one of
#: these did not do its job, and until 2026-11-20 it still exited 0 -- so the
#: sidecar's own `POLLER_FAILED` check could never fire and scheduler_health
#: graded the run healthy.
#:
#: Measured that evening: the 18:16 run hit Gmail's
#: "[ALERT] Too many simultaneous connections", read nothing, exited 0, and the
#: sidecar logged exit=0. The operator had sent an approval reply and spent an
#: hour asking why the lane had not picked it up, while every surface said the
#: poller was fine ([[feedback_swallowed_errors_lie]] -- a swallowed failure
#: needs a loud channel, and here the loud channel already existed and was
#: simply never given a non-zero code to notice).
#:
#: Deliberately NOT in this set: every `skipped_*` outcome, `no_new_replies`
#: and a refused command. Those are a mailbox that WAS read and had nothing to
#: act on, which is the normal state three times out of four -- failing on
#: them would make a quiet inbox look like a broken job, the mistake
#: `run_drop_intake.sh` already documents for refused drops.
MAILBOX_UNREACHED_OUTCOMES = frozenset({
    "imap_connect_failed",
    "mailbox_disabled",
    "no_trusted_sender",
})


def unreached_outcomes(receipts: list[dict[str, Any]]) -> list[str]:
    """Which 'never read the mailbox' outcomes a run ended in, if any."""
    return [str(r.get("outcome")) for r in receipts
            if isinstance(r, dict)
            and str(r.get("outcome")) in MAILBOX_UNREACHED_OUTCOMES]


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll the operator inbox for Duck Ops command replies.")
    parser.add_argument("--dry-run", action="store_true", help="Parse replies but do not dispatch or mark Seen.")
    parser.add_argument("--limit", type=int, default=25, help="Max messages to process per run.")
    args = parser.parse_args()
    receipts = poll_inbox(dry_run=args.dry_run, limit=args.limit)
    print(json.dumps(receipts, indent=2, ensure_ascii=False))
    unreached = unreached_outcomes(receipts)
    if unreached:
        # Loud on stderr AND non-zero: the wrapper greps for neither, it
        # checks the exit code, and scheduler_health grades the wrapper.
        print(f"[operator-inbox-poller] MAILBOX NOT READ ({', '.join(unreached)}): "
              "no approval reply could have been seen this run. Every pending "
              "approval is still waiting.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
