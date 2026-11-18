"""Is the operator's reply channel actually carrying replies?

Built 2026-11-17, after an approval the operator had sent at 08:40 sat
unread in Gmail's Trash while the digest went on re-sending the same four
drops. Both halves of the loop looked fine from where they stood: the
digest had sent, the operator had replied, and the poller -- which only
ever selected INBOX -- truthfully reported "no new replies".

Nothing read the two together. That is this module's whole job, and it is
deliberately the ONLY implementation: the digest renders it into the email
and the OS card grades it, so a surface cannot disagree with an email
about whether an answer arrived ([[feedback_two_card_observability_bracket]]).

Everything here is derived from `state/operator_inbox_receipts.jsonl`,
which the poller appends to on every message it touches -- including the
ones it refuses. A refusal is the interesting record.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
RECEIPT_PATH = REPO_ROOT / "state" / "operator_inbox_receipts.jsonl"

#: Producer-on-schedule + cheap-reader: the poller writes this after every
#: run, the OS card reads it in ~1ms. The card cannot parse the receipt log
#: itself without carrying a second copy of the rules above, and two copies
#: of a rule is how a surface ends up disagreeing with an email about
#: whether an answer arrived.
STATE_PATH = REPO_ROOT / "state" / "operator_reply_health.json"
_FROZEN_PRODUCTION_STATE_PATH = (
    REPO_ROOT / "state" / "operator_reply_health.json").resolve()
_TEST_MODE_ENV = "DUCK_TEST_MODE"


class TestModeRefusalError(RuntimeError):
    """Raised when a test would write the real production state file."""

#: The receipt log is append-only and already 2,700 lines. Reading the tail
#: bounds the work: a run that matters is always at the end, and a question
#: about a reply from six months ago is not a health question.
TAIL_BYTES = 512 * 1024

#: Outcomes that mean a real operator command reached DuckAgent.
_DISPATCHED = {"ok"}

#: Outcomes that mean a reply ARRIVED and was deliberately not acted on.
#: These are the ones worth saying out loud -- a refusal nobody reads is
#: the same as a message nobody received.
_REFUSED = {"skipped_stale_reply", "dispatch_failed", "process_exception"}

#: How recent a refusal has to be to count as news. A dispatch_failed from
#: seven weeks ago is history, not a thing to act on, and surfacing it beside
#: a drop that arrived a minute ago teaches the operator to skip the line.
REFUSAL_RECENT_HOURS = 72

#: Any folder other than INBOX means the reply was archived or deleted
#: before the poller reached it. It still worked, but the operator's
#: mailbox is eating approvals and that is worth knowing before it eats
#: one we cannot recover.
_NORMAL_FOLDER = "INBOX"


def _tail_lines(path: Path) -> list[str]:
    try:
        size = path.stat().st_size
    except OSError:
        return []
    try:
        with path.open("rb") as fh:
            if size > TAIL_BYTES:
                fh.seek(size - TAIL_BYTES)
                fh.readline()  # discard a partial first line
            raw = fh.read()
    except OSError:
        return []
    return raw.decode("utf-8", errors="replace").splitlines()


def _parse_at(value: object) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _age_hours(dt: datetime | None, now: datetime) -> float | None:
    if dt is None:
        return None
    return round((now - dt).total_seconds() / 3600.0, 2)


def read(receipt_path: Path | None = None,
         *, now: datetime | None = None) -> dict[str, Any]:
    """A reading of the reply channel. Never raises.

    A missing or unreadable log yields `polled_ever: False` rather than an
    exception or a zero: "we have no measurement" and "nothing arrived" are
    different facts, and a card that conflates them reports health it never
    checked ([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).
    """
    now = now or datetime.now(timezone.utc).astimezone()
    path = Path(receipt_path) if receipt_path is not None else RECEIPT_PATH

    last_poll: datetime | None = None
    last_dispatch: datetime | None = None
    last_dispatch_subject: str | None = None
    refused: list[dict[str, Any]] = []
    outside_inbox: list[dict[str, Any]] = []
    rows = 0

    for line in _tail_lines(path):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        # A --dry-run receipt records outcome "ok" and dispatches nothing.
        # Counting it as delivery made this helper report "your last reply
        # was processed 0h ago" the moment someone probed the poller, which
        # is precisely the false reassurance it exists to prevent
        # ([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).
        if row.get("dry_run"):
            continue
        rows += 1
        at = _parse_at(row.get("at"))
        if at and (last_poll is None or at > last_poll):
            last_poll = at
        outcome = str(row.get("outcome") or "")
        if outcome in _DISPATCHED:
            if at and (last_dispatch is None or at > last_dispatch):
                last_dispatch = at
                last_dispatch_subject = row.get("subject")
            folder = str(row.get("folder") or _NORMAL_FOLDER)
            if folder != _NORMAL_FOLDER:
                outside_inbox.append({
                    "at": row.get("at"), "folder": folder,
                    "subject": row.get("subject"),
                    "command_text": row.get("command_text"),
                })
        elif outcome in _REFUSED:
            refused.append({
                "at": row.get("at"), "outcome": outcome,
                "folder": row.get("folder"),
                "subject": row.get("subject"),
                "command_text": row.get("command_text"),
                "age_hours": row.get("age_hours"),
            })

    # Only the most recent refusals are actionable, and a stale reply is
    # re-reported on every run until it ages out of the search window, so
    # collapse by command text and keep the newest.
    deduped: dict[str, dict[str, Any]] = {}
    for item in refused:
        key = str(item.get("command_text") or item.get("subject") or item.get("at"))
        prior = deduped.get(key)
        if prior is None or str(item.get("at") or "") > str(prior.get("at") or ""):
            deduped[key] = item

    recent: list[dict[str, Any]] = []
    stale_count = 0
    for item in deduped.values():
        age = _age_hours(_parse_at(item.get("at")), now)
        if age is not None and age <= REFUSAL_RECENT_HOURS:
            item["refused_age_hours"] = age
            recent.append(item)
        else:
            stale_count += 1

    return {
        "receipt_path": str(path),
        "polled_ever": rows > 0,
        "rows_read": rows,
        "last_poll_at": last_poll.isoformat() if last_poll else None,
        "last_poll_age_hours": _age_hours(last_poll, now),
        "last_dispatch_at": last_dispatch.isoformat() if last_dispatch else None,
        "last_dispatch_age_hours": _age_hours(last_dispatch, now),
        "last_dispatch_subject": last_dispatch_subject,
        "refused": sorted(recent,
                          key=lambda r: str(r.get("at") or ""), reverse=True),
        "refused_older_count": stale_count,
        "dispatched_outside_inbox": outside_inbox[-5:],
    }


def describe_for_operator(health: dict[str, Any]) -> str:
    """One plain sentence for an email. Says the measurement, not a verdict."""
    if not health.get("polled_ever"):
        return ("No reply has ever been read from your mailbox — the poller "
                "has no receipts, so it may not be running at all.")
    refused = health.get("refused") or []
    if refused:
        first = refused[0]
        cmd = str(first.get("command_text") or "").strip() or "(no command text)"
        return (f"A reply reached the mailbox and was NOT acted on: "
                f"\"{cmd}\" ({first.get('outcome')}). It is still unread.")
    age = health.get("last_dispatch_age_hours")
    if age is None:
        return ("No reply from you has ever been processed, though the poller "
                "is running. If you have replied, it never arrived.")
    if age < 24:
        return f"Your last reply was processed {age:.0f}h ago."
    return (f"Nothing from you has been processed in {age / 24:.0f} days — "
            "if you replied to an earlier one of these, it did not arrive.")


def write_state(health: dict[str, Any] | None = None,
                path: Path | None = None) -> Path:
    """Persist a reading for the OS card. Atomic; refuses in test mode.

    The frozen-path guard is the third layer the house rule asks for: a test
    that forgets to monkeypatch STATE_PATH would otherwise overwrite the real
    file with fixture data and turn the card red on nothing
    ([[feedback_test_isolation_from_production_state]]).
    """
    target = Path(path) if path is not None else STATE_PATH
    in_test_mode = str(os.environ.get(_TEST_MODE_ENV) or "").strip() in {
        "1", "true", "TRUE", "yes"}
    if in_test_mode and target.resolve() == _FROZEN_PRODUCTION_STATE_PATH:
        raise TestModeRefusalError(
            f"refusing to write production state {target} under "
            f"{_TEST_MODE_ENV}=1 -- monkeypatch STATE_PATH in the test")
    payload = dict(health if health is not None else read())
    payload["generated_at"] = datetime.now(timezone.utc).astimezone().isoformat()
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent),
                                    prefix=".operator_reply_health.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp_name, target)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise
    return target
