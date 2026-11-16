"""Tell the operator a drop is waiting, without telling them six times a day.

The drop scan runs 4-hourly. A digest that mailed on every scan with anything
pending would send six identical emails a day and be filtered inside a week,
which is the failure this is written to avoid rather than discover
([[feedback_cadence_bypass_keys_must_be_rare]] -- a bypass key that is almost
always true collapses a cadence).

So it sends when the SITUATION CHANGES, not when the state is non-empty:

  * something new is waiting that was not in the last email, or
  * nothing new, but the oldest item has crossed a day boundary we have not
    mentioned yet (1, 3, 7 days), or
  * nothing at all is waiting and the last email said something was -- the
    all-clear, so a resolved queue does not look like a dead job.

Everything else is silence, and silence here is correct: the OS card already
shows the count for anyone who looks.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parent
REPO_ROOT = RUNTIME_DIR.parent
DUCK_AGENT_ROOT = REPO_ROOT.parent / "duckAgent"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402
import drop_operator  # noqa: E402

STATE_PATH = REPO_ROOT / "state" / "drop_digest.json"
_FROZEN_PRODUCTION_STATE_PATH = (REPO_ROOT / "state" / "drop_digest.json").resolve()
_TEST_MODE_ENV = "DUCK_TEST_MODE"

#: Ages we will re-mail about, in days. Deliberately sparse: a nag every day
#: is a nag nobody reads, and the card carries the continuous signal.
AGE_MILESTONES = (1, 3, 7)


class TestModeRefusalError(RuntimeError):
    pass


def _write_state(payload: dict, path: Path | None = None) -> None:
    target = Path(path) if path is not None else STATE_PATH
    if (os.getenv(_TEST_MODE_ENV) == "1"
            and target.resolve() == _FROZEN_PRODUCTION_STATE_PATH):
        raise TestModeRefusalError(
            f"Refusing to write the production drop digest state ({target}) "
            f"under {_TEST_MODE_ENV}=1.")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, target)


def _read_state(path: Path | None = None) -> dict:
    target = Path(path) if path is not None else STATE_PATH
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _oldest_age_days(items: list[dict], now: datetime) -> float:
    ages = []
    for entry in items:
        try:
            stamp = datetime.fromisoformat(str(entry.get("received_at")))
            if stamp.tzinfo is None:
                stamp = stamp.astimezone()
            ages.append((now - stamp).total_seconds() / 86400.0)
        except Exception:
            continue
    return max(ages) if ages else 0.0


def _milestone(age_days: float) -> int:
    crossed = [m for m in AGE_MILESTONES if age_days >= m]
    return max(crossed) if crossed else 0


def decide(now: datetime | None = None, state_path: Path | None = None) -> dict:
    """Should we send, and why? Pure enough to test without a mailbox."""
    now = now or datetime.now().astimezone()
    proposals = drop_intake._read_ledger(drop_intake.DROP_PROPOSALS_PATH)
    pending = [dict(e, stem=s) for s, e in drop_operator._pending(proposals)]
    notes = drop_operator._unanswered_notes()
    stems = sorted(e["stem"] for e in pending)
    note_files = sorted(str(n.get("file")) for n in notes)

    prior = _read_state(state_path)
    seen = set(prior.get("stems") or []) | set(prior.get("note_files") or [])
    current = set(stems) | set(note_files)
    new_items = sorted(current - seen)

    age = _oldest_age_days(pending, now)
    milestone = _milestone(age)
    prior_milestone = int(prior.get("milestone") or 0)

    if not current:
        if prior.get("stems") or prior.get("note_files"):
            reason = "all_clear"
        else:
            reason = "nothing_waiting"
        return {"send": reason == "all_clear", "reason": reason,
                "stems": stems, "note_files": note_files,
                "new_items": [], "milestone": 0, "oldest_age_days": 0.0,
                "pending": pending, "notes": notes}

    if new_items:
        reason = "new_items"
    elif milestone > prior_milestone:
        reason = f"aged_past_{milestone}d"
    else:
        reason = "no_change"

    return {"send": reason != "no_change", "reason": reason,
            "stems": stems, "note_files": note_files,
            "new_items": new_items, "milestone": milestone,
            "oldest_age_days": round(age, 2),
            "pending": pending, "notes": notes}


def render_email(decision: dict, now: datetime | None = None) -> tuple[str, str, str]:
    now = now or datetime.now().astimezone()
    run_id = now.strftime("%Y-%m-%d")
    n = len(decision["stems"])
    notes = len(decision["note_files"])

    if decision["reason"] == "all_clear":
        subject = (f"MJD: [DROPS] queue clear | FLOW:drops | RUN:{run_id} "
                   "| ACTION:notify")
        text = ("Nothing from an agent is waiting on you any more.\n\n"
                "This is the all-clear, so a resolved queue does not look "
                "like a dead job.")
        return subject, text, f"<p>{text}</p>"

    bits = []
    if n:
        bits.append(f"{n} drop{'s' if n != 1 else ''}")
    if notes:
        bits.append(f"{notes} note{'s' if notes != 1 else ''}")
    subject = (f"MJD: [DROPS] {' and '.join(bits)} waiting | FLOW:drops "
               f"| RUN:{run_id} | ACTION:approve")

    body = drop_operator.format_waiting(decision["pending"], decision["notes"])
    text = (f"{body}\n\n"
            "Reply to this email with one of the lines above.\n\n"
            f"Oldest item: {decision['oldest_age_days']} day(s). "
            f"Sent because: {decision['reason']}.")
    html = ("<div style='font-family:system-ui,Arial;line-height:1.5'>"
            f"<pre style='white-space:pre-wrap;font-family:inherit'>{body}</pre>"
            "<p>Reply to this email with one of the lines above.</p>"
            f"<p style='color:#666;font-size:12px'>Oldest item: "
            f"{decision['oldest_age_days']} day(s). Sent because: "
            f"{decision['reason']}.</p></div>")
    return subject, text, html


def send(decision: dict, now: datetime | None = None) -> bool:
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
    except Exception:
        pass
    from helpers.email_helper import send_email  # type: ignore

    subject, text, html = render_email(decision, now=now)
    send_email(subject, html, text)
    return True


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="print the decision and the email, send nothing")
    args = ap.parse_args(argv)

    now = datetime.now().astimezone()
    decision = decide(now=now)
    print(f"[drop-digest] send={decision['send']} reason={decision['reason']} "
          f"drops={len(decision['stems'])} notes={len(decision['note_files'])}")

    if args.dry_run:
        subject, text, _ = render_email(decision, now=now)
        print(f"\nSubject: {subject}\n\n{text}")
        return 0

    if not decision["send"]:
        return 0

    send(decision, now=now)
    _write_state({
        "last_sent_at": now.isoformat(),
        "reason": decision["reason"],
        "stems": decision["stems"],
        "note_files": decision["note_files"],
        "milestone": decision["milestone"],
    })
    print("[drop-digest] sent")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
