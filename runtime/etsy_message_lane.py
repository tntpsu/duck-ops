#!/usr/bin/env python3
"""Is Duck Ops doing anything with Etsy *messages*? One place that answers it.

Etsy messages and Etsy reviews are two different lanes that happen to share a
browser session, and conflating them is the mistake this module exists to make
impossible. On 2026-11-18 the operator switched the message lane off -- their
own Grok bot answers Etsy messages now -- and explicitly kept the REVIEW lane
replying. Nothing here touches `review_reply`.

Both switches live in config/etsy_browser_batch.json:

  customer_read.enabled           -- may we OPEN message threads at all
  customer_message_reply.enabled  -- may we stage, type or send a reply

Two accessors with deliberately OPPOSITE defaults when the config cannot be
read, because the safe answer depends entirely on what is being asked:

  replies_allowed()  fails CLOSED. An unreadable switch must never authorise
                     typing into a customer's thread ("fail closed when
                     approval state is uncertain", ai-agents/CLAUDE.md).
  lane_is_off()      fails OPEN (i.e. "not off"). An unreadable switch must
                     never silently park a real backlog of waiting customers
                     ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]]).

Getting those two the same way round is how you either post an unapproved
reply or hide a queue. They are tested in both directions.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONFIG_FILENAME = "etsy_browser_batch.json"
READ_KEY = "customer_read"
REPLY_KEY = "customer_message_reply"

_DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


@dataclass(frozen=True)
class MessageLanePosture:
    reads_enabled: bool
    replies_enabled: bool
    #: True when the lane does no work at all, for grading purposes only.
    off: bool
    #: The operator's own stated reason, verbatim. Surfaces quote this rather
    #: than inventing their own wording -- the morning's content_publish_cadence
    #: fix is the precedent.
    note: str | None
    resume_hint: str | None
    #: True when the config was readable. A surface that cannot tell should say
    #: so rather than render a confident "all clear".
    known: bool


def _section(config_payload: Any, key: str) -> dict:
    if not isinstance(config_payload, dict):
        return {}
    section = config_payload.get(key)
    return section if isinstance(section, dict) else {}


def _explicitly_disabled(section: dict) -> bool:
    """Only a real boolean False disables. A string "false" is a typo, and
    reading a typo as a decision is how a lane turns itself off."""
    value = section.get("enabled")
    return isinstance(value, bool) and value is False


def _first_note(*sections: dict) -> tuple[str | None, str | None]:
    for section in sections:
        note = str(section.get("disabled_because") or "").strip()
        if note:
            return note, str(section.get("resume_hint") or "").strip() or None
    return None, None


def read_message_lane(config_payload: Any) -> MessageLanePosture:
    read = _section(config_payload, READ_KEY)
    reply = _section(config_payload, REPLY_KEY)
    known = isinstance(config_payload, dict) and bool(config_payload)
    reads_off = _explicitly_disabled(read)
    replies_off = _explicitly_disabled(reply)
    # The reply section is named first: when both are off its reason is the
    # one that answers "why is nobody answering this customer?".
    note, resume = _first_note(reply, read)
    return MessageLanePosture(
        reads_enabled=not reads_off,
        replies_enabled=not replies_off,
        off=reads_off or replies_off,
        note=note,
        resume_hint=resume,
        known=known,
    )


def load_message_lane(config_dir: Path | None = None) -> MessageLanePosture:
    path = (config_dir or _DEFAULT_CONFIG_DIR) / CONFIG_FILENAME
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return MessageLanePosture(True, True, False, None, None, known=False)
    return read_message_lane(payload)


def replies_allowed(config_dir: Path | None = None) -> bool:
    """May we stage, type or send a reply to an Etsy MESSAGE right now?

    Fails CLOSED: an unreadable or absent config returns False. Refusing a
    reply the operator wanted costs one error message; sending one they had
    switched off is not recoverable.
    """
    path = (config_dir or _DEFAULT_CONFIG_DIR) / CONFIG_FILENAME
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    section = _section(payload, REPLY_KEY)
    return section.get("enabled") is True


def lane_is_off(config_dir: Path | None = None) -> bool:
    """Is the lane switched off, for GRADING? Fails open (returns False when
    it cannot tell), so an unreadable config never parks a live backlog."""
    return load_message_lane(config_dir).off


def refusal_message(posture: MessageLanePosture) -> str:
    """What to tell the operator when they ask us to reply anyway."""
    lines = [
        "Etsy message replies are switched off, so I did not stage anything.",
    ]
    if posture.note:
        lines.append(f"- Why: {posture.note}")
    elif not posture.known:
        lines.append("- Why: I could not read "
                     f"config/{CONFIG_FILENAME}, so I am refusing rather than guessing.")
    lines.append(f"- Switch: config/{CONFIG_FILENAME} -> {REPLY_KEY}.enabled")
    if posture.resume_hint:
        lines.append(f"- To resume: {posture.resume_hint}")
    lines.append("- Etsy REVIEW replies are a separate lane and are unaffected.")
    return "\n".join(lines)


def operator_surface_phrase(posture: MessageLanePosture) -> str | None:
    """One line for an operator surface that would otherwise render a silent
    'nothing to do'. None when the lane is running normally.

    An empty section and a switched-off section look identical, and the
    operator asked for the difference to be visible: "Update cards so it
    doesn't look like something is broken."
    """
    if not posture.off:
        return None
    what = []
    if not posture.replies_enabled:
        what.append("replies")
    if not posture.reads_enabled:
        what.append("inbox reads")
    detail = " and ".join(what) or "this lane"
    phrase = f"Etsy message {detail} are off by operator decision"
    if posture.note:
        # First sentence only. The config holds the full reasoning; a surface
        # line that runs to four sentences stops being read at all
        # ([[feedback_no_silent_caps]] -- derive the copy from what fits).
        headline = posture.note.split(". ")[0].rstrip(".")
        phrase += f" — {headline}"
    if not phrase.endswith("."):
        phrase += "."
    return phrase + " Etsy review replies are unaffected."
