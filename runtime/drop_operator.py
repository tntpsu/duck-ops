"""Operator commands for the agent drop lane, so approving needs no shell.

Everything the drop lane produces has been invisible to the operator unless
they were at a terminal: three drops sat awaiting approval while the only
thing that could surface them was a CLI flag. Every other gated lane here is
approvable by replying to an email, because `operator_inbox_poller` pulls the
reply text out of the message and hands it to `review_loop.handle_operator_text`.
This module is what that text can now say.

Namespaced under `drop ` for the same reason the business desk is namespaced
under `desk `: `approve` already appears 27 times in review_loop's scope, and
a bare `approve <stem>` would collide with the review queue's own numbered
approvals ([[feedback_edits_into_shared_scope_collide]]). `drop approve` can
only ever mean one thing.

Replies are OPERATOR input, never an agent's. Nothing here is reachable from
the drop folder: an agent can ask for something in `feedback/`, and a human
still has to type the word that acts on it.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

RUNTIME_DIR = Path(__file__).resolve().parent
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402

HELP = (
    "Agent drop commands:\n"
    "  drops                       what is waiting on you\n"
    "  drop approve <stem>         let its lane publish it\n"
    "  drop approve <stem> only    approve it and decline the rest in that lane\n"
    "  drop approve <stem> now     express drops only: send it immediately,\n"
    "                              ignoring the time the agent asked for\n"
    "  drop decline <stem>         never publish it\n"
    "  drop answer <file> <text>   reply to an agent's note\n"
    "\n'only' and 'now' can be combined in either order.\n"
    "Several lines in one reply are fine; each is run in order.\n"
)

#: Suffix words that modify an approval rather than naming the drop. Stripped
#: as TOKENS from the end, in any order, for the same reason the separator
#: strip is token-wise: stems end in things like `_v4` and `_now_2`, and a
#: character-wise or anywhere-in-the-string match would eat a real suffix.
_APPROVE_FLAGS = {"only", "now"}


def _split_approve_flags(target: str) -> tuple[str, set[str]]:
    """("<stem>", {"only", "now"}) from "<stem> now only"."""
    tokens = (target or "").split()
    flags: set[str] = set()
    while len(tokens) > 1 and tokens[-1].lower() in _APPROVE_FLAGS:
        flags.add(tokens.pop().lower())
    return " ".join(tokens), flags


def should_delegate_to_drops(text: str) -> bool:
    raw = (text or "").strip().lower()
    if not raw:
        return False
    return raw in {"drop", "drops"} or raw.startswith(("drop ", "drops "))


def _pending(proposals: dict) -> list[tuple[str, dict]]:
    return [(stem, e) for stem, e in sorted(proposals.items())
            if isinstance(e, dict)
            and str(e.get("approval") or "") == "operator_required"
            and not e.get("claimed_by")]


def _status() -> str:
    proposals = drop_intake._read_ledger(drop_intake.DROP_PROPOSALS_PATH)
    return format_waiting(_pending(proposals), _unanswered_notes())


def format_waiting(pending: list, notes: list) -> str:
    """Render what is waiting, from DATA rather than from a fresh read.

    The digest renders the same text, and a second read there could disagree
    with the one its decision was made from -- an email saying three drops
    wait while the decision counted two. Hand it the data.
    """
    pending = [(e.get("stem"), e) if isinstance(e, dict) else e for e in pending]
    lines: list[str] = []
    if not pending:
        lines.append("No agent drop is waiting on you.")
    else:
        lines.append(f"{len(pending)} agent drop(s) waiting on you:")
        for stem, entry in pending:
            ready = "" if entry.get("post_ready") else "  [post_ready:false — no run will take it]"
            lines.append(f"  {stem}")
            lines.append(f"      lane={entry.get('lane')} from={entry.get('source_agent')}{ready}")
            # An express drop's WHEN is the operator's decision too, not just
            # its whether: approving one whose time has passed does nothing,
            # and that has to be visible before the reply is typed.
            window, when = drop_intake.publish_window(entry)
            if window == drop_intake.PUBLISH_AT_ASAP:
                lines.append("      when: immediately on approval")
            elif window == "scheduled" and when is not None:
                lines.append(f"      when: {when.isoformat()}")
            elif window in {"missed", "unreadable"}:
                lines.append(
                    "      when: REQUESTED TIME HAS PASSED — approving alone "
                    f"sends nothing; use 'drop approve {stem} now'")
            caption = str(entry.get("caption") or "").strip().replace("\n", " ")
            if caption:
                lines.append(f"      \"{caption[:110]}\"")
            lines.append(f"      reply: drop approve {stem}")

    if notes:
        lines.append("")
        lines.append(f"{len(notes)} unanswered note(s) from an agent:")
        for note in notes:
            lines.append(f"  {note.get('file')} — {note.get('subject') or '(no subject)'}")
            body = str(note.get("body") or "").strip().replace("\n", " ")
            if body:
                lines.append(f"      \"{body[:140]}\"")
            lines.append(f"      reply: drop answer {note.get('file')} <your answer>")
    return "\n".join(lines)


def _unanswered_notes() -> list[dict]:
    path = drop_intake.DROP_FEEDBACK_PATH
    if not Path(path).exists():
        return []
    payload = drop_intake._read_json_file(Path(path))
    notes = payload.get("notes")
    notes = notes if isinstance(notes, list) else []
    return [n for n in notes if isinstance(n, dict) and not n.get("answered_at")]


#: Tokens made only of these are mail-template furniture, never part of a
#: stem. Gmail rendered the digest's reply body with the `----` separator on
#: the SAME line as the command, so the real 2026-11-17 approval arrived as
#: `drop approve <stem> only ----` -- and the whole command was refused
#: because the target no longer ended in " only". Exit 0, nothing applied.
_SEPARATOR_CHARS = set("-=_*~.")


def _strip_trailing_separator(target: str) -> str:
    """Drop trailing separator tokens from a command's target.

    Token-wise, never character-wise: stems end in things like `_v4`, and a
    char-wise strip would eat a legitimate suffix.
    """
    tokens = (target or "").split()
    while tokens:
        last = tokens[-1]
        if len(last) >= 2 and set(last) <= _SEPARATOR_CHARS:
            tokens.pop()
            continue
        break
    return " ".join(tokens)


def _resolve_stem(token: str, proposals: dict) -> tuple[str | None, str | None]:
    """Exact match, else a unique prefix.

    Stems are long and an operator typing one on a phone will shorten it. An
    AMBIGUOUS prefix is refused rather than guessed: the stems in play right
    now differ only by a `_v2`/`_v3` suffix, and picking the wrong one
    publishes the wrong cut of the same video.
    """
    token = (token or "").strip()
    if not token:
        return None, "name the drop: drop approve <stem>"
    if token in proposals:
        return token, None
    hits = [s for s in proposals if s.startswith(token)]
    if len(hits) == 1:
        return hits[0], None
    if not hits:
        return None, f"no drop named {token!r}. Reply 'drops' to see what is waiting."
    return None, (f"{token!r} matches {len(hits)} drops — "
                  + ", ".join(sorted(hits)[:4]) + ". Use the full stem.")


def handle_drop_text(text: str) -> str:
    """Run every `drop ...` line in the reply, not just the first.

    An agent that re-sends a corrected version leaves the earlier ones
    pending, so clearing a queue routinely means several decisions at once --
    and the operator's answer to being told to send four separate emails was
    a flat "I don't want to send 4 emails".
    """
    return apply_drop_text(text)["response"]


def apply_drop_text(text: str) -> dict:
    """`handle_drop_text` plus whether anything was actually applied.

    The email path needs the count, not the prose. `operator_inbox_poller`
    marks a reply Seen on a successful dispatch, and on 2026-11-17 a
    dispatch that refused every command returned exit 0 -- so the
    operator's approval was consumed and discarded in one step. Deciding
    that from the response TEXT would mean grepping our own copy for its
    wording ([[feedback_pin_the_rule_not_the_reading]]); this is the
    structured answer instead.
    """
    lines = [ln.strip() for ln in str(text or "").splitlines()]
    commands = [ln for ln in lines if should_delegate_to_drops(ln)]
    if len(commands) > 1:
        out: list[str] = []
        applied = 0
        for line in commands:
            message, changed = _handle_one(line)
            out.append(f"> {line}")
            out.append(message)
            applied += 1 if changed else 0
        return {"response": "\n".join(out), "applied": applied,
                "commands": len(commands)}
    message, changed = _handle_one(commands[0] if commands else text)
    return {"response": message, "applied": 1 if changed else 0,
            "commands": len(commands)}


def _decline_siblings(stem: str, proposals: dict) -> list[str]:
    """Decline every OTHER pending drop in the same lane.

    Superseded versions are the normal case, not an edge one: a corrected
    re-send leaves its predecessors sitting in the ledger, ageing through
    every digest milestone until someone clears them by hand.
    """
    lane = (proposals.get(stem) or {}).get("lane")
    declined = []
    for other, entry in sorted(proposals.items()):
        if other == stem or not isinstance(entry, dict):
            continue
        if entry.get("lane") != lane or entry.get("claimed_by"):
            continue
        if str(entry.get("approval") or "") != "operator_required":
            continue
        try:
            drop_intake.set_approval(other, False, by="operator (superseded)")
            declined.append(other)
        except drop_intake.DropRefused:
            continue
    return declined


def _what_happens_next(stem: str, lane: Any, *, publish_now: bool) -> str:
    """What the operator should expect, re-read from the ledger AFTER writing.

    Read back rather than predicted: `publish_now` rewrites `publish_at` in
    the same write, and a message composed from what we INTENDED would say
    "going out now" for a write that was refused. The ledger is the thing the
    runner will read, so it is the thing this sentence should describe.
    """
    entry = (drop_intake._read_ledger(drop_intake.DROP_PROPOSALS_PATH)
             .get(stem) or {})
    if str(lane or "") != drop_intake.EXPRESS_LANE:
        return f"The {lane} lane will take it on its next scheduled run."
    window, when = drop_intake.publish_window(entry)
    if window == drop_intake.PUBLISH_AT_ASAP:
        prefix = "Released immediately — " if publish_now else ""
        return (f"{prefix}the express runner fires every ten minutes, so it "
                "will be rendered and posted within about that.")
    if window == "scheduled" and when is not None:
        return (f"It will be rendered within about ten minutes and scheduled "
                f"to publish at {when.isoformat()}.")
    if window in {"missed", "unreadable"}:
        # Approval is recorded but nothing will send it. Said here rather than
        # left to the agent's status file, because the operator has a one-word
        # fix and is reading this sentence right now.
        detail = (f"the time it asked for ({when.isoformat()}) has already "
                  "passed" if when is not None
                  else "its stored publish_at no longer parses")
        return (f"NOTHING WILL SEND IT: {detail}, so the express runner will "
                f"skip it every time. Reply `drop approve {stem} now` to send "
                "it immediately instead, or decline it and ask for a re-send.")
    return "The express runner will pick it up within about ten minutes."


def _handle_one(text: str) -> tuple[str, bool]:
    """The message for the operator, and whether anything actually CHANGED.

    The boolean exists because exit 0 is not proof. On 2026-11-17 the real
    approval reply dispatched successfully, returned "no drop named '<stem>
    only ----'", and the poller marked the email Seen -- consuming the
    operator's decision while applying none of it. A caller that can mark a
    message read needs to know the difference between "ran" and "did
    something" ([[feedback_swallowed_errors_lie]]).
    """
    raw = (text or "").strip()
    lowered = raw.lower()

    if lowered in {"drop", "drops", "drop status", "drops status"}:
        return _status(), False

    parts = raw.split(None, 2)
    verb = parts[1].lower() if len(parts) > 1 else ""

    if verb in {"help", ""}:
        return HELP, False

    if verb in {"approve", "decline"}:
        proposals = drop_intake._read_ledger(drop_intake.DROP_PROPOSALS_PATH)
        target = _strip_trailing_separator(parts[2] if len(parts) > 2 else "")
        target, flags = _split_approve_flags(target)
        only = "only" in flags
        publish_now = "now" in flags
        stem, problem = _resolve_stem(target, proposals)
        if problem:
            return problem, False
        entry = proposals.get(stem) or {}
        if verb == "approve" and not entry.get("post_ready"):
            # Approving something the AGENT never declared finished would make
            # the operator's half of a two-key gate carry both halves.
            return (f"{stem} has post_ready:false — the agent has not declared "
                    "it finished, so approving it would not let any run take "
                    "it. Ask the agent to re-send it as ready."), False
        if publish_now and verb == "decline":
            return "'now' only applies to an approval.", False
        try:
            drop_intake.set_approval(stem, verb == "approve", by="operator",
                                     publish_now=publish_now)
        except drop_intake.DropRefused as exc:
            return str(exc), False
        if verb == "approve":
            lane = entry.get("lane")
            msg = f"Approved {stem}. " + _what_happens_next(
                stem, lane, publish_now=publish_now)
            if only:
                others = _decline_siblings(stem, proposals)
                msg += (f" Declined {len(others)} superseded drop(s) in that "
                        "lane." if others else
                        " Nothing else was waiting in that lane.")
            return msg, True
        return f"Declined {stem}. No run will ever take it.", True

    if verb == "answer":
        rest = parts[2] if len(parts) > 2 else ""
        bits = rest.split(None, 1)
        if len(bits) < 2:
            return "usage: drop answer <file> <your answer>", False
        try:
            note = drop_intake.answer_feedback(bits[0], bits[1])
        except drop_intake.DropRefused as exc:
            return str(exc), False
        return (f"Answered {note.get('file')}. The reply is in "
                f"{drop_intake.ANSWERS_DIR}/ where the agent will see it."), True

    return HELP, False
