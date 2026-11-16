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

RUNTIME_DIR = Path(__file__).resolve().parent
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402

HELP = (
    "Agent drop commands:\n"
    "  drops                  what is waiting on you\n"
    "  drop approve <stem>    let its lane publish it\n"
    "  drop decline <stem>    never publish it\n"
    "  drop answer <file> <text>   reply to an agent's note\n"
)


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
    raw = (text or "").strip()
    lowered = raw.lower()

    if lowered in {"drop", "drops", "drop status", "drops status"}:
        return _status()

    parts = raw.split(None, 2)
    verb = parts[1].lower() if len(parts) > 1 else ""

    if verb in {"help", ""}:
        return HELP

    if verb in {"approve", "decline"}:
        proposals = drop_intake._read_ledger(drop_intake.DROP_PROPOSALS_PATH)
        stem, problem = _resolve_stem(parts[2] if len(parts) > 2 else "", proposals)
        if problem:
            return problem
        entry = proposals.get(stem) or {}
        if verb == "approve" and not entry.get("post_ready"):
            # Approving something the AGENT never declared finished would make
            # the operator's half of a two-key gate carry both halves.
            return (f"{stem} has post_ready:false — the agent has not declared "
                    "it finished, so approving it would not let any run take "
                    "it. Ask the agent to re-send it as ready.")
        try:
            drop_intake.set_approval(stem, verb == "approve", by="operator")
        except drop_intake.DropRefused as exc:
            return str(exc)
        if verb == "approve":
            lane = entry.get("lane")
            return (f"Approved {stem}. The {lane} lane will take it on its next "
                    "scheduled run.")
        return f"Declined {stem}. No run will ever take it."

    if verb == "answer":
        rest = parts[2] if len(parts) > 2 else ""
        bits = rest.split(None, 1)
        if len(bits) < 2:
            return "usage: drop answer <file> <your answer>"
        try:
            note = drop_intake.answer_feedback(bits[0], bits[1])
        except drop_intake.DropRefused as exc:
            return str(exc)
        return (f"Answered {note.get('file')}. The reply is in "
                f"{drop_intake.ANSWERS_DIR}/ where the agent will see it.")

    return HELP
