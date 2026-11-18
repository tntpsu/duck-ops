"""One place that answers "is this lane intentionally off?"

Written 2026-11-18 after answering that question FOUR separate times in one
day, in four different ways, in four files -- and still leaving a fifth
consumer that gets it wrong on a known date.

The four, in the order they were written:

  1. ``main_agent._flow_is_paused_by_operator`` -- strict: the explicit
     ``operator_paused`` flag AND ``mode == "off"``.
  2. ``business_operator_desk`` lane posture -- inline ``mode == "off"``,
     plus its own ``paused_lane_note`` reading two record shapes.
  3. ``viewer._load_review_carousel_health`` -- inline ``mode == "off"``,
     plus a SECOND copy of the note derivation that reads the same two
     shapes slightly differently.
  4. ``viewer._weekly_lane_is_paused`` -- inline ``mode == "off"``, no note.

Each one was correct when written and each one was written because the
previous one was not reachable from where the next card lived. That is the
tell: the predicate was never the hard part, the LOCATION was. A fifth
consumer, ``_load_content_publish_health``, has no pause awareness at all
and goes RED about two deliberate pauses on 2026-11-29 (gtdf) and
2026-12-05 (jeepfact) -- dates computed, not guessed, from its own 17-day
threshold and each lane's last publish.

THE DISPLAY / BEHAVIOUR SPLIT IS REAL AND IS KEPT.

It would be tidier to collapse these to one boolean. That would be wrong,
and the distinction cost three broken tests to learn:

  ``display_paused`` (``mode == "off"``) is enough to SAY a lane is not
  running, because it is simply true -- the scheduler will not run it.

  ``operator_paused`` additionally requires the explicit flag, and is what
  may REFUSE an operator's action. ``mode`` is overloaded across lanes:
  blog, gtdf and review_carousel were all ``off`` on 2026-11-18 for
  unrelated reasons, and review_carousel is off as a REACH decision ("no
  one looks at the review carousels", operator 2026-11-13) while its
  portal Reject button must keep working. A mail-path refusal keyed on
  ``mode`` alone broke that button for three lanes nobody had asked about.

So: one read, two verdicts, one note. Pure, stdlib-only, dict-in -- it
takes an ALREADY-LOADED payload rather than a path, because the four
callers legitimately locate their config differently (a FlowSpec field, a
``parents[4]`` walk, a name map) and only the interpretation was ever
duplicated.

Lives in duck-ops because duck-ops is the layer that evaluates and grades
lanes, and because duckAgent -> duck-ops is the established import
direction: duckAgent's CI checks out duck-ops as a sibling
unconditionally, while duck-ops' CI reaches duckAgent only through an
optional token. Canonical here is importable in both CI runs today; the
reverse is not.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: The one value of ``mode`` that means "the scheduler will not run this".
OFF_MODE = "off"


@dataclass(frozen=True)
class LanePause:
    """What a lane's execution config says about being switched off."""

    #: ``mode``, lowercased and stripped. Empty when unreadable/absent.
    mode: str
    #: ``mode == "off"`` -- safe to SAY the lane is not running.
    display_paused: bool
    #: ``operator_paused`` AND off -- strong enough to REFUSE an action.
    operator_paused: bool
    #: The operator's own words for why, or None.
    note: str | None
    #: Operator-authored resume instructions, or None.
    resume_hint: str | None


def read_lane_pause(config_payload: Any) -> LanePause:
    """Interpret a lane execution config. Never raises.

    Anything that is not a dict -- None, a load failure's default, a JSON
    list -- reads as NOT paused. That is the fail-open default three of the
    four callers already chose deliberately: a config we could not read is
    not evidence of a pause, and treating it as one would deadlock a lane
    on a typo. The one caller that must fail CLOSED does so at its import,
    not here, because a missing module is an environment fault and a
    missing field is a content fault
    ([[feedback_plausible_fallbacks_mask_failure]]).
    """
    if not isinstance(config_payload, dict):
        return LanePause("", False, False, None, None)

    mode = str(config_payload.get("mode") or "").strip().lower()
    display_paused = mode == OFF_MODE
    operator_paused = bool(config_payload.get("operator_paused")) and display_paused
    resume_hint = str(config_payload.get("to_resume") or "").strip() or None
    return LanePause(
        mode=mode,
        display_paused=display_paused,
        operator_paused=operator_paused,
        note=pause_note(config_payload),
        resume_hint=resume_hint,
    )


def pause_note(config_payload: Any) -> str | None:
    """Why this lane is off, in the operator's own words, or None.

    TWO record shapes exist because the two pauses were recorded by
    different hands: review_carousel carries ``operator_mode_history``
    (2026-11-13), jeepfact carries ``paused_on``/``paused_because``
    (2026-11-18). Both are read rather than one being migrated -- rewriting
    a record of what the operator decided is not a display fix.

    The explicit pair wins when present; the history is the fallback, and
    only its LAST entry, and only if that entry switched the lane off (an
    older off->on entry is not a reason the lane is off now).
    """
    if not isinstance(config_payload, dict):
        return None

    reason = str(config_payload.get("paused_because") or "").strip()
    when = str(config_payload.get("paused_on") or "").strip()

    if not reason:
        history = config_payload.get("operator_mode_history")
        if isinstance(history, list) and history and isinstance(history[-1], dict):
            last = history[-1]
            if str(last.get("to") or "").strip().lower() == OFF_MODE:
                reason = str(last.get("reason") or "").strip()
                when = when or str(last.get("at") or "").strip()[:10]

    if not reason:
        return None
    return f"Paused by the operator{f' on {when}' if when else ''}: {reason}"
