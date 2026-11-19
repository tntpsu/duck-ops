#!/usr/bin/env python3
"""How much deliberate sleep still fits before the watchdog kills us?

The once-a-day review-reply drain sleeps a random 0-30 min before submitting so
the post does not land on the same clock tick every day. That sleep is the
opposite of a hang, and `run_with_receipt.sh`'s watchdog cannot tell the two
apart -- it only sees a process tree that has not exited.

Until 2026-11-18 the drain was appended as the LAST step of the 18:00 sidecar
run, whose earlier steps had already spent ~27 of the 30 minute budget (the
observer alone took 21 min that evening). A 0-1800s sleep inside the ~150s that
remained could never complete, and the failure was worse than a kill: the
sidecar's `trap cleanup EXIT INT TERM HUP` returned 0, so zsh *swallowed* the
watchdog's SIGTERM, resumed the step, and ran the drain immediately -- at
exactly 18:30:0x, the one fixed timestamp the jitter existed to avoid -- then
took SIGKILL ten seconds later, mid-browser-automation. The receipt recorded
`status: timeout` with `exit_code: 0`.

So the sleep has to be sized against the budget that is actually left, which
means the job has to know its own deadline. `run_with_receipt.sh` exports
DUCK_JOB_STARTED_EPOCH and DUCK_JOB_TIMEOUT_SECONDS for that; this module is
the one place that interprets them.

Fails toward running with no jitter rather than skipping: a missing export
should cost us the disguise, never the drain.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time
from dataclasses import dataclass

# Wall-clock the drain itself needs once the sleep is over: resolve the thread,
# drive the browser, submit up to max-items, write receipts. The Etsy browser
# batch budgets 720s for one session; 900 leaves headroom for a slow page.
DEFAULT_DRAIN_RESERVE_SECONDS = 900
# Keep the original 0-30 min intent as the ceiling; the budget only ever
# narrows it.
DEFAULT_MAX_JITTER_SECONDS = 1800

NO_TIMEOUT = "no_timeout"
ELAPSED_UNKNOWN = "elapsed_unknown"
FITS = "fits"
NO_ROOM = "no_room_for_drain"


@dataclass(frozen=True)
class DrainWindow:
    """fits=False means skip the drain entirely -- and leave the marker file
    alone, so tomorrow's run takes it rather than today's silently burning it."""

    fits: bool
    jitter_seconds: int
    reason: str
    remaining_seconds: int | None


def plan_drain_jitter(
    *,
    elapsed_seconds: float | None,
    timeout_seconds: float | None,
    drain_reserve_seconds: int = DEFAULT_DRAIN_RESERVE_SECONDS,
    max_jitter_seconds: int = DEFAULT_MAX_JITTER_SECONDS,
    randint=None,
) -> DrainWindow:
    pick = randint or (lambda lo, hi: random.randint(lo, hi))
    ceiling = max(0, int(max_jitter_seconds))

    # No watchdog, no deadline. Nothing can cut the sleep short.
    if timeout_seconds is None or float(timeout_seconds) <= 0:
        return DrainWindow(True, pick(0, ceiling), NO_TIMEOUT, None)

    # We know there IS a deadline but not how much we have spent. Jitter would
    # be a coin flip against a kill, so drop it and keep the drain.
    if elapsed_seconds is None or float(elapsed_seconds) < 0:
        return DrainWindow(True, 0, ELAPSED_UNKNOWN, None)

    remaining = int(float(timeout_seconds) - float(elapsed_seconds))
    headroom = remaining - max(0, int(drain_reserve_seconds))
    if headroom < 0:
        return DrainWindow(False, 0, NO_ROOM, remaining)
    return DrainWindow(True, pick(0, min(ceiling, headroom)), FITS, remaining)


def plan_from_env(env=None, *, now=None, **kwargs) -> DrainWindow:
    """Read the deadline the wrapper exported. A malformed value is treated as
    absent, never as zero -- `timeout_seconds=0` would read as 'already past
    the deadline' and skip the drain forever."""
    environ = os.environ if env is None else env

    def _number(name):
        raw = str(environ.get(name) or "").strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    timeout = _number("DUCK_JOB_TIMEOUT_SECONDS")
    started = _number("DUCK_JOB_STARTED_EPOCH")
    elapsed = None
    if started is not None:
        elapsed = (time.time() if now is None else now) - started
    return plan_drain_jitter(elapsed_seconds=elapsed, timeout_seconds=timeout, **kwargs)


def main(argv=None) -> int:
    """Prints `<jitter_seconds> <reason>` on stdout for the shell to read, and
    exits 75 (EX_TEMPFAIL, the stack's 'declined to work') when the drain does
    not fit -- so the caller cannot mistake a skip for a zero-second sleep."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--drain-reserve-seconds", type=int, default=DEFAULT_DRAIN_RESERVE_SECONDS)
    parser.add_argument("--max-jitter-seconds", type=int, default=DEFAULT_MAX_JITTER_SECONDS)
    args = parser.parse_args(argv)

    window = plan_from_env(
        drain_reserve_seconds=args.drain_reserve_seconds,
        max_jitter_seconds=args.max_jitter_seconds,
    )
    print(f"{window.jitter_seconds} {window.reason}")
    if not window.fits:
        print(
            f"[drain-window] SKIP the drain does not fit: {window.remaining_seconds}s left, "
            f"{args.drain_reserve_seconds}s needed",
            file=sys.stderr,
        )
        return 75
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
