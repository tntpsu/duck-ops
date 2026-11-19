"""The drain's deliberate sleep must never be the thing that trips the
watchdog. These are written against the 2026-11-18 18:00 run, where it did.

The bug this file defends: jitter was drawn from a fixed 0-1800s range with no
reference to the budget already spent, so a 1800s-timeout job that had used
~1650s slept past its own deadline every single evening.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runtime"))

from drain_window import (  # noqa: E402
    ELAPSED_UNKNOWN,
    FITS,
    NO_ROOM,
    NO_TIMEOUT,
    DrainWindow,
    plan_drain_jitter,
    plan_from_env,
)


def _always_max(lo, hi):
    """Pick the worst case every time -- a jitter range is only safe if its
    TOP end is safe, and a random draw hides that four times in five."""
    return hi


class TheRunThatWasKilled(unittest.TestCase):
    """18:00:04 start, 1800s timeout, drain reached at 18:27:32 (1648s in)."""

    def test_the_old_fixed_range_would_have_overrun_and_this_one_cannot(self):
        window = plan_drain_jitter(
            elapsed_seconds=1648, timeout_seconds=1800, randint=_always_max
        )
        # 152s left, 900s needed for the drain itself: no room at all.
        self.assertFalse(window.fits)
        self.assertEqual(window.reason, NO_ROOM)
        self.assertEqual(window.remaining_seconds, 152)

    def test_a_skip_asks_for_no_sleep_at_all(self):
        window = plan_drain_jitter(elapsed_seconds=1648, timeout_seconds=1800)
        self.assertEqual(window.jitter_seconds, 0)

    def test_the_same_run_fits_comfortably_at_the_raised_timeout(self):
        window = plan_drain_jitter(
            elapsed_seconds=1648, timeout_seconds=3600, randint=_always_max
        )
        self.assertTrue(window.fits)
        self.assertEqual(window.reason, FITS)
        # 1952s left - 900s reserve = 1052s of sleep is affordable.
        self.assertEqual(window.jitter_seconds, 1052)

    def test_it_still_keeps_the_original_thirty_minute_ceiling(self):
        window = plan_drain_jitter(
            elapsed_seconds=0, timeout_seconds=86400, randint=_always_max
        )
        self.assertEqual(window.jitter_seconds, 1800)


class TheInvariantThatMatters(unittest.TestCase):
    def test_sleep_plus_the_drain_always_finishes_inside_the_budget(self):
        reserve = 900
        for timeout in (600, 1800, 2400, 3600, 5400):
            for elapsed in range(0, timeout + 1, 37):
                with self.subTest(timeout=timeout, elapsed=elapsed):
                    window = plan_drain_jitter(
                        elapsed_seconds=elapsed,
                        timeout_seconds=timeout,
                        drain_reserve_seconds=reserve,
                        randint=_always_max,
                    )
                    if not window.fits:
                        continue
                    self.assertLessEqual(
                        elapsed + window.jitter_seconds + reserve,
                        timeout,
                        "the planned sleep leaves the drain no time to finish",
                    )

    def test_it_never_asks_for_a_negative_sleep(self):
        for elapsed in (0, 900, 1799, 1800, 5000):
            window = plan_drain_jitter(elapsed_seconds=elapsed, timeout_seconds=1800)
            self.assertGreaterEqual(window.jitter_seconds, 0)


class WhenTheDeadlineIsNotKnown(unittest.TestCase):
    """Fail toward running. Losing the disguise is a cosmetic regression;
    losing the drain is a customer-facing one."""

    def test_no_timeout_means_no_watchdog_so_the_full_range_is_safe(self):
        window = plan_drain_jitter(
            elapsed_seconds=10, timeout_seconds=None, randint=_always_max
        )
        self.assertTrue(window.fits)
        self.assertEqual(window.reason, NO_TIMEOUT)
        self.assertEqual(window.jitter_seconds, 1800)

    def test_a_deadline_with_unknown_elapsed_drops_the_jitter_but_keeps_the_drain(self):
        window = plan_drain_jitter(
            elapsed_seconds=None, timeout_seconds=1800, randint=_always_max
        )
        self.assertTrue(window.fits)
        self.assertEqual(window.jitter_seconds, 0)
        self.assertEqual(window.reason, ELAPSED_UNKNOWN)

    def test_a_zero_timeout_is_read_as_absent_not_as_already_expired(self):
        # scheduler_health learned this one the hard way: _safe_int(0, default)
        # is 0, and a literal zero budget would skip the drain forever.
        window = plan_drain_jitter(
            elapsed_seconds=10, timeout_seconds=0, randint=_always_max
        )
        self.assertTrue(window.fits)
        self.assertEqual(window.reason, NO_TIMEOUT)


class ReadingWhatTheWrapperExported(unittest.TestCase):
    def test_it_computes_elapsed_from_the_exported_start(self):
        window = plan_from_env(
            env={"DUCK_JOB_STARTED_EPOCH": "1000", "DUCK_JOB_TIMEOUT_SECONDS": "3600"},
            now=2648,
            randint=_always_max,
        )
        self.assertEqual(window.remaining_seconds, 1952)
        self.assertEqual(window.jitter_seconds, 1052)

    def test_a_missing_export_is_absent_not_zero(self):
        window = plan_from_env(env={}, now=2648, randint=_always_max)
        self.assertTrue(window.fits)
        self.assertEqual(window.reason, NO_TIMEOUT)

    def test_a_malformed_export_is_absent_not_zero(self):
        window = plan_from_env(
            env={"DUCK_JOB_STARTED_EPOCH": "yesterday", "DUCK_JOB_TIMEOUT_SECONDS": "1800"},
            now=2648,
            randint=_always_max,
        )
        self.assertTrue(window.fits)
        self.assertEqual(window.reason, ELAPSED_UNKNOWN)

    def test_a_real_draw_stays_inside_the_affordable_range(self):
        for _ in range(200):
            window = plan_from_env(
                env={"DUCK_JOB_STARTED_EPOCH": "1000", "DUCK_JOB_TIMEOUT_SECONDS": "3600"},
                now=2648,
            )
            self.assertTrue(0 <= window.jitter_seconds <= 1052)


class TheCliContract(unittest.TestCase):
    """The shell reads stdout and the exit code; both are load-bearing."""

    def test_it_exits_seventy_five_when_the_drain_does_not_fit(self):
        import drain_window

        rc = self._run(drain_window, {"DUCK_JOB_STARTED_EPOCH": "1000",
                                      "DUCK_JOB_TIMEOUT_SECONDS": "1800"}, now=2648)
        self.assertEqual(rc, 75, "a skip must not look like a zero-second sleep")

    def test_it_exits_zero_and_prints_the_sleep_when_it_fits(self):
        import drain_window

        rc = self._run(drain_window, {"DUCK_JOB_STARTED_EPOCH": "1000",
                                      "DUCK_JOB_TIMEOUT_SECONDS": "3600"}, now=2648)
        self.assertEqual(rc, 0)
        self.assertIn(FITS, self.stdout)
        self.assertTrue(0 <= int(self.stdout.split()[0]) <= 1052)

    def _run(self, module, env, *, now):
        import contextlib
        import io
        from unittest import mock

        buf = io.StringIO()
        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(module.time, "time", return_value=now), \
                contextlib.redirect_stdout(buf), \
                contextlib.redirect_stderr(io.StringIO()):
            rc = module.main([])
        self.stdout = buf.getvalue().strip()
        return rc


if __name__ == "__main__":
    unittest.main()
