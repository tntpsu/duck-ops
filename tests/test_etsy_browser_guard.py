import importlib.util
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path("/Users/philtullai/ai-agents/duck-ops/runtime/etsy_browser_guard.py")
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("etsy_browser_guard", MODULE_PATH)
etsy_browser_guard = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(etsy_browser_guard)


class EtsyBrowserGuardTests(unittest.TestCase):
    def test_detect_block_in_output_finds_bot_phrase(self) -> None:
        reason = etsy_browser_guard.detect_block_in_output("Sorry, we detected bot activity on your account.")
        self.assertEqual(reason, "bot activity")

    def test_before_command_blocks_during_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            with patch.object(etsy_browser_guard, "STATE_PATH", state_path):
                etsy_browser_guard.save_state(
                    {
                        "blocked_until": (datetime.now().astimezone() + timedelta(minutes=5)).isoformat(),
                        "block_reason": "bot activity",
                        "events": [],
                    }
                )
                with (
                    patch.object(etsy_browser_guard, "cleanup_stale_playwright_processes"),
                    self.assertRaises(RuntimeError),
                ):
                    etsy_browser_guard.before_command("esd", ("snapshot",))

    def test_after_command_records_event_and_sets_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            with patch.object(etsy_browser_guard, "STATE_PATH", state_path):
                etsy_browser_guard.after_command("esd", ("snapshot",), "We noticed unusual activity and need to verify you're a human.")
                state = etsy_browser_guard.load_state()
                self.assertEqual(len(state["events"]), 1)
                self.assertTrue(state["blocked_until"])
                self.assertEqual(state["block_reason"], "unusual activity")

    def test_local_only_commands_excluded_from_burst_budget(self) -> None:
        """The 8:37 stuck-state burst was 18 commands of which 10 were
        local Playwright ops Etsy can't see (snapshot, state-save,
        state-load, open). Those must not count toward
        MAX_COMMANDS_PER_WINDOW or the guard trips on normal inbox
        sync activity."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            now = datetime.now().astimezone()
            local_events = [
                {
                    "at": (now - timedelta(seconds=10 + i)).isoformat(),
                    "session": "esd",
                    "command": cmd,
                    "mutating": False,
                }
                for i, cmd in enumerate(
                    ["snapshot", "snapshot", "snapshot", "snapshot", "snapshot",
                     "snapshot", "state-load", "state-save", "state-save", "open",
                     "snapshot", "snapshot", "snapshot", "snapshot", "snapshot",
                     "state-save", "state-save", "state-save", "state-save", "state-save"]
                )
            ]
            with patch.object(etsy_browser_guard, "STATE_PATH", state_path):
                etsy_browser_guard.save_state(
                    {"blocked_until": None, "block_reason": None, "events": local_events}
                )
                with patch.object(etsy_browser_guard, "cleanup_stale_playwright_processes"):
                    # 20 local-only events queued; a fresh click should
                    # be allowed because none count toward the budget.
                    try:
                        etsy_browser_guard.before_command("esd", ("click", "#thread"))
                    except RuntimeError as exc:  # pragma: no cover — defensive
                        self.fail(f"local-only events tripped the budget: {exc}")

    def test_eval_with_dot_click_no_longer_mutating(self) -> None:
        """`.click(` matches every "click to expand thread" eval. Real
        Etsy review-reply submits use `submit.click(` (specific token).
        Today's burst had 4 evals with `.click(` flagged mutating
        despite being read-only thread expansions."""
        eval_args = ("eval", "document.querySelector('a.thread').click(); return true;")
        self.assertFalse(etsy_browser_guard._is_mutating_command(eval_args))

    def test_eval_submit_click_still_mutating(self) -> None:
        """The real review-reply submit pattern from
        review_reply_executor.py:1357 must still flag mutating."""
        submit_eval = (
            "eval",
            "if (submit.disabled) return {ok:false}; submit.click(); return {ok:true};",
        )
        self.assertTrue(etsy_browser_guard._is_mutating_command(submit_eval))

    def test_navigation_eval_still_mutating(self) -> None:
        """location.assign / window.location are genuine page changes."""
        nav_eval = ("eval", "window.location.href = '/listings/123/edit';")
        self.assertTrue(etsy_browser_guard._is_mutating_command(nav_eval))

    def test_click_command_still_mutating(self) -> None:
        """Direct Playwright `click` commands hit Etsy regardless of
        selector. Keep treating all of them as mutating."""
        self.assertTrue(etsy_browser_guard._is_mutating_command(("click", "#button")))
        self.assertTrue(etsy_browser_guard._is_mutating_command(("fill", "input", "text")))

    def test_burst_still_trips_on_visible_commands(self) -> None:
        """The guard must still catch a real burst of Etsy-visible
        commands. Regression-pin so a future tweak doesn't disable it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            now = datetime.now().astimezone()
            max_visible = etsy_browser_guard.MAX_COMMANDS_PER_WINDOW
            visible_events = [
                {
                    "at": (now - timedelta(seconds=10 + i)).isoformat(),
                    "session": "esd",
                    "command": "click",
                    "mutating": True,
                }
                for i in range(max_visible)
            ]
            with patch.object(etsy_browser_guard, "STATE_PATH", state_path):
                etsy_browser_guard.save_state(
                    {"blocked_until": None, "block_reason": None, "events": visible_events}
                )
                with patch.object(etsy_browser_guard, "cleanup_stale_playwright_processes"):
                    with self.assertRaises(RuntimeError) as ctx:
                        etsy_browser_guard.before_command("esd", ("click", "#anything"))
            self.assertIn("cooling down", str(ctx.exception))

    def test_cleanup_stale_playwright_processes_respects_keepalive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            discovery_path = Path(tmpdir) / "sessions.json"
            discovery_path.write_text(
                """
{
  "sessions": {
    "esd": {
      "pid": 111,
      "process_group_id": 111,
      "keepalive_until": "2099-01-01T00:00:00+00:00"
    }
  }
}
                """.strip(),
                encoding="utf-8",
            )
            ps_output = "\n".join(
                [
                    "  111     1   111 02:10:00 /usr/bin/node /tmp/playwright-core/lib/entry/cliDaemon.js esd",
                    "  112   111   111 02:09:59 /Applications/Google Chrome --user-data-dir=/tmp/playwright_chromiumdev_profile-abc --remote-debugging-pipe",
                ]
            )
            completed = subprocess.CompletedProcess(
                args=["ps", "-axo", "pid,ppid,pgid,etime,command"],
                returncode=0,
                stdout=ps_output,
            )

            with (
                patch.object(etsy_browser_guard, "STATE_PATH", state_path),
                patch.object(etsy_browser_guard, "DISCOVERY_SESSION_STATE_PATH", discovery_path),
                patch.object(etsy_browser_guard.subprocess, "run", return_value=completed),
                patch.object(etsy_browser_guard.os, "killpg") as killpg_mock,
                patch.object(etsy_browser_guard.shutil, "rmtree") as rmtree_mock,
            ):
                result = etsy_browser_guard.cleanup_stale_playwright_processes(force=True, reason="test-keepalive")

            self.assertEqual(result["killed_group_count"], 0)
            self.assertEqual(result["skipped_keepalive_groups"], [111])
            killpg_mock.assert_not_called()
            rmtree_mock.assert_not_called()

    def test_cleanup_stale_playwright_processes_can_override_keepalive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            discovery_path = Path(tmpdir) / "sessions.json"
            discovery_path.write_text(
                """
{
  "sessions": {
    "esd": {
      "pid": 111,
      "process_group_id": 111,
      "keepalive_until": "2099-01-01T00:00:00+00:00",
      "ready": true,
      "already_open": true
    }
  }
}
                """.strip(),
                encoding="utf-8",
            )
            ps_output = "\n".join(
                [
                    "  111     1   111 02:10:00 /usr/bin/node /tmp/playwright-core/lib/entry/cliDaemon.js esd",
                    "  112   111   111 02:09:59 /Applications/Google Chrome --user-data-dir=/tmp/playwright_chromiumdev_profile-abc --remote-debugging-pipe",
                ]
            )
            completed = subprocess.CompletedProcess(
                args=["ps", "-axo", "pid,ppid,pgid,etime,command"],
                returncode=0,
                stdout=ps_output,
            )

            with (
                patch.object(etsy_browser_guard, "STATE_PATH", state_path),
                patch.object(etsy_browser_guard, "DISCOVERY_SESSION_STATE_PATH", discovery_path),
                patch.object(etsy_browser_guard.subprocess, "run", return_value=completed),
                patch.object(etsy_browser_guard.os, "killpg") as killpg_mock,
                patch.object(etsy_browser_guard.shutil, "rmtree") as rmtree_mock,
                patch.object(etsy_browser_guard.time, "sleep"),
            ):
                result = etsy_browser_guard.cleanup_stale_playwright_processes(
                    force=True,
                    reason="manual-cleanup",
                    respect_keepalive=False,
                )

            self.assertEqual(result["killed_group_count"], 1)
            self.assertEqual(result["killed_pids"], [111, 112])
            self.assertEqual(result["removed_profile_paths"], ["/tmp/playwright_chromiumdev_profile-abc"])
            self.assertFalse(result["skipped_keepalive_groups"])
            self.assertFalse(result["respect_keepalive"])
            self.assertEqual(killpg_mock.call_count, 2)
            rmtree_mock.assert_called_once_with("/tmp/playwright_chromiumdev_profile-abc", ignore_errors=True)

            updated_sessions = etsy_browser_guard._load_json(discovery_path, {"sessions": {}})
            session = updated_sessions["sessions"]["esd"]
            self.assertFalse(session["ready"])
            self.assertFalse(session["already_open"])
            self.assertIsNone(session["keepalive_until"])
            self.assertEqual(session["cleanup_status"], "stale_process_cleaned")


class ReservationTests(unittest.TestCase):
    """Phase 2: read-only commands are soft-deferred to keep posting budget."""

    def _visible_reads(self, n: int):
        now = datetime.now().astimezone()
        return [
            {"at": (now - timedelta(seconds=10 + i)).isoformat(),
             "session": "esd", "command": "goto", "mutating": False}
            for i in range(n)
        ]

    def _run(self, events, args):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            with patch.object(etsy_browser_guard, "STATE_PATH", state_path):
                etsy_browser_guard.save_state(
                    {"blocked_until": None, "block_reason": None, "events": events}
                )
                with patch.object(etsy_browser_guard, "cleanup_stale_playwright_processes"):
                    exc = None
                    try:
                        etsy_browser_guard.before_command("esd", args)
                    except BaseException as e:  # noqa: BLE001
                        exc = e
                    return exc, etsy_browser_guard.load_state()

    def test_read_deferred_when_reserve_reached(self) -> None:
        # Surface 56: reads are no longer Etsy-facing, so the soft reserve now
        # keys off the TOTAL-command backstop. At BACKSTOP - RESERVED reads the
        # next read is deferred (a runaway read-loop yields before it hard-cools).
        near = etsy_browser_guard.TOTAL_COMMANDS_BACKSTOP - etsy_browser_guard.RESERVED_FOR_MUTATING
        exc, state = self._run(self._visible_reads(near), ("goto", "https://etsy.com/x"))
        self.assertIsInstance(exc, etsy_browser_guard.PacingReservationError)
        # Soft: it must NOT persist a global cooldown (posts stay free).
        self.assertIsNone(state.get("blocked_until"))

    def test_read_burst_below_backstop_does_not_trip(self) -> None:
        # Surface 56 core invariant: a big burst of pure reads (past the OLD 18
        # ceiling) must NOT cool down — that self-throttle was the bug.
        exc, state = self._run(self._visible_reads(30), ("eval", "return document.title;"))
        self.assertIsNone(exc, f"read burst wrongly throttled: {exc}")
        self.assertIsNone(state.get("blocked_until"))

    def test_post_allowed_when_reads_filled_reserve(self) -> None:
        # Same 14 visible reads, but a mutating POST gets through (it's what
        # the reserve is for).
        exc, _ = self._run(self._visible_reads(14), ("click", "#reply-submit"))
        self.assertIsNone(exc, f"post should not be blocked at the reserve line: {exc}")

    def test_read_allowed_below_reserve(self) -> None:
        exc, _ = self._run(self._visible_reads(13), ("goto", "https://etsy.com/x"))
        self.assertIsNone(exc, f"read below reserve should pass: {exc}")

    def test_hard_cooldown_still_trips_at_backstop(self) -> None:
        # Surface 56: a real overload (runaway read-loop at the TOTAL backstop)
        # still trips the persistent cooldown for any command — the backstop
        # replaces the old count-everything ceiling as the runaway guard.
        exc, state = self._run(
            self._visible_reads(etsy_browser_guard.TOTAL_COMMANDS_BACKSTOP),
            ("click", "#reply-submit"),
        )
        self.assertIsInstance(exc, RuntimeError)
        self.assertNotIsInstance(exc, etsy_browser_guard.PacingReservationError)
        self.assertEqual(state.get("block_reason"), "rate_limit_preemptive_cooldown")
        self.assertTrue(state.get("blocked_until"))


if __name__ == "__main__":
    unittest.main()
