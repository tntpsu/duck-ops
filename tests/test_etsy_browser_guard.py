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


if __name__ == "__main__":
    unittest.main()
