import importlib.util
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
                with self.assertRaises(RuntimeError):
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


if __name__ == "__main__":
    unittest.main()
