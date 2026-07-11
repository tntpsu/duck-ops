"""Surface 56 — the guard hard ceiling must count Etsy-FACING commands, not
local read-evals. Regression for 2026-07-11: a drain's ~6 read-evals per reply
(querySelector reads, textarea fill, open-composer click) counted against
MAX_COMMANDS_PER_WINDOW=18 even though they generate zero Etsy traffic, so the
batch ran but posted 0. The only evals that touch Etsy — navigation
(location.assign) and the submit (submit.click) — are already classified
mutating and capped by MAX_MUTATING_COMMANDS_PER_WINDOW; that cap stays the true
governor. A high TOTAL_COMMANDS_BACKSTOP still catches a runaway read-loop."""
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
guard = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(guard)


def _events(now, specs):
    """specs: list of (command, mutating). Newest last."""
    out = []
    for i, (cmd, mut) in enumerate(specs):
        out.append({
            "at": (now - timedelta(seconds=10 + len(specs) - i)).isoformat(),
            "session": "esd",
            "command": cmd,
            "mutating": mut,
        })
    return out


class GuardEvalBudgetTests(unittest.TestCase):
    def _run(self, seeded, next_args):
        """Seed `seeded` events, run before_command(next_args); return the
        raised exception (or None if it was allowed)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "guard.json"
            now = datetime.now().astimezone()
            with patch.object(guard, "STATE_PATH", state_path):
                guard.save_state({"blocked_until": None, "block_reason": None,
                                  "events": _events(now, seeded)})
                with patch.object(guard, "cleanup_stale_playwright_processes"), \
                     patch.object(guard, "time") as fake_time:
                    fake_time.sleep = lambda *_a, **_k: None
                    try:
                        guard.before_command("esd", next_args)
                        return None
                    except Exception as exc:  # noqa: BLE001
                        return exc

    def test_reads_do_not_block_post(self):
        # 20 local read-evals in the window — well past the old 18 ceiling.
        seeded = [("eval", False)] * 20
        # A fresh review-reply submit (mutating) must still be allowed.
        exc = self._run(seeded, ("eval", "submit.click(); return {ok:true};"))
        self.assertIsNone(exc, f"read burst wrongly blocked a post: {exc}")
        # And another read must also be allowed (reads are effectively local).
        exc = self._run(seeded, ("eval", "return document.querySelector('x');"))
        self.assertIsNone(exc, f"read burst wrongly blocked a read: {exc}")

    def test_mutating_cap_still_trips(self):
        # MAX_MUTATING mutating events already in the window → next post cools down.
        seeded = [("eval", True)] * guard.MAX_MUTATING_COMMANDS_PER_WINDOW
        exc = self._run(seeded, ("eval", "submit.click(); return {ok:true};"))
        self.assertIsInstance(exc, RuntimeError)
        self.assertIn("cooling down", str(exc))

    def test_location_assign_counts_mutating(self):
        self.assertTrue(guard._is_mutating_command(
            ("eval", "window.location.assign('/shop/reviews?page=2'); return 'nav';")))

    def test_run_code_submit_is_mutating(self):
        # run-code was never content-inspected; a run-code submit must count.
        self.assertTrue(guard._is_mutating_command(
            ("run-code", "if(!submit.disabled){ submit.click(); }")))

    def test_backstop_trips_at_total(self):
        # A runaway read-loop (reads no longer count toward the facing ceiling)
        # must still be stopped by the total-command backstop.
        seeded = [("eval", False)] * guard.TOTAL_COMMANDS_BACKSTOP
        exc = self._run(seeded, ("eval", "return document.title;"))
        self.assertIsInstance(exc, Exception)


if __name__ == "__main__":
    unittest.main()
