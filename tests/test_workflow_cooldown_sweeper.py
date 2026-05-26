"""Contract tests for workflow_cooldown_sweeper.

The April 24 → May 26 stuck state was the recurring pattern this
sweeper has to catch automatically. These tests pin:
- only whitelisted state_reasons get swept (auth_blocked,
  execution_failed, etc. are NEVER touched)
- only stale lanes (>4h since update) get swept
- a sweep writes a history row + flips state to observed/cooldown_expired
- dry_run reports without writing
- malformed JSON / missing updated_at are skipped silently
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import workflow_cooldown_sweeper as sweeper  # noqa: E402


def _now() -> datetime:
    return datetime.now(timezone.utc).astimezone()


def _ago(hours: float) -> str:
    return (_now() - timedelta(hours=hours)).isoformat()


def _write_state(dirpath: Path, lane: str, payload: dict) -> Path:
    dirpath.mkdir(parents=True, exist_ok=True)
    p = dirpath / f"{lane}.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


class SweeperWhitelistTests(unittest.TestCase):
    """The sweeper MUST only touch state_reasons that are stale
    cooldown side-effects. Anything that needs human action stays
    put. Pin every category."""

    def _setup(self, tmp: Path, reason: str) -> Path:
        wcdir = tmp / "workflow_control"
        return _write_state(wcdir, "test_lane", {
            "state": "blocked",
            "state_reason": reason,
            "updated_at": _ago(24),  # well past threshold
        })

    def test_refresh_failed_is_swept(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = self._setup(Path(tmp), "refresh_failed")
            cleared = sweeper.sweep_stale_cooldowns(p.parent)
        self.assertEqual(len(cleared), 1)
        self.assertEqual(cleared[0]["prior_reason"], "refresh_failed")

    def test_browser_batch_failed_is_swept(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = self._setup(Path(tmp), "browser_batch_failed")
            cleared = sweeper.sweep_stale_cooldowns(p.parent)
        self.assertEqual(len(cleared), 1)

    def test_auth_blocked_is_NEVER_swept(self) -> None:
        # auth requires operator action — sweeping just makes the
        # lane re-attempt and fail again, masking the real problem.
        with tempfile.TemporaryDirectory() as tmp:
            p = self._setup(Path(tmp), "auth_blocked")
            cleared = sweeper.sweep_stale_cooldowns(p.parent)
        self.assertEqual(len(cleared), 0)

    def test_execution_failed_is_NEVER_swept(self) -> None:
        # Real failures need investigation, not auto-clear.
        with tempfile.TemporaryDirectory() as tmp:
            p = self._setup(Path(tmp), "execution_failed")
            cleared = sweeper.sweep_stale_cooldowns(p.parent)
        self.assertEqual(len(cleared), 0)

    def test_manual_intervention_required_is_NEVER_swept(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = self._setup(Path(tmp), "manual_intervention_required")
            cleared = sweeper.sweep_stale_cooldowns(p.parent)
        self.assertEqual(len(cleared), 0)

    def test_blocked_by_upstream_is_NEVER_swept(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = self._setup(Path(tmp), "blocked_by_upstream")
            cleared = sweeper.sweep_stale_cooldowns(p.parent)
        self.assertEqual(len(cleared), 0)


class SweeperStalenessTests(unittest.TestCase):
    def test_recent_failure_not_swept(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wcdir = Path(tmp) / "workflow_control"
            _write_state(wcdir, "fresh_fail", {
                "state": "blocked",
                "state_reason": "refresh_failed",
                "updated_at": _ago(1),  # 1h — under 4h threshold
            })
            cleared = sweeper.sweep_stale_cooldowns(wcdir)
        self.assertEqual(len(cleared), 0)

    def test_threshold_boundary(self) -> None:
        # Just past 4h should sweep.
        with tempfile.TemporaryDirectory() as tmp:
            wcdir = Path(tmp) / "workflow_control"
            _write_state(wcdir, "boundary", {
                "state": "blocked",
                "state_reason": "refresh_failed",
                "updated_at": _ago(4.1),
            })
            cleared = sweeper.sweep_stale_cooldowns(wcdir)
        self.assertEqual(len(cleared), 1)
        self.assertGreater(cleared[0]["hours_stale"], 4.0)


class SweeperWritebackTests(unittest.TestCase):
    def test_sweep_writes_observed_cooldown_expired_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wcdir = Path(tmp) / "workflow_control"
            p = _write_state(wcdir, "customer-inbox-refresh", {
                "state": "blocked",
                "state_reason": "refresh_failed",
                "updated_at": _ago(33 * 24),  # 33-day stuck state
                "history": [],
            })
            cleared = sweeper.sweep_stale_cooldowns(wcdir)
            after = json.loads(p.read_text())
        self.assertEqual(len(cleared), 1)
        self.assertEqual(after["state"], "observed")
        self.assertEqual(after["state_reason"], "cooldown_expired")
        # History row tells the next reader what happened.
        self.assertEqual(len(after["history"]), 1)
        h = after["history"][0]
        self.assertEqual(h["state"], "observed")
        self.assertEqual(h["state_reason"], "cooldown_expired")
        self.assertIn("Auto-cleared by workflow_cooldown_sweeper", h["summary"])
        self.assertIn("refresh_failed", h["summary"])
        self.assertTrue(h["receipt_id"].startswith("cooldown-sweeper-"))

    def test_history_is_appended_not_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wcdir = Path(tmp) / "workflow_control"
            existing_history = [{"state": "verified", "state_reason": "report_emailed", "at": _ago(50)}]
            p = _write_state(wcdir, "lane", {
                "state": "blocked",
                "state_reason": "refresh_failed",
                "updated_at": _ago(24),
                "history": list(existing_history),
            })
            sweeper.sweep_stale_cooldowns(wcdir)
            after = json.loads(p.read_text())
        self.assertEqual(len(after["history"]), 2)
        self.assertEqual(after["history"][0], existing_history[0])

    def test_dry_run_does_not_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wcdir = Path(tmp) / "workflow_control"
            p = _write_state(wcdir, "lane", {
                "state": "blocked",
                "state_reason": "refresh_failed",
                "updated_at": _ago(24),
            })
            before = p.read_text()
            cleared = sweeper.sweep_stale_cooldowns(wcdir, dry_run=True)
            after = p.read_text()
        self.assertEqual(len(cleared), 1)
        self.assertTrue(cleared[0]["dry_run"])
        self.assertEqual(after, before)


class SweeperResilienceTests(unittest.TestCase):
    def test_malformed_json_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wcdir = Path(tmp) / "workflow_control"
            wcdir.mkdir(parents=True)
            (wcdir / "bad.json").write_text("not json", encoding="utf-8")
            cleared = sweeper.sweep_stale_cooldowns(wcdir)
        self.assertEqual(len(cleared), 0)

    def test_missing_updated_at_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wcdir = Path(tmp) / "workflow_control"
            _write_state(wcdir, "lane", {
                "state": "blocked",
                "state_reason": "refresh_failed",
                # no updated_at — can't determine staleness, must skip
            })
            cleared = sweeper.sweep_stale_cooldowns(wcdir)
        self.assertEqual(len(cleared), 0)

    def test_missing_directory_returns_empty(self) -> None:
        cleared = sweeper.sweep_stale_cooldowns(Path("/nonexistent/dir"))
        self.assertEqual(cleared, [])


if __name__ == "__main__":
    unittest.main()
