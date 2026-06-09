"""Pin auto-dismissal of stale review-reply receipts at drain start.

2026-06-09 fix: previously the drain would chew through ancient
approvals that fail the pre-flight transaction_id check (verified
2026-06-08: 2 of 3 backfill drain attempts failed including a fresh
6-day-old approval). Each failed attempt wastes the per-run cap
(default 3) and looks like system failure when it's actually
expected stale-data behavior.

These tests pin:
  - threshold parsing from policy (default 14 days)
  - artifact_id review-date parsing
  - exclude already-terminal (posted/failed/dismissed)
  - exclude un-parseable artifact_ids (fail-closed)
  - boundary behavior (exactly at threshold)
  - threshold=0 disables the feature
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_reply_executor as rre


def _queue_item(*, age_days: int, status: str = "queued",
                review_date: datetime | None = None,
                now: datetime | None = None) -> tuple[str, dict]:
    """Return (artifact_id, item_dict) for an item that's `age_days`
    old at `now`."""
    now = now or datetime(2026, 6, 9, 12, 0, 0)
    rd = review_date or (now - timedelta(days=age_days))
    aid = f"publish::reviews_reply_positive::{rd.strftime('%Y-%m-%d')}::review-1"
    return aid, {
        "artifact_id": aid,
        "status": status,
        "queued_at": "2026-04-01T00:00:00-04:00",
        "queued_by": "test_seed",
    }


class ArtifactDateParseTests(unittest.TestCase):
    def test_happy_path(self) -> None:
        d = rre._artifact_review_date("publish::reviews_reply_positive::2026-04-12::review-1")
        self.assertEqual(d, datetime(2026, 4, 12))

    def test_returns_none_on_unparseable_date(self) -> None:
        self.assertIsNone(rre._artifact_review_date("publish::reviews_reply_positive::not-a-date::review-1"))
        self.assertIsNone(rre._artifact_review_date("publish::reviews_reply_positive::2026-13-99::review-1"))

    def test_returns_none_on_short_artifact_id(self) -> None:
        self.assertIsNone(rre._artifact_review_date("only::two"))
        self.assertIsNone(rre._artifact_review_date(""))
        self.assertIsNone(rre._artifact_review_date(None))


class AutoDismissStaleQueuedTests(unittest.TestCase):
    def _isolate_transitions(self) -> None:
        """Avoid hitting workflow_control during tests."""
        self._patcher = patch.object(rre, "_record_review_execution_transition")
        self._patcher.start()

    def setUp(self) -> None:
        self._isolate_transitions()

    def tearDown(self) -> None:
        self._patcher.stop()

    def test_dismisses_receipts_older_than_threshold(self) -> None:
        now = datetime(2026, 6, 9, 12, 0, 0)
        aid_old, item_old = _queue_item(age_days=30, now=now)
        aid_new, item_new = _queue_item(age_days=5, now=now)
        items = {aid_old: item_old, aid_new: item_new}
        dismissed = rre.auto_dismiss_stale_queued(
            items, policy={"auto_dismiss_after_days": 14}, now=now,
        )
        self.assertEqual(dismissed, [aid_old])
        self.assertEqual(items[aid_old]["status"], "dismissed")
        self.assertIn("stale_auto_dismiss", items[aid_old]["dismissed_reason"])
        self.assertEqual(items[aid_new]["status"], "queued")  # untouched

    def test_threshold_zero_disables(self) -> None:
        now = datetime(2026, 6, 9, 12, 0, 0)
        aid, item = _queue_item(age_days=999, now=now)
        items = {aid: item}
        dismissed = rre.auto_dismiss_stale_queued(
            items, policy={"auto_dismiss_after_days": 0}, now=now,
        )
        self.assertEqual(dismissed, [])
        self.assertEqual(items[aid]["status"], "queued")

    def test_missing_policy_key_disables(self) -> None:
        """If the policy key is missing entirely, treat as disabled."""
        now = datetime(2026, 6, 9, 12, 0, 0)
        aid, item = _queue_item(age_days=999, now=now)
        items = {aid: item}
        dismissed = rre.auto_dismiss_stale_queued(items, policy={}, now=now)
        self.assertEqual(dismissed, [])

    def test_only_queued_items_are_dismissed(self) -> None:
        """posted / failed / dismissed / running receipts must not flip."""
        now = datetime(2026, 6, 9, 12, 0, 0)
        items = {}
        for st in ("posted", "failed", "dismissed", "running"):
            aid, it = _queue_item(age_days=999, status=st, now=now)
            items[aid] = it
        dismissed = rre.auto_dismiss_stale_queued(
            items, policy={"auto_dismiss_after_days": 14}, now=now,
        )
        self.assertEqual(dismissed, [])
        for aid, it in items.items():
            self.assertNotEqual(it["status"], "dismissed",
                                f"{aid} flipped to dismissed but its starting state was terminal")

    def test_unparseable_artifact_id_skipped(self) -> None:
        """Fail-closed when we can't tell the age — don't dismiss what
        we can't measure."""
        items = {
            "garbage-not-a-valid-shape": {
                "artifact_id": "garbage-not-a-valid-shape",
                "status": "queued",
            },
        }
        dismissed = rre.auto_dismiss_stale_queued(
            items, policy={"auto_dismiss_after_days": 14},
            now=datetime(2026, 6, 9, 12, 0, 0),
        )
        self.assertEqual(dismissed, [])
        self.assertEqual(items["garbage-not-a-valid-shape"]["status"], "queued")

    def test_safely_inside_threshold_not_dismissed(self) -> None:
        """Receipt dated 13 days ago: NOT dismissed. The artifact_id
        date format strips the time component, so 13d-ago-by-date is
        always strictly < 14d-old when measured at any time today."""
        now = datetime(2026, 6, 9, 12, 0, 0)
        aid, item = _queue_item(age_days=13, now=now)
        items = {aid: item}
        dismissed = rre.auto_dismiss_stale_queued(
            items, policy={"auto_dismiss_after_days": 14}, now=now,
        )
        self.assertEqual(dismissed, [])
        self.assertEqual(item["status"], "queued")

    def test_boundary_one_day_past_threshold(self) -> None:
        now = datetime(2026, 6, 9, 12, 0, 0)
        aid, item = _queue_item(age_days=15, now=now)
        items = {aid: item}
        dismissed = rre.auto_dismiss_stale_queued(
            items, policy={"auto_dismiss_after_days": 14}, now=now,
        )
        self.assertEqual(len(dismissed), 1)

    def test_transition_write_failure_skips_item_but_continues_loop(self) -> None:
        """A workflow_control write failure for one item must not stop
        the loop from dismissing others (one bad receipt shouldn't
        block all the others). The bad one stays queued."""
        now = datetime(2026, 6, 9, 12, 0, 0)
        aid_a, item_a = _queue_item(age_days=30, now=now)
        aid_b, item_b = _queue_item(age_days=30,
                                     review_date=datetime(2026, 1, 1),
                                     now=now)
        items = {aid_a: item_a, aid_b: item_b}
        # First call raises, second succeeds.
        call_count = {"n": 0}
        original = self._patcher.stop()
        try:
            def flaky(*a, **kw):
                call_count["n"] += 1
                if call_count["n"] == 1:
                    raise RuntimeError("disk full")
                return {}
            with patch.object(rre, "_record_review_execution_transition", side_effect=flaky):
                dismissed = rre.auto_dismiss_stale_queued(
                    items, policy={"auto_dismiss_after_days": 14}, now=now,
                )
        finally:
            # Restart so tearDown's stop doesn't blow up.
            self._patcher = patch.object(rre, "_record_review_execution_transition")
            self._patcher.start()
        # One should have been dismissed; one should still be queued.
        self.assertEqual(len(dismissed), 1)
        statuses = {it["status"] for it in items.values()}
        self.assertEqual(statuses, {"queued", "dismissed"})


if __name__ == "__main__":
    unittest.main()
