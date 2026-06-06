"""Phase 5 Step 5 (duck-ops side): _executed_experiments_from_receipts
globs creative_quality_receipts/*.json and exposes the summary the
Learning Inspector consumes.

Critical behaviors:
  - Counts only receipts with outcome_status in {partial_24h, final_7d}
  - Filters by published_at age (last 14 days by default)
  - Returns empty shape gracefully when receipts dir is missing
  - Skips malformed receipts without crashing
"""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))


import current_learnings  # noqa: E402


def _write_receipt(receipts_dir: Path, *, flow: str, run_id: str,
                   outcome_status: str = "partial_24h",
                   published_at: str | None = None,
                   outcomes: list[dict] | None = None,
                   post_id: str = "abc",
                   include_publish: bool = True) -> None:
    receipts_dir.mkdir(parents=True, exist_ok=True)
    receipt = {
        "schema_version": "duck.creative_quality_loop.v1",
        "flow": flow,
        "run_id": run_id,
        "outcome_status": outcome_status,
        "outcomes": outcomes or [
            {"window": "24h", "engagement_score": 47.0, "metrics": {}}
        ],
    }
    if include_publish:
        receipt["publish"] = {
            "post_id": post_id,
            "platform": "instagram",
            "published_at": published_at or "2026-06-06T18:00:00-04:00",
        }
    path = receipts_dir / f"{flow}_{run_id}.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")


class ExecutedExperimentsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.receipts_dir = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_empty_dir_returns_zero_count(self) -> None:
        result = current_learnings._executed_experiments_from_receipts(
            receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["window_days"], 14)
        self.assertEqual(result["receipts"], [])

    def test_missing_dir_returns_zero_count(self) -> None:
        result = current_learnings._executed_experiments_from_receipts(
            receipts_dir=self.receipts_dir / "nope"
        )
        self.assertEqual(result["count"], 0)

    def test_counts_only_outcome_status_partial_or_final(self) -> None:
        now = "2026-06-06T18:00:00-04:00"
        _write_receipt(self.receipts_dir, flow="meme", run_id="r1",
                       outcome_status="partial_24h", published_at=now)
        _write_receipt(self.receipts_dir, flow="meme", run_id="r2",
                       outcome_status="final_7d", published_at=now)
        _write_receipt(self.receipts_dir, flow="meme", run_id="r3",
                       outcome_status="pending", published_at=now)
        _write_receipt(self.receipts_dir, flow="meme", run_id="r4",
                       outcome_status="", published_at=now)
        result = current_learnings._executed_experiments_from_receipts(
            receipts_dir=self.receipts_dir,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(result["count"], 2)
        runs = {r["run_id"] for r in result["receipts"]}
        self.assertEqual(runs, {"r1", "r2"})

    def test_filters_by_published_at_window(self) -> None:
        """Receipts older than window_days are excluded so the Inspector
        always shows 'last N days of learning' even when receipts
        accumulate over months."""
        now_dt = datetime(2026, 6, 6, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        fresh_dt = now_dt - timedelta(days=5)
        old_dt = now_dt - timedelta(days=20)
        _write_receipt(self.receipts_dir, flow="meme", run_id="fresh",
                       published_at=fresh_dt.isoformat())
        _write_receipt(self.receipts_dir, flow="meme", run_id="old",
                       published_at=old_dt.isoformat())
        result = current_learnings._executed_experiments_from_receipts(
            receipts_dir=self.receipts_dir,
            now_iso=now_dt.isoformat(),
        )
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["receipts"][0]["run_id"], "fresh")

    def test_custom_window_days(self) -> None:
        now_dt = datetime(2026, 6, 6, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        # Receipt 10 days ago — outside 7d window, inside 14d window.
        _write_receipt(self.receipts_dir, flow="meme", run_id="r1",
                       published_at=(now_dt - timedelta(days=10)).isoformat())
        r7 = current_learnings._executed_experiments_from_receipts(
            window_days=7,
            receipts_dir=self.receipts_dir,
            now_iso=now_dt.isoformat(),
        )
        r14 = current_learnings._executed_experiments_from_receipts(
            window_days=14,
            receipts_dir=self.receipts_dir,
            now_iso=now_dt.isoformat(),
        )
        self.assertEqual(r7["count"], 0)
        self.assertEqual(r14["count"], 1)

    def test_skips_receipts_without_publish_block(self) -> None:
        """Phase 4-era receipts (no Step 3 stamp) shouldn't count as
        executed — without published_at we can't apply the window."""
        _write_receipt(self.receipts_dir, flow="meme", run_id="r1",
                       outcome_status="partial_24h", include_publish=False)
        result = current_learnings._executed_experiments_from_receipts(
            receipts_dir=self.receipts_dir,
        )
        self.assertEqual(result["count"], 0)

    def test_malformed_receipts_are_skipped(self) -> None:
        self.receipts_dir.mkdir(parents=True, exist_ok=True)
        (self.receipts_dir / "broken.json").write_text("not valid json")
        (self.receipts_dir / "wrong_shape.json").write_text('"a string"')
        # And one valid receipt mixed in.
        _write_receipt(self.receipts_dir, flow="meme", run_id="ok",
                       published_at="2026-06-06T18:00:00-04:00")
        result = current_learnings._executed_experiments_from_receipts(
            receipts_dir=self.receipts_dir,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(result["count"], 1)

    def test_outcomes_passed_through_for_inspector_use(self) -> None:
        outcomes = [
            {"window": "24h", "engagement_score": 47.0},
            {"window": "7d", "engagement_score": 120.0},
        ]
        _write_receipt(self.receipts_dir, flow="meme", run_id="r1",
                       outcome_status="final_7d",
                       outcomes=outcomes,
                       published_at="2026-06-06T18:00:00-04:00")
        result = current_learnings._executed_experiments_from_receipts(
            receipts_dir=self.receipts_dir,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        receipt = result["receipts"][0]
        self.assertEqual(len(receipt["outcomes"]), 2)
        self.assertEqual(receipt["post_id"], "abc")
        self.assertEqual(receipt["outcome_status"], "final_7d")


if __name__ == "__main__":
    unittest.main()
