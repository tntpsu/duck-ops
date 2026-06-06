"""Phase 5 Step 4: collector → creative_quality_receipt writeback.

These tests pin the `writeback_outcome_to_creative_quality_receipt`
function — the bridge that closes the Creative Quality Loop. For
each post the daily collector fetches engagement for, it looks up
the matching receipt and appends an outcome at the right age window.

Critical behaviors:
  - Only writes at the right age window (20-30h → "24h", 6-8d → "7d")
  - Idempotent on same-window replay (delegated to record_engagement_outcome
    which has its own tests; smoke-checked here too)
  - Graceful no-op when receipt is missing (Phase 4 didn't run)
  - post_id mismatch is surfaced, not silently corrupted
  - metric_status filters: only "ok" / "partial" writeback; "fetch_failed"
    skips (transient — next daily run can retry)
  - 7d window also marks final so the collector stops re-fetching
  - Exceptions never break the collector — they return a skipped action
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))
DUCK_AGENT_ROOT = Path("/Users/philtullai/ai-agents/duckAgent")
if str(DUCK_AGENT_ROOT) not in sys.path:
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
os.environ.setdefault("3D_AI_STUDIO_KEY", "ci-test-key")


import social_performance_collector  # noqa: E402
from helpers.creative_quality_loop import (  # noqa: E402
    load_creative_quality_receipt,
    stamp_publish_link,
    write_creative_quality_receipt,
)


def _seed_receipt(receipts_dir: Path, *, flow: str, run_id: str,
                  publish_post_id: str | None = None) -> None:
    """Seed a Phase-4-era receipt, optionally with the Step-3 publish
    link stamped (mimics what production looks like at writeback time)."""
    write_creative_quality_receipt(
        flow, run_id,
        {
            "schema_version": "duck.creative_quality_loop.v1",
            "flow": flow,
            "run_id": run_id,
            "candidate_count": 3,
            "top_candidate_id": 1,
            "ranked_candidates": [
                {"candidate_id": 1, "rank": 1, "recommendation": "recommended"},
            ],
        },
        receipts_dir=receipts_dir,
    )
    if publish_post_id:
        stamp_publish_link(
            flow, run_id,
            post_id=publish_post_id, platform="instagram",
            published_rank=1,
            receipts_dir=receipts_dir,
        )


def _post(*, workflow: str = "meme", run_id: str = "2026-06-08",
          post_id: str = "17912345", metric_status: str = "ok",
          published_at: str | None = None,
          metrics: dict | None = None,
          engagement_score: float = 47.0,
          engagement_rate: float = 0.063) -> dict:
    """Shape-matched to what `_load_normalized_posts` + the engagement
    decoration in `build_social_performance_payload` produces."""
    return {
        "workflow": workflow,
        "run_id": run_id,
        "platform": "instagram",
        "post_id": post_id,
        "published_at": published_at or "2026-06-08T18:00:00-04:00",
        "metric_status": metric_status,
        "metrics": metrics or {"like_count": 41, "comments_count": 6, "reach": 740},
        "engagement_score": engagement_score,
        "engagement_rate": engagement_rate,
    }


class WindowSelectionTests(unittest.TestCase):
    """Unit-test the age → window mapping in isolation. Edge cases
    matter because the daily collector hits this function once per
    post per day; the wrong window means duplicate or missing
    outcomes."""

    def test_too_early_returns_none(self) -> None:
        self.assertIsNone(social_performance_collector._outcome_window_for_age(0))
        self.assertIsNone(social_performance_collector._outcome_window_for_age(10))
        self.assertIsNone(social_performance_collector._outcome_window_for_age(19.9))

    def test_24h_window_inclusive_lower_exclusive_upper(self) -> None:
        self.assertEqual(social_performance_collector._outcome_window_for_age(20), "24h")
        self.assertEqual(social_performance_collector._outcome_window_for_age(24), "24h")
        self.assertEqual(social_performance_collector._outcome_window_for_age(29.99), "24h")
        self.assertIsNone(social_performance_collector._outcome_window_for_age(30))

    def test_between_windows_returns_none(self) -> None:
        self.assertIsNone(social_performance_collector._outcome_window_for_age(48))
        self.assertIsNone(social_performance_collector._outcome_window_for_age(100))
        self.assertIsNone(social_performance_collector._outcome_window_for_age(143.9))

    def test_7d_window(self) -> None:
        self.assertEqual(social_performance_collector._outcome_window_for_age(144), "7d")
        self.assertEqual(social_performance_collector._outcome_window_for_age(168), "7d")
        self.assertEqual(social_performance_collector._outcome_window_for_age(191.9), "7d")

    def test_after_7d_returns_none(self) -> None:
        """Posts older than 8d are out of scope — Phase 6 might do
        long-tail tracking later but Phase 5 stops at the 7d final."""
        self.assertIsNone(social_performance_collector._outcome_window_for_age(192))
        self.assertIsNone(social_performance_collector._outcome_window_for_age(720))


class WritebackHappyPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.receipts_dir = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_writes_24h_outcome_when_post_is_24h_old(self) -> None:
        """The headline test: closes the loop. Publish step's
        stamp_publish_link + collector's writeback → receipt now
        knows engagement_score for the published variant."""
        _seed_receipt(self.receipts_dir, flow="meme", run_id="2026-06-08",
                      publish_post_id="17912345")
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        now = published_at + timedelta(hours=24)
        post = _post(published_at=published_at.isoformat())
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=now, receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "wrote_outcome")
        self.assertEqual(result["window"], "24h")
        loaded = load_creative_quality_receipt("meme", "2026-06-08",
                                               receipts_dir=self.receipts_dir)
        self.assertEqual(loaded["outcome_status"], "partial_24h")
        self.assertEqual(len(loaded["outcomes"]), 1)
        self.assertEqual(loaded["outcomes"][0]["window"], "24h")
        self.assertEqual(loaded["outcomes"][0]["engagement_score"], 47.0)

    def test_writes_7d_outcome_and_marks_final(self) -> None:
        _seed_receipt(self.receipts_dir, flow="meme", run_id="2026-06-08",
                      publish_post_id="17912345")
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        now = published_at + timedelta(days=7)
        post = _post(published_at=published_at.isoformat(), engagement_score=120.0)
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=now, receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "wrote_outcome_final")
        self.assertEqual(result["window"], "7d")
        loaded = load_creative_quality_receipt("meme", "2026-06-08",
                                               receipts_dir=self.receipts_dir)
        self.assertEqual(loaded["outcome_status"], "final_7d")
        self.assertEqual(loaded["outcome_final_reason"], "7d_window_reached")
        self.assertEqual(loaded["outcomes"][0]["window"], "7d")

    def test_skips_too_early(self) -> None:
        _seed_receipt(self.receipts_dir, flow="meme", run_id="2026-06-08",
                      publish_post_id="17912345")
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        # Collector firing 12h after publish — too early for the 24h window.
        now = published_at + timedelta(hours=12)
        post = _post(published_at=published_at.isoformat())
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=now, receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "too_early")
        loaded = load_creative_quality_receipt("meme", "2026-06-08",
                                               receipts_dir=self.receipts_dir)
        self.assertNotIn("outcomes", loaded)

    def test_skips_between_windows(self) -> None:
        _seed_receipt(self.receipts_dir, flow="meme", run_id="2026-06-08",
                      publish_post_id="17912345")
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        now = published_at + timedelta(hours=72)  # 3 days — between 24h and 7d
        post = _post(published_at=published_at.isoformat())
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=now, receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "out_of_window")


class WritebackFailureModesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.receipts_dir = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_skips_when_receipt_missing(self) -> None:
        """Phase 4 didn't run for this post — collector silently skips.
        Not all publishes go through the ranker (one-off publishes,
        legacy flows). This is the correct behavior."""
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        now = published_at + timedelta(hours=24)
        post = _post(published_at=published_at.isoformat())
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=now, receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "no_receipt")

    def test_skips_on_fetch_failed_metric_status(self) -> None:
        """IG returns a 4xx → status=fetch_failed. Don't write a bad
        outcome from a fetch that didn't succeed. Next daily run can
        retry — if the post is permanently deleted, the receipt just
        stays at pending."""
        _seed_receipt(self.receipts_dir, flow="meme", run_id="2026-06-08",
                      publish_post_id="x")
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        now = published_at + timedelta(hours=24)
        post = _post(metric_status="fetch_failed", published_at=published_at.isoformat())
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=now, receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "skipped")
        self.assertIn("fetch_failed", result["reason"])

    def test_skips_on_scheduled_future_status(self) -> None:
        post = _post(metric_status="scheduled_future")
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=datetime.now().astimezone(), receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "skipped")

    def test_post_id_mismatch_surfaces(self) -> None:
        """Delete-republish edge case: same workflow+run_id, different
        post_id from what's stamped on the receipt. Don't silently
        attach the new engagement to the old receipt."""
        _seed_receipt(self.receipts_dir, flow="meme", run_id="2026-06-08",
                      publish_post_id="original")
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        now = published_at + timedelta(hours=24)
        post = _post(post_id="republished", published_at=published_at.isoformat())
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=now, receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "post_id_mismatch")
        self.assertEqual(result["receipt_post_id"], "original")
        self.assertEqual(result["observed_post_id"], "republished")
        # And NO outcome was attached.
        loaded = load_creative_quality_receipt("meme", "2026-06-08",
                                               receipts_dir=self.receipts_dir)
        self.assertNotIn("outcomes", loaded)

    def test_missing_join_keys_skips(self) -> None:
        """If post has no workflow or run_id (legacy data), no writeback
        is possible. Skip cleanly."""
        post = _post()
        post["workflow"] = ""
        result = social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post, now=datetime.now().astimezone(), receipts_dir=self.receipts_dir
        )
        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "missing_join_keys")

    def test_idempotent_on_same_window_replay(self) -> None:
        """Daily collector runs → second day-of-24h hit must not append
        a duplicate outcome. The record_engagement_outcome helper
        handles this; smoke-check it from the writeback side too."""
        _seed_receipt(self.receipts_dir, flow="meme", run_id="2026-06-08",
                      publish_post_id="17912345")
        published_at = datetime(2026, 6, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
        now_first = published_at + timedelta(hours=24)
        now_second = published_at + timedelta(hours=27)  # Also in 24h window.
        post1 = _post(published_at=published_at.isoformat(), engagement_score=47.0)
        post2 = _post(published_at=published_at.isoformat(), engagement_score=99.0)
        social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post1, now=now_first, receipts_dir=self.receipts_dir
        )
        social_performance_collector.writeback_outcome_to_creative_quality_receipt(
            post2, now=now_second, receipts_dir=self.receipts_dir
        )
        loaded = load_creative_quality_receipt("meme", "2026-06-08",
                                               receipts_dir=self.receipts_dir)
        # Only ONE 24h outcome — first observation wins per helper contract.
        self.assertEqual(len(loaded["outcomes"]), 1)
        self.assertEqual(loaded["outcomes"][0]["engagement_score"], 47.0)


if __name__ == "__main__":
    unittest.main()
