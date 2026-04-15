from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import current_learnings


class CurrentLearningsTests(unittest.TestCase):
    def test_build_current_learnings_combines_social_and_competitor_inputs(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(
                json.dumps(
                    {
                        "summary": {"post_count": 4, "metrics_coverage_pct": 75.0, "data_quality_note": "Sparse but useful."},
                        "current_learnings": [{"headline": "Evening works best.", "confidence": "low", "evidence": "2 posts", "recommendation": "Keep testing."}],
                        "changes_since_previous": [{"headline": "Best posting window changed.", "kind": "window_shift"}],
                        "rollups": {
                            "by_time_window": [{"label": "evening", "post_count": 2, "avg_engagement_score": 11.0}],
                            "by_workflow": [{"label": "meme", "post_count": 2, "avg_engagement_score": 12.0}],
                        },
                        "top_posts": [{"title": "Cowgirl Duck", "platform": "instagram", "post_id": "123"}],
                    }
                ),
                encoding="utf-8",
            )
            competitor_path.write_text(
                json.dumps(
                    {
                        "summary": {"observation_days": 10},
                        "market_learnings": [{"headline": "Cowgirl is trending with competitors.", "confidence": "medium", "evidence": "6 listings", "recommendation": "Test content first."}],
                        "changes_since_previous": [{"headline": "Top motif changed.", "kind": "motif_shift"}],
                        "emergent_motifs": [{"keyword": "cowgirl", "score": 10, "listing_count": 6}],
                        "ideas_to_test": ["Test a `cowgirl`-led duck or post angle; competitors are surfacing it across `6` recent listings."],
                    }
                ),
                encoding="utf-8",
            )
            competitor_social_path.write_text(
                json.dumps(
                    {
                        "summary": {"post_count": 12},
                        "current_learnings": [
                            {
                                "headline": "Reels are the dominant competitor format.",
                                "confidence": "medium",
                                "evidence": "8 competitor posts",
                                "recommendation": "Test one reel without changing cadence broadly.",
                            }
                        ],
                        "changes_since_previous": [{"headline": "Top competitor account changed.", "kind": "account_shift"}],
                        "by_theme": [{"label": "music", "post_count": 5, "avg_engagement_score": 18.0}],
                        "ideas_to_test": ["Try one `engagement_prompt` hook on a music-themed post."],
                    }
                ),
                encoding="utf-8",
            )
            competitor_snapshots_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-15T09:00:00-04:00",
                        "summary": {
                            "post_count": 12,
                            "collected_account_count": 4,
                            "live_account_count": 2,
                            "cached_account_count": 1,
                            "degraded_account_count": 1,
                            "failed_account_count": 1,
                            "data_quality_note": "Snapshot collector reused cache for one account and hard-failed on another.",
                        },
                    }
                ),
                encoding="utf-8",
            )
            state_path = root / "state" / "current_learnings.json"
            operator_json_path = root / "output" / "operator" / "current_learnings.json"
            markdown_path = root / "output" / "operator" / "current_learnings.md"

            with patch.object(current_learnings, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                current_learnings, "COMPETITOR_BENCHMARK_PATH", competitor_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_BENCHMARK_PATH", competitor_social_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_snapshots_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            self.assertEqual(len(payload["current_beliefs"]), 3)
            self.assertEqual(len(payload["changes_since_previous"]), 3)
            self.assertTrue(payload["ideas_to_test"])
            self.assertEqual(payload["summary"]["competitor_social_post_count"], 12)
            self.assertEqual(payload["summary"]["competitor_social_snapshot_generated_at"], "2026-04-15T09:00:00-04:00")
            self.assertEqual(payload["summary"]["competitor_social_freshness_label"], "hard_failure")
            self.assertEqual(payload["summary"]["competitor_social_live_account_count"], 2)
            self.assertEqual(payload["summary"]["competitor_social_cached_account_count"], 1)
            self.assertEqual(payload["summary"]["competitor_social_degraded_account_count"], 1)
            self.assertEqual(payload["summary"]["competitor_social_failed_account_count"], 1)
            self.assertTrue(state_path.exists())
            self.assertTrue(operator_json_path.exists())
            self.assertTrue(markdown_path.exists())
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("## Competitor Social Freshness", markdown)
            self.assertIn("Hard failure truth", markdown)
            self.assertIn("Cached fallback accounts", markdown)

    def test_build_current_learnings_marks_staggered_refresh_truth(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(json.dumps({"summary": {"post_count": 2, "metrics_coverage_pct": 100.0}}), encoding="utf-8")
            competitor_path.write_text(json.dumps({"summary": {}}), encoding="utf-8")
            competitor_social_path.write_text(json.dumps({"summary": {"post_count": 10}}), encoding="utf-8")
            competitor_snapshots_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-15T09:00:00-04:00",
                        "summary": {
                            "post_count": 10,
                            "collected_account_count": 4,
                            "live_account_count": 2,
                            "cached_account_count": 2,
                            "degraded_account_count": 0,
                            "failed_account_count": 0,
                            "scheduled_skip_account_count": 2,
                            "active_refresh_target_count": 2,
                        },
                    }
                ),
                encoding="utf-8",
            )
            state_path = root / "state" / "current_learnings.json"
            operator_json_path = root / "output" / "operator" / "current_learnings.json"
            markdown_path = root / "output" / "operator" / "current_learnings.md"

            with patch.object(current_learnings, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                current_learnings, "COMPETITOR_BENCHMARK_PATH", competitor_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_BENCHMARK_PATH", competitor_social_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_snapshots_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            self.assertEqual(payload["summary"]["competitor_social_freshness_label"], "staggered")
            self.assertEqual(payload["summary"]["competitor_social_scheduled_skip_account_count"], 2)
            self.assertEqual(payload["summary"]["competitor_social_active_refresh_target_count"], 2)
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("Staggered refresh truth", markdown)


if __name__ == "__main__":
    unittest.main()
