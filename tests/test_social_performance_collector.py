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

import social_performance_collector


class SocialPerformanceCollectorTests(unittest.TestCase):
    def test_build_social_performance_normalizes_receipts_and_rolls_up(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            receipt = root / "runs" / "run-1" / "meme_posts.json"
            receipt.parent.mkdir(parents=True, exist_ok=True)
            receipt.write_text(
                json.dumps(
                    {
                        "workflow": "meme",
                        "run_id": "run-1",
                        "posts": [
                            {
                                "platform": "instagram",
                                "post_id": "ig-1",
                                "status": "scheduled",
                                "scheduled_time": "2026-04-14T18:00:00-04:00",
                                "saved_at": "2026-04-14T12:00:00-04:00",
                                "url": "https://example.com/p/ig-1",
                                "meta_data": {
                                    "receipt_contract_version": 1,
                                    "content_type": "image",
                                    "title": "Cowgirl Duck",
                                    "theme": "cowgirl",
                                    "caption": "Meet the duck! #DuckDuckJeep #CowgirlDuck",
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            state_path = root / "state" / "social_performance_posts.json"
            rollups_path = root / "state" / "social_performance_rollups.json"
            output_path = root / "output" / "operator" / "social_insights.md"

            with patch.object(social_performance_collector, "STATE_PATH", state_path), patch.object(
                social_performance_collector, "ROLLUPS_PATH", rollups_path
            ), patch.object(
                social_performance_collector, "OUTPUT_MD_PATH", output_path
            ), patch.object(
                social_performance_collector, "_receipt_paths", return_value=[receipt]
            ), patch.object(
                social_performance_collector,
                "fetch_post_metrics",
                return_value={
                    "status": "ok",
                    "metrics": {"like_count": 11, "comments_count": 2, "reach": 100, "saved": 3, "permalink": "https://example.com/p/ig-1"},
                    "errors": [],
                },
            ), patch.object(
                social_performance_collector,
                "datetime",
                wraps=social_performance_collector.datetime,
            ) as mock_datetime:
                mock_datetime.now.return_value = social_performance_collector.datetime.fromisoformat("2026-04-15T09:00:00-04:00")
                post_payload, rollup_payload = social_performance_collector.build_social_performance(window_days=30, fetch_metrics=True)

            self.assertEqual(post_payload["summary"]["normalized_post_count"], 1)
            self.assertEqual(post_payload["posts"][0]["hashtags"], ["DuckDuckJeep", "CowgirlDuck"])
            self.assertEqual(post_payload["posts"][0]["engagement_score"], 16.0)
            self.assertEqual(rollup_payload["summary"]["metrics_coverage_pct"], 100.0)
            self.assertTrue(rollup_payload["current_learnings"])
            self.assertTrue(state_path.exists())
            self.assertTrue(rollups_path.exists())
            self.assertTrue(output_path.exists())

    def test_future_posts_are_not_fetched(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            receipt = root / "runs" / "run-2" / "jeepfact_posts.json"
            receipt.parent.mkdir(parents=True, exist_ok=True)
            receipt.write_text(
                json.dumps(
                    {
                        "workflow": "jeepfact",
                        "run_id": "run-2",
                        "posts": [
                            {
                                "platform": "instagram",
                                "post_id": "ig-future",
                                "status": "scheduled",
                                "scheduled_time": "2026-04-16T18:00:00-04:00",
                                "saved_at": "2026-04-15T12:00:00-04:00",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(social_performance_collector, "_receipt_paths", return_value=[receipt]), patch.object(
                social_performance_collector,
                "datetime",
                wraps=social_performance_collector.datetime,
            ) as mock_datetime:
                mock_datetime.now.return_value = social_performance_collector.datetime.fromisoformat("2026-04-15T09:00:00-04:00")
                post_payload, rollup_payload = social_performance_collector.build_social_performance_payload(window_days=30, fetch_metrics=True)

            self.assertEqual(post_payload["posts"][0]["metric_status"], "scheduled_future")
            self.assertEqual(post_payload["summary"]["metric_status_counts"]["scheduled_future"], 1)
            self.assertEqual(rollup_payload["summary"]["metrics_coverage_pct"], 0.0)

    def test_render_social_insights_mentions_data_quality(self) -> None:
        markdown = social_performance_collector.render_social_insights_markdown(
            {
                "summary": {
                    "metric_status_counts": {"ok": 1, "partial": 1},
                    "malformed_receipt_count": 0,
                }
            },
            {
                "generated_at": "2026-04-15T09:00:00-04:00",
                "window_days": 30,
                "summary": {
                    "post_count": 2,
                    "metrics_coverage_pct": 50.0,
                    "data_quality_note": "Receipt history is still sparse.",
                },
                "current_learnings": [
                    {
                        "headline": "Evening is the current best-performing posting window.",
                        "confidence": "low",
                        "evidence": "2 posts with average engagement score 10.",
                        "recommendation": "Keep testing evening.",
                    }
                ],
                "top_posts": [
                    {
                        "workflow": "meme",
                        "platform": "instagram",
                        "post_id": "123",
                        "title": "Cowgirl Duck",
                        "url": "https://example.com/p/123",
                        "engagement_score": 10,
                        "engagement_rate": 0.1,
                    }
                ],
                "rollups": {
                    "by_workflow": [{"label": "meme", "post_count": 2, "avg_engagement_score": 10, "avg_engagement_rate": 0.1}],
                    "by_platform": [],
                    "by_time_window": [],
                    "by_theme": [],
                },
            },
        )

        self.assertIn("Current Learnings", markdown)
        self.assertIn("Receipt history is still sparse", markdown)
        self.assertIn("Cowgirl Duck", markdown)


if __name__ == "__main__":
    unittest.main()
