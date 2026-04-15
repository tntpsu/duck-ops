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

import weekly_strategy_recommendation_packet


class WeeklyStrategyRecommendationPacketTests(unittest.TestCase):
    def test_build_packet_combines_own_and_competitor_signals(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_benchmark_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshot_path = root / "state" / "competitor_social_snapshots.json"
            competitor_snapshot_history_path = root / "state" / "competitor_social_snapshot_history.json"
            current_learnings_path = root / "state" / "current_learnings.json"
            state_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            operator_json_path = root / "output" / "operator" / "weekly_strategy_recommendation_packet.json"
            md_path = root / "output" / "operator" / "weekly_strategy_recommendation_packet.md"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(
                json.dumps(
                    {
                        "summary": {"post_count": 5, "metrics_coverage_pct": 100.0},
                        "rollups": {
                            "by_time_window": [{"label": "evening", "post_count": 3, "avg_engagement_score": 11.0}],
                            "by_workflow": [{"label": "meme", "post_count": 2, "avg_engagement_score": 12.0}],
                        },
                    }
                ),
                encoding="utf-8",
            )
            competitor_benchmark_path.write_text(
                json.dumps(
                    {
                        "summary": {"post_count": 36},
                        "current_learnings": [{"evidence": "36 competitor posts."}],
                        "by_theme": [
                            {"label": "music", "post_count": 8, "avg_engagement_score": 25.0},
                            {"label": "decor", "post_count": 5, "avg_engagement_score": 12.0},
                        ],
                        "by_format": [
                            {"label": "reel", "post_count": 20, "avg_engagement_score": 31.0},
                            {"label": "image", "post_count": 10, "avg_engagement_score": 9.0},
                        ],
                        "ideas_to_test": [
                            "Test one `music`-themed post in a format we already execute well.",
                            "Validate whether `reel` is worth adding to our mix with one bounded experiment.",
                        ],
                    }
                ),
                encoding="utf-8",
            )
            competitor_snapshot_path.write_text(
                json.dumps(
                    {
                        "profiles": [
                            {"account_handle": "f3dprinted", "display_name": "f3dprinted"},
                            {"account_handle": "mattmade.me", "display_name": "MattMadeMe"},
                        ],
                        "summary": {
                            "live_account_count": 3,
                            "cached_account_count": 2,
                            "failed_account_count": 1,
                            "degraded_account_count": 3,
                            "profile_only_backoff_account_count": 1,
                        }
                    }
                ),
                encoding="utf-8",
            )
            competitor_snapshot_history_path.write_text(
                json.dumps(
                    {
                        "snapshots": [
                            {"top_account": "f3dprinted", "top_theme": "music"},
                            {"top_account": "f3dprinted", "top_theme": "music"},
                            {"top_account": "f3dprinted", "top_theme": "music"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            current_learnings_path.write_text(
                json.dumps({"changes_since_previous": [{"headline": "Top competitor account changed."}]}),
                encoding="utf-8",
            )

            with patch.object(weekly_strategy_recommendation_packet, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                weekly_strategy_recommendation_packet, "COMPETITOR_SOCIAL_BENCHMARK_PATH", competitor_benchmark_path
            ), patch.object(
                weekly_strategy_recommendation_packet, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_snapshot_path
            ), patch.object(
                weekly_strategy_recommendation_packet, "COMPETITOR_SOCIAL_SNAPSHOT_HISTORY_PATH", competitor_snapshot_history_path
            ), patch.object(
                weekly_strategy_recommendation_packet, "CURRENT_LEARNINGS_PATH", current_learnings_path
            ), patch.object(
                weekly_strategy_recommendation_packet, "PACKET_STATE_PATH", state_path
            ), patch.object(
                weekly_strategy_recommendation_packet, "PACKET_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                weekly_strategy_recommendation_packet, "PACKET_MD_PATH", md_path
            ):
                payload = weekly_strategy_recommendation_packet.build_weekly_strategy_recommendation_packet()
                self.assertEqual(payload["summary"]["recommendation_count"], len(payload["recommendations"]))
                self.assertEqual(payload["summary"]["watchout_count"], len(payload["watchouts"]))
                self.assertIn("f3dprinted", payload["summary"]["competitor_stability_note"])
                self.assertTrue(any(item["category"] == "timing" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "workflow" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "competitor_watch" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "competitor_theme" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "competitor_format" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "data_quality" for item in payload["recommendations"]))
                self.assertTrue(any("cached fallback" in item for item in payload["watchouts"]))
                self.assertTrue(any("profile-only backoff" in item.lower() for item in payload["watchouts"]))
                self.assertTrue(state_path.exists())
                self.assertTrue(operator_json_path.exists())
                self.assertTrue(md_path.exists())


if __name__ == "__main__":
    unittest.main()
