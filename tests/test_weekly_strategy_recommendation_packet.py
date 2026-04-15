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
            current_learnings_path.write_text(
                json.dumps({"changes_since_previous": [{"headline": "Top competitor account changed."}]}),
                encoding="utf-8",
            )

            with patch.object(weekly_strategy_recommendation_packet, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                weekly_strategy_recommendation_packet, "COMPETITOR_SOCIAL_BENCHMARK_PATH", competitor_benchmark_path
            ), patch.object(
                weekly_strategy_recommendation_packet, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_snapshot_path
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
                self.assertTrue(any(item["category"] == "timing" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "workflow" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "competitor_test" for item in payload["recommendations"]))
                self.assertTrue(any(item["category"] == "data_quality" for item in payload["recommendations"]))
                self.assertTrue(any("cached fallback" in item for item in payload["watchouts"]))
                self.assertTrue(any("profile-only backoff" in item.lower() for item in payload["watchouts"]))
                self.assertTrue(state_path.exists())
                self.assertTrue(operator_json_path.exists())
                self.assertTrue(md_path.exists())


if __name__ == "__main__":
    unittest.main()
