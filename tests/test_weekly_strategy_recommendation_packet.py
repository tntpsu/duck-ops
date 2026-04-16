from __future__ import annotations

from datetime import datetime
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
    def test_preferred_slot_lane_surfaces_fit_and_alternates(self) -> None:
        theme_choice = weekly_strategy_recommendation_packet._preferred_slot_lane(
            signal_type="competitor_theme",
            anchor_workflow="meme",
            available_workflows=["meme", "jeepfact", "review_carousel"],
            metadata={"theme_label": "trail stories"},
        )
        self.assertEqual(theme_choice["suggested_lane"], "jeepfact")
        self.assertEqual(theme_choice["lane_fit_strength"], "medium")
        self.assertEqual(theme_choice["alternate_lane"], "meme")
        self.assertIn("trail stories", theme_choice["lane_fit_reason"])
        self.assertIn("meme", theme_choice["alternate_lane_reason"])

        manual_choice = weekly_strategy_recommendation_packet._preferred_slot_lane(
            signal_type="competitor_format",
            anchor_workflow="meme",
            available_workflows=["meme", "jeepfact", "review_carousel"],
            metadata={"format_label": "video"},
        )
        self.assertEqual(manual_choice["suggested_lane"], "manual_social_experiment")
        self.assertEqual(manual_choice["lane_fit_strength"], "manual")
        self.assertEqual(manual_choice["alternate_lane"], "meme")
        self.assertIn("first-class `video` lane", manual_choice["lane_fit_reason"])
        self.assertIn("supported test", manual_choice["alternate_lane_reason"])

    def test_build_packet_combines_own_and_competitor_signals(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_posts_path = root / "state" / "social_performance_posts.json"
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_benchmark_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshot_path = root / "state" / "competitor_social_snapshots.json"
            competitor_snapshot_history_path = root / "state" / "competitor_social_snapshot_history.json"
            current_learnings_path = root / "state" / "current_learnings.json"
            state_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            operator_json_path = root / "output" / "operator" / "weekly_strategy_recommendation_packet.json"
            md_path = root / "output" / "operator" / "weekly_strategy_recommendation_packet.md"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_posts_path.write_text(
                json.dumps(
                    {
                        "summary": {"post_count": 3},
                        "posts": [
                            {
                                "workflow": "meme",
                                "platform": "instagram",
                                "post_id": "ig_monday",
                                "published_at": "2026-04-13T18:00:00-04:00",
                                "published_date": "2026-04-13",
                                "time_window": "evening",
                                "is_future_post": False,
                                "engagement_score": 23.0,
                                "engagement_rate": 0.1285,
                                "url": "https://example.com/meme",
                            },
                            {
                                "workflow": "review_carousel",
                                "platform": "instagram",
                                "post_id": "ig_tuesday",
                                "published_at": "2026-04-14T19:00:00-04:00",
                                "published_date": "2026-04-14",
                                "time_window": "evening",
                                "is_future_post": False,
                                "engagement_score": 3.0,
                                "engagement_rate": 0.0612,
                                "url": "https://example.com/review",
                            },
                            {
                                "workflow": "jeepfact",
                                "platform": "instagram",
                                "post_id": "ig_wednesday",
                                "published_at": "2026-04-15T18:00:00-04:00",
                                "published_date": "2026-04-15",
                                "time_window": "evening",
                                "is_future_post": False,
                                "engagement_score": 8.0,
                                "engagement_rate": 0.0625,
                                "url": "https://example.com/jeepfact",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            social_path.write_text(
                json.dumps(
                    {
                        "summary": {"post_count": 5, "metrics_coverage_pct": 100.0},
                        "rollups": {
                            "by_time_window": [{"label": "evening", "post_count": 3, "avg_engagement_score": 11.0}],
                            "by_workflow": [
                                {"label": "meme", "post_count": 2, "avg_engagement_score": 12.0},
                                {"label": "jeepfact", "post_count": 1, "avg_engagement_score": 8.0},
                                {"label": "review_carousel", "post_count": 1, "avg_engagement_score": 3.0},
                            ],
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
                            "live_canary_limited_account_count": 1,
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

            with patch.object(weekly_strategy_recommendation_packet, "_now_local", return_value=datetime.fromisoformat("2026-04-16T09:00:00-04:00")), patch.object(
                weekly_strategy_recommendation_packet, "SOCIAL_POSTS_PATH", social_posts_path
            ), patch.object(weekly_strategy_recommendation_packet, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
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
                self.assertGreaterEqual(payload["summary"]["stable_pattern_count"], 2)
                self.assertGreaterEqual(payload["summary"]["experimental_idea_count"], 2)
                self.assertGreaterEqual(payload["summary"]["do_not_copy_count"], 1)
                self.assertTrue(payload["social_plan"]["items"])
                self.assertTrue(payload["social_plan"]["slots"])
                self.assertEqual(payload["social_plan"]["readiness_counts"]["ready_with_approval"], 3)
                self.assertEqual(payload["social_plan"]["readiness_counts"]["manual_experiment"], 1)
                self.assertEqual(payload["social_plan"]["readiness_counts"]["ready_now"], 1)
                self.assertEqual(payload["social_plan"]["execution_feedback"]["recommended_lane_executed"], 1)
                self.assertEqual(payload["social_plan"]["execution_feedback"]["alternate_lane_executed"], 1)
                self.assertEqual(payload["social_plan"]["execution_feedback"]["awaiting_slot"], 1)
                self.assertEqual(payload["social_plan"]["execution_feedback"]["no_post_observed"], 1)
                self.assertEqual(payload["social_plan"]["execution_feedback"]["review_slot"], 1)
                self.assertTrue(payload["social_plan"]["ready_this_week"])
                self.assertTrue(payload["recommendations"])
                self.assertEqual(payload["social_plan"]["anchor_window"], "evening")
                self.assertEqual(payload["social_plan"]["slots"][0]["slot"], "Slot 1")
                self.assertIn("Early week", payload["social_plan"]["slots"][0]["timing_hint"])
                self.assertEqual(payload["social_plan"]["slots"][0]["suggested_lane"], "meme")
                self.assertEqual(payload["social_plan"]["slots"][0]["content_family"], "meme")
                self.assertEqual(payload["social_plan"]["slots"][0]["calendar_label"], "Monday evening")
                self.assertEqual(payload["social_plan"]["slots"][3]["calendar_label"], "Saturday evening")
                self.assertEqual(payload["social_plan"]["slots"][0]["execution_readiness"], "ready_with_approval")
                self.assertEqual(payload["social_plan"]["slots"][3]["execution_readiness"], "manual_experiment")
                self.assertEqual(payload["social_plan"]["slots"][4]["execution_readiness"], "ready_now")
                self.assertEqual(payload["social_plan"]["slots"][0]["lane_fit_strength"], "strong")
                self.assertIn("safest baseline lane", payload["social_plan"]["slots"][0]["lane_fit_reason"])
                self.assertEqual(payload["social_plan"]["slots"][3]["lane_fit_strength"], "manual")
                self.assertEqual(payload["social_plan"]["slots"][3]["alternate_lane"], "meme")
                self.assertIn("first-class `reel` lane", payload["social_plan"]["slots"][3]["lane_fit_reason"])
                self.assertEqual(payload["social_plan"]["slots"][0]["tracking_status"], "recommended_lane_executed")
                self.assertEqual(payload["social_plan"]["slots"][0]["actual_lane"], "meme")
                self.assertEqual(payload["social_plan"]["slots"][0]["performance_label"], "strong")
                self.assertEqual(payload["social_plan"]["slots"][1]["tracking_status"], "alternate_lane_executed")
                self.assertEqual(payload["social_plan"]["slots"][1]["actual_lane"], "jeepfact")
                self.assertEqual(payload["social_plan"]["slots"][1]["performance_label"], "watch")
                self.assertEqual(payload["social_plan"]["slots"][2]["tracking_status"], "no_post_observed")
                self.assertEqual(payload["social_plan"]["slots"][4]["tracking_status"], "review_slot")
                self.assertEqual(payload["social_plan"]["slots"][0]["operator_action_label"], "Run Meme Flow")
                self.assertIn("Reply `publish`", payload["social_plan"]["slots"][0]["approval_followthrough"])
                self.assertEqual(payload["social_plan"]["ready_this_week"][0]["lane_fit_strength"], "strong")
                self.assertEqual(payload["social_plan"]["ready_this_week"][0]["tracking_status"], "recommended_lane_executed")
                self.assertTrue(any(item["category"] == "stable_pattern" for item in payload["stable_patterns"]))
                self.assertTrue(any(item["category"] == "experimental_idea" for item in payload["experimental_ideas"]))
                self.assertTrue(any(item["category"] == "data_quality" for item in payload["recommendations"]))
                self.assertTrue(any("cached fallback" in item for item in payload["watchouts"]))
                self.assertTrue(any("profile-only backoff" in item.lower() for item in payload["watchouts"]))
                self.assertTrue(any("live canary policy" in item.lower() for item in payload["watchouts"]))
                self.assertTrue(state_path.exists())
                self.assertTrue(operator_json_path.exists())
                self.assertTrue(md_path.exists())
                markdown = md_path.read_text(encoding="utf-8")
                self.assertIn("## This Week's Social Plan", markdown)
                self.assertIn("## Stable Competitor Patterns", markdown)
                self.assertIn("## Experimental Ideas", markdown)
                self.assertIn("## Do Not Copy", markdown)
                self.assertIn("Slot 1", markdown)
                self.assertIn("Lane: `meme`", markdown)
                self.assertIn("Calendar: `Monday evening`", markdown)
                self.assertIn("## Ready This Week", markdown)
                self.assertIn("ready_with_approval", markdown)
                self.assertIn("Use: Run Meme Flow", markdown)
                self.assertIn("Then: Reply `publish`", markdown)
                self.assertIn("Fit: `manual`", markdown)
                self.assertIn("Alternate: `meme`", markdown)
                self.assertIn("Lane reason:", markdown)
                self.assertIn("Execution feedback: `recommended=1`, `alternate=1`", markdown)
                self.assertIn("Outcome: `alternate_lane_executed`", markdown)
                self.assertIn("Performance: `strong`", markdown)


if __name__ == "__main__":
    unittest.main()
