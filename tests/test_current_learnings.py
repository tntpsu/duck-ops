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
            weekly_strategy_path = root / "state" / "weekly_strategy_recommendation_packet.json"
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
            weekly_strategy_path.write_text(
                json.dumps(
                    {
                        "social_plan": {
                            "headline": "Keep `meme` anchored, test one bounded alternate, and track whether the week stays on plan.",
                            "execution_feedback": {
                                "recommended_lane_executed": 1,
                                "alternate_lane_executed": 1,
                                "different_lane_executed": 0,
                                "awaiting_slot": 1,
                                "no_post_observed": 1,
                                "review_slot": 1,
                            },
                            "execution_truth": {
                                "label": "mixed",
                                "headline": "The weekly plan has mixed execution truth.",
                                "note": "`1` planned slot landed cleanly, `1` used fallback, and `1` still has no observed post.",
                            },
                            "lane_guidance_summary": {
                                "ready_to_scale": 0,
                                "keep_anchor": 1,
                                "fallback_only": 1,
                                "experiment_only": 0,
                                "pull_back": 0,
                            },
                            "lane_guidance": [
                                {
                                    "lane": "meme",
                                    "decision": "keep_anchor",
                                    "title": "`meme` should stay in the weekly anchor mix, but not scale yet.",
                                    "summary": "This lane still has enough direct proof to stay in the mix, but the evidence is not clean enough to expand it aggressively.",
                                    "recommended_action": "Keep `meme` in the weekly plan, but wait for another clean win before scaling it.",
                                    "evidence": "planned=2, recommended=1, fallback=0, slipped=1, missed=0, strong=1, supportive=0, weak=0",
                                    "confidence": "medium",
                                },
                                {
                                    "lane": "jeepfact",
                                    "decision": "fallback_only",
                                    "title": "`jeepfact` is helping as a fallback, but it has not won planned slots directly yet.",
                                    "summary": "Keep this lane available as a fallback or rescue lane until it lands clean planned-slot wins of its own.",
                                    "recommended_action": "Keep `jeepfact` as a fallback-only lane until it validates planned slots directly.",
                                    "evidence": "planned=1, recommended=0, fallback=1, slipped=0, missed=1, strong=0, supportive=1, weak=0",
                                    "confidence": "medium",
                                },
                            ],
                            "slots": [
                                {
                                    "slot": "Slot 1",
                                    "calendar_date": "2026-04-13",
                                    "calendar_label": "Monday evening",
                                    "suggested_lane": "meme",
                                    "tracking_status": "recommended_lane_executed",
                                    "tracking_note": "The recommended lane `meme` was observed on `2026-04-13`.",
                                    "actual_lane": "meme",
                                    "performance_label": "strong",
                                    "performance_note": "This landed in the top third of the current social window.",
                                },
                                {
                                    "slot": "Slot 2",
                                    "calendar_date": "2026-04-15",
                                    "calendar_label": "Wednesday evening",
                                    "suggested_lane": "meme",
                                    "alternate_lane": "jeepfact",
                                    "tracking_status": "alternate_lane_executed",
                                    "tracking_note": "The primary lane `meme` did not land, but the planned fallback `jeepfact` was observed on `2026-04-15`.",
                                    "actual_lane": "jeepfact",
                                    "performance_label": "watch",
                                    "performance_note": "This landed in the middle of the current social window.",
                                },
                                {
                                    "slot": "Slot 3",
                                    "calendar_date": "2026-04-16",
                                    "calendar_label": "Thursday evening",
                                    "suggested_lane": "jeepfact",
                                    "tracking_status": "no_post_observed",
                                    "tracking_note": "No observed social post was found for the `2026-04-16` target date yet.",
                                },
                            ],
                        }
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
                current_learnings, "WEEKLY_STRATEGY_PACKET_PATH", weekly_strategy_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            self.assertEqual(len(payload["current_beliefs"]), 6)
            self.assertEqual(len(payload["changes_since_previous"]), 7)
            self.assertIn(
                "competitor_social_freshness_degraded",
                {item.get("kind") for item in payload["changes_since_previous"] if isinstance(item, dict)},
            )
            self.assertTrue(payload["ideas_to_test"])
            self.assertEqual(payload["summary"]["competitor_social_post_count"], 12)
            self.assertEqual(payload["summary"]["competitor_social_snapshot_generated_at"], "2026-04-15T09:00:00-04:00")
            self.assertEqual(payload["summary"]["competitor_social_freshness_label"], "hard_failure")
            self.assertEqual(payload["summary"]["competitor_social_live_account_count"], 2)
            self.assertEqual(payload["summary"]["competitor_social_cached_account_count"], 1)
            self.assertEqual(payload["summary"]["competitor_social_degraded_account_count"], 1)
            self.assertEqual(payload["summary"]["competitor_social_failed_account_count"], 1)
            self.assertEqual(payload["summary"]["weekly_strategy_recommended_lane_executed_count"], 1)
            self.assertEqual(payload["summary"]["weekly_strategy_alternate_lane_executed_count"], 1)
            self.assertEqual(payload["summary"]["weekly_strategy_no_post_observed_count"], 1)
            self.assertEqual(payload["summary"]["weekly_strategy_execution_truth_label"], "mixed")
            self.assertEqual(payload["summary"]["weekly_strategy_lane_keep_anchor_count"], 1)
            self.assertEqual(payload["summary"]["weekly_strategy_lane_fallback_only_count"], 1)
            self.assertTrue(payload["weekly_strategy_feedback"]["available"])
            self.assertEqual(len(payload["weekly_strategy_feedback"]["slot_feedback_items"]), 3)
            missed_slot_feedback = payload["weekly_strategy_feedback"]["slot_feedback_items"][2]
            self.assertEqual(missed_slot_feedback["priority"], "missed_slot")
            self.assertIn("failed to publish", missed_slot_feedback["recommended_action"])
            self.assertEqual(payload["paths"]["weekly_strategy_packet"], str(weekly_strategy_path))
            self.assertTrue(state_path.exists())
            self.assertTrue(operator_json_path.exists())
            self.assertTrue(markdown_path.exists())
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("## Competitor Social Freshness", markdown)
            self.assertIn("## Weekly Strategy Follow-Through", markdown)
            self.assertIn("Planned lane wins", markdown)
            self.assertIn("Execution truth label", markdown)
            self.assertIn("### Lane Guidance", markdown)
            self.assertIn("### Slot Feedback", markdown)
            self.assertIn("Check whether the `jeepfact` slot failed to publish", markdown)
            self.assertIn("fallback_only", markdown)
            self.assertIn("Slot 2 shifted into alternate `jeepfact` instead of planned `meme`.", markdown)
            self.assertIn("Hard failure truth", markdown)
            self.assertIn("Cached fallback accounts", markdown)

    def test_build_current_learnings_marks_staggered_refresh_truth(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            weekly_strategy_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(json.dumps({"summary": {"post_count": 2, "metrics_coverage_pct": 100.0}}), encoding="utf-8")
            competitor_path.write_text(json.dumps({"summary": {}}), encoding="utf-8")
            competitor_social_path.write_text(json.dumps({"summary": {"post_count": 10}}), encoding="utf-8")
            weekly_strategy_path.write_text(json.dumps({}), encoding="utf-8")
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
                current_learnings, "WEEKLY_STRATEGY_PACKET_PATH", weekly_strategy_path
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

    def test_build_current_learnings_marks_profile_only_backoff_truth(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            weekly_strategy_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(json.dumps({"summary": {"post_count": 2, "metrics_coverage_pct": 100.0}}), encoding="utf-8")
            competitor_path.write_text(json.dumps({"summary": {}}), encoding="utf-8")
            competitor_social_path.write_text(json.dumps({"summary": {"post_count": 10}}), encoding="utf-8")
            weekly_strategy_path.write_text(json.dumps({}), encoding="utf-8")
            competitor_snapshots_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-15T09:00:00-04:00",
                        "summary": {
                            "post_count": 10,
                            "collected_account_count": 4,
                            "live_account_count": 1,
                            "cached_account_count": 3,
                            "degraded_account_count": 0,
                            "failed_account_count": 0,
                            "scheduled_skip_account_count": 3,
                            "profile_only_backoff_account_count": 2,
                            "active_refresh_target_count": 1,
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
                current_learnings, "WEEKLY_STRATEGY_PACKET_PATH", weekly_strategy_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            self.assertEqual(payload["summary"]["competitor_social_freshness_label"], "cached")
            self.assertEqual(payload["summary"]["competitor_social_profile_only_backoff_account_count"], 2)
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("Profile-only backoff truth", markdown)
            self.assertIn("Profile-only backoff accounts", markdown)

    def test_build_current_learnings_marks_live_canary_truth(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            weekly_strategy_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(json.dumps({"summary": {"post_count": 2, "metrics_coverage_pct": 100.0}}), encoding="utf-8")
            competitor_path.write_text(json.dumps({"summary": {}}), encoding="utf-8")
            competitor_social_path.write_text(json.dumps({"summary": {"post_count": 10}}), encoding="utf-8")
            weekly_strategy_path.write_text(json.dumps({}), encoding="utf-8")
            competitor_snapshots_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-15T09:00:00-04:00",
                        "summary": {
                            "post_count": 10,
                            "collected_account_count": 4,
                            "live_account_count": 0,
                            "cached_account_count": 4,
                            "degraded_account_count": 0,
                            "failed_account_count": 0,
                            "scheduled_skip_account_count": 4,
                            "live_canary_limited_account_count": 2,
                            "live_canary_target_count": 1,
                            "max_live_canary_targets": 1,
                            "active_refresh_target_count": 1,
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
                current_learnings, "WEEKLY_STRATEGY_PACKET_PATH", weekly_strategy_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            self.assertEqual(payload["summary"]["competitor_social_freshness_label"], "staggered")
            self.assertEqual(payload["summary"]["competitor_social_live_canary_limited_account_count"], 2)
            self.assertEqual(payload["summary"]["competitor_social_live_canary_target_count"], 1)
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("Live canary truth", markdown)
            self.assertIn("Live canary-limited accounts", markdown)

    def test_build_current_learnings_change_notifier_surfaces_material_changes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            weekly_strategy_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            state_path = root / "state" / "current_learnings.json"
            operator_json_path = root / "output" / "operator" / "current_learnings.json"
            markdown_path = root / "output" / "operator" / "current_learnings.md"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(json.dumps({"summary": {"post_count": 1, "metrics_coverage_pct": 100.0}}), encoding="utf-8")
            competitor_path.write_text(json.dumps({"summary": {}}), encoding="utf-8")
            competitor_social_path.write_text(json.dumps({"summary": {"post_count": 4}}), encoding="utf-8")
            competitor_snapshots_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-16T09:00:00-04:00",
                        "summary": {
                            "post_count": 4,
                            "collected_account_count": 3,
                            "live_account_count": 1,
                            "cached_account_count": 2,
                            "degraded_account_count": 1,
                            "failed_account_count": 0,
                        },
                    }
                ),
                encoding="utf-8",
            )
            weekly_strategy_path.write_text(
                json.dumps(
                    {
                        "social_plan": {
                            "headline": "Keep the best lane, but flag misses quickly.",
                            "execution_feedback": {
                                "recommended_lane_executed": 0,
                                "alternate_lane_executed": 0,
                                "different_lane_executed": 0,
                                "awaiting_slot": 0,
                                "no_post_observed": 1,
                                "review_slot": 0,
                            },
                            "slots": [
                                {
                                    "slot": "Slot 3",
                                    "calendar_date": "2026-04-16",
                                    "calendar_label": "Thursday evening",
                                    "suggested_lane": "jeepfact",
                                    "tracking_status": "no_post_observed",
                                    "tracking_note": "No observed social post was found for the target date yet.",
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "competitor_social_freshness_label": "live",
                        },
                        "weekly_strategy_feedback": {
                            "slot_outcomes": [
                                {
                                    "slot": "Slot 3",
                                    "tracking_status": "awaiting_slot",
                                    "suggested_lane": "jeepfact",
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(current_learnings, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                current_learnings, "COMPETITOR_BENCHMARK_PATH", competitor_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_BENCHMARK_PATH", competitor_social_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_snapshots_path
            ), patch.object(
                current_learnings, "WEEKLY_STRATEGY_PACKET_PATH", weekly_strategy_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            notifier = payload["change_notifier"]
            self.assertTrue(notifier["available"])
            self.assertGreaterEqual(notifier["material_change_count"], 2)
            self.assertTrue(any(item["kind"] == "weekly_strategy_slot_missed" for item in notifier["items"]))
            self.assertTrue(any(item["kind"] == "competitor_social_freshness_degraded" for item in notifier["items"]))
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("## Change Notifier", markdown)
            self.assertIn("Competitor social freshness degraded", markdown)

    def test_build_current_learnings_surfaces_strategy_pattern_changes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            weekly_strategy_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            state_path = root / "state" / "current_learnings.json"
            operator_json_path = root / "output" / "operator" / "current_learnings.json"
            markdown_path = root / "output" / "operator" / "current_learnings.md"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(json.dumps({"summary": {"post_count": 6, "metrics_coverage_pct": 100.0}}), encoding="utf-8")
            competitor_path.write_text(json.dumps({"summary": {}}), encoding="utf-8")
            competitor_social_path.write_text(json.dumps({"summary": {"post_count": 8}}), encoding="utf-8")
            competitor_snapshots_path.write_text(
                json.dumps({"generated_at": "2026-04-16T09:00:00-04:00", "summary": {"post_count": 8, "collected_account_count": 3, "live_account_count": 3}}),
                encoding="utf-8",
            )
            weekly_strategy_path.write_text(
                json.dumps(
                    {
                        "social_plan": {
                            "headline": "Keep one strong anchor lane this week.",
                            "execution_feedback": {},
                            "slots": [],
                        },
                        "stable_patterns": [
                            {
                                "title": "`evening` is still the default test window",
                                "recommendation": "Keep evening as the anchor window.",
                                "evidence": "Observed posts still cluster there.",
                                "confidence": "medium",
                            }
                        ],
                        "experimental_ideas": [
                            {
                                "title": "Borrow one bounded hook from `f3dprinted`",
                                "recommendation": "Test one bounded competitor-inspired hook.",
                                "evidence": "That account stayed steady across recent snapshots.",
                                "confidence": "low_medium",
                            }
                        ],
                        "do_not_copy_patterns": [
                            {
                                "title": "Do not chase degraded competitor snapshots too aggressively",
                                "guidance": "Keep tests small when freshness is mixed.",
                                "evidence": "Freshness can still swing quickly.",
                                "confidence": "high",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps(
                    {
                        "summary": {"competitor_social_freshness_label": "live"},
                        "weekly_strategy_feedback": {
                            "slot_outcomes": [],
                            "stable_patterns": [{"title": "`midday` used to be the default test window"}],
                            "experimental_ideas": [{"title": "Reuse the old caption family"}],
                            "do_not_copy_patterns": [{"title": "Old guardrail"}],
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(current_learnings, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                current_learnings, "COMPETITOR_BENCHMARK_PATH", competitor_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_BENCHMARK_PATH", competitor_social_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_snapshots_path
            ), patch.object(
                current_learnings, "WEEKLY_STRATEGY_PACKET_PATH", weekly_strategy_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            notifier = payload["change_notifier"]
            kinds = {item["kind"] for item in notifier["items"]}
            self.assertIn("weekly_strategy_stable_pattern_changed", kinds)
            self.assertIn("weekly_strategy_experiment_changed", kinds)
            self.assertIn("weekly_strategy_guardrail_changed", kinds)

    def test_build_current_learnings_surfaces_lane_guidance_changes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            social_path = root / "state" / "social_performance_rollups.json"
            competitor_path = root / "state" / "social_competitor_benchmark.json"
            competitor_social_path = root / "state" / "competitor_social_benchmark.json"
            competitor_snapshots_path = root / "state" / "competitor_social_snapshots.json"
            weekly_strategy_path = root / "state" / "weekly_strategy_recommendation_packet.json"
            state_path = root / "state" / "current_learnings.json"
            operator_json_path = root / "output" / "operator" / "current_learnings.json"
            markdown_path = root / "output" / "operator" / "current_learnings.md"
            social_path.parent.mkdir(parents=True, exist_ok=True)
            social_path.write_text(json.dumps({"summary": {"post_count": 6, "metrics_coverage_pct": 100.0}}), encoding="utf-8")
            competitor_path.write_text(json.dumps({"summary": {}}), encoding="utf-8")
            competitor_social_path.write_text(json.dumps({"summary": {"post_count": 8}}), encoding="utf-8")
            competitor_snapshots_path.write_text(
                json.dumps({"generated_at": "2026-04-16T09:00:00-04:00", "summary": {"post_count": 8, "collected_account_count": 3, "live_account_count": 3}}),
                encoding="utf-8",
            )
            weekly_strategy_path.write_text(
                json.dumps(
                    {
                        "social_plan": {
                            "headline": "Use execution truth before changing the calendar.",
                            "execution_feedback": {
                                "recommended_lane_executed": 2,
                                "alternate_lane_executed": 0,
                                "different_lane_executed": 0,
                                "awaiting_slot": 0,
                                "no_post_observed": 0,
                                "review_slot": 0,
                            },
                            "execution_truth": {
                                "label": "validated",
                                "headline": "The weekly plan is holding on executed slots.",
                                "note": "`meme` is now winning planned slots repeatedly without drift or misses.",
                            },
                            "lane_guidance_summary": {
                                "ready_to_scale": 1,
                                "keep_anchor": 0,
                                "fallback_only": 0,
                                "experiment_only": 0,
                                "pull_back": 1,
                            },
                            "lane_guidance": [
                                {
                                    "lane": "meme",
                                    "decision": "ready_to_scale",
                                    "title": "`meme` has enough repeated clean wins to scale carefully.",
                                    "summary": "This lane is earning repeated planned-slot wins, so it can absorb a little more weekly volume without becoming the whole calendar.",
                                    "recommended_action": "Promote `meme` into a stronger weekly anchor while continuing to watch for drift.",
                                    "evidence": "planned=2, recommended=2, fallback=0, slipped=0, missed=0, strong=2, supportive=0, weak=0",
                                    "confidence": "high",
                                },
                                {
                                    "lane": "jeepfact",
                                    "decision": "pull_back",
                                    "title": "`jeepfact` should not absorb more calendar volume yet.",
                                    "summary": "Recent misses, weak results, or lane drift mean this lane should stay constrained until execution stabilizes.",
                                    "recommended_action": "Do not scale `jeepfact` right now; tighten the concept or scheduling fit before giving it more volume.",
                                    "evidence": "planned=2, recommended=0, fallback=0, slipped=1, missed=1, strong=0, supportive=0, weak=1",
                                    "confidence": "high",
                                },
                            ],
                            "slots": [],
                        }
                    }
                ),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps(
                    {
                        "summary": {"competitor_social_freshness_label": "live"},
                        "weekly_strategy_feedback": {
                            "execution_truth": {"label": "mixed"},
                            "lane_guidance": [
                                {"lane": "meme", "decision": "keep_anchor"},
                                {"lane": "jeepfact", "decision": "fallback_only"},
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(current_learnings, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                current_learnings, "COMPETITOR_BENCHMARK_PATH", competitor_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_BENCHMARK_PATH", competitor_social_path
            ), patch.object(
                current_learnings, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_snapshots_path
            ), patch.object(
                current_learnings, "WEEKLY_STRATEGY_PACKET_PATH", weekly_strategy_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            notifier = payload["change_notifier"]
            kinds = {item["kind"] for item in notifier["items"]}
            self.assertIn("weekly_strategy_execution_truth_changed", kinds)
            self.assertIn("weekly_strategy_lane_ready_to_scale", kinds)
            self.assertIn("weekly_strategy_lane_pull_back", kinds)
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("ready_to_scale", markdown)
            self.assertIn("pull_back", markdown)


class WeeklyStrategySlotMissedFalsePositiveTests(unittest.TestCase):
    """Pins the 2026-06-04 false-positive incident as a regression
    test.

    Root cause: the weekly_strategy_recommendation_packet's
    `slot_outcomes` lags the live posts file by 10-20 min in the
    morning. current_learnings ran at 07:10 against a packet
    generated before social_performance_posts had been refreshed,
    saw Slot 2 (Wednesday) as `no_post_observed` even though
    yesterday's jeepfact post was sitting in the live posts file,
    and emitted a `weekly_strategy_slot_missed` change.

    Fix: at change-emit time, verify against the live posts file
    directly. If a matching post exists, suppress. If the live
    posts file is itself stale, emit `weekly_strategy_feed_stale`
    (different operator playbook) instead.
    """

    @staticmethod
    def _feedback_slot(*, slot: str, lane: str, status: str, date: str) -> dict:
        return {
            "slot": slot,
            "suggested_lane": lane,
            "tracking_status": status,
            "calendar_date": date,
            "performance_label": None,
            "actual_lane": None,
        }

    @staticmethod
    def _live_post(*, workflow: str, date: str, platform: str = "instagram",
                   is_future: bool = False) -> dict:
        return {
            "workflow": workflow,
            "platform": platform,
            "published_date": date,
            "published_at": f"{date}T18:00:00-04:00",
            "is_future_post": is_future,
        }

    def _previous_payload_for(self, slot: str, prior_status: str = "awaiting_slot") -> dict:
        return {
            "weekly_strategy_feedback": {
                "slot_outcomes": [
                    {"slot": slot, "tracking_status": prior_status}
                ],
            }
        }

    def test_live_post_matching_slot_suppresses_false_positive(self) -> None:
        """The 2026-06-04 incident shape: cached slot_outcomes says
        no_post_observed but the live posts file has a matching
        post. The slot_missed change MUST be suppressed."""
        feedback = {
            "slot_outcomes": [
                self._feedback_slot(
                    slot="Slot 2", lane="jeepfact",
                    status="no_post_observed", date="2026-06-03",
                ),
            ],
            "execution_truth": {},
        }
        from datetime import datetime
        live_posts = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "posts": [self._live_post(workflow="jeepfact", date="2026-06-03")],
        }
        changes = current_learnings._weekly_strategy_changes(
            feedback,
            self._previous_payload_for("Slot 2"),
            live_posts_payload=live_posts,
        )
        kinds = [c.get("kind") for c in changes]
        self.assertNotIn(
            "weekly_strategy_slot_missed", kinds,
            "Live posts file has matching jeepfact post for Slot 2 — "
            "the cached slot_outcomes was stale and the emitter must "
            "suppress the false positive (the 2026-06-04 incident).",
        )
        self.assertNotIn(
            "weekly_strategy_feed_stale", kinds,
            "Feed is fresh, so feed_stale must NOT be emitted either.",
        )

    def test_truly_missed_slot_still_emits(self) -> None:
        """Don't over-suppress: when the live posts file has no
        matching post for the slot, the change still fires."""
        feedback = {
            "slot_outcomes": [
                self._feedback_slot(
                    slot="Slot 1", lane="jeepfact",
                    status="no_post_observed", date="2026-06-01",
                ),
            ],
            "execution_truth": {},
        }
        from datetime import datetime
        # Live posts file has plenty of posts but NONE for Slot 1's date+lane.
        live_posts = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "posts": [
                self._live_post(workflow="meme", date="2026-06-01"),
                self._live_post(workflow="jeepfact", date="2026-06-03"),
            ],
        }
        changes = current_learnings._weekly_strategy_changes(
            feedback,
            self._previous_payload_for("Slot 1"),
            live_posts_payload=live_posts,
        )
        kinds = [c.get("kind") for c in changes]
        self.assertIn(
            "weekly_strategy_slot_missed", kinds,
            "When live posts truly have no matching post, the slot-"
            "missed change must still fire — don't over-suppress.",
        )

    def test_future_dated_post_does_not_count_as_observed(self) -> None:
        """A scheduled-but-not-yet-published post in the queue (e.g.
        from the IG local-queue sidecar) must NOT count as an
        observed post — otherwise a queued-for-later entry would
        silently suppress a real missed-slot signal."""
        feedback = {
            "slot_outcomes": [
                self._feedback_slot(
                    slot="Slot 2", lane="jeepfact",
                    status="no_post_observed", date="2026-06-03",
                ),
            ],
            "execution_truth": {},
        }
        from datetime import datetime
        live_posts = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "posts": [
                # Same date + lane as the slot, but is_future_post=True.
                self._live_post(
                    workflow="jeepfact", date="2026-06-03", is_future=True,
                ),
            ],
        }
        changes = current_learnings._weekly_strategy_changes(
            feedback,
            self._previous_payload_for("Slot 2"),
            live_posts_payload=live_posts,
        )
        kinds = [c.get("kind") for c in changes]
        self.assertIn(
            "weekly_strategy_slot_missed", kinds,
            "Future-dated posts don't prove the slot was filled — they "
            "could be scheduled-but-not-yet-fired entries.",
        )

    def test_stale_live_posts_file_emits_feed_stale_not_slot_missed(self) -> None:
        """If we can't trust the live posts file (older than 24h),
        emitting slot_missed would be guessing. Tell the operator
        the observability feed needs refreshing instead — different
        playbook, no operator panic about a missed lane."""
        feedback = {
            "slot_outcomes": [
                self._feedback_slot(
                    slot="Slot 2", lane="jeepfact",
                    status="no_post_observed", date="2026-06-03",
                ),
            ],
            "execution_truth": {},
        }
        from datetime import datetime, timedelta
        stale = (datetime.now().astimezone() - timedelta(hours=48)).isoformat()
        live_posts = {
            "generated_at": stale,
            "posts": [],
        }
        changes = current_learnings._weekly_strategy_changes(
            feedback,
            self._previous_payload_for("Slot 2"),
            live_posts_payload=live_posts,
        )
        kinds = [c.get("kind") for c in changes]
        self.assertNotIn(
            "weekly_strategy_slot_missed", kinds,
            "Stale feed can't prove no post — don't claim a missed slot.",
        )
        self.assertIn(
            "weekly_strategy_feed_stale", kinds,
            "Operator must be told the feed is stale (so they refresh "
            "it) rather than misled into thinking a lane was missed.",
        )

    def test_missing_live_posts_payload_falls_through_to_old_behavior(self) -> None:
        """Callers that don't pass live_posts_payload (e.g. legacy
        tests, ad-hoc invocations) keep the previous behavior — emit
        the slot_missed change. Backward compat."""
        feedback = {
            "slot_outcomes": [
                self._feedback_slot(
                    slot="Slot 2", lane="jeepfact",
                    status="no_post_observed", date="2026-06-03",
                ),
            ],
            "execution_truth": {},
        }
        changes = current_learnings._weekly_strategy_changes(
            feedback,
            self._previous_payload_for("Slot 2"),
        )
        kinds = [c.get("kind") for c in changes]
        self.assertIn(
            "weekly_strategy_slot_missed", kinds,
            "Without live_posts_payload, verification can't run — "
            "preserve the previous behavior so tests + ad-hoc callers "
            "don't silently change.",
        )

    def test_workflow_match_is_case_insensitive(self) -> None:
        """Live posts file uses lower-case workflow strings; packet
        slot_outcomes also lowercase. Edge case: a future schema
        drift could mix case. Match case-insensitively to be safe."""
        feedback = {
            "slot_outcomes": [
                self._feedback_slot(
                    slot="Slot 2", lane="JEEPFACT",  # upper-case
                    status="no_post_observed", date="2026-06-03",
                ),
            ],
            "execution_truth": {},
        }
        from datetime import datetime
        live_posts = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "posts": [self._live_post(workflow="jeepfact", date="2026-06-03")],
        }
        changes = current_learnings._weekly_strategy_changes(
            feedback,
            self._previous_payload_for("Slot 2"),
            live_posts_payload=live_posts,
        )
        kinds = [c.get("kind") for c in changes]
        self.assertNotIn("weekly_strategy_slot_missed", kinds)

    def test_only_relevant_slot_suppressed_not_others(self) -> None:
        """When the live posts file matches Slot 2 but not Slot 1,
        only Slot 2's slot_missed is suppressed; Slot 1's still fires.
        This is the actual production case from 2026-06-04."""
        feedback = {
            "slot_outcomes": [
                self._feedback_slot(
                    slot="Slot 1", lane="jeepfact",
                    status="no_post_observed", date="2026-06-01",
                ),
                self._feedback_slot(
                    slot="Slot 2", lane="jeepfact",
                    status="no_post_observed", date="2026-06-03",
                ),
            ],
            "execution_truth": {},
        }
        from datetime import datetime
        live_posts = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "posts": [self._live_post(workflow="jeepfact", date="2026-06-03")],
        }
        # Previous payload says both slots were previously awaiting.
        prev = {"weekly_strategy_feedback": {"slot_outcomes": [
            {"slot": "Slot 1", "tracking_status": "awaiting_slot"},
            {"slot": "Slot 2", "tracking_status": "awaiting_slot"},
        ]}}
        changes = current_learnings._weekly_strategy_changes(
            feedback, prev, live_posts_payload=live_posts,
        )
        slot_missed_headlines = [
            c.get("headline") for c in changes
            if c.get("kind") == "weekly_strategy_slot_missed"
        ]
        self.assertEqual(
            len(slot_missed_headlines), 1,
            f"Exactly one Slot 1 missed-slot should fire; got: {slot_missed_headlines}",
        )
        self.assertIn("Slot 1", slot_missed_headlines[0])
        self.assertNotIn("Slot 2", slot_missed_headlines[0])


if __name__ == "__main__":
    unittest.main()
