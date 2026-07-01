from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import product_concept_queue


class ProductConceptQueueTests(unittest.TestCase):
    def test_gap_trend_becomes_design_brief_signal(self) -> None:
        payload = product_concept_queue.build_product_concept_queue(
            trend_candidates={
                "items": [
                    {
                        "artifact_id": "trend::pizza-fidget-duck",
                        "theme": "pizza fidget duck",
                        "source_refs": [{"path": "state/normalized/trend_candidates.json"}],
                        "signal_summary": {
                            "trending_score": 820,
                            "sold_last_7d": 4,
                            "revenue_last_7d": 72.0,
                        },
                        "catalog_match": {"status": "gap"},
                        "input_confidence_cap": 0.75,
                    }
                ]
            },
            current_learnings={},
            competitor_social_benchmark={},
            write_outputs=False,
        )

        self.assertEqual(payload["status"], "ready_for_brief_review")
        self.assertEqual(payload["summary"]["ready_for_brief_review_count"], 1)
        item = payload["items"][0]
        self.assertEqual(item["theme"], "Pizza")
        self.assertEqual(item["queue_state"], "ready_for_brief_review")
        self.assertIn("public_concept_allowed", item["guardrails"])
        design_signal = payload["design_brief_input"]["candidate_signals"][0]
        self.assertEqual(design_signal["theme"], "Pizza")
        self.assertEqual(design_signal["source"], "duck-ops.product_concept_queue")
        self.assertEqual(payload["design_brief_input"]["source_contract"], "duck-ops.product_concept_queue")
        self.assertEqual(design_signal["concept_design_brief"]["concept_title"], "Pizza Duck")
        self.assertEqual(design_signal["trend_quality_gate"]["status"], "needs_reframe")

    def test_ip_sensitive_trend_is_blocked_from_design_brief_input(self) -> None:
        payload = product_concept_queue.build_product_concept_queue(
            trend_candidates={
                "items": [
                    {
                        "artifact_id": "trend::tennessee-vols-duck",
                        "theme": "tennessee vols duck",
                        "source_refs": [{"path": "state/normalized/trend_candidates.json"}],
                        "signal_summary": {
                            "trending_score": 900,
                            "sold_last_7d": 5,
                        },
                        "catalog_match": {"status": "gap"},
                        "input_confidence_cap": 0.75,
                    }
                ]
            },
            current_learnings={},
            competitor_social_benchmark={},
            write_outputs=False,
        )

        self.assertEqual(payload["status"], "blocked_by_guardrail")
        self.assertEqual(payload["summary"]["blocked_by_guardrail_count"], 1)
        self.assertEqual(payload["items"][0]["queue_state"], "blocked_by_guardrail")
        self.assertEqual(payload["items"][0]["trend_quality_gate"]["status"], "blocked_by_policy")
        self.assertEqual(payload["design_brief_input"]["candidate_signals"], [])

    def test_operator_feedback_suppresses_rejected_concept_from_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            feedback_path = Path(tmpdir) / "product_concept_feedback.json"
            feedback_path.write_text(
                json.dumps(
                    {
                        "schema_version": "duck.product_concept_feedback.v1",
                        "concepts": {
                            "pizza": {
                                "latest_resolution": "discarded",
                                "latest_reason": "not a product direction we want",
                                "updated_at": "2026-05-18T08:00:00-04:00",
                                "aliases": ["pizza", "pizza-fidget"],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(product_concept_queue, "PRODUCT_CONCEPT_FEEDBACK_PATH", feedback_path):
                payload = product_concept_queue.build_product_concept_queue(
                    trend_candidates={
                        "items": [
                            {
                                "artifact_id": "trend::pizza-fidget-duck",
                                "theme": "pizza fidget duck",
                                "source_refs": [{"path": "state/normalized/trend_candidates.json"}],
                                "signal_summary": {
                                    "trending_score": 820,
                                    "sold_last_7d": 4,
                                    "revenue_last_7d": 72.0,
                                },
                                "catalog_match": {"status": "gap"},
                                "input_confidence_cap": 0.75,
                            }
                        ]
                    },
                    current_learnings={},
                    competitor_social_benchmark={},
                    write_outputs=False,
                )

        self.assertEqual(payload["status"], "clear")
        self.assertEqual(payload["summary"]["ready_for_brief_review_count"], 0)
        self.assertEqual(payload["summary"]["suppressed_by_operator_count"], 1)
        self.assertEqual(payload["design_brief_input"]["candidate_signals"], [])

    def test_raw_relationship_theme_is_blocked_until_reframed(self) -> None:
        payload = product_concept_queue.build_product_concept_queue(
            trend_candidates={
                "items": [
                    {
                        "artifact_id": "trend::child-maternal-love-duck",
                        "theme": "child maternal love duck",
                        "source_refs": [{"path": "state/normalized/trend_candidates.json"}],
                        "signal_summary": {"trending_score": 900, "sold_last_7d": 5},
                        "catalog_match": {"status": "gap"},
                    }
                ]
            },
            current_learnings={},
            competitor_social_benchmark={},
            write_outputs=False,
        )

        self.assertEqual(payload["status"], "blocked_by_guardrail")
        self.assertEqual(payload["summary"]["ready_for_brief_review_count"], 0)
        self.assertEqual(payload["items"][0]["queue_state"], "blocked_by_guardrail")
        self.assertEqual(payload["items"][0]["name_quality"]["status"], "needs_reframe")
        self.assertEqual(payload["design_brief_input"]["candidate_signals"], [])

    def test_school_and_sport_themes_need_manual_abstraction(self) -> None:
        payload = product_concept_queue.build_product_concept_queue(
            trend_candidates={
                "items": [
                    {
                        "artifact_id": "trend::gcu-lopes-duck",
                        "theme": "gcu lopes duck",
                        "source_refs": [{"path": "state/normalized/trend_candidates.json"}],
                        "signal_summary": {"trending_score": 900, "sold_last_7d": 5},
                        "catalog_match": {"status": "gap"},
                    },
                    {
                        "artifact_id": "trend::chicago-football-duck",
                        "theme": "chicago football duck",
                        "source_refs": [{"path": "state/normalized/trend_candidates.json"}],
                        "signal_summary": {"trending_score": 800, "sold_last_7d": 4},
                        "catalog_match": {"status": "gap"},
                    },
                ]
            },
            current_learnings={},
            competitor_social_benchmark={},
            write_outputs=False,
        )

        self.assertEqual(payload["summary"]["ready_for_brief_review_count"], 0)
        self.assertEqual(payload["summary"]["blocked_by_guardrail_count"], 2)
        self.assertEqual(payload["design_brief_input"]["candidate_signals"], [])

    def test_writes_queue_and_duckagent_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            queue_path = root / "product_concept_queue.json"
            operator_json_path = root / "operator" / "product_concept_queue.json"
            operator_md_path = root / "operator" / "product_concept_queue.md"
            design_input_path = root / "product_concept_queue_design_brief_input.json"

            with (
                mock.patch.object(product_concept_queue, "PRODUCT_CONCEPT_QUEUE_PATH", queue_path),
                mock.patch.object(product_concept_queue, "PRODUCT_CONCEPT_QUEUE_OPERATOR_JSON_PATH", operator_json_path),
                mock.patch.object(product_concept_queue, "PRODUCT_CONCEPT_QUEUE_MD_PATH", operator_md_path),
                mock.patch.object(product_concept_queue, "PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH", design_input_path),
            ):
                product_concept_queue.build_product_concept_queue(
                    trend_candidates={
                        "items": [
                            {
                                "artifact_id": "trend::orange-cat-duck",
                                "theme": "orange cat duck",
                                "source_refs": [{"path": "state/normalized/trend_candidates.json"}],
                                "signal_summary": {"trending_score": 700, "sold_last_7d": 3},
                                "catalog_match": {"status": "gap"},
                            }
                        ]
                    },
                    current_learnings={},
                    competitor_social_benchmark={},
                )

            self.assertTrue(queue_path.exists())
            self.assertTrue(operator_json_path.exists())
            self.assertTrue(operator_md_path.exists())
            self.assertTrue(design_input_path.exists())
            design_input = json.loads(design_input_path.read_text(encoding="utf-8"))
            self.assertEqual(design_input["channel"], "product_concept")
            self.assertEqual(design_input["candidate_signals"][0]["concept_design_brief"]["concept_title"], "Orange Cat Duck")


class BuildNextPromotionIngestionTests(unittest.TestCase):
    """Surface 16 Phase C: operator promotions from the Build-Next page
    flow into the concept queue as design briefs (brief_source=build_next),
    but off-policy names still fail closed."""

    def test_promoted_concept_becomes_ready_brief(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promo_path = Path(tmp) / "build_next_promotions.json"
            promo_path.write_text(json.dumps({"promotions": [
                {"concept_key": "medieval-knight", "title": "Medieval Knight Duck",
                 "listing_id": "9"}]}), encoding="utf-8")
            with mock.patch.object(product_concept_queue,
                                   "BUILD_NEXT_PROMOTIONS_PATH", promo_path):
                payload = product_concept_queue.build_product_concept_queue(
                    trend_candidates={"items": []}, current_learnings={},
                    competitor_social_benchmark={}, write_outputs=False)
        items = [i for i in payload["items"] if i.get("source_type") == "build_next_promotion"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["queue_state"], "ready_for_brief_review")
        self.assertEqual(
            items[0]["concept_design_brief"].get("brief_source"), "build_next")

    def test_off_policy_promoted_name_fails_closed(self) -> None:
        items = product_concept_queue._build_next_promotion_items(
            {"promotions": [{"concept_key": "tennessee-vols",
                             "title": "Tennessee Vols Duck", "listing_id": "1"}]})
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["queue_state"], "blocked_by_guardrail")

    def test_empty_promotions_is_noop(self) -> None:
        self.assertEqual(product_concept_queue._build_next_promotion_items({"promotions": []}), [])
        self.assertEqual(product_concept_queue._build_next_promotion_items({}), [])


class CrossScoutMergeTests(unittest.TestCase):
    """Surface 49 follow-up: two scouts proposing the same duck under
    differently-cleaned themes must collapse to one queue item. The old
    exact-slug key missed keyword-stuffed themes; the boilerplate-stripped
    merge key catches them."""

    def _key(self, theme):
        return product_concept_queue._theme_merge_key(theme)

    def test_merge_key_strips_boilerplate_to_core_subject(self) -> None:
        # The real miss: trend "Chef" vs a Build-Next promote cleaned to
        # "3d Chef Pla Plastic" — same subject, now the same key.
        self.assertEqual(self._key("Chef"), "chef")
        self.assertEqual(self._key("3d Chef Pla Plastic"), "chef")
        self.assertEqual(self._key("Golden Retriever"), self._key("Golden Retriever Duck 3D Printed"))

    def test_merge_key_keeps_distinct_subjects_apart(self) -> None:
        # wine <-> highland cow must NOT collapse (thematic words survive).
        self.assertNotEqual(self._key("Wine Ducks Adventure Collectibles"),
                            self._key("Highland Cow Adventure Rustic"))

    def test_merge_key_all_boilerplate_falls_back_to_slug(self) -> None:
        # If nothing survives the strip, keep the raw slug so two all-boilerplate
        # themes stay distinct instead of both collapsing to "".
        self.assertEqual(self._key("Car Duck"), "car-duck")
        self.assertNotEqual(self._key("Car Duck"), self._key("Jeep Duck"))

    def test_merge_collapses_cross_scout_duplicate(self) -> None:
        items = [
            {"theme": "Chef", "source_type": "trend_candidate", "score": 0.5,
             "evidence": ["trend chef"], "guardrails": ["a"]},
            {"theme": "3d Chef Pla Plastic", "source_type": "build_next_promotion",
             "score": 0.8, "evidence": ["competitor chef"], "guardrails": ["b"]},
        ]
        merged = product_concept_queue._merge_duplicate_themes(items)
        self.assertEqual(len(merged), 1)                      # collapsed
        self.assertEqual(merged[0]["score"], 0.8)             # higher-score wins
        self.assertIn("b", merged[0]["guardrails"])
        self.assertIn("a", merged[0]["guardrails"])           # evidence/guardrails combined

    def test_merge_keeps_distinct_concepts_separate(self) -> None:
        items = [
            {"theme": "Wine Ducks Adventure", "source_type": "trend_candidate", "score": 0.5},
            {"theme": "Highland Cow Adventure Rustic", "source_type": "competitor_motif", "score": 0.6},
        ]
        self.assertEqual(len(product_concept_queue._merge_duplicate_themes(items)), 2)


if __name__ == "__main__":
    unittest.main()
