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


if __name__ == "__main__":
    unittest.main()
