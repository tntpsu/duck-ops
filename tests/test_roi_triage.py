from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import roi_triage as roi_triage_module
from roi_triage import build_roi_triage, render_roi_triage_markdown


class RoiTriageTests(unittest.TestCase):
    def test_roi_triage_ranks_recommendations(self) -> None:
        payload = build_roi_triage(write_outputs=False)
        recommendations = payload.get("recommendations") or []

        self.assertGreaterEqual(len(recommendations), 3)
        self.assertEqual(recommendations[0]["rank"], 1)
        scores = [item["score_breakdown"]["roi_score"] for item in recommendations]
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertTrue(all(item.get("lifecycle_status") != "completed" for item in recommendations))

    def test_roi_triage_filters_completed_curated_work(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            creative_policies = root / "creative_policies.py"
            creative_tasks = root / "tasks.py"
            business_desk = root / "business_operator_desk.py"
            design_doc = root / "DESIGN_BRIEF_QUEUE_PLAN.md"
            readme = root / "README.md"
            creative_policies.write_text(
                "def run_semantic_visual_qa(): pass\nsemantic_visual_review = True\nDUCK_SEMANTIC_VISUAL_QA = 'auto'\n",
                encoding="utf-8",
            )
            creative_tasks.write_text(
                "def _filter_public_design_brief_input(input_model):\n    filtered_private_signals = []\n",
                encoding="utf-8",
            )
            business_desk.write_text(
                "def _load_maintenance_freshness_surface(): pass\nmaintenance_freshness_surface = {}\n",
                encoding="utf-8",
            )
            design_doc.write_text("PUBLIC_CONCEPT_ALLOWED", encoding="utf-8")
            readme.write_text("maintenance freshness", encoding="utf-8")

            with patch.object(roi_triage_module, "CREATIVE_POLICIES_PATH", creative_policies), patch.object(
                roi_triage_module, "CREATIVE_TASKS_PATH", creative_tasks
            ), patch.object(roi_triage_module, "BUSINESS_OPERATOR_DESK_PATH", business_desk), patch.object(
                roi_triage_module, "DESIGN_BRIEF_QUEUE_DOC_PATH", design_doc
            ), patch.object(
                roi_triage_module, "README_PATH", readme
            ):
                payload = build_roi_triage(write_outputs=False)

        active_ids = {item.get("candidate_id") for item in payload.get("recommendations") or []}
        completed_ids = {item.get("candidate_id") for item in payload.get("recently_completed") or []}

        self.assertNotIn("semantic-visual-qa", active_ids)
        self.assertNotIn("design-brief-source-hygiene", active_ids)
        self.assertNotIn("maintenance-freshness-desk", active_ids)
        self.assertIn("semantic-visual-qa", completed_ids)
        self.assertIn("design-brief-source-hygiene", completed_ids)
        self.assertIn("maintenance-freshness-desk", completed_ids)

    def test_roi_triage_suppresses_stale_digest_recommendations(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest_path = root / "engineering_governance_digest.json"
            data_model_path = root / "data_model_governance_review.json"
            digest_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-21T08:00:00-04:00",
                        "review_recommendations": [
                            {
                                "priority": "P1",
                                "source": "data_model_governance_review",
                                "title": "documentation_governance_review contract drift risk",
                                "summary": "Required contract files are missing.",
                                "next_action": "Refresh the canonical writer/reader contract.",
                                "suggested_owner_skill": "duck-data-model-governance",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            data_model_path.write_text(
                json.dumps({"generated_at": "2026-04-22T08:00:00-04:00", "surfaces": []}),
                encoding="utf-8",
            )

            with patch.object(roi_triage_module, "GOVERNANCE_PATH", digest_path), patch.dict(
                roi_triage_module.GOVERNANCE_SOURCE_PATHS,
                {"data_model_governance_review": data_model_path},
            ):
                payload = build_roi_triage(write_outputs=False)

        titles = {item.get("title") for item in payload.get("recommendations") or []}
        suppressed_titles = {item.get("title") for item in payload.get("suppressed_recommendations") or []}

        self.assertNotIn("documentation_governance_review contract drift risk", titles)
        self.assertIn("Refresh engineering governance digest", titles)
        self.assertIn("documentation_governance_review contract drift risk", suppressed_titles)
        self.assertEqual((payload.get("summary") or {}).get("stale_recommendation_count"), 1)

    def test_roi_triage_markdown_is_operator_readable(self) -> None:
        payload = {
            "generated_at": "2026-04-26T08:00:00-04:00",
            "summary": {
                "candidate_count": 1,
                "completed_count": 1,
                "stale_recommendation_count": 1,
                "top_score": 4.4,
                "headline": "Top ROI slice: Semantic visual QA.",
                "recommended_action": "Run the checker.",
            },
            "recommendations": [
                {
                    "rank": 1,
                    "title": "Semantic visual QA",
                    "why_now": "Image drift needs a real gate.",
                    "recommended_next_slice": "Run the checker.",
                    "score_breakdown": {"roi_score": 4.4},
                    "owner_skill": "duck-reliability-review",
                    "constraints": ["Manual review if the model key is missing."],
                }
            ],
            "recently_completed": [
                {
                    "candidate_id": "design-brief-source-hygiene",
                    "title": "Design brief source hygiene",
                    "lifecycle_reason": "Already implemented.",
                }
            ],
            "suppressed_recommendations": [
                {
                    "title": "documentation_governance_review contract drift risk",
                    "source_review": "data_model_governance_review",
                    "lifecycle_reason": "Source review is newer.",
                }
            ],
        }

        markdown = render_roi_triage_markdown(payload)

        self.assertIn("# Duck ROI Triage", markdown)
        self.assertIn("Semantic visual QA", markdown)
        self.assertIn("duck-reliability-review", markdown)
        self.assertIn("Recently Completed / Filtered", markdown)
        self.assertIn("Suppressed Stale Signals", markdown)


if __name__ == "__main__":
    unittest.main()
