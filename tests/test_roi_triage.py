from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from roi_triage import build_roi_triage, render_roi_triage_markdown


class RoiTriageTests(unittest.TestCase):
    def test_roi_triage_ranks_recommendations(self) -> None:
        payload = build_roi_triage(write_outputs=False)
        recommendations = payload.get("recommendations") or []

        self.assertGreaterEqual(len(recommendations), 3)
        self.assertEqual(recommendations[0]["rank"], 1)
        scores = [item["score_breakdown"]["roi_score"] for item in recommendations]
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertTrue(any(item.get("owner_skill") == "duck-reliability-review" for item in recommendations))

    def test_roi_triage_markdown_is_operator_readable(self) -> None:
        payload = {
            "generated_at": "2026-04-26T08:00:00-04:00",
            "summary": {
                "candidate_count": 1,
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
        }

        markdown = render_roi_triage_markdown(payload)

        self.assertIn("# Duck ROI Triage", markdown)
        self.assertIn("Semantic visual QA", markdown)
        self.assertIn("duck-reliability-review", markdown)


if __name__ == "__main__":
    unittest.main()
