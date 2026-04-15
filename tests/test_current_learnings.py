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
            state_path = root / "state" / "current_learnings.json"
            operator_json_path = root / "output" / "operator" / "current_learnings.json"
            markdown_path = root / "output" / "operator" / "current_learnings.md"

            with patch.object(current_learnings, "SOCIAL_ROLLUPS_PATH", social_path), patch.object(
                current_learnings, "COMPETITOR_BENCHMARK_PATH", competitor_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_STATE_PATH", state_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                current_learnings, "CURRENT_LEARNINGS_MD_PATH", markdown_path
            ):
                payload = current_learnings.build_current_learnings()

            self.assertEqual(len(payload["current_beliefs"]), 2)
            self.assertEqual(len(payload["changes_since_previous"]), 2)
            self.assertTrue(payload["ideas_to_test"])
            self.assertTrue(state_path.exists())
            self.assertTrue(operator_json_path.exists())
            self.assertTrue(markdown_path.exists())


if __name__ == "__main__":
    unittest.main()
