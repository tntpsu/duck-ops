from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_loop


class ReviewLoopHealthTests(unittest.TestCase):
    def test_render_system_health_summary_lists_flows(self) -> None:
        payload = {
            "generated_at": "2026-04-12T16:15:00-04:00",
            "overall_status": "warn",
            "flow_health": [
                {
                    "flow_id": "meme",
                    "label": "Meme Monday",
                    "status": "bad",
                    "last_run_state": "execution_failed",
                    "success_rate_label": "75% (3/4)",
                },
                {
                    "flow_id": "weekly",
                    "label": "Weekly Campaign",
                    "status": "ok",
                    "last_run_state": "sale_rotation_published",
                    "success_rate_label": "100% (4/4)",
                },
            ],
        }
        with patch.object(review_loop, "load_json", return_value=payload):
            text = review_loop.render_system_health_summary()

        self.assertIn("System health: warn", text)
        self.assertIn("Meme Monday: bad | execution failed | 75% (3/4)", text)
        self.assertIn("Weekly Campaign: ok | sale rotation published | 100% (4/4)", text)

    def test_handle_operator_text_supports_health_command(self) -> None:
        state_bundle = {"quality_gate": {"artifacts": {}}, "trend_ranker": {"artifacts": {}}}
        operator_state = {}
        payload = {
            "generated_at": "2026-04-12T16:15:00-04:00",
            "overall_status": "warn",
            "flow_health": [
                {
                    "flow_id": "meme",
                    "label": "Meme Monday",
                    "status": "bad",
                    "last_run_state": "execution_failed",
                    "success_rate_label": "75% (3/4)",
                }
            ],
        }
        with (
            patch.object(review_loop, "reconcile_state_bundle"),
            patch.object(review_loop, "build_review_items", return_value=[]),
            patch.object(review_loop, "assign_short_ids"),
            patch.object(review_loop, "sync_current_item", return_value=None),
            patch.object(review_loop, "surfaced_review_items", return_value=[]),
            patch.object(review_loop, "write_review_queue"),
            patch.object(review_loop, "load_json", return_value=payload),
        ):
            text = review_loop.handle_operator_text(state_bundle, operator_state, "health")

        self.assertIn("System health: warn", text)
        self.assertIn("Meme Monday: bad", text)


if __name__ == "__main__":
    unittest.main()
