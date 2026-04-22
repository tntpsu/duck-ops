from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_loop


class ReviewLoopReconciliationTests(unittest.TestCase):
    def test_reconcile_quality_gate_archives_superseded_newduck_runs(self) -> None:
        state = {
            "artifacts": {
                "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck": {
                    "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                    "decision": {
                        "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                        "artifact_type": "listing",
                        "flow": "newduck",
                        "run_id": "orange-cat-duck-2026-04-21-2116",
                        "review_status": "pending",
                        "created_at": "2026-04-21T21:48:29-04:00",
                        "title": "Orange Cat Duck",
                    },
                }
            }
        }

        workflow_states = [
            {
                "workflow_id": "newduck::orange-cat-duck-2026-04-21-2116",
                "lane": "newduck",
                "run_id": "orange-cat-duck-2026-04-21-2116",
                "state": "proposed",
                "updated_at": "2026-04-21T21:48:29-04:00",
                "_path": "/tmp/newduck-orange-cat-duck-2026-04-21-2116.json",
            },
            {
                "workflow_id": "newduck::orange-cat-duck-2026-04-21-2308",
                "lane": "newduck",
                "run_id": "orange-cat-duck-2026-04-21-2308",
                "state": "verified",
                "state_reason": "shopify_activated",
                "updated_at": "2026-04-21T23:49:35-04:00",
                "_path": "/tmp/newduck-orange-cat-duck-2026-04-21-2308.json",
            },
        ]

        with (
            patch.object(review_loop, "latest_override_index", return_value={}),
            patch.object(review_loop, "duckagent_publish_reconciliation", return_value=None),
            patch.object(review_loop, "list_workflow_states", return_value=workflow_states),
            patch.object(review_loop, "now_iso", return_value="2026-04-22T00:10:00-04:00"),
        ):
            changed = review_loop.reconcile_quality_gate_state(state)

        self.assertTrue(changed)
        decision = state["artifacts"]["publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck"]["decision"]
        self.assertEqual(decision["review_status"], "archived")
        self.assertEqual(decision["archive_reason"], "superseded by newer newduck run")
        self.assertEqual(decision["superseded_by_run_id"], "orange-cat-duck-2026-04-21-2308")
        self.assertEqual(decision["human_review"]["resolution"], "superseded")
        self.assertIn("newer newduck run `orange-cat-duck-2026-04-21-2308`", decision["human_review"]["note"])

    def test_reconcile_quality_gate_leaves_unrelated_newduck_runs_pending(self) -> None:
        state = {
            "artifacts": {
                "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck": {
                    "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                    "decision": {
                        "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                        "artifact_type": "listing",
                        "flow": "newduck",
                        "run_id": "orange-cat-duck-2026-04-21-2116",
                        "review_status": "pending",
                        "created_at": "2026-04-21T21:48:29-04:00",
                        "title": "Orange Cat Duck",
                    },
                }
            }
        }

        workflow_states = [
            {
                "workflow_id": "newduck::monster-truck-duck-2026-04-21-2308",
                "lane": "newduck",
                "run_id": "monster-truck-duck-2026-04-21-2308",
                "state": "verified",
                "state_reason": "shopify_activated",
                "updated_at": "2026-04-21T23:49:35-04:00",
                "_path": "/tmp/newduck-monster-truck-duck-2026-04-21-2308.json",
            }
        ]

        with (
            patch.object(review_loop, "latest_override_index", return_value={}),
            patch.object(review_loop, "duckagent_publish_reconciliation", return_value=None),
            patch.object(review_loop, "list_workflow_states", return_value=workflow_states),
        ):
            changed = review_loop.reconcile_quality_gate_state(state)

        self.assertFalse(changed)
        decision = state["artifacts"]["publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck"]["decision"]
        self.assertEqual(decision["review_status"], "pending")


if __name__ == "__main__":
    unittest.main()
