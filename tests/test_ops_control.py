from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import ops_control


class OpsControlTests(unittest.TestCase):
    def test_sync_ops_control_marks_backlog_present(self) -> None:
        calls: list[dict] = []
        generated_at = datetime.now().astimezone().isoformat()
        customer_queue = {
            "generated_at": generated_at,
            "counts": {"operator_queue_items": 3},
            "items": [{}, {}, {}],
        }
        review_queue = {
            "generated_at": generated_at,
            "pending_count": 1,
            "items": [{}],
        }
        with patch.object(ops_control, "record_workflow_transition", side_effect=lambda **kwargs: calls.append(kwargs) or kwargs):
            ops_control.sync_ops_control(customer_queue, review_queue)
        self.assertTrue(calls)
        self.assertEqual(calls[0]["workflow_id"], "ops")
        self.assertEqual(calls[0]["state"], "observed")
        self.assertEqual(calls[0]["state_reason"], "backlog_present")

    def test_sync_ops_control_marks_desk_ready(self) -> None:
        calls: list[dict] = []
        generated_at = datetime.now().astimezone().isoformat()
        customer_queue = {
            "generated_at": generated_at,
            "counts": {"operator_queue_items": 0},
            "items": [],
        }
        review_queue = {
            "generated_at": generated_at,
            "pending_count": 0,
            "items": [],
        }
        with patch.object(ops_control, "record_workflow_transition", side_effect=lambda **kwargs: calls.append(kwargs) or kwargs):
            ops_control.sync_ops_control(customer_queue, review_queue)
        self.assertTrue(calls)
        self.assertEqual(calls[0]["workflow_id"], "ops")
        self.assertEqual(calls[0]["state"], "verified")
        self.assertEqual(calls[0]["state_reason"], "desk_ready")


if __name__ == "__main__":
    unittest.main()
