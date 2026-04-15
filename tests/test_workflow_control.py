from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import workflow_control


class WorkflowControlTests(unittest.TestCase):
    def test_record_transition_writes_state_and_receipt(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            state_dir = tmp_root / "workflow_control"
            receipt_root = tmp_root / "workflow_receipts"
            with patch.object(workflow_control, "now_iso", return_value="2026-04-12T17:00:00-04:00"):
                state = workflow_control.record_workflow_transition(
                    workflow_id="customer_reply::C392",
                    lane="customer_reply",
                    display_label="Customer Reply C392",
                    entity_id="C392",
                    state="proposed",
                    state_reason="reply_preview_staged",
                    requires_confirmation=True,
                    next_action="Confirm before send.",
                    metadata={"conversation_contact": "R Henderson"},
                    receipt_kind="preview",
                    receipt_payload={"reply_excerpt": "Yes, I can do that."},
                    state_dir=state_dir,
                    receipt_root=receipt_root,
                )

            state_path = workflow_control.workflow_state_path("customer_reply::C392", state_dir=state_dir)
            self.assertTrue(state_path.exists())
            self.assertEqual(state["state"], "proposed")
            self.assertEqual(state["state_reason"], "reply_preview_staged")
            self.assertTrue(state["requires_confirmation"])
            self.assertEqual(state["metadata"]["conversation_contact"], "R Henderson")
            self.assertEqual(state["history"][0]["summary"], "reply preview staged")
            latest_receipt = state.get("latest_receipt") or {}
            self.assertTrue(Path(str(latest_receipt.get("path"))).exists())

    def test_list_workflow_states_reads_saved_states(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            state_dir = tmp_root / "workflow_control"
            workflow_control.record_workflow_transition(
                workflow_id="review_execution::artifact-1",
                lane="review_execution",
                display_label="Review Execution artifact-1",
                entity_id="artifact-1",
                state="verified",
                state_reason="reply_posted",
                state_dir=state_dir,
            )
            states = workflow_control.list_workflow_states(state_dir=state_dir)
            self.assertEqual(len(states), 1)
            self.assertEqual(states[0]["workflow_id"], "review_execution::artifact-1")


if __name__ == "__main__":
    unittest.main()
