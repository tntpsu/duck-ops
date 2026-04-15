from __future__ import annotations

import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_reply_executor


class ReviewReplyExecutorWorkflowTests(unittest.TestCase):
    def test_queue_review_reply_records_workflow_control_state(self) -> None:
        quality_state = {
            "artifacts": {
                "artifact-1": {
                    "decision": {
                        "artifact_id": "artifact-1",
                        "flow": "reviews_reply_positive",
                        "artifact_type": "review_reply",
                        "decision": "publish_ready",
                        "execution_state": "not_queued",
                        "execution_mode": "manual",
                        "approved_reply_text": "Thanks so much!",
                        "review_target": {"transaction_id": "tx-1", "listing_id": "listing-1"},
                    }
                }
            }
        }
        queue_state = {"generated_at": None, "items": {}}
        decision = quality_state["artifacts"]["artifact-1"]["decision"]
        with (
            patch.object(review_reply_executor, "load_quality_gate_state", return_value=quality_state),
            patch.object(review_reply_executor, "load_queue_state", return_value=queue_state),
            patch.object(review_reply_executor, "load_discovery_approvals", return_value={"approvals": {}}),
            patch.object(review_reply_executor, "artifact_record", return_value=quality_state["artifacts"]["artifact-1"]),
            patch.object(review_reply_executor, "latest_discovery_packet_for_artifact", return_value={"artifact_id": "artifact-1"}),
            patch.object(review_reply_executor, "validate_record_for_queue", return_value=(decision, {"scope": "artifact", "packet_generated_at": "2026-04-12T17:00:00-04:00", "packet_path": "/tmp/pkt.json"})),
            patch.object(review_reply_executor, "write_decision", return_value={}),
            patch.object(review_reply_executor, "save_quality_gate_state"),
            patch.object(review_reply_executor, "save_queue_state"),
            patch.object(review_reply_executor, "record_workflow_transition") as control_mock,
        ):
            result = review_reply_executor.queue_review_reply("artifact-1")

        self.assertEqual(result["status"], "queued")
        control_mock.assert_called_once()
        kwargs = control_mock.call_args.kwargs
        self.assertEqual(kwargs["workflow_id"], "review_execution::artifact-1")
        self.assertEqual(kwargs["state"], "approved")
        self.assertEqual(kwargs["state_reason"], "queued_for_execution")

    def test_handle_auth_blocked_attempt_records_blocked_control_state(self) -> None:
        quality_state = {"artifacts": {"artifact-1": {"decision": {}}}}
        queue_state = {"items": {"artifact-1": {}}}
        session_state = {"sessions": {}}
        decision = {
            "artifact_id": "artifact-1",
            "flow": "reviews_reply_positive",
            "review_target": {"transaction_id": "tx-1", "listing_id": "listing-1"},
        }
        attempt = {
            "attempt_id": "dry-run-1",
            "session_name": "esd",
            "error": "Etsy seller session is showing a public signed-out view.",
        }
        with (
            patch.object(review_reply_executor, "load_auth_state", return_value={"storage_state": {}}),
            patch.object(review_reply_executor, "mark_auth_blocked", return_value={"auth_status": "blocked", "blocked_at": "2026-04-12T17:00:00-04:00", "next_retry_after": "2026-04-12T17:30:00-04:00"}),
            patch.object(review_reply_executor, "maybe_send_auth_alert", return_value=None),
            patch.object(review_reply_executor, "save_auth_state"),
            patch.object(review_reply_executor, "record_attempt", return_value={"status": "queued"}),
            patch.object(review_reply_executor, "record_session_event", return_value={"items": {"artifact-1": {}}}),
            patch.object(review_reply_executor, "save_session_state"),
            patch.object(review_reply_executor, "record_workflow_transition") as control_mock,
        ):
            review_reply_executor.handle_auth_blocked_attempt(
                quality_state=quality_state,
                queue_state=queue_state,
                session_state=session_state,
                artifact_id="artifact-1",
                decision=decision,
                attempt=attempt,
                policy={"auth_block_retry_delay_seconds": 1800},
            )

        control_mock.assert_called_once()
        kwargs = control_mock.call_args.kwargs
        self.assertEqual(kwargs["workflow_id"], "review_execution::artifact-1")
        self.assertEqual(kwargs["state"], "blocked")
        self.assertEqual(kwargs["state_reason"], "auth_blocked")

    def test_run_live_submit_records_submit_confirmed_before_verified(self) -> None:
        quality_state = {
            "artifacts": {
                "artifact-1": {
                    "decision": {
                        "artifact_id": "artifact-1",
                        "flow": "reviews_reply_positive",
                        "artifact_type": "review_reply",
                        "decision": "publish_ready",
                        "execution_state": "queued",
                        "execution_mode": "manual",
                        "approved_reply_text": "Thanks so much!",
                        "review_target": {"transaction_id": "tx-1", "listing_id": "listing-1"},
                    }
                }
            }
        }
        queue_state = {
            "generated_at": None,
            "items": {"artifact-1": {"status": "queued", "last_preflight_status": "dry_run_filled"}},
        }
        session_state = {"sessions": {}}
        decision = quality_state["artifacts"]["artifact-1"]["decision"]

        with ExitStack() as stack:
            stack.enter_context(patch.object(review_reply_executor, "load_execution_policy", return_value={}))
            stack.enter_context(patch.object(review_reply_executor, "load_quality_gate_state", return_value=quality_state))
            stack.enter_context(patch.object(review_reply_executor, "load_queue_state", return_value=queue_state))
            stack.enter_context(patch.object(review_reply_executor, "load_session_state", return_value=session_state))
            stack.enter_context(patch.object(review_reply_executor, "load_discovery_approvals", return_value={"approvals": {}}))
            stack.enter_context(patch.object(review_reply_executor, "artifact_record", return_value=quality_state["artifacts"]["artifact-1"]))
            stack.enter_context(patch.object(review_reply_executor, "latest_discovery_packet_for_artifact", return_value={"artifact_id": "artifact-1"}))
            stack.enter_context(patch.object(review_reply_executor, "validate_record_for_queue", return_value=(decision, {})))
            stack.enter_context(patch.object(review_reply_executor, "choose_session", return_value=("esd", "https://www.etsy.com/shop/myJeepDuck?ref=dashboard-header#reviews")))
            stack.enter_context(patch.object(review_reply_executor, "ensure_authenticated_session", return_value={"reused_existing_session": True}))
            stack.enter_context(patch.object(review_reply_executor, "load_auth_state", return_value={"storage_state": {}}))
            stack.enter_context(patch.object(review_reply_executor, "mark_auth_healthy", return_value={"auth_status": "healthy"}))
            stack.enter_context(patch.object(review_reply_executor, "save_auth_state"))
            stack.enter_context(patch.object(review_reply_executor, "prepare_review_row_for_execution", return_value=("tx-1", {})))
            stack.enter_context(patch.object(review_reply_executor, "fill_reply_text_without_submit", return_value={"ok": True}))
            stack.enter_context(
                patch.object(
                    review_reply_executor,
                    "inspect_reply_row_state",
                    side_effect=[
                        {
                            "ok": True,
                            "textareaVisible": True,
                            "valueMatches": True,
                            "submitVisible": True,
                            "submitDisabled": False,
                        },
                        {
                            "ok": True,
                            "textareaVisible": False,
                            "submitVisible": False,
                            "rowTextContainsReplySnippet": True,
                        },
                    ],
                )
            )
            stack.enter_context(patch.object(review_reply_executor, "submit_reply_after_verification", return_value={"ok": True}))
            stack.enter_context(patch.object(review_reply_executor, "capture_target_review_screenshot", return_value="/tmp/review.png"))
            stack.enter_context(patch.object(review_reply_executor, "record_attempt", return_value={"status": "posted", "last_preflight_status": "submitted"}))
            stack.enter_context(patch.object(review_reply_executor, "record_session_event", return_value={"session_id": "sess-1", "items": {}}))
            stack.enter_context(patch.object(review_reply_executor, "save_session_state"))
            stack.enter_context(patch.object(review_reply_executor, "save_quality_gate_state"))
            stack.enter_context(patch.object(review_reply_executor, "save_queue_state"))
            time_mock = stack.enter_context(patch.object(review_reply_executor, "time"))
            control_mock = stack.enter_context(patch.object(review_reply_executor, "record_workflow_transition"))
            time_mock.sleep.return_value = None
            result = review_reply_executor.run_live_submit("artifact-1")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "posted")
        states = [call.kwargs["state_reason"] for call in control_mock.call_args_list]
        self.assertIn("submit_confirmed", states)
        self.assertIn("reply_posted", states)


if __name__ == "__main__":
    unittest.main()
