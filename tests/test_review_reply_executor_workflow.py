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
    def test_cleanup_review_reply_browsers_uses_shared_janitor_for_force_cleanup(self) -> None:
        with (
            patch.object(review_reply_executor, "_known_review_reply_browser_sessions", return_value=["esd"]),
            patch.object(review_reply_executor, "session_is_open", return_value=True),
            patch.object(review_reply_executor, "run_pw_command"),
            patch.object(
                review_reply_executor,
                "cleanup_stale_playwright_processes",
                return_value={"killed_group_count": 1},
            ) as cleanup_mock,
        ):
            result = review_reply_executor.cleanup_review_reply_browsers(force_kill_temp_profiles=True)

        cleanup_mock.assert_called_once_with(
            stale_after_seconds=0,
            force=True,
            reason="review_reply_browser_cleanup",
            respect_keepalive=False,
        )
        self.assertIn("force-cleaned 1 lingering Playwright browser group", result["message"])
        self.assertEqual(result["closed_count"], 1)

    def test_annotate_attempt_failure_captures_row_not_found_breadcrumbs(self) -> None:
        attempt = {
            "session": {
                "current_url": "https://www.etsy.com/your/shops/me/reviews?page=1",
                "auth_probe": {"signInVisible": False, "sellerControlsVisible": True},
            },
            "navigation": {
                "strategy": "review_page_probe_search",
                "landed_url": "https://www.etsy.com/your/shops/me/reviews?page=1",
            },
            "initial_match": {"found": False, "candidateCount": 0},
            "review_page_probes": [
                {"url": "https://www.etsy.com/your/shops/me/reviews?page=2", "found": False},
                {"url": "https://www.etsy.com/your/shops/me/reviews?page=3", "found": False},
            ],
            "surface_refresh": {
                "landed_url": "https://www.etsy.com/your/shops/me/reviews?page=1",
                "found": False,
            },
        }
        with patch.object(
            review_reply_executor,
            "etsy_browser_blocked_status",
            return_value={
                "blocked": True,
                "block_reason": "unusual activity",
                "blocked_until": "2026-04-15T07:44:21-04:00",
            },
        ):
            failure = review_reply_executor.annotate_attempt_failure(
                attempt,
                "Exact review row could not be found in the signed-in Etsy session.",
            )

        self.assertEqual(failure["failure_class"], "review_row_not_found")
        self.assertEqual(failure["phase"], "preflight")
        self.assertTrue(failure["retryable"])
        self.assertTrue(failure["browser_guard_active"])
        self.assertIn("probe pages", failure["breadcrumb_summary"])
        self.assertTrue(attempt["breadcrumbs"]["surface_refresh_attempted"])

    def test_record_attempt_persists_failure_metadata_to_queue_item(self) -> None:
        quality_state = {
            "artifacts": {
                "artifact-1": {
                    "decision": {
                        "artifact_id": "artifact-1",
                        "run_id": "run-1",
                        "execution_attempts": [],
                    }
                }
            }
        }
        queue_state = {"items": {"artifact-1": {}}}
        attempt = {
            "attempt_id": "dry-run-1",
            "started_at": "2026-04-15T01:00:00-04:00",
            "finished_at": "2026-04-15T01:01:00-04:00",
            "outcome": "failed",
            "failure": {
                "failure_class": "review_row_not_found",
                "phase": "preflight",
                "breadcrumb_summary": "initial match not found | probe pages p2:miss, p3:miss",
            },
        }
        with (
            patch.object(review_reply_executor, "write_decision", return_value={}),
            patch.object(review_reply_executor, "write_attempt_artifact", return_value={"json_path": "/tmp/a.json"}),
            patch.object(review_reply_executor, "save_quality_gate_state"),
            patch.object(review_reply_executor, "save_queue_state"),
        ):
            queue_item = review_reply_executor.record_attempt(
                quality_state,
                queue_state,
                "artifact-1",
                attempt,
                final_queue_status="queued",
                final_execution_state="queued",
                last_preflight_status="waiting_for_review_row",
            )

        self.assertEqual(queue_item["last_failure_class"], "review_row_not_found")
        self.assertEqual(queue_item["last_failure_phase"], "preflight")
        self.assertIn("probe pages", queue_item["last_breadcrumb_summary"])

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

    def test_prepare_auth_for_drain_treats_browser_cooldown_as_nonfatal_pause(self) -> None:
        with (
            patch.object(review_reply_executor, "load_auth_state", return_value={"auth_status": "healthy"}),
            patch.object(review_reply_executor, "choose_session", return_value=("esd", "https://www.etsy.com/your/shops/me/reviews")),
            patch.object(
                review_reply_executor,
                "ensure_authenticated_session",
                side_effect=RuntimeError("Etsy automation is cooling down until 2026-04-20T09:15:00-04:00 because: rate_limit_preemptive_cooldown"),
            ),
            patch.object(
                review_reply_executor,
                "etsy_browser_blocked_status",
                return_value={
                    "blocked": True,
                    "blocked_until": "2026-04-20T09:15:00-04:00",
                    "block_reason": "rate_limit_preemptive_cooldown",
                },
            ),
        ):
            result = review_reply_executor.prepare_auth_for_drain({"auto_execution_enabled": True})

        self.assertTrue(result["ok"])
        self.assertFalse(result["ready"])
        self.assertEqual(result["status"], "cooldown")
        self.assertEqual(result["blocked_until"], "2026-04-20T09:15:00-04:00")
        self.assertEqual(result["block_reason"], "rate_limit_preemptive_cooldown")

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
            stack.enter_context(patch.object(review_reply_executor, "run_pw_command"))
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
