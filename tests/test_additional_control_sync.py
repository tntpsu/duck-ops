from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import notifier
import quality_gate_pilot
import trend_ranker


class AdditionalControlSyncTests(unittest.TestCase):
    def test_quality_gate_sync_marks_alerts_pending(self) -> None:
        calls: list[dict] = []
        state = {
            "last_digest_date": "2026-04-12",
            "artifacts": {
                "artifact-1": {
                    "evaluated_at": "2026-04-12T10:00:00-04:00",
                    "decision": {"review_status": "approved"},
                }
            },
            "alerts": {"artifact-1::hash": {"created_at": "2026-04-12T10:05:00-04:00"}},
        }
        with patch.object(quality_gate_pilot, "record_workflow_transition", side_effect=lambda **kwargs: calls.append(kwargs) or kwargs):
            quality_gate_pilot.sync_quality_gate_control(state)
        self.assertTrue(calls)
        self.assertEqual(calls[0]["workflow_id"], "quality_gate")
        self.assertEqual(calls[0]["state"], "observed")
        self.assertEqual(calls[0]["state_reason"], "alerts_pending")

    def test_trend_ranker_sync_marks_pending_review(self) -> None:
        calls: list[dict] = []
        state = {
            "last_digest_date": "2026-04-12",
            "artifacts": {"artifact-1": {}},
            "concepts": {
                "concept-1": {
                    "created_at": "2026-04-12T09:00:00-04:00",
                    "review_status": "pending",
                    "operator_surface": True,
                    "decision": "watch",
                    "action_frame": "build",
                    "new_in_run": True,
                }
            },
        }
        with patch.object(trend_ranker, "record_workflow_transition", side_effect=lambda **kwargs: calls.append(kwargs) or kwargs):
            trend_ranker.sync_trend_ranker_control(state)
        self.assertTrue(calls)
        self.assertEqual(calls[0]["workflow_id"], "trend_ranker")
        self.assertEqual(calls[0]["state"], "observed")
        self.assertEqual(calls[0]["state_reason"], "pending_review")

    def test_trend_ranker_sync_treats_non_actionable_pending_as_backlog(self) -> None:
        calls: list[dict] = []
        state = {
            "last_digest_date": "2026-04-12",
            "artifacts": {"artifact-1": {}},
            "concepts": {
                "concept-1": {
                    "created_at": "2026-04-12T09:00:00-04:00",
                    "review_status": "pending",
                    "operator_surface": False,
                    "decision": "watch",
                    "action_frame": "ignore",
                    "new_in_run": False,
                }
            },
        }
        with patch.object(trend_ranker, "record_workflow_transition", side_effect=lambda **kwargs: calls.append(kwargs) or kwargs):
            trend_ranker.sync_trend_ranker_control(state)
        self.assertTrue(calls)
        self.assertEqual(calls[0]["state"], "verified")
        self.assertEqual(calls[0]["state_reason"], "backlog_outside_operator_queue")
        self.assertEqual(calls[0]["metadata"]["actionable_pending_review_count"], 0)
        self.assertEqual(calls[0]["metadata"]["backlog_pending_review_count"], 1)

    def test_notifier_sync_marks_pending_delivery(self) -> None:
        calls: list[dict] = []
        state = {"sent": {}, "last_digest_sent_at": None, "last_trend_digest_sent_at": None, "last_reviews_whatsapp_sent_at": None}
        pending = [{"kind": "digest"}, {"kind": "urgent"}]
        with patch.object(notifier, "record_workflow_transition", side_effect=lambda **kwargs: calls.append(kwargs) or kwargs):
            notifier.sync_notifier_control(state, pending_artifacts=pending, whatsapp_summary=None)
        self.assertTrue(calls)
        self.assertEqual(calls[0]["workflow_id"], "notifier")
        self.assertEqual(calls[0]["state"], "observed")
        self.assertEqual(calls[0]["state_reason"], "pending_delivery")


if __name__ == "__main__":
    unittest.main()
