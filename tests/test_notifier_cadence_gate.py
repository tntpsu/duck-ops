"""Contract tests for notifier ↔ email_cadence_gate wiring.

The cadence gate itself is covered by test_email_cadence_gate.py.
This file pins only the notifier-side wiring: that the
``learning_change_digest`` artifact is gated, the bypass payload is
shaped correctly (re-nesting attention_change_count under
change_notifier), and unmapped artifact kinds pass through with no
gate applied.
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import notifier  # noqa: E402
import email_cadence_gate  # noqa: E402


def _at(date_iso: str) -> datetime:
    return datetime.fromisoformat(date_iso + "T12:00:00").replace(tzinfo=timezone.utc)


class CadenceGateArtifactRoutingTests(unittest.TestCase):
    def test_unmapped_kind_returns_none(self) -> None:
        artifact = {
            "kind": "digest",  # not in _CADENCE_SURFACE_BY_ARTIFACT_KIND
            "payload": {},
            "json_path": Path("/tmp/x.json"),
        }
        self.assertIsNone(notifier.cadence_gate_decision_for_artifact(artifact))

    def test_learning_change_digest_routes_to_learnings_surface(self) -> None:
        artifact = {
            "kind": "learning_change_digest",
            "payload": {
                "attention_change_count": 0,
                "material_change_count": 1,
            },
            "json_path": Path("/tmp/learning.json"),
        }
        with mock.patch.object(
            notifier, "should_send_email", wraps=email_cadence_gate.should_send_email
        ) as should_send, mock.patch.object(notifier, "log_cadence_decision"):
            # Force non-Monday so the deferred path fires.
            with mock.patch(
                "email_cadence_gate.datetime"
            ) as dt_mock:
                dt_mock.now.return_value = _at("2026-05-26")  # Tuesday
                dt_mock.fromisoformat = datetime.fromisoformat
                decision = notifier.cadence_gate_decision_for_artifact(artifact)
        self.assertIsNotNone(decision)
        self.assertEqual(should_send.call_args.args[0], "learnings")
        passed_payload = should_send.call_args.args[1]
        # The notifier MUST re-nest the digest's flat
        # attention_change_count into change_notifier.attention_change_count
        # so the policy's dotted-path bypass key resolves correctly.
        self.assertEqual(passed_payload, {"change_notifier": {"attention_change_count": 0}})

    def test_learning_change_attention_count_triggers_bypass(self) -> None:
        artifact = {
            "kind": "learning_change_digest",
            "payload": {"attention_change_count": 2},
            "json_path": Path("/tmp/learning.json"),
        }
        with mock.patch.object(notifier, "log_cadence_decision"):
            decision = notifier.cadence_gate_decision_for_artifact(artifact)
        self.assertIsNotNone(decision)
        # Regardless of weekday (Monday OR Tuesday), should_send must
        # be True because the bypass key is truthy.
        self.assertTrue(decision.should_send)

    def test_zero_attention_count_on_non_monday_defers(self) -> None:
        artifact = {
            "kind": "learning_change_digest",
            "payload": {"attention_change_count": 0},
            "json_path": Path("/tmp/learning.json"),
        }
        with mock.patch.object(notifier, "log_cadence_decision"):
            with mock.patch("email_cadence_gate.datetime") as dt_mock:
                dt_mock.now.return_value = _at("2026-05-26")  # Tuesday
                dt_mock.fromisoformat = datetime.fromisoformat
                decision = notifier.cadence_gate_decision_for_artifact(artifact)
        self.assertFalse(decision.should_send)
        self.assertEqual(decision.cadence, "weekly_monday")


if __name__ == "__main__":
    unittest.main()
