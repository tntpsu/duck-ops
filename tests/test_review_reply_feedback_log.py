"""Tests for the operator-agreement feedback log written by record_action."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_loop


def _decision(artifact_id: str, *, customer_review: str, draft_reply: str) -> dict:
    return {
        "artifact_type": "review_reply",
        "flow": "reviews_reply_positive",
        "decision": "needs_revision",
        "preview": {
            "context_text": customer_review,
            "proposed_text": draft_reply,
        },
    }


class FeedbackLogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.feedback_path = Path(self.tmp.name) / "review_reply_feedback.jsonl"
        self.operator_state_path = Path(self.tmp.name) / "operator_state.json"
        self.overrides_path = Path(self.tmp.name) / "overrides.jsonl"
        self._patches = [
            patch.object(review_loop, "REVIEW_REPLY_FEEDBACK_PATH", self.feedback_path),
            patch.object(review_loop, "OPERATOR_STATE_PATH", self.operator_state_path),
            patch.object(review_loop, "OVERRIDES_PATH", self.overrides_path),
        ]
        for p in self._patches:
            p.start()
        self.addCleanup(self._stop_patches)
        self.addCleanup(self.tmp.cleanup)
        # Seed an artifact registry compatible with record_action.
        self.artifact_id = "publish::reviews_reply_positive::2026-05-23::review-X"
        self.state_bundle = {
            "quality_gate": {
                "artifacts": {
                    self.artifact_id: {
                        "decision": _decision(
                            self.artifact_id,
                            customer_review="Cute duck. Arrived fast and made my kid laugh.",
                            draft_reply="Thank you for your kind words!",
                        ),
                    },
                },
            },
        }

    def _stop_patches(self) -> None:
        for p in self._patches:
            p.stop()

    def _write_operator_state(self, payload: dict) -> None:
        self.operator_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.operator_state_path.write_text(json.dumps(payload), encoding="utf-8")

    def _read_feedback(self) -> list[dict]:
        if not self.feedback_path.exists():
            return []
        return [json.loads(line) for line in self.feedback_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def test_approve_writes_feedback_entry(self) -> None:
        review_loop.record_action(self.state_bundle, self.artifact_id, "approve", note=None)
        entries = self._read_feedback()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["artifact_id"], self.artifact_id)
        self.assertEqual(entry["operator_action"], "approve")
        self.assertEqual(entry["customer_review"], "Cute duck. Arrived fast and made my kid laugh.")
        self.assertEqual(entry["approved_reply_text"], "Thank you for your kind words!")
        self.assertIsNone(entry["rewrite_source"])  # no rewrite was cached
        self.assertIsNone(entry["used_rewrite"])

    def test_approve_with_cached_llm_rewrite_captures_source(self) -> None:
        self._write_operator_state({
            "rewrite_suggestions": {
                self.artifact_id: {
                    "text": "Thank you for your kind words!",  # operator approved original (matches draft) — used_rewrite=False
                    "source": "llm",
                    "model": "gpt-4o-mini",
                    "hint": "warmer",
                },
            },
        })
        review_loop.record_action(self.state_bundle, self.artifact_id, "approve", note="original was good")
        entries = self._read_feedback()
        entry = entries[0]
        self.assertEqual(entry["rewrite_source"], "llm")
        self.assertEqual(entry["rewrite_model"], "gpt-4o-mini")
        self.assertEqual(entry["rewrite_hint"], "warmer")
        # In this test the rewrite text and approved text are identical →
        # used_rewrite is True (operator did accept the rewrite, even though
        # it happened to match the draft text).
        self.assertTrue(entry["used_rewrite"])

    def test_approve_with_rewrite_text_different_records_used_rewrite_false(self) -> None:
        self._write_operator_state({
            "rewrite_suggestions": {
                self.artifact_id: {
                    "text": "So glad the duck made your kid laugh — thanks for the kind review.",
                    "source": "llm",
                    "model": "gpt-4o-mini",
                },
            },
        })
        # Operator approves the ORIGINAL DRAFT text instead of the rewrite by
        # passing approved_reply_text explicitly that matches the draft.
        review_loop.record_action(
            self.state_bundle, self.artifact_id, "approve",
            note=None,
            approved_reply_text="Thank you for your kind words!",
        )
        entries = self._read_feedback()
        entry = entries[0]
        self.assertEqual(entry["rewrite_source"], "llm")
        self.assertFalse(entry["used_rewrite"])

    def test_reject_writes_feedback_entry(self) -> None:
        review_loop.record_action(self.state_bundle, self.artifact_id, "reject", note="off-tone")
        entries = self._read_feedback()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["operator_action"], "discard")
        self.assertEqual(entry["note"], "off-tone")

    def test_override_writes_feedback_entry(self) -> None:
        review_loop.record_action(
            self.state_bundle, self.artifact_id, "override",
            note="needs to mention the kid laughing",
            resolution="needs_changes",
        )
        entries = self._read_feedback()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["operator_action"], "needs_changes")
        self.assertEqual(entry["note"], "needs to mention the kid laughing")


if __name__ == "__main__":
    unittest.main()
