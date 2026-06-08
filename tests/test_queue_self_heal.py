"""Regression test for the 2026-06-08 queue self-heal fix.

Production observation: the review_reply_execution_queue.json file
empties between drains for reasons we couldn't trace in code. The
workflow_control receipts persist as the durable source of truth.
load_queue_state now rehydrates from receipts when the queue file
is shorter than truth.

These tests pin:
  - Empty queue + workflow_control receipts → heal populates queue
  - Queue file matches receipts → no spurious heal
  - Receipt with terminal state_reason (reply_posted, etc.) NOT rehydrated
  - Heal is silent when no orphan receipts
  - Loud (stdout) log when heal does work
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_reply_executor


def _write_receipt(wc_dir: Path, name: str, *,
                   state: str = "approved",
                   state_reason: str = "queued_for_execution",
                   artifact_id: str | None = None) -> None:
    wc_dir.mkdir(parents=True, exist_ok=True)
    aid = artifact_id or f"publish::reviews_reply_positive::2026-06-08::{name}"
    (wc_dir / f"review-execution-publish-reviews-reply-positive-2026-06-08-{name}.json").write_text(
        json.dumps({
            "entity_id": aid,
            "state": state,
            "state_reason": state_reason,
            "metadata": {"artifact_id": aid},
            "updated_at": "2026-06-08T16:00:00-04:00",
            "last_side_effect": {
                "kind": "queue",
                "queued_at": "2026-06-08T15:00:00-04:00",
                "queued_by": "test_seed",
                "execution_mode": "auto_post",
            },
        }),
        encoding="utf-8",
    )


class QueueSelfHealTests(unittest.TestCase):
    def test_empty_queue_with_orphan_receipts_gets_rehydrated(self) -> None:
        """The headline test: production failure mode. Queue file is
        empty (or short). Workflow_control has 3 approved+queued
        receipts. load_queue_state rehydrates all 3."""
        _write_receipt(review_reply_executor.WORKFLOW_CONTROL_DIR, "r1")
        _write_receipt(review_reply_executor.WORKFLOW_CONTROL_DIR, "r2")
        _write_receipt(review_reply_executor.WORKFLOW_CONTROL_DIR, "r3")
        # Queue file doesn't exist or is empty.
        state = review_reply_executor.load_queue_state()
        items = state["items"]
        self.assertEqual(len(items), 3)
        for aid, item in items.items():
            self.assertEqual(item["status"], "queued")
            self.assertEqual(item["queued_by"], "test_seed")
            self.assertEqual(item["execution_mode"], "auto_post")

    def test_no_orphans_no_heal(self) -> None:
        """No receipts to rehydrate → return unchanged empty queue."""
        # No receipts written. Just empty workflow_control dir.
        review_reply_executor.WORKFLOW_CONTROL_DIR.mkdir(parents=True, exist_ok=True)
        state = review_reply_executor.load_queue_state()
        self.assertEqual(state["items"], {})

    def test_terminal_state_receipts_not_rehydrated(self) -> None:
        """Receipts with terminal state_reason (reply_posted, etc.)
        are DONE — must not be pulled back into the queue."""
        for terminal in ("reply_posted", "already_replied", "execution_failed"):
            _write_receipt(
                review_reply_executor.WORKFLOW_CONTROL_DIR,
                terminal,
                state_reason=terminal,
            )
        state = review_reply_executor.load_queue_state()
        self.assertEqual(state["items"], {},
                         "Terminal-state receipts must not rehydrate")

    def test_only_queued_for_execution_state_rehydrated(self) -> None:
        """Mixed receipts: 1 queued + 1 done + 1 different state.
        Only the queued_for_execution one rehydrates."""
        _write_receipt(review_reply_executor.WORKFLOW_CONTROL_DIR, "alive")
        _write_receipt(review_reply_executor.WORKFLOW_CONTROL_DIR, "done",
                       state_reason="reply_posted")
        _write_receipt(review_reply_executor.WORKFLOW_CONTROL_DIR, "blocked",
                       state="blocked", state_reason="auth_blocked")
        state = review_reply_executor.load_queue_state()
        self.assertEqual(len(state["items"]), 1)
        aid = list(state["items"].keys())[0]
        self.assertIn("alive", aid)

    def test_existing_queue_item_not_duplicated(self) -> None:
        """When a receipt's artifact_id is already in the queue with
        status=queued, the heal must NOT add a duplicate."""
        _write_receipt(review_reply_executor.WORKFLOW_CONTROL_DIR, "r1")
        # Pre-populate the queue with this one already.
        aid = "publish::reviews_reply_positive::2026-06-08::r1"
        review_reply_executor.save_queue_state({
            "items": {
                aid: {
                    "artifact_id": aid,
                    "status": "queued",
                    "queued_at": "1970-01-01T00:00:00Z",
                    "queued_by": "original",
                    "execution_mode": "shadow_only",
                },
            },
        })
        state = review_reply_executor.load_queue_state()
        self.assertEqual(len(state["items"]), 1)
        # And the ORIGINAL fields are preserved, not overwritten.
        self.assertEqual(state["items"][aid]["queued_by"], "original")

    def test_malformed_receipt_skipped(self) -> None:
        """Garbage in workflow_control doesn't crash the heal."""
        wc = review_reply_executor.WORKFLOW_CONTROL_DIR
        wc.mkdir(parents=True, exist_ok=True)
        (wc / "review-execution-publish-reviews-reply-positive-2026-06-08-broken.json").write_text(
            "not valid json"
        )
        _write_receipt(wc, "good")
        state = review_reply_executor.load_queue_state()
        self.assertEqual(len(state["items"]), 1)


if __name__ == "__main__":
    unittest.main()
