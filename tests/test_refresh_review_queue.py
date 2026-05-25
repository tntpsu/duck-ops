"""Tests for the periodic review_queue refresher.

The runner is thin (delegates to review_loop.write_review_queue), so
these tests focus on the receipt-writing contract and the
never-raise guarantee that lets launchd keep firing the job even
when something upstream breaks.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import refresh_review_queue as runner


class RefreshContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.receipt_path = Path(self._tmp.name) / "receipts.jsonl"

    def test_success_receipt_carries_pending_counts(self) -> None:
        """write_review_queue returns paths, not the queue payload, so
        the runner re-reads review_queue.json after writing. Mock the
        re-read by patching the payload-loader directly."""
        sample_payload = {
            "generated_at": "2026-05-25T20:00:00-04:00",
            "pending_count": 3,
            "pending_count_all": 5,
            "items": [],
            "surfaced_items": [],
        }
        with patch.object(runner.review_loop, "load_state_bundle", return_value={}):
            with patch.object(runner.review_loop, "load_operator_state", return_value={}):
                with patch.object(runner.review_loop, "write_review_queue", return_value={}):
                    with patch.object(runner, "_load_review_queue_payload", return_value=sample_payload):
                        receipt = runner.refresh(receipt_path=self.receipt_path)
        self.assertEqual(receipt["outcome"], "ok")
        self.assertEqual(receipt["pending_count"], 3)
        self.assertEqual(receipt["pending_count_all"], 5)
        self.assertEqual(receipt["generated_at"], "2026-05-25T20:00:00-04:00")
        self.assertIn("at", receipt)
        lines = self.receipt_path.read_text(encoding="utf-8").strip().split("\n")
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["outcome"], "ok")

    def test_exception_in_load_state_does_not_raise(self) -> None:
        """launchd's ThrottleInterval back-off behavior would amplify
        a real outage if the runner kept exiting non-zero. The
        contract here is: never raise, never exit non-zero. Record
        the error in the receipt log instead."""
        with patch.object(runner.review_loop, "load_state_bundle", side_effect=RuntimeError("disk full")):
            receipt = runner.refresh(receipt_path=self.receipt_path)
        self.assertTrue(receipt["outcome"].startswith("error:"))
        self.assertEqual(receipt["outcome"], "error:RuntimeError")
        self.assertIn("disk full", receipt["error"])

    def test_exception_in_write_review_queue_does_not_raise(self) -> None:
        with patch.object(runner.review_loop, "load_state_bundle", return_value={}):
            with patch.object(runner.review_loop, "load_operator_state", return_value={}):
                with patch.object(runner.review_loop, "write_review_queue", side_effect=OSError("read-only fs")):
                    receipt = runner.refresh(receipt_path=self.receipt_path)
        self.assertEqual(receipt["outcome"], "error:OSError")
        self.assertIn("read-only fs", receipt["error"])

    def test_receipts_appended_across_runs(self) -> None:
        """The receipt log is append-only — each refresh adds a row,
        nothing rewrites the file. Confirms grep over time works."""
        with patch.object(runner.review_loop, "load_state_bundle", return_value={}):
            with patch.object(runner.review_loop, "load_operator_state", return_value={}):
                with patch.object(runner.review_loop, "write_review_queue", return_value={}):
                    with patch.object(runner, "_load_review_queue_payload", return_value={"pending_count": 0, "pending_count_all": 0}):
                        runner.refresh(receipt_path=self.receipt_path)
                        runner.refresh(receipt_path=self.receipt_path)
                        runner.refresh(receipt_path=self.receipt_path)
        lines = self.receipt_path.read_text(encoding="utf-8").strip().split("\n")
        self.assertEqual(len(lines), 3)

    def test_main_returns_zero_even_on_error(self) -> None:
        """main() exits 0 unconditionally so launchd doesn't back off.
        The receipt log carries the real status."""
        with patch.object(runner.review_loop, "load_state_bundle", side_effect=RuntimeError("oops")):
            with patch.object(runner, "RECEIPT_PATH", self.receipt_path):
                rc = runner.main()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
