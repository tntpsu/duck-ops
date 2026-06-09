"""Pin the TEST_MODE source-level write-refusal in workflow_control.

Source-level guard: record_workflow_transition raises
TestModeRefusalError when DUCK_TEST_MODE=1 AND the resolved write
path is the production workflow_control dir. Path-patched tests
sail through; tests that forgot to patch get caught loud.

This is layer 2 of 3:
  - Layer 1: conftest.py autouse stubs (won't reach this function)
  - Layer 2: this guard (catches autouse bypasses)
  - Layer 3: post-suite audit test (catches anything past layers 1+2)
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))


import workflow_control as wc


class TestModeGuardTests(unittest.TestCase):
    """The guard fires on prod writes only.

    Note: duck-ops/tests/conftest.py monkeypatches
    WORKFLOW_CONTROL_STATE_DIR to a tmp dir for every test. These
    tests TEMPORARILY UNDO that patch to simulate the bug we're
    guarding against — a test that forgot to set up isolation."""

    def test_test_mode_set_and_no_state_dir_raises(self) -> None:
        """DUCK_TEST_MODE=1 + state_dir=None + no path isolation
        → raises. The exact failure mode that produced
        meme-test-run.json on 2026-06-06."""
        with patch.dict(os.environ, {"DUCK_TEST_MODE": "1"}), \
             patch.object(wc, "WORKFLOW_CONTROL_STATE_DIR",
                          wc._FROZEN_PRODUCTION_WORKFLOW_CONTROL_PATH):
            with self.assertRaises(wc.TestModeRefusalError):
                wc.record_workflow_transition(
                    workflow_id="meme::TEST-RUN",
                    lane="meme",
                    display_label="Meme TEST-RUN",
                    state="proposed",
                    state_reason="awaiting_review",
                )

    def test_test_mode_set_with_tmp_state_dir_does_not_raise(self) -> None:
        """Path-patched tests are legitimate — don't break the duck-ops
        conftest pattern of `monkeypatch.setattr(_wc,
        'WORKFLOW_CONTROL_STATE_DIR', tmp_wc)`."""
        with patch.dict(os.environ, {"DUCK_TEST_MODE": "1"}), TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result = wc.record_workflow_transition(
                workflow_id="meme::test-allowed",
                lane="meme",
                display_label="Allowed test write",
                state="proposed",
                state_reason="awaiting_review",
                state_dir=tmp_path,
                receipt_root=tmp_path / "receipts",
            )
            self.assertIsInstance(result, dict)
            # And the write actually landed in tmp.
            self.assertTrue(any(tmp_path.glob("*.json")))

    def test_test_mode_unset_does_not_raise_even_for_prod_path(self) -> None:
        """When NOT in test mode (production), the guard is silent —
        real production code paths work as before."""
        # Clear DUCK_TEST_MODE temporarily.
        with patch.dict(os.environ, {}, clear=False), TemporaryDirectory() as tmp:
            os.environ.pop("DUCK_TEST_MODE", None)
            # Use a tmp dir so we don't actually touch prod from this test.
            tmp_path = Path(tmp)
            result = wc.record_workflow_transition(
                workflow_id="meme::real-prod-call",
                lane="meme",
                display_label="Real prod write",
                state="proposed",
                state_reason="awaiting_review",
                state_dir=tmp_path,
                receipt_root=tmp_path / "receipts",
            )
            self.assertIsInstance(result, dict)

    def test_test_mode_various_truthy_values_trigger_guard(self) -> None:
        for truthy in ("1", "true", "TRUE", "yes"):
            with self.subTest(value=truthy):
                # Also undo conftest's path patch so the resolved
                # state_dir is actually the frozen prod path.
                with patch.dict(os.environ, {"DUCK_TEST_MODE": truthy}), \
                     patch.object(wc, "WORKFLOW_CONTROL_STATE_DIR",
                                  wc._FROZEN_PRODUCTION_WORKFLOW_CONTROL_PATH):
                    with self.assertRaises(wc.TestModeRefusalError):
                        wc.record_workflow_transition(
                            workflow_id="test::probe",
                            lane="test",
                            display_label="probe",
                            state="proposed",
                            state_reason="x",
                        )

    def test_test_mode_falsy_values_do_not_trigger(self) -> None:
        """'0', empty, anything not in the truthy set is treated as off."""
        for falsy in ("0", "", "no", "False"):
            with self.subTest(value=falsy), TemporaryDirectory() as tmp:
                with patch.dict(os.environ, {"DUCK_TEST_MODE": falsy}):
                    # Doesn't raise — write goes through to the tmp dir.
                    result = wc.record_workflow_transition(
                        workflow_id="test::probe",
                        lane="test",
                        display_label="probe",
                        state="proposed",
                        state_reason="x",
                        state_dir=Path(tmp),
                        receipt_root=Path(tmp) / "receipts",
                    )
                    self.assertIsInstance(result, dict)

    def test_error_message_names_the_workflow_id_and_remediation(self) -> None:
        """Error must surface the workflow_id (so triage knows which
        test) AND the remediation (so the next operator knows what
        to do)."""
        with patch.dict(os.environ, {"DUCK_TEST_MODE": "1"}), \
             patch.object(wc, "WORKFLOW_CONTROL_STATE_DIR",
                          wc._FROZEN_PRODUCTION_WORKFLOW_CONTROL_PATH):
            try:
                wc.record_workflow_transition(
                    workflow_id="meme::TEST-RUN",
                    lane="meme",
                    display_label="m",
                    state="proposed",
                    state_reason="x",
                )
                self.fail("Should have raised")
            except wc.TestModeRefusalError as e:
                msg = str(e)
                self.assertIn("meme::TEST-RUN", msg)
                self.assertIn("conftest.py", msg)


if __name__ == "__main__":
    unittest.main()
