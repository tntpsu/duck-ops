"""Belt-and-suspenders pollution audit for the reply-channel state file.

`state/operator_reply_health.json` is graded by the drop OS card, so a
fixture reading in it is not inert. The two directions it can lie in are
not symmetrical: a fake "no reply has ever been processed" turns the card
red on nothing and wastes an afternoon; a fake "processed 0h ago" turns a
real outage GREEN, which is how the 2026-11-17 approval sat unread in
Trash for nine hours with every component reporting healthy.

Three layers, per the house rule:
  1. `_isolate_operator_reply_health` in tests/conftest.py redirects both
     STATE_PATH and RECEIPT_PATH to tmp for every test, autouse.
  2. `write_state` raises TestModeRefusalError when DUCK_TEST_MODE=1 and
     the resolved target is the frozen production path.
  3. THIS test, which audits the real file at suite end.

Single-repo by construction, and that is why there is no duckAgent-side
fixture: `operator_reply_health` lives in duck-ops/runtime, which is not
on duckAgent's sys.path, and the OS card consumes the FILE rather than
importing the module. If that ever changes -- if a duckAgent module
imports this one -- the second fixture becomes required, because
duckAgent's conftest does not set DUCK_TEST_MODE and layer 2 would not
fire there.

Do NOT delete this test if it starts failing. Find the writer.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import operator_reply_health  # noqa: E402

PRODUCTION_STATE = REPO_ROOT / "state" / "operator_reply_health.json"


class NoPollutionTests(unittest.TestCase):
    def test_production_state_points_at_the_real_receipt_log(self) -> None:
        """A tmp receipt_path in the live file means a test wrote it."""
        if not PRODUCTION_STATE.exists():
            self.skipTest("producer has not run yet")
        payload = json.loads(PRODUCTION_STATE.read_text(encoding="utf-8"))
        receipt_path = str(payload.get("receipt_path") or "")
        self.assertTrue(receipt_path, "reading has no receipt_path — who wrote it?")
        self.assertFalse(
            any(frag in receipt_path for frag in ("/tmp/", "pytest-", "/var/folders/")),
            f"test pollution: production reading was derived from {receipt_path}")
        self.assertTrue(
            receipt_path.endswith("state/operator_inbox_receipts.jsonl"),
            f"unexpected receipt source in production state: {receipt_path}")

    def test_the_frozen_path_guard_is_armed(self) -> None:
        """Layer 2, asserted rather than assumed."""
        self.assertEqual(
            operator_reply_health._FROZEN_PRODUCTION_STATE_PATH,
            PRODUCTION_STATE.resolve())

    def test_writing_the_frozen_path_under_test_mode_raises(self) -> None:
        import os
        from unittest import mock
        with mock.patch.dict(os.environ, {"DUCK_TEST_MODE": "1"}), \
             mock.patch.object(operator_reply_health, "STATE_PATH",
                               PRODUCTION_STATE):
            with self.assertRaises(operator_reply_health.TestModeRefusalError):
                operator_reply_health.write_state({"polled_ever": False})

    def test_the_autouse_fixture_redirected_this_very_test(self) -> None:
        """Proves layer 1 is live in this suite, not just written down."""
        self.assertNotEqual(
            Path(operator_reply_health.STATE_PATH).resolve(),
            PRODUCTION_STATE.resolve(),
            "conftest fixture did not redirect STATE_PATH — layer 1 is off")


if __name__ == "__main__":
    unittest.main()
