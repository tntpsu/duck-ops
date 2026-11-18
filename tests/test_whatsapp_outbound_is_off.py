"""The WhatsApp outbound push is OFF, and must not come back by accident.

Context, because a bare `"enabled": false` invites someone to flip it:
the operator stopped using WhatsApp; the inbound bridge last processed a
message on 2026-06-22. Outbound was still pushing review notifications on
every notifier run, and that `docker exec` into the openclaw gateway is
what blocked for eight hours against an unresponsive Docker daemon on
2026-11-17 -- holding the sidecar lock and taking seven other steps down
with it, including the operator inbox poller that reads approvals.

The 60-second subprocess timeout added the same day stops it wedging
again. This test is about the second half: a notifier run should not be
spending 60 seconds per cycle on a channel nobody reads.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import notifier  # noqa: E402


class WhatsAppOutboundOffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = json.loads(
            (REPO_ROOT / "config" / "notifier.json").read_text(encoding="utf-8"))
        self.whatsapp = self.config.get("whatsapp") or {}

    def test_the_shipped_config_has_it_disabled(self) -> None:
        self.assertFalse(
            self.whatsapp.get("enabled"),
            "WhatsApp outbound is off deliberately — see disabled_because in "
            "config/notifier.json before turning it back on")

    def test_the_reason_travels_with_the_flag(self) -> None:
        """A flag without its reason gets flipped back by the next reader."""
        self.assertTrue(str(self.whatsapp.get("disabled_because") or "").strip())
        self.assertTrue(str(self.whatsapp.get("disabled_on") or "").strip())

    def test_no_summary_is_built_so_the_send_is_unreachable(self) -> None:
        """`enabled` gates the BUILD, which is what keeps docker exec away.

        Asserting the behaviour rather than the flag: the send block is
        guarded separately, and a future refactor that reads only one of the
        two is exactly how this would come back.
        """
        self.assertIsNone(
            notifier.build_operator_whatsapp_summary(
                {"whatsapp": self.whatsapp}, {"any": "state"}))

    def test_a_push_would_still_be_built_if_re_enabled(self) -> None:
        """Proves the test above measures the flag, not a dead code path.

        Without this, deleting the push builder entirely would also make
        the suite green ([[feedback_pin_the_rule_not_the_reading]]).
        """
        enabled = dict(self.whatsapp, enabled=True)
        with mock.patch.object(notifier, "build_reviews_whatsapp_operator_push",
                               return_value={"run_id": "r", "message": "m"}):
            self.assertIsNotNone(
                notifier.build_operator_whatsapp_summary(
                    {"whatsapp": enabled}, {"any": "state"}))


if __name__ == "__main__":
    unittest.main()
