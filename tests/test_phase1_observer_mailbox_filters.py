from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from phase1_observer import looks_like_customer_issue_email


class Phase1ObserverMailboxFilterTests(unittest.TestCase):
    def test_operator_started_vendor_support_reply_is_not_customer_issue(self) -> None:
        email_item = {
            "subject": "AW: 3D AI Studio API issue: Tripo multiview returns 503 while P1 image-to-3d works",
            "from": "Julia from 3D AI Studio <support@3daistudio.com>",
            "body_text": (
                "Hi Phil,\nthank you for the detailed report.\n\n"
                "From: tullaipsu@gmail.com <tullaipsu@gmail.com>\n"
                "Sent: Monday, April 6, 2026 5:18 AM\n"
                "Subject: 3D AI Studio API issue: Tripo multiview returns 503 while P1 image-to-3d works\n"
                "Can you confirm whether there is a current issue with the Tripo multiview endpoints?"
            ),
        }

        self.assertFalse(looks_like_customer_issue_email(email_item))

    def test_etsy_conversation_still_counts_as_customer_issue(self) -> None:
        email_item = {
            "subject": "Etsy conversation with Amy about Order #4027700359",
            "from": "Etsy Conversations <transaction@etsy.com>",
            "body_text": "Amy sent you a message about her order and needs help with the color option.",
        }

        self.assertTrue(looks_like_customer_issue_email(email_item))


if __name__ == "__main__":
    unittest.main()
