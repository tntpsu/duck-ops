from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from customer_action_packets import build_customer_action_packets


class CustomerActionPacketsTests(unittest.TestCase):
    def test_reply_packet_is_suppressed_when_thread_is_waiting_on_customer(self) -> None:
        cases = [
            {
                "item_type": "customer_case",
                "source_artifact_id": "artifact-1",
                "priority": "medium",
                "recommended_action": "reply_with_context",
                "summary": "Customer asked for an update.",
                "details": {
                    "channel": "mailbox_email",
                    "issue_type": "conversation",
                    "response_recommendation": {"label": "reply", "reason": "Reply with context"},
                    "recovery_recommendation": {},
                    "order_enrichment": {"product_title": "Duck", "matched": True},
                    "tracking_enrichment": {},
                    "resolution_enrichment": {},
                    "operator_decision": {},
                    "conversation_contact": "Amy",
                    "conversation_thread_key": "thread-amy",
                    "browser_url_candidates": [],
                    "grouped_message_count": 1,
                    "latest_message_preview": "Any update?",
                },
                "source_refs": [],
            }
        ]

        packets = build_customer_action_packets(
            cases,
            browser_captures={
                "items": [
                    {
                        "conversation_thread_key": "thread-amy",
                        "follow_up_state": "waiting_on_customer",
                        "browser_review_status": "captured",
                    }
                ]
            },
        )

        self.assertEqual(packets, [])


if __name__ == "__main__":
    unittest.main()
