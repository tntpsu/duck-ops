from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from etsy_conversation_browser_sync import build_etsy_conversation_browser_sync


class EtsyConversationBrowserSyncTests(unittest.TestCase):
    def test_draft_reply_promotes_thread_to_reply_drafted(self) -> None:
        payload = build_etsy_conversation_browser_sync(
            [
                {
                    "item_type": "customer_case",
                    "source_artifact_id": "artifact-1",
                    "summary": "Customer asked about color.",
                    "details": {
                        "channel": "mailbox_email",
                        "conversation_thread_key": "thread-1",
                        "conversation_contact": "Ben",
                        "browser_url_candidates": ["https://www.etsy.com/your/messages/1"],
                        "order_enrichment": {"receipt_id": "R1", "product_title": "Test Duck"},
                    },
                }
            ],
            browser_captures={
                "items": [
                    {
                        "conversation_thread_key": "thread-1",
                        "draft_reply": "Yes, I can make it blue.",
                    }
                ]
            },
        )

        item = payload["items"][0]
        self.assertEqual(item["browser_review_status"], "captured")
        self.assertEqual(item["follow_up_state"], "reply_drafted")
        self.assertEqual(payload["counts"]["threads_with_reply_draft"], 1)
        self.assertEqual(payload["counts"]["active_followups"], 1)

    def test_operator_short_id_mapping_links_threads_back_to_customer_packets(self) -> None:
        with patch(
            "etsy_conversation_browser_sync._load_customer_operator_short_ids",
            return_value={"packet-1": "C301"},
        ):
            payload = build_etsy_conversation_browser_sync(
                [
                    {
                        "item_type": "customer_case",
                        "source_artifact_id": "artifact-1",
                        "summary": "Customer asked about color.",
                        "details": {
                            "channel": "mailbox_email",
                            "conversation_thread_key": "thread-1",
                            "conversation_contact": "Ben",
                            "browser_url_candidates": ["https://www.etsy.com/your/messages/1"],
                            "order_enrichment": {"receipt_id": "R1", "product_title": "Test Duck"},
                        },
                    }
                ],
                customer_packets={
                    "items": [
                        {
                            "packet_id": "packet-1",
                            "source_artifact_id": "artifact-1",
                            "conversation_thread_key": "thread-1",
                            "status": "reply_needed",
                        }
                    ]
                },
            )

        item = payload["items"][0]
        self.assertEqual(item["linked_customer_short_id"], "C301")
        self.assertEqual(item["open_command"], "customer open C301")

    def test_reply_needed_false_prevents_draft_from_showing_as_reply_drafted(self) -> None:
        payload = build_etsy_conversation_browser_sync(
            [
                {
                    "item_type": "customer_case",
                    "source_artifact_id": "artifact-1",
                    "summary": "Customer asked about a UGA duck.",
                    "details": {
                        "channel": "mailbox_email",
                        "conversation_thread_key": "thread-1",
                        "conversation_contact": "Lisa",
                        "browser_url_candidates": ["https://www.etsy.com/your/messages/1"],
                        "order_enrichment": {"receipt_id": "R1", "product_title": "Custom Duck"},
                    },
                }
            ],
            browser_captures={
                "items": [
                    {
                        "conversation_thread_key": "thread-1",
                        "draft_reply": "Yes, I can make a UGA one.",
                        "reply_needed": False,
                        "open_loop_owner": "customer",
                        "last_customer_message": "Can you make a UGA one?",
                        "last_seller_message": "Yes",
                    }
                ]
            },
        )

        item = payload["items"][0]
        self.assertEqual(item["follow_up_state"], "waiting_on_customer")
        self.assertEqual(payload["counts"]["threads_with_reply_draft"], 0)


if __name__ == "__main__":
    unittest.main()
