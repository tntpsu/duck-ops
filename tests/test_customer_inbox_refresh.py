from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import customer_inbox_refresh


class CustomerInboxRefreshTests(unittest.TestCase):
    def test_select_refresh_candidates_prioritizes_top_actions_and_missing_direct_links(self) -> None:
        browser_sync = {
            "items": [
                {
                    "linked_customer_short_id": "C309",
                    "conversation_contact": "grannya2006",
                    "primary_browser_url": "https://ablink.account.etsy.com/redirect",
                    "browser_review_status": "captured",
                    "follow_up_state": "ready_for_task",
                },
                {
                    "linked_customer_short_id": "C387",
                    "conversation_contact": "Amy",
                    "primary_browser_url": "https://www.etsy.com/messages/1661266079",
                    "browser_review_status": "captured",
                    "follow_up_state": "waiting_on_customer",
                },
                {
                    "linked_customer_short_id": "C303",
                    "conversation_contact": "amz21671",
                    "primary_browser_url": "https://ablink.account.etsy.com/redirect",
                    "browser_review_status": "captured",
                    "follow_up_state": "waiting_on_operator",
                },
            ]
        }
        nightly = {"top_customer_actions": [{"short_id": "C309"}]}
        selected = customer_inbox_refresh.select_refresh_candidates(browser_sync, nightly, limit=2, include_waiting=False)
        self.assertEqual([item["linked_customer_short_id"] for item in selected], ["C309", "C303"])

    def test_run_refresh_skips_quiet_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            refresh_path = Path(tmpdir) / "customer_inbox_refresh.json"
            with (
                patch.object(customer_inbox_refresh, "REFRESH_STATE_PATH", refresh_path),
                patch.object(customer_inbox_refresh, "_local_now", return_value=datetime.fromisoformat("2026-04-13T02:15:00-04:00")),
                patch.object(customer_inbox_refresh, "record_workflow_transition") as control_mock,
            ):
                result = customer_inbox_refresh.run_refresh(
                    limit=5,
                    include_waiting=False,
                    skip_outside_hours=True,
                    start_hour=7,
                    start_minute=30,
                    end_hour=23,
                    end_minute=59,
                )
            self.assertTrue(refresh_path.exists())
            stored = json.loads(refresh_path.read_text())

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "quiet_hours")
        self.assertEqual(stored["reason"], "quiet_hours")
        control_mock.assert_called_once()

    def test_refresh_packet_does_not_persist_unverified_direct_url(self) -> None:
        packet = {
            "short_id": "C397",
            "title": "Customer",
            "latest_message_preview": "Latest Etsy conversation needs review.",
            "customer_summary": "Latest Etsy conversation needs review.",
        }
        with (
            patch.object(
                customer_inbox_refresh,
                "_open_in_trusted_etsy_session",
                return_value={
                    "current_url": "https://www.etsy.com/messages/1660743861",
                    "target_url": "https://www.etsy.com/messages/1660743861",
                    "target_resolution_strategy": "inbox_search",
                    "target_verification_required": True,
                    "target_resolution_details": {
                        "ok": True,
                        "strategy": "inbox_search",
                        "landed_url": "https://www.etsy.com/messages/1660743861",
                        "verification_required": True,
                    },
                    "thread_verification": {
                        "verification_required": True,
                        "contactMatch": False,
                        "summaryMatches": [],
                    },
                },
            ),
            patch.object(customer_inbox_refresh, "_persist_resolved_thread_url") as persist_mock,
            patch.object(customer_inbox_refresh, "_persist_refresh_metadata") as metadata_mock,
            patch.object(customer_inbox_refresh, "_maybe_mark_waiting_on_customer", return_value=False),
        ):
            refreshed = customer_inbox_refresh.refresh_packet(packet)

        persist_mock.assert_not_called()
        metadata_mock.assert_called_once()
        refresh_row = metadata_mock.call_args.args[1]
        self.assertNotIn("thread_url", refresh_row)
        self.assertIsNone(refreshed["persisted_direct_url"])
        self.assertTrue(refreshed["verification_required"])


if __name__ == "__main__":
    unittest.main()
