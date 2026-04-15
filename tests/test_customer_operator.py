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

import customer_operator
from customer_operator import (
    _best_browser_url,
    _browser_url_requires_manual_verification,
    _canonical_direct_etsy_thread_url,
    _contact_hint_from_source_refs,
    _persist_resolved_thread_url,
    _record_browser_capture,
    _parse_capture_note,
    _quick_capture_note,
    _resolve_trusted_etsy_session,
    _select_inbox_search_candidate,
    backfill_exact_thread_urls,
    handle_customer_text,
    parse_customer_command,
)


class CustomerOperatorTests(unittest.TestCase):
    def test_parse_customer_drafted_command(self) -> None:
        command, target, note = parse_customer_command("customer drafted C301 Thanks, I can do blue.")
        self.assertEqual(command, "state::reply_drafted")
        self.assertEqual(target, "C301")
        self.assertEqual(note, "Thanks, I can do blue.")

    def test_quick_capture_note_for_waiting(self) -> None:
        note = _quick_capture_note("waiting_on_customer", "Waiting for the customer to confirm the size.")
        parsed = _parse_capture_note({}, note)
        self.assertEqual(parsed["follow_up_state"], "waiting_on_customer")
        self.assertIn("Wait for the customer", parsed["recommended_action"])

    def test_quick_capture_note_for_drafted_reply(self) -> None:
        note = _quick_capture_note("reply_drafted", "Yes, I can make it blue.")
        parsed = _parse_capture_note({}, note)
        self.assertEqual(parsed["follow_up_state"], "needs_reply")
        self.assertEqual(parsed["draft_reply"], "Yes, I can make it blue.")

    def test_capture_note_can_explicitly_suppress_reply_when_seller_already_answered(self) -> None:
        parsed = _parse_capture_note(
            {},
            (
                "summary: Seller already answered the customer.; "
                "customer_latest: Can you make a UGA one?; "
                "seller_latest: Yes; "
                "reply_needed: no; "
                "open_loop: customer"
            ),
        )
        self.assertEqual(parsed["follow_up_state"], "waiting_on_customer")
        self.assertEqual(parsed["last_customer_message"], "Can you make a UGA one?")
        self.assertEqual(parsed["last_seller_message"], "Yes")
        self.assertFalse(parsed["reply_needed"])

    def test_parse_customer_threads_commands(self) -> None:
        command, target, note = parse_customer_command("customer threads")
        self.assertEqual(command, "threads")
        self.assertIsNone(target)
        self.assertEqual(note, "new")

        command, target, note = parse_customer_command("customer followups")
        self.assertEqual(command, "threads")
        self.assertIsNone(target)
        self.assertEqual(note, "followups")

        command, target, note = parse_customer_command("customer preview C301")
        self.assertEqual(command, "preview")
        self.assertEqual(target, "C301")
        self.assertEqual(note, "")

        command, target, note = parse_customer_command("customer confirm C301")
        self.assertEqual(command, "confirm")
        self.assertEqual(target, "C301")
        self.assertEqual(note, "")

        command, target, note = parse_customer_command("customer sent C301")
        self.assertEqual(command, "verify")
        self.assertEqual(target, "C301")
        self.assertEqual(note, "")

    def test_best_browser_url_prefers_direct_etsy_thread_and_avoids_ablink(self) -> None:
        packet = {
            "browser_url_candidates": [
                "https://ablink.account.etsy.com/redirect",
                "https://www.etsy.com/your/account/messages/123",
                "https://www.etsy.com/messages",
            ]
        }
        self.assertEqual(_best_browser_url(packet), "https://www.etsy.com/messages/123")

    def test_canonical_direct_etsy_thread_url_normalizes_legacy_paths(self) -> None:
        self.assertEqual(
            _canonical_direct_etsy_thread_url("https://www.etsy.com/your/account/messages/123"),
            "https://www.etsy.com/messages/123",
        )
        self.assertEqual(
            _canonical_direct_etsy_thread_url("https://www.etsy.com/messages/456"),
            "https://www.etsy.com/messages/456",
        )
        self.assertIsNone(_canonical_direct_etsy_thread_url("https://www.etsy.com/messages?ref=hdr_user_menu-messages"))

    def test_generic_messages_urls_still_require_manual_verification(self) -> None:
        self.assertTrue(_browser_url_requires_manual_verification("https://www.etsy.com/messages"))
        self.assertTrue(_browser_url_requires_manual_verification("https://www.etsy.com/your/account/messages"))
        self.assertTrue(_browser_url_requires_manual_verification("https://www.etsy.com/messages?ref=hdr_user_menu-messages"))
        self.assertFalse(_browser_url_requires_manual_verification("https://www.etsy.com/messages/1660743861"))

    def test_select_inbox_search_candidate_prefers_contact_match(self) -> None:
        chosen = _select_inbox_search_candidate(
            [
                {"href": "https://www.etsy.com/messages/1", "text": "Kelly Lefever"},
                {"href": "https://www.etsy.com/messages/2", "text": "R Henderson soccer ducks custom order"},
            ],
            expected_contact="R Henderson",
            expected_summary="Buyer wants 13 personalized soccer ducks with player names and jersey numbers.",
        )
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen["href"], "https://www.etsy.com/messages/2")

    def test_select_inbox_search_candidate_ignores_generic_from_token_in_contact_hint(self) -> None:
        chosen = _select_inbox_search_candidate(
            [
                {
                    "href": "https://www.etsy.com/messages/1",
                    "text": "Select this conversation with Kelly Lefever from 30 minutes ago read message Kelly Lefever sorry! i replied to the wrong thread!",
                },
                {
                    "href": "https://www.etsy.com/messages/2",
                    "text": "Select this conversation with R Henderson from 33 minutes ago read message R Henderson Yes, I have everything I need for your order.",
                },
            ],
            expected_contact="Kelly Lefever from SillyMillieandCo",
            expected_summary="Buyer placed the order and wants reassurance that we have everything needed before production and shipping.",
        )
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen["href"], "https://www.etsy.com/messages/1")

    def test_contact_hint_from_source_refs_strips_shop_suffix(self) -> None:
        packet = {
            "source_refs": [
                {"subject": "Re: Etsy Conversation with Kelly Lefever from SillyMillieandCo"},
            ]
        }
        self.assertEqual(_contact_hint_from_source_refs(packet), "Kelly Lefever")

    def test_contact_hint_from_source_refs_supports_needs_help_subject(self) -> None:
        packet = {
            "source_refs": [
                {"subject": "MEREDITH needs help with an order they placed"},
            ]
        }
        self.assertEqual(_contact_hint_from_source_refs(packet), "MEREDITH")

    def test_persist_resolved_thread_url_creates_capture_when_missing(self) -> None:
        packet = {
            "short_id": "C397",
            "conversation_thread_key": "etsy-thread::meredith-needs-help-with-an-order-they-placed",
            "source_artifact_id": "customer_case::mail::81344",
            "conversation_contact": "MEREDITH",
            "title": "Customer",
            "latest_message_preview": "Latest Etsy conversation needs review.",
            "customer_summary": "Latest Etsy conversation needs review.",
            "order_enrichment": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            captures_path = Path(tmpdir) / "captures.json"
            captures_path.write_text(json.dumps({"generated_at": "2026-04-13T12:00:00-04:00", "items": []}), encoding="utf-8")
            workflow_path = Path(tmpdir) / "workflow.json"
            with (
                patch.object(customer_operator, "ETSY_BROWSER_CAPTURES_PATH", captures_path),
                patch.object(customer_operator, "workflow_state_path", return_value=workflow_path),
            ):
                _persist_resolved_thread_url(packet, "https://www.etsy.com/messages/1660979059")

            stored = json.loads(captures_path.read_text(encoding="utf-8"))

        self.assertEqual(len(stored["items"]), 1)
        self.assertEqual(stored["items"][0]["packet_short_id"], "C397")
        self.assertEqual(stored["items"][0]["thread_url"], "https://www.etsy.com/messages/1660979059")

    def test_resolve_trusted_etsy_session_prefers_healthy_auth_state(self) -> None:
        with (
            patch("review_reply_executor.choose_session", return_value=("esd", "https://www.etsy.com/your/shops/me/dashboard")),
            patch.object(
                customer_operator,
                "load_json",
                side_effect=[
                    {
                        "auth_status": "healthy",
                        "last_session_name": "esd",
                        "last_checked_url": "https://www.etsy.com/shop/myJeepDuck?ref=dashboard-header#reviews",
                    },
                    {"sessions": {"esd": {"url": "https://www.etsy.com/your/shops/me/dashboard"}}},
                ],
            ),
        ):
            session_name, start_url = _resolve_trusted_etsy_session()

        self.assertEqual(session_name, "esd")
        self.assertEqual(start_url, "https://www.etsy.com/shop/myJeepDuck?ref=dashboard-header#reviews")

    def test_resolve_trusted_etsy_session_ignores_bad_messages_url(self) -> None:
        with (
            patch("review_reply_executor.choose_session", return_value=("esd", "https://www.etsy.com/your/shops/me/dashboard")),
            patch.object(
                customer_operator,
                "load_json",
                side_effect=[
                    {
                        "auth_status": "healthy",
                        "last_session_name": "esd",
                        "last_checked_url": "https://www.etsy.com/your/account/messages",
                    },
                    {"sessions": {"esd": {"url": "https://www.etsy.com/your/shops/me/dashboard"}}},
                ],
            ),
        ):
            session_name, start_url = _resolve_trusted_etsy_session()

        self.assertEqual(session_name, "esd")
        self.assertEqual(start_url, "https://www.etsy.com/your/shops/me/dashboard")

    def test_handle_customer_open_uses_trusted_etsy_session(self) -> None:
        packet_payload = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [
                {
                    "packet_id": "pkt-1",
                    "title": "Amber ME",
                    "browser_url_candidates": ["https://www.etsy.com/your/account/messages/123"],
                    "order_enrichment": {"receipt_id": "3346"},
                }
            ],
        }
        operator_state = {
            "next_short_id": 301,
            "packet_short_ids": {},
            "current_packet_id": None,
        }
        with (
            patch.object(customer_operator, "load_packets", return_value=packet_payload),
            patch.object(customer_operator, "load_operator_state", return_value=operator_state),
            patch.object(customer_operator, "write_customer_operator_outputs"),
            patch.object(
                customer_operator,
                "_open_in_trusted_etsy_session",
                return_value={
                    "session_name": "esd",
                    "current_url": "https://www.etsy.com/your/messages/123",
                    "reused_existing_session": True,
                },
            ) as open_mock,
        ):
            response = handle_customer_text("customer open C301")

        self.assertIn("Etsy session `esd`", response)
        self.assertIn("Reused session: True", response)
        open_mock.assert_called_once()

    def test_open_in_trusted_etsy_session_does_not_persist_unverified_search_result(self) -> None:
        packet = {
            "packet_id": "pkt-1",
            "short_id": "C397",
            "title": "Customer",
            "browser_url_candidates": ["https://www.etsy.com/messages?ref=hdr_user_menu-messages"],
        }
        with (
            patch.object(customer_operator, "_best_browser_url", return_value="https://www.etsy.com/messages?ref=hdr_user_menu-messages"),
            patch.object(customer_operator, "_resolve_trusted_etsy_session", return_value=("esd", "https://www.etsy.com/messages")),
            patch("review_reply_executor.ensure_authenticated_session", return_value={"reused_existing_session": True}),
            patch.object(
                customer_operator,
                "_locate_thread_via_inbox_search",
                return_value={
                    "ok": True,
                    "strategy": "inbox_search",
                    "target_url": "https://www.etsy.com/messages/1660743861",
                    "landed_url": "https://www.etsy.com/messages/1660743861",
                    "verification_required": True,
                },
            ),
            patch("review_reply_discovery.run_pw_command", return_value=""),
            patch("review_reply_discovery.parse_page_metadata", return_value=("https://www.etsy.com/messages/1660743861", "Messages")),
            patch.object(customer_operator, "_verify_thread_context", return_value={"verification_required": True}),
            patch.object(customer_operator, "_persist_resolved_thread_url") as persist_mock,
        ):
            opened = customer_operator._open_in_trusted_etsy_session(packet)

        persist_mock.assert_not_called()
        self.assertTrue(opened["target_verification_required"])

    def test_handle_customer_open_reports_resolution_strategy(self) -> None:
        packet_payload = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [
                {
                    "packet_id": "pkt-1",
                    "title": "R Henderson",
                    "browser_url_candidates": ["https://ablink.account.etsy.com/redirect"],
                    "order_enrichment": {"receipt_id": "3347"},
                }
            ],
        }
        operator_state = {
            "next_short_id": 301,
            "packet_short_ids": {},
            "current_packet_id": None,
        }
        with (
            patch.object(customer_operator, "load_packets", return_value=packet_payload),
            patch.object(customer_operator, "load_operator_state", return_value=operator_state),
            patch.object(customer_operator, "write_customer_operator_outputs"),
            patch.object(
                customer_operator,
                "_open_in_trusted_etsy_session",
                return_value={
                    "session_name": "esd",
                    "current_url": "https://www.etsy.com/messages/999",
                    "reused_existing_session": True,
                    "target_verification_required": True,
                    "target_resolution_strategy": "inbox_search",
                },
            ),
        ):
            response = handle_customer_text("customer open C301")

        self.assertIn("Resolution strategy: inbox_search", response)
        self.assertIn("Verification required: True", response)

    def test_handle_customer_preview_uses_staged_draft_without_sending(self) -> None:
        packet_payload = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [],
        }
        operator_state = {
            "next_short_id": 301,
            "packet_short_ids": {},
            "current_packet_id": None,
        }
        browser_sync = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [
                {
                    "linked_customer_short_id": "C301",
                    "conversation_contact": "R Henderson",
                    "conversation_thread_key": "etsy-thread::r",
                    "source_artifact_id": "customer_case::mail::133360",
                    "browser_url_candidates": ["https://www.etsy.com/messages/999"],
                    "draft_reply": "Soccer draft",
                    "latest_message_preview": "Buyer wants soccer ducks.",
                    "browser_summary": "Buyer wants soccer ducks.",
                    "order_enrichment": {},
                }
            ],
        }
        with (
            patch.object(customer_operator, "load_packets", return_value=packet_payload),
            patch.object(customer_operator, "load_operator_state", return_value=operator_state),
            patch.object(customer_operator, "load_browser_sync", return_value=browser_sync),
            patch.object(customer_operator, "write_customer_operator_outputs"),
            patch.object(
                customer_operator,
                "_stage_reply_preview_in_trusted_etsy_session",
                return_value={
                    "session_name": "esd",
                    "current_url": "https://www.etsy.com/messages/999",
                    "target_resolution_strategy": "inbox_search",
                    "target_verification_required": False,
                    "thread_verification": {"contactMatch": True, "summaryMatches": ["soccer"]},
                    "preview_typed": True,
                    "screenshot_path": "/tmp/preview.png",
                },
            ),
        ):
            response = handle_customer_text("customer preview C301")

        self.assertIn("Preview staged for C301", response)
        self.assertIn("Reply is typed on the page and not sent.", response)

    def test_record_browser_capture_updates_workflow_control(self) -> None:
        packet = {
            "short_id": "C301",
            "title": "Lisa",
            "conversation_thread_key": "etsy-thread::lisa",
            "source_artifact_id": "customer_case::mail::1",
            "browser_url_candidates": ["https://www.etsy.com/messages/123"],
            "order_enrichment": {"receipt_id": "3348", "transaction_id": "555"},
        }
        with (
            patch.object(customer_operator, "load_json", return_value={"generated_at": "2026-04-12T17:00:00-04:00", "items": []}),
            patch.object(customer_operator, "write_json"),
            patch.object(customer_operator, "record_workflow_transition") as control_mock,
        ):
            _record_browser_capture(packet, "state: waiting_on_customer; summary: Seller already answered.; reply_needed: no; open_loop: customer")

        control_mock.assert_called_once()
        kwargs = control_mock.call_args.kwargs
        self.assertEqual(kwargs["workflow_id"], "customer_reply::C301")
        self.assertEqual(kwargs["state"], "verified")
        self.assertEqual(kwargs["state_reason"], "awaiting_customer")

    def test_backfill_exact_thread_urls_updates_verified_threads_only(self) -> None:
        browser_sync = {
            "generated_at": "2026-04-13T12:00:00-04:00",
            "counts": {},
            "items": [
                {
                    "linked_customer_short_id": "C301",
                    "conversation_contact": "R Henderson",
                    "conversation_thread_key": "etsy-thread::r",
                    "source_artifact_id": "customer_case::mail::133360",
                    "primary_browser_url": "https://ablink.account.etsy.com/redirect",
                    "browser_url_candidates": ["https://ablink.account.etsy.com/redirect"],
                    "latest_message_preview": "Buyer wants 13 personalized soccer ducks with player names.",
                },
                {
                    "linked_customer_short_id": "C302",
                    "conversation_contact": "Kelly",
                    "conversation_thread_key": "etsy-thread::k",
                    "source_artifact_id": "customer_case::mail::133361",
                    "primary_browser_url": "https://www.etsy.com/messages/123",
                    "browser_url_candidates": ["https://www.etsy.com/messages/123"],
                    "latest_message_preview": "Already direct.",
                },
            ],
        }
        with (
            patch.object(customer_operator, "load_browser_sync", return_value=browser_sync),
            patch.object(customer_operator, "_resolve_trusted_etsy_session", return_value=("esd", "https://www.etsy.com/messages?ref=hdr_user_menu-messages")),
            patch("review_reply_executor.ensure_authenticated_session"),
            patch.object(
                customer_operator,
                "_locate_thread_via_inbox_search",
                return_value={
                    "ok": True,
                    "landed_url": "https://www.etsy.com/messages/999",
                    "target_url": "https://www.etsy.com/messages/999",
                    "verification_required": False,
                    "verification": {"summaryMatches": ["soccer"]},
                },
            ),
            patch.object(customer_operator, "_persist_resolved_thread_url") as persist_mock,
            patch.object(customer_operator, "_rerun_observer") as rerun_mock,
        ):
            summary = backfill_exact_thread_urls()

        self.assertEqual(summary["attempted"], 1)
        self.assertEqual(summary["updated"], 1)
        self.assertEqual(summary["updated_short_ids"], ["C301"])
        persist_mock.assert_called_once()
        rerun_mock.assert_called_once()

    def test_handle_customer_preview_records_proposed_control_state(self) -> None:
        packet_payload = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [],
        }
        operator_state = {
            "next_short_id": 301,
            "packet_short_ids": {},
            "current_packet_id": None,
        }
        browser_sync = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [
                {
                    "linked_customer_short_id": "C301",
                    "conversation_contact": "R Henderson",
                    "conversation_thread_key": "etsy-thread::r",
                    "source_artifact_id": "customer_case::mail::133360",
                    "browser_url_candidates": ["https://www.etsy.com/messages/999"],
                    "draft_reply": "Soccer draft",
                    "latest_message_preview": "Buyer wants soccer ducks.",
                    "browser_summary": "Buyer wants soccer ducks.",
                    "order_enrichment": {},
                }
            ],
        }
        with (
            patch.object(customer_operator, "load_packets", return_value=packet_payload),
            patch.object(customer_operator, "load_operator_state", return_value=operator_state),
            patch.object(customer_operator, "load_browser_sync", return_value=browser_sync),
            patch.object(customer_operator, "write_customer_operator_outputs"),
            patch.object(customer_operator, "_stage_reply_preview_in_trusted_etsy_session", return_value={"preview_typed": True, "session_name": "esd", "current_url": "https://www.etsy.com/messages/999", "thread_verification": {"contactMatch": True}, "target_resolution_strategy": "inbox_search"}),
            patch.object(customer_operator, "record_workflow_transition") as control_mock,
        ):
            handle_customer_text("customer preview C301")

        control_mock.assert_called_once()
        kwargs = control_mock.call_args.kwargs
        self.assertEqual(kwargs["workflow_id"], "customer_reply::C301")
        self.assertEqual(kwargs["state"], "proposed")
        self.assertEqual(kwargs["state_reason"], "reply_preview_staged")

    def test_handle_customer_confirm_records_approved_control_state(self) -> None:
        packet_payload = {"generated_at": "2026-04-11T12:00:00-04:00", "counts": {}, "items": []}
        operator_state = {"next_short_id": 301, "packet_short_ids": {}, "current_packet_id": None}
        browser_sync = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [
                {
                    "linked_customer_short_id": "C301",
                    "conversation_contact": "R Henderson",
                    "conversation_thread_key": "etsy-thread::r",
                    "source_artifact_id": "customer_case::mail::133360",
                    "browser_url_candidates": ["https://www.etsy.com/messages/999"],
                    "draft_reply": "Soccer draft",
                    "latest_message_preview": "Buyer wants soccer ducks.",
                    "browser_summary": "Buyer wants soccer ducks.",
                    "order_enrichment": {},
                }
            ],
        }
        with (
            patch.object(customer_operator, "load_packets", return_value=packet_payload),
            patch.object(customer_operator, "load_operator_state", return_value=operator_state),
            patch.object(customer_operator, "load_browser_sync", return_value=browser_sync),
            patch.object(customer_operator, "write_customer_operator_outputs"),
            patch.object(
                customer_operator,
                "_confirm_reply_preview_in_trusted_etsy_session",
                return_value={
                    "preview_confirmed": True,
                    "session_name": "esd",
                    "current_url": "https://www.etsy.com/messages/999",
                    "target_resolution_strategy": "inbox_search",
                    "preview_state": {"textareaVisible": True, "valueMatches": True, "submitVisible": True},
                },
            ),
            patch.object(customer_operator, "record_workflow_transition") as control_mock,
        ):
            response = handle_customer_text("customer confirm C301")

        self.assertIn("Preview is approved for send", response)
        self.assertTrue(control_mock.called)
        kwargs = control_mock.call_args.kwargs
        self.assertEqual(kwargs["workflow_id"], "customer_reply::C301")
        self.assertEqual(kwargs["state"], "approved")
        self.assertEqual(kwargs["state_reason"], "reply_send_confirmed")

    def test_handle_customer_verify_records_send_verification(self) -> None:
        packet_payload = {"generated_at": "2026-04-11T12:00:00-04:00", "counts": {}, "items": []}
        operator_state = {"next_short_id": 301, "packet_short_ids": {}, "current_packet_id": None}
        browser_sync = {
            "generated_at": "2026-04-11T12:00:00-04:00",
            "counts": {},
            "items": [
                {
                    "linked_customer_short_id": "C301",
                    "conversation_contact": "R Henderson",
                    "conversation_thread_key": "etsy-thread::r",
                    "source_artifact_id": "customer_case::mail::133360",
                    "browser_url_candidates": ["https://www.etsy.com/messages/999"],
                    "draft_reply": "Soccer draft",
                    "latest_message_preview": "Buyer wants soccer ducks.",
                    "browser_summary": "Buyer wants soccer ducks.",
                    "order_enrichment": {},
                }
            ],
        }
        with (
            patch.object(customer_operator, "load_packets", side_effect=[packet_payload, packet_payload]),
            patch.object(customer_operator, "load_operator_state", return_value=operator_state),
            patch.object(customer_operator, "load_browser_sync", return_value=browser_sync),
            patch.object(customer_operator, "write_customer_operator_outputs"),
            patch.object(
                customer_operator,
                "_verify_reply_sent_in_trusted_etsy_session",
                return_value={
                    "reply_sent_verified": True,
                    "session_name": "esd",
                    "current_url": "https://www.etsy.com/messages/999",
                    "target_resolution_strategy": "inbox_search",
                    "posted_state": {"bodyContainsReply": True, "textareaVisible": False},
                },
            ),
            patch.object(customer_operator, "_record_browser_capture"),
            patch.object(customer_operator, "_rerun_observer"),
            patch.object(customer_operator, "record_workflow_transition") as control_mock,
        ):
            response = handle_customer_text("customer verify C301")

        self.assertIn("Verified: the reply is posted", response)
        self.assertTrue(control_mock.called)
        kwargs = control_mock.call_args.kwargs
        self.assertEqual(kwargs["workflow_id"], "customer_reply::C301")
        self.assertEqual(kwargs["state"], "verified")
        self.assertEqual(kwargs["state_reason"], "reply_sent_verified")


if __name__ == "__main__":
    unittest.main()
