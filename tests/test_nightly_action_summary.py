from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from nightly_action_summary import (
    build_nightly_action_summary,
    render_nightly_action_summary_html,
    render_nightly_action_summary_markdown,
)


class NightlyActionSummaryTests(unittest.TestCase):
    def test_orders_to_pack_is_mobile_friendly(self) -> None:
        payload = {
            "generated_at": "2026-04-11T20:51:47-04:00",
            "counts": {
                "customer_attention_items": 0,
                "replacement_labels_now": 0,
                "orders_to_pack_titles": 2,
                "orders_to_pack_units": 5,
                "custom_order_lines": 0,
            },
            "sections": {
                "customer_issues_needing_attention": [],
                "buy_replacement_labels_now": [],
                "orders_to_pack": [
                    {
                        "product_title": "Dachshund Duck",
                        "urgency_label": "Today",
                        "order_count": 2,
                        "buyer_count": 2,
                        "total_quantity": 3,
                        "by_channel": {"etsy": 1, "shopify": 2},
                    },
                    {
                        "product_title": "Football Ducks",
                        "urgency_label": "Aging order",
                        "order_count": 1,
                        "buyer_count": 1,
                        "total_quantity": 2,
                        "by_channel": {"etsy": 2, "shopify": 0},
                    },
                ],
                "custom_novel_ducks_to_make": {},
                "watch_list": [],
            },
        }

        markdown = render_nightly_action_summary_markdown(payload)

        self.assertNotIn("| Duck | When | Orders |", markdown)
        self.assertIn("- Totals: Etsy 3 / Shopify 2 / Total units 5", markdown)
        self.assertIn("- Dachshund Duck", markdown)
        self.assertIn("  Orders: 2 | Total units: 3", markdown)
        self.assertIn("  Channels: Etsy 1 / Shopify 2", markdown)

    def test_long_order_titles_are_trimmed(self) -> None:
        payload = {
            "generated_at": "2026-04-11T20:51:47-04:00",
            "counts": {
                "customer_attention_items": 0,
                "replacement_labels_now": 0,
                "orders_to_pack_titles": 1,
                "orders_to_pack_units": 1,
                "custom_order_lines": 0,
            },
            "sections": {
                "customer_issues_needing_attention": [],
                "buy_replacement_labels_now": [],
                "orders_to_pack": [
                    {
                        "product_title": "Very Long Duck Title " * 10,
                        "urgency_label": "Open",
                        "order_count": 1,
                        "buyer_count": 1,
                        "total_quantity": 1,
                        "by_channel": {"etsy": 1, "shopify": 0},
                    },
                ],
                "custom_novel_ducks_to_make": {},
                "watch_list": [],
            },
        }

        markdown = render_nightly_action_summary_markdown(payload)

        self.assertIn("...", markdown)

    def test_order_display_name_strips_seo_filler(self) -> None:
        payload = {
            "generated_at": "2026-04-11T20:51:47-04:00",
            "counts": {
                "customer_attention_items": 0,
                "replacement_labels_now": 0,
                "orders_to_pack_titles": 1,
                "orders_to_pack_units": 1,
                "custom_order_lines": 0,
            },
            "sections": {
                "customer_issues_needing_attention": [],
                "buy_replacement_labels_now": [],
                "orders_to_pack": [
                    {
                        "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                        "urgency_label": "Today",
                        "order_count": 1,
                        "buyer_count": 1,
                        "total_quantity": 1,
                        "by_channel": {"etsy": 1, "shopify": 0},
                    },
                ],
                "custom_novel_ducks_to_make": {},
                "watch_list": [],
            },
        }

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertIn("- Dachshund Duck", markdown)
        self.assertNotIn("Gift for Dog Lovers", markdown)
        self.assertIn(">Dachshund Duck<", html)
        self.assertIn('title="Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor"', html)

    def test_orders_to_pack_html_uses_clean_tables(self) -> None:
        payload = {
            "generated_at": "2026-04-11T20:51:47-04:00",
            "counts": {
                "customer_attention_items": 0,
                "replacement_labels_now": 0,
                "orders_to_pack_titles": 2,
                "orders_to_pack_units": 5,
                "custom_order_lines": 0,
            },
            "sections": {
                "customer_issues_needing_attention": [],
                "buy_replacement_labels_now": [],
                "orders_to_pack": [
                    {
                        "product_title": "Dachshund Duck",
                        "urgency_label": "Today",
                        "order_count": 2,
                        "buyer_count": 2,
                        "total_quantity": 3,
                        "by_channel": {"etsy": 1, "shopify": 2},
                    },
                    {
                        "product_title": "Football Ducks",
                        "urgency_label": "Aging order",
                        "order_count": 1,
                        "buyer_count": 1,
                        "total_quantity": 2,
                        "by_channel": {"etsy": 2, "shopify": 0},
                    },
                ],
                "custom_novel_ducks_to_make": {},
                "watch_list": [],
            },
        }

        html = render_nightly_action_summary_html(payload)

        self.assertIn("<table", html)
        self.assertIn(">Duck<", html)
        self.assertIn(">Orders<", html)
        self.assertIn(">Etsy<", html)
        self.assertIn(">Shopify<", html)
        self.assertIn(">Units<", html)

    def test_unknown_buyers_render_as_unknown_not_zero(self) -> None:
        payload = {
            "generated_at": "2026-04-11T20:51:47-04:00",
            "counts": {
                "customer_attention_items": 0,
                "replacement_labels_now": 0,
                "orders_to_pack_titles": 1,
                "orders_to_pack_units": 1,
                "custom_order_lines": 0,
            },
            "sections": {
                "customer_issues_needing_attention": [],
                "buy_replacement_labels_now": [],
                "orders_to_pack": [
                    {
                        "product_title": "Patrick Star Duck",
                        "urgency_label": "Open",
                        "order_count": 1,
                        "buyer_count": 0,
                        "buyer_count_display": "Unknown",
                        "total_quantity": 1,
                        "by_channel": {"etsy": 0, "shopify": 1},
                    },
                ],
                "custom_novel_ducks_to_make": {},
                "watch_list": [],
            },
        }

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertNotIn("Buyers:", markdown)
        self.assertNotIn(">Unknown<", html)

    def test_orders_to_pack_show_choice_summary(self) -> None:
        payload = {
            "generated_at": "2026-04-11T20:51:47-04:00",
            "counts": {
                "customer_attention_items": 0,
                "replacement_labels_now": 0,
                "orders_to_pack_titles": 1,
                "orders_to_pack_units": 2,
                "custom_order_lines": 0,
            },
            "sections": {
                "customer_issues_needing_attention": [],
                "buy_replacement_labels_now": [],
                "orders_to_pack": [
                    {
                        "product_title": "Duckzilla Monster Duck",
                        "urgency_label": "Open",
                        "order_count": 1,
                        "buyer_count": 1,
                        "buyer_count_display": "1",
                        "total_quantity": 2,
                        "option_summary": "Color: Blue, Color: Pink",
                        "by_channel": {"etsy": 2, "shopify": 0},
                    },
                ],
                "custom_novel_ducks_to_make": {},
                "watch_list": [],
            },
        }

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertIn("Choices: Color: Blue, Color: Pink", markdown)
        self.assertIn(">Color: Blue, Color: Pink<", html)

    def test_orders_to_pack_show_snapshot_freshness(self) -> None:
        payload = {
            "generated_at": "2026-04-13T00:07:43-04:00",
            "counts": {
                "customer_attention_items": 0,
                "replacement_labels_now": 0,
                "orders_to_pack_titles": 1,
                "orders_to_pack_units": 2,
                "custom_order_lines": 0,
            },
            "order_snapshot_refresh": {
                "sources": {
                    "etsy": {
                        "status": "live",
                        "generated_at": "2026-04-13T00:07:00-04:00",
                    },
                    "shopify": {
                        "status": "fallback_cached",
                        "generated_at": "2026-04-12T23:04:09-04:00",
                    },
                }
            },
            "sections": {
                "customer_issues_needing_attention": [],
                "buy_replacement_labels_now": [],
                "orders_to_pack": [
                    {
                        "product_title": "Duckzilla Monster Duck",
                        "order_count": 1,
                        "total_quantity": 2,
                        "option_summary": "Color: Blue, Color: Pink",
                        "by_channel": {"etsy": 2, "shopify": 0},
                    }
                ],
                "custom_novel_ducks_to_make": {},
                "watch_list": [],
            },
        }

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertIn("Snapshot freshness: Etsy live at 12:07 AM | Shopify cached fallback at 11:04 PM", markdown)
        self.assertIn("Snapshot freshness:", html)

    def test_build_summary_splits_new_threads_and_followups(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            etsy_browser_sync={
                "items": [
                    {
                        "conversation_contact": "Amy",
                        "browser_review_status": "needs_browser_review",
                        "recommended_next_action": "Open the Etsy thread.",
                        "latest_message_preview": "Do you have this in blue?",
                    },
                    {
                        "conversation_contact": "Ben",
                        "browser_review_status": "captured",
                        "follow_up_state": "reply_drafted",
                        "draft_reply": "Yes, I can do that.",
                        "recommended_next_action": "Send the staged draft reply.",
                        "latest_message_preview": "Can you change the color?",
                    },
                    {
                        "conversation_contact": "Cara",
                        "browser_review_status": "captured",
                        "follow_up_state": "waiting_on_customer",
                        "recommended_next_action": "Wait for customer confirmation.",
                        "latest_message_preview": "I will send the size tonight.",
                    },
                ]
            },
        )

        self.assertEqual(payload["counts"]["customer_new_thread_items"], 1)
        self.assertEqual(payload["counts"]["customer_follow_up_items"], 1)
        self.assertEqual(payload["counts"]["customer_waiting_on_customer"], 1)
        self.assertEqual(payload["counts"]["customer_follow_up_reply_drafts"], 1)
        markdown = render_nightly_action_summary_markdown(payload)
        self.assertIn("New customer threads: 1", markdown)
        self.assertIn("Customer actions already in motion: 1", markdown)
        self.assertIn("Waiting on the customer (info only): 1", markdown)
        self.assertIn("## 1. Top Customer Actions Tonight", markdown)
        self.assertIn("## 2. New Customer Threads", markdown)
        self.assertIn("## 3. Customer Status Counts", markdown)
        self.assertIn("1 new Etsy thread(s) still need first review", markdown)
        self.assertIn("Those waiting-on-customer threads are intentionally omitted", markdown)

    def test_nightly_summary_limits_customer_actions_to_top_five(self) -> None:
        followup_items = []
        for idx in range(6):
            followup_items.append(
                {
                    "conversation_contact": f"Buyer {idx}",
                    "browser_review_status": "captured",
                    "follow_up_state": "reply_drafted",
                    "draft_reply": f"Reply {idx}",
                    "recommended_next_action": f"Send reply {idx}",
                    "latest_message_preview": f"Preview {idx}",
                }
            )

        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            etsy_browser_sync={"items": followup_items},
        )

        markdown = render_nightly_action_summary_markdown(payload)

        self.assertEqual(payload["counts"]["customer_top_action_items"], 5)
        self.assertEqual(payload["counts"]["customer_hidden_action_items"], 1)
        self.assertIn("Showing the top 5 customer actions. 1 more are queued behind these.", markdown)
        self.assertIn("Buyer 0", markdown)
        self.assertNotIn("Buyer 5", markdown)

    def test_nightly_summary_shows_customer_categories_and_open_targets(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            etsy_browser_sync={
                "items": [
                    {
                        "conversation_contact": "Ben",
                        "browser_review_status": "captured",
                        "follow_up_state": "reply_drafted",
                        "draft_reply": "Yes, I can do that.",
                        "recommended_next_action": "Send the staged draft reply.",
                        "latest_message_preview": "Can you change the color?",
                        "linked_customer_short_id": "C381",
                        "open_command": "customer open C381",
                        "primary_browser_url": "https://www.etsy.com/messages/1660743861",
                    }
                ]
            },
        )

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertIn("- Category: Reply ready to send", markdown)
        self.assertIn("- Thread ID: C381", markdown)
        self.assertIn("https://www.etsy.com/messages/1660743861", markdown)
        self.assertIn("customer open C381", markdown)
        self.assertIn("Open thread", html)
        self.assertIn("Thread ID:</strong> C381", html)

    def test_nightly_summary_prefers_specific_direct_thread_actions_over_generic_inbox_items(self) -> None:
        payload = build_nightly_action_summary(
            {
                "items": [
                    {
                        "title": "Customer reply",
                        "priority": "medium",
                        "packet_type": "reply",
                        "customer_name": "Customer",
                        "customer_summary": "Latest Etsy conversation needs review.",
                        "source_refs": [
                            {"subject": "MEREDITH needs help with an order they placed"},
                        ],
                    }
                ]
            },
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            etsy_browser_sync={
                "items": [
                    {
                        "conversation_contact": "Ben",
                        "browser_review_status": "captured",
                        "follow_up_state": "reply_drafted",
                        "draft_reply": "Yes, I can do that.",
                        "recommended_next_action": "Send the staged draft reply.",
                        "latest_message_preview": "Can you change the color?",
                        "linked_customer_short_id": "C381",
                        "open_command": "customer open C381",
                        "primary_browser_url": "https://www.etsy.com/messages/1660743861",
                    }
                ]
            },
        )

        top = payload["sections"]["customer_top_actions"]
        self.assertEqual(top[0]["contact"], "Ben")
        self.assertEqual(top[0]["open_link"], "https://www.etsy.com/messages/1660743861")
        self.assertEqual(top[1]["contact"], "MEREDITH")
        self.assertEqual(top[1]["open_link"], "https://www.etsy.com/messages?ref=hdr_user_menu-messages")

    def test_nightly_summary_renders_customer_thread_sync_and_workflow_notes(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            etsy_browser_sync={
                "generated_at": "2026-04-13T19:41:59-04:00",
                "items": [
                    {
                        "conversation_contact": "Cara",
                        "browser_review_status": "captured",
                        "follow_up_state": "waiting_on_customer",
                        "recommended_next_action": "Wait for customer confirmation.",
                        "latest_message_preview": "I will send the size tonight.",
                    },
                ],
            },
            workflow_followthrough=[
                {
                    "lane": "weekly_sale_monitor",
                    "title": "Weekly Sale Monitor",
                    "summary": "stale input",
                    "next_action": "No manual refresh is needed if you are waiting for the next weekly sale or campaign.",
                    "actionable": False,
                    "latest_receipt": "snapshot at Apr 13, 7:27 PM",
                }
            ],
        )

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertIn("Customer thread sync: 7:41 PM", markdown)
        self.assertIn("## 9. Workflow Notes", markdown)
        self.assertIn("weekly_sale_monitor: Weekly Sale Monitor", markdown)
        self.assertIn("Customer thread sync:", html)
        self.assertIn("Workflow Notes", html)

    def test_nightly_summary_accepts_real_etsy_inbox_link(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            etsy_browser_sync={
                "items": [
                    {
                        "conversation_contact": "Patrick",
                        "browser_review_status": "captured",
                        "follow_up_state": "needs_reply",
                        "recommended_next_action": "Open the thread and reply.",
                        "latest_message_preview": "Buyer has a follow-up question.",
                        "linked_customer_short_id": "C315",
                        "open_command": "customer open C315",
                        "primary_browser_url": "https://www.etsy.com/messages?ref=hdr_user_menu-messages",
                    }
                ]
            },
        )

        markdown = render_nightly_action_summary_markdown(payload)

        self.assertIn("https://www.etsy.com/messages?ref=hdr_user_menu-messages", markdown)

    def test_nightly_summary_includes_workflow_followthrough(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            workflow_followthrough=[
                {
                    "lane": "weekly",
                    "title": "Spring Ducks",
                    "summary": "stale input | article 123",
                    "next_action": "Refresh the weekly draft",
                }
            ],
        )

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertEqual(payload["counts"]["workflow_followthrough_items"], 1)
        self.assertIn("Workflow follow-through items: 1", markdown)
        self.assertIn("## 8. Workflow Follow-Through", markdown)
        self.assertIn("weekly: Spring Ducks", markdown)
        self.assertIn("Refresh the weekly draft", html)

    def test_nightly_summary_includes_workflow_root_cause_and_fix(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            workflow_followthrough=[
                {
                    "lane": "meme",
                    "title": "Meme 2026-04-06",
                    "summary": "execution failed",
                    "next_action": "Retry publish",
                    "root_cause": "Facebook object id is invalid and Instagram returned a transient OAuth error.",
                    "fix_hint": "Fix the Meta target and retry after the transient error clears.",
                }
            ],
        )

        markdown = render_nightly_action_summary_markdown(payload)

        self.assertIn("Why: Facebook object id is invalid", markdown)
        self.assertIn("Fix: Fix the Meta target", markdown)

    def test_nightly_summary_includes_inline_quality_gate_urgent_items(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            workflow_followthrough=[
                {
                    "lane": "quality_gate",
                    "title": "Quality Gate",
                    "summary": "alerts pending",
                    "next_action": "Review the urgent quality gate alerts.",
                    "root_cause": "1 urgent quality gate item is still open.",
                    "fix_hint": "Archive or rerun the stale item.",
                    "urgent_items": [
                        {
                            "title": "Weekly Sale Playbook",
                            "decision": "discard",
                            "priority": "high",
                            "why": "Weekly sale playbook is stale for a publish decision and should not be acted on as-is.",
                        }
                    ],
                }
            ],
        )

        markdown = render_nightly_action_summary_markdown(payload)
        html = render_nightly_action_summary_html(payload)

        self.assertIn("Urgent items:", markdown)
        self.assertIn("Weekly Sale Playbook | discard | high", markdown)
        self.assertIn("Weekly sale playbook is stale", markdown)
        self.assertIn("<strong>Urgent items:</strong>", html)

    def test_nightly_summary_collapses_new_thread_noise(self) -> None:
        payload = build_nightly_action_summary(
            {"items": []},
            [],
            {"orders_to_pack": [], "custom_orders_to_make": []},
            etsy_browser_sync={
                "items": [
                    {
                        "conversation_contact": "Logan",
                        "browser_review_status": "needs_browser_review",
                        "recommended_next_action": "Open the Etsy thread.",
                        "latest_message_preview": "Need help with my order.",
                    },
                    {
                        "conversation_contact": "MEREDITH",
                        "browser_review_status": "needs_browser_review",
                        "recommended_next_action": "Open the Etsy thread.",
                        "latest_message_preview": "Question about sizing.",
                    },
                ]
            },
        )

        markdown = render_nightly_action_summary_markdown(payload)

        self.assertIn("2 new Etsy thread(s) still need first review.", markdown)
        self.assertIn("use `customer threads`", markdown)
        self.assertNotIn("Logan", markdown)
        self.assertNotIn("MEREDITH", markdown)


if __name__ == "__main__":
    unittest.main()
