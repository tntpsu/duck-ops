from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from business_operator_desk import build_business_operator_desk, render_business_operator_desk_markdown


class BusinessOperatorDeskTests(unittest.TestCase):
    def test_operator_desk_shortens_pack_and_sale_titles(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={
                "counts": {"orders_to_pack_units": 3},
                "sections": {
                    "orders_to_pack": [
                        {
                            "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                            "urgency_label": "Today",
                            "order_count": 2,
                            "buyer_count": 2,
                            "total_quantity": 3,
                            "by_channel": {"etsy": 1, "shopify": 2},
                        }
                    ]
                },
            },
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={
                "items": [
                    {
                        "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                        "discount": "15% off",
                        "effectiveness": "weak",
                        "sales_7d": 1,
                        "sales_30d": 3,
                        "marketing_recommendation": "Try a simpler hero angle.",
                        "recommendation": "Rotate or rewrite.",
                    }
                ]
            },
            review_queue={"items": []},
        )

        next_actions = payload.get("next_actions") or []
        packing_action = next(item for item in next_actions if item.get("lane") == "packing")
        weekly_action = next(item for item in next_actions if item.get("lane") == "weekly_sale")

        self.assertEqual(packing_action.get("title"), "Dachshund Duck")
        self.assertEqual(weekly_action.get("title"), "Dachshund Duck")

    def test_operator_desk_markdown_shortens_visible_titles(self) -> None:
        markdown = render_business_operator_desk_markdown(
            {
                "generated_at": "2026-04-11T21:00:00-04:00",
                "counts": {
                    "customer_attention_items": 0,
                    "replacement_labels_now": 0,
                    "etsy_browser_threads": 0,
                    "threads_with_staged_reply": 0,
                    "threads_waiting_on_customer": 0,
                    "custom_build_candidates": 0,
                    "custom_build_tasks_live": 0,
                    "orders_to_pack_units": 3,
                    "stock_print_candidates": 1,
                    "active_weekly_sale_items": 1,
                    "weak_weekly_sale_items": 1,
                    "review_queue_items": 0,
                    "review_queue_backlog": 0,
                    "usps_live_customer_items": 0,
                },
                "next_actions": [],
                "sections": {
                    "customer_packets": [],
                    "etsy_browser_threads": [],
                    "custom_build_candidates": [],
                    "orders_to_pack": [
                        {
                            "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                            "urgency_label": "Today",
                            "total_quantity": 3,
                            "by_channel": {"etsy": 1, "shopify": 2},
                        }
                    ],
                    "stock_print_candidates": [
                        {
                            "product_title": "Michigan Wolverines Duck – Officially Licensed Duck with Team Spirit & M Pride",
                            "priority": "high",
                            "recent_demand": 4,
                            "why_now": "Inventory is low.",
                        }
                    ],
                    "weekly_sale_monitor": [
                        {
                            "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                            "discount": "15% off",
                            "effectiveness": "weak",
                            "sales_7d": 1,
                            "sales_30d": 3,
                            "recommendation": "Rotate or rewrite.",
                            "marketing_recommendation": "Try a simpler hero angle.",
                        }
                    ],
                    "review_queue": [],
                },
            }
        )

        self.assertIn("Dachshund Duck", markdown)
        self.assertNotIn("Gift for Dog Lovers Desk Decor", markdown)
        self.assertIn("Michigan Wolverines Duck", markdown)
        self.assertNotIn("Officially Licensed Duck with Team Spirit", markdown)

    def test_operator_desk_packing_summary_uses_unknown_buyer_display(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={
                "counts": {"orders_to_pack_units": 1},
                "sections": {
                    "orders_to_pack": [
                        {
                            "product_title": "Patrick Star Duck – Goofy Underwater Duck Collectible",
                            "urgency_label": "Open",
                            "order_count": 1,
                            "buyer_count": 0,
                            "buyer_count_display": "Hidden by Shopify",
                            "total_quantity": 1,
                            "by_channel": {"etsy": 0, "shopify": 1},
                        }
                    ]
                },
            },
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={"items": []},
            review_queue={"items": []},
        )

        packing_action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "packing")
        self.assertIn("Hidden by Shopify buyer", packing_action.get("summary") or "")

    def test_operator_desk_pack_section_shows_choices(self) -> None:
        markdown = render_business_operator_desk_markdown(
            {
                "generated_at": "2026-04-11T21:00:00-04:00",
                "counts": {"orders_to_pack_units": 2},
                "next_actions": [],
                "sections": {
                    "customer_packets": [],
                    "etsy_browser_threads": [],
                    "custom_build_candidates": [],
                    "orders_to_pack": [
                        {
                            "product_title": "Duckzilla Monster Duck",
                            "urgency_label": "Open",
                            "total_quantity": 2,
                            "buyer_count_display": "1",
                            "option_summary": "Color: Blue, Color: Pink",
                            "by_channel": {"etsy": 2, "shopify": 0},
                        }
                    ],
                    "stock_print_candidates": [],
                    "weekly_sale_monitor": [],
                    "review_queue": [],
                    "workflow_followthrough": [],
                },
            }
        )

        self.assertIn("Choices: Color: Blue, Color: Pink", markdown)

    def test_operator_desk_includes_workflow_followthrough(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={"counts": {}, "sections": {}},
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={"items": []},
            review_queue={"items": []},
            workflow_followthrough=[
                {
                    "lane": "weekly",
                    "title": "Spring Ducks",
                    "summary": "stale input | article 123",
                    "next_action": "Refresh the weekly draft",
                }
            ],
        )

        markdown = render_business_operator_desk_markdown(payload)

        self.assertEqual(payload["counts"]["workflow_followthrough_items"], 1)
        self.assertIn("## Workflow Follow-Through", markdown)
        self.assertIn("weekly: Spring Ducks", markdown)
        self.assertIn("Refresh the weekly draft", markdown)

    def test_operator_desk_workflow_followthrough_shows_root_cause(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={"counts": {}, "sections": {}},
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={"items": []},
            review_queue={"items": []},
            workflow_followthrough=[
                {
                    "lane": "meme",
                    "title": "Meme 2026-04-06",
                    "summary": "execution failed",
                    "root_cause": "Facebook object id is invalid.",
                    "fix_hint": "Fix the Meta target.",
                    "next_action": "Retry publish",
                }
            ],
        )

        markdown = render_business_operator_desk_markdown(payload)

        self.assertIn("Why: Facebook object id is invalid.", markdown)
        self.assertIn("Fix: Fix the Meta target.", markdown)

    def test_operator_desk_surfaces_current_learnings(self) -> None:
        with patch(
            "business_operator_desk._load_learning_surface",
            return_value={
                "available": True,
                "path": "/tmp/current_learnings.md",
                "items": [{"headline": "Evening is the current best-performing posting window."}],
                "change_count": 1,
                "idea_count": 2,
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        self.assertEqual(payload["counts"]["learning_beliefs"], 1)
        self.assertIn("## Learning Surface", markdown)
        self.assertIn("Evening is the current best-performing posting window.", markdown)


if __name__ == "__main__":
    unittest.main()
