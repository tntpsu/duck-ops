from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import weekly_campaign_coordination
import weekly_sale_monitor


class WeeklyControlTests(unittest.TestCase):
    def test_build_weekly_sale_monitor_rehydrates_stale_active_sales(self) -> None:
        with TemporaryDirectory() as tmp:
            active_sales_path = Path(tmp) / "active_sales.json"
            stale_payload = {
                "shopify": [],
                "timestamp": "2026-04-12T07:10:54-04:00",
            }
            refreshed_payload = {
                "shopify": [{"id": "1", "title": "Sale Duck", "discount": "20%"}],
                "timestamp": "2026-04-15T06:30:00-04:00",
                "source": "shopify_live_collection",
            }
            sales_cache = {"last_sync": "2026-04-15T06:15:00-04:00", "lifetime": {"1": 22}, "last_30d": {"1": 4}}
            with patch.object(weekly_sale_monitor, "ACTIVE_SALES_PATH", active_sales_path), patch.object(
                weekly_sale_monitor,
                "_fetch_live_active_sales_payload",
                return_value=refreshed_payload,
            ):
                payload = weekly_sale_monitor.build_weekly_sale_monitor(
                    active_sales_payload=stale_payload,
                    sales_cache_payload=sales_cache,
                    weekly_insights_payload={},
                )

            self.assertEqual(payload["counts"]["active_sale_items"], 1)
            self.assertEqual(payload["source_timestamps"]["active_sales"], refreshed_payload["timestamp"])
            self.assertEqual(payload["items"][0]["product_title"], "Sale Duck")
            self.assertTrue(active_sales_path.exists())

    def test_weekly_sale_monitor_sync_marks_stale_input_as_blocked(self) -> None:
        payload = {
            "generated_at": "2026-04-12T17:00:00-04:00",
            "source_freshness_hours": {"active_sales": 31.0, "sales_cache": 2.0},
            "counts": {"active_sale_items": 4, "strong": 1, "working": 1, "watch": 1, "weak": 1},
            "summary": {"top_keep_titles": ["Duck A"], "top_rotate_titles": ["Duck B"]},
        }
        with patch.object(weekly_sale_monitor, "record_workflow_transition", return_value={"state": "blocked", "state_reason": "stale_input", "updated_at": "2026-04-12T17:01:00-04:00", "next_action": "refresh"} ) as control_mock:
            result = weekly_sale_monitor.sync_weekly_sale_monitor_control(payload)

        self.assertEqual(result["workflow_control"]["state_reason"], "stale_input")
        self.assertEqual(control_mock.call_args.kwargs["state"], "blocked")
        self.assertIn("rebuild it automatically", control_mock.call_args.kwargs["next_action"])

    def test_weekly_campaign_coordination_sync_marks_missing_sale_playbook_as_blocked(self) -> None:
        payload = {
            "generated_at": "2026-04-12T17:00:00-04:00",
            "weekly_theme": {"is_sale_primary_week": True, "theme_name": "Special Offers", "rotation_week": 3},
            "latest_weekly_state": {"weekly_sale_playbook_sent": False, "weekly_sale_published": False},
            "sale_monitor_snapshot": {"freshness_hours": 2.0},
            "coordination": {
                "lead_lane": "weekly_sale",
                "publication_lane": "weekly_sale_blog",
                "publication_source": "waiting_for_sale_playbook",
                "summary": "Sale-led week.",
            },
        }
        with patch.object(weekly_campaign_coordination, "record_workflow_transition", return_value={"state": "blocked", "state_reason": "blocked_by_upstream", "updated_at": "2026-04-12T17:01:00-04:00", "next_action": "finish playbook"} ) as control_mock:
            result = weekly_campaign_coordination.sync_weekly_campaign_coordination_control(payload)

        self.assertEqual(result["workflow_control"]["state_reason"], "blocked_by_upstream")
        self.assertEqual(control_mock.call_args.kwargs["state"], "blocked")


if __name__ == "__main__":
    unittest.main()
