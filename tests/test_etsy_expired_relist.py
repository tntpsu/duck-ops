import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path("/Users/philtullai/ai-agents/duck-ops/runtime/etsy_expired_relist.py")
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("etsy_expired_relist", MODULE_PATH)
etsy_expired_relist = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(etsy_expired_relist)


class EtsyExpiredRelistTests(unittest.TestCase):
    def test_build_relist_queue_applies_sales_and_daily_cap(self) -> None:
        expired_payload = {
            "generated_at": "2026-04-13T12:00:00-04:00",
            "items": [
                {
                    "listing_id": "1",
                    "title": "Best Seller Duck",
                    "num_favorers": 10,
                    "ending_timestamp": 1776019882,
                },
                {
                    "listing_id": "2",
                    "title": "No Sales Duck",
                    "num_favorers": 50,
                    "ending_timestamp": 1776019883,
                },
                {
                    "listing_id": "3",
                    "title": "Already Renewed Duck",
                    "num_favorers": 5,
                    "ending_timestamp": 1776019884,
                },
            ],
        }
        sales_counts = {
            "generated_at": "2026-04-13T11:00:00-04:00",
            "counts": {"1": 4, "2": 0, "3": 2},
        }
        history = [
            {
                "date_local": "2026-04-13",
                "listing_id": "3",
                "status": "renewed",
            }
        ]

        queue = etsy_expired_relist.build_relist_queue(expired_payload, sales_counts, history, daily_limit=3)

        eligible_ids = [item["listing_id"] for item in queue["eligible_items"]]
        skipped = {item["listing_id"]: item["eligibility_reason"] for item in queue["skipped_items"]}

        self.assertEqual(eligible_ids, ["1"])
        self.assertEqual(skipped["2"], "no_recorded_sales")
        self.assertEqual(skipped["3"], "already_renewed_today")
        self.assertEqual(queue["renewed_today"], 1)
        self.assertEqual(queue["remaining_today"], 2)

    def test_build_relist_queue_blocks_when_daily_limit_is_spent(self) -> None:
        expired_payload = {
            "generated_at": "2026-04-13T12:00:00-04:00",
            "items": [
                {"listing_id": "1", "title": "Duck One", "num_favorers": 2, "ending_timestamp": 1776019882},
                {"listing_id": "2", "title": "Duck Two", "num_favorers": 3, "ending_timestamp": 1776019883},
            ],
        }
        sales_counts = {"generated_at": "2026-04-13T11:00:00-04:00", "counts": {"1": 1, "2": 1}}
        history = [
            {"date_local": "2026-04-13", "listing_id": "10", "status": "renewed"},
            {"date_local": "2026-04-13", "listing_id": "11", "status": "renewed"},
            {"date_local": "2026-04-13", "listing_id": "12", "status": "renewed"},
        ]

        queue = etsy_expired_relist.build_relist_queue(expired_payload, sales_counts, history, daily_limit=3)

        self.assertEqual(queue["eligible_count"], 0)
        self.assertEqual(queue["remaining_today"], 0)
        self.assertTrue(all(item["eligibility_reason"] == "daily_limit_reached" for item in queue["skipped_items"]))

    def test_render_markdown_surfaces_eligible_and_skipped_sections(self) -> None:
        payload = {
            "generated_at": "2026-04-13T12:00:00-04:00",
            "expired_count": 2,
            "eligible_count": 1,
            "daily_limit": 3,
            "renewed_today": 1,
            "remaining_today": 2,
            "sales_counts_generated_at": "2026-04-13T11:00:00-04:00",
            "eligible_items": [
                {
                    "title": "Best Seller Duck",
                    "listing_id": "1",
                    "sales_count": 4,
                    "num_favorers": 10,
                    "expires_on": "Apr 13, 2026",
                    "edit_url": "https://www.etsy.com/your/shops/me/listing-editor/edit/1",
                }
            ],
            "skipped_items": [
                {
                    "title": "No Sales Duck",
                    "listing_id": "2",
                    "sales_count": 0,
                    "eligibility_reason": "no_recorded_sales",
                }
            ],
            "last_run": {
                "mode": "preview",
                "at": "2026-04-13T12:05:00-04:00",
                "results": [{"title": "Best Seller Duck", "status": "previewed"}],
            },
        }

        rendered = etsy_expired_relist.render_markdown(payload)

        self.assertIn("## Eligible Today", rendered)
        self.assertIn("Best Seller Duck", rendered)
        self.assertIn("## Skipped", rendered)
        self.assertIn("No Sales Duck | `no_recorded_sales`", rendered)
        self.assertIn("## Last Run", rendered)


if __name__ == "__main__":
    unittest.main()
