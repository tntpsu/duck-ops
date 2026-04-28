from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import inventory_truth


class InventoryTruthTests(unittest.TestCase):
    def test_demand_only_candidate_is_not_treated_as_low_stock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            products_path = root / "products_cache.json"
            weekly_report_path = root / "weekly_report.json"
            output_path = root / "inventory_truth.json"
            output_md_path = root / "inventory_truth.md"
            products_path.write_text(json.dumps({"items": {"p1": {"id": "p1", "title": "Dachshund Duck"}}}), encoding="utf-8")
            weekly_report_path.write_text(json.dumps({"items": {}}), encoding="utf-8")

            with (
                mock.patch.object(inventory_truth, "PRODUCTS_CACHE_PATH", products_path),
                mock.patch.object(inventory_truth, "WEEKLY_REPORT_PATH", weekly_report_path),
                mock.patch.object(inventory_truth, "INVENTORY_TRUTH_PATH", output_path),
                mock.patch.object(inventory_truth, "INVENTORY_TRUTH_MD_PATH", output_md_path),
            ):
                payload = inventory_truth.build_inventory_truth(
                    print_queue_candidates={
                        "items": [
                            {
                                "product_id": "p1",
                                "product_title": "Dachshund Duck",
                                "priority": "high",
                                "recent_demand": 70,
                                "lifetime_demand": 8024,
                                "confidence": 0.68,
                            }
                        ]
                    }
                )

            self.assertEqual(payload["status"], "demand_only")
            self.assertEqual(payload["summary"]["demand_only_count"], 1)
            self.assertEqual(payload["summary"]["confirmed_low_stock_count"], 0)
            self.assertEqual(payload["items"][0]["inventory_evidence_level"], "demand_only")
            self.assertEqual(payload["items"][0]["stock_evidence"], "not_yet_available")
            self.assertLessEqual(payload["items"][0]["confidence"], 0.45)
            self.assertIn("demand signal only", output_md_path.read_text(encoding="utf-8"))

    def test_cached_variant_inventory_can_mark_low_stock_after_live_verification(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            products_path = root / "products_cache.json"
            weekly_report_path = root / "weekly_report.json"
            products_path.write_text(
                json.dumps(
                    {
                        "items": {
                            "p2": {
                                "id": "p2",
                                "title": "Orange Cat Duck",
                                "variants": [{"inventory_quantity": 1}],
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            weekly_report_path.write_text(json.dumps({"items": {}}), encoding="utf-8")

            with (
                mock.patch.object(inventory_truth, "PRODUCTS_CACHE_PATH", products_path),
                mock.patch.object(inventory_truth, "WEEKLY_REPORT_PATH", weekly_report_path),
            ):
                payload = inventory_truth.build_inventory_truth(
                    print_queue_candidates={
                        "items": [
                            {
                                "product_id": "p2",
                                "product_title": "Orange Cat Duck",
                                "priority": "medium",
                                "recent_demand": 9,
                            }
                        ]
                    },
                    write_outputs=False,
                )

            self.assertEqual(payload["status"], "print_review_needed")
            self.assertEqual(payload["summary"]["confirmed_low_stock_count"], 1)
            self.assertEqual(payload["items"][0]["inventory_evidence_level"], "confirmed_low_stock")
            self.assertEqual(payload["items"][0]["cached_inventory_total"], 1)


if __name__ == "__main__":
    unittest.main()
