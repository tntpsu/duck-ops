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

import operator_interface_contracts as contracts  # noqa: E402


class OperatorInterfaceContractsTests(unittest.TestCase):
    def test_compact_surface_and_widget_payload_share_the_same_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            packets_dir = root / "customer_intelligence"
            packets_dir.mkdir(parents=True, exist_ok=True)

            packing_path = root / "packing_summary.json"
            cases_path = root / "customer_cases.json"
            publish_path = root / "publish_candidates.json"
            tasks_path = root / "custom_build_task_candidates.json"
            trends_path = root / "trend_candidates.json"
            receipts_path = root / "etsy_receipts_snapshot.json"
            catalog_path = root / "catalog_index.json"
            usps_path = root / "usps_tracking_snapshot.json"
            etsy_tx_path = root / "etsy_transactions_snapshot.json"
            shopify_path = root / "shopify_open_orders_snapshot.json"
            rejected_path = root / "operator_rejected_artifacts.json"
            packets_path = packets_dir / "customer_action_packets__latest.json"

            packing_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-24T07:00:00-04:00",
                        "counts": {"non_custom_units": 3},
                        "orders_to_pack": [
                            {
                                "product_title": "Orange Cat Duck - Desk Decor",
                                "total_quantity": 3,
                                "by_channel": {"etsy": 1, "shopify": 2},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            cases_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-24T07:01:00-04:00",
                        "items": [
                            {"response_recommendation": {"label": "Reply now"}},
                            {"response_recommendation": {}},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            publish_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "artifact_id": "art-1",
                                "flow": "meme",
                                "execution_state": {
                                    "state": "draft",
                                    "state_source": "/Users/philtullai/ai-agents/duckAgent/runs/2026-04-24/state_meme.json",
                                },
                                "candidate_summary": {
                                    "title": "Orange Cat Duck Meme",
                                    "publish_token": "2026-04-24T07:15:00-04:00",
                                    "platform_targets": ["instagram", "facebook"],
                                    "body": "Fresh orange cat duck energy.",
                                },
                            },
                            {
                                "artifact_id": "art-2",
                                "flow": "jeepfact",
                                "execution_state": {"state": "published"},
                                "candidate_summary": {"title": "Already out"},
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            tasks_path.write_text(
                json.dumps({"items": [{"product_title": "Orange Cat Duck", "quantity": 1, "due_label": "Tonight"}]}),
                encoding="utf-8",
            )
            trends_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {"theme": "Orange pet ducks", "catalog_match": {"status": "partial"}, "signal_summary": {"trending_score": 9}},
                            {"theme": "Already covered", "catalog_match": {"status": "covered"}, "signal_summary": {"trending_score": 10}},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            receipts_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "created_timestamp": 1777032000,
                                "transactions": [{"title": "Orange Cat Duck", "quantity": 2, "listing_id": 111}],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            catalog_path.write_text(
                json.dumps(
                    {
                        "items": {
                            "111": {"status": "active", "title": "Orange Cat Duck"},
                            "222": {"status": "active", "title": "Monster Truck Duck"},
                        }
                    }
                ),
                encoding="utf-8",
            )
            usps_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-24T07:00:00+00:00",
                        "items": {
                            "9400": {"buyer_name": "Alex", "last_status_at": "2026-04-16T07:00:00+00:00"}
                        },
                    }
                ),
                encoding="utf-8",
            )
            etsy_tx_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-24T07:00:00-04:00",
                        "items": [
                            {"created_timestamp": 1777032000, "quantity": 2, "receipt_id": "r-1"}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            shopify_path.write_text(
                json.dumps(
                    {
                        "counts": {"orders": 2, "units": 5},
                        "items": [
                            {
                                "created_at": "2026-04-24T06:00:00-04:00",
                                "line_items": [{"quantity": 3}],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            rejected_path.write_text(json.dumps({"rejected": []}), encoding="utf-8")
            packets_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "short_id": "C301",
                                "priority": "high",
                                "status": "reply_needed",
                                "packet_type": "reply",
                                "next_operator_action": "Review reply",
                                "customer_summary": "Need a quick order update.",
                                "customer_name": "Alex",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(contracts, "PACKING_SUMMARY", packing_path),
                mock.patch.object(contracts, "CUSTOMER_CASES", cases_path),
                mock.patch.object(contracts, "PUBLISH_CANDIDATES", publish_path),
                mock.patch.object(contracts, "CUSTOM_BUILD_TASKS", tasks_path),
                mock.patch.object(contracts, "TREND_CANDIDATES", trends_path),
                mock.patch.object(contracts, "ETSY_RECEIPTS", receipts_path),
                mock.patch.object(contracts, "CATALOG_INDEX", catalog_path),
                mock.patch.object(contracts, "USPS_TRACKING", usps_path),
                mock.patch.object(contracts, "ETSY_TRANSACTIONS", etsy_tx_path),
                mock.patch.object(contracts, "SHOPIFY_OPEN_ORDERS", shopify_path),
                mock.patch.object(contracts, "OPERATOR_REJECTED_PATH", rejected_path),
                mock.patch.object(contracts, "CUSTOMER_ACTION_PACKETS_DIR", packets_dir),
            ):
                surface = contracts.build_compact_operator_surface()
                widget_payload = contracts.build_widget_status_payload(surface)

        self.assertEqual(surface["surface_version"], 1)
        self.assertEqual(surface["metrics"]["ducks_to_pack_today"], 3)
        self.assertEqual(surface["metrics"]["customers_to_reply"], 1)
        self.assertEqual(surface["metrics"]["pending_approvals"], 1)
        self.assertEqual(surface["metrics"]["trend_ideas"], 1)
        self.assertEqual(surface["pending_approvals"][0]["artifact_id"], "art-1")
        self.assertEqual(surface["top_tasks"][0]["id"], "C301")
        self.assertEqual(widget_payload["surfaceVersion"], surface["surface_version"])
        self.assertEqual(widget_payload["ducksToPackToday"], surface["metrics"]["ducks_to_pack_today"])
        self.assertEqual(widget_payload["customersToReply"], surface["metrics"]["customers_to_reply"])
        self.assertEqual(widget_payload["pendingApprovals"][0]["artifactId"], "art-1")
        self.assertEqual(widget_payload["topTasks"][0]["customerName"], "Alex")


if __name__ == "__main__":
    unittest.main()
