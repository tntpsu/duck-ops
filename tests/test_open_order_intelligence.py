from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import open_order_intelligence as oo
from open_order_intelligence import _shopify_buyer_name


def test_shopify_buyer_name_prefers_customer_name():
    order = {
        "customer": {"first_name": "Jamie", "last_name": "Smith"},
        "email": "fallback@example.com",
    }
    assert _shopify_buyer_name(order) == "Jamie Smith"


def test_shopify_buyer_name_falls_back_to_email_prefix():
    order = {
        "customer": {},
        "shipping_address": {},
        "billing_address": {},
        "email": "duck.fan_47@example.com",
    }
    assert _shopify_buyer_name(order) == "Duck Fan 47"


def test_build_packing_summary_surfaces_options_and_shopify_hidden_buyers():
    with TemporaryDirectory() as tmp:
        original_path = oo.PACKING_SUMMARY_PATH
        oo.PACKING_SUMMARY_PATH = Path(tmp) / "packing_summary.json"
        try:
            payload = oo.build_packing_summary(
                etsy_open_orders={
                    "items": [
                        {
                            "buyer_name": "Holli Sandberg",
                            "created_at": "2026-04-09T20:33:16-04:00",
                            "expected_ship_date": None,
                            "line_items": [
                                {
                                    "channel": "etsy",
                                    "order_ref": "4027700359",
                                    "product_title": "Duckzilla Monster Duck",
                                    "product_id": "1860293715",
                                    "quantity": 1,
                                    "is_custom": False,
                                    "variant_title": None,
                                    "variation_pairs": [{"name": "Color", "value": "Pink"}],
                                },
                                {
                                    "channel": "etsy",
                                    "order_ref": "4027700359",
                                    "product_title": "Duckzilla Monster Duck",
                                    "product_id": "1860293715",
                                    "quantity": 1,
                                    "is_custom": False,
                                    "variant_title": None,
                                    "variation_pairs": [{"name": "Color", "value": "Blue"}],
                                },
                            ],
                        }
                    ]
                },
                shopify_open_orders={
                    "items": [
                        {
                            "buyer_name": None,
                            "created_at": "2026-04-12T11:40:23-04:00",
                            "expected_ship_date": None,
                            "line_items": [
                                {
                                    "channel": "shopify",
                                    "order_ref": "#3348",
                                    "product_title": "MLB Baseball Ducks",
                                    "product_id": "8028036858039",
                                    "quantity": 1,
                                    "is_custom": False,
                                    "variant_title": "NYM",
                                }
                            ],
                        }
                    ]
                },
            )
        finally:
            oo.PACKING_SUMMARY_PATH = original_path

    orders = payload.get("orders_to_pack") or []
    duckzilla = next(item for item in orders if item.get("product_title") == "Duckzilla Monster Duck")
    baseball = next(item for item in orders if item.get("product_title") == "MLB Baseball Ducks")

    assert duckzilla.get("option_summary") == "Color: Blue, Color: Pink"
    assert baseball.get("option_summary") == "NYM"
    assert baseball.get("buyer_count_display") == "Hidden by Shopify"


def test_refresh_order_snapshots_uses_cached_fallback_for_failed_source():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        original_etsy_path = oo.ETSY_OPEN_ORDERS_PATH
        original_shopify_path = oo.SHOPIFY_OPEN_ORDERS_PATH
        original_packing_path = oo.PACKING_SUMMARY_PATH
        original_refresh_state_path = oo.ORDER_SNAPSHOT_REFRESH_STATE_PATH
        original_runner = oo._run_open_order_subcommand
        original_record_transition = oo.record_workflow_transition
        original_write_receipt = oo.write_workflow_receipt

        oo.ETSY_OPEN_ORDERS_PATH = tmp_path / "etsy_open_orders_snapshot.json"
        oo.SHOPIFY_OPEN_ORDERS_PATH = tmp_path / "shopify_open_orders_snapshot.json"
        oo.PACKING_SUMMARY_PATH = tmp_path / "packing_summary.json"
        oo.ORDER_SNAPSHOT_REFRESH_STATE_PATH = tmp_path / "order_snapshot_refresh.json"

        cached_shopify = {
            "generated_at": "2026-04-12T23:04:09-04:00",
            "counts": {"orders": 1, "units": 1},
            "items": [
                {
                    "buyer_name": None,
                    "created_at": "2026-04-12T11:40:23-04:00",
                    "line_items": [
                        {
                            "channel": "shopify",
                            "order_ref": "#3348",
                            "product_title": "MLB Baseball Ducks",
                            "product_id": "8028036858039",
                            "quantity": 1,
                            "is_custom": False,
                            "variant_title": "NYM",
                        }
                    ],
                }
            ],
        }
        oo.SHOPIFY_OPEN_ORDERS_PATH.write_text(json.dumps(cached_shopify), encoding="utf-8")

        live_etsy = {
            "generated_at": "2026-04-13T00:07:00-04:00",
            "counts": {"orders": 1, "units": 2},
            "items": [
                {
                    "buyer_name": "Holli Sandberg",
                    "created_at": "2026-04-09T20:33:16-04:00",
                    "expected_ship_date": None,
                    "line_items": [
                        {
                            "channel": "etsy",
                            "order_ref": "4027700359",
                            "product_title": "Duckzilla Monster Duck",
                            "product_id": "1860293715",
                            "quantity": 1,
                            "is_custom": False,
                            "variant_title": None,
                            "variation_pairs": [{"name": "Color", "value": "Pink"}],
                        },
                        {
                            "channel": "etsy",
                            "order_ref": "4027700359",
                            "product_title": "Duckzilla Monster Duck",
                            "product_id": "1860293715",
                            "quantity": 1,
                            "is_custom": False,
                            "variant_title": None,
                            "variation_pairs": [{"name": "Color", "value": "Blue"}],
                        },
                    ],
                }
            ],
        }

        def fake_runner(command: str, *, timeout_seconds: int) -> dict:
            if command == "etsy-open-orders":
                return live_etsy
            if command == "shopify-open-orders":
                raise RuntimeError("shopify api timeout")
            raise AssertionError(command)

        def fake_transition(**kwargs):
            return {
                "state": kwargs["state"],
                "state_reason": kwargs["state_reason"],
                "updated_at": "2026-04-13T00:07:43-04:00",
                "next_action": kwargs["next_action"],
            }

        oo._run_open_order_subcommand = fake_runner
        oo.record_workflow_transition = fake_transition
        oo.write_workflow_receipt = lambda *args, **kwargs: {"ok": True}
        try:
            payload = oo.refresh_order_snapshots()
        finally:
            oo.ETSY_OPEN_ORDERS_PATH = original_etsy_path
            oo.SHOPIFY_OPEN_ORDERS_PATH = original_shopify_path
            oo.PACKING_SUMMARY_PATH = original_packing_path
            oo.ORDER_SNAPSHOT_REFRESH_STATE_PATH = original_refresh_state_path
            oo._run_open_order_subcommand = original_runner
            oo.record_workflow_transition = original_record_transition
            oo.write_workflow_receipt = original_write_receipt

    refresh_state = payload["refresh_state"]
    assert refresh_state["sources"]["etsy"]["status"] == "live"
    assert refresh_state["sources"]["shopify"]["status"] == "fallback_cached"
    assert payload["packing_summary"]["snapshot_refresh"]["sources"]["shopify"]["status"] == "fallback_cached"


def test_etsy_timestamp_bounds_use_oldest_open_receipt_with_buffer():
    min_created, max_created = oo._etsy_timestamp_bounds(
        [
            {"created_timestamp": 1712700000},
            {"created_timestamp": 1712786400},
        ]
    )

    assert min_created is not None
    assert max_created is not None
    assert "T" in min_created
    assert "T" in max_created


def test_build_etsy_open_orders_snapshot_filters_transaction_lookup_to_open_window():
    original_receipts_loader = oo.load_recent_etsy_receipts_snapshot
    original_tx_loader = oo.load_etsy_transaction_details
    original_writer = oo._write_json
    captured: dict[str, object] = {}

    def fake_receipts_loader(days_back: int, max_age_hours: int) -> dict:
        captured["days_back"] = days_back
        return {
            "generated_at": "2026-04-13T10:27:44-04:00",
            "items": [
                {
                    "receipt_id": "4026157289",
                    "buyer_name": "Kelly Lefever",
                    "is_paid": True,
                    "is_shipped": False,
                    "created_timestamp": 1712692800,
                    "transactions": [
                        {
                            "transaction_id": "5020540836",
                            "listing_id": "1762536946",
                            "title": "Build Your Custom 3D Printed Duck!",
                            "quantity": 1,
                        }
                    ],
                }
            ],
        }

    def fake_tx_loader(transaction_ids, *, min_created=None, max_created=None):
        captured["transaction_ids"] = transaction_ids
        captured["min_created"] = min_created
        captured["max_created"] = max_created
        return {
            "generated_at": "2026-04-13T10:29:48-04:00",
            "items": [],
        }

    oo.load_recent_etsy_receipts_snapshot = fake_receipts_loader
    oo.load_etsy_transaction_details = fake_tx_loader
    oo._write_json = lambda *args, **kwargs: None
    try:
        payload = oo.build_etsy_open_orders_snapshot()
    finally:
        oo.load_recent_etsy_receipts_snapshot = original_receipts_loader
        oo.load_etsy_transaction_details = original_tx_loader
        oo._write_json = original_writer

    assert captured["days_back"] == oo.ETSY_OPEN_ORDER_LOOKBACK_DAYS
    assert captured["transaction_ids"] == ["5020540836"]
    assert captured["min_created"] is not None
    assert captured["max_created"] is not None
    assert payload["counts"]["orders"] == 1
