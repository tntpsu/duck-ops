"""Surface 11 producer contract tests.

Pin aggregate_per_product behavior so the /portal/intel/profit
"By product" drill-down reads a stable shape.
"""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import profit_per_product
from profit_per_product import (  # noqa: E402
    MIN_UNITS_FOR_CONFIDENT_MARGIN,
    aggregate_per_product,
)


def _line_item(*, product_title: str = "Test Duck",
               sku: str = "test-1",
               qty: int = 1,
               revenue_ex_tax: float = 10.0,
               cogs_unit: float = 1.0,
               cogs_total: float | None = None,
               net_profit: float | None = None,
               gross_profit: float | None = None,
               product_id: str = "p1",
               product_handle: str | None = None,
               **extra) -> dict:
    cogs_t = cogs_total if cogs_total is not None else (cogs_unit * qty)
    np = net_profit if net_profit is not None else (revenue_ex_tax - cogs_t)
    gp = gross_profit if gross_profit is not None else np
    return {
        "product_title": product_title,
        "product_id": product_id,
        "product_handle": product_handle,
        "sku": sku,
        "qty": qty,
        "unit_price": revenue_ex_tax / max(qty, 1),
        "discount_alloc": 0.0,
        "revenue_ex_tax": revenue_ex_tax,
        "cogs_unit": cogs_unit,
        "cogs_total": cogs_t,
        "gross_profit": gp,
        "net_profit": np,
        **extra,
    }


def _order(line_items: list[dict], *, order_id: str = "o1") -> dict:
    return {"order_id": order_id, "line_items": line_items}


def _write_cache(cache_dir: Path, date_str: str, orders: list[dict], *, platform: str = "shopify") -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{date_str}_{platform}_orders.json"
    path.write_text(json.dumps(orders), encoding="utf-8")


class HappyPathAggregationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.cache = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_two_orders_same_product_aggregate(self) -> None:
        """Same product across two orders → one row with combined units."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=2, revenue_ex_tax=20.0, cogs_unit=2.0)]),
        ])
        _write_cache(self.cache, "2026-06-04", [
            _order([_line_item(qty=3, revenue_ex_tax=30.0, cogs_unit=2.0)],
                   order_id="o2"),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(len(s["products"]), 1)
        p = s["products"][0]
        self.assertEqual(p["label"], "Test Duck")
        self.assertEqual(p["units_sold"], 5)
        self.assertEqual(p["order_count"], 2)
        self.assertEqual(p["revenue_total"], 50.0)
        self.assertEqual(p["cogs_total"], 10.0)
        self.assertEqual(p["net_profit_total"], 40.0)
        self.assertEqual(p["margin_pct"], 80.0)
        self.assertTrue(p["is_confident_margin"])

    def test_different_skus_same_title_roll_up(self) -> None:
        """Variants (different SKUs, same product_title) roll up together
        — operator thinks in 'ducks,' not in SKUs."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Jeep Wave Duck", sku="jw-small", qty=2,
                           revenue_ex_tax=20.0, cogs_unit=2.0),
                _line_item(product_title="Jeep Wave Duck", sku="jw-large", qty=1,
                           revenue_ex_tax=15.0, cogs_unit=3.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(len(s["products"]), 1)
        p = s["products"][0]
        self.assertEqual(p["units_sold"], 3)
        self.assertEqual(p["distinct_sku_count"], 2)
        self.assertIn("jw-small", p["distinct_skus"])
        self.assertIn("jw-large", p["distinct_skus"])

    def test_sort_by_net_profit_descending(self) -> None:
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Big Earner", revenue_ex_tax=100.0, cogs_unit=10.0),
                _line_item(product_title="Small Earner", revenue_ex_tax=20.0, cogs_unit=2.0),
                _line_item(product_title="Loss Maker", revenue_ex_tax=5.0, cogs_unit=10.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        labels = [p["label"] for p in s["products"]]
        self.assertEqual(labels, ["Big Earner", "Small Earner", "Loss Maker"])

    def test_loss_makers_isolated(self) -> None:
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Winner", revenue_ex_tax=100.0, cogs_unit=10.0),
                _line_item(product_title="Loser", revenue_ex_tax=5.0, cogs_unit=10.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(len(s["loss_makers"]), 1)
        self.assertEqual(s["loss_makers"][0]["label"], "Loser")

    def test_low_n_products_flagged_not_confident(self) -> None:
        """Products with fewer than MIN_UNITS_FOR_CONFIDENT_MARGIN units
        get is_confident_margin=False so the page can de-emphasize them
        and the operator doesn't retire a duck on n=1 data."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0)]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertFalse(s["products"][0]["is_confident_margin"])

    def test_low_margin_excludes_low_n(self) -> None:
        """Low-margin watch list filters to confident-only — otherwise
        every one-off sale shows up."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                # Confident, 20% margin → makes the list.
                _line_item(product_title="Confident", qty=5,
                           revenue_ex_tax=50.0, cogs_unit=8.0),
                # Low-n, 10% margin → does NOT make the list.
                _line_item(product_title="One-off", qty=1,
                           revenue_ex_tax=10.0, cogs_unit=9.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        labels = [p["label"] for p in s["low_margin"]]
        self.assertIn("Confident", labels)
        self.assertNotIn("One-off", labels)

    def test_orders_dict_wrapper_format(self) -> None:
        """Cache files may be {"orders": [...]} or [...] directly. Both must work."""
        wrapped = {"orders": [_order([_line_item(qty=2, revenue_ex_tax=20.0, cogs_unit=2.0)])]}
        cache_path = self.cache / "2026-06-05_etsy_orders.json"
        self.cache.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(wrapped))
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(s["products"][0]["units_sold"], 2)

    def test_fallback_label_when_no_product_title(self) -> None:
        """Missing product_title falls through to handle, then sku."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title=None, product_handle="jeep-wave-duck",
                           sku="jw-1", qty=1, revenue_ex_tax=10.0, cogs_unit=1.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        # Label uses handle form when title is missing.
        self.assertEqual(s["products"][0]["label"], "(jeep-wave-duck)")


class WindowAndMalformedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.cache = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_empty_cache_returns_zero_totals(self) -> None:
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(s["totals"]["units"], 0)
        self.assertEqual(s["products"], [])
        self.assertEqual(s["loss_makers"], [])

    def test_missing_cache_dir_returns_zero_totals(self) -> None:
        s = aggregate_per_product(
            cache_dir=self.cache / "does_not_exist",
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(s["totals"]["products"], 0)

    def test_window_excludes_old_files(self) -> None:
        """Files outside the window must not be aggregated."""
        now_dt = datetime(2026, 6, 6, 18, 0, tzinfo=timezone(timedelta(hours=-4)))
        # Recent — included.
        _write_cache(self.cache, (now_dt - timedelta(days=5)).date().isoformat(),
                     [_order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0)])])
        # Old — excluded by 30d window.
        _write_cache(self.cache, (now_dt - timedelta(days=60)).date().isoformat(),
                     [_order([_line_item(qty=100, revenue_ex_tax=1000.0, cogs_unit=10.0,
                                          product_title="Should Not Appear")],
                             order_id="ancient")])
        s = aggregate_per_product(
            cache_dir=self.cache, window_days=30,
            now_iso=now_dt.isoformat(),
        )
        labels = [p["label"] for p in s["products"]]
        self.assertNotIn("Should Not Appear", labels)
        self.assertEqual(s["totals"]["units"], 1)

    def test_malformed_file_counted_not_crashed(self) -> None:
        self.cache.mkdir(parents=True, exist_ok=True)
        (self.cache / "2026-06-05_shopify_orders.json").write_text("not valid json")
        # And a valid file alongside.
        _write_cache(self.cache, "2026-06-04",
                     [_order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0)])])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(s["data_quality"]["malformed_files"], 1)
        self.assertEqual(s["totals"]["units"], 1)

    def test_unparseable_filename_skipped(self) -> None:
        """Filename without YYYY-MM-DD prefix → skipped silently."""
        self.cache.mkdir(parents=True, exist_ok=True)
        (self.cache / "garbage.json").write_text("[]")
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(s["file_count"], 0)

    def test_data_quality_flags_missing_revenue_and_cogs(self) -> None:
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(revenue_ex_tax=0, cogs_unit=0),  # both missing
                _line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0),  # both fine
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertGreaterEqual(s["data_quality"]["line_items_without_revenue"], 1)
        self.assertGreaterEqual(s["data_quality"]["line_items_without_cogs"], 1)


if __name__ == "__main__":
    unittest.main()
