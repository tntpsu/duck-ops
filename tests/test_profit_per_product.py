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
        # Real data: each distinct product has its own product_id —
        # so the fixture mirrors production shape.
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Big Earner", product_id="p-big",
                           revenue_ex_tax=100.0, cogs_unit=10.0),
                _line_item(product_title="Small Earner", product_id="p-small",
                           revenue_ex_tax=20.0, cogs_unit=2.0),
                _line_item(product_title="Loss Maker", product_id="p-loss",
                           revenue_ex_tax=5.0, cogs_unit=10.0),
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
                _line_item(product_title="Winner", product_id="p-win",
                           revenue_ex_tax=100.0, cogs_unit=10.0),
                _line_item(product_title="Loser", product_id="p-lose",
                           revenue_ex_tax=5.0, cogs_unit=10.0),
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

    def test_fallback_label_when_no_product_id_or_title(self) -> None:
        """When product_id AND title are both missing, group key falls
        through to sku, label uses handle.

        2026-06-06 update: with the product_id-first grouping fix,
        we have to explicitly clear product_id to exercise the
        fallback chain — real orders almost always have product_id."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title=None, product_id=None,
                           product_handle="jeep-wave-duck",
                           sku="jw-1", qty=1, revenue_ex_tax=10.0, cogs_unit=1.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        # No title, no product_id → falls through to sku group with
        # handle as the display label.
        self.assertEqual(s["products"][0]["label"], "jeep-wave-duck")


class ProductIdGroupingTests(unittest.TestCase):
    """The 2026-06-06 grouping fix. Operator spotted Michigan Wolverines
    appearing as 3 rows because Shopify variant options carry different
    line-item titles ("with Team Spirit" vs "in Maize and Blue") while
    sharing the same product_id and sku. Grouping must merge them under
    one bucket so the per-product page shows one Michigan Wolverines row
    with the full revenue, not three fractional rows."""

    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.cache = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_same_product_id_different_titles_merge(self) -> None:
        """The headline test: Michigan Wolverines scenario. Same
        product_id, different titles → one merged row."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Michigan Wolverines Duck — Team Spirit",
                           product_id="8065962442935", sku="erimich",
                           qty=1, revenue_ex_tax=9.0, cogs_unit=0.75),
                _line_item(product_title="Michigan Wolverines Duck — Maize and Blue",
                           product_id="8065962442935", sku="erimich",
                           qty=1, revenue_ex_tax=9.0, cogs_unit=0.75),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(len(s["products"]), 1,
                         f"Expected one row; got {[p['label'] for p in s['products']]}")
        p = s["products"][0]
        self.assertEqual(p["units_sold"], 2)
        self.assertEqual(p["sample_product_id"], "8065962442935")
        # Both variant titles are preserved for operator transparency.
        self.assertEqual(p["title_variant_count"], 2)
        self.assertIn("Michigan Wolverines Duck — Team Spirit", p["title_variants"])
        self.assertIn("Michigan Wolverines Duck — Maize and Blue", p["title_variants"])

    def test_different_product_ids_stay_separate(self) -> None:
        """Two distinct product_ids = two distinct rows, even if titles
        are similar. Cross-platform alias merging is a separate (future)
        Surface 11.1 feature."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Michigan Wolverines Duck (Etsy)",
                           product_id="1889876564", sku="ERIMI",
                           qty=1, revenue_ex_tax=12.0, cogs_unit=0.75),
                _line_item(product_title="Michigan Wolverines Duck (Shopify)",
                           product_id="8065962442935", sku="erimich",
                           qty=1, revenue_ex_tax=9.0, cogs_unit=0.75),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(len(s["products"]), 2)

    def test_same_sku_different_product_id_stay_separate(self) -> None:
        """SKU-only collision (case-different, or platform-rebrand)
        with different product_ids must NOT merge — product_id is the
        load-bearing identifier."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Etsy Listing", product_id="etsy-1",
                           sku="ERIMI", qty=2, revenue_ex_tax=20.0, cogs_unit=1.0),
                _line_item(product_title="Shopify Listing", product_id="shopify-1",
                           sku="erimi", qty=3, revenue_ex_tax=30.0, cogs_unit=1.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(len(s["products"]), 2)

    def test_sku_grouping_when_product_id_missing(self) -> None:
        """Fall-through: no product_id but same SKU → merge by SKU.
        Survives platform sync drift where product_id wasn't populated."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Title A", product_id=None,
                           sku="JEEP-WAVE-1", qty=1, revenue_ex_tax=10.0, cogs_unit=1.0),
                _line_item(product_title="Title B", product_id=None,
                           sku="jeep-wave-1", qty=2, revenue_ex_tax=20.0, cogs_unit=1.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        # Case-folded SKU collapses "JEEP-WAVE-1" + "jeep-wave-1".
        self.assertEqual(len(s["products"]), 1)
        self.assertEqual(s["products"][0]["units_sold"], 3)

    def test_title_variants_empty_when_only_one_title_seen(self) -> None:
        """No spurious empty title in the variant set when every line
        item shares the same title."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_title="Single Title", product_id="p1",
                           qty=2, revenue_ex_tax=20.0, cogs_unit=2.0),
                _line_item(product_title="Single Title", product_id="p1",
                           qty=1, revenue_ex_tax=10.0, cogs_unit=1.0),
            ]),
        ])
        s = aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(s["products"][0]["title_variant_count"], 1)
        self.assertEqual(s["products"][0]["title_variants"], ["Single Title"])


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


class ChannelSplitTests(unittest.TestCase):
    """Surface 92: per-product profit, split by the channel that earned it.

    The producer read every cache file identically and summed Etsy and
    Shopify line items into one number, so `profit_per_product.json` could
    say WHICH duck made money but never WHERE. That is the one question the
    catalog decisions actually turn on: on 2026-10-14 the site took 25% more
    traffic than Etsy and returned 39% of the net profit, and nothing in this
    payload could show it.

    The channel is read from the ORDER's `platform` field, never from the
    file name -- `<date>_shopify_orders.json` holds Etsy orders too (181 of
    273 rows in the 28 days to 2026-10-14), so a filename-keyed split credits
    Etsy's money to Shopify. The disagreement is counted rather than
    smoothed over, because a silent reconciliation is how it stayed invisible
    ([[feedback_swallowed_errors_lie]]).
    """

    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.cache = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def _run(self):
        return aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )

    def test_one_product_two_channels_splits(self) -> None:
        """The same duck sold on both channels keeps one row and gains a
        per-channel breakdown underneath it."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=2, revenue_ex_tax=20.0, cogs_unit=1.0)],
                   order_id="shop-1") | {"platform": "shopify"},
        ], platform="shopify")
        _write_cache(self.cache, "2026-06-04", [
            _order([_line_item(qty=3, revenue_ex_tax=45.0, cogs_unit=1.0)],
                   order_id="etsy-1") | {"platform": "etsy"},
        ], platform="etsy")
        s = self._run()
        self.assertEqual(len(s["products"]), 1)
        row = s["products"][0]
        self.assertEqual(row["units_sold"], 5)
        self.assertEqual(sorted(row["by_channel"]), ["etsy", "shopify"])
        self.assertEqual(row["channel_count"], 2)
        self.assertEqual(row["by_channel"]["shopify"]["units"], 2)
        self.assertEqual(row["by_channel"]["shopify"]["revenue_total"], 20.0)
        self.assertEqual(row["by_channel"]["etsy"]["units"], 3)
        self.assertEqual(row["by_channel"]["etsy"]["revenue_total"], 45.0)

    def test_channel_revenue_sums_to_the_row_total(self) -> None:
        """The split is a partition, not an estimate: every channel slice
        adds back up to the row the operator already trusts."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=12.0, cogs_unit=1.0)],
                   order_id="s1") | {"platform": "shopify"},
            _order([_line_item(qty=1, revenue_ex_tax=15.0, cogs_unit=1.0)],
                   order_id="e1") | {"platform": "etsy"},
            _order([_line_item(qty=4, revenue_ex_tax=40.0, cogs_unit=1.0)],
                   order_id="e2") | {"platform": "etsy"},
        ])
        row = self._run()["products"][0]
        for field_name, total_name in (("revenue_total", "revenue_total"),
                                       ("net_profit_total", "net_profit_total"),
                                       ("cogs_total", "cogs_total")):
            split = sum(c[field_name] for c in row["by_channel"].values())
            self.assertAlmostEqual(split, row[total_name], places=2,
                                   msg=f"{field_name} does not partition")
        self.assertEqual(sum(c["units"] for c in row["by_channel"].values()),
                         row["units_sold"])

    def test_top_level_channels_sum_to_top_level_totals(self) -> None:
        """Same invariant one level up, so the two headline numbers can
        never disagree on the same surface."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(product_id="p1", qty=1, revenue_ex_tax=10.0)],
                   order_id="s1") | {"platform": "shopify"},
            _order([_line_item(product_id="p2", qty=2, revenue_ex_tax=30.0)],
                   order_id="e1") | {"platform": "etsy"},
        ])
        s = self._run()
        ch = s["channels"]
        self.assertAlmostEqual(sum(c["revenue"] for c in ch.values()),
                               s["totals"]["revenue"], places=2)
        self.assertAlmostEqual(sum(c["net_profit"] for c in ch.values()),
                               s["totals"]["net_profit"], places=2)
        self.assertEqual(sum(c["units"] for c in ch.values()),
                         s["totals"]["units"])
        self.assertEqual(ch["shopify"]["products"], 1)
        self.assertEqual(ch["etsy"]["products"], 1)

    def test_the_platform_field_beats_the_file_name(self) -> None:
        """THE defect this ships for. An Etsy order sitting in a
        shopify-named file is Etsy revenue; the file name is not evidence."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=9.0, cogs_unit=1.0)],
                   order_id="etsy-in-the-wrong-file") | {"platform": "etsy"},
        ], platform="shopify")
        s = self._run()
        row = s["products"][0]
        self.assertEqual(list(row["by_channel"]), ["etsy"])
        self.assertEqual(row["by_channel"]["etsy"]["revenue_total"], 9.0)
        self.assertNotIn("shopify", s["channels"])

    def test_the_filename_disagreement_is_counted_not_hidden(self) -> None:
        """Reading the field quietly fixes the number and loses the signal.
        The count is what tells the operator the writer is still wrong."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item()], order_id="e1") | {"platform": "etsy"},
            _order([_line_item()], order_id="e2") | {"platform": "etsy"},
            _order([_line_item()], order_id="s1") | {"platform": "shopify"},
        ], platform="shopify")
        dq = self._run()["data_quality"]
        self.assertEqual(dq["platform_filename_mismatch"], 2)
        self.assertEqual(dq["orders_without_platform"], 0)

    def test_a_missing_platform_falls_back_to_the_file_name(self) -> None:
        """Old cache files predate the field. Falling back keeps their
        revenue in the right channel, and the counter says it was a guess."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=10.0)], order_id="old-1"),
        ], platform="etsy")
        s = self._run()
        row = s["products"][0]
        self.assertEqual(list(row["by_channel"]), ["etsy"])
        self.assertEqual(s["data_quality"]["orders_without_platform"], 1)
        self.assertEqual(s["data_quality"]["platform_filename_mismatch"], 0)

    def test_an_unreadable_file_name_lands_in_unknown(self) -> None:
        """Fail visible, not closed: unattributable money is its own
        channel rather than being folded into a real one."""
        self.cache.mkdir(parents=True, exist_ok=True)
        (self.cache / "2026-06-05_orders.json").write_text(
            json.dumps([_order([_line_item(qty=1, revenue_ex_tax=10.0)])]),
            encoding="utf-8")
        s = self._run()
        self.assertEqual(list(s["products"][0]["by_channel"]), ["unknown"])
        self.assertIn("unknown", s["channels"])

    def test_per_channel_margin_is_computed_from_that_channel(self) -> None:
        """A channel's margin is its own net over its own revenue -- not the
        row's margin copied down, which would make a thin channel look fat."""
        _write_cache(self.cache, "2026-06-05", [
            # Shopify: 10.0 revenue, 9.0 net -> 90%
            _order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0)],
                   order_id="s1") | {"platform": "shopify"},
            # Etsy: 10.0 revenue, 2.0 net -> 20%
            _order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0,
                               net_profit=2.0)],
                   order_id="e1") | {"platform": "etsy"},
        ])
        row = self._run()["products"][0]
        self.assertEqual(row["by_channel"]["shopify"]["margin_pct"], 90.0)
        self.assertEqual(row["by_channel"]["etsy"]["margin_pct"], 20.0)
        self.assertEqual(row["margin_pct"], 55.0)

    def test_primary_channel_follows_profit_not_revenue(self) -> None:
        """"Where does this duck belong" is a profit question. Etsy can take
        more revenue and still be the worse home for it."""
        _write_cache(self.cache, "2026-06-05", [
            # Etsy: more revenue, less profit.
            _order([_line_item(qty=1, revenue_ex_tax=40.0, cogs_unit=1.0,
                               net_profit=5.0)],
                   order_id="e1") | {"platform": "etsy"},
            _order([_line_item(qty=1, revenue_ex_tax=20.0, cogs_unit=1.0,
                               net_profit=18.0)],
                   order_id="s1") | {"platform": "shopify"},
        ])
        row = self._run()["products"][0]
        self.assertEqual(row["by_channel"]["etsy"]["revenue_total"], 40.0)
        self.assertEqual(row["primary_channel"], "shopify")

    def test_single_channel_product_says_so(self) -> None:
        """An Etsy-only duck is the actionable case -- it is the candidate
        for the site, and vice versa."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=2, revenue_ex_tax=20.0)], order_id="e1")
            | {"platform": "etsy"},
        ], platform="etsy")
        row = self._run()["products"][0]
        self.assertEqual(row["channel_count"], 1)
        self.assertEqual(row["primary_channel"], "etsy")

    def test_distinct_orders_are_counted_separately_from_line_items(self) -> None:
        """order_count has always counted LINE ITEMS; two variants of one
        duck in one order read as two orders. The number stays put so no
        surface shifts under the operator, and the honest count arrives
        beside it under its own name ([[feedback_status_fields_one_meaning]]).
        """
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(sku="a", qty=1, revenue_ex_tax=10.0),
                _line_item(sku="b", qty=1, revenue_ex_tax=10.0),
            ], order_id="one-order") | {"platform": "etsy"},
        ])
        row = self._run()["products"][0]
        self.assertEqual(row["order_count"], 2)
        self.assertEqual(row["order_count_distinct"], 1)
        self.assertEqual(row["by_channel"]["etsy"]["orders"], 1)
        self.assertEqual(row["by_channel"]["etsy"]["line_items"], 2)

    def test_an_order_spanning_products_is_not_double_counted(self) -> None:
        """Channel-level order counts dedupe across products, or a 3-duck
        order would read as three Etsy orders."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_id="p1", qty=1),
                _line_item(product_id="p2", qty=1),
                _line_item(product_id="p3", qty=1),
            ], order_id="basket") | {"platform": "etsy"},
        ])
        s = self._run()
        self.assertEqual(len(s["products"]), 3)
        self.assertEqual(s["channels"]["etsy"]["orders"], 1)
        self.assertEqual(s["channels"]["etsy"]["line_items"], 3)

    def test_empty_cache_still_has_the_channel_keys(self) -> None:
        """A reader that only sometimes finds the key is a reader that
        crashes on a quiet week."""
        self.cache.mkdir(parents=True, exist_ok=True)
        s = self._run()
        self.assertEqual(s["channels"], {})
        self.assertEqual(s["data_quality"]["platform_filename_mismatch"], 0)
        self.assertEqual(s["data_quality"]["orders_without_platform"], 0)

    def test_channel_names_are_normalised(self) -> None:
        """"Etsy", "ETSY " and "etsy" are one channel, or the split silently
        fragments into near-duplicate columns."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=10.0)], order_id="a")
            | {"platform": "Etsy"},
            _order([_line_item(qty=1, revenue_ex_tax=10.0)], order_id="b")
            | {"platform": "ETSY "},
        ], platform="etsy")
        row = self._run()["products"][0]
        self.assertEqual(list(row["by_channel"]), ["etsy"])
        self.assertEqual(row["by_channel"]["etsy"]["units"], 2)


class OrderCostAllocationTests(unittest.TestCase):
    """Surface 92b: push the order-level costs down onto the line items.

    Every line item carries only `revenue_ex_tax - cogs`, so the per-product
    numbers never saw a platform fee, a payment fee or a shipping label. That
    was survivable while the payload was channel-blind -- and became the whole
    problem the moment it was not, because the costs it drops are exactly the
    ones that DIFFER between channels. On the 28 days to 2026-10-14 the two
    channels looked 92.7% vs 91.3% margin on this basis, while their real net
    per order was $12.61 against $9.59: Etsy pays fees, and Shopify collects
    $4.50 of shipping against a $4.90 label.

    The identity being relied on, verified against the live cache:
      order.net_profit == sum(line.gross_profit) - order_costs
      order_costs == platform_fees + payment_fees + ads_fees
                     + shipping_label_cost - shipping_collected
    Allocation is pro-rata on line revenue. The existing net_profit_total is
    deliberately left where it is; the cost-bearing number arrives under its
    own name ([[feedback_status_fields_one_meaning]]).
    """

    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.cache = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def _run(self):
        return aggregate_per_product(
            cache_dir=self.cache,
            now_iso="2026-06-06T18:00:00-04:00",
        )

    def test_costs_split_pro_rata_on_revenue(self) -> None:
        """A $4 fee on a basket that is 75% one duck lands 75% on that duck."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_id="p1", qty=1, revenue_ex_tax=30.0, cogs_unit=0.0),
                _line_item(product_id="p2", qty=1, revenue_ex_tax=10.0, cogs_unit=0.0),
            ], order_id="o1") | {"platform": "etsy", "payment_fees": 4.0,
                                 "shipping_label_cost": 0.0,
                                 "shipping_collected": 0.0},
        ])
        rows = {r["sample_product_id"]: r for r in self._run()["products"]}
        self.assertEqual(rows["p1"]["order_costs_allocated"], 3.0)
        self.assertEqual(rows["p2"]["order_costs_allocated"], 1.0)
        self.assertEqual(rows["p1"]["net_after_order_costs"], 27.0)
        self.assertEqual(rows["p2"]["net_after_order_costs"], 9.0)

    def test_allocation_reproduces_the_order_net(self) -> None:
        """The allocation is a redistribution, not a new estimate: summed
        back up it has to equal the net the order itself recorded."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_id="p1", qty=1, revenue_ex_tax=7.5, cogs_unit=0.75),
            ], order_id="o1") | {"platform": "shopify", "platform_fees": 0.0,
                                 "payment_fees": 0.648, "ads_fees": 0.0,
                                 "shipping_label_cost": 4.9,
                                 "shipping_collected": 4.5,
                                 "net_profit": 5.702},
        ])
        s = self._run()
        self.assertAlmostEqual(s["products"][0]["net_after_order_costs"], 5.702, places=2)
        self.assertAlmostEqual(s["totals"]["net_after_order_costs"], 5.702, places=2)

    def test_shipping_overcollected_is_a_negative_cost(self) -> None:
        """Charging more for shipping than the label costs is profit, and
        must not be clamped to zero."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=0.0)],
                   order_id="o1") | {"platform": "etsy",
                                     "shipping_collected": 6.0,
                                     "shipping_label_cost": 4.0},
        ])
        row = self._run()["products"][0]
        self.assertEqual(row["order_costs_allocated"], -2.0)
        self.assertEqual(row["net_after_order_costs"], 12.0)

    def test_a_revenueless_order_splits_its_costs_evenly(self) -> None:
        """A fully discounted or zero-revenue basket has no revenue to
        weight by; dividing by it would drop the cost on the floor."""
        _write_cache(self.cache, "2026-06-05", [
            _order([
                _line_item(product_id="p1", qty=1, revenue_ex_tax=0.0, cogs_unit=0.0),
                _line_item(product_id="p2", qty=1, revenue_ex_tax=0.0, cogs_unit=0.0),
            ], order_id="o1") | {"platform": "etsy", "payment_fees": 3.0,
                                 "shipping_label_cost": 0.0,
                                 "shipping_collected": 0.0},
        ])
        rows = {r["sample_product_id"]: r for r in self._run()["products"]}
        self.assertEqual(rows["p1"]["order_costs_allocated"], 1.5)
        self.assertEqual(rows["p2"]["order_costs_allocated"], 1.5)

    def test_fees_show_up_as_a_channel_difference(self) -> None:
        """The point of the exercise: two channels with identical gross
        margin must stop looking identical once their own costs land."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(product_id="p1", qty=1, revenue_ex_tax=10.0,
                               cogs_unit=0.0)],
                   order_id="e1") | {"platform": "etsy", "payment_fees": 2.0},
            _order([_line_item(product_id="p1", qty=1, revenue_ex_tax=10.0,
                               cogs_unit=0.0)],
                   order_id="s1") | {"platform": "shopify", "payment_fees": 0.0},
        ])
        s = self._run()
        row = s["products"][0]
        self.assertEqual(row["by_channel"]["etsy"]["margin_pct"],
                         row["by_channel"]["shopify"]["margin_pct"])
        self.assertEqual(row["by_channel"]["etsy"]["net_after_order_costs"], 8.0)
        self.assertEqual(row["by_channel"]["shopify"]["net_after_order_costs"], 10.0)
        self.assertEqual(s["channels"]["etsy"]["margin_after_costs_pct"], 80.0)
        self.assertEqual(s["channels"]["shopify"]["margin_after_costs_pct"], 100.0)

    def test_the_existing_net_is_left_alone(self) -> None:
        """Regression guard. Allocating costs must not move a number the
        profit page already renders."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0)],
                   order_id="o1") | {"platform": "etsy", "payment_fees": 2.0},
        ])
        s = self._run()
        row = s["products"][0]
        self.assertEqual(row["net_profit_total"], 9.0)
        self.assertEqual(row["margin_pct"], 90.0)
        self.assertEqual(s["totals"]["net_profit"], 9.0)
        self.assertEqual(row["net_after_order_costs"], 7.0)

    def test_an_order_with_no_cost_fields_allocates_nothing(self) -> None:
        """Old cache rows carry no fee fields; absent must mean zero, not
        a crash and not a guess."""
        _write_cache(self.cache, "2026-06-05", [
            _order([_line_item(qty=1, revenue_ex_tax=10.0, cogs_unit=1.0)],
                   order_id="o1") | {"platform": "etsy"},
        ])
        row = self._run()["products"][0]
        self.assertEqual(row["order_costs_allocated"], 0.0)
        self.assertEqual(row["net_after_order_costs"], row["net_profit_total"])


if __name__ == "__main__":
    unittest.main()
