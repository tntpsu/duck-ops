"""Demand-intel bucketing + fusion (Demand page, 2026-07-03). Calibrated to real
data: views are sparse (only ~1/5 of catalog), sales are the rich signal, so
'no sale this week' must NOT read as fading."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runtime"))
import demand_intel as di  # noqa: E402


def _rec(funnel, trend="flat", verdict=None, offseason=False):
    return {"funnel": funnel, "trend_arrow": trend, "ga4_verdict": verdict,
            "occasion": {"is_seasonal_offseason": offseason}}


CFG = di.load_config()


def test_winner_on_sales_even_without_views():
    b = di.classify_bucket(_rec({"buys_30d": 10}), CFG)
    assert b["bucket"] == "winner" and b["action_target"] == "sale_steering.exclude"


def test_sale_when_engaged_but_not_buying():
    b = di.classify_bucket(_rec({"views_7d": 25, "engagement_rate": 0.7, "buys_7d": 0, "buys_30d": 1}), CFG)
    assert b["bucket"] == "sale" and b["action_target"] == "sale_steering.prioritize"


def test_refresh_when_traffic_bounces():
    b = di.classify_bucket(_rec({"views_7d": 25, "engagement_rate": 0.2, "buys_7d": 0, "buys_30d": 1}), CFG)
    assert b["bucket"] == "refresh" and b["action_target"] == "seo_review.refresh_request"


def test_low_signal_not_fading_when_no_traffic():
    # sells occasionally, no live views -> steady long-tail, NOT fading
    b = di.classify_bucket(_rec({"views_7d": 3, "buys_7d": None, "buys_30d": 2}), CFG)
    assert b["bucket"] == "low_signal"


def test_fading_requires_view_based_decline():
    b = di.classify_bucket(_rec({"views_7d": 18, "engagement_rate": 0.5, "buys_7d": 0, "buys_30d": 3}, trend="down"), CFG)
    assert b["bucket"] == "fading"


def test_seasonal_dormant_preempts_fading():
    b = di.classify_bucket(_rec({"views_7d": 18, "engagement_rate": 0.5, "buys_7d": 0, "buys_30d": 3}, trend="down", offseason=True), CFG)
    assert b["bucket"] == "seasonal_dormant"


class _FakeCtx:
    def __init__(self, sig): self._sig = sig
    def listing_signal(self, title): return self._sig


def test_build_fuses_by_id_and_flags_coverage():
    catalog = {"items": {"8033147617463": {"id": 8033147617463, "handle": "h", "title": "Astronaut Duck",
                               "image_src": "x", "theme_classification": {"occasions": []}}}}
    profit = {"products": [{"label": "Astronaut Duck", "sample_product_id": "8033147617463",
                            "units_sold": 1, "margin_pct": 40, "is_confident_margin": True, "distinct_skus": ["astro-1"]}]}
    ctx = _FakeCtx({"views_by_window": {"7": 25, "28": 50}, "engagement_rate": 0.2, "verdict": "fix", "matched_title": "Astronaut Duck - Etsy"})
    p = di.build_demand_intel(catalog=catalog, ctx=ctx, profit=profit,
                             tx_snapshot={"items": []}, occasion={"active_occasions": []}, now_epoch=0)
    d = p["ducks"][0]
    assert d["funnel"]["views_7d"] == 25 and d["funnel"]["buys_30d"] == 1  # id-matched
    assert d["coverage"]["has_ga4_views"] is True
    assert d["coverage"]["etsy_click_data"] is False and d["coverage"]["has_favorites"] is False
    assert d["bucket"] == "refresh"


def test_buys_join_by_id_not_title():
    # REGRESSION (Boxer/Doberman inherited the Dachshund's 22 buys via boilerplate
    # title overlap, 2026-07-03). Sales attach ONLY to the id that made them.
    catalog = {"items": {
        "111": {"id": 111, "handle": "dach", "title": "Dachshund Duck – 3D-Printed Loyal Dog Duck Collectible", "theme_classification": {"occasions": []}},
        "222": {"id": 222, "handle": "box", "title": "Boxer Duck – 3D-Printed Loyal Dog Duck Collectible", "theme_classification": {"occasions": []}},
    }}
    profit = {"products": [{"label": "Dachshund Duck", "sample_product_id": "111", "units_sold": 22, "distinct_skus": ["dach-1"]}]}
    p = di.build_demand_intel(catalog=catalog, ctx=_FakeCtx(None), profit=profit,
                             tx_snapshot={"items": []}, occasion={"active_occasions": []}, now_epoch=0)
    by_id = {d["product_id"]: d for d in p["ducks"]}
    assert by_id["111"]["funnel"]["buys_30d"] == 22 and by_id["111"]["bucket"] == "winner"
    assert by_id["222"]["funnel"]["buys_30d"] is None and by_id["222"]["bucket"] == "low_signal"  # Boxer: no sales


def test_ga4_match_requires_shared_subject():
    # Oklahoma must NOT inherit Michigan's views (shared 'officially licensed
    # college … team spirit pride duck' template, different subject).
    assert di._ga4_match_is_trustworthy("Oklahoma Sooners Duck – Officially Licensed Duck With Team Spirit & Pride",
                                        "Michigan Wolverines Duck – Officially Licensed Duck With Team Spirit & Pride") is False
    assert di._ga4_match_is_trustworthy("Michigan Wolverines Duck – Officially Licensed",
                                        "Michigan Wolverines Duck – Team Spirit") is True
