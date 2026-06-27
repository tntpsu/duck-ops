"""Surface 42: GA4-verdict weekly-sale steering (read-only, fail-soft)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import sale_steering as ss  # noqa: E402
import seo_demand_context as sdc  # noqa: E402


def _ctx(listings):
    return sdc.SeoDemandContext(queries=[], term_scores={}, listings=listings)


ID2TITLE = {"1": "Wrangler Trail Duck", "2": "Pirate Captain Duck", "3": "Cat Lover Duck"}
TARGETS = [{"product_id": "1", "discount": "20%"},
           {"product_id": "2", "discount": "20%"},
           {"product_id": "3", "discount": "20%"}]


def test_promote_winner_dropped_from_sale():
    ctx = _ctx([{"title": "Pirate Captain Duck", "channel": "shopify",
                 "verdict": "promote", "engagement_rate": 0.8}])
    kept, dropped = ss.steer_sale_targets(TARGETS, demand_context=ctx, id_to_title=ID2TITLE)
    kept_ids = [t["product_id"] for t in kept]
    assert "2" not in kept_ids and [d["product_id"] for d in dropped] == ["2"]


def test_fix_listing_prioritized():
    ctx = _ctx([{"title": "Cat Lover Duck", "channel": "shopify",
                 "verdict": "fix", "engagement_rate": 0.15}])
    kept, dropped = ss.steer_sale_targets(TARGETS, demand_context=ctx, id_to_title=ID2TITLE)
    assert kept[0]["product_id"] == "3" and kept[0]["_steer_verdict"] == "fix"
    assert dropped == []


def test_neutral_and_unmatched_kept_unchanged():
    ctx = _ctx([{"title": "Something Else Entirely", "channel": "shopify",
                 "verdict": "promote", "engagement_rate": 0.8}])
    kept, dropped = ss.steer_sale_targets(TARGETS, demand_context=ctx, id_to_title=ID2TITLE)
    assert [t["product_id"] for t in kept] == ["1", "2", "3"] and dropped == []


def test_empty_context_passes_targets_through():
    empty = sdc.SeoDemandContext(queries=[], term_scores={}, listings=[])
    kept, dropped = ss.steer_sale_targets(TARGETS, demand_context=empty, id_to_title=ID2TITLE)
    assert kept == TARGETS and dropped == []


def test_combined_drop_promote_prioritize_fix():
    ctx = _ctx([
        {"title": "Pirate Captain Duck", "channel": "shopify", "verdict": "promote", "engagement_rate": 0.8},
        {"title": "Cat Lover Duck", "channel": "shopify", "verdict": "fix", "engagement_rate": 0.15},
    ])
    kept, dropped = ss.steer_sale_targets(TARGETS, demand_context=ctx, id_to_title=ID2TITLE)
    assert [d["product_id"] for d in dropped] == ["2"]          # winner dropped
    assert kept[0]["product_id"] == "3"                          # fix first
    assert [t["product_id"] for t in kept] == ["3", "1"]         # then the neutral one


def test_id_to_title_helper():
    catalog = {"items": {"9": {"title": "Foo Duck"}, "x": "bad"}}
    assert ss._id_to_title(catalog) == {"9": "Foo Duck"}


# ---- directives producer (consumed by duckAgent weekly flow) --------------

def _catalog():
    return {"items": {"1": {"title": "Wrangler Trail Duck"},
                      "2": {"title": "Pirate Captain Duck"},
                      "3": {"title": "Cat Lover Duck"}}}


def test_directives_split_promote_and_fix():
    ctx = _ctx([
        {"title": "Pirate Captain Duck", "channel": "shopify", "verdict": "promote", "engagement_rate": 0.8},
        {"title": "Cat Lover Duck", "channel": "shopify", "verdict": "fix", "engagement_rate": 0.15},
    ])
    d = ss.build_steering_directives(demand_context=ctx, catalog=_catalog())
    assert d["available"] is True
    assert d["exclude_product_ids"] == ["2"] and d["prioritize_product_ids"] == ["3"]


def test_directives_unavailable_when_no_ga4():
    empty = sdc.SeoDemandContext(queries=[], term_scores={}, listings=[])
    d = ss.build_steering_directives(demand_context=empty, catalog=_catalog())
    assert d["available"] is False
    assert d["exclude_product_ids"] == [] and d["prioritize_product_ids"] == []


def test_directives_write_guard(monkeypatch):
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    with pytest.raises(ss.TestModeRefusalError):
        ss.write_steering_directives({}, path=ss._FROZEN_PRODUCTION_SALE_STEERING_PATH)


def test_directives_roundtrip(tmp_path):
    out = ss.write_steering_directives({"available": True, "exclude_product_ids": ["2"]},
                                       path=tmp_path / "sale_steering.json")
    import json as _j
    assert _j.loads(out.read_text())["exclude_product_ids"] == ["2"]
