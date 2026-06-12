"""Surface 13 Phase 2: occasion nominations section of the weekly packet."""
from __future__ import annotations

from weekly_strategy_recommendation_packet import _build_occasion_nominations


def test_active_occasion_nominated_with_top_products():
    intel = {"active_occasions": [{
        "id": "july_4", "name": "Independence Day", "peak_date": "2026-07-04",
        "days_until_peak": 9, "messaging_angle": "patriotic spotlight",
        "products": [{"title": f"Duck {i}", "score": 10 - i, "reasons": ["classifier_occasion"]}
                     for i in range(10)],
    }]}
    noms = _build_occasion_nominations(intel)
    assert len(noms) == 1
    assert noms[0]["occasion"] == "Independence Day"
    assert noms[0]["pick_count"] == 10
    assert len(noms[0]["top_products"]) == 6  # capped
    assert noms[0]["top_products"][0]["title"] == "Duck 0"


def test_missing_or_empty_intel_yields_empty_list():
    assert _build_occasion_nominations({}) == []
    assert _build_occasion_nominations(None) == []
    assert _build_occasion_nominations({"active_occasions": []}) == []


def test_malformed_entries_skipped():
    noms = _build_occasion_nominations({"active_occasions": ["not-a-dict",
                                                             {"id": "x", "products": None}]})
    assert len(noms) == 1
    assert noms[0]["pick_count"] == 0
