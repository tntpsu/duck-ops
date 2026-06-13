"""Surface 16 Phase E: Build-Next nominations section of the weekly packet."""
from __future__ import annotations

from weekly_strategy_recommendation_packet import _build_build_next_nominations


def test_top_three_concepts_nominated_in_rank_order():
    queue = {"queue": [
        {"title": f"Duck {i}", "score": round(1.0 - i * 0.1, 2),
         "factors": {"demand": 1.0}, "reasons": [f"reason {i}"], "listing_id": str(i)}
        for i in range(8)
    ]}
    noms = _build_build_next_nominations(queue)
    assert len(noms) == 3  # capped at top 3
    assert noms[0]["title"] == "Duck 0"
    assert noms[0]["why"] == ["reason 0"]
    assert noms[0]["listing_id"] == "0"


def test_missing_or_empty_queue_yields_empty_list():
    assert _build_build_next_nominations({}) == []
    assert _build_build_next_nominations(None) == []
    assert _build_build_next_nominations({"queue": []}) == []


def test_malformed_entries_skipped():
    noms = _build_build_next_nominations({"queue": ["nope", {"title": "Duck", "score": 0.5}]})
    assert len(noms) == 1
    assert noms[0]["title"] == "Duck"
    assert noms[0]["why"] == []
