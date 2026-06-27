"""Surface 41 Stage A: gap-query → draft collection planner (read-only).

Covers the defining scope rule (0/1 → skip, ≥N → candidate), the match bar
(≥2 shared + coverage), dedup vs existing collections, fail-soft/staleness, the
fail-closed under-threshold drop, and the DUCK_TEST_MODE write guard."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import shopify_collection_planner as scp  # noqa: E402


def _gsc(*queries, available=True):
    return {"available": available,
            "gap_queries": [{"query": q, "impressions": impr, "trend": tr}
                            for (q, impr, tr) in queries]}


def _catalog(*titles):
    return {"items": {str(i): {"title": t, "core_terms": t, "tags": [], "status": "active"}
                      for i, t in enumerate(titles)}}


def _audit(*collection_titles):
    return {"resources": [{"kind": "collection", "title": t, "resource_url": f"/collections/{t.lower().replace(' ', '-')}"}
                          for t in collection_titles]}


# ---- scope rule ------------------------------------------------------------

def test_three_plus_matches_make_a_candidate():
    gsc = _gsc(("jeep wrangler duck", 300, "rising"))
    cat = _catalog("Wrangler Trail Duck", "Wrangler Sahara Duck", "Wrangler Rubicon Duck", "Cat Duck")
    cands = scp.build_collection_candidates(gsc, cat, _audit())
    assert len(cands) == 1
    c = cands[0]
    assert c["member_count"] == 3 and "Cat Duck" not in c["member_titles"]
    assert c["proposed_handle"] == "jeep-wrangler-duck"

def test_two_matches_below_threshold_skipped():
    gsc = _gsc(("jeep wrangler duck", 300, "rising"))
    cat = _catalog("Wrangler Trail Duck", "Wrangler Sahara Duck", "Cat Duck")
    assert scp.build_collection_candidates(gsc, cat, _audit()) == []

def test_single_match_is_not_a_collection():
    gsc = _gsc(("garfield duck", 120, "steady"))
    cat = _catalog("Garfield Duck", "Wrangler Duck", "Pirate Duck")
    assert scp.build_collection_candidates(gsc, cat, _audit()) == []


# ---- match bar -------------------------------------------------------------

def test_all_generic_query_is_skipped():
    # "jeep duck dashboard" is entirely niche-stopwords → no distinctive tokens
    # survive → never a candidate (the real protection against generic matches).
    gsc = _gsc(("jeep duck dashboard", 200, "rising"))
    cat = _catalog("Jeep Duck One", "Jeep Duck Two", "Jeep Duck Three")
    assert scp.build_collection_candidates(gsc, cat, _audit()) == []

def test_low_coverage_multi_token_query_skipped():
    # 3 distinctive query tokens, product shares only 1 → coverage 0.33 < 0.5
    gsc = _gsc(("wrangler gladiator rubicon", 200, "rising"))
    cat = _catalog("Wrangler Trail Mount", "Wrangler Sahara Mount", "Wrangler Rubicon Bracket")
    cands = scp.build_collection_candidates(gsc, cat, _audit())
    # only "Wrangler Rubicon Bracket" shares 2/3 (wrangler+rubicon); others share
    # 1/3 → below coverage → fewer than 3 members → skipped
    assert cands == []


# ---- dedup -----------------------------------------------------------------

def test_dedup_against_existing_collection_by_handle():
    gsc = _gsc(("jeep wrangler duck", 300, "rising"))
    cat = _catalog("Wrangler Trail Duck", "Wrangler Sahara Duck", "Wrangler Rubicon Duck")
    audit = _audit("Jeep Wrangler Duck")  # already exists → slug jeep-wrangler-duck
    assert scp.build_collection_candidates(gsc, cat, audit) == []

def test_dedup_when_query_subset_of_existing_title():
    gsc = _gsc(("pirate duck", 90, "steady"))
    cat = _catalog("Pirate Captain Duck", "Pirate Sailor Duck", "Pirate Ghost Duck")
    audit = _audit("Pirate Themed Collectible Ducks")  # tokens superset of {pirate}
    assert scp.build_collection_candidates(gsc, cat, audit) == []


# ---- fail-soft / staleness -------------------------------------------------

def test_unavailable_demand_yields_no_candidates():
    gsc = _gsc(("jeep wrangler duck", 300, "rising"), available=False)
    cat = _catalog("Wrangler Trail Duck", "Wrangler Sahara Duck", "Wrangler Rubicon Duck")
    assert scp.build_collection_candidates(gsc, cat, _audit()) == []

def test_stale_demand_yields_no_candidates():
    gsc = _gsc(("jeep wrangler duck", 300, "rising"))
    gsc["generated_at"] = "2020-01-01T00:00:00"
    cat = _catalog("Wrangler Trail Duck", "Wrangler Sahara Duck", "Wrangler Rubicon Duck")
    assert scp.build_collection_candidates(gsc, cat, _audit()) == []


# ---- active-only + ranking -------------------------------------------------

def test_drafts_excluded_can_drop_below_threshold():
    gsc = _gsc(("jeep wrangler duck", 300, "rising"))
    cat = _catalog("Wrangler Trail Duck", "Wrangler Sahara Duck")
    cat["items"]["2"] = {"title": "Wrangler Rubicon Duck", "core_terms": "Wrangler Rubicon Duck",
                         "tags": [], "status": "draft"}  # not active → only 2 active → skip
    assert scp.build_collection_candidates(gsc, cat, _audit()) == []

def test_candidates_sorted_by_impressions():
    gsc = _gsc(("jeep wrangler duck", 100, "steady"), ("pirate duck", 500, "rising"))
    cat = _catalog("Wrangler A Duck", "Wrangler B Duck", "Wrangler C Duck",
                   "Pirate A Duck", "Pirate B Duck", "Pirate C Duck")
    cands = scp.build_collection_candidates(gsc, cat, _audit())
    assert [c["source_query"] for c in cands] == ["pirate duck", "jeep wrangler duck"]


# ---- write guard -----------------------------------------------------------

def test_plan_shape_and_roundtrip(tmp_path):
    gsc = _gsc(("jeep wrangler duck", 300, "rising"))
    cat = _catalog("Wrangler Trail Duck", "Wrangler Sahara Duck", "Wrangler Rubicon Duck")
    plan = scp.build_collection_plan(gsc, cat, _audit())
    assert plan["status"] == "awaiting_review" and plan["candidate_count"] == 1
    out = scp.write_collection_plan(plan, path=tmp_path / "latest.json")
    assert json.loads(out.read_text())["candidates"][0]["member_count"] == 3

def test_write_guard_refuses_prod_in_test_mode(monkeypatch):
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    with pytest.raises(scp.TestModeRefusalError):
        scp.write_collection_plan({}, path=scp._FROZEN_PRODUCTION_COLLECTION_PLAN_PATH)
