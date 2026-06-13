"""Surface 16 Build-Next scoring producer tests.

Matrix rows in duck-ops TESTS.md Surface 16. The scorer is deterministic
(no LLM), so these are exhaustive on the factor math + degradation paths:
each factor scored in isolation, suppression (already-made + operator
feedback), empty/missing inputs returning [] rather than inventing picks,
and the three-layer write isolation (conftest redirect here + the
DUCK_TEST_MODE guard + the pollution audit test)."""
from __future__ import annotations

from pathlib import Path

import build_next_engine as bne


def _report(*, ducks=None, trending=None):
    return {"ducks_to_build": ducks or [], "trending_products": trending or []}


def _duck(listing_id, title, *, engagement=1000, views=500, favorites=20,
          tags=None, priority="medium"):
    return {
        "listing_id": listing_id, "title": title, "shop_name": "CompShop",
        "engagement_score": engagement, "views": views, "favorites": favorites,
        "tags": tags or [], "priority": priority,
    }


def _catalog(items):
    return {"items": _cat_items(items)}


def _cat_items(items):
    return {str(i): it for i, it in enumerate(items)}


def _cat_item(title, *, status="active", core_terms="", classified=False):
    item = {"title": title, "status": status, "core_terms": core_terms}
    if classified:
        item["theme_classification"] = {"primary_category": "Animals & Pets"}
    return item


# --------------------------------------------------------------------------
# Candidate assembly
# --------------------------------------------------------------------------

class TestAssembleCandidates:
    def test_union_dedupes_by_listing_id(self):
        report = _report(
            ducks=[_duck("1", "Dragon Duck")],
            trending=[_duck("1", "Dragon Duck"), _duck("2", "Cactus Duck")],
        )
        cands = bne.assemble_candidates(report)
        ids = sorted(c["listing_id"] for c in cands)
        assert ids == ["1", "2"]

    def test_ducks_to_build_priority_wins_on_merge(self):
        report = _report(
            ducks=[_duck("1", "Dragon Duck", priority="high")],
            trending=[_duck("1", "Dragon Duck", priority="low")],
        )
        cand = bne.assemble_candidates(report)[0]
        assert cand["priority"] == "high"
        assert set(cand["_sources"]) == {"ducks_to_build", "trending_products"}

    def test_empty_report_yields_no_candidates(self):
        assert bne.assemble_candidates({}) == []


class TestLatestCompetitorReport:
    def test_picks_newest_dated_report(self, tmp_path):
        for name in ("2026-06-10_competitor_report.json", "2026-06-12_competitor_report.json"):
            (tmp_path / name).write_text('{"ducks_to_build": []}', encoding="utf-8")
        _, picked = bne.latest_competitor_report(tmp_path)
        assert picked == "2026-06-12_competitor_report.json"

    def test_ignores_non_dated_dev_report(self, tmp_path):
        """A dev/test file sorts AFTER dated ones alphabetically; it must
        never be chosen (live-run bug 2026-06-12)."""
        (tmp_path / "2026-06-12_competitor_report.json").write_text('{}', encoding="utf-8")
        (tmp_path / "test-smart-similarity_competitor_report.json").write_text('{}', encoding="utf-8")
        _, picked = bne.latest_competitor_report(tmp_path)
        assert picked == "2026-06-12_competitor_report.json"

    def test_missing_dir_returns_none(self, tmp_path):
        _, picked = bne.latest_competitor_report(tmp_path / "nope")
        assert picked is None


# --------------------------------------------------------------------------
# Factor: demand
# --------------------------------------------------------------------------

class TestDemand:
    def test_pool_max_normalizes_to_one(self):
        row = _duck("1", "X", engagement=5000)
        value, reason = bne.score_demand(row, pool_max=5000.0)
        assert value == 1.0
        assert "5000" in reason

    def test_zero_pool_returns_zero(self):
        value, _ = bne.score_demand(_duck("1", "X"), pool_max=0.0)
        assert value == 0.0

    def test_falls_back_to_views_plus_favorites(self):
        row = {"listing_id": "1", "title": "X", "views": 100, "favorites": 10}
        # strength = 100 + 5*10 = 150
        assert bne._demand_strength(row) == 150.0


# --------------------------------------------------------------------------
# Factor: margin
# --------------------------------------------------------------------------

class TestMargin:
    def _profit(self):
        return {"products": [
            {"title_variants": ["Golden Retriever Duck Dog Lover"],
             "margin_pct": 90.0, "is_confident_margin": True},
            {"title_variants": ["Cactus Succulent Duck Plant"],
             "margin_pct": 40.0, "is_confident_margin": True},
        ]}

    def test_confident_title_match_uses_real_margin(self):
        pairs, med = bne._margin_index(self._profit())
        toks = bne._tokens("Golden Retriever Duck Dog Lover Gift")
        value, reason, estimated = bne.score_margin(toks, pairs, med)
        assert value == 0.9
        assert estimated is False
        assert "90%" in reason

    def test_no_match_uses_flagged_median_estimate(self):
        pairs, med = bne._margin_index(self._profit())
        toks = bne._tokens("Unrelated Wizard Sorcerer Duck")
        value, reason, estimated = bne.score_margin(toks, pairs, med)
        assert estimated is True
        assert "estimated" in reason
        assert value == 0.65  # median(90,40)=65 -> 0.65

    def test_no_profit_data_neutral_flagged(self):
        value, reason, estimated = bne.score_margin({"wizard"}, [], None)
        assert value == bne.NEUTRAL_MARGIN
        assert estimated is True


# --------------------------------------------------------------------------
# Factor: catalog gap / already-made
# --------------------------------------------------------------------------

class TestCatalogGap:
    def test_no_overlap_is_full_gap(self):
        sets = bne._catalog_token_sets(_cat_items([_cat_item("Astronaut Space Helmet")]))
        toks = bne._tokens("Medieval Knight Armor Sword")
        gap, reason, already = bne.score_catalog_gap(toks, sets)
        assert gap == 1.0
        assert already is None

    def test_high_overlap_flags_already_made(self):
        sets = bne._catalog_token_sets(
            _cat_items([_cat_item("Golden Retriever Dog Duck", core_terms="retriever, golden, dog")]))
        toks = bne._tokens("Golden Retriever Dog Duck Lover")
        gap, reason, already = bne.score_catalog_gap(toks, sets)
        assert gap < (1.0 - bne.ALREADY_MADE_OVERLAP)
        assert already is not None
        assert "already make" in reason

    def test_discontinued_items_excluded_from_catalog(self):
        sets = bne._catalog_token_sets(
            _cat_items([_cat_item("Golden Retriever Dog Duck", status="archived")]))
        assert sets == []


# --------------------------------------------------------------------------
# Factor: occasion fit
# --------------------------------------------------------------------------

class TestOccasionFit:
    def test_active_occasion_keyword_hit_boosts(self):
        active = [{"id": "fathers_day", "name": "Father's Day",
                   "days_until_peak": 9, "keywords": ["dad", "grill", "fishing"]}]
        toks = bne._tokens("BBQ Grill Master Duck")
        value, reason = bne.score_occasion_fit(toks, active)
        assert value == 1.0
        assert "Father's Day" in reason

    def test_no_active_match_is_neutral_evergreen(self):
        active = [{"id": "fathers_day", "keywords": ["dad"]}]
        value, reason = bne.score_occasion_fit(bne._tokens("Wizard Duck"), active)
        assert value == bne.NEUTRAL_OCCASION
        assert "evergreen" in reason

    def test_no_active_occasions_is_neutral(self):
        value, _ = bne.score_occasion_fit(bne._tokens("Wizard Duck"), [])
        assert value == bne.NEUTRAL_OCCASION


# --------------------------------------------------------------------------
# build + suppression + empty inputs
# --------------------------------------------------------------------------

class TestBuild:
    def _kwargs(self, **over):
        base = dict(
            report=_report(ducks=[_duck("1", "Medieval Knight Armor Sword Duck", engagement=4000)]),
            report_name="2026-06-12_competitor_report.json",
            catalog=_catalog([_cat_item("Astronaut Space Helmet Duck")]),
            profit={"products": [{"title_variants": ["Medieval Knight Armor Sword Duck"],
                                  "margin_pct": 80.0, "is_confident_margin": True}]},
            occasion_intel={"active_occasions": []},
            feedback={"concepts": {}},
        )
        base.update(over)
        return base

    def test_happy_path_ranks_candidate(self):
        payload = bne.build_build_next_queue(**self._kwargs())
        assert payload["queue_count"] == 1
        entry = payload["queue"][0]
        assert entry["score"] > 0
        assert entry["factors"]["demand"] == 1.0
        assert entry["factors"]["margin"] == 0.8
        assert len(entry["reasons"]) == 4

    def test_already_made_candidate_is_suppressed(self):
        payload = bne.build_build_next_queue(**self._kwargs(
            catalog=_catalog([_cat_item("Medieval Knight Armor Sword Duck",
                                        core_terms="medieval, knight, armor, sword")])))
        assert payload["queue_count"] == 0
        assert payload["suppressed_count"] == 1
        assert "already made" in payload["suppressed"][0]["suppressed_reason"]

    def test_operator_rejected_concept_is_suppressed(self):
        fb = {"concepts": {"medieval knight armor sword": {"latest_resolution": "rejected"}}}
        payload = bne.build_build_next_queue(**self._kwargs(feedback=fb))
        assert payload["queue_count"] == 0
        assert payload["suppressed_count"] == 1
        assert "feedback" in payload["suppressed"][0]["suppressed_reason"]

    def test_empty_report_yields_empty_queue_not_invented(self):
        payload = bne.build_build_next_queue(**self._kwargs(
            report={}, report_name=None))
        assert payload["queue"] == []
        assert payload["sources"]["competitor_report"] is None

    def test_missing_profit_and_occasion_degrade_not_crash(self):
        payload = bne.build_build_next_queue(**self._kwargs(
            profit={}, occasion_intel={}))
        assert payload["queue_count"] == 1
        entry = payload["queue"][0]
        assert entry["margin_estimated"] is True
        assert entry["factors"]["occasion_fit"] == bne.NEUTRAL_OCCASION

    def test_classifier_coverage_surfaced_in_sources(self):
        payload = bne.build_build_next_queue(**self._kwargs(
            catalog=_catalog([_cat_item("Astronaut Duck", classified=True),
                              _cat_item("Wizard Duck", classified=False)])))
        assert payload["sources"]["classifier_coverage"] == 1


# --------------------------------------------------------------------------
# Write isolation (three-layer policy)
# --------------------------------------------------------------------------

class TestWriteIsolation:
    def test_write_is_atomic_and_isolated(self, tmp_path):
        # conftest already redirects BUILD_NEXT_QUEUE_PATH to tmp.
        path = bne.write_build_next_queue({"queue": [], "suppressed": []})
        assert Path(path).exists()
        assert "build_next_queue.json" in str(path)

    def test_write_guard_refuses_prod_path_in_test_mode(self, monkeypatch):
        monkeypatch.setattr(bne, "BUILD_NEXT_QUEUE_PATH",
                            bne._FROZEN_PRODUCTION_BUILD_NEXT_QUEUE_PATH)
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        try:
            bne.write_build_next_queue({"queue": []})
            assert False, "expected TestModeRefusalError"
        except bne.TestModeRefusalError:
            pass
