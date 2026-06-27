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
          tags=None, priority="medium", trending=None, sold_7d=None):
    row = {
        "listing_id": listing_id, "title": title, "shop_name": "CompShop",
        "engagement_score": engagement, "views": views, "favorites": favorites,
        "tags": tags or [], "priority": priority,
    }
    if trending is not None:
        row["trending_score"] = trending
    if sold_7d is not None:
        row["sold_last_7d"] = sold_7d
    return row


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

    def test_skips_daily_snapshot_for_weekly_analysis(self, tmp_path):
        """Regression (2026-06-15): the 06-12 cadence split made daily runs
        write snapshot-only reports (period=daily_snapshot, empty
        ducks_to_build) to the same filename, silently emptying Build-Next.
        Must walk past newer snapshots to the latest real analysis."""
        import json as _json
        (tmp_path / "2026-06-12_competitor_report.json").write_text(
            _json.dumps({"period": "weekly", "ducks_to_build": [{"listing_id": "1", "title": "X"}]}),
            encoding="utf-8")
        (tmp_path / "2026-06-14_competitor_report.json").write_text(
            _json.dumps({"period": "daily_snapshot", "ducks_to_build": []}), encoding="utf-8")
        report, picked = bne.latest_competitor_report(tmp_path)
        assert picked == "2026-06-12_competitor_report.json"
        assert report["ducks_to_build"]

    def test_snapshot_detected_via_ai_insights_flag(self, tmp_path):
        import json as _json
        (tmp_path / "2026-06-12_competitor_report.json").write_text(
            _json.dumps({"ducks_to_build": [{"listing_id": "1"}]}), encoding="utf-8")
        (tmp_path / "2026-06-14_competitor_report.json").write_text(
            _json.dumps({"ai_insights": {"_snapshot_only": True}}), encoding="utf-8")
        _, picked = bne.latest_competitor_report(tmp_path)
        assert picked == "2026-06-12_competitor_report.json"

    def test_all_snapshots_returns_none(self, tmp_path):
        import json as _json
        (tmp_path / "2026-06-14_competitor_report.json").write_text(
            _json.dumps({"period": "daily_snapshot"}), encoding="utf-8")
        _, picked = bne.latest_competitor_report(tmp_path)
        assert picked is None


# --------------------------------------------------------------------------
# Factor: demand
# --------------------------------------------------------------------------

class TestDemand:
    def test_momentum_pool_max_normalizes_to_one(self):
        row = {"listing_id": "1", "title": "X", "trending_score": 5000, "sold_last_7d": 30}
        value, reason = bne.score_demand(row, momentum_max=5000.0, alltime_max=0.0)
        assert value == 1.0
        assert "5000" in reason and "momentum" in reason

    def test_alltime_only_row_is_capped_below_momentum_band(self):
        """An all-time-only row maxes out at FALLBACK_DEMAND_CEILING even when
        it tops its own pool — it can never reach the band a mover occupies."""
        row = _duck("1", "X", engagement=5000)
        value, reason = bne.score_demand(row, momentum_max=0.0, alltime_max=5000.0)
        assert value == bne.FALLBACK_DEMAND_CEILING
        assert "5000" in reason and "capped" in reason

    def test_zero_pool_returns_zero(self):
        value, _ = bne.score_demand(_duck("1", "X"), momentum_max=0.0, alltime_max=0.0)
        assert value == 0.0

    def test_falls_back_to_views_plus_favorites(self):
        row = {"listing_id": "1", "title": "X", "views": 100, "favorites": 10}
        # strength = 100 + 5*10 = 150
        assert bne._demand_strength(row) == 150.0

    def test_prefers_momentum_over_all_time_engagement(self):
        """trending_score (recent sales momentum) wins over engagement_score
        (lifetime popularity) — they disagree and momentum is the truth we want."""
        row = {"listing_id": "1", "title": "X",
               "engagement_score": 9000, "trending_score": 42, "sold_last_7d": 0}
        assert bne._demand_strength(row) == 42.0
        _, reason = bne.score_demand(row, momentum_max=100.0, alltime_max=9000.0)
        assert "momentum" in reason and "sold/7d" in reason

    def test_zero_trending_score_falls_back_to_engagement(self):
        """A row tracked but with no recent movement (trending_score 0) must NOT
        score 0 demand — fall back to all-time so it still ranks, flagged."""
        row = {"listing_id": "1", "title": "X",
               "engagement_score": 800, "trending_score": 0, "views": 500, "favorites": 60}
        assert bne._demand_strength(row) == 800.0
        _, reason = bne.score_demand(row, momentum_max=0.0, alltime_max=1000.0)
        assert "all-time" in reason and "no recent-momentum" in reason

    def test_new_listing_trending_score_is_not_treated_as_momentum(self):
        """Golden trap (Duckpool, 2026-06-22): a freshly-discovered listing has
        delta_source='new_listing', so its trending_score (8309) is really
        favs*3 + views*0.5 — a LIFETIME proxy, not 7-day movement. It must be
        routed to the capped all-time pool and lose to a real snapshot mover."""
        new_listing = {"listing_id": "dp", "title": "Duckpool",
                       "trending_score": 8309, "delta_source": "new_listing",
                       "sold_last_30d": 8, "engagement_score": 12323,
                       "views": 10175, "favorites": 1074}
        mover = {"listing_id": "bc", "title": "Breast Cancer Survivor Duck",
                 "trending_score": 5713, "delta_source": "snapshot", "sold_last_7d": 25}
        assert bne._has_snapshot_momentum(new_listing) is False
        assert bne._has_snapshot_momentum(mover) is True
        bases = [bne._demand_basis(new_listing), bne._demand_basis(mover)]
        momentum_max = max((s for s, m in bases if m), default=0.0)
        alltime_max = max((s for s, m in bases if not m), default=0.0)
        nl_val, nl_reason = bne.score_demand(new_listing, momentum_max, alltime_max)
        mv_val, _ = bne.score_demand(mover, momentum_max, alltime_max)
        assert mv_val == 1.0 and mv_val > nl_val
        assert nl_val <= bne.FALLBACK_DEMAND_CEILING
        assert "no recent-momentum" in nl_reason

    def test_cooled_off_past_hit_loses_to_current_mover(self):
        """Golden divergence (2026-06-22 report): Duckpool was #2 all-time
        (engagement 8000) but sold 0 in 7 days, while the Breast Cancer Survivor
        Duck was the #1 mover (25 sold/7d). Momentum ranking must put the mover
        on top — the exact inversion this change exists to fix."""
        duckpool = {"listing_id": "dp", "title": "Duckpool",
                    "engagement_score": 8000, "trending_score": 0, "sold_last_7d": 0,
                    "views": 7000, "favorites": 200}
        mover = {"listing_id": "bc", "title": "Breast Cancer Survivor Duck",
                 "engagement_score": 1200, "trending_score": 5500, "sold_last_7d": 25}
        bases = [bne._demand_basis(duckpool), bne._demand_basis(mover)]
        momentum_max = max((s for s, m in bases if m), default=0.0)
        alltime_max = max((s for s, m in bases if not m), default=0.0)
        dp_val, _ = bne.score_demand(duckpool, momentum_max, alltime_max)
        mv_val, _ = bne.score_demand(mover, momentum_max, alltime_max)
        assert mv_val > dp_val
        assert mv_val == 1.0  # the current mover tops the momentum pool
        assert dp_val <= bne.FALLBACK_DEMAND_CEILING  # cooled-off hit stays capped


# --------------------------------------------------------------------------
# Factor: search demand (Surface 38)
# --------------------------------------------------------------------------

class TestSearchDemand:
    def test_no_data_is_neutral(self):
        value, reason = bne.score_search_demand({"pirate"}, {})
        assert value == bne.NEUTRAL_SEARCH and "no first-party search signal" in reason

    def test_no_match_is_neutral(self):
        value, reason = bne.score_search_demand({"pirate"}, {"wizard": 1.0})
        assert value == bne.NEUTRAL_SEARCH and "no first-party search match" in reason

    def test_hot_term_boosts_above_neutral_to_one(self):
        value, reason = bne.score_search_demand({"pirate", "hat"}, {"pirate": 1.0})
        assert value == 1.0 and "pirate" in reason

    def test_partial_term_scales_between_neutral_and_one(self):
        value, _ = bne.score_search_demand({"pirate"}, {"pirate": 0.5})
        # NEUTRAL_SEARCH + (1-NEUTRAL_SEARCH)*0.5
        assert bne.NEUTRAL_SEARCH < value < 1.0

    def test_absent_signal_does_not_change_ranking(self):
        """Uniform neutral when no GSC data: two candidates keep their relative
        order (the no-op-until-live guarantee)."""
        a = bne.score_search_demand({"alpha"}, {})[0]
        b = bne.score_search_demand({"beta"}, {})[0]
        assert a == b == bne.NEUTRAL_SEARCH

    def test_loader_ignores_unavailable_payload(self, tmp_path):
        p = tmp_path / "gsc.json"
        p.write_text('{"available": false, "term_scores": {"pirate": 1.0}}')
        assert bne._load_search_demand_terms(p) == {}

    def test_loader_reads_available_term_scores(self, tmp_path):
        p = tmp_path / "gsc.json"
        p.write_text('{"available": true, "term_scores": {"pirate": 0.8, "bad": "x"}}')
        terms = bne._load_search_demand_terms(p)
        assert terms == {"pirate": 0.8}  # non-numeric dropped


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
            report=_report(ducks=[_duck("1", "Medieval Knight Armor Sword Duck",
                                        engagement=4000, trending=4000, sold_7d=18)]),
            report_name="2026-06-12_competitor_report.json",
            catalog=_catalog([_cat_item("Astronaut Space Helmet Duck")]),
            profit={"products": [{"title_variants": ["Medieval Knight Armor Sword Duck"],
                                  "margin_pct": 80.0, "is_confident_margin": True}]},
            occasion_intel={"active_occasions": []},
            feedback={"concepts": {}},
            # Default to the deterministic token path — passing {} (not None)
            # prevents build_build_next_queue from making a live embedding call.
            semantic_map={},
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
        assert entry["factors"]["search_demand"] == bne.NEUTRAL_SEARCH  # no GSC data in test
        assert len(entry["reasons"]) == 5

    def test_already_made_candidate_is_suppressed(self):
        payload = bne.build_build_next_queue(**self._kwargs(
            catalog=_catalog([_cat_item("Medieval Knight Armor Sword Duck",
                                        core_terms="medieval, knight, armor, sword")])))
        assert payload["queue_count"] == 0

    # ---- Surface 21 algorithm fixes -------------------------------------

    def test_stopword_fix_no_false_already_made(self):
        """Regression: 'Owl Duck' must NOT suppress 'Couple Ducks' just because
        both are car/dashboard decor (the 2026-06-15 boilerplate-overlap bug)."""
        payload = bne.build_build_next_queue(**self._kwargs(
            report=_report(ducks=[_duck("1", "Couple Ducks Car Dashboard Decor Cruise Accessory", engagement=4000)]),
            catalog=_catalog([_cat_item("Owl Duck Car Dashboard Decor")])))
        assert payload["queue_count"] == 1  # NOT suppressed

    def test_semantic_band_suppresses(self):
        """An injected semantic 'already_made' band suppresses regardless of tokens."""
        payload = bne.build_build_next_queue(**self._kwargs(
            report=_report(ducks=[_duck("1", "Wiener Dog Figure", engagement=4000)]),
            catalog=_catalog([_cat_item("Dachshund Duck")]),
            semantic_map={"Wiener Dog Figure": {"match": "Dachshund Duck", "score": 0.81, "band": "already_made"}}))
        assert payload["queue_count"] == 0
        assert "semantic match" in payload["suppressed"][0]["suppressed_reason"]

    def test_semantic_margin_from_catalog_match(self):
        """A semantic match to a confident-margin catalog product uses its real margin."""
        payload = bne.build_build_next_queue(**self._kwargs(
            report=_report(ducks=[_duck("1", "Sausage Dog Buddy", engagement=4000)]),
            catalog=_catalog([_cat_item("Dachshund Duck Long Loyal")]),
            profit={"products": [{"title_variants": ["Dachshund Duck Long Loyal"],
                                  "margin_pct": 60.0, "is_confident_margin": True}]},
            semantic_map={"Sausage Dog Buddy": {"match": "Dachshund Duck Long Loyal", "score": 0.45, "band": "distinct"}}))
        assert payload["queue_count"] == 1
        assert payload["queue"][0]["factors"]["margin"] == 0.6  # 60% / 100, not the median

    def test_off_brand_trending_suppressed(self):
        """A trending-only candidate that isn't a duck/dashboard concept is dropped."""
        report = {"ducks_to_build": [], "trending_products": [_duck("1", "Mini Pizza Fidget Toy", engagement=9000)]}
        payload = bne.build_build_next_queue(**self._kwargs(report=report))
        assert payload["queue_count"] == 0
        assert "off-brand" in payload["suppressed"][0]["suppressed_reason"]

    def test_on_brand_trending_kept(self):
        report = {"ducks_to_build": [], "trending_products": [_duck("1", "Cowboy Duck Dashboard", engagement=9000)]}
        payload = bne.build_build_next_queue(**self._kwargs(
            report=report, catalog=_catalog([_cat_item("Astronaut Duck")])))
        assert payload["queue_count"] == 1

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


# --------------------------------------------------------------------------
# Surface 28: possible-dupe soft-flag + one-click confirm (duplicate/distinct)
# --------------------------------------------------------------------------

class TestDupeFlag:
    def _kwargs(self, **over):
        base = dict(
            report=_report(ducks=[_duck("1", "Wiener Dog Figure", engagement=4000)]),
            report_name="2026-06-12_competitor_report.json",
            catalog=_catalog([_cat_item("Dachshund Duck")]),
            profit={"products": []},
            occasion_intel={"active_occasions": []},
            feedback={"concepts": {}},
            semantic_map={"Wiener Dog Figure": {"match": "Dachshund Duck", "score": 0.55, "band": "possible_dupe"}},
        )
        base.update(over)
        return base

    def test_possible_dupe_kept_and_flagged(self):
        payload = bne.build_build_next_queue(**self._kwargs())
        assert payload["queue_count"] == 1          # KEPT, not suppressed
        e = payload["queue"][0]
        assert e["possible_dupe"] is True
        assert e["dupe_score"] == 0.55
        assert e["semantic_match"] == "Dachshund Duck"

    def test_distinct_band_not_flagged(self):
        payload = bne.build_build_next_queue(**self._kwargs(
            semantic_map={"Wiener Dog Figure": {"match": "Dachshund Duck", "score": 0.20, "band": "distinct"}}))
        assert payload["queue"][0]["possible_dupe"] is False
        assert payload["queue"][0]["dupe_score"] is None

    def test_operator_confirmed_duplicate_is_suppressed(self):
        bne.record_dupe_decision("Wiener Dog Figure", "duplicate", matched="Dachshund Duck")
        payload = bne.build_build_next_queue(**self._kwargs())
        assert payload["queue_count"] == 0
        assert "operator confirmed duplicate" in payload["suppressed"][0]["suppressed_reason"]

    def test_operator_distinct_clears_the_flag(self):
        bne.record_dupe_decision("Wiener Dog Figure", "distinct")
        payload = bne.build_build_next_queue(**self._kwargs())
        assert payload["queue_count"] == 1
        assert payload["queue"][0]["possible_dupe"] is False   # ruling stops the flag

    def test_record_rejects_bad_decision(self):
        try:
            bne.record_dupe_decision("X", "maybe")
            assert False, "expected ValueError"
        except ValueError:
            pass

    def test_record_round_trips_by_concept_key(self):
        rec = bne.record_dupe_decision("Wiener Dog Figure", "duplicate", matched="Dachshund Duck")
        assert rec["decision"] == "duplicate" and rec["matched"] == "Dachshund Duck"
        loaded = bne._load_dupe_decisions()
        assert bne._dupe_decision_for(loaded, "Wiener Dog Figure") == "duplicate"

    def test_dupe_write_guard_refuses_prod_path_in_test_mode(self, monkeypatch):
        monkeypatch.setattr(bne, "BUILD_NEXT_DUPE_DECISIONS_PATH",
                            bne._FROZEN_PRODUCTION_BUILD_NEXT_DUPE_DECISIONS_PATH)
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        try:
            bne.record_dupe_decision("X", "duplicate")
            assert False, "expected TestModeRefusalError"
        except bne.TestModeRefusalError:
            pass
