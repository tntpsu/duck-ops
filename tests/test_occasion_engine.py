"""Surface 13 tests: occasion recurrence resolution, active windows,
messaging phases, product selection scoring, and the test-mode write
guard. Matrix rows in TESTS.md Surface 13."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

import occasion_engine as oe
from workflow_control import TestModeRefusalError


FATHERS_DAY_RULE = {"type": "nth_weekday", "month": 6, "weekday": 6, "nth": 3}
JULY4_RULE = {"type": "fixed", "month": 7, "day": 4}


def _occasion(**overrides):
    base = {
        "id": "test_fathers_day",
        "name": "Father's Day",
        "rule": dict(FATHERS_DAY_RULE),
        "lead_days": 14,
        "keywords": ["dad", "grill", "for him"],
        "recipients": ["dad", "husband"],
        "etsy_tag_candidates": ["gift for dad"],
        "messaging_phases": [
            {"max_days_until_peak": 2, "angle": "day-of"},
            {"max_days_until_peak": 7, "angle": "last call"},
            {"max_days_until_peak": 14, "angle": "gift guide"},
        ],
    }
    base.update(overrides)
    return base


def _product(pid="1", title="Grill Master Duck", status="active",
             occasions=(), recipients=(), tags=""):
    return {
        "id": pid,
        "handle": title.lower().replace(" ", "-"),
        "title": title,
        "status": status,
        "tags": tags,
        "theme_classification": {
            "primary_category": "Food & Beverages",
            "occasions": list(occasions),
            "recipients": list(recipients),
        },
    }


class TestRecurrence:
    def test_fathers_day_2026_is_june_21(self):
        assert oe.resolve_next_occurrence(FATHERS_DAY_RULE, date(2026, 6, 11)) == date(2026, 6, 21)

    def test_fixed_july4(self):
        assert oe.resolve_next_occurrence(JULY4_RULE, date(2026, 6, 11)) == date(2026, 7, 4)

    def test_peak_passed_rolls_to_next_year(self):
        assert oe.resolve_next_occurrence(JULY4_RULE, date(2026, 7, 5)) == date(2027, 7, 4)
        # Mother's Day (2nd Sunday of May) already passed in June.
        mothers = {"type": "nth_weekday", "month": 5, "weekday": 6, "nth": 2}
        assert oe.resolve_next_occurrence(mothers, date(2026, 6, 11)) == date(2027, 5, 9)

    def test_peak_day_itself_not_rolled(self):
        assert oe.resolve_next_occurrence(JULY4_RULE, date(2026, 7, 4)) == date(2026, 7, 4)

    def test_unknown_rule_type_raises(self):
        with pytest.raises(ValueError):
            oe.resolve_next_occurrence({"type": "lunar_phase"}, date(2026, 6, 11))

    def test_fixed_dates_picks_next_upcoming(self):
        rule = {"type": "fixed_dates", "dates": ["2026-04-05", "2027-03-28", "2028-04-16"]}
        assert oe.resolve_next_occurrence(rule, date(2026, 1, 1)) == date(2026, 4, 5)
        assert oe.resolve_next_occurrence(rule, date(2026, 4, 5)) == date(2026, 4, 5)
        assert oe.resolve_next_occurrence(rule, date(2026, 4, 6)) == date(2027, 3, 28)

    def test_fixed_dates_exhausted_raises(self):
        rule = {"type": "fixed_dates", "dates": ["2020-04-12"]}
        with pytest.raises(ValueError):
            oe.resolve_next_occurrence(rule, date(2026, 1, 1))

    def test_thanksgiving_4th_thursday(self):
        rule = {"type": "nth_weekday", "month": 11, "weekday": 3, "nth": 4}
        assert oe.resolve_next_occurrence(rule, date(2026, 1, 1)) == date(2026, 11, 26)


class TestActiveWindow:
    def test_inside_lead_window_active_with_phase(self):
        cal = {"occasions": [_occasion()]}
        active, errors = oe.resolve_active_occasions(cal, date(2026, 6, 11))
        assert errors == []
        assert len(active) == 1
        occ = active[0]
        assert occ["peak_date"] == "2026-06-21"
        assert occ["days_until_peak"] == 10
        assert occ["messaging_angle"] == "gift guide"

    def test_phase_boundaries(self):
        cal = {"occasions": [_occasion()]}
        active, _ = oe.resolve_active_occasions(cal, date(2026, 6, 16))  # 5 days out
        assert active[0]["messaging_angle"] == "last call"
        active, _ = oe.resolve_active_occasions(cal, date(2026, 6, 21))  # peak day
        assert active[0]["messaging_angle"] == "day-of"

    def test_outside_window_inactive(self):
        cal = {"occasions": [_occasion()]}
        active, errors = oe.resolve_active_occasions(cal, date(2026, 5, 1))
        assert active == [] and errors == []

    def test_day_after_peak_inactive(self):
        """After the peak, the next occurrence is ~1 year out — far
        beyond lead_days, so the occasion deactivates immediately."""
        cal = {"occasions": [_occasion()]}
        active, _ = oe.resolve_active_occasions(cal, date(2026, 6, 22))
        assert active == []

    def test_malformed_occasion_skipped_and_reported(self):
        cal = {"occasions": [
            {"id": "broken", "rule": {"type": "lunar_phase"}, "lead_days": 10},
            _occasion(),
        ]}
        active, errors = oe.resolve_active_occasions(cal, date(2026, 6, 11))
        assert len(active) == 1
        assert len(errors) == 1 and errors[0].startswith("broken:")


class TestSelector:
    def _active_occ(self):
        cal = {"occasions": [_occasion()]}
        active, _ = oe.resolve_active_occasions(cal, date(2026, 6, 11))
        return active[0]

    def test_classifier_occasion_outranks_keywords(self):
        # Product 2 needs >= floor (2.0) to be selected at all: 4 keyword
        # hits x 0.5 = 2.0. Product 1's classifier-occasion hit (3.0)
        # must still rank it first.
        items = {
            "1": _product("1", "Grill Master Duck", occasions=["test_fathers_day"]),
            "2": _product("2", "Random Duck", tags="dad, grill, for him, beer"),
        }
        occ = self._active_occ()
        occ = dict(occ, keywords=["dad", "grill", "for him", "beer"])
        picked = oe.select_products_for_occasion(occ, items)
        assert [p["product_id"] for p in picked] == ["1", "2"]
        assert "classifier_occasion" in picked[0]["reasons"]

    def test_score_floor_excludes_weak_matches(self):
        items = {"1": _product("1", "Plain Duck", tags="dad")}  # 0.5 < floor
        assert oe.select_products_for_occasion(self._active_occ(), items) == []

    def test_inactive_products_excluded(self):
        items = {"1": _product("1", "Draft Duck", status="draft",
                               occasions=["test_fathers_day"])}
        assert oe.select_products_for_occasion(self._active_occ(), items) == []

    def test_recipient_overlap_scores(self):
        items = {"1": _product("1", "Hubby Duck", recipients=["husband"], tags="")}
        # 1.5 recipient < 2.0 floor on its own
        assert oe.select_products_for_occasion(self._active_occ(), items) == []
        items = {"1": _product("1", "Hubby Duck", recipients=["husband", "dad"])}
        picked = oe.select_products_for_occasion(self._active_occ(), items)
        assert len(picked) == 1
        assert any(r.startswith("recipients:") for r in picked[0]["reasons"])

    def test_top_n_cap(self):
        items = {
            str(i): _product(str(i), f"Dad Duck {i:02d}", occasions=["test_fathers_day"])
            for i in range(20)
        }
        picked = oe.select_products_for_occasion(self._active_occ(), items)
        assert len(picked) == oe.TOP_N_PRODUCTS

    def test_empty_catalog_returns_empty(self):
        assert oe.select_products_for_occasion(self._active_occ(), {}) == []


class TestBuildAndWrite:
    def test_build_intel_shape(self):
        cal = {"calendar_version": 1, "occasions": [_occasion()]}
        catalog = {"generated_at": "2026-06-11T00:00:00",
                   "items": {"1": _product("1", occasions=["test_fathers_day"])}}
        intel = oe.build_occasion_intel(cal, catalog, date(2026, 6, 11))
        assert intel["catalog_products_seen"] == 1
        assert intel["active_occasions"][0]["product_count"] == 1
        assert intel["errors"] == []

    def test_missing_catalog_yields_empty_selection_not_crash(self):
        cal = {"occasions": [_occasion()]}
        intel = oe.build_occasion_intel(cal, {}, date(2026, 6, 11))
        assert intel["catalog_products_seen"] == 0
        assert intel["active_occasions"][0]["product_count"] == 0

    def test_write_is_atomic_and_isolated(self, tmp_path):
        # conftest already redirects OCCASION_INTEL_PATH to tmp.
        path = oe.write_occasion_intel({"generated_at": "x", "active_occasions": []})
        assert Path(path).exists()
        assert "occasion_intel.json" in str(path)
        assert not str(path).startswith(str(oe._FROZEN_PRODUCTION_OCCASION_INTEL_PATH.parent)) or \
            Path(path).resolve() != oe._FROZEN_PRODUCTION_OCCASION_INTEL_PATH

    def test_write_guard_refuses_prod_path_in_test_mode(self, monkeypatch):
        """Layer 2 of the three-layer policy: even if the conftest
        fixture is bypassed, the source guard refuses the frozen
        production path under DUCK_TEST_MODE=1."""
        monkeypatch.setattr(oe, "OCCASION_INTEL_PATH",
                            oe._FROZEN_PRODUCTION_OCCASION_INTEL_PATH)
        with pytest.raises(TestModeRefusalError):
            oe.write_occasion_intel({"generated_at": "x"})
