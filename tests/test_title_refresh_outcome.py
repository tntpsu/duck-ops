"""Did rewriting 60 Etsy titles on 2026-09-08/09 change anything?

Etsy is 70% of revenue, so this is the most valuable thing we can learn about
that channel -- which is exactly why it must not come back with a confident
wrong answer. Three things would each produce one:

* **Season.** The window is September to October, with Halloween ducks launching
  inside it. A treated-only number measures autumn, so the ~218 never-touched
  listings are the control arm over the same window.
* **Selection.** The treated listings were picked as the top performers, so
  regression to the mean pushes against the treatment and a raw comparison
  understates a real effect. Hence the within-strata comparison too.
* **Skew.** One listing holds 49,932 lifetime views against a median near 1,100.
  A mean is that one listing's opinion, so every headline is a median.

The tests below pin those rules and the refusals, not today's numbers
([[feedback_pin_the_rule_not_the_reading]]) -- the live figures move daily.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from etsy_title_refresh_outcome import (
    MIN_ARM_SIZE,
    build_outcome,
    load_baseline,
    load_treated,
    units_by_listing,
)

BASELINE_AT = datetime.fromisoformat("2026-09-07T00:00:00-04:00")
NOW = datetime.fromisoformat("2026-10-12T00:00:00-04:00")


def _baseline(n, *, start_id=1000, views=1000, favorites=50, age_days=400):
    """n listings with identical history, so any arm difference is the treatment."""
    return {
        start_id + i: {
            "listing_id": start_id + i,
            "views": views,
            "favorites": favorites,
            "title": f"listing {i}",
            "age_days": age_days,
        }
        for i in range(n)
    }


def _current(baseline, *, views_gain, favorites_gain=5):
    return {
        lid: {"listing_id": lid, "views": row["views"] + views_gain,
              "num_favorers": row["favorites"] + favorites_gain}
        for lid, row in baseline.items()
    }


def _run(treated_gain, control_gain, *, n=12, **over):
    """Treated and control arms identical except for views gained."""
    treated = _baseline(n, start_id=1000)
    control = _baseline(n, start_id=2000)
    baseline = {**treated, **control}
    current = {**_current(treated, views_gain=treated_gain),
               **_current(control, views_gain=control_gain)}
    kwargs = {
        "baseline": baseline,
        "current": current,
        "applied": set(treated),
        "failed": set(),
        "units_pre": {},
        "units_post": {},
        "baseline_at": BASELINE_AT,
        "now": NOW,
    }
    kwargs.update(over)
    return build_outcome(**kwargs)


# --- the arms ---------------------------------------------------------------

def test_a_lift_in_the_treated_arm_only_is_reported_as_a_lift():
    out = _run(treated_gain=200, control_gain=100)
    assert out["available"] is True
    assert out["views"]["verdict"] == "improved"
    assert out["views"]["lift_ratio"] == pytest.approx(1.0)  # 2x the control


def test_the_same_movement_in_both_arms_is_the_season_not_the_titles():
    """The whole reason the control arm exists."""
    out = _run(treated_gain=200, control_gain=200)
    assert out["views"]["verdict"] == "no_effect"
    assert out["views"]["lift_ratio"] == pytest.approx(0.0)


def test_a_drop_against_the_control_is_reported_as_worse():
    """A rewrite can hurt. The measurement has to be able to say so."""
    out = _run(treated_gain=50, control_gain=200)
    assert out["views"]["verdict"] == "worse"
    assert out["views"]["lift_ratio"] < 0


def test_a_listing_whose_apply_failed_is_in_neither_arm():
    """Rank 20 came back HTTP 400 'quantity cannot be empty'. Counting it as
    treated measures a title that was never written; counting it as control
    measures one we tried to change."""
    treated = _baseline(12, start_id=1000)
    control = _baseline(12, start_id=2000)
    broken = _baseline(1, start_id=3000)
    baseline = {**treated, **control, **broken}
    current = {**_current(treated, views_gain=200), **_current(control, views_gain=100),
               **_current(broken, views_gain=9999)}
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(broken),
        units_pre={}, units_post={}, baseline_at=BASELINE_AT, now=NOW,
    )
    assert out["treated_count"] == 12
    assert out["control_count"] == 12
    assert out["excluded"]["apply_failed"] == [3000]
    # the 9999-view outlier must not have reached either arm
    assert out["views"]["lift_ratio"] == pytest.approx(1.0)


# --- missing and degenerate data -------------------------------------------

def test_a_listing_that_vanished_is_excluded_not_read_as_zero():
    """Sold out or expired listings disappear from the active fetch. Treating a
    missing listing as 0 views would invent a catastrophic decline."""
    treated = _baseline(12, start_id=1000)
    control = _baseline(12, start_id=2000)
    baseline = {**treated, **control}
    current = {**_current(treated, views_gain=200), **_current(control, views_gain=200)}
    gone = sorted(treated)[0]
    del current[gone]
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(),
        units_pre={}, units_post={}, baseline_at=BASELINE_AT, now=NOW,
    )
    assert out["excluded"]["missing_now"] == [gone]
    assert out["treated_count"] == 11
    assert out["views"]["verdict"] == "no_effect"


def test_a_listing_created_after_the_baseline_is_in_neither_arm():
    """It has no before, so it cannot contribute a change."""
    treated = _baseline(12, start_id=1000)
    control = _baseline(12, start_id=2000)
    baseline = {**treated, **control}
    current = {**_current(treated, views_gain=200), **_current(control, views_gain=200)}
    current[9999] = {"listing_id": 9999, "views": 500, "num_favorers": 10}
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(),
        units_pre={}, units_post={}, baseline_at=BASELINE_AT, now=NOW,
    )
    assert out["excluded"]["new_since_baseline"] == [9999]
    assert out["treated_count"] + out["control_count"] == 24


def test_a_listing_with_no_age_is_excluded_from_the_ratio_not_divided_by_zero():
    treated = _baseline(12, start_id=1000)
    control = _baseline(12, start_id=2000)
    treated[1000]["age_days"] = 0
    baseline = {**treated, **control}
    current = {**_current(treated, views_gain=200), **_current(control, views_gain=100)}
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(),
        units_pre={}, units_post={}, baseline_at=BASELINE_AT, now=NOW,
    )
    assert 1000 in out["excluded"]["no_pre_period"]
    assert out["views"]["verdict"] == "improved"


def test_a_negative_delta_is_kept_and_flagged_never_clamped():
    """Etsy recounts views occasionally. Clamping to 0 would hide a data problem
    behind a plausible number ([[feedback_plausible_fallbacks_mask_failure]])."""
    treated = _baseline(12, start_id=1000)
    control = _baseline(12, start_id=2000)
    baseline = {**treated, **control}
    current = {**_current(treated, views_gain=200), **_current(control, views_gain=200)}
    current[1000]["views"] = baseline[1000]["views"] - 40
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(),
        units_pre={}, units_post={}, baseline_at=BASELINE_AT, now=NOW,
    )
    assert 1000 in out["negative_view_deltas"]
    assert out["treated_count"] == 12, "the row was dropped instead of flagged"


# --- refusals ---------------------------------------------------------------

def test_too_few_listings_refuses_to_give_a_number():
    out = _run(treated_gain=200, control_gain=100, n=MIN_ARM_SIZE - 1)
    assert out["views"]["verdict"] == "insufficient_data"
    assert "lift_ratio" not in out["views"] or out["views"]["lift_ratio"] is None


def test_a_zero_length_window_refuses():
    out = _run(treated_gain=200, control_gain=100, now=BASELINE_AT)
    assert out["available"] is False
    assert "window" in out["reason"]


# --- sales ------------------------------------------------------------------

def test_sales_are_compared_across_equal_windows():
    treated = _baseline(12, start_id=1000)
    control = _baseline(12, start_id=2000)
    baseline = {**treated, **control}
    current = {**_current(treated, views_gain=100), **_current(control, views_gain=100)}
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(),
        units_pre={lid: 1 for lid in treated} | {lid: 1 for lid in control},
        units_post={lid: 3 for lid in treated} | {lid: 1 for lid in control},
        baseline_at=BASELINE_AT, now=NOW,
    )
    assert out["sales"]["treated_units_post"] == 36
    assert out["sales"]["control_units_post"] == 12
    assert out["sales"]["verdict"] == "improved"


def test_a_listing_with_no_sales_in_either_window_is_a_real_zero():
    """Unlike a missing listing, no sales is a measurement, not an absence."""
    out = _run(treated_gain=100, control_gain=100)
    assert out["sales"]["treated_units_pre"] == 0
    assert out["sales"]["treated_units_post"] == 0
    assert out["sales"]["verdict"] in {"no_effect", "insufficient_data"}


def test_units_are_bucketed_by_transaction_timestamp():
    start = int(datetime(2026, 9, 9, tzinfo=timezone.utc).timestamp())
    end = int(datetime(2026, 10, 12, tzinfo=timezone.utc).timestamp())
    txns = [
        {"listing_id": 7, "quantity": 2, "created_timestamp": start + 100},
        {"listing_id": 7, "quantity": 1, "created_timestamp": end + 100},   # after
        {"listing_id": 8, "quantity": 5, "created_timestamp": start - 100},  # before
    ]
    assert units_by_listing(txns, start, end) == {7: 2}


# --- honesty ----------------------------------------------------------------

def test_the_payload_carries_its_own_caveats():
    """A reader must not be able to quote the lift without the confounds."""
    out = _run(treated_gain=200, control_gain=100)
    blob = " ".join(out["caveats"]).lower()
    assert "top" in blob or "selection" in blob
    assert "season" in blob or "halloween" in blob


def test_a_win_rate_corroborates_the_median():
    """Share of treated listings beating the control median. Near 0.5 when the
    arms are drawn from the same spread, so it catches a median that one or two
    big rows moved on their own."""
    treated = _baseline(12, start_id=1000)
    control = _baseline(12, start_id=2000)
    baseline = {**treated, **control}
    # Both arms spread over the same range of gains, interleaved.
    gains = [60, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280]
    current = {}
    for lid, gain in zip(sorted(treated), gains):
        current[lid] = {"listing_id": lid, "views": treated[lid]["views"] + gain,
                        "num_favorers": treated[lid]["favorites"] + 5}
    for lid, gain in zip(sorted(control), gains):
        current[lid] = {"listing_id": lid, "views": control[lid]["views"] + gain,
                        "num_favorers": control[lid]["favorites"] + 5}
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(),
        units_pre={}, units_post={}, baseline_at=BASELINE_AT, now=NOW,
    )
    assert out["views"]["verdict"] == "no_effect"
    assert out["views"]["treated_win_rate"] == pytest.approx(0.5, abs=0.09)

    lifted = _run(treated_gain=200, control_gain=100)
    assert lifted["views"]["treated_win_rate"] == pytest.approx(1.0)


# --- loaders ----------------------------------------------------------------

def test_the_ledger_splits_applied_from_failed(tmp_path):
    import json
    p = tmp_path / "ledger.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in [
        {"action": "apply", "rank": 1, "listing_id": 11, "result": "ok"},
        {"action": "apply", "rank": 2, "listing_id": 22,
         "result": "error: [etsy] update listing failed: HTTP 400"},
        {"action": "apply", "rank": 1, "listing_id": 11, "result": "ok"},  # retry
    ]))
    applied, failed = load_treated(p)
    assert applied == {11}
    assert failed == {22}


def test_a_listing_that_failed_then_succeeded_counts_as_applied(tmp_path):
    import json
    p = tmp_path / "ledger.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in [
        {"action": "apply", "listing_id": 33, "result": "error: HTTP 400"},
        {"action": "apply", "listing_id": 33, "result": "ok"},
    ]))
    applied, failed = load_treated(p)
    assert applied == {33}
    assert failed == set()


def test_the_baseline_loads_views_favorites_and_title(tmp_path):
    import json
    p = tmp_path / "baseline.json"
    p.write_text(json.dumps([
        {"listing_id": 5, "views": 100, "num_favorers": 7, "title": "A duck",
         "state": "active"},
    ]))
    rows = load_baseline(p)
    assert rows[5]["views"] == 100
    assert rows[5]["favorites"] == 7


def test_a_missing_baseline_file_never_raises(tmp_path):
    assert load_baseline(tmp_path / "nope.json") == {}


def test_sales_are_also_compared_within_size_strata():
    """The aggregate sales figure cannot separate the rewrite from regression to
    the mean: the treated arm was picked for selling well (285 units pre-window
    against the control's 54). Only like-for-like buckets can."""
    small = _baseline(10, start_id=1000, views=100)      # treated, small
    big = _baseline(10, start_id=1100, views=2000)       # treated, big
    small_c = _baseline(10, start_id=2000, views=100)    # control, small
    baseline = {**small, **big, **small_c}
    current = {**_current(small, views_gain=100), **_current(big, views_gain=100),
               **_current(small_c, views_gain=100)}
    out = build_outcome(
        baseline=baseline, current=current,
        applied=set(small) | set(big), failed=set(),
        units_pre={**{l: 1 for l in small}, **{l: 5 for l in big},
                   **{l: 1 for l in small_c}},
        units_post={**{l: 2 for l in small}, **{l: 4 for l in big},
                    **{l: 1 for l in small_c}},
        baseline_at=BASELINE_AT, now=NOW,
    )
    by = {r["stratum"]: r for r in out["sales_by_stratum"]}
    # The small bucket has a control to compare against; the big one does not.
    assert by["0-250"]["comparable"] is True
    assert by["0-250"]["treated_change"] == pytest.approx(1.0)   # 10 -> 20 units
    assert by["0-250"]["control_change"] == pytest.approx(0.0)
    assert by["1.5k-3k"]["comparable"] is False, "a bucket with no control arm"
    assert by["1.5k-3k"]["control_units_pre"] == 0


def test_a_stratum_without_a_control_arm_is_named_not_dropped():
    """The six largest treated listings have no same-size control. Hiding that
    would make the result look more complete than it is."""
    treated = _baseline(12, start_id=1000, views=9000)
    control = _baseline(12, start_id=2000, views=100)
    baseline = {**treated, **control}
    current = {**_current(treated, views_gain=200), **_current(control, views_gain=100)}
    out = build_outcome(
        baseline=baseline, current=current, applied=set(treated), failed=set(),
        units_pre={}, units_post={}, baseline_at=BASELINE_AT, now=NOW,
    )
    strata = {r["stratum"]: r for r in out["views_by_stratum"]}
    assert strata["3k+"]["comparable"] is False
    assert strata["3k+"]["lift_ratio"] is None
    assert strata["3k+"]["treated_n"] == 12


class TestAnUnmeasurableAggregateSaysSo:
    """The first live run reported `sales: worse, lift -0.78`.

    That number was an artifact: 73% of the treated arm's pre-window units sat
    in the two largest strata, which have 1 and 0 control listings. Those are
    the shop's best sellers -- the listings most subject to regression to the
    mean, and the ones whose season (college football) ended inside the window.
    Where a control arm actually exists the same data shows treated BEATING
    control. A reader quoting the aggregate would have concluded the rewrite
    hurt sales and stopped doing it.
    """

    def _split(self, *, treated_small_units, control_small_units,
               treated_big_units, control_big_n):
        small_t = _baseline(10, start_id=1000, views=100)
        big_t = _baseline(10, start_id=1100, views=9000)
        small_c = _baseline(10, start_id=2000, views=100)
        big_c = _baseline(control_big_n, start_id=2100, views=9000) if control_big_n else {}
        baseline = {**small_t, **big_t, **small_c, **big_c}
        current = {}
        for grp in (small_t, big_t, small_c, big_c):
            current.update(_current(grp, views_gain=100))
        units_pre, units_post = {}, {}
        for lid in small_t:
            units_pre[lid], units_post[lid] = 1, treated_small_units
        for lid in big_t:
            units_pre[lid], units_post[lid] = 20, treated_big_units
        for lid in small_c:
            units_pre[lid], units_post[lid] = 1, control_small_units
        for lid in big_c:
            units_pre[lid], units_post[lid] = 20, 20
        return build_outcome(
            baseline=baseline, current=current,
            applied=set(small_t) | set(big_t), failed=set(),
            units_pre=units_pre, units_post=units_post,
            baseline_at=BASELINE_AT, now=NOW,
        )

    def test_an_aggregate_dominated_by_uncontrolled_strata_is_confounded(self):
        out = self._split(treated_small_units=3, control_small_units=1,
                          treated_big_units=5, control_big_n=0)
        assert out["sales"]["verdict"] == "confounded_by_selection"
        assert out["sales"]["lift_ratio"] is None, \
            "a confounded aggregate must not also publish a number"
        assert "control" in out["sales"]["reason"]
        assert out["sales"]["uncontrolled_units_share"] > 0.5

    def test_the_comparable_strata_are_reported_separately_and_keep_their_verdict(self):
        """The usable half of the answer must survive the confounded aggregate."""
        out = self._split(treated_small_units=3, control_small_units=1,
                          treated_big_units=5, control_big_n=0)
        assert out["sales"]["comparable_verdict"] == "improved"
        assert out["sales"]["comparable_lift_ratio"] == pytest.approx(2.0)
        assert out["sales"]["comparable_treated_n"] == 10

    def test_a_balanced_aggregate_is_judged_normally(self):
        """The guard must not fire when every stratum has a control arm --
        otherwise it suppresses every future result."""
        out = self._split(treated_small_units=3, control_small_units=1,
                          treated_big_units=60, control_big_n=10)
        assert out["sales"]["verdict"] in {"improved", "no_effect", "worse"}
        assert out["sales"]["uncontrolled_units_share"] == pytest.approx(0.0)
