"""Surface 86: did the September Etsy title rewrite move anything?

On 2026-09-08/09, 60 listings had their titles and tags rewritten. A snapshot of
all 279 active listings was taken the day before. Etsy is ~70% of revenue, so
whether listing copy pays is the most valuable thing we can learn about that
channel -- and nothing had read the baseline back.

`views` and `num_favorers` from the Etsy API are lifetime cumulative, so
`now - baseline` is exactly what a listing earned in the window. The hard part
is not the subtraction, it is the three ways this measurement lies if you let it:

* Season -- the window is September to October with Halloween ducks launching
  inside it, so the ~218 untouched listings are a control arm over the same days.
* Selection -- the treated listings were picked as the top performers, so
  regression to the mean works against the treatment and a raw comparison
  understates it. The comparison is therefore also run within baseline-view
  strata.
* Skew -- one listing holds 49,932 lifetime views against a median near 1,100,
  so every headline figure is a median, never a mean.

Read-only against Etsy. Fails closed: a failed fetch writes
``available: false`` with the error rather than a plausible 0.0 lift
([[feedback_plausible_fallbacks_mask_failure]]).
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable

from governance_review_common import DUCK_OPS_ROOT, now_local_iso
from workflow_control import TestModeRefusalError

BASELINE_PATH = DUCK_OPS_ROOT / "state" / "etsy_title_refresh" / "my_all_listings.json"
LEDGER_PATH = DUCK_OPS_ROOT / "state" / "etsy_title_refresh_ledger.jsonl"
TITLE_REFRESH_OUTCOME_PATH = DUCK_OPS_ROOT / "state" / "etsy_title_refresh_outcome.json"
_FROZEN_PRODUCTION_TITLE_REFRESH_OUTCOME_PATH = TITLE_REFRESH_OUTCOME_PATH.resolve()

# The day the baseline snapshot was taken (titles applied the next two days).
BASELINE_CAPTURED_AT = "2026-09-07T00:00:00-04:00"

# Below this, an arm is a handful of listings and any median is noise. Eight is
# the smallest arm where a median is not just one row's opinion.
MIN_ARM_SIZE = 8

# A lift smaller than this is inside the noise of two medians over ~35 days.
EFFECT_THRESHOLD = 0.10

CAVEATS = (
    "The treated listings were selected as the shop's top performers, so "
    "regression to the mean pushes against the treatment: a real gain is "
    "understated here, not overstated.",
    "The window spans the start of the Halloween season, which is why every "
    "figure is a treated-vs-control difference rather than a treated-only "
    "change.",
    "Medians, not means -- one listing holds roughly 40x the median's lifetime "
    "views and would otherwise set the result on its own.",
    "A point estimate with no significance test. Treat a lift under ~20% as a "
    "direction to re-measure, not a proven effect.",
)


# --- loaders ----------------------------------------------------------------

def load_baseline(path: Any = None) -> dict[int, dict[str, Any]]:
    """listing_id -> {views, favorites, title, age_days}. Never raises."""
    p = Path(path or BASELINE_PATH)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    rows = raw.get("results") if isinstance(raw, dict) else raw
    out: dict[int, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            lid = int(row["listing_id"])
        except (KeyError, TypeError, ValueError):
            continue
        out[lid] = {
            "listing_id": lid,
            "views": int(row.get("views") or 0),
            "favorites": int(row.get("num_favorers") or 0),
            "title": str(row.get("title") or ""),
            # Filled in from the live fetch, which is the only place the
            # creation timestamp is available -- the baseline snapshot did not
            # record it.
            "age_days": row.get("age_days"),
        }
    return out


def load_treated(path: Any = None) -> tuple[set[int], set[int]]:
    """(applied_ok, failed_only). A listing that failed then succeeded on a
    retry is applied; rank 20's HTTP 400 never succeeded and belongs to neither
    arm."""
    p = Path(path or LEDGER_PATH)
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
    except OSError:
        return set(), set()
    ok: set[int] = set()
    errored: set[int] = set()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("action") != "apply":
            continue
        try:
            lid = int(row["listing_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if str(row.get("result") or "").strip() == "ok":
            ok.add(lid)
        else:
            errored.add(lid)
    return ok, errored - ok


def units_by_listing(transactions: Iterable[dict[str, Any]],
                     start_ts: int, end_ts: int) -> dict[int, int]:
    """Units sold per listing in [start_ts, end_ts). The transactions endpoint
    ignores min_created/max_created and returns lifetime newest-first
    ([[reference_etsy_transactions_ignores_date_window]]), so the window is
    applied here."""
    out: dict[int, int] = {}
    for txn in transactions or []:
        if not isinstance(txn, dict):
            continue
        created = txn.get("created_timestamp")
        if not isinstance(created, (int, float)):
            continue
        if not (start_ts <= created < end_ts):
            continue
        try:
            lid = int(txn["listing_id"])
        except (KeyError, TypeError, ValueError):
            continue
        out[lid] = out.get(lid, 0) + int(txn.get("quantity") or 0)
    return out


# --- the comparison ---------------------------------------------------------

def _verdict(lift: float | None, n_a: int, n_b: int) -> str:
    if lift is None or n_a < MIN_ARM_SIZE or n_b < MIN_ARM_SIZE:
        return "insufficient_data"
    if lift >= EFFECT_THRESHOLD:
        return "improved"
    if lift <= -EFFECT_THRESHOLD:
        return "worse"
    return "no_effect"


def _lift(treated: float | None, control: float | None) -> float | None:
    """Treated relative to control. None when the control cannot divide."""
    if treated is None or control is None or control == 0:
        return None
    return treated / control - 1.0


def _stratum(views: int) -> str:
    """Baseline-size bucket, so a treated listing is compared against controls
    of similar standing rather than against the whole shop."""
    for edge, label in ((250, "0-250"), (750, "250-750"), (1500, "750-1.5k"),
                        (3000, "1.5k-3k")):
        if views < edge:
            return label
    return "3k+"


def _arm_stats(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    """Unrounded on purpose: rounding a median and then dividing two of them
    turns an exact 2x into 0.9999. Rounding happens once, on output."""
    per_day = [r[f"{key}_per_day"] for r in rows]
    ratios = [r[f"{key}_ratio"] for r in rows if r[f"{key}_ratio"] is not None]
    return {
        "n": len(rows),
        "median_per_day": median(per_day) if per_day else None,
        "median_ratio": median(ratios) if ratios else None,
        "n_with_ratio": len(ratios),
        "total_gained": sum(r[f"{key}_gained"] for r in rows),
    }


def _rounded(stats: dict[str, Any]) -> dict[str, Any]:
    out = dict(stats)
    for k in ("median_per_day", "median_ratio"):
        if out.get(k) is not None:
            out[k] = round(out[k], 4)
    return out


def _metric_block(treated: list[dict[str, Any]], control: list[dict[str, Any]],
                  key: str) -> dict[str, Any]:
    t = _arm_stats(treated, key)
    c = _arm_stats(control, key)
    # The ratio-of-ratios is the headline: each listing is first measured
    # against its own pre-period rate, which absorbs the size difference
    # between the arms, and only then compared across arms.
    lift = _lift(t["median_ratio"], c["median_ratio"])
    if lift is None:
        lift = _lift(t["median_per_day"], c["median_per_day"])
    verdict = _verdict(lift, t["n"], c["n"])
    block = {
        "treated": _rounded(t),
        "control": _rounded(c),
        # An arm too small to judge must not also publish a number, or the
        # number gets quoted without the verdict that disowns it.
        "lift_ratio": (None if lift is None or verdict == "insufficient_data"
                       else round(lift, 4)),
        "verdict": verdict,
    }
    # Share of treated listings above the control median -- 0.5 under no effect.
    c_mid = c["median_ratio"] if c["median_ratio"] is not None else c["median_per_day"]
    t_vals = [r[f"{key}_ratio"] if r[f"{key}_ratio"] is not None else r[f"{key}_per_day"]
              for r in treated]
    if c_mid is not None and t_vals:
        wins = sum(1 for v in t_vals if v > c_mid)
        ties = sum(1 for v in t_vals if v == c_mid)
        block["treated_win_rate"] = round((wins + 0.5 * ties) / len(t_vals), 4)
    else:
        block["treated_win_rate"] = None
    return block


def _strata_block(treated: list[dict[str, Any]], control: list[dict[str, Any]],
                  key: str) -> list[dict[str, Any]]:
    """The same comparison inside each baseline-size bucket. This is the arm of
    the analysis that answers the selection-bias objection."""
    buckets: dict[str, dict[str, list]] = {}
    for row in treated:
        buckets.setdefault(row["stratum"], {"t": [], "c": []})["t"].append(row)
    for row in control:
        buckets.setdefault(row["stratum"], {"t": [], "c": []})["c"].append(row)
    out = []
    for name in sorted(buckets):
        t_rows, c_rows = buckets[name]["t"], buckets[name]["c"]
        t = _arm_stats(t_rows, key) if t_rows else {"n": 0, "median_ratio": None,
                                                   "median_per_day": None}
        c = _arm_stats(c_rows, key) if c_rows else {"n": 0, "median_ratio": None,
                                                    "median_per_day": None}
        strat_lift = (None if not (t["n"] and c["n"])
                      else _lift(t.get("median_ratio"), c.get("median_ratio")))
        out.append({
            "stratum": name,
            "treated_n": t["n"],
            "control_n": c["n"],
            "treated_median_ratio": _rounded(t).get("median_ratio"),
            "control_median_ratio": _rounded(c).get("median_ratio"),
            "lift_ratio": None if strat_lift is None else round(strat_lift, 4),
            # Named rather than dropped, so a bucket with no comparison is
            # visible instead of quietly missing.
            "comparable": bool(t["n"] and c["n"]),
        })
    return out


def _sales_strata_block(treated: list[dict[str, Any]],
                        control: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Units pre/post inside each baseline-size bucket.

    The aggregate sales figure cannot distinguish the rewrite from regression to
    the mean, because the treated arm was chosen for selling well: 285 units
    pre-window against the control's 54. Comparing like-sized listings is the
    only way to see past that.
    """
    buckets: dict[str, dict[str, list]] = {}
    for row in treated:
        buckets.setdefault(row["stratum"], {"t": [], "c": []})["t"].append(row)
    for row in control:
        buckets.setdefault(row["stratum"], {"t": [], "c": []})["c"].append(row)
    out = []
    for name in sorted(buckets):
        t_rows, c_rows = buckets[name]["t"], buckets[name]["c"]
        t_pre = sum(r["units_pre"] for r in t_rows)
        t_post = sum(r["units_post"] for r in t_rows)
        c_pre = sum(r["units_pre"] for r in c_rows)
        c_post = sum(r["units_post"] for r in c_rows)
        t_chg = (t_post / t_pre - 1.0) if t_pre else None
        c_chg = (c_post / c_pre - 1.0) if c_pre else None
        out.append({
            "stratum": name,
            "treated_n": len(t_rows), "control_n": len(c_rows),
            "treated_units_pre": t_pre, "treated_units_post": t_post,
            "control_units_pre": c_pre, "control_units_post": c_post,
            "treated_change": None if t_chg is None else round(t_chg, 4),
            "control_change": None if c_chg is None else round(c_chg, 4),
            "lift_ratio": (None if t_chg is None or c_chg is None
                           else round(t_chg - c_chg, 4)),
            "comparable": bool(t_pre and c_pre),
        })
    return out


def build_outcome(*, baseline: dict[int, dict[str, Any]],
                  current: dict[int, dict[str, Any]],
                  applied: set[int], failed: set[int],
                  units_pre: dict[int, int], units_post: dict[int, int],
                  baseline_at: datetime, now: datetime) -> dict[str, Any]:
    """Compare what the treated listings earned against what the untouched ones
    earned over the same days. Never raises."""
    window_days = (now - baseline_at).total_seconds() / 86400.0
    if window_days <= 0:
        return {
            "generated_at": now_local_iso(),
            "available": False,
            "reason": f"window is not positive ({window_days:.2f} days)",
        }

    excluded: dict[str, list[int]] = {
        "apply_failed": sorted(failed),
        "missing_now": [],
        "new_since_baseline": sorted(set(current) - set(baseline)),
        "no_pre_period": [],
    }
    negative_views: list[int] = []
    treated_rows: list[dict[str, Any]] = []
    control_rows: list[dict[str, Any]] = []

    for lid, base in sorted(baseline.items()):
        if lid in failed:
            continue
        live = current.get(lid)
        if not live:
            # Sold out or expired. Reading this as 0 views would invent a
            # catastrophic decline out of a listing simply leaving the shop.
            excluded["missing_now"].append(lid)
            continue
        views_gained = int(live.get("views") or 0) - int(base["views"])
        favs_gained = int(live.get("num_favorers") or 0) - int(base["favorites"])
        if views_gained < 0:
            negative_views.append(lid)

        age_days = base.get("age_days") or live.get("age_days")
        try:
            age_days = float(age_days) if age_days is not None else 0.0
        except (TypeError, ValueError):
            age_days = 0.0
        has_pre = age_days > 0
        if not has_pre:
            excluded["no_pre_period"].append(lid)

        row = {
            "listing_id": lid,
            "title": base["title"][:90],
            "stratum": _stratum(int(base["views"])),
            "baseline_views": int(base["views"]),
            "views_gained": views_gained,
            "views_per_day": views_gained / window_days,
            # Each listing against its own history: lifetime rate before the
            # rewrite versus the rate since it.
            "views_ratio": ((views_gained / window_days) / (int(base["views"]) / age_days)
                            if has_pre and int(base["views"]) > 0 else None),
            "favorites_gained": favs_gained,
            "favorites_per_day": favs_gained / window_days,
            "favorites_ratio": ((favs_gained / window_days)
                                / (int(base["favorites"]) / age_days)
                                if has_pre and int(base["favorites"]) > 0 else None),
            "units_pre": int(units_pre.get(lid, 0)),
            "units_post": int(units_post.get(lid, 0)),
        }
        (treated_rows if lid in applied else control_rows).append(row)

    views = _metric_block(treated_rows, control_rows, "views")
    favorites = _metric_block(treated_rows, control_rows, "favorites")

    t_pre = sum(r["units_pre"] for r in treated_rows)
    t_post = sum(r["units_post"] for r in treated_rows)
    c_pre = sum(r["units_pre"] for r in control_rows)
    c_post = sum(r["units_post"] for r in control_rows)
    # Each arm against itself first, so the arms' different sizes cancel.
    t_change = (t_post / t_pre - 1.0) if t_pre else None
    c_change = (c_post / c_pre - 1.0) if c_pre else None
    sales_lift = (None if t_change is None or c_change is None
                  else t_change - c_change)
    sales = {
        "treated_units_pre": t_pre, "treated_units_post": t_post,
        "control_units_pre": c_pre, "control_units_post": c_post,
        "treated_change": None if t_change is None else round(t_change, 4),
        "control_change": None if c_change is None else round(c_change, 4),
        "lift_ratio": None if sales_lift is None else round(sales_lift, 4),
        "verdict": _verdict(sales_lift, len(treated_rows), len(control_rows)),
    }
    if sales["verdict"] == "insufficient_data":
        sales["lift_ratio"] = None
    if t_pre == 0 and t_post == 0:
        # No sales either side is a real measurement, not a missing one -- but
        # it cannot support a verdict.
        sales["verdict"] = "no_effect" if c_post == 0 else "insufficient_data"

    # An aggregate the strata cannot support must not publish a number.
    # First live run: 73% of treated pre-window units sat in the two largest
    # strata, which hold 1 and 0 control listings. Those are the best sellers --
    # the most mean-reverting listings in the shop, whose season ended inside
    # the window -- so the aggregate read "worse" while every stratum that had
    # a control arm read better. The usable half of the answer is kept.
    sales_strata = _sales_strata_block(treated_rows, control_rows)
    uncontrolled = sum(r["treated_units_pre"] for r in sales_strata
                       if not r["comparable"])
    sales["uncontrolled_units_share"] = round(uncontrolled / t_pre, 4) if t_pre else 0.0

    comparable = [r for r in sales_strata if r["comparable"]]
    ct_pre = sum(r["treated_units_pre"] for r in comparable)
    ct_post = sum(r["treated_units_post"] for r in comparable)
    cc_pre = sum(r["control_units_pre"] for r in comparable)
    cc_post = sum(r["control_units_post"] for r in comparable)
    ct_change = (ct_post / ct_pre - 1.0) if ct_pre else None
    cc_change = (cc_post / cc_pre - 1.0) if cc_pre else None
    comp_lift = (None if ct_change is None or cc_change is None
                 else ct_change - cc_change)
    comp_n = sum(r["treated_n"] for r in comparable)
    sales["comparable_treated_n"] = comp_n
    sales["comparable_control_n"] = sum(r["control_n"] for r in comparable)
    sales["comparable_lift_ratio"] = None if comp_lift is None else round(comp_lift, 4)
    sales["comparable_verdict"] = _verdict(
        comp_lift, comp_n, sum(r["control_n"] for r in comparable))

    if sales["uncontrolled_units_share"] > 0.5:
        sales["verdict"] = "confounded_by_selection"
        sales["lift_ratio"] = None
        sales["reason"] = (
            f"{sales['uncontrolled_units_share']:.0%} of the treated arm's "
            f"pre-window units are in size strata with no control listings, so "
            f"the aggregate cannot separate the rewrite from regression to the "
            f"mean. See comparable_verdict for the half that can be measured.")

    return {
        "generated_at": now_local_iso(),
        "available": True,
        "baseline_at": baseline_at.isoformat(),
        "measured_at": now.isoformat(),
        "window_days": round(window_days, 2),
        "treated_count": len(treated_rows),
        "control_count": len(control_rows),
        "excluded": excluded,
        "negative_view_deltas": sorted(negative_views),
        "views": views,
        "favorites": favorites,
        "sales": sales,
        "views_by_stratum": _strata_block(treated_rows, control_rows, "views"),
        "sales_by_stratum": _sales_strata_block(treated_rows, control_rows),
        "caveats": list(CAVEATS),
        "treated_listings": sorted(treated_rows,
                                   key=lambda r: r["views_gained"], reverse=True)[:25],
    }


# --- live data --------------------------------------------------------------

def fetch_current() -> dict[int, dict[str, Any]]:
    """Live active listings with lifetime views, favourites and age. Raises on
    failure -- the caller turns that into available: false."""
    import sys
    duck_agent = "/Users/philtullai/ai-agents/duckAgent"
    if duck_agent not in sys.path:
        sys.path.insert(0, duck_agent)
    from dotenv import load_dotenv
    load_dotenv(f"{duck_agent}/.env", override=True)
    from helpers.etsy_helper import etsy_get_shop_listings

    now = datetime.now(timezone.utc)
    out: dict[int, dict[str, Any]] = {}
    for row in etsy_get_shop_listings(state="active", limit=100, max_pages=20):
        try:
            lid = int(row["listing_id"])
        except (KeyError, TypeError, ValueError):
            continue
        created = row.get("original_creation_timestamp")
        age = None
        if isinstance(created, (int, float)) and created > 0:
            age = (now - datetime.fromtimestamp(created, timezone.utc)).days
        out[lid] = {
            "listing_id": lid,
            "views": int(row.get("views") or 0),
            "num_favorers": int(row.get("num_favorers") or 0),
            "age_days": age,
            "title": str(row.get("title") or ""),
        }
    return out


def fetch_transactions_since(oldest_ts: int, *, max_pages: int = 40) -> list[dict[str, Any]]:
    """Newest-first transactions down to oldest_ts, then stop. The endpoint
    ignores its own date parameters, so the stop is ours to make."""
    import sys
    duck_agent = "/Users/philtullai/ai-agents/duckAgent"
    if duck_agent not in sys.path:
        sys.path.insert(0, duck_agent)
    from dotenv import load_dotenv
    load_dotenv(f"{duck_agent}/.env", override=True)
    from helpers.etsy_helper import etsy_get_shop_transactions

    shop_id = os.getenv("ETSY_SHOP_ID")
    if not shop_id:
        raise RuntimeError("Missing ETSY_SHOP_ID.")
    rows: list[dict[str, Any]] = []
    for page in range(max_pages):
        batch = (etsy_get_shop_transactions(shop_id, limit=100,
                                            offset=page * 100).get("results") or [])
        rows.extend(batch)
        if len(batch) < 100:
            break
        oldest_in_batch = min((r.get("created_timestamp") or 0) for r in batch)
        if oldest_in_batch <= oldest_ts:
            break
    return rows


def build_from_live(*, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc).astimezone()
    baseline_at = datetime.fromisoformat(BASELINE_CAPTURED_AT)
    baseline = load_baseline()
    applied, failed = load_treated()
    if not baseline or not applied:
        return {"generated_at": now_local_iso(), "available": False,
                "reason": f"baseline={len(baseline)} listings, treated={len(applied)}"}
    try:
        current = fetch_current()
    except Exception as exc:  # noqa: BLE001 - any failure must fail closed
        return {"generated_at": now_local_iso(), "available": False,
                "reason": f"etsy listing fetch failed: {exc}"}

    # Age is only on the live record; carry it onto the baseline rows and roll
    # it back to the baseline date so the pre-period rate is a pre-period rate.
    window_days = (now - baseline_at).total_seconds() / 86400.0
    for lid, row in baseline.items():
        live_age = (current.get(lid) or {}).get("age_days")
        if isinstance(live_age, (int, float)):
            row["age_days"] = max(live_age - window_days, 0.0)

    post_start = baseline_at + timedelta(days=2)      # titles applied 09-09
    post_end = now
    span = post_end - post_start
    pre_start, pre_end = post_start - span, post_start
    units_pre: dict[int, int] = {}
    units_post: dict[int, int] = {}
    sales_error = None
    try:
        txns = fetch_transactions_since(int(pre_start.timestamp()))
        units_pre = units_by_listing(txns, int(pre_start.timestamp()),
                                     int(pre_end.timestamp()))
        units_post = units_by_listing(txns, int(post_start.timestamp()),
                                      int(post_end.timestamp()))
    except Exception as exc:  # noqa: BLE001
        sales_error = str(exc)

    payload = build_outcome(
        baseline=baseline, current=current, applied=applied, failed=failed,
        units_pre=units_pre, units_post=units_post, baseline_at=baseline_at, now=now,
    )
    if payload.get("available"):
        payload["sales_windows"] = {
            "pre": [pre_start.date().isoformat(), pre_end.date().isoformat()],
            "post": [post_start.date().isoformat(), post_end.date().isoformat()],
        }
        if sales_error:
            # Views and favourites are still real; only the sales half is not.
            payload["sales"] = {"available": False, "reason": sales_error,
                                "verdict": "insufficient_data"}
    return payload


def write_outcome(payload: dict[str, Any], path: Any = None) -> Path:
    out = Path(path or TITLE_REFRESH_OUTCOME_PATH)
    if os.environ.get("DUCK_TEST_MODE") == "1" and \
            out.resolve() == _FROZEN_PRODUCTION_TITLE_REFRESH_OUTCOME_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but TITLE_REFRESH_OUTCOME_PATH still points at "
            "production. Monkeypatch "
            "etsy_title_refresh_outcome.TITLE_REFRESH_OUTCOME_PATH to a tmp path.")
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure the September Etsy title rewrite against an untouched control arm.")
    parser.add_argument("--dry-run", action="store_true", help="Print, don't write")
    args = parser.parse_args()
    payload = build_from_live()
    if not payload.get("available"):
        print(f"[title-refresh] unavailable: {payload.get('reason')}")
    else:
        v, f, s = payload["views"], payload["favorites"], payload["sales"]
        print(f"[title-refresh] window={payload['window_days']}d "
              f"treated={payload['treated_count']} control={payload['control_count']}")
        for name, blk in (("views", v), ("favorites", f)):
            print(f"  {name:10s} verdict={blk['verdict']:17s} "
                  f"lift={blk['lift_ratio']} win_rate={blk.get('treated_win_rate')}")
            print(f"             treated median/day={blk['treated']['median_per_day']} "
                  f"ratio={blk['treated']['median_ratio']} | "
                  f"control median/day={blk['control']['median_per_day']} "
                  f"ratio={blk['control']['median_ratio']}")
        print(f"  sales      verdict={s['verdict']} lift={s.get('lift_ratio')} "
              f"treated {s.get('treated_units_pre')}->{s.get('treated_units_post')} "
              f"control {s.get('control_units_pre')}->{s.get('control_units_post')}")
    if not args.dry_run:
        print(f"  -> {write_outcome(payload)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
