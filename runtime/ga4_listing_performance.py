#!/usr/bin/env python3
"""Surface 39 producer: pull per-listing on-site behavior from the Google
Analytics 4 Data API and classify each listing into a FIX / PROMOTE / WATCH
decision — the "Fix-or-Promote" lane.

Why this is a separate lane from Build-Next: Build-Next answers "what new
product to MAKE"; GA4 mostly answers "what to FIX or PROMOTE on what we ALREADY
sell" — monetizing traffic we already earned (usually higher ROI than making
new). Only the PROMOTE (proven-winner) slice loops back to Build-Next as a
"make more like this" signal; the bulk drives listing optimization.

The operator already has GA4 receiving listing traffic (Etsy's web-analytics
hookup → page titles are Etsy listing titles). This reads it via the Data API
runReport endpoint, reusing the same Google OAuth refresh->access pattern as
gsc_search_demand.py (scope differs: analytics.readonly; one refresh token can
carry both webmasters + analytics scopes).

Fail-soft (swallowed_errors_lie): missing creds or any API error writes an
available:false payload (error string = the loud channel) and exits 0 rather
than crashing the schedule. Three-layer write isolation (conftest redirect +
DUCK_TEST_MODE FROZEN-path guard + a no-pollution audit test).
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from statistics import quantiles
from typing import Any

from governance_review_common import DUCK_AGENT_ROOT, DUCK_OPS_ROOT, load_json, now_local_iso
from workflow_control import TestModeRefusalError

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GA4_RUN_REPORT_URL_TEMPLATE = (
    "https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
)
# Mint the refresh token with THIS scope (read-only Analytics).
GA4_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"

LISTING_PERFORMANCE_PATH = DUCK_OPS_ROOT / "state" / "listing_performance.json"
_FROZEN_PRODUCTION_LISTING_PERFORMANCE_PATH = LISTING_PERFORMANCE_PATH.resolve()

# Multi-window: PRIMARY drives the Fix/Promote classification; the others tag
# each listing rising / steady / fading by recent-vs-long view rate.
WINDOWS = (7, 28, 90)
PRIMARY_WINDOW = 28
# A listing needs at least this many views in the window to be classified —
# below it there isn't enough traffic to judge fix-vs-promote.
MIN_VIEWS_TO_JUDGE = 25
TOP_LIST_LIMIT = 50

# Dimensions + metrics requested from the GA4 Data API. hostName splits the
# property by domain so Etsy listing pages (etsy.com) and the Shopify store
# (myjeepduck.com) are attributed to the right CHANNEL — the property carries
# both because Etsy injects our GA tag into its listing pages.
_DIMENSIONS = ["pageTitle", "hostName"]
# GA4's runReport maximum per request; pages are walked until rowCount is met.
PAGE_SIZE = 250
MAX_REPORT_PAGES = 12
# Channels we actually sell on. A channel with no GA4 rows is reported as
# absent rather than simply missing from the output -- 2026-10-12: the property
# had never received a single myjeepduck.com pageview, and because the payload
# only ever listed the channels it found, the Shopify store looked covered.
EXPECTED_CHANNELS = ("etsy", "shopify")
# userEngagementDuration is total engagement seconds (a valid Data API metric);
# averageEngagementTimePerActiveUser is NOT a real metric — we derive the
# average ourselves from total / activeUsers.
_METRICS = [
    "screenPageViews", "activeUsers", "newUsers",
    "engagementRate", "bounceRate", "userEngagementDuration",
]


def _channel_for(host: str) -> str:
    """Map a GA4 hostName to a sales channel label."""
    h = (host or "").lower()
    if "etsy" in h:
        return "etsy"
    if "myjeepduck" in h or "myshopify" in h:
        return "shopify"
    return "web"


# --------------------------------------------------------------------------
# HTTP (one indirection so tests monkeypatch _request_json, never the network)
# --------------------------------------------------------------------------

def _request_json(request: urllib.request.Request, timeout: int = 30) -> dict[str, Any]:
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _safe_http_detail(exc: urllib.error.HTTPError) -> str:
    try:
        return exc.read().decode("utf-8")[:300]
    except Exception:
        return ""


def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# --------------------------------------------------------------------------
# Config + auth (mirrors gsc_search_demand / google_tasks_bridge)
# --------------------------------------------------------------------------

def _first(env: dict[str, str], names: list[str]) -> str:
    for n in names:
        v = (env.get(n) or "").strip()
        if v:
            return v
    return ""


def _property_ids(env: dict[str, str]) -> list[str]:
    """GA4_PROPERTY_ID accepts one id or several, comma- or space-separated.

    Kept as one variable rather than adding GA4_PROPERTY_ID_2 so there is no
    "which slot is Shopify" question and no silent ceiling at two.
    """
    raw = _first(env, ["GA4_PROPERTY_IDS", "GA4_PROPERTY_ID"])
    out: list[str] = []
    for chunk in raw.replace(",", " ").split():
        pid = chunk.strip().removeprefix("properties/")
        if pid and pid not in out:
            out.append(pid)
    return out


def ga4_config(env: dict[str, str] | None = None) -> dict[str, Any]:
    env = env if env is not None else os.environ
    # Existing Google OAuth client lives under GOOGLE_TASKS_* in .env.
    client_id = _first(env, ["GOOGLE_TASKS_CLIENT_ID", "GOOGLE_CLIENT_ID"])
    client_secret = _first(env, ["GOOGLE_TASKS_CLIENT_SECRET", "GOOGLE_CLIENT_SECRET"])
    # Reuse the GSC refresh token if it was minted with both scopes; else a
    # dedicated GA4 token.
    refresh_token = _first(env, ["GA4_REFRESH_TOKEN", "GSC_REFRESH_TOKEN"])
    # 2026-10-14: one property was never enough. The Shopify store reports to a
    # SEPARATE GA4 property (468950523, stream "My Website") from Etsy
    # (471407647, "My Etsy Jeep Site"), and GA4 cannot move a stream between
    # properties -- so a single-property read structurally cannot see both
    # channels. We read 471407647 only, so 4,281 myjeepduck.com pageviews in 28
    # days were invisible while `channel_coverage` correctly reported shopify
    # missing and we read that as "the store has no tag". It had a tag the whole
    # time, pointed somewhere we never looked.
    property_ids = _property_ids(env)
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        # First one stays the headline value so existing readers of
        # payload["property_id"] keep working.
        "property_id": property_ids[0] if property_ids else "",
        "property_ids": property_ids,
        "credentials_ready": bool(client_id and refresh_token and property_ids),
    }


def fetch_ga4_access_token(config: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    payload = {
        "client_id": config.get("client_id") or "",
        "refresh_token": config.get("refresh_token") or "",
        "grant_type": "refresh_token",
    }
    secret = str(config.get("client_secret") or "").strip()
    if secret:
        payload["client_secret"] = secret
    data = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(
        GOOGLE_TOKEN_URL, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
    try:
        body = _request_json(request)
    except urllib.error.HTTPError as exc:
        return None, {"ok": False, "error": f"token_http_{exc.code}", "detail": _safe_http_detail(exc)}
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return None, {"ok": False, "error": "token_network", "detail": str(exc)[:200]}
    token = str(body.get("access_token") or "")
    return (token or None), {"ok": bool(token)}


def _report_page(access_token: str, property_id: str, start_date: str, end_date: str,
                 *, offset: int) -> dict[str, Any]:
    url = GA4_RUN_REPORT_URL_TEMPLATE.format(property_id=urllib.parse.quote(property_id, safe=""))
    body = json.dumps({
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "dimensions": [{"name": d} for d in _DIMENSIONS],
        "metrics": [{"name": m} for m in _METRICS],
        "limit": PAGE_SIZE,
        "offset": offset,
    }).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    })
    return _request_json(request)


def run_report(access_token: str, property_id: str, start_date: str, end_date: str,
               ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return (rows, meta). Each row: {title, page_views, active_users, new_users,
    engagement_rate, bounce_rate, avg_engagement_time}.

    Paginated. Until 2026-10-12 this asked for a flat 250 rows and reported
    whatever came back, and the property held 278 pageTitle x hostName rows --
    so 28 listings were dropped on every run with nothing saying so, and
    `listing_count` sat at exactly 250 looking like a real total. The dropped
    tail is the low-traffic end, which is precisely what a "views but no sales"
    read is about ([[feedback_no_silent_caps]]). meta now carries
    `rows_available` and `truncated` so a future cap cannot be silent.
    """
    rows: list[dict[str, Any]] = []
    raw: list[dict[str, Any]] = []
    row_count = None
    for page in range(MAX_REPORT_PAGES):
        try:
            payload = _report_page(access_token, property_id, start_date, end_date,
                                   offset=page * PAGE_SIZE)
        except urllib.error.HTTPError as exc:
            if page == 0:
                return [], {"ok": False, "error": f"report_http_{exc.code}",
                            "detail": _safe_http_detail(exc)}
            # A later page failing would leave a partial set that looks whole.
            return [], {"ok": False, "error": f"report_http_{exc.code}",
                        "detail": f"page {page}: {_safe_http_detail(exc)}"}
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return [], {"ok": False, "error": "report_network", "detail": str(exc)[:200]}
        batch = payload.get("rows") or []
        raw.extend(batch)
        if row_count is None:
            row_count = payload.get("rowCount")
        if len(batch) < PAGE_SIZE:
            break
        if isinstance(row_count, int) and len(raw) >= row_count:
            break
    truncated = bool(isinstance(row_count, int) and len(raw) < row_count)
    for r in raw:
        dims = r.get("dimensionValues") or []
        mets = r.get("metricValues") or []
        title = str(dims[0].get("value")).strip() if len(dims) >= 1 else ""
        host = str(dims[1].get("value")).strip() if len(dims) >= 2 else ""
        if not title or len(mets) < len(_METRICS):
            continue
        vals = [_num(m.get("value")) for m in mets]
        active = vals[1]
        # vals[5] is TOTAL engagement seconds; average per active user is derived.
        avg_engagement = round(vals[5] / active, 1) if active else 0.0
        rows.append({
            "title": title,
            "host": host,
            "channel": _channel_for(host),
            "page_views": vals[0],
            "active_users": active,
            "new_users": vals[2],
            "engagement_rate": round(vals[3], 4),
            "bounce_rate": round(vals[4], 4),
            "avg_engagement_time": avg_engagement,
        })
    return rows, {"ok": True, "rows_available": row_count,
                  "rows_fetched": len(raw), "truncated": truncated}


# --------------------------------------------------------------------------
# Classification (relative terciles, deterministic)
# --------------------------------------------------------------------------

def _tercile_thresholds(values: list[float]) -> tuple[float, float]:
    """(low_cut, high_cut) at the 33rd/67th percentiles. Falls back to the
    single value when the sample is too small for quantiles."""
    vals = [v for v in values if v is not None]
    if len(vals) < 3:
        v = max(vals, default=0.0)
        return v, v
    cuts = quantiles(vals, n=3)  # two cut points
    return cuts[0], cuts[1]


def _classify_cohort(cohort: list[dict[str, Any]], *, min_views: int,
                     channel: str) -> list[dict[str, Any]]:
    """Classify one channel's listings against ITS OWN terciles (Etsy and
    Shopify traffic baselines aren't comparable, so each is judged internally)."""
    judged = [r for r in cohort if r["page_views"] >= min_views]
    view_lo, view_hi = _tercile_thresholds([r["page_views"] for r in judged])
    eng_lo, eng_hi = _tercile_thresholds([r["engagement_rate"] for r in judged])
    label = f" on {channel}" if channel and channel != "web" else ""
    out: list[dict[str, Any]] = []
    for r in cohort:
        verdict, reason = "neutral", f"only {int(r['page_views'])} views{label} — not enough traffic to judge"
        if r["page_views"] >= min_views:
            high_views = r["page_views"] >= view_hi
            low_views = r["page_views"] <= view_lo
            high_eng = r["engagement_rate"] >= eng_hi
            low_eng = r["engagement_rate"] <= eng_lo
            if high_views and low_eng:
                verdict = "fix"
                reason = (f"high traffic{label} ({int(r['page_views'])} views) + low engagement "
                          f"({r['engagement_rate']:.0%}) — conversion leak, fix photos/price/copy")
            elif high_views and high_eng:
                verdict = "promote"
                reason = (f"proven winner{label} — {int(r['page_views'])} views at "
                          f"{r['engagement_rate']:.0%} engagement; promote / make variations")
            elif low_views and high_eng:
                verdict = "watch"
                reason = (f"engages well ({r['engagement_rate']:.0%}) but starved of traffic"
                          f"{label} ({int(r['page_views'])} views) — needs SEO/ads")
        out.append({**r, "verdict": verdict, "reason": reason})
    return out


def classify_listings(rows: list[dict[str, Any]], *,
                      min_views: int = MIN_VIEWS_TO_JUDGE) -> list[dict[str, Any]]:
    """Attach a verdict (fix|promote|watch|neutral) + reason to each row,
    judged PER CHANNEL (etsy vs shopify) so a big channel can't swamp a small
    one. High/low are terciles within each channel's judged set."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        groups.setdefault(r.get("channel") or "web", []).append(r)
    out: list[dict[str, Any]] = []
    for channel, cohort in groups.items():
        out.extend(_classify_cohort(cohort, min_views=min_views, channel=channel))
    return out


# --------------------------------------------------------------------------
# Collect + payload
# --------------------------------------------------------------------------

def _today_iso() -> str:
    return date.today().isoformat()


def _days_before(end_iso: str, days: int) -> str:
    return (date.fromisoformat(end_iso) - timedelta(days=days)).isoformat()


def _channel_totals(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """Per-channel (etsy / shopify / web) roll-up so Etsy vs Shopify traffic is
    visible at a glance."""
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        ch = out.setdefault(r.get("channel") or "web",
                            {"listings": 0, "page_views": 0, "active_users": 0})
        ch["listings"] += 1
        ch["page_views"] += int(r["page_views"])
        ch["active_users"] += int(r["active_users"])
    return out


def _trend(views_by_window: dict[str, int], windows: tuple[int, ...]) -> str:
    """rising / steady / fading / new / flat from short-vs-long daily view rate."""
    short, long = min(windows), max(windows)
    v_s = views_by_window.get(str(short), 0)
    v_l = views_by_window.get(str(long), 0)
    if v_l <= 0:
        return "new" if v_s > 0 else "flat"
    ratio = (v_s / short) / (v_l / long)
    if ratio >= 1.5:
        return "rising"
    if ratio <= 0.5:
        return "fading"
    return "steady"


def channel_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Which channels GA4 actually has data for, including the ones it does not.

    A payload that lists only the channels it found cannot distinguish "the
    Shopify store had no traffic" from "the Shopify store has no analytics tag",
    and the second is what was true: zero myjeepduck.com pageviews had ever
    reached this property, while `channels` showed a healthy etsy entry and
    nothing else. The absent channel has to be named to be noticed
    ([[feedback_plausible_fallbacks_mask_failure]]).
    """
    seen: dict[str, int] = {}
    for r in rows:
        ch = r.get("channel") or "web"
        seen[ch] = seen.get(ch, 0) + 1
    missing = [c for c in EXPECTED_CHANNELS if not seen.get(c)]
    return {
        "present": {c: seen[c] for c in sorted(seen)},
        "missing": missing,
        "complete": not missing,
        "note": ("" if not missing else
                 f"No GA4 rows for: {', '.join(missing)}. Either the channel had "
                 f"no traffic in the window or it has no analytics tag sending to "
                 f"this property; the second cannot be ruled out from here."),
    }


def _merge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fold rows from several properties into one set, keyed by (title, host).

    Two properties can carry the same page title -- a duck's name appears on
    both its Etsy listing and its Shopify product page -- and the host is what
    separates them. Additive metrics sum; the rates are averages, so they are
    re-weighted by active users rather than averaged, which would let a 3-user
    page outvote a 300-user one.
    """
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r.get("title") or "", r.get("host") or "")
        cur = merged.get(key)
        if cur is None:
            merged[key] = dict(r)
            continue
        a, b = cur.get("active_users") or 0.0, r.get("active_users") or 0.0
        total = a + b
        for field in ("page_views", "active_users", "new_users"):
            cur[field] = (cur.get(field) or 0.0) + (r.get(field) or 0.0)
        for field in ("engagement_rate", "bounce_rate", "avg_engagement_time"):
            if total:
                cur[field] = round(
                    ((cur.get(field) or 0.0) * a + (r.get(field) or 0.0) * b) / total,
                    4 if field.endswith("_rate") else 1)
    return list(merged.values())


def _merge_report_meta(metas: list[dict[str, Any]]) -> dict[str, Any]:
    """One meta across properties. Truncation anywhere is truncation."""
    ok = [m for m in metas if m.get("ok")]
    avail = [m.get("rows_available") for m in ok if isinstance(m.get("rows_available"), int)]
    return {
        "ok": bool(ok),
        "rows_available": sum(avail) if avail else None,
        "rows_fetched": sum(int(m.get("rows_fetched") or 0) for m in ok),
        "truncated": any(bool(m.get("truncated")) for m in ok),
        "properties": [{"property_id": m.get("property_id"), "ok": bool(m.get("ok")),
                        "rows_fetched": m.get("rows_fetched"), "error": m.get("error")}
                       for m in metas],
    }


def _first_error(metas: list[dict[str, Any]]) -> str:
    for m in metas:
        if not m.get("ok") and m.get("error"):
            return str(m["error"])
    return ""


def build_payload(per_window: dict[int, list[dict[str, Any]]], *,
                  windows: tuple[int, ...], primary_window: int,
                  property_ids: list[str] | None = None, property_id: str | None = None,
                  available: bool = True, error: str | None = None,
                  report_meta: dict[str, Any] | None = None,
                  property_errors: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    pids = list(property_ids) if property_ids else ([property_id] if property_id else [])
    primary_rows = per_window.get(primary_window, [])
    classified = classify_listings(primary_rows)
    # Keyed by (title, host), not title: the same duck name appears on its Etsy
    # listing and its Shopify page, and with two properties merged a title-only
    # key silently credits one channel's views to the other.
    views_by_window = {w: {(r["title"], r.get("host") or ""): int(r["page_views"])
                           for r in per_window.get(w, [])}
                       for w in windows}
    for r in classified:
        key = (r["title"], r.get("host") or "")
        wmap = {str(w): views_by_window.get(w, {}).get(key, 0) for w in windows}
        r["views_by_window"] = wmap
        r["trend"] = _trend(wmap, windows)
    classified.sort(key=lambda r: r["page_views"], reverse=True)
    by = lambda v: [r for r in classified if r["verdict"] == v]  # noqa: E731
    return {
        "generated_at": now_local_iso(),
        "available": available,
        "source": "google_analytics_4",
        "property_id": pids[0] if pids else "",
        "property_ids": pids,
        "property_errors": property_errors or [],
        "windows": list(windows),
        "primary_window": primary_window,
        "listing_count": len(primary_rows),
        "totals": {
            "page_views": int(sum(r["page_views"] for r in primary_rows)),
            "active_users": int(sum(r["active_users"] for r in primary_rows)),
            "new_users": int(sum(r["new_users"] for r in primary_rows)),
        },
        "channels": _channel_totals(primary_rows),
        "channel_coverage": channel_coverage(primary_rows),
        "rows_available": (report_meta or {}).get("rows_available"),
        "rows_truncated": bool((report_meta or {}).get("truncated")),
        "fix": by("fix")[:TOP_LIST_LIMIT],
        "promote": by("promote")[:TOP_LIST_LIMIT],
        "watch": by("watch")[:TOP_LIST_LIMIT],
        "listings": classified[:TOP_LIST_LIMIT],
        "error": error,
    }


def collect(config: dict[str, Any], *, windows: tuple[int, ...] = WINDOWS,
            primary_window: int = PRIMARY_WINDOW, today: str | None = None) -> dict[str, Any]:
    """token -> per-window runReport -> classify primary. Always returns a payload;
    a primary-window failure degrades to available:false, a secondary window that
    fails is omitted. Never raises."""
    pids = list(config.get("property_ids") or ([config["property_id"]] if config.get("property_id") else []))

    def empty(err: str) -> dict[str, Any]:
        return build_payload({}, windows=windows, primary_window=primary_window,
                             property_ids=pids, available=False, error=err)

    if not config.get("credentials_ready"):
        return empty("credentials_not_ready")
    token, tok_meta = fetch_ga4_access_token(config)
    if not token:
        return empty(tok_meta.get("error") or "token_unavailable")
    end = today or _today_iso()
    per_window: dict[int, list[dict[str, Any]]] = {}
    primary_meta: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []
    for w in windows:
        collected: list[dict[str, Any]] = []
        metas: list[dict[str, Any]] = []
        ok_any = False
        for pid in pids:
            rows, meta = run_report(token, pid, _days_before(end, w), end)
            meta = dict(meta, property_id=pid)
            metas.append(meta)
            if meta.get("ok"):
                ok_any = True
                collected.extend(rows)
            elif w == primary_window:
                # One property failing must not look like no data at all, and
                # must not pass silently either ([[feedback_no_silent_caps]]).
                failures.append({"property_id": pid, "error": meta.get("error"),
                                 "detail": meta.get("detail")})
        if w == primary_window:
            primary_meta = _merge_report_meta(metas)
            if not ok_any:
                return empty(_first_error(metas) or "report_failed")
        per_window[w] = _merge_rows(collected) if ok_any else []
    return build_payload(per_window, windows=windows, primary_window=primary_window,
                         property_ids=pids, available=True, report_meta=primary_meta,
                         property_errors=failures)


# --------------------------------------------------------------------------
# Write (three-layer isolation)
# --------------------------------------------------------------------------

def write_listing_performance(payload: dict[str, Any], path: Any = None) -> Path:
    out = Path(path or LISTING_PERFORMANCE_PATH)
    if os.environ.get("DUCK_TEST_MODE") == "1" and \
            out.resolve() == _FROZEN_PRODUCTION_LISTING_PERFORMANCE_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but LISTING_PERFORMANCE_PATH still points at the "
            "production state file. Monkeypatch "
            "ga4_listing_performance.LISTING_PERFORMANCE_PATH to a tmp path.")
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
        description="Collect GA4 per-listing performance and classify Fix-or-Promote.")
    parser.add_argument("--dry-run", action="store_true", help="Print summary, don't write")
    args = parser.parse_args()

    if not (os.environ.get("GA4_REFRESH_TOKEN") or os.environ.get("GSC_REFRESH_TOKEN")):
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
        except Exception:
            pass

    payload = collect(ga4_config())
    avail = payload.get("available")
    print(f"[ga4-listing-performance] available={avail} windows={payload.get('windows')} "
          f"listings={payload.get('listing_count')} fix={len(payload.get('fix') or [])} "
          f"promote={len(payload.get('promote') or [])} watch={len(payload.get('watch') or [])}"
          + ("" if avail else f"  error={payload.get('error')}"))
    for r in (payload.get("fix") or [])[:5]:
        print(f"  FIX     [{r.get('trend','?'):>6}] {int(r['page_views']):>5} views  {r['engagement_rate']:.0%} eng  {r['title'][:46]}")
    for r in (payload.get("promote") or [])[:5]:
        print(f"  PROMOTE [{r.get('trend','?'):>6}] {int(r['page_views']):>5} views  {r['engagement_rate']:.0%} eng  {r['title'][:46]}")
    if not args.dry_run:
        out = write_listing_performance(payload)
        print(f"  -> {out}")
    elif not avail and payload.get("error") == "credentials_not_ready":
        print("  [note] set GA4_PROPERTY_ID + GA4_REFRESH_TOKEN (or a both-scopes GSC token) in .env, "
              "then re-run. Mint with --scope https://www.googleapis.com/auth/analytics.readonly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
