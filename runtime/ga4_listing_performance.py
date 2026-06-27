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

DEFAULT_WINDOW_DAYS = 28
# A listing needs at least this many views in the window to be classified —
# below it there isn't enough traffic to judge fix-vs-promote.
MIN_VIEWS_TO_JUDGE = 25
TOP_LIST_LIMIT = 50

# Dimensions + metrics requested from the GA4 Data API. hostName splits the
# property by domain so Etsy listing pages (etsy.com) and the Shopify store
# (myjeepduck.com) are attributed to the right CHANNEL — the property carries
# both because Etsy injects our GA tag into its listing pages.
_DIMENSIONS = ["pageTitle", "hostName"]
_METRICS = [
    "screenPageViews", "activeUsers", "newUsers",
    "engagementRate", "bounceRate", "averageEngagementTimePerActiveUser",
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

def ga4_config(env: dict[str, str] | None = None) -> dict[str, Any]:
    env = env if env is not None else os.environ
    client_id = (env.get("GOOGLE_CLIENT_ID") or "").strip()
    client_secret = (env.get("GOOGLE_CLIENT_SECRET") or "").strip()
    # Reuse the GSC refresh token if it was minted with both scopes; else a
    # dedicated GA4 token.
    refresh_token = (env.get("GA4_REFRESH_TOKEN") or env.get("GSC_REFRESH_TOKEN") or "").strip()
    property_id = (env.get("GA4_PROPERTY_ID") or "").strip().removeprefix("properties/")
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "property_id": property_id,
        "credentials_ready": bool(client_id and refresh_token and property_id),
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


def run_report(access_token: str, property_id: str, start_date: str, end_date: str,
               ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return (rows, meta). Each row: {title, page_views, active_users, new_users,
    engagement_rate, bounce_rate, avg_engagement_time}."""
    url = GA4_RUN_REPORT_URL_TEMPLATE.format(property_id=urllib.parse.quote(property_id, safe=""))
    body = json.dumps({
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "dimensions": [{"name": d} for d in _DIMENSIONS],
        "metrics": [{"name": m} for m in _METRICS],
        "limit": 250,
    }).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    })
    try:
        payload = _request_json(request)
    except urllib.error.HTTPError as exc:
        return [], {"ok": False, "error": f"report_http_{exc.code}", "detail": _safe_http_detail(exc)}
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return [], {"ok": False, "error": "report_network", "detail": str(exc)[:200]}
    rows: list[dict[str, Any]] = []
    for r in payload.get("rows") or []:
        dims = r.get("dimensionValues") or []
        mets = r.get("metricValues") or []
        title = str(dims[0].get("value")).strip() if len(dims) >= 1 else ""
        host = str(dims[1].get("value")).strip() if len(dims) >= 2 else ""
        if not title or len(mets) < len(_METRICS):
            continue
        vals = [_num(m.get("value")) for m in mets]
        rows.append({
            "title": title,
            "host": host,
            "channel": _channel_for(host),
            "page_views": vals[0],
            "active_users": vals[1],
            "new_users": vals[2],
            "engagement_rate": round(vals[3], 4),
            "bounce_rate": round(vals[4], 4),
            "avg_engagement_time": round(vals[5], 1),
        })
    return rows, {"ok": True}


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


def build_payload(rows: list[dict[str, Any]], *, window_days: int, property_id: str,
                  available: bool = True, error: str | None = None) -> dict[str, Any]:
    classified = classify_listings(rows)
    classified.sort(key=lambda r: r["page_views"], reverse=True)
    by = lambda v: [r for r in classified if r["verdict"] == v]  # noqa: E731
    return {
        "generated_at": now_local_iso(),
        "available": available,
        "source": "google_analytics_4",
        "property_id": property_id,
        "window_days": window_days,
        "listing_count": len(rows),
        "totals": {
            "page_views": int(sum(r["page_views"] for r in rows)),
            "active_users": int(sum(r["active_users"] for r in rows)),
            "new_users": int(sum(r["new_users"] for r in rows)),
        },
        "channels": _channel_totals(rows),
        "fix": by("fix")[:TOP_LIST_LIMIT],
        "promote": by("promote")[:TOP_LIST_LIMIT],
        "watch": by("watch")[:TOP_LIST_LIMIT],
        "listings": classified[:TOP_LIST_LIMIT],
        "error": error,
    }


def collect(config: dict[str, Any], *, window_days: int = DEFAULT_WINDOW_DAYS,
            today: str | None = None) -> dict[str, Any]:
    """token -> runReport -> classify. Always returns a payload; never raises."""
    pid = config.get("property_id") or ""
    if not config.get("credentials_ready"):
        return build_payload([], window_days=window_days, property_id=pid,
                             available=False, error="credentials_not_ready")
    token, tok_meta = fetch_ga4_access_token(config)
    if not token:
        return build_payload([], window_days=window_days, property_id=pid,
                             available=False, error=tok_meta.get("error") or "token_unavailable")
    end = today or _today_iso()
    start = _days_before(end, window_days)
    rows, meta = run_report(token, pid, start, end)
    if not meta.get("ok"):
        return build_payload([], window_days=window_days, property_id=pid,
                             available=False, error=meta.get("error") or "report_failed")
    return build_payload(rows, window_days=window_days, property_id=pid, available=True)


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
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    args = parser.parse_args()

    if not (os.environ.get("GA4_REFRESH_TOKEN") or os.environ.get("GSC_REFRESH_TOKEN")):
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
        except Exception:
            pass

    payload = collect(ga4_config(), window_days=args.window_days)
    avail = payload.get("available")
    print(f"[ga4-listing-performance] available={avail} listings={payload.get('listing_count')} "
          f"fix={len(payload.get('fix') or [])} promote={len(payload.get('promote') or [])} "
          f"watch={len(payload.get('watch') or [])}"
          + ("" if avail else f"  error={payload.get('error')}"))
    for r in (payload.get("fix") or [])[:5]:
        print(f"  FIX      {int(r['page_views']):>5} views  {r['engagement_rate']:.0%} eng  {r['title'][:50]}")
    for r in (payload.get("promote") or [])[:5]:
        print(f"  PROMOTE  {int(r['page_views']):>5} views  {r['engagement_rate']:.0%} eng  {r['title'][:50]}")
    if not args.dry_run:
        out = write_listing_performance(payload)
        print(f"  -> {out}")
    elif not avail and payload.get("error") == "credentials_not_ready":
        print("  [note] set GA4_PROPERTY_ID + GA4_REFRESH_TOKEN (or a both-scopes GSC token) in .env, "
              "then re-run. Mint with --scope https://www.googleapis.com/auth/analytics.readonly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
