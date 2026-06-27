#!/usr/bin/env python3
"""Surface 38 producer: pull FIRST-PARTY search demand from Google Search
Console — the real queries shoppers typed on Google to reach myjeepduck.com —
and write state/gsc_search_demand.json for Build-Next.

Why GSC and not Shopify: Shopify exposes no search-term API (ShopifyQL covers
sales/sessions/customers only; verified 2026-06-26). GSC is the clean-API path
and reuses the same Google OAuth refresh->access pattern as
google_tasks_bridge.py — only the scope differs (webmasters.readonly, minted
once via google_tasks_oauth_helper.py with SCOPE swapped).

Producer-on-schedule + cheap-reader: weekly (Sunday ~07:15, BEFORE build-next
~07:30). Build-Next reads term_scores as a soft demand factor.

Fail-soft (swallowed_errors_lie): missing creds or any API error writes an
available:false payload (with the error string as the LOUD channel) and exits
0 rather than crashing the schedule. The two OS bracket cards alarm on
stale/unavailable feed and on a dead gap signal.

Three-layer write isolation (conftest redirect + DUCK_TEST_MODE FROZEN-path
guard + a no-pollution audit test), per the cross-repo-state convention.
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
from typing import Any

from governance_review_common import DUCK_AGENT_ROOT, DUCK_OPS_ROOT, load_json, now_local_iso
from workflow_control import TestModeRefusalError
# Reuse Build-Next's tokenizer so "covered vs gap" is judged with the SAME
# stopwords/normalization the scorer uses (single source of truth, no drift).
from build_next_engine import _tokens

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GSC_QUERY_URL_TEMPLATE = (
    "https://searchconsole.googleapis.com/webmasters/v3/sites/{site}/searchAnalytics/query"
)
# Mint the refresh token with THIS scope (read-only Search Console).
GSC_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"

GSC_SEARCH_DEMAND_PATH = DUCK_OPS_ROOT / "state" / "gsc_search_demand.json"
_FROZEN_PRODUCTION_GSC_SEARCH_DEMAND_PATH = GSC_SEARCH_DEMAND_PATH.resolve()
CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"

DEFAULT_WINDOW_DAYS = 28
DEFAULT_ROW_LIMIT = 250
# A query needs at least this many impressions to count as a real unmet-demand
# "gap" — filters one-off long-tail noise from the operator-facing list.
GAP_MIN_IMPRESSIONS = 30
# Clicks are a stronger intent signal than impressions; weight them up when
# turning a query into per-token demand.
CLICK_WEIGHT = 10.0
TOP_LIST_LIMIT = 25


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


# --------------------------------------------------------------------------
# Config + auth (mirrors google_tasks_bridge.fetch_google_access_token)
# --------------------------------------------------------------------------

def gsc_config(env: dict[str, str] | None = None) -> dict[str, Any]:
    env = env if env is not None else os.environ
    client_id = (env.get("GOOGLE_CLIENT_ID") or "").strip()
    client_secret = (env.get("GOOGLE_CLIENT_SECRET") or "").strip()
    refresh_token = (env.get("GSC_REFRESH_TOKEN") or "").strip()
    site_url = (env.get("GSC_SITE_URL") or "").strip()
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "site_url": site_url,
        "credentials_ready": bool(client_id and refresh_token and site_url),
    }


def fetch_gsc_access_token(config: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
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


def query_search_analytics(access_token: str, site_url: str, start_date: str, end_date: str, *,
                           dimensions: tuple[str, ...] = ("query",),
                           row_limit: int = DEFAULT_ROW_LIMIT) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    url = GSC_QUERY_URL_TEMPLATE.format(site=urllib.parse.quote(site_url, safe=""))
    body = json.dumps({
        "startDate": start_date, "endDate": end_date,
        "dimensions": list(dimensions), "rowLimit": row_limit,
    }).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    })
    try:
        payload = _request_json(request)
    except urllib.error.HTTPError as exc:
        return [], {"ok": False, "error": f"query_http_{exc.code}", "detail": _safe_http_detail(exc)}
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return [], {"ok": False, "error": "query_network", "detail": str(exc)[:200]}
    rows: list[dict[str, Any]] = []
    for r in payload.get("rows") or []:
        keys = r.get("keys") or []
        query = str(keys[0]).strip() if keys else ""
        if not query:
            continue
        rows.append({
            "query": query,
            "clicks": float(r.get("clicks") or 0),
            "impressions": float(r.get("impressions") or 0),
            "ctr": round(float(r.get("ctr") or 0), 4),
            "position": round(float(r.get("position") or 0), 1),
        })
    return rows, {"ok": True}


# --------------------------------------------------------------------------
# Aggregation
# --------------------------------------------------------------------------

def _public_row(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "query": r["query"],
        "clicks": int(r["clicks"]),
        "impressions": int(r["impressions"]),
        "ctr": r.get("ctr", 0),
        "position": r.get("position", 0),
    }


def aggregate_search_demand(rows: list[dict[str, Any]], catalog_tokens: set[str], *,
                            gap_min_impressions: int = GAP_MIN_IMPRESSIONS) -> dict[str, Any]:
    """term_scores: each query's (clicks*W + impressions) accrues to its tokens,
    max-normalized to 0..1. gap_queries: meaningful-impression queries whose
    tokens have NO catalog overlap (= unmet demand)."""
    term_weight: dict[str, float] = {}
    gap_rows: list[dict[str, Any]] = []
    for r in rows:
        toks = _tokens(r["query"])
        if not toks:
            continue
        weight = r["clicks"] * CLICK_WEIGHT + r["impressions"]
        for t in toks:
            term_weight[t] = term_weight.get(t, 0.0) + weight
        covered = bool(toks & catalog_tokens)
        if not covered and r["impressions"] >= gap_min_impressions:
            gap_rows.append(r)
    max_w = max(term_weight.values(), default=0.0)
    term_scores = ({t: round(w / max_w, 4) for t, w in term_weight.items()}
                   if max_w > 0 else {})
    top_queries = sorted(rows, key=lambda r: (r["clicks"], r["impressions"]), reverse=True)
    gap_rows.sort(key=lambda r: (r["impressions"], r["clicks"]), reverse=True)
    return {
        "term_scores": term_scores,
        "top_queries": [_public_row(r) for r in top_queries[:TOP_LIST_LIMIT]],
        "gap_queries": [_public_row(r) for r in gap_rows[:TOP_LIST_LIMIT]],
    }


def _load_catalog_tokens(path: Any = None) -> set[str]:
    data = load_json(path or CATALOG_INDEX_PATH, {})
    items = data.get("items") if isinstance(data, dict) else None
    toks: set[str] = set()
    if isinstance(items, dict):
        for it in items.values():
            if isinstance(it, dict):
                toks |= _tokens(it.get("title"))
                toks |= _tokens(it.get("core_terms"))
    return toks


def build_payload(rows: list[dict[str, Any]], catalog_tokens: set[str], *,
                  window_days: int, site_url: str,
                  available: bool = True, error: str | None = None) -> dict[str, Any]:
    agg = aggregate_search_demand(rows, catalog_tokens)
    return {
        "generated_at": now_local_iso(),
        "available": available,
        "source": "google_search_console",
        "site_url": site_url,
        "window_days": window_days,
        "query_count": len(rows),
        "error": error,
        **agg,
    }


def _today_iso() -> str:
    return date.today().isoformat()


def _days_before(end_iso: str, days: int) -> str:
    return (date.fromisoformat(end_iso) - timedelta(days=days)).isoformat()


def collect(config: dict[str, Any], *, window_days: int = DEFAULT_WINDOW_DAYS,
            today: str | None = None, catalog_tokens: set[str] | None = None) -> dict[str, Any]:
    """Orchestrate token -> query -> aggregate. Always returns a payload; on any
    failure it's available:false with an error, never a raise."""
    site = config.get("site_url") or ""
    if not config.get("credentials_ready"):
        return build_payload([], set(), window_days=window_days, site_url=site,
                              available=False, error="credentials_not_ready")
    cat = catalog_tokens if catalog_tokens is not None else _load_catalog_tokens()
    token, tok_meta = fetch_gsc_access_token(config)
    if not token:
        return build_payload([], cat, window_days=window_days, site_url=site,
                             available=False, error=tok_meta.get("error") or "token_unavailable")
    end = today or _today_iso()
    start = _days_before(end, window_days)
    rows, q_meta = query_search_analytics(token, site, start, end)
    if not q_meta.get("ok"):
        return build_payload([], cat, window_days=window_days, site_url=site,
                             available=False, error=q_meta.get("error") or "query_failed")
    return build_payload(rows, cat, window_days=window_days, site_url=site, available=True)


# --------------------------------------------------------------------------
# Write (three-layer isolation: conftest redirect + this guard + audit test)
# --------------------------------------------------------------------------

def write_search_demand(payload: dict[str, Any], path: Any = None) -> Path:
    out = Path(path or GSC_SEARCH_DEMAND_PATH)
    if os.environ.get("DUCK_TEST_MODE") == "1" and \
            out.resolve() == _FROZEN_PRODUCTION_GSC_SEARCH_DEMAND_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but GSC_SEARCH_DEMAND_PATH still points at the "
            "production state file. Monkeypatch gsc_search_demand.GSC_SEARCH_DEMAND_PATH "
            "to a tmp path.")
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
        description="Collect Google Search Console first-party search demand for Build-Next.")
    parser.add_argument("--dry-run", action="store_true", help="Print summary, don't write")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    args = parser.parse_args()

    # Credentials live in duckAgent/.env (per the env-in-dotenv convention);
    # load it so a launchd-run job sees GSC_REFRESH_TOKEN/GSC_SITE_URL.
    if not os.environ.get("GSC_REFRESH_TOKEN"):
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
        except Exception:
            pass

    payload = collect(gsc_config(), window_days=args.window_days)
    avail = payload.get("available")
    print(f"[gsc-search-demand] available={avail} queries={payload.get('query_count')} "
          f"gaps={len(payload.get('gap_queries') or [])} terms={len(payload.get('term_scores') or {})}"
          + ("" if avail else f"  error={payload.get('error')}"))
    for q in (payload.get("gap_queries") or [])[:8]:
        print(f"  gap  {q['impressions']:>5} impr  {q['clicks']:>3} clk  {q['query']}")
    if not args.dry_run:
        out = write_search_demand(payload)
        print(f"  -> {out}")
    elif not avail and payload.get("error") == "credentials_not_ready":
        print("  [note] set GOOGLE_CLIENT_ID/SECRET + GSC_REFRESH_TOKEN + GSC_SITE_URL in .env, "
              "then re-run. Mint the refresh token once with the webmasters.readonly scope.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
