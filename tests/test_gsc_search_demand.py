"""Surface 38 producer tests — Google Search Console first-party search demand.

The HTTP layer is mocked at gsc_search_demand._request_json (one indirection,
never the network). Covers: config readiness, fail-soft auth/query errors,
row parsing, aggregation + gap detection, the available:false degrade paths,
and the DUCK_TEST_MODE write guard. State path is redirected to tmp by the
autouse conftest fixture."""
from __future__ import annotations

import json
import sys
import urllib.error
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import gsc_search_demand as gsc  # noqa: E402


READY_CONFIG = {
    "client_id": "cid", "client_secret": "secret", "refresh_token": "rt",
    "site_url": "sc-domain:myjeepduck.com", "credentials_ready": True,
}


def _rows(*specs):
    """specs: (query, clicks, impressions)."""
    out = []
    for q, c, i in specs:
        out.append({"query": q, "clicks": float(c), "impressions": float(i), "ctr": 0.1, "position": 5.0})
    return out


# ---- config ------------------------------------------------------------------

class TestConfig:
    def test_ready_when_all_present(self):
        cfg = gsc.gsc_config({
            "GOOGLE_CLIENT_ID": "x", "GSC_REFRESH_TOKEN": "y", "GSC_SITE_URL": "z"})
        assert cfg["credentials_ready"] is True

    def test_not_ready_missing_any(self):
        assert gsc.gsc_config({"GOOGLE_CLIENT_ID": "x"})["credentials_ready"] is False
        assert gsc.gsc_config({})["credentials_ready"] is False


# ---- auth (mocked) -----------------------------------------------------------

class TestAuth:
    def test_token_parsed(self, monkeypatch):
        monkeypatch.setattr(gsc, "_request_json", lambda req, timeout=30: {"access_token": "AT"})
        token, meta = gsc.fetch_gsc_access_token(READY_CONFIG)
        assert token == "AT" and meta["ok"]

    def test_http_error_is_fail_soft(self, monkeypatch):
        def boom(req, timeout=30):
            raise urllib.error.HTTPError(req.full_url, 401, "no", {}, None)
        monkeypatch.setattr(gsc, "_request_json", boom)
        token, meta = gsc.fetch_gsc_access_token(READY_CONFIG)
        assert token is None and meta["error"] == "token_http_401"

    def test_network_error_is_fail_soft(self, monkeypatch):
        def boom(req, timeout=30):
            raise urllib.error.URLError("dns")
        monkeypatch.setattr(gsc, "_request_json", boom)
        token, meta = gsc.fetch_gsc_access_token(READY_CONFIG)
        assert token is None and meta["error"] == "token_network"


# ---- query parsing (mocked) --------------------------------------------------

class TestQuery:
    def test_rows_parsed(self, monkeypatch):
        monkeypatch.setattr(gsc, "_request_json", lambda req, timeout=30: {
            "rows": [{"keys": ["pirate duck"], "clicks": 4, "impressions": 120, "ctr": 0.03, "position": 7.2}]})
        rows, meta = gsc.query_search_analytics("AT", "sc-domain:x", "2026-05-01", "2026-05-28")
        assert meta["ok"] and len(rows) == 1
        assert rows[0]["query"] == "pirate duck" and rows[0]["clicks"] == 4.0

    def test_empty_rows_not_invented(self, monkeypatch):
        monkeypatch.setattr(gsc, "_request_json", lambda req, timeout=30: {})
        rows, meta = gsc.query_search_analytics("AT", "sc-domain:x", "a", "b")
        assert rows == [] and meta["ok"]

    def test_query_http_error_fail_soft(self, monkeypatch):
        def boom(req, timeout=30):
            raise urllib.error.HTTPError(req.full_url, 403, "forbidden", {}, None)
        monkeypatch.setattr(gsc, "_request_json", boom)
        rows, meta = gsc.query_search_analytics("AT", "sc-domain:x", "a", "b")
        assert rows == [] and meta["error"] == "query_http_403"

    def test_row_without_keys_skipped(self, monkeypatch):
        monkeypatch.setattr(gsc, "_request_json", lambda req, timeout=30: {
            "rows": [{"keys": [], "clicks": 1, "impressions": 9}, {"keys": ["ok duck"], "clicks": 1, "impressions": 9}]})
        rows, _ = gsc.query_search_analytics("AT", "x", "a", "b")
        assert [r["query"] for r in rows] == ["ok duck"]


# ---- aggregation + gap detection ---------------------------------------------

class TestAggregate:
    def test_term_scores_max_normalized(self):
        rows = _rows(("pirate duck", 10, 500), ("wizard duck", 1, 20))
        agg = gsc.aggregate_search_demand(rows, catalog_tokens=set())
        assert max(agg["term_scores"].values()) == 1.0
        assert agg["term_scores"]["pirate"] > agg["term_scores"]["wizard"]

    def test_gap_query_is_uncovered_high_impression(self):
        # "pirate" not in catalog -> gap; "wizard" in catalog -> covered (not a gap)
        rows = _rows(("pirate duck", 2, 200), ("wizard duck", 2, 200))
        agg = gsc.aggregate_search_demand(rows, catalog_tokens={"wizard"})
        gaps = [g["query"] for g in agg["gap_queries"]]
        assert "pirate duck" in gaps and "wizard duck" not in gaps

    def test_low_impression_uncovered_is_not_a_gap(self):
        rows = _rows(("obscure duck", 0, 3))  # below GAP_MIN_IMPRESSIONS
        agg = gsc.aggregate_search_demand(rows, catalog_tokens=set())
        assert agg["gap_queries"] == []

    def test_empty_rows_empty_maps(self):
        agg = gsc.aggregate_search_demand([], catalog_tokens={"wizard"})
        assert agg["term_scores"] == {} and agg["gap_queries"] == [] and agg["top_queries"] == []


# ---- collect orchestration (fail-soft degrade) -------------------------------

class TestCollect:
    def test_not_ready_returns_available_false(self):
        payload = gsc.collect(gsc.gsc_config({}), catalog_tokens=set())
        assert payload["available"] is False and payload["error"] == "credentials_not_ready"
        assert payload["term_scores"] == {}

    def test_token_failure_degrades(self, monkeypatch):
        monkeypatch.setattr(gsc, "fetch_gsc_access_token", lambda cfg: (None, {"error": "token_http_401"}))
        payload = gsc.collect(READY_CONFIG, catalog_tokens=set())
        assert payload["available"] is False and payload["error"] == "token_http_401"

    def test_happy_path_builds_live_payload(self, monkeypatch):
        monkeypatch.setattr(gsc, "fetch_gsc_access_token", lambda cfg: ("AT", {"ok": True}))
        monkeypatch.setattr(gsc, "query_search_analytics",
                            lambda *a, **k: (_rows(("pirate duck", 9, 300)), {"ok": True}))
        payload = gsc.collect(READY_CONFIG, today="2026-05-28", catalog_tokens=set())
        assert payload["available"] is True and payload["query_count"] == 1
        assert payload["term_scores"]["pirate"] == 1.0


# ---- write isolation ---------------------------------------------------------

class TestWrite:
    def test_write_roundtrips(self, tmp_path):
        p = tmp_path / "gsc_search_demand.json"
        out = gsc.write_search_demand({"available": True, "term_scores": {}}, path=p)
        assert json.loads(out.read_text())["available"] is True

    def test_write_guard_refuses_prod_path_in_test_mode(self, monkeypatch):
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(gsc.TestModeRefusalError):
            gsc.write_search_demand({}, path=gsc._FROZEN_PRODUCTION_GSC_SEARCH_DEMAND_PATH)
