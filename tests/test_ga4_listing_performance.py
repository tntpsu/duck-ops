"""Surface 39 producer tests — GA4 listing performance + Fix-or-Promote classifier.

HTTP is mocked at ga4_listing_performance._request_json (never the network).
Covers config readiness, fail-soft auth/report errors, row parsing, the
relative tercile classifier (fix/promote/watch/neutral), the available:false
degrade paths, and the DUCK_TEST_MODE write guard. State path is redirected to
tmp by the autouse conftest fixture."""
from __future__ import annotations

import json
import sys
import urllib.error
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import ga4_listing_performance as ga4  # noqa: E402


READY = {
    "client_id": "cid", "client_secret": "sec", "refresh_token": "rt",
    "property_id": "123456789", "credentials_ready": True,
}


def _api_row(title, views, eng, host="www.etsy.com"):
    return {
        "dimensionValues": [{"value": title}, {"value": host}],
        "metricValues": [
            {"value": str(views)}, {"value": str(views)}, {"value": "0"},
            {"value": str(eng)}, {"value": str(1 - eng)}, {"value": "30"},
        ],
    }


# ---- config ------------------------------------------------------------------

class TestConfig:
    def test_ready(self):
        cfg = ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GA4_REFRESH_TOKEN": "y", "GA4_PROPERTY_ID": "123"})
        assert cfg["credentials_ready"] and cfg["property_id"] == "123"

    def test_strips_properties_prefix(self):
        cfg = ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GA4_REFRESH_TOKEN": "y",
                              "GA4_PROPERTY_ID": "properties/999"})
        assert cfg["property_id"] == "999"

    def test_falls_back_to_gsc_token(self):
        cfg = ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GSC_REFRESH_TOKEN": "shared", "GA4_PROPERTY_ID": "1"})
        assert cfg["refresh_token"] == "shared" and cfg["credentials_ready"]

    def test_reads_google_tasks_prefixed_client_creds(self):
        cfg = ga4.ga4_config({
            "GOOGLE_TASKS_CLIENT_ID": "cid", "GOOGLE_TASKS_CLIENT_SECRET": "sec",
            "GA4_REFRESH_TOKEN": "rt", "GA4_PROPERTY_ID": "471407647"})
        assert cfg["client_id"] == "cid" and cfg["client_secret"] == "sec"
        assert cfg["credentials_ready"] is True

    def test_not_ready_missing(self):
        assert ga4.ga4_config({})["credentials_ready"] is False


# ---- auth + report (mocked) --------------------------------------------------

class TestApi:
    def test_token_parsed(self, monkeypatch):
        monkeypatch.setattr(ga4, "_request_json", lambda req, timeout=30: {"access_token": "AT"})
        token, meta = ga4.fetch_ga4_access_token(READY)
        assert token == "AT" and meta["ok"]

    def test_token_http_error_fail_soft(self, monkeypatch):
        def boom(req, timeout=30):
            raise urllib.error.HTTPError(req.full_url, 403, "no", {}, None)
        monkeypatch.setattr(ga4, "_request_json", boom)
        token, meta = ga4.fetch_ga4_access_token(READY)
        assert token is None and meta["error"] == "token_http_403"

    def test_report_rows_parsed(self, monkeypatch):
        monkeypatch.setattr(ga4, "_request_json", lambda req, timeout=30: {
            "rows": [_api_row("Pirate Duck - Etsy", 300, 0.7, host="www.etsy.com")]})
        rows, meta = ga4.run_report("AT", "123", "2026-05-01", "2026-05-28")
        assert meta["ok"] and rows[0]["title"] == "Pirate Duck - Etsy"
        assert rows[0]["page_views"] == 300 and rows[0]["engagement_rate"] == 0.7
        assert rows[0]["host"] == "www.etsy.com" and rows[0]["channel"] == "etsy"

    def test_channel_derivation(self):
        assert ga4._channel_for("www.etsy.com") == "etsy"
        assert ga4._channel_for("myjeepduck.com") == "shopify"
        assert ga4._channel_for("shop.myshopify.com") == "shopify"
        assert ga4._channel_for("") == "web"

    def test_report_skips_short_metric_rows(self, monkeypatch):
        monkeypatch.setattr(ga4, "_request_json", lambda req, timeout=30: {
            "rows": [{"dimensionValues": [{"value": "x"}], "metricValues": [{"value": "1"}]}]})
        rows, _ = ga4.run_report("AT", "123", "a", "b")
        assert rows == []

    def test_report_http_error_fail_soft(self, monkeypatch):
        def boom(req, timeout=30):
            raise urllib.error.HTTPError(req.full_url, 500, "err", {}, None)
        monkeypatch.setattr(ga4, "_request_json", boom)
        rows, meta = ga4.run_report("AT", "123", "a", "b")
        assert rows == [] and meta["error"] == "report_http_500"


# ---- classifier (relative terciles) ------------------------------------------

class TestClassify:
    def _rows(self):
        # high views: 500/400/350 ; low views: 200/150/120 ; engagement varied
        return [
            {"title": "leak", "page_views": 500, "active_users": 300, "new_users": 0,
             "engagement_rate": 0.15, "bounce_rate": 0.85, "avg_engagement_time": 10},
            {"title": "winner", "page_views": 480, "active_users": 300, "new_users": 0,
             "engagement_rate": 0.80, "bounce_rate": 0.20, "avg_engagement_time": 60},
            {"title": "starved", "page_views": 120, "active_users": 100, "new_users": 0,
             "engagement_rate": 0.82, "bounce_rate": 0.18, "avg_engagement_time": 55},
            {"title": "mid", "page_views": 300, "active_users": 200, "new_users": 0,
             "engagement_rate": 0.45, "bounce_rate": 0.55, "avg_engagement_time": 30},
            {"title": "low2", "page_views": 150, "active_users": 90, "new_users": 0,
             "engagement_rate": 0.50, "bounce_rate": 0.50, "avg_engagement_time": 25},
            {"title": "mid2", "page_views": 350, "active_users": 220, "new_users": 0,
             "engagement_rate": 0.55, "bounce_rate": 0.45, "avg_engagement_time": 33},
        ]

    def _verdict(self, classified, title):
        return next(r["verdict"] for r in classified if r["title"] == title)

    def test_high_views_low_eng_is_fix(self):
        c = ga4.classify_listings(self._rows())
        assert self._verdict(c, "leak") == "fix"

    def test_high_views_high_eng_is_promote(self):
        c = ga4.classify_listings(self._rows())
        assert self._verdict(c, "winner") == "promote"

    def test_low_views_high_eng_is_watch(self):
        c = ga4.classify_listings(self._rows())
        assert self._verdict(c, "starved") == "watch"

    def test_below_min_views_is_neutral(self):
        rows = [{"title": "tiny", "page_views": 5, "active_users": 3, "new_users": 0,
                 "engagement_rate": 0.9, "bounce_rate": 0.1, "avg_engagement_time": 40}]
        c = ga4.classify_listings(rows)
        assert c[0]["verdict"] == "neutral" and "not enough traffic" in c[0]["reason"]

    def test_empty_is_empty(self):
        assert ga4.classify_listings([]) == []


class TestMultiWindow:
    def test_trend_labels(self):
        W = (7, 28, 90)
        assert ga4._trend({"7": 200, "28": 300, "90": 320}, W) == "rising"
        assert ga4._trend({"7": 2, "28": 100, "90": 300}, W) == "fading"
        assert ga4._trend({"7": 7, "28": 30, "90": 90}, W) == "steady"
        assert ga4._trend({"7": 5, "28": 0, "90": 0}, W) == "new"

    def test_build_payload_enriches_listings_with_windows_and_trend(self):
        def r(title, views):
            return {"title": title, "host": "www.etsy.com", "channel": "etsy",
                    "page_views": views, "active_users": int(views * 0.6), "new_users": 0,
                    "engagement_rate": 0.7, "bounce_rate": 0.3, "avg_engagement_time": 40}
        per_window = {7: [r("winner", 200)], 28: [r("winner", 300)], 90: [r("winner", 320)]}
        p = ga4.build_payload(per_window, windows=(7, 28, 90), primary_window=28,
                              property_id="test-prop")
        assert p["windows"] == [7, 28, 90] and p["listing_count"] == 1
        w = p["listings"][0]
        assert w["views_by_window"] == {"7": 200, "28": 300, "90": 320}
        assert w["trend"] == "rising"

    def test_channels_judged_independently(self):
        """A small Etsy winner must not be drowned by a huge Shopify cohort —
        each channel uses its own terciles."""
        def _r(title, views, eng, channel):
            return {"title": title, "channel": channel, "page_views": views,
                    "active_users": int(views * 0.6), "new_users": 0,
                    "engagement_rate": eng, "bounce_rate": round(1 - eng, 2),
                    "avg_engagement_time": 40}
        rows = [
            # shopify cohort (big traffic)
            _r("s-win", 2000, 0.85, "shopify"), _r("s-leak", 1900, 0.10, "shopify"),
            _r("s-a", 1500, 0.5, "shopify"), _r("s-b", 1200, 0.4, "shopify"),
            _r("s-c", 900, 0.6, "shopify"), _r("s-d", 800, 0.3, "shopify"),
            # etsy cohort (small traffic — its winner has far fewer views than any shopify row)
            _r("e-win", 200, 0.85, "etsy"), _r("e-leak", 190, 0.10, "etsy"),
            _r("e-a", 150, 0.5, "etsy"), _r("e-b", 120, 0.4, "etsy"),
            _r("e-c", 90, 0.6, "etsy"), _r("e-d", 80, 0.3, "etsy"),
        ]
        c = ga4.classify_listings(rows)
        verdict = {r["title"]: r["verdict"] for r in c}
        # The Etsy winner is promoted despite tiny absolute views (judged vs Etsy)
        assert verdict["e-win"] == "promote" and verdict["e-leak"] == "fix"
        assert verdict["s-win"] == "promote" and verdict["s-leak"] == "fix"


# ---- collect orchestration ---------------------------------------------------

class TestCollect:
    def test_not_ready_available_false(self):
        payload = ga4.collect(ga4.ga4_config({}))
        assert payload["available"] is False and payload["error"] == "credentials_not_ready"
        assert payload["fix"] == [] and payload["promote"] == []

    def test_token_failure_degrades(self, monkeypatch):
        monkeypatch.setattr(ga4, "fetch_ga4_access_token", lambda c: (None, {"error": "token_http_401"}))
        payload = ga4.collect(READY)
        assert payload["available"] is False and payload["error"] == "token_http_401"

    def test_happy_path_classifies_and_totals(self, monkeypatch):
        def _r(title, views, eng):
            return {"title": title, "host": "www.etsy.com", "channel": "etsy",
                    "page_views": views, "active_users": int(views * 0.6),
                    "new_users": 10, "engagement_rate": eng, "bounce_rate": round(1 - eng, 2),
                    "avg_engagement_time": 40}
        cohort = [_r("winner", 600, 0.85), _r("leak", 580, 0.12), _r("a", 520, 0.5),
                  _r("b", 400, 0.4), _r("c", 300, 0.6), _r("d", 280, 0.3)]
        monkeypatch.setattr(ga4, "fetch_ga4_access_token", lambda c: ("AT", {"ok": True}))
        monkeypatch.setattr(ga4, "run_report", lambda *a, **k: (cohort, {"ok": True}))
        payload = ga4.collect(READY, today="2026-06-25")
        assert payload["available"] is True and payload["listing_count"] == 6
        assert payload["totals"]["page_views"] == 2680
        assert payload["channels"]["etsy"]["listings"] == 6
        assert any(r["title"] == "winner" for r in payload["promote"])
        assert any(r["title"] == "leak" for r in payload["fix"])


# ---- write isolation ---------------------------------------------------------

class TestWrite:
    def test_roundtrips(self, tmp_path):
        out = ga4.write_listing_performance({"available": True, "listings": []},
                                            path=tmp_path / "lp.json")
        assert json.loads(out.read_text())["available"] is True

    def test_guard_refuses_prod_in_test_mode(self, monkeypatch):
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(ga4.TestModeRefusalError):
            ga4.write_listing_performance({}, path=ga4._FROZEN_PRODUCTION_LISTING_PERFORMANCE_PATH)
