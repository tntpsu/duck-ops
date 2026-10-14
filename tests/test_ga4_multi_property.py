"""Both GA4 properties are read, because one property cannot hold both channels.

2026-10-14. `first_party_demand_feeds` reported "no rows for shopify — that
channel has no analytics reaching this property", and that was read as "the
Shopify store has no GA4 tag". It had one the whole time: Shopify's Google &
YouTube app was connected to `G-TEWTY33W75 (myJeepDuck)`, which lives in GA4
property **468950523**. Our producer read **471407647**, whose only stream is
"My Etsy Jeep Site".

28-day truth at the time of the fix:

    471407647  stream "My Etsy Jeep Site"  www.etsy.com        2,787 views
    468950523  stream "My Website"         www.myjeepduck.com  4,281 views

GA4 cannot move a data stream between properties, so this is not a
misconfiguration to tidy up — a single-property read is structurally incapable
of seeing both channels. The producer takes a list.

The card's own wording had named the ambiguity correctly ("either the channel
had no traffic or it has no tag sending to *this property*") and the third
possibility -- a tag sending to a property we never queried -- is what was true
([[feedback_plausible_fallbacks_mask_failure]]).
"""
from __future__ import annotations

import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import ga4_listing_performance as ga4  # noqa: E402

READY = {"client_id": "cid", "client_secret": "sec", "refresh_token": "rt",
         "property_id": "471407647", "property_ids": ["471407647", "468950523"],
         "credentials_ready": True}


def _row(title, views, host):
    return {"title": title, "host": host, "channel": ga4._channel_for(host),
            "page_views": float(views), "active_users": float(views),
            "new_users": 0.0, "engagement_rate": 0.5, "bounce_rate": 0.5,
            "avg_engagement_time": 30.0}


class TestConfigTakesAList:
    def test_one_id_still_works(self):
        cfg = ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GA4_REFRESH_TOKEN": "y",
                              "GA4_PROPERTY_ID": "471407647"})
        assert cfg["property_ids"] == ["471407647"]
        assert cfg["property_id"] == "471407647"   # existing readers unaffected

    def test_a_comma_separated_pair_is_two_properties(self):
        cfg = ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GA4_REFRESH_TOKEN": "y",
                              "GA4_PROPERTY_ID": "471407647,468950523"})
        assert cfg["property_ids"] == ["471407647", "468950523"]

    def test_spaces_and_the_properties_prefix_are_tolerated(self):
        cfg = ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GA4_REFRESH_TOKEN": "y",
                              "GA4_PROPERTY_ID": "properties/471407647, properties/468950523"})
        assert cfg["property_ids"] == ["471407647", "468950523"]

    def test_a_repeated_id_is_not_read_twice(self):
        """Double-counting every metric is the cost of a copy-paste in .env."""
        cfg = ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GA4_REFRESH_TOKEN": "y",
                              "GA4_PROPERTY_ID": "471407647,471407647"})
        assert cfg["property_ids"] == ["471407647"]

    def test_no_property_is_not_ready(self):
        assert ga4.ga4_config({"GOOGLE_CLIENT_ID": "x", "GA4_REFRESH_TOKEN": "y"})[
            "credentials_ready"] is False


class TestBothPropertiesAreRead:
    def _collect(self, monkeypatch, by_pid, windows=(28,)):
        seen = []
        monkeypatch.setattr(ga4, "fetch_ga4_access_token", lambda c: ("tok", {}))

        def fake(token, pid, start, end):
            seen.append(pid)
            return by_pid[pid]
        monkeypatch.setattr(ga4, "run_report", fake)
        out = ga4.collect(READY, windows=windows, primary_window=windows[0])
        return out, seen

    def test_every_configured_property_is_queried(self, monkeypatch):
        ok = {"ok": True, "rows_available": 1, "rows_fetched": 1, "truncated": False}
        out, seen = self._collect(monkeypatch, {
            "471407647": ([_row("Etsy Duck", 10, "www.etsy.com")], ok),
            "468950523": ([_row("Shop Duck", 20, "www.myjeepduck.com")], ok),
        })
        assert seen == ["471407647", "468950523"]
        assert out["available"] is True
        assert out["property_ids"] == ["471407647", "468950523"]

    def test_both_channels_land_in_one_payload(self, monkeypatch):
        """The whole point: shopify stops being 'missing'."""
        ok = {"ok": True, "rows_available": 1, "rows_fetched": 1, "truncated": False}
        out, _ = self._collect(monkeypatch, {
            "471407647": ([_row("Etsy Duck", 10, "www.etsy.com")], ok),
            "468950523": ([_row("Shop Duck", 20, "www.myjeepduck.com")], ok),
        })
        assert out["channel_coverage"]["missing"] == []
        assert out["channel_coverage"]["complete"] is True
        assert set(out["channel_coverage"]["present"]) >= {"etsy", "shopify"}
        assert out["totals"]["page_views"] == 30

    def test_the_same_title_on_two_hosts_stays_two_rows(self, monkeypatch):
        """A duck's name appears on its Etsy listing AND its Shopify page. A
        title-only key would merge them and credit one channel's views to the
        other."""
        ok = {"ok": True, "rows_available": 1, "rows_fetched": 1, "truncated": False}
        out, _ = self._collect(monkeypatch, {
            "471407647": ([_row("Candy Corn Quacker", 10, "www.etsy.com")], ok),
            "468950523": ([_row("Candy Corn Quacker", 20, "www.myjeepduck.com")], ok),
        })
        hosts = {r["host"]: r["page_views"] for r in out["listings"]}
        assert hosts == {"www.etsy.com": 10, "www.myjeepduck.com": 20}

    def test_views_by_window_does_not_cross_the_channels(self, monkeypatch):
        ok = {"ok": True, "rows_available": 1, "rows_fetched": 1, "truncated": False}
        out, _ = self._collect(monkeypatch, {
            "471407647": ([_row("Dup", 10, "www.etsy.com")], ok),
            "468950523": ([_row("Dup", 20, "www.myjeepduck.com")], ok),
        }, windows=(28,))
        for r in out["listings"]:
            expected = 10 if r["host"] == "www.etsy.com" else 20
            assert r["views_by_window"]["28"] == expected


class TestWhenOnePropertyFails:
    def _collect(self, monkeypatch, by_pid):
        monkeypatch.setattr(ga4, "fetch_ga4_access_token", lambda c: ("tok", {}))
        monkeypatch.setattr(ga4, "run_report", lambda t, pid, s, e: by_pid[pid])
        return ga4.collect(READY, windows=(28,), primary_window=28)

    def test_the_surviving_property_still_reports(self, monkeypatch):
        """Half the truth beats none -- but it must not look like all of it."""
        out = self._collect(monkeypatch, {
            "471407647": ([_row("Etsy Duck", 10, "www.etsy.com")],
                          {"ok": True, "rows_fetched": 1, "rows_available": 1}),
            "468950523": ([], {"ok": False, "error": "report_http_403"}),
        })
        assert out["available"] is True
        assert out["totals"]["page_views"] == 10

    def test_the_failure_is_named_not_swallowed(self, monkeypatch):
        out = self._collect(monkeypatch, {
            "471407647": ([_row("Etsy Duck", 10, "www.etsy.com")],
                          {"ok": True, "rows_fetched": 1, "rows_available": 1}),
            "468950523": ([], {"ok": False, "error": "report_http_403"}),
        })
        assert out["property_errors"] == [
            {"property_id": "468950523", "error": "report_http_403", "detail": None}]

    def test_a_partial_read_does_not_claim_the_channel_is_absent(self, monkeypatch):
        """The exact trap this whole surface came from: shopify shows 'missing'
        because we could not READ it, and that gets read as 'no tag'."""
        out = self._collect(monkeypatch, {
            "471407647": ([_row("Etsy Duck", 10, "www.etsy.com")],
                          {"ok": True, "rows_fetched": 1, "rows_available": 1}),
            "468950523": ([], {"ok": False, "error": "report_http_403"}),
        })
        assert out["channel_coverage"]["missing"] == ["shopify"]
        assert out["property_errors"], (
            "a reader seeing shopify 'missing' must be able to tell a failed "
            "read from a genuinely silent channel")

    def test_all_properties_failing_is_unavailable(self, monkeypatch):
        out = self._collect(monkeypatch, {
            "471407647": ([], {"ok": False, "error": "report_http_403"}),
            "468950523": ([], {"ok": False, "error": "report_http_500"}),
        })
        assert out["available"] is False
        assert out["error"] == "report_http_403"


class TestMergeArithmetic:
    def test_additive_metrics_sum(self):
        rows = [_row("A", 10, "h"), _row("A", 5, "h")]
        merged = ga4._merge_rows(rows)
        assert len(merged) == 1
        assert merged[0]["page_views"] == 15 and merged[0]["active_users"] == 15

    def test_rates_are_weighted_by_users_not_averaged(self):
        """A 3-user page must not outvote a 300-user one."""
        big = _row("A", 300, "h"); big["engagement_rate"] = 0.9
        small = _row("A", 3, "h"); small["engagement_rate"] = 0.1
        merged = ga4._merge_rows([big, small])[0]
        assert 0.88 < merged["engagement_rate"] < 0.9     # not the 0.5 mean

    def test_distinct_rows_are_left_alone(self):
        rows = [_row("A", 1, "x"), _row("B", 2, "y")]
        assert len(ga4._merge_rows(rows)) == 2

    def test_truncation_anywhere_is_truncation(self):
        meta = ga4._merge_report_meta([
            {"ok": True, "rows_fetched": 10, "rows_available": 10, "truncated": False},
            {"ok": True, "rows_fetched": 250, "rows_available": 400, "truncated": True},
        ])
        assert meta["truncated"] is True
        assert meta["rows_available"] == 410 and meta["rows_fetched"] == 260
