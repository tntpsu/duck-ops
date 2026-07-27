"""Surface 63: SEO outcome loop producer. All GSC traffic mocked via
gsc_search_demand._request_json (the single HTTP indirection); receipts are
fixtures. The dead-token degrade path is a first-class test, not a blocker —
the roster must ship as `unmeasured` with live countdowns.
"""
from __future__ import annotations

import json

import pytest

import seo_outcome_intel as soi
from workflow_control import TestModeRefusalError


def _receipt(rid="gid://shopify/Product/1", url="/products/duck-a", verified="2026-06-01T10:00:00-04:00", **over):
    base = {
        "receipt_id": f"r-{rid[-4:]}-{verified[:10]}", "verified_at": verified,
        "status": "verified", "lane": "shopify_seo_review",
        "resource_kind": "product", "resource_id": rid,
        "resource_url": url, "title": "Duck A", "applied_fields": ["seo_title"],
    }
    base.update(over)
    return base


def _write_receipts(receipts):
    d = soi.SEO_WRITEBACK_RECEIPT_DIR
    d.mkdir(parents=True, exist_ok=True)
    for i, r in enumerate(receipts):
        (d / f"{i:03d}.json").write_text(json.dumps(r))


def _gsc_ok(monkeypatch, rows_by_window):
    """Mock the HTTP layer: token call returns a token; searchanalytics calls
    are routed by the request body's startDate. Records every analytics call."""
    import gsc_search_demand as gsc
    calls = []

    def fake(request, timeout=30):
        raw = request.data.decode() if request.data else ""
        try:
            body = json.loads(raw)
        except ValueError:
            # form-urlencoded -> the OAuth token call
            return {"access_token": "tok"}
        calls.append((body["startDate"], body["endDate"], tuple(body.get("dimensions") or [])))
        return {"rows": rows_by_window.get(body["startDate"], [])}

    monkeypatch.setattr(gsc, "_request_json", fake)
    return calls


_CONFIG = {"client_id": "cid", "client_secret": "sec", "refresh_token": "rt",
           "site_url": "https://www.myjeepduck.com/", "credentials_ready": True}


# ---- roster ------------------------------------------------------------------

def test_roster_latest_per_resource_and_superseded():
    _write_receipts([
        _receipt(verified="2026-05-01T10:00:00-04:00"),
        _receipt(verified="2026-06-01T10:00:00-04:00"),  # newer -> anchor
        _receipt(rid="gid://shopify/Product/2", url="/products/duck-b"),
    ])
    tracked, unjoinable, superseded = soi._load_receipts()
    assert len(tracked) == 2 and superseded == 1 and unjoinable == []
    a = next(t for t in tracked if t["resource_id"].endswith("/1"))
    assert a["verified_at"].startswith("2026-06-01")


def test_roster_missing_url_is_unjoinable():
    _write_receipts([_receipt(url="")])
    tracked, unjoinable, _ = soi._load_receipts()
    assert tracked == [] and unjoinable[0]["reason"] == "missing resource_url"


def test_roster_unparseable_date_is_unjoinable():
    _write_receipts([_receipt(verified="not-a-date")])
    tracked, unjoinable, _ = soi._load_receipts()
    assert tracked == [] and "verified_at" in unjoinable[0]["reason"]


# ---- window math -------------------------------------------------------------

def test_windows_anchored_to_verified_at():
    from datetime import date
    w = soi._windows(date(2026, 6, 1), date(2026, 7, 26))
    assert w["before"] == ("2026-05-04", "2026-05-31")
    assert w["after"] == ("2026-06-04", "2026-07-01")
    assert w["complete"] is True


def test_windows_incomplete_is_pending_never_flat():
    from datetime import date
    w = soi._windows(date(2026, 7, 20), date(2026, 7, 26))
    assert w["complete"] is False and w["days_remaining"] > 0
    v, reason, _ = soi._verdict({"impressions": 100}, {"impressions": 100},
                                complete=False, measured=True)
    assert v == "pending"


# ---- verdicts (table-driven) -------------------------------------------------

@pytest.mark.parametrize("before,after,expect", [
    ({"impressions": 100, "position": 20.0}, {"impressions": 140, "position": 19.0}, "improved"),  # +40% impr
    ({"impressions": 100, "position": 20.0}, {"impressions": 60, "position": 21.0}, "declined"),   # -40% impr
    ({"impressions": 100, "position": 20.0}, {"impressions": 105, "position": 12.0}, "improved"),  # position -8
    ({"impressions": 100, "position": 12.0}, {"impressions": 105, "position": 20.0}, "declined"),  # position +8
    ({"impressions": 100, "position": 20.0}, {"impressions": 110, "position": 19.5}, "flat"),
    ({"impressions": 0, "position": 0.0}, {"impressions": 15, "position": 30.0}, "improved"),      # new visibility
    ({"impressions": 0, "position": 0.0}, {"impressions": 0, "position": 0.0}, "no_data"),
    ({"impressions": 5, "position": 40.0}, {"impressions": 8, "position": 35.0}, "low_data"),
])
def test_verdict_table(before, after, expect):
    v, _, _ = soi._verdict(before, after, complete=True, measured=True)
    assert v == expect


def test_clicks_never_drive_verdict():
    # click explosion with flat impressions/position must stay flat
    v, _, _ = soi._verdict({"impressions": 100, "position": 10.0, "clicks": 0},
                           {"impressions": 102, "position": 10.1, "clicks": 50},
                           complete=True, measured=True)
    assert v == "flat"


# ---- collect: cohort batching + join ----------------------------------------

def test_collect_two_calls_per_cohort_and_join(monkeypatch):
    page = "https://www.myjeepduck.com/products/duck-a"
    calls = _gsc_ok(monkeypatch, {
        "2026-05-04": [{"keys": [page], "clicks": 3, "impressions": 100, "ctr": 0.03, "position": 20.0}],
        "2026-06-04": [{"keys": [page], "clicks": 5, "impressions": 150, "ctr": 0.033, "position": 15.0}],
    })
    payload = soi.collect(_CONFIG, today="2026-07-26",
                          receipts=[_receipt(verified="2026-06-01T10:00:00-04:00")])
    assert payload["available"] is True
    assert len(calls) == 2  # one cohort -> before + after
    assert all(dims == ("page",) for _, _, dims in calls)
    pg = payload["pages"][0]
    assert pg["verdict"] == "improved"
    assert pg["delta"]["impressions_pct"] == 0.5
    assert payload["summary"]["improved"] == 1


def test_collect_skips_after_call_before_window_starts(monkeypatch):
    calls = _gsc_ok(monkeypatch, {})
    payload = soi.collect(_CONFIG, today="2026-07-26",
                          receipts=[_receipt(verified="2026-07-25T10:00:00-04:00")])
    assert len(calls) == 1  # after window hasn't started -> only the before call
    assert payload["pages"][0]["verdict"] == "pending"


def test_collect_zero_rows_is_no_data_not_error(monkeypatch):
    _gsc_ok(monkeypatch, {})  # every window returns no rows
    payload = soi.collect(_CONFIG, today="2026-07-26",
                          receipts=[_receipt(verified="2026-06-01T10:00:00-04:00")])
    assert payload["available"] is True
    assert payload["pages"][0]["verdict"] == "no_data"


# ---- degrade: dead token -----------------------------------------------------

def test_dead_token_ships_full_roster_unmeasured(monkeypatch):
    import gsc_search_demand as gsc
    def boom(request, timeout=30):
        raise RuntimeError("HTTP 400 invalid_grant")
    monkeypatch.setattr(gsc, "_request_json", boom)
    payload = soi.collect(_CONFIG, today="2026-07-26",
                          receipts=[_receipt(), _receipt(rid="gid://shopify/Product/2", url="/products/duck-b")])
    assert payload["available"] is False and payload["error"]
    assert len(payload["pages"]) == 2
    assert all(p["verdict"] == "unmeasured" for p in payload["pages"])
    assert payload["summary"]["unmeasured"] == 2


def test_missing_credentials_ships_roster(monkeypatch):
    payload = soi.collect({"credentials_ready": False, "site_url": "https://www.myjeepduck.com/"},
                          today="2026-07-26", receipts=[_receipt()])
    assert payload["available"] is False and payload["error"] == "credentials_missing"
    assert payload["pages"][0]["verdict"] == "unmeasured"


# ---- write guard (3-layer, layer 2) ------------------------------------------

def test_write_to_tmp_path_ok(tmp_path):
    out = soi.write_seo_outcome_intel({"x": 1}, path=tmp_path / "intel.json")
    assert json.loads(out.read_text()) == {"x": 1}


def test_write_guard_refuses_frozen_prod(monkeypatch):
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    with pytest.raises(TestModeRefusalError):
        soi.write_seo_outcome_intel({}, path=soi._FROZEN_PRODUCTION_SEO_OUTCOME_INTEL_PATH)
