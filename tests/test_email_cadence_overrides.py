"""Surface 23: operator-writable email-cadence overrides. The autouse conftest
fixture redirects EMAIL_CADENCE_OVERRIDES_PATH to a tmp file, so these write +
read freely without touching prod."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import email_cadence_gate as g  # noqa: E402

MONDAY = datetime(2026, 6, 15, 8, 0).astimezone()
TUESDAY = datetime(2026, 6, 16, 8, 0).astimezone()


# ---- override overlays default --------------------------------------------

def test_override_daily_beats_weekly_default():
    # 'reviews' defaults to weekly_monday; override to daily → sends on Tuesday.
    assert g.should_send_email("reviews", {}, now=TUESDAY).should_send is False  # default
    g.set_override("reviews", "daily")
    assert g.should_send_email("reviews", {}, now=TUESDAY).should_send is True


def test_override_off_beats_default():
    g.set_override("recommendations", "off")
    assert g.should_send_email("recommendations", {}, now=MONDAY).should_send is False


def test_missing_file_uses_hardcoded_default():
    assert g.load_overrides() == {}
    # weekly_monday default: True Monday, False Tuesday
    assert g.should_send_email("reviews", {}, now=MONDAY).should_send is True
    assert g.should_send_email("reviews", {}, now=TUESDAY).should_send is False


def test_corrupt_file_falls_back_to_default():
    g.EMAIL_CADENCE_OVERRIDES_PATH.write_text("{not json", encoding="utf-8")
    assert g.load_overrides() == {}
    assert g.should_send_email("reviews", {}, now=MONDAY).should_send is True  # unchanged


# ---- "off" behavior --------------------------------------------------------

def test_off_suppresses_routine_send():
    g.set_override("reviews", "off")
    assert g.should_send_email("reviews", {}, now=MONDAY).should_send is False
    assert g.should_send_email("reviews", {}, now=TUESDAY).should_send is False


def test_off_still_fires_on_anomaly_bypass():
    # 'reviews' has bypass_keys=("low_rating_count",) — a ≤2★ still breaks through.
    g.set_override("reviews", "off")
    dec = g.should_send_email("reviews", {"low_rating_count": 3}, now=TUESDAY)
    assert dec.should_send is True and dec.bypass_active is True
    # without the anomaly, off stays silent
    assert g.should_send_email("reviews", {"low_rating_count": 0}, now=TUESDAY).should_send is False


def test_off_with_no_bypass_keys_fully_silent():
    # business_intelligence has bypass_keys=() → off means truly nothing.
    g.set_override("business_intelligence", "off")
    assert g.should_send_email("business_intelligence", {"action_items_count": 9}, now=MONDAY).should_send is False


# ---- set_override validation + persistence ---------------------------------

def test_set_override_persists_and_reads_back():
    g.set_override("profit", "daily")
    assert g.load_overrides().get("profit") == "daily"
    data = json.loads(g.EMAIL_CADENCE_OVERRIDES_PATH.read_text(encoding="utf-8"))
    assert data["overrides"]["profit"] == "daily"
    assert data["override_history"][-1]["to"] == "daily"


def test_set_override_clear_removes_it():
    g.set_override("profit", "daily")
    g.set_override("profit", "default")
    assert "profit" not in g.load_overrides()


def test_set_override_unknown_surface_raises():
    with pytest.raises(g.UnknownSurfaceError):
        g.set_override("not-a-surface", "daily")


def test_set_override_bad_cadence_raises():
    with pytest.raises(ValueError):
        g.set_override("reviews", "hourly")


def test_list_effective_cadences_reports_source_and_folded():
    g.set_override("reviews", "off")
    rows = {r["surface"]: r for r in g.list_effective_cadences()}
    assert rows["reviews"]["effective_cadence"] == "off"
    assert rows["reviews"]["source"] == "override"
    assert rows["reviews"]["folds_into_monday_digest"] is True
    assert rows["profit"]["source"] == "default"  # untouched


# ---- digest interaction ----------------------------------------------------

def test_off_surface_still_eligible_for_monday_digest():
    # "off" only gates the standalone send; it must NOT drop the surface from
    # the Monday-digest fold set (operator decision 2026-06-17).
    g.set_override("reviews", "off")
    assert "reviews" in g.DIGEST_FOLDED_SURFACES


# ---- test-mode write guard -------------------------------------------------

def test_set_override_refuses_frozen_prod_path_under_test_mode(monkeypatch):
    # Point the path back at the FROZEN prod default + DUCK_TEST_MODE=1 → refuse.
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    monkeypatch.setattr(g, "EMAIL_CADENCE_OVERRIDES_PATH", g._FROZEN_PRODUCTION_OVERRIDES_PATH)
    with pytest.raises(g.TestModeRefusalError):
        g.set_override("reviews", "off")
