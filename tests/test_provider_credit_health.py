"""Surface 99 — the alarm for an AI provider that ran out of money.

The bug this defends against is not a crash. On 2026-11-02 Gemini's prepaid
credits ran out, every Gemini-backed surface fell through to a path that still
produced presentable output, and all 57 OS cards stayed green. So the tests that
matter here are the ones that distinguish "working" from "degraded but
plausible", and "no bad news" from "good news".
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import provider_credit_health as pch


NOW = datetime.fromisoformat("2026-11-02T12:00:00-05:00")


def _at(**delta) -> str:
    return (NOW - timedelta(**delta)).isoformat()


def _failure(kind="credits_exhausted", *, provider="gemini", hours=1, detail="402 RESOURCE_EXHAUSTED"):
    return {"at": _at(hours=hours), "event": "failure", "provider": provider, "kind": kind, "detail": detail}


def _ok(*, provider="gemini", hours=1):
    return {"at": _at(hours=hours), "event": "ok", "provider": provider}


def _fallback(*, surface="meme_ai_panels", hours=1, reason="ai_panels_failed_text_check"):
    return {"at": _at(hours=hours), "event": "fallback", "provider": "gemini", "surface": surface, "reason": reason}


def test_a_credit_refusal_with_no_later_success_reads_red():
    payload = pch.grade([_failure(hours=2)], now=NOW)
    assert payload["status"] == "red"
    assert payload["out_of_credits"] == ["gemini"]


def test_a_success_after_the_refusal_clears_the_verdict():
    """A top-up has no button to press: the next successful call clears it."""
    payload = pch.grade([_failure(hours=5), _ok(hours=1)], now=NOW)
    assert payload["status"] == "green"
    assert payload["out_of_credits"] == []


def test_an_exhausted_provider_stays_red_long_after_the_window():
    """The sharp one.

    Gemini is mostly exercised by the Monday meme. A window-only grader sees no
    402 by Wednesday, reports green and stops emailing, while the account is
    still empty and every render that week is degraded. Out-of-credits is a
    state, so a refusal eight days old with no success since is still red.
    """
    payload = pch.grade([_failure(hours=24 * 8)], now=NOW, window_hours=24)
    assert payload["status"] == "red"
    assert payload["providers"][0]["failures_in_window"] == 0, (
        "nothing failed inside the window — the red must come from the standing state, "
        "not from recent activity"
    )


def test_an_empty_ledger_is_not_green():
    """Absence of evidence is not health."""
    payload = pch.grade([], now=NOW)
    assert payload["status"] != "green"
    assert payload["out_of_credits"] == []


def test_rate_limiting_is_not_a_billing_problem():
    """Both stop the work and look alike in a log; only one is fixed by paying."""
    payload = pch.grade([_failure("rate_limited", hours=1)], now=NOW)
    assert payload["status"] == "yellow"
    assert payload["out_of_credits"] == []


def test_fallback_runs_without_a_refusal_read_yellow():
    payload = pch.grade([_ok(hours=3), _fallback(hours=1)], now=NOW)
    assert payload["status"] == "yellow"
    assert payload["fallback_count_in_window"] == 1


def test_a_fallback_older_than_the_window_does_not_raise_the_alarm():
    payload = pch.grade([_ok(hours=1), _fallback(hours=48)], now=NOW, window_hours=24)
    assert payload["status"] == "green"
    assert payload["fallback_count_in_window"] == 0


def test_ordering_is_by_timestamp_not_file_order():
    """Two repos append concurrently, so file order is not chronological."""
    scrambled = [_ok(hours=1), _failure(hours=6)]  # success is NEWER but written first
    assert pch.grade(scrambled, now=NOW)["status"] == "green"
    scrambled_other = [_ok(hours=6), _failure(hours=1)]
    assert pch.grade(scrambled_other, now=NOW)["status"] == "red"


def test_one_provider_failing_does_not_mask_another_that_is_fine():
    payload = pch.grade([_failure(hours=2, provider="gemini"), _ok(hours=1, provider="openai")], now=NOW)
    assert payload["status"] == "red"
    assert payload["out_of_credits"] == ["gemini"]
    states = {p["provider"]: p["state"] for p in payload["providers"]}
    assert states == {"gemini": "credits_exhausted", "openai": "ok"}


def test_malformed_lines_are_counted_not_fatal(tmp_path, monkeypatch):
    log = tmp_path / "provider_health_log.jsonl"
    log.write_text(
        json.dumps(_failure(hours=1)) + "\n" + "{not json\n" + json.dumps(_ok(provider="openai", hours=2)) + "\n"
    )
    monkeypatch.setattr(pch, "PROVIDER_HEALTH_LOG_PATH", log)
    payload = pch.grade(pch.read_ledger(), now=NOW)
    assert payload["status"] == "red"
    assert payload["data_quality"]["malformed_ledger_lines"] == 1


def test_a_missing_ledger_is_not_an_exception(tmp_path, monkeypatch):
    monkeypatch.setattr(pch, "PROVIDER_HEALTH_LOG_PATH", tmp_path / "nope.jsonl")
    assert pch.read_ledger() == []


# --- the daily email ---------------------------------------------------------


def _red():
    return pch.grade([_failure(hours=2)], now=NOW)


def test_red_emails():
    assert pch.should_email(_red(), last_sent_date=None, today="2026-11-02") is True


def test_no_second_email_the_same_day():
    assert pch.should_email(_red(), last_sent_date="2026-11-02", today="2026-11-02") is False


def test_a_new_day_emails_again():
    """The operator asked to be told DAILY while it is broken, not once."""
    assert pch.should_email(_red(), last_sent_date="2026-11-01", today="2026-11-02") is True


def test_the_no_evidence_yellow_does_not_email():
    """A daily email that fires because nothing ran trains the operator to ignore it."""
    quiet = pch.grade([], now=NOW)
    assert quiet["status"] == "yellow"
    assert pch.should_email(quiet, last_sent_date=None, today="2026-11-02") is False


def test_fallback_without_a_refusal_still_emails():
    payload = pch.grade([_ok(hours=3), _fallback(hours=1)], now=NOW)
    assert pch.should_email(payload, last_sent_date=None, today="2026-11-02") is True


def test_green_does_not_email():
    payload = pch.grade([_ok(hours=1)], now=NOW)
    assert payload["status"] == "green"
    assert pch.should_email(payload, last_sent_date=None, today="2026-11-02") is False


def test_the_email_names_the_provider_and_offers_the_topup_link():
    subject, text, html = pch.render_email(_red())
    assert "gemini" in subject
    assert "aistudio.google.com" in text and "aistudio.google.com" in html


def test_the_email_body_reports_never_recorded_rather_than_a_blank_success():
    _, text, _ = pch.render_email(_red())
    assert "never recorded" in text


# --- write guard -------------------------------------------------------------


def test_writing_the_production_state_under_test_mode_refuses(monkeypatch):
    """Layer 2. A test that bypasses the conftest redirect must not reach prod."""
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    with pytest.raises(pch.TestModeRefusalError):
        pch.write_state({"status": "red"}, pch._FROZEN_PRODUCTION_STATE_PATH)


def test_writing_a_redirected_path_is_allowed(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    target = tmp_path / "provider_credit_health.json"
    pch.write_state({"status": "green"}, target)
    assert json.loads(target.read_text())["status"] == "green"


def test_the_daily_email_is_captured_not_sent_under_test_mode(monkeypatch):
    """The trap that already burned us once: on 2026-09-25 a pytest run sent
    four real emails to the operator. This surface emails on red, so a test that
    wandered into the send path would mail a fabricated outage.

    Layer 3 of the outbound guard — proves `send_email` actually refuses rather
    than trusting that it does ([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).
    """
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "duckAgent"))
    from helpers.email_helper import send_email

    subject, text, html = pch.render_email(_red())
    result = send_email(subject, html, text)
    assert result["status"] == "blocked_test_mode"
