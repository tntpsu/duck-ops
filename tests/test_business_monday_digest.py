"""Surface 15.5: Monday business digest + cadence fold mode."""
from __future__ import annotations

import json
import sys
from datetime import datetime

import email_cadence_gate as gate
import business_monday_digest as digest


_MONDAY = datetime(2026, 6, 15, 9, 0, 0).astimezone()  # a Monday


class TestFoldMode:
    def test_folded_surface_defers_in_digest_mode(self, monkeypatch):
        monkeypatch.setenv("DUCK_EMAIL_DIGEST_MODE", "1")
        d = gate.should_send_email("reviews", {}, now=_MONDAY)
        assert d.should_send is False
        assert d.reason == "folded_into_monday_business_digest"

    def test_anomaly_bypass_still_fires_in_digest_mode(self, monkeypatch):
        """A ≤2★ review must still break through same-day even when folded."""
        monkeypatch.setenv("DUCK_EMAIL_DIGEST_MODE", "1")
        d = gate.should_send_email("reviews", {"low_rating_count": 3}, now=_MONDAY)
        assert d.should_send is True  # bypass active

    def test_digest_mode_off_keeps_normal_monday_send(self, monkeypatch):
        monkeypatch.delenv("DUCK_EMAIL_DIGEST_MODE", raising=False)
        d = gate.should_send_email("reviews", {}, now=_MONDAY)
        assert d.should_send is True

    def test_digest_surface_itself_sends_on_monday(self, monkeypatch):
        monkeypatch.setenv("DUCK_EMAIL_DIGEST_MODE", "1")
        d = gate.should_send_email("business_digest", {}, now=_MONDAY)
        assert d.should_send is True  # the rollup is never folded

    def test_folded_surface_not_monday_still_defers(self, monkeypatch):
        monkeypatch.setenv("DUCK_EMAIL_DIGEST_MODE", "1")
        tuesday = datetime(2026, 6, 16, 9, 0, 0).astimezone()
        d = gate.should_send_email("profit", {}, now=tuesday)
        assert d.should_send is False


class TestDigestBuilder:
    def test_sections_fail_soft_when_state_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr(digest, "STATE_DIR", tmp_path)  # empty dir
        sections = digest.build_digest_sections()
        assert len(sections) == 4
        # all degraded (empty) but none crash
        assert all(s["status"] in {"empty", "error"} for s in sections)
        assert {s["title"] for s in sections} == {
            "Profit", "Strategy & Recommendations", "Learnings", "Competitors"}

    def test_profit_section_renders_from_state(self, monkeypatch, tmp_path):
        (tmp_path / "profit_intel.json").write_text(json.dumps({
            "yesterday": {"orders": 9, "revenue": 145.36, "margin": 55.1},
            "trend_7d": {"net_delta_pct": 37.9},
        }))
        monkeypatch.setattr(digest, "STATE_DIR", tmp_path)
        sections = {s["title"]: s for s in digest.build_digest_sections()}
        assert sections["Profit"]["status"] == "ok"
        assert "9 orders" in sections["Profit"]["body"]

    def test_render_html_has_section_badges(self):
        sections = [
            {"title": "Profit", "status": "ok", "body": "good"},
            {"title": "Learnings", "status": "empty", "body": "none"},
        ]
        html, text = digest.render_digest_html(sections)
        assert "Profit" in html and "Learnings" in html
        assert "🟢" in html and "🟡" in html
        assert "Monday Business Digest" in text


class TestSendPathRegression:
    """The send path called log_cadence_decision('business_digest', decision)
    with 2 positional args against a 1-arg signature — crashing every Monday
    so the digest never sent. main() --send-email must run the cadence-log +
    send path with no TypeError. The builder tests above never reached it."""

    def test_send_path_does_not_crash_on_cadence_log(self, monkeypatch, tmp_path):
        monkeypatch.setattr(digest, "STATE_DIR", tmp_path)
        monkeypatch.setattr(gate, "DECISION_LOG_PATH", tmp_path / "decisions.jsonl")
        sent: dict = {}
        monkeypatch.setattr(digest, "_ensure_send_email",
                            lambda: (lambda subject, html, text: sent.update(subject=subject)))
        monkeypatch.setattr(sys, "argv", ["business_monday_digest.py", "--send-email", "--force"])
        assert digest.main() == 0
        assert "subject" in sent  # reached send_email past the cadence-log line
