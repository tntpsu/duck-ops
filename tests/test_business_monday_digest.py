"""Surface 15.5: Monday business digest + cadence fold mode.
Surface 66.1/66.3: fold contract + the rebuilt flagship sections."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta

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


class TestFoldContract:
    """A fold reason that names a destination is a promise. The weak_title SEO
    batch (23 items) was 'folded_into_monday_business_digest' on 2026-07-06 and
    2026-08-31 and never rendered anywhere — the digest had no SEO section."""

    def test_every_folded_surface_has_a_digest_section(self):
        assert digest.fold_contract_gaps() == set()
        assert gate.DIGEST_FOLDED_SURFACES <= set(digest.DIGEST_SECTION_BUILDERS)

    def test_shopify_seo_is_not_a_folded_surface(self, monkeypatch):
        # Approval lanes with a reply verb send their own email even in digest
        # mode; the digest only LISTS them under "Decisions waiting on you".
        assert "shopify_seo" not in gate.DIGEST_FOLDED_SURFACES
        monkeypatch.setenv("DUCK_EMAIL_DIGEST_MODE", "1")
        d = gate.should_send_email("shopify_seo", {"high_severity_issue_count": 0}, now=_MONDAY)
        assert d.should_send is True
        assert d.reason != "folded_into_monday_business_digest"


def _isolate(monkeypatch, tmp_path):
    monkeypatch.setattr(digest, "STATE_DIR", tmp_path / "state")
    monkeypatch.setattr(digest, "OUTPUT_DIGESTS_DIR", tmp_path / "digests")
    monkeypatch.setattr(digest, "COMPETITOR_REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(digest, "DUCK_AGENT_RUNS_DIR", tmp_path / "runs")
    monkeypatch.setattr(digest, "DUCK_AGENT_ROOT", tmp_path / "duckAgent")
    for sub in ("state", "digests", "reports", "runs", "duckAgent"):
        (tmp_path / sub).mkdir()
    return tmp_path


class TestDigestSections:
    def test_sections_fail_soft_when_state_missing(self, monkeypatch, tmp_path):
        _isolate(monkeypatch, tmp_path)
        sections = digest.build_digest_sections()
        assert len(sections) == len(digest.DIGEST_SECTION_BUILDERS)
        titles = {s["title"] for s in sections}
        assert {"Profit", "Decisions waiting on you", "Build next (evidence)", "Competitors", "Reviews"} <= titles
        # Nothing crashes; the decisions section is the only one with an
        # honest "ok" on empty state (nothing waiting IS the good outcome).
        assert all(s["status"] in {"empty", "error", "ok"} for s in sections)
        assert {s["title"]: s["status"] for s in sections}["Profit"] == "empty"

    def test_profit_section_has_week_over_week(self, monkeypatch, tmp_path):
        root = _isolate(monkeypatch, tmp_path)
        per_day = [
            {"date": f"2026-08-{day:02d}", "orders": 10 if day <= 24 else 5, "revenue": 200.0 if day <= 24 else 100.0}
            for day in range(18, 32)
        ]
        (root / "state" / "profit_intel.json").write_text(json.dumps({
            "yesterday": {"orders": 9, "revenue": 145.36, "margin": 55.1},
            "trend_7d": {"net_delta_pct": 37.9},
            "per_day": per_day,
            "channel_mix_7d": {"shopify_orders": 3, "etsy_orders": 4, "shopify_revenue": 60.0, "etsy_revenue": 80.0},
        }))
        sections = {s["title"]: s for s in digest.build_digest_sections()}
        body = sections["Profit"]["body"]
        assert sections["Profit"]["status"] == "ok"
        assert "9 orders" in body
        assert "Last 7 days: 35 orders / $700.00 vs 70 orders / $1,400.00" in body
        assert "-50.0% revenue" in body
        assert "Shopify 3 orders / $60.00" in body

    def test_decisions_section_lists_open_seo_batch(self, monkeypatch, tmp_path):
        root = _isolate(monkeypatch, tmp_path)
        seo_dir = root / "state" / "shopify_seo_review"
        seo_dir.mkdir()
        (seo_dir / "latest.json").write_text(json.dumps({
            "status": "awaiting_review",
            "category_label": "Weak or raw-match SEO titles",
            "item_count": 23,
            "email_subject": "MJD: [shopify_seo] Weak | FLOW:shopify_seo | RUN:x | ACTION:review",
            "emailed_at": datetime.now().astimezone().isoformat(),
        }))
        sections = {s["title"]: s for s in digest.build_digest_sections()}
        body = sections["Decisions waiting on you"]["body"]
        assert "Weak or raw-match SEO titles: 23 item(s)" in body
        assert "Reply `apply`" in body
        assert sections["Decisions waiting on you"]["status"] == "attention"

    def test_decisions_section_flags_stale_deferred_batch(self, monkeypatch, tmp_path):
        root = _isolate(monkeypatch, tmp_path)
        seo_dir = root / "state" / "shopify_seo_review"
        seo_dir.mkdir()
        (seo_dir / "latest.json").write_text(json.dumps({
            "status": "deferred_by_cadence",
            "category_label": "Weak or raw-match SEO titles",
            "item_count": 23,
            "deferred_since": (datetime.now().astimezone() - timedelta(days=9)).isoformat(),
        }))
        sections = {s["title"]: s for s in digest.build_digest_sections()}
        assert sections["Decisions waiting on you"]["status"] == "error"
        assert "never emailed" in sections["Decisions waiting on you"]["body"]

    def test_build_section_cross_references_trend_and_competitor(self, monkeypatch, tmp_path):
        root = _isolate(monkeypatch, tmp_path)
        (root / "digests" / "trend_digest__2026-09-06.json").write_text(json.dumps({
            "items": [{
                "theme": "female nurse duck",
                "action_frame": "build",
                "reasoning": ["Commercial signal 30/30 from sold 7d `14`, sold 30d `21`, quantity drop `14`."],
                "trend_metadata": {"catalog_status": "gap"},
            }],
        }))
        (root / "reports" / "2026-09-06_competitor_report.json").write_text(json.dumps({
            "ai_insights": {},
            "trending_products": [{"title": "3D Printed Female Nurse Duck | PLA", "shop_name": "StarkPrintingCo"}],
            "rising_competitors": [{"shop_name": "StarkPrintingCo", "exact_sold_7d": 186, "momentum_score": 64.3}],
        }))
        sections = {s["title"]: s for s in digest.build_digest_sections()}
        body = sections["Build next (evidence)"]["body"]
        assert "female nurse duck" in body
        assert "sold 14 in 7d / 21 in 30d" in body
        assert "catalog gap" in body
        assert "StarkPrintingCo's trending" in body

    def test_competitor_section_skips_snapshot_stub(self, monkeypatch, tmp_path):
        root = _isolate(monkeypatch, tmp_path)
        full = root / "reports" / "2026-09-06_competitor_report.json"
        full.write_text(json.dumps({
            "ai_insights": {},
            "trending_products": [{"title": "x", "shop_name": "A"}],
            "rising_competitors": [{"shop_name": "WilderkindStudioCo", "exact_sold_7d": 227, "momentum_score": 94.15}],
            "comparison_metrics": [{"keyword_category": "core_ducks", "my_rank": 7, "total_competitors": 9}],
            "category_gaps": [{"category": "core_ducks", "competitor_listings": 430, "my_listings": 100}],
            "my_shop_velocity": {"actual_sold_7d": 72},
        }))
        stub = root / "reports" / "2026-09-07_competitor_report.json"
        stub.write_text(json.dumps({"ai_insights": {"_snapshot_only": True}, "trending_products": [], "rising_competitors": []}))
        sections = {s["title"]: s for s in digest.build_digest_sections()}
        body = sections["Competitors"]["body"]
        assert "WilderkindStudioCo sold 227 units in 7d (you: 72)" in body
        assert "core ducks #7/9" in body
        assert "competitors 430 listings vs your 100" in body

    def test_learnings_section_omitted_when_empty(self, monkeypatch, tmp_path):
        root = _isolate(monkeypatch, tmp_path)
        (root / "state" / "current_learnings.json").write_text(json.dumps({"changes_since_previous": [], "change_notifier": {}}))
        sections = digest.build_digest_sections()
        html, text = digest.render_digest_html(sections)
        assert "Learnings" not in text
        assert "Learnings" not in html

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
        _isolate(monkeypatch, tmp_path)
        monkeypatch.setattr(gate, "DECISION_LOG_PATH", tmp_path / "decisions.jsonl")
        sent: dict = {}
        monkeypatch.setattr(digest, "_ensure_send_email",
                            lambda: (lambda subject, html, text: sent.update(subject=subject)))
        monkeypatch.setattr(sys, "argv", ["business_monday_digest.py", "--send-email", "--force"])
        assert digest.main() == 0
        assert "subject" in sent  # reached send_email past the cadence-log line
