"""Surface 66.1 — the Shopify SEO lane must advance past the category it last
worked on, resend a cadence-deferred batch instead of rebuilding it, report
its real status, refresh the audit before the weekly stamp it compares to,
and read the bypass count from the payload key that actually exists."""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import shopify_seo_kickoff  # noqa: E402
import shopify_seo_review  # noqa: E402


def _audit_with(categories: dict[str, int]) -> dict:
    """Minimal audit payload: N resources per category via the category's first issue code."""
    resources = []
    for category, count in categories.items():
        spec = shopify_seo_review.SEO_REVIEW_CATEGORY_SPECS[category]
        code = sorted(spec["issue_codes"])[0]
        for i in range(count):
            resources.append({"id": f"{category}-{i}", "kind": "product", "title": f"{category} {i}", "issues": [{"code": code, "severity": "medium"}]})
    return {"resources": resources}


class CursorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._stale = patch.object(shopify_seo_kickoff, "_audit_is_stale", return_value=False)
        self._build = patch.object(shopify_seo_kickoff, "build_shopify_seo_audit", return_value={})
        self._stale.start()
        self._build.start()
        self.addCleanup(self._stale.stop)
        self.addCleanup(self._build.stop)

    def test_cursor_advances_past_last_category(self) -> None:
        audit = _audit_with({"long_title": 1, "weak_title": 23})
        with patch.object(shopify_seo_kickoff, "_load_latest_review", return_value={"status": "applied", "seo_category": "long_title"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email", return_value={"status": "awaiting_review", "run_id": "r", "category_label": "Weak", "item_count": 23}) as send:
            payload = shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(send.call_args.kwargs["issue_category"], "weak_title")
        self.assertEqual(payload["status"], "emailed")
        self.assertEqual(payload["item_count"], 23)

    def test_cursor_wraps_to_start_when_tail_is_empty(self) -> None:
        audit = _audit_with({"long_title": 1})
        with patch.object(shopify_seo_kickoff, "_load_latest_review", return_value={"status": "applied", "seo_category": "weak_description"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email", return_value={"status": "awaiting_review"}) as send:
            shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(send.call_args.kwargs["issue_category"], "long_title")

    def test_kickoff_status_mirrors_payload_status(self) -> None:
        audit = _audit_with({"long_title": 1})
        with patch.object(shopify_seo_kickoff, "_load_latest_review", return_value={"status": "applied"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email", return_value={"status": "deferred_by_cadence", "run_id": "r"}):
            payload = shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(payload["status"], "deferred_by_cadence")

    def test_deferred_run_is_resent_on_monday_not_rebuilt(self) -> None:
        latest = {"status": "deferred_by_cadence", "run_id": "shopify_seo_weak_title_20260831_002358", "seo_category": "weak_title", "category_label": "Weak"}
        with patch.object(shopify_seo_kickoff, "_load_latest_review", return_value=latest), \
             patch.object(shopify_seo_kickoff, "resend_deferred_shopify_seo_review", return_value={"status": "awaiting_review", "run_id": latest["run_id"], "category_label": "Weak", "item_count": 23}) as resend, \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email") as send:
            payload = shopify_seo_kickoff.kickoff_shopify_seo_review()
        resend.assert_called_once_with(latest["run_id"])
        send.assert_not_called()
        self.assertEqual(payload["status"], "emailed")
        self.assertEqual(payload["run_id"], latest["run_id"])



class AuditFreshnessTests(unittest.TestCase):
    def test_audit_stale_at_six_and_a_half_days(self) -> None:
        with TemporaryDirectory() as tmp:
            p = Path(tmp) / "audit.json"
            with patch.object(shopify_seo_kickoff, "SEO_AUDIT_PATH", p):
                # 6 days 23 hours ago: the kickoff runs 18 minutes BEFORE the
                # weekly stamp, so a strict 7.0d threshold never fired.
                p.write_text(json.dumps({"generated_at": (datetime.now(timezone.utc) - timedelta(days=6, hours=23)).isoformat()}))
                self.assertTrue(shopify_seo_kickoff._audit_is_stale())
                p.write_text(json.dumps({"generated_at": (datetime.now(timezone.utc) - timedelta(days=6)).isoformat()}))
                self.assertFalse(shopify_seo_kickoff._audit_is_stale())


class BypassAndResendTests(unittest.TestCase):
    def test_high_severity_bypass_reads_items(self) -> None:
        payload = {"items": [{"issues": [{"severity": "high"}, {"severity": "medium"}]}, {"issues": [{"severity": "high"}]}]}
        self.assertEqual(shopify_seo_review._count_high_severity_issues(payload), 2)

    def test_resend_returns_none_when_run_is_not_deferred(self) -> None:
        with TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs"
            run_dir.mkdir()
            (run_dir / "r1.json").write_text(json.dumps({"run_id": "r1", "status": "awaiting_review"}))
            with patch.object(shopify_seo_review, "REVIEW_RUN_DIR", run_dir):
                self.assertIsNone(shopify_seo_review.resend_deferred_shopify_seo_review("r1"))
                self.assertIsNone(shopify_seo_review.resend_deferred_shopify_seo_review("missing"))

    def test_resend_regates_and_marks_awaiting_review(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "runs"
            run_dir.mkdir()
            run = {
                "run_id": "r1", "status": "deferred_by_cadence", "generated_at": "2026-08-31T00:23:58-04:00",
                "review_type": "issue_category_batch", "seo_category": "weak_title", "category_label": "Weak",
                "items": [], "item_count": 0, "kind_counts": {}, "approval_action": "Reply apply",
                "shopify_domain": "x.myshopify.com", "auto_send_next_category": True,
            }
            (run_dir / "r1.json").write_text(json.dumps(run))
            sent: dict = {}
            with patch.object(shopify_seo_review, "REVIEW_RUN_DIR", run_dir), \
                 patch.object(shopify_seo_review, "_latest_path", return_value=root / "latest.json"), \
                 patch.object(shopify_seo_review, "_ensure_duckagent_imports", return_value=(None, lambda s, h, t: sent.update(subject=s))), \
                 patch.object(shopify_seo_review, "render_shopify_seo_review_email", return_value=("subj", "text", "<p>html</p>")), \
                 patch.object(shopify_seo_review, "log_cadence_decision"), \
                 patch.object(shopify_seo_review, "should_send_email", return_value=_decision(True)):
                payload = shopify_seo_review.resend_deferred_shopify_seo_review("r1")
        self.assertEqual(payload["status"], "awaiting_review")
        self.assertEqual(sent["subject"], "subj")
        self.assertNotIn("deferred_since", payload)
        self.assertIn("emailed_at", payload)


def _decision(should_send: bool):
    from email_cadence_gate import CadenceDecision
    return CadenceDecision(surface_name="shopify_seo", should_send=should_send, reason="test", cadence="weekly_monday", next_send_iso=None)


if __name__ == "__main__":
    unittest.main()
