from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import shopify_seo_kickoff


class ShopifySeoKickoffTests(unittest.TestCase):
    def setUp(self) -> None:
        # The weekly audit-refresh gate would otherwise call the real
        # build_shopify_seo_audit (Shopify API) when the prod cache is
        # stale. Default it off for the review-flow tests; the gate has
        # its own dedicated tests below.
        self._stale = patch.object(shopify_seo_kickoff, "_audit_is_stale", return_value=False)
        self._build = patch.object(shopify_seo_kickoff, "build_shopify_seo_audit", return_value={})
        self._stale.start()
        self._build.start()
        self.addCleanup(self._stale.stop)
        self.addCleanup(self._build.stop)

    def test_kickoff_skips_when_review_is_already_open(self) -> None:
        with patch.object(
            shopify_seo_kickoff,
            "_load_latest_review",
            return_value={
                "run_id": "shopify_seo_duplicate_title_1",
                "status": "awaiting_review",
                "category_label": "Duplicate SEO titles",
            },
        ):
            payload = shopify_seo_kickoff.kickoff_shopify_seo_review()

        self.assertEqual(payload["status"], "skipped_open_review")
        self.assertEqual(payload["category_label"], "Duplicate SEO titles")

    def test_kickoff_requests_next_category_email(self) -> None:
        with patch.object(shopify_seo_kickoff, "_load_latest_review", return_value={"status": "applied"}), patch.object(
            shopify_seo_kickoff, "_load_audit_payload", return_value={"resources": [{"id": "1"}]}
        ), patch.object(
            shopify_seo_kickoff, "_next_issue_category", return_value="missing_description"
        ), patch.object(
            shopify_seo_kickoff,
            "send_shopify_seo_review_email",
            return_value={
                "run_id": "shopify_seo_missing_description_1",
                "category_label": "Missing SEO descriptions",
                "item_count": 4,
            },
        ) as send_mock:
            payload = shopify_seo_kickoff.kickoff_shopify_seo_review()

        self.assertEqual(payload["status"], "emailed")
        self.assertEqual(payload["category_label"], "Missing SEO descriptions")
        self.assertEqual(payload["item_count"], 4)
        send_mock.assert_called_once_with(
            limit=0,
            force_audit=False,
            review_type="issue_category_batch",
            issue_category="missing_description",
            auto_send_next_category=True,
        )

    def test_kickoff_reports_no_remaining_categories(self) -> None:
        with patch.object(shopify_seo_kickoff, "_load_latest_review", return_value={"status": "applied"}), patch.object(
            shopify_seo_kickoff, "_load_audit_payload", return_value={"resources": []}
        ), patch.object(
            shopify_seo_kickoff, "_next_issue_category", return_value=None
        ):
            payload = shopify_seo_kickoff.kickoff_shopify_seo_review()

        self.assertEqual(payload["status"], "no_remaining_categories")


class AuditFreshnessGateTests(unittest.TestCase):
    """2026-06-14: the daily kickoff reused a cached audit and only rebuilt on
    --force-audit, so the snapshot silently drifted to 37 days. The kickoff
    now rebuilds the audit when it's older than SEO_AUDIT_MAX_AGE_DAYS,
    independent of review state."""

    def test_stale_audit_triggers_rebuild_even_when_review_open(self) -> None:
        # A parked review returns early — but the audit must still refresh
        # (otherwise a long-parked review lets it drift forever).
        with patch.object(shopify_seo_kickoff, "_audit_is_stale", return_value=True), \
             patch.object(shopify_seo_kickoff, "build_shopify_seo_audit", return_value={}) as build, \
             patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "awaiting_review", "category_label": "X"}):
            shopify_seo_kickoff.kickoff_shopify_seo_review()
        build.assert_called_once()

    def test_fresh_audit_does_not_rebuild(self) -> None:
        with patch.object(shopify_seo_kickoff, "_audit_is_stale", return_value=False), \
             patch.object(shopify_seo_kickoff, "build_shopify_seo_audit", return_value={}) as build, \
             patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "awaiting_review", "category_label": "X"}):
            shopify_seo_kickoff.kickoff_shopify_seo_review()
        build.assert_not_called()

    def test_age_threshold_boundary(self) -> None:
        import json
        import tempfile
        from datetime import datetime, timezone, timedelta
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "audit.json"
            with patch.object(shopify_seo_kickoff, "SEO_AUDIT_PATH", p):
                self.assertTrue(shopify_seo_kickoff._audit_is_stale())  # missing
                p.write_text(json.dumps({"generated_at": (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()}))
                self.assertFalse(shopify_seo_kickoff._audit_is_stale())
                p.write_text(json.dumps({"generated_at": (datetime.now(timezone.utc) - timedelta(days=9)).isoformat()}))
                self.assertTrue(shopify_seo_kickoff._audit_is_stale())


if __name__ == "__main__":
    unittest.main()
