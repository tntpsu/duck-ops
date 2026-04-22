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


if __name__ == "__main__":
    unittest.main()
