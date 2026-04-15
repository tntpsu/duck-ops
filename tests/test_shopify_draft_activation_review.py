from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import shopify_draft_activation_review


class ShopifyDraftActivationReviewTests(unittest.TestCase):
    def test_build_review_sorts_ready_first_and_writes_artifacts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review_state_dir = root / "state" / "shopify_draft_activation_review"
            review_run_dir = review_state_dir / "runs"
            output_path = root / "output" / "operator" / "shopify_draft_activation_review.md"

            fake_products = [
                {
                    "id": 202,
                    "title": "Blocked Duck",
                    "handle": "blocked-duck",
                    "status": "draft",
                    "body_html": "",
                    "vendor": "MyJeepDuck",
                    "product_type": "Collectible Duck",
                    "tags": "duck",
                    "updated_at": "2026-04-14T18:00:00-04:00",
                    "images": [],
                    "variants": [{"sku": "", "price": "0", "inventory_quantity": 0}],
                },
                {
                    "id": 101,
                    "title": "Ready Duck",
                    "handle": "ready-duck",
                    "status": "draft",
                    "body_html": "<p>Ready to go</p>",
                    "vendor": "MyJeepDuck",
                    "product_type": "Collectible Duck",
                    "tags": "duck, dashboard",
                    "updated_at": "2026-04-14T18:00:00-04:00",
                    "images": [{"src": "https://example.com/ready.png"}],
                    "variants": [{"sku": "RDY-1", "price": "24.99", "inventory_quantity": 3}],
                },
            ]

            def fake_imports():
                return (
                    lambda *args, **kwargs: None,
                    lambda **kwargs: "<html></html>",
                    lambda *args, **kwargs: "badge",
                    lambda *args, **kwargs: "card",
                    lambda href, label: f"{label}:{href}",
                    lambda *args, **kwargs: [],
                    lambda value: f"gid://shopify/Product/{value}",
                    lambda query, variables=None: {
                        "data": {
                            "product": {
                                "status": "draft",
                                "seo": {
                                    "title": "Ready SEO" if variables and str(variables.get("id", "")).endswith("/101") else "",
                                    "description": "Ready description" if variables and str(variables.get("id", "")).endswith("/101") else "",
                                },
                                "category": {
                                    "id": "gid://shopify/TaxonomyCategory/1" if variables and str(variables.get("id", "")).endswith("/101") else "",
                                    "fullName": "Vehicles > Accessories" if variables and str(variables.get("id", "")).endswith("/101") else "",
                                },
                            }
                        }
                    },
                )

            with patch.object(shopify_draft_activation_review, "REVIEW_STATE_DIR", review_state_dir), patch.object(
                shopify_draft_activation_review, "REVIEW_RUN_DIR", review_run_dir
            ), patch.object(shopify_draft_activation_review, "REVIEW_OUTPUT_MD", output_path), patch.object(
                shopify_draft_activation_review, "_ensure_duckagent_imports", side_effect=fake_imports
            ), patch.object(
                shopify_draft_activation_review, "_fetch_draft_products", return_value=fake_products
            ):
                payload = shopify_draft_activation_review.build_shopify_draft_activation_review()

            self.assertEqual(payload["item_count"], 2)
            self.assertEqual(payload["ready_count"], 1)
            self.assertEqual(payload["blocked_count"], 1)
            self.assertEqual(payload["suggestion_count"], 1)
            self.assertEqual(payload["items"][0]["title"], "Ready Duck")
            self.assertTrue(payload["items"][0]["quality_suggestions"])
            self.assertTrue((review_run_dir / f"{payload['run_id']}.json").exists())
            self.assertTrue(output_path.exists())

    def test_email_render_uses_publish_apply_language(self) -> None:
        subject, text_body, html_body = shopify_draft_activation_review.render_shopify_draft_activation_email(
            {
                "run_id": "shopify_draft_activation_20260414_193000",
                "item_count": 2,
                "ready_count": 1,
                "blocked_count": 1,
                "items": [
                    {
                        "legacy_product_id": 101,
                        "title": "Ready Duck",
                        "status": "draft",
                        "ready_for_activation": True,
                        "admin_url": "https://admin.shopify.com/store/myjeepduck/products/101",
                        "image_count": 3,
                        "category_name": "Vehicles > Accessories",
                    },
                    {
                        "legacy_product_id": 202,
                        "title": "Blocked Duck",
                        "status": "draft",
                        "ready_for_activation": False,
                        "admin_url": "https://admin.shopify.com/store/myjeepduck/products/202",
                        "blocking_issues": ["SEO present: SEO title or description is missing."],
                    },
                ],
            },
            render_report_email=lambda **kwargs: kwargs.get("body_html", ""),
            report_badge=lambda *args, **kwargs: "badge",
            report_card=lambda _title, body, **kwargs: body,
            report_link=lambda href, label: f"{label}:{href}",
        )

        self.assertIn("FLOW:shopify_draft_activation", subject)
        self.assertIn('Reply "publish" or "apply"', text_body)
        self.assertIn("Quality suggestions are advisory only", text_body)
        self.assertIn("Reply <strong>\"publish\"</strong> or <strong>\"apply\"</strong>", html_body)
        self.assertIn("Quality suggestions are advisory only", html_body)


if __name__ == "__main__":
    unittest.main()
