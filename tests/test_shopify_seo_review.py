from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import shopify_seo_review


class ShopifySeoReviewTests(unittest.TestCase):
    def test_build_review_uses_top_actions_and_proposals(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review_state_dir = root / "state" / "shopify_seo_review"
            review_run_dir = review_state_dir / "runs"
            output_path = root / "output" / "operator" / "shopify_seo_review.md"

            audit_payload = {
                "generated_at": "2026-04-14T12:00:00-04:00",
                "shopify_domain": "example.myshopify.com",
                "top_actions": [
                    {
                        "id": "gid://shopify/Product/1",
                        "kind": "product",
                        "title": "Bigfoot Duck",
                        "resource_url": "/products/bigfoot-duck",
                        "seo_title": "",
                        "seo_description": "",
                        "issues": [{"code": "missing_seo_title", "message": "Missing SEO title."}],
                    }
                ],
            }

            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", review_state_dir), patch.object(
                shopify_seo_review, "REVIEW_RUN_DIR", review_run_dir
            ), patch.object(shopify_seo_review, "REVIEW_OUTPUT_MD", output_path), patch.object(
                shopify_seo_review, "build_shopify_seo_audit", return_value=audit_payload
            ), patch.object(
                shopify_seo_review,
                "_generate_proposals",
                return_value=[
                    {
                        "id": "gid://shopify/Product/1",
                        "seo_title": "Bigfoot Duck collectible flock favorite",
                        "seo_description": "Too short",
                        "rationale": "Fills missing product metadata with stronger search intent.",
                    }
                ],
            ):
                payload = shopify_seo_review.build_shopify_seo_review(limit=10, force_audit=True)

            self.assertEqual(payload["item_count"], 1)
            self.assertNotIn("collectible flock favorite", payload["items"][0]["proposed_seo_title"].lower())
            self.assertGreaterEqual(len(payload["items"][0]["proposed_seo_description"]), 150)
            self.assertTrue((review_run_dir / f"{payload['run_id']}.json").exists())
            self.assertTrue(output_path.exists())

    def test_email_render_uses_reply_apply_language(self) -> None:
        subject, text_body, html_body = shopify_seo_review.render_shopify_seo_review_email(
            {
                "run_id": "shopify_seo_20260414_120000",
                "generated_at": "2026-04-14T12:00:00-04:00",
                "shopify_domain": "example.myshopify.com",
                "items": [
                    {
                        "title": "About Us",
                        "kind": "page",
                        "resource_url": "/pages/about-us",
                        "issues": [{"message": "Missing SEO title."}],
                        "current_seo_title": "",
                        "current_seo_description": "",
                        "proposed_seo_title": "About MyJeepDuck - Collectible Duck Makers and Gift Ideas",
                        "proposed_seo_description": "Learn how MyJeepDuck creates collectible dashboard ducks, custom gifts, and playful flock favorites for ducking fans and curious shoppers.",
                        "rationale": "Adds missing SEO metadata to a key trust-building page.",
                    }
                ],
            }
        )

        self.assertIn("FLOW:shopify_seo", subject)
        self.assertIn("Reply apply", text_body)
        self.assertIn("Reply <code>apply</code>", html_body)

    def test_missing_only_bulk_review_keeps_existing_fields_unchanged(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review_state_dir = root / "state" / "shopify_seo_review"
            review_run_dir = review_state_dir / "runs"
            output_path = root / "output" / "operator" / "shopify_seo_review.md"

            audit_payload = {
                "generated_at": "2026-04-14T12:00:00-04:00",
                "shopify_domain": "example.myshopify.com",
                "resources": [
                    {
                        "id": "gid://shopify/Product/1",
                        "kind": "product",
                        "title": "Bigfoot Duck",
                        "resource_url": "/products/bigfoot-duck",
                        "seo_title": "",
                        "seo_description": "Already strong description.",
                        "issues": [{"code": "missing_seo_title", "message": "Missing SEO title."}],
                    },
                    {
                        "id": "gid://shopify/Page/2",
                        "kind": "page",
                        "title": "About Us",
                        "resource_url": "/pages/about-us",
                        "seo_title": "",
                        "seo_description": "",
                        "issues": [
                            {"code": "missing_seo_title", "message": "Missing SEO title."},
                            {"code": "missing_seo_description", "message": "Missing SEO description."},
                        ],
                    },
                    {
                        "id": "gid://shopify/Product/3",
                        "kind": "product",
                        "title": "Weak Product",
                        "resource_url": "/products/weak",
                        "seo_title": "Existing title",
                        "seo_description": "Existing description",
                        "issues": [{"code": "short_seo_description", "message": "Short."}],
                    },
                ],
            }

            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", review_state_dir), patch.object(
                shopify_seo_review, "REVIEW_RUN_DIR", review_run_dir
            ), patch.object(shopify_seo_review, "REVIEW_OUTPUT_MD", output_path), patch.object(
                shopify_seo_review, "build_shopify_seo_audit", return_value=audit_payload
            ):
                payload = shopify_seo_review.build_shopify_seo_review(limit=0, force_audit=True, review_type="missing_only_bulk")

            self.assertEqual(payload["review_type"], "missing_only_bulk")
            self.assertEqual(payload["item_count"], 2)
            by_id = {item["id"]: item for item in payload["items"]}
            first = by_id["gid://shopify/Product/1"]
            self.assertTrue(first["apply_seo_title"])
            self.assertFalse(first["apply_seo_description"])
            self.assertEqual(first["proposed_seo_description"], "Already strong description.")
            self.assertIn("Existing SEO metadata will be left unchanged", payload["approval_action"])

    def test_finalize_sentence_avoids_cutting_last_word(self) -> None:
        value = shopify_seo_review._finalize_sentence(
            "Shop Football Ducks at MyJeepDuck for dashboard decor, gift-ready ducking fun, and playful collectible style that helps your flock stand out anywhere MyJeepDuck",
            max_len=160,
        )
        self.assertTrue(value.endswith("."))
        self.assertNotIn("MyJeepDuc.", value)
        self.assertLessEqual(len(value), 160)
        trailing = shopify_seo_review._finalize_sentence(
            "Explore Mix & Match at MyJeepDuck for collectible ducks, custom gift ideas, and playful flock favorites built for dashboard displays, ducking fans, and",
            max_len=160,
        )
        self.assertFalse(trailing.endswith("and."))

    def test_issue_category_batch_only_updates_requested_field_and_sets_auto_next(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review_state_dir = root / "state" / "shopify_seo_review"
            review_run_dir = review_state_dir / "runs"
            output_path = root / "output" / "operator" / "shopify_seo_review.md"

            audit_payload = {
                "generated_at": "2026-04-14T12:00:00-04:00",
                "shopify_domain": "example.myshopify.com",
                "resources": [
                    {
                        "id": "gid://shopify/Product/1",
                        "kind": "product",
                        "title": "Bigfoot Duck",
                        "resource_url": "/products/bigfoot-duck",
                        "seo_title": "This title is dramatically too long for search results and should be trimmed right away",
                        "seo_description": "Already strong description that should stay in place for this test resource.",
                        "issues": [{"code": "long_seo_title", "message": "Long title."}],
                    }
                ],
            }

            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", review_state_dir), patch.object(
                shopify_seo_review, "REVIEW_RUN_DIR", review_run_dir
            ), patch.object(shopify_seo_review, "REVIEW_OUTPUT_MD", output_path), patch.object(
                shopify_seo_review, "build_shopify_seo_audit", return_value=audit_payload
            ):
                payload = shopify_seo_review.build_shopify_seo_review(
                    limit=0,
                    force_audit=True,
                    review_type="issue_category_batch",
                    issue_category="long_title",
                    auto_send_next_category=True,
                )

            self.assertEqual(payload["category_label"], "SEO titles too long")
            self.assertEqual(payload["item_count"], 1)
            self.assertTrue(payload["auto_send_next_category"])
            item = payload["items"][0]
            self.assertTrue(item["apply_seo_title"])
            self.assertFalse(item["apply_seo_description"])
            self.assertEqual(item["proposed_seo_description"], "Already strong description that should stay in place for this test resource.")
            self.assertIn("DuckAgent will email the next remaining SEO category automatically", payload["approval_action"])

    def test_weak_title_category_batch_uses_existing_issue_lane(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review_state_dir = root / "state" / "shopify_seo_review"
            review_run_dir = review_state_dir / "runs"
            output_path = root / "output" / "operator" / "shopify_seo_review.md"

            audit_payload = {
                "generated_at": "2026-04-14T12:00:00-04:00",
                "shopify_domain": "example.myshopify.com",
                "resources": [
                    {
                        "id": "gid://shopify/Product/7",
                        "kind": "product",
                        "title": "Bigfoot Duck",
                        "resource_url": "/products/bigfoot-duck",
                        "seo_title": "Bigfoot Duck | MyJeepDuck",
                        "seo_description": "Already strong description that should stay in place for this test resource.",
                        "issues": [{"code": "seo_title_matches_raw_title", "message": "Raw match title."}],
                    }
                ],
            }

            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", review_state_dir), patch.object(
                shopify_seo_review, "REVIEW_RUN_DIR", review_run_dir
            ), patch.object(shopify_seo_review, "REVIEW_OUTPUT_MD", output_path), patch.object(
                shopify_seo_review, "build_shopify_seo_audit", return_value=audit_payload
            ):
                payload = shopify_seo_review.build_shopify_seo_review(
                    limit=0,
                    force_audit=True,
                    review_type="issue_category_batch",
                    issue_category="weak_title",
                )

            self.assertEqual(payload["category_label"], "Weak or raw-match SEO titles")
            self.assertEqual(payload["item_count"], 1)
            item = payload["items"][0]
            self.assertTrue(item["apply_seo_title"])
            self.assertFalse(item["apply_seo_description"])
            self.assertEqual(
                item["proposed_seo_description"],
                "Already strong description that should stay in place for this test resource.",
            )

    def test_short_product_titles_keep_duck_keyword_and_avoid_duplicate_brand_padding(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review_state_dir = root / "state" / "shopify_seo_review"
            review_run_dir = review_state_dir / "runs"
            output_path = root / "output" / "operator" / "shopify_seo_review.md"

            audit_payload = {
                "generated_at": "2026-04-20T23:00:00-04:00",
                "shopify_domain": "example.myshopify.com",
                "resources": [
                    {
                        "id": "gid://shopify/Product/8",
                        "kind": "product",
                        "title": "Cow Duck – Farmyard Rubber Duck Collectible for Dash, Farm & Gifts",
                        "resource_url": "/products/cow-duck",
                        "seo_title": "",
                        "seo_description": "Shop Cow Duck at MyJeepDuck for dashboard decor, gift-ready ducking fun, and playful collectible style that helps your flock stand out anywhere.",
                        "issues": [{"code": "missing_seo_title", "message": "Missing SEO title."}],
                    },
                    {
                        "id": "gid://shopify/Product/9",
                        "kind": "product",
                        "title": "E.D.T. – Extra-Duck Terrestrial Alien Duck for Vehicle Dashboard and Sci-Fi Fans",
                        "resource_url": "/products/edt-duck",
                        "seo_title": "",
                        "seo_description": "Shop E.D.T. at MyJeepDuck for dashboard decor, gift-ready ducking fun, and playful collectible style that helps your flock stand out anywhere.",
                        "issues": [{"code": "missing_seo_title", "message": "Missing SEO title."}],
                    },
                ],
            }

            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", review_state_dir), patch.object(
                shopify_seo_review, "REVIEW_RUN_DIR", review_run_dir
            ), patch.object(shopify_seo_review, "REVIEW_OUTPUT_MD", output_path), patch.object(
                shopify_seo_review, "build_shopify_seo_audit", return_value=audit_payload
            ):
                payload = shopify_seo_review.build_shopify_seo_review(
                    limit=0,
                    force_audit=True,
                    review_type="issue_category_batch",
                    issue_category="missing_title",
                )

            proposed_titles = [item["proposed_seo_title"] for item in payload["items"]]
            self.assertTrue(any("Cow Duck" in title for title in proposed_titles))
            self.assertTrue(any("E.D.T. Duck" in title for title in proposed_titles))
            self.assertFalse(any("MyJeepDuck gift idea MyJeepDuck" in title for title in proposed_titles))
            self.assertFalse(any("collectible ducks and gift" in title.lower() for title in proposed_titles))

    def test_privacy_choices_page_gets_distinct_default_title(self) -> None:
        title = shopify_seo_review._default_title_for_resource(
            {
                "kind": "page",
                "title": "Your Privacy Choices",
                "resource_url": "/pages/data-sharing-opt-out",
                "issues": [{"code": "duplicate_seo_title", "message": "Duplicate title."}],
            }
        )

        self.assertEqual(title, "Your Privacy Choices | MyJeepDuck Data Sharing Opt-Out")
        self.assertNotEqual(title, "MyJeepDuck Privacy Policy for Collectible Duck Orders")

    def test_issue_category_batch_supersedes_older_open_run_for_same_category(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            review_state_dir = root / "state" / "shopify_seo_review"
            review_run_dir = review_state_dir / "runs"
            output_path = root / "output" / "operator" / "shopify_seo_review.md"
            review_run_dir.mkdir(parents=True, exist_ok=True)
            review_run_dir.joinpath("older.json").write_text(
                json.dumps(
                    {
                        "run_id": "older",
                        "review_type": "issue_category_batch",
                        "seo_category": "duplicate_title",
                        "category_label": "Duplicate SEO titles",
                        "status": "awaiting_review",
                        "items": [{"id": "gid://shopify/Page/1"}],
                    }
                ),
                encoding="utf-8",
            )

            audit_payload = {
                "generated_at": "2026-04-20T23:50:00-04:00",
                "shopify_domain": "example.myshopify.com",
                "resources": [
                    {
                        "id": "gid://shopify/Page/1",
                        "kind": "page",
                        "title": "Duck Agent - Privacy Policy",
                        "resource_url": "/pages/privacy-policy",
                        "seo_title": "Same title",
                        "seo_description": "Strong description.",
                        "issues": [{"code": "duplicate_seo_title", "message": "Duplicate title."}],
                    }
                ],
            }

            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", review_state_dir), patch.object(
                shopify_seo_review, "REVIEW_RUN_DIR", review_run_dir
            ), patch.object(shopify_seo_review, "REVIEW_OUTPUT_MD", output_path), patch.object(
                shopify_seo_review, "build_shopify_seo_audit", return_value=audit_payload
            ):
                payload = shopify_seo_review.build_shopify_seo_review(
                    limit=0,
                    force_audit=True,
                    review_type="issue_category_batch",
                    issue_category="duplicate_title",
                )

            older_payload = json.loads(review_run_dir.joinpath("older.json").read_text(encoding="utf-8"))
            self.assertEqual(older_payload["status"], "superseded")
            self.assertEqual(older_payload["superseded_by_run_id"], payload["run_id"])


if __name__ == "__main__":
    unittest.main()
