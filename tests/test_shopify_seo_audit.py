from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runtime"))

import shopify_seo_audit as audit


class ShopifySeoAuditTests(unittest.TestCase):
    def test_resource_url_building(self) -> None:
        self.assertEqual(audit._resource_url("product", {"handle": "bigfoot-duck"}), "/products/bigfoot-duck")
        self.assertEqual(audit._resource_url("collection", {"handle": "holiday"}), "/collections/holiday")
        self.assertEqual(audit._resource_url("page", {"handle": "contact"}), "/pages/contact")
        self.assertEqual(
            audit._resource_url("article", {"handle": "hello", "blog": {"handle": "news"}}),
            "/blogs/news/hello",
        )

    def test_product_seo_issues_detect_missing_and_length_problems(self) -> None:
        issues = audit._issues_for_resource(
            "product",
            {
                "seo": {
                    "title": "Short title",
                    "description": "",
                }
            },
        )
        codes = {issue["code"] for issue in issues}
        self.assertIn("short_seo_title", codes)
        self.assertIn("missing_seo_description", codes)

    def test_page_seo_uses_metafields(self) -> None:
        issues = audit._issues_for_resource(
            "page",
            {
                "titleTag": {"value": "A" * 80},
                "descriptionTag": {"value": "B" * 60},
            },
        )
        codes = {issue["code"] for issue in issues}
        self.assertIn("long_seo_title", codes)
        self.assertIn("short_seo_description", codes)

    def test_duplicate_titles_are_marked(self) -> None:
        resources = [
            {
                "id": "a",
                "seo_title": "Same title",
                "issues": [],
            },
            {
                "id": "b",
                "seo_title": "Same title",
                "issues": [],
            },
        ]
        audit._decorate_duplicates(resources)
        self.assertTrue(any(issue["code"] == "duplicate_seo_title" for issue in resources[0]["issues"]))
        self.assertTrue(any(issue["code"] == "duplicate_seo_title" for issue in resources[1]["issues"]))


if __name__ == "__main__":
    unittest.main()
