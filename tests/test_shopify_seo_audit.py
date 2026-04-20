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

    def test_audit_flags_raw_match_titles_and_generic_descriptions(self) -> None:
        issues = audit._issues_for_resource(
            "product",
            {
                "title": "Bigfoot Dashboard Duck Collector Figure",
                "seo": {
                    "title": "Bigfoot Dashboard Duck Collector Figure | MyJeepDuck",
                    "description": "Shop MyJeepDuck for collectible ducks, custom gift ideas, and playful dashboard decor.",
                },
            },
        )
        codes = {issue["code"] for issue in issues}
        self.assertIn("seo_title_matches_raw_title", codes)
        self.assertIn("weak_generic_seo_description", codes)

    def test_audit_flags_weak_generic_title_without_specific_terms(self) -> None:
        issues = audit._issues_for_resource(
            "product",
            {
                "title": "Bigfoot Dashboard Duck Collector Figure",
                "seo": {
                    "title": "Collectible Dashboard Duck Gift Idea | MyJeepDuck",
                    "description": "Meet the Bigfoot duck collectible with gift-ready style and playful flock energy for dashboards, swaps, and fun surprises.",
                },
            },
        )
        codes = {issue["code"] for issue in issues}
        self.assertIn("weak_generic_seo_title", codes)

    def test_audit_flags_low_value_seo_copy(self) -> None:
        issues = audit._issues_for_resource(
            "page",
            {
                "title": "Monster Truck Duck Display Guide",
                "titleTag": {"value": "Monster Truck Duck Display Guide | MyJeepDuck"},
                "descriptionTag": {
                    "value": "Shop our collectible ducks, dashboard decor, and gift ideas at MyJeepDuck for playful flock favorites.",
                },
            },
        )
        codes = {issue["code"] for issue in issues}
        self.assertIn("low_value_seo_copy", codes)

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

    def test_near_duplicate_titles_are_marked(self) -> None:
        resources = [
            {
                "id": "a",
                "seo_title": "Bigfoot Duck | MyJeepDuck",
                "issues": [],
            },
            {
                "id": "b",
                "seo_title": "Bigfoot Duck Gift | MyJeepDuck",
                "issues": [],
            },
        ]
        audit._decorate_duplicates(resources)
        self.assertTrue(any(issue["code"] == "near_duplicate_seo_title" for issue in resources[0]["issues"]))
        self.assertTrue(any(issue["code"] == "near_duplicate_seo_title" for issue in resources[1]["issues"]))


if __name__ == "__main__":
    unittest.main()
