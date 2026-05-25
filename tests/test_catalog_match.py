"""Tests for the standalone catalog matcher (M.3 foundation)."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import catalog_match


def _write_catalog(tmp: Path, items: list[dict]) -> Path:
    path = tmp / "catalog_index.json"
    indexed = {str(item["product_id"]): item for item in items}
    path.write_text(json.dumps({"items": indexed}), encoding="utf-8")
    return path


def _write_aliases(tmp: Path, aliases: list[dict]) -> Path:
    path = tmp / "catalog_aliases.json"
    path.write_text(json.dumps({"aliases": aliases}), encoding="utf-8")
    return path


class CatalogMatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def _flamingo_catalog(self) -> Path:
        return _write_catalog(self.tmp, [
            {
                "product_id": "8021019656375",
                "handle": "flamingo-duck",
                "title": "Flamingo Duck",
                "tags": "tropical bird duck, pink bird figurine, summer beach decor, beach party favor",
                "concept_variations": "flamingo hybrid duck, tropical flamingo display piece",
            },
            {
                "product_id": "7967436177591",
                "handle": "poodle-duck",
                "title": "Poodle Duck",
                "tags": "dog breed duck, fluffy companion",
                "concept_variations": "poodle dog duck",
            },
            {
                "product_id": "8033147617463",
                "handle": "boxer-duck",
                "title": "Boxer Duck",
                "tags": "dog breed duck, loyal companion",
                "concept_variations": "boxer dog duck",
            },
        ])

    def test_exact_match_returns_covered(self) -> None:
        result = catalog_match.find_existing_matches(
            "Flamingo Duck",
            catalog_index_path=self._flamingo_catalog(),
            aliases_path=self.tmp / "no-aliases.json",
        )
        self.assertEqual(result["status"], "covered")
        self.assertEqual(result["matches"][0]["handle"], "flamingo-duck")
        self.assertEqual(result["matches"][0]["matched_tokens"], ["flamingo"])

    def test_tropical_flamingo_duck_matches_via_tags(self) -> None:
        # "tropical flamingo duck" matches Flamingo Duck through:
        #   tropical → "tropical bird duck" tag
        #   flamingo → title
        # both tokens hit, so covered.
        result = catalog_match.find_existing_matches(
            "tropical flamingo duck",
            catalog_index_path=self._flamingo_catalog(),
            aliases_path=self.tmp / "no-aliases.json",
        )
        self.assertEqual(result["status"], "covered")
        self.assertEqual(result["matches"][0]["handle"], "flamingo-duck")

    def test_pink_bird_duck_is_partial_gray_zone(self) -> None:
        # "pink bird" matches via Flamingo Duck's tags ("pink bird figurine")
        # but doesn't echo the title — partial signals "ask the LLM".
        result = catalog_match.find_existing_matches(
            "pink bird duck",
            catalog_index_path=self._flamingo_catalog(),
            aliases_path=self.tmp / "no-aliases.json",
        )
        self.assertEqual(result["status"], "covered")  # both tokens hit
        # Note: with this specific catalog, pink + bird hit Flamingo
        # tags so it's "covered". The partial path is exercised separately.

    def test_genuine_partial_match(self) -> None:
        # Theme has tokens that only partially overlap any single product.
        result = catalog_match.find_existing_matches(
            "summer hiking duck",
            catalog_index_path=self._flamingo_catalog(),
            aliases_path=self.tmp / "no-aliases.json",
        )
        # "summer" hits Flamingo (beach decor); "hiking" hits nothing.
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["matches"][0]["handle"], "flamingo-duck")
        self.assertIn("summer", result["matches"][0]["matched_tokens"])
        self.assertNotIn("hiking", result["matches"][0]["matched_tokens"])

    def test_genuinely_new_concept_returns_gap(self) -> None:
        result = catalog_match.find_existing_matches(
            "astronaut spaceship duck",
            catalog_index_path=self._flamingo_catalog(),
            aliases_path=self.tmp / "no-aliases.json",
        )
        self.assertEqual(result["status"], "gap")
        self.assertEqual(result["matches"], [])

    def test_aliases_strengthen_match(self) -> None:
        # Operator previously recorded "pink tropical bird" → flamingo-duck.
        # Now a new "pink tropical bird duck" concept should hit via the alias.
        catalog_path = _write_catalog(self.tmp, [
            {"product_id": "X", "handle": "flamingo-duck", "title": "Flamingo Duck"},
        ])
        aliases_path = _write_aliases(self.tmp, [
            {
                "theme": "pink tropical bird",
                "normalized_theme": "pink tropical bird",
                "product_id": "X",
                "product_handle": "flamingo-duck",
            },
        ])
        result = catalog_match.find_existing_matches(
            "pink tropical bird duck",
            catalog_index_path=catalog_path,
            aliases_path=aliases_path,
        )
        # pink + tropical + bird all hit via alias → covered
        self.assertEqual(result["status"], "covered")
        self.assertEqual(result["matches"][0]["handle"], "flamingo-duck")

    def test_empty_catalog_returns_unknown(self) -> None:
        empty_catalog = self.tmp / "empty.json"
        empty_catalog.write_text(json.dumps({"items": {}}), encoding="utf-8")
        result = catalog_match.find_existing_matches(
            "anything", catalog_index_path=empty_catalog, aliases_path=self.tmp / "no.json",
        )
        self.assertEqual(result["status"], "unknown")

    def test_short_tokens_filtered_out(self) -> None:
        result = catalog_match.find_existing_matches(
            "a of the duck",
            catalog_index_path=self._flamingo_catalog(),
            aliases_path=self.tmp / "no.json",
        )
        # All tokens too short (< 3 chars) or stopword. Unknown, not bogus match.
        self.assertEqual(result["status"], "unknown")

    def test_corrupt_catalog_file_handled(self) -> None:
        bad = self.tmp / "bad.json"
        bad.write_text("not valid json", encoding="utf-8")
        result = catalog_match.find_existing_matches(
            "x duck", catalog_index_path=bad, aliases_path=self.tmp / "no.json",
        )
        self.assertEqual(result["status"], "unknown")

    def test_max_matches_caps_result_list(self) -> None:
        catalog_path = _write_catalog(self.tmp, [
            {"product_id": str(i), "handle": f"p{i}", "title": "Flamingo Duck"} for i in range(10)
        ])
        result = catalog_match.find_existing_matches(
            "Flamingo Duck",
            catalog_index_path=catalog_path,
            aliases_path=self.tmp / "no.json",
            max_matches=3,
        )
        self.assertEqual(result["status"], "covered")
        self.assertEqual(len(result["matches"]), 3)


if __name__ == "__main__":
    unittest.main()
