"""Schema + idempotency tests for the catalog_aliases writer."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import catalog_aliases


class CatalogAliasesTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "catalog_aliases.json"

    def _load(self) -> list[dict]:
        return json.loads(self.path.read_text(encoding="utf-8"))["aliases"]

    def test_appends_new_alias_creating_file(self) -> None:
        result = catalog_aliases.record_catalog_alias(
            theme="Flamingo Duck",
            product_id="8021019656375",
            product_title="Flamingo Duck",
            product_handle="flamingo-duck",
            path=self.path,
        )
        self.assertIsNotNone(result)
        aliases = self._load()
        self.assertEqual(len(aliases), 1)
        self.assertEqual(aliases[0]["normalized_theme"], "flamingo duck")
        self.assertEqual(aliases[0]["product_id"], "8021019656375")
        self.assertEqual(aliases[0]["product_handle"], "flamingo-duck")
        self.assertTrue(aliases[0]["source_artifact_id"].startswith("operator::flamingo-duck"))

    def test_idempotent_on_same_theme_and_product(self) -> None:
        catalog_aliases.record_catalog_alias(
            theme="flamingo duck", product_id="X", path=self.path,
        )
        second = catalog_aliases.record_catalog_alias(
            theme="Flamingo Duck", product_id="X", path=self.path,
        )
        self.assertIsNone(second)
        self.assertEqual(len(self._load()), 1)

    def test_allows_same_theme_against_different_product(self) -> None:
        # Two products legitimately share a theme — both aliases preserved.
        catalog_aliases.record_catalog_alias(theme="hiking duck", product_id="A", path=self.path)
        catalog_aliases.record_catalog_alias(theme="hiking duck", product_id="B", path=self.path)
        aliases = self._load()
        self.assertEqual(len(aliases), 2)
        self.assertEqual({a["product_id"] for a in aliases}, {"A", "B"})

    def test_normalizes_theme_consistently(self) -> None:
        catalog_aliases.record_catalog_alias(theme="  Pink   Flamingo  Duck  ", product_id="X", path=self.path)
        record = self._load()[0]
        self.assertEqual(record["normalized_theme"], "pink flamingo duck")
        self.assertEqual(record["theme"], "Pink   Flamingo  Duck")

    def test_preserves_existing_entries_on_append(self) -> None:
        # Seed with hand-written entries mirroring the real file.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({
            "aliases": [
                {
                    "theme": "poodle duck",
                    "normalized_theme": "poodle duck",
                    "product_id": "7967436177591",
                    "product_title": "Poodle Duck",
                    "product_handle": "poodle-duck",
                    "recorded_at": "2026-04-03T20:30:03-04:00",
                    "source_artifact_id": "trend::poodle-duck::2026-03-16",
                },
            ],
        }, indent=2), encoding="utf-8")
        catalog_aliases.record_catalog_alias(
            theme="Flamingo Duck", product_id="8021019656375", product_handle="flamingo-duck", path=self.path,
        )
        aliases = self._load()
        self.assertEqual(len(aliases), 2)
        self.assertEqual(aliases[0]["product_handle"], "poodle-duck")
        self.assertEqual(aliases[1]["product_handle"], "flamingo-duck")

    def test_recovers_from_corrupt_file(self) -> None:
        # If the file is unreadable JSON, the writer must not crash — it
        # treats the file as empty and overwrites. Otherwise one bad write
        # could permanently block future appends.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("{ not valid json", encoding="utf-8")
        result = catalog_aliases.record_catalog_alias(
            theme="recovery duck", product_id="Z", path=self.path,
        )
        self.assertIsNotNone(result)
        self.assertEqual(len(self._load()), 1)

    def test_raises_on_missing_theme(self) -> None:
        with self.assertRaises(ValueError):
            catalog_aliases.record_catalog_alias(theme="", product_id="X", path=self.path)
        with self.assertRaises(ValueError):
            catalog_aliases.record_catalog_alias(theme="   ", product_id="X", path=self.path)

    def test_raises_on_missing_product_id(self) -> None:
        with self.assertRaises(ValueError):
            catalog_aliases.record_catalog_alias(theme="x duck", product_id="", path=self.path)

    def test_custom_source_artifact_id_is_respected(self) -> None:
        catalog_aliases.record_catalog_alias(
            theme="x duck",
            product_id="X",
            source_artifact_id="manual::operator-csv-import::2026-05-24",
            path=self.path,
        )
        self.assertEqual(self._load()[0]["source_artifact_id"], "manual::operator-csv-import::2026-05-24")


if __name__ == "__main__":
    unittest.main()
