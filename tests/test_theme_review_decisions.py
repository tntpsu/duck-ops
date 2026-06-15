"""Override store for theme-review decisions (Surface 20, Phase 1).

Upsert-by-handle persistent record of operator theme decisions — the
analog of catalog_aliases.record_catalog_alias. The classifier consults
it to re-assert the operator's category and never re-flag.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import theme_review_decisions as trd  # noqa: E402


class RecordAndLoadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = TemporaryDirectory()
        self.path = Path(self.ctx.name) / "theme_review_decisions.json"

    def tearDown(self) -> None:
        self.ctx.cleanup()

    def _record(self, handle, cat, dtype="change_to", **kw):
        return trd.record_theme_review_decision(
            handle=handle, chosen_category=cat, decision_type=dtype, path=self.path, **kw)

    def test_record_then_load(self) -> None:
        self._record("test-safari", "Professions & Careers", product_id="123",
                     previous_category="Wild West Adventure",
                     suggested_category="Animals & Pets",
                     flag_reason="keyword_evidence_favors:Animals & Pets",
                     taxonomy_version_at_decision=3)
        loaded = trd.load_theme_review_decisions(self.path)
        self.assertIn("test-safari", loaded)
        d = loaded["test-safari"]
        self.assertEqual(d["chosen_category"], "Professions & Careers")
        self.assertEqual(d["decision_type"], "change_to")
        self.assertEqual(d["suggested_category"], "Animals & Pets")

    def test_upsert_by_handle_latest_wins(self) -> None:
        self._record("test-x", "Animals & Pets", dtype="approve")
        self._record("test-x", "Dogs & Canines", dtype="change_to")
        loaded = trd.load_theme_review_decisions(self.path)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded["test-x"]["chosen_category"], "Dogs & Canines")
        self.assertEqual(loaded["test-x"]["decision_type"], "change_to")

    def test_get_decision_for_handle(self) -> None:
        self._record("test-y", "Food & Beverages", dtype="approve")
        self.assertEqual(trd.get_decision_for_handle("test-y", self.path)["chosen_category"], "Food & Beverages")
        self.assertIsNone(trd.get_decision_for_handle("test-missing", self.path))

    def test_keep_current_decision_type(self) -> None:
        d = self._record("test-keep", "Wild West Adventure", dtype="keep_current")
        self.assertEqual(d["decision_type"], "keep_current")

    def test_atomic_write_valid_json(self) -> None:
        self._record("test-a", "Animals & Pets")
        # File parses cleanly (no partial write).
        self.assertIsInstance(json.loads(self.path.read_text()), dict)

    def test_missing_handle_raises(self) -> None:
        with self.assertRaises(ValueError):
            self._record("", "Animals & Pets")

    def test_missing_category_raises(self) -> None:
        with self.assertRaises(ValueError):
            self._record("test-z", "")

    def test_invalid_decision_type_raises(self) -> None:
        with self.assertRaises(ValueError):
            self._record("test-z", "Animals & Pets", dtype="bogus")

    def test_load_missing_file_is_empty(self) -> None:
        self.assertEqual(trd.load_theme_review_decisions(Path(self.ctx.name) / "nope.json"), {})


class SourceGuardTests(unittest.TestCase):
    def test_refuses_frozen_prod_path_in_test_mode(self) -> None:
        import os
        from unittest import mock
        # Force the write target to the frozen prod path while DUCK_TEST_MODE=1.
        with mock.patch.dict(os.environ, {"DUCK_TEST_MODE": "1"}):
            with self.assertRaises(trd.TestModeRefusalError):
                trd.record_theme_review_decision(
                    handle="test-guard", chosen_category="Animals & Pets",
                    decision_type="approve", path=trd._FROZEN_THEME_REVIEW_DECISIONS_PATH)


if __name__ == "__main__":
    unittest.main()
