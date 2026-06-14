"""Regression: load_json must treat an unreadable/malformed file the same
as a missing one. A weekly governance review reads sibling state files only
to report freshness; a producer caught mid-write (empty/partial JSON) used
to crash the whole run with JSONDecodeError (data_model_governance_weekly,
2026-06-14)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

_RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(_RUNTIME) not in sys.path:
    sys.path.insert(0, str(_RUNTIME))

from governance_review_common import load_json  # noqa: E402


class LoadJsonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = TemporaryDirectory()
        self.root = Path(self.ctx.name)

    def tearDown(self) -> None:
        self.ctx.cleanup()

    def test_missing_file_returns_default(self) -> None:
        self.assertEqual(load_json(self.root / "nope.json", {"d": 1}), {"d": 1})

    def test_empty_file_returns_default_not_crash(self) -> None:
        p = self.root / "empty.json"
        p.write_text("", encoding="utf-8")
        self.assertEqual(load_json(p, {"d": 2}), {"d": 2})

    def test_partial_write_returns_default(self) -> None:
        p = self.root / "partial.json"
        p.write_text('{"a": 1', encoding="utf-8")  # truncated mid-write
        self.assertEqual(load_json(p, {}), {})

    def test_valid_json_still_loads(self) -> None:
        p = self.root / "ok.json"
        p.write_text('{"generated_at": "2026-06-14"}', encoding="utf-8")
        self.assertEqual(load_json(p, {}), {"generated_at": "2026-06-14"})


if __name__ == "__main__":
    unittest.main()
