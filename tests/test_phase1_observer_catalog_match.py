"""match_catalog cap semantics.

2026-08-19 regression: the old `exact_matches[:5]` in dict-iteration order
silently cut fully-matching products with an arbitrary tie-break — the
"football team duck" trend matched 8 college ducks but stored only 5, so
Penn State and both Florida Gators ducks could never rotate into a
football-trend montage. Cap is now CATALOG_MATCH_MAX (10), title-sorted."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from phase1_observer import CATALOG_MATCH_MAX, match_catalog


def _products(titles: list[str]) -> dict[str, dict]:
    return {
        f"90000000{i:02d}": {"title": t, "handle": t.lower().replace(" ", "-"),
                             "tags": "football, team, college duck"}
        for i, t in enumerate(titles)
    }


class MatchCatalogCapTests(unittest.TestCase):
    def test_eight_full_matches_all_stored(self) -> None:
        titles = ["Alabama Duck", "Buffalo Duck", "Drunken Tuna Duck",
                  "Football Ducks", "Michigan Duck", "Penn State Duck",
                  "Florida Gators Duck", "Florida Mascot Duck"]
        result = match_catalog("football team duck", _products(titles), {})
        self.assertEqual(result.status, "covered")
        self.assertEqual(len(result.matching_products), 8)
        stored = {m["title"] for m in result.matching_products}
        self.assertIn("Penn State Duck", stored)
        self.assertIn("Florida Gators Duck", stored)

    def test_over_cap_truncates_deterministically_by_title(self) -> None:
        titles = [f"Duck {chr(ord('A') + i)}" for i in range(14)]
        result = match_catalog("football team duck", _products(titles), {})
        self.assertEqual(len(result.matching_products), CATALOG_MATCH_MAX)
        stored = [m["title"] for m in result.matching_products]
        self.assertEqual(stored, sorted(titles)[:CATALOG_MATCH_MAX])


if __name__ == "__main__":
    unittest.main()
