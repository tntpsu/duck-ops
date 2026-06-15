"""Pollution audit (layer 3) for theme_review_decisions.json (Surface 20).

Scans the PRODUCTION store for decisions whose handle looks like a test
fixture. Three layers of defense: conftest autouse fixture (tmp path) +
source-level TestModeRefusalError guard + THIS suite-end audit. Do NOT
delete this test if it starts failing — find the polluting test instead.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

PRODUCTION_PATH = Path("/Users/philtullai/ai-agents/duck-ops/state/theme_review_decisions.json")

# Handles used by the unit tests start with these markers.
_TEST_HANDLE_PREFIXES = ("test-", "test_", "fixture-", "dummy-")


class NoTestPollutionInThemeReviewDecisionsTests(unittest.TestCase):
    def test_no_test_handles_in_production_store(self) -> None:
        if not PRODUCTION_PATH.exists():
            self.skipTest("Production theme_review_decisions.json does not exist")
        try:
            payload = json.loads(PRODUCTION_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self.skipTest("Production store unreadable")
        offenders = [
            str(d.get("handle"))
            for d in (payload.get("decisions") or [])
            if isinstance(d, dict)
            and str(d.get("handle") or "").strip().lower().startswith(_TEST_HANDLE_PREFIXES)
        ]
        if offenders:
            self.fail(
                f"Production theme_review_decisions.json has {len(offenders)} test "
                f"handle(s): {offenders}. Both conftest isolation and the "
                f"DUCK_TEST_MODE guard were bypassed. Find the polluting test "
                f"(grep tests/ for the handle) and patch its path."
            )


if __name__ == "__main__":
    unittest.main()
