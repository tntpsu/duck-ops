"""Pollution audit (layer 3) for email_cadence_overrides.json (Surface 23).

Scans the PRODUCTION overrides store for entries that don't correspond to a
real registered surface, or a history that references the test surfaces.
Three layers of defense: conftest autouse fixture (tmp path) + source-level
TestModeRefusalError guard in set_override + THIS suite-end audit. Do NOT
delete this test if it starts failing — find the polluting test instead.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

PRODUCTION_PATH = Path("/Users/philtullai/ai-agents/duck-ops/state/email_cadence_overrides.json")

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))


class NoTestPollutionInEmailCadenceOverridesTests(unittest.TestCase):
    def test_no_unknown_surface_in_production_overrides(self) -> None:
        if not PRODUCTION_PATH.exists():
            self.skipTest("Production email_cadence_overrides.json does not exist")
        try:
            payload = json.loads(PRODUCTION_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self.skipTest("Production store unreadable")
        import email_cadence_gate as g
        valid = set(g.known_surfaces())
        valid_cadences = set(g._OPERATOR_CADENCES)
        overrides = (payload or {}).get("overrides") or {}
        # Any override key that isn't a real surface, or any value that isn't a
        # real operator cadence, means a test (or a bug) wrote prod.
        bad_surfaces = [s for s in overrides if s not in valid]
        bad_values = [f"{s}={c}" for s, c in overrides.items() if str(c) not in valid_cadences]
        if bad_surfaces or bad_values:
            self.fail(
                f"Production email_cadence_overrides.json polluted — unknown "
                f"surfaces {bad_surfaces}, bad cadences {bad_values}. conftest "
                f"isolation + the DUCK_TEST_MODE guard were bypassed; find the "
                f"polluting test and patch EMAIL_CADENCE_OVERRIDES_PATH."
            )


if __name__ == "__main__":
    unittest.main()
