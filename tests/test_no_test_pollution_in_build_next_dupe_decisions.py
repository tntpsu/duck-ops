"""Layer 3 of the three-layer test-isolation policy for Surface 28:
post-suite audit that the PRODUCTION build_next_dupe_decisions.json
contains no test fixtures. Test rulings use titles like "Wiener Dog
Figure" / "X" — if one ever lands in prod state, a fixture bypassed both
the conftest redirect and the DUCK_TEST_MODE write guard in
record_dupe_decision.

Canonical sibling: test_no_test_pollution_in_build_next.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD_DUPE_DECISIONS = Path(
    "/Users/philtullai/ai-agents/duck-ops/state/build_next_dupe_decisions.json"
)

# Titles only the test suite uses (TestDupeFlag in test_build_next_engine.py).
_TEST_TITLE_MARKERS = {"wiener dog figure", "x"}


def test_production_dupe_decisions_has_no_test_markers():
    if not PROD_DUPE_DECISIONS.exists():
        return  # No operator rulings yet — nothing to audit.
    data = json.loads(PROD_DUPE_DECISIONS.read_text(encoding="utf-8"))
    decisions = data.get("decisions", {}) if isinstance(data, dict) else {}
    polluted = [
        rec.get("title")
        for rec in decisions.values()
        if isinstance(rec, dict) and str(rec.get("title") or "").strip().lower() in _TEST_TITLE_MARKERS
    ]
    assert not polluted, (
        f"Production build_next_dupe_decisions.json contains test markers {polluted} — "
        "a test bypassed both isolation layers. Find it, fix it, then remove the "
        "polluting rulings from prod state."
    )
