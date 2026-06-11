"""Layer 3 of the three-layer test-isolation policy for Surface 13:
post-suite audit that the PRODUCTION occasion_intel.json contains no
test fixtures. Test calendars use occasion ids prefixed "test_" —
if one ever lands in prod state, a fixture bypassed both the conftest
redirect and the DUCK_TEST_MODE write guard.

Canonical sibling: test_no_test_pollution_in_workflow_control.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD_OCCASION_INTEL = Path("/Users/philtullai/ai-agents/duck-ops/state/occasion_intel.json")


def test_production_occasion_intel_has_no_test_markers():
    if not PROD_OCCASION_INTEL.exists():
        return  # Producer hasn't run yet — nothing to audit.
    payload = json.loads(PROD_OCCASION_INTEL.read_text(encoding="utf-8"))
    polluted = [
        occ.get("id")
        for occ in payload.get("active_occasions") or []
        if str(occ.get("id") or "").startswith("test_")
    ]
    assert not polluted, (
        f"Production occasion_intel.json contains test occasion ids {polluted} — "
        "a test bypassed both isolation layers. Find it, fix it, then re-run "
        "runtime/occasion_engine.py to restore real state."
    )
