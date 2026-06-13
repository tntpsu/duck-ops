"""Layer 3 of the three-layer test-isolation policy for Surface 16:
post-suite audit that the PRODUCTION build_next_queue.json contains no
test fixtures. Test candidates use listing_ids / titles prefixed
"test_" — if one ever lands in prod state, a fixture bypassed both the
conftest redirect and the DUCK_TEST_MODE write guard.

Canonical sibling: test_no_test_pollution_in_occasion_intel.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD_BUILD_NEXT = Path("/Users/philtullai/ai-agents/duck-ops/state/build_next_queue.json")


def test_production_build_next_queue_has_no_test_markers():
    if not PROD_BUILD_NEXT.exists():
        return  # Producer hasn't run yet — nothing to audit.
    payload = json.loads(PROD_BUILD_NEXT.read_text(encoding="utf-8"))
    entries = (payload.get("queue") or []) + (payload.get("suppressed") or [])
    polluted = [
        e.get("listing_id") or e.get("title")
        for e in entries
        if str(e.get("listing_id") or "").startswith("test_")
        or str(e.get("title") or "").lower().startswith("test ")
    ]
    assert not polluted, (
        f"Production build_next_queue.json contains test markers {polluted} — "
        "a test bypassed both isolation layers. Find it, fix it, then re-run "
        "runtime/build_next_engine.py to restore real state."
    )
