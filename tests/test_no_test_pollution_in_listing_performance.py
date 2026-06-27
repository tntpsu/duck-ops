"""Layer 3 of the three-layer test-isolation policy for Surface 39: post-suite
audit that the PRODUCTION listing_performance.json carries no test markers
(sentinel property_id "test-prop" / "test " titles). A hit means a fixture
bypassed both the conftest redirect and the DUCK_TEST_MODE guard.

Canonical sibling: test_no_test_pollution_in_gsc_search_demand.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD = Path("/Users/philtullai/ai-agents/duck-ops/state/listing_performance.json")


def test_production_listing_performance_has_no_test_markers():
    if not PROD.exists():
        return  # Producer hasn't run yet — nothing to audit.
    payload = json.loads(PROD.read_text(encoding="utf-8"))
    assert str(payload.get("property_id") or "") != "test-prop", (
        "Production listing_performance.json has the test property_id — a test "
        "bypassed both isolation layers. Re-run runtime/ga4_listing_performance.py.")
    polluted = [r.get("title") for r in (payload.get("listings") or [])
                if str(r.get("title") or "").lower().startswith("test ")]
    assert not polluted, f"Production listing_performance.json contains test titles {polluted}."
