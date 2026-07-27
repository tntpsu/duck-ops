"""Layer 3 of the three-layer test-isolation policy for Surface 63: post-suite
audit that the PRODUCTION seo_outcome_intel.json carries no test markers. The
producer tests use fixture receipt_ids ("r-...") and duck-a/duck-b URLs; if one
lands in prod state, a fixture bypassed both the conftest redirect and the
DUCK_TEST_MODE guard.

Canonical sibling: test_no_test_pollution_in_gsc_search_demand.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD_INTEL = Path("/Users/philtullai/ai-agents/duck-ops/state/seo_outcome_intel.json")


def test_production_seo_outcome_intel_has_no_test_markers():
    if not PROD_INTEL.exists():
        return  # Producer hasn't run yet — nothing to audit.
    payload = json.loads(PROD_INTEL.read_text(encoding="utf-8"))
    urls = [str(p.get("resource_url") or "") for p in payload.get("pages") or []]
    polluted = [u for u in urls if u in ("/products/duck-a", "/products/duck-b")]
    assert not polluted, (
        f"Production seo_outcome_intel.json contains fixture pages {polluted} — a "
        "test bypassed both isolation layers. Find it, fix it, then re-run "
        "runtime/seo_outcome_intel.py to restore real state.")
    receipt_ids = [str(p.get("receipt_id") or "") for p in payload.get("pages") or []]
    fixture_ids = [r for r in receipt_ids if r.startswith("r-")]
    assert not fixture_ids, f"Production seo_outcome_intel.json contains fixture receipts {fixture_ids}."
