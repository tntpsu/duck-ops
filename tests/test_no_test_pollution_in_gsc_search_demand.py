"""Layer 3 of the three-layer test-isolation policy for Surface 38: post-suite
audit that the PRODUCTION gsc_search_demand.json carries no test markers. The
producer tests use a sentinel site_url ("test://"); if one lands in prod state,
a fixture bypassed both the conftest redirect and the DUCK_TEST_MODE guard.

Canonical sibling: test_no_test_pollution_in_build_next.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD_GSC = Path("/Users/philtullai/ai-agents/duck-ops/state/gsc_search_demand.json")


def test_production_gsc_search_demand_has_no_test_markers():
    if not PROD_GSC.exists():
        return  # Producer hasn't run yet — nothing to audit.
    payload = json.loads(PROD_GSC.read_text(encoding="utf-8"))
    site = str(payload.get("site_url") or "")
    queries = [q.get("query") for q in (payload.get("top_queries") or [])]
    assert not site.startswith("test://"), (
        f"Production gsc_search_demand.json has test site_url {site!r} — a test "
        "bypassed both isolation layers. Find it, fix it, then re-run "
        "runtime/gsc_search_demand.py to restore real state.")
    polluted = [q for q in queries if str(q or "").lower().startswith("test ")]
    assert not polluted, f"Production gsc_search_demand.json contains test queries {polluted}."
