"""Surface 58 layer-3 isolation: the live production customer-ask candidates
file must never contain golden-fixture sentinels. If it does, a test wrote to
prod (conftest redirect or the DUCK_TEST_MODE write guard slipped). Mirrors
tests/test_no_test_pollution_in_demand_intel.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD_PATH = Path("/Users/philtullai/ai-agents/duck-ops/state/customer_ask_candidates.json")
# Distinctive fixture-only subjects that should never appear in real output.
SENTINELS = {"green bay packers", "dachshund"}


def test_no_fixture_pollution_in_prod_customer_ask_candidates():
    if not PROD_PATH.exists():
        return  # nothing produced yet — clean by definition
    payload = json.loads(PROD_PATH.read_text(encoding="utf-8"))
    subjects = {str(c.get("subject", "")).lower() for c in payload.get("candidates", [])}
    leaked = SENTINELS & subjects
    assert not leaked, f"test fixtures leaked into prod customer_ask_candidates: {leaked}"
