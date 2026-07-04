"""Layer-3 audit: no test data in the production demand_intel.json. Mirrors
test_no_test_pollution_in_build_next.py. If a test forgot to patch
demand_intel.DEMAND_INTEL_PATH, the FROZEN write guard raises, but this catches
any pollution that slipped in earlier."""
from __future__ import annotations
import json
from pathlib import Path

PROD = Path(__file__).resolve().parents[1] / "state" / "demand_intel.json"
_SENTINELS = ("Astronaut Duck", "TEST-RUN", "sentinel")


def test_no_test_pollution_in_demand_intel():
    if not PROD.exists():
        return
    try:
        payload = json.loads(PROD.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    titles = " ".join(str((d or {}).get("title") or "") for d in (payload.get("ducks") or []))
    for s in _SENTINELS:
        assert s not in titles, f"test pollution marker {s!r} in production demand_intel.json"
