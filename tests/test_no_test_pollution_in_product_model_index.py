"""Layer 3 of the product_model_index isolation policy (Surface 64): scan the
LIVE production index for fixture markers from test_product_model_index.py.
A hit means a test escaped the conftest redirect + DUCK_TEST_MODE guard.
"""
from __future__ import annotations

import json
from pathlib import Path

_LIVE_INDEX = Path(__file__).resolve().parents[1] / "state" / "product_model_index.json"
_FIXTURE_MARKERS = ("unit-test-fixture-duck", "999900000")


def test_live_product_model_index_has_no_fixture_entries():
    if not _LIVE_INDEX.exists():
        return
    try:
        blob = json.dumps(json.loads(_LIVE_INDEX.read_text()))
    except (OSError, ValueError):
        return
    offenders = [m for m in _FIXTURE_MARKERS if m in blob]
    assert not offenders, (
        f"LIVE product_model_index.json contains fixture markers {offenders} — "
        "a test escaped the conftest redirect + DUCK_TEST_MODE guard. Find it, "
        "then re-run the producer to rebuild the index from real sources."
    )
