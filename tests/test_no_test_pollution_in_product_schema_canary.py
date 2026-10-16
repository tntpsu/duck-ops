"""Post-suite audit: the live production canary state must not contain test
fixtures.

Layer 3 of the three-layer convention. The conftest redirect (layer 1) and the
frozen-path refusal (layer 2) both prevent the write; this test proves no write
got through anyway, which is the only layer that catches a path a future module
reaches by a route nobody anticipated (re-import, subprocess, a script run by
hand).

The fingerprint of pollution here is a `base_url` that is not the real
storefront, or page handles from the test fixtures.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import product_schema_canary as psc  # noqa: E402

# Handles that exist only in tests/test_product_schema_canary.py fixtures.
FIXTURE_MARKERS = {"plain-duck", "boom-duck", "a-duck", "b-duck",
                   "r00", "r01", "u00", "u01"}


def _production_payload() -> dict | None:
    path = psc._FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def test_production_canary_state_has_no_fixture_handles():
    payload = _production_payload()
    if payload is None:
        return  # never produced yet — nothing to pollute
    handles = {str(page.get("handle") or "") for page in payload.get("pages") or []}
    leaked = handles & FIXTURE_MARKERS
    assert not leaked, (
        f"test fixture handles reached production "
        f"{psc._FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH}: {sorted(leaked)}. "
        "A test wrote production state; check the conftest redirect."
    )


def test_production_canary_state_points_at_the_real_storefront():
    payload = _production_payload()
    if payload is None:
        return
    base = str(payload.get("base_url") or "")
    assert base == psc.STOREFRONT_BASE_URL, (
        f"production canary state records base_url={base!r}, expected "
        f"{psc.STOREFRONT_BASE_URL!r} — this payload was produced by a test "
        "or against a non-production host."
    )


def test_the_frozen_path_constant_still_points_at_production():
    # If a refactor renames the state file, the two tests above would silently
    # audit a path nothing writes and pass forever.
    assert psc._FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH.name == "product_schema_canary.json"
    assert psc._FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH.parent.name == "state"
