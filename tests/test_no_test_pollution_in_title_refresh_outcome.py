"""Layer 3 of the prod-write isolation for Surface 86.

The conftest redirect and the DUCK_TEST_MODE guard both stop a test writing the
real file. This checks the result after the fact, because a redirect that
silently stops applying is exactly the failure the other two layers cannot see
(it bit llm_call_log, execution_queue and workflow_control in a 10-day run).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import etsy_title_refresh_outcome as etro
import workflow_control

PROD = Path("/Users/philtullai/ai-agents/duck-ops/state/etsy_title_refresh_outcome.json")


def test_the_production_outcome_file_holds_no_fixture_listings():
    if not PROD.is_file():
        pytest.skip("producer has not run yet")
    payload = json.loads(PROD.read_text())
    ids = {row.get("listing_id") for row in payload.get("treated_listings") or []}
    # The test fixtures use 1000/2000/3000-block ids and listing 9999.
    assert not (ids & {1000, 1001, 2000, 2001, 3000, 9999}), \
        f"fixture listing ids reached production: {sorted(ids & {1000, 2000, 3000, 9999})}"
    assert payload.get("treated_count", 0) != 12, \
        "treated_count of 12 is the test fixture's arm size, not the real 60"


def test_the_write_guard_refuses_the_production_path_under_test_mode(monkeypatch):
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    with pytest.raises(workflow_control.TestModeRefusalError):
        etro.write_outcome({"available": False}, etro._FROZEN_PRODUCTION_TITLE_REFRESH_OUTCOME_PATH)


def test_the_conftest_redirect_is_actually_applied():
    """If this fails, the autouse fixture stopped matching the module."""
    assert etro.TITLE_REFRESH_OUTCOME_PATH != \
        etro._FROZEN_PRODUCTION_TITLE_REFRESH_OUTCOME_PATH
