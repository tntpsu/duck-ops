"""Layer 3 of the 3-layer isolation for the provider-health ledger (Surface 99).

Layers 1 and 2 are the autouse conftest redirects (both repos) and the
``TestModeRefusalError`` write guards in ``helpers/provider_health.py`` and
``runtime/provider_credit_health.py``. This audits the real files afterwards.

Pollution here is worse than a dirty file: the ledger drives an OS card AND a
daily operator email, so a stray test record would mail the operator that the
shop is out of credits when it is not — and the next real alert would be read as
more of the same ([[feedback_red_ci_is_no_ci]]).

Honest about its reach: test fixtures use realistic provider names, so this
cannot fingerprint every possible stray record. It catches the shapes a test
actually produces — the truncation filler, synthetic surfaces — and proves the
production files still parse.
"""
from __future__ import annotations

import json
from pathlib import Path

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
LEDGER = DUCK_OPS_ROOT / "state" / "provider_health_log.jsonl"
STATE = DUCK_OPS_ROOT / "state" / "provider_credit_health.json"

_TEST_SURFACE_MARKERS = ("test_", "tmp_path", "pytest", "TEST-RUN")


def test_production_ledger_has_no_test_records():
    if not LEDGER.exists():
        return
    offenders = []
    for number, line in enumerate(LEDGER.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except Exception:
            offenders.append(f"line {number}: not JSON")
            continue
        surface = str(entry.get("surface") or "")
        if any(marker in surface for marker in _TEST_SURFACE_MARKERS):
            offenders.append(f"line {number}: test surface {surface!r}")
        # the truncation test writes 500 x's
        if "xxxxxxxxxx" in str(entry.get("detail") or ""):
            offenders.append(f"line {number}: truncation-test filler in detail")
    assert not offenders, "test pollution in the production provider ledger:\n" + "\n".join(offenders)


def test_production_state_file_is_well_formed():
    if not STATE.exists():
        return
    payload = json.loads(STATE.read_text(encoding="utf-8"))
    assert payload.get("schema_version") == "duck_ops.provider_credit_health.v1"
    assert payload.get("status") in {"green", "yellow", "red"}
