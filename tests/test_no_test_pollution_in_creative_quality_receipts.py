"""Layer 3 of the three-layer isolation for the cross-repo creative_quality_
receipts WRITE (Surface 9): the duck-ops social_performance_collector writes
outcomes into duckAgent's receipts dir. This post-suite audit pins that no
duck-ops test ever pollutes those prod receipts (a placeholder post_id or a
test-slugged filename). Layer 1 = the conftest redirect of
CREATIVE_QUALITY_RECEIPTS_DIR; Layer 2 = the source guard in duckAgent's
creative_quality_loop._guard_receipt_write.

Canonical sibling: test_no_test_pollution_in_build_next.py."""
from __future__ import annotations

import json
from pathlib import Path

PROD_RECEIPTS = Path("/Users/philtullai/ai-agents/duckAgent/data/creative_quality_receipts")
_PLACEHOLDER_POST_IDS = {"1234567890", "9876543210", "17912345"}


def test_prod_creative_receipts_have_no_test_pollution():
    if not PROD_RECEIPTS.exists():
        return  # nothing produced yet
    bad: list[str] = []
    for f in PROD_RECEIPTS.glob("*.json"):
        if f.name.lower().startswith(("test_", "test-")):
            bad.append(f.name)
            continue
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        pid = str(((d.get("publish") or {}).get("post_id")) or "")
        if pid in _PLACEHOLDER_POST_IDS:
            bad.append(f"{f.name}: placeholder post_id {pid}")
        for o in d.get("outcomes") or []:
            if str((o or {}).get("source") or "").startswith("test"):
                bad.append(f"{f.name}: test-sourced outcome")
    assert not bad, (
        f"Production creative_quality_receipts polluted by duck-ops tests: {bad} — "
        "a collector writeback test bypassed the conftest redirect AND the "
        "duckAgent source guard. Find it, fix it, and clean the receipt."
    )
