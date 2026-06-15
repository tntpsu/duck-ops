"""Test isolation for the duck-ops LLM lanes.

2026-05-31: discovered that every test exercising the LLM-backed
producers (rewriter, scorer, jeepfact, weekly_sale, catalog_dedup)
was writing to the PRODUCTION llm_call_log.jsonl via the
log_llm_call helper. At audit time, 713 of 744 log entries (96%)
were test pollution, which had been silently dragging the OS
review_reply_rewriter card red all week. Real failure rates were
near zero; the card was grading on fake fixtures.

The autouse fixture below redirects llm_call_helpers.LLM_CALL_LOG_PATH
to a per-test tmp file for every test under duck-ops/tests/. That
makes the existing tests safe without touching their bodies AND
ensures future tests can't reintroduce the pollution.

Autouse + unittest.TestCase: pytest delivers autouse fixtures to
TestCase methods via the same mechanism it uses for normal
functions, so no per-class wiring is needed.

If you ever need to assert against the on-disk log file inside a
test, accept the redirected path as a fixture parameter — DO NOT
re-patch back to the production path. Production paths in tests
are always wrong here.
"""
from __future__ import annotations

import sys
from pathlib import Path

import os

import pytest


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))


# 2026-06-09: source-level guard. workflow_control.record_workflow_transition
# raises TestModeRefusalError if DUCK_TEST_MODE=1. Belt-and-suspenders
# with the workflow_control state_dir patches below — even if a test
# bypasses the path patches via re-import, subprocess, or direct file
# write, the function itself refuses.
os.environ.setdefault("DUCK_TEST_MODE", "1")


@pytest.fixture(autouse=True)
def _redirect_llm_call_log(tmp_path, monkeypatch):
    """Redirect llm_call_helpers.LLM_CALL_LOG_PATH to a tmp file so
    no test write reaches the production log. Autouse: applies to
    every test in this directory automatically.

    The fixture also patches the constant on caller modules that
    imported it by name (LLM_CALL_LOG_PATH is re-exported via
    `from llm_call_helpers import LLM_CALL_LOG_PATH` patterns).
    Patch them all to the same tmp path."""
    try:
        import llm_call_helpers
    except ImportError:
        # Some tests don't need duck-ops runtime on path — let
        # them run with no patching (they wouldn't pollute anyway).
        yield
        return

    tmp_log = tmp_path / "llm_call_log.jsonl"
    monkeypatch.setattr(llm_call_helpers, "LLM_CALL_LOG_PATH", tmp_log)

    # The actual log_llm_call function reads LLM_CALL_LOG_PATH at
    # call time, so patching on the helpers module is sufficient
    # for the canonical path. Caller modules that did
    # `from llm_call_helpers import log_llm_call` get the same
    # function object — they read the patched constant. Test files
    # that did `import llm_call_helpers` similarly see the patched
    # value. No per-caller patching needed.

    # Workflow-control receipt isolation. Discovered 2026-05-31
    # alongside the LLM log pollution: one stranded test fixture
    # (gtdf-winner-test-gtdf-winner-blocked.json) had been sitting
    # in production workflow_control since 2026-05-26. The writers
    # in workflow_control.py accept a state_dir kwarg defaulting
    # to the production path; not every test was passing it. Patch
    # the module-level constant so even tests that omit the kwarg
    # land in a tmp dir.
    try:
        import workflow_control as _wc
    except ImportError:
        yield
        return
    tmp_wc = tmp_path / "workflow_control"
    tmp_wc.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(_wc, "WORKFLOW_CONTROL_STATE_DIR", tmp_wc)
    if hasattr(_wc, "WORKFLOW_RECEIPT_STATE_DIR"):
        tmp_receipts = tmp_path / "workflow_receipts"
        tmp_receipts.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(_wc, "WORKFLOW_RECEIPT_STATE_DIR", tmp_receipts)

    # 2026-06-08: review_reply_executor.EXECUTION_QUEUE_STATE_PATH
    # was NOT isolated, so any test calling queue_review_reply or
    # save_queue_state wrote to the production execution queue.
    # Production-state pollution shape: 34 review-reply receipts
    # had to be backfilled twice within 2 days because their queue
    # items disappeared between drains. We don't have definitive
    # proof tests were the cause (no code path deletes items, no
    # current test omits patching) but the SAME failure mode as
    # the LLM log pollution justifies the same fix.
    try:
        import review_reply_executor as _rre
    except ImportError:
        yield
        return
    tmp_queue = tmp_path / "review_reply_execution_queue.json"
    monkeypatch.setattr(_rre, "EXECUTION_QUEUE_STATE_PATH", tmp_queue)
    if hasattr(_rre, "WORKFLOW_CONTROL_DIR"):
        # The self-heal path reads from this dir; redirect alongside.
        monkeypatch.setattr(_rre, "WORKFLOW_CONTROL_DIR", tmp_wc)

    # 2026-06-11: occasion_engine.OCCASION_INTEL_PATH (Surface 13) —
    # new module-level prod-path constant, isolated on arrival per the
    # three-layer policy (this fixture + the source-level DUCK_TEST_MODE
    # guard in write_occasion_intel + tests/test_no_test_pollution_in_occasion_intel.py).
    try:
        import occasion_engine as _oe
    except ImportError:
        yield
        return
    monkeypatch.setattr(_oe, "OCCASION_INTEL_PATH", tmp_path / "occasion_intel.json")

    # 2026-06-12: build_next_engine.BUILD_NEXT_QUEUE_PATH (Surface 16) —
    # new module-level prod-path constant, isolated on arrival per the
    # three-layer policy (this fixture + the source-level DUCK_TEST_MODE
    # guard in write_build_next_queue + tests/test_no_test_pollution_in_build_next.py).
    try:
        import build_next_engine as _bne
    except ImportError:
        yield
        return
    monkeypatch.setattr(_bne, "BUILD_NEXT_QUEUE_PATH", tmp_path / "build_next_queue.json")

    # 2026-06-12: product_concept_queue.BUILD_NEXT_PROMOTIONS_PATH (Surface 16
    # ingestion). build_product_concept_queue() reads this file by default;
    # without isolation a real operator promotion would leak into every
    # concept-queue test. Redirect to an absent tmp path (load_json returns
    # the empty default), so promotion ingestion is a no-op unless a test
    # writes the file itself.
    try:
        import product_concept_queue as _pcq
    except ImportError:
        yield
        return
    monkeypatch.setattr(_pcq, "BUILD_NEXT_PROMOTIONS_PATH", tmp_path / "build_next_promotions.json")

    # 2026-06-15: theme_review_decisions.THEME_REVIEW_DECISIONS_PATH (Surface 20) —
    # new module-level prod-path constant, isolated on arrival per the
    # three-layer policy (this fixture + the source-level DUCK_TEST_MODE guard
    # in record_theme_review_decision + tests/test_no_test_pollution_in_theme_review_decisions.py).
    try:
        import theme_review_decisions as _trd
    except ImportError:
        yield
        return
    monkeypatch.setattr(_trd, "THEME_REVIEW_DECISIONS_PATH", tmp_path / "theme_review_decisions.json")
    yield
