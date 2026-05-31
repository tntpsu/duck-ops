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

import pytest


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))


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
    yield
