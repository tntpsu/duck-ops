"""Meta-test: confirm the autouse fixture in conftest.py redirects
LLM_CALL_LOG_PATH away from the production path during tests.

Discovered 2026-05-31 after a week of "high failure rate" reds on
the OS review_reply_rewriter card turned out to be 96% test
pollution — every test exercising the LLM-backed producers
(rewriter, scorer, jeepfact, weekly_sale, catalog_dedup) was
writing to the production llm_call_log.jsonl via the log_llm_call
helper. The OS card graded on the fake fixtures and showed 17%
sanity + 17% api failure rates that didn't exist in real traffic.

Fix was conftest.py's `_redirect_llm_call_log` autouse fixture
that patches the constant to a per-test tmp file. This meta-test
pins that the fixture stays active. If a future refactor moves
the constant, deletes the conftest, or accidentally narrows the
fixture scope, this test fails loudly BEFORE pollution can
accumulate again."""
from __future__ import annotations

import sys
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))


def test_llm_call_log_path_is_redirected_during_tests():
    """The autouse fixture must redirect LLM_CALL_LOG_PATH off the
    production path. Without this redirect, every test that calls
    generate_rewrite_via_llm / evaluate_gray_zone /
    generate_jeepfact_config_via_llm / etc. silently appends fake
    entries that the OS card grades on for the next 7 days."""
    import llm_call_helpers
    prod_path = "/Users/philtullai/ai-agents/duck-ops/state/llm_call_log.jsonl"
    actual = str(llm_call_helpers.LLM_CALL_LOG_PATH)
    assert actual != prod_path, (
        f"LLM_CALL_LOG_PATH must be redirected during tests. Got "
        f"the production path: {actual!r}. The autouse fixture in "
        f"conftest.py is the single isolation point — if it broke, "
        f"the OS review_reply_rewriter card will silently grade on "
        f"test fixtures again (the 2026-05-31 root cause)."
    )
    # Should point inside a pytest-managed tmp dir.
    assert "pytest" in actual.lower() or "tmp" in actual.lower(), (
        f"LLM_CALL_LOG_PATH should land in a pytest tmp dir; "
        f"got {actual!r}"
    )


def test_workflow_control_state_dir_is_redirected_during_tests():
    """Sister pin to the LLM call log: workflow_control writers
    accept a state_dir override but tests don't always pass it.
    The autouse fixture also patches WORKFLOW_CONTROL_STATE_DIR so
    tests that omit the kwarg land in a tmp dir instead of
    polluting production receipts.

    Discovered 2026-05-31: one stranded fixture
    (gtdf-winner-test-gtdf-winner-blocked.json) had been sitting
    in production workflow_control since 2026-05-26."""
    import workflow_control as _wc
    prod_path = "/Users/philtullai/ai-agents/duck-ops/state/workflow_control"
    actual = str(_wc.WORKFLOW_CONTROL_STATE_DIR)
    assert actual != prod_path, (
        f"WORKFLOW_CONTROL_STATE_DIR must be redirected during "
        f"tests. Got the production path: {actual!r}. The autouse "
        f"fixture in conftest.py was the fix for the 2026-05-31 "
        f"gtdf-winner-test stranded receipt."
    )
    assert "pytest" in actual.lower() or "tmp" in actual.lower(), (
        f"WORKFLOW_CONTROL_STATE_DIR should land in a pytest tmp "
        f"dir; got {actual!r}"
    )


def test_log_llm_call_writes_only_to_redirected_path():
    """End-to-end pin: calling log_llm_call inside a test must not
    grow the production file. Uses Path.stat for the byte size
    delta as the empirical signal."""
    import llm_call_helpers
    prod_path = Path("/Users/philtullai/ai-agents/duck-ops/state/llm_call_log.jsonl")
    size_before = prod_path.stat().st_size if prod_path.exists() else 0
    llm_call_helpers.log_llm_call({"at": "test-timestamp", "kind": "isolation_meta_test", "outcome": "test"})
    size_after = prod_path.stat().st_size if prod_path.exists() else 0
    assert size_after == size_before, (
        f"Production llm_call_log.jsonl grew by {size_after - size_before} "
        f"bytes during a test call to log_llm_call. The autouse "
        f"fixture isn't intercepting — pollution is happening."
    )
