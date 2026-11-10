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


# Derived, not hard-coded. Until 2026-11-09 this was an absolute path into one
# particular checkout, so `pytest` in a fresh clone imported the ORIGINAL
# working tree's runtime/ and graded code the checkout did not contain: a probe
# run that day reported 1486 passing while testing a different copy of the repo
# ([[feedback_a_failed_probe_is_a_fact_about_the_probe]]). It is also why this
# suite had never run in CI.
REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime"

# The sibling repo, for the handful of tests that assert duck-ops and duckAgent
# agree about something. Overridable, and ABSENT is a legitimate state -- a CI
# runner checks out one repo. Those tests skip by name rather than failing, so
# "the cross-repo check did not run here" is visible instead of implied.
DUCKAGENT_ROOT = Path(
    os.environ.get("DUCKAGENT_ROOT") or (REPO_ROOT.parent / "duckAgent"))

#: Decorate any test that needs duckAgent's code to be importable. It skips
#: with a reason naming the missing repo rather than failing, so a one-repo
#: runner reports "did not check this" instead of "this is broken" -- and
#: rather than passing quietly, which is how a cross-repo drift check stops
#: existing without anyone noticing.
requires_duckagent = pytest.mark.skipif(
    not (DUCKAGENT_ROOT / "helpers").is_dir(),
    reason=f"needs duckAgent checked out at {DUCKAGENT_ROOT}")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))


# 2026-06-09: source-level guard. workflow_control.record_workflow_transition
# raises TestModeRefusalError if DUCK_TEST_MODE=1. Belt-and-suspenders
# with the workflow_control state_dir patches below — even if a test
# bypasses the path patches via re-import, subprocess, or direct file
# write, the function itself refuses.
os.environ.setdefault("DUCK_TEST_MODE", "1")


@pytest.fixture(autouse=True)
def _redirect_provider_health_state(tmp_path, monkeypatch):
    """Layer 1 of the 3-layer isolation for the provider-credit producer.

    Without this, a test that grades a fixture ledger would overwrite the live
    state file, turn the OS card whatever the fixture said, and -- because the
    producer emails on red -- send the operator a real "you are out of credits"
    email from pytest.
    """
    try:
        import provider_credit_health
    except ImportError:
        yield
        return
    monkeypatch.setattr(
        provider_credit_health, "STATE_PATH", tmp_path / "provider_credit_health.json"
    )
    monkeypatch.setattr(
        provider_credit_health,
        "PROVIDER_HEALTH_LOG_PATH",
        tmp_path / "provider_health_log.jsonl",
    )
    yield


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
    # 2026-06-15: requeue_recoverable_failed (failed-recovery sweep) writes
    # QUALITY_GATE_STATE_PATH via save_quality_gate_state. Isolate it too so
    # the sweep's tests (or any drain test) never flip prod artifacts'
    # execution_state. Same pollution shape as the queue-path fix above.
    if hasattr(_rre, "QUALITY_GATE_STATE_PATH"):
        monkeypatch.setattr(_rre, "QUALITY_GATE_STATE_PATH", tmp_path / "quality_gate_state.json")

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
    # 2026-06-21: build_next_engine.BUILD_NEXT_DUPE_DECISIONS_PATH (Surface 28) —
    # operator dupe rulings; same three-layer isolation (this redirect + the
    # DUCK_TEST_MODE guard in record_dupe_decision + the no-pollution audit test).
    monkeypatch.setattr(_bne, "BUILD_NEXT_DUPE_DECISIONS_PATH", tmp_path / "build_next_dupe_decisions.json")
    # 2026-07-11: customer_ask_scout output paths (Surface 58) — same three-layer
    # isolation (this redirect + the DUCK_TEST_MODE write guard in
    # customer_ask_scout._write_json + tests/test_no_test_pollution_in_customer_ask.py).
    _ask_candidates = tmp_path / "customer_ask_candidates.json"
    try:
        import customer_ask_scout as _cas
        monkeypatch.setattr(_cas, "CUSTOMER_ASK_CANDIDATES_PATH", _ask_candidates)
        monkeypatch.setattr(_cas, "CUSTOMER_ASK_NEEDS_REVIEW_PATH", tmp_path / "customer_ask_needs_review.json")
    except ImportError:
        pass
    try:
        import product_concept_queue as _pcq
        monkeypatch.setattr(_pcq, "CUSTOMER_ASK_CANDIDATES_PATH", _ask_candidates)
    except ImportError:
        pass
    # 2026-06-26: gsc_search_demand state (Surface 38). build_next_engine READS
    # it (deterministic tmp so the read doesn't pick up prod), and the producer
    # gsc_search_demand.py WRITES it (same three-layer isolation: this redirect
    # + DUCK_TEST_MODE guard in write_search_demand + no-pollution audit test).
    monkeypatch.setattr(_bne, "GSC_SEARCH_DEMAND_PATH", tmp_path / "gsc_search_demand.json")
    try:
        import gsc_search_demand as _gsc
        monkeypatch.setattr(_gsc, "GSC_SEARCH_DEMAND_PATH", tmp_path / "gsc_search_demand.json")
    except ImportError:
        pass
    # 2026-07-26: seo_outcome_intel (Surface 63) — redirect BOTH the write path
    # (3-layer isolation: this + DUCK_TEST_MODE guard in write_seo_outcome_intel
    # + no-pollution audit test) AND the receipts read dir (hygiene: tests never
    # see prod writeback receipts).
    try:
        import seo_outcome_intel as _soi
        monkeypatch.setattr(_soi, "SEO_OUTCOME_INTEL_PATH", tmp_path / "seo_outcome_intel.json")
        monkeypatch.setattr(_soi, "SEO_WRITEBACK_RECEIPT_DIR", tmp_path / "seo_receipts")
    except ImportError:
        pass
    # 2026-06-28: competitor reports dir (Surface 47) — build_next_engine READS
    # it for the demand signal + staleness check. Read-only (duckAgent writes it),
    # so this is test-hygiene (hermetic staleness path), NOT the 3-layer write
    # isolation — no FROZEN guard / pollution-audit test is needed for a read.
    monkeypatch.setattr(_bne, "COMPETITOR_REPORTS_DIR", tmp_path / "competitor_reports")
    # 2026-06-28: creative_quality_receipts dir (Surface 9 isolation gap). The
    # social_performance_collector WRITES outcomes into duckAgent's receipts dir
    # cross-repo (and current_learnings READS it). Redirect both module constants
    # to tmp so the full-run collector path can't pollute prod receipts. The
    # WRITE itself is also guarded source-side by creative_quality_loop's
    # DUCK_TEST_MODE frozen-dir check + a pollution-audit test.
    for _modname, _const in (("social_performance_collector", "CREATIVE_QUALITY_RECEIPTS_DIR"),
                             ("current_learnings", "CREATIVE_QUALITY_RECEIPTS_DIR")):
        try:
            _m = __import__(_modname)
            monkeypatch.setattr(_m, _const, tmp_path / "creative_quality_receipts")
        except (ImportError, AttributeError):
            pass
    # 2026-06-26: ga4_listing_performance state (Surface 39) — same three-layer
    # isolation (this redirect + DUCK_TEST_MODE guard in write_listing_performance
    # + no-pollution audit test).
    try:
        import ga4_listing_performance as _ga4
        monkeypatch.setattr(_ga4, "LISTING_PERFORMANCE_PATH", tmp_path / "listing_performance.json")
    except ImportError:
        pass
    # 2026-06-27: seo_demand_context (Surface 40) READS the two state files; point
    # them at tmp (absent) so SEO tests see an empty context = pre-Surface-40
    # behavior unless a test writes fixtures there.
    try:
        import seo_demand_context as _sdc
        monkeypatch.setattr(_sdc, "GSC_SEARCH_DEMAND_PATH", tmp_path / "gsc_search_demand.json")
        monkeypatch.setattr(_sdc, "LISTING_PERFORMANCE_PATH", tmp_path / "listing_performance.json")
    except ImportError:
        pass
    # 2026-06-27: shopify_collection_planner (Surface 41) — new prod-WRITE path;
    # redirect so the planner's write tests can't pollute production state
    # (paired with the DUCK_TEST_MODE FROZEN guard + pollution-audit test).
    try:
        import shopify_collection_planner as _scp
        monkeypatch.setattr(_scp, "COLLECTION_PLAN_PATH", tmp_path / "shopify_collection_review" / "latest.json")
    except ImportError:
        pass
    # 2026-06-27: sale_steering directives (Surface 42) — new prod-WRITE path
    # read by the duckAgent weekly-sale flow.
    try:
        import sale_steering as _sst
        monkeypatch.setattr(_sst, "SALE_STEERING_PATH", tmp_path / "sale_steering.json")
    except ImportError:
        pass
    # 2026-10-12: etsy_title_refresh_outcome (Surface 86) — new prod-WRITE path.
    # Three-layer isolation: this redirect + the DUCK_TEST_MODE guard in
    # write_outcome + tests/test_no_test_pollution_in_title_refresh_outcome.py.
    # duckAgent-side fixture is n/a: nothing in duckAgent imports this module.
    try:
        import etsy_title_refresh_outcome as _etro
        monkeypatch.setattr(_etro, "TITLE_REFRESH_OUTCOME_PATH",
                            tmp_path / "etsy_title_refresh_outcome.json")
    except ImportError:
        pass
    # 2026-10-15: product_schema_canary (Surface 95) — new prod-WRITE path.
    # Three-layer isolation: this redirect + the DUCK_TEST_MODE frozen-path
    # guard in write_product_schema_canary + tests/
    # test_no_test_pollution_in_product_schema_canary.py. duckAgent reads the
    # STATE FILE for its two OS cards and never imports this module, so there
    # is no duckAgent-side constant to patch.
    try:
        import product_schema_canary as _psc
        monkeypatch.setattr(_psc, "PRODUCT_SCHEMA_CANARY_PATH",
                            tmp_path / "product_schema_canary.json")
    except ImportError:
        pass
    # 2026-07-03: demand_intel (Demand page) — new prod-WRITE path (funnel action
    # buckets); three-layer isolation (this redirect + DUCK_TEST_MODE guard in
    # write_demand_intel + tests/test_no_test_pollution_in_demand_intel.py). Also
    # isolates the _carry_bucket_since prod read.
    try:
        import demand_intel as _dmi
        monkeypatch.setattr(_dmi, "DEMAND_INTEL_PATH", tmp_path / "demand_intel.json")
    except ImportError:
        pass

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
    monkeypatch.setattr(_trd, "THEME_KEYWORD_FALSE_FLAGS_PATH", tmp_path / "theme_keyword_false_flags.json")

    # 2026-06-17: email_cadence_gate.EMAIL_CADENCE_OVERRIDES_PATH (Surface 23) —
    # new module-level prod-path constant, isolated per the three-layer policy
    # (this fixture + the source-level DUCK_TEST_MODE guard in set_override +
    # tests/test_no_test_pollution_in_email_cadence_overrides.py). duckAgent's
    # conftest patches it too, since duckAgent tests import this gate via the loader.
    try:
        import email_cadence_gate as _ecg
        monkeypatch.setattr(_ecg, "EMAIL_CADENCE_OVERRIDES_PATH", tmp_path / "email_cadence_overrides.json")
    except ImportError:
        pass

    # 2026-08-07: product_model_index.PRODUCT_MODEL_INDEX_PATH (Surface 64) —
    # new module-level prod-path constant, isolated on arrival per the
    # three-layer policy (this fixture + the source-level DUCK_TEST_MODE guard
    # in write_product_model_index + tests/test_no_test_pollution_in_product_model_index.py).
    # duckAgent's conftest patches it too — flows/duckvideo reads it cross-repo.
    try:
        import product_model_index as _pmi
        monkeypatch.setattr(_pmi, "PRODUCT_MODEL_INDEX_PATH", tmp_path / "product_model_index.json")
        # Overrides config is a read, not a write, but the production file
        # carries real operator decisions — tests must not see them.
        monkeypatch.setattr(_pmi, "PRODUCT_MODEL_OVERRIDES_PATH", tmp_path / "product_model_overrides.json")
    except ImportError:
        pass

    # 2026-11-04: google_model_availability.STATE_PATH (Surface 64f) — new
    # module-level prod-path constant, isolated on arrival per the three-layer
    # policy (this fixture + the source-level DUCK_TEST_MODE guard in
    # write_state + the no-pollution audit in
    # tests/test_google_model_availability.py). A fixture verdict reaching the
    # live file would paint the OS card with invented model ids.
    try:
        from runtime import google_model_availability as _gma
        monkeypatch.setattr(_gma, "STATE_PATH", tmp_path / "google_model_availability.json")
    except ImportError:
        pass
    yield


@pytest.fixture(autouse=True)
def _no_outbound_mail_or_whatsapp_from_tests(monkeypatch):
    """Second layer under the per-sender DUCK_TEST_MODE guards: the suite
    physically cannot reach the operator.

    Surface 101 (2026-11-08) guarded notifier.send_message,
    notifier.send_whatsapp_message and both widget_api senders by construction,
    and added an AST enumeration so a sixth unguarded sender is *visible*. This
    fixture makes one *harmless*. Outbound side effects had only ever had the
    source layer, which is how a review email reached the operator during a
    verification run while the suite was green.

    Both interceptors RAISE rather than quietly absorbing the call. A mock that
    swallowed sends would make the Surface 101 sharpness checks pass with the
    guards deleted -- every "it refuses under test mode" test would go green
    because nothing reached the wire, and a real guard would have been traded
    for the appearance of one.

    The WhatsApp interceptor FILTERS rather than replacing subprocess.run
    wholesale: nine test files use subprocess legitimately, and a blanket
    replacement would break them while teaching nobody anything. It fires only
    on the gateway's own command shape, so an unrelated subprocess call is
    untouched and a real gateway send cannot slip by.
    """
    import smtplib
    import subprocess

    def _refuse_smtp(*args, **kwargs):
        raise AssertionError(
            "A test opened an SMTP connection. Either a sender is missing its "
            "DUCK_TEST_MODE guard (see notifier._refuse_outbound_under_test_mode "
            "and widget_api._blocked_by_test_mode), or this test means to "
            "exercise the real send path and should monkeypatch smtplib itself. "
            f"Args: {args!r}"
        )

    real_run = subprocess.run

    def _filtered_run(cmd, *args, **kwargs):
        argv = [str(part) for part in cmd] if isinstance(cmd, (list, tuple)) else [str(cmd)]
        joined = " ".join(argv)
        looks_like_gateway = "--channel" in argv and "whatsapp" in joined
        if looks_like_gateway:
            raise AssertionError(
                "A test invoked the WhatsApp gateway. notifier."
                "send_whatsapp_message should have refused under "
                f"DUCK_TEST_MODE. Command: {joined[:200]!r}"
            )
        return real_run(cmd, *args, **kwargs)

    monkeypatch.setattr(smtplib, "SMTP", _refuse_smtp)
    monkeypatch.setattr(smtplib, "SMTP_SSL", _refuse_smtp)
    monkeypatch.setattr(subprocess, "run", _filtered_run)
