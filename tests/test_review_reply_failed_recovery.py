"""Phase 3: requeue_recoverable_failed recovers FRESH failed review replies
whose cause is transient/now-fixed (cooldown/pacing, auth-era, row-not-found),
and leaves stale / drift / capped / unparseable ones alone. Plus the conservative
failure_is_retryable allowlist. State load/save is stubbed so nothing touches
production quality-gate state."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_reply_executor as rre  # noqa: E402

NOW = datetime(2026, 6, 15)
FRESH = "2026-06-15"   # 0 days old
STALE = "2026-05-01"   # 45 days old > 14d window


def _aid(date: str, n: int = 1) -> str:
    return f"publish::reviews_reply_positive::{date}::review-{n}"


def _failed_artifact(error: str | None = None, failure_class: str | None = None) -> dict:
    failure = {"failure_class": failure_class} if failure_class else {}
    return {
        "decision": {
            "flow": "reviews_reply_positive",
            "decision": "publish_ready",
            "review_status": "approved",
            "execution_state": "failed",
            "review_target": {"transaction_id": "t1", "listing_id": "l1"},
            "approved_reply_text": "Thank you!",
            "execution_attempts": [{"error": error, "failure": failure}],
        }
    }


@pytest.fixture
def patched(monkeypatch):
    """Capture quality-gate writes + stub the workflow_control transition."""
    holder: dict = {"quality": None, "saved_quality": False, "saved_queue": False}

    def _load_quality():
        return holder["quality"]

    def _save_quality(payload):
        holder["saved_quality"] = True

    monkeypatch.setattr(rre, "load_quality_gate_state", _load_quality)
    monkeypatch.setattr(rre, "save_quality_gate_state", _save_quality)
    monkeypatch.setattr(rre, "save_queue_state", lambda payload: holder.__setitem__("saved_queue", True))
    monkeypatch.setattr(rre, "_record_review_execution_transition", lambda *a, **k: None)
    return holder


# ---- failure_is_retryable (conservative allowlist) -------------------------

def test_retryable_cooldown():
    assert rre.failure_is_retryable({"error": "Etsy automation hit the shared pacing budget and is cooling down until X"}) is True


def test_retryable_auth_and_row_not_found():
    assert rre.failure_is_retryable({"failure": {"failure_class": "auth_required"}}) is True
    assert rre.failure_is_retryable({"failure": {"failure_class": "review_row_not_found"}}) is True


def test_not_retryable_drift_or_generic():
    assert rre.failure_is_retryable({"failure": {"failure_class": "review_row_transaction_mismatch"}}) is False
    assert rre.failure_is_retryable({"error": "weird", "failure": {"failure_class": "unexpected_executor_failure"}}) is False
    assert rre.failure_is_retryable(None) is False


# ---- requeue_recoverable_failed -------------------------------------------

def test_recovers_fresh_cooldown_failed(patched):
    aid = _aid(FRESH)
    patched["quality"] = {"artifacts": {aid: _failed_artifact(
        error="Etsy automation hit the shared pacing budget and is cooling down until X")}}
    queue_state: dict = {"items": {}}
    recovered = rre.requeue_recoverable_failed(queue_state, policy=dict(rre.DEFAULT_EXECUTION_POLICY), now=NOW)
    assert recovered == [aid]
    item = queue_state["items"][aid]
    assert item["status"] == "queued"
    assert item["retry_reason"] == "recovered_failed"
    assert item["recovery_requeue_count"] == 1
    # decision execution_state flipped + persisted
    assert patched["quality"]["artifacts"][aid]["decision"]["execution_state"] == "queued"
    assert patched["saved_quality"] is True


def test_recovers_fresh_auth_failed(patched):
    aid = _aid(FRESH, 2)
    patched["quality"] = {"artifacts": {aid: _failed_artifact(failure_class="auth_required")}}
    queue_state: dict = {"items": {}}
    assert rre.requeue_recoverable_failed(queue_state, policy=dict(rre.DEFAULT_EXECUTION_POLICY), now=NOW) == [aid]


def test_skips_stale_failed(patched):
    aid = _aid(STALE)
    patched["quality"] = {"artifacts": {aid: _failed_artifact(
        error="Etsy automation hit the shared pacing budget and is cooling down")}}
    queue_state: dict = {"items": {}}
    assert rre.requeue_recoverable_failed(queue_state, policy=dict(rre.DEFAULT_EXECUTION_POLICY), now=NOW) == []
    assert queue_state["items"] == {}


def test_skips_transaction_mismatch_even_if_fresh(patched):
    aid = _aid(FRESH, 3)
    patched["quality"] = {"artifacts": {aid: _failed_artifact(failure_class="review_row_transaction_mismatch")}}
    queue_state: dict = {"items": {}}
    assert rre.requeue_recoverable_failed(queue_state, policy=dict(rre.DEFAULT_EXECUTION_POLICY), now=NOW) == []


def test_respects_recovery_cap(patched):
    aid = _aid(FRESH, 4)
    patched["quality"] = {"artifacts": {aid: _failed_artifact(failure_class="auth_required")}}
    # already recovered twice (max=2) → not recovered again
    queue_state: dict = {"items": {aid: {"recovery_requeue_count": 2}}}
    assert rre.requeue_recoverable_failed(queue_state, policy=dict(rre.DEFAULT_EXECUTION_POLICY), now=NOW) == []


def test_unparseable_date_not_recovered(patched):
    aid = "publish::reviews_reply_positive::notadate"  # < 4 parts → None date
    patched["quality"] = {"artifacts": {aid: _failed_artifact(failure_class="auth_required")}}
    queue_state: dict = {"items": {}}
    assert rre.requeue_recoverable_failed(queue_state, policy=dict(rre.DEFAULT_EXECUTION_POLICY), now=NOW) == []


def test_disabled_by_policy(patched):
    aid = _aid(FRESH, 5)
    patched["quality"] = {"artifacts": {aid: _failed_artifact(failure_class="auth_required")}}
    policy = {**rre.DEFAULT_EXECUTION_POLICY, "failed_requeue_enabled": False}
    queue_state: dict = {"items": {}}
    assert rre.requeue_recoverable_failed(queue_state, policy=policy, now=NOW) == []


def test_ignores_non_failed_and_other_flows(patched):
    patched["quality"] = {"artifacts": {
        _aid(FRESH, 6): {"decision": {"flow": "reviews_reply_positive", "execution_state": "posted",
                                       "execution_attempts": [{"failure": {"failure_class": "auth_required"}}]}},
        "publish::meme::2026-06-15::x": {"decision": {"flow": "meme", "execution_state": "failed",
                                                      "execution_attempts": [{"failure": {"failure_class": "auth_required"}}]}},
    }}
    queue_state: dict = {"items": {}}
    assert rre.requeue_recoverable_failed(queue_state, policy=dict(rre.DEFAULT_EXECUTION_POLICY), now=NOW) == []
