"""Review-reply reliability regressions (2026-06-22).

Two distinct field failures that kept review replies from posting:

1. PACING SELF-THROTTLE — at 14/18 visible Etsy commands the soft reservation
   deferred *all* reads to protect post slots, but a review-reply POST does its
   own setup reads (navigate to row, auth probe, post-submit verify). Those got
   deferred too, leaving replies filled-but-not-submitted. The fix exempts reads
   issued inside review_reply_post_window() from the soft reservation (the hard
   ceiling + mutating cap still apply).

2. STALE FAILED RECEIPTS — auto_dismiss_stale_queued only cleared *queued*
   receipts. A failed transaction_mismatch older than the freshness window will
   never post (Etsy review-row metadata drift) yet sat in the queue forever as a
   permanent false "failure" (4 such items, ages 14-71d). The fix also dismisses
   stale FAILED receipts.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import etsy_browser_guard as guard  # noqa: E402
import review_reply_executor as rre  # noqa: E402

NOW = datetime(2026, 6, 22)
STALE = "2026-05-01"   # 52 days old > 14d window
FRESH = "2026-06-22"   # 0 days old


def _aid(date: str, n: int = 1) -> str:
    return f"publish::reviews_reply_positive::{date}::review-{n}"


# ---- Fix 1: pacing reservation exempts review-reply post reads -------------

def _state_at_14_visible_reads() -> dict:
    """14 visible non-mutating Etsy reads, well-spaced so the gap sleep is a
    no-op. 14 == MAX_COMMANDS_PER_WINDOW - RESERVED_FOR_MUTATING (18-4)."""
    # Events must sit inside the 5-min rolling window (RATE_WINDOW_SECONDS) or
    # _prune_events drops them, and >MIN_GAP_SECONDS before now or before_command
    # sleeps to enforce the inter-command gap. 30s ago satisfies both. Use real
    # wall-clock now (the guard prunes against datetime.now(), not our NOW const).
    recent = (datetime.now().astimezone() - timedelta(seconds=30)).isoformat()
    events = [
        {"at": recent, "session": "esr", "command": "eval", "mutating": False}
        for _ in range(14)
    ]
    return {"blocked_until": None, "block_reason": None, "events": events}


def test_read_deferred_at_reservation_threshold_outside_post_window(tmp_path):
    state_path = tmp_path / "guard_state.json"
    with patch.object(guard, "STATE_PATH", state_path):
        guard.save_state(_state_at_14_visible_reads())
        # A plain read at 14/18 is deferred to reserve slots for posts.
        with pytest.raises(guard.PacingReservationError):
            guard.before_command("esr", ("eval", "() => document.title"))


def test_read_exempt_inside_review_reply_post_window(tmp_path):
    state_path = tmp_path / "guard_state.json"
    with patch.object(guard, "STATE_PATH", state_path):
        guard.save_state(_state_at_14_visible_reads())
        # The SAME read, issued during a review-reply post, is exempt — the
        # reservation must not block the post's own setup/verify reads.
        with guard.review_reply_post_window():
            guard.before_command("esr", ("eval", "() => document.title"))
        # depth unwinds back to zero on exit → reservation active again.
        assert guard._in_review_reply_post() is False
        with pytest.raises(guard.PacingReservationError):
            guard.before_command("esr", ("eval", "() => document.title"))


def test_hard_ceiling_still_applies_inside_post_window(tmp_path):
    """The exemption is only for the SOFT reservation. At the hard ceiling
    (18/18) even a post-window read must still cool down."""
    state_path = tmp_path / "guard_state.json"
    with patch.object(guard, "STATE_PATH", state_path):
        st = _state_at_14_visible_reads()
        old = st["events"][0]["at"]
        st["events"] = [
            {"at": old, "session": "esr", "command": "eval", "mutating": False}
            for _ in range(guard.MAX_COMMANDS_PER_WINDOW)
        ]
        guard.save_state(st)
        with guard.review_reply_post_window():
            with pytest.raises(RuntimeError) as exc:
                guard.before_command("esr", ("eval", "() => document.title"))
        # the hard ceiling raises plain RuntimeError, NOT PacingReservationError
        assert not isinstance(exc.value, guard.PacingReservationError)


# ---- Fix 2: auto_dismiss_stale_queued also clears stale FAILED -------------

@pytest.fixture
def no_transition(monkeypatch):
    monkeypatch.setattr(rre, "_record_review_execution_transition", lambda *a, **k: None)


def test_dismisses_stale_failed_receipt(no_transition):
    aid = _aid(STALE)
    items = {aid: {"status": "failed", "failure_class": "review_row_transaction_mismatch"}}
    dismissed = rre.auto_dismiss_stale_queued(
        items, policy={"auto_dismiss_after_days": 14}, now=NOW
    )
    assert dismissed == [aid]
    assert items[aid]["status"] == "dismissed"


def test_dismisses_stale_queued_receipt(no_transition):
    aid = _aid(STALE, 2)
    items = {aid: {"status": "queued"}}
    assert rre.auto_dismiss_stale_queued(
        items, policy={"auto_dismiss_after_days": 14}, now=NOW
    ) == [aid]


def test_keeps_fresh_failed_receipt(no_transition):
    """A failed receipt inside the freshness window may still be recoverable —
    do NOT dismiss it (the recovery sweep owns those)."""
    aid = _aid(FRESH, 3)
    items = {aid: {"status": "failed"}}
    assert rre.auto_dismiss_stale_queued(
        items, policy={"auto_dismiss_after_days": 14}, now=NOW
    ) == []
    assert items[aid]["status"] == "failed"


def test_ignores_terminal_statuses(no_transition):
    """posted / dismissed / skipped are terminal — never re-dismissed even if old."""
    items = {
        _aid(STALE, 4): {"status": "posted"},
        _aid(STALE, 5): {"status": "dismissed"},
        _aid(STALE, 6): {"status": "skipped"},
    }
    assert rre.auto_dismiss_stale_queued(
        items, policy={"auto_dismiss_after_days": 14}, now=NOW
    ) == []


def test_result_is_retryable_throttle_detection():
    # Primary signal: the failed queue_item carries last_failure_class.
    assert rre._result_is_retryable_throttle(
        {"queue_item": {"last_failure_class": "pacing_cooldown"}}
    ) is True
    # A real hard fault is NOT a throttle.
    assert rre._result_is_retryable_throttle(
        {"queue_item": {"last_failure_class": "reply_box_not_visible"},
         "message": "reply textarea did not appear"}
    ) is False
    assert rre._result_is_retryable_throttle({}) is False
    assert rre._result_is_retryable_throttle(None) is False


def test_pacing_throttle_on_front_item_does_not_halt_drain():
    # Regression (11-day outage): a retryable pacing_cooldown on the FIRST
    # eligible item must NOT trip stop_after_first_failure and wedge the whole
    # drain. The throttled item defers; later items still post.
    from contextlib import nullcontext

    item_a = {"artifact_id": "A", "status": "queued",
              "queued_at": "2026-06-26T00:00:00+00:00", "last_preflight_status": "dry_run_filled"}
    item_b = {"artifact_id": "B", "status": "queued",
              "queued_at": "2026-06-27T00:00:00+00:00", "last_preflight_status": "dry_run_filled"}
    queue_state = {"items": {"A": item_a, "B": item_b}}

    def fake_submit(artifact_id, *, keep_browser_open=False):
        if artifact_id == "A":
            return {"ok": False, "status": "failed", "message": "preemptive cooldown",
                    "queue_item": {"last_failure_class": "pacing_cooldown"}}
        return {"ok": True, "status": "posted"}

    with patch.object(rre, "load_queue_state", return_value=queue_state), \
         patch.object(rre, "save_queue_state", lambda *a, **k: None), \
         patch.object(rre, "auto_dismiss_stale_queued", lambda *a, **k: {}), \
         patch.object(rre, "requeue_recoverable_failed", lambda *a, **k: {}), \
         patch.object(rre, "prepare_auth_for_drain", lambda *a, **k: {"ready": True}), \
         patch.object(rre, "run_live_submit", side_effect=fake_submit), \
         patch.object(rre, "review_reply_post_window", lambda *a, **k: nullcontext()), \
         patch.object(rre, "load_session_state", lambda *a, **k: {}), \
         patch.object(rre, "close_open_session", lambda *a, **k: False), \
         patch.object(rre, "save_session_state", lambda *a, **k: None), \
         patch.object(rre, "send_session_summary_email", lambda *a, **k: None):
        result = rre.drain_queue(
            keep_browser_open=True,
            send_summary=False,
            policy_override={
                "auto_execution_enabled": True,
                "auto_drain_enabled": True,
                "stop_after_first_failure": True,
                "auto_drain_max_submits_per_run": 5,
            },
        )

    assert result["posted_count"] == 1, result   # B posted despite A throttling first
    assert result["throttled_count"] == 1, result
    assert result["failed_count"] == 0, result
