"""Surface 57 fix 1 — a transient "row could not be located" failure must skip
the item and keep draining, NOT trip stop_after_first_failure. Regression for
2026-07-11: one unfindable head-of-queue row halted the whole drain and blocked
~14 postable rows behind it (0 posted). A genuine auth/identity/text-mismatch
fault must still stop."""
from __future__ import annotations

import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_reply_executor as rre  # noqa: E402


def test_classifier_recognizes_transient_locate_failures():
    assert rre._result_is_transient_locate_failure(
        {"message": "Exact review row could not be found in the signed-in Etsy session. Auto-retrying later."}
    ) is True
    assert rre._result_is_transient_locate_failure(
        {"queue_item": {"last_failure_class": "review_row_not_found"}}
    ) is True
    # Genuine faults are NOT transient-locate — they must still stop the drain.
    assert rre._result_is_transient_locate_failure(
        {"message": "The textarea no longer matches the exact approved reply text."}
    ) is False
    assert rre._result_is_transient_locate_failure(
        {"queue_item": {"last_failure_class": "review_row_transaction_mismatch"}}
    ) is False
    assert rre._result_is_transient_locate_failure(None) is False


def _wire_drain(monkeypatch, dry_run_fn, submit_fn, queue_items):
    monkeypatch.setattr(rre, "load_execution_policy", lambda: {
        "auto_execution_enabled": True,
        "auto_drain_enabled": True,
        "stop_after_first_failure": True,
        "auto_drain_max_submits_per_run": 5,
    })
    monkeypatch.setattr(rre, "load_queue_state", lambda: {"items": queue_items})
    monkeypatch.setattr(rre, "save_queue_state", lambda *a, **k: None)
    monkeypatch.setattr(rre, "auto_dismiss_stale_queued", lambda *a, **k: [])
    monkeypatch.setattr(rre, "requeue_recoverable_failed", lambda *a, **k: [])
    monkeypatch.setattr(rre, "prepare_auth_for_drain", lambda policy: {"ready": True})
    monkeypatch.setattr(rre, "run_dry_run_fill", dry_run_fn)
    monkeypatch.setattr(rre, "run_live_submit", submit_fn)
    monkeypatch.setattr(rre, "load_session_state", lambda: {})
    monkeypatch.setattr(rre, "close_open_session", lambda *a, **k: False)


def test_drain_skips_not_found_head_and_posts_next(monkeypatch):
    items = {
        "aid_notfound": {"artifact_id": "aid_notfound", "status": "queued", "queued_at": "2026-07-01T00:00:00+00:00"},
        "aid_postable": {"artifact_id": "aid_postable", "status": "queued", "queued_at": "2026-07-02T00:00:00+00:00"},
    }

    def dry(aid, **k):
        if aid == "aid_notfound":
            return {"ok": False, "status": "queued",
                    "message": "Exact review row could not be found in the signed-in Etsy session. Auto-retrying later."}
        return {"ok": True, "status": "dry_run_filled"}

    def submit(aid, **k):
        return {"ok": True, "status": "posted"}

    _wire_drain(monkeypatch, dry, submit, items)
    res = rre.drain_queue(keep_browser_open=True, send_summary=False)
    # The not-found head item did NOT halt the drain — the postable one posted.
    assert res["posted_count"] == 1, res
    assert res["locate_deferred_count"] == 1, res
    assert res["failed_count"] == 0, res


def test_drain_still_stops_on_genuine_fault(monkeypatch):
    items = {
        "aid_fault": {"artifact_id": "aid_fault", "status": "queued", "queued_at": "2026-07-01T00:00:00+00:00"},
        "aid_postable": {"artifact_id": "aid_postable", "status": "queued", "queued_at": "2026-07-02T00:00:00+00:00"},
    }

    def dry(aid, **k):
        if aid == "aid_fault":
            return {"ok": False, "status": "failed",
                    "message": "The textarea no longer matches the exact approved reply text."}
        return {"ok": True, "status": "dry_run_filled"}

    def submit(aid, **k):
        return {"ok": True, "status": "posted"}

    _wire_drain(monkeypatch, dry, submit, items)
    res = rre.drain_queue(keep_browser_open=True, send_summary=False)
    # A real fault at the head still stops (stop_after_first_failure) — the
    # postable item is NOT reached, and it's counted as a failure, not deferred.
    assert res["failed_count"] == 1, res
    assert res["posted_count"] == 0, res
    assert res["locate_deferred_count"] == 0, res
