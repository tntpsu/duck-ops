"""Receipts flip from `scheduled` to `published` once there is proof.

Receipts were written at schedule time and never updated, so a post three weeks
old still read "scheduled". Status recorded intent, not outcome, and there was
no local answer to "did this actually go out?".

The dangerous version of this fix is one that flips on absence of evidence. A
failed fetch means we could not reach Meta, which is not the same as "it never
published" -- collapsing the two would quietly relabel healthy posts during an
outage. So: positive evidence only.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from runtime.social_performance_collector import writeback_published_status

NOW = datetime.fromisoformat("2026-10-12T20:00:00-04:00")


def _receipt(tmp_path, status="scheduled", post_id="PAGE_1"):
    path = tmp_path / "meme_posts.json"
    path.write_text(json.dumps({"posts": [
        {"platform": "facebook", "post_id": post_id, "status": status},
        {"platform": "facebook", "post_id": "PAGE_OTHER", "status": "scheduled"},
    ]}))
    return path


def _post(path, **over):
    base = {
        "post_id": "PAGE_1",
        "status": "scheduled",
        "metric_status": "ok",
        "metrics": {"timestamp": "2026-10-12T22:00:08+0000"},
        "scheduled_time": "2026-10-12T18:00:00-04:00",
        "receipt_path": str(path),
    }
    base.update(over)
    return base


def test_a_confirmed_post_is_marked_published(tmp_path):
    path = _receipt(tmp_path)
    out = writeback_published_status(_post(path), now=NOW)
    assert out["action"] == "marked_published"
    rows = {p["post_id"]: p for p in json.loads(path.read_text())["posts"]}
    assert rows["PAGE_1"]["status"] == "published"
    assert rows["PAGE_1"]["published_at"] == "2026-10-12T22:00:08+0000"
    # and nothing else moved
    assert rows["PAGE_OTHER"]["status"] == "scheduled"


def test_a_failed_fetch_leaves_the_receipt_alone(tmp_path):
    """"We could not reach Meta" is not "it never published"."""
    path = _receipt(tmp_path)
    out = writeback_published_status(_post(path, metric_status="fetch_failed"), now=NOW)
    assert out["action"] == "skipped"
    assert out["reason"] == "no_positive_evidence"
    assert json.loads(path.read_text())["posts"][0]["status"] == "scheduled"


def test_a_post_whose_slot_has_not_passed_is_left_alone(tmp_path):
    path = _receipt(tmp_path)
    out = writeback_published_status(
        _post(path), now=datetime.fromisoformat("2026-10-12T17:00:00-04:00")
    )
    assert out["reason"] == "slot_has_not_passed"
    assert json.loads(path.read_text())["posts"][0]["status"] == "scheduled"


def test_a_post_with_no_observed_timestamp_is_not_proof(tmp_path):
    path = _receipt(tmp_path)
    out = writeback_published_status(_post(path, metrics={}), now=NOW)
    assert out["reason"] == "no_observed_timestamp"


def test_a_cancelled_receipt_is_never_resurrected(tmp_path):
    """A post we deleted on purpose must not come back as published."""
    path = _receipt(tmp_path, status="cancelled")
    out = writeback_published_status(_post(path, status="cancelled"), now=NOW)
    assert out["reason"] == "receipt_not_scheduled"
    assert json.loads(path.read_text())["posts"][0]["status"] == "cancelled"


def test_it_is_idempotent(tmp_path):
    """The collector re-runs on a cadence over the same window."""
    path = _receipt(tmp_path)
    assert writeback_published_status(_post(path), now=NOW)["action"] == "marked_published"
    second = writeback_published_status(_post(path), now=NOW)
    assert second["action"] == "post_id_mismatch"  # nothing left in 'scheduled'
    assert json.loads(path.read_text())["posts"][0]["status"] == "published"


def test_a_missing_receipt_file_never_raises(tmp_path):
    out = writeback_published_status(
        _post(tmp_path / "nope.json"), now=NOW
    )
    assert out["action"] == "no_receipt"


def test_a_partial_fetch_still_counts_as_proof(tmp_path):
    """Facebook posts are 'partial' by nature now -- reach is unavailable for
    this page -- so requiring 'ok' would mean Facebook receipts never flip,
    which is the exact lane this was built for."""
    path = _receipt(tmp_path)
    out = writeback_published_status(_post(path, metric_status="partial"), now=NOW)
    assert out["action"] == "marked_published"
