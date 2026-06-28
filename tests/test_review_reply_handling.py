"""Surface 44: resolve_handling (the auto/manual single source of truth) and the
auto-enqueue gating repoint (keys off handling, not the overloaded
review_status=="pending"), with the operator-kill + dedup safety guards."""
from __future__ import annotations

import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_reply_executor as rre  # noqa: E402

ON = {"auto_execution_enabled": True, "auto_queue_publish_ready_positive": True}
OFF = {"auto_execution_enabled": False, "auto_queue_publish_ready_positive": True}


# ---- resolve_handling -----------------------------------------------------

def test_persisted_handling_wins():
    assert rre.resolve_handling({"handling": "manual", "flow": "reviews_reply_positive"}, policy=ON) == "manual"
    assert rre.resolve_handling({"handling": "auto", "flow": "newduck"}, policy=OFF) == "auto"


def test_derived_auto_when_policy_on():
    assert rre.resolve_handling({"flow": "reviews_reply_positive"}, policy=ON) == "auto"


def test_derived_manual_when_policy_off_or_other_flow():
    assert rre.resolve_handling({"flow": "reviews_reply_positive"}, policy=OFF) == "manual"
    assert rre.resolve_handling({"flow": "meme"}, policy=ON) == "manual"


# ---- auto_enqueue gating ---------------------------------------------------

def _decision(**over):
    d = {"flow": "reviews_reply_positive", "decision": "publish_ready",
         "review_status": "pending", "execution_state": "not_queued",
         "review_target": {"transaction_id": "111", "listing_id": "222"}}
    d.update(over)
    return d


def _run_auto_enqueue(monkeypatch, artifacts: dict):
    monkeypatch.setattr(rre, "load_execution_policy",
                        lambda: {"auto_execution_enabled": True, "auto_queue_publish_ready_positive": True,
                                 "auto_queue_requires_browser_approval": False})
    monkeypatch.setattr(rre, "load_quality_gate_state", lambda: {"artifacts": artifacts})
    queued: list[str] = []
    def fake_queue(artifact_id, **kw):
        queued.append(artifact_id)
        return {"status": "queued"}
    monkeypatch.setattr(rre, "queue_review_reply", fake_queue)
    rre.auto_enqueue_publish_ready(queued_by="test")
    return queued


def test_auto_pending_item_is_queued(monkeypatch):
    q = _run_auto_enqueue(monkeypatch, {"a1": {"decision": _decision()}})
    assert q == ["a1"]


def test_manual_handling_is_skipped(monkeypatch):
    q = _run_auto_enqueue(monkeypatch, {"a1": {"decision": _decision(handling="manual")}})
    assert q == []


def test_operator_rejected_never_auto_posts(monkeypatch):
    # publish_ready + auto, but operator rejected → must NOT post (kill wins)
    q = _run_auto_enqueue(monkeypatch, {"a1": {"decision": _decision(review_status="rejected")}})
    assert q == []
    q2 = _run_auto_enqueue(monkeypatch, {"a1": {"decision": _decision(review_status="archived")}})
    assert q2 == []


def test_already_queued_is_not_requeued(monkeypatch):
    q = _run_auto_enqueue(monkeypatch, {"a1": {"decision": _decision(execution_state="queued")}})
    assert q == []


def test_approved_but_unqueued_auto_item_now_posts(monkeypatch):
    # intended behavior change: an approved (not rejected) auto reply that never
    # got queued does post — it's approved, so posting is correct.
    q = _run_auto_enqueue(monkeypatch, {"a1": {"decision": _decision(review_status="approved")}})
    assert q == ["a1"]


def test_non_publish_ready_skipped(monkeypatch):
    q = _run_auto_enqueue(monkeypatch, {"a1": {"decision": _decision(decision="needs_revision")}})
    assert q == []
