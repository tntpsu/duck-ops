"""2026-06-27: auto-approved flows must NOT appear in the operator review
decision queue. A reviews_reply_positive reply that passed the gate
(publish_ready) posts automatically — surfacing it as a 'needs my decision'
item was a bug (operator asked to approve something already auto-posting).

The fix is display-only: items stay review_status=pending (the auto-enqueue
keys off that) but are excluded from surfaced_review_items. Driven by a single
auto-approved-flow set so any FUTURE auto-approve flow is covered automatically."""
from __future__ import annotations

import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_loop as rl  # noqa: E402

AUTO = {"reviews_reply_positive"}


def _item(flow, decision, *, is_fresh=True):
    return {"artifact_id": f"a::{flow}::{decision}", "flow": flow,
            "decision": decision, "is_fresh": is_fresh}


# ---- the predicate --------------------------------------------------------

def test_auto_approved_publish_ready_is_auto_handled():
    assert rl.item_is_auto_handled(_item("reviews_reply_positive", "publish_ready"), AUTO) is True


def test_non_publish_ready_is_not_auto_handled():
    # needs_revision still needs a human even on an auto-approve flow
    assert rl.item_is_auto_handled(_item("reviews_reply_positive", "needs_revision"), AUTO) is False


def test_non_auto_flow_is_not_auto_handled():
    assert rl.item_is_auto_handled(_item("newduck", "publish_ready"), AUTO) is False


def test_generality_any_flow_in_the_set_is_covered():
    # proves a FUTURE auto-approve flow is handled with zero extra code
    assert rl.item_is_auto_handled(_item("meme", "publish_ready"), {"meme"}) is True


# ---- the queue filter -----------------------------------------------------

def test_surfaced_excludes_auto_handled_keeps_real_decisions():
    items = [
        {"artifact_id": "auto", "is_fresh": True, "auto_handled": True},
        {"artifact_id": "human", "is_fresh": True, "auto_handled": False},
        {"artifact_id": "stale", "is_fresh": False, "auto_handled": False},
    ]
    surfaced = [i["artifact_id"] for i in rl.surfaced_review_items(items)]
    assert surfaced == ["human"]


def test_annotate_sets_auto_handled(monkeypatch):
    monkeypatch.setattr(rl, "_auto_approved_flows", lambda: AUTO)
    items = [_item("reviews_reply_positive", "publish_ready"),
             _item("newduck", "publish_ready")]
    rl.annotate_review_freshness(items)
    by = {i["flow"]: i["auto_handled"] for i in items}
    assert by["reviews_reply_positive"] is True and by["newduck"] is False


# ---- policy wiring (single source of truth) -------------------------------

def test_auto_approved_flows_reads_policy(monkeypatch):
    monkeypatch.setattr(rl, "_auto_approved_flows", rl._auto_approved_flows)  # ensure real fn
    import review_reply_executor as rre
    monkeypatch.setattr(rre, "load_execution_policy",
                        lambda: {"auto_execution_enabled": True, "auto_queue_publish_ready_positive": True})
    assert "reviews_reply_positive" in rl._auto_approved_flows()
    monkeypatch.setattr(rre, "load_execution_policy",
                        lambda: {"auto_execution_enabled": False, "auto_queue_publish_ready_positive": True})
    assert rl._auto_approved_flows() == set()
