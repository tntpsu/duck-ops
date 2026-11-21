"""An agent drop waiting on the operator belongs in the decision queue.

Surface 181, from a /new-flow audit of the express lane on 2026-11-21.

The lane shipped with the digest email and four OS cards as its ONLY operator
surfaces. On 2026-11-20 the digest did not fire (it runs from the launchd
wrapper, not the scan) and then an approval reply never arrived — and there was
nowhere else to look. The drop existed in a ledger and on a card, and in no
queue. `review_queue.json` had no mention of it and `desk status` did not list
it, both verified before this was written.

That is the email-to-portal inversion ([[feedback_email_to_portal_inversion]]):
if the operator has to read an email to know something, the surface is wrong.

The contract this has to honour in BOTH directions
([[feedback_auto_manual_queue_contract]], asked 12+ times): a MANUAL/gated lane
belongs in the queue with a visible why, and an AUTO lane must stay out of it.
"""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402
import review_loop  # noqa: E402


def _now(**kw) -> str:
    return (datetime.now().astimezone() + timedelta(**kw)).isoformat()


class DropsReachTheOperatorQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        # conftest already redirects DROP_PROPOSALS_PATH to a tmp file.
        self.ledger = drop_intake.DROP_PROPOSALS_PATH

    def write(self, **entries) -> None:
        self.ledger.parent.mkdir(parents=True, exist_ok=True)
        self.ledger.write_text(json.dumps({
            "generated_at": _now(), "proposals": entries}), encoding="utf-8")

    def drop(self, **kw) -> dict:
        entry = {"lane": "express", "media_type": "video", "post_ready": True,
                 "approval": "operator_required", "claimed_by": None,
                 "caption": "Flannel beer duck or barrel whiskey duck?",
                 "source_agent": "gemini-post-writer",
                 "received_at": _now(hours=-2), "publish_at": "asap"}
        entry.update(kw)
        return entry

    # -- the gap this closes ------------------------------------------------

    def test_a_pending_drop_becomes_a_queue_item(self):
        self.write(beer_vs_whiskey=self.drop())
        items = review_loop.build_drop_items()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["artifact_id"],
                         "drop::express::beer_vs_whiskey")
        self.assertEqual(items[0]["flow"], "drops")
        self.assertEqual(items[0]["review_status"], "pending")

    def test_it_is_reachable_through_build_review_items(self):
        # The builder existing is not the fix; being WIRED is the fix
        # ([[feedback_loader_and_registration_both_need_tests]]).
        self.write(beer_vs_whiskey=self.drop())
        items = review_loop.build_review_items(
            {"quality_gate": {}, "trend_ranker": {}})
        self.assertIn("drop::express::beer_vs_whiskey",
                      [i.get("artifact_id") for i in items])

    def test_it_surfaces_rather_than_sitting_in_the_unsurfaced_tail(self):
        # `surfaced_review_items` is what the operator actually sees; an item
        # that lands only in `items` is still invisible.
        self.write(beer_vs_whiskey=self.drop())
        items = review_loop.annotate_review_freshness(
            review_loop.build_drop_items())
        surfaced = review_loop.surfaced_review_items(items)
        self.assertEqual(len(surfaced), 1)

    def test_the_item_carries_a_visible_why(self):
        self.write(beer_vs_whiskey=self.drop())
        why = " ".join(review_loop.build_drop_items()[0]["reasoning"])
        self.assertIn("gemini-post-writer", why)
        self.assertIn("express", why)
        self.assertIn("ten minutes of approval", why)

    def test_the_item_carries_the_exact_reply_to_send(self):
        self.write(beer_vs_whiskey=self.drop())
        self.assertEqual(review_loop.build_drop_items()[0]["action_frame"],
                         "drop approve beer_vs_whiskey")

    # -- the other direction of the contract --------------------------------

    def test_an_approved_drop_is_NOT_in_the_queue(self):
        # Already decided. Re-asking is the nag this contract forbids.
        self.write(beer_vs_whiskey=self.drop(approval="approved"))
        self.assertEqual(review_loop.build_drop_items(), [])

    def test_a_declined_drop_is_NOT_in_the_queue(self):
        self.write(beer_vs_whiskey=self.drop(approval="declined"))
        self.assertEqual(review_loop.build_drop_items(), [])

    def test_a_claimed_drop_is_NOT_in_the_queue(self):
        self.write(beer_vs_whiskey=self.drop(claimed_by="express::run1"))
        self.assertEqual(review_loop.build_drop_items(), [])

    # -- the cases that would otherwise mislead -----------------------------

    def test_a_missed_window_says_approving_alone_will_not_send_it(self):
        # The most dangerous queue entry in this lane: approving it looks like
        # it worked and nothing goes out.
        self.write(beer_vs_whiskey=self.drop(publish_at=_now(hours=-3)))
        item = review_loop.build_drop_items()[0]
        why = " ".join(item["reasoning"])
        self.assertIn("will NOT send it", why)
        self.assertIn("drop approve beer_vs_whiskey now", why)

    def test_a_scheduled_drop_names_the_time_it_is_held_for(self):
        self.write(beer_vs_whiskey=self.drop(publish_at=_now(hours=+5)))
        self.assertIn("held for", " ".join(
            review_loop.build_drop_items()[0]["reasoning"]))

    def test_a_not_ready_drop_says_approval_changes_nothing(self):
        self.write(beer_vs_whiskey=self.drop(post_ready=False))
        item = review_loop.build_drop_items()[0]
        self.assertEqual(item["decision"], "not_ready")
        self.assertIn("even if you approve", " ".join(item["reasoning"]))

    def test_a_weekly_lane_drop_is_queued_without_express_timing_copy(self):
        # meme/jeepfact/duckvideo drops belong in the queue too, and the
        # express timing sentences would be a lie there.
        self.write(monday_meme=self.drop(lane="meme", publish_at=None))
        why = " ".join(review_loop.build_drop_items()[0]["reasoning"])
        self.assertIn("meme", why)
        self.assertNotIn("ten minutes", why)

    # -- robustness ---------------------------------------------------------

    def test_an_absent_ledger_yields_nothing_rather_than_raising(self):
        if self.ledger.exists():
            self.ledger.unlink()
        self.assertEqual(review_loop.build_drop_items(), [])

    def test_a_junk_ledger_yields_nothing_rather_than_raising(self):
        self.ledger.parent.mkdir(parents=True, exist_ok=True)
        self.ledger.write_text("{not json", encoding="utf-8")
        self.assertEqual(review_loop.build_drop_items(), [])

    def test_the_item_count_is_capped(self):
        # A agent that drops fifty posts must not bury the rest of the queue.
        self.write(**{f"d{i}": self.drop() for i in range(25)})
        self.assertLessEqual(len(review_loop.build_drop_items()),
                             review_loop.MAX_DROP_OPERATOR_ITEMS)


class TheDropsFlowSpecSaysOnlyWhatIsTrueTests(unittest.TestCase):
    """A FlowSpec with the wrong fields filled in is worse than none.

    `duck_flows.FLOWS` is the single description every consumer reads, so an
    entry claiming a publish-state probe this lane cannot support would send
    reconciliation looking for a run dir that will never exist.
    """

    def spec(self):
        import duck_flows

        return duck_flows.FLOWS["drops"]

    def test_the_lane_is_registered_at_all(self):
        self.assertEqual(self.spec().name, "drops")

    def test_it_declares_the_handoff_the_mail_path_uses(self):
        spec = self.spec()
        self.assertEqual(spec.handoff_actions.get("approve"), "publish")
        self.assertEqual(spec.handoff_target_flow, "drops")

    def test_the_approval_meaning_distinguishes_express_from_a_weekly_lane(self):
        # The operator is approving two quite different things depending on
        # the lane, and the email says so.
        meaning = self.spec().approval_meaning
        self.assertIn("ten minutes", meaning)
        self.assertIn("weekly", meaning)

    def test_it_declares_NO_publish_state_probe(self):
        # The probe reads runs/<run_id>/<state_file>, and a drop's approval
        # email carries RUN:<date> while the publish happens later under
        # runs/express_<timestamp>/. Declaring one would point reconciliation
        # at a directory that never exists.
        spec = self.spec()
        self.assertEqual(spec.state_file, "")
        self.assertEqual(spec.publish_status_key, "")
        self.assertEqual(spec.publish_truthy_keys, ())
        self.assertEqual(spec.scheduled_at_keys, ())

    def test_it_declares_NO_static_reply_actions(self):
        # Every drop command names its own stem, so a fixed mailto body
        # cannot express one. The digest builds per-stem actions through the
        # same shared renderer, which is what this field exists to ensure.
        self.assertEqual(self.spec().reply_actions, ())

    def test_the_digest_really_does_use_the_shared_renderer(self):
        # The claim the empty reply_actions rests on. If the digest ever
        # hand-rolled its buttons, that omission would become a real gap.
        digest = (Path(__file__).resolve().parents[1]
                  / "runtime" / "drop_digest.py").read_text(encoding="utf-8")
        self.assertIn("render_reply_action_buttons", digest)
        self.assertIn("email_reply_action_helper", digest)
