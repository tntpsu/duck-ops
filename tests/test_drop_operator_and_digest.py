"""Surface 135 -- approving without a shell.

Three drops sat awaiting the operator while the only thing that could surface
them was a CLI flag, on a day the operator had no terminal. Every other gated
lane here is approvable by replying to an email, because
`operator_inbox_poller` pulls the reply text out of the message and hands it
to `review_loop.handle_operator_text`. These commands are what that text can
now say, and the digest is what prompts it.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402
import drop_digest  # noqa: E402
import drop_operator  # noqa: E402

NOW = datetime(2026, 11, 16, 12, 0).astimezone()


def _ledger(tmp: Path, entries: dict) -> Path:
    path = tmp / "proposals.json"
    path.write_text(json.dumps({"proposals": entries}), encoding="utf-8")
    return path


def _entry(**kw):
    base = {"lane": "meme", "approval": "operator_required", "claimed_by": None,
            "post_ready": True, "caption": "A duck.",
            "source_agent": "gemini", "received_at": NOW.isoformat()}
    base.update(kw)
    return base


class RoutingTests(unittest.TestCase):
    """Namespaced under `drop ` because `approve` already appears 27 times in
    review_loop's scope ([[feedback_edits_into_shared_scope_collide]])."""

    def test_it_claims_only_its_own_namespace(self):
        for text in ("drops", "drop", "drop approve x", "drops status",
                     "DROP APPROVE X"):
            with self.subTest(text=text):
                self.assertTrue(drop_operator.should_delegate_to_drops(text))

    def test_it_does_not_swallow_the_review_queues_commands(self):
        for text in ("approve 240", "agree 205", "desk status", "status",
                     "help", "", "   ", "dropped the ball"):
            with self.subTest(text=text):
                self.assertFalse(drop_operator.should_delegate_to_drops(text))

    def test_review_loop_delegates_to_us(self):
        import review_loop
        self.assertTrue(review_loop.should_delegate_to_drops("drops"))
        self.assertFalse(review_loop.should_delegate_to_drops("approve 240"))

    def test_the_operator_help_lists_the_commands(self):
        import review_loop
        text = "\n".join(review_loop.operator_help(None)) \
            if isinstance(review_loop.operator_help(None), list) \
            else str(review_loop.operator_help(None))
        self.assertIn("drop approve", text)


class ApprovingByReplyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def _with(self, entries):
        path = _ledger(self.root, entries)
        return mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path)

    def test_approving_names_the_lane_and_what_happens_next(self):
        with self._with({"holiday_clip": _entry()}):
            out = drop_operator.handle_drop_text("drop approve holiday_clip")
        self.assertIn("Approved holiday_clip", out)
        self.assertIn("meme", out)
        self.assertIn("next scheduled run", out)

    def test_a_unique_prefix_is_enough_to_type_on_a_phone(self):
        with self._with({"mid_november_holiday_horror": _entry()}):
            out = drop_operator.handle_drop_text("drop approve mid_nov")
        self.assertIn("Approved mid_november_holiday_horror", out)

    def test_an_ambiguous_prefix_is_refused_not_guessed(self):
        """The live stems differ only by a _v2/_v3 suffix. Guessing publishes
        the wrong cut of the same video."""
        with self._with({"clip_v2": _entry(), "clip_v3": _entry()}):
            out = drop_operator.handle_drop_text("drop approve clip")
        self.assertIn("matches 2 drops", out)
        self.assertIn("clip_v2", out)
        self.assertNotIn("Approved", out)

    def test_an_unknown_stem_says_how_to_see_the_list(self):
        with self._with({"a": _entry()}):
            out = drop_operator.handle_drop_text("drop approve nope")
        self.assertIn("no drop named", out)
        self.assertIn("drops", out)

    def test_approving_something_the_agent_never_declared_ready_is_refused(self):
        """Otherwise the operator's half of a two-key gate carries both
        halves: post_ready is the AGENT saying it is finished."""
        with self._with({"draft": _entry(post_ready=False)}):
            out = drop_operator.handle_drop_text("drop approve draft")
        self.assertIn("post_ready:false", out)
        self.assertNotIn("Approved", out)

    def test_declining_is_permanent_and_says_so(self):
        with self._with({"bad": _entry()}):
            out = drop_operator.handle_drop_text("drop decline bad")
        self.assertIn("Declined bad", out)
        self.assertIn("no run will ever take it", out.lower())

    def test_a_bare_verb_asks_for_the_stem(self):
        with self._with({"a": _entry()}):
            self.assertIn("name the drop",
                          drop_operator.handle_drop_text("drop approve"))

    def test_an_unknown_verb_returns_help(self):
        self.assertIn("drop approve",
                      drop_operator.handle_drop_text("drop frobnicate x"))


class StatusReadsLikeSomethingYouCanReplyToTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_each_item_carries_the_exact_line_to_send_back(self):
        path = _ledger(self.root, {"holiday_clip": _entry()})
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=[]):
            out = drop_operator.handle_drop_text("drops")
        self.assertIn("reply: drop approve holiday_clip", out)

    def test_a_not_ready_drop_is_flagged_so_approving_is_not_wasted(self):
        path = _ledger(self.root, {"draft": _entry(post_ready=False)})
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=[]):
            out = drop_operator.handle_drop_text("drops")
        self.assertIn("post_ready:false", out)

    def test_an_empty_queue_says_so_plainly(self):
        path = _ledger(self.root, {})
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=[]):
            out = drop_operator.handle_drop_text("drops")
        self.assertIn("No agent drop is waiting", out)

    def test_unanswered_notes_appear_beside_the_drops(self):
        path = _ledger(self.root, {})
        notes = [{"file": "q.json", "subject": "hook length", "body": "hard limit?"}]
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=notes):
            out = drop_operator.handle_drop_text("drops")
        self.assertIn("q.json", out)
        self.assertIn("reply: drop answer q.json", out)


class TheDigestOnlySpeaksWhenSomethingChangesTests(unittest.TestCase):
    """The scan runs 4-hourly. Mailing on every non-empty scan would send six
    identical emails a day and be filtered inside a week."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = self.root / "digest.json"

    def _decide(self, entries, notes=(), now=NOW):
        path = _ledger(self.root, entries)
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes",
                               return_value=list(notes)):
            return drop_digest.decide(now=now, state_path=self.state)

    def _remember(self, decision):
        self.state.write_text(json.dumps({
            "stems": decision["stems"], "note_files": decision["note_files"],
            "milestone": decision["milestone"]}), encoding="utf-8")

    def test_a_new_drop_sends(self):
        d = self._decide({"a": _entry()})
        self.assertTrue(d["send"])
        self.assertEqual(d["reason"], "new_items")

    def test_the_same_drop_again_is_silent(self):
        first = self._decide({"a": _entry()})
        self._remember(first)
        again = self._decide({"a": _entry()})
        self.assertFalse(again["send"])
        self.assertEqual(again["reason"], "no_change")

    def test_a_second_drop_breaks_the_silence(self):
        self._remember(self._decide({"a": _entry()}))
        d = self._decide({"a": _entry(), "b": _entry()})
        self.assertTrue(d["send"])
        self.assertEqual(d["new_items"], ["b"])

    def test_it_re_mails_when_the_oldest_crosses_a_milestone(self):
        self._remember(self._decide({"a": _entry()}))
        later = self._decide({"a": _entry()}, now=NOW + timedelta(days=3, hours=1))
        self.assertTrue(later["send"])
        self.assertEqual(later["reason"], "aged_past_3d")

    def test_it_does_not_nag_between_milestones(self):
        first = self._decide({"a": _entry()})
        self._remember(first)
        d = self._decide({"a": _entry()}, now=NOW + timedelta(hours=18))
        self.assertFalse(d["send"])

    def test_a_cleared_queue_sends_one_all_clear(self):
        """Otherwise a resolved queue is indistinguishable from a dead job."""
        self._remember(self._decide({"a": _entry()}))
        d = self._decide({})
        self.assertTrue(d["send"])
        self.assertEqual(d["reason"], "all_clear")

    def test_a_queue_that_was_always_empty_stays_silent(self):
        d = self._decide({})
        self.assertFalse(d["send"])
        self.assertEqual(d["reason"], "nothing_waiting")

    def test_an_unanswered_note_counts_as_something_waiting(self):
        d = self._decide({}, notes=[{"file": "q.json", "subject": "s"}])
        self.assertTrue(d["send"])
        self.assertEqual(d["new_items"], ["q.json"])

    def test_the_subject_follows_the_house_pattern(self):
        d = self._decide({"a": _entry()})
        subject, _text, _html = drop_digest.render_email(d, now=NOW)
        self.assertTrue(subject.startswith("MJD: [DROPS]"))
        self.assertIn("FLOW:drops", subject)
        self.assertIn("ACTION:approve", subject)

    def test_the_all_clear_is_a_notify_not_an_action(self):
        self._remember(self._decide({"a": _entry()}))
        d = self._decide({})
        subject, _t, _h = drop_digest.render_email(d, now=NOW)
        self.assertIn("ACTION:notify", subject)

    def test_the_body_is_built_from_the_decision_not_a_second_read(self):
        """A second read could disagree with the one the decision was made
        from -- an email saying three drops wait while the count said two."""
        d = self._decide({"holiday_clip": _entry()})
        _s, text, _h = drop_digest.render_email(d, now=NOW)
        self.assertIn("drop approve holiday_clip", text)
        self.assertIn("Reply to this email", text)


class TheDigestStateIsGuardedTests(unittest.TestCase):
    def test_a_test_run_cannot_write_the_live_digest_state(self):
        import os
        os.environ["DUCK_TEST_MODE"] = "1"
        with self.assertRaises(drop_digest.TestModeRefusalError):
            drop_digest._write_state({},
                                     path=drop_digest._FROZEN_PRODUCTION_STATE_PATH)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
