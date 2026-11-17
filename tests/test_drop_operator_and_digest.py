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


class ClearingTheQueueInOneReplyTests(unittest.TestCase):
    """"I don't want to send 4 emails."

    A corrected re-send leaves its predecessors pending, so clearing a queue
    routinely means several decisions at once. One tap, or one multi-line
    reply, rather than one email per drop.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def _with(self, entries):
        return mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH",
                                 _ledger(self.root, entries))

    def _ledger_now(self):
        return json.loads((self.root / "proposals.json").read_text())["proposals"]

    def test_approve_only_declines_the_rest_of_that_lane(self):
        with self._with({"v1": _entry(), "v2": _entry(), "v4": _entry()}):
            out = drop_operator.handle_drop_text("drop approve v4 only")
        self.assertIn("Approved v4", out)
        self.assertIn("Declined 2 superseded", out)
        led = self._ledger_now()
        self.assertEqual(led["v4"]["approval"], "approved")
        self.assertEqual(led["v1"]["approval"], "declined")
        self.assertEqual(led["v2"]["approval"], "declined")

    def test_only_does_not_touch_another_lane(self):
        """Clearing Meme Monday must not silently cancel Jeep Fact
        Wednesday."""
        with self._with({"m1": _entry(lane="meme"), "m2": _entry(lane="meme"),
                         "j1": _entry(lane="jeepfact")}):
            drop_operator.handle_drop_text("drop approve m1 only")
        led = self._ledger_now()
        self.assertEqual(led["m2"]["approval"], "declined")
        self.assertEqual(led["j1"]["approval"], "operator_required")

    def test_only_does_not_touch_an_already_claimed_drop(self):
        with self._with({"a": _entry(), "done": _entry(claimed_by="meme::r1")}):
            drop_operator.handle_drop_text("drop approve a only")
        self.assertEqual(self._ledger_now()["done"]["approval"],
                         "operator_required")

    def test_only_says_so_when_nothing_else_was_waiting(self):
        with self._with({"solo": _entry()}):
            out = drop_operator.handle_drop_text("drop approve solo only")
        self.assertIn("Nothing else was waiting", out)

    def test_several_commands_in_one_reply_all_run(self):
        with self._with({"a": _entry(), "b": _entry(), "c": _entry()}):
            out = drop_operator.handle_drop_text(
                "drop decline a\ndrop decline b\ndrop approve c")
        led = self._ledger_now()
        self.assertEqual(led["a"]["approval"], "declined")
        self.assertEqual(led["b"]["approval"], "declined")
        self.assertEqual(led["c"]["approval"], "approved")
        self.assertIn("> drop decline a", out)

    def test_quoted_reply_text_around_the_commands_is_ignored(self):
        """An email reply carries the original beneath it."""
        with self._with({"a": _entry()}):
            out = drop_operator.handle_drop_text(
                "Hi,\ndrop approve a\n\nOn Mon someone wrote:\n> old stuff")
        self.assertIn("Approved a", out)

    def test_one_bad_line_does_not_stop_the_others(self):
        with self._with({"a": _entry()}):
            out = drop_operator.handle_drop_text(
                "drop decline nope\ndrop approve a")
        self.assertIn("no drop named", out)
        self.assertIn("Approved a", out)
        self.assertEqual(self._ledger_now()["a"]["approval"], "approved")

    def test_the_email_offers_the_one_tap_button_when_there_are_siblings(self):
        import io
        from contextlib import redirect_stderr
        state = self.root / "d.json"
        with self._with({"a": _entry(), "b": _entry()}), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=[]):
            d = drop_digest.decide(now=NOW, state_path=state)
        with redirect_stderr(io.StringIO()):
            _s, _t, html = drop_digest.render_email(
                d, now=NOW, reply_to="ops@example.com", previews=[])
        self.assertIn("Approve only this (clears 1)", html)
        self.assertNotIn("&amp;amp;", html)

    def test_a_solo_drop_gets_no_clear_button(self):
        import io
        from contextlib import redirect_stderr
        state = self.root / "d.json"
        with self._with({"a": _entry()}), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=[]):
            d = drop_digest.decide(now=NOW, state_path=state)
        with redirect_stderr(io.StringIO()):
            _s, _t, html = drop_digest.render_email(
                d, now=NOW, reply_to="ops@example.com", previews=[])
        self.assertNotIn("Approve only this", html)


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


class TheEmailIsReadableOnAPhoneTests(unittest.TestCase):
    """The first digest went out as a <pre> blob and the operator said they
    could barely read it, and that it asked them to approve something without
    showing it. Both are operator-surface failures, which is the recurring
    one here -- tests passing is not the same as the surface being usable.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = self.root / "digest.json"

    def _decide(self, entries, notes=()):
        path = _ledger(self.root, entries)
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes",
                               return_value=list(notes)):
            return drop_digest.decide(now=NOW, state_path=self.state)

    def _html(self, entries, previews=None, notes=(), reply_to="ops@example.com"):
        d = self._decide(entries, notes=notes)
        _s, _t, html = drop_digest.render_email(d, now=NOW, previews=previews,
                                                reply_to=reply_to)
        return html

    def test_the_media_is_shown_inline_not_merely_attached(self):
        """Approving something you cannot see is rubber-stamping it."""
        html = self._html(
            {"clip": _entry()},
            previews=[{"stem": "clip", "is_video": True,
                       "frames": [{"cid": "drop1a", "path": "/t/p.jpg",
                                   "label": None}]}])
        self.assertIn("src='cid:drop1a'", html)

    def test_a_drop_with_no_preview_still_appears_and_says_why(self):
        html = self._html({"clip": _entry()},
                          previews=[{"stem": "clip", "frames": [],
                                     "note": "the file is gone"}])
        self.assertIn("the file is gone", html)
        self.assertIn("drop%20approve%20clip", html.replace("+", "%20"))

    def test_no_reply_address_is_loud_rather_than_silently_buttonless(self):
        """An approval email with nothing to approve with, and no error, is
        the worst of both ([[feedback_swallowed_errors_lie]])."""
        import io
        import sys as _sys
        from contextlib import redirect_stderr
        _sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "duckAgent"))
        from helpers import email_reply_action_helper as erah

        err = io.StringIO()
        with redirect_stderr(err), \
             mock.patch.object(erah, "approval_reply_to", return_value=""):
            html = self._html({"clip": _entry()}, reply_to="")
        self.assertNotIn("mailto:", html)
        self.assertIn("APPROVAL_REPLY_TO", err.getvalue())

    def test_there_is_no_pre_block(self):
        """The thing that made it unreadable."""
        self.assertNotIn("<pre", self._html({"clip": _entry()}))

    def test_it_uses_the_house_approval_buttons_not_its_own(self):
        """Every other approval email here renders mailto: buttons that open a
        pre-filled draft. The first version told the operator to TYPE a
        44-character stem, which is unusable on the phone where it is read --
        "we shouldn't reinvent the wheel from our other approval emails"."""
        html = self._html({"clip": _entry()})
        self.assertIn("mailto:", html)
        self.assertIn("drop%20approve%20clip", html.replace("+", "%20"))
        # the house teal, from render_reply_action_buttons
        self.assertIn("#0f766e", html)

    def test_both_verbs_get_a_button(self):
        html = self._html({"clip": _entry()})
        self.assertIn(">Approve<", html)
        self.assertIn(">Decline<", html)

    def test_it_does_not_carry_a_second_stylesheet(self):
        """One more look for the operator to learn, for no benefit."""
        html = self._html({"clip": _entry()})
        self.assertNotIn("<style>", html)
        self.assertNotIn("duckdrop", html)

    def test_each_card_shows_the_caption(self):
        html = self._html({"clip": _entry(caption="Santa duck or doll duck?")})
        self.assertIn("Santa duck or doll duck?", html)

    def test_it_says_the_preview_is_the_finished_post(self):
        """The first version showed the raw clip and the operator said "I
        don't see memes, just photos". The preview now carries the burned-in
        caption, and the email says so rather than leaving them to wonder."""
        html = self._html({"clip": _entry()})
        self.assertIn("caption burned in", html)

    def test_a_not_ready_drop_offers_no_approve_button(self):
        html = self._html({"draft": _entry(post_ready=False)})
        self.assertIn("post_ready:false", html)
        self.assertNotIn("drop%20approve%20draft", html.replace("+", "%20"))

    def test_a_not_ready_drop_can_still_be_DECLINED(self):
        """It is precisely the drop you want to clear. The first version gated
        the whole button block on post_ready, so the only drop that could
        never be published was also the only one that could not be dismissed
        -- and it would have aged through every digest milestone forever."""
        html = self._html({"draft": _entry(post_ready=False)})
        self.assertIn("drop%20decline%20draft", html.replace("+", "%20"))
        self.assertIn(">Decline<", html)

    def test_every_pending_drop_has_at_least_one_button(self):
        """No card is a dead end."""
        import re
        html = self._html({"a": _entry(), "b": _entry(post_ready=False)})
        for stem in ("a", "b"):
            with self.subTest(stem=stem):
                found = re.findall(
                    r"drop(?:%20|\+)(approve|decline)(?:%20|\+)([A-Za-z0-9_]+)",
                    html)
                self.assertTrue([v for _k, v in found if v == stem],
                                f"{stem} has no actionable button")

    def test_user_text_is_escaped(self):
        """Captions come from an outside agent and land in our mail client."""
        html = self._html({"x": _entry(caption="<script>alert(1)</script>")})
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn("&lt;script&gt;", html)

    def test_the_all_clear_is_styled_too_not_a_bare_paragraph(self):
        self.state.write_text(json.dumps({"stems": ["old"], "note_files": [],
                                          "milestone": 0}), encoding="utf-8")
        html = self._html({})
        self.assertIn("Drop queue is clear", html)
        self.assertNotIn("<pre", html)


class TheSecondBeatSurvivesTests(unittest.TestCase):
    """The first drop's hook was one 132-character sentence with the joke at
    the end. It did not fit as a single card, so it was trimmed to fit -- and
    the trim cost the punchline. The operator spotted it in the email:
    "it's not really the full meme or punchline".

    `overlay_hook_line` could always render a second card timed to appear
    partway through; nothing carried one from the drop to it.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = self.root / "digest.json"

    def test_a_two_beat_drop_previews_one_frame_per_beat(self):
        """A single still shows half the joke -- and it is the half the
        operator already read in the caption."""
        entry = {"stem": "clip", "asset_path": "/t/c.mp4", "media_type": "video",
                 "on_screen_hook_text": "setup", "closing_line": "payoff",
                 "closing_at": 5.0, "lane": "meme"}
        grabbed = []

        def fake_grab(video, dest, at_seconds=0.0):
            grabbed.append(at_seconds)
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_bytes(b"\x89PNG\r\n\x1a\n")
            return {"status": "ready", "poster_path": str(dest)}

        with mock.patch("pathlib.Path.is_file", return_value=True), \
             mock.patch.object(drop_digest, "_downscale",
                               side_effect=lambda s, d: d), \
             mock.patch.dict(sys.modules):
            import helpers.image_lane_reel as ilr  # type: ignore
            import helpers.duck_video_helper as dvh  # type: ignore
            with mock.patch.object(ilr, "caption_and_poster",
                                   return_value={"status": "ready",
                                                 "video_path": "/t/burned.mp4",
                                                 "clip_seconds": 10.0}), \
                 mock.patch.object(dvh, "extract_poster_frame",
                                   side_effect=fake_grab):
                out = drop_digest.build_previews([entry],
                                                 out_dir=self.root / "p")
        self.assertEqual(len(out[0]["frames"]), 2, out[0])
        self.assertEqual([f["label"] for f in out[0]["frames"]],
                         ["first beat", "second beat"])
        self.assertLess(grabbed[0], 5.0, "first frame must precede the payoff")
        self.assertGreater(grabbed[1], 5.0, "second frame must follow it")

    def test_a_one_beat_drop_still_previews_once(self):
        entry = {"stem": "clip", "asset_path": "/t/c.mp4", "media_type": "video",
                 "on_screen_hook_text": "setup", "lane": "meme"}
        with mock.patch("pathlib.Path.is_file", return_value=True), \
             mock.patch.object(drop_digest, "_downscale",
                               side_effect=lambda s, d: d), \
             mock.patch.dict(sys.modules):
            import helpers.image_lane_reel as ilr  # type: ignore
            import helpers.duck_video_helper as dvh  # type: ignore
            with mock.patch.object(ilr, "caption_and_poster",
                                   return_value={"status": "ready",
                                                 "video_path": "/t/b.mp4",
                                                 "clip_seconds": 10.0}), \
                 mock.patch.object(dvh, "extract_poster_frame",
                                   return_value={"status": "ready",
                                                 "poster_path": "/t/f.png"}):
                out = drop_digest.build_previews([entry], out_dir=self.root / "p")
        self.assertEqual(len(out[0]["frames"]), 1)

    def test_the_email_says_a_still_is_a_still(self):
        """Approving a video off a photo is approving the thing you did not
        watch, so the email does not let that pass silently."""
        path = _ledger(self.root, {"clip": _entry()})
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=[]):
            d = drop_digest.decide(now=NOW, state_path=self.state)
        _s, _t, html = drop_digest.render_email(
            d, now=NOW, reply_to="ops@example.com",
            previews=[{"stem": "clip", "is_video": True,
                       "frames": [{"cid": "a", "path": "/t/a.jpg", "label": None}]}])
        self.assertIn("Still frame", html)
        self.assertIn("DuckDrop/accepted", html)

    def test_a_refused_caption_is_labelled_not_silently_different(self):
        """"I only saw one with the meme written on it" -- two drops carried a
        hook too long to fit, fell back to a bare frame, and looked like a
        glitch rather than a refusal."""
        path = _ledger(self.root, {"clip": _entry()})
        with mock.patch.object(drop_intake, "DROP_PROPOSALS_PATH", path), \
             mock.patch.object(drop_operator, "_unanswered_notes", return_value=[]):
            d = drop_digest.decide(now=NOW, state_path=self.state)
        _s, _t, html = drop_digest.render_email(
            d, now=NOW, reply_to="ops@example.com",
            previews=[{"stem": "clip", "is_video": True,
                       "note": "the hook does not fit the frame",
                       "frames": [{"cid": "a", "path": "/t/a.jpg",
                                   "label": "caption NOT applied"}]}])
        self.assertIn("does not fit the frame", html)
        self.assertIn("caption NOT applied", html)


class PreviewsAreBuiltSafelyTests(unittest.TestCase):
    def test_a_missing_asset_yields_a_note_not_an_exception(self):
        out = drop_digest.build_previews(
            [{"stem": "gone", "asset_path": "/nope.mp4", "media_type": "video"}])
        self.assertEqual(out[0]["frames"], [])
        self.assertIn("gone", out[0]["note"])

    def test_a_raising_frame_grab_does_not_take_the_email_down(self):
        with tempfile.TemporaryDirectory() as tmp:
            clip = Path(tmp) / "c.mp4"
            clip.write_bytes(b"\x00\x00\x00\x18ftypmp42")
            with mock.patch.dict(sys.modules):
                out = drop_digest.build_previews(
                    [{"stem": "c", "asset_path": str(clip),
                      "media_type": "video"}], out_dir=Path(tmp) / "out")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["stem"], "c")
        self.assertIsInstance(out[0]["frames"], list)

    def test_previews_are_downscaled_so_the_email_stays_small(self):
        self.assertLessEqual(drop_digest.PREVIEW_WIDTH, 600)


class TheDigestDoesNotLeakProductionEnvIntoTestsTests(unittest.TestCase):
    """Found by the full suite, not by this file.

    `_load_duckagent_env` ran unconditionally so that the reply-to address
    would resolve at render time. In a test process that pulled the whole
    production .env into os.environ: it flipped DUCK_EMAIL_DIGEST_MODE and
    broke six unrelated cadence tests, and it populated live SMTP credentials
    in a suite whose whole point is that it cannot reach anyone
    ([[feedback_test_runs_reach_the_outside_world]]).
    """

    def test_it_refuses_to_load_dotenv_under_test_mode(self):
        import os
        marker = "DUCK_EMAIL_DIGEST_MODE"
        os.environ["DUCK_TEST_MODE"] = "1"
        os.environ.pop(marker, None)
        drop_digest._load_duckagent_env()
        self.assertNotIn(marker, os.environ,
                         "production .env leaked into the test process")

    def test_the_guard_reads_the_same_flag_everything_else_does(self):
        self.assertEqual(drop_digest._TEST_MODE_ENV, "DUCK_TEST_MODE")


class TheDigestStateIsGuardedTests(unittest.TestCase):
    def test_a_test_run_cannot_write_the_live_digest_state(self):
        import os
        os.environ["DUCK_TEST_MODE"] = "1"
        with self.assertRaises(drop_digest.TestModeRefusalError):
            drop_digest._write_state({},
                                     path=drop_digest._FROZEN_PRODUCTION_STATE_PATH)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
