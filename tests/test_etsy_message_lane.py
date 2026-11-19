"""The Etsy MESSAGE lane switch, and the two directions it must fail in.

The whole point of this module is that "can we reply?" and "is the lane off?"
are different questions whose safe answer, when the config cannot be read, is
the OPPOSITE of each other. Getting them the same way round either posts an
unapproved reply to a customer or silently hides a queue of people waiting.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import etsy_message_lane as eml  # noqa: E402


class ReadingThePosture(unittest.TestCase):
    def test_both_on(self):
        p = eml.read_message_lane({"customer_read": {"enabled": True},
                                   "customer_message_reply": {"enabled": True}})
        self.assertTrue(p.reads_enabled)
        self.assertTrue(p.replies_enabled)
        self.assertFalse(p.off)

    def test_replies_off_reads_on(self):
        p = eml.read_message_lane({"customer_read": {"enabled": True},
                                   "customer_message_reply": {"enabled": False}})
        self.assertTrue(p.reads_enabled)
        self.assertFalse(p.replies_enabled)
        self.assertTrue(p.off)

    def test_the_reply_reason_wins_when_both_are_off(self):
        # When nobody is answering a customer, the reason that matters is why
        # we are not REPLYING, not why we stopped reading.
        p = eml.read_message_lane({
            "customer_read": {"enabled": False, "disabled_because": "reads reason"},
            "customer_message_reply": {"enabled": False, "disabled_because": "reply reason"},
        })
        self.assertEqual(p.note, "reply reason")

    def test_it_falls_back_to_the_read_reason_when_the_reply_section_has_none(self):
        p = eml.read_message_lane({
            "customer_read": {"enabled": False, "disabled_because": "reads reason"},
            "customer_message_reply": {"enabled": False},
        })
        self.assertEqual(p.note, "reads reason")

    def test_only_a_real_boolean_false_switches_a_lane_off(self):
        for value in ("false", "False", "off", 0, "", None, []):
            with self.subTest(value=value):
                p = eml.read_message_lane({"customer_message_reply": {"enabled": value}})
                self.assertFalse(p.off, "a typo is not a decision")


class TheTwoFailDirections(unittest.TestCase):
    """This class is the reason the module exists."""

    def setUp(self) -> None:
        self.ctx = TemporaryDirectory()
        self.dir = Path(self.ctx.name)

    def tearDown(self) -> None:
        self.ctx.cleanup()

    def _write(self, payload):
        (self.dir / eml.CONFIG_FILENAME).write_text(
            payload if isinstance(payload, str) else json.dumps(payload),
            encoding="utf-8")

    def test_a_missing_config_refuses_the_reply_but_does_not_park_the_queue(self):
        self.assertFalse(eml.replies_allowed(self.dir),
                         "an unreadable switch must never authorise a reply")
        self.assertFalse(eml.lane_is_off(self.dir),
                         "an unreadable switch must never park a live backlog")

    def test_a_malformed_config_fails_the_same_two_ways(self):
        self._write("{not json")
        self.assertFalse(eml.replies_allowed(self.dir))
        self.assertFalse(eml.lane_is_off(self.dir))

    def test_a_config_with_no_reply_section_still_refuses_the_reply(self):
        # Absence is not permission. The previous shape of this config had no
        # reply switch at all, and that must not read as "yes".
        self._write({"customer_read": {"enabled": True}})
        self.assertFalse(eml.replies_allowed(self.dir))

    def test_only_an_explicit_true_allows_a_reply(self):
        for value, allowed in ((True, True), (False, False), ("true", False),
                               (1, False), (None, False)):
            with self.subTest(value=value):
                self._write({"customer_message_reply": {"enabled": value}})
                self.assertIs(eml.replies_allowed(self.dir), allowed)

    def test_the_committed_config_refuses_replies_and_reports_off(self):
        # Pins the operator's actual decision, read through the real file.
        self.assertFalse(eml.replies_allowed())
        self.assertTrue(eml.lane_is_off())


class TheReviewLaneIsUntouched(unittest.TestCase):
    """The operator kept Etsy REVIEW replies on. Nothing here may read, write
    or imply anything about that lane."""

    def test_no_review_lane_switch_can_change_the_message_posture(self):
        off = {"customer_message_reply": {"enabled": False}}
        for review in (True, False, None, "anything"):
            with self.subTest(review=review):
                posture = eml.read_message_lane({**off, "review_reply": {"enabled": review}})
                self.assertTrue(posture.off)
                self.assertFalse(posture.replies_enabled)
        on = {"customer_message_reply": {"enabled": True}, "customer_read": {"enabled": True}}
        for review in (True, False):
            with self.subTest(review=review, lane="on"):
                posture = eml.read_message_lane({**on, "review_reply": {"enabled": review}})
                self.assertFalse(posture.off, "the review switch must not leak in")

    def test_the_committed_config_still_has_review_replies_enabled(self):
        config = json.loads(
            (RUNTIME_DIR.parent / "config" / eml.CONFIG_FILENAME).read_text(encoding="utf-8"))
        self.assertIs(config["review_reply"]["enabled"], True,
                      "the operator asked to keep the review lane replying")

    def test_switching_messages_off_does_not_change_the_review_section(self):
        posture = eml.read_message_lane({
            "customer_message_reply": {"enabled": False},
            "review_reply": {"enabled": True},
        })
        self.assertTrue(posture.off)
        # Nothing in the posture can express an opinion about review replies.
        self.assertNotIn("review", str(posture).lower())


class WhatTheOperatorSees(unittest.TestCase):
    def test_a_running_lane_adds_no_noise(self):
        p = eml.read_message_lane({"customer_read": {"enabled": True},
                                   "customer_message_reply": {"enabled": True}})
        self.assertIsNone(eml.operator_surface_phrase(p))

    def test_an_off_lane_says_so_and_names_the_review_lane_as_safe(self):
        p = eml.read_message_lane({
            "customer_read": {"enabled": False},
            "customer_message_reply": {"enabled": False,
                                       "disabled_because": "Grok bot answers these. Long tail here."},
        })
        phrase = eml.operator_surface_phrase(p)
        self.assertIn("off by operator decision", phrase)
        self.assertIn("Grok bot answers these", phrase)
        self.assertNotIn("Long tail here", phrase, "surfaces take the first sentence only")
        self.assertIn("review replies are unaffected", phrase)

    def test_it_names_which_half_is_off(self):
        reads_only = eml.read_message_lane({"customer_read": {"enabled": False},
                                            "customer_message_reply": {"enabled": True}})
        replies_only = eml.read_message_lane({"customer_read": {"enabled": True},
                                              "customer_message_reply": {"enabled": False}})
        def lead(posture):
            # Everything before the standing "Etsy review replies are
            # unaffected" sentence, which legitimately contains "replies".
            return eml.operator_surface_phrase(posture).split(". Etsy review")[0]

        self.assertIn("inbox reads", lead(reads_only))
        self.assertNotIn("replies", lead(reads_only))
        self.assertIn("replies", lead(replies_only))
        self.assertNotIn("inbox reads", lead(replies_only))

    def test_the_refusal_names_the_switch_and_the_way_back(self):
        p = eml.read_message_lane({"customer_message_reply": {
            "enabled": False, "disabled_because": "why", "resume_hint": "flip it back"}})
        msg = eml.refusal_message(p)
        self.assertIn("did not stage anything", msg)
        self.assertIn("why", msg)
        self.assertIn("customer_message_reply.enabled", msg)
        self.assertIn("flip it back", msg)
        self.assertIn("REVIEW replies are a separate lane", msg)

    def test_a_refusal_we_could_not_explain_says_that_instead_of_inventing_one(self):
        unknown = eml.MessageLanePosture(True, True, False, None, None, known=False)
        msg = eml.refusal_message(unknown)
        self.assertIn("could not read", msg)
        self.assertIn("refusing rather than guessing", msg)


if __name__ == "__main__":
    unittest.main()
