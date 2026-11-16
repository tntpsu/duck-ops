"""Surface 134 -- the return channel, so the two agents can correct each other.

Until this, the contract was one-way. We published a README and the agent
published files, and the only thing it could ever learn was that ONE file was
refused -- by finding a .reason.txt it had to know to look for. It could not
ask what a field meant, say a rule was ambiguous, or find out whether anything
it sent was ever posted. So the same misunderstanding could repeat weekly with
nobody able to name it, which is exactly how Gemini's first drop arrived
malformed and nobody noticed for days.

Three channels close it: `status.json` (what happened to everything),
`feedback/` (agent -> us) and `answers/` (us -> agent).
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402

NOW = datetime(2026, 11, 16, 12, 0).astimezone()


def _entry(**kw):
    base = {"lane": "meme", "approval": "operator_required", "claimed_by": None,
            "post_ready": True, "received_at": NOW.isoformat()}
    base.update(kw)
    return base


class StatusTellsTheAgentWhatHappenedTests(unittest.TestCase):
    """The half of the loop that was missing entirely."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = drop_intake.init_drop_root(Path(self.tmp.name) / "DuckDrop")

    def _write(self, proposals, refused=None):
        drop_intake.write_status_file(self.root, proposals,
                                      refused=refused, now=NOW)
        return json.loads((self.root / drop_intake.STATUS_FILE).read_text())

    def test_every_outcome_has_a_state_the_agent_can_act_on(self):
        cases = {
            "a": (_entry(), "waiting_for_a_human"),
            "b": (_entry(approval="approved"), "approved"),
            "c": (_entry(claimed_by="meme::r1"), "published"),
            "d": (_entry(approval="declined"), "declined"),
            "e": (_entry(post_ready=False), "not_ready"),
        }
        out = self._write({k: v[0] for k, v in cases.items()})
        for stem, (_entry_, expected) in cases.items():
            with self.subTest(stem=stem):
                self.assertEqual(out["drops"][stem]["state"], expected)
                self.assertTrue(out["drops"][stem]["detail"],
                                "a state with no explanation is not actionable")

    def test_a_claim_outranks_an_unapproved_flag(self):
        # If a run took it, it is published whatever the approval field says.
        out = self._write({"x": _entry(approval="operator_required",
                                       claimed_by="meme::r9")})
        self.assertEqual(out["drops"]["x"]["state"], "published")

    def test_refusals_from_this_scan_are_named(self):
        out = self._write({}, refused=[{"asset": "bad.mp4",
                                        "reason": "the sidecar json does not parse"}])
        self.assertEqual(out["refused_this_scan"][0]["asset"], "bad.mp4")
        self.assertIn("parse", out["refused_this_scan"][0]["reason"])

    def test_it_says_where_to_look_when_a_file_is_absent(self):
        """Absence is the confusing case: refused before becoming a proposal,
        so it appears nowhere. Silence would read as 'lost'."""
        out = self._write({})
        self.assertIn(drop_intake.REJECTED_DIR, out["doc"])
        self.assertIn("reason", out["doc"])

    def test_it_lists_the_lanes_that_are_open(self):
        out = self._write({})
        self.assertEqual(out["lanes_open_for_drops"],
                         sorted(drop_intake.consuming_lanes()))

    def test_it_is_written_even_when_nothing_changed(self):
        """"Nothing changed" is an answer. An agent with no answer assumes the
        worst or repeats itself."""
        out = self._write({})
        self.assertEqual(out["drops"], {})
        self.assertTrue(out["generated_at"])


class TheAgentCanTalkBackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = drop_intake.init_drop_root(Path(self.tmp.name) / "DuckDrop")
        self.ledger = Path(self.tmp.name) / "feedback.json"

    def _note(self, name: str, body: str):
        (self.root / drop_intake.FEEDBACK_DIR / name).write_text(
            body, encoding="utf-8")

    def test_a_json_note_is_read_structurally(self):
        self._note("q1.json", json.dumps({
            "from": "gemini", "subject": "hook length",
            "about": "meme", "body": "Is 80 characters a hard limit?"}))
        notes = drop_intake.collect_feedback(self.root, now=NOW)
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["from"], "gemini")
        self.assertEqual(notes[0]["subject"], "hook length")
        self.assertTrue(notes[0]["parsed"])

    def test_prose_is_accepted_too(self):
        self._note("thought.md", "# The caption rule is ambiguous\n\nWhich?")
        notes = drop_intake.collect_feedback(self.root, now=NOW)
        self.assertEqual(len(notes), 1)
        self.assertIn("ambiguous", notes[0]["body"])
        self.assertFalse(notes[0]["parsed"])

    def test_unparseable_json_is_surfaced_not_refused(self):
        """A refused drop is safe; a refused question is a channel nobody uses
        twice. And a note we cannot parse is itself a message about the
        contract."""
        self._note("broken.json", "{not json at all")
        notes = drop_intake.collect_feedback(self.root, now=NOW)
        self.assertEqual(len(notes), 1)
        self.assertIn("not json", notes[0]["body"])

    def test_a_read_note_is_filed_and_not_collected_twice(self):
        self._note("q.md", "hello")
        self.assertEqual(len(drop_intake.collect_feedback(self.root, now=NOW)), 1)
        self.assertEqual(drop_intake.collect_feedback(self.root, now=NOW), [])
        self.assertTrue((self.root / drop_intake.FEEDBACK_READ_DIR / "q.md").is_file())

    def test_an_unknown_extension_is_left_alone(self):
        (self.root / drop_intake.FEEDBACK_DIR / "clip.mp4").write_bytes(b"\x00")
        self.assertEqual(drop_intake.collect_feedback(self.root, now=NOW), [])

    def test_an_oversized_note_does_not_block_the_scan(self):
        self._note("huge.txt", "x" * (drop_intake.MAX_FEEDBACK_BYTES + 10))
        notes = drop_intake.collect_feedback(self.root, now=NOW)
        self.assertEqual(len(notes), 1)
        self.assertIn("too large", notes[0]["body"])


class AnswersReachTheAgentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = drop_intake.init_drop_root(Path(self.tmp.name) / "DuckDrop")
        self.ledger = Path(self.tmp.name) / "feedback.json"
        (self.root / drop_intake.FEEDBACK_DIR / "q.json").write_text(
            json.dumps({"from": "gemini", "subject": "hook length",
                        "body": "hard limit?"}), encoding="utf-8")
        drop_intake.record_feedback(
            drop_intake.collect_feedback(self.root, now=NOW),
            ledger_path=self.ledger)

    def test_the_note_survives_in_a_durable_ledger(self):
        payload = json.loads(self.ledger.read_text())
        self.assertEqual(payload["note_count"], 1)
        self.assertIsNone(payload["notes"][0]["answered_at"])

    def test_an_answer_is_published_where_the_agent_will_see_it(self):
        """Recording it only in our ledger would be the same one-way failure
        this channel exists to fix."""
        drop_intake.answer_feedback(
            "q.json", "120 is hard; ~80 is what fits.",
            drop_root=self.root, ledger_path=self.ledger)
        reply = self.root / drop_intake.ANSWERS_DIR / "q.answer.md"
        self.assertTrue(reply.is_file())
        text = reply.read_text()
        self.assertIn("hook length", text)
        self.assertIn("~80 is what fits", text)

    def test_the_ledger_records_that_it_was_answered(self):
        drop_intake.answer_feedback("q.json", "yes",
                                    drop_root=self.root, ledger_path=self.ledger)
        payload = json.loads(self.ledger.read_text())
        self.assertTrue(payload["notes"][0]["answered_at"])
        self.assertEqual(payload["notes"][0]["answer"], "yes")

    def test_answering_an_unknown_note_is_refused(self):
        with self.assertRaises(drop_intake.DropRefused):
            drop_intake.answer_feedback("nope.json", "hi",
                                        drop_root=self.root,
                                        ledger_path=self.ledger)

    def test_an_older_note_is_never_dropped_by_a_later_one(self):
        (self.root / drop_intake.FEEDBACK_DIR / "q2.md").write_text(
            "second", encoding="utf-8")
        drop_intake.record_feedback(
            drop_intake.collect_feedback(self.root, now=NOW + timedelta(hours=1)),
            ledger_path=self.ledger)
        payload = json.loads(self.ledger.read_text())
        self.assertEqual(payload["note_count"], 2)


class TheReadmeExplainsTheLoopTests(unittest.TestCase):
    def test_it_tells_the_agent_to_read_status_first(self):
        import re
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "D")
            text = re.sub(r"\s+", " ", (root / "README.md").read_text())
        for token in (drop_intake.STATUS_FILE, drop_intake.FEEDBACK_DIR,
                      drop_intake.ANSWERS_DIR, "waiting_for_a_human",
                      "not_ready"):
            with self.subTest(token=token):
                self.assertIn(token, text)

    def test_it_invites_disagreement_rather_than_only_questions(self):
        """A channel that only takes questions gets questions. The useful
        feedback is 'this rule is wrong'."""
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "D")
            text = (root / "README.md").read_text().lower()
        self.assertIn("ambiguous", text)
        self.assertIn("a rule here is wrong", text)

    def test_the_folders_it_names_actually_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "D")
            for sub in (drop_intake.FEEDBACK_DIR, drop_intake.ANSWERS_DIR,
                        drop_intake.FEEDBACK_READ_DIR):
                with self.subTest(sub=sub):
                    self.assertTrue((root / sub).is_dir())


class TestModeGuardTests(unittest.TestCase):
    def test_the_live_feedback_ledger_refuses_a_test_write(self):
        """Same three-layer treatment as the proposals ledger: a fixture note
        in the live file would read as an unanswered question from a real
        agent."""
        os.environ["DUCK_TEST_MODE"] = "1"
        with self.assertRaises(drop_intake.TestModeRefusalError):
            drop_intake.record_feedback(
                [{"file": "x.md", "received_at": NOW.isoformat()}],
                ledger_path=drop_intake._FROZEN_PRODUCTION_FEEDBACK_PATH)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
