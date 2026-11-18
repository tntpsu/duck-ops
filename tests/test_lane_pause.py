"""The one interpretation of "is this lane intentionally off?"

Four files answered this question four different ways on 2026-11-18, and a
fifth never asked. `runtime/lane_pause.py` is the single reader; these tests
hold its contract and, at the bottom, guard against a sixth copy appearing.

The tests that matter most are the ones about the DISPLAY/BEHAVIOUR split,
because collapsing it is the tempting simplification and it is wrong: three
pre-existing tests broke when a mail-path refusal was first keyed on `mode`
alone.
"""
from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runtime"))

from lane_pause import OFF_MODE, LanePause, pause_note, read_lane_pause  # noqa: E402


class TheTwoVerdictsAreDistinctTests(unittest.TestCase):
    """`display_paused` may SAY a lane is idle; `operator_paused` may REFUSE
    an operator's action. One is weaker on purpose."""

    def test_mode_off_is_enough_to_say_a_lane_is_not_running(self):
        v = read_lane_pause({"mode": "off"})
        self.assertTrue(v.display_paused)

    def test_mode_off_alone_is_NOT_enough_to_refuse_an_operator_action(self):
        """The whole reason the split exists. review_carousel, gtdf and blog
        were all `off` on 2026-11-18 for unrelated reasons, and
        review_carousel is off as a REACH decision while its portal Reject
        button must keep working. A refusal keyed on `mode` broke that
        button for three lanes nobody had asked about."""
        v = read_lane_pause({"mode": "off"})
        self.assertFalse(v.operator_paused)

    def test_both_fields_together_refuse(self):
        v = read_lane_pause({"mode": "off", "operator_paused": True})
        self.assertTrue(v.operator_paused)
        self.assertTrue(v.display_paused)

    def test_the_flag_without_off_mode_refuses_nothing(self):
        """A lane still running is not paused however the flag reads. The
        two must agree, so a stale flag left behind after a resume cannot
        deadlock a live lane."""
        v = read_lane_pause({"mode": "auto_schedule_meta", "operator_paused": True})
        self.assertFalse(v.operator_paused)
        self.assertFalse(v.display_paused)

    def test_operator_paused_implies_display_paused_for_every_input(self):
        """A lane strong enough to refuse an action must also read as idle;
        the reverse does not hold. Asserted over the cross-product rather
        than one example, because this is the invariant the split rests on."""
        for mode in ("off", "OFF", " off ", "manual", "auto_schedule_meta", "", None):
            for flag in (True, False, None, "yes", 0):
                v = read_lane_pause({"mode": mode, "operator_paused": flag})
                with self.subTest(mode=mode, flag=flag):
                    if v.operator_paused:
                        self.assertTrue(v.display_paused)


class ModeNormalisationTests(unittest.TestCase):
    def test_case_and_whitespace_do_not_decide_a_lane_posture(self):
        for raw in ("off", "OFF", " Off ", "oFF\n"):
            with self.subTest(raw=raw):
                self.assertTrue(read_lane_pause({"mode": raw}).display_paused)

    def test_a_mode_that_merely_contains_off_is_not_off(self):
        """Substring matching here would catch a future `off_peak` or
        `handoff` mode and silently stop grading a live lane."""
        for raw in ("off_peak", "handoff", "offline", "kickoff"):
            with self.subTest(raw=raw):
                self.assertFalse(read_lane_pause({"mode": raw}).display_paused)

    def test_the_off_constant_is_what_the_module_compares_against(self):
        self.assertEqual(OFF_MODE, "off")


class UnreadableConfigFailsOpenTests(unittest.TestCase):
    """Three of the four callers chose fail-open deliberately: a config we
    could not read is not evidence of a pause, and treating it as one would
    deadlock a lane on a typo."""

    def test_none_is_not_a_pause(self):
        v = read_lane_pause(None)
        self.assertFalse(v.display_paused)
        self.assertFalse(v.operator_paused)
        self.assertEqual(v.mode, "")

    def test_a_json_list_is_not_a_pause(self):
        self.assertFalse(read_lane_pause([{"mode": "off"}]).display_paused)

    def test_an_empty_dict_is_not_a_pause(self):
        self.assertFalse(read_lane_pause({}).display_paused)

    def test_it_returns_the_declared_type_even_for_junk(self):
        """Callers read attributes off the result unconditionally; a None
        return would turn a bad config into an AttributeError inside a card
        whose contract is never to raise."""
        for junk in (None, 7, "off", [], set()):
            with self.subTest(junk=junk):
                self.assertIsInstance(read_lane_pause(junk), LanePause)


class NoteDerivationTests(unittest.TestCase):
    """The genuinely duplicated part. TWO record shapes exist because the two
    pauses were recorded by different hands, and both are read rather than
    one being migrated -- rewriting a record of what the operator decided is
    not a display fix."""

    def test_the_explicit_pair_reads(self):
        note = pause_note({"paused_because": "reworking the lane",
                           "paused_on": "2026-11-18"})
        self.assertEqual(note, "Paused by the operator on 2026-11-18: reworking the lane")

    def test_the_history_shape_reads(self):
        note = pause_note({"operator_mode_history": [
            {"to": "off", "reason": "no one looks at the review carousels",
             "at": "2026-11-13T09:12:00"}]})
        self.assertEqual(
            note,
            "Paused by the operator on 2026-11-13: no one looks at the review carousels")

    def test_the_explicit_pair_wins_over_the_history(self):
        note = pause_note({
            "paused_because": "reworking", "paused_on": "2026-11-18",
            "operator_mode_history": [{"to": "off", "reason": "older", "at": "2026-01-01"}]})
        self.assertIn("reworking", note)
        self.assertNotIn("older", note)

    def test_only_the_LAST_history_entry_counts_and_only_if_it_turned_the_lane_off(self):
        """An older off->on entry is not a reason the lane is off now. A
        history walked without this check reports a resume as a pause."""
        note = pause_note({"operator_mode_history": [
            {"to": "off", "reason": "paused in March", "at": "2026-03-01"},
            {"to": "auto_schedule_meta", "reason": "resumed", "at": "2026-04-01"}]})
        self.assertIsNone(note)

    def test_a_reason_without_a_date_still_reads(self):
        self.assertEqual(pause_note({"paused_because": "reworking"}),
                         "Paused by the operator: reworking")

    def test_no_reason_anywhere_is_None_not_a_filler_sentence(self):
        """A card that prints "Paused by the operator: " with nothing after
        it has told the operator less than saying nothing would."""
        for payload in ({}, {"mode": "off"}, {"paused_on": "2026-11-18"},
                        {"paused_because": "   "}, {"operator_mode_history": []},
                        {"operator_mode_history": [{"to": "off"}]}, None, "off"):
            with self.subTest(payload=payload):
                self.assertIsNone(pause_note(payload))

    def test_the_note_rides_along_on_the_verdict(self):
        v = read_lane_pause({"mode": "off", "paused_because": "x", "paused_on": "2026-11-18"})
        self.assertEqual(v.note, pause_note({"paused_because": "x", "paused_on": "2026-11-18"}))

    def test_resume_hint_is_the_operators_own_instructions(self):
        v = read_lane_pause({"mode": "off", "to_resume": "flip mode back and reload the job"})
        self.assertEqual(v.resume_hint, "flip mode back and reload the job")
        self.assertIsNone(read_lane_pause({"mode": "off"}).resume_hint)


class TheRealConfigsStillGradeAsTheyDidTests(unittest.TestCase):
    """Reads the operator's live configs. Asserts the RULE each lane's
    fields imply, not today's values -- the lanes' modes are the operator's
    to change, and a test pinning them defends against the operator
    ([[feedback_pin_the_rule_not_the_reading]]).
    """

    CONFIG_DIR = REPO_ROOT.parent / "duckAgent" / "config"

    def test_every_lane_config_is_readable_and_self_consistent(self):
        import json
        paths = sorted(self.CONFIG_DIR.glob("*_execution.json"))
        if not paths:
            self.skipTest("duckAgent checkout not present")
        for path in paths:
            with self.subTest(lane=path.stem):
                payload = json.loads(path.read_text())
                v = read_lane_pause(payload)
                # The invariant, not the value.
                if v.operator_paused:
                    self.assertTrue(v.display_paused)
                    self.assertIsNotNone(
                        v.note,
                        f"{path.name} refuses operator actions but records no reason; "
                        "the operator cannot tell why their reply did nothing")
                self.assertEqual(
                    v.display_paused,
                    str(payload.get("mode") or "").strip().lower() == "off")


class NoSixthCopyTests(unittest.TestCase):
    """The anti-rot guard, and the reason this is a structural fix rather
    than a fifth patch.

    Each of the four inline readings was written because the previous one
    was not reachable from where the next card lived. Nothing stopped a
    fifth, and nothing would stop a sixth. This test does: a new inline
    `mode == "off"` comparison in a graded surface has to either route
    through `lane_pause` or be added here with a reason.
    """

    #: Files allowed to compare `mode` to "off" directly. Each one is a
    #: degradation fallback for when duck-ops is unreachable, documented at
    #: the call site -- a single comparison, not a second interpretation.
    #: This list only shrinks.
    ALLOWED = {
        "lane_pause.py",            # the canonical reader itself
        "viewer.py",                # _read_lane_pause ImportError fallback
        "main_agent.py",            # _flow_is_paused_by_operator fallback
        "workflows_card.py",        # get_flow_pause ImportError fallback
        "business_operator_desk.py",  # operator_mode_history writer, not a reader
        # A DIFFERENT "off": an email CADENCE override, not a lane posture
        # (`effective == "off"` where effective is a send frequency). It
        # has its own anomaly-bypass semantics and nothing to do with
        # whether a lane runs. Listed so the distinction is recorded rather
        # than rediscovered.
        "email_cadence_gate.py",
    }

    SEARCH_DIRS = (
        REPO_ROOT / "runtime",
        REPO_ROOT.parent / "duckAgent" / "src",
        REPO_ROOT.parent / "duckAgent" / "helpers",
        REPO_ROOT.parent / "duckAgent" / "creative_agent" / "runtime" / "src",
    )

    def _inline_off_comparisons(self, path: Path) -> list[int]:
        """`<something> == "off"` or `"off" == <something>`, by AST -- a grep
        misses the form where the comparison sits on a continuation line,
        which is where it sat in two of the four files."""
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError):
            return []
        hits = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Compare):
                continue
            if not any(isinstance(op, ast.Eq) for op in node.ops):
                continue
            operands = [node.left, *node.comparators]
            if any(isinstance(o, ast.Constant) and o.value == "off" for o in operands):
                hits.append(node.lineno)
        return hits

    def test_no_new_file_interprets_mode_off_for_itself(self):
        offenders = []
        scanned = 0
        for root in self.SEARCH_DIRS:
            if not root.is_dir():
                continue
            for path in sorted(root.rglob("*.py")):
                if "test" in path.name or path.name in self.ALLOWED:
                    continue
                scanned += 1
                for line in self._inline_off_comparisons(path):
                    offenders.append(f"{path.name}:{line}")
        # A scan that opened nothing would agree with itself forever
        # ([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).
        self.assertGreater(scanned, 20, "the scan opened almost nothing")
        self.assertEqual(
            offenders, [],
            "a lane posture is being interpreted outside lane_pause.py -- that is "
            "how four copies became five:\n  " + "\n  ".join(offenders))

    def test_the_scan_can_actually_see_one(self):
        """Positive control for the AST walker, including the continuation-line
        form a grep misses."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "probe.py"
            f.write_text('x = (\n    str(cfg.get("mode") or "").strip().lower()\n    == "off"\n)\n')
            self.assertTrue(self._inline_off_comparisons(f))

    def test_the_allowlist_only_names_files_that_exist(self):
        """A stale entry silently exempts whatever takes that filename next.
        Judged only over roots this runner could open -- duckAgent may be
        absent in duck-ops CI
        ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]])."""
        present = set()
        opened_roots = 0
        for root in self.SEARCH_DIRS:
            if not root.is_dir():
                continue
            opened_roots += 1
            present.update(p.name for p in root.rglob("*.py"))
        self.assertTrue(opened_roots, "no search root exists")
        unjudgeable = {"viewer.py", "main_agent.py"} - present
        stale = self.ALLOWED - present - unjudgeable
        self.assertEqual(stale, set(), "allowlist entries that name no file")


if __name__ == "__main__":
    unittest.main()
