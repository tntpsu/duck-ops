"""Stuck receipts vs parked receipts.

The whole value is in the DECLARED policy, so most of these tests are about
the classification rather than the arithmetic. Three in particular pin
mistakes made while writing it:

  - `blocked` fell into "unclassified" and made that bucket 101 items of
    noise, which is how a signal meaning "I do not know" gets ignored.
  - every receipt was graded individually, so 127 healthy daily deferral
    records read as 127 overdue items and buried the 48 real ones.
  - a paused lane's receipts were graded, which is the bug class the whole
    day was spent removing.
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runtime"))

import receipt_lifecycle as rl  # noqa: E402

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)


def _receipt(lane, state, reason, *, days_old=1.0, workflow_id="w"):
    return {
        "lane": lane,
        "state": state,
        "state_reason": reason,
        "workflow_id": workflow_id,
        "updated_at": (NOW - timedelta(days=days_old)).isoformat(),
    }


def _classify(receipt, **kw):
    return rl.classify_receipt(receipt, now=NOW, **kw)


class TerminalStatesAreNotGradedTests(unittest.TestCase):
    def test_a_universally_terminal_state_is_dropped_entirely(self):
        for state in sorted(rl.UNIVERSALLY_TERMINAL):
            with self.subTest(state=state):
                self.assertIsNone(
                    _classify(_receipt("newduck", state, "draft_ready", days_old=900)))

    def test_skipped_off_is_terminal_because_the_operator_caused_it(self):
        """21 gtdf + 22 gtdf_winner receipts carry it. A paused lane writing a
        receipt that says "I was skipped because you paused me" is the system
        working, and grading it would re-create today's bug in a new place."""
        self.assertIn("skipped_off", rl.UNIVERSALLY_TERMINAL)

    def test_blocked_is_NOT_terminal_but_is_NOT_windowed_either(self):
        """It needs action, so it is not dropped; it is owned by
        `build_workflow_followthrough_items(include_all_blocked=True)`, so it
        is not graded here. Two cards for one problem is worse than one."""
        self.assertNotIn("blocked", rl.UNIVERSALLY_TERMINAL)
        v = _classify(_receipt("meme", "blocked", "execution_failed", days_old=400))
        self.assertEqual(v.kind, rl.BLOCKED)
        self.assertFalse(v.overdue)

    def test_a_blocked_receipt_is_not_reported_as_unclassified(self):
        """The regression. All 101 live blocked receipts landed in
        "unclassified" on the first pass."""
        v = _classify(_receipt("etsy_token_auth", "blocked", "auth_blocked"))
        self.assertNotEqual(v.kind, rl.UNCLASSIFIED)


class ParkedVsPendingTests(unittest.TestCase):
    def test_an_observation_lane_is_parked_however_old(self):
        v = _classify(_receipt("competitor", "observed", "snapshot_collected",
                               days_old=500))
        self.assertEqual(v.kind, rl.PARKED)
        self.assertFalse(v.overdue)

    def test_a_production_draft_is_pending_and_goes_overdue(self):
        fresh = _classify(_receipt("newduck", "observed", "draft_ready", days_old=5))
        stale = _classify(_receipt("newduck", "observed", "draft_ready", days_old=213))
        self.assertEqual(fresh.kind, rl.PENDING)
        self.assertFalse(fresh.overdue)
        self.assertTrue(stale.overdue)

    def test_the_SAME_state_means_different_things_in_different_lanes(self):
        """`observed` is the whole reason this policy is data and not a rule.
        competitor/snapshot_collected and newduck/draft_ready are both
        `observed`; one is finished, one is waiting on a human."""
        parked = _classify(_receipt("competitor", "observed", "snapshot_collected",
                                    days_old=300))
        pending = _classify(_receipt("newduck", "observed", "draft_ready",
                                     days_old=300))
        self.assertEqual(parked.state, pending.state)
        self.assertEqual(parked.kind, rl.PARKED)
        self.assertEqual(pending.kind, rl.PENDING)

    def test_exactly_at_the_window_is_not_yet_overdue(self):
        v = _classify(_receipt("meme", "observed", "draft_ready", days_old=14.0))
        self.assertEqual(v.window_days, 14)
        self.assertFalse(v.overdue)

    def test_a_receipt_with_no_timestamp_is_not_overdue(self):
        """Unknown age is not evidence of staleness."""
        receipt = _receipt("newduck", "observed", "draft_ready")
        receipt["updated_at"] = ""
        v = _classify(receipt)
        self.assertIsNone(v.age_days)
        self.assertFalse(v.overdue)


class UndeclaredPairsAreLoudTests(unittest.TestCase):
    def test_an_unknown_pair_is_unclassified_not_guessed(self):
        v = _classify(_receipt("a_new_lane", "observed", "a_new_reason",
                               days_old=400))
        self.assertEqual(v.kind, rl.UNCLASSIFIED)
        self.assertFalse(v.overdue, "an undeclared pair must not be graded as stuck")

    def test_an_unknown_pair_is_also_not_silently_parked(self):
        """Either default would be a confident wrong answer. The point of the
        third kind is that the operator gets asked."""
        v = _classify(_receipt("a_new_lane", "observed", "a_new_reason"))
        self.assertNotEqual(v.kind, rl.PARKED)
        self.assertNotEqual(v.kind, rl.PENDING)

    def test_every_policy_entry_declares_a_basis(self):
        """A window with no stated basis is a number someone picked at speed,
        and the next person cannot tell it from a measured one."""
        for key, policy in rl.POLICY.items():
            with self.subTest(pair=key):
                self.assertTrue(policy.basis.strip(), f"{key} has no basis")

    def test_every_pending_entry_has_a_positive_window(self):
        for key, policy in rl.POLICY.items():
            if policy.kind != rl.PENDING:
                continue
            with self.subTest(pair=key):
                self.assertIsInstance(policy.window_days, int)
                self.assertGreater(policy.window_days, 0)

    def test_a_parked_entry_declares_no_window(self):
        for key, policy in rl.POLICY.items():
            if policy.kind == rl.PARKED:
                with self.subTest(pair=key):
                    self.assertIsNone(policy.window_days)


class PausedLanesAreNotGradedTests(unittest.TestCase):
    def test_a_paused_lane_receipt_is_never_overdue(self):
        v = _classify(_receipt("blog", "observed", "draft_article_ready",
                               days_old=199),
                      paused_lanes={"blog"})
        self.assertEqual(v.kind, rl.PENDING)
        self.assertTrue(v.paused)
        self.assertFalse(v.overdue)

    def test_the_same_receipt_IS_overdue_when_the_lane_runs(self):
        """The mutation partner. A paused check that always fired would pass
        the test above and disarm the card."""
        v = _classify(_receipt("blog", "observed", "draft_article_ready",
                               days_old=199))
        self.assertTrue(v.overdue)


class PerRunPairsCollapseToTheirNewestTests(unittest.TestCase):
    """The fix that took the overdue count from 185 to 48 without hiding
    anything real."""

    def test_a_live_per_run_log_reports_nothing(self):
        """148 profit deferral receipts, newest 0 days old: the Monday digest
        is carrying the handoff and the backlog behind it is a log."""
        verdicts = [
            _classify(_receipt("profit", "observed", "weekly_operator_email_deferred",
                               days_old=age))
            for age in (0.2, 30, 90, 179)
        ]
        self.assertEqual(rl.overdue_items(verdicts), [])

    def test_a_dead_per_run_log_reports_exactly_one_item(self):
        """A per-run log whose NEWEST entry is past the window is one finding,
        not one per entry ([[feedback_fold_defer_is_a_handoff_contract]]).

        Written against the profit pair with constructed ages, deliberately.
        The first version used the competitor deferral because its newest was
        167 days old -- and that reading was WRONG: the competitor report
        sends directly now and arrived three days before this was written, so
        those receipts are a retired path and are classified PARKED. A test
        pinned to that reading defended a false alarm
        ([[feedback_pin_the_rule_not_the_reading]])."""
        verdicts = [
            _classify(_receipt("profit", "observed",
                               "weekly_operator_email_deferred", days_old=age))
            for age in (40, 120, 180)
        ]
        overdue = rl.overdue_items(verdicts)
        self.assertEqual(len(overdue), 1)
        self.assertAlmostEqual(overdue[0].age_days, 40, places=0)

    def test_a_retired_path_is_parked_not_reported_as_a_dead_handoff(self):
        """The correction itself, as a regression. The competitor deferral
        receipts stop at 167 days because the report moved to a direct weekly
        send, not because a handoff died -- verified by reading the delivered
        mail, which is the only thing that settles it."""
        verdicts = [
            _classify(_receipt("competitor", "observed",
                               "weekly_operator_email_deferred", days_old=age))
            for age in (167, 175, 183)
        ]
        for v in verdicts:
            self.assertEqual(v.kind, rl.PARKED)
        self.assertEqual(rl.overdue_items(verdicts), [])

    def test_per_entity_receipts_are_each_counted(self):
        """14 stuck newduck drafts are 14 distinct listings. Collapsing these
        to the newest would hide 13 of them -- the opposite failure."""
        verdicts = [
            _classify(_receipt("newduck", "observed", "draft_ready",
                               days_old=age, workflow_id=f"nd-{age}"))
            for age in (197, 203, 213)
        ]
        self.assertEqual(len(rl.overdue_items(verdicts)), 3)

    def test_the_two_cardinalities_are_both_actually_used(self):
        """A policy where every entry shared one cardinality would make the
        distinction decorative."""
        kinds = {p.cardinality for p in rl.POLICY.values() if p.kind == rl.PENDING}
        self.assertEqual(kinds, {rl.PER_ENTITY, rl.PER_RUN})

    def test_results_are_sorted_oldest_first(self):
        verdicts = [
            _classify(_receipt("newduck", "observed", "draft_ready",
                               days_old=age, workflow_id=f"nd-{age}"))
            for age in (40, 213, 100)
        ]
        ages = [v.age_days for v in rl.overdue_items(verdicts)]
        self.assertEqual(ages, sorted(ages, reverse=True))

    def test_non_pending_kinds_never_reach_the_overdue_list(self):
        verdicts = [
            _classify(_receipt("competitor", "observed", "snapshot_collected",
                               days_old=400)),
            _classify(_receipt("meme", "blocked", "execution_failed", days_old=400)),
            _classify(_receipt("x", "observed", "y", days_old=400)),
        ]
        self.assertEqual(rl.overdue_items(verdicts), [])


class SupersededReceiptsTests(unittest.TestCase):
    """A frozen snapshot of an entity a live producer has re-keyed.

    Found by working the five oldest customer_reply receipts on 2026-11-18.
    All five were April/May snapshots and the daily
    `etsy_conversation_browser_sync` tracked every one of the same threads --
    three under a DIFFERENT `linked_customer_short_id` because the thread had
    been re-keyed to a newer case. An age window cannot tell that from open
    work, which is why four of five looked like 220-day-old unanswered
    customers when only one was.
    """

    THREAD = "etsy-thread::meredith"

    def _receipt(self, entity_id, *, days_old=220.0):
        r = _receipt("customer_reply", "observed", "needs_thread_review",
                     days_old=days_old)
        r["entity_id"] = entity_id
        r["metadata"] = {"conversation_thread_key": self.THREAD}
        return r

    def test_a_rekeyed_thread_is_superseded_not_overdue(self):
        v = _classify(self._receipt("C394"),
                      live_identity={self.THREAD: "C426"})
        self.assertEqual(v.kind, rl.SUPERSEDED)
        self.assertFalse(v.overdue)
        self.assertEqual(v.superseded_by, "C426")

    def test_the_SAME_identity_is_still_graded(self):
        """The mutation partner. A supersession check that fired whenever the
        live producer knew the thread would retire every open loop, including
        the one real one this investigation kept (C376 / peter, which the
        sync reports as reply_needed with open_loop_owner=seller)."""
        v = _classify(self._receipt("C394"),
                      live_identity={self.THREAD: "C394"})
        self.assertEqual(v.kind, rl.PENDING)
        self.assertTrue(v.overdue)

    def test_no_live_index_grades_normally(self):
        """Fail OPEN. A missing or unreadable sync file is not evidence that
        anything was superseded, and treating it as such would silently
        retire every customer_reply receipt at once."""
        v = _classify(self._receipt("C394"))
        self.assertEqual(v.kind, rl.PENDING)
        self.assertTrue(v.overdue)

    def test_a_thread_absent_from_the_live_index_grades_normally(self):
        v = _classify(self._receipt("C394"),
                      live_identity={"etsy-thread::someone-else": "C999"})
        self.assertEqual(v.kind, rl.PENDING)

    def test_a_receipt_with_no_thread_key_grades_normally(self):
        r = _receipt("customer_reply", "observed", "needs_thread_review",
                     days_old=220)
        r["entity_id"] = "C394"
        v = _classify(r, live_identity={self.THREAD: "C426"})
        self.assertEqual(v.kind, rl.PENDING)

    def test_a_receipt_with_no_entity_id_grades_normally(self):
        """Without our own identity there is nothing to compare, so claiming
        supersession would be a guess."""
        r = self._receipt("C394")
        r["entity_id"] = ""
        v = _classify(r, live_identity={self.THREAD: "C426"})
        self.assertEqual(v.kind, rl.PENDING)

    def test_only_lanes_with_a_declared_entity_key_are_checked(self):
        """The index is keyed on a lane-specific metadata field. A lane with
        no entry in SUPERSESSION_KEYS must not accidentally match on some
        other lane's thread key."""
        r = _receipt("newduck", "observed", "draft_ready", days_old=213)
        r["entity_id"] = "ND1"
        r["metadata"] = {"conversation_thread_key": self.THREAD}
        v = _classify(r, live_identity={self.THREAD: "C426"})
        self.assertEqual(v.kind, rl.PENDING)
        self.assertIn("customer_reply", rl.SUPERSESSION_KEYS)
        self.assertNotIn("newduck", rl.SUPERSESSION_KEYS)

    def test_a_superseded_receipt_never_reaches_the_overdue_list(self):
        verdicts = [_classify(self._receipt("C394"),
                              live_identity={self.THREAD: "C426"})]
        self.assertEqual(rl.overdue_items(verdicts), [])

    def test_the_three_real_rekeys_would_all_have_been_caught(self):
        """The measured cases, as a regression. These three were retired by
        hand on 2026-11-18 after reading five receipts; the point of the
        check is that the next three need no one to be looking."""
        for thread, old_id, new_id in (
                ("etsy-thread::r", "C301", "C392"),
                ("etsy-thread::meredith", "C394", "C426"),
                ("etsy-thread::logan", "C395", "C396")):
            with self.subTest(thread=thread):
                r = _receipt("customer_reply", "observed", "thread_observed",
                             days_old=220)
                r["entity_id"] = old_id
                r["metadata"] = {"conversation_thread_key": thread}
                v = _classify(r, live_identity={thread: new_id})
                self.assertEqual(v.kind, rl.SUPERSEDED)
                self.assertEqual(v.superseded_by, new_id)


class JunkInputTests(unittest.TestCase):
    def test_a_non_dict_receipt_is_dropped(self):
        for junk in (None, [], "verified", 3):
            with self.subTest(junk=junk):
                self.assertIsNone(_classify(junk))

    def test_a_receipt_with_no_lane_is_unclassified_not_crashing(self):
        v = _classify({"state": "observed", "updated_at": NOW.isoformat()})
        self.assertEqual(v.kind, rl.UNCLASSIFIED)

    def test_an_unparseable_timestamp_is_an_unknown_age(self):
        receipt = _receipt("newduck", "observed", "draft_ready")
        receipt["updated_at"] = "last Tuesday"
        self.assertIsNone(_classify(receipt).age_days)


class TheLiveStoreIsFullyDeclaredTests(unittest.TestCase):
    """Reads the operator's real receipts. Asserts the POLICY has no BLIND
    SPOT, not today's counts -- the counts are the operator's to change."""

    WC = REPO_ROOT / "state" / "workflow_control"

    def test_no_live_receipt_falls_into_an_undeclared_pair(self):
        import json
        if not self.WC.is_dir():
            self.skipTest("no workflow_control dir")
        paths = sorted(self.WC.glob("*.json"))
        if not paths:
            self.skipTest("no receipts")
        undeclared = set()
        seen = 0
        for path in paths:
            if ".bak" in path.name:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            verdict = rl.classify_receipt(payload, path=str(path))
            if verdict is None:
                continue
            seen += 1
            if verdict.kind == rl.UNCLASSIFIED:
                undeclared.add(f"{verdict.lane}/{verdict.state}/{verdict.state_reason}")
        self.assertGreater(seen, 50, "the scan read almost nothing")
        self.assertEqual(
            undeclared, set(),
            "these (lane, state_reason) pairs exist in the live store but have "
            "no declared policy, so nothing grades them:\n  "
            + "\n  ".join(sorted(undeclared)))


if __name__ == "__main__":
    unittest.main()
