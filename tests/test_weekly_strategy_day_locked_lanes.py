"""Tests pinning the day-locked-lane override in weekly_strategy planner.

Background: 2026-06-04 incident — the signal-driven _preferred_slot_lane
assigned `jeepfact` to Slot 1 (Monday) because jeepfact is the anchor
workflow. But jeepfact is hard-locked to JEEPFACT_DOW=WED — no Monday
post can ever fill that slot. The learnings emitter then flagged
'Slot 1 missed jeepfact' forever even when Monday's meme was sitting
in workflow_control awaiting approval.

Fix: _apply_day_locked_lane post-processes each slot. If the slot's
target_day has a known cron-scheduled lane (Mon=meme, Wed=jeepfact,
Thu=thursday, Sun=gtdf_winner), override suggested_lane to that.
Annotates day_locked_lane + day_locked_override_from for transparency.
Signal-driven planning still applies for unlocked days (Tue/Sat).
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import weekly_strategy_recommendation_packet as wsp


class DayLockedLaneTests(unittest.TestCase):
    def test_monday_locks_to_meme_even_if_planner_picked_jeepfact(self) -> None:
        """The 2026-06-04 incident shape. Planner picks jeepfact
        as the anchor; day-lock forces meme for Monday."""
        slot = {
            "slot": "Slot 1",
            "target_day": "Monday",
            "suggested_lane": "jeepfact",
            "workflow": "jeepfact",
            "calendar_date": "2026-06-01",
        }
        wsp._apply_day_locked_lane(slot)
        self.assertEqual(slot["suggested_lane"], "meme")
        self.assertEqual(slot["workflow"], "meme")
        self.assertEqual(slot["day_locked_lane"], "meme")
        # Audit trail preserves the planner's original suggestion.
        self.assertEqual(slot["day_locked_override_from"], "jeepfact")

    def test_wednesday_locks_to_jeepfact(self) -> None:
        slot = {
            "slot": "Slot 2",
            "target_day": "Wednesday",
            "suggested_lane": "jeepfact",
            "workflow": "jeepfact",
            "calendar_date": "2026-06-03",
        }
        wsp._apply_day_locked_lane(slot)
        self.assertEqual(slot["suggested_lane"], "jeepfact")
        # Already correct: no override_from key.
        self.assertNotIn("day_locked_override_from", slot)
        self.assertEqual(slot["day_locked_lane"], "jeepfact")

    def test_thursday_locks_to_thursday(self) -> None:
        slot = {
            "slot": "Slot 3",
            "target_day": "Thursday",
            "suggested_lane": "blog",
            "workflow": "blog",
            "calendar_date": "2026-06-04",
        }
        wsp._apply_day_locked_lane(slot)
        self.assertEqual(slot["suggested_lane"], "thursday")
        self.assertEqual(slot["day_locked_override_from"], "blog")

    def test_sunday_locks_to_gtdf_winner(self) -> None:
        slot = {
            "slot": "Slot 5",
            "target_day": "Sunday",
            "suggested_lane": "operator_review",
            "workflow": "operator_review",
            "calendar_date": "2026-06-07",
        }
        wsp._apply_day_locked_lane(slot)
        self.assertEqual(slot["suggested_lane"], "gtdf_winner")
        self.assertEqual(slot["day_locked_override_from"], "operator_review")

    def test_saturday_unlocked_preserves_planner_suggestion(self) -> None:
        """Saturday is the experiment slot — no fixed cron. The
        planner's signal-driven suggestion must be preserved."""
        slot = {
            "slot": "Slot 4",
            "target_day": "Saturday",
            "suggested_lane": "manual_social_experiment",
            "workflow": "manual_social_experiment",
            "calendar_date": "2026-06-06",
        }
        wsp._apply_day_locked_lane(slot)
        self.assertEqual(slot["suggested_lane"], "manual_social_experiment")
        self.assertNotIn("day_locked_lane", slot)
        self.assertNotIn("day_locked_override_from", slot)

    def test_tuesday_unlocked_preserves_planner_suggestion(self) -> None:
        """Tuesday is review_carousel's day, but the lane mode is
        operator-controllable (auto_schedule_instagram vs
        approval_gated). Leave day unlocked so the planner can
        recommend competitor experiments here."""
        slot = {
            "slot": "Slot X",
            "target_day": "Tuesday",
            "suggested_lane": "blog",
            "workflow": "blog",
            "calendar_date": "2026-06-02",
        }
        wsp._apply_day_locked_lane(slot)
        self.assertEqual(slot["suggested_lane"], "blog")
        self.assertNotIn("day_locked_lane", slot)

    def test_missing_target_day_is_noop(self) -> None:
        """If a slot somehow lacks target_day (legacy or partial
        data), do nothing rather than crash. The planner must keep
        building the packet even on malformed input."""
        slot = {
            "slot": "Slot Z",
            "suggested_lane": "jeepfact",
            "workflow": "jeepfact",
        }
        wsp._apply_day_locked_lane(slot)
        self.assertEqual(slot["suggested_lane"], "jeepfact")
        self.assertNotIn("day_locked_lane", slot)


if __name__ == "__main__":
    unittest.main()
