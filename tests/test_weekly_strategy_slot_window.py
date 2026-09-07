"""Surface 67 — a planned slot is graded only after its window CLOSES.

Origin 2026-09-07: the 07:20 Monday packet graded Slot 1 (meme, Monday
evening, scheduled 18:00) as ``no_post_observed`` because the only guard was
``target_date > today``.  With one "completed" slot in a fresh week the whole
execution truth flipped to ``drifting`` and the 07:10 learnings email told
the operator "do not scale this plan yet".  The June 4 live-feed re-check
cannot rescue this case because it looks for a post that already exists.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from runtime import weekly_strategy_recommendation_packet as packet

EASTERN = ZoneInfo("America/New_York")


def _monday(hour: int, minute: int = 0) -> datetime:
    # 2026-09-07 is a Monday.
    return datetime(2026, 9, 7, hour, minute, tzinfo=EASTERN)


def _slot(target_window: str = "evening", lane: str = "meme") -> dict:
    return {
        "slot": "Slot 1",
        "suggested_lane": lane,
        "alternate_lane": "jeepfact",
        "execution_mode": "auto",
        "target_window": target_window,
    }


def _posts(*entries: tuple[str, str, str, str]) -> dict:
    return {
        "posts": [
            {
                "workflow": workflow,
                "published_date": published_date,
                "published_at": f"{published_date}T{hhmm}:00-04:00",
                "time_window": window,
                "is_future_post": False,
                "post_id": f"{workflow}-{published_date}",
                "engagement_score": 1.0,
            }
            for workflow, published_date, hhmm, window in entries
        ]
    }


class SameDaySlotGradingTests(unittest.TestCase):
    def test_same_day_evening_slot_is_awaiting_before_window_closes(self):
        outcome = packet._slot_execution_feedback(_slot(), social_posts_payload={"posts": []}, packet_now=_monday(7, 20))
        self.assertEqual(outcome["tracking_status"], "awaiting_slot")
        self.assertIn("window", outcome["tracking_note"].lower())
        self.assertEqual(outcome["calendar_date"], "2026-09-07")

    def test_same_day_evening_slot_is_missed_after_window_closes(self):
        outcome = packet._slot_execution_feedback(_slot(), social_posts_payload={"posts": []}, packet_now=_monday(21, 30))
        self.assertEqual(outcome["tracking_status"], "no_post_observed")

    def test_same_day_post_already_observed_wins_over_awaiting(self):
        posts = _posts(("meme", "2026-09-07", "09:15", "morning"))
        outcome = packet._slot_execution_feedback(_slot(), social_posts_payload=posts, packet_now=_monday(7, 20))
        self.assertEqual(outcome["tracking_status"], "recommended_lane_executed")

    def test_past_day_without_post_is_still_missed(self):
        outcome = packet._slot_execution_feedback(
            _slot(), social_posts_payload={"posts": []}, packet_now=_monday(7, 20) + timedelta(days=1)
        )
        self.assertEqual(outcome["tracking_status"], "no_post_observed")

    def test_future_day_is_awaiting(self):
        outcome = packet._slot_execution_feedback(
            {**_slot(), "slot": "Slot 2"}, social_posts_payload={"posts": []}, packet_now=_monday(7, 20)
        )
        self.assertEqual(outcome["tracking_status"], "awaiting_slot")

    def test_unknown_window_closes_at_end_of_day(self):
        slot = _slot(target_window="best available window")
        self.assertEqual(
            packet._slot_execution_feedback(slot, social_posts_payload={"posts": []}, packet_now=_monday(23, 0))["tracking_status"],
            "awaiting_slot",
        )
        self.assertEqual(
            packet._slot_execution_feedback(
                slot, social_posts_payload={"posts": []}, packet_now=_monday(0, 5) + timedelta(days=1)
            )["tracking_status"],
            "no_post_observed",
        )

    def test_window_close_table_matches_collector_buckets(self):
        # The collector buckets posts into these windows; the grader must
        # close each window at the same hour the collector stops using it.
        self.assertEqual(packet._slot_window_close_hour("early_morning"), 8)
        self.assertEqual(packet._slot_window_close_hour("morning"), 11)
        self.assertEqual(packet._slot_window_close_hour("midday"), 14)
        self.assertEqual(packet._slot_window_close_hour("afternoon"), 17)
        self.assertEqual(packet._slot_window_close_hour("evening"), 21)
        self.assertEqual(packet._slot_window_close_hour("late_night"), 24)
        self.assertEqual(packet._slot_window_close_hour("review block"), 24)

    def test_naive_packet_clock_is_treated_as_local(self):
        outcome = packet._slot_execution_feedback(
            _slot(), social_posts_payload={"posts": []}, packet_now=datetime(2026, 9, 7, 7, 20)
        )
        self.assertEqual(outcome["tracking_status"], "awaiting_slot")


class ExecutionTruthTests(unittest.TestCase):
    def test_fresh_week_before_first_window_is_pending_not_drifting(self):
        slots = [
            packet._slot_execution_feedback(_slot(), social_posts_payload={"posts": []}, packet_now=_monday(7, 20)),
            {"tracking_status": "awaiting_slot"},
            {"tracking_status": "awaiting_slot"},
            {"tracking_status": "review_slot"},
        ]
        truth = packet._execution_truth(slots)
        self.assertEqual(truth["label"], "pending")
        self.assertEqual(truth["awaiting_slot_count"], 3)

    def test_missed_after_close_still_drifts(self):
        slots = [packet._slot_execution_feedback(_slot(), social_posts_payload={"posts": []}, packet_now=_monday(22, 0))]
        self.assertEqual(packet._execution_truth(slots)["label"], "drifting")


class StatusLabelTests(unittest.TestCase):
    def test_same_day_awaiting_reads_due_today(self):
        slot = {**_slot(), "calendar_date": "2026-09-07", "tracking_status": "awaiting_slot"}
        self.assertEqual(packet._slot_status_label(slot, packet_now=_monday(7, 20)), ("today", "due today"))


if __name__ == "__main__":
    unittest.main()
