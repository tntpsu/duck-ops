"""Tests for the reply-channel reading.

The thing being defended: on 2026-11-17 the digest said "4 drops waiting"
for the third time while the operator's answer sat unread in Trash. Both
halves were individually truthful. This module is what reads them
together, so the cases below are mostly about REFUSING to report health
it did not measure.
"""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import operator_reply_health as health  # noqa: E402

NOW = datetime(2026, 11, 17, 18, 0, tzinfo=timezone.utc)


def _at(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


class ReplyHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self.dir = Path(tempfile.mkdtemp())
        self.log = self.dir / "operator_inbox_receipts.jsonl"

    def _write(self, rows: list[dict]) -> None:
        self.log.write_text(
            "".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")

    def _read(self):
        return health.read(self.log, now=NOW)

    def test_missing_log_is_not_reported_as_healthy(self) -> None:
        """No measurement and nothing-arrived are different facts."""
        h = health.read(self.dir / "absent.jsonl", now=NOW)
        self.assertFalse(h["polled_ever"])
        self.assertIsNone(h["last_dispatch_at"])
        self.assertIn("may not be running", health.describe_for_operator(h))

    def test_dispatch_is_recorded_with_its_age(self) -> None:
        self._write([{"at": _at(3), "outcome": "ok", "folder": "INBOX",
                      "subject": "Re: drops", "command_text": "drop approve a"}])
        h = self._read()
        self.assertEqual(h["last_dispatch_age_hours"], 3.0)
        self.assertIn("3h ago", health.describe_for_operator(h))

    def test_dry_run_receipt_is_not_a_dispatch(self) -> None:
        """A probe of the poller must not read as a delivered reply."""
        self._write([
            {"at": _at(400), "outcome": "ok", "folder": "INBOX",
             "command_text": "drop approve old"},
            {"at": _at(1), "outcome": "ok", "folder": "INBOX",
             "dry_run": True, "command_text": "drop approve a"},
        ])
        h = self._read()
        self.assertEqual(h["last_dispatch_age_hours"], 400.0)
        # Asserting the AGE, not today's wording: the sentence is copy and
        # will change ([[feedback_pin_the_rule_not_the_reading]]). What must
        # hold is that the dry run did not reset the clock.
        self.assertNotIn("ago", health.describe_for_operator(h))

    def test_a_refused_reply_is_surfaced_over_a_quiet_channel(self) -> None:
        """A reply that ARRIVED and was not acted on is the loudest case."""
        self._write([
            {"at": _at(2), "outcome": "skipped_stale_reply",
             "folder": '"[Gmail]/Trash"', "subject": "Re: THURSDAY",
             "command_text": "publish 1188", "age_hours": 120.0},
        ])
        h = self._read()
        self.assertEqual(len(h["refused"]), 1)
        note = health.describe_for_operator(h)
        self.assertIn("publish 1188", note)
        self.assertIn("NOT acted on", note)

    def test_an_old_refusal_is_not_news(self) -> None:
        """A seven-week-old dispatch_failed beside a fresh drop is noise."""
        self._write([
            {"at": _at(health.REFUSAL_RECENT_HOURS + 10),
             "outcome": "dispatch_failed", "command_text": "publish"},
            {"at": _at(5), "outcome": "ok", "folder": "INBOX"},
        ])
        h = self._read()
        self.assertEqual(h["refused"], [])
        self.assertEqual(h["refused_older_count"], 1)

    def test_repeated_refusal_of_the_same_command_collapses(self) -> None:
        """A stale reply is re-skipped every run until it ages out."""
        rows = [{"at": _at(n), "outcome": "skipped_stale_reply",
                 "command_text": "publish 1188", "folder": '"[Gmail]/Trash"'}
                for n in (6, 5, 4, 3, 2, 1)]
        self._write(rows)
        h = self._read()
        self.assertEqual(len(h["refused"]), 1)
        self.assertEqual(h["refused"][0]["refused_age_hours"], 1.0)

    def test_dispatch_from_trash_is_flagged(self) -> None:
        """It worked, but the mailbox is eating approvals."""
        self._write([{"at": _at(1), "outcome": "ok",
                      "folder": '"[Gmail]/Trash"',
                      "command_text": "drop approve a"}])
        h = self._read()
        self.assertEqual(len(h["dispatched_outside_inbox"]), 1)
        self.assertEqual(h["dispatched_outside_inbox"][0]["folder"],
                         '"[Gmail]/Trash"')

    def test_inbox_dispatch_is_not_flagged(self) -> None:
        self._write([{"at": _at(1), "outcome": "ok", "folder": "INBOX"}])
        self.assertEqual(self._read()["dispatched_outside_inbox"], [])

    def test_polled_but_never_answered_says_so(self) -> None:
        self._write([{"at": _at(1), "outcome": "no_new_replies",
                      "folder": "INBOX"}])
        h = self._read()
        self.assertTrue(h["polled_ever"])
        self.assertIsNone(h["last_dispatch_at"])
        self.assertIn("never arrived", health.describe_for_operator(h))

    def test_corrupt_lines_do_not_raise(self) -> None:
        self.log.write_text(
            "not json\n"
            + json.dumps({"at": _at(2), "outcome": "ok", "folder": "INBOX"}) + "\n"
            + "[1,2,3]\n", encoding="utf-8")
        h = self._read()
        self.assertEqual(h["last_dispatch_age_hours"], 2.0)

    def test_naive_timestamp_does_not_raise(self) -> None:
        self._write([{"at": "2026-11-17T17:00:00", "outcome": "ok",
                      "folder": "INBOX"}])
        self.assertIsNotNone(self._read()["last_dispatch_at"])

    def test_tail_bound_keeps_the_newest_rows(self) -> None:
        """The log is append-only; a health question is about the end of it."""
        filler = [{"at": _at(500), "outcome": "no_new_replies",
                   "pad": "x" * 400} for _ in range(4000)]
        self._write(filler + [{"at": _at(1), "outcome": "ok", "folder": "INBOX"}])
        self.assertGreater(self.log.stat().st_size, health.TAIL_BYTES)
        self.assertEqual(self._read()["last_dispatch_age_hours"], 1.0)


if __name__ == "__main__":
    unittest.main()
