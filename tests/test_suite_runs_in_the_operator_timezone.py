"""The suite's own timezone is a precondition, so it should fail like one.

Five tests in this suite assert rendered Eastern wall-clock, because ~182 bare
`.astimezone()` calls across 66 runtime modules treat system-local time as the
operator's time -- true in production, where the launchd jobs, the producers
and the viewer all run on one Mac set to America/New_York.

Run the suite in another zone and those five fail with four-hour diffs in
assertions about review emails and packing snapshots, which look like broken
features and read as a regression from whatever was pushed last. That is
exactly how the 2026-11-11 run was first read. This test turns that into one
failure that says what is actually wrong.

It is NOT a substitute for fixing the convention. See TESTS.md Surface 113.
"""
from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

OPERATOR_TZ_NAME = "America/New_York"

ZONE_DEPENDENT_TESTS = (
    "tests/test_etsy_browser_batch.py",
    "tests/test_nightly_action_summary.py",
    "tests/test_weekly_strategy_slot_window.py",
    "tests/test_workflow_operator_summary.py",
)


class OperatorTimezoneTests(unittest.TestCase):
    def test_the_suite_runs_at_the_operator_offset(self) -> None:
        now = datetime.now()
        system_offset = now.astimezone().utcoffset()
        operator_offset = now.replace(tzinfo=ZoneInfo(OPERATOR_TZ_NAME)).utcoffset()

        self.assertEqual(
            system_offset,
            operator_offset,
            msg=(
                f"This suite must run at the {OPERATOR_TZ_NAME} offset "
                f"(got {system_offset}, expected {operator_offset}).\n"
                "Nothing is broken in the code under test -- these tests "
                "assert rendered operator-local wall-clock:\n  "
                + "\n  ".join(ZONE_DEPENDENT_TESTS)
                + f"\n\nFix: run with TZ={OPERATOR_TZ_NAME}. CI sets this as "
                "job-level `env:` in .github/workflows/python-checks.yml; if "
                "that block was dropped, restore it rather than editing the "
                "tests.\nSee TESTS.md Surface 113 for why this is pinned "
                "rather than fixed."
            ),
        )

    def test_scheduler_health_is_exempt_because_it_pins_its_own_zone(self) -> None:
        """Guards the claim the failure message above rests on.

        scheduler_health resolves every stamp in SCHEDULER_TZ, so it passes
        under any TZ and is deliberately absent from ZONE_DEPENDENT_TESTS. If
        a bare .astimezone() creeps back into it, that list becomes wrong.
        """
        import scheduler_health

        self.assertEqual(str(scheduler_health.SCHEDULER_TZ), OPERATOR_TZ_NAME)
        self.assertNotIn("tests/test_scheduler_health.py", ZONE_DEPENDENT_TESTS)


if __name__ == "__main__":
    unittest.main()
