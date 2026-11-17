from __future__ import annotations

import json
import os
import plistlib
import sys
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import scheduler_health


def _write_plist(path: Path, *, label: str, job_name: str, schedule: dict[str, int]) -> None:
    path.write_bytes(
        plistlib.dumps(
            {
                "Label": label,
                "ProgramArguments": [
                    "/Users/philtullai/ai-agents/duckAgent_runtime/run_scheduled_flow.sh",
                    job_name,
                    "--flow",
                    "thursday",
                    "--all",
                ],
                "StartCalendarInterval": schedule,
            },
            sort_keys=False,
        )
    )


class SchedulerHealthTests(unittest.TestCase):
    def test_completed_job_is_healthy_from_scheduler_log(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            launch_agents.mkdir()
            log_path = root / "duckagent_scheduler.log"
            receipt_dir = root / "receipts"
            _write_plist(
                launch_agents / "com.philtullai.duckagent.thursday.weekly.plist",
                label="com.philtullai.duckagent.thursday.weekly",
                job_name="thursday_weekly",
                schedule={"Weekday": 4, "Hour": 9, "Minute": 0},
            )
            log_path.write_text(
                "\n".join(
                    [
                        "[2026-04-23 09:00:00 EDT] START thursday_weekly :: python src/main_agent.py --flow thursday --all",
                        "[2026-04-23 09:02:00 EDT] END   thursday_weekly :: exit=0",
                    ]
                ),
                encoding="utf-8",
            )

            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat("2026-04-23T10:00:00-04:00"),
                launch_agents_dir=launch_agents,
                scheduler_log_path=log_path,
                receipt_dir=receipt_dir,
                write_outputs=False,
            )

            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["summary"]["healthy_count"], 1)
            self.assertEqual(payload["items"][0]["status"], "healthy")

    def test_missed_job_detects_expected_start_without_log_event(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            launch_agents.mkdir()
            _write_plist(
                launch_agents / "com.philtullai.duckagent.reviews.daily.plist",
                label="com.philtullai.duckagent.reviews.daily",
                job_name="reviews_daily",
                schedule={"Hour": 9, "Minute": 0},
            )

            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat("2026-04-23T11:00:00-04:00"),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "missing.log",
                receipt_dir=root / "receipts",
                write_outputs=False,
            )

            self.assertEqual(payload["status"], "bad")
            self.assertEqual(payload["summary"]["missed_count"], 1)
            self.assertEqual(payload["items"][0]["status"], "missed_run")
            # Surface 67: the miss is only a valid verdict until the next
            # window (09:00 tomorrow) plus grace has passed.
            self.assertEqual(payload["items"][0]["verdict_valid_until"], "2026-04-24T09:30:00-04:00")
            self.assertEqual(payload["verdict_valid_until"], "2026-04-24T09:30:00-04:00")

    def test_healthy_job_has_no_verdict_deadline(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            launch_agents.mkdir()
            _write_plist(
                launch_agents / "com.philtullai.duckagent.reviews.daily.plist",
                label="com.philtullai.duckagent.reviews.daily",
                job_name="reviews_daily",
                schedule={"Hour": 9, "Minute": 0},
            )
            log_path = root / "scheduler.log"
            log_path.write_text(
                "[2026-04-23 09:00:01 EDT] START reviews_daily\n[2026-04-23 09:00:09 EDT] END   reviews_daily :: exit=0\n",
                encoding="utf-8",
            )
            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat("2026-04-23T11:00:00-04:00"),
                launch_agents_dir=launch_agents,
                scheduler_log_path=log_path,
                receipt_dir=root / "receipts",
                write_outputs=False,
            )
            self.assertEqual(payload["items"][0]["status"], "healthy")
            self.assertIsNone(payload["items"][0]["verdict_valid_until"])
            self.assertIsNone(payload["verdict_valid_until"])

    def test_running_receipt_over_budget_detects_hung_process(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            _write_plist(
                launch_agents / "com.philtullai.duckagent.jeepfact.wednesday.plist",
                label="com.philtullai.duckagent.jeepfact.wednesday",
                job_name="jeepfact_wednesday",
                schedule={"Weekday": 3, "Hour": 9, "Minute": 0},
            )
            (receipt_dir / "jeepfact_wednesday.json").write_text(
                json.dumps(
                    {
                        "job_name": "jeepfact_wednesday",
                        "run_id": "jeepfact_wednesday_20260422_090000_123",
                        "status": "running",
                        "started_at": "2026-04-22T09:00:00-0400",
                        "updated_at": "2026-04-22T09:00:00-0400",
                        "finished_at": None,
                        "timeout_seconds": 1800,
                        "pid": 123,
                        "child_pid": 456,
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(scheduler_health, "_pid_alive", return_value=True):
                payload = scheduler_health.build_scheduler_health(
                    now=datetime.fromisoformat("2026-04-22T10:00:00-04:00"),
                    launch_agents_dir=launch_agents,
                    scheduler_log_path=root / "empty.log",
                    receipt_dir=receipt_dir,
                    write_outputs=False,
                )

            self.assertEqual(payload["status"], "bad")
            self.assertEqual(payload["summary"]["hung_count"], 1)
            self.assertEqual(payload["items"][0]["status"], "hung")

    def test_duckops_plist_and_receipt_are_discovered(self) -> None:
        """2026-06-12 (Surface 15.1): duck-ops jobs run via
        run_duck_ops_observe_review.sh under com.philtullai.duckops.*
        plists. They were invisible to scheduler_health before the
        discovery widening — 17 producers unmonitored."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            (launch_agents / "com.philtullai.duckops.occasion-engine.daily.plist").write_bytes(
                plistlib.dumps({
                    "Label": "com.philtullai.duckops.occasion-engine.daily",
                    "ProgramArguments": [
                        "/Users/philtullai/ai-agents/duckAgent_runtime/run_duck_ops_observe_review.sh",
                        "occasion_engine_daily",
                        str(Path(__file__).resolve().parents[1] / "runtime" / "occasion_engine.py"),
                    ],
                    "StartCalendarInterval": {"Hour": 7, "Minute": 25},
                }, sort_keys=False)
            )
            (receipt_dir / "occasion_engine_daily.json").write_text(
                json.dumps({
                    "job_name": "occasion_engine_daily",
                    "run_id": "occasion_engine_daily_20260612_072500_99",
                    "status": "succeeded", "exit_code": 0,
                    "started_at": "2026-06-12T07:25:00-0400",
                    "updated_at": "2026-06-12T07:25:08-0400",
                    "finished_at": "2026-06-12T07:25:08-0400",
                    "timeout_seconds": 900, "pid": 99,
                }), encoding="utf-8")

            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat("2026-06-12T08:00:00-04:00"),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "empty.log",
                receipt_dir=receipt_dir,
                write_outputs=False,
            )
            names = {item["job_name"] for item in payload["items"]}
            self.assertIn("occasion_engine_daily", names)
            occ = next(i for i in payload["items"] if i["job_name"] == "occasion_engine_daily")
            self.assertEqual(occ["status"], "healthy")

    def test_photoroom_quota_failure_is_dependency_warning_not_scheduler_failure(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            _write_plist(
                launch_agents / "com.philtullai.duckagent.meme.monday.plist",
                label="com.philtullai.duckagent.meme.monday",
                job_name="meme_monday",
                schedule={"Weekday": 1, "Hour": 9, "Minute": 0},
            )
            (receipt_dir / "meme_monday.json").write_text(
                json.dumps(
                    {
                        "job_name": "meme_monday",
                        "run_id": "meme_monday_20260427_090001_65509",
                        "status": "failed",
                        "exit_code": 1,
                        "started_at": "2026-04-27T09:00:01-0400",
                        "finished_at": "2026-04-27T09:00:19-0400",
                        "timeout_seconds": 1800,
                    }
                ),
                encoding="utf-8",
            )
            log_path = root / "duckagent_scheduler.log"
            log_path.write_text(
                "\n".join(
                    [
                        "[2026-04-27 09:00:01 EDT] START meme_monday :: python src/main_agent.py --flow meme --all",
                        "[photoroom] v2/edit templateId=d626efa0-5b2e-47b0-a0c9-376f88150463",
                        'RuntimeError: Photoroom v2/edit failed [402]: {"error":{"message":"You have exhausted the number of images in your plan."}}',
                        "[2026-04-27 09:00:19 EDT] END   meme_monday :: exit=1",
                    ]
                ),
                encoding="utf-8",
            )

            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat("2026-04-27T10:00:00-04:00"),
                launch_agents_dir=launch_agents,
                scheduler_log_path=log_path,
                receipt_dir=receipt_dir,
                write_outputs=False,
            )

            self.assertEqual(payload["status"], "warn")
            self.assertEqual(payload["summary"]["failed_count"], 0)
            self.assertEqual(payload["summary"]["warn_count"], 1)
            self.assertEqual(payload["summary"]["attention_count"], 0)
            item = payload["items"][0]
            self.assertEqual(item["status"], "dependency_blocked_recent")
            self.assertEqual(item["dependency_blocker"], "photoroom_quota_exhausted")

    def test_failed_job_with_matching_resolution_is_fixed_pending_warning(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            resolution_dir = root / "resolutions"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            resolution_dir.mkdir()
            _write_plist(
                launch_agents / "com.philtullai.duckagent.weekly.sunday.plist",
                label="com.philtullai.duckagent.weekly.sunday",
                job_name="weekly_sunday",
                schedule={"Weekday": 0, "Hour": 7, "Minute": 0},
            )
            (receipt_dir / "weekly_sunday.json").write_text(
                json.dumps(
                    {
                        "job_name": "weekly_sunday",
                        "run_id": "weekly_sunday_20260510_070002_19333",
                        "status": "failed",
                        "exit_code": 1,
                        "started_at": "2026-05-10T07:00:02-0400",
                        "finished_at": "2026-05-10T07:03:27-0400",
                        "timeout_seconds": 5400,
                    }
                ),
                encoding="utf-8",
            )
            (resolution_dir / "weekly_sunday.json").write_text(
                json.dumps(
                    {
                        "job_name": "weekly_sunday",
                        "run_id": "weekly_sunday_20260510_070002_19333",
                        "exit_code": 1,
                        "resolution_status": "fixed_pending_next_run",
                        "root_cause_class": "workflow_validation",
                        "fix_summary": "SEO meta description repair handles short generated copy.",
                    }
                ),
                encoding="utf-8",
            )

            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat("2026-05-10T08:00:00-04:00"),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "duckagent_scheduler.log",
                receipt_dir=receipt_dir,
                resolution_dir=resolution_dir,
                write_outputs=False,
            )

            self.assertEqual(payload["status"], "warn")
            self.assertEqual(payload["summary"]["bad_count"], 0)
            self.assertEqual(payload["summary"]["warn_count"], 1)
            self.assertEqual(payload["summary"]["fixed_pending_count"], 1)
            self.assertEqual(payload["summary"]["attention_count"], 0)
            item = payload["items"][0]
            self.assertEqual(item["status"], "fixed_pending_next_run")
            self.assertEqual(item["severity"], "warn")
            self.assertIsNotNone(item["resolution"])


def _write_duckops_plist(path: Path, *, label: str, job_name: str, schedule) -> None:
    """A duck-ops job, whose StartCalendarInterval may legitimately be a dict
    OR an array of dicts (launchd accepts both)."""
    path.write_bytes(
        plistlib.dumps(
            {
                "Label": label,
                "ProgramArguments": [
                    "/Users/philtullai/ai-agents/duckAgent_runtime/run_duck_ops_observe_review.sh",
                    job_name,
                    str(Path(__file__).resolve().parents[1] / "runtime" / f"{job_name}.py"),
                ],
                "StartCalendarInterval": schedule,
            },
            sort_keys=False,
        )
    )


def _receipt(job_name: str, *, started: str, finished: str) -> str:
    return json.dumps(
        {
            "job_name": job_name,
            "run_id": f"{job_name}_run_1",
            "status": "succeeded",
            "exit_code": 0,
            "started_at": started,
            "updated_at": finished,
            "finished_at": finished,
            "timeout_seconds": 1800,
            "pid": 4242,
        }
    )


class ClockChangeTests(unittest.TestCase):
    """2026-11-01: twelve jobs reported `missed_run` with exit 0 the morning
    DST ended. None had missed anything.

    `now` arrives as a FIXED-offset datetime, so attaching its offset to a
    past wall-clock time stamps today's offset (EST, -05:00) onto a window
    that actually occurred in EDT (-04:00). The computed window lands one hour
    late in absolute terms, which exceeds the 30-minute grace, so every job
    whose last window fell before the transition reads as missed.

    The fall-back direction produces false alarms. Spring-forward moves the
    threshold an hour EARLIER, which is more permissive -- but with daily and
    weekly cadences the previous run is a day or a week away, so the one-hour
    error never reaches it and nothing is hidden. Only this direction is a bug
    worth grading, which is why these tests are all fall-back.

    Second false-alarm incident on this grader (see TESTS.md: the 2026-06
    weekly-governance false "missed" runs), hence a permanent test.
    """

    FALL_BACK_NOW = "2026-11-01T11:05:00-05:00"  # EST, hours after the change

    def _daily_payload(self, *, with_receipt: bool):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            _write_plist(
                launch_agents / "com.philtullai.duckagent.reviews.daily.plist",
                label="com.philtullai.duckagent.reviews.daily",
                job_name="reviews_daily",
                schedule={"Hour": 23, "Minute": 55},
            )
            if with_receipt:
                # Ran in its window, while EDT was still in force.
                (receipt_dir / "reviews_daily.json").write_text(
                    _receipt(
                        "reviews_daily",
                        started="2026-10-31T23:55:04-0400",
                        finished="2026-10-31T23:55:38-0400",
                    ),
                    encoding="utf-8",
                )
            return scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat(self.FALL_BACK_NOW),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "empty.log",
                receipt_dir=receipt_dir,
                write_outputs=False,
            )

    def test_a_window_before_the_fall_back_is_not_a_phantom_miss(self) -> None:
        payload = self._daily_payload(with_receipt=True)
        item = payload["items"][0]
        self.assertEqual(item["status"], "healthy")
        self.assertEqual(item["severity"], "ok")
        self.assertEqual(payload["summary"]["missed_count"], 0)

    def test_a_genuinely_missed_window_before_the_fall_back_is_still_caught(self) -> None:
        """The fix must not buy a green card by making every pre-transition
        window unmissable."""
        payload = self._daily_payload(with_receipt=False)
        self.assertEqual(payload["items"][0]["status"], "missed_run")
        self.assertEqual(payload["summary"]["missed_count"], 1)

    def test_the_expected_window_carries_the_offset_in_force_at_that_time(self) -> None:
        """The defect, stated directly: the window is 23:55 EDT, not 23:55 EST."""
        payload = self._daily_payload(with_receipt=True)
        self.assertEqual(payload["items"][0]["expected_at"], "2026-10-31T23:55:00-04:00")

    def test_the_next_window_after_the_change_uses_the_new_offset(self) -> None:
        payload = self._daily_payload(with_receipt=True)
        self.assertEqual(payload["items"][0]["next_expected_at"], "2026-11-01T23:55:00-05:00")

    def test_a_weekly_window_before_the_change_keeps_its_own_offset(self) -> None:
        """The weeklies are the slow half of the incident: each one stays red
        until its next firing, so a Tuesday job misread on Sunday stays wrong
        for two more days."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            _write_duckops_plist(
                launch_agents / "com.philtullai.duckops.competitor-benchmark.weekly.plist",
                label="com.philtullai.duckops.competitor-benchmark.weekly",
                job_name="competitor_benchmark_weekly",
                schedule={"Weekday": 2, "Hour": 6, "Minute": 45},
            )
            (receipt_dir / "competitor_benchmark_weekly.json").write_text(
                _receipt(
                    "competitor_benchmark_weekly",
                    started="2026-10-27T06:45:04-0400",
                    finished="2026-10-27T06:45:12-0400",
                ),
                encoding="utf-8",
            )
            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat(self.FALL_BACK_NOW),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "empty.log",
                receipt_dir=receipt_dir,
                write_outputs=False,
            )
            item = payload["items"][0]
            self.assertEqual(item["status"], "healthy")
            self.assertEqual(item["expected_at"], "2026-10-27T06:45:00-04:00")


class ScheduleShapeTests(unittest.TestCase):
    """launchd accepts StartCalendarInterval as a dict OR an array of dicts.
    Only the dict was read, so an array-form job was tracked with an EMPTY
    schedule -- which grades as `no_history` forever instead of being checked
    against its window. The product-schema canary ships in the array form.
    """

    def _array_form_payload(self, *, with_receipt: bool):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            _write_duckops_plist(
                launch_agents / "com.philtullai.duckops.product-model-index.daily.plist",
                label="com.philtullai.duckops.product-model-index.daily",
                job_name="product_model_index_daily",
                schedule=[{"Hour": 7, "Minute": 35}],
            )
            if with_receipt:
                (receipt_dir / "product_model_index_daily.json").write_text(
                    _receipt(
                        "product_model_index_daily",
                        started="2026-11-01T07:35:02-0500",
                        finished="2026-11-01T07:35:40-0500",
                    ),
                    encoding="utf-8",
                )
            return scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat("2026-11-01T11:05:00-05:00"),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "empty.log",
                receipt_dir=receipt_dir,
                write_outputs=False,
            )

    def test_an_array_form_schedule_is_actually_graded(self) -> None:
        """`missed_run` rather than `no_history` is the proof the schedule was
        read -- an empty schedule can never produce a miss."""
        payload = self._array_form_payload(with_receipt=False)
        item = payload["items"][0]
        self.assertEqual(item["status"], "missed_run")
        self.assertEqual(item["expected_at"], "2026-11-01T07:35:00-05:00")

    def test_an_array_form_schedule_grades_a_real_run_healthy(self) -> None:
        payload = self._array_form_payload(with_receipt=True)
        self.assertEqual(payload["items"][0]["status"], "healthy")


if __name__ == "__main__":
    unittest.main()


class DeclaredAfterTheWindowTests(unittest.TestCase):
    """2026-11-12: twelve jobs were wrapped to write receipts, and six of them
    immediately reported `missed_run` / bad for windows that had closed hours
    or days BEFORE the wrapper existed. A job cannot miss a window it had no
    way to run in -- that is grading against evidence that could not exist,
    the same shape as the phantom `scheduler_health_probe` one level up.

    The `no_history` branch already said exactly the right thing and sat
    BELOW `missed_run`, so it was unreachable for any job whose window had
    passed. The declaration time is the plist's mtime; a mtime later than the
    grading clock is not usable evidence and must not suppress a real miss
    (the array-form fixtures above write their plists at real wall-clock time
    while faking `now` into the past)."""

    def _payload(self, *, plist_mtime: str | None, now: str):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            plist = launch_agents / "com.philtullai.duckops.review-carousel.tuesday.plist"
            _write_duckops_plist(
                plist,
                label="com.philtullai.duckops.review-carousel.tuesday",
                job_name="review_carousel_tuesday",
                schedule={"Weekday": 3, "Hour": 16, "Minute": 0},
            )
            if plist_mtime is not None:
                stamp = datetime.fromisoformat(plist_mtime).timestamp()
                os.utime(plist, (stamp, stamp))
            return scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat(now),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "empty.log",
                receipt_dir=receipt_dir,
                write_outputs=False,
            )

    def test_a_job_declared_after_the_window_is_not_blamed_for_it(self) -> None:
        """Tuesday 16:00 passed; the plist was written Thursday 18:00. No
        receipt exists because none could."""
        item = self._payload(
            plist_mtime="2026-11-12T18:00:00-05:00",
            now="2026-11-12T18:04:00-05:00",
        )["items"][0]
        self.assertEqual(item["status"], "no_history")
        self.assertEqual(item["severity"], "warn")

    def test_a_job_declared_before_the_window_still_reports_a_real_miss(self) -> None:
        """The guard must not swallow the failure it was built around: same
        job, same absent receipt, but declared a week before the window."""
        item = self._payload(
            plist_mtime="2026-11-04T09:00:00-05:00",
            now="2026-11-12T18:04:00-05:00",
        )["items"][0]
        self.assertEqual(item["status"], "missed_run")
        self.assertEqual(item["severity"], "bad")

    def test_a_declaration_stamp_after_the_grading_clock_is_not_evidence(self) -> None:
        """A mtime in the future relative to `now` tells us nothing, so it may
        not downgrade a miss. This is what keeps the array-form fixtures --
        which write real files against a faked past clock -- still graded."""
        item = self._payload(
            plist_mtime="2026-11-20T09:00:00-05:00",
            now="2026-11-12T18:04:00-05:00",
        )["items"][0]
        self.assertEqual(item["status"], "missed_run")


class ConsecutiveLockSkipTests(unittest.TestCase):
    """A job that declines to work must not be able to report health.

    2026-11-01: a sidecar run was killed mid-step and left an ownerless lock.
    Every firing for the next twelve days logged `SKIP :: another run is
    already in progress` and exited 0, so `run_with_receipt.sh` wrote
    `succeeded`, launchd recorded `last exit code = 0`, and this card graded
    the job healthy while eight steps — including the reader for the
    operator's email approvals — did nothing.

    `skipped_lock_active` was already in this module's vocabulary and nothing
    could produce it. The producer is now exit 75 (EX_TEMPFAIL) from the
    sidecar, mapped by the wrapper; these tests cover the escalation from
    "busy once" to "stuck".
    """

    def _history(self, root: Path, job_name: str, statuses: list[str]) -> Path:
        history = root / "history"
        history.mkdir(exist_ok=True)
        rows = []
        for i, status in enumerate(statuses):
            # Every run writes a `running` row then a terminal row, exactly as
            # run_with_receipt.sh does. Counting the running rows would halve
            # any streak, so the fixture includes them on purpose.
            rows.append({"job_name": job_name, "run_id": f"r{i}", "status": "running"})
            rows.append({"job_name": job_name, "run_id": f"r{i}", "status": status})
        (history / f"{job_name}.jsonl").write_text(
            "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
        )
        return history

    def test_one_skip_is_a_busy_lock_and_stays_a_warning(self):
        """A run that outlived its own interval legitimately blocks the next
        firing. Calling that an outage would cry wolf on normal overlap."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            history = self._history(
                root, "duck_ops_sidecar", ["succeeded", "succeeded", "skipped_lock_active"]
            )
            self.assertEqual(
                1, scheduler_health._consecutive_lock_skips(
                    "duck_ops_sidecar", history_dir=history
                ),
            )

    def test_the_streak_counts_only_back_to_the_last_run_that_worked(self):
        """A lifetime total would stay red forever after one bad week. The
        number has to be a CURRENT streak."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            history = self._history(
                root,
                "duck_ops_sidecar",
                ["skipped_lock_active"] * 9 + ["succeeded"] + ["skipped_lock_active"] * 2,
            )
            self.assertEqual(
                2, scheduler_health._consecutive_lock_skips(
                    "duck_ops_sidecar", history_dir=history
                ),
            )

    def test_running_rows_neither_count_nor_break_the_streak(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            history = self._history(
                root, "duck_ops_sidecar", ["skipped_lock_active"] * 3
            )
            rows = (history / "duck_ops_sidecar.jsonl").read_text().splitlines()
            self.assertEqual(6, len(rows), "fixture must include the running rows")
            self.assertEqual(
                3, scheduler_health._consecutive_lock_skips(
                    "duck_ops_sidecar", history_dir=history
                ),
            )

    def test_a_missing_or_unreadable_history_reads_as_no_streak(self):
        """Losing the instrument must not manufacture a red card
        ([[feedback_receipts_never_load_bearing]])."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(
                0, scheduler_health._consecutive_lock_skips(
                    "never_ran", history_dir=root / "nope"
                ),
            )
            history = root / "history"
            history.mkdir()
            (history / "garbled.jsonl").write_text("{not json\n\n[]\n", encoding="utf-8")
            self.assertEqual(
                0, scheduler_health._consecutive_lock_skips(
                    "garbled", history_dir=history
                ),
            )

    def _payload(self, *, statuses: list[str], now: str):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            launch_agents = root / "LaunchAgents"
            receipt_dir = root / "receipts"
            launch_agents.mkdir()
            receipt_dir.mkdir()
            _write_duckops_plist(
                launch_agents / "com.philtullai.duckops.sidecar.plist",
                label="com.philtullai.duckops.sidecar",
                job_name="duck_ops_sidecar",
                schedule={"Hour": 12, "Minute": 0},
            )
            (receipt_dir / "duck_ops_sidecar.json").write_text(
                json.dumps({
                    "job_name": "duck_ops_sidecar",
                    "status": statuses[-1],
                    "exit_code": 75,
                    "started_at": "2026-11-13T12:00:04-05:00",
                    "finished_at": "2026-11-13T12:00:05-05:00",
                }),
                encoding="utf-8",
            )
            history = self._history(root, "duck_ops_sidecar", statuses)
            payload = scheduler_health.build_scheduler_health(
                now=datetime.fromisoformat(now),
                launch_agents_dir=launch_agents,
                scheduler_log_path=root / "empty.log",
                receipt_dir=receipt_dir,
                history_dir=history,
                write_outputs=False,
            )
        return {i["job_name"]: i for i in payload["items"]}["duck_ops_sidecar"]

    def test_a_single_skip_grades_warn_not_bad(self):
        item = self._payload(
            statuses=["succeeded", "skipped_lock_active"],
            now="2026-11-13T13:00:00-05:00",
        )
        self.assertEqual("skipped_lock_active", item["status"])
        self.assertEqual("warn", item["severity"])

    def test_three_consecutive_skips_grade_bad(self):
        """The twelve-day incident. Three firings at a 6h cadence is 18 hours
        of no work, past the 2x-cadence freshness rule."""
        item = self._payload(
            statuses=["skipped_lock_active"] * 3,
            now="2026-11-13T13:00:00-05:00",
        )
        self.assertEqual("skipped_lock_stuck", item["status"])
        self.assertEqual("bad", item["severity"])
        self.assertIn("3", item["summary"])
        self.assertIn("lock", item["recommended_action"].lower())

    def test_the_stuck_status_is_graded_bad_and_demands_attention(self):
        """A status absent from these sets would render as an unknown colour
        and never reach the operator ([[feedback_loader_and_registration_both_need_tests]])."""
        self.assertIn("skipped_lock_stuck", scheduler_health.BAD_STATUSES)
        self.assertIn("skipped_lock_stuck", scheduler_health.ATTENTION_STATUSES)

    def test_a_healthy_job_is_untouched_by_any_of_this(self):
        item = self._payload(
            statuses=["succeeded"], now="2026-11-13T13:00:00-05:00",
        )
        self.assertNotIn("skipped_lock", item["status"])


class TimeoutIsItsOwnVerdictTests(unittest.TestCase):
    """A job the wrapper KILLED is not a job that failed.

    `timeout` sat in this module's KNOWN and BAD status sets with nothing able
    to produce it — the same shape as `skipped_lock_active` before 2026-11-13.
    `run_with_receipt.sh` recorded `timeout_seconds` and never enforced it, so
    the duck-ops sidecar reported `running` for three days (2026-11-13) and
    again for eight hours (2026-11-17), each time holding its lock and
    silently killing the operator inbox poller. A timeout nothing acts on is
    a comment.

    The watchdog added 2026-11-17 is the producer. These pin the verdict, and
    that it stays distinct from `failed`: the fixes differ, because a killed
    job means something it CALLED is blocking, while a failed one means its
    own logic went wrong.
    """

    def _evaluate(self, receipt: dict) -> dict:
        now = datetime(2026, 11, 17, 9, 0).astimezone()
        with TemporaryDirectory() as tmp:
            return scheduler_health._evaluate_job(
                {"job_name": "duck_ops_sidecar", "schedule": {},
                 "declared_at": None},
                receipt=receipt, resolution=None, events=[], now=now,
                scheduler_log_path=Path(tmp) / "scheduler.log",
                history_dir=Path(tmp),
            )

    def _receipt(self, status: str, **kw) -> dict:
        base = {
            "job_name": "duck_ops_sidecar",
            "status": status,
            "exit_code": 143,
            "started_at": "2026-11-17T00:28:00-0500",
            "finished_at": "2026-11-17T00:58:00-0500",
            "timeout_seconds": 1800,
        }
        base.update(kw)
        return base

    def test_a_timed_out_run_is_bad_and_says_it_was_killed(self):
        out = self._evaluate(self._receipt("timeout"))
        self.assertEqual(out["status"], "timeout")
        self.assertEqual(out["severity"], "bad")
        self.assertIn("killed", out["summary"].lower())

    def test_it_is_not_collapsed_into_failed(self):
        timed = self._evaluate(self._receipt("timeout"))
        failed = self._evaluate(self._receipt("failed", exit_code=3))
        self.assertNotEqual(timed["status"], failed["status"])

    def test_the_advice_is_to_find_the_blocker_not_to_rerun(self):
        """A rerun blocks on the same thing. Both times this happened the
        blocker was an unresponsive Docker daemon, which a retry cannot fix."""
        out = self._evaluate(self._receipt("timeout"))
        action = out["recommended_action"].lower()
        self.assertIn("blocking", action)
        self.assertIn("same thing", action)

    def test_timeout_is_already_a_known_bad_status(self):
        self.assertIn("timeout", scheduler_health.ATTENTION_STATUSES)
        self.assertIn("timeout", scheduler_health.BAD_STATUSES)
