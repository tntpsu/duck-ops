from __future__ import annotations

import json
import plistlib
import sys
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
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


if __name__ == "__main__":
    unittest.main()
