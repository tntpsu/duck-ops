"""2026-11-09 — the card that grades every scheduled job was grading two thirds.

`scheduler_health` reported 35 jobs, all healthy, on a machine holding 50 loaded
duck launchd agents. `_load_scheduled_jobs` accepts a plist only when its
`ProgramArguments[0]` ends in one of two wrapper names, so a job wired through a
third wrapper is skipped in silence -- and the ONE loaded agent with a nonzero
last exit was one of the skipped ones.

The same filter had already hidden 17 duck-ops producers until 2026-06-12. It was
widened that day rather than removed, so it re-closed around the next new wrapper
([[feedback_incomplete_fix_enumerate_all_branches]]).

These jobs write no receipt, so there is nothing to grade them on. Inventing a
`missed_run` for each would be a confident wrong answer
([[feedback_a_failed_probe_is_a_fact_about_the_probe]]); the honest output is
"these exist and I cannot see them", at warn, never bad.
"""
from __future__ import annotations

import plistlib
import sys
import json
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import scheduler_health

NOW = datetime.fromisoformat("2026-11-09T14:00:00-05:00")
GRADED_RUNNER = "/Users/philtullai/ai-agents/duckAgent_runtime/run_scheduled_flow.sh"
OTHER_RUNNER = "/Users/philtullai/ai-agents/duckAgent_runtime/run_engineering_governance_digest_morning.sh"
CLEAN_RUN_LOG = (
    "[2026-11-09 09:00:01 EST] START reviews_daily\n"
    "[2026-11-09 09:00:09 EST] END   reviews_daily :: exit=0\n"
)
HEALTHY_PLIST_ARGS = [GRADED_RUNNER, "reviews_daily", "--flow", "reviews", "--all"]


def _plist(path: Path, *, label: str, args: list[str], schedule: dict | None,
           env: dict | None = None) -> None:
    payload: dict = {"Label": label, "ProgramArguments": args}
    if schedule is not None:
        payload["StartCalendarInterval"] = schedule
    if env is not None:
        payload["EnvironmentVariables"] = env
    path.write_bytes(plistlib.dumps(payload, sort_keys=False))


class _Harness(unittest.TestCase):
    def _build(self, make_plists, *, log: str = "") -> dict:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            agents = root / "LaunchAgents"
            agents.mkdir()
            receipt_dir = root / "receipts"
            receipt_dir.mkdir()
            make_plists(agents)
            log_path = root / "scheduler.log"
            if log:
                log_path.write_text(log, encoding="utf-8")
            return scheduler_health.build_scheduler_health(
                now=NOW,
                launch_agents_dir=agents,
                scheduler_log_path=log_path,
                receipt_dir=receipt_dir,
                resolution_dir=root / "resolutions",
                write_outputs=False,
            )


class TestWhatTheRosterSees(_Harness):
    def test_a_job_on_an_unrecognised_wrapper_is_listed_not_dropped(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckops.engineering-governance-digest.morning.plist",
                label="com.philtullai.duckops.engineering-governance-digest.morning",
                args=[OTHER_RUNNER, "engineering_governance_digest_morning"],
                schedule={"Hour": 7, "Minute": 30},
            )

        payload = self._build(make)
        blind = payload["unmonitored_jobs"]
        self.assertEqual(len(blind), 1)
        # args[1] of an UNRECOGNISED wrapper is not a job name, it is
        # whatever that script happens to take first. Reporting it as one
        # was a guess wearing a fact's clothes.
        self.assertIsNone(blind[0]["job_name"])
        self.assertEqual(blind[0]["reason"], "declares_no_job_name")
        self.assertEqual(blind[0]["runner"], "run_engineering_governance_digest_morning.sh")
        self.assertEqual(payload["summary"]["unmonitored_count"], 1)

    def test_a_keepalive_daemon_is_deliberately_not_listed(self) -> None:
        """creative_viewer / publish_idle / the WhatsApp bridge have no
        StartCalendarInterval. "Did it run on time" is not their question, and
        grading them on a cadence they do not have would manufacture a second
        false verdict to fix the first."""
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckagent.creative_viewer.plist",
                label="com.philtullai.duckagent.creative_viewer",
                args=["/Users/philtullai/ai-agents/duckAgent_runtime/run_creative_viewer.sh"],
                schedule=None,
            )

        payload = self._build(make)
        self.assertEqual(payload["unmonitored_jobs"], [])
        self.assertEqual(payload["summary"]["unmonitored_count"], 0)

    def test_a_calendar_plist_with_no_job_name_is_named_by_its_label(self) -> None:
        """One real plist invokes python3 on a script path with no job-name
        argument at all; dropping it for the shape of its arguments is the same
        blindness in a different coat."""
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckops.review-queue-refresh.plist",
                label="com.philtullai.duckops.review-queue-refresh",
                args=["python3"],
                schedule={"Minute": 0},
            )

        payload = self._build(make)
        blind = payload["unmonitored_jobs"]
        self.assertEqual(len(blind), 1)
        self.assertIsNone(blind[0]["job_name"])
        self.assertEqual(blind[0]["label"], "com.philtullai.duckops.review-queue-refresh")
        self.assertEqual(blind[0]["reason"], "declares_no_job_name")
        self.assertIn(scheduler_health.JOB_NAME_ENV_KEY, blind[0]["how_to_fix"])

    def test_a_graded_job_is_never_also_reported_as_blind(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckagent.thursday.weekly.plist",
                label="com.philtullai.duckagent.thursday.weekly",
                args=[GRADED_RUNNER, "thursday_weekly", "--flow", "thursday", "--all"],
                schedule={"Weekday": 4, "Hour": 9},
            )

        payload = self._build(make)
        self.assertEqual(payload["unmonitored_jobs"], [])
        self.assertEqual([i["job_name"] for i in payload["items"]], ["thursday_weekly"])

    def test_an_unreadable_plist_is_skipped_and_never_raises(self) -> None:
        def make(agents: Path) -> None:
            (agents / "com.philtullai.duckops.corrupt.plist").write_bytes(b"not a plist")
            _plist(
                agents / "com.philtullai.duckops.other.plist",
                label="com.philtullai.duckops.other",
                args=[OTHER_RUNNER, "other_job"],
                schedule={"Hour": 6},
            )

        payload = self._build(make)
        blind = payload["unmonitored_jobs"]
        self.assertEqual([i["label"] for i in blind], ["com.philtullai.duckops.other"])
        self.assertIsNone(blind[0]["job_name"])


class TestWhatTheRollupSays(_Harness):
    def test_blind_jobs_make_the_card_warn_rather_than_ok(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckops.digest.plist",
                label="com.philtullai.duckops.digest",
                args=[OTHER_RUNNER, "digest_morning"],
                schedule={"Hour": 7, "Minute": 30},
            )

        payload = self._build(make)
        self.assertEqual(payload["status"], "warn")

    def test_the_clean_copy_never_claims_coverage_it_does_not_have(self) -> None:
        """'Scheduler health is clean across tracked scheduled jobs' was true and
        unreadable: it is the sentence an operator reads as 'everything is fine'
        ([[feedback_a_number_needs_its_denominator]])."""
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckagent.reviews.daily.plist",
                label="com.philtullai.duckagent.reviews.daily",
                args=HEALTHY_PLIST_ARGS,
                schedule={"Hour": 9, "Minute": 0},
            )
            _plist(
                agents / "com.philtullai.duckops.digest.plist",
                label="com.philtullai.duckops.digest",
                args=[OTHER_RUNNER, "digest_morning"],
                schedule={"Hour": 7, "Minute": 30},
            )

        payload = self._build(make, log=CLEAN_RUN_LOG)
        self.assertEqual(payload["status"], "warn")
        self.assertEqual(payload["summary"]["warn_count"], 0)
        self.assertIn("NOT graded here", payload["headline"])
        self.assertIn("1 scheduled job(s)", payload["headline"])
        self.assertNotIn("clean across", payload["headline"])

    def test_a_real_bad_job_still_outranks_the_blind_ones(self) -> None:
        """Unmonitored is a weaker claim than broken and must never mask one."""
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckagent.reviews.daily.plist",
                label="com.philtullai.duckagent.reviews.daily",
                args=[GRADED_RUNNER, "reviews_daily", "--flow", "reviews", "--all"],
                schedule={"Hour": 6},
            )
            _plist(
                agents / "com.philtullai.duckops.digest.plist",
                label="com.philtullai.duckops.digest",
                args=[OTHER_RUNNER, "digest_morning"],
                schedule={"Hour": 7, "Minute": 30},
            )

        payload = self._build(make)
        self.assertEqual(payload["status"], "bad")
        self.assertIn("NOT graded here", payload["headline"])
        self.assertIn(
            "Resolve the top scheduler item",
            payload["recommended_action"],
        )

    def test_with_nothing_blind_the_old_clean_copy_is_untouched(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckagent.reviews.daily.plist",
                label="com.philtullai.duckagent.reviews.daily",
                args=HEALTHY_PLIST_ARGS,
                schedule={"Hour": 9, "Minute": 0},
            )

        payload = self._build(make, log=CLEAN_RUN_LOG)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["summary"]["unmonitored_count"], 0)
        self.assertNotIn("NOT graded here", payload["headline"])
        self.assertIn("clean across", payload["headline"])
        self.assertEqual(payload["recommended_action"], "No action needed.")

    def test_the_action_names_the_fix_when_blindness_is_the_only_problem(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckops.digest.plist",
                label="com.philtullai.duckops.digest",
                args=[OTHER_RUNNER, "digest_morning"],
                schedule={"Hour": 7, "Minute": 30},
            )

        payload = self._build(make)
        self.assertIn("receipt-writing wrapper", payload["recommended_action"])


class TestTheMarkdownSurface(_Harness):
    def test_blind_jobs_get_their_own_section(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckops.digest.plist",
                label="com.philtullai.duckops.digest",
                args=[OTHER_RUNNER, "digest_morning"],
                schedule={"Hour": 7, "Minute": 30},
            )

        md = scheduler_health.render_scheduler_health_markdown(self._build(make))
        self.assertIn("## Scheduled but NOT graded here", md)
        self.assertIn("com.philtullai.duckops.digest", md)
        self.assertIn("- Not graded here: `1`", md)

    def test_the_section_is_absent_when_nothing_is_blind(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckagent.thursday.weekly.plist",
                label="com.philtullai.duckagent.thursday.weekly",
                args=[GRADED_RUNNER, "thursday_weekly", "--flow", "thursday", "--all"],
                schedule={"Weekday": 4, "Hour": 9},
            )

        md = scheduler_health.render_scheduler_health_markdown(self._build(make))
        self.assertNotIn("## Scheduled but NOT graded here", md)
        self.assertIn("- Not graded here: `0`", md)


class TestTheLiveMachine(unittest.TestCase):
    def test_the_whitelist_is_a_single_shared_constant(self) -> None:
        """Two copies of this tuple is how the 2026-06-12 widening missed a
        reader ([[feedback_edits_into_shared_scope_collide]])."""
        source = (RUNTIME_DIR / "scheduler_health.py").read_text(encoding="utf-8")
        self.assertEqual(source.count('"run_scheduled_flow.sh", "run_duck_ops_observe_review.sh"'), 1)


if __name__ == "__main__":
    unittest.main()


class TestTheDeclarationContract(_Harness):
    """2026-11-09, second pass. The whitelist of runner NAMES had closed twice:
    widened on 2026-06-12 to admit 17 duck-ops producers instead of being
    removed, then re-closed around twelve more jobs by November. A third name
    would close a third time, so the RULE changed instead of the list -- a
    plist now opts IN by declaring the job name it writes receipts under.
    """

    def test_any_wrapper_that_declares_a_job_name_is_graded(self) -> None:
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckops.brand-new.plist",
                label="com.philtullai.duckops.brand-new",
                args=["/some/wrapper/nobody/whitelisted.sh", "--flag"],
                schedule={"Hour": 9, "Minute": 0},
                env={scheduler_health.JOB_NAME_ENV_KEY: "reviews_daily"},
            )

        payload = self._build(make, log=CLEAN_RUN_LOG)
        self.assertEqual([i["job_name"] for i in payload["items"]], ["reviews_daily"])
        self.assertEqual(payload["unmonitored_jobs"], [])

    def test_a_declared_job_that_never_ran_reads_as_missed_not_invisible(self) -> None:
        """The declaration is a CLAIM. A false one must surface as a bad
        verdict, not as the job quietly leaving the roster."""
        def make(agents: Path) -> None:
            _plist(
                agents / "com.philtullai.duckops.liar.plist",
                label="com.philtullai.duckops.liar",
                args=["/some/wrapper.sh"],
                schedule={"Hour": 3, "Minute": 0},
                env={scheduler_health.JOB_NAME_ENV_KEY: "never_runs"},
            )

        payload = self._build(make)
        self.assertIn("never_runs", [i["job_name"] for i in payload["items"]])
        self.assertEqual(payload["unmonitored_jobs"], [])

    def test_the_two_lists_partition_the_plists(self) -> None:
        """Graded and not-graded come from ONE resolver. A plist falling out of
        both would be a job nobody is looking at -- the exact shape of the bug
        this surface exists to fix."""
        def make(agents: Path) -> None:
            _plist(agents / "com.philtullai.duckops.a.plist", label="a",
                   args=[GRADED_RUNNER, "reviews_daily"],
                   schedule={"Hour": 9, "Minute": 0})
            _plist(agents / "com.philtullai.duckops.b.plist", label="b",
                   args=["/x/anything.sh"], schedule={"Hour": 4, "Minute": 0},
                   env={scheduler_health.JOB_NAME_ENV_KEY: "declared_b"})
            _plist(agents / "com.philtullai.duckops.c.plist", label="c",
                   args=["python3"], schedule={"Hour": 5, "Minute": 0})

        payload = self._build(make, log=CLEAN_RUN_LOG)
        graded = {i["job_name"] for i in payload["items"]}
        blind = {i["label"] for i in payload["unmonitored_jobs"]}
        self.assertEqual(graded, {"reviews_daily", "declared_b"})
        self.assertEqual(blind, {"c"})

    def test_nobody_added_a_third_runner_name(self) -> None:
        """The tuple is historical and closed. Growing it is the move that has
        now failed twice."""
        self.assertEqual(
            scheduler_health.ACCEPTED_RUNNERS,
            ("run_scheduled_flow.sh", "run_duck_ops_observe_review.sh"))


class TestItDoesNotGradeItsOwnOpenWindow(unittest.TestCase):
    """Once this producer runs under the receipt shim it is itself a graded
    job -- and the shim writes `running` BEFORE the producer regenerates the
    card, so the card would report its own job as forever in flight. Not a
    verdict that self-corrects next run; structural
    ([[feedback_never_grade_an_open_window]])."""

    JOB = "scheduler_health_refresh"

    def _receipts(self, status: str) -> dict:
        return {self.JOB: {"job_name": self.JOB, "status": status,
                           "run_id": "now", "receipt_path": "/x.json"}}

    def _history(self, tmp: Path, *entries: dict) -> Path:
        tmp.mkdir(parents=True, exist_ok=True)
        (tmp / f"{self.JOB}.jsonl").write_text(
            "\n".join(json.dumps(e) for e in entries), encoding="utf-8")
        return tmp

    def test_its_own_running_receipt_falls_back_to_the_last_finished_run(self):
        with TemporaryDirectory() as raw:
            history = self._history(
                Path(raw) / "h",
                {"job_name": self.JOB, "status": "running", "run_id": "older"},
                {"job_name": self.JOB, "status": "succeeded", "run_id": "older",
                 "exit_code": 0},
                {"job_name": self.JOB, "status": "running", "run_id": "now"},
            )
            out = scheduler_health._unwind_own_in_flight_run(
                self._receipts("running"), own_job=self.JOB, history_dir=history)
        self.assertEqual(out[self.JOB]["status"], "succeeded")
        self.assertIn("still in flight", out[self.JOB]["note"])

    def test_another_jobs_running_receipt_is_left_alone(self):
        """A genuinely stuck job must still show up. This rule is about the
        window the producer is standing in, not about `running` in general."""
        with TemporaryDirectory() as raw:
            history = self._history(
                Path(raw) / "h",
                {"job_name": self.JOB, "status": "succeeded", "run_id": "older"})
            out = scheduler_health._unwind_own_in_flight_run(
                self._receipts("running"), own_job="some_other_job",
                history_dir=history)
        self.assertEqual(out[self.JOB]["status"], "running")

    def test_a_finished_own_receipt_is_not_rewound(self):
        with TemporaryDirectory() as raw:
            history = self._history(
                Path(raw) / "h",
                {"job_name": self.JOB, "status": "failed", "run_id": "older"})
            out = scheduler_health._unwind_own_in_flight_run(
                self._receipts("succeeded"), own_job=self.JOB, history_dir=history)
        self.assertEqual(out[self.JOB]["status"], "succeeded")

    def test_no_history_leaves_the_receipt_untouched(self):
        with TemporaryDirectory() as raw:
            out = scheduler_health._unwind_own_in_flight_run(
                self._receipts("running"), own_job=self.JOB,
                history_dir=Path(raw) / "missing")
        self.assertEqual(out[self.JOB]["status"], "running")
