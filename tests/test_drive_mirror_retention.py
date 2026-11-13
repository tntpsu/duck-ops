"""Retention: keep the newest N per group, in STAGING only.

The dangerous failure here is not "kept too much" -- it is a grouping key
that merges two unrelated reports and then deletes one of them. The obvious
implementation (collapse digit runs to a placeholder) does exactly that to
`2026-graduation-duck` and `2026-custom-graduation-duck`, so the first test
below pins those two apart and would fail that implementation.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drive_mirror  # noqa: E402


def _touch(path: Path, body: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


class SplitTrailingDateTests(unittest.TestCase):

    def test_two_concepts_whose_names_start_with_a_year_stay_apart(self):
        """The whole reason the splitter is not a digit-collapsing regex.

        `2026-graduation-duck` and `2026-custom-graduation-duck` are two real
        products. A `\\d+ -> N` shape key makes them one group, and newest-1
        then deletes one of them permanently.
        """
        a = drive_mirror._split_trailing_date(
            "trend__2026-graduation-duck__2026-11-01.json"
        )
        b = drive_mirror._split_trailing_date(
            "trend__2026-custom-graduation-duck__2026-11-01.json"
        )
        self.assertIsNotNone(a)
        self.assertIsNotNone(b)
        self.assertNotEqual(a[0], b[0])

    def test_every_digit_bearing_live_concept_keeps_its_own_group(self):
        names = [
            "10-000-lakes-duck",
            "2-quot-duck",
            "2026-custom-graduation-duck",
            "2026-graduation-duck",
            "39-s-magnetic-duck",
            "blazing-420-duck",
            "sister-39-s-duck",
        ]
        groups = {
            drive_mirror._split_trailing_date(f"trend__{n}__2026-11-01.json")[0]
            for n in names
        }
        self.assertEqual(len(groups), len(names))

    def test_the_date_is_the_version_and_the_rest_is_the_group(self):
        self.assertEqual(
            drive_mirror._split_trailing_date("trend__wolf-duck__2026-11-01.json"),
            ("trend__wolf-duck.json", "2026-11-01"),
        )

    def test_the_two_extensions_of_one_report_are_separate_groups(self):
        """Otherwise newest-1 keeps the .json and deletes the .md, or vice
        versa, and the mirror holds half of every report."""
        j = drive_mirror._split_trailing_date("trend__wolf-duck__2026-11-01.json")
        m = drive_mirror._split_trailing_date("trend__wolf-duck__2026-11-01.md")
        self.assertNotEqual(j[0], m[0])

    def test_a_week_numbered_report_splits_too(self):
        """phase_readiness is YYYY-WW, which no date regex would match."""
        self.assertEqual(
            drive_mirror._split_trailing_date("phase_readiness__2026-15.json"),
            ("phase_readiness.json", "2026-15"),
        )

    def test_a_name_with_no_separator_is_unreadable_not_guessed(self):
        self.assertIsNone(
            drive_mirror._split_trailing_date("learning_change_digest.json")
        )

    def test_a_name_with_no_extension_is_unreadable(self):
        self.assertIsNone(drive_mirror._split_trailing_date("trend__wolf__2026-11-01"))


class SplitLeadingTimestampTests(unittest.TestCase):

    def test_an_ops_receipt_groups_on_its_kind(self):
        self.assertEqual(
            drive_mirror._split_leading_timestamp(
                "202604121531590852790400-state-sync.json"
            ),
            ("state-sync.json", "202604121531590852790400"),
        )

    def test_a_non_numeric_leader_is_unreadable(self):
        self.assertIsNone(
            drive_mirror._split_leading_timestamp("ops-state-sync.json")
        )

    def test_two_receipt_kinds_stay_apart(self):
        a = drive_mirror._split_leading_timestamp("20260412153159-state-sync.json")
        b = drive_mirror._split_leading_timestamp("20260412153159-auth-probe.json")
        self.assertNotEqual(a[0], b[0])


class PruneStagedRetentionTests(unittest.TestCase):

    def setUp(self):
        self._saved = drive_mirror.RETENTION_RULES

    def tearDown(self):
        drive_mirror.RETENTION_RULES = self._saved

    def _rule(self, path, keep, split=None):
        drive_mirror.RETENTION_RULES = (
            {
                "path": path,
                "keep": keep,
                "split": split or drive_mirror.SPLIT_TRAILING_DATE,
            },
        )

    def test_it_keeps_the_newest_and_deletes_the_rest(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            d = staging / "t"
            for day in ("2026-11-01", "2026-10-31", "2026-09-15"):
                _touch(d / f"trend__wolf-duck__{day}.json")
            self._rule("t", 1)
            (res,) = drive_mirror.prune_staged_retention(staging)
            self.assertEqual(res["status"], "ok")
            self.assertEqual(res["files_before"], 3)
            self.assertEqual(res["files_after"], 1)
            self.assertEqual(res["deleted"], 2)
            self.assertEqual(
                [p.name for p in sorted(d.iterdir())],
                ["trend__wolf-duck__2026-11-01.json"],
            )

    def test_newest_is_decided_by_the_name_not_the_mtime(self):
        """An rsync into fresh staging can restamp mtimes. If ordering came
        from mtime, "newest" would silently mean "copied last"."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            d = staging / "t"
            new = _touch(d / "trend__wolf-duck__2026-11-01.json")
            old = _touch(d / "trend__wolf-duck__2026-09-15.json")
            # Make the OLDER report look freshly written.
            os.utime(new, (1_600_000_000, 1_600_000_000))
            os.utime(old, (1_900_000_000, 1_900_000_000))
            self._rule("t", 1)
            drive_mirror.prune_staged_retention(staging)
            self.assertTrue(new.exists(), "kept the wrong file: mtime beat the name")
            self.assertFalse(old.exists())

    def test_each_group_is_capped_independently(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            d = staging / "t"
            for concept in ("wolf-duck", "fox-duck"):
                for day in ("2026-11-01", "2026-10-31"):
                    _touch(d / f"trend__{concept}__{day}.json")
            self._rule("t", 1)
            (res,) = drive_mirror.prune_staged_retention(staging)
            self.assertEqual(res["groups"], 2)
            self.assertEqual(res["files_after"], 2)
            self.assertEqual(sorted(p.name for p in d.iterdir()),
                             ["trend__fox-duck__2026-11-01.json",
                              "trend__wolf-duck__2026-11-01.json"])

    def test_an_unreadable_name_is_kept_and_counted(self):
        """Deleting on a parse failure turns a producer rename into silent
        data loss. Keeping turns it into a number we can alarm on."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            d = staging / "t"
            _touch(d / "promotion_readiness.json")
            _touch(d / "trend__wolf-duck__2026-11-01.json")
            _touch(d / "trend__wolf-duck__2026-10-31.json")
            self._rule("t", 1)
            (res,) = drive_mirror.prune_staged_retention(staging)
            self.assertEqual(res["unparsed"], 1)
            self.assertEqual(res["files_after"], 2)
            self.assertTrue((d / "promotion_readiness.json").exists())

    def test_a_missing_configured_directory_is_reported_not_skipped(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            self._rule("does/not/exist", 1)
            (res,) = drive_mirror.prune_staged_retention(Path(tmp))
            self.assertEqual(res["status"], "path_missing")
            self.assertEqual(res["deleted"], 0)

    def test_subdirectories_are_capped_separately(self):
        """Receipts live one directory per run; a cap pooled across runs would
        delete whole runs rather than trim each one."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            for run in ("run-a", "run-b"):
                for ts in ("20260412153159", "20260412153100"):
                    _touch(staging / "t" / run / f"{ts}-state-sync.json")
            self._rule("t", 1, drive_mirror.SPLIT_LEADING_TIMESTAMP)
            (res,) = drive_mirror.prune_staged_retention(staging)
            self.assertEqual(res["groups"], 2)
            self.assertEqual(res["files_after"], 2)
            for run in ("run-a", "run-b"):
                self.assertEqual(
                    [p.name for p in (staging / "t" / run).iterdir()],
                    ["20260412153159-state-sync.json"],
                )

    def test_a_directory_with_fewer_files_than_the_cap_is_untouched(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            d = staging / "t"
            _touch(d / "trend__wolf-duck__2026-11-01.json")
            self._rule("t", 30)
            (res,) = drive_mirror.prune_staged_retention(staging)
            self.assertEqual(res["deleted"], 0)
            self.assertEqual(res["files_after"], 1)

    def test_byte_counts_are_measured_on_both_sides(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            d = staging / "t"
            _touch(d / "trend__wolf-duck__2026-11-01.json", "a" * 100)
            _touch(d / "trend__wolf-duck__2026-10-31.json", "b" * 500)
            self._rule("t", 1)
            (res,) = drive_mirror.prune_staged_retention(staging)
            self.assertEqual(res["bytes_before"], 600)
            self.assertEqual(res["bytes_after"], 100)


class RetentionRulesConfigTests(unittest.TestCase):

    def test_every_rule_names_a_known_splitter_and_keeps_at_least_one(self):
        """A `keep` of 0 would empty a directory; an unknown splitter would
        raise inside the run rather than at import."""
        self.assertTrue(drive_mirror.RETENTION_RULES)
        for rule in drive_mirror.RETENTION_RULES:
            with self.subTest(path=rule["path"]):
                self.assertIn(rule["split"], drive_mirror._SPLITTERS)
                self.assertGreaterEqual(rule["keep"], 1)

    def test_every_rule_points_inside_a_published_tree(self):
        """A rule whose path is not under an allowlisted tree silently never
        applies, because nothing stages that path -- and the only symptom is
        the mirror growing back.

        Loads a PRISTINE copy of the module: the autouse isolation fixture
        replaces WHOLE_TREES with a single `fixture` tree, so asserting
        against the patched value would compare the rules to the test
        harness rather than to what production stages.
        """
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "_drive_mirror_pristine", RUNTIME_DIR / "drive_mirror.py"
        )
        pristine = importlib.util.module_from_spec(spec)
        # dataclasses resolves its own module out of sys.modules at class
        # creation, so GateResult cannot be defined in an unregistered module.
        sys.modules[spec.name] = pristine
        try:
            spec.loader.exec_module(pristine)
        finally:
            sys.modules.pop(spec.name, None)

        trees = [subdir for subdir, _ in pristine.WHOLE_TREES]
        trees.append(pristine.CREATIVE_DEST)
        self.assertNotEqual(trees, ["fixture"], "got the patched trees, not real ones")
        for rule in pristine.RETENTION_RULES:
            with self.subTest(path=rule["path"]):
                self.assertTrue(
                    any(rule["path"].startswith(t + "/") for t in trees),
                    f"{rule['path']} is not inside any staged tree {trees}",
                )

    def test_no_rule_targets_a_source_tree_path(self):
        """Paths are relative to STAGING. An absolute path, or one that
        resolves outside staging, would delete the operator's real data."""
        for rule in drive_mirror.RETENTION_RULES:
            with self.subTest(path=rule["path"]):
                self.assertFalse(Path(rule["path"]).is_absolute())
                self.assertNotIn("..", Path(rule["path"]).parts)


class RunIntegrationTests(unittest.TestCase):
    """Retention as `run()` applies it: pruned before the gate and before the
    counts, recorded in the manifest, carried into the run history."""

    def setUp(self):
        import tempfile
        import unittest.mock as mock

        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp,
                                                            ignore_errors=True))
        self.src = self.tmp / "src"
        self.runs = self.tmp / "runs"
        self.mirror = self.tmp / "DuckData"
        self.staging = self.tmp / "staging"
        self.state = self.tmp / "state" / "drive_mirror.json"
        self.ledger = self.tmp / "state" / "ledger.json"
        self.history = self.tmp / "state" / "runs.jsonl"

        # Three dated snapshots of one report, plus one of another.
        for day in ("2026-11-01", "2026-10-31", "2026-09-15"):
            _touch(self.src / "digests" / f"observation_summary__{day}.json", "{}")
        _touch(self.src / "digests" / "promotion_readiness.json", "{}")
        for lane, pattern in drive_mirror.CREATIVE_LANES.items():
            _touch(self.runs / "r1" / pattern.replace("*", lane))

        for target, value in (
            ("WHOLE_TREES", (("tree", self.src),)),
            ("RETENTION_RULES", ({"path": "tree/digests", "keep": 1,
                                  "split": drive_mirror.SPLIT_TRAILING_DATE},)),
        ):
            patcher = mock.patch.object(drive_mirror, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _run(self, **kw):
        return drive_mirror.run(
            mirror_root=self.mirror,
            staging_root=self.staging,
            state_path=self.state,
            ledger_path=self.ledger,
            runs_path=self.history,
            creative_source=self.runs,
            **kw,
        )

    def test_the_published_mirror_holds_only_the_newest_snapshot(self):
        manifest = self._run()

        self.assertTrue(manifest["ok"], manifest["failures"])
        published = sorted(p.name for p in (self.mirror / "tree" / "digests").iterdir())
        self.assertEqual(
            published,
            ["observation_summary__2026-11-01.json", "promotion_readiness.json"],
        )

    def test_the_tree_count_reports_what_was_published_not_what_was_copied(self):
        """stage_whole_trees counts after the rsync, which is before pruning.
        If that number survived into the manifest the selection card would
        grade a file count the mirror does not contain -- and would never see
        a thinning at all."""
        manifest = self._run()

        by_tree = {t["tree"]: t for t in manifest["trees"]}
        self.assertEqual(2, by_tree["tree"]["files"])

    def test_the_manifest_records_the_per_rule_outcome(self):
        manifest = self._run()

        (rule,) = manifest["retention"]
        self.assertEqual("tree/digests", rule["path"])
        self.assertEqual("ok", rule["status"])
        self.assertEqual(4, rule["files_before"])
        self.assertEqual(2, rule["files_after"])
        self.assertEqual(2, rule["deleted"])
        self.assertEqual(1, rule["unparsed"])

    def test_the_run_history_carries_the_retention_version(self):
        """The selection card builds its shrink baseline only from rows whose
        version matches the current one, so this field is what makes a
        deliberate 96% thinning reset the baseline instead of reading as an
        outage."""
        self._run()

        row = json.loads(self.history.read_text(encoding="utf-8").splitlines()[-1])
        self.assertEqual(drive_mirror.RETENTION_VERSION, row["retention_version"])
        self.assertEqual("tree/digests", row["retention"][0]["path"])
        self.assertEqual(2, row["retention"][0]["deleted"])

    def test_a_pruned_file_is_never_scanned_by_the_gate(self):
        """Pruning must happen BEFORE the gate: a file deleted after being
        scanned would be counted as published when it is not, and one deleted
        after the ledger was written would be carried next run."""
        manifest = self._run()

        # 2 digests + 1 unparsed... i.e. the 2 survivors, the README, the
        # manifest, and one file per creative lane. Never the 2 pruned ones.
        self.assertEqual(manifest["gate"]["files_eligible"],
                         manifest["gate"]["files_scanned"]
                         + manifest["gate"]["files_carried"])
        self.assertLess(manifest["gate"]["files_eligible"],
                        4 + len(drive_mirror.CREATIVE_LANES) + 2)

    def test_a_retention_rule_pointing_at_nothing_does_not_refuse_the_publish(self):
        """Nothing wrong is published when a cap fails to apply -- the mirror
        is only fatter. Refusing would hold good data hostage to a renamed
        directory. The status is recorded for the card to grade."""
        import unittest.mock as mock

        with mock.patch.object(
            drive_mirror, "RETENTION_RULES",
            ({"path": "tree/gone", "keep": 1,
              "split": drive_mirror.SPLIT_TRAILING_DATE},),
        ):
            manifest = self._run()

        self.assertTrue(manifest["ok"], manifest["failures"])
        self.assertEqual("path_missing", manifest["retention"][0]["status"])

    def test_the_source_tree_is_never_pruned(self):
        """Retention thins what Drive uploads. Deleting the operator's own
        data would be the worst possible version of this feature."""
        self._run()

        self.assertEqual(
            4, len(list((self.src / "digests").iterdir())),
            "retention reached back into the source tree",
        )


if __name__ == "__main__":
    unittest.main()
