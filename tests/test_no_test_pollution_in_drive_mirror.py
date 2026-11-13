"""Layer 3 of the drive-mirror isolation policy: audit the real destination.

Scans the PRODUCTION mirror root (`~/DuckData/`) and its staging directory for
evidence that a test run reached them. Unlike the sibling audits in this
directory, a hit here means fixture data was handed to Google Drive for upload,
so the check also looks for the shape of the two incidents that have actually
happened rather than only for a marker string.

The three layers:
  1. conftest.py `_redirect_drive_mirror_paths` -- autouse, points every path
     AND both sources at tmp. Default for every test.
  2. `drive_mirror._refusing_test_mode_prod_write` -- raises
     TestModeRefusalError when DUCK_TEST_MODE=1 and the destination resolves to
     the frozen factory default. Belt for tests that bypass the fixture via
     re-import, subprocess, or a direct call.
  3. THIS test -- audits the actual directories.

An ABSENT mirror root is a pass, not a skip: the mirror is operator-installed
and "not set up yet" is a legitimate state. What must never be true is a mirror
root holding test fixtures.

If this fails, do NOT delete the test. Find the test that reached production
(grep tests/ for the marker), fix its isolation, then clear the directory.
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

# Read from the module so a future rename of the constant fails loudly here
# rather than silently auditing a path nothing writes to.
import drive_mirror  # noqa: E402

PRODUCTION_MIRROR_ROOT = drive_mirror._FROZEN_PRODUCTION_MIRROR_ROOT
PRODUCTION_STAGING_ROOT = Path.home() / ".DuckData.staging"
PRODUCTION_RUNS_PATH = drive_mirror._FROZEN_PRODUCTION_RUNS_PATH

#: The source trees a real run records. A row naming anything else was written
#: by a fixture -- this is a structural rule, not a reading of today's counts
#: ([[feedback_pin_the_rule_not_the_reading]]).
REAL_TREE_NAMES = frozenset(subdir for subdir, _ in drive_mirror.WHOLE_TREES)

#: Filenames a test fixture would leave but a real run never produces.
FIXTURE_MARKERS = (
    "fixture_source",
    "fixture_runs",
    "meme_meme.png",
    "jeepfact_jeepfact.png",
    "leaky.json",
    "locked.json",
)

#: 2026-11-12: a mutation run with the guard disarmed rsynced /tmp into the
#: mirror root. These are /tmp residents that have no business in a data mirror
#: and are the cheapest fingerprint of that specific accident recurring.
TMP_SPILL_PREFIXES = ("__KMP_REGISTERED_LIB_", "com.apple.launchd.", ".X11-unix")


def _offenders(root: Path) -> list[str]:
    if not root.is_dir():
        return []
    hits: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in drive_mirror.EXCLUDE_DIR_NAMES]
        for name in dirnames + filenames:
            if name in FIXTURE_MARKERS or name.startswith(TMP_SPILL_PREFIXES):
                hits.append(str((Path(dirpath) / name).relative_to(root)))
        if len(hits) > 40:
            break
    return hits


class DriveMirrorPollutionAuditTests(unittest.TestCase):

    def test_the_production_mirror_root_holds_no_test_fixtures(self):
        hits = _offenders(PRODUCTION_MIRROR_ROOT)
        self.assertEqual(
            [], hits,
            f"test fixtures found in the PUBLISHED mirror {PRODUCTION_MIRROR_ROOT} "
            f"-- Google Drive uploads this directory. First 40: {hits[:40]}",
        )

    def test_the_production_staging_root_holds_no_test_fixtures(self):
        hits = _offenders(PRODUCTION_STAGING_ROOT)
        self.assertEqual(
            [], hits,
            f"test fixtures found in {PRODUCTION_STAGING_ROOT}; the gate runs "
            f"here, so a fixture here becomes a fixture in the mirror. "
            f"First 40: {hits[:40]}",
        )

    def test_the_production_run_history_holds_no_fixture_rows(self):
        """The history is the baseline the selection card grades against.

        Fixture rows here are not cosmetic: a nine-file test run recorded
        beside a 178,731-file real one teaches the shrink detector that the
        real one is the anomaly. Thirteen reached this file on 2026-11-13,
        because DRIVE_MIRROR_RUNS_PATH was added without being added to the
        conftest fixture — the exact gap the test below now closes.
        """
        import json
        if not PRODUCTION_RUNS_PATH.exists():
            return
        offenders = []
        for i, line in enumerate(
            PRODUCTION_RUNS_PATH.read_text(encoding="utf-8").splitlines()
        ):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                offenders.append(f"row {i}: unparseable")
                continue
            names = set((row.get("tree_files") or {}).keys())
            unknown = names - REAL_TREE_NAMES
            if unknown:
                offenders.append(
                    f"row {i} ({row.get('generated_at')}): "
                    f"tree(s) {sorted(unknown)} are not real source trees"
                )
        self.assertEqual(
            [], offenders,
            f"fixture rows found in {PRODUCTION_RUNS_PATH}. These corrupt the "
            f"baseline the drive_mirror_selection card grades against. "
            f"First 10: {offenders[:10]}",
        )

    def test_the_isolation_fixture_covers_every_production_path(self):
        """The audits above can only fail after the damage. This asserts the
        fixture that prevents it still exists AND still names every path.

        Derived from the module, never a hand-kept list. Until 2026-11-13 this
        checked three literal names, so adding DRIVE_MIRROR_RUNS_PATH to the
        module without adding it to the fixture passed here and wrote fixture
        rows to production. A list that has to be remembered is the bug.
        """
        conftest = (Path(__file__).resolve().parents[0] / "conftest.py").read_text()
        self.assertIn("_redirect_drive_mirror_paths", conftest)

        path_constants = sorted(
            name
            for name in dir(drive_mirror)
            if name.startswith("DRIVE_MIRROR_")
            and isinstance(getattr(drive_mirror, name), Path)
        )
        self.assertTrue(path_constants, "no DRIVE_MIRROR_* Path constants found")
        for constant in path_constants + ["WHOLE_TREES", "CREATIVE_SOURCE"]:
            with self.subTest(constant=constant):
                self.assertIn(
                    constant, conftest,
                    f"{constant} is a production path the fixture does not "
                    f"redirect; a test touching it writes to production.",
                )


if __name__ == "__main__":
    unittest.main()
