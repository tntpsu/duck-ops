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

    def test_the_isolation_fixture_is_still_wired(self):
        """The audit above can only fail after the damage. This asserts the
        fixture that prevents it still exists, so deleting it is loud."""
        conftest = (Path(__file__).resolve().parents[0] / "conftest.py").read_text()
        self.assertIn("_redirect_drive_mirror_paths", conftest)
        for constant in ("DRIVE_MIRROR_ROOT", "WHOLE_TREES", "CREATIVE_SOURCE"):
            with self.subTest(constant=constant):
                self.assertIn(constant, conftest)


if __name__ == "__main__":
    unittest.main()
