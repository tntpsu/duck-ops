"""The mirror must publish INTO My Drive, never into a Drive mirror root.

Until 2026-11-13 the mirror published to `~/DuckData`, registered with Drive for
desktop as a "folder from your computer". That is a different destination
wearing the same clothes:

  * a mirror root uploads to `Computers -> <machine>`, not to My Drive;
  * those folders report `canEdit: false`;
  * My Drive can only reference one through a *shortcut*, which Drive mounts
    locally as a symlink into `.shortcut-targets-by-id/`.

So the external agent pointed at "the DuckData folder in Drive" found a shortcut
to a read-only backup sitting outside My Drive. Nothing in the stack noticed,
because the publish itself succeeded every time -- the bug was entirely in
*which directory* succeeded.

These tests pin the rule (the destination is a real directory inside the mounted
My Drive) rather than today's path string, per
[[feedback_pin_the_rule_not_the_reading]].
"""
from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drive_mirror  # noqa: E402


def _pristine_drive_mirror():
    """Load drive_mirror unpatched by the autouse isolation fixture.

    The fixture repoints DRIVE_MIRROR_ROOT at tmp, which is exactly what these
    tests need to inspect, so they need the module as production imports it.
    Registered in sys.modules before exec because dataclasses resolves its own
    module from there at class-creation time.
    """
    spec = importlib.util.spec_from_file_location(
        "_drive_mirror_pristine_mydrive", RUNTIME_DIR / "drive_mirror.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


class MyDriveDiscoveryTests(unittest.TestCase):

    def test_absent_cloudstorage_yields_none_not_an_exception(self):
        """Drive not installed is a legitimate operator state."""
        with TemporaryDirectory() as tmp:
            drive_mirror.DRIVEFS_CLOUDSTORAGE_ROOT = Path(tmp) / "nope"
            try:
                self.assertIsNone(drive_mirror._my_drive_dir())
            finally:
                importlib.reload(drive_mirror)

    def test_a_mount_without_a_my_drive_subdir_is_skipped(self):
        """A signed-out or still-initialising account has the mount but no
        `My Drive` inside it. Returning it would publish into a directory Drive
        does not sync."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "GoogleDrive-aaa@example.com").mkdir()  # no My Drive inside
            wanted = root / "GoogleDrive-zzz@example.com" / "My Drive"
            wanted.mkdir(parents=True)
            mod = _pristine_drive_mirror()
            mod.DRIVEFS_CLOUDSTORAGE_ROOT = root
            self.assertEqual(wanted, mod._my_drive_dir())

    def test_two_accounts_resolve_deterministically(self):
        """Whichever mount the filesystem lists first must not decide where
        5 GB of business data goes."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for acct in ("GoogleDrive-zzz@example.com", "GoogleDrive-aaa@example.com"):
                (root / acct / "My Drive").mkdir(parents=True)
            mod = _pristine_drive_mirror()
            mod.DRIVEFS_CLOUDSTORAGE_ROOT = root
            first = mod._my_drive_dir()
            self.assertEqual(root / "GoogleDrive-aaa@example.com" / "My Drive", first)
            self.assertEqual(first, mod._my_drive_dir())

    def test_falls_back_to_a_plain_local_dir_with_no_drive(self):
        with TemporaryDirectory() as tmp:
            mod = _pristine_drive_mirror()
            mod.DRIVEFS_CLOUDSTORAGE_ROOT = Path(tmp) / "nope"
            self.assertEqual(Path.home() / "DuckData", mod._default_mirror_root())


class ThePublishedRootIsInsideMyDriveTests(unittest.TestCase):

    def test_the_default_root_sits_directly_inside_my_drive(self):
        """The rule, not the path: whatever the folder is called, its parent is
        the mounted `My Drive`. A mirror root would fail this -- it lives under
        the home directory and uploads to Computers."""
        mod = _pristine_drive_mirror()
        mount = mod._my_drive_dir()
        if mount is None:
            self.skipTest("Drive for desktop is not signed in on this machine")
        self.assertEqual(mount, mod._default_mirror_root().parent)
        self.assertEqual("My Drive", mod._default_mirror_root().parent.name)

    def test_the_default_root_is_not_the_legacy_mirror_root(self):
        mod = _pristine_drive_mirror()
        if mod._my_drive_dir() is None:
            self.skipTest("Drive for desktop is not signed in on this machine")
        self.assertNotEqual(Path.home() / "DuckData", mod._default_mirror_root())


class TestModeGuardTests(unittest.TestCase):

    def setUp(self):
        self.mod = _pristine_drive_mirror()
        # Restore the PRIOR value rather than popping. conftest sets this for
        # the whole session, so popping it deletes the flag every later
        # test-mode guard in the run depends on -- which silently disarmed four
        # unrelated guards the first time this file ran.
        prior = os.environ.get("DUCK_TEST_MODE")

        def _restore():
            if prior is None:
                os.environ.pop("DUCK_TEST_MODE", None)
            else:
                os.environ["DUCK_TEST_MODE"] = prior

        self.addCleanup(_restore)
        os.environ["DUCK_TEST_MODE"] = "1"

    def test_refuses_the_my_drive_destination(self):
        self.assertTrue(
            self.mod._refusing_test_mode_prod_write(self.mod._default_mirror_root())
        )

    def test_refuses_the_legacy_destination_which_still_exists_on_disk(self):
        """Un-mirroring in Drive's preferences leaves the local folder behind."""
        self.assertTrue(
            self.mod._refusing_test_mode_prod_write(Path.home() / "DuckData")
        )

    def test_refuses_any_path_inside_a_drive_mount_whatever_it_is_called(self):
        """Structural, not an equality check against today's two literals: a
        renamed published folder still uploads to Google."""
        if self.mod._my_drive_dir() is None:
            self.skipTest("Drive for desktop is not signed in on this machine")
        self.assertTrue(
            self.mod._refusing_test_mode_prod_write(
                self.mod._my_drive_dir() / "SomeOtherName"
            )
        )

    def test_the_frozen_root_is_always_guarded(self):
        """True on any machine, Drive mounted or not."""
        self.assertIn(self.mod._FROZEN_PRODUCTION_MIRROR_ROOT,
                      self.mod._guarded_mirror_roots())

    def test_both_spellings_of_a_symlinked_destination_are_refused(self):
        """`My Drive/DuckData` can already exist as a Drive *shortcut*, which
        Drive for desktop mounts as a symlink into
        `.shortcut-targets-by-id/`. `resolve()` then reports the target
        rather than the guarded directory, so a guard holding one spelling
        misses the other.

        The condition is CONSTRUCTED here rather than borrowed from this
        machine. The old test asserted `len(_guarded_mirror_roots()) >= 2`,
        which is a reading of the local Drive setup: on a runner with no
        Drive, `_default_mirror_root()` falls back to the legacy
        `~/DuckData`, so the two named destinations coincide, the union
        collapses to one entry and duck-ops CI failed with "1 not greater
        than or equal to 2". Worse, it never exercised the shortcut case
        even here, because DuckData is not currently a shortcut -- so the
        count it asserted came from the two DESTINATIONS, not the two
        spellings it claimed to be about
        ([[feedback_pin_the_rule_not_the_reading]]).

        A real symlink makes the rule testable anywhere. It relies on
        `_guarded_mirror_roots` deriving from the constant at CALL time,
        which its own docstring requires for exactly this reason.
        """
        from unittest import mock

        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "shortcut_target"
            target.mkdir()
            link = Path(tmp) / "DuckData"
            link.symlink_to(target, target_is_directory=True)
            self.assertNotEqual(link.resolve(), Path(os.path.normpath(str(link))),
                                "the fixture must actually be a symlink")
            with mock.patch.object(
                    self.mod, "_FROZEN_PRODUCTION_MIRROR_ROOT", link):
                for label, spelling in (("the link", link),
                                        ("its target", target)):
                    with self.subTest(spelling=label):
                        self.assertTrue(
                            self.mod._refusing_test_mode_prod_write(spelling),
                            f"{label} is not refused, so the guard misses "
                            "whichever spelling the caller passes")

    def test_allows_an_isolated_tmp_destination(self):
        with TemporaryDirectory() as tmp:
            self.assertFalse(
                self.mod._refusing_test_mode_prod_write(Path(tmp) / "DuckData")
            )

    def test_the_audit_scans_the_my_drive_folder_not_a_shortcut_target(self):
        """The pollution audit reads _FROZEN_PRODUCTION_MIRROR_ROOT. If that
        followed the shortcut it would scan the Computers backup and report a
        clean My Drive it never looked at."""
        if self.mod._my_drive_dir() is None:
            self.skipTest("Drive for desktop is not signed in on this machine")
        self.assertNotIn(
            ".shortcut-targets-by-id",
            str(self.mod._FROZEN_PRODUCTION_MIRROR_ROOT),
        )


class ReleaseRefusesASymlinkedRootTests(unittest.TestCase):
    """The failure this would have caused, had it not been caught first.

    With the shortcut present, `My Drive/DuckData` was a symlink to the
    read-only Computers backup. `rsync -a` follows a symlinked destination, so
    the publish would have reported success while writing the data back into the
    directory the move was supposed to escape.
    """

    def test_a_symlinked_destination_is_refused_and_nothing_is_written(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            staging = root / "staging"
            (staging / "duck-ops").mkdir(parents=True)
            (staging / "duck-ops" / "a.json").write_text("{}", encoding="utf-8")
            real_target = root / "elsewhere"
            real_target.mkdir()
            link = root / "DuckData"
            link.symlink_to(real_target, target_is_directory=True)

            ok, detail = drive_mirror.release(staging, link)

            self.assertFalse(ok)
            self.assertIn("symlink", detail)
            self.assertEqual([], sorted(p.name for p in real_target.iterdir()))

    def test_a_real_directory_destination_still_publishes(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            staging = root / "staging"
            (staging / "duck-ops").mkdir(parents=True)
            (staging / "duck-ops" / "a.json").write_text("{}", encoding="utf-8")
            dest = root / "DuckData"

            ok, detail = drive_mirror.release(staging, dest)

            self.assertTrue(ok, detail)
            self.assertTrue((dest / "duck-ops" / "a.json").is_file())


if __name__ == "__main__":
    unittest.main()
