"""Surface 130 -- the source-code bundles published beside the data.

The mirror's release gate scans the staged filesystem and skips binary files,
which is correct for a 3 MB PNG and wrong for a zip: a credential inside a
bundle would reach Drive without any gate having read it. So the bundle builds
its own scan over its members before the zip exists, and these tests exist to
prove that scan refuses rather than to prove a zip gets written.

The planted-credential test is the one that matters. A bundle asserted only by
"the zip appeared and contained files" is the fail-open bug wearing a test.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drive_mirror  # noqa: E402

# A fake credential carrying a real pattern's shape, so a refusal here is
# evidence about the scan and not about this particular string.
PLANTED_KEY = "sk-ant-" + "Z9y8X7w6V5u4T3s2R1q0P9o8"


def _git_repo(root: Path, files: dict[str, str]) -> Path:
    """A real git repo, because the bundle's boundary IS `git ls-files`."""
    root.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
    }
    subprocess.run(["git", "init", "-q", str(root)], check=True, env=env)
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    subprocess.run(
        ["git", "-C", str(root), "add", "-A", "-f"], check=True, env=env
    )
    return root


def _names(zip_path: Path) -> set[str]:
    with zipfile.ZipFile(zip_path) as zf:
        return set(zf.namelist())


class BundleContentsTests(unittest.TestCase):
    """What the boundary of "our code" actually is."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.staging = self.base / "staging"

    def _stage(self, files: dict[str, str], name: str = "repo-a"):
        repo = _git_repo(self.base / name, files)
        with mock.patch.object(drive_mirror, "CODE_REPOS", ((name, repo),)):
            return drive_mirror.stage_code_bundles(self.staging), repo

    def test_tracked_source_is_bundled_under_the_repo_name(self):
        results, _ = self._stage({"runtime/a.py": "x = 1\n", "README.md": "hi"})
        self.assertEqual(results[0]["status"], "ok")
        dest = self.staging / "code" / "repo-a-source.zip"
        self.assertTrue(dest.is_file())
        self.assertEqual(
            _names(dest), {"repo-a/runtime/a.py", "repo-a/README.md"}
        )

    def test_an_untracked_file_is_not_bundled(self):
        repo = _git_repo(self.base / "repo-a", {"a.py": "x = 1\n"})
        (repo / "secret_notes.md").write_text("not added to git")
        with mock.patch.object(drive_mirror, "CODE_REPOS", (("repo-a", repo),)):
            drive_mirror.stage_code_bundles(self.staging)
        self.assertEqual(
            _names(self.staging / "code" / "repo-a-source.zip"), {"repo-a/a.py"}
        )

    def test_a_tracked_env_file_is_excluded_even_though_git_tracks_it(self):
        """The real case: a private sibling repo tracks derived `.env` copies
        holding real credentials. `git ls-files` is the INCLUDE boundary;
        EXCLUDE_NAME_PATTERNS still wins over it."""
        results, _ = self._stage({
            "a.py": "x = 1\n",
            ".env": "SHOPIFY_TOKEN=shpat_live\n",
            "test/.env.clean": "SHOPIFY_TOKEN=shpat_live\n",
            "test/.env.final": "SHOPIFY_TOKEN=shpat_live\n",
            "etsy_tokens.json": "{}",
            "server.pem": "x",
        })
        self.assertEqual(
            _names(self.staging / "code" / "repo-a-source.zip"), {"repo-a/a.py"}
        )
        self.assertEqual(results[0]["excluded"], 5)

    def test_a_non_repository_is_recorded_not_bundled(self):
        plain = self.base / "not-a-repo"
        plain.mkdir()
        (plain / "a.py").write_text("x = 1\n")
        with mock.patch.object(drive_mirror, "CODE_REPOS", (("plain", plain),)):
            results = drive_mirror.stage_code_bundles(self.staging)
        self.assertEqual(results[0]["status"], "source_missing")
        self.assertFalse((self.staging / "code").exists())


class ACredentialRefusesTheBundleTests(unittest.TestCase):
    """The positive control. Without this the rest proves only that zip works."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.staging = self.base / "staging"
        self.dest = self.staging / "code" / "repo-a-source.zip"

    def _stage(self, files):
        repo = _git_repo(self.base / "repo-a", files)
        with mock.patch.object(drive_mirror, "CODE_REPOS", (("repo-a", repo),)):
            return drive_mirror.stage_code_bundles(self.staging)

    def test_a_planted_credential_refuses_the_bundle(self):
        results = self._stage({"helpers/cfg.py": f'KEY = "{PLANTED_KEY}"\n'})
        self.assertEqual(results[0]["status"], "refused")
        self.assertGreaterEqual(results[0]["finding_count"], 1)
        self.assertFalse(self.dest.exists())

    def test_the_refusal_never_echoes_the_credential(self):
        results = self._stage({"helpers/cfg.py": f'KEY = "{PLANTED_KEY}"\n'})
        self.assertNotIn(PLANTED_KEY, repr(results))

    def test_a_refusal_removes_a_bundle_staged_earlier(self):
        """A clean zip from a previous run is not evidence about the tree that
        just failed, and leaving it staged keeps publishing code while the run
        reports a refusal."""
        self._stage({"a.py": "x = 1\n"})
        self.assertTrue(self.dest.exists())

        repo = self.base / "repo-a"
        (repo / "leak.py").write_text(f'KEY = "{PLANTED_KEY}"\n')
        subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
        with mock.patch.object(drive_mirror, "CODE_REPOS", (("repo-a", repo),)):
            results = drive_mirror.stage_code_bundles(self.staging)

        self.assertEqual(results[0]["status"], "refused")
        self.assertTrue(results[0]["removed_stale_bundle"])
        self.assertFalse(self.dest.exists())

    def test_the_release_gate_would_not_have_caught_it(self):
        """Why this scan exists at all. If a zip were scannable at the gate,
        the pre-zip scan would be belt-and-braces; it is not."""
        self.assertFalse(drive_mirror._is_scannable(Path("code/x-source.zip")))


class TheScannerCanReadEnvVariantsTests(unittest.TestCase):
    """Regression for the blind spot that hid the live keys for 9 months.

    `.env` is in TEXT_EXTENSIONS, which only matched a file whose SUFFIX is
    `.env`. `.env.clean` has suffix `.clean`, so the scan skipped it and
    reported clean.
    """

    def test_env_variants_are_scannable(self):
        for name in (".env", ".env.clean", ".env.clean2", ".env.final",
                     ".env.example", "env.production"):
            with self.subTest(name=name):
                self.assertTrue(drive_mirror._is_scannable(Path("x") / name))

    def test_a_credential_in_an_env_variant_is_found(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / ".env.clean"
            path.write_text(f"OPENAI_API_KEY={PLANTED_KEY}\n")
            findings = drive_mirror._scan_one(path, ".env.clean")
        self.assertTrue(findings)

    def test_an_ordinary_binary_is_still_skipped(self):
        self.assertFalse(drive_mirror._is_scannable(Path("a/photo.png")))


class BundleBytesAreStableTests(unittest.TestCase):
    """An unchanged repo must produce an unchanged file, or Drive re-uploads
    every bundle nightly for nothing."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.staging = self.base / "staging"
        self.repo = _git_repo(self.base / "repo-a", {"a.py": "x = 1\n"})
        self.dest = self.staging / "code" / "repo-a-source.zip"

    def _stage(self):
        with mock.patch.object(
            drive_mirror, "CODE_REPOS", (("repo-a", self.repo),)
        ):
            return drive_mirror.stage_code_bundles(self.staging)

    def test_an_unchanged_repo_is_not_rewritten(self):
        first = self._stage()
        self.assertTrue(first[0]["rewritten"])
        before = self.dest.read_bytes()
        mtime = self.dest.stat().st_mtime_ns

        second = self._stage()
        self.assertFalse(second[0]["rewritten"])
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertEqual(self.dest.stat().st_mtime_ns, mtime)

    def test_a_changed_file_rewrites_the_bundle(self):
        self._stage()
        before = self.dest.read_bytes()
        (self.repo / "a.py").write_text("x = 2\n")

        results = self._stage()
        self.assertTrue(results[0]["rewritten"])
        self.assertNotEqual(self.dest.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
