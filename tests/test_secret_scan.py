"""The secret scanner's own tests -- it had none in any repo until 2026-11-16.

That absence is why the leak below survived nine months: `.env` IS in
`TEXT_EXTENSIONS`, so nothing in the config said "skip env files", but the list
is matched against `path.suffix` and the suffix of `.env.clean` is `.clean`.
Three git-tracked files in duckAgent (`test/.env.clean`, `.env.clean2`,
`.env.final`) held the live Shopify / Etsy / OpenAI credentials and the scan
called the repo clean from the 2026-02-24 initial commit until 2026-11-13.

So the bulk of this file tests the SELECTION predicate, not the patterns. A
scan reporting zero findings is a statement about which files it chose to open;
the patterns only matter for the files that get that far.

Every token below is a fake built from a repeated literal so its length is
provable by construction -- if a length is wrong the pattern simply will not
fire and the test says so. The file is named `test_secret_scan.py` because
`SKIP_PATH_FRAGMENTS` carries the fragment `test_secret_scan`: that is what
keeps these fixtures from making the live `--all` scan report findings on
itself. `test_the_live_scan_skips_this_very_file` pins that, because renaming
this file would silently arm 18 false positives.
"""
from __future__ import annotations

import importlib.util
import io
import subprocess
import sys
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCANNER_PATH = REPO_ROOT / "scripts" / "secret_scan.py"


def _load_scanner():
    """Import the scanner by path -- `scripts/` is not on sys.path and the
    conftest only adds `runtime/`. Loading by path also keeps the test honest
    about WHICH file production runs."""
    spec = importlib.util.spec_from_file_location("duck_secret_scan", SCANNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


secret_scan = _load_scanner()


# One fake per pattern, keyed by the pattern's own name string so a renamed or
# removed pattern surfaces as a failure here rather than as silent loss of
# coverage. `test_every_pattern_has_a_fixture` is what keeps this honest when
# someone adds the nineteenth pattern.
PATTERN_FIXTURES: dict[str, str] = {
    "Anthropic API key": "sk-ant-api03-" + "F" * 30,
    "OpenAI project key": "sk-proj-" + "F" * 24,
    "OpenAI API key (generic)": "sk-" + "F" * 48,
    "Google API key": "AIza" + "F" * 35,
    "GitHub PAT (classic)": "ghp_" + "F" * 36,
    "GitHub PAT (server)": "ghs_" + "F" * 36,
    "GitHub OAuth token": "gho_" + "F" * 36,
    "GitHub fine-grained PAT": "github_pat_" + "F" * 60,
    "AWS access key": "AKIA" + "FAKE0000EXAMPLE0",
    "Meta long-lived token": "EAA" + "F" * 100,
    "Shopify access token": "shpat_" + "ab" * 16,
    "Shopify private app token": "shppa_" + "ab" * 16,
    "Shopify shared secret": "shpss_" + "ab" * 16,
    "Slack token": "xoxb-1111111111-2222222222-" + "F" * 24,
    "SendGrid API key": "SG." + "F" * 22 + "." + "F" * 22,
    # The Twilio pattern ends in \b, so the fake needs a non-word char after it.
    "Twilio Account SID": "AC" + "ab" * 16 + " trailing",
    "Private key block": "-----BEGIN RSA PRIVATE KEY-----",
    "Bearer/Authorization header (verify)": "Authorization: Bearer " + "F" * 32,
}

# A real value from the leak's shape, used for the end-to-end regression. Fake
# token, real filename.
LEAK_SHAPE_LINE = 'SHOPIFY_TOKEN=shpat_' + "cd" * 16


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args],
                   check=True, capture_output=True, text=True)


def _make_repo(root: Path, tracked: dict[str, str] | None = None,
               untracked: dict[str, str] | None = None,
               gitignore: str | None = None) -> Path:
    """A real git repo, because the scanner's boundary IS `git ls-files`.

    Files are staged, never committed: `ls-files` reads the index, so staging
    is the whole contract and a commit would only need a user identity the CI
    environment may not have.
    """
    root.mkdir(parents=True, exist_ok=True)
    _git(root, "init", "-q")
    if gitignore is not None:
        (root / ".gitignore").write_text(gitignore, encoding="utf-8")
    for rel, body in (tracked or {}).items():
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(body, encoding="utf-8")
    for rel, body in (untracked or {}).items():
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(body, encoding="utf-8")
    if tracked:
        _git(root, "add", "--", *tracked.keys())
    return root


class TheSelectionPredicateTests(unittest.TestCase):
    """`_is_scannable` is where the nine-month blind spot lived."""

    def test_env_files_with_a_derived_suffix_are_scanned(self):
        # The actual leak. A derived name is the normal way these get made --
        # someone copies `.env`, works on the copy, and the copy keeps the
        # credentials. Every one of these has a suffix that is NOT `.env`.
        for name in (".env", ".env.clean", ".env.clean2", ".env.final",
                     ".env.local", ".env.bak", ".env.orig", ".env.backup",
                     ".env.production", ".env.save", "env.production",
                     "env.sh.bak", ".ENV.CLEAN"):
            with self.subTest(name=name):
                self.assertTrue(
                    secret_scan._is_scannable(Path("test") / name),
                    f"{name} must be scanned -- this is the 2026-11-13 leak shape")

    def test_known_text_extensions_are_scanned(self):
        for name in ("a.py", "a.json", "a.yaml", "a.sh", "a.md", "a.plist",
                     "a.toml", "a.ts", "a.sql", "a.conf"):
            with self.subTest(name=name):
                self.assertTrue(secret_scan._is_scannable(Path(name)))

    def test_extensionless_files_are_scanned(self):
        # Scripts and dotfiles carry credentials as often as .py files do.
        for name in ("Makefile", "Dockerfile", ".gitignore", "run_the_thing"):
            with self.subTest(name=name):
                self.assertTrue(secret_scan._is_scannable(Path(name)))

    def test_binary_and_archive_files_are_not_scanned(self):
        # This blindness is deliberate and load-bearing elsewhere: because a
        # zip is skipped here, `drive_mirror.stage_code_bundles` scans every
        # member BEFORE the archive exists, and that pre-zip pass is the only
        # scan a bundle ever gets. See
        # tests/test_drive_mirror_code_bundles.py::
        #   ACredentialRefusesTheBundleTests::
        #   test_the_release_gate_would_not_have_caught_it
        for name in ("code/duck-ops-source.zip", "a.png", "a.mp4", "a.pyc",
                     "a.jpg", "a.woff2", "a.tar.gz"):
            with self.subTest(name=name):
                self.assertFalse(secret_scan._is_scannable(Path(name)))

    def test_skip_fragments_win_over_a_scannable_extension(self):
        for name in ("node_modules/pkg/index.js",
                     ".venv/lib/python3.12/site-packages/x.py",
                     "runtime/__pycache__/x.py",
                     "web/dist/bundle.js",
                     "web/build/out.js",
                     "static/jquery.min.js",
                     "static/site.min.css"):
            with self.subTest(name=name):
                self.assertFalse(secret_scan._is_scannable(Path(name)))

    def test_the_scanner_does_not_scan_its_own_pattern_strings(self):
        self.assertFalse(secret_scan._is_scannable(SCANNER_PATH))

    def test_the_live_scan_skips_this_very_file(self):
        # These fixtures are real token SHAPES. They stay invisible to
        # `secret_scan.py --all` only because SKIP_PATH_FRAGMENTS contains
        # "test_secret_scan". Rename this file and the live scan starts
        # reporting 18 findings in duck-ops, which is how a scanner gets
        # muted by its own noise.
        self.assertFalse(secret_scan._is_scannable(Path(__file__).resolve()))
        self.assertIn("test_secret_scan", secret_scan.SKIP_PATH_FRAGMENTS)


class EveryPatternFiresTests(unittest.TestCase):
    """One fake per pattern, all in one repo, so a pattern that is shadowed by
    another pattern's match still has to produce its own finding."""

    @classmethod
    def setUpClass(cls):
        import tempfile
        cls._tmp = tempfile.TemporaryDirectory()
        tracked = {
            f"fixture_{i:02d}.txt": f"KEY_FOR_TEST = {sample}\n"
            for i, sample in enumerate(PATTERN_FIXTURES.values())
        }
        cls.repo = _make_repo(Path(cls._tmp.name) / "repo", tracked=tracked)
        cls.findings = secret_scan.scan_repo(cls.repo)
        cls.found_names = {f["pattern"] for f in cls.findings}

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_each_pattern_matches_its_fixture(self):
        for name in PATTERN_FIXTURES:
            with self.subTest(pattern=name):
                self.assertIn(name, self.found_names)

    def test_every_pattern_has_a_fixture(self):
        # The guard that keeps this suite from rotting: a pattern added to
        # PATTERNS without a fixture here is an untested pattern, and an
        # untested pattern is indistinguishable from a broken one.
        declared = {name for _, name in secret_scan.PATTERNS}
        self.assertEqual(
            declared - set(PATTERN_FIXTURES), set(),
            "pattern(s) in PATTERNS have no fixture in PATTERN_FIXTURES")
        self.assertEqual(
            set(PATTERN_FIXTURES) - declared, set(),
            "fixture(s) name a pattern that no longer exists in PATTERNS")

    def test_a_finding_carries_path_line_and_pattern(self):
        one = self.findings[0]
        self.assertEqual(set(one), {"repo", "path", "line", "pattern",
                                    "redacted_excerpt"})
        self.assertEqual(one["line"], 1)
        self.assertTrue(one["path"].startswith("fixture_"))


class CleanTextDoesNotTripTheScannerTests(unittest.TestCase):
    """False positives mute a scanner as effectively as false negatives."""

    def test_ordinary_code_and_prose_are_clean(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(
                Path(tmp) / "repo",
                tracked={
                    "mod.py": (
                        "import os\n"
                        "TOKEN = os.getenv('SHOPIFY_TOKEN')\n"
                        "HEADERS = {'Authorization': f'Bearer {TOKEN}'}\n"
                        "# sk-ant-REDACTED\n"
                        "sha = 'a' * 40  # a git sha is 40 hex chars\n"
                    ),
                    "notes.md": (
                        "The key lives in .env as SHOPIFY_TOKEN.\n"
                        "Rotate it in the Shopify admin under Apps.\n"
                    ),
                    "conf.json": '{"api_key_env": "OPENAI_API_KEY"}\n',
                })
            self.assertEqual(secret_scan.scan_repo(repo), [])


class TheSecretIsNeverEchoedTests(unittest.TestCase):
    """The scan output is meant to be safe to paste into a ticket."""

    def test_redact_keeps_only_the_ends(self):
        snippet = "shpat_" + "ab" * 16
        out = secret_scan._redact(snippet)
        self.assertNotIn(snippet, out)
        self.assertIn(str(len(snippet)), out)
        self.assertTrue(out.startswith(snippet[:4]))
        self.assertTrue("ab" * 8 not in out)

    def test_a_short_match_is_redacted_entirely(self):
        self.assertEqual(secret_scan._redact("shortish"), "***redacted***")

    def test_no_finding_field_contains_the_full_secret(self):
        import tempfile
        token = "shpat_" + "ef" * 16
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo",
                              tracked={"leak.py": f"T = '{token}'\n"})
            findings = secret_scan.scan_repo(repo)
            self.assertEqual(len(findings), 1)
            for key, value in findings[0].items():
                with self.subTest(field=key):
                    self.assertNotIn(token, str(value))

    def test_the_printed_report_does_not_contain_the_full_secret(self):
        import tempfile
        token = "ghp_" + "F" * 36
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo",
                              tracked={"leak.py": f"T = '{token}'\n"})
            buf, err = io.StringIO(), io.StringIO()
            with redirect_stdout(buf), redirect_stderr(err):
                rc = secret_scan.main([str(repo)])
            report = buf.getvalue() + err.getvalue()
            self.assertEqual(rc, 1)
            self.assertNotIn(token, report)
            self.assertIn("leak.py", report)
            self.assertIn("GitHub PAT (classic)", report)


class TheBoundaryIsGitTrackingTests(unittest.TestCase):
    """`git ls-files` is the scanner's whole notion of what exists."""

    def test_an_untracked_file_is_not_scanned(self):
        import tempfile
        token = "shpat_" + "ab" * 16
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo",
                              tracked={"ok.py": "x = 1\n"},
                              untracked={"scratch.py": f"T = '{token}'\n"})
            self.assertEqual(secret_scan.scan_repo(repo), [])

    def test_a_gitignored_env_file_is_not_scanned(self):
        # The docstring's claim that live `.env` is "automatically excluded"
        # holds only because it is ignored, never because of its name.
        import tempfile
        token = "shpat_" + "ab" * 16
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo",
                              tracked={"ok.py": "x = 1\n"},
                              untracked={".env": f"SHOPIFY_TOKEN={token}\n"},
                              gitignore=".env\n")
            self.assertEqual(secret_scan.scan_repo(repo), [])

    def test_a_tracked_env_copy_is_found(self):
        # End-to-end regression for the real leak: same filenames, same repo
        # layout, fake token.
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo", tracked={
                "test/.env.clean": LEAK_SHAPE_LINE + "\n",
                "test/.env.clean2": LEAK_SHAPE_LINE + "\n",
                "test/.env.final": LEAK_SHAPE_LINE + "\n",
            })
            findings = secret_scan.scan_repo(repo)
            self.assertEqual(
                {f["path"] for f in findings},
                {"test/.env.clean", "test/.env.clean2", "test/.env.final"})

    def test_a_tracked_but_deleted_file_does_not_crash_the_scan(self):
        import tempfile
        token = "shpat_" + "ab" * 16
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo",
                              tracked={"gone.py": f"T = '{token}'\n",
                                       "here.py": "x = 1\n"})
            (repo / "gone.py").unlink()  # still in the index
            self.assertEqual(secret_scan.scan_repo(repo), [])

    def test_the_reported_line_number_is_the_matching_line(self):
        import tempfile
        token = "AKIA" + "FAKE0000EXAMPLE0"
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo", tracked={
                "mod.py": "# line one\n# line two\n# line three\n"
                          f"KEY = '{token}'\n"})
            findings = secret_scan.scan_repo(repo)
            self.assertEqual([f["line"] for f in findings], [4])


class ExitCodeTests(unittest.TestCase):
    """`/deploy-check` BLOCKS on a non-zero exit, so the code is the contract."""

    def _run(self, argv: list[str]) -> int:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return secret_scan.main(argv)

    def test_clean_repo_exits_zero(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo", tracked={"ok.py": "x = 1\n"})
            self.assertEqual(self._run([str(repo)]), 0)

    def test_a_finding_exits_one(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo",
                              tracked={"leak.py":
                                       f"T = '{'shpat_' + 'ab' * 16}'\n"})
            self.assertEqual(self._run([str(repo)]), 1)

    def test_a_non_git_path_is_skipped_not_reported_clean(self):
        # Fail-visible, not fail-silent: a mistyped path must not read as a
        # clean scan. The skip goes to stderr and the exit stays 0, so the
        # message is the only signal -- assert it exists.
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            plain = Path(tmp) / "not_a_repo"
            plain.mkdir()
            err = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(err):
                rc = secret_scan.main([str(plain)])
            self.assertEqual(rc, 0)
            self.assertIn("not a git repo", err.getvalue())


class OwnedReposTests(unittest.TestCase):
    def test_all_scans_the_three_owned_repos_and_not_openclaw(self):
        # openclaw is third-party with mock-token fixtures; scanning it by
        # default would bury real findings under 27 known false positives.
        self.assertEqual(len(secret_scan._OWNED_REPOS), 3)
        self.assertTrue(all("openclaw" not in p for p in secret_scan._OWNED_REPOS))
        for path in secret_scan._OWNED_REPOS:
            with self.subTest(repo=path):
                self.assertTrue(Path(path).name in
                                {"duck-ops", "duckAgent", "paint-to-print-3d"})


if __name__ == "__main__":  # pragma: no cover
    unittest.main()


class SecretNamedKeysWithoutAPrefixTests(unittest.TestCase):
    """The 2026-11-16 addition: credentials with no distinctive prefix.

    The leak had five credentials and PATTERNS matched two. These tests pin the
    rule that closes the other three, and -- more importantly -- pin WHY it is
    keyed on the variable name rather than on entropy.
    """

    def _scan(self, files: dict[str, str]) -> list[dict]:
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            repo = _make_repo(Path(tmp) / "repo", tracked=files)
            return secret_scan.scan_repo(repo)

    def test_the_three_prefixless_credentials_are_found(self):
        # Real names and real lengths from test/.env.clean; fake values.
        for key, value in (("ETSY_API_KEY", "v7fq8x2mz0kd4ptw9r3o"),
                           ("PHOTOROOM_API_KEY", "sk_p" + "7bq3mz8xv2ld9ktw" * 2),
                           ("SMTP_PASS", "vheoqxzlmrktpqmb"),
                           ("AWS_SECRET_ACCESS_KEY", "9f2kqz7mxv3bld8twr4npc6h"),
                           ("SHOPIFY_TOKEN_SECRET", "q8mz3xv7bld9ktwr2npc")):
            with self.subTest(key=key):
                self.assertGreaterEqual(len(value), 16, "fixture too short to qualify")
                findings = self._scan({".env.leaked": f"{key}={value}\n"})
                self.assertEqual(len(findings), 1, f"{key} not flagged")
                self.assertIn(key, findings[0]["pattern"])
                self.assertNotIn(value, findings[0]["redacted_excerpt"])

    def test_entropy_alone_would_have_flagged_the_harmless_values(self):
        # The measurement that decided the design, kept as a test so nobody
        # "simplifies" the rule into a bare entropy threshold later. These are
        # real values from the same file as the real SMTP_PASS, and they score
        # HIGHER than it does.
        real_secret = "vheoqxzlmrktpqmb"                    # SMTP_PASS, H=3.88
        harmless = {
            "PHOTOROOM_BG_PROMPT": "a clean studio backdrop, soft light, no props",
            "MEME_TPL_SPLIT_CTA": "meme_split_cta_template_v2_final_x",
            "WEEKLY_TOP_DUCKS_TEMPLATE_ID": "d41d8cd98f00b204e9800998",
        }
        for name, value in harmless.items():
            with self.subTest(name=name):
                self.assertGreaterEqual(
                    secret_scan._shannon_entropy(value),
                    secret_scan.MIN_SECRET_VALUE_ENTROPY,
                    "fixture should clear the entropy floor -- that is the point")
        self.assertGreater(
            secret_scan._shannon_entropy(harmless["PHOTOROOM_BG_PROMPT"]),
            secret_scan._shannon_entropy(real_secret),
            "the harmless prompt really does out-score the real password")
        # ...and none of them is flagged, because none is keyed on a secret word.
        body = "".join(f"{k}={v}\n" for k, v in harmless.items())
        self.assertEqual(self._scan({".env.harmless": body}), [])

    def test_placeholder_values_stay_quiet(self):
        # A tracked `.env.example` is normal and its values are placeholders.
        # If this rule fires there it gets switched off, and then it protects
        # nothing ([[feedback_no_silent_caps]] -- a muted guard is no guard).
        for value in ("your_token_here_goes_here", "CHANGEME_CHANGEME_CHANGEME",
                      "${SHOPIFY_TOKEN}", "$SHOPIFY_TOKEN",
                      "<insert-your-key-here>", "xxxxxxxxxxxxxxxxxxxx",
                      "aaaaaaaaaaaaaaaaaaaaaa", "replace-with-real-key",
                      "example-key-not-real-value", "sk-ant-REDACTED-example"):
            with self.subTest(value=value):
                self.assertEqual(
                    self._scan({".env.example": f"SHOPIFY_TOKEN={value}\n"}), [],
                    f"placeholder {value!r} must not be reported")

    def test_a_short_value_is_not_key_material(self):
        fifteen = "q8mz3xv7bld9ktw"
        self.assertEqual(len(fifteen), 15)
        self.assertEqual(self._scan({".env.x": f"API_KEY={fifteen}\n"}), [])
        sixteen = fifteen + "r"
        self.assertEqual(len(self._scan({".env.x": f"API_KEY={sixteen}\n"})), 1)

    def test_the_rule_is_scoped_to_env_shaped_files(self):
        # `TOKEN = os.getenv(...)` and `{"api_key_env": "OPENAI_API_KEY"}` are
        # everywhere in .py/.json and carry no secret, so the generic rule must
        # not run there. The prefix PATTERNS still do.
        body = "SHOPIFY_TOKEN=q8mz3xv7bld9ktwr2npc6h\n"
        self.assertEqual(self._scan({"settings.py": body}), [])
        self.assertEqual(self._scan({"settings.json": body}), [])
        self.assertEqual(len(self._scan({".env.staging": body})), 1)

    def test_a_prefix_match_is_not_reported_twice(self):
        # shpat_ is both a PATTERN match and a secret-named assignment. One
        # line, one finding, and it is the specific one.
        token = "shpat_" + "ab" * 16
        findings = self._scan({".env.dup": f"SHOPIFY_TOKEN={token}\n"})
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0]["pattern"], "Shopify access token")

    def test_export_and_quoting_forms_are_handled(self):
        value = "q8mz3xv7bld9ktwr2npc6h"
        for line in (f'export ETSY_API_KEY={value}',
                     f'ETSY_API_KEY="{value}"',
                     f"ETSY_API_KEY='{value}'",
                     f"ETSY_API_KEY = {value}"):
            with self.subTest(line=line):
                self.assertEqual(
                    len(self._scan({".env.forms": line + "\n"})), 1, line)

    def test_comments_and_blank_lines_are_ignored(self):
        value = "q8mz3xv7bld9ktwr2npc6h"
        body = f"# ETSY_API_KEY={value}\n\n   \n#SMTP_PASS={value}\n"
        self.assertEqual(self._scan({".env.commented": body}), [])

    def test_a_key_named_for_an_identifier_is_not_a_secret(self):
        # TEMPLATE_ID / API_VERSION / _DOMAIN are the shapes that share a file
        # with the real keys. The name anchor is what keeps them out.
        for key in ("WEEKLY_TOP_DUCKS_TEMPLATE_ID", "GTDF_TEMPLATE_ID",
                    "SHOPIFY_API_VERSION", "SHOPIFY_DOMAIN", "SMTP_USER",
                    "EMAIL_TO", "MEME_TEMPLATES_ALLOWED", "ETSY_KEYSTRING_ID"):
            with self.subTest(key=key):
                self.assertEqual(
                    self._scan({".env.ids": f"{key}=q8mz3xv7bld9ktwr2npc6h\n"}), [],
                    f"{key} is an identifier, not a credential")
