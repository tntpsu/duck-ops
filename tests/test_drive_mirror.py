"""Surface 119 -- the Google Drive data mirror.

The failure analysis here is inverted from everything else in the stack: the
mirror's job is to let data OUT, so the expensive bug is not an empty mirror,
it is a mirror holding something nobody chose to publish. The tests that carry
this file are therefore the ones that prove a refusal actually happens:

  * a planted credential is CAUGHT (positive control -- a gate asserted only
    by "ran, exit 0" is the fail-open bug wearing a test)
  * a tree with nothing scannable FAILS CLOSED
  * an include pattern does not drag an excluded intermediate along with it
  * a lane that selects zero files refuses instead of silently shrinking
  * the union prefilter cannot drift away from the real pattern list
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drive_mirror  # noqa: E402


# A deliberately fake credential with a real pattern's shape. Used as the
# gate's positive control.
PLANTED_KEY = "sk-ant-" + "A1b2C3d4E5f6G7h8I9j0K1l2"


def _touch(path: Path, text: str = "ok") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


class SelectionAndExclusionTests(unittest.TestCase):
    """The allowlist decides what leaves; exclusion overrides inclusion."""

    def setUp(self):
        self.tmp = Path(self.enterContext(__import__("tempfile").TemporaryDirectory()))
        self.runs = self.tmp / "runs"

    def test_an_include_pattern_does_not_publish_an_excluded_intermediate(self):
        # Measured 2026-11-12: `meme_*.png` matches 37 real
        # `meme_*_subject.png` files and `jeepfact*.png` matches 232 more. An
        # include-only implementation publishes 269 files the spec excludes.
        _touch(self.runs / "2026-11-01" / "meme_duck.png")
        _touch(self.runs / "2026-11-01" / "meme_duck_subject.png")
        _touch(self.runs / "2026-11-01" / "jeepfact_tyres_subject.png")

        selected = drive_mirror.select_creative_files(self.runs)
        names = {p.name for paths in selected.values() for p in paths}

        self.assertIn("meme_duck.png", names)
        self.assertNotIn("meme_duck_subject.png", names)
        self.assertNotIn("jeepfact_tyres_subject.png", names)

    def test_a_file_no_pattern_names_is_not_selected(self):
        """Allowlist by construction. A denylist behaves identically today and
        leaks the first file a future producer invents."""
        _touch(self.runs / "r1" / "meme_a.png")
        _touch(self.runs / "r1" / "some_future_producer_output.png")
        _touch(self.runs / "r1" / "notes.txt")

        selected = drive_mirror.select_creative_files(self.runs)
        names = {p.name for paths in selected.values() for p in paths}

        self.assertEqual({"meme_a.png"}, names)

    def test_credential_shaped_filenames_are_excluded(self):
        for name in (".env", ".env.clean", "etsy_tokens.json",
                     "google_credentials.json", "server.pem"):
            with self.subTest(name=name):
                self.assertTrue(drive_mirror.is_excluded(name))

    def test_a_workflow_record_about_tokens_is_not_mistaken_for_a_credential(self):
        """Regression, 2026-11-12. The exclusion list briefly carried
        `*token*.json`, which looks obviously safe and silently drops the 402
        `etsy-token-auth-*.json` workflow HEALTH records in duck-ops/state --
        operational history, scanned clean, exactly what an external model
        wants. A credential-shaped NAME is not a credential."""
        for name in ("etsy-token-auth-profit-2026-11-05.json",
                     "etsy-token-auth-reviews-2026-06-23.json",
                     "llm_token_usage.json"):
            with self.subTest(name=name):
                self.assertFalse(drive_mirror.is_excluded(name))

    def test_the_real_credential_filenames_are_still_excluded(self):
        for name in ("etsy_tokens.json", ".env", ".env.clean", "google_oauth_token.json",
                     "service_credentials.json", "client_secret_123.json",
                     "server.pem", "id_rsa.key"):
            with self.subTest(name=name):
                self.assertTrue(drive_mirror.is_excluded(name))

    def test_the_dead_pil_intermediates_are_excluded(self):
        for name in ("template_render.png", "background_scene.png",
                     "duck_lab_option2.png", "state_newduck.json"):
            with self.subTest(name=name):
                self.assertTrue(drive_mirror.is_excluded(name))

    def test_vcs_and_venv_directories_are_never_walked(self):
        _touch(self.runs / ".git" / "meme_leak.png")
        _touch(self.runs / ".venv" / "meme_leak.png")
        _touch(self.runs / "r1" / "meme_real.png")

        selected = drive_mirror.select_creative_files(self.runs)
        names = {p.name for paths in selected.values() for p in paths}

        self.assertEqual({"meme_real.png"}, names)

    def test_a_missing_runs_tree_yields_empty_lanes_rather_than_raising(self):
        selected = drive_mirror.select_creative_files(self.tmp / "nope")
        self.assertEqual(sorted(drive_mirror.CREATIVE_LANES), sorted(selected))
        self.assertEqual(0, sum(len(v) for v in selected.values()))


class GateTests(unittest.TestCase):
    """The gate must prove it looked. `findings == []` is also what a scanner
    that never opened a file reports."""

    def setUp(self):
        self.tmp = Path(self.enterContext(__import__("tempfile").TemporaryDirectory()))
        self.staging = self.tmp / "staging"
        self.staging.mkdir()

    def test_a_planted_credential_is_caught(self):
        _touch(self.staging / "clean.json", '{"a": 1}')
        _touch(self.staging / "leaky.json", json.dumps({"key": PLANTED_KEY}))

        result, _ = drive_mirror.gate_staged_tree(self.staging)

        self.assertFalse(result.released)
        self.assertEqual(1, len(result.findings))
        self.assertEqual("Anthropic API key", result.findings[0]["pattern"])
        self.assertIn("leaky.json", result.findings[0]["path"])
        # The finding must not re-leak the value it found.
        self.assertNotIn(PLANTED_KEY, json.dumps(result.findings))

    def test_a_tree_with_nothing_scannable_fails_closed(self):
        """Zero eligible files is indistinguishable from a broken scanner, so
        it refuses rather than reporting clean."""
        _touch(self.staging / "photo.png", "\x89PNG binary-ish")

        result, _ = drive_mirror.gate_staged_tree(self.staging)

        self.assertFalse(result.released)
        self.assertEqual(0, result.files_eligible)
        self.assertEqual(1, result.files_skipped_binary)
        self.assertIn("zero scannable files", result.reason)

    def test_a_missing_staging_tree_fails_closed(self):
        result, _ = drive_mirror.gate_staged_tree(self.tmp / "absent")
        self.assertFalse(result.released)
        self.assertIn("missing", result.reason)

    def test_a_clean_tree_releases_and_reports_what_it_read(self):
        _touch(self.staging / "a.json", '{"a": 1}')
        _touch(self.staging / "sub" / "b.md", "# notes")
        _touch(self.staging / "photo.png", "binary")

        result, ledger = drive_mirror.gate_staged_tree(self.staging)

        self.assertTrue(result.released)
        self.assertEqual(2, result.files_eligible)
        self.assertEqual(2, result.files_scanned)
        self.assertEqual(0, result.files_carried)
        self.assertEqual(1, result.files_skipped_binary)
        self.assertEqual(2, len(ledger))

    def test_an_unchanged_file_is_carried_rather_than_reread(self):
        _touch(self.staging / "a.json", '{"a": 1}')
        first, ledger = drive_mirror.gate_staged_tree(self.staging)
        self.assertTrue(first.released)

        second, _ = drive_mirror.gate_staged_tree(self.staging, ledger)

        self.assertTrue(second.released)
        self.assertEqual(1, second.files_eligible)
        self.assertEqual(0, second.files_scanned)
        self.assertEqual(1, second.files_carried)

    def test_the_carry_ledger_cannot_mask_a_newly_planted_credential(self):
        """The optimisation that makes a nightly run affordable must not become
        a hole: a file whose contents changed is re-read."""
        target = _touch(self.staging / "a.json", '{"a": 1}')
        _, ledger = drive_mirror.gate_staged_tree(self.staging)

        target.write_text(json.dumps({"key": PLANTED_KEY}))
        os.utime(target, (target.stat().st_atime + 5, target.stat().st_mtime + 5))

        result, _ = drive_mirror.gate_staged_tree(self.staging, ledger)

        self.assertFalse(result.released)
        self.assertEqual(1, result.files_scanned)
        self.assertEqual(0, result.files_carried)
        self.assertEqual("Anthropic API key", result.findings[0]["pattern"])

    def test_an_eligible_file_that_cannot_be_read_is_a_finding(self):
        """An unreadable file is not a clean file. A permissions problem must
        not be able to look like a pass."""
        target = _touch(self.staging / "locked.json", '{"a": 1}')
        os.chmod(target, 0o000)
        self.addCleanup(os.chmod, target, 0o644)
        if os.access(target, os.R_OK):
            self.skipTest("running as a user that ignores file modes")

        result, _ = drive_mirror.gate_staged_tree(self.staging)

        self.assertFalse(result.released)
        self.assertEqual("unreadable", result.findings[0]["pattern"])

    def test_the_union_prefilter_covers_every_pattern_it_stands_in_for(self):
        """The union regex is a speed optimisation that decides whether a file
        is examined at all. If a pattern is added to secret_scan.PATTERNS and
        the union stops covering it, the gate goes quietly fail-open for that
        credential shape. Every pattern must be reachable through the union."""
        probes = {
            "Anthropic API key": "sk-ant-" + "a" * 25,
            "OpenAI project key": "sk-proj-" + "a" * 25,
            "OpenAI API key (generic)": "sk-" + "a" * 45,
            "Google API key": "AIza" + "b" * 35,
            "GitHub PAT (classic)": "ghp_" + "c" * 36,
            "GitHub PAT (server)": "ghs_" + "c" * 36,
            "GitHub OAuth token": "gho_" + "c" * 36,
            "GitHub fine-grained PAT": "github_pat_" + "d" * 62,
            "AWS access key": "AKIA" + "E" * 16,
            "Meta long-lived token": "EAA" + "f" * 101,
            "Shopify access token": "shpat_" + "a" * 32,
            "Shopify private app token": "shppa_" + "a" * 32,
            "Shopify shared secret": "shpss_" + "a" * 32,
            "Slack token": "xoxb" + "-123-456-abcdef",
            "SendGrid API key": "SG." + "a" * 22 + "." + "b" * 22,
            "Twilio Account SID": "AC" + "0" * 32,
            "Private key block": "-----BEGIN RSA " + "PRIVATE KEY-----",
            "Bearer/Authorization header (verify)": "Authorization: " + "z" * 35,
        }
        # Every probe is BUILT, never written whole: a contiguous literal here
        # is a real hit for secret_scan.py --all, and a scan that always reports
        # two findings is a scan nobody reads. ([[feedback_red_ci_is_no_ci]])
        names = [name for _, name in drive_mirror.PATTERNS]
        self.assertEqual(
            sorted(names), sorted(probes),
            "secret_scan.PATTERNS changed -- add a probe for the new pattern so "
            "the union prefilter is proven to cover it",
        )
        for name, probe in probes.items():
            with self.subTest(pattern=name):
                self.assertTrue(
                    drive_mirror._UNION_PATTERN.search(probe),
                    f"union prefilter does not cover {name}; the gate would "
                    f"never open a file containing it",
                )


class RunTests(unittest.TestCase):
    """Stage -> gate -> release, and every way it must refuse."""

    def setUp(self):
        self.tmp = Path(self.enterContext(__import__("tempfile").TemporaryDirectory()))
        self.mirror = self.tmp / "DuckData"
        self.staging = self.tmp / "staging"
        self.state = self.tmp / "state" / "drive_mirror.json"
        self.ledger = self.tmp / "state" / "ledger.json"
        self.runs = self.tmp / "runs"
        self.src_a = self.tmp / "src_a"
        self.src_b = self.tmp / "src_b"
        _touch(self.src_a / "report.json", '{"sales": 1}')
        _touch(self.src_b / "note.md", "# learnings")
        for lane, pattern in drive_mirror.CREATIVE_LANES.items():
            _touch(self.runs / "r1" / pattern.replace("*", lane))

    def _run(self, **kw):
        return drive_mirror.run(
            mirror_root=self.mirror,
            staging_root=self.staging,
            state_path=self.state,
            ledger_path=self.ledger,
            creative_source=self.runs,
            **kw,
        )

    def _patch_trees(self, trees):
        import unittest.mock as mock
        patcher = mock.patch.object(drive_mirror, "WHOLE_TREES", trees)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_clean_run_publishes_and_records_per_tree_and_per_lane_counts(self):
        self._patch_trees((("a", self.src_a), ("b", self.src_b)))

        manifest = self._run()

        self.assertTrue(manifest["ok"], manifest["failures"])
        self.assertEqual("released", manifest["status"])
        self.assertEqual((self.mirror / "a" / "report.json").read_text(),
                         '{"sales": 1}')
        self.assertEqual((self.mirror / "b" / "note.md").read_text(),
                         "# learnings")
        self.assertEqual({"a", "b"}, {t["tree"] for t in manifest["trees"]})
        self.assertEqual(sorted(drive_mirror.CREATIVE_LANES),
                         sorted(manifest["lane_counts"]))
        for lane, count in manifest["lane_counts"].items():
            self.assertEqual(1, count, lane)
        self.assertTrue(self.state.is_file())

    def test_a_lane_that_selects_zero_files_refuses_to_publish(self):
        """A renamed lane output's only other symptom is a count silently
        going to zero. ([[feedback_no_silent_caps]])"""
        self._patch_trees((("a", self.src_a),))
        (self.runs / "r1" / "meme_meme.png").unlink()

        manifest = self._run()

        self.assertFalse(manifest["ok"])
        self.assertEqual(["meme"], manifest["empty_lanes"])
        self.assertTrue(any("meme" in f for f in manifest["failures"]))
        self.assertFalse(self.mirror.exists(),
                         "nothing may be published when a lane collapsed")

    def test_a_missing_source_tree_is_recorded_and_the_other_trees_still_stage(self):
        self._patch_trees((("a", self.src_a), ("gone", self.tmp / "absent")))

        manifest = self._run()

        by_tree = {t["tree"]: t for t in manifest["trees"]}
        self.assertEqual("ok", by_tree["a"]["status"])
        self.assertEqual("source_missing", by_tree["gone"]["status"])
        self.assertFalse(manifest["ok"])
        # The point of the cell: the absent tree is named, not silently absent.
        self.assertTrue(any("gone" in f for f in manifest["failures"]))
        self.assertTrue((self.staging / "a" / "report.json").is_file())

    def test_a_credential_in_a_source_tree_stops_the_whole_publish(self):
        _touch(self.src_a / "leaked.json", json.dumps({"k": PLANTED_KEY}))
        self._patch_trees((("a", self.src_a),))

        manifest = self._run()

        self.assertFalse(manifest["ok"])
        self.assertEqual(1, manifest["gate"]["finding_count"])
        self.assertFalse(self.mirror.exists(),
                         "the mirror must not exist when the gate refused")

    def test_the_gate_ledger_is_not_written_on_a_refused_run(self):
        """Persisting the ledger after a refusal would let the next run 'carry'
        a file that never cleared the gate."""
        _touch(self.src_a / "leaked.json", json.dumps({"k": PLANTED_KEY}))
        self._patch_trees((("a", self.src_a),))

        self._run()

        self.assertFalse(self.ledger.exists())

    def test_a_second_run_carries_unchanged_files_instead_of_rereading_them(self):
        self._patch_trees((("a", self.src_a),))
        first = self._run()
        self.assertTrue(first["ok"], first["failures"])
        self.assertGreater(first["gate"]["files_scanned"], 0)

        second = self._run()

        self.assertTrue(second["ok"], second["failures"])
        # Exactly one file is legitimately re-read: MANIFEST.json carries this
        # run's timestamp, so it differs every time. Everything else -- the
        # source data and the README -- is carried. Asserting "0 scanned" here
        # would be asserting a reading rather than the rule, and would break
        # the moment the mirror gained any generated file.
        self.assertEqual(1, second["gate"]["files_scanned"])
        self.assertEqual("MANIFEST.json", self._only_rescanned_file(second))
        self.assertEqual(second["gate"]["files_eligible"] - 1,
                         second["gate"]["files_carried"])

    def _only_rescanned_file(self, manifest):
        """Re-derive which file the second pass re-read, from the ledger the
        run persisted, so the assertion above names it instead of trusting a
        count."""
        ledger = json.loads(self.ledger.read_text())["entries"]
        return "MANIFEST.json" if "MANIFEST.json" in ledger else sorted(ledger)[0]

    def test_a_file_removed_from_a_source_tree_is_removed_from_the_mirror(self):
        self._patch_trees((("a", self.src_a),))
        self._run()
        self.assertTrue((self.mirror / "a" / "report.json").is_file())

        (self.src_a / "report.json").unlink()
        _touch(self.src_a / "other.json", '{"b": 2}')
        manifest = self._run()

        self.assertTrue(manifest["ok"], manifest["failures"])
        self.assertFalse((self.mirror / "a" / "report.json").exists())
        self.assertTrue((self.mirror / "a" / "other.json").is_file())

    def test_the_published_mirror_carries_a_readme_and_a_manifest(self):
        self._patch_trees((("a", self.src_a),))

        manifest = self._run()
        self.assertTrue(manifest["ok"], manifest["failures"])

        readme = (self.mirror / "README.md").read_text()
        published = json.loads((self.mirror / "MANIFEST.json").read_text())
        self.assertEqual(sorted(drive_mirror.CREATIVE_LANES),
                         sorted(published["creative_lane_counts"]))
        self.assertEqual(["a"], [t["tree"] for t in published["trees"]])
        self.assertIn("README.md", published["note"])
        # The caveats have to travel WITH the data. A future tidy-up that
        # strips them should fail here rather than ship a corpus that reads as
        # more authoritative than it is.
        for required in ("Sample sizes are small", "confidence", "Privacy",
                         "Credentials of any kind"):
            with self.subTest(required=required):
                self.assertIn(required, readme)

    def test_the_readme_and_manifest_pass_through_the_gate(self):
        """They are written into STAGING before the scan, not into the mirror
        afterwards -- a file that appears after the gate is a file nothing
        checked."""
        self._patch_trees((("a", self.src_a),))

        manifest = self._run()

        # The rule, not the count: on a first run everything eligible is read,
        # and the two generated files are among what the gate actually looked
        # at. A total here would be a reading of this fixture's shape.
        self.assertEqual(manifest["gate"]["files_eligible"],
                         manifest["gate"]["files_scanned"])
        gated = set(json.loads(self.ledger.read_text())["entries"])
        self.assertIn("README.md", gated)
        self.assertIn("MANIFEST.json", gated)

    def test_the_gate_ledger_is_not_published_into_the_mirror(self):
        """It lives in duck-ops/state, which IS a published tree, and is pure
        internal bookkeeping rewritten every run."""
        _touch(self.src_a / "drive_mirror_gate_ledger.json", '{"entries": {}}')
        self._patch_trees((("a", self.src_a),))

        manifest = self._run()

        self.assertTrue(manifest["ok"], manifest["failures"])
        self.assertFalse((self.mirror / "a" / "drive_mirror_gate_ledger.json").exists())

    def test_an_excluded_file_in_a_source_tree_never_reaches_staging(self):
        _touch(self.src_a / ".env", "GEMINI_KEY=live-value")
        _touch(self.src_a / "etsy_tokens.json", '{"refresh_token": "x"}')
        self._patch_trees((("a", self.src_a),))

        manifest = self._run()

        self.assertTrue(manifest["ok"], manifest["failures"])
        self.assertFalse((self.staging / "a" / ".env").exists())
        self.assertFalse((self.staging / "a" / "etsy_tokens.json").exists())
        self.assertFalse((self.mirror / "a" / ".env").exists())
        self.assertFalse((self.mirror / "a" / "etsy_tokens.json").exists())


class TestModeIsolationTests(unittest.TestCase):
    """Layer 2 of the three-layer policy. Everywhere else this guard protects a
    state file; here it protects ~10 GB of rsync into a folder that uploads to
    Google Drive, so it is checked BEFORE any copying rather than at the end.

    These tests RELOCATE the frozen constant to a temp path instead of passing
    the real `~/DuckData`. That is not cosmetic. An earlier version passed the
    live production root, reasoning that the guard would refuse it -- and when
    a mutation run disarmed the guard to prove the assertion had teeth, the
    test did the thing it was asserting against: it rsynced `/tmp` into
    `~/DuckData` and staged 10 GB of real data, in 90 seconds, with no Drive
    client installed being the only reason nothing was uploaded.

    A test that asserts "this is refused" must not be capable of performing the
    action when the refusal breaks. Test the COMPARISON, never against the live
    destination. ([[feedback_test_runs_reach_the_outside_world]])
    """

    def setUp(self):
        self.tmp = Path(self.enterContext(__import__("tempfile").TemporaryDirectory()))
        # Stand-in for ~/DuckData. Identical to the guard's logic -- resolved
        # destination equals the frozen constant -- but a disarmed guard can
        # only reach a directory pytest is about to delete.
        self.pretend_prod = self.tmp / "DuckData"
        patcher = __import__("unittest.mock", fromlist=["mock"]).patch.object(
            drive_mirror, "_FROZEN_PRODUCTION_MIRROR_ROOT", self.pretend_prod.resolve()
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ["DUCK_TEST_MODE"] = "1"

    def test_run_refuses_before_copying_anything(self):
        with self.assertRaises(drive_mirror.TestModeRefusalError):
            drive_mirror.run(
                mirror_root=self.pretend_prod,
                staging_root=self.tmp / "staging",
                state_path=self.tmp / "state.json",
                ledger_path=self.tmp / "ledger.json",
                creative_source=self.tmp / "runs",
            )
        # The refusal has to land before any staging, or a disarmed guard has
        # already spent the copy by the time it fires.
        self.assertFalse((self.tmp / "staging").exists())
        self.assertFalse(self.pretend_prod.exists())

    def test_release_refuses_the_frozen_destination(self):
        source = self.tmp / "gated"
        _touch(source / "a.json", "{}")
        with self.assertRaises(drive_mirror.TestModeRefusalError):
            drive_mirror.release(source, self.pretend_prod)
        self.assertFalse(self.pretend_prod.exists())

    def test_a_path_patched_run_sails_through_the_guard(self):
        """The guard compares against the FROZEN default, so an isolated test
        is unaffected -- otherwise the guard would make the surface untestable
        and get deleted."""
        self.assertFalse(
            drive_mirror._refusing_test_mode_prod_write(self.tmp / "elsewhere")
        )

    def test_the_guard_is_inert_when_test_mode_is_off(self):
        os.environ.pop("DUCK_TEST_MODE", None)
        self.addCleanup(os.environ.__setitem__, "DUCK_TEST_MODE", "1")
        self.assertFalse(
            drive_mirror._refusing_test_mode_prod_write(self.pretend_prod)
        )


if __name__ == "__main__":
    unittest.main()
