"""The one versioned SEO rules config, and the invariants that make it safe.

Before seo_rules_version 1 the same two fields carried three different bands:

    duckAgent/helpers/seo_rules.py          newduck Shopify title   70-80
    duck-ops/runtime/shopify_seo_review.py  the generator           45-70
    duck-ops/runtime/shopify_seo_audit.py   the auditor             35-70

So the newduck builder aimed at a window in which 9 of its 11 possible lengths
were rejects of the auditor two repos over, and nothing anywhere compared the
two numbers. The fingerprint was visible in the live store the whole time: 20
product titles sat at exactly 70 characters, pinned against a ceiling their own
producer was trying to exceed.

These tests pin the RELATIONSHIPS, never today's numbers
([[feedback_pin_the_rule_not_the_reading]]) -- a test asserting "the title band
is 45-70" would fail the moment the operator makes a deliberate bump, which is
the one thing this design is for.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import seo_rules_config as config  # noqa: E402

from _siblings import sibling_repo, skip_without_siblings  # noqa: E402


class TheGenerateBandAlwaysFitsInsideTheAcceptBand(unittest.TestCase):
    """The invariant that stops the lane looping on its own output.

    If a producer may emit a value the auditor rejects, the lane re-flags copy
    it just wrote and proposes a fix for its own work forever. duck-ops/TESTS.md
    recorded that risk as a warning to be careful; the subset relation makes it
    a structural guarantee instead.
    """

    def test_every_field_in_every_platform(self):
        rules = config.load_seo_rules()
        checked = 0
        for platform, profile in (rules.get("platforms") or {}).items():
            for field in (profile.get("fields") or {}):
                with self.subTest(platform=platform, field=field):
                    g_lo, g_hi = config.field_band(platform, field, "generate")
                    a_lo, a_hi = config.field_band(platform, field, "accept")
                    self.assertLessEqual(
                        a_lo, g_lo,
                        f"{platform}.{field}: the generator may emit {g_lo} chars, "
                        f"which the auditor rejects below {a_lo}",
                    )
                    self.assertLessEqual(
                        g_hi, a_hi,
                        f"{platform}.{field}: the generator may emit {g_hi} chars, "
                        f"which the auditor rejects above {a_hi}",
                    )
                    checked += 1
        self.assertGreater(checked, 0, "no fields were checked at all")

    def test_a_band_is_ordered_and_positive(self):
        rules = config.load_seo_rules()
        for platform, profile in (rules.get("platforms") or {}).items():
            for field in (profile.get("fields") or {}):
                for which in ("generate", "accept"):
                    with self.subTest(platform=platform, field=field, which=which):
                        lo, hi = config.field_band(platform, field, which)
                        self.assertGreater(lo, 0)
                        self.assertLessEqual(lo, hi)


class TheAuditorReadsTheConfigRatherThanItsOwnNumbers(unittest.TestCase):
    def test_the_bands_it_enforces_come_from_the_config(self):
        import shopify_seo_audit as audit

        self.assertEqual(
            (audit.TITLE_ACCEPT_MIN, audit.TITLE_ACCEPT_MAX),
            config.accept_band("seo_title"),
        )
        self.assertEqual(
            (audit.DESCRIPTION_ACCEPT_MIN, audit.DESCRIPTION_ACCEPT_MAX),
            config.accept_band("seo_description"),
        )

    def test_no_length_literal_survives_in_the_issue_function(self):
        """An AST guard, because a reintroduced literal reads as harmless.

        Only the comparison operands of a length check are examined -- the
        module is full of legitimate numbers (token counts, score weights).
        """
        import ast

        source = (RUNTIME / "shopify_seo_audit.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        func = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "_issues_for_resource"
        )
        offenders = []
        for node in ast.walk(func):
            if not isinstance(node, ast.Compare):
                continue
            if not (isinstance(node.left, ast.Call)
                    and isinstance(node.left.func, ast.Name)
                    and node.left.func.id == "len"):
                continue
            for comparator in node.comparators:
                if isinstance(comparator, ast.Constant) and isinstance(comparator.value, int):
                    offenders.append(ast.unparse(node))
        self.assertEqual(
            offenders, [],
            "a length threshold was hardcoded again instead of read from the "
            f"config: {offenders}",
        )

    def test_every_issue_code_the_auditor_emits_is_declared(self):
        """A code with no config entry has no declared severity or blocking
        status, so the eval and the lane would each guess -- which is how the
        eval gate came to block on a code production ships happily."""
        import ast

        source = (RUNTIME / "shopify_seo_audit.py").read_text(encoding="utf-8")
        emitted = set()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if (isinstance(key, ast.Constant) and key.value == "code"
                            and isinstance(value, ast.Constant)
                            and isinstance(value.value, str)):
                        emitted.add(value.value)
        declared = set(config.issue_codes())
        self.assertEqual(
            emitted - declared, set(),
            "these issue codes are emitted by the auditor but absent from "
            "config/seo_rules.json issue_codes",
        )


class TheGeneratorsQualityVocabularyIsFullyDeclared(unittest.TestCase):
    """Both directions, because each one hides a different bug.

    An emitted-but-undeclared code has no blocking status, so the generator and
    the eval each guess -- that is how the eval came to gate on
    `subject_missing`, which production ships happily, making its gate
    unsatisfiable. A declared-but-never-emitted code is the quieter failure: it
    reads as covered while nothing can ever raise it.
    """

    def _emitted_codes(self):
        import ast

        source = (RUNTIME / "shopify_seo_review.py").read_text(encoding="utf-8")
        func = next(
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.FunctionDef) and node.name == "_seo_title_quality"
        )
        codes = set()
        for node in ast.walk(func):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "append"):
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        codes.add(arg.value)
        return codes

    def test_every_emitted_code_is_declared(self):
        emitted = self._emitted_codes()
        self.assertTrue(emitted, "found no emitted codes -- the AST walk is wrong")
        self.assertEqual(
            emitted - set(config.title_quality_codes()), set(),
            "emitted by _seo_title_quality but absent from config title_quality_codes",
        )

    def test_every_declared_code_is_actually_emitted(self):
        declared = set(config.title_quality_codes())
        self.assertEqual(
            declared - self._emitted_codes(), set(),
            "declared in config title_quality_codes but never emitted -- it reads "
            "as covered while nothing can raise it",
        )

    def test_the_live_withhold_set_is_the_configs_blocking_set(self):
        import shopify_seo_review as review

        self.assertEqual(
            set(review._LIVE_QUALITY_ISSUE_CODES),
            set(config.blocking_title_quality_codes()),
        )

    def test_the_advisory_code_is_not_in_the_withhold_set(self):
        """Pins the specific disagreement that made the eval gate
        unsatisfiable, so it cannot come back quietly."""
        import shopify_seo_review as review

        self.assertNotIn("subject_missing", review._LIVE_QUALITY_ISSUE_CODES)
        self.assertFalse(
            config.title_quality_codes()["subject_missing"].get("blocking")
        )


class TheNewduckBuilderAgreesWithTheAuditorTwoReposOver(unittest.TestCase):
    """The test that would have caught the original defect.

    newduck writes a Shopify SEO title; the duck-ops auditor grades it. Nothing
    compared the two bands, so new listings were born needing repair.
    """

    def setUp(self):
        skip_without_siblings(self)

    def test_the_newduck_title_band_cannot_produce_an_auditor_reject(self):
        duck_agent = sibling_repo("duckAgent")
        config_path = duck_agent / "config" / "seo_rules.json"
        shared = json.loads(config_path.read_text(encoding="utf-8"))

        # Both repos must be reading the SAME file, not two copies of it.
        self.assertEqual(
            shared["seo_rules_version"], config.seo_rules_version(),
            "duck-ops and duckAgent disagree about the rules version, which "
            "means they are not reading the same config",
        )

        generate = tuple(shared["platforms"]["shopify"]["fields"]["seo_title"]["generate"])
        accept = config.accept_band("seo_title")
        self.assertGreaterEqual(generate[0], accept[0])
        self.assertLessEqual(generate[1], accept[1])

    def test_the_helper_constants_match_the_shared_config(self):
        duck_agent = sibling_repo("duckAgent")
        if str(duck_agent) not in sys.path:
            sys.path.insert(0, str(duck_agent))
        from helpers.seo_rules import (  # type: ignore
            NEWDUCK_SEO_TITLE_MAX,
            NEWDUCK_SEO_TITLE_MIN,
        )

        self.assertEqual(
            (NEWDUCK_SEO_TITLE_MIN, NEWDUCK_SEO_TITLE_MAX),
            config.generate_band("seo_title"),
            "the duckAgent helper's band drifted from the shared config",
        )


class AMissingConfigFailsLoudly(unittest.TestCase):
    """No defaults. A plausible default would put every surface back on its own
    private numbers, which is the failure this config exists to end."""

    def test_a_missing_file_raises(self):
        import importlib

        module = importlib.reload(config)
        original = module.CONFIG_PATH
        try:
            module.CONFIG_PATH = Path("/nonexistent/seo_rules.json")
            module.load_seo_rules.cache_clear()
            with self.assertRaises(module.SeoRulesConfigError):
                module.load_seo_rules()
        finally:
            module.CONFIG_PATH = original
            module.load_seo_rules.cache_clear()

    def test_malformed_json_raises(self):
        import importlib
        import tempfile

        module = importlib.reload(config)
        original = module.CONFIG_PATH
        try:
            with tempfile.TemporaryDirectory() as tmp:
                bad = Path(tmp) / "seo_rules.json"
                bad.write_text("{not json", encoding="utf-8")
                module.CONFIG_PATH = bad
                module.load_seo_rules.cache_clear()
                with self.assertRaises(module.SeoRulesConfigError):
                    module.load_seo_rules()
        finally:
            module.CONFIG_PATH = original
            module.load_seo_rules.cache_clear()

    def test_a_config_with_no_version_raises(self):
        import importlib
        import tempfile

        module = importlib.reload(config)
        original = module.CONFIG_PATH
        try:
            with tempfile.TemporaryDirectory() as tmp:
                bad = Path(tmp) / "seo_rules.json"
                bad.write_text(json.dumps({"platforms": {}}), encoding="utf-8")
                module.CONFIG_PATH = bad
                module.load_seo_rules.cache_clear()
                with self.assertRaises(module.SeoRulesConfigError):
                    module.load_seo_rules()
        finally:
            module.CONFIG_PATH = original
            module.load_seo_rules.cache_clear()


class TheTwoPlatformsKeepTheirOwnPolicies(unittest.TestCase):
    """Shopify wants Jeep in the title; Etsy forbids it. One shared profile
    would silently apply the wrong policy to whichever platform came second."""

    def test_etsy_bans_title_terms_that_shopify_allows(self):
        etsy_banned = {t.lower() for t in config.load_seo_rules()["platforms"]["etsy"]["banned_title_terms"]}
        shopify_banned = {
            t.lower()
            for t in config.load_seo_rules()["platforms"]["shopify"]["banned_title_terms"]
        }
        self.assertIn("jeep", etsy_banned)
        self.assertNotIn("jeep", shopify_banned)

    def test_the_two_platforms_do_not_share_a_title_band(self):
        self.assertNotEqual(
            config.generate_band("seo_title", "shopify"),
            config.generate_band("title", "etsy"),
        )


if __name__ == "__main__":
    unittest.main()


class TheReviewCategoriesAreTheOneDefinition(unittest.TestCase):
    """The category table was the last rule list living in code.

    Until 2026-11-19 `SEO_REVIEW_CATEGORY_SPECS` and `SEO_REVIEW_CATEGORY_ORDER`
    were two hardcoded literals in shopify_seo_review.py, while the bands,
    severities and quality vocabulary had all moved to the config -- and they
    are rules in exactly the same sense, because they decide which auditor
    codes become proposable work and which fields a batch may write.
    """

    def test_every_category_code_is_a_declared_issue_code(self):
        """A typo'd code makes that category permanently empty.

        Nothing would raise: the selector looks for resources carrying a code
        that no auditor ever emits, finds none, and the kickoff reports the
        category as having nothing to do. A dead category reads exactly like a
        clean one.
        """
        declared = set(config.issue_codes())
        self.assertTrue(declared, "the config declares no issue codes at all")
        for entry in config.review_categories():
            for code in entry.get("issue_codes") or ():
                with self.subTest(category=entry.get("key"), code=code):
                    self.assertIn(
                        code, declared,
                        f"category {entry.get('key')!r} feeds on issue code {code!r}, "
                        "which no auditor declares -- that category can never "
                        "collect a resource",
                    )

    def test_no_issue_code_feeds_two_categories(self):
        """One code in two categories proposes the same resource twice.

        The two categories can carry different `apply_title` /
        `apply_description` flags, so the same resource would be offered under
        two different write rules, and whichever batch the operator approved
        second would re-write a field the first had already fixed.
        """
        seen: dict[str, str] = {}
        for entry in config.review_categories():
            key = str(entry.get("key"))
            for code in entry.get("issue_codes") or ():
                with self.subTest(code=code):
                    self.assertNotIn(
                        str(code), seen,
                        f"issue code {code!r} feeds both {seen.get(str(code))!r} "
                        f"and {key!r}",
                    )
                seen[str(code)] = key

    def test_every_category_writes_at_least_one_field(self):
        for entry in config.review_categories():
            with self.subTest(category=entry.get("key")):
                self.assertTrue(
                    entry.get("apply_title") or entry.get("apply_description"),
                    f"category {entry.get('key')!r} may write neither field, so a "
                    "batch in it can only ever be a no-op",
                )

    def test_every_category_has_a_key_a_label_and_a_code(self):
        for entry in config.review_categories():
            with self.subTest(category=entry.get("key")):
                self.assertTrue(str(entry.get("key") or "").strip())
                self.assertTrue(str(entry.get("label") or "").strip())
                self.assertTrue(entry.get("issue_codes"))

    def test_keys_are_unique(self):
        keys = [str(e.get("key")) for e in config.review_categories()]
        self.assertEqual(len(keys), len(set(keys)), f"duplicate category keys in {keys}")

    def test_the_review_module_derives_both_tables_from_the_config(self):
        """No second copy in code, and the ORDER survives the move.

        The order is now only a tie-break in the ranking, but it is the
        tie-break that makes a system with no run history reproduce the old
        round-robin behaviour -- so a reordering of the config is a behaviour
        change, not a cosmetic one.
        """
        import shopify_seo_review as ssr

        expected_order = [str(e.get("key")) for e in config.review_categories()]
        self.assertEqual(list(ssr.SEO_REVIEW_CATEGORY_ORDER), expected_order)
        self.assertEqual(set(ssr.SEO_REVIEW_CATEGORY_SPECS), set(expected_order))
        for entry in config.review_categories():
            spec = ssr.SEO_REVIEW_CATEGORY_SPECS[str(entry.get("key"))]
            with self.subTest(category=entry.get("key")):
                self.assertEqual(spec["label"], entry.get("label"))
                self.assertEqual(spec["issue_codes"], {str(c) for c in entry["issue_codes"]})
                self.assertEqual(spec["apply_title"], bool(entry.get("apply_title")))
                self.assertEqual(spec["apply_description"], bool(entry.get("apply_description")))

    def test_an_absent_category_list_raises(self):
        """An empty table is a lane that reports nothing to do forever."""
        import importlib
        import tempfile

        module = importlib.reload(config)
        original = module.CONFIG_PATH
        try:
            for payload in ({"seo_rules_version": 1}, {"seo_rules_version": 1, "review_categories": []}):
                with self.subTest(payload=payload):
                    with tempfile.TemporaryDirectory() as tmp:
                        bad = Path(tmp) / "seo_rules.json"
                        bad.write_text(json.dumps(payload), encoding="utf-8")
                        module.CONFIG_PATH = bad
                        module.load_seo_rules.cache_clear()
                        with self.assertRaises(module.SeoRulesConfigError):
                            module.review_categories()
        finally:
            module.CONFIG_PATH = original
            module.load_seo_rules.cache_clear()


class TheBatchCapIsConfiguredAndIsNotACopyRule(unittest.TestCase):
    """How many resources one review email may carry.

    Nothing capped this until 2026-11-19: the kickoff passes `limit=0` meaning
    "every eligible resource", which for `weak_description` was 66 -- nine
    model calls and 66 live fields written on a single one-word reply.
    """

    def test_a_cap_is_declared_and_is_an_integer(self):
        self.assertIsInstance(config.max_items_per_batch(), int)

    def test_the_cap_does_not_exceed_what_the_email_shows(self):
        """Otherwise the operator approves copy they were never shown.

        The email truncates its item list at `preview_limit` while the approval
        reply still applies EVERY item in the run, so a cap above the preview
        asks for blind approval. Pinned as a relation, not a number: raise one
        and the other has to move too.
        """
        import shopify_seo_review as ssr

        cap = config.max_items_per_batch()
        if cap <= 0:
            self.skipTest("cap disabled")
        payload = {"items": [{"id": str(i)} for i in range(cap)], "preview_limit": 15}
        self.assertEqual(
            len(ssr._display_items(payload)), cap,
            "the batch cap is larger than the email's preview limit, so the "
            "operator would be asked to approve items the email never rendered",
        )

    def test_an_absent_cap_raises_rather_than_meaning_unlimited(self):
        """A plausible default here IS the 66-item batch."""
        import importlib
        import tempfile

        module = importlib.reload(config)
        original = module.CONFIG_PATH
        try:
            with tempfile.TemporaryDirectory() as tmp:
                bad = Path(tmp) / "seo_rules.json"
                bad.write_text(json.dumps({"seo_rules_version": 1}), encoding="utf-8")
                module.CONFIG_PATH = bad
                module.load_seo_rules.cache_clear()
                with self.assertRaises(module.SeoRulesConfigError):
                    module.max_items_per_batch()
        finally:
            module.CONFIG_PATH = original
            module.load_seo_rules.cache_clear()

    def test_a_non_integer_cap_raises(self):
        import importlib
        import tempfile

        module = importlib.reload(config)
        original = module.CONFIG_PATH
        try:
            with tempfile.TemporaryDirectory() as tmp:
                bad = Path(tmp) / "seo_rules.json"
                bad.write_text(
                    json.dumps({"seo_rules_version": 1, "review_batch": {"max_items_per_batch": "lots"}}),
                    encoding="utf-8",
                )
                module.CONFIG_PATH = bad
                module.load_seo_rules.cache_clear()
                with self.assertRaises(module.SeoRulesConfigError):
                    module.max_items_per_batch()
        finally:
            module.CONFIG_PATH = original
            module.load_seo_rules.cache_clear()

    def test_the_cap_lives_outside_the_versioned_copy_rules(self):
        """Changing a batch size must never need a seo_rules_version bump.

        `seo_rules_version` records which COPY RULES a resource's text was
        written under, and every ledger row below the current version re-enters
        the review queue. If the batch cap lived inside `platforms`, raising it
        would look like a copy-rule change and a bump would re-propose the
        whole indexed catalogue.
        """
        rules = config.load_seo_rules()
        self.assertIn("review_batch", rules)
        for platform, profile in (rules.get("platforms") or {}).items():
            with self.subTest(platform=platform):
                self.assertNotIn("max_items_per_batch", profile)
                self.assertNotIn("review_batch", profile)
