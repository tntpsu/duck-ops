"""Surface 165 — the Shopify SEO batch the operator refused on 2026-11-09.

The operator declined to approve `shopify_seo_weak_title_20261104_073503`
("I wasn't impressed by the last run"). Reading the batch rather than the card
found four independent defects, each pinned here:

  1. **Nothing was asking the model.** `_generate_proposals` had exactly ONE
     call site -- the `top_actions` review type, which the daily kickoff never
     uses. Both `issue_category_batch` and `missing_only_bulk` set
     `proposals = []`, so every category batch the operator has ever reviewed
     was built from `_default_title_for_resource`. The answer to "this SEO
     title is too generic" was a template identical for every product
     (11 of 23 items shared `| MyJeepDuck Dashboard Decor Duck Gift`), and
     Surface 40's demand enrichment plus Surface 60's in-prompt differentiation
     rule were wired to a path that does not run in production.

  2. **Titles had no trailing-separator guard; descriptions had two.** A
     word-boundary truncation at 70 chars left 4 of 23 titles ending in a bare
     `|`. `_finalize_sentence` rstrips separators and
     `_description_needs_fallback` rejects a trailing one -- the title path had
     neither, so the defect was invisible to every gate.

  3. **The no-op guard was gated to the wrong category.** Surface 60's echo
     check only ran when the item carried `duplicate_seo_title` /
     `near_duplicate_seo_title`, so for `weak_title` a proposal byte-identical
     to the live value shipped with `apply_seo_title: True` -- 7 of 23.

  4. **The count and the applier disagreed.** `_item_is_applyable` re-derived
     the no-op (so the payload honestly said 16) but never persisted it, and
     the applier reads `apply_seo_title` off the item. Replying `apply` would
     have written all 23.

Third recurrence in this family ([[feedback_llm_detection_vs_generation]],
[[feedback_incomplete_fix_enumerate_all_branches]]): 2026-07-13 fixed "don't
auto-apply a non-fix" for duplicates, 2026-09-07 fixed "don't ask for approval
of nothing", and neither generalised to the other categories.
"""
from __future__ import annotations

import ast
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import shopify_seo_review as S  # noqa: E402

from _siblings import skip_without_siblings, sibling_repo  # noqa: E402

# The four titles that were actually emailed to the operator, as the model
# returned them (brand suffix attached, over the 70-char ceiling).
REAL_OVERLONG_TITLES = [
    "25 Things to Get Your Kid for Easter (That Aren’t Just Candy) | MyJeepDuck Blog",
    "Choosing the Right Jeep Wrangler: What I Wish I Knew When I Started | MyJeepDuck",
    "Shop Duck Plugs & Jeep Accessories – Hitch Covers, Dash & More | MyJeepDuck",
    "Shop On Sale Duck Collection – 3D-Printed Ducks at MyJeepDuck Collectibles",
]


class TheDanglingTailIsStrippedFromTitles(unittest.TestCase):
    """Defect 2 — titles and descriptions now share one definition of a tail."""

    def test_a_truncated_real_title_never_ends_on_a_separator(self) -> None:
        for raw in REAL_OVERLONG_TITLES:
            with self.subTest(raw[:40]):
                out = S._trim_to_range(raw, min_len=45, max_len=70, extras=["MyJeepDuck"])
                self.assertTrue(out, "a title must survive the trim")
                self.assertFalse(
                    out.rstrip().endswith(("|", "-", "–", "—", ",", ";", ":", "&", "/")),
                    f"left a dangling separator: {out!r}",
                )

    def test_a_truncated_title_never_ends_on_a_connective_word(self) -> None:
        for raw, banned in [
            ("Shop On Sale Duck Collection 3D-Printed Ducks at MyJeepDuck Collectibles", "at"),
            ("Duck Gifts and Dashboard Decor Collectibles Built for Jeep Owners and", "and"),
        ]:
            with self.subTest(banned):
                out = S._trim_to_range(raw, min_len=45, max_len=70, extras=[])
                self.assertNotEqual(out.split()[-1].lower(), banned, f"dangling word in {out!r}")

    def test_the_title_fallback_rejects_a_tail_exactly_as_the_description_one_does(self) -> None:
        """The asymmetry that let this ship: same defect, opposite verdicts."""
        for tail in ("|", "-", "&", "at", "and", "for"):
            with self.subTest(tail):
                title = ("Firefighter Duck Collectible Gift for First Responders " + tail).strip()
                description = ("x" * 150 + " " + tail).strip()
                self.assertTrue(
                    S._title_needs_fallback(title),
                    f"title gate accepted a {tail!r} tail",
                )
                self.assertTrue(
                    S._description_needs_fallback(description),
                    f"description gate accepted a {tail!r} tail",
                )

    def test_surface_61_scores_a_dangling_tail_and_fails_closed_on_it(self) -> None:
        codes = S._seo_title_quality(
            "Choosing the Right Jeep Wrangler: What I Wish I Knew When I Started |",
            subject="Choosing the Right Jeep Wrangler",
        )
        self.assertIn("dangling_tail", codes)
        self.assertIn(
            "dangling_tail",
            S._LIVE_QUALITY_ISSUE_CODES,
            "scoring it but not acting on it is how the 4 titles shipped",
        )

    def test_a_clean_title_is_left_alone(self) -> None:
        """Positive control: the guard must not chew on good titles."""
        for good in [
            "Firefighter Duck Collectible Gift for First Responders",
            "Husky Duck | MyJeepDuck Dashboard Decor Duck Gift",
            "Top 10 Mythical Creature Ducks for Your Jeep | MyJeepDuck Blog",
        ]:
            with self.subTest(good):
                self.assertEqual(S._strip_dangling_tail(good), good)
                self.assertFalse(S._has_dangling_tail(good))

    def test_stripping_is_idempotent(self) -> None:
        once = S._strip_dangling_tail("Duck Gifts for Jeep Owners and |")
        self.assertEqual(S._strip_dangling_tail(once), once)

    def test_a_tail_of_nothing_but_separators_does_not_return_a_partial_word(self) -> None:
        self.assertEqual(S._strip_dangling_tail("| - , &"), "")
        self.assertEqual(S._strip_dangling_tail("Ducks"), "Ducks")


class AnEchoIsNeverAFixWhateverCategoryFoundIt(unittest.TestCase):
    """Defect 3 — the echo check was gated behind the duplicate-title codes."""

    @staticmethod
    def _item(*, code: str, current: str, proposed: str) -> dict:
        return {
            "id": "gid://shopify/Product/1",
            "kind": "product",
            "title": "E.D.T. Alien Duck",
            "current_seo_title": current,
            "proposed_seo_title": proposed,
            "issues": [{"code": code, "severity": "medium"}],
            "apply_seo_title": True,
            "apply_seo_description": False,
        }

    def test_an_echo_is_withheld_for_a_category_that_is_not_about_duplicates(self) -> None:
        """The exact 2026-11-09 regression, for both weak_title issue codes."""
        for code in ("seo_title_matches_raw_title", "weak_generic_seo_title"):
            with self.subTest(code):
                live = "E.D.T. Duck | MyJeepDuck Dashboard Decor Duck Gift"
                items = [self._item(code=code, current=live, proposed=live)]
                S._flag_title_differentiation(items)
                self.assertFalse(
                    items[0]["apply_seo_title"],
                    "a proposal identical to the live title was offered as a fix",
                )
                self.assertTrue(items[0].get("title_needs_differentiation"))

    def test_a_genuine_change_is_still_applied(self) -> None:
        """Positive control — the guard must not withhold real work."""
        items = [
            self._item(
                code="weak_generic_seo_title",
                current="E.D.T. Duck | MyJeepDuck Dashboard Decor Duck Gift",
                proposed="Alien Duck Dashboard Buddy for Sci-Fi Jeep Owners",
            )
        ]
        S._flag_title_differentiation(items)
        self.assertTrue(items[0]["apply_seo_title"])
        self.assertIsNone(items[0].get("title_needs_differentiation"))

    def test_an_echo_differing_only_in_case_and_ampersand_is_still_an_echo(self) -> None:
        items = [
            self._item(
                code="weak_generic_seo_title",
                current="Mix & Match | MyJeepDuck Collectible Ducks",
                proposed="mix and match | myjeepduck collectible ducks",
            )
        ]
        S._flag_title_differentiation(items)
        self.assertFalse(items[0]["apply_seo_title"])

    def test_the_sibling_collision_branch_still_fires(self) -> None:
        """Surface 60 must not regress while the echo gate is widened."""
        shared = "Mix & Match | MyJeepDuck Collectible Ducks"
        items = [
            self._item(code="near_duplicate_seo_title", current=shared, proposed="Duck Bundles | MyJeepDuck"),
            self._item(code="near_duplicate_seo_title", current=shared, proposed="Duck Bundles | MyJeepDuck"),
        ]
        S._flag_title_differentiation(items)
        for item in items:
            self.assertFalse(item["apply_seo_title"], "two items proposing the same title is not a fix")
            self.assertTrue(item.get("title_needs_differentiation"))

    def test_a_description_equal_to_the_live_one_is_withheld(self) -> None:
        """The description path had no echo guard at all -- it matters for the
        `weak_description` category, which holds 60 of the 90 open items."""
        live = "Shop E.D.T. at MyJeepDuck for dashboard decor and gift-ready ducking fun."
        items = [{
            "id": "1",
            "current_seo_description": live,
            "proposed_seo_description": live,
            "apply_seo_title": False,
            "apply_seo_description": True,
        }]
        S._withhold_no_op_descriptions(items)
        self.assertFalse(items[0]["apply_seo_description"])
        self.assertTrue(items[0].get("description_is_no_op"))

    def test_a_changed_description_is_still_applied(self) -> None:
        items = [{
            "id": "1",
            "current_seo_description": "old copy",
            "proposed_seo_description": "new copy that actually differs",
            "apply_seo_title": False,
            "apply_seo_description": True,
        }]
        S._withhold_no_op_descriptions(items)
        self.assertTrue(items[0]["apply_seo_description"])


class TheCountAndTheApplierAgree(unittest.TestCase):
    """Defect 4 — `applyable_item_count` said 16 while the applier would write 23."""

    def test_every_item_the_flags_allow_is_an_item_the_count_counted(self) -> None:
        live = "E.D.T. Duck | MyJeepDuck Dashboard Decor Duck Gift"
        items = [
            {  # echo
                "id": "1", "kind": "product", "title": "E.D.T.",
                "current_seo_title": live, "proposed_seo_title": live,
                "issues": [{"code": "seo_title_matches_raw_title"}],
                "apply_seo_title": True, "apply_seo_description": False,
            },
            {  # real change
                "id": "2", "kind": "product", "title": "Husky Duck",
                "current_seo_title": "Husky Duck", "proposed_seo_title": "Husky Duck Dashboard Buddy Gift for Jeep Owners",
                "issues": [{"code": "weak_generic_seo_title"}],
                "apply_seo_title": True, "apply_seo_description": False,
            },
        ]
        S._flag_title_differentiation(items)
        S._withhold_no_op_descriptions(items)

        counted = S._applyable_item_count(items)
        would_write = sum(
            1 for i in items if i.get("apply_seo_title") or i.get("apply_seo_description")
        )
        self.assertEqual(counted, 1, "only the real change is work")
        self.assertEqual(
            counted, would_write,
            "the count and the per-item flags the applier reads must not disagree",
        )


class TheCategoryBatchActuallyAsksTheModel(unittest.TestCase):
    """Defect 1 — the generator was unreachable from the daily kickoff."""

    def _build(self, resources: list[dict], *, category: str, generate=None) -> dict:
        audit = {
            "generated_at": "2026-11-17T07:35:02-05:00",
            "shopify_domain": "example.myshopify.com",
            "resources": resources,
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            stack = [
                patch.object(S, "REVIEW_STATE_DIR", root / "s"),
                patch.object(S, "REVIEW_RUN_DIR", root / "s" / "runs"),
                patch.object(S, "REVIEW_OUTPUT_MD", root / "out.md"),
                patch.object(S, "build_shopify_seo_audit", return_value=audit),
            ]
            if generate is not None:
                stack.append(patch.object(S, "_generate_proposals", generate))
            for ctx in stack:
                ctx.start()
            try:
                return S.build_shopify_seo_review(
                    limit=0, force_audit=True,
                    review_type="issue_category_batch", issue_category=category,
                )
            finally:
                for ctx in reversed(stack):
                    ctx.stop()

    @staticmethod
    def _weak_product() -> list[dict]:
        return [{
            "id": "gid://shopify/Product/1",
            "kind": "product",
            "title": "Husky Duck – 3D-Printed Duck Inspired by Huskies",
            "resource_url": "/products/husky-duck",
            "seo_title": "Husky Duck | MyJeepDuck Dashboard Decor Duck Gift",
            "seo_description": "Shop Husky Duck at MyJeepDuck for dashboard decor, gift-ready ducking fun, and playful collectible style that helps your flock stand out anywhere.",
            "issues": [{"code": "weak_generic_seo_title", "severity": "medium"}],
        }]

    def test_a_category_batch_calls_the_generator(self) -> None:
        calls: list[int] = []

        def fake(resources, **kw):
            calls.append(len(resources))
            return [{
                "id": "gid://shopify/Product/1",
                "seo_title": "Husky Duck Dashboard Buddy Gift for Jeep Owners",
                "seo_description": "y" * 155,
                "rationale": "named the breed and the use",
                "title_source": "llm",
                "description_source": "llm",
            }]

        payload = self._build(self._weak_product(), category="weak_title", generate=fake)
        self.assertEqual(calls, [1], "the category batch must ask the model, not the template")
        self.assertEqual(payload["items"][0]["title_source"], "llm")
        self.assertNotEqual(
            payload["items"][0]["proposed_seo_title"],
            payload["items"][0]["current_seo_title"],
        )

    def test_a_refusal_degrades_to_the_template_and_says_so(self) -> None:
        """Fail-soft, but never silent -- the template is a plausible answer."""
        payload = self._build(self._weak_product(), category="weak_title")
        self.assertIsNotNone(payload["proposal_generation_error"])
        self.assertIn("DUCK_TEST_MODE", payload["proposal_generation_error"])

    def test_the_template_can_never_answer_the_weak_title_category(self) -> None:
        """Why 7 of the 23 items were byte-identical no-ops: this product's live
        SEO title IS what `_default_title_for_resource` produces, so with no
        model in the loop the "fix" could only ever be the input. With the echo
        guard widened the batch now collapses to nothing applyable instead of
        asking the operator to approve its own input."""
        payload = self._build(self._weak_product(), category="weak_title")
        self.assertEqual(payload["item_count"], 1)
        self.assertEqual(payload["applyable_item_count"], 0)
        self.assertTrue(payload["items"][0].get("title_needs_differentiation"))

    def test_template_fallbacks_are_counted_over_writable_fields_only(self) -> None:
        resources = self._weak_product()
        resources[0]["seo_title"] = "Husky Duck Cheap"  # differs from the template
        payload = self._build(resources, category="weak_title")
        counts = payload["template_fallback_counts"]
        self.assertEqual(counts["applyable_items"], payload["applyable_item_count"])
        self.assertEqual(counts["applyable_items"], 1)
        self.assertEqual(counts["title"], 1, "the template wrote this title and must say so")
        self.assertEqual(counts["description"], 0, "this category does not write descriptions")


class TheGeneratorIsGuardedAndWiredToTheRegistry(unittest.TestCase):

    def test_the_generator_refuses_to_reach_the_api_under_test_mode(self) -> None:
        with self.assertRaises(S.SeoProposalGenerationRefused):
            S._generate_proposals([{"id": "1", "kind": "product", "title": "Duck"}])

    def test_an_injected_client_is_allowed_so_a_test_can_read_the_prompt(self) -> None:
        seen = {}

        def fake(system, user, **kw):
            seen["user"] = user
            seen["kw"] = kw
            return {"items": []}

        S._generate_proposals(
            [{"id": "1", "kind": "product", "title": "Duck", "seo_title": "", "seo_description": "", "issues": []}],
            openai_json=fake,
        )
        self.assertIn("RESOURCES_JSON", seen["user"])

    def test_the_prompt_forbids_echoing_the_current_value_in_every_category(self) -> None:
        """Surface 60 fed the DUPLICATE finding into the prompt, but only for
        dup groups. Nothing told the model not to echo in any other category."""
        seen = {}

        def fake(system, user, **kw):
            seen["user"] = user
            return {"items": []}

        S._generate_proposals(
            [{
                "id": "1", "kind": "product", "title": "Husky Duck",
                "seo_title": "Husky Duck | MyJeepDuck Dashboard Decor Duck Gift",
                "seo_description": "x" * 155,
                "issues": [{"code": "weak_generic_seo_title"}],
            }],
            openai_json=fake,
        )
        self.assertNotIn("CRITICAL DIFFERENTIATION", seen["user"], "no dup group here")
        self.assertIn("NEVER echo current_seo_title", seen["user"])
        self.assertIn("separator", seen["user"].lower())

    def test_a_large_batch_is_chunked_and_no_resource_is_dropped(self) -> None:
        """`limit=0` makes a category batch unbounded -- `weak_description`
        holds 69 resources. One call carrying all of them returned EMPTY on
        2026-11-19: a reasoning model bills its thinking against the same
        ceiling, so it was spent before any content was emitted and the
        fail-soft path turned that into 23 template titles."""
        calls: list[dict] = []

        def fake(system, user, **kw):
            calls.append({"user": user, **kw})
            return {"items": []}

        resources = [
            {"id": str(n), "kind": "product", "title": f"Duck {n}",
             "seo_title": "", "seo_description": "", "issues": []}
            for n in range(69)
        ]
        S._generate_proposals(resources, openai_json=fake)

        self.assertGreater(len(calls), 1, "69 resources in one call is what came back empty")
        asked: list[str] = []
        for call in calls:
            payload = json.loads(call["user"].split("RESOURCES_JSON:\n", 1)[1].split("\n", 1)[0])
            self.assertLessEqual(len(payload), S._PROPOSAL_CHUNK_SIZE)
            self.assertGreaterEqual(
                call["max_tokens"], 2000 + 500 * len(payload),
                "the ceiling must leave room for the model's reasoning",
            )
            asked.extend(str(e["id"]) for e in payload)
        self.assertEqual(
            sorted(asked, key=int), [str(n) for n in range(69)],
            "chunking must not drop or duplicate a resource",
        )

    def test_one_failing_chunk_does_not_cost_the_whole_batch(self) -> None:
        """Before chunking, a single bad response meant every item in the
        category fell back to the template."""
        seen: list[int] = []

        def flaky(system, user, **kw):
            payload = json.loads(user.split("RESOURCES_JSON:\n", 1)[1].split("\n", 1)[0])
            seen.append(len(payload))
            if len(seen) == 1:
                raise RuntimeError("OpenAI JSON parse failed")
            return {"items": [
                {"id": e["id"], "seo_title": f"Duck {e['id']} Dashboard Buddy Gift for Jeep Owners",
                 "seo_description": "y" * 155, "rationale": "named it"}
                for e in payload
            ]}

        resources = [
            {"id": str(n), "kind": "product", "title": f"Duck {n}",
             "seo_title": "weak", "seo_description": "weak", "issues": []}
            for n in range(16)
        ]
        proposals = S._generate_proposals(resources, openai_json=flaky)
        sources = [p["title_source"] for p in proposals]
        self.assertEqual(sources.count("template"), 8, "only the failed chunk falls back")
        self.assertEqual(sources.count("llm"), 8, "the healthy chunk still got real proposals")

    def test_the_generator_names_a_registry_role_not_a_model_literal(self) -> None:
        seen = {}

        def fake(system, user, **kw):
            seen.update(kw)
            return {"items": []}

        S._generate_proposals(
            [{"id": "1", "kind": "product", "title": "Duck", "seo_title": "", "seo_description": "", "issues": []}],
            openai_json=fake,
        )
        self.assertEqual(seen.get("role"), "seo_meta")
        self.assertIsNone(seen.get("model"), "a literal here bypasses the registry")

    def test_no_model_literal_survives_anywhere_in_this_module(self) -> None:
        """A literal is how this surface ran on gpt-4o-mini unnoticed."""
        tree = ast.parse((RUNTIME_DIR / "shopify_seo_review.py").read_text(encoding="utf-8"))
        offenders = [
            node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
            and node.value.startswith(("gpt-", "o1", "o3", "o4", "claude-", "gemini-"))
        ]
        self.assertEqual(offenders, [], f"model literal(s) in the SEO generator: {offenders}")

    def test_the_seo_meta_role_exists_and_is_priced(self) -> None:
        """An unknown role falls back to gpt-4o-mini SILENTLY, so the role's
        existence is load-bearing and belongs in a test, not a reading.

        The registry lives in duckAgent, so this is the cross-repo half and it
        skips on a runner without that checkout rather than passing vacuously.
        """
        skip_without_siblings(self, "duckAgent")
        registry = sibling_repo("duckAgent") / "config" / "models.json"
        roles = json.loads(registry.read_text(encoding="utf-8"))["roles"]
        self.assertIn("seo_meta", roles, "duck-ops asks for this role by name")
        model = roles["seo_meta"]["model"]
        self.assertNotEqual(model, "gpt-4o-mini", "the weak model is what the operator rejected")

        import llm_call_helpers
        self.assertIn(
            model, llm_call_helpers.MODEL_PRICING_USD_PER_1M_TOKENS,
            "an unpriced model books as duckvideo on the cost card",
        )


if __name__ == "__main__":
    unittest.main()


class TheApprovalCopyMatchesWhatApplyWouldDo(unittest.TestCase):
    """The email promised "all 23 items" for a batch that would write 16.
    Operator-facing copy is an assertion and nothing fails when it stops being
    true -- so the number in it is the one the applier uses."""

    def test_it_names_the_applyable_count_when_some_are_withheld(self) -> None:
        copy = S._approval_action(
            "issue_category_batch", 23, issue_category="weak_title", applyable_count=16
        )
        self.assertIn("16 of the 23", copy)
        self.assertIn("other 7", copy)
        self.assertNotIn("all 23", copy)

    def test_it_still_says_all_when_every_item_is_real_work(self) -> None:
        copy = S._approval_action(
            "issue_category_batch", 23, issue_category="weak_title", applyable_count=23
        )
        self.assertIn("all 23 items", copy)

    def test_the_built_payload_copy_agrees_with_its_own_count(self) -> None:
        resources = TheCategoryBatchActuallyAsksTheModel._weak_product()
        payload = TheCategoryBatchActuallyAsksTheModel()._build(resources, category="weak_title")
        applyable = payload["applyable_item_count"]
        count = payload["item_count"]
        copy = payload["approval_action"]
        if applyable == 0:
            # Surface 82e owns this case: a batch with nothing to apply is not
            # a review and says so instead of asking for a reply.
            self.assertIn("nothing", copy.lower())
            self.assertEqual(payload["status"], "not_reviewable")
        elif applyable != count:
            self.assertIn(f"{applyable} of the {count}", copy)
        else:
            self.assertIn(f"all {count} items", copy)
