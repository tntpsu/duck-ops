"""Surface 168 — one review email may not ask for the whole category at once.

Nothing capped a category batch until 2026-11-19. The daily kickoff calls
`send_shopify_seo_review_email(limit=0, ...)`, and `limit=0` means "every
eligible resource" -- so once the ranking reached `weak_description`, with 66
ledger-eligible resources, the lane would have built a 66-item batch: nine
model calls, and 66 live Shopify fields written on a single one-word `apply`
reply, 51 of them never rendered in the email that asked for approval.

Three things are pinned here:

  1. the cap comes from the shared config, an explicit `--limit` still wins,
     and an unreadable cap fails CLOSED to one chunk rather than open to 66;
  2. the remainder is reported as a DEFERRAL -- counts on the payload and a
     line in the email -- because a cap the operator cannot see reads as "that
     was all there was" ([[feedback_no_silent_caps]]);
  3. the cap never splits a duplicate-title group, because a proposal
     differentiated from a sibling that is not in the batch can land straight
     back on that sibling's live title.

The cap is only safe because the copy ledger exists: an applied resource gets a
ledger row and drops out of the eligible set, so the next batch is the next
slice. Without the ledger the same top 15 would be re-proposed forever.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import seo_rules_config  # noqa: E402
import shopify_seo_review as ssr  # noqa: E402


def _weak_description_resources(count: int, *, start: int = 0) -> list[dict]:
    """`count` products the auditor flags for weak SEO descriptions.

    Distinct titles, so none of them form a duplicate group -- the group
    behaviour is pinned separately.
    """
    return [
        {
            "id": f"gid://shopify/Product/{900000 + start + i}",
            "kind": "product",
            "title": f"Duck Number {start + i:03d}",
            "resource_url": f"/products/duck-{start + i:03d}",
            "seo_title": f"Duck Number {start + i:03d} Collectible for Dashboards",
            "seo_description": "Shop MyJeepDuck for collectible ducks.",
            "issues": [{"code": "weak_generic_seo_description", "severity": "medium"}],
        }
        for i in range(count)
    ]


class TheBatchIsCapped(unittest.TestCase):
    def _build(self, resources: list[dict], *, limit: int = 0, category: str = "weak_description") -> dict:
        audit = {
            "generated_at": "2026-11-19T07:35:00-05:00",
            "shopify_domain": "example.myshopify.com",
            "resources": resources,
        }
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state" / "shopify_seo_review"
            with patch.object(ssr, "REVIEW_STATE_DIR", state_dir), \
                 patch.object(ssr, "REVIEW_RUN_DIR", state_dir / "runs"), \
                 patch.object(ssr, "REVIEW_OUTPUT_MD", Path(tmp) / "out.md"), \
                 patch.object(ssr, "build_shopify_seo_audit", return_value=audit):
                return ssr.build_shopify_seo_review(
                    limit=limit,
                    force_audit=True,
                    review_type="issue_category_batch",
                    issue_category=category,
                )

    def test_sixty_six_eligible_resources_do_not_become_a_sixty_six_item_batch(self):
        """The measured 2026-11-19 shape, which is what this cap is for."""
        cap = seo_rules_config.max_items_per_batch()
        payload = self._build(_weak_description_resources(66))
        self.assertEqual(payload["eligible_item_count"], 66)
        self.assertEqual(payload["item_count"], cap)
        self.assertEqual(payload["deferred_item_count"], 66 - cap)
        self.assertEqual(payload["batch_cap_source"], "config")

    def test_a_category_smaller_than_the_cap_is_sent_whole(self):
        payload = self._build(_weak_description_resources(3))
        self.assertEqual(payload["item_count"], 3)
        self.assertEqual(payload["eligible_item_count"], 3)
        self.assertEqual(payload["deferred_item_count"], 0)

    def test_an_explicit_limit_beats_the_configured_cap(self):
        """An operator asking for 4 gets 4, and the deferral is still counted."""
        payload = self._build(_weak_description_resources(20), limit=4)
        self.assertEqual(payload["item_count"], 4)
        self.assertEqual(payload["eligible_item_count"], 20)
        self.assertEqual(payload["deferred_item_count"], 16)
        self.assertEqual(payload["batch_cap_source"], "explicit_limit")

    def test_an_unreadable_cap_fails_closed_to_one_chunk(self):
        """The plausible fallback here is an UNCAPPED batch, so it must not be
        the fallback ([[feedback_plausible_fallbacks_mask_failure]])."""
        with patch.object(
            ssr._seo_rules_config, "max_items_per_batch",
            side_effect=seo_rules_config.SeoRulesConfigError("gone"),
        ):
            payload = self._build(_weak_description_resources(66))
        self.assertEqual(payload["batch_cap_source"], "fallback")
        self.assertEqual(payload["item_count"], ssr._PROPOSAL_CHUNK_SIZE)
        self.assertEqual(payload["deferred_item_count"], 66 - ssr._PROPOSAL_CHUNK_SIZE)

    def test_a_disabled_cap_sends_everything(self):
        """0 means no cap -- the pre-2026-11-19 behaviour, available on purpose."""
        with patch.object(ssr._seo_rules_config, "max_items_per_batch", return_value=0):
            payload = self._build(_weak_description_resources(40))
        self.assertEqual(payload["item_count"], 40)
        self.assertEqual(payload["deferred_item_count"], 0)

    def test_the_cap_only_applies_to_category_batches(self):
        """`top_actions` has its own limit and no eligible-set notion, so it
        must not grow these fields and start reporting a phantom deferral."""
        audit = {
            "generated_at": "2026-11-19T07:35:00-05:00",
            "shopify_domain": "example.myshopify.com",
            "resources": _weak_description_resources(3),
            "top_actions": _weak_description_resources(3),
        }
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state" / "shopify_seo_review"
            with patch.object(ssr, "REVIEW_STATE_DIR", state_dir), \
                 patch.object(ssr, "REVIEW_RUN_DIR", state_dir / "runs"), \
                 patch.object(ssr, "REVIEW_OUTPUT_MD", Path(tmp) / "out.md"), \
                 patch.object(ssr, "build_shopify_seo_audit", return_value=audit), \
                 patch.object(ssr, "_generate_proposals", return_value=[]):
                payload = ssr.build_shopify_seo_review(limit=2, review_type="top_actions", force_audit=True)
        self.assertIsNone(payload["eligible_item_count"])
        self.assertEqual(payload["deferred_item_count"], 0)


class TheRemainderIsADeferralNotADrop(unittest.TestCase):
    """A cap nobody can see is indistinguishable from an empty category."""

    def _payload(self, *, item_count=15, eligible=66, cap=15, source="config"):
        return {
            "run_id": "shopify_seo_weak_description_20261120_073500",
            "review_type": "issue_category_batch",
            "seo_category": "weak_description",
            "category_label": "Weak or generic SEO descriptions",
            "item_count": item_count,
            "eligible_item_count": eligible,
            "deferred_item_count": max(0, eligible - item_count),
            "batch_cap": cap,
            "batch_cap_source": source,
            "items": [{"id": str(i), "title": f"Duck {i}", "apply_seo_description": True} for i in range(item_count)],
            "approval_action": "Reply apply to update all 15 proposed SEO fixes.",
        }

    def test_the_email_says_how_many_are_open_and_that_they_are_not_lost(self):
        subject, text, html = ssr.render_shopify_seo_review_email(self._payload())
        for body in (text, html):
            with self.subTest(body="html" if body is html else "text"):
                self.assertIn("15 of 66", body)
                self.assertIn("not dropped", body)

    def test_a_full_batch_says_nothing_about_batch_size(self):
        """Noise on every email is how an operator learns to skip the line."""
        _, text, html = ssr.render_shopify_seo_review_email(self._payload(item_count=12, eligible=12))
        for body in (text, html):
            self.assertNotIn("Batch size", body)

    def test_the_email_says_so_when_the_cap_came_from_the_fallback(self):
        """Otherwise a config typo silently shrinks every batch forever, and
        only a print nobody reads records it."""
        _, text, _ = ssr.render_shopify_seo_review_email(
            self._payload(item_count=8, cap=8, source="fallback")
        )
        self.assertIn("review_batch.max_items_per_batch", text)

    def test_the_markdown_carries_the_same_two_numbers(self):
        md = ssr.render_shopify_seo_review_markdown(self._payload())
        self.assertIn("Eligible in this category", md)
        self.assertIn("66", md)
        self.assertIn("51", md)


class TheCapNeverSplitsADuplicateGroup(unittest.TestCase):
    """A half-group gets a proposal differentiated from nothing.

    `_flag_title_differentiation` and the generation prompt can only see titles
    that are IN the batch. Cut a duplicate group across the cap and the kept
    member is differentiated against an empty sibling list, so it can land
    straight back on the absent sibling's live title -- which the auditor then
    flags as a duplicate, forever.
    """

    def _dup_pair(self, suffix: str) -> list[dict]:
        return [
            {
                "id": f"gid://shopify/Page/{suffix}1",
                "kind": "page",
                "title": "Mix & Match",
                "seo_title": "Mix & Match | MyJeepDuck Collectible Ducks",
                "seo_description": "Shop MyJeepDuck for collectible ducks.",
                "issues": [{"code": "weak_generic_seo_description", "severity": "medium"}],
            },
            {
                "id": f"gid://shopify/Page/{suffix}2",
                "kind": "page",
                "title": "Mix and Match",
                "seo_title": "Mix and Match | MyJeepDuck Collectible Ducks",
                "seo_description": "Shop MyJeepDuck for collectible ducks.",
                "issues": [{"code": "weak_generic_seo_description", "severity": "medium"}],
            },
        ]

    def test_a_group_straddling_the_cap_is_kept_whole(self):
        resources = _weak_description_resources(3) + self._dup_pair("77")
        kept = ssr._cap_batch_keeping_dup_groups(resources, 4)
        kept_ids = {r["id"] for r in kept}
        pair_ids = {r["id"] for r in self._dup_pair("77")}
        self.assertEqual(
            pair_ids & kept_ids, pair_ids,
            "the duplicate pair was split by the cap",
        )
        self.assertEqual(len(kept), 5, "overshooting by one sibling is the cheaper error")

    def test_a_group_entirely_past_the_cap_is_deferred_whole(self):
        resources = _weak_description_resources(6) + self._dup_pair("88")
        kept = ssr._cap_batch_keeping_dup_groups(resources, 3)
        kept_ids = {r["id"] for r in kept}
        self.assertEqual(len(kept), 3)
        self.assertFalse(kept_ids & {r["id"] for r in self._dup_pair("88")})

    def test_without_duplicates_the_cap_is_exact(self):
        kept = ssr._cap_batch_keeping_dup_groups(_weak_description_resources(20), 7)
        self.assertEqual(len(kept), 7)
        self.assertEqual([r["id"] for r in kept], [r["id"] for r in _weak_description_resources(7)])

    def test_no_resource_is_kept_twice(self):
        resources = self._dup_pair("99") + _weak_description_resources(5)
        kept = ssr._cap_batch_keeping_dup_groups(resources, 3)
        ids = [r["id"] for r in kept]
        self.assertEqual(len(ids), len(set(ids)))

    def test_a_cap_at_or_above_the_input_returns_everything(self):
        resources = _weak_description_resources(4)
        for cap in (0, -1, 4, 9):
            with self.subTest(cap=cap):
                self.assertEqual(len(ssr._cap_batch_keeping_dup_groups(resources, cap)), 4)


if __name__ == "__main__":
    unittest.main()
