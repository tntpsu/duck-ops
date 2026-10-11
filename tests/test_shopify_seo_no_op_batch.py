"""Surface 82e — a Shopify SEO batch with nothing to apply is not a review.

The 2026-09-07 `near_duplicate_title` batch parked the whole SEO lane for 34
days. Both of its items had `apply_seo_title` withheld by the Surface 60
differentiation guard, so there was no field left to write — yet the batch was
still emailed with "Reply apply to update all 2 items", the kickoff refused to
send any further category while it sat `awaiting_review`, and the only reply
the operator could have given (`apply`) would have written nothing and reported
"Updated and verified" on both items anyway.

Three things are pinned here:
  1. a batch whose every field is withheld builds as `not_reviewable` and is
     never emailed;
  2. the kickoff walks PAST such a category instead of re-parking on it;
  3. an open review can be dismissed in the authoritative store.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import shopify_seo_kickoff  # noqa: E402
import shopify_seo_review  # noqa: E402


def _near_dup_pages() -> list[dict]:
    """The real shape that parked the lane: two pages whose SEO titles differ
    only by `&` vs `and`, which the deterministic title generator cannot
    differentiate because it derives the title from the page's own name."""
    return [
        {
            "id": "gid://shopify/Page/102642548919",
            "kind": "page",
            "title": "Mix & Match",
            "resource_url": "/pages/collection-bundles",
            "seo_title": "Mix & Match | MyJeepDuck Collectible Ducks gift idea",
            "seo_description": "Explore Mix & Match at MyJeepDuck for collectible ducks and playful flock favorites built for dashboard displays, ducking fans everywhere.",
            "issues": [{"code": "near_duplicate_seo_title", "severity": "medium"}],
        },
        {
            "id": "gid://shopify/Page/102642581687",
            "kind": "page",
            "title": "Mix and Match",
            "resource_url": "/pages/collection-bundle",
            "seo_title": "Mix and Match | MyJeepDuck Collectible Ducks gift idea",
            "seo_description": "Explore Mix and Match at MyJeepDuck for collectible ducks and playful flock favorites built for dashboard displays, ducking fans everywhere.",
            "issues": [{"code": "near_duplicate_seo_title", "severity": "medium"}],
        },
    ]


class TestNothingToApplyIsNotAReview(unittest.TestCase):
    def _build(self, resources: list[dict], *, category: str, tmp: Path) -> dict:
        audit = {
            "generated_at": "2026-09-07T11:21:27-04:00",
            "shopify_domain": "example.myshopify.com",
            "resources": resources,
        }
        state_dir = tmp / "state" / "shopify_seo_review"
        with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", state_dir), \
             patch.object(shopify_seo_review, "REVIEW_RUN_DIR", state_dir / "runs"), \
             patch.object(shopify_seo_review, "REVIEW_OUTPUT_MD", tmp / "out.md"), \
             patch.object(shopify_seo_review, "build_shopify_seo_audit", return_value=audit):
            return shopify_seo_review.build_shopify_seo_review(
                limit=0,
                force_audit=True,
                review_type="issue_category_batch",
                issue_category=category,
            )

    def test_an_all_withheld_batch_builds_as_not_reviewable(self) -> None:
        with TemporaryDirectory() as tmp:
            payload = self._build(_near_dup_pages(), category="near_duplicate_title", tmp=Path(tmp))
        self.assertEqual(payload["item_count"], 2, "both pages are still reported")
        self.assertEqual(payload["applyable_item_count"], 0)
        self.assertEqual(payload["status"], "not_reviewable")

    def test_the_reason_names_what_was_found_not_an_example(self) -> None:
        with TemporaryDirectory() as tmp:
            payload = self._build(_near_dup_pages(), category="near_duplicate_title", tmp=Path(tmp))
        reason = payload["not_reviewable_reason"]
        # The operator needs the real fix named: these are duplicate PAGES.
        self.assertIn("Mix & Match", reason)
        self.assertIn("Mix and Match", reason)
        self.assertIn("301 redirect", reason)
        # And the approval copy must stop asking for a reply it cannot use.
        self.assertNotIn("Reply apply", payload["approval_action"])

    def test_a_batch_with_real_work_still_builds_as_a_review(self) -> None:
        """The guard must not swallow the categories that do have work — the
        9 high-severity missing-title products behind the parked batch are
        exactly what the lane exists to fix."""
        resources = [
            {
                "id": "gid://shopify/Product/1",
                "kind": "product",
                "title": "Camp Slasher Duck",
                "resource_url": "/products/camp-slasher-duck",
                "seo_title": "",
                "seo_description": "",
                "issues": [{"code": "missing_seo_title", "severity": "high"}],
            }
        ]
        with TemporaryDirectory() as tmp:
            payload = self._build(resources, category="missing_title", tmp=Path(tmp))
        self.assertEqual(payload["status"], "awaiting_review")
        self.assertEqual(payload["applyable_item_count"], 1)

    def test_an_empty_category_is_not_mislabelled_not_reviewable(self) -> None:
        with TemporaryDirectory() as tmp:
            payload = self._build([], category="missing_title", tmp=Path(tmp))
        self.assertEqual(payload["item_count"], 0)
        self.assertEqual(payload["status"], "awaiting_review")

    def test_a_not_reviewable_batch_is_never_emailed(self) -> None:
        payload = {
            "run_id": "r1",
            "status": "not_reviewable",
            "not_reviewable_reason": "nothing to apply",
            "items": [],
        }
        with patch.object(shopify_seo_review, "_ensure_duckagent_imports") as imports, \
             patch.object(shopify_seo_review, "should_send_email") as gate:
            out = shopify_seo_review._gate_and_send_review_payload(payload)
        imports.assert_not_called()
        gate.assert_not_called()
        self.assertEqual(out["status"], "not_reviewable")


class TestTheKickoffWalksPastADeadCategory(unittest.TestCase):
    def setUp(self) -> None:
        for name, kwargs in (("_audit_is_stale", {"return_value": False}),
                             ("build_shopify_seo_audit", {"return_value": {}})):
            patcher = patch.object(shopify_seo_kickoff, name, **kwargs)
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def _audit_with(categories: dict[str, int], *, high: set[str] | None = None) -> dict:
        high = high or set()
        resources = []
        for category, count in categories.items():
            code = sorted(shopify_seo_review.SEO_REVIEW_CATEGORY_SPECS[category]["issue_codes"])[0]
            severity = "high" if category in high else "medium"
            for i in range(count):
                resources.append({
                    "id": f"{category}-{i}", "kind": "product", "title": f"{category} {i}",
                    "issues": [{"code": code, "severity": severity}],
                })
        return {"resources": resources}

    def test_high_severity_work_jumps_the_round_robin_queue(self) -> None:
        """A product with NO SEO title is high severity and sits at the front
        of the category order. The anti-starvation cursor alone left 9 of them
        behind 93 cosmetic items for a full cycle (2026-10-11)."""
        audit = self._audit_with(
            {"missing_title": 9, "weak_title": 23, "weak_description": 70},
            high={"missing_title"},
        )
        with patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "dismissed", "seo_category": "near_duplicate_title"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email",
                          return_value={"status": "awaiting_review", "run_id": "live",
                                        "category_label": "Missing", "item_count": 9}) as send:
            shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(send.call_args.kwargs["issue_category"], "missing_title")

    def test_the_category_just_worked_is_never_re_picked(self) -> None:
        """Otherwise a high-severity category the operator just actioned would
        be re-sent on the next kickoff, forever."""
        audit = self._audit_with({"missing_title": 9, "weak_title": 2}, high={"missing_title"})
        with patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "applied", "seo_category": "missing_title"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email",
                          return_value={"status": "awaiting_review", "run_id": "live",
                                        "category_label": "Weak", "item_count": 2}) as send:
            shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(send.call_args.kwargs["issue_category"], "weak_title")

    def test_with_no_high_severity_the_round_robin_is_unchanged(self) -> None:
        audit = self._audit_with({"long_title": 1, "weak_title": 23})
        with patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "applied", "seo_category": "long_title"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email",
                          return_value={"status": "awaiting_review", "run_id": "live",
                                        "category_label": "Weak", "item_count": 23}) as send:
            shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(send.call_args.kwargs["issue_category"], "weak_title")

    def test_a_dead_category_does_not_cost_a_day(self) -> None:
        audit = self._audit_with({"near_duplicate_title": 2, "weak_title": 23})
        sent: list[str] = []

        def fake_send(**kwargs):
            category = kwargs["issue_category"]
            sent.append(category)
            if category == "near_duplicate_title":
                return {"status": "not_reviewable", "run_id": "dead",
                        "category_label": "Near-duplicate SEO titles"}
            return {"status": "awaiting_review", "run_id": "live",
                    "category_label": "Weak", "item_count": 23}

        with patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "applied", "seo_category": "duplicate_title"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email", side_effect=fake_send):
            result = shopify_seo_kickoff.kickoff_shopify_seo_review()

        self.assertEqual(sent, ["near_duplicate_title", "weak_title"])
        self.assertEqual(result["status"], "emailed")
        self.assertEqual(result["item_count"], 23)
        self.assertEqual(result["skipped_categories"], ["Near-duplicate SEO titles"])

    def test_all_categories_dead_terminates_and_says_so(self) -> None:
        audit = self._audit_with({"near_duplicate_title": 2})
        with patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "applied", "seo_category": "duplicate_title"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email",
                          return_value={"status": "not_reviewable", "run_id": "dead",
                                        "category_label": "Near-duplicate SEO titles"}) as send:
            result = shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(result["status"], "no_reviewable_categories")
        self.assertIn("Near-duplicate SEO titles", result["summary"])
        # Bounded: one attempt per category at most, never an endless loop.
        self.assertLessEqual(send.call_count, len(shopify_seo_review.SEO_REVIEW_CATEGORY_ORDER))

    def test_a_not_reviewable_latest_does_not_park_the_lane(self) -> None:
        """`not_reviewable` is a finished state, like `applied` — only
        awaiting_review / apply_attempted may park the kickoff."""
        audit = self._audit_with({"weak_title": 4})
        with patch.object(shopify_seo_kickoff, "_load_latest_review",
                          return_value={"status": "not_reviewable", "seo_category": "near_duplicate_title"}), \
             patch.object(shopify_seo_kickoff, "_load_audit_payload", return_value=audit), \
             patch.object(shopify_seo_kickoff, "send_shopify_seo_review_email",
                          return_value={"status": "awaiting_review", "run_id": "live",
                                        "category_label": "Weak", "item_count": 4}) as send:
            result = shopify_seo_kickoff.kickoff_shopify_seo_review()
        self.assertEqual(send.call_args.kwargs["issue_category"], "weak_title")
        self.assertEqual(result["status"], "emailed")


class TestDismissRetiresItInTheAuthoritativeStore(unittest.TestCase):
    def _write_run(self, tmp: Path, *, status: str) -> tuple[Path, Path]:
        state_dir = tmp / "state" / "shopify_seo_review"
        run_dir = state_dir / "runs"
        run_dir.mkdir(parents=True)
        payload = {"run_id": "r1", "status": status, "seo_category": "near_duplicate_title",
                   "category_label": "Near-duplicate SEO titles", "items": []}
        (run_dir / "r1.json").write_text(json.dumps(payload), encoding="utf-8")
        (state_dir / "latest.json").write_text(json.dumps(payload), encoding="utf-8")
        return state_dir, run_dir

    def test_dismiss_writes_both_the_run_and_latest(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir, run_dir = self._write_run(Path(tmp), status="awaiting_review")
            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", state_dir), \
                 patch.object(shopify_seo_review, "REVIEW_RUN_DIR", run_dir):
                shopify_seo_review.dismiss_shopify_seo_review_run("r1", reason="no-op batch")
                run = json.loads((run_dir / "r1.json").read_text())
                latest = json.loads((state_dir / "latest.json").read_text())
        # Both, because the kickoff guard reads latest.json and the card reads
        # it too — dismissing in only one leaves a zombie in the other.
        for payload in (run, latest):
            self.assertEqual(payload["status"], "dismissed")
            self.assertEqual(payload["dismiss_reason"], "no-op batch")
            self.assertEqual(payload["previous_status"], "awaiting_review")
            self.assertTrue(payload["dismissed_at"])

    def test_dismiss_is_idempotent(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir, run_dir = self._write_run(Path(tmp), status="awaiting_review")
            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", state_dir), \
                 patch.object(shopify_seo_review, "REVIEW_RUN_DIR", run_dir):
                first = shopify_seo_review.dismiss_shopify_seo_review_run("r1", reason="a")
                again = shopify_seo_review.dismiss_shopify_seo_review_run("r1", reason="b")
        self.assertEqual(again["dismissed_at"], first["dismissed_at"])
        self.assertEqual(again["dismiss_reason"], "a")

    def test_an_applied_run_is_not_dismissable(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir, run_dir = self._write_run(Path(tmp), status="applied")
            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", state_dir), \
                 patch.object(shopify_seo_review, "REVIEW_RUN_DIR", run_dir), \
                 self.assertRaises(RuntimeError):
                shopify_seo_review.dismiss_shopify_seo_review_run("r1", reason="x")

    def test_an_unknown_run_raises_rather_than_silently_passing(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir, run_dir = self._write_run(Path(tmp), status="awaiting_review")
            with patch.object(shopify_seo_review, "REVIEW_STATE_DIR", state_dir), \
                 patch.object(shopify_seo_review, "REVIEW_RUN_DIR", run_dir), \
                 self.assertRaises(RuntimeError):
                shopify_seo_review.dismiss_shopify_seo_review_run("nope", reason="x")


class TestTheDismissAffordanceExists(unittest.TestCase):
    def test_the_seo_review_email_offers_a_dismiss_button(self) -> None:
        """The freshness card tells the operator to 'action or dismiss' the
        parked review. For 34 days there was no dismiss button, no dismiss
        verb in the reply parser, and no handler — the instruction was copy
        describing a path that did not exist."""
        import duck_flows

        actions = {a.action for a in duck_flows.FLOWS["shopify_seo"].reply_actions}
        self.assertIn("apply", actions)
        self.assertIn("dismiss", actions)


if __name__ == "__main__":
    unittest.main()
