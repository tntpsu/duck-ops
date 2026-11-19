"""Choosing what the operator sees next, by need rather than by position.

WHAT THE CURSOR DID
-------------------
`_next_issue_category` walked SEO_REVIEW_CATEGORY_ORDER one step per day and
wrapped. Three faults, all of them recorded in this repo's own history:

  * It inverted priority. A product with NO SEO title is high severity and
    sits at the FRONT of the order, so once the cursor walked past it those
    products queued behind every cosmetic category for a full cycle -- 9
    high-severity missing titles behind 93 weak-copy items on 2026-10-11. The
    fix at the time was a high-severity override bolted on top of the walk.
  * It spent a day per empty category, so the kickoff carried a walk-forward
    loop to step past categories that built into a no-op batch.
  * Starvation was bounded by POSITION, not time: `weak_title`'s 23 items
    waited behind a nine-category walk however long since they were last
    offered.

Severity is now the primary sort key, least-recently-served is the second, and
the historical order is only a tie-break.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import seo_copy_ledger as ledger  # noqa: E402
import shopify_seo_review as review  # noqa: E402

EMPTY_LEDGER = {"ledger_version": 1, "rows": {}}


def _resource(rid, *, code, severity="medium", title="A Duck"):
    return {
        "id": rid,
        "kind": "product",
        "title": title,
        "seo_title": title,
        "seo_description": "A description that is long enough to be plausible.",
        "issues": [{"code": code, "severity": severity}],
    }


class SeverityOutranksEverything(unittest.TestCase):
    def test_a_high_severity_category_is_chosen_over_a_bigger_one(self):
        """The 2026-10-11 inversion: 1 missing title must beat 93 weak items."""
        audit = {
            "resources": [
                _resource("gid://shopify/Product/1", code="missing_seo_title", severity="high"),
                *[
                    _resource(f"gid://shopify/Product/{i}", code="weak_generic_seo_title")
                    for i in range(2, 95)
                ],
            ]
        }
        with patch.object(review, "_category_service_history", return_value={}):
            ranked = review.rank_issue_categories(audit)
        self.assertEqual(ranked[0]["category"], "missing_title")
        self.assertTrue(ranked[0]["high_severity"])
        self.assertEqual(ranked[0]["eligible"], 1)

    def test_severity_wins_even_when_it_was_served_most_recently(self):
        audit = {
            "resources": [
                _resource("gid://shopify/Product/1", code="missing_seo_title", severity="high"),
                _resource("gid://shopify/Product/2", code="weak_generic_seo_title"),
            ]
        }
        history = {"missing_title": "2026-11-18T07:00:00-05:00", "weak_title": ""}
        with patch.object(review, "_category_service_history", return_value=history):
            ranked = review.rank_issue_categories(audit)
        self.assertEqual(ranked[0]["category"], "missing_title")


class StarvationIsBoundedByTimeNotPosition(unittest.TestCase):
    def test_the_longest_unserved_category_goes_first(self):
        audit = {
            "resources": [
                _resource("gid://shopify/Product/1", code="weak_generic_seo_title"),
                _resource("gid://shopify/Product/2", code="low_value_seo_copy"),
            ]
        }
        # weak_title sits EARLIER in the historical order but was served
        # yesterday; weak_description is later but has waited since April.
        history = {
            "weak_title": "2026-11-18T07:00:00-05:00",
            "weak_description": "2026-04-01T07:00:00-04:00",
        }
        with patch.object(review, "_category_service_history", return_value=history):
            ranked = review.rank_issue_categories(audit)
        self.assertEqual(ranked[0]["category"], "weak_description")

    def test_a_category_never_served_counts_as_the_longest_wait(self):
        audit = {
            "resources": [
                _resource("gid://shopify/Product/1", code="weak_generic_seo_title"),
                _resource("gid://shopify/Product/2", code="low_value_seo_copy"),
            ]
        }
        history = {"weak_title": "2026-04-01T07:00:00-04:00"}
        with patch.object(review, "_category_service_history", return_value=history):
            ranked = review.rank_issue_categories(audit)
        self.assertEqual(ranked[0]["category"], "weak_description")

    def test_the_historical_order_is_only_a_tie_break(self):
        """With no history at all, the ranking must reproduce the old order --
        a fresh system should not change behaviour."""
        audit = {
            "resources": [
                _resource("gid://shopify/Product/1", code="low_value_seo_copy"),
                _resource("gid://shopify/Product/2", code="weak_generic_seo_title"),
            ]
        }
        with patch.object(review, "_category_service_history", return_value={}):
            ranked = review.rank_issue_categories(audit)
        self.assertEqual(
            [r["category"] for r in ranked], ["weak_title", "weak_description"]
        )


class AnEmptyCategoryNeverCostsADay(unittest.TestCase):
    def test_categories_with_no_eligible_work_are_absent(self):
        audit = {"resources": [_resource("gid://shopify/Product/1", code="weak_generic_seo_title")]}
        with patch.object(review, "_category_service_history", return_value={}):
            ranked = review.rank_issue_categories(audit)
        self.assertEqual([r["category"] for r in ranked], ["weak_title"])

    def test_a_category_the_ledger_emptied_is_absent(self):
        """The ledger and the ranking compose: a category whose every item was
        already rewritten under the current rules simply does not appear."""
        rid = "gid://shopify/Product/1"
        audit = {"resources": [_resource(rid, code="weak_generic_seo_title")]}
        book = {"ledger_version": 1, "rows": {}}
        ledger.record_write(
            book,
            resource_id=rid,
            fields={"seo_title": "A Duck"},
            source="llm",
            rules_version=ledger.seo_rules_version(),
        )
        with patch.object(review, "_category_service_history", return_value={}), patch.object(
            ledger, "load_ledger", return_value=book
        ):
            ranked = review.rank_issue_categories(audit)
        self.assertEqual(ranked, [])

    def test_nothing_eligible_means_no_next_category(self):
        with patch.object(review, "_category_service_history", return_value={}):
            self.assertIsNone(review._next_issue_category(None, {"resources": []}))


class TheExclusionSetKeepsTheWalkForwardAlive(unittest.TestCase):
    """The regression this nearly shipped with.

    The kickoff's walk-forward loop asks for the next category after a batch
    builds into a no-op. The ranking is deterministic and does NOT read "what
    went last", so without an exclusion set it returns the same dead category,
    the loop sees it equal to the one just tried, and gives up -- burning the
    day the walk-forward exists to save.
    """

    def _audit(self):
        return {
            "resources": [
                _resource("gid://shopify/Product/1", code="weak_generic_seo_title"),
                _resource("gid://shopify/Product/2", code="low_value_seo_copy"),
            ]
        }

    def test_an_excluded_category_is_skipped(self):
        with patch.object(review, "_category_service_history", return_value={}):
            self.assertEqual(
                review._next_issue_category(None, self._audit(), exclude=["weak_title"]),
                "weak_description",
            )

    def test_excluding_everything_returns_none_rather_than_looping(self):
        with patch.object(review, "_category_service_history", return_value={}):
            self.assertIsNone(
                review._next_issue_category(
                    None, self._audit(), exclude=["weak_title", "weak_description"]
                )
            )


class TheServiceHistoryComesFromTheRunFiles(unittest.TestCase):
    """Derived, so the ranking needs no state file of its own to isolate."""

    def test_the_newest_run_per_category_wins(self):
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runs = root / "state" / "shopify_seo_review" / "runs"
            runs.mkdir(parents=True)
            for stamp in ("2026-04-01T07:00:00-04:00", "2026-11-18T07:00:00-05:00"):
                (runs / f"run_{stamp[:10]}.json").write_text(
                    json.dumps({"seo_category": "weak_title", "generated_at": stamp}),
                    encoding="utf-8",
                )
            with patch.object(review, "DUCK_OPS_ROOT", root):
                history = review._category_service_history()
        self.assertEqual(history["weak_title"], "2026-11-18T07:00:00-05:00")

    def test_a_dismissed_run_still_counts_as_served(self):
        """Otherwise a dismissed-but-still-failing category is re-sent forever,
        which is exactly the special case the old cursor needed."""
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runs = root / "state" / "shopify_seo_review" / "runs"
            runs.mkdir(parents=True)
            (runs / "run.json").write_text(
                json.dumps(
                    {
                        "seo_category": "weak_title",
                        "generated_at": "2026-11-18T07:00:00-05:00",
                        "status": "dismissed",
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(review, "DUCK_OPS_ROOT", root):
                history = review._category_service_history()
        self.assertIn("weak_title", history)

    def test_the_latest_EVENT_counts_not_the_build_time(self):
        """The regression this nearly shipped with.

        `generated_at` is when the batch was BUILT. A batch built in October
        and dismissed today would read as "served in October" and could be
        re-picked the next morning -- the dismissed-but-still-failing loop the
        old cursor needed an explicit special case to avoid. The live 11-04
        weak_title run is exactly this shape: generated 11-04, emailed 11-09,
        dismissed 11-19.
        """
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runs = root / "state" / "shopify_seo_review" / "runs"
            runs.mkdir(parents=True)
            (runs / "run.json").write_text(
                json.dumps(
                    {
                        "seo_category": "weak_title",
                        "generated_at": "2026-11-04T07:35:03-05:00",
                        "emailed_at": "2026-11-09T07:35:05-05:00",
                        "dismissed_at": "2026-11-19T09:51:16-05:00",
                        "status": "dismissed",
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(review, "DUCK_OPS_ROOT", root):
                history = review._category_service_history()
        self.assertEqual(history["weak_title"], "2026-11-19T09:51:16-05:00")

    def test_an_apply_timestamp_also_counts_as_served(self):
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runs = root / "state" / "shopify_seo_review" / "runs"
            runs.mkdir(parents=True)
            (runs / "run.json").write_text(
                json.dumps(
                    {
                        "seo_category": "weak_title",
                        "generated_at": "2026-11-04T07:35:03-05:00",
                        "apply_result": {"applied_at": "2026-11-18T08:00:00-05:00"},
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(review, "DUCK_OPS_ROOT", root):
                history = review._category_service_history()
        self.assertEqual(history["weak_title"], "2026-11-18T08:00:00-05:00")

    def test_a_missing_runs_directory_is_not_a_crash(self):
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            with patch.object(review, "DUCK_OPS_ROOT", Path(raw)):
                self.assertEqual(review._category_service_history(), {})


class TheWithheldCountReachesTheOperator(unittest.TestCase):
    """`issue_persists` is computed, correct, and was reported nowhere.

    A resource the lane already rewrote under the current rules that the
    auditor still flags is withheld by design. Withheld and uncounted, "the
    rules need changing" is indistinguishable from "the queue is empty".
    """

    def test_a_persisting_issue_is_counted(self):
        rid = "gid://shopify/Product/1"
        audit = {"resources": [_resource(rid, code="weak_generic_seo_title")]}
        book = {"ledger_version": 1, "rows": {}}
        ledger.record_write(
            book,
            resource_id=rid,
            fields={"seo_title": "A Duck"},
            source="llm",
            rules_version=ledger.seo_rules_version(),
        )
        counts = review.ledger_withheld_counts(
            audit, issue_category="weak_title", ledger=book
        )
        self.assertEqual(counts.get("issue_persists"), 1)

    def test_nothing_withheld_counts_nothing(self):
        audit = {"resources": [_resource("gid://shopify/Product/1", code="weak_generic_seo_title")]}
        counts = review.ledger_withheld_counts(
            audit, issue_category="weak_title", ledger=EMPTY_LEDGER
        )
        self.assertEqual(counts, {})

    def test_counting_does_not_change_what_is_proposed(self):
        """Recomputed rather than captured during selection, so it cannot have
        a side effect on the batch."""
        rid = "gid://shopify/Product/1"
        audit = {"resources": [_resource(rid, code="weak_generic_seo_title")]}
        before = len(
            review._select_issue_category_candidates(
                audit, issue_category="weak_title", ledger=EMPTY_LEDGER
            )
        )
        review.ledger_withheld_counts(audit, issue_category="weak_title", ledger=EMPTY_LEDGER)
        after = len(
            review._select_issue_category_candidates(
                audit, issue_category="weak_title", ledger=EMPTY_LEDGER
            )
        )
        self.assertEqual(before, after)

    def test_an_unreadable_ledger_counts_nothing_rather_than_guessing(self):
        audit = {"resources": [_resource("gid://shopify/Product/1", code="weak_generic_seo_title")]}
        counts = review.ledger_withheld_counts(
            audit,
            issue_category="weak_title",
            ledger={"ledger_version": 1, "rows": {}, "load_error": True},
        )
        self.assertEqual(counts, {})


if __name__ == "__main__":
    unittest.main()


class TheCategoryJustWorkedIsNotImmediatelyRePicked(unittest.TestCase):
    """The guarantee the old cursor bought with an explicit `continue`.

    Made explicit rather than left emergent: the ranking sinks an actioned
    batch only if its run file carries a timestamp, and a run file written by
    an older code path might not.
    """

    def _audit(self):
        return {
            "resources": [
                _resource("gid://shopify/Product/1", code="weak_generic_seo_title"),
                _resource("gid://shopify/Product/2", code="low_value_seo_copy"),
            ]
        }

    def test_the_last_worked_category_is_skipped(self):
        import shopify_seo_kickoff as kickoff

        with patch.object(review, "_category_service_history", return_value={}):
            chosen = kickoff._choose_next_category("weak_title", self._audit())
        self.assertEqual(chosen, "weak_description")

    def test_it_is_a_preference_not_an_absolute(self):
        """If the category just worked is the only one with eligible work left,
        sending it again beats sending nothing. The old cursor wrapped to index
        0 and behaved the same way."""
        import shopify_seo_kickoff as kickoff

        only = {"resources": [_resource("gid://shopify/Product/1", code="weak_generic_seo_title")]}
        with patch.object(review, "_category_service_history", return_value={}):
            chosen = kickoff._choose_next_category("weak_title", only)
        self.assertEqual(chosen, "weak_title")
