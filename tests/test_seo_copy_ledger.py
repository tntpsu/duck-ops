"""The per-resource SEO copy ledger: what was written, under which rules.

WHAT WENT WRONG WITHOUT IT
--------------------------
Selection filtered purely on today's audit issue codes and a round-robin cursor
walked nine issue categories, one batch a day, wrapping about every nine days
and proposing the same categories again. 176 writeback receipts recorded
exactly what had been applied and verified; nothing in the selection path ever
read one.

THE MISTAKE THIS FILE ALSO PINS
-------------------------------
The first implementation made "no ledger row" a trigger in its own right. Run
against the live snapshot it classified all 355 resources as needing work -- a
catalogue-wide re-propose, the exact outcome the receipt backfill exists to
prevent. The auditor decides whether there is anything to FIX; the ledger only
decides whether we have already TRIED. Absence of a row is not a defect in the
copy, and `test_a_clean_resource_with_no_row_is_not_work` is that lesson.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import seo_copy_ledger as ledger  # noqa: E402

CURRENT = 1


def _resource(rid="gid://shopify/Product/1", title="A Duck", desc="A description", codes=()):
    return {
        "id": rid,
        "kind": "product",
        "title": title,
        "seo_title": title,
        "seo_description": desc,
        "issues": [{"code": c, "severity": "medium"} for c in codes],
    }


def _row(rid, *, version, title=None, desc=None):
    book = {"ledger_version": 1, "rows": {}}
    fields = {}
    if title is not None:
        fields["seo_title"] = title
    if desc is not None:
        fields["seo_description"] = desc
    ledger.record_write(
        book, resource_id=rid, fields=fields, source="llm", rules_version=version
    )
    return book


class TheAuditorGatesTheWork(unittest.TestCase):
    def test_a_clean_resource_with_no_row_is_not_work(self):
        """The bug the first implementation shipped: 355 of 355 needed work."""
        verdict = ledger.classify_resource(
            None, live_values={"seo_title": "Fine"}, issue_codes=[], current_rules_version=CURRENT
        )
        self.assertFalse(verdict["needs_work"])
        self.assertEqual(verdict["trigger"], ledger.TRIGGER_NONE)

    def test_a_flagged_resource_with_no_row_is_work(self):
        verdict = ledger.classify_resource(
            None,
            live_values={"seo_title": "Weak"},
            issue_codes=["weak_generic_seo_title"],
            current_rules_version=CURRENT,
        )
        self.assertTrue(verdict["needs_work"])
        self.assertEqual(verdict["trigger"], ledger.TRIGGER_ISSUE_UNSEEN)

    def test_a_clean_resource_with_a_stale_row_is_not_work_by_default(self):
        """A version bump must not automatically re-propose every old row --
        that would make the bump unbounded, and its size is the operator's
        decision."""
        book = _row("gid://shopify/Product/1", version=0, title="Fine")
        row = book["rows"][ledger.row_key("shopify", "gid://shopify/Product/1")]
        verdict = ledger.classify_resource(
            row, live_values={"seo_title": "Fine"}, issue_codes=[], current_rules_version=CURRENT
        )
        self.assertFalse(verdict["needs_work"])


class TheLedgerSuppressesRework(unittest.TestCase):
    def test_a_resource_written_under_current_rules_and_still_flagged_is_withheld(self):
        """Regenerating with the same rules and the same generator would make
        the same copy. That is a rules problem, not a queue item."""
        rid = "gid://shopify/Product/1"
        book = _row(rid, version=CURRENT, title="Already Rewritten")
        row = book["rows"][ledger.row_key("shopify", rid)]
        verdict = ledger.classify_resource(
            row,
            live_values={"seo_title": "Already Rewritten"},
            issue_codes=["weak_generic_seo_title"],
            current_rules_version=CURRENT,
        )
        self.assertEqual(verdict["trigger"], ledger.TRIGGER_ISSUE_PERSISTS)
        self.assertFalse(verdict["needs_work"])

    def test_a_resource_written_under_older_rules_is_work_again(self):
        rid = "gid://shopify/Product/1"
        book = _row(rid, version=0, title="Old Copy")
        row = book["rows"][ledger.row_key("shopify", rid)]
        verdict = ledger.classify_resource(
            row,
            live_values={"seo_title": "Old Copy"},
            issue_codes=["weak_generic_seo_title"],
            current_rules_version=CURRENT,
        )
        self.assertEqual(verdict["trigger"], ledger.TRIGGER_ISSUE_UNSEEN)
        self.assertTrue(verdict["needs_work"])


class TheHashKeepsTheLedgerHonest(unittest.TestCase):
    """Without the hash the ledger would claim a resource is current after an
    operator edited it by hand in the Shopify admin."""

    def test_an_admin_edit_re_enters_the_queue(self):
        rid = "gid://shopify/Product/1"
        book = _row(rid, version=CURRENT, title="What We Wrote")
        row = book["rows"][ledger.row_key("shopify", rid)]
        verdict = ledger.classify_resource(
            row,
            live_values={"seo_title": "What The Operator Typed Instead"},
            issue_codes=["weak_generic_seo_title"],
            current_rules_version=CURRENT,
        )
        self.assertEqual(verdict["trigger"], ledger.TRIGGER_DRIFT_WITH_ISSUE)
        self.assertTrue(verdict["needs_work"])
        self.assertEqual(verdict["drifted_fields"], ["seo_title"])

    def test_drift_the_auditor_is_content_with_is_an_observation_not_work(self):
        """It is still not nothing: the row's hash is stale and must be
        refreshed, or the ledger lies about what is live."""
        rid = "gid://shopify/Product/1"
        book = _row(rid, version=CURRENT, title="What We Wrote")
        row = book["rows"][ledger.row_key("shopify", rid)]
        verdict = ledger.classify_resource(
            row,
            live_values={"seo_title": "A Perfectly Good Hand Written Title"},
            issue_codes=[],
            current_rules_version=CURRENT,
        )
        self.assertFalse(verdict["needs_work"])
        self.assertEqual(verdict["observation"], ledger.OBSERVATION_DRIFT_CLEAN)

    def test_whitespace_only_change_is_not_drift(self):
        """A cosmetic re-save in the admin must not read as an operator edit."""
        self.assertEqual(
            ledger.value_hash("A  Duck   Title"), ledger.value_hash("A Duck Title")
        )

    def test_a_one_word_change_is_drift(self):
        self.assertNotEqual(
            ledger.value_hash("A Duck Title"), ledger.value_hash("A Goose Title")
        )


class TheDeliberateResweepIsOptIn(unittest.TestCase):
    def test_resweep_pulls_in_clean_resources_below_the_current_version(self):
        rid = "gid://shopify/Product/1"
        book = _row(rid, version=0, title="Fine")
        row = book["rows"][ledger.row_key("shopify", rid)]
        verdict = ledger.classify_resource(
            row,
            live_values={"seo_title": "Fine"},
            issue_codes=[],
            current_rules_version=CURRENT,
            resweep=True,
        )
        self.assertEqual(verdict["trigger"], ledger.TRIGGER_RESWEEP)
        self.assertTrue(verdict["needs_work"])

    def test_resweep_does_not_pull_in_resources_already_at_the_current_version(self):
        rid = "gid://shopify/Product/1"
        book = _row(rid, version=CURRENT, title="Fine")
        row = book["rows"][ledger.row_key("shopify", rid)]
        verdict = ledger.classify_resource(
            row,
            live_values={"seo_title": "Fine"},
            issue_codes=[],
            current_rules_version=CURRENT,
            resweep=True,
        )
        self.assertFalse(verdict["needs_work"])


class TheBackfillReadsTheReceipts(unittest.TestCase):
    """Starting empty would make every resource look new and re-propose the
    whole catalogue -- the opposite of the point."""

    def _receipt(self, tmp, rid, *, status="verified", observed=None, fields=("seo_title",), version=None):
        payload = {
            "receipt_id": f"r-{rid}",
            "verified_at": "2026-11-19T10:00:00-05:00",
            "status": status,
            "run_id": "run_1",
            "resource_id": rid,
            "applied_fields": list(fields),
            "observed": observed or {"seo_title": "Live Title"},
        }
        if version is not None:
            payload["seo_rules_version"] = version
        (tmp / f"{rid.replace('/', '_')}.json").write_text(json.dumps(payload), encoding="utf-8")

    def test_a_verified_receipt_becomes_a_row(self):
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._receipt(tmp, "gid://shopify/Product/1")
            book = {"ledger_version": 1, "rows": {}}
            stats = ledger.backfill_from_receipts(book, receipts_dir=tmp)
            self.assertEqual(stats["receipts_used"], 1)
            self.assertEqual(len(book["rows"]), 1)

    def test_an_unverified_receipt_does_not(self):
        """A failed write must not leave a row claiming the resource is
        current ([[feedback_verify_against_the_marketplace]])."""
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._receipt(tmp, "gid://shopify/Product/1", status="failed")
            book = {"ledger_version": 1, "rows": {}}
            stats = ledger.backfill_from_receipts(book, receipts_dir=tmp)
            self.assertEqual(stats["skipped_unverified"], 1)
            self.assertEqual(book["rows"], {})

    def test_the_row_hashes_the_OBSERVED_value_not_the_proposed_one(self):
        """The receipt's `observed` is what the store actually held on
        read-back. Hashing the proposal would make a partially applied write
        look complete."""
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._receipt(
                tmp, "gid://shopify/Product/1", observed={"seo_title": "What Shopify Stored"}
            )
            book = {"ledger_version": 1, "rows": {}}
            ledger.backfill_from_receipts(book, receipts_dir=tmp)
            row = book["rows"][ledger.row_key("shopify", "gid://shopify/Product/1")]
            self.assertEqual(
                row["live_value_hash"]["seo_title"], ledger.value_hash("What Shopify Stored")
            )

    def test_a_receipt_with_no_version_reads_as_pre_config(self):
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._receipt(tmp, "gid://shopify/Product/1")
            book = {"ledger_version": 1, "rows": {}}
            ledger.backfill_from_receipts(book, receipts_dir=tmp)
            row = book["rows"][ledger.row_key("shopify", "gid://shopify/Product/1")]
            self.assertEqual(row["rules_version"], 0)

    def test_a_receipt_carrying_a_version_keeps_it(self):
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._receipt(tmp, "gid://shopify/Product/1", version=CURRENT)
            book = {"ledger_version": 1, "rows": {}}
            ledger.backfill_from_receipts(book, receipts_dir=tmp)
            row = book["rows"][ledger.row_key("shopify", "gid://shopify/Product/1")]
            self.assertEqual(row["rules_version"], CURRENT)

    def test_a_missing_receipts_directory_is_not_a_crash(self):
        book = {"ledger_version": 1, "rows": {}}
        stats = ledger.backfill_from_receipts(book, receipts_dir=Path("/nonexistent"))
        self.assertEqual(stats["receipts_seen"], 0)


class ACorruptLedgerDoesNotReadAsEmpty(unittest.TestCase):
    """An unreadable ledger that returned {} would look like 'nothing was ever
    written' and re-propose the whole catalogue."""

    def test_it_carries_a_load_error_marker(self):
        import tempfile

        with tempfile.TemporaryDirectory() as raw:
            bad = Path(raw) / "seo_copy_ledger.json"
            bad.write_text("{not json", encoding="utf-8")
            book = ledger.load_ledger(bad)
            self.assertTrue(book.get("load_error"))


class TheSelectorConsultsTheLedger(unittest.TestCase):
    def test_a_resource_written_under_current_rules_leaves_the_queue(self):
        import shopify_seo_review as review

        rid = "gid://shopify/Product/1"
        audit = {"resources": [_resource(rid, codes=["weak_generic_seo_title"])]}
        empty = {"ledger_version": 1, "rows": {}}
        self.assertEqual(
            len(review._select_issue_category_candidates(
                audit, issue_category="weak_title", ledger=empty)),
            1,
        )
        book = _row(rid, version=ledger.seo_rules_version(), title="A Duck")
        self.assertEqual(
            len(review._select_issue_category_candidates(
                audit, issue_category="weak_title", ledger=book)),
            0,
        )

    def test_it_fails_OPEN_when_the_ledger_cannot_be_read(self):
        """Failing closed would silently empty the queue, and an empty queue
        looks exactly like a healthy one ([[feedback_swallowed_errors_lie]])."""
        import shopify_seo_review as review

        audit = {"resources": [_resource(codes=["weak_generic_seo_title"])]}
        broken = {"ledger_version": 1, "rows": {}, "load_error": True}
        kept = review._select_issue_category_candidates(
            audit, issue_category="weak_title", ledger=broken
        )
        self.assertEqual(len(kept), 1)
        self.assertIn("unreadable", kept[0]["ledger_unavailable"])

    def test_the_withheld_reason_is_recorded_on_the_resource(self):
        """A resource that silently vanishes from a batch is unexplainable to
        the operator."""
        import shopify_seo_review as review

        rid = "gid://shopify/Product/1"
        audit = {"resources": [_resource(rid, codes=["weak_generic_seo_title"])]}
        book = _row(rid, version=ledger.seo_rules_version(), title="A Duck")
        review._withhold_ledger_current(audit["resources"], ledger=book)
        self.assertEqual(
            audit["resources"][0]["ledger_trigger"], ledger.TRIGGER_ISSUE_PERSISTS
        )
        self.assertIn("still flags", audit["resources"][0]["ledger_reason"])


if __name__ == "__main__":
    unittest.main()
