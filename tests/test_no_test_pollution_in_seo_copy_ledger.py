"""Post-suite audit: no test row ever reached the production SEO copy ledger.

The third of the three isolation layers every new production-state path ships
with (CLAUDE.md, "Cross-repo state writes need both-repo isolation + source-level
guard"). The first two -- the autouse conftest fixture and the DUCK_TEST_MODE
frozen-path refusal in `save_ledger` -- are preventive. This one is the detector,
and it exists because the same bug shipped three times in ten days in 2026-06
before it was fixed structurally.

Why this ledger in particular is worth auditing: it decides which resources the
Shopify SEO lane proposes. A test row landing in production would mark real
resources as "already fixed under the current rules" and silently suppress the
work -- and an empty review queue looks exactly like a healthy one
([[feedback_swallowed_errors_lie]]).
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

# The FACTORY-DEFAULT path, read without going through the module (whose
# constant the autouse fixture has redirected to tmp by the time tests run).
PRODUCTION_LEDGER = (
    Path(__file__).resolve().parents[1] / "state" / "seo_copy_ledger.json"
)

# Markers a fixture would leave behind. Test resource ids are synthetic; a real
# one is always a Shopify global id.
POLLUTION_MARKERS = (
    "tmp_path",
    "pytest",
    "fixture",
    "test-resource",
    "test_resource",
    "sim_run",
    "example.com",
)


class TheProductionLedgerIsFreeOfTestRows(unittest.TestCase):
    def test_no_row_carries_a_test_marker(self):
        if not PRODUCTION_LEDGER.exists():
            self.skipTest("no production ledger yet")
        raw = PRODUCTION_LEDGER.read_text(encoding="utf-8")
        for marker in POLLUTION_MARKERS:
            self.assertNotIn(
                marker,
                raw,
                msg=(
                    f"{PRODUCTION_LEDGER} contains {marker!r} -- a test wrote to the "
                    "production SEO copy ledger. Check the autouse fixture in "
                    "tests/conftest.py patched SEO_COPY_LEDGER_PATH."
                ),
            )

    def test_every_resource_id_looks_like_a_real_shopify_resource(self):
        """Two legitimate id shapes, not one.

        The product/collection lanes record a `gid://shopify/...` global id,
        but the BLOG lane's receipts carry a bare numeric article id -- eight
        of the 176 live receipts look like that. An assertion that every id
        starts with `gid://` reads as a pollution detector and is really a
        false alarm on a second real format.
        """
        if not PRODUCTION_LEDGER.exists():
            self.skipTest("no production ledger yet")
        try:
            book = json.loads(PRODUCTION_LEDGER.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            self.fail(f"{PRODUCTION_LEDGER} is not valid JSON")
        for key, row in (book.get("rows") or {}).items():
            resource_id = str((row or {}).get("resource_id") or "")
            looks_real = resource_id.startswith("gid://shopify/") or resource_id.isdigit()
            self.assertTrue(
                looks_real,
                msg=(
                    f"ledger row {key!r} has a resource_id {resource_id!r} that is "
                    "neither a Shopify global id nor a numeric article id"
                ),
            )


class TheSourceGuardRefusesAProductionWriteUnderTestMode(unittest.TestCase):
    """Proves layer 2 is armed, rather than trusting that it is.

    A guard nobody exercises is indistinguishable from a guard that was
    refactored away ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]]).
    """

    def test_it_raises_when_the_write_would_land_in_production(self):
        import seo_copy_ledger as ledger

        previous = os.environ.get("DUCK_TEST_MODE")
        os.environ["DUCK_TEST_MODE"] = "1"
        try:
            with self.assertRaises(ledger.TestModeRefusalError):
                ledger.save_ledger(
                    {"ledger_version": 1, "rows": {}},
                    path=ledger._FROZEN_PRODUCTION_LEDGER_PATH,
                )
        finally:
            if previous is None:
                os.environ.pop("DUCK_TEST_MODE", None)
            else:
                os.environ["DUCK_TEST_MODE"] = previous

    def test_an_isolated_path_is_not_refused(self):
        """The guard must not block the conftest pattern it exists to allow."""
        import tempfile

        import seo_copy_ledger as ledger

        previous = os.environ.get("DUCK_TEST_MODE")
        os.environ["DUCK_TEST_MODE"] = "1"
        try:
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "seo_copy_ledger.json"
                ledger.save_ledger({"ledger_version": 1, "rows": {}}, path=target)
                self.assertTrue(target.exists())
        finally:
            if previous is None:
                os.environ.pop("DUCK_TEST_MODE", None)
            else:
                os.environ["DUCK_TEST_MODE"] = previous


if __name__ == "__main__":
    unittest.main()
