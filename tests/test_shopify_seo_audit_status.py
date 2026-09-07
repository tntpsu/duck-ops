"""Surface 66 (SEO audit scope) — draft/archived products are not on the
storefront; the audit fetched `status` and never read it, so the operator was
emailed SEO fixes for pages Google cannot see."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runtime"))

import shopify_seo_audit as audit  # noqa: E402


def _product(pid: str, status: str) -> dict:
    return {
        "id": f"gid://shopify/Product/{pid}",
        "title": f"Product {pid}",
        "handle": f"product-{pid}",
        "status": status,
        "seo": {"title": "", "description": ""},
    }


class DraftExclusionTests(unittest.TestCase):
    def test_draft_and_archived_products_are_excluded(self) -> None:
        nodes = {
            "products": [_product("1", "ACTIVE"), _product("2", "DRAFT"), _product("3", "ARCHIVED"), _product("4", "")],
            "collections": [],
            "pages": [],
            "articles": [],
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(audit, "_ensure_shopify_imports", return_value=object()), \
                 patch.object(audit, "_paginate_connection", side_effect=lambda gql, name, fields: nodes[name]), \
                 patch.object(audit, "STATE_PATH", root / "audit.json"), \
                 patch.object(audit, "OUTPUT_MD_PATH", root / "audit.md"):
                payload = audit.build_shopify_seo_audit()
        ids = {r["id"] for r in payload["resources"]}
        self.assertEqual(ids, {"gid://shopify/Product/1", "gid://shopify/Product/4"})
        self.assertEqual(payload["summary"]["skipped_products_by_status"], {"draft": 1, "archived": 1})
        self.assertEqual(payload["by_kind"]["product"]["total"], 2)


if __name__ == "__main__":
    unittest.main()
