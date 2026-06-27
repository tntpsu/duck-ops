"""Surface 42: steer the weekly Shopify SALE target list by GA4 verdict.

Don't discount proven winners (verdict PROMOTE → drop from the sale, protect
margin); prioritize the leaks (verdict FIX → high traffic, low engagement →
keep + move to the front so a discount can convert them); leave everything else
(neutral / no GA4 match) unchanged.

Read-only and fail-soft: no/stale GA4 data → targets pass through untouched, so
the weekly sale behaves exactly as it did before this surface. The live discount
apply still runs through evaluate_weekly_sale_policy's manual-review/approval
gate — this only reshapes the candidate list feeding that gate.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, load_json, now_local_iso
from workflow_control import TestModeRefusalError
from seo_demand_context import load_seo_demand_context

CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"
# Directives file the duckAgent weekly-sale flow reads (file-based cross-repo,
# same pattern as the SEO writeback receipts — no cross-repo import).
SALE_STEERING_PATH = DUCK_OPS_ROOT / "state" / "sale_steering.json"
_FROZEN_PRODUCTION_SALE_STEERING_PATH = SALE_STEERING_PATH.resolve()


def _id_to_title(catalog: dict[str, Any]) -> dict[str, str]:
    items = catalog.get("items") if isinstance(catalog, dict) else {}
    return {str(cid): str(it.get("title") or "")
            for cid, it in (items or {}).items() if isinstance(it, dict)}


def steer_sale_targets(targets: list[dict[str, Any]], *, demand_context: Any = None,
                       id_to_title: dict[str, str] | None = None
                       ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (kept, dropped). dropped = PROMOTE winners excluded from the sale;
    kept = FIX-prioritized then the rest, order otherwise preserved. Fail-soft:
    empty GA4 context → (targets unchanged, [])."""
    if demand_context is None:
        demand_context = load_seo_demand_context()
    if id_to_title is None:
        id_to_title = _id_to_title(load_json(CATALOG_INDEX_PATH, {}))
    if getattr(demand_context, "is_empty", True):
        return list(targets), []

    fix: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for t in targets:
        pid = str(t.get("product_id") or "").strip()
        title = id_to_title.get(pid) or str(t.get("product_title") or t.get("title") or "")
        sig = demand_context.listing_signal(title) if title else None
        verdict = (sig or {}).get("verdict")
        if verdict == "promote":
            dropped.append({**t, "_steer_verdict": "promote",
                            "_steer_reason": "PROMOTE winner — not discounted (protect margin)"})
        elif verdict == "fix":
            fix.append({**t, "_steer_verdict": "fix",
                        "_steer_reason": "FIX (high traffic, low engagement) — prioritized to convert"})
        else:
            rest.append(t)
    return fix + rest, dropped


# --------------------------------------------------------------------------
# Directives producer — duckAgent's weekly-sale flow reads sale_steering.json
# --------------------------------------------------------------------------

def build_steering_directives(*, demand_context: Any = None,
                              catalog: dict[str, Any] | None = None) -> dict[str, Any]:
    """Per-catalog-product GA4 verdict → {exclude (promote), prioritize (fix)}
    product-id lists. available:false (empty lists) when GA4 data is missing/stale
    so the consumer passes targets through unchanged."""
    if demand_context is None:
        demand_context = load_seo_demand_context()
    if catalog is None:
        catalog = load_json(CATALOG_INDEX_PATH, {})
    id2title = _id_to_title(catalog)
    exclude: list[str] = []
    prioritize: list[str] = []
    available = not getattr(demand_context, "is_empty", True)
    if available:
        for pid, title in id2title.items():
            if not title:
                continue
            verdict = (demand_context.listing_signal(title) or {}).get("verdict")
            if verdict == "promote":
                exclude.append(pid)
            elif verdict == "fix":
                prioritize.append(pid)
    return {
        "generated_at": now_local_iso(),
        "available": available,
        "exclude_product_ids": sorted(set(exclude)),
        "prioritize_product_ids": sorted(set(prioritize)),
    }


def write_steering_directives(payload: dict[str, Any], path: Any = None) -> Path:
    out = Path(path or SALE_STEERING_PATH)
    if os.environ.get("DUCK_TEST_MODE") == "1" and \
            out.resolve() == _FROZEN_PRODUCTION_SALE_STEERING_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but SALE_STEERING_PATH still points at production. "
            "Monkeypatch sale_steering.SALE_STEERING_PATH to a tmp path.")
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Write GA4-verdict weekly-sale steering directives for the duckAgent sale flow.")
    parser.add_argument("--dry-run", action="store_true", help="Print, don't write")
    args = parser.parse_args()
    directives = build_steering_directives()
    print(f"[sale-steering] available={directives['available']} "
          f"exclude_promote={len(directives['exclude_product_ids'])} "
          f"prioritize_fix={len(directives['prioritize_product_ids'])}")
    if not args.dry_run:
        out = write_steering_directives(directives)
        print(f"  -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
