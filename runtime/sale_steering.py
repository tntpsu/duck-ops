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

from typing import Any

from governance_review_common import DUCK_OPS_ROOT, load_json
from seo_demand_context import load_seo_demand_context

CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"


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
