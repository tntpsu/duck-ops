#!/usr/bin/env python3
"""
Inventory truth surface for stock-watch and print-queue recommendations.

The print queue starts from demand signals. This layer makes the stock evidence
explicit so the Business Desk does not imply a duck is low-stock before live
inventory has been verified.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from governance_review_common import (
    DUCK_AGENT_ROOT,
    OUTPUT_OPERATOR_DIR,
    STATE_DIR,
    load_json,
    now_local_iso,
    write_json,
    write_markdown,
)


PRINT_QUEUE_CANDIDATES_PATH = STATE_DIR / "normalized" / "print_queue_candidates.json"
PRODUCTS_CACHE_PATH = DUCK_AGENT_ROOT / "cache" / "products_cache.json"
WEEKLY_REPORT_PATH = DUCK_AGENT_ROOT / "cache" / "weekly_report.json"
INVENTORY_TRUTH_PATH = STATE_DIR / "inventory_truth.json"
INVENTORY_TRUTH_MD_PATH = OUTPUT_OPERATOR_DIR / "inventory_truth.md"

LOW_STOCK_THRESHOLD = 2


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _priority_rank(value: Any) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(value or "low").lower(), 9)


def _load_items_map(path: Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, {})
    if not isinstance(payload, dict):
        return {}
    items = payload.get("items")
    if isinstance(items, dict):
        return {str(key): value for key, value in items.items() if isinstance(value, dict)}
    if isinstance(items, list):
        mapped: dict[str, dict[str, Any]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            product_id = str(item.get("id") or item.get("product_id") or "").strip()
            if product_id:
                mapped[product_id] = item
        return mapped
    return {}


def _candidate_items(print_queue_candidates: dict[str, Any] | list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    payload: Any = print_queue_candidates
    if payload is None:
        payload = load_json(PRINT_QUEUE_CANDIDATES_PATH, {})
    if isinstance(payload, dict):
        payload = payload.get("items") or []
    return [item for item in list(payload or []) if isinstance(item, dict)]


def _inventory_values(product: dict[str, Any] | None) -> list[int]:
    if not isinstance(product, dict):
        return []
    values: list[int] = []
    for key in ("inventory_quantity", "total_inventory", "available_quantity", "quantity_available"):
        parsed = _as_int(product.get(key))
        if parsed is not None:
            values.append(parsed)
    for variant in list(product.get("variants") or []):
        if not isinstance(variant, dict):
            continue
        for key in ("inventory_quantity", "available_quantity", "quantity_available"):
            parsed = _as_int(variant.get(key))
            if parsed is not None:
                values.append(parsed)
    return values


def _truth_item(
    candidate: dict[str, Any],
    *,
    products: dict[str, dict[str, Any]],
    weekly_report: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    product_id = str(candidate.get("product_id") or "").strip()
    product = products.get(product_id) or weekly_report.get(product_id) or {}
    inventory_values = _inventory_values(product)
    demand = _as_int(candidate.get("recent_demand")) or 0
    lifetime_demand = _as_int(candidate.get("lifetime_demand")) or 0
    source_confidence = float(candidate.get("confidence") or 0.0)

    if inventory_values:
        cached_total = sum(max(0, value) for value in inventory_values)
        evidence_level = "confirmed_low_stock" if cached_total <= LOW_STOCK_THRESHOLD else "cached_inventory"
        stock_evidence = "cached_inventory_low" if evidence_level == "confirmed_low_stock" else "cached_inventory_available"
        confidence = min(0.82, max(source_confidence, 0.65))
        recommended_action = (
            "Queue a replenishment print after checking the live Shopify product screen."
            if evidence_level == "confirmed_low_stock"
            else "No print action yet. Recheck live inventory before treating this as low stock."
        )
        evidence_summary = f"Cached inventory total is {cached_total}; live Shopify verification is still recommended."
        live_inventory_available = False
    else:
        cached_total = None
        evidence_level = "demand_only"
        stock_evidence = "not_yet_available"
        confidence = min(source_confidence, 0.45)
        recommended_action = "Verify live Shopify inventory before queuing any replenishment print."
        evidence_summary = "This is a demand signal only; no cached or live stock quantity is available."
        live_inventory_available = False

    return {
        "product_id": product_id or None,
        "product_title": candidate.get("product_title") or product.get("title"),
        "priority": str(candidate.get("priority") or "low").lower(),
        "recent_demand": demand,
        "lifetime_demand": lifetime_demand,
        "inventory_evidence_level": evidence_level,
        "stock_evidence": stock_evidence,
        "cached_inventory_total": cached_total,
        "live_inventory_available": live_inventory_available,
        "confidence": round(confidence, 2),
        "evidence_summary": evidence_summary,
        "recommended_action": recommended_action,
        "source_artifact_id": candidate.get("artifact_id"),
        "source_refs": list(candidate.get("source_refs") or [])[:3],
    }


def build_inventory_truth(
    *,
    print_queue_candidates: dict[str, Any] | list[dict[str, Any]] | None = None,
    write_outputs: bool = True,
) -> dict[str, Any]:
    products = _load_items_map(PRODUCTS_CACHE_PATH)
    weekly_report = _load_items_map(WEEKLY_REPORT_PATH)
    items = [
        _truth_item(candidate, products=products, weekly_report=weekly_report)
        for candidate in _candidate_items(print_queue_candidates)
    ]
    items.sort(
        key=lambda item: (
            _priority_rank(item.get("priority")),
            -int(item.get("recent_demand") or 0),
            str(item.get("product_title") or "").lower(),
        )
    )

    demand_only_count = sum(1 for item in items if item.get("inventory_evidence_level") == "demand_only")
    confirmed_low_stock_count = sum(1 for item in items if item.get("inventory_evidence_level") == "confirmed_low_stock")
    cached_inventory_count = sum(1 for item in items if item.get("inventory_evidence_level") in {"cached_inventory", "confirmed_low_stock"})
    live_inventory_available_count = sum(1 for item in items if item.get("live_inventory_available"))

    if confirmed_low_stock_count:
        status = "print_review_needed"
        headline = f"{confirmed_low_stock_count} candidate(s) have cached low-stock evidence."
        recommended_action = "Verify live Shopify inventory, then queue replenishment prints for confirmed low-stock ducks."
    elif demand_only_count:
        status = "demand_only"
        headline = f"{demand_only_count} candidate(s) are demand-only and need live stock verification."
        recommended_action = "Treat these as stock-watch leads, not print commands, until live inventory is checked."
    else:
        status = "clear"
        headline = "No stock-watch candidates need inventory verification right now."
        recommended_action = "No inventory action needed."

    payload = {
        "generated_at": now_local_iso(),
        "surface_version": 1,
        "status": status,
        "headline": headline,
        "recommended_action": recommended_action,
        "source": "print_queue_candidates_plus_product_cache",
        "source_paths": {
            "print_queue_candidates": str(PRINT_QUEUE_CANDIDATES_PATH),
            "products_cache": str(PRODUCTS_CACHE_PATH),
            "weekly_report": str(WEEKLY_REPORT_PATH),
        },
        "summary": {
            "candidate_count": len(items),
            "demand_only_count": demand_only_count,
            "cached_inventory_count": cached_inventory_count,
            "confirmed_low_stock_count": confirmed_low_stock_count,
            "live_inventory_available_count": live_inventory_available_count,
        },
        "items": items[:20],
    }
    if write_outputs:
        write_json(INVENTORY_TRUTH_PATH, payload)
        write_markdown(INVENTORY_TRUTH_MD_PATH, render_inventory_truth_markdown(payload))
    return payload


def render_inventory_truth_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    items = [item for item in list(payload.get("items") or []) if isinstance(item, dict)]
    lines = [
        "# Inventory Truth",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Status: `{payload.get('status') or 'unknown'}`",
        f"- Candidates: `{summary.get('candidate_count', len(items))}`",
        f"- Demand-only: `{summary.get('demand_only_count', 0)}`",
        f"- Cached inventory evidence: `{summary.get('cached_inventory_count', 0)}`",
        f"- Confirmed low-stock candidates: `{summary.get('confirmed_low_stock_count', 0)}`",
        f"- Live inventory available: `{summary.get('live_inventory_available_count', 0)}`",
        f"- Headline: {payload.get('headline')}",
        f"- Recommended action: {payload.get('recommended_action')}",
        "",
        "## Candidates",
        "",
    ]
    if not items:
        lines.append("No stock-watch candidates are staged right now.")
    for item in items:
        lines.append(
            f"- {item.get('product_title') or item.get('product_id') or 'Unknown product'} | "
            f"`{item.get('inventory_evidence_level')}` | demand `{item.get('recent_demand', 0)}`"
        )
        lines.append(f"  Evidence: {item.get('evidence_summary')}")
        lines.append(f"  Next: {item.get('recommended_action')}")
    return "\n".join(lines) + "\n"


def main() -> None:
    build_inventory_truth(write_outputs=True)


if __name__ == "__main__":
    main()
