from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from shopify_seo_review import (
    DUCK_OPS_ROOT,
    _next_issue_category,
    build_shopify_seo_audit,
    send_shopify_seo_review_email,
)


SEO_AUDIT_PATH = DUCK_OPS_ROOT / "state" / "shopify_seo_audit.json"
SEO_REVIEW_LATEST_PATH = DUCK_OPS_ROOT / "state" / "shopify_seo_review" / "latest.json"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _load_latest_review() -> dict[str, Any]:
    payload = _load_json(SEO_REVIEW_LATEST_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _load_audit_payload(*, force_audit: bool) -> dict[str, Any]:
    payload = build_shopify_seo_audit() if force_audit else _load_json(SEO_AUDIT_PATH, {})
    if not isinstance(payload, dict) or not payload:
        payload = build_shopify_seo_audit()
    return payload if isinstance(payload, dict) else {}


def kickoff_shopify_seo_review(*, force_audit: bool = False) -> dict[str, Any]:
    latest_review = _load_latest_review()
    latest_status = str(latest_review.get("status") or "").strip().lower()
    latest_run_id = str(latest_review.get("run_id") or "").strip() or None
    latest_label = str(latest_review.get("category_label") or latest_review.get("seo_category") or "Shopify SEO review").strip()

    if latest_status == "awaiting_review":
        return {
            "status": "skipped_open_review",
            "summary": f"An SEO review email is already awaiting review for {latest_label}.",
            "run_id": latest_run_id,
            "category_label": latest_label,
        }

    if latest_status == "apply_attempted":
        return {
            "status": "manual_attention_required",
            "summary": f"The latest SEO apply for {latest_label} needs manual attention before another category batch is sent.",
            "run_id": latest_run_id,
            "category_label": latest_label,
        }

    audit_payload = _load_audit_payload(force_audit=force_audit)
    next_category = _next_issue_category(None, audit_payload)
    if not next_category:
        return {
            "status": "no_remaining_categories",
            "summary": "No remaining Shopify SEO issue categories need a new review email right now.",
            "run_id": latest_run_id,
        }

    payload = send_shopify_seo_review_email(
        limit=0,
        force_audit=False,
        review_type="issue_category_batch",
        issue_category=next_category,
        auto_send_next_category=True,
    )
    return {
        "status": "emailed",
        "summary": f"Sent the next Shopify SEO category review email for {payload.get('category_label') or next_category}.",
        "run_id": payload.get("run_id"),
        "category_label": payload.get("category_label") or next_category,
        "item_count": int(payload.get("item_count") or 0),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely kick off the next Shopify SEO category review email.")
    parser.add_argument("--force-audit", action="store_true", help="Refresh the Shopify SEO audit before choosing the next category.")
    args = parser.parse_args()
    result = kickoff_shopify_seo_review(force_audit=args.force_audit)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
