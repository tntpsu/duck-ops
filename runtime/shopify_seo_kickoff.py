from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from shopify_seo_review import (
    DUCK_OPS_ROOT,
    SEO_REVIEW_CATEGORY_ORDER,
    _next_issue_category,
    build_shopify_seo_audit,
    resend_deferred_shopify_seo_review,
    send_shopify_seo_review_email,
)


SEO_AUDIT_PATH = DUCK_OPS_ROOT / "state" / "shopify_seo_audit.json"
SEO_REVIEW_LATEST_PATH = DUCK_OPS_ROOT / "state" / "shopify_seo_review" / "latest.json"

# The daily kickoff reuses a CACHED audit and only rebuilds it when forced,
# so without a cadence the snapshot silently drifts (it hit 37 days). Refresh
# weekly: fresh enough to reflect recent listing edits, infrequent enough to
# not hit the Shopify API for a near-identical analysis every day. 6.5 rather
# than 7.0 because the kickoff (07:35) runs BEFORE the stamp it compares
# against (07:53) — a 7.0 threshold missed by 18 minutes every single week.
SEO_AUDIT_MAX_AGE_DAYS = 6.5


def _audit_age_days() -> float | None:
    payload = _load_json(SEO_AUDIT_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return None
    raw = payload.get("generated_at")
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return (datetime.now(dt.tzinfo) - dt).total_seconds() / 86400.0


def _audit_is_stale(max_age_days: float = SEO_AUDIT_MAX_AGE_DAYS) -> bool:
    age = _audit_age_days()
    return age is None or age > max_age_days


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


def _high_severity_category(after_category: str | None, audit_payload: dict[str, Any]) -> str | None:
    """The earliest category in ORDER holding a HIGH-severity resource, if any.

    The round-robin cursor below is anti-starvation, but on its own it inverts
    priority: a product with NO SEO title at all is high severity and sits in
    `missing_title`, at the FRONT of the order — so once the cursor has walked
    past it, those products queue behind every cosmetic category for a full
    cycle (on 2026-10-11, 9 high-severity missing titles behind 93 weak-copy
    items). High severity is already this lane's urgency signal: it is what
    bypasses the email cadence gate for a same-day send. Honour it here too.
    """
    for category in SEO_REVIEW_CATEGORY_ORDER:
        if category == after_category:
            # Never re-pick the batch just worked, or a dismissed-but-still-
            # failing category would be re-sent forever.
            continue
        for resource in _select_issue_category_candidates_for(audit_payload, category):
            issues = resource.get("issues") if isinstance(resource.get("issues"), list) else []
            if any(isinstance(i, dict) and str(i.get("severity") or "").lower() == "high" for i in issues):
                return category
    return None


def _select_issue_category_candidates_for(audit_payload: dict[str, Any], category: str) -> list[dict[str, Any]]:
    from shopify_seo_review import _select_issue_category_candidates

    return _select_issue_category_candidates(audit_payload, issue_category=category)


def _choose_next_category(after_category: str | None, audit_payload: dict[str, Any]) -> str | None:
    """Advance past the category the lane last worked on, wrapping to the
    front once the tail is empty. Always restarting at index 0 let a 1-item
    `long_title` batch block 92 items in `weak_title`/`weak_description`
    for weeks (2026-09-07). High-severity work jumps the queue."""
    urgent = _high_severity_category(after_category, audit_payload)
    if urgent:
        return urgent
    if after_category in SEO_REVIEW_CATEGORY_ORDER:
        following = _next_issue_category(after_category, audit_payload)
        if following:
            return following
    return _next_issue_category(None, audit_payload)


def _kickoff_status_for(payload: dict[str, Any] | None) -> str:
    if not payload:
        return "no_remaining_categories"
    return "deferred_by_cadence" if str(payload.get("status") or "") == "deferred_by_cadence" else "emailed"


def kickoff_shopify_seo_review(*, force_audit: bool = False) -> dict[str, Any]:
    # Keep the audit fresh on a weekly cadence regardless of review state.
    # Done at the top (before the parked-review early returns) so a
    # long-parked review can't let the snapshot drift either. build_*
    # persists shopify_seo_audit.json, which is what the freshness card
    # reads, so this also clears the "audit N days stale" yellow.
    audit_refreshed = False
    if force_audit or _audit_is_stale():
        build_shopify_seo_audit()
        audit_refreshed = True

    latest_review = _load_latest_review()
    latest_status = str(latest_review.get("status") or "").strip().lower()
    latest_run_id = str(latest_review.get("run_id") or "").strip() or None
    latest_category = str(latest_review.get("seo_category") or "").strip() or None
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

    if latest_status == "deferred_by_cadence" and latest_run_id:
        # A deferred batch is still the open batch: re-gate and send the SAME
        # run rather than rebuilding a new one every morning (the rebuild loop
        # is what orphaned the 2026-07-06 and 2026-08-31 weak_title batches).
        payload = resend_deferred_shopify_seo_review(latest_run_id)
        if payload is not None:
            status = _kickoff_status_for(payload)
            verb = "Sent" if status == "emailed" else "Still deferred by cadence:"
            return {
                "status": status,
                "summary": f"{verb} the deferred Shopify SEO category review email for {payload.get('category_label') or latest_label}.",
                "run_id": payload.get("run_id"),
                "category_label": payload.get("category_label") or latest_label,
                "item_count": int(payload.get("item_count") or 0),
                "deferred_since": payload.get("deferred_since"),
            }

    # Audit already (re)built above when forced/stale, so read the cache.
    audit_payload = _load_audit_payload(force_audit=False)
    next_category = _choose_next_category(latest_category, audit_payload)
    if not next_category:
        return {
            "status": "no_remaining_categories",
            "summary": "No remaining Shopify SEO issue categories need a new review email right now.",
            "run_id": latest_run_id,
        }

    # Walk forward past categories that build into a no-op batch instead of
    # burning a day per dead category. `near_duplicate_title` is dead by
    # construction (the deterministic title generator derives a title from the
    # resource's own name, so two near-identically named resources can never be
    # differentiated that way), and without this the lane advances one step a
    # day and re-parks the moment it reaches one.
    skipped: list[str] = []
    payload = None
    for _ in range(len(SEO_REVIEW_CATEGORY_ORDER)):
        payload = send_shopify_seo_review_email(
            limit=0,
            force_audit=False,
            review_type="issue_category_batch",
            issue_category=next_category,
            auto_send_next_category=True,
        )
        if str(payload.get("status") or "") != "not_reviewable":
            break
        skipped.append(str(payload.get("category_label") or next_category))
        following = _choose_next_category(next_category, audit_payload)
        if not following or following == next_category:
            payload = None
            break
        next_category = following

    if payload is None or str(payload.get("status") or "") == "not_reviewable":
        summary = "No Shopify SEO category has anything applyable right now."
        if skipped:
            summary = f"{summary[:-1]}; skipped {', '.join(skipped)}."
        return {
            "status": "no_reviewable_categories",
            "summary": summary,
            "run_id": (payload or {}).get("run_id"),
            "skipped_categories": skipped,
            "audit_refreshed": audit_refreshed,
        }

    status = _kickoff_status_for(payload)
    verb = "Sent" if status == "emailed" else "Built (deferred by cadence)"
    return {
        "status": status,
        "summary": f"{verb} the next Shopify SEO category review email for {payload.get('category_label') or next_category}.",
        "run_id": payload.get("run_id"),
        "category_label": payload.get("category_label") or next_category,
        "item_count": int(payload.get("item_count") or 0),
        "skipped_categories": skipped,
        "audit_refreshed": audit_refreshed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely kick off the next Shopify SEO category review email.")
    parser.add_argument("--force-audit", action="store_true", help="Refresh the Shopify SEO audit before choosing the next category.")
    args = parser.parse_args()
    result = kickoff_shopify_seo_review(force_audit=args.force_audit)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
