from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

from governance_review_common import OUTPUT_OPERATOR_DIR, STATE_DIR, load_json, now_local_iso, parse_iso, write_json, write_markdown


SEO_AUDIT_PATH = STATE_DIR / "shopify_seo_audit.json"
SEO_REVIEW_RUN_DIR = STATE_DIR / "shopify_seo_review" / "runs"
SEO_REVIEW_LATEST_PATH = STATE_DIR / "shopify_seo_review" / "latest.json"
SEO_WRITEBACK_RECEIPT_DIR = STATE_DIR / "shopify_seo_writeback" / "receipts"
SEO_OUTCOMES_STATE_PATH = STATE_DIR / "shopify_seo_outcomes.json"
SEO_OUTCOMES_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "shopify_seo_outcomes.json"
SEO_OUTCOMES_MD_PATH = OUTPUT_OPERATOR_DIR / "shopify_seo_outcomes.md"

MONITORING_WINDOW_DAYS = 7.0

TITLE_ISSUE_CODES = {
    "duplicate_seo_title",
    "long_seo_title",
    "missing_seo_title",
    "near_duplicate_seo_title",
    "seo_title_matches_raw_title",
    "short_seo_title",
    "weak_generic_seo_title",
}
DESCRIPTION_ISSUE_CODES = {
    "long_seo_description",
    "low_value_seo_copy",
    "missing_seo_description",
    "short_seo_description",
    "weak_generic_seo_description",
}
CATEGORY_TARGET_ISSUES = {
    "missing_title": {"missing_seo_title"},
    "missing_description": {"missing_seo_description"},
    "missing_title_and_description": {"missing_seo_title", "missing_seo_description"},
    "long_title": {"long_seo_title"},
    "long_description": {"long_seo_description"},
    "short_title": {"short_seo_title"},
    "duplicate_title": {"duplicate_seo_title"},
    "near_duplicate_title": {"near_duplicate_seo_title"},
    "weak_title": {"seo_title_matches_raw_title", "weak_generic_seo_title"},
    "weak_description": {"low_value_seo_copy", "weak_generic_seo_description"},
}
CATEGORY_LABELS = {
    "missing_title": "Missing SEO titles",
    "missing_description": "Missing SEO descriptions",
    "missing_title_and_description": "Missing SEO titles + descriptions",
    "long_title": "SEO titles too long",
    "long_description": "SEO descriptions too long",
    "short_title": "SEO titles too short",
    "duplicate_title": "Duplicate SEO titles",
    "near_duplicate_title": "Near-duplicate SEO titles",
    "weak_title": "Weak SEO titles",
    "weak_description": "Weak SEO descriptions",
}
CATEGORY_REVIEW_ORDER = [
    "missing_title",
    "missing_description",
    "long_title",
    "long_description",
    "short_title",
    "duplicate_title",
    "near_duplicate_title",
    "weak_title",
    "weak_description",
]
STATUS_ORDER = {
    "writeback_verification_failed": -1,
    "issue_still_present": 0,
    "missing_from_audit": 1,
    "awaiting_audit_refresh": 2,
    "stable": 3,
    "monitoring": 4,
    "writeback_verified": 5,
}
GUIDANCE_ORDER = {
    "fix_now": 0,
    "investigate_audit": 1,
    "refresh_audit": 2,
    "watch_window": 3,
    "keep_running": 4,
    "validated": 5,
}


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _humanize_category(value: Any) -> str:
    key = _normalize_text(value).lower()
    if key in CATEGORY_LABELS:
        return CATEGORY_LABELS[key]
    text = key.replace("_", " ")
    return text.title() if text else "SEO review"


def _age_days(value: Any, *, now: datetime | None = None) -> float | None:
    parsed = parse_iso(value)
    if parsed is None:
        return None
    current = now or datetime.now().astimezone()
    return round(max(0.0, (current - parsed.astimezone()).total_seconds() / 86400.0), 1)


def _issue_codes(payload: dict[str, Any]) -> set[str]:
    issues = payload.get("issues") if isinstance(payload.get("issues"), list) else []
    return {
        _normalize_text(issue.get("code"))
        for issue in issues
        if isinstance(issue, dict) and _normalize_text(issue.get("code"))
    }


def _resource_map(audit_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    resources = audit_payload.get("resources") if isinstance(audit_payload.get("resources"), list) else []
    return {
        str(resource.get("id") or ""): resource
        for resource in resources
        if isinstance(resource, dict) and str(resource.get("id") or "").strip()
    }


def _target_issue_codes(run_payload: dict[str, Any], item: dict[str, Any]) -> list[str]:
    category = _normalize_text(run_payload.get("seo_category")).lower()
    explicit = CATEGORY_TARGET_ISSUES.get(category)
    if explicit:
        return sorted(explicit)

    apply_title = bool(item.get("apply_seo_title", True))
    apply_description = bool(item.get("apply_seo_description", True))
    codes = _issue_codes(item)
    if apply_title and apply_description:
        return sorted(codes)
    if apply_title:
        return sorted(code for code in codes if code in TITLE_ISSUE_CODES)
    if apply_description:
        return sorted(code for code in codes if code in DESCRIPTION_ISSUE_CODES)
    return sorted(codes)


def _infer_issue_family_category(target_issue_codes: list[str]) -> tuple[str | None, str | None]:
    codes = {code for code in target_issue_codes if _normalize_text(code)}
    if not codes:
        return None, None
    for category, expected in CATEGORY_TARGET_ISSUES.items():
        if codes == set(expected):
            return category, _humanize_category(category)
    return None, None


def _applied_fields(item: dict[str, Any]) -> list[str]:
    fields: list[str] = []
    if bool(item.get("apply_seo_title", True)):
        fields.append("seo_title")
    if bool(item.get("apply_seo_description", True)):
        fields.append("seo_description")
    return fields


def _verification_note(status: str, *, age_days: float | None, remaining_codes: list[str]) -> str:
    if status == "awaiting_audit_refresh":
        return "The latest SEO audit predates this apply event, so the change has not been rechecked yet."
    if status == "missing_from_audit":
        return "The resource was not present in the latest SEO audit, so the applied fix could not be verified."
    if status == "issue_still_present":
        codes = ", ".join(remaining_codes) or "targeted issue"
        return f"The latest SEO audit still reports `{codes}` for this resource."
    if status == "monitoring":
        age_display = "unknown" if age_days is None else f"{age_days:.1f}"
        return f"The targeted SEO issue is cleared, but this fix is only `{age_display}` day(s) old and is still in the observation window."
    age_display = "unknown" if age_days is None else f"{age_days:.1f}"
    return f"The targeted SEO issue is cleared and has stayed clean for `{age_display}` day(s)."


def _build_outcome_item(
    run_payload: dict[str, Any],
    item: dict[str, Any],
    audit_generated_at: str | None,
    resources_by_id: dict[str, dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    applied_at = (
        _normalize_text(((run_payload.get("apply_result") or {}) if isinstance(run_payload.get("apply_result"), dict) else {}).get("applied_at"))
        or _normalize_text(run_payload.get("generated_at"))
        or None
    )
    age_days = _age_days(applied_at, now=now)
    target_issue_codes = _target_issue_codes(run_payload, item)
    inferred_category, inferred_category_label = _infer_issue_family_category(target_issue_codes)
    current_resource = resources_by_id.get(str(item.get("id") or ""))
    current_issue_codes = sorted(_issue_codes(current_resource or {}))
    remaining_codes = sorted(code for code in target_issue_codes if code in current_issue_codes)

    audit_generated = parse_iso(audit_generated_at)
    applied_dt = parse_iso(applied_at)
    if audit_generated is not None and applied_dt is not None and audit_generated < applied_dt:
        status = "awaiting_audit_refresh"
    elif current_resource is None:
        status = "missing_from_audit"
    elif remaining_codes:
        status = "issue_still_present"
    elif age_days is not None and age_days < MONITORING_WINDOW_DAYS:
        status = "monitoring"
    else:
        status = "stable"

    category = _normalize_text(run_payload.get("seo_category"))
    category_label = _normalize_text(run_payload.get("category_label"))
    if not category or category in {"seo_review", "seo review"}:
        category = inferred_category or category
    if not category_label or category_label.lower() in {"seo review"}:
        category_label = inferred_category_label or _humanize_category(category)
    return {
        "run_id": _normalize_text(run_payload.get("run_id")) or None,
        "seo_category": category or None,
        "category_label": category_label or _humanize_category(category),
        "status": status,
        "title": _normalize_text(item.get("title")) or _normalize_text(item.get("resource_url")) or "SEO resource",
        "kind": _normalize_text(item.get("kind")) or None,
        "resource_id": _normalize_text(item.get("id")) or None,
        "resource_url": _normalize_text(item.get("resource_url")) or None,
        "applied_at": applied_at,
        "age_days": age_days,
        "applied_fields": _applied_fields(item),
        "target_issue_codes": target_issue_codes,
        "remaining_target_issue_codes": remaining_codes,
        "current_issue_codes": current_issue_codes,
        "current_seo_title": _normalize_text((current_resource or {}).get("seo_title")) or None,
        "current_seo_description": _normalize_text((current_resource or {}).get("seo_description")) or None,
        "verification_note": _verification_note(status, age_days=age_days, remaining_codes=remaining_codes),
        "traffic_signal": {
            "available": False,
            "status": "unavailable",
            "note": "No search-click or traffic collector is wired into Duck Ops yet, so this monitor only verifies whether the SEO fix stayed resolved.",
        },
    }


def _review_runs() -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    if not SEO_REVIEW_RUN_DIR.exists():
        return runs
    for path in sorted(SEO_REVIEW_RUN_DIR.glob("*.json")):
        payload = load_json(path, {})
        if not isinstance(payload, dict):
            continue
        status = _normalize_text(payload.get("status"))
        if status != "applied":
            continue
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        if not items:
            continue
        payload.setdefault("run_id", path.stem)
        runs.append(payload)
    return runs


def _latest_review_run() -> dict[str, Any]:
    payload = load_json(SEO_REVIEW_LATEST_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _remaining_review_categories(audit_payload: dict[str, Any]) -> list[dict[str, Any]]:
    resources = audit_payload.get("resources") if isinstance(audit_payload.get("resources"), list) else []
    remaining: list[dict[str, Any]] = []
    for category in CATEGORY_REVIEW_ORDER:
        target_codes = set(CATEGORY_TARGET_ISSUES.get(category) or set())
        if not target_codes:
            continue
        matching_resources: list[dict[str, Any]] = []
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            codes = _issue_codes(resource)
            if codes & target_codes:
                matching_resources.append(resource)
        if not matching_resources:
            continue
        issue_count = 0
        for resource in matching_resources:
            issue_count += len(_issue_codes(resource) & target_codes)
        remaining.append(
            {
                "seo_category": category,
                "category_label": _humanize_category(category),
                "resource_count": len(matching_resources),
                "issue_count": issue_count,
            }
        )
    return remaining


def _latest_applied_run(applied_runs: list[dict[str, Any]]) -> dict[str, Any]:
    latest: dict[str, Any] | None = None
    latest_dt: datetime | None = None
    for run_payload in applied_runs:
        result = (run_payload.get("apply_result") or {}) if isinstance(run_payload.get("apply_result"), dict) else {}
        candidate_dt = parse_iso(result.get("applied_at")) or parse_iso(run_payload.get("generated_at"))
        if candidate_dt is None:
            continue
        if latest is None or latest_dt is None or candidate_dt >= latest_dt:
            latest = run_payload
            latest_dt = candidate_dt
    return latest or {}


def _review_chain_surface(audit_payload: dict[str, Any], applied_runs: list[dict[str, Any]]) -> dict[str, Any]:
    latest_review = _latest_review_run()
    latest_status = _normalize_text(latest_review.get("status")).lower()
    latest_category = _normalize_text(latest_review.get("seo_category")) or None
    latest_label = _normalize_text(latest_review.get("category_label")) or _humanize_category(latest_category)
    remaining_categories = _remaining_review_categories(audit_payload)
    latest_applied = _latest_applied_run(applied_runs)
    latest_applied_result = (latest_applied.get("apply_result") or {}) if isinstance(latest_applied.get("apply_result"), dict) else {}

    if latest_status == "awaiting_review":
        chain_state = "awaiting_review"
        headline = f"{latest_label} is currently waiting for a reply apply decision."
        recommended_action = "Reply `apply` to the current Shopify SEO review email after you spot-check the examples."
    elif latest_status == "apply_attempted":
        chain_state = "apply_attention"
        headline = f"{latest_label} needs manual attention before the chain continues."
        recommended_action = "Resolve the latest Shopify SEO apply failures before sending another category batch."
    elif remaining_categories:
        next_category = remaining_categories[0]
        chain_state = "ready_to_send_next"
        headline = f"{next_category.get('category_label')} is the next Shopify SEO category still open in the audit."
        recommended_action = "Run `python runtime/shopify_seo_kickoff.py` to send the next Shopify SEO category review email now, or let the morning kickoff do it."
    elif applied_runs:
        chain_state = "all_clear"
        headline = "The Shopify SEO category chain is clear for now."
        recommended_action = "Keep watching the monitoring window and only restart the chain when a new audit shows a real category backlog again."
    else:
        chain_state = "idle"
        headline = "The Shopify SEO category chain has not started yet."
        recommended_action = "Run `python runtime/shopify_seo_kickoff.py --force-audit` to send the first Shopify SEO category review email."

    return {
        "available": True,
        "chain_state": chain_state,
        "headline": headline,
        "recommended_action": recommended_action,
        "current_review": (
            {
                "run_id": latest_review.get("run_id"),
                "seo_category": latest_category,
                "category_label": latest_label,
                "status": latest_status or None,
                "item_count": int(latest_review.get("item_count") or 0),
                "generated_at": latest_review.get("generated_at"),
                "approval_action": latest_review.get("approval_action"),
                "email_subject": latest_review.get("email_subject"),
            }
            if latest_review
            else {}
        ),
        "last_applied": (
            {
                "run_id": latest_applied.get("run_id"),
                "seo_category": _normalize_text(latest_applied.get("seo_category")) or None,
                "category_label": _normalize_text(latest_applied.get("category_label")) or _humanize_category(latest_applied.get("seo_category")),
                "applied_at": latest_applied_result.get("applied_at") or latest_applied.get("generated_at"),
                "item_count": int(latest_applied.get("item_count") or len(latest_applied.get("items") or [])),
            }
            if latest_applied
            else {}
        ),
        "remaining_categories": remaining_categories[:5],
        "remaining_count": len(remaining_categories),
        "kickoff_command": "python runtime/shopify_seo_kickoff.py",
    }


def _latest_writeback_receipts() -> list[dict[str, Any]]:
    if not SEO_WRITEBACK_RECEIPT_DIR.exists():
        return []
    latest: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in sorted(SEO_WRITEBACK_RECEIPT_DIR.glob("*.json")):
        payload = load_json(path, {})
        if not isinstance(payload, dict):
            continue
        key = (
            _normalize_text(payload.get("resource_kind")).lower(),
            _normalize_text(payload.get("resource_id")),
            _normalize_text(payload.get("source")).lower(),
        )
        if not any(key):
            continue
        current = latest.get(key)
        current_dt = parse_iso((current or {}).get("verified_at")) if isinstance(current, dict) else None
        candidate_dt = parse_iso(payload.get("verified_at"))
        if current is None or (candidate_dt is not None and (current_dt is None or candidate_dt >= current_dt)):
            latest[key] = payload
    return list(latest.values())


def _writeback_outcome_item(receipt: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    verified_at = _normalize_text(receipt.get("verified_at")) or None
    failure_codes = [_normalize_text(code) for code in list(receipt.get("failure_codes") or []) if _normalize_text(code)]
    status = "writeback_verification_failed" if failure_codes or _normalize_text(receipt.get("status")).lower() != "verified" else "writeback_verified"
    lane = _normalize_text(receipt.get("lane"))
    label = f"{lane.replace('_', ' ').title()} SEO writeback" if lane else "SEO writeback"
    return {
        "run_id": _normalize_text(receipt.get("run_id")) or None,
        "seo_category": None,
        "category_label": label,
        "status": status,
        "title": _normalize_text(receipt.get("title")) or "SEO writeback",
        "kind": _normalize_text(receipt.get("resource_kind")) or None,
        "resource_id": _normalize_text(receipt.get("resource_id")) or None,
        "resource_url": _normalize_text(receipt.get("resource_url")) or None,
        "applied_at": verified_at,
        "age_days": _age_days(verified_at, now=now),
        "applied_fields": list(receipt.get("applied_fields") or []),
        "target_issue_codes": [],
        "remaining_target_issue_codes": failure_codes,
        "current_issue_codes": failure_codes,
        "current_seo_title": _normalize_text(((receipt.get("observed") or {}).get("seo_title"))),
        "current_seo_description": _normalize_text(((receipt.get("observed") or {}).get("seo_description"))),
        "verification_note": _normalize_text(receipt.get("summary")) or "SEO writeback receipt captured.",
        "traffic_signal": {
            "available": False,
            "status": "unavailable",
            "note": "Immediate writeback verification confirms fields stuck in Shopify, but it does not measure organic search lift yet.",
        },
        "lane": lane or None,
        "source": _normalize_text(receipt.get("source")) or None,
    }


def _category_summary(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in items:
        category = _normalize_text(item.get("seo_category")) or "uncategorized"
        entry = grouped.setdefault(
            category,
            {
                "seo_category": category,
                "category_label": item.get("category_label") or _humanize_category(category),
                "applied_item_count": 0,
                "stable_count": 0,
                "monitoring_count": 0,
                "issue_still_present_count": 0,
                "missing_from_audit_count": 0,
                "awaiting_audit_refresh_count": 0,
            },
        )
        entry["applied_item_count"] += 1
        entry[f"{item.get('status')}_count"] += 1
    return sorted(grouped.values(), key=lambda item: (-int(item.get("applied_item_count") or 0), str(item.get("seo_category") or "")))


def _verification_truth(monitored_items: list[dict[str, Any]], writeback_items: list[dict[str, Any]]) -> dict[str, Any]:
    writeback_failed_count = sum(1 for item in writeback_items if item.get("status") == "writeback_verification_failed")
    writeback_verified_count = sum(1 for item in writeback_items if item.get("status") == "writeback_verified")
    stable_count = sum(1 for item in monitored_items if item.get("status") == "stable")
    monitoring_count = sum(1 for item in monitored_items if item.get("status") == "monitoring")
    issue_still_present_count = sum(1 for item in monitored_items if item.get("status") == "issue_still_present")
    missing_from_audit_count = sum(1 for item in monitored_items if item.get("status") == "missing_from_audit")
    awaiting_audit_refresh_count = sum(1 for item in monitored_items if item.get("status") == "awaiting_audit_refresh")

    if writeback_failed_count > 0:
        label = "writeback_failing"
        headline = "Immediate Shopify SEO verification is still catching failures."
        note = (
            f"`{writeback_failed_count}` writeback receipt(s) failed immediate verification, so do not trust the longer-term audit view alone yet."
        )
        recommended_action = "Fix the writeback verification failures first, then use the audit monitor to judge whether those fixes stay resolved."
    elif issue_still_present_count > 0 or missing_from_audit_count > 0:
        label = "reopened"
        headline = "Some applied SEO fixes are not staying resolved cleanly."
        note = (
            f"`{issue_still_present_count}` targeted issue(s) are still present and `{missing_from_audit_count}` item(s) could not be verified from the latest audit."
        )
        recommended_action = "Prioritize the reopening categories before sending more broad SEO apply batches."
    elif awaiting_audit_refresh_count > 0 and stable_count == 0 and monitoring_count == 0:
        label = "awaiting_recheck"
        headline = "Recent SEO applies are waiting on a fresh audit."
        note = (
            f"`{awaiting_audit_refresh_count}` item(s) were applied after the latest audit, so the lane is waiting for recheck truth rather than showing durable wins yet."
        )
        recommended_action = "Refresh the SEO audit before drawing conclusions from the latest apply run."
    elif monitoring_count > 0 and stable_count == 0:
        label = "monitoring"
        headline = "SEO fixes are clearing so far, but they are still inside the observation window."
        note = (
            f"`{monitoring_count}` item(s) are currently clear, but they have not aged beyond the {MONITORING_WINDOW_DAYS:.0f}-day monitoring window yet."
        )
        recommended_action = "Let the monitoring window finish before treating these categories as durable wins."
    elif monitoring_count > 0 or stable_count > 0 or writeback_verified_count > 0:
        label = "healthy_with_watch"
        headline = "SEO cleanup is moving in the right direction."
        note = (
            f"`{stable_count}` stable item(s), `{monitoring_count}` monitored item(s), and `{writeback_verified_count}` immediate writeback confirmation(s) show the lane is improving even though traffic lift is not wired yet."
        )
        recommended_action = "Keep the current SEO review/apply rhythm, but use category guidance to decide where to focus the next batch."
    else:
        label = "idle"
        headline = "No applied SEO fixes are being tracked yet."
        note = "Duck Ops has not observed any applied SEO review runs or writeback receipts yet."
        recommended_action = "Run an SEO apply batch first so outcome monitoring has something real to judge."

    return {
        "label": label,
        "headline": headline,
        "note": note,
        "recommended_action": recommended_action,
    }


def _category_guidance(category_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in category_rows:
        category = _normalize_text(item.get("seo_category")) or "uncategorized"
        label = _normalize_text(item.get("category_label")) or _humanize_category(category)
        total = int(item.get("applied_item_count") or 0)
        stable = int(item.get("stable_count") or 0)
        monitoring = int(item.get("monitoring_count") or 0)
        open_count = int(item.get("issue_still_present_count") or 0)
        missing = int(item.get("missing_from_audit_count") or 0)
        awaiting = int(item.get("awaiting_audit_refresh_count") or 0)

        if open_count > 0:
            decision = "fix_now"
            title = f"`{label}` is reopening after apply."
            summary = "This category still shows targeted SEO issues after an apply, so the current copy or apply path needs inspection before more volume."
            recommended_action = f"Spot-check `{label}` items first and tighten the apply logic or copy before sending another batch in this category."
        elif missing > 0:
            decision = "investigate_audit"
            title = f"`{label}` is missing from the latest audit coverage."
            summary = "Duck Ops cannot verify whether these fixes held because the latest audit did not include the resources."
            recommended_action = f"Confirm why `{label}` items are missing from the SEO audit before treating this category as healthy."
        elif awaiting > 0:
            decision = "refresh_audit"
            title = f"`{label}` needs a fresh audit before it can be judged."
            summary = "Recent applies landed after the latest audit snapshot, so this category is waiting on recheck truth rather than durable outcome evidence."
            recommended_action = f"Refresh the SEO audit before making more decisions about `{label}`."
        elif monitoring > 0 and stable == 0:
            decision = "watch_window"
            title = f"`{label}` is clear so far, but still in the monitoring window."
            summary = "The targeted issues are currently cleared, but the fixes are still too fresh to call them durable."
            recommended_action = f"Keep `{label}` in watch mode until some of these fixes age into stable wins."
        elif monitoring > 0 and stable > 0:
            decision = "keep_running"
            title = f"`{label}` has older wins and newer fixes still under watch."
            summary = "This category already has some durable proof, and the newer fixes are still progressing through the observation window."
            recommended_action = f"Keep `{label}` in the normal rotation while watching the newest fixes age into stable outcomes."
        else:
            decision = "validated"
            title = f"`{label}` is proving durable."
            summary = "Tracked fixes in this category are staying resolved cleanly enough to count as durable outcome evidence."
            recommended_action = f"Keep `{label}` in the normal monthly SEO rotation."

        confidence = "high" if total >= 10 or open_count > 0 or stable >= 3 else "medium"
        items.append(
            {
                "seo_category": category,
                "category_label": label,
                "decision": decision,
                "title": title,
                "summary": summary,
                "recommended_action": recommended_action,
                "confidence": confidence,
                "evidence": (
                    f"total={total}, stable={stable}, monitoring={monitoring}, "
                    f"open={open_count}, missing={missing}, awaiting={awaiting}"
                ),
                "applied_item_count": total,
                "stable_count": stable,
                "monitoring_count": monitoring,
                "issue_still_present_count": open_count,
                "missing_from_audit_count": missing,
                "awaiting_audit_refresh_count": awaiting,
            }
        )

    items.sort(
        key=lambda entry: (
            GUIDANCE_ORDER.get(str(entry.get("decision") or ""), 99),
            -int(entry.get("issue_still_present_count") or 0),
            -int(entry.get("awaiting_audit_refresh_count") or 0),
            -int(entry.get("applied_item_count") or 0),
            str(entry.get("category_label") or ""),
        )
    )
    return items[:5]


def build_shopify_seo_outcomes_payload() -> dict[str, Any]:
    audit_payload = load_json(SEO_AUDIT_PATH, {})
    if not isinstance(audit_payload, dict):
        audit_payload = {}
    resources_by_id = _resource_map(audit_payload)
    audit_generated_at = _normalize_text(audit_payload.get("generated_at")) or None
    now = datetime.now().astimezone()

    monitored_items: list[dict[str, Any]] = []
    applied_runs = _review_runs()
    for run_payload in applied_runs:
        items = run_payload.get("items") if isinstance(run_payload.get("items"), list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            if not _applied_fields(item):
                continue
            monitored_items.append(
                _build_outcome_item(
                    run_payload,
                    item,
                    audit_generated_at,
                    resources_by_id,
                    now=now,
                )
            )

    writeback_items = [
        _writeback_outcome_item(receipt, now=now)
        for receipt in _latest_writeback_receipts()
        if isinstance(receipt, dict)
    ]
    category_summary = _category_summary(monitored_items)
    verification_truth = _verification_truth(monitored_items, writeback_items)
    category_guidance = _category_guidance(category_summary)
    review_chain = _review_chain_surface(audit_payload, applied_runs)
    monitored_items.sort(
        key=lambda item: (
            STATUS_ORDER.get(str(item.get("status") or ""), 9),
            -(parse_iso(item.get("applied_at")).timestamp() if parse_iso(item.get("applied_at")) is not None else 0.0),
            str(item.get("title") or "").lower(),
        )
    )

    writeback_attention = [item for item in writeback_items if item.get("status") == "writeback_verification_failed"]
    attention_items = (
        writeback_attention
        + [item for item in monitored_items if item.get("status") in {"issue_still_present", "missing_from_audit", "awaiting_audit_refresh"}]
    )[:5]
    recent_wins = [item for item in monitored_items if item.get("status") in {"stable", "monitoring"}]
    recent_wins = [item for item in writeback_items if item.get("status") == "writeback_verified" and item.get("lane") in {"blog", "newduck"}] + recent_wins
    recent_wins.sort(
        key=lambda item: (
            0 if item.get("status") == "stable" else 1,
            -(parse_iso(item.get("applied_at")).timestamp() if parse_iso(item.get("applied_at")) is not None else 0.0),
        )
    )

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Track immediate Shopify SEO writeback verification now, then keep monitoring whether applied SEO fixes stay resolved after fresh audits land.",
            "audit_generated_at": audit_generated_at,
            "applied_run_count": len(applied_runs),
            "applied_item_count": len(monitored_items),
            "stable_count": sum(1 for item in monitored_items if item.get("status") == "stable"),
            "monitoring_count": sum(1 for item in monitored_items if item.get("status") == "monitoring"),
            "issue_still_present_count": sum(1 for item in monitored_items if item.get("status") == "issue_still_present"),
            "missing_from_audit_count": sum(1 for item in monitored_items if item.get("status") == "missing_from_audit"),
            "awaiting_audit_refresh_count": sum(1 for item in monitored_items if item.get("status") == "awaiting_audit_refresh"),
            "writeback_receipt_count": len(writeback_items),
            "writeback_verified_count": sum(1 for item in writeback_items if item.get("status") == "writeback_verified"),
            "writeback_failed_count": sum(1 for item in writeback_items if item.get("status") == "writeback_verification_failed"),
            "traffic_signal_available_count": 0,
            "traffic_signal_note": "No search-click or traffic collector is wired into Duck Ops yet, so this monitor currently measures durable SEO cleanup rather than organic lift.",
        },
        "verification_truth": verification_truth,
        "review_chain": review_chain,
        "category_guidance": category_guidance,
        "attention_items": attention_items,
        "recent_wins": recent_wins[:5],
        "by_category": category_summary,
        "items": monitored_items,
        "paths": {
            "seo_audit": str(SEO_AUDIT_PATH),
            "seo_review_runs": str(SEO_REVIEW_RUN_DIR),
            "seo_writeback_receipts": str(SEO_WRITEBACK_RECEIPT_DIR),
        },
    }
    return payload


def render_shopify_seo_outcomes_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    lines = [
        "# Shopify SEO Outcomes",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Latest SEO audit: `{summary.get('audit_generated_at') or 'unknown'}`",
        f"- Applied runs tracked: `{summary.get('applied_run_count') or 0}`",
        f"- Applied items tracked: `{summary.get('applied_item_count') or 0}`",
        f"- Stable fixes: `{summary.get('stable_count') or 0}`",
        f"- Monitoring window: `{summary.get('monitoring_count') or 0}`",
        f"- Still-open targeted issues: `{summary.get('issue_still_present_count') or 0}`",
        f"- Missing from latest audit: `{summary.get('missing_from_audit_count') or 0}`",
        f"- Awaiting audit refresh: `{summary.get('awaiting_audit_refresh_count') or 0}`",
        f"- Immediate writeback receipts: `{summary.get('writeback_receipt_count') or 0}`",
        f"- Immediate writeback failures: `{summary.get('writeback_failed_count') or 0}`",
        f"- Traffic signals available: `{summary.get('traffic_signal_available_count') or 0}`",
        "",
        str(summary.get("headline") or ""),
        "",
        str(summary.get("traffic_signal_note") or ""),
        "",
        "## Outcome Truth",
        "",
    ]

    verification_truth = payload.get("verification_truth") if isinstance(payload.get("verification_truth"), dict) else {}
    if verification_truth.get("headline"):
        lines.append(f"- Headline: {verification_truth.get('headline')}")
    if verification_truth.get("note"):
        lines.append(f"- Note: {verification_truth.get('note')}")
    if verification_truth.get("recommended_action"):
        lines.append(f"- Recommended action: {verification_truth.get('recommended_action')}")
    lines.extend(["", "## Review Chain", ""])

    review_chain = payload.get("review_chain") if isinstance(payload.get("review_chain"), dict) else {}
    if not review_chain.get("available"):
        lines.append("Shopify SEO review-chain status is not available yet.")
        lines.append("")
    else:
        lines.append(f"- State: `{review_chain.get('chain_state') or 'unknown'}`")
        if review_chain.get("headline"):
            lines.append(f"- Headline: {review_chain.get('headline')}")
        if review_chain.get("recommended_action"):
            lines.append(f"- Recommended action: {review_chain.get('recommended_action')}")
        current_review = review_chain.get("current_review") if isinstance(review_chain.get("current_review"), dict) else {}
        if current_review.get("run_id"):
            lines.append(
                f"- Current review: `{current_review.get('category_label') or current_review.get('seo_category')}` | status `{current_review.get('status')}` | items `{current_review.get('item_count') or 0}`"
            )
        last_applied = review_chain.get("last_applied") if isinstance(review_chain.get("last_applied"), dict) else {}
        if last_applied.get("run_id"):
            lines.append(
                f"- Last applied: `{last_applied.get('category_label') or last_applied.get('seo_category')}` | items `{last_applied.get('item_count') or 0}` | applied `{last_applied.get('applied_at') or 'unknown'}`"
            )
        lines.append(f"- Remaining categories: `{review_chain.get('remaining_count') or 0}`")
        remaining_categories = review_chain.get("remaining_categories") if isinstance(review_chain.get("remaining_categories"), list) else []
        if remaining_categories:
            for item in remaining_categories:
                lines.append(
                    f"  - {item.get('category_label')} | resources `{item.get('resource_count') or 0}` | issues `{item.get('issue_count') or 0}`"
                )
        lines.append("")
    lines.extend(["", "## Category Guidance", ""])

    category_guidance = payload.get("category_guidance") if isinstance(payload.get("category_guidance"), list) else []
    if not category_guidance:
        lines.append("No category guidance is available yet.")
        lines.append("")
    else:
        for item in category_guidance:
            lines.append(f"- {item.get('category_label')} | `{item.get('decision')}` | `{item.get('confidence')}`")
            lines.append(f"  Why: {item.get('summary')}")
            lines.append(f"  Evidence: {item.get('evidence')}")
            lines.append(f"  Recommended action: {item.get('recommended_action')}")
            lines.append("")

    lines.extend(["## Needs Attention", ""])

    attention_items = payload.get("attention_items") if isinstance(payload.get("attention_items"), list) else []
    if not attention_items:
        lines.append("No SEO outcome items need attention right now.")
        lines.append("")
    else:
        for item in attention_items:
            lines.append(f"- {item.get('title')} | `{item.get('category_label')}` | `{item.get('status')}`")
            if item.get("resource_url"):
                lines.append(f"  Path: `{item.get('resource_url')}`")
            if item.get("applied_at"):
                lines.append(f"  Applied: `{item.get('applied_at')}`")
            if item.get("age_days") is not None:
                lines.append(f"  Age: `{item.get('age_days')}` day(s)")
            if item.get("remaining_target_issue_codes"):
                lines.append(f"  Remaining issues: `{', '.join(item.get('remaining_target_issue_codes') or [])}`")
            lines.append(f"  Note: {item.get('verification_note')}")
            lines.append("")

    lines.extend(["## Recent Wins", ""])
    recent_wins = payload.get("recent_wins") if isinstance(payload.get("recent_wins"), list) else []
    if not recent_wins:
        lines.append("No verified SEO wins are tracked yet.")
        lines.append("")
    else:
        for item in recent_wins:
            lines.append(f"- {item.get('title')} | `{item.get('category_label')}` | `{item.get('status')}`")
            if item.get("resource_url"):
                lines.append(f"  Path: `{item.get('resource_url')}`")
            if item.get("applied_at"):
                lines.append(f"  Applied: `{item.get('applied_at')}`")
            if item.get("age_days") is not None:
                lines.append(f"  Age: `{item.get('age_days')}` day(s)")
            lines.append(f"  Note: {item.get('verification_note')}")
            lines.append("")

    lines.extend(["## Category Summary", ""])
    category_rows = payload.get("by_category") if isinstance(payload.get("by_category"), list) else []
    if not category_rows:
        lines.append("No applied SEO review categories are available yet.")
        lines.append("")
    else:
        for item in category_rows:
            lines.append(
                f"- `{item.get('category_label')}`: total `{item.get('applied_item_count') or 0}` | stable `{item.get('stable_count') or 0}` | monitoring `{item.get('monitoring_count') or 0}` | still open `{item.get('issue_still_present_count') or 0}` | missing `{item.get('missing_from_audit_count') or 0}` | awaiting audit `{item.get('awaiting_audit_refresh_count') or 0}`"
            )
        lines.append("")

    return "\n".join(lines)


def build_shopify_seo_outcomes() -> dict[str, Any]:
    payload = build_shopify_seo_outcomes_payload()
    write_json(SEO_OUTCOMES_STATE_PATH, payload)
    write_json(SEO_OUTCOMES_OPERATOR_JSON_PATH, payload)
    write_markdown(SEO_OUTCOMES_MD_PATH, render_shopify_seo_outcomes_markdown(payload))
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Shopify SEO outcomes monitor.")
    parser.parse_args()
    payload = build_shopify_seo_outcomes()
    print(
        {
            "generated_at": payload.get("generated_at"),
            "applied_item_count": ((payload.get("summary") or {}).get("applied_item_count") or 0),
            "attention_item_count": len(payload.get("attention_items") or []),
        }
    )


if __name__ == "__main__":
    main()
