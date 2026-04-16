from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

from governance_review_common import OUTPUT_OPERATOR_DIR, STATE_DIR, load_json, now_local_iso, parse_iso, write_json, write_markdown


SEO_AUDIT_PATH = STATE_DIR / "shopify_seo_audit.json"
SEO_REVIEW_RUN_DIR = STATE_DIR / "shopify_seo_review" / "runs"
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
    "long_title": {"long_seo_title"},
    "long_description": {"long_seo_description"},
    "short_title": {"short_seo_title"},
    "duplicate_title": {"duplicate_seo_title"},
}
STATUS_ORDER = {
    "issue_still_present": 0,
    "missing_from_audit": 1,
    "awaiting_audit_refresh": 2,
    "stable": 3,
    "monitoring": 4,
}


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _humanize_category(value: Any) -> str:
    text = _normalize_text(value).replace("_", " ")
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
    return {
        "run_id": _normalize_text(run_payload.get("run_id")) or None,
        "seo_category": category or None,
        "category_label": _normalize_text(run_payload.get("category_label")) or _humanize_category(category),
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

    monitored_items.sort(
        key=lambda item: (
            STATUS_ORDER.get(str(item.get("status") or ""), 9),
            -(parse_iso(item.get("applied_at")).timestamp() if parse_iso(item.get("applied_at")) is not None else 0.0),
            str(item.get("title") or "").lower(),
        )
    )

    attention_items = [item for item in monitored_items if item.get("status") in {"issue_still_present", "missing_from_audit", "awaiting_audit_refresh"}][:5]
    recent_wins = [item for item in monitored_items if item.get("status") in {"stable", "monitoring"}]
    recent_wins.sort(
        key=lambda item: (
            0 if item.get("status") == "stable" else 1,
            -(parse_iso(item.get("applied_at")).timestamp() if parse_iso(item.get("applied_at")) is not None else 0.0),
        )
    )

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Track whether applied SEO fixes stay resolved, then attach traffic or click evidence later when a search-performance collector exists.",
            "audit_generated_at": audit_generated_at,
            "applied_run_count": len(applied_runs),
            "applied_item_count": len(monitored_items),
            "stable_count": sum(1 for item in monitored_items if item.get("status") == "stable"),
            "monitoring_count": sum(1 for item in monitored_items if item.get("status") == "monitoring"),
            "issue_still_present_count": sum(1 for item in monitored_items if item.get("status") == "issue_still_present"),
            "missing_from_audit_count": sum(1 for item in monitored_items if item.get("status") == "missing_from_audit"),
            "awaiting_audit_refresh_count": sum(1 for item in monitored_items if item.get("status") == "awaiting_audit_refresh"),
            "traffic_signal_available_count": 0,
            "traffic_signal_note": "No search-click or traffic collector is wired into Duck Ops yet, so this monitor currently measures durable SEO cleanup rather than organic lift.",
        },
        "attention_items": attention_items,
        "recent_wins": recent_wins[:5],
        "by_category": _category_summary(monitored_items),
        "items": monitored_items,
        "paths": {
            "seo_audit": str(SEO_AUDIT_PATH),
            "seo_review_runs": str(SEO_REVIEW_RUN_DIR),
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
        f"- Traffic signals available: `{summary.get('traffic_signal_available_count') or 0}`",
        "",
        str(summary.get("headline") or ""),
        "",
        str(summary.get("traffic_signal_note") or ""),
        "",
        "## Needs Attention",
        "",
    ]

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
