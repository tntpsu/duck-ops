#!/usr/bin/env python3
"""
Unified operator desk for Duck Ops.

This keeps the main business lanes visible in one place without replacing the
specialized queues that already exist.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from etsy_browser_guard import blocked_status as etsy_browser_blocked_status
from nightly_action_summary import format_operator_duck_name, load_master_roadmap_focus
from workflow_control import list_workflow_states, load_json
from workflow_operator_summary import build_workflow_followthrough_items

CURRENT_LEARNINGS_PATH = Path("/Users/philtullai/ai-agents/duck-ops/state/current_learnings.json")
CURRENT_LEARNINGS_MD_PATH = Path("/Users/philtullai/ai-agents/duck-ops/output/operator/current_learnings.md")
WEEKLY_STRATEGY_PACKET_PATH = Path("/Users/philtullai/ai-agents/duck-ops/state/weekly_strategy_recommendation_packet.json")
WEEKLY_STRATEGY_PACKET_MD_PATH = Path("/Users/philtullai/ai-agents/duck-ops/output/operator/weekly_strategy_recommendation_packet.md")
SHOPIFY_SEO_OUTCOMES_PATH = Path("/Users/philtullai/ai-agents/duck-ops/state/shopify_seo_outcomes.json")
SHOPIFY_SEO_OUTCOMES_MD_PATH = Path("/Users/philtullai/ai-agents/duck-ops/output/operator/shopify_seo_outcomes.md")
ENGINEERING_GOVERNANCE_DIGEST_PATH = Path("/Users/philtullai/ai-agents/duck-ops/state/engineering_governance_digest.json")
ENGINEERING_GOVERNANCE_DIGEST_MD_PATH = Path("/Users/philtullai/ai-agents/duck-ops/output/operator/engineering_governance_digest.md")
WEEKLY_SALE_EXECUTION_CONFIG_PATH = Path("/Users/philtullai/ai-agents/duckAgent/config/weekly_sale_execution.json")
MEME_EXECUTION_CONFIG_PATH = Path("/Users/philtullai/ai-agents/duckAgent/config/meme_execution.json")
REVIEW_CAROUSEL_EXECUTION_CONFIG_PATH = Path("/Users/philtullai/ai-agents/duckAgent/config/review_carousel_execution.json")
JEEPFACT_EXECUTION_CONFIG_PATH = Path("/Users/philtullai/ai-agents/duckAgent/config/jeepfact_execution.json")
REVIEW_REPLY_EXECUTION_CONFIG_PATH = Path("/Users/philtullai/ai-agents/duck-ops/config/review_reply_execution.json")
WEEKLY_SALE_POLICY_PROMOTION_THRESHOLD = 3
MEME_POLICY_PROMOTION_THRESHOLD = 3
REVIEW_CAROUSEL_POLICY_PROMOTION_THRESHOLD = 3
JEEPFACT_POLICY_PROMOTION_THRESHOLD = 3


WEEKLY_SALE_POLICY_REASON_LABELS = {
    "approval_gated_mode": "Manual review mode is still the only remaining gate.",
    "non_sale_primary_week": "This is not a promotional-offers primary week.",
    "products_touched_exceeds_limit": "The playbook touches more Shopify products than the unattended limit.",
    "stale_inputs": "Weekly sale inputs were stale.",
    "refresh_errors_present": "Weekly sale snapshots failed to refresh cleanly.",
    "non_numeric_shopify_product_ids": "One or more Shopify targets are missing numeric product IDs.",
    "duplicate_shopify_product_ids": "The playbook repeated a Shopify product target.",
    "out_of_policy_discount": "One or more Shopify discounts fell outside the allowed policy set.",
    "no_shopify_targets": "The playbook did not include any Shopify targets.",
    "already_published_for_run": "This weekly sale run was already published.",
    "policy_config_error": "The weekly sale execution config could not be loaded cleanly.",
}

MEME_POLICY_REASON_LABELS = {
    "approval_gated_mode": "Manual review mode is still the only remaining gate.",
    "meta_configuration_missing": "One or more required Meta IDs or tokens are missing.",
    "rendered_asset_missing": "The meme image was not uploaded cleanly enough to publish.",
    "caption_missing": "The meme caption is blank.",
    "meme_text_missing": "The meme text is blank.",
    "product_reference_missing": "The selected product metadata is incomplete.",
    "already_scheduled_for_run": "This Meme Monday run is already scheduled.",
    "policy_config_error": "The meme execution config could not be loaded cleanly.",
}

REVIEW_CAROUSEL_POLICY_REASON_LABELS = {
    "approval_gated_mode": "Manual review mode is still the only remaining gate.",
    "meta_configuration_missing": "Instagram scheduling credentials are missing.",
    "slides_missing": "The review carousel has no slides to publish.",
    "slide_assets_missing": "One or more review carousel slide images are missing on disk.",
    "caption_missing": "The review carousel caption is blank.",
    "already_scheduled_for_run": "This review carousel run is already scheduled.",
    "policy_config_error": "The review carousel execution config could not be loaded cleanly.",
}

JEEPFACT_POLICY_REASON_LABELS = {
    "approval_gated_mode": "Manual review mode is still the only remaining gate.",
    "meta_configuration_missing": "One or more required Meta IDs or tokens are missing.",
    "images_missing": "The rendered Jeep Fact slides are missing.",
    "image_urls_missing": "One or more Jeep Fact slides are missing a public URL.",
    "post_content_missing": "The Jeep Fact caption package is incomplete.",
    "facts_missing": "The Jeep facts are missing or incomplete.",
    "products_missing": "The Jeep Fact product set is missing or incomplete.",
    "already_scheduled_for_run": "This Jeep Fact Wednesday run is already scheduled.",
    "policy_config_error": "The Jeep Fact execution config could not be loaded cleanly.",
}


def _parse_iso(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _weekly_sale_policy_reason_text(value: Any) -> str:
    key = str(value or "").strip()
    if not key:
        return ""
    return WEEKLY_SALE_POLICY_REASON_LABELS.get(key, key.replace("_", " "))


def _meme_policy_reason_text(value: Any) -> str:
    key = str(value or "").strip()
    if not key:
        return ""
    return MEME_POLICY_REASON_LABELS.get(key, key.replace("_", " "))


def _review_carousel_policy_reason_text(value: Any) -> str:
    key = str(value or "").strip()
    if not key:
        return ""
    return REVIEW_CAROUSEL_POLICY_REASON_LABELS.get(key, key.replace("_", " "))


def _jeepfact_policy_reason_text(value: Any) -> str:
    key = str(value or "").strip()
    if not key:
        return ""
    return JEEPFACT_POLICY_REASON_LABELS.get(key, key.replace("_", " "))


def _load_weekly_sale_policy_surface() -> dict[str, Any]:
    config_payload = load_json(WEEKLY_SALE_EXECUTION_CONFIG_PATH, {})
    config = config_payload if isinstance(config_payload, dict) else {}
    mode = str(config.get("mode") or "approval_gated").strip() or "approval_gated"
    workflow_items = [
        item for item in list_workflow_states()
        if str(item.get("lane") or "").strip() == "weekly"
        and isinstance(item.get("metadata"), dict)
        and str((item.get("metadata") or {}).get("weekly_sale_policy_decision") or "").strip()
    ]
    workflow_items.sort(key=lambda item: _parse_iso(item.get("updated_at")), reverse=True)
    recent = workflow_items[:6]

    def _recent_entry(item: dict[str, Any]) -> dict[str, Any]:
        metadata = item.get("metadata") or {}
        return {
            "run_id": str(item.get("run_id") or item.get("entity_id") or "").strip() or None,
            "updated_at": item.get("updated_at"),
            "decision": str(metadata.get("weekly_sale_policy_decision") or "").strip() or "unknown",
            "reason": str(metadata.get("weekly_sale_policy_reason") or "").strip() or None,
            "blockers": [str(v).strip() for v in list(metadata.get("weekly_sale_policy_blockers") or []) if str(v).strip()],
            "manual_review_reasons": [str(v).strip() for v in list(metadata.get("weekly_sale_policy_manual_review_reasons") or []) if str(v).strip()],
            "state_reason": str(item.get("state_reason") or "").strip() or None,
            "title": str(metadata.get("sale_theme_name") or metadata.get("theme_name") or item.get("display_label") or "Weekly sale").strip(),
        }

    recent_runs = [_recent_entry(item) for item in recent]

    def _is_clean_gated(entry: dict[str, Any]) -> bool:
        blockers = list(entry.get("blockers") or [])
        review_reasons = list(entry.get("manual_review_reasons") or [])
        return (
            str(entry.get("decision") or "") == "manual_review_required"
            and not blockers
            and set(review_reasons or []) <= {"approval_gated_mode"}
        )

    clean_gated_streak = 0
    for entry in recent_runs:
        if _is_clean_gated(entry):
            clean_gated_streak += 1
            continue
        break

    blocked_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "blocked")
    clean_gated_recent_count = sum(1 for entry in recent_runs if _is_clean_gated(entry))
    auto_apply_eligible_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "auto_apply_allowed")
    latest = recent_runs[0] if recent_runs else {}
    promote_ready = bool(
        mode == "approval_gated"
        and clean_gated_streak >= WEEKLY_SALE_POLICY_PROMOTION_THRESHOLD
    )
    if mode == "auto_apply_shopify":
        readiness_headline = "Weekly sale auto-apply is already enabled."
        recommended_action = "Watch the next Sunday run closely and keep the manual `publish` reply as the fallback if the lane degrades."
    elif promote_ready:
        readiness_headline = (
            f"Weekly sale policy is ready for promotion after {clean_gated_streak} clean gated run(s)."
        )
        recommended_action = (
            "Flip `weekly_sale_execution.json` from `approval_gated` to `auto_apply_shopify`, "
            "then supervise the next Sunday run."
        )
    elif recent_runs:
        remaining = max(0, WEEKLY_SALE_POLICY_PROMOTION_THRESHOLD - clean_gated_streak)
        readiness_headline = (
            f"Weekly sale policy is not ready for promotion yet; {remaining} more clean gated run(s) are recommended."
        )
        recommended_action = "Keep replying `publish` on Sunday while the policy streak builds and watch for any blocked decisions."
    else:
        readiness_headline = "Weekly sale policy history is not available yet."
        recommended_action = "Run the weekly sale flow a few times in approval-gated mode so Duck Ops can judge whether promotion is safe."

    return {
        "available": True,
        "path": str(WEEKLY_SALE_EXECUTION_CONFIG_PATH),
        "mode": mode,
        "promotion_threshold": WEEKLY_SALE_POLICY_PROMOTION_THRESHOLD,
        "clean_gated_streak": clean_gated_streak,
        "clean_gated_recent_count": clean_gated_recent_count,
        "blocked_recent_count": blocked_recent_count,
        "auto_apply_eligible_recent_count": auto_apply_eligible_recent_count,
        "promote_ready": promote_ready,
        "latest_run_id": latest.get("run_id"),
        "latest_decision": latest.get("decision"),
        "latest_reason": latest.get("reason"),
        "latest_blockers": list(latest.get("blockers") or []),
        "latest_manual_review_reasons": list(latest.get("manual_review_reasons") or []),
        "latest_updated_at": latest.get("updated_at"),
        "readiness_headline": readiness_headline,
        "recommended_action": recommended_action,
        "recent_runs": recent_runs[:4],
    }


def _weekly_sale_policy_promotion_candidate(policy_surface: dict[str, Any]) -> dict[str, Any] | None:
    if not policy_surface.get("available"):
        return None

    mode = str(policy_surface.get("mode") or "approval_gated").strip() or "approval_gated"
    clean_streak = int(policy_surface.get("clean_gated_streak") or 0)
    threshold = int(policy_surface.get("promotion_threshold") or WEEKLY_SALE_POLICY_PROMOTION_THRESHOLD)
    latest_decision = str(policy_surface.get("latest_decision") or "").strip()
    blockers = [
        _weekly_sale_policy_reason_text(value)
        for value in list(policy_surface.get("latest_blockers") or [])
        if _weekly_sale_policy_reason_text(value)
    ]
    review_reasons = [
        _weekly_sale_policy_reason_text(value)
        for value in list(policy_surface.get("latest_manual_review_reasons") or [])
        if _weekly_sale_policy_reason_text(value)
    ]
    if mode == "auto_apply_shopify":
        promotion_state = "active"
        action_title = "Weekly sale auto-apply active"
    elif bool(policy_surface.get("promote_ready")):
        promotion_state = "ready"
        action_title = "Promote weekly sale auto-apply"
    elif latest_decision == "blocked":
        promotion_state = "blocked"
        action_title = "Weekly sale auto-apply promotion blocked"
    else:
        promotion_state = "observing"
        action_title = "Weekly sale auto-apply still building evidence"

    evidence: list[str] = [
        f"Clean gated streak {clean_streak}/{threshold}.",
        f"Mode is {mode}.",
    ]
    if policy_surface.get("readiness_headline"):
        evidence.append(str(policy_surface.get("readiness_headline")))
    if blockers:
        evidence.extend(blockers[:2])
    elif review_reasons:
        evidence.extend(review_reasons[:2])

    return {
        "promotion_id": "weekly_sale_auto_apply",
        "lane": "weekly_sale_policy",
        "title": "Weekly sale auto-apply",
        "action_title": action_title,
        "promotion_state": promotion_state,
        "ready": promotion_state == "ready",
        "already_promoted": promotion_state == "active",
        "summary": str(policy_surface.get("readiness_headline") or "").strip()
        or f"Clean gated streak {clean_streak}/{threshold}.",
        "recommended_action": str(policy_surface.get("recommended_action") or "").strip() or None,
        "secondary_action": str(policy_surface.get("path") or "").strip() or None,
        "source_path": str(policy_surface.get("path") or "").strip() or None,
        "updated_at": policy_surface.get("latest_updated_at"),
        "latest_run_id": policy_surface.get("latest_run_id"),
        "progress_label": f"{clean_streak}/{threshold} clean gated run(s)",
        "threshold": threshold,
        "progress_value": clean_streak,
        "blockers": blockers[:3],
        "manual_review_reasons": review_reasons[:3],
        "evidence": evidence[:4],
    }


def _load_meme_policy_surface() -> dict[str, Any]:
    config_payload = load_json(MEME_EXECUTION_CONFIG_PATH, {})
    config = config_payload if isinstance(config_payload, dict) else {}
    mode = str(config.get("mode") or "approval_gated").strip() or "approval_gated"
    workflow_items = [
        item
        for item in list_workflow_states()
        if str(item.get("lane") or "").strip() == "meme"
        and isinstance(item.get("metadata"), dict)
        and str((item.get("metadata") or {}).get("meme_policy_decision") or "").strip()
    ]
    workflow_items.sort(key=lambda item: _parse_iso(item.get("updated_at")), reverse=True)
    recent = workflow_items[:6]

    def _recent_entry(item: dict[str, Any]) -> dict[str, Any]:
        metadata = item.get("metadata") or {}
        return {
            "run_id": str(item.get("run_id") or item.get("entity_id") or "").strip() or None,
            "updated_at": item.get("updated_at"),
            "decision": str(metadata.get("meme_policy_decision") or "").strip() or "unknown",
            "reason": str(metadata.get("meme_policy_reason") or "").strip() or None,
            "blockers": [str(v).strip() for v in list(metadata.get("meme_policy_blockers") or []) if str(v).strip()],
            "manual_review_reasons": [str(v).strip() for v in list(metadata.get("meme_policy_manual_review_reasons") or []) if str(v).strip()],
            "state_reason": str(item.get("state_reason") or "").strip() or None,
            "title": str(metadata.get("product_title") or item.get("display_label") or "Meme Monday").strip(),
        }

    recent_runs = [_recent_entry(item) for item in recent]

    def _is_clean_gated(entry: dict[str, Any]) -> bool:
        blockers = list(entry.get("blockers") or [])
        review_reasons = list(entry.get("manual_review_reasons") or [])
        return (
            str(entry.get("decision") or "") == "manual_review_required"
            and not blockers
            and set(review_reasons or []) <= {"approval_gated_mode"}
        )

    clean_gated_streak = 0
    for entry in recent_runs:
        if _is_clean_gated(entry):
            clean_gated_streak += 1
            continue
        break

    blocked_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "blocked")
    auto_schedule_eligible_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "auto_schedule_allowed")
    latest = recent_runs[0] if recent_runs else {}
    promote_ready = bool(mode == "approval_gated" and clean_gated_streak >= MEME_POLICY_PROMOTION_THRESHOLD)

    if mode == "auto_schedule_meta":
        readiness_headline = "Meme Monday auto-schedule is already enabled."
        recommended_action = "Watch the next Monday run closely and keep the manual `publish` reply as the fallback if the lane degrades."
    elif promote_ready:
        readiness_headline = f"Meme Monday policy is ready for promotion after {clean_gated_streak} clean gated run(s)."
        recommended_action = (
            "Flip `meme_execution.json` from `approval_gated` to `auto_schedule_meta`, "
            "then supervise the next Monday run."
        )
    elif recent_runs:
        remaining = max(0, MEME_POLICY_PROMOTION_THRESHOLD - clean_gated_streak)
        readiness_headline = f"Meme Monday policy is not ready for promotion yet; {remaining} more clean gated run(s) are recommended."
        recommended_action = "Keep replying `publish` on Monday while the policy streak builds and watch for any blocked decisions."
    else:
        readiness_headline = "Meme Monday policy history is not available yet."
        recommended_action = "Run Meme Monday a few times in approval-gated mode so Duck Ops can judge whether promotion is safe."

    return {
        "available": True,
        "path": str(MEME_EXECUTION_CONFIG_PATH),
        "mode": mode,
        "promotion_threshold": MEME_POLICY_PROMOTION_THRESHOLD,
        "clean_gated_streak": clean_gated_streak,
        "blocked_recent_count": blocked_recent_count,
        "auto_schedule_eligible_recent_count": auto_schedule_eligible_recent_count,
        "promote_ready": promote_ready,
        "latest_run_id": latest.get("run_id"),
        "latest_decision": latest.get("decision"),
        "latest_reason": latest.get("reason"),
        "latest_blockers": list(latest.get("blockers") or []),
        "latest_manual_review_reasons": list(latest.get("manual_review_reasons") or []),
        "latest_updated_at": latest.get("updated_at"),
        "readiness_headline": readiness_headline,
        "recommended_action": recommended_action,
        "recent_runs": recent_runs[:4],
    }


def _meme_policy_promotion_candidate(policy_surface: dict[str, Any]) -> dict[str, Any] | None:
    if not policy_surface.get("available"):
        return None
    if not list(policy_surface.get("recent_runs") or []) and str(policy_surface.get("mode") or "") != "auto_schedule_meta":
        return None

    mode = str(policy_surface.get("mode") or "approval_gated").strip() or "approval_gated"
    clean_streak = int(policy_surface.get("clean_gated_streak") or 0)
    threshold = int(policy_surface.get("promotion_threshold") or MEME_POLICY_PROMOTION_THRESHOLD)
    latest_decision = str(policy_surface.get("latest_decision") or "").strip()
    blockers = [
        _meme_policy_reason_text(value)
        for value in list(policy_surface.get("latest_blockers") or [])
        if _meme_policy_reason_text(value)
    ]
    review_reasons = [
        _meme_policy_reason_text(value)
        for value in list(policy_surface.get("latest_manual_review_reasons") or [])
        if _meme_policy_reason_text(value)
    ]
    if mode == "auto_schedule_meta":
        promotion_state = "active"
        action_title = "Meme Monday auto-schedule active"
    elif bool(policy_surface.get("promote_ready")):
        promotion_state = "ready"
        action_title = "Promote Meme Monday auto-schedule"
    elif latest_decision == "blocked":
        promotion_state = "blocked"
        action_title = "Meme Monday auto-schedule promotion blocked"
    else:
        promotion_state = "observing"
        action_title = "Meme Monday auto-schedule still building evidence"

    evidence: list[str] = [
        f"Clean gated streak {clean_streak}/{threshold}.",
        f"Mode is {mode}.",
    ]
    if policy_surface.get("readiness_headline"):
        evidence.append(str(policy_surface.get("readiness_headline")))
    if blockers:
        evidence.extend(blockers[:2])
    elif review_reasons:
        evidence.extend(review_reasons[:2])

    return {
        "promotion_id": "meme_auto_schedule",
        "lane": "meme_policy",
        "title": "Meme Monday auto-schedule",
        "action_title": action_title,
        "promotion_state": promotion_state,
        "ready": promotion_state == "ready",
        "already_promoted": promotion_state == "active",
        "summary": str(policy_surface.get("readiness_headline") or "").strip()
        or f"Clean gated streak {clean_streak}/{threshold}.",
        "recommended_action": str(policy_surface.get("recommended_action") or "").strip() or None,
        "secondary_action": str(policy_surface.get("path") or "").strip() or None,
        "source_path": str(policy_surface.get("path") or "").strip() or None,
        "updated_at": policy_surface.get("latest_updated_at"),
        "latest_run_id": policy_surface.get("latest_run_id"),
        "progress_label": f"{clean_streak}/{threshold} clean gated run(s)",
        "threshold": threshold,
        "progress_value": clean_streak,
        "blockers": blockers[:3],
        "manual_review_reasons": review_reasons[:3],
        "evidence": evidence[:4],
    }


def _load_review_carousel_policy_surface() -> dict[str, Any]:
    config_payload = load_json(REVIEW_CAROUSEL_EXECUTION_CONFIG_PATH, {})
    config = config_payload if isinstance(config_payload, dict) else {}
    mode = str(config.get("mode") or "approval_gated").strip() or "approval_gated"
    workflow_items = [
        item
        for item in list_workflow_states()
        if str(item.get("lane") or "").strip() == "review_carousel"
        and isinstance(item.get("metadata"), dict)
        and str((item.get("metadata") or {}).get("review_carousel_policy_decision") or "").strip()
    ]
    workflow_items.sort(key=lambda item: _parse_iso(item.get("updated_at")), reverse=True)
    recent = workflow_items[:6]

    def _recent_entry(item: dict[str, Any]) -> dict[str, Any]:
        metadata = item.get("metadata") or {}
        return {
            "run_id": str(item.get("run_id") or item.get("entity_id") or "").strip() or None,
            "updated_at": item.get("updated_at"),
            "decision": str(metadata.get("review_carousel_policy_decision") or "").strip() or "unknown",
            "reason": str(metadata.get("review_carousel_policy_reason") or "").strip() or None,
            "blockers": [str(v).strip() for v in list(metadata.get("review_carousel_policy_blockers") or []) if str(v).strip()],
            "manual_review_reasons": [str(v).strip() for v in list(metadata.get("review_carousel_policy_manual_review_reasons") or []) if str(v).strip()],
            "state_reason": str(item.get("state_reason") or "").strip() or None,
            "title": str(metadata.get("headline") or item.get("display_label") or "Tuesday review carousel").strip(),
        }

    recent_runs = [_recent_entry(item) for item in recent]

    def _is_clean_gated(entry: dict[str, Any]) -> bool:
        blockers = list(entry.get("blockers") or [])
        review_reasons = list(entry.get("manual_review_reasons") or [])
        return (
            str(entry.get("decision") or "") == "manual_review_required"
            and not blockers
            and set(review_reasons or []) <= {"approval_gated_mode"}
        )

    clean_gated_streak = 0
    for entry in recent_runs:
        if _is_clean_gated(entry):
            clean_gated_streak += 1
            continue
        break

    blocked_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "blocked")
    auto_schedule_eligible_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "auto_schedule_allowed")
    latest = recent_runs[0] if recent_runs else {}
    promote_ready = bool(mode == "approval_gated" and clean_gated_streak >= REVIEW_CAROUSEL_POLICY_PROMOTION_THRESHOLD)

    if mode == "auto_schedule_instagram":
        readiness_headline = "Tuesday review carousel auto-schedule is already enabled."
        recommended_action = "Watch the next Tuesday run closely and keep the manual `publish` reply as the fallback if the lane degrades."
    elif promote_ready:
        readiness_headline = f"Tuesday review carousel policy is ready for promotion after {clean_gated_streak} clean gated run(s)."
        recommended_action = (
            "Flip `review_carousel_execution.json` from `approval_gated` to `auto_schedule_instagram`, "
            "then supervise the next Tuesday run."
        )
    elif recent_runs:
        remaining = max(0, REVIEW_CAROUSEL_POLICY_PROMOTION_THRESHOLD - clean_gated_streak)
        readiness_headline = f"Tuesday review carousel policy is not ready for promotion yet; {remaining} more clean gated run(s) are recommended."
        recommended_action = "Keep replying `publish` on Tuesday while the policy streak builds and watch for any blocked decisions."
    else:
        readiness_headline = "Tuesday review carousel policy history is not available yet."
        recommended_action = "Run the Tuesday carousel a few times in approval-gated mode so Duck Ops can judge whether promotion is safe."

    return {
        "available": True,
        "path": str(REVIEW_CAROUSEL_EXECUTION_CONFIG_PATH),
        "mode": mode,
        "promotion_threshold": REVIEW_CAROUSEL_POLICY_PROMOTION_THRESHOLD,
        "clean_gated_streak": clean_gated_streak,
        "blocked_recent_count": blocked_recent_count,
        "auto_schedule_eligible_recent_count": auto_schedule_eligible_recent_count,
        "promote_ready": promote_ready,
        "latest_run_id": latest.get("run_id"),
        "latest_decision": latest.get("decision"),
        "latest_reason": latest.get("reason"),
        "latest_blockers": list(latest.get("blockers") or []),
        "latest_manual_review_reasons": list(latest.get("manual_review_reasons") or []),
        "latest_updated_at": latest.get("updated_at"),
        "readiness_headline": readiness_headline,
        "recommended_action": recommended_action,
        "recent_runs": recent_runs[:4],
    }


def _review_carousel_policy_promotion_candidate(policy_surface: dict[str, Any]) -> dict[str, Any] | None:
    if not policy_surface.get("available"):
        return None
    if not list(policy_surface.get("recent_runs") or []) and str(policy_surface.get("mode") or "") != "auto_schedule_instagram":
        return None

    mode = str(policy_surface.get("mode") or "approval_gated").strip() or "approval_gated"
    clean_streak = int(policy_surface.get("clean_gated_streak") or 0)
    threshold = int(policy_surface.get("promotion_threshold") or REVIEW_CAROUSEL_POLICY_PROMOTION_THRESHOLD)
    latest_decision = str(policy_surface.get("latest_decision") or "").strip()
    blockers = [
        _review_carousel_policy_reason_text(value)
        for value in list(policy_surface.get("latest_blockers") or [])
        if _review_carousel_policy_reason_text(value)
    ]
    review_reasons = [
        _review_carousel_policy_reason_text(value)
        for value in list(policy_surface.get("latest_manual_review_reasons") or [])
        if _review_carousel_policy_reason_text(value)
    ]
    if mode == "auto_schedule_instagram":
        promotion_state = "active"
        action_title = "Tuesday review carousel auto-schedule active"
    elif bool(policy_surface.get("promote_ready")):
        promotion_state = "ready"
        action_title = "Promote Tuesday review carousel auto-schedule"
    elif latest_decision == "blocked":
        promotion_state = "blocked"
        action_title = "Tuesday review carousel promotion blocked"
    else:
        promotion_state = "observing"
        action_title = "Tuesday review carousel still building evidence"

    evidence: list[str] = [
        f"Clean gated streak {clean_streak}/{threshold}.",
        f"Mode is {mode}.",
    ]
    if policy_surface.get("readiness_headline"):
        evidence.append(str(policy_surface.get("readiness_headline")))
    if blockers:
        evidence.extend(blockers[:2])
    elif review_reasons:
        evidence.extend(review_reasons[:2])

    return {
        "promotion_id": "review_carousel_auto_schedule",
        "lane": "review_carousel_policy",
        "title": "Tuesday review carousel auto-schedule",
        "action_title": action_title,
        "promotion_state": promotion_state,
        "ready": promotion_state == "ready",
        "already_promoted": promotion_state == "active",
        "summary": str(policy_surface.get("readiness_headline") or "").strip()
        or f"Clean gated streak {clean_streak}/{threshold}.",
        "recommended_action": str(policy_surface.get("recommended_action") or "").strip() or None,
        "secondary_action": str(policy_surface.get("path") or "").strip() or None,
        "source_path": str(policy_surface.get("path") or "").strip() or None,
        "updated_at": policy_surface.get("latest_updated_at"),
        "latest_run_id": policy_surface.get("latest_run_id"),
        "progress_label": f"{clean_streak}/{threshold} clean gated run(s)",
        "threshold": threshold,
        "progress_value": clean_streak,
        "blockers": blockers[:3],
        "manual_review_reasons": review_reasons[:3],
        "evidence": evidence[:4],
    }


def _load_jeepfact_policy_surface() -> dict[str, Any]:
    config_payload = load_json(JEEPFACT_EXECUTION_CONFIG_PATH, {})
    config = config_payload if isinstance(config_payload, dict) else {}
    mode = str(config.get("mode") or "approval_gated").strip() or "approval_gated"
    workflow_items = [
        item
        for item in list_workflow_states()
        if str(item.get("lane") or "").strip() == "jeepfact"
        and isinstance(item.get("metadata"), dict)
        and str((item.get("metadata") or {}).get("jeepfact_policy_decision") or "").strip()
    ]
    workflow_items.sort(key=lambda item: _parse_iso(item.get("updated_at")), reverse=True)
    recent = workflow_items[:6]

    def _recent_entry(item: dict[str, Any]) -> dict[str, Any]:
        metadata = item.get("metadata") or {}
        return {
            "run_id": str(item.get("run_id") or item.get("entity_id") or "").strip() or None,
            "updated_at": item.get("updated_at"),
            "decision": str(metadata.get("jeepfact_policy_decision") or "").strip() or "unknown",
            "reason": str(metadata.get("jeepfact_policy_reason") or "").strip() or None,
            "blockers": [str(v).strip() for v in list(metadata.get("jeepfact_policy_blockers") or []) if str(v).strip()],
            "manual_review_reasons": [str(v).strip() for v in list(metadata.get("jeepfact_policy_manual_review_reasons") or []) if str(v).strip()],
            "state_reason": str(item.get("state_reason") or "").strip() or None,
            "title": str(metadata.get("cover_hook") or item.get("display_label") or "Jeep Fact Wednesday").strip(),
        }

    recent_runs = [_recent_entry(item) for item in recent]

    def _is_clean_gated(entry: dict[str, Any]) -> bool:
        blockers = list(entry.get("blockers") or [])
        review_reasons = list(entry.get("manual_review_reasons") or [])
        return (
            str(entry.get("decision") or "") == "manual_review_required"
            and not blockers
            and set(review_reasons or []) <= {"approval_gated_mode"}
        )

    clean_gated_streak = 0
    for entry in recent_runs:
        if _is_clean_gated(entry):
            clean_gated_streak += 1
            continue
        break

    blocked_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "blocked")
    auto_schedule_eligible_recent_count = sum(1 for entry in recent_runs if str(entry.get("decision") or "") == "auto_schedule_allowed")
    latest = recent_runs[0] if recent_runs else {}
    promote_ready = bool(mode == "approval_gated" and clean_gated_streak >= JEEPFACT_POLICY_PROMOTION_THRESHOLD)

    if mode == "auto_schedule_meta":
        readiness_headline = "Jeep Fact Wednesday auto-schedule is already enabled."
        recommended_action = "Watch the next Wednesday run closely and keep the manual `publish` reply as the fallback if the lane degrades."
    elif promote_ready:
        readiness_headline = f"Jeep Fact Wednesday policy is ready for promotion after {clean_gated_streak} clean gated run(s)."
        recommended_action = (
            "Flip `jeepfact_execution.json` from `approval_gated` to `auto_schedule_meta`, "
            "then supervise the next Wednesday run."
        )
    elif recent_runs:
        remaining = max(0, JEEPFACT_POLICY_PROMOTION_THRESHOLD - clean_gated_streak)
        readiness_headline = f"Jeep Fact Wednesday policy is not ready for promotion yet; {remaining} more clean gated run(s) are recommended."
        recommended_action = "Keep replying `publish` on Wednesday while the policy streak builds and watch for any blocked decisions."
    else:
        readiness_headline = "Jeep Fact Wednesday policy history is not available yet."
        recommended_action = "Run Jeep Fact Wednesday a few times in approval-gated mode so Duck Ops can judge whether promotion is safe."

    return {
        "available": True,
        "path": str(JEEPFACT_EXECUTION_CONFIG_PATH),
        "mode": mode,
        "promotion_threshold": JEEPFACT_POLICY_PROMOTION_THRESHOLD,
        "clean_gated_streak": clean_gated_streak,
        "blocked_recent_count": blocked_recent_count,
        "auto_schedule_eligible_recent_count": auto_schedule_eligible_recent_count,
        "promote_ready": promote_ready,
        "latest_run_id": latest.get("run_id"),
        "latest_decision": latest.get("decision"),
        "latest_reason": latest.get("reason"),
        "latest_blockers": list(latest.get("blockers") or []),
        "latest_manual_review_reasons": list(latest.get("manual_review_reasons") or []),
        "latest_updated_at": latest.get("updated_at"),
        "readiness_headline": readiness_headline,
        "recommended_action": recommended_action,
        "recent_runs": recent_runs[:4],
    }


def _jeepfact_policy_promotion_candidate(policy_surface: dict[str, Any]) -> dict[str, Any] | None:
    if not policy_surface.get("available"):
        return None
    if not list(policy_surface.get("recent_runs") or []) and str(policy_surface.get("mode") or "") != "auto_schedule_meta":
        return None

    mode = str(policy_surface.get("mode") or "approval_gated").strip() or "approval_gated"
    clean_streak = int(policy_surface.get("clean_gated_streak") or 0)
    threshold = int(policy_surface.get("promotion_threshold") or JEEPFACT_POLICY_PROMOTION_THRESHOLD)
    latest_decision = str(policy_surface.get("latest_decision") or "").strip()
    blockers = [
        _jeepfact_policy_reason_text(value)
        for value in list(policy_surface.get("latest_blockers") or [])
        if _jeepfact_policy_reason_text(value)
    ]
    review_reasons = [
        _jeepfact_policy_reason_text(value)
        for value in list(policy_surface.get("latest_manual_review_reasons") or [])
        if _jeepfact_policy_reason_text(value)
    ]
    if mode == "auto_schedule_meta":
        promotion_state = "active"
        action_title = "Jeep Fact Wednesday auto-schedule active"
    elif bool(policy_surface.get("promote_ready")):
        promotion_state = "ready"
        action_title = "Promote Jeep Fact Wednesday auto-schedule"
    elif latest_decision == "blocked":
        promotion_state = "blocked"
        action_title = "Jeep Fact Wednesday promotion blocked"
    else:
        promotion_state = "observing"
        action_title = "Jeep Fact Wednesday still building evidence"

    evidence: list[str] = [
        f"Clean gated streak {clean_streak}/{threshold}.",
        f"Mode is {mode}.",
    ]
    if policy_surface.get("readiness_headline"):
        evidence.append(str(policy_surface.get("readiness_headline")))
    if blockers:
        evidence.extend(blockers[:2])
    elif review_reasons:
        evidence.extend(review_reasons[:2])

    return {
        "promotion_id": "jeepfact_auto_schedule",
        "lane": "jeepfact_policy",
        "title": "Jeep Fact Wednesday auto-schedule",
        "action_title": action_title,
        "promotion_state": promotion_state,
        "ready": promotion_state == "ready",
        "already_promoted": promotion_state == "active",
        "summary": str(policy_surface.get("readiness_headline") or "").strip()
        or f"Clean gated streak {clean_streak}/{threshold}.",
        "recommended_action": str(policy_surface.get("recommended_action") or "").strip() or None,
        "secondary_action": str(policy_surface.get("path") or "").strip() or None,
        "source_path": str(policy_surface.get("path") or "").strip() or None,
        "updated_at": policy_surface.get("latest_updated_at"),
        "latest_run_id": policy_surface.get("latest_run_id"),
        "progress_label": f"{clean_streak}/{threshold} clean gated run(s)",
        "threshold": threshold,
        "progress_value": clean_streak,
        "blockers": blockers[:3],
        "manual_review_reasons": review_reasons[:3],
        "evidence": evidence[:4],
    }


def _load_promotion_watch_surface(
    *,
    weekly_sale_policy_surface: dict[str, Any] | None = None,
    meme_policy_surface: dict[str, Any] | None = None,
    review_carousel_policy_surface: dict[str, Any] | None = None,
    jeepfact_policy_surface: dict[str, Any] | None = None,
    review_reply_execution_surface: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy_surface = weekly_sale_policy_surface or _load_weekly_sale_policy_surface()
    meme_surface = meme_policy_surface or _load_meme_policy_surface()
    review_carousel_surface = review_carousel_policy_surface or _load_review_carousel_policy_surface()
    jeepfact_surface = jeepfact_policy_surface or _load_jeepfact_policy_surface()
    review_surface = review_reply_execution_surface or _load_review_reply_execution_surface()
    items = [
        item
        for item in [
            _weekly_sale_policy_promotion_candidate(policy_surface),
            _meme_policy_promotion_candidate(meme_surface),
            _review_carousel_policy_promotion_candidate(review_carousel_surface),
            _jeepfact_policy_promotion_candidate(jeepfact_surface),
            _review_reply_execution_promotion_candidate(review_surface),
        ]
        if isinstance(item, dict)
    ]
    ready_items = [item for item in items if item.get("promotion_state") == "ready"]
    blocked_items = [item for item in items if item.get("promotion_state") == "blocked"]
    active_items = [item for item in items if item.get("promotion_state") == "active"]
    observing_items = [item for item in items if item.get("promotion_state") == "observing"]

    if ready_items:
        headline = f"{len(ready_items)} promotion candidate(s) are ready to promote."
        recommended_action = "Review the promotion-ready item in the business desk and promote it when you are comfortable with the recent evidence."
    elif blocked_items:
        headline = f"{len(blocked_items)} promotion candidate(s) are blocked and need more cleanup before promotion."
        recommended_action = "Clear the blockers on the affected promotion candidate before promoting it."
    elif active_items:
        headline = f"{len(active_items)} promotion candidate(s) are already active."
        recommended_action = "Monitor the live canary closely and only split out dedicated cooldown rules if the lane starts drifting."
    elif observing_items:
        headline = f"{len(observing_items)} promotion candidate(s) are still collecting evidence."
        recommended_action = "Keep running the manual lane until the evidence threshold is met."
    else:
        headline = "Promotion watch is not available yet."
        recommended_action = "Build at least one promotion candidate surface before relying on this watch."

    return {
        "available": bool(items),
        "item_count": len(items),
        "ready_count": len(ready_items),
        "blocked_count": len(blocked_items),
        "active_count": len(active_items),
        "observing_count": len(observing_items),
        "headline": headline,
        "recommended_action": recommended_action,
        "items": items[:6],
    }


def _load_review_reply_execution_surface() -> dict[str, Any]:
    config_payload = load_json(REVIEW_REPLY_EXECUTION_CONFIG_PATH, {})
    config = config_payload if isinstance(config_payload, dict) else {}
    browser_guard = etsy_browser_blocked_status()
    return {
        "available": REVIEW_REPLY_EXECUTION_CONFIG_PATH.exists(),
        "path": str(REVIEW_REPLY_EXECUTION_CONFIG_PATH),
        "auto_execution_enabled": bool(config.get("auto_execution_enabled")),
        "auto_queue_enabled": bool(config.get("auto_queue_publish_ready_positive", True)),
        "auto_drain_enabled": bool(config.get("auto_drain_enabled", True)),
        "max_submits_per_run": int(config.get("auto_drain_max_submits_per_run") or 0),
        "browser_guard": browser_guard if isinstance(browser_guard, dict) else {},
    }


def _review_reply_execution_promotion_candidate(surface: dict[str, Any]) -> dict[str, Any] | None:
    if not surface.get("available"):
        return None

    browser_guard = surface.get("browser_guard") if isinstance(surface.get("browser_guard"), dict) else {}
    auto_execution_enabled = bool(surface.get("auto_execution_enabled"))
    blocked = bool(browser_guard.get("blocked"))
    blocked_until = str(browser_guard.get("blocked_until") or "").strip() or None
    block_reason = str(browser_guard.get("block_reason") or "").strip() or None

    if auto_execution_enabled and not blocked:
        promotion_state = "active"
        action_title = "Etsy review auto-execution active"
        summary = "Etsy review auto-execution is enabled and the browser guard is currently clear."
        recommended_action = "Keep monitoring the live lane and only widen automation after the browser guard stays quiet."
    elif blocked:
        promotion_state = "blocked"
        action_title = "Etsy review auto-execution cooling down"
        summary = (
            f"Etsy browser automation is cooling down until {blocked_until}."
            if blocked_until
            else "Etsy browser automation is cooling down and should stay paused."
        )
        recommended_action = "Keep the Etsy review lane in manual mode until the browser cooldown clears and the sidecar stays healthy again."
    else:
        promotion_state = "observing"
        action_title = "Etsy review auto-execution still gated"
        summary = "Review auto-execution is still intentionally gated while we supervise the Etsy lane manually."
        recommended_action = "Keep manual review replies as the control path until the cooldown and failure signals stay quiet long enough to revisit promotion."

    evidence = [
        f"Auto execution enabled: {auto_execution_enabled}.",
        f"Auto queue enabled: {bool(surface.get('auto_queue_enabled'))}.",
        f"Auto drain enabled: {bool(surface.get('auto_drain_enabled'))}.",
        (
            f"Browser guard reason: {block_reason}."
            if block_reason
            else "Browser guard is clear right now."
        ),
    ]
    return {
        "promotion_id": "review_reply_auto_execution",
        "lane": "review_reply_execution",
        "title": "Etsy review auto-execution",
        "action_title": action_title,
        "promotion_state": promotion_state,
        "ready": False,
        "already_promoted": promotion_state == "active",
        "summary": summary,
        "recommended_action": recommended_action,
        "secondary_action": str(surface.get("path") or "").strip() or None,
        "source_path": str(surface.get("path") or "").strip() or None,
        "updated_at": blocked_until,
        "latest_run_id": None,
        "progress_label": "manual supervision" if not auto_execution_enabled else "live canary",
        "threshold": None,
        "progress_value": None,
        "blockers": [entry for entry in [block_reason, blocked_until] if entry],
        "manual_review_reasons": ["browser automation remains intentionally gated"] if not auto_execution_enabled else [],
        "evidence": evidence[:4],
    }


def _trim_text(value: str | None, limit: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _display_duck_name(title: str | None, limit: int = 36) -> str:
    return format_operator_duck_name(title, limit=limit)


def _load_learning_surface() -> dict[str, Any]:
    if not CURRENT_LEARNINGS_PATH.exists():
        return {
            "available": False,
            "path": str(CURRENT_LEARNINGS_MD_PATH),
            "items": [],
            "change_count": 0,
            "idea_count": 0,
            "material_change_count": 0,
            "change_notifier": {"available": False, "items": []},
        }
    try:
        payload = json.loads(CURRENT_LEARNINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {
            "available": False,
            "path": str(CURRENT_LEARNINGS_MD_PATH),
            "items": [],
            "change_count": 0,
            "idea_count": 0,
            "material_change_count": 0,
            "change_notifier": {"available": False, "items": []},
        }
    if not isinstance(payload, dict):
        return {
            "available": False,
            "path": str(CURRENT_LEARNINGS_MD_PATH),
            "items": [],
            "change_count": 0,
            "idea_count": 0,
            "material_change_count": 0,
            "change_notifier": {"available": False, "items": []},
        }
    items = list(payload.get("current_beliefs") or [])
    notifier = payload.get("change_notifier") if isinstance(payload.get("change_notifier"), dict) else {}
    return {
        "available": True,
        "path": str(CURRENT_LEARNINGS_MD_PATH),
        "generated_at": payload.get("generated_at"),
        "items": items[:4],
        "change_count": len(payload.get("changes_since_previous") or []),
        "idea_count": len(payload.get("ideas_to_test") or []),
        "material_change_count": int(notifier.get("material_change_count") or 0),
        "change_notifier": {
            "available": bool(notifier.get("available", True)),
            "headline": notifier.get("headline"),
            "recommended_action": notifier.get("recommended_action"),
            "items": list(notifier.get("items") or [])[:3],
        },
    }


def _load_weekly_strategy_packet() -> dict[str, Any]:
    if not WEEKLY_STRATEGY_PACKET_PATH.exists():
        return {"available": False, "path": str(WEEKLY_STRATEGY_PACKET_MD_PATH), "recommendations": [], "watchouts": [], "social_plan": {}}
    try:
        payload = json.loads(WEEKLY_STRATEGY_PACKET_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"available": False, "path": str(WEEKLY_STRATEGY_PACKET_MD_PATH), "recommendations": [], "watchouts": [], "social_plan": {}}
    if not isinstance(payload, dict):
        return {"available": False, "path": str(WEEKLY_STRATEGY_PACKET_MD_PATH), "recommendations": [], "watchouts": [], "social_plan": {}}

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    recommendations = list(payload.get("recommendations") or [])
    watchouts = list(payload.get("watchouts") or [])
    social_plan = payload.get("social_plan") if isinstance(payload.get("social_plan"), dict) else {}
    stable_patterns = list(payload.get("stable_patterns") or [])
    experimental_ideas = list(payload.get("experimental_ideas") or [])
    do_not_copy_patterns = list(payload.get("do_not_copy_patterns") or [])
    return {
        "available": True,
        "path": str(WEEKLY_STRATEGY_PACKET_MD_PATH),
        "generated_at": payload.get("generated_at"),
        "headline": summary.get("headline"),
        "own_signal_confidence": summary.get("own_signal_confidence"),
        "own_signal_note": summary.get("own_signal_note"),
        "competitor_signal_confidence": summary.get("competitor_signal_confidence"),
        "competitor_signal_note": summary.get("competitor_signal_note"),
        "competitor_stability_note": summary.get("competitor_stability_note"),
        "stable_pattern_count": len(stable_patterns),
        "experimental_idea_count": len(experimental_ideas),
        "do_not_copy_count": len(do_not_copy_patterns),
        "recommendation_count": len(recommendations),
        "watchout_count": len(watchouts),
        "recommendations": recommendations[:4],
        "stable_patterns": stable_patterns[:4],
        "experimental_ideas": experimental_ideas[:4],
        "do_not_copy_patterns": do_not_copy_patterns[:4],
        "social_plan": {
            "headline": social_plan.get("headline"),
            "anchor_window": social_plan.get("anchor_window"),
            "anchor_workflow": social_plan.get("anchor_workflow"),
            "watch_account": social_plan.get("watch_account"),
            "slot_count": int(social_plan.get("slot_count") or len(social_plan.get("slots") or [])),
            "readiness_counts": dict(social_plan.get("readiness_counts") or {}),
            "execution_feedback": dict(social_plan.get("execution_feedback") or {}),
            "ready_this_week": list(social_plan.get("ready_this_week") or [])[:5],
            "slots": list(social_plan.get("slots") or [])[:5],
            "items": list(social_plan.get("items") or [])[:5],
        },
        "watchouts": watchouts[:3],
    }


def _load_seo_outcome_surface() -> dict[str, Any]:
    if not SHOPIFY_SEO_OUTCOMES_PATH.exists():
        return {"available": False, "path": str(SHOPIFY_SEO_OUTCOMES_MD_PATH), "attention_items": [], "recent_wins": []}
    try:
        payload = json.loads(SHOPIFY_SEO_OUTCOMES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"available": False, "path": str(SHOPIFY_SEO_OUTCOMES_MD_PATH), "attention_items": [], "recent_wins": []}
    if not isinstance(payload, dict):
        return {"available": False, "path": str(SHOPIFY_SEO_OUTCOMES_MD_PATH), "attention_items": [], "recent_wins": []}

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return {
        "available": True,
        "path": str(SHOPIFY_SEO_OUTCOMES_MD_PATH),
        "generated_at": payload.get("generated_at"),
        "applied_item_count": int(summary.get("applied_item_count") or len(payload.get("items") or [])),
        "stable_count": int(summary.get("stable_count") or 0),
        "monitoring_count": int(summary.get("monitoring_count") or 0),
        "issue_still_present_count": int(summary.get("issue_still_present_count") or 0),
        "missing_from_audit_count": int(summary.get("missing_from_audit_count") or 0),
        "awaiting_audit_refresh_count": int(summary.get("awaiting_audit_refresh_count") or 0),
        "writeback_receipt_count": int(summary.get("writeback_receipt_count") or 0),
        "writeback_verified_count": int(summary.get("writeback_verified_count") or 0),
        "writeback_failed_count": int(summary.get("writeback_failed_count") or 0),
        "traffic_signal_available_count": int(summary.get("traffic_signal_available_count") or 0),
        "traffic_signal_note": summary.get("traffic_signal_note"),
        "attention_items": list(payload.get("attention_items") or [])[:4],
        "recent_wins": list(payload.get("recent_wins") or [])[:4],
    }


def _load_governance_surface() -> dict[str, Any]:
    if not ENGINEERING_GOVERNANCE_DIGEST_PATH.exists():
        return {
            "available": False,
            "path": str(ENGINEERING_GOVERNANCE_DIGEST_MD_PATH),
            "findings": [],
            "recommendations": [],
        }
    try:
        payload = json.loads(ENGINEERING_GOVERNANCE_DIGEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {
            "available": False,
            "path": str(ENGINEERING_GOVERNANCE_DIGEST_MD_PATH),
            "findings": [],
            "recommendations": [],
        }
    if not isinstance(payload, dict):
        return {
            "available": False,
            "path": str(ENGINEERING_GOVERNANCE_DIGEST_MD_PATH),
            "findings": [],
            "recommendations": [],
        }

    findings = list(payload.get("findings") or [])
    recommendations = list(payload.get("review_recommendations") or [])
    recommendation_summary = payload.get("review_recommendation_summary") if isinstance(payload.get("review_recommendation_summary"), dict) else {}
    return {
        "available": True,
        "path": str(ENGINEERING_GOVERNANCE_DIGEST_MD_PATH),
        "generated_at": payload.get("generated_at"),
        "phase_focus": payload.get("phase_focus"),
        "next_step": payload.get("next_step"),
        "finding_count": len(findings),
        "recommendation_count": int(recommendation_summary.get("count") or len(recommendations)),
        "top_priority_count": int(
            recommendation_summary.get("top_priority_count")
            or sum(1 for item in recommendations if str((item or {}).get("priority") or "").upper() == "P1")
        ),
        "findings": findings[:3],
        "recommendations": recommendations[:4],
    }


def _customer_action_items(customer_packets: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in list((customer_packets or {}).get("items") or []):
        short_id = str(item.get("short_id") or "").strip()
        items.append(
            {
                **item,
                "detail_command": f"customer show {short_id}" if short_id else "customer status",
                "open_command": f"customer open {short_id}" if short_id else None,
                "tracking_live_label": ((item.get("tracking_enrichment") or {}).get("live_status_label")),
            }
        )
    return items


def _browser_review_items(etsy_browser_sync: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in list((etsy_browser_sync or {}).get("items") or []):
        linked_short_id = str(item.get("linked_customer_short_id") or "").strip()
        items.append(
            {
                **item,
                "detail_command": f"customer show {linked_short_id}" if linked_short_id else None,
                "open_command": f"customer open {linked_short_id}" if linked_short_id else None,
            }
        )
    return items


def _custom_build_items(custom_build_candidates: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in list((custom_build_candidates or {}).get("items") or []):
        detail = _trim_text(item.get("custom_design_summary"), 140)
        order_ref = str(item.get("order_ref") or "").strip()
        channel = str(item.get("channel") or "").strip()
        google_task_status = str(item.get("google_task_status") or "").strip()
        google_sync_status = str(item.get("google_task_sync_status") or "").strip()
        browser_state = str(item.get("browser_follow_up_state") or item.get("browser_review_status") or "").strip()
        if google_task_status == "created":
            next_action = "Open the live Google Task and move the concept forward."
            if browser_state == "waiting_on_customer":
                next_action = "Task is live, but this one is blocked on the customer answering the Etsy thread."
            elif browser_state == "reply_needed_before_design":
                next_action = "Task is live. Reply on Etsy first so the brief is locked before more concept work."
        elif browser_state == "waiting_on_customer":
            next_action = "Waiting on the customer. No design work tonight unless new Etsy context arrives."
        elif browser_state == "needs_reply":
            next_action = "Reply on Etsy, then create or update the Google Task once the brief is firm."
        else:
            next_action = "Stage this as a Google Task and move it into concept work."
        items.append(
            {
                **item,
                "next_action_summary": next_action,
                "detail_summary": detail,
                "operator_hint": f"{channel} order {order_ref}" if channel or order_ref else "custom build candidate",
                "google_task_sync_status": google_sync_status or None,
            }
        )
    return items


def _review_queue_items(review_queue: dict[str, Any] | None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    source_items = list((review_queue or {}).get("surfaced_items") or [])
    if not source_items:
        source_items = list((review_queue or {}).get("items") or [])
    for item in source_items:
        short_id = str(item.get("short_id") or "").strip()
        decision = str(item.get("decision") or "").strip()
        if decision == "publish_ready":
            approve_command = f"approve {short_id} because ..."
        elif decision == "needs_revision":
            approve_command = f"needs changes {short_id} because ..."
        elif decision == "discard":
            approve_command = f"discard {short_id} because ..."
        else:
            approve_command = f"agree {short_id}" if short_id else None
        items.append(
            {
                **item,
                "detail_command": f"why {short_id}" if short_id else None,
                "approve_command": approve_command,
            }
        )
    return items


def _print_queue_items(print_queue_candidates: dict[str, Any] | list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    payload_items = print_queue_candidates
    if isinstance(print_queue_candidates, dict):
        payload_items = print_queue_candidates.get("items") or []
    items = list(payload_items or [])
    return sorted(
        items,
        key=lambda item: (
            {"high": 0, "medium": 1, "low": 2}.get(str(item.get("priority") or "low").lower(), 9),
            -int(item.get("recent_demand") or 0),
            str(item.get("product_title") or "").lower(),
        ),
    )


def _weekly_sale_items(weekly_sale_monitor: dict[str, Any] | None) -> list[dict[str, Any]]:
    items = list((weekly_sale_monitor or {}).get("items") or [])
    return sorted(
        items,
        key=lambda item: (
            {"weak": 0, "watch": 1, "working": 2, "strong": 3}.get(str(item.get("effectiveness") or "watch").lower(), 9),
            -int(item.get("sales_7d") or 0),
            -int(item.get("sales_30d") or 0),
        ),
    )


def _social_plan_action_item(weekly_strategy_packet: dict[str, Any]) -> dict[str, Any] | None:
    social_plan = weekly_strategy_packet.get("social_plan") or {}
    if not isinstance(social_plan, dict) or not social_plan:
        return None

    readiness_rank = {
        "ready_now": 0,
        "ready_with_approval": 1,
        "manual_experiment": 2,
        "review_slot": 3,
        "not_supported_yet": 4,
    }

    def _pick(items: list[dict[str, Any]]) -> dict[str, Any] | None:
        actionable = [
            item
            for item in items
            if str(item.get("execution_readiness") or "").strip() in {"ready_now", "ready_with_approval", "manual_experiment"}
        ]
        if not actionable:
            return None
        actionable.sort(
            key=lambda item: (
                readiness_rank.get(str(item.get("execution_readiness") or "").strip(), 9),
                str(item.get("calendar_date") or "9999-12-31"),
                str(item.get("slot") or ""),
            )
        )
        return actionable[0]

    ready_this_week = [item for item in list(social_plan.get("ready_this_week") or []) if isinstance(item, dict)]
    slot = _pick(ready_this_week)
    if slot is None:
        slots = [item for item in list(social_plan.get("slots") or []) if isinstance(item, dict)]
        slot = _pick(slots)
    if slot is None:
        return None

    slot_label = str(slot.get("slot") or "Social slot").strip()
    calendar_label = str(slot.get("calendar_label") or slot.get("timing_hint") or "This week").strip()
    lane = str(slot.get("suggested_lane") or slot.get("workflow") or "social").strip()
    readiness = str(slot.get("execution_readiness") or "ready_now").strip()
    goal = str(slot.get("goal") or slot.get("action") or slot.get("next_step") or slot.get("headline") or "").strip()
    summary_bits = [calendar_label, readiness]
    if goal:
        summary_bits.append(_trim_text(goal, 90))
    command = str(slot.get("command_hint") or slot.get("operator_action_label") or slot.get("next_step") or "").strip()
    secondary_command = str(slot.get("approval_followthrough") or "").strip() or None
    if secondary_command and secondary_command == command:
        secondary_command = None
    return {
        "lane": "social_plan",
        "title": f"{slot_label}: {lane}" if lane else slot_label,
        "summary": " | ".join(bit for bit in summary_bits if bit),
        "command": command or None,
        "secondary_command": secondary_command,
    }


def _governance_action_item(governance_surface: dict[str, Any]) -> dict[str, Any] | None:
    recommendations = [item for item in list(governance_surface.get("recommendations") or []) if isinstance(item, dict)]
    if not recommendations:
        return None

    priority_rank = {"P1": 0, "P2": 1, "P3": 2}
    recommendations.sort(
        key=lambda item: (
            priority_rank.get(str(item.get("priority") or "P3").upper(), 9),
            str(item.get("title") or ""),
        )
    )
    top = recommendations[0]
    summary_parts = [
        str(top.get("priority") or "P3"),
        str(top.get("source") or "governance"),
        _trim_text(str(top.get("summary") or ""), 90),
    ]
    return {
        "lane": "engineering_governance",
        "title": str(top.get("title") or "Engineering governance"),
        "summary": " | ".join(part for part in summary_parts if part),
        "command": str(top.get("next_action") or "").strip() or None,
        "secondary_command": str(top.get("suggested_owner_skill") or "").strip() or None,
    }


def _promotion_watch_action_item(promotion_surface: dict[str, Any]) -> dict[str, Any] | None:
    if not promotion_surface.get("available"):
        return None

    ready_items = [item for item in list(promotion_surface.get("items") or []) if item.get("promotion_state") == "ready"]
    if ready_items:
        item = ready_items[0]
        return {
            "lane": item.get("lane") or "promotion_watch",
            "title": item.get("action_title") or item.get("title") or "Promotion ready",
            "summary": " | ".join(
                part
                for part in [
                    str(item.get("progress_label") or "").strip(),
                    _trim_text(item.get("summary"), 90),
                ]
                if part
            ),
            "command": item.get("recommended_action") or promotion_surface.get("recommended_action"),
            "secondary_command": item.get("secondary_action"),
        }

    blocked_items = [item for item in list(promotion_surface.get("items") or []) if item.get("promotion_state") == "blocked"]
    if blocked_items:
        item = blocked_items[0]
        blockers = [str(value).strip() for value in list(item.get("blockers") or [])[:2] if str(value).strip()]
        return {
            "lane": item.get("lane") or "promotion_watch",
            "title": item.get("action_title") or item.get("title") or "Promotion blocked",
            "summary": " | ".join(blockers) if blockers else _trim_text(item.get("summary"), 120),
            "command": item.get("recommended_action") or promotion_surface.get("recommended_action"),
            "secondary_command": item.get("secondary_action"),
        }

    return None


def _learning_change_action_item(learning_surface: dict[str, Any]) -> dict[str, Any] | None:
    if not learning_surface.get("available"):
        return None
    notifier = learning_surface.get("change_notifier") if isinstance(learning_surface.get("change_notifier"), dict) else {}
    if not notifier.get("available"):
        return None
    items = [item for item in list(notifier.get("items") or []) if isinstance(item, dict)]
    if not items:
        return None
    top = items[0]
    summary_parts = [
        str(top.get("urgency") or "opportunity"),
        f"{int(learning_surface.get('material_change_count') or 0)} material change(s)",
        _trim_text(str(top.get("headline") or ""), 90),
    ]
    return {
        "lane": "learning_surface",
        "title": str(notifier.get("headline") or "Review learning changes"),
        "summary": " | ".join(part for part in summary_parts if part),
        "command": str(notifier.get("recommended_action") or "").strip() or "review current_learnings + weekly_strategy_recommendation_packet",
        "secondary_command": str(learning_surface.get("path") or "").strip() or None,
    }


def _build_next_actions(
    *,
    customer_items: list[dict[str, Any]],
    browser_items: list[dict[str, Any]],
    build_items: list[dict[str, Any]],
    pack_items: list[dict[str, Any]],
    stock_items: list[dict[str, Any]],
    weekly_sale_items: list[dict[str, Any]],
    review_items: list[dict[str, Any]],
    workflow_items: list[dict[str, Any]],
    learning_surface: dict[str, Any],
    weekly_strategy_packet: dict[str, Any],
    governance_surface: dict[str, Any],
    promotion_watch_surface: dict[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if customer_items:
        first = customer_items[0]
        actions.append(
            {
                "lane": "customer",
                "title": first.get("title") or "Customer issue",
                "summary": _trim_text(first.get("customer_summary") or first.get("title"), 120),
                "command": first.get("detail_command") or "customer status",
                "secondary_command": first.get("open_command"),
            }
        )
    unresolved_browser = [
        item
        for item in browser_items
        if str(item.get("follow_up_state") or "") in {"needs_reply", "ready_for_task", "concept_in_progress"}
        or not item.get("linked_customer_short_id")
    ]
    if unresolved_browser:
        first = unresolved_browser[0]
        actions.append(
            {
                "lane": "etsy_thread",
                "title": first.get("conversation_contact") or "Etsy thread",
                "summary": _trim_text(first.get("recommended_next_action") or first.get("latest_message_preview") or first.get("open_in_browser_hint"), 120),
                "command": first.get("open_command") or first.get("primary_browser_url"),
                "secondary_command": (
                    f"reply: {first.get('draft_reply')}"
                    if str(first.get("draft_reply") or "").strip()
                    else None
                ),
            }
        )
    if build_items:
        first = build_items[0]
        actions.append(
            {
                "lane": "custom_build",
                "title": first.get("buyer_name") or "Custom build",
                "summary": _trim_text(first.get("detail_summary") or first.get("custom_design_summary"), 120),
                "command": first.get("next_action_summary"),
                "secondary_command": None,
            }
        )
    if pack_items:
        first = pack_items[0]
        channels = first.get("by_channel") or {}
        order_count = int(first.get("order_count") or 0)
        buyer_count = str(first.get("buyer_count_display") or first.get("buyer_count") or 0)
        option_summary = str(first.get("option_summary") or "").strip()
        summary = (
            f"{first.get('urgency_label') or 'Open'} | "
            f"Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {first.get('total_quantity', 0)}"
            f" | {order_count} order(s), {buyer_count} buyer(s)"
        )
        if option_summary:
            summary += f" | choices: {option_summary}"
        actions.append(
            {
                "lane": "packing",
                "title": _display_duck_name(first.get("product_title")) or "Pack tonight",
                "summary": summary,
                "command": "Pack this duck tonight.",
                "secondary_command": None,
            }
        )
    if stock_items:
        first = stock_items[0]
        actions.append(
            {
                "lane": "stock_print",
                "title": _display_duck_name(first.get("product_title")) or "Stock print candidate",
                "summary": f"{first.get('priority', 'low')} priority | recent demand {int(first.get('recent_demand') or 0)}",
                "command": "Check live stock and queue a replenishment print.",
                "secondary_command": None,
            }
        )
    weak_sale_items = [item for item in weekly_sale_items if str(item.get("effectiveness") or "") in {"weak", "watch"}]
    if weak_sale_items:
        first = weak_sale_items[0]
        actions.append(
            {
                "lane": "weekly_sale",
                "title": _display_duck_name(first.get("product_title")) or "Weekly sale review",
                "summary": (
                    f"{first.get('effectiveness')} | {first.get('discount')} | "
                    f"7d {int(first.get('sales_7d') or 0)} | 30d {int(first.get('sales_30d') or 0)}"
                ),
                "command": "Rewrite or rotate this sale item in the next weekly sale cycle.",
                "secondary_command": first.get("marketing_recommendation"),
            }
        )
    if review_items:
        first = review_items[0]
        actions.append(
            {
                "lane": "creative_review",
                "title": first.get("title") or "Creative review",
                "summary": f"{first.get('decision') or 'pending'} | {first.get('priority') or 'medium'} priority",
                "command": first.get("detail_command") or "status",
                "secondary_command": first.get("approve_command"),
            }
        )
    social_plan_action = _social_plan_action_item(weekly_strategy_packet)
    if social_plan_action:
        actions.append(social_plan_action)
    learning_action = _learning_change_action_item(learning_surface)
    if learning_action:
        actions.append(learning_action)
    governance_action = _governance_action_item(governance_surface)
    if governance_action:
        actions.append(governance_action)
    promotion_action = _promotion_watch_action_item(promotion_watch_surface)
    if promotion_action:
        actions.append(promotion_action)
    for item in workflow_items[:3]:
        actions.append(
            {
                "lane": item.get("lane") or "workflow",
                "title": item.get("title") or "Workflow follow-through",
                "summary": _trim_text(item.get("summary"), 120),
                "command": item.get("command") or item.get("next_action"),
                "secondary_command": (item.get("next_action") if item.get("command") else None),
            }
        )
    return actions[:8]


def build_business_operator_desk(
    *,
    customer_packets: dict[str, Any],
    nightly_summary: dict[str, Any],
    etsy_browser_sync: dict[str, Any],
    custom_build_candidates: dict[str, Any],
    print_queue_candidates: dict[str, Any] | list[dict[str, Any]] | None,
    weekly_sale_monitor: dict[str, Any] | None,
    review_queue: dict[str, Any] | None,
    workflow_followthrough: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    review_items = _review_queue_items(review_queue)
    customer_items = _customer_action_items(customer_packets)
    browser_items = _browser_review_items(etsy_browser_sync)
    build_items = _custom_build_items(custom_build_candidates)
    stock_items = _print_queue_items(print_queue_candidates)
    weekly_sale_items = _weekly_sale_items(weekly_sale_monitor)
    workflow_items = list(workflow_followthrough or build_workflow_followthrough_items(limit=6))
    learning_surface = _load_learning_surface()
    weekly_strategy_packet = _load_weekly_strategy_packet()
    seo_outcomes = _load_seo_outcome_surface()
    governance_surface = _load_governance_surface()
    weekly_sale_policy_surface = _load_weekly_sale_policy_surface()
    meme_policy_surface = _load_meme_policy_surface()
    review_carousel_policy_surface = _load_review_carousel_policy_surface()
    jeepfact_policy_surface = _load_jeepfact_policy_surface()
    promotion_watch_surface = _load_promotion_watch_surface(
        weekly_sale_policy_surface=weekly_sale_policy_surface,
        meme_policy_surface=meme_policy_surface,
        review_carousel_policy_surface=review_carousel_policy_surface,
        jeepfact_policy_surface=jeepfact_policy_surface,
    )
    social_plan = weekly_strategy_packet.get("social_plan") or {}
    ready_counts = social_plan.get("readiness_counts") if isinstance(social_plan, dict) else {}
    social_ready_slots = 0
    if isinstance(ready_counts, dict):
        social_ready_slots = sum(int(ready_counts.get(key) or 0) for key in ("ready_now", "ready_with_approval", "manual_experiment"))
    counts = (nightly_summary or {}).get("counts") or {}
    pack_items = list(((nightly_summary or {}).get("sections") or {}).get("orders_to_pack") or [])
    review_queue_backlog = int((review_queue or {}).get("pending_count_all") or len((review_queue or {}).get("items") or []))
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "strategy_focus": load_master_roadmap_focus(),
        "learning_surface": learning_surface,
        "weekly_strategy_packet": weekly_strategy_packet,
        "seo_outcomes": seo_outcomes,
        "counts": {
            "customer_packets": len(customer_items),
            "customer_attention_items": int(counts.get("customer_attention_items") or 0),
            "replacement_labels_now": int(counts.get("replacement_labels_now") or 0),
            "etsy_browser_threads": len(browser_items),
            "threads_with_staged_reply": sum(1 for item in browser_items if str(item.get("draft_reply") or "").strip()),
            "threads_waiting_on_customer": sum(1 for item in browser_items if str(item.get("follow_up_state") or "") == "waiting_on_customer"),
            "custom_build_candidates": len(build_items),
            "custom_build_tasks_live": sum(1 for item in build_items if str(item.get("google_task_status") or "") == "created"),
            "orders_to_pack_units": int(counts.get("orders_to_pack_units") or 0),
            "stock_print_candidates": len(stock_items),
            "active_weekly_sale_items": len(weekly_sale_items),
            "weak_weekly_sale_items": sum(1 for item in weekly_sale_items if str(item.get("effectiveness") or "") == "weak"),
            "review_queue_items": len(review_items),
            "review_queue_backlog": review_queue_backlog,
            "usps_live_customer_items": sum(1 for item in customer_items if str(item.get("tracking_live_label") or "").strip()),
            "workflow_followthrough_items": len(workflow_items),
            "promotion_candidates": int(promotion_watch_surface.get("item_count") or 0),
            "promotion_ready_candidates": int(promotion_watch_surface.get("ready_count") or 0),
            "promotion_blocked_candidates": int(promotion_watch_surface.get("blocked_count") or 0),
            "weekly_sale_policy_clean_streak": int(weekly_sale_policy_surface.get("clean_gated_streak") or 0),
            "weekly_sale_policy_blocked_recent": int(weekly_sale_policy_surface.get("blocked_recent_count") or 0),
            "weekly_sale_policy_promote_ready": 1 if weekly_sale_policy_surface.get("promote_ready") else 0,
            "meme_policy_clean_streak": int(meme_policy_surface.get("clean_gated_streak") or 0),
            "meme_policy_blocked_recent": int(meme_policy_surface.get("blocked_recent_count") or 0),
            "meme_policy_promote_ready": 1 if meme_policy_surface.get("promote_ready") else 0,
            "review_carousel_policy_clean_streak": int(review_carousel_policy_surface.get("clean_gated_streak") or 0),
            "review_carousel_policy_blocked_recent": int(review_carousel_policy_surface.get("blocked_recent_count") or 0),
            "review_carousel_policy_promote_ready": 1 if review_carousel_policy_surface.get("promote_ready") else 0,
            "jeepfact_policy_clean_streak": int(jeepfact_policy_surface.get("clean_gated_streak") or 0),
            "jeepfact_policy_blocked_recent": int(jeepfact_policy_surface.get("blocked_recent_count") or 0),
            "jeepfact_policy_promote_ready": 1 if jeepfact_policy_surface.get("promote_ready") else 0,
            "governance_findings": int(governance_surface.get("finding_count") or 0),
            "governance_recommendations": int(governance_surface.get("recommendation_count") or 0),
            "governance_top_priority_items": int(governance_surface.get("top_priority_count") or 0),
            "learning_beliefs": len(learning_surface.get("items") or []),
            "learning_changes": int(learning_surface.get("change_count") or 0),
            "learning_material_changes": int(learning_surface.get("material_change_count") or 0),
            "strategy_recommendations": len(weekly_strategy_packet.get("recommendations") or []),
            "strategy_watchouts": len(weekly_strategy_packet.get("watchouts") or []),
            "strategy_plan_items": len(((weekly_strategy_packet.get("social_plan") or {}).get("slots") or []) or ((weekly_strategy_packet.get("social_plan") or {}).get("items") or [])),
            "strategy_ready_slots": social_ready_slots,
            "seo_outcome_items": int(seo_outcomes.get("applied_item_count") or 0),
            "seo_outcome_attention_items": len(seo_outcomes.get("attention_items") or []),
            "seo_outcome_stable_items": int(seo_outcomes.get("stable_count") or 0),
        },
        "next_actions": _build_next_actions(
            customer_items=customer_items,
            browser_items=browser_items,
            build_items=build_items,
            pack_items=pack_items,
            stock_items=stock_items,
            weekly_sale_items=weekly_sale_items,
            review_items=review_items,
            workflow_items=workflow_items,
            learning_surface=learning_surface,
            weekly_strategy_packet=weekly_strategy_packet,
            governance_surface=governance_surface,
            promotion_watch_surface=promotion_watch_surface,
        ),
        "governance_surface": governance_surface,
        "weekly_sale_policy_surface": weekly_sale_policy_surface,
        "meme_policy_surface": meme_policy_surface,
        "review_carousel_policy_surface": review_carousel_policy_surface,
        "jeepfact_policy_surface": jeepfact_policy_surface,
        "promotion_watch_surface": promotion_watch_surface,
        "sections": {
            "customer_packets": customer_items[:6],
            "etsy_browser_threads": browser_items[:6],
            "custom_build_candidates": build_items[:6],
            "orders_to_pack": pack_items[:8],
            "stock_print_candidates": stock_items[:6],
            "weekly_sale_monitor": weekly_sale_items[:6],
            "review_queue": review_items[:6],
            "workflow_followthrough": workflow_items[:6],
            "promotion_watch": list(promotion_watch_surface.get("items") or [])[:4],
            "weekly_sale_policy": list(weekly_sale_policy_surface.get("recent_runs") or [])[:4],
            "meme_policy": list(meme_policy_surface.get("recent_runs") or [])[:4],
            "review_carousel_policy": list(review_carousel_policy_surface.get("recent_runs") or [])[:4],
            "jeepfact_policy": list(jeepfact_policy_surface.get("recent_runs") or [])[:4],
            "engineering_governance": list(governance_surface.get("recommendations") or [])[:4],
            "learning_surface": list(learning_surface.get("items") or [])[:4],
            "weekly_strategy_packet": list(weekly_strategy_packet.get("recommendations") or [])[:4],
            "weekly_social_plan": list(((weekly_strategy_packet.get("social_plan") or {}).get("slots") or []) or ((weekly_strategy_packet.get("social_plan") or {}).get("items") or []))[:5],
            "seo_outcomes": (list(seo_outcomes.get("attention_items") or []) or list(seo_outcomes.get("recent_wins") or []))[:4],
        },
    }


def render_business_operator_desk_markdown(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") or {}
    sections = payload.get("sections") or {}
    strategy_focus = payload.get("strategy_focus") or {}
    governance_surface = payload.get("governance_surface") or {}
    promotion_watch_surface = payload.get("promotion_watch_surface") or {}
    weekly_sale_policy_surface = payload.get("weekly_sale_policy_surface") or {}
    meme_policy_surface = payload.get("meme_policy_surface") or {}
    review_carousel_policy_surface = payload.get("review_carousel_policy_surface") or {}
    jeepfact_policy_surface = payload.get("jeepfact_policy_surface") or {}
    learning_surface = payload.get("learning_surface") or {}
    weekly_strategy_packet = payload.get("weekly_strategy_packet") or {}
    seo_outcomes = payload.get("seo_outcomes") or {}
    if not learning_surface.get("available"):
        learning_surface = _load_learning_surface()
    learning_items = (sections.get("learning_surface") or []) or list(learning_surface.get("items") or [])
    if not weekly_strategy_packet.get("available"):
        weekly_strategy_packet = _load_weekly_strategy_packet()
    strategy_items = (sections.get("weekly_strategy_packet") or []) or list(weekly_strategy_packet.get("recommendations") or [])
    if not seo_outcomes.get("available"):
        seo_outcomes = _load_seo_outcome_surface()
    seo_items = (sections.get("seo_outcomes") or []) or list(seo_outcomes.get("attention_items") or []) or list(seo_outcomes.get("recent_wins") or [])
    lines = [
        "# Duck Ops Business Desk",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Customer attention items: `{counts.get('customer_attention_items', 0)}`",
        f"- Replacement labels now: `{counts.get('replacement_labels_now', 0)}`",
        f"- Etsy browser-review threads: `{counts.get('etsy_browser_threads', 0)}`",
        f"- Threads with staged reply drafts: `{counts.get('threads_with_staged_reply', 0)}`",
        f"- Threads waiting on customer: `{counts.get('threads_waiting_on_customer', 0)}`",
        f"- Custom build candidates: `{counts.get('custom_build_candidates', 0)}`",
        f"- Live Google Tasks for builds: `{counts.get('custom_build_tasks_live', 0)}`",
        f"- Non-custom units to pack: `{counts.get('orders_to_pack_units', 0)}`",
        f"- Print-soon candidates: `{counts.get('stock_print_candidates', 0)}`",
        f"- Active weekly sale items: `{counts.get('active_weekly_sale_items', 0)}`",
        f"- Weak weekly sale items: `{counts.get('weak_weekly_sale_items', 0)}`",
        f"- Creative/operator review items: `{counts.get('review_queue_items', 0)}`",
        f"- Older creative/operator backlog: `{max(0, int(counts.get('review_queue_backlog', 0)) - int(counts.get('review_queue_items', 0)))}`",
        f"- Customer cases with live USPS context: `{counts.get('usps_live_customer_items', 0)}`",
        f"- Workflow follow-through items: `{counts.get('workflow_followthrough_items', 0)}`",
        f"- Promotion candidates surfaced: `{counts.get('promotion_candidates', 0)}`",
        f"- Promotion-ready candidates: `{counts.get('promotion_ready_candidates', 0)}`",
        f"- Promotion-blocked candidates: `{counts.get('promotion_blocked_candidates', 0)}`",
        f"- Weekly sale policy clean streak: `{counts.get('weekly_sale_policy_clean_streak', 0)}`",
        f"- Weekly sale policy blocked recent runs: `{counts.get('weekly_sale_policy_blocked_recent', 0)}`",
        f"- Weekly sale policy promote-ready: `{counts.get('weekly_sale_policy_promote_ready', 0)}`",
        f"- Meme Monday policy clean streak: `{counts.get('meme_policy_clean_streak', 0)}`",
        f"- Meme Monday policy blocked recent runs: `{counts.get('meme_policy_blocked_recent', 0)}`",
        f"- Meme Monday policy promote-ready: `{counts.get('meme_policy_promote_ready', 0)}`",
        f"- Tuesday review carousel policy clean streak: `{counts.get('review_carousel_policy_clean_streak', 0)}`",
        f"- Tuesday review carousel policy blocked recent runs: `{counts.get('review_carousel_policy_blocked_recent', 0)}`",
        f"- Tuesday review carousel policy promote-ready: `{counts.get('review_carousel_policy_promote_ready', 0)}`",
        f"- Jeep Fact Wednesday policy clean streak: `{counts.get('jeepfact_policy_clean_streak', 0)}`",
        f"- Jeep Fact Wednesday policy blocked recent runs: `{counts.get('jeepfact_policy_blocked_recent', 0)}`",
        f"- Jeep Fact Wednesday policy promote-ready: `{counts.get('jeepfact_policy_promote_ready', 0)}`",
        f"- Governance findings surfaced: `{counts.get('governance_findings', 0)}`",
        f"- Governance recommendations surfaced: `{counts.get('governance_recommendations', 0)}`",
        f"- Top-priority governance items: `{counts.get('governance_top_priority_items', 0)}`",
        f"- Learning beliefs surfaced: `{counts.get('learning_beliefs') or len(learning_items)}`",
        f"- Learning changes surfaced: `{counts.get('learning_changes') or learning_surface.get('change_count') or 0}`",
        f"- Material learning changes: `{counts.get('learning_material_changes') or learning_surface.get('material_change_count') or 0}`",
        f"- Strategy recommendations surfaced: `{counts.get('strategy_recommendations') or len(strategy_items)}`",
        f"- Strategy watchouts surfaced: `{counts.get('strategy_watchouts') or len(weekly_strategy_packet.get('watchouts') or [])}`",
        f"- Social plan items surfaced: `{counts.get('strategy_plan_items') or len((weekly_strategy_packet.get('social_plan') or {}).get('slots') or []) or len((weekly_strategy_packet.get('social_plan') or {}).get('items') or [])}`",
        f"- SEO fixes tracked: `{counts.get('seo_outcome_items') or seo_outcomes.get('applied_item_count') or 0}`",
        f"- SEO items needing follow-up: `{counts.get('seo_outcome_attention_items') or len(seo_outcomes.get('attention_items') or [])}`",
        f"- Stable SEO fixes: `{counts.get('seo_outcome_stable_items') or seo_outcomes.get('stable_count') or 0}`",
        "",
        "## Strategic Focus",
        "",
    ]
    if not strategy_focus.get("available"):
        lines.append("Master roadmap not available.")
    else:
        lines.append(f"- Roadmap: `{strategy_focus.get('path')}`")
        next_steps = strategy_focus.get("next_steps") or []
        if next_steps:
            lines.append("- Next major steps:")
            for step in next_steps:
                lines.append(f"  - {step.get('title')}: {_trim_text(step.get('summary'), 160)}")
    lines.extend([
        "",
        "## Promotion Watch",
        "",
    ])
    promotion_items = (sections.get("promotion_watch") or []) or list(promotion_watch_surface.get("items") or [])
    if not promotion_watch_surface.get("available"):
        lines.append("Promotion watch is not available yet.")
    else:
        lines.append(f"- Promotion candidates: `{promotion_watch_surface.get('item_count', len(promotion_items))}`")
        lines.append(f"- Ready to promote: `{promotion_watch_surface.get('ready_count', 0)}`")
        lines.append(f"- Blocked: `{promotion_watch_surface.get('blocked_count', 0)}`")
        lines.append(f"- Already active: `{promotion_watch_surface.get('active_count', 0)}`")
        lines.append(f"- Observing only: `{promotion_watch_surface.get('observing_count', 0)}`")
        if promotion_watch_surface.get("headline"):
            lines.append(f"- Status: {_trim_text(promotion_watch_surface.get('headline'), 180)}")
        if promotion_watch_surface.get("recommended_action"):
            lines.append(f"- Recommended action: {_trim_text(promotion_watch_surface.get('recommended_action'), 180)}")
        if promotion_items:
            lines.append("- Promotion candidates:")
            for item in promotion_items[:4]:
                lines.append(
                    f"  - {_trim_text(item.get('title'), 90)} | `{item.get('promotion_state') or 'unknown'}` | {_trim_text(item.get('progress_label'), 80)}"
                )
                if item.get("summary"):
                    lines.append(f"    Why: {_trim_text(item.get('summary'), 170)}")
                if item.get("recommended_action"):
                    lines.append(f"    Next: {_trim_text(item.get('recommended_action'), 170)}")
    lines.extend([
        "",
        "## Engineering Governance",
        "",
    ])
    if not governance_surface.get("available"):
        governance_surface = _load_governance_surface()
    governance_items = (sections.get("engineering_governance") or []) or list(governance_surface.get("recommendations") or [])
    governance_findings = list(governance_surface.get("findings") or [])
    if not governance_surface.get("available"):
        lines.append("Engineering governance digest is not available yet.")
    else:
        lines.append(f"- Page: `{governance_surface.get('path')}`")
        lines.append(f"- Phase focus: `{governance_surface.get('phase_focus') or 'unknown'}`")
        lines.append(f"- Findings: `{governance_surface.get('finding_count', len(governance_findings))}`")
        lines.append(f"- Recommendations: `{governance_surface.get('recommendation_count', len(governance_items))}`")
        lines.append(f"- Top-priority recommendations: `{governance_surface.get('top_priority_count', 0)}`")
        if governance_surface.get("next_step"):
            lines.append(f"- Next step: {_trim_text(governance_surface.get('next_step'), 180)}")
        if governance_findings:
            lines.append("- Top findings:")
            for item in governance_findings[:3]:
                lines.append(
                    f"  - {item.get('priority') or 'P3'} | {_trim_text(item.get('title'), 90)} | {_trim_text(item.get('summary'), 120)}"
                )
        if governance_items:
            lines.append("- Recommended follow-through:")
            for item in governance_items[:4]:
                lines.append(
                    f"  - {item.get('priority') or 'P3'} | {item.get('recommendation_type') or 'governance'} | {_trim_text(item.get('title'), 100)}"
                )
                if item.get("summary"):
                    lines.append(f"    Why: {_trim_text(item.get('summary'), 160)}")
                if item.get("next_action"):
                    lines.append(f"    Next: {_trim_text(item.get('next_action'), 180)}")
    lines.extend([
        "",
        "## Learning Surface",
        "",
    ])
    if not learning_surface.get("available"):
        lines.append("Current learnings page is not available yet.")
    else:
        change_notifier = learning_surface.get("change_notifier") if isinstance(learning_surface.get("change_notifier"), dict) else {}
        lines.append(f"- Page: `{learning_surface.get('path')}`")
        lines.append(f"- Changes since previous snapshot: `{learning_surface.get('change_count', 0)}`")
        lines.append(f"- Material changes needing review: `{learning_surface.get('material_change_count', 0)}`")
        lines.append(f"- Ideas worth testing: `{learning_surface.get('idea_count', 0)}`")
        if change_notifier.get("headline"):
            lines.append(f"- Change notifier: {_trim_text(change_notifier.get('headline'), 180)}")
        if change_notifier.get("recommended_action"):
            lines.append(f"- Review command: `{change_notifier.get('recommended_action')}`")
        notifier_items = list(change_notifier.get("items") or [])
        if notifier_items:
            lines.append("- Recent learning changes:")
            for item in notifier_items:
                lines.append(
                    f"  - {_trim_text(item.get('headline'), 150)}"
                )
        if learning_items:
            lines.append("- Top beliefs:")
            for item in learning_items:
                lines.append(f"  - {_trim_text(item.get('headline'), 150)}")
    lines.extend([
        "",
        "## SEO Outcomes",
        "",
    ])
    if not seo_outcomes.get("available"):
        lines.append("SEO outcome monitoring is not available yet.")
    else:
        lines.append(f"- Page: `{seo_outcomes.get('path')}`")
        lines.append(f"- Applied fixes tracked: `{seo_outcomes.get('applied_item_count', 0)}`")
        lines.append(f"- Stable fixes: `{seo_outcomes.get('stable_count', 0)}`")
        lines.append(f"- Monitoring window: `{seo_outcomes.get('monitoring_count', 0)}`")
        lines.append(f"- Still-open targeted issues: `{seo_outcomes.get('issue_still_present_count', 0)}`")
        lines.append(f"- Missing from latest audit: `{seo_outcomes.get('missing_from_audit_count', 0)}`")
        lines.append(f"- Awaiting audit refresh: `{seo_outcomes.get('awaiting_audit_refresh_count', 0)}`")
        lines.append(f"- Immediate writeback receipts: `{seo_outcomes.get('writeback_receipt_count', 0)}`")
        lines.append(f"- Immediate writeback failures: `{seo_outcomes.get('writeback_failed_count', 0)}`")
        lines.append(f"- Traffic signals available: `{seo_outcomes.get('traffic_signal_available_count', 0)}`")
        if seo_outcomes.get("traffic_signal_note"):
            lines.append(f"- Signal note: {_trim_text(seo_outcomes.get('traffic_signal_note'), 180)}")
        if seo_items:
            lines.append("- Top SEO follow-through items:")
            for item in seo_items:
                lines.append(
                    f"  - {_trim_text(item.get('title'), 100)} | `{item.get('category_label') or item.get('seo_category') or 'SEO review'}` | `{item.get('status') or 'unknown'}`"
                )
                if item.get("verification_note"):
                    lines.append(f"    Note: {_trim_text(item.get('verification_note'), 170)}")
    lines.extend([
        "",
        "## Weekly Strategy Packet",
        "",
    ])
    if not weekly_strategy_packet.get("available"):
        lines.append("Weekly strategy packet is not available yet.")
    else:
        lines.append(f"- Page: `{weekly_strategy_packet.get('path')}`")
        lines.append(f"- Own signal confidence: `{weekly_strategy_packet.get('own_signal_confidence') or 'unknown'}`")
        lines.append(f"- Competitor signal confidence: `{weekly_strategy_packet.get('competitor_signal_confidence') or 'unknown'}`")
        lines.append(f"- Recommendations: `{weekly_strategy_packet.get('recommendation_count', len(strategy_items))}`")
        lines.append(f"- Watchouts: `{weekly_strategy_packet.get('watchout_count', len(weekly_strategy_packet.get('watchouts') or []))}`")
        lines.append(f"- Stable patterns: `{weekly_strategy_packet.get('stable_pattern_count', len(weekly_strategy_packet.get('stable_patterns') or []))}`")
        lines.append(f"- Experimental ideas: `{weekly_strategy_packet.get('experimental_idea_count', len(weekly_strategy_packet.get('experimental_ideas') or []))}`")
        lines.append(f"- Do-not-copy guardrails: `{weekly_strategy_packet.get('do_not_copy_count', len(weekly_strategy_packet.get('do_not_copy_patterns') or []))}`")
        if weekly_strategy_packet.get("own_signal_note"):
            lines.append(f"- Own-signal note: {_trim_text(weekly_strategy_packet.get('own_signal_note'), 180)}")
        if weekly_strategy_packet.get("competitor_signal_note"):
            lines.append(f"- Competitor-signal note: {_trim_text(weekly_strategy_packet.get('competitor_signal_note'), 180)}")
        if weekly_strategy_packet.get("competitor_stability_note"):
            lines.append(f"- Competitor-stability note: {_trim_text(weekly_strategy_packet.get('competitor_stability_note'), 180)}")
        if strategy_items:
            lines.append("- Top recommendations:")
            for item in strategy_items:
                lines.append(f"  - {_trim_text(item.get('title'), 120)}")
        watchouts = weekly_strategy_packet.get("watchouts") or []
        if watchouts:
            lines.append("- Watchouts:")
            for item in watchouts[:3]:
                lines.append(f"  - {_trim_text(item, 160)}")
    lines.extend([
        "",
        "## This Week's Social Plan",
        "",
    ])
    social_plan = weekly_strategy_packet.get("social_plan") or {}
    if not social_plan:
        lines.append("No weekly social plan is available yet.")
    else:
        if social_plan.get("headline"):
            lines.append(f"- Headline: {_trim_text(social_plan.get('headline'), 180)}")
        if social_plan.get("anchor_window"):
            lines.append(f"- Anchor window: `{social_plan.get('anchor_window')}`")
        if social_plan.get("anchor_workflow"):
            lines.append(f"- Anchor workflow: `{social_plan.get('anchor_workflow')}`")
        if social_plan.get("watch_account"):
            lines.append(f"- Watch account: `{social_plan.get('watch_account')}`")
        readiness_counts = social_plan.get("readiness_counts") or {}
        if readiness_counts:
            lines.append(
                "- Readiness: "
                f"`ready_now={readiness_counts.get('ready_now', 0)}`, "
                f"`ready_with_approval={readiness_counts.get('ready_with_approval', 0)}`, "
                f"`manual_experiment={readiness_counts.get('manual_experiment', 0)}`, "
                f"`not_supported_yet={readiness_counts.get('not_supported_yet', 0)}`"
            )
        execution_feedback = social_plan.get("execution_feedback") or {}
        if execution_feedback:
            lines.append(
                "- Execution feedback: "
                f"`recommended={execution_feedback.get('recommended_lane_executed', 0)}`, "
                f"`alternate={execution_feedback.get('alternate_lane_executed', 0)}`, "
                f"`different={execution_feedback.get('different_lane_executed', 0)}`, "
                f"`awaiting={execution_feedback.get('awaiting_slot', 0)}`, "
                f"`no_post={execution_feedback.get('no_post_observed', 0)}`, "
                f"`review={execution_feedback.get('review_slot', 0)}`"
            )
        slots = (sections.get("weekly_social_plan") or []) or list(social_plan.get("slots") or [])
        if slots and isinstance(slots[0], dict):
            lines.append("- Suggested slots:")
            for item in slots[:5]:
                lines.append(
                    f"  - {item.get('slot')}: {_trim_text(item.get('timing_hint'), 60)} | {_trim_text(item.get('goal'), 90)}"
                )
                if item.get("action"):
                    lines.append(f"    Action: {_trim_text(item.get('action'), 160)}")
                if item.get("suggested_lane"):
                    lines.append(f"    Lane: `{item.get('suggested_lane')}`")
                elif item.get("workflow"):
                    lines.append(f"    Lane: `{item.get('workflow')}`")
                if item.get("content_family"):
                    lines.append(f"    Family: `{item.get('content_family')}`")
                if item.get("execution_mode"):
                    lines.append(f"    Mode: `{item.get('execution_mode')}`")
                if item.get("calendar_date"):
                    lines.append(f"    Date: `{item.get('calendar_date')}`")
                if item.get("calendar_label"):
                    lines.append(f"    Calendar: `{item.get('calendar_label')}`")
                if item.get("cadence_reason"):
                    lines.append(f"    Cadence: {_trim_text(item.get('cadence_reason'), 160)}")
                if item.get("lane_fit_strength"):
                    lines.append(f"    Fit: `{item.get('lane_fit_strength')}`")
                if item.get("lane_fit_reason"):
                    lines.append(f"    Lane reason: {_trim_text(item.get('lane_fit_reason'), 180)}")
                if item.get("execution_readiness"):
                    lines.append(f"    Readiness: `{item.get('execution_readiness')}`")
                if item.get("schedule_reference"):
                    lines.append(f"    Schedule: {_trim_text(item.get('schedule_reference'), 140)}")
                if item.get("operator_action_label"):
                    lines.append(f"    Use: {_trim_text(item.get('operator_action_label'), 120)}")
                if item.get("command_hint"):
                    lines.append(f"    Hint: `{item.get('command_hint')}`")
                if item.get("approval_followthrough"):
                    lines.append(f"    Then: {_trim_text(item.get('approval_followthrough'), 160)}")
                if item.get("next_step"):
                    lines.append(f"    Next: {_trim_text(item.get('next_step'), 160)}")
                if item.get("watch_account"):
                    lines.append(f"    Watch: `{item.get('watch_account')}`")
                if item.get("alternate_lane"):
                    lines.append(f"    Alternate: `{item.get('alternate_lane')}`")
                if item.get("alternate_lane_reason"):
                    lines.append(f"    Alternate reason: {_trim_text(item.get('alternate_lane_reason'), 180)}")
                if item.get("tracking_status"):
                    lines.append(f"    Outcome: `{item.get('tracking_status')}`")
                if item.get("tracking_note"):
                    lines.append(f"    Outcome note: {_trim_text(item.get('tracking_note'), 180)}")
                if item.get("actual_lane"):
                    lines.append(f"    Actual lane: `{item.get('actual_lane')}`")
                if item.get("actual_platforms"):
                    lines.append(f"    Platforms: `{', '.join(item.get('actual_platforms') or [])}`")
                if item.get("performance_label"):
                    lines.append(f"    Performance: `{item.get('performance_label')}`")
                if item.get("performance_note"):
                    lines.append(f"    Performance note: {_trim_text(item.get('performance_note'), 180)}")
        else:
            items = slots or list(social_plan.get("items") or [])
            if items:
                lines.append("- Plan:")
                for item in items[:5]:
                    lines.append(f"  - {_trim_text(item, 160)}")
        ready_this_week = social_plan.get("ready_this_week") or []
        if ready_this_week:
            lines.append("- Ready this week:")
            for item in ready_this_week[:5]:
                lines.append(
                    f"  - {item.get('slot')}: `{item.get('calendar_label') or 'this week'}` | `{item.get('suggested_lane') or 'unknown'}` | `{item.get('execution_readiness')}`"
                )
                if item.get("operator_action_label"):
                    lines.append(f"    Use: {_trim_text(item.get('operator_action_label'), 120)}")
                if item.get("schedule_reference"):
                    lines.append(f"    Schedule: {_trim_text(item.get('schedule_reference'), 140)}")
                if item.get("command_hint"):
                    lines.append(f"    Hint: `{item.get('command_hint')}`")
                if item.get("approval_followthrough"):
                    lines.append(f"    Then: {_trim_text(item.get('approval_followthrough'), 160)}")
                if item.get("tracking_status"):
                    lines.append(f"    Outcome: `{item.get('tracking_status')}`")
                if item.get("performance_label"):
                    lines.append(f"    Performance: `{item.get('performance_label')}`")
    lines.extend([
        "",
        "## Do Next",
        "",
    ])
    next_actions = payload.get("next_actions") or []
    if not next_actions:
        lines.append("No urgent next actions are staged right now.")
    else:
        for item in next_actions:
            command = item.get("command")
            secondary = item.get("secondary_command")
            command_text = f"`{command}`" if command and not str(command).startswith("http") else str(command or "(none)")
            secondary_text = (
                f" | then `{secondary}`"
                if secondary and not str(secondary).startswith("http")
                else f" | then {secondary}"
                if secondary
                else ""
            )
            lines.append(f"- {item.get('lane')}: {item.get('title')} - {_trim_text(item.get('summary'), 110)}")
            lines.append(f"  Do: {command_text}{secondary_text}")

    lines.extend(["", "## Customer Queue", ""])

    customer_items = sections.get("customer_packets") or []
    if not customer_items:
        lines.append("No customer packets are staged right now.")
    else:
        for item in customer_items:
            lines.append(
                f"- {item.get('short_id') or '?'} | {item.get('status') or 'unknown'} | {item.get('title') or 'Customer item'}"
            )
            if item.get("tracking_live_label"):
                lines.append(f"  USPS live: {item.get('tracking_live_label')}")
            if item.get("detail_command"):
                lines.append(f"  Command: `{item.get('detail_command')}`")
            if item.get("open_command"):
                lines.append(f"  Open: `{item.get('open_command')}`")
    lines.extend(["", "## Etsy Browser Review", ""])
    browser_items = sections.get("etsy_browser_threads") or []
    if not browser_items:
        lines.append("No Etsy browser-review threads are staged right now.")
    else:
        for item in browser_items:
            lines.append(
                f"- {item.get('conversation_contact') or 'Customer'} | {item.get('grouped_message_count') or 1} messages | {_trim_text(item.get('open_in_browser_hint'))}"
            )
            if item.get("draft_reply"):
                lines.append(f"  Draft reply: {_trim_text(item.get('draft_reply'), 140)}")
            if item.get("recommended_next_action"):
                lines.append(f"  Next: {_trim_text(item.get('recommended_next_action'), 140)}")
            if item.get("open_command"):
                lines.append(f"  Command: `{item.get('open_command')}`")
            elif item.get("primary_browser_url"):
                lines.append(f"  Open: {item.get('primary_browser_url')}")

    lines.extend(["", "## Custom Builds", ""])
    build_items = sections.get("custom_build_candidates") or []
    if not build_items:
        lines.append("No custom build candidates are ready right now.")
    else:
        for item in build_items:
            lines.append(
                f"- {item.get('buyer_name') or 'Customer'} | {item.get('quantity') or 0}x | {_trim_text(item.get('custom_design_summary'))}"
            )
            if item.get("next_action_summary"):
                lines.append(f"  Next: {item.get('next_action_summary')}")
            if item.get("google_task_web_view_link"):
                lines.append(f"  Task: {item.get('google_task_web_view_link')}")

    lines.extend(["", "## Pack Tonight", ""])
    pack_items = sections.get("orders_to_pack") or []
    if not pack_items:
        lines.append("No non-custom ducks are open for packing right now.")
    else:
        for item in pack_items:
            channels = item.get("by_channel") or {}
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('urgency_label') or 'Open'} | Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {item.get('total_quantity', 0)} | Buyers {item.get('buyer_count_display') or item.get('buyer_count') or 0}"
            )
            if item.get("option_summary"):
                lines.append(f"  Choices: {_trim_text(item.get('option_summary'), 120)}")

    lines.extend(["", "## Print Soon / Stock Watch", ""])
    stock_items = sections.get("stock_print_candidates") or []
    if not stock_items:
        lines.append("No stock-print candidates are staged right now.")
    else:
        for item in stock_items:
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('priority') or 'low'} priority | recent demand {int(item.get('recent_demand') or 0)}"
            )
            lines.append(f"  Why: {_trim_text(item.get('why_now'), 120)}")

    lines.extend(["", "## Weekly Sale Monitor", ""])
    weekly_sale_items = sections.get("weekly_sale_monitor") or []
    if not weekly_sale_items:
        lines.append("No active weekly sale items are available right now.")
    else:
        for item in weekly_sale_items:
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('discount')} | {item.get('effectiveness')} | 7d {int(item.get('sales_7d') or 0)} | 30d {int(item.get('sales_30d') or 0)}"
            )
            lines.append(f"  Recommendation: {item.get('recommendation')}")
            lines.append(f"  Marketing: {_trim_text(item.get('marketing_recommendation'), 120)}")

    lines.extend(["", "## Weekly Sale Policy", ""])
    if not weekly_sale_policy_surface.get("available"):
        lines.append("Weekly sale policy history is not available yet.")
    else:
        lines.append(f"- Config: `{weekly_sale_policy_surface.get('path')}`")
        lines.append(f"- Mode: `{weekly_sale_policy_surface.get('mode') or 'approval_gated'}`")
        lines.append(f"- Clean gated streak: `{weekly_sale_policy_surface.get('clean_gated_streak', 0)}`")
        lines.append(f"- Blocked recent runs: `{weekly_sale_policy_surface.get('blocked_recent_count', 0)}`")
        lines.append(f"- Auto-eligible recent runs: `{weekly_sale_policy_surface.get('auto_apply_eligible_recent_count', 0)}`")
        lines.append(f"- Promote after clean streak: `{weekly_sale_policy_surface.get('promotion_threshold', WEEKLY_SALE_POLICY_PROMOTION_THRESHOLD)}`")
        if weekly_sale_policy_surface.get("readiness_headline"):
            lines.append(f"- Promotion status: {_trim_text(weekly_sale_policy_surface.get('readiness_headline'), 180)}")
        if weekly_sale_policy_surface.get("recommended_action"):
            lines.append(f"- Recommended action: {_trim_text(weekly_sale_policy_surface.get('recommended_action'), 180)}")
        recent_policy_runs = sections.get("weekly_sale_policy") or []
        if recent_policy_runs:
            lines.append("- Recent policy runs:")
            for item in recent_policy_runs:
                title = _trim_text(item.get("title"), 48) or "Weekly sale"
                decision = str(item.get("decision") or "unknown")
                state_reason = str(item.get("state_reason") or "").strip()
                bits = [title, decision]
                if state_reason:
                    bits.append(state_reason)
                lines.append(f"  - {' | '.join(bits)}")
                blockers = [
                    _weekly_sale_policy_reason_text(value)
                    for value in list(item.get("blockers") or [])[:2]
                    if _weekly_sale_policy_reason_text(value)
                ]
                review_reasons = [
                    _weekly_sale_policy_reason_text(value)
                    for value in list(item.get("manual_review_reasons") or [])[:2]
                    if _weekly_sale_policy_reason_text(value)
                ]
                if blockers:
                    lines.append(f"    Blockers: {_trim_text('; '.join(blockers), 180)}")
                elif review_reasons:
                    lines.append(f"    Gate: {_trim_text('; '.join(review_reasons), 180)}")
                if item.get("updated_at"):
                    lines.append(f"    Updated: `{item.get('updated_at')}`")

    lines.extend(["", "## Meme Monday Policy", ""])
    if not meme_policy_surface.get("available"):
        lines.append("Meme Monday policy history is not available yet.")
    else:
        lines.append(f"- Config: `{meme_policy_surface.get('path')}`")
        lines.append(f"- Mode: `{meme_policy_surface.get('mode') or 'approval_gated'}`")
        lines.append(f"- Clean gated streak: `{meme_policy_surface.get('clean_gated_streak', 0)}`")
        lines.append(f"- Blocked recent runs: `{meme_policy_surface.get('blocked_recent_count', 0)}`")
        lines.append(f"- Auto-eligible recent runs: `{meme_policy_surface.get('auto_schedule_eligible_recent_count', 0)}`")
        lines.append(f"- Promote after clean streak: `{meme_policy_surface.get('promotion_threshold', MEME_POLICY_PROMOTION_THRESHOLD)}`")
        if meme_policy_surface.get("readiness_headline"):
            lines.append(f"- Promotion status: {_trim_text(meme_policy_surface.get('readiness_headline'), 180)}")
        if meme_policy_surface.get("recommended_action"):
            lines.append(f"- Recommended action: {_trim_text(meme_policy_surface.get('recommended_action'), 180)}")
        recent_meme_runs = sections.get("meme_policy") or []
        if recent_meme_runs:
            lines.append("- Recent policy runs:")
            for item in recent_meme_runs:
                title = _trim_text(item.get("title"), 48) or "Meme Monday"
                decision = str(item.get("decision") or "unknown")
                state_reason = str(item.get("state_reason") or "").strip()
                bits = [title, decision]
                if state_reason:
                    bits.append(state_reason)
                lines.append(f"  - {' | '.join(bits)}")
                blockers = [
                    _meme_policy_reason_text(value)
                    for value in list(item.get("blockers") or [])[:2]
                    if _meme_policy_reason_text(value)
                ]
                review_reasons = [
                    _meme_policy_reason_text(value)
                    for value in list(item.get("manual_review_reasons") or [])[:2]
                    if _meme_policy_reason_text(value)
                ]
                if blockers:
                    lines.append(f"    Blockers: {_trim_text('; '.join(blockers), 180)}")
                elif review_reasons:
                    lines.append(f"    Gate: {_trim_text('; '.join(review_reasons), 180)}")
                if item.get("updated_at"):
                    lines.append(f"    Updated: `{item.get('updated_at')}`")

    lines.extend(["", "## Tuesday Review Carousel Policy", ""])
    if not review_carousel_policy_surface.get("available"):
        lines.append("Tuesday review carousel policy history is not available yet.")
    else:
        lines.append(f"- Config: `{review_carousel_policy_surface.get('path')}`")
        lines.append(f"- Mode: `{review_carousel_policy_surface.get('mode') or 'approval_gated'}`")
        lines.append(f"- Clean gated streak: `{review_carousel_policy_surface.get('clean_gated_streak', 0)}`")
        lines.append(f"- Blocked recent runs: `{review_carousel_policy_surface.get('blocked_recent_count', 0)}`")
        lines.append(f"- Auto-eligible recent runs: `{review_carousel_policy_surface.get('auto_schedule_eligible_recent_count', 0)}`")
        lines.append(f"- Promote after clean streak: `{review_carousel_policy_surface.get('promotion_threshold', REVIEW_CAROUSEL_POLICY_PROMOTION_THRESHOLD)}`")
        if review_carousel_policy_surface.get("readiness_headline"):
            lines.append(f"- Promotion status: {_trim_text(review_carousel_policy_surface.get('readiness_headline'), 180)}")
        if review_carousel_policy_surface.get("recommended_action"):
            lines.append(f"- Recommended action: {_trim_text(review_carousel_policy_surface.get('recommended_action'), 180)}")
        recent_review_carousel_runs = sections.get("review_carousel_policy") or []
        if recent_review_carousel_runs:
            lines.append("- Recent policy runs:")
            for item in recent_review_carousel_runs:
                title = _trim_text(item.get("title"), 48) or "Tuesday review carousel"
                decision = str(item.get("decision") or "unknown")
                state_reason = str(item.get("state_reason") or "").strip()
                bits = [title, decision]
                if state_reason:
                    bits.append(state_reason)
                lines.append(f"  - {' | '.join(bits)}")
                blockers = [
                    _review_carousel_policy_reason_text(value)
                    for value in list(item.get("blockers") or [])[:2]
                    if _review_carousel_policy_reason_text(value)
                ]
                review_reasons = [
                    _review_carousel_policy_reason_text(value)
                    for value in list(item.get("manual_review_reasons") or [])[:2]
                    if _review_carousel_policy_reason_text(value)
                ]
                if blockers:
                    lines.append(f"    Blockers: {_trim_text('; '.join(blockers), 180)}")
                elif review_reasons:
                    lines.append(f"    Gate: {_trim_text('; '.join(review_reasons), 180)}")
                if item.get("updated_at"):
                    lines.append(f"    Updated: `{item.get('updated_at')}`")

    lines.extend(["", "## Jeep Fact Wednesday Policy", ""])
    if not jeepfact_policy_surface.get("available"):
        lines.append("Jeep Fact Wednesday policy history is not available yet.")
    else:
        lines.append(f"- Config: `{jeepfact_policy_surface.get('path')}`")
        lines.append(f"- Mode: `{jeepfact_policy_surface.get('mode') or 'approval_gated'}`")
        lines.append(f"- Clean gated streak: `{jeepfact_policy_surface.get('clean_gated_streak', 0)}`")
        lines.append(f"- Blocked recent runs: `{jeepfact_policy_surface.get('blocked_recent_count', 0)}`")
        lines.append(f"- Auto-eligible recent runs: `{jeepfact_policy_surface.get('auto_schedule_eligible_recent_count', 0)}`")
        lines.append(f"- Promote after clean streak: `{jeepfact_policy_surface.get('promotion_threshold', JEEPFACT_POLICY_PROMOTION_THRESHOLD)}`")
        if jeepfact_policy_surface.get("readiness_headline"):
            lines.append(f"- Promotion status: {_trim_text(jeepfact_policy_surface.get('readiness_headline'), 180)}")
        if jeepfact_policy_surface.get("recommended_action"):
            lines.append(f"- Recommended action: {_trim_text(jeepfact_policy_surface.get('recommended_action'), 180)}")
        recent_jeepfact_runs = sections.get("jeepfact_policy") or []
        if recent_jeepfact_runs:
            lines.append("- Recent policy runs:")
            for item in recent_jeepfact_runs:
                title = _trim_text(item.get("title"), 48) or "Jeep Fact Wednesday"
                decision = str(item.get("decision") or "unknown")
                state_reason = str(item.get("state_reason") or "").strip()
                bits = [title, decision]
                if state_reason:
                    bits.append(state_reason)
                lines.append(f"  - {' | '.join(bits)}")
                blockers = [
                    _jeepfact_policy_reason_text(value)
                    for value in list(item.get("blockers") or [])[:2]
                    if _jeepfact_policy_reason_text(value)
                ]
                review_reasons = [
                    _jeepfact_policy_reason_text(value)
                    for value in list(item.get("manual_review_reasons") or [])[:2]
                    if _jeepfact_policy_reason_text(value)
                ]
                if blockers:
                    lines.append(f"    Blockers: {_trim_text('; '.join(blockers), 180)}")
                elif review_reasons:
                    lines.append(f"    Gate: {_trim_text('; '.join(review_reasons), 180)}")
                if item.get("updated_at"):
                    lines.append(f"    Updated: `{item.get('updated_at')}`")

    lines.extend(["", "## Creative Review Queue", ""])
    review_items = sections.get("review_queue") or []
    if not review_items:
        backlog_total = int(counts.get("review_queue_backlog", 0))
        if backlog_total > 0:
            lines.append("No new creative/operator review items are surfaced right now.")
            lines.append("Older backlog exists. Use `status all` if you want to inspect it directly.")
        else:
            lines.append("No creative/operator review items are pending right now.")
    else:
        for item in review_items:
            lines.append(
                f"- {item.get('short_id') or item.get('operator_id') or '?'} | {item.get('review_status') or item.get('status') or 'pending'} | {_trim_text(item.get('title') or item.get('candidate_summary') or 'Review item')}"
            )
            if item.get("detail_command"):
                lines.append(f"  Detail: `{item.get('detail_command')}`")
            if item.get("approve_command"):
                lines.append(f"  Decide: `{item.get('approve_command')}`")

    lines.extend(["", "## Workflow Follow-Through", ""])
    workflow_items = sections.get("workflow_followthrough") or []
    if not workflow_items:
        lines.append("No workflow follow-through items are staged right now.")
    else:
        for item in workflow_items:
            lines.append(
                f"- {item.get('lane')}: {item.get('title')} | {item.get('summary') or item.get('state_reason') or 'needs follow-through'}"
            )
            if item.get("root_cause"):
                lines.append(f"  Why: {_trim_text(item.get('root_cause'), 180)}")
            if item.get("fix_hint"):
                lines.append(f"  Fix: {_trim_text(item.get('fix_hint'), 180)}")
            if item.get("latest_receipt"):
                lines.append(f"  Last receipt: {item.get('latest_receipt')}")
            if item.get("recent_history"):
                lines.append(f"  Trail: {item.get('recent_history')}")
            if item.get("next_action"):
                lines.append(f"  Do: {item.get('next_action')}")
            if item.get("command"):
                lines.append(f"  Run: `{item.get('command')}`")

    lines.append("")
    return "\n".join(lines)


def render_business_section(payload: dict[str, Any], section: str) -> str:
    section_key = section.strip().lower()
    sections = payload.get("sections") or {}
    if section_key in {"status", "all", ""}:
        return render_business_operator_desk_markdown(payload)

    aliases = {
        "customer": "customer_packets",
        "customers": "customer_packets",
        "threads": "etsy_browser_threads",
        "etsy": "etsy_browser_threads",
        "builds": "custom_build_candidates",
        "custom": "custom_build_candidates",
        "packing": "orders_to_pack",
        "pack": "orders_to_pack",
        "sale": "weekly_sale_monitor",
        "sales": "weekly_sale_monitor",
        "weekly_sales": "weekly_sale_monitor",
        "promotion": "promotion_watch",
        "promotions": "promotion_watch",
        "promotion_watch": "promotion_watch",
        "policy": "weekly_sale_policy",
        "sale_policy": "weekly_sale_policy",
        "weekly_sale_policy": "weekly_sale_policy",
        "meme_policy": "meme_policy",
        "meme_monday_policy": "meme_policy",
        "review_carousel_policy": "review_carousel_policy",
        "tuesday_policy": "review_carousel_policy",
        "jeepfact_policy": "jeepfact_policy",
        "wednesday_policy": "jeepfact_policy",
        "stock": "stock_print_candidates",
        "print": "stock_print_candidates",
        "reviews": "review_queue",
        "creative": "review_queue",
        "next": "next_actions",
        "workflow": "workflow_followthrough",
        "workflows": "workflow_followthrough",
        "roadmap": "strategy_focus",
        "strategy": "strategy_focus",
        "governance": "engineering_governance",
        "engineering": "engineering_governance",
        "digest": "engineering_governance",
        "governance_digest": "engineering_governance",
        "learning": "learning_surface",
        "learnings": "learning_surface",
        "seo": "seo_outcomes",
        "seo_outcome": "seo_outcomes",
        "seo_outcomes": "seo_outcomes",
        "packet": "weekly_strategy_packet",
        "weekly_strategy": "weekly_strategy_packet",
        "strategy_packet": "weekly_strategy_packet",
        "recommendations": "weekly_strategy_packet",
        "social_plan": "social_plan",
        "plan": "social_plan",
    }
    normalized = aliases.get(section_key, section_key)
    if normalized == "next_actions":
        lines = ["Duck Ops business next actions", ""]
        items = payload.get("next_actions") or []
        if not items:
            lines.append("No urgent next actions are staged right now.")
        else:
            for item in items:
                lines.append(f"- {item.get('lane')}: {item.get('title')} - {_trim_text(item.get('summary'), 120)}")
                if item.get("command"):
                    lines.append(f"  Do: {item.get('command')}")
                if item.get("secondary_command"):
                    lines.append(f"  Then: {item.get('secondary_command')}")
        return "\n".join(lines)
    if normalized == "strategy_focus":
        lines = ["Duck Ops Strategic Focus", ""]
        strategy_focus = payload.get("strategy_focus") or {}
        if not strategy_focus.get("available"):
            lines.append("Master roadmap not available.")
        else:
            lines.append(f"Roadmap: {strategy_focus.get('path')}")
            next_steps = strategy_focus.get("next_steps") or []
            if next_steps:
                lines.append("")
                for step in next_steps:
                    lines.append(f"- {step.get('title')}: {_trim_text(step.get('summary'), 160)}")
        return "\n".join(lines)
    if normalized == "engineering_governance":
        lines = ["Duck Ops Engineering Governance", ""]
        governance_surface = payload.get("governance_surface") or {}
        if not governance_surface.get("available"):
            governance_surface = _load_governance_surface()
        governance_items = (sections.get("engineering_governance") or []) or list(governance_surface.get("recommendations") or [])
        governance_findings = list(governance_surface.get("findings") or [])
        if not governance_surface.get("available"):
            lines.append("Engineering governance digest is not available yet.")
        else:
            lines.append(f"Page: {governance_surface.get('path')}")
            lines.append(f"Phase focus: {governance_surface.get('phase_focus') or 'unknown'}")
            lines.append(f"Findings: {governance_surface.get('finding_count', len(governance_findings))}")
            lines.append(f"Recommendations: {governance_surface.get('recommendation_count', len(governance_items))}")
            lines.append(f"Top-priority recommendations: {governance_surface.get('top_priority_count', 0)}")
            if governance_surface.get("next_step"):
                lines.append(f"Next step: {_trim_text(governance_surface.get('next_step'), 180)}")
            if governance_findings:
                lines.append("")
                lines.append("Top findings:")
                for item in governance_findings[:3]:
                    lines.append(
                        f"- {item.get('priority') or 'P3'} | {_trim_text(item.get('title'), 100)} | {_trim_text(item.get('summary'), 160)}"
                    )
            if governance_items:
                lines.append("")
                lines.append("Recommended follow-through:")
                for item in governance_items[:4]:
                    lines.append(
                        f"- {item.get('priority') or 'P3'} | {item.get('recommendation_type') or 'governance'} | {_trim_text(item.get('title'), 120)}"
                    )
                    if item.get("summary"):
                        lines.append(f"  Why: {_trim_text(item.get('summary'), 180)}")
                    if item.get("next_action"):
                        lines.append(f"  Next: {_trim_text(item.get('next_action'), 180)}")
        return "\n".join(lines)
    if normalized == "learning_surface":
        lines = ["Duck Ops Current Learnings", ""]
        learning_surface = payload.get("learning_surface") or {}
        if not learning_surface.get("available"):
            learning_surface = _load_learning_surface()
        learning_items = (sections.get("learning_surface") or []) or list(learning_surface.get("items") or [])
        if not learning_surface.get("available"):
            lines.append("Current learnings page is not available yet.")
        else:
            lines.append(f"Page: {learning_surface.get('path')}")
            lines.append(f"Changes since previous snapshot: {learning_surface.get('change_count', 0)}")
            lines.append(f"Ideas worth testing: {learning_surface.get('idea_count', 0)}")
            lines.append("")
            for item in learning_items:
                lines.append(f"- {_trim_text(item.get('headline'), 150)}")
        return "\n".join(lines)

    if normalized == "promotion_watch":
        lines = ["Duck Ops Promotion Watch", ""]
        promotion_watch_surface = payload.get("promotion_watch_surface") or {}
        promotion_items = (sections.get("promotion_watch") or []) or list(promotion_watch_surface.get("items") or [])
        if not promotion_watch_surface.get("available"):
            lines.append("Promotion watch is not available yet.")
        else:
            lines.append(f"Promotion candidates: {promotion_watch_surface.get('item_count', len(promotion_items))}")
            lines.append(f"Ready to promote: {promotion_watch_surface.get('ready_count', 0)}")
            lines.append(f"Blocked: {promotion_watch_surface.get('blocked_count', 0)}")
            lines.append(f"Already active: {promotion_watch_surface.get('active_count', 0)}")
            lines.append(f"Observing only: {promotion_watch_surface.get('observing_count', 0)}")
            if promotion_watch_surface.get("headline"):
                lines.append(f"Status: {_trim_text(promotion_watch_surface.get('headline'), 180)}")
            if promotion_watch_surface.get("recommended_action"):
                lines.append(f"Recommended action: {_trim_text(promotion_watch_surface.get('recommended_action'), 180)}")
            if promotion_items:
                lines.append("")
                lines.append("Candidates:")
                for item in promotion_items[:6]:
                    lines.append(
                        f"- {_trim_text(item.get('title'), 100)} | {item.get('promotion_state') or 'unknown'} | {_trim_text(item.get('progress_label'), 100)}"
                    )
                    if item.get("summary"):
                        lines.append(f"  Why: {_trim_text(item.get('summary'), 180)}")
                    if item.get("recommended_action"):
                        lines.append(f"  Next: {_trim_text(item.get('recommended_action'), 180)}")
        return "\n".join(lines)

    if normalized == "weekly_sale_policy":
        lines = ["Duck Ops Weekly Sale Policy", ""]
        weekly_sale_policy_surface = payload.get("weekly_sale_policy_surface") or {}
        if not weekly_sale_policy_surface.get("available"):
            weekly_sale_policy_surface = _load_weekly_sale_policy_surface()
        recent_runs = (sections.get("weekly_sale_policy") or []) or list(weekly_sale_policy_surface.get("recent_runs") or [])
        if not weekly_sale_policy_surface.get("available"):
            lines.append("Weekly sale policy history is not available yet.")
        else:
            lines.append(f"Config: {weekly_sale_policy_surface.get('path')}")
            lines.append(f"Mode: {weekly_sale_policy_surface.get('mode') or 'approval_gated'}")
            lines.append(f"Clean gated streak: {weekly_sale_policy_surface.get('clean_gated_streak', 0)}")
            lines.append(f"Blocked recent runs: {weekly_sale_policy_surface.get('blocked_recent_count', 0)}")
            lines.append(f"Auto-eligible recent runs: {weekly_sale_policy_surface.get('auto_apply_eligible_recent_count', 0)}")
            lines.append(f"Promote after clean streak: {weekly_sale_policy_surface.get('promotion_threshold', WEEKLY_SALE_POLICY_PROMOTION_THRESHOLD)}")
            if weekly_sale_policy_surface.get("readiness_headline"):
                lines.append(f"Promotion status: {_trim_text(weekly_sale_policy_surface.get('readiness_headline'), 180)}")
            if weekly_sale_policy_surface.get("recommended_action"):
                lines.append(f"Recommended action: {_trim_text(weekly_sale_policy_surface.get('recommended_action'), 180)}")
            if recent_runs:
                lines.append("")
                lines.append("Recent policy runs:")
                for item in recent_runs:
                    title = _trim_text(item.get("title"), 48) or "Weekly sale"
                    decision = str(item.get("decision") or "unknown")
                    state_reason = str(item.get("state_reason") or "").strip()
                    bits = [title, decision]
                    if state_reason:
                        bits.append(state_reason)
                    lines.append(f"- {' | '.join(bits)}")
                    blockers = [
                        _weekly_sale_policy_reason_text(value)
                        for value in list(item.get("blockers") or [])[:2]
                        if _weekly_sale_policy_reason_text(value)
                    ]
                    review_reasons = [
                        _weekly_sale_policy_reason_text(value)
                        for value in list(item.get("manual_review_reasons") or [])[:2]
                        if _weekly_sale_policy_reason_text(value)
                    ]
                    if blockers:
                        lines.append(f"  Blockers: {_trim_text('; '.join(blockers), 180)}")
                    elif review_reasons:
                        lines.append(f"  Gate: {_trim_text('; '.join(review_reasons), 180)}")
                    if item.get("updated_at"):
                        lines.append(f"  Updated: {item.get('updated_at')}")
        return "\n".join(lines)

    if normalized == "meme_policy":
        lines = ["Duck Ops Meme Monday Policy", ""]
        meme_policy_surface = payload.get("meme_policy_surface") or {}
        if not meme_policy_surface.get("available"):
            meme_policy_surface = _load_meme_policy_surface()
        recent_runs = (sections.get("meme_policy") or []) or list(meme_policy_surface.get("recent_runs") or [])
        if not meme_policy_surface.get("available"):
            lines.append("Meme Monday policy history is not available yet.")
        else:
            lines.append(f"Config: {meme_policy_surface.get('path')}")
            lines.append(f"Mode: {meme_policy_surface.get('mode') or 'approval_gated'}")
            lines.append(f"Clean gated streak: {meme_policy_surface.get('clean_gated_streak', 0)}")
            lines.append(f"Blocked recent runs: {meme_policy_surface.get('blocked_recent_count', 0)}")
            lines.append(f"Auto-eligible recent runs: {meme_policy_surface.get('auto_schedule_eligible_recent_count', 0)}")
            lines.append(f"Promote after clean streak: {meme_policy_surface.get('promotion_threshold', MEME_POLICY_PROMOTION_THRESHOLD)}")
            if meme_policy_surface.get("readiness_headline"):
                lines.append(f"Promotion status: {_trim_text(meme_policy_surface.get('readiness_headline'), 180)}")
            if meme_policy_surface.get("recommended_action"):
                lines.append(f"Recommended action: {_trim_text(meme_policy_surface.get('recommended_action'), 180)}")
            if recent_runs:
                lines.append("")
                lines.append("Recent policy runs:")
                for item in recent_runs:
                    title = _trim_text(item.get("title"), 48) or "Meme Monday"
                    decision = str(item.get("decision") or "unknown")
                    state_reason = str(item.get("state_reason") or "").strip()
                    bits = [title, decision]
                    if state_reason:
                        bits.append(state_reason)
                    lines.append(f"- {' | '.join(bits)}")
                    blockers = [
                        _meme_policy_reason_text(value)
                        for value in list(item.get("blockers") or [])[:2]
                        if _meme_policy_reason_text(value)
                    ]
                    review_reasons = [
                        _meme_policy_reason_text(value)
                        for value in list(item.get("manual_review_reasons") or [])[:2]
                        if _meme_policy_reason_text(value)
                    ]
                    if blockers:
                        lines.append(f"  Blockers: {_trim_text('; '.join(blockers), 180)}")
                    elif review_reasons:
                        lines.append(f"  Gate: {_trim_text('; '.join(review_reasons), 180)}")
                    if item.get("updated_at"):
                        lines.append(f"  Updated: {item.get('updated_at')}")
        return "\n".join(lines)

    if normalized == "review_carousel_policy":
        lines = ["Duck Ops Tuesday Review Carousel Policy", ""]
        review_carousel_policy_surface = payload.get("review_carousel_policy_surface") or {}
        if not review_carousel_policy_surface.get("available"):
            review_carousel_policy_surface = _load_review_carousel_policy_surface()
        recent_runs = (sections.get("review_carousel_policy") or []) or list(review_carousel_policy_surface.get("recent_runs") or [])
        if not review_carousel_policy_surface.get("available"):
            lines.append("Tuesday review carousel policy history is not available yet.")
        else:
            lines.append(f"Config: {review_carousel_policy_surface.get('path')}")
            lines.append(f"Mode: {review_carousel_policy_surface.get('mode') or 'approval_gated'}")
            lines.append(f"Clean gated streak: {review_carousel_policy_surface.get('clean_gated_streak', 0)}")
            lines.append(f"Blocked recent runs: {review_carousel_policy_surface.get('blocked_recent_count', 0)}")
            lines.append(f"Auto-eligible recent runs: {review_carousel_policy_surface.get('auto_schedule_eligible_recent_count', 0)}")
            lines.append(f"Promote after clean streak: {review_carousel_policy_surface.get('promotion_threshold', REVIEW_CAROUSEL_POLICY_PROMOTION_THRESHOLD)}")
            if review_carousel_policy_surface.get("readiness_headline"):
                lines.append(f"Promotion status: {_trim_text(review_carousel_policy_surface.get('readiness_headline'), 180)}")
            if review_carousel_policy_surface.get("recommended_action"):
                lines.append(f"Recommended action: {_trim_text(review_carousel_policy_surface.get('recommended_action'), 180)}")
            if recent_runs:
                lines.append("")
                lines.append("Recent policy runs:")
                for item in recent_runs:
                    title = _trim_text(item.get("title"), 48) or "Tuesday review carousel"
                    decision = str(item.get("decision") or "unknown")
                    state_reason = str(item.get("state_reason") or "").strip()
                    bits = [title, decision]
                    if state_reason:
                        bits.append(state_reason)
                    lines.append(f"- {' | '.join(bits)}")
                    blockers = [
                        _review_carousel_policy_reason_text(value)
                        for value in list(item.get("blockers") or [])[:2]
                        if _review_carousel_policy_reason_text(value)
                    ]
                    review_reasons = [
                        _review_carousel_policy_reason_text(value)
                        for value in list(item.get("manual_review_reasons") or [])[:2]
                        if _review_carousel_policy_reason_text(value)
                    ]
                    if blockers:
                        lines.append(f"  Blockers: {_trim_text('; '.join(blockers), 180)}")
                    elif review_reasons:
                        lines.append(f"  Gate: {_trim_text('; '.join(review_reasons), 180)}")
                    if item.get("updated_at"):
                        lines.append(f"  Updated: {item.get('updated_at')}")
        return "\n".join(lines)

    if normalized == "jeepfact_policy":
        lines = ["Duck Ops Jeep Fact Wednesday Policy", ""]
        jeepfact_policy_surface = payload.get("jeepfact_policy_surface") or {}
        if not jeepfact_policy_surface.get("available"):
            jeepfact_policy_surface = _load_jeepfact_policy_surface()
        recent_runs = (sections.get("jeepfact_policy") or []) or list(jeepfact_policy_surface.get("recent_runs") or [])
        if not jeepfact_policy_surface.get("available"):
            lines.append("Jeep Fact Wednesday policy history is not available yet.")
        else:
            lines.append(f"Config: {jeepfact_policy_surface.get('path')}")
            lines.append(f"Mode: {jeepfact_policy_surface.get('mode') or 'approval_gated'}")
            lines.append(f"Clean gated streak: {jeepfact_policy_surface.get('clean_gated_streak', 0)}")
            lines.append(f"Blocked recent runs: {jeepfact_policy_surface.get('blocked_recent_count', 0)}")
            lines.append(f"Auto-eligible recent runs: {jeepfact_policy_surface.get('auto_schedule_eligible_recent_count', 0)}")
            lines.append(f"Promote after clean streak: {jeepfact_policy_surface.get('promotion_threshold', JEEPFACT_POLICY_PROMOTION_THRESHOLD)}")
            if jeepfact_policy_surface.get("readiness_headline"):
                lines.append(f"Promotion status: {_trim_text(jeepfact_policy_surface.get('readiness_headline'), 180)}")
            if jeepfact_policy_surface.get("recommended_action"):
                lines.append(f"Recommended action: {_trim_text(jeepfact_policy_surface.get('recommended_action'), 180)}")
            if recent_runs:
                lines.append("")
                lines.append("Recent policy runs:")
                for item in recent_runs:
                    title = _trim_text(item.get("title"), 48) or "Jeep Fact Wednesday"
                    decision = str(item.get("decision") or "unknown")
                    state_reason = str(item.get("state_reason") or "").strip()
                    bits = [title, decision]
                    if state_reason:
                        bits.append(state_reason)
                    lines.append(f"- {' | '.join(bits)}")
                    blockers = [
                        _jeepfact_policy_reason_text(value)
                        for value in list(item.get("blockers") or [])[:2]
                        if _jeepfact_policy_reason_text(value)
                    ]
                    review_reasons = [
                        _jeepfact_policy_reason_text(value)
                        for value in list(item.get("manual_review_reasons") or [])[:2]
                        if _jeepfact_policy_reason_text(value)
                    ]
                    if blockers:
                        lines.append(f"  Blockers: {_trim_text('; '.join(blockers), 180)}")
                    elif review_reasons:
                        lines.append(f"  Gate: {_trim_text('; '.join(review_reasons), 180)}")
                    if item.get("updated_at"):
                        lines.append(f"  Updated: {item.get('updated_at')}")
        return "\n".join(lines)

    if normalized == "seo_outcomes":
        lines = ["Duck Ops SEO Outcomes", ""]
        seo_outcomes = payload.get("seo_outcomes") or {}
        if not seo_outcomes.get("available"):
            seo_outcomes = _load_seo_outcome_surface()
        seo_items = (sections.get("seo_outcomes") or []) or list(seo_outcomes.get("attention_items") or []) or list(seo_outcomes.get("recent_wins") or [])
        if not seo_outcomes.get("available"):
            lines.append("SEO outcome monitoring is not available yet.")
        else:
            lines.append(f"Page: {seo_outcomes.get('path')}")
            lines.append(f"Applied fixes tracked: {seo_outcomes.get('applied_item_count', 0)}")
            lines.append(f"Stable fixes: {seo_outcomes.get('stable_count', 0)}")
            lines.append(f"Monitoring window: {seo_outcomes.get('monitoring_count', 0)}")
            lines.append(f"Still-open targeted issues: {seo_outcomes.get('issue_still_present_count', 0)}")
            lines.append(f"Missing from latest audit: {seo_outcomes.get('missing_from_audit_count', 0)}")
            lines.append(f"Awaiting audit refresh: {seo_outcomes.get('awaiting_audit_refresh_count', 0)}")
            lines.append(f"Immediate writeback receipts: {seo_outcomes.get('writeback_receipt_count', 0)}")
            lines.append(f"Immediate writeback failures: {seo_outcomes.get('writeback_failed_count', 0)}")
            lines.append(f"Traffic signals available: {seo_outcomes.get('traffic_signal_available_count', 0)}")
            if seo_outcomes.get("traffic_signal_note"):
                lines.append(f"Signal note: {_trim_text(seo_outcomes.get('traffic_signal_note'), 180)}")
            lines.append("")
            if seo_items:
                for item in seo_items:
                    lines.append(
                        f"- {_trim_text(item.get('title'), 100)} | {item.get('category_label') or item.get('seo_category') or 'SEO review'} | {item.get('status') or 'unknown'}"
                    )
                    if item.get("verification_note"):
                        lines.append(f"  Note: {_trim_text(item.get('verification_note'), 180)}")
            else:
                lines.append("No SEO outcome items are staged yet.")
        return "\n".join(lines)

    if normalized == "weekly_strategy_packet":
        lines = ["Duck Ops Weekly Strategy Packet", ""]
        weekly_strategy_packet = payload.get("weekly_strategy_packet") or {}
        if not weekly_strategy_packet.get("available"):
            weekly_strategy_packet = _load_weekly_strategy_packet()
        strategy_items = (sections.get("weekly_strategy_packet") or []) or list(weekly_strategy_packet.get("recommendations") or [])
        if not weekly_strategy_packet.get("available"):
            lines.append("Weekly strategy packet is not available yet.")
        else:
            lines.append(f"Page: {weekly_strategy_packet.get('path')}")
            lines.append(f"Own signal confidence: {weekly_strategy_packet.get('own_signal_confidence') or 'unknown'}")
            lines.append(f"Competitor signal confidence: {weekly_strategy_packet.get('competitor_signal_confidence') or 'unknown'}")
            lines.append(f"Recommendations: {weekly_strategy_packet.get('recommendation_count', len(strategy_items))}")
            lines.append(f"Watchouts: {weekly_strategy_packet.get('watchout_count', len(weekly_strategy_packet.get('watchouts') or []))}")
            lines.append(f"Stable patterns: {weekly_strategy_packet.get('stable_pattern_count', len(weekly_strategy_packet.get('stable_patterns') or []))}")
            lines.append(f"Experimental ideas: {weekly_strategy_packet.get('experimental_idea_count', len(weekly_strategy_packet.get('experimental_ideas') or []))}")
            lines.append(f"Do-not-copy guardrails: {weekly_strategy_packet.get('do_not_copy_count', len(weekly_strategy_packet.get('do_not_copy_patterns') or []))}")
            if weekly_strategy_packet.get("own_signal_note"):
                lines.append(f"Own-signal note: {_trim_text(weekly_strategy_packet.get('own_signal_note'), 180)}")
            if weekly_strategy_packet.get("competitor_signal_note"):
                lines.append(f"Competitor-signal note: {_trim_text(weekly_strategy_packet.get('competitor_signal_note'), 180)}")
            if weekly_strategy_packet.get("competitor_stability_note"):
                lines.append(f"Competitor-stability note: {_trim_text(weekly_strategy_packet.get('competitor_stability_note'), 180)}")
            for item in strategy_items:
                lines.append(f"- {item.get('priority')} | {item.get('category')} | {_trim_text(item.get('title'), 140)}")
                if item.get("recommendation"):
                    lines.append(f"  Recommendation: {_trim_text(item.get('recommendation'), 180)}")
                if item.get("evidence"):
                    lines.append(f"  Evidence: {_trim_text(item.get('evidence'), 180)}")
            watchouts = weekly_strategy_packet.get("watchouts") or []
            if watchouts:
                lines.append("Watchouts:")
                for item in watchouts[:3]:
                    lines.append(f"- {_trim_text(item, 180)}")
        return "\n".join(lines)

    if normalized == "social_plan":
        lines = ["Duck Ops This Week's Social Plan", ""]
        weekly_strategy_packet = payload.get("weekly_strategy_packet") or {}
        if not weekly_strategy_packet.get("available"):
            weekly_strategy_packet = _load_weekly_strategy_packet()
        social_plan = weekly_strategy_packet.get("social_plan") or {}
        items = (sections.get("weekly_social_plan") or []) or list(social_plan.get("slots") or []) or list(social_plan.get("items") or [])
        if not weekly_strategy_packet.get("available") or not social_plan:
            lines.append("Weekly social plan is not available yet.")
        else:
            if social_plan.get("headline"):
                lines.append(f"Headline: {_trim_text(social_plan.get('headline'), 180)}")
            if social_plan.get("anchor_window"):
                lines.append(f"Anchor window: {social_plan.get('anchor_window')}")
            if social_plan.get("anchor_workflow"):
                lines.append(f"Anchor workflow: {social_plan.get('anchor_workflow')}")
            if social_plan.get("watch_account"):
                lines.append(f"Watch account: {social_plan.get('watch_account')}")
            readiness_counts = social_plan.get("readiness_counts") or {}
            if readiness_counts:
                lines.append(
                    "Readiness: "
                    f"ready_now={readiness_counts.get('ready_now', 0)}, "
                    f"ready_with_approval={readiness_counts.get('ready_with_approval', 0)}, "
                    f"manual_experiment={readiness_counts.get('manual_experiment', 0)}, "
                    f"not_supported_yet={readiness_counts.get('not_supported_yet', 0)}"
                )
            execution_feedback = social_plan.get("execution_feedback") or {}
            if execution_feedback:
                lines.append(
                    "Execution feedback: "
                    f"recommended={execution_feedback.get('recommended_lane_executed', 0)}, "
                    f"alternate={execution_feedback.get('alternate_lane_executed', 0)}, "
                    f"different={execution_feedback.get('different_lane_executed', 0)}, "
                    f"awaiting={execution_feedback.get('awaiting_slot', 0)}, "
                    f"no_post={execution_feedback.get('no_post_observed', 0)}, "
                    f"review={execution_feedback.get('review_slot', 0)}"
                )
            lines.append("")
            if items and isinstance(items[0], dict):
                for item in items:
                    lines.append(f"- {item.get('slot')}: {_trim_text(item.get('timing_hint'), 60)} | {_trim_text(item.get('goal'), 120)}")
                    if item.get("action"):
                        lines.append(f"  Action: {_trim_text(item.get('action'), 180)}")
                    if item.get("suggested_lane"):
                        lines.append(f"  Lane: {item.get('suggested_lane')}")
                    elif item.get("workflow"):
                        lines.append(f"  Lane: {item.get('workflow')}")
                    if item.get("content_family"):
                        lines.append(f"  Family: {item.get('content_family')}")
                    if item.get("execution_mode"):
                        lines.append(f"  Mode: {item.get('execution_mode')}")
                    if item.get("calendar_date"):
                        lines.append(f"  Date: {item.get('calendar_date')}")
                    if item.get("calendar_label"):
                        lines.append(f"  Calendar: {item.get('calendar_label')}")
                    if item.get("cadence_reason"):
                        lines.append(f"  Cadence: {_trim_text(item.get('cadence_reason'), 180)}")
                    if item.get("lane_fit_strength"):
                        lines.append(f"  Fit: {item.get('lane_fit_strength')}")
                    if item.get("lane_fit_reason"):
                        lines.append(f"  Lane reason: {_trim_text(item.get('lane_fit_reason'), 180)}")
                    if item.get("execution_readiness"):
                        lines.append(f"  Readiness: {item.get('execution_readiness')}")
                    if item.get("schedule_reference"):
                        lines.append(f"  Schedule: {_trim_text(item.get('schedule_reference'), 180)}")
                    if item.get("operator_action_label"):
                        lines.append(f"  Use: {_trim_text(item.get('operator_action_label'), 140)}")
                    if item.get("command_hint"):
                        lines.append(f"  Hint: {item.get('command_hint')}")
                    if item.get("approval_followthrough"):
                        lines.append(f"  Then: {_trim_text(item.get('approval_followthrough'), 180)}")
                    if item.get("next_step"):
                        lines.append(f"  Next: {_trim_text(item.get('next_step'), 180)}")
                    if item.get("watch_account"):
                        lines.append(f"  Watch: {item.get('watch_account')}")
                    if item.get("alternate_lane"):
                        lines.append(f"  Alternate: {item.get('alternate_lane')}")
                    if item.get("alternate_lane_reason"):
                        lines.append(f"  Alternate reason: {_trim_text(item.get('alternate_lane_reason'), 180)}")
                    if item.get("tracking_status"):
                        lines.append(f"  Outcome: {item.get('tracking_status')}")
                    if item.get("tracking_note"):
                        lines.append(f"  Outcome note: {_trim_text(item.get('tracking_note'), 180)}")
                    if item.get("actual_lane"):
                        lines.append(f"  Actual lane: {item.get('actual_lane')}")
                    if item.get("actual_platforms"):
                        lines.append(f"  Platforms: {', '.join(item.get('actual_platforms') or [])}")
                    if item.get("performance_label"):
                        lines.append(f"  Performance: {item.get('performance_label')}")
                    if item.get("performance_note"):
                        lines.append(f"  Performance note: {_trim_text(item.get('performance_note'), 180)}")
            else:
                for item in items:
                    lines.append(f"- {_trim_text(item, 180)}")
            ready_this_week = social_plan.get("ready_this_week") or []
            if ready_this_week:
                lines.append("")
                lines.append("Ready this week:")
                for item in ready_this_week[:5]:
                    lines.append(
                        f"- {item.get('slot')}: {item.get('calendar_label') or 'this week'} | {item.get('suggested_lane') or 'unknown'} | {item.get('execution_readiness')}"
                    )
                    if item.get("operator_action_label"):
                        lines.append(f"  Use: {_trim_text(item.get('operator_action_label'), 140)}")
                    if item.get("schedule_reference"):
                        lines.append(f"  Schedule: {_trim_text(item.get('schedule_reference'), 180)}")
                    if item.get("command_hint"):
                        lines.append(f"  Hint: {item.get('command_hint')}")
                    if item.get("approval_followthrough"):
                        lines.append(f"  Then: {_trim_text(item.get('approval_followthrough'), 180)}")
                    if item.get("lane_fit_strength"):
                        lines.append(f"  Fit: {item.get('lane_fit_strength')}")
                    if item.get("lane_fit_reason"):
                        lines.append(f"  Lane reason: {_trim_text(item.get('lane_fit_reason'), 180)}")
                    if item.get("alternate_lane"):
                        lines.append(f"  Alternate: {item.get('alternate_lane')}")
                    if item.get("alternate_lane_reason"):
                        lines.append(f"  Alternate reason: {_trim_text(item.get('alternate_lane_reason'), 180)}")
                    if item.get("tracking_status"):
                        lines.append(f"  Outcome: {item.get('tracking_status')}")
                    if item.get("performance_label"):
                        lines.append(f"  Performance: {item.get('performance_label')}")
        return "\n".join(lines)

    items = sections.get(normalized) or []
    title_map = {
        "customer_packets": "Customer Queue",
        "etsy_browser_threads": "Etsy Browser Review",
        "custom_build_candidates": "Custom Builds",
        "orders_to_pack": "Pack Tonight",
        "stock_print_candidates": "Print Soon / Stock Watch",
        "weekly_sale_monitor": "Weekly Sale Monitor",
        "review_queue": "Creative Review Queue",
        "workflow_followthrough": "Workflow Follow-Through",
    }
    lines = [f"Duck Ops {title_map.get(normalized, normalized)}", ""]
    if not items:
        if normalized == "review_queue" and int((payload.get("counts") or {}).get("review_queue_backlog", 0)) > 0:
            lines.append("No new creative/operator review items are surfaced right now.")
            lines.append("Use `status all` if you want to inspect older backlog.")
        else:
            lines.append("Nothing is staged in this section right now.")
        return "\n".join(lines)
    for item in items:
        if normalized == "customer_packets":
            lines.append(f"- {item.get('short_id')} | {item.get('status')} | {item.get('title')}")
            lines.append(f"  Summary: {_trim_text(item.get('customer_summary'), 120)}")
            if item.get("detail_command"):
                lines.append(f"  Detail: {item.get('detail_command')}")
        elif normalized == "etsy_browser_threads":
            lines.append(f"- {item.get('conversation_contact')} | {item.get('grouped_message_count')} messages")
            lines.append(f"  Open hint: {_trim_text(item.get('open_in_browser_hint'), 120)}")
            if item.get("draft_reply"):
                lines.append(f"  Draft reply: {_trim_text(item.get('draft_reply'), 140)}")
            if item.get("recommended_next_action"):
                lines.append(f"  Next: {_trim_text(item.get('recommended_next_action'), 140)}")
            if item.get("open_command"):
                lines.append(f"  Command: {item.get('open_command')}")
            elif item.get("primary_browser_url"):
                lines.append(f"  Open: {item.get('primary_browser_url')}")
        elif normalized == "custom_build_candidates":
            lines.append(f"- {item.get('buyer_name')} | {item.get('quantity')}x | {_trim_text(item.get('custom_design_summary'), 120)}")
            if item.get("next_action_summary"):
                lines.append(f"  Next: {item.get('next_action_summary')}")
            if item.get("google_task_web_view_link"):
                lines.append(f"  Task: {item.get('google_task_web_view_link')}")
        elif normalized == "orders_to_pack":
            channels = item.get("by_channel") or {}
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('urgency_label')} | Etsy {channels.get('etsy', 0)} / Shopify {channels.get('shopify', 0)} / Total {item.get('total_quantity', 0)} | Buyers {item.get('buyer_count_display') or item.get('buyer_count') or 0}"
            )
            if item.get("option_summary"):
                lines.append(f"  Choices: {_trim_text(item.get('option_summary'), 120)}")
        elif normalized == "stock_print_candidates":
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('priority')} priority | recent demand {int(item.get('recent_demand') or 0)}"
            )
            lines.append(f"  Why: {_trim_text(item.get('why_now'), 120)}")
        elif normalized == "weekly_sale_monitor":
            lines.append(
                f"- {_display_duck_name(item.get('product_title'), 44)} | {item.get('discount')} | {item.get('effectiveness')} | 7d {int(item.get('sales_7d') or 0)} | 30d {int(item.get('sales_30d') or 0)}"
            )
            lines.append(f"  Recommendation: {item.get('recommendation')}")
            lines.append(f"  Marketing: {_trim_text(item.get('marketing_recommendation'), 120)}")
        elif normalized == "review_queue":
            lines.append(f"- {item.get('short_id')} | {item.get('decision')} | {item.get('title')}")
            if item.get("detail_command"):
                lines.append(f"  Detail: {item.get('detail_command')}")
            if item.get("approve_command"):
                lines.append(f"  Decide: {item.get('approve_command')}")
        elif normalized == "workflow_followthrough":
            lines.append(
                f"- {item.get('lane')}: {item.get('title')} | {_trim_text(item.get('summary'), 120)}"
            )
            if item.get("root_cause"):
                lines.append(f"  Why: {_trim_text(item.get('root_cause'), 180)}")
            if item.get("fix_hint"):
                lines.append(f"  Fix: {_trim_text(item.get('fix_hint'), 180)}")
            if item.get("latest_receipt"):
                lines.append(f"  Last receipt: {item.get('latest_receipt')}")
            if item.get("recent_history"):
                lines.append(f"  Trail: {item.get('recent_history')}")
            if item.get("next_action"):
                lines.append(f"  Do: {item.get('next_action')}")
            if item.get("command"):
                lines.append(f"  Run: {item.get('command')}")
        else:
            lines.append(f"- {_trim_text(str(item), 120)}")
    return "\n".join(lines)
