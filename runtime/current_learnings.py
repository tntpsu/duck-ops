from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, age_hours, load_json, now_local_iso, write_json, write_markdown


SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
COMPETITOR_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "social_competitor_benchmark.json"
COMPETITOR_SOCIAL_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_benchmark.json"
COMPETITOR_SOCIAL_SNAPSHOTS_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshots.json"
WEEKLY_STRATEGY_PACKET_PATH = DUCK_OPS_ROOT / "state" / "weekly_strategy_recommendation_packet.json"
CURRENT_LEARNINGS_STATE_PATH = DUCK_OPS_ROOT / "state" / "current_learnings.json"
CURRENT_LEARNINGS_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.json"
CURRENT_LEARNINGS_MD_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.md"

MATERIAL_CHANGE_KINDS = {
    "weekly_strategy_planned_lane_validated",
    "weekly_strategy_alternate_lane_won",
    "weekly_strategy_different_lane_won",
    "weekly_strategy_slot_missed",
    "competitor_social_freshness_degraded",
    "competitor_social_freshness_recovered",
    "competitor_social_freshness_staggered",
}
ATTENTION_CHANGE_KINDS = {
    "weekly_strategy_slot_missed",
    "competitor_social_freshness_degraded",
}


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _competitor_social_freshness(snapshot_payload: dict[str, Any]) -> dict[str, Any]:
    summary = snapshot_payload.get("summary") or {}
    profiles = list(snapshot_payload.get("profiles") or [])
    failures = list(snapshot_payload.get("failures") or [])
    collected_account_count = int((summary.get("collected_account_count") or len(profiles) or 0))
    cached_account_count = int(
        summary.get("cached_account_count")
        or sum(1 for item in profiles if isinstance(item, dict) and _compact_text(item.get("snapshot_source")) not in {"", "live"})
    )
    live_account_count = int(summary.get("live_account_count") or max(0, collected_account_count - cached_account_count))
    degraded_account_count = int(summary.get("degraded_account_count") or len(failures))
    failed_account_count = int(
        summary.get("failed_account_count")
        or sum(1 for item in failures if isinstance(item, dict) and not bool(item.get("fallback_used")))
    )
    scheduled_skip_account_count = int(summary.get("scheduled_skip_account_count") or summary.get("scheduled_skip_count") or 0)
    profile_only_backoff_account_count = int(summary.get("profile_only_backoff_account_count") or 0)
    live_canary_limited_account_count = int(summary.get("live_canary_limited_account_count") or 0)
    live_canary_target_count = int(summary.get("live_canary_target_count") or 0)
    max_live_canary_targets = int(summary.get("max_live_canary_targets") or 0)
    active_refresh_target_count = int(summary.get("active_refresh_target_count") or 0)
    post_count = int(summary.get("post_count") or len(snapshot_payload.get("posts") or []))
    generated_at = _compact_text(snapshot_payload.get("generated_at"))
    snapshot_age_hours = age_hours(generated_at) if generated_at else None

    if not snapshot_payload:
        freshness_label = "missing"
        freshness_note = "No competitor social snapshot is available yet."
    elif failed_account_count > 0:
        freshness_label = "hard_failure"
        freshness_note = (
            f"Hard failure truth: {failed_account_count} account(s) could not be refreshed cleanly, "
            f"so this snapshot is not fully live."
        )
    elif cached_account_count > 0 or degraded_account_count > 0 or profile_only_backoff_account_count > 0:
        if degraded_account_count > 0:
            freshness_label = "cached"
            freshness_note = (
                f"Cached fallback truth: {cached_account_count} account(s) used cached data and "
                f"{degraded_account_count} account(s) had degraded fetches."
            )
            if profile_only_backoff_account_count > 0:
                freshness_note += f" `{profile_only_backoff_account_count}` profile-only account(s) were held on backoff."
            if live_canary_limited_account_count > 0:
                freshness_note += f" `{live_canary_limited_account_count}` account(s) were deferred by the live canary limit."
        elif profile_only_backoff_account_count > 0:
            freshness_label = "cached"
            freshness_note = (
                f"Profile-only backoff truth: {profile_only_backoff_account_count} account(s) reused cached profile-only state "
                f"because recent public refreshes still could not recover post timelines; {active_refresh_target_count} account(s) "
                f"were still targeted live this run."
            )
        elif live_canary_limited_account_count > 0:
            freshness_label = "staggered"
            freshness_note = (
                f"Live canary truth: {live_canary_target_count} canary target(s) were allowed live this run while "
                f"{live_canary_limited_account_count} account(s) reused cache because the bounded canary limit is `{max_live_canary_targets}`."
            )
        elif scheduled_skip_account_count > 0:
            freshness_label = "staggered"
            freshness_note = (
                f"Staggered refresh truth: {scheduled_skip_account_count} account(s) were intentionally reused from recent cache "
                f"while {active_refresh_target_count} account(s) were targeted for refresh this run."
            )
        else:
            freshness_label = "cached"
            freshness_note = f"Cached fallback truth: {cached_account_count} account(s) used cached data."
    elif collected_account_count > 0:
        freshness_label = "live"
        freshness_note = f"Live truth: {collected_account_count} account(s) were collected without cached fallback."
    else:
        freshness_label = "missing"
        freshness_note = "No competitor social snapshot is available yet."

    return {
        "competitor_social_snapshot_generated_at": generated_at or None,
        "competitor_social_snapshot_age_hours": snapshot_age_hours,
        "competitor_social_snapshot_post_count": post_count,
        "competitor_social_collected_account_count": collected_account_count,
        "competitor_social_live_account_count": live_account_count,
        "competitor_social_cached_account_count": cached_account_count,
        "competitor_social_degraded_account_count": degraded_account_count,
        "competitor_social_failed_account_count": failed_account_count,
        "competitor_social_scheduled_skip_account_count": scheduled_skip_account_count,
        "competitor_social_profile_only_backoff_account_count": profile_only_backoff_account_count,
        "competitor_social_live_canary_limited_account_count": live_canary_limited_account_count,
        "competitor_social_live_canary_target_count": live_canary_target_count,
        "competitor_social_max_live_canary_targets": max_live_canary_targets,
        "competitor_social_active_refresh_target_count": active_refresh_target_count,
        "competitor_social_freshness_label": freshness_label,
        "competitor_social_freshness_note": freshness_note,
    }


def _current_beliefs(
    social_payload: dict[str, Any],
    weekly_strategy_feedback: dict[str, Any],
    competitor_market_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    beliefs: list[dict[str, Any]] = []
    for item in social_payload.get("current_learnings") or []:
        if isinstance(item, dict):
            beliefs.append({"source": "own_social", **item})
    for item in _weekly_strategy_beliefs(weekly_strategy_feedback):
        beliefs.append({"source": "weekly_strategy", **item})
    for item in competitor_market_payload.get("market_learnings") or []:
        if isinstance(item, dict):
            beliefs.append({"source": "competitor_market", **item})
    for item in competitor_social_payload.get("current_learnings") or []:
        if isinstance(item, dict):
            beliefs.append({"source": "competitor_social", **item})
    return beliefs[:8]


def _best_windows(social_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return list(((social_payload.get("rollups") or {}).get("by_time_window") or []))[:5]


def _strongest_workflows(social_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return list(((social_payload.get("rollups") or {}).get("by_workflow") or []))[:5]


def _changes(
    social_payload: dict[str, Any],
    weekly_strategy_feedback: dict[str, Any],
    previous_current_learnings_payload: dict[str, Any],
    competitor_market_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
    competitor_social_freshness: dict[str, Any],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in social_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "own_social", **item})
    for item in _weekly_strategy_changes(weekly_strategy_feedback, previous_current_learnings_payload):
        changes.append({"source": "weekly_strategy", **item})
    for item in competitor_market_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "competitor_market", **item})
    for item in competitor_social_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "competitor_social", **item})
    for item in _competitor_social_freshness_changes(competitor_social_freshness, previous_current_learnings_payload):
        changes.append({"source": "competitor_social_snapshot", **item})
    return changes


def _weekly_strategy_feedback(packet_payload: dict[str, Any]) -> dict[str, Any]:
    social_plan = packet_payload.get("social_plan") if isinstance(packet_payload.get("social_plan"), dict) else {}
    if not social_plan:
        return {"available": False, "execution_feedback": {}, "slot_outcomes": []}

    slot_outcomes: list[dict[str, Any]] = []
    for item in social_plan.get("slots") or []:
        if not isinstance(item, dict):
            continue
        slot_outcomes.append(
            {
                "slot": _compact_text(item.get("slot")),
                "calendar_date": _compact_text(item.get("calendar_date")) or None,
                "calendar_label": _compact_text(item.get("calendar_label")) or None,
                "suggested_lane": _compact_text(item.get("suggested_lane")) or None,
                "alternate_lane": _compact_text(item.get("alternate_lane")) or None,
                "tracking_status": _compact_text(item.get("tracking_status")) or None,
                "tracking_note": _compact_text(item.get("tracking_note")) or None,
                "actual_lane": _compact_text(item.get("actual_lane")) or None,
                "performance_label": _compact_text(item.get("performance_label")) or None,
                "performance_note": _compact_text(item.get("performance_note")) or None,
            }
        )

    return {
        "available": True,
        "headline": _compact_text(social_plan.get("headline")) or None,
        "execution_feedback": dict(social_plan.get("execution_feedback") or {}),
        "slot_outcomes": slot_outcomes[:5],
    }


def _weekly_strategy_summary(feedback_payload: dict[str, Any]) -> dict[str, Any]:
    counts = feedback_payload.get("execution_feedback") if isinstance(feedback_payload.get("execution_feedback"), dict) else {}
    return {
        "weekly_strategy_feedback_available": bool(feedback_payload.get("available")),
        "weekly_strategy_recommended_lane_executed_count": int(counts.get("recommended_lane_executed") or 0),
        "weekly_strategy_alternate_lane_executed_count": int(counts.get("alternate_lane_executed") or 0),
        "weekly_strategy_different_lane_executed_count": int(counts.get("different_lane_executed") or 0),
        "weekly_strategy_awaiting_slot_count": int(counts.get("awaiting_slot") or 0),
        "weekly_strategy_no_post_observed_count": int(counts.get("no_post_observed") or 0),
        "weekly_strategy_review_slot_count": int(counts.get("review_slot") or 0),
    }


def _weekly_strategy_beliefs(feedback_payload: dict[str, Any]) -> list[dict[str, Any]]:
    beliefs: list[dict[str, Any]] = []
    for item in feedback_payload.get("slot_outcomes") or []:
        slot = _compact_text(item.get("slot")) or "Weekly slot"
        suggested = _compact_text(item.get("suggested_lane")) or "planned lane"
        actual = _compact_text(item.get("actual_lane"))
        status = _compact_text(item.get("tracking_status"))
        performance = _compact_text(item.get("performance_label"))
        if status == "recommended_lane_executed" and performance == "strong":
            beliefs.append(
                {
                    "headline": f"{slot} validated planned `{suggested}` with a strong observed result.",
                    "confidence": "medium",
                    "evidence": _compact_text(item.get("performance_note")) or _compact_text(item.get("tracking_note")) or "Observed lane and performance both matched the plan.",
                    "recommendation": f"Keep one more `{suggested}` slot in the weekly mix while this signal stays strong.",
                }
            )
        elif status == "alternate_lane_executed" and actual:
            beliefs.append(
                {
                    "headline": f"{slot} resolved into alternate `{actual}` instead of planned `{suggested}`.",
                    "confidence": "medium",
                    "evidence": _compact_text(item.get("tracking_note")) or "The fallback lane landed instead of the first recommendation.",
                    "recommendation": f"Bias similar concepts toward `{actual}` until `{suggested}` proves it can carry them cleanly.",
                }
            )
        elif status == "different_lane_executed" and actual:
            beliefs.append(
                {
                    "headline": f"{slot} landed in `{actual}` instead of the planned social lane.",
                    "confidence": "low",
                    "evidence": _compact_text(item.get("tracking_note")) or "A different lane executed than the one recommended in the packet.",
                    "recommendation": "Review whether the scheduling or lane-fit heuristic should be tightened before reusing this pattern.",
                }
            )
    return beliefs[:3]


def _weekly_strategy_changes(
    feedback_payload: dict[str, Any],
    previous_current_learnings_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    previous_feedback = (
        previous_current_learnings_payload.get("weekly_strategy_feedback")
        if isinstance(previous_current_learnings_payload.get("weekly_strategy_feedback"), dict)
        else {}
    )
    previous_slots = {
        _compact_text(item.get("slot")): item
        for item in (previous_feedback.get("slot_outcomes") or [])
        if isinstance(item, dict) and _compact_text(item.get("slot"))
    }
    changes: list[dict[str, Any]] = []
    for item in feedback_payload.get("slot_outcomes") or []:
        slot = _compact_text(item.get("slot"))
        status = _compact_text(item.get("tracking_status"))
        performance = _compact_text(item.get("performance_label"))
        suggested = _compact_text(item.get("suggested_lane")) or "planned lane"
        actual = _compact_text(item.get("actual_lane"))
        previous_item = previous_slots.get(slot, {})
        previous_status = _compact_text(previous_item.get("tracking_status"))
        previous_performance = _compact_text(previous_item.get("performance_label"))
        previous_actual = _compact_text(previous_item.get("actual_lane"))

        if status == "recommended_lane_executed" and performance == "strong":
            if previous_status != status or previous_performance != performance:
                changes.append(
                    {
                        "kind": "weekly_strategy_planned_lane_validated",
                        "headline": f"{slot} validated planned `{suggested}` with a strong result.",
                    }
                )
        elif status == "alternate_lane_executed" and actual:
            if previous_status != status or previous_actual != actual:
                changes.append(
                    {
                        "kind": "weekly_strategy_alternate_lane_won",
                        "headline": f"{slot} shifted into alternate `{actual}` instead of planned `{suggested}`.",
                    }
                )
        elif status == "different_lane_executed" and actual:
            if previous_status != status or previous_actual != actual:
                changes.append(
                    {
                        "kind": "weekly_strategy_different_lane_won",
                        "headline": f"{slot} landed in `{actual}` instead of the planned `{suggested}` lane.",
                    }
                )
        elif status == "no_post_observed":
            if previous_status != status:
                changes.append(
                    {
                        "kind": "weekly_strategy_slot_missed",
                        "headline": f"{slot} has no observed post yet for the planned `{suggested}` slot.",
                    }
                )
    return changes[:4]


def _competitor_social_freshness_changes(
    competitor_social_freshness: dict[str, Any],
    previous_current_learnings_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    previous_summary = (
        previous_current_learnings_payload.get("summary")
        if isinstance(previous_current_learnings_payload.get("summary"), dict)
        else {}
    )
    previous_label = _compact_text(previous_summary.get("competitor_social_freshness_label")) or "missing"
    current_label = _compact_text(competitor_social_freshness.get("competitor_social_freshness_label")) or "missing"
    if current_label == previous_label:
        return []

    if current_label == "live":
        headline = "Competitor social freshness recovered to live coverage."
        kind = "competitor_social_freshness_recovered"
    elif current_label == "staggered":
        headline = f"Competitor social freshness shifted into staggered cadence from `{previous_label}`."
        kind = "competitor_social_freshness_staggered"
    elif current_label in {"cached", "hard_failure"}:
        headline = f"Competitor social freshness degraded from `{previous_label}` to `{current_label}`."
        kind = "competitor_social_freshness_degraded"
    else:
        headline = f"Competitor social freshness changed from `{previous_label}` to `{current_label}`."
        kind = "competitor_social_freshness_changed"

    detail = _compact_text(competitor_social_freshness.get("competitor_social_freshness_note"))
    payload = {"kind": kind, "headline": headline}
    if detail:
        payload["detail"] = detail
    return [payload]


def _change_notifier(changes: list[dict[str, Any]]) -> dict[str, Any]:
    material_items: list[dict[str, Any]] = []
    for item in changes:
        kind = _compact_text(item.get("kind"))
        if kind not in MATERIAL_CHANGE_KINDS:
            continue
        material_items.append(
            {
                "source": _compact_text(item.get("source")) or "learning",
                "kind": kind,
                "urgency": "attention" if kind in ATTENTION_CHANGE_KINDS else "opportunity",
                "headline": _compact_text(item.get("headline")) or "Learning changed.",
                "detail": _compact_text(item.get("detail")) or None,
            }
        )

    attention_count = sum(1 for item in material_items if item.get("urgency") == "attention")
    opportunity_count = sum(1 for item in material_items if item.get("urgency") == "opportunity")
    if attention_count:
        headline = f"{attention_count} attention-level learning change(s) need review in the next planning pass."
    elif material_items:
        headline = f"{len(material_items)} meaningful learning change(s) landed since the previous snapshot."
    else:
        headline = "No material learning change needs operator action right now."

    return {
        "available": True,
        "headline": headline,
        "change_count": len(changes),
        "material_change_count": len(material_items),
        "attention_change_count": attention_count,
        "opportunity_change_count": opportunity_count,
        "recommended_action": "review current_learnings + weekly_strategy_recommendation_packet" if material_items else None,
        "items": material_items[:4],
    }


def build_current_learnings_payload() -> dict[str, Any]:
    social_payload = load_json(SOCIAL_ROLLUPS_PATH, {})
    competitor_market_payload = load_json(COMPETITOR_BENCHMARK_PATH, {})
    competitor_social_payload = load_json(COMPETITOR_SOCIAL_BENCHMARK_PATH, {})
    competitor_social_snapshots_payload = load_json(COMPETITOR_SOCIAL_SNAPSHOTS_PATH, {})
    weekly_strategy_packet_payload = load_json(WEEKLY_STRATEGY_PACKET_PATH, {})
    previous_current_learnings_payload = load_json(CURRENT_LEARNINGS_STATE_PATH, {})
    if not isinstance(social_payload, dict):
        social_payload = {}
    if not isinstance(competitor_market_payload, dict):
        competitor_market_payload = {}
    if not isinstance(competitor_social_payload, dict):
        competitor_social_payload = {}
    if not isinstance(competitor_social_snapshots_payload, dict):
        competitor_social_snapshots_payload = {}
    if not isinstance(weekly_strategy_packet_payload, dict):
        weekly_strategy_packet_payload = {}
    if not isinstance(previous_current_learnings_payload, dict):
        previous_current_learnings_payload = {}

    competitor_social_freshness = _competitor_social_freshness(competitor_social_snapshots_payload)
    weekly_strategy_feedback = _weekly_strategy_feedback(weekly_strategy_packet_payload)
    changes = _changes(
        social_payload,
        weekly_strategy_feedback,
        previous_current_learnings_payload,
        competitor_market_payload,
        competitor_social_payload,
        competitor_social_freshness,
    )

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Current learnings across our own social results, competitor market signals, and competitor social snapshots.",
            "social_post_count": int(((social_payload.get("summary") or {}).get("post_count")) or 0),
            "social_metrics_coverage_pct": float(((social_payload.get("summary") or {}).get("metrics_coverage_pct")) or 0.0),
            "competitor_observation_days": int(((competitor_market_payload.get("summary") or {}).get("observation_days")) or 0),
            "competitor_social_post_count": int(((competitor_social_payload.get("summary") or {}).get("post_count")) or 0),
            **competitor_social_freshness,
            **_weekly_strategy_summary(weekly_strategy_feedback),
            "data_quality_note": _compact_text((social_payload.get("summary") or {}).get("data_quality_note"))
            or _compact_text((competitor_social_payload.get("summary") or {}).get("data_quality_note"))
            or _compact_text((competitor_social_snapshots_payload.get("summary") or {}).get("data_quality_note"))
            or _compact_text((competitor_market_payload.get("summary") or {}).get("data_quality_note")),
        },
        "current_beliefs": _current_beliefs(
            social_payload,
            weekly_strategy_feedback,
            competitor_market_payload,
            competitor_social_payload,
        ),
        "changes_since_previous": changes,
        "change_notifier": _change_notifier(changes),
        "weekly_strategy_feedback": weekly_strategy_feedback,
        "best_windows": _best_windows(social_payload),
        "strongest_workflows": _strongest_workflows(social_payload),
        "top_posts": list(social_payload.get("top_posts") or [])[:5],
        "competitor_motifs": list(
            (competitor_social_payload.get("by_theme") or competitor_market_payload.get("emergent_motifs") or [])
        )[:8],
        "ideas_to_test": list(
            competitor_social_payload.get("ideas_to_test") or competitor_market_payload.get("ideas_to_test") or []
        )[:6],
        "paths": {
            "social_rollups": str(SOCIAL_ROLLUPS_PATH),
            "competitor_benchmark": str(COMPETITOR_BENCHMARK_PATH),
            "competitor_social_benchmark": str(COMPETITOR_SOCIAL_BENCHMARK_PATH),
            "competitor_social_snapshots": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
            "weekly_strategy_packet": str(WEEKLY_STRATEGY_PACKET_PATH),
        },
    }
    return payload


def render_current_learnings_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    weekly_strategy_feedback = payload.get("weekly_strategy_feedback") if isinstance(payload.get("weekly_strategy_feedback"), dict) else {}
    change_notifier = payload.get("change_notifier") if isinstance(payload.get("change_notifier"), dict) else {}
    lines = [
        "# Current Learnings",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Own posts observed: `{summary.get('social_post_count') or 0}`",
        f"- Own metrics coverage: `{summary.get('social_metrics_coverage_pct') or 0}%`",
        f"- Competitor observation days: `{summary.get('competitor_observation_days') or 0}`",
        f"- Competitor social posts observed: `{summary.get('competitor_social_post_count') or 0}`",
        "",
        str(summary.get("headline") or ""),
        "",
        str(summary.get("data_quality_note") or ""),
        "",
        "## Competitor Social Freshness",
        "",
        f"- Snapshot generated: `{summary.get('competitor_social_snapshot_generated_at') or 'unknown'}`",
        f"- Snapshot age: `{summary.get('competitor_social_snapshot_age_hours') if summary.get('competitor_social_snapshot_age_hours') is not None else 'unknown'}` hour(s)",
        f"- Collected accounts: `{summary.get('competitor_social_collected_account_count') or 0}`",
        f"- Live accounts: `{summary.get('competitor_social_live_account_count') or 0}`",
        f"- Cached fallback accounts: `{summary.get('competitor_social_cached_account_count') or 0}`",
        f"- Degraded fetches: `{summary.get('competitor_social_degraded_account_count') or 0}`",
        f"- Hard failures: `{summary.get('competitor_social_failed_account_count') or 0}`",
        f"- Scheduled skip accounts: `{summary.get('competitor_social_scheduled_skip_account_count') or 0}`",
        f"- Profile-only backoff accounts: `{summary.get('competitor_social_profile_only_backoff_account_count') or 0}`",
        f"- Live canary-limited accounts: `{summary.get('competitor_social_live_canary_limited_account_count') or 0}`",
        f"- Live canary targets: `{summary.get('competitor_social_live_canary_target_count') or 0}` of `{summary.get('competitor_social_max_live_canary_targets') or 0}`",
        f"- Active refresh targets: `{summary.get('competitor_social_active_refresh_target_count') or 0}`",
        f"- Truth: {summary.get('competitor_social_freshness_note') or 'No competitor social snapshot is available yet.'}",
        "",
        "## Weekly Strategy Follow-Through",
        "",
    ]

    if not weekly_strategy_feedback.get("available"):
        lines.append("No weekly strategy follow-through is available yet.")
        lines.append("")
    else:
        lines.extend(
            [
                str(weekly_strategy_feedback.get("headline") or "Weekly strategy follow-through is available."),
                "",
                f"- Planned lane wins: `{summary.get('weekly_strategy_recommended_lane_executed_count') or 0}`",
                f"- Alternate lane wins: `{summary.get('weekly_strategy_alternate_lane_executed_count') or 0}`",
                f"- Different lane wins: `{summary.get('weekly_strategy_different_lane_executed_count') or 0}`",
                f"- Awaiting slots: `{summary.get('weekly_strategy_awaiting_slot_count') or 0}`",
                f"- Missed slots: `{summary.get('weekly_strategy_no_post_observed_count') or 0}`",
                f"- Review slots: `{summary.get('weekly_strategy_review_slot_count') or 0}`",
                "",
            ]
        )
        for item in weekly_strategy_feedback.get("slot_outcomes") or []:
            slot = item.get("slot") or "Weekly slot"
            calendar_parts = [item.get("calendar_label"), item.get("calendar_date")]
            calendar_label = " | ".join(part for part in calendar_parts if part)
            lines.append(f"### {slot}")
            lines.append("")
            if calendar_label:
                lines.append(f"- Calendar: `{calendar_label}`")
            lines.append(f"- Planned lane: `{item.get('suggested_lane') or 'unknown'}`")
            lines.append(f"- Outcome: `{item.get('tracking_status') or 'unknown'}`")
            if item.get("actual_lane"):
                lines.append(f"- Actual lane: `{item.get('actual_lane')}`")
            if item.get("performance_label"):
                lines.append(f"- Performance: `{item.get('performance_label')}`")
            if item.get("tracking_note"):
                lines.append(f"- Note: {item.get('tracking_note')}")
            if item.get("performance_note"):
                lines.append(f"- Performance detail: {item.get('performance_note')}")
            lines.append("")

    lines.extend(
        [
        "## Change Notifier",
        "",
        ]
    )

    if not change_notifier.get("available"):
        lines.append("No change notifier is available yet.")
        lines.append("")
    else:
        lines.append(str(change_notifier.get("headline") or "No material learning change needs operator action right now."))
        lines.append("")
        lines.append(f"- Total changes observed: `{change_notifier.get('change_count') or 0}`")
        lines.append(f"- Material changes: `{change_notifier.get('material_change_count') or 0}`")
        lines.append(f"- Attention-level changes: `{change_notifier.get('attention_change_count') or 0}`")
        if change_notifier.get("recommended_action"):
            lines.append(f"- Review command: `{change_notifier.get('recommended_action')}`")
        notifier_items = change_notifier.get("items") or []
        if notifier_items:
            lines.append("- Highlights:")
            for item in notifier_items:
                lines.append(
                    f"  - `{item.get('urgency') or 'info'}` | `{item.get('source') or 'learning'}` | {item.get('headline')}"
                )
                if item.get("detail"):
                    lines.append(f"    Detail: {item.get('detail')}")
        lines.append("")

    lines.extend(
        [
        "## What Changed",
        "",
        ]
    )

    changes = payload.get("changes_since_previous") or []
    if not changes:
        lines.append("No major learning change was detected since the previous snapshot.")
        lines.append("")
    else:
        for item in changes:
            lines.append(f"- `{item.get('source')}`: {item.get('headline')}")
        lines.append("")

    lines.extend(["## Current Beliefs", ""])
    beliefs = payload.get("current_beliefs") or []
    if not beliefs:
        lines.append("No current beliefs are available yet.")
        lines.append("")
    else:
        for item in beliefs:
            lines.extend(
                [
                    f"### {item.get('headline')}",
                    "",
                    f"- Source: `{item.get('source')}`",
                    f"- Confidence: `{item.get('confidence')}`",
                    f"- Evidence: {item.get('evidence')}",
                    f"- Recommendation: {item.get('recommendation')}",
                    "",
                ]
            )

    lines.extend(["## Best Windows", ""])
    for item in payload.get("best_windows") or []:
        lines.append(f"- `{item.get('label')}`: `{item.get('post_count')}` posts | avg score `{item.get('avg_engagement_score')}`")
    if not (payload.get("best_windows") or []):
        lines.append("No posting-window learnings are available yet.")
    lines.append("")

    lines.extend(["## Strongest Workflows", ""])
    for item in payload.get("strongest_workflows") or []:
        lines.append(f"- `{item.get('label')}`: `{item.get('post_count')}` posts | avg score `{item.get('avg_engagement_score')}`")
    if not (payload.get("strongest_workflows") or []):
        lines.append("No workflow learnings are available yet.")
    lines.append("")

    lines.extend(["## Top Competitor Motifs", ""])
    motifs = payload.get("competitor_motifs") or []
    if not motifs:
        lines.append("No competitor motifs are available yet.")
        lines.append("")
    else:
        for item in motifs:
            label = item.get("keyword") or item.get("label")
            score = item.get("score") if item.get("score") is not None else item.get("avg_engagement_score")
            count = item.get("listing_count") if item.get("listing_count") is not None else item.get("post_count")
            lines.append(f"- `{label}` | score `{score}` | count `{count}`")
        lines.append("")

    lines.extend(["## Ideas Worth Testing", ""])
    ideas = payload.get("ideas_to_test") or []
    if not ideas:
        lines.append("No test ideas are staged yet.")
        lines.append("")
    else:
        for idea in ideas:
            lines.append(f"- {idea}")
        lines.append("")

    return "\n".join(lines)


def build_current_learnings() -> dict[str, Any]:
    payload = build_current_learnings_payload()
    write_json(CURRENT_LEARNINGS_STATE_PATH, payload)
    write_json(CURRENT_LEARNINGS_OPERATOR_JSON_PATH, payload)
    write_markdown(CURRENT_LEARNINGS_MD_PATH, render_current_learnings_markdown(payload))
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the current learnings operator page.")
    parser.parse_args()
    payload = build_current_learnings()
    print(
        {
            "generated_at": payload.get("generated_at"),
            "belief_count": len(payload.get("current_beliefs") or []),
            "idea_count": len(payload.get("ideas_to_test") or []),
        }
    )


if __name__ == "__main__":
    main()
