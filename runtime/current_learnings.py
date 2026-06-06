from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, age_hours, load_json, now_local_iso, write_json, write_markdown


SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
SOCIAL_POSTS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_posts.json"
COMPETITOR_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "social_competitor_benchmark.json"
COMPETITOR_SOCIAL_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_benchmark.json"
COMPETITOR_SOCIAL_SNAPSHOTS_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshots.json"
WEEKLY_STRATEGY_PACKET_PATH = DUCK_OPS_ROOT / "state" / "weekly_strategy_recommendation_packet.json"
CURRENT_LEARNINGS_STATE_PATH = DUCK_OPS_ROOT / "state" / "current_learnings.json"
CURRENT_LEARNINGS_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.json"
CURRENT_LEARNINGS_MD_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.md"

# Phase 5 Step 5: read creative_quality_receipts that have outcomes
# attached (Step 4 writeback) so the Learning Inspector's "executed
# experiments" count + queued→executed promotion can light up. The
# receipt directory lives in the duckAgent repo (sibling of duck-ops).
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
CREATIVE_QUALITY_RECEIPTS_DIR = DUCK_AGENT_ROOT / "data" / "creative_quality_receipts"

MATERIAL_CHANGE_KINDS = {
    "weekly_strategy_planned_lane_validated",
    "weekly_strategy_alternate_lane_won",
    "weekly_strategy_different_lane_won",
    "weekly_strategy_slot_missed",
    "weekly_strategy_feed_stale",
    "weekly_strategy_execution_truth_changed",
    "weekly_strategy_lane_ready_to_scale",
    "weekly_strategy_lane_pull_back",
    "weekly_strategy_stable_pattern_changed",
    "weekly_strategy_experiment_changed",
    "weekly_strategy_guardrail_changed",
    "competitor_social_freshness_degraded",
    "competitor_social_freshness_recovered",
    "competitor_social_freshness_staggered",
}
ATTENTION_CHANGE_KINDS = {
    "weekly_strategy_slot_missed",
    "weekly_strategy_feed_stale",
    "weekly_strategy_lane_pull_back",
    "weekly_strategy_guardrail_changed",
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
    *,
    live_posts_payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in social_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "own_social", **item})
    for item in _weekly_strategy_changes(
        weekly_strategy_feedback,
        previous_current_learnings_payload,
        live_posts_payload=live_posts_payload,
    ):
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
    strategy_frames = packet_payload.get("strategy_frames") if isinstance(packet_payload.get("strategy_frames"), dict) else {}
    stable_patterns = list(packet_payload.get("stable_patterns") or [])
    experimental_ideas = list(packet_payload.get("experimental_ideas") or [])
    do_not_copy_patterns = list(packet_payload.get("do_not_copy_patterns") or [])
    if not social_plan:
        return {
            "available": False,
            "execution_feedback": {},
            "execution_truth": {},
            "lane_guidance_summary": {},
            "lane_guidance": [],
            "slot_outcomes": [],
            "slot_feedback_items": [],
            "stable_patterns": [],
            "experimental_ideas": [],
            "do_not_copy_patterns": [],
            "strategy_frames": {},
        }

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
                "execution_mode": _compact_text(item.get("execution_mode")) or None,
                "execution_readiness": _compact_text(item.get("execution_readiness")) or None,
                "tracking_status": _compact_text(item.get("tracking_status")) or None,
                "tracking_note": _compact_text(item.get("tracking_note")) or None,
                "actual_lane": _compact_text(item.get("actual_lane")) or None,
                "performance_label": _compact_text(item.get("performance_label")) or None,
                "performance_note": _compact_text(item.get("performance_note")) or None,
            }
        )

    slot_feedback_items: list[dict[str, Any]] = []
    for item in slot_outcomes:
        status = _compact_text(item.get("tracking_status")) or "unknown"
        slot = _compact_text(item.get("slot")) or "Weekly slot"
        planned = _compact_text(item.get("suggested_lane")) or "planned lane"
        actual = _compact_text(item.get("actual_lane")) or None
        performance = _compact_text(item.get("performance_label")) or None
        if status == "recommended_lane_executed":
            if performance == "strong":
                action = f"Keep `{planned}` as a planned lane candidate next week; it has a clean execution win."
                priority = "scale_signal"
            else:
                action = f"Keep `{planned}` in the mix, but wait for a stronger result before scaling it."
                priority = "watch_signal"
        elif status == "alternate_lane_executed" and actual:
            action = f"Treat `{actual}` as the safer fallback for similar slots until `{planned}` lands cleanly."
            priority = "fallback_signal"
        elif status == "different_lane_executed" and actual:
            action = f"Review why `{actual}` ran instead of `{planned}` before trusting this slot recommendation."
            priority = "routing_drift"
        elif status == "no_post_observed":
            action = f"Check whether the `{planned}` slot failed to publish or simply lacks metrics before reusing the recommendation."
            priority = "missed_slot"
        elif status == "review_slot":
            action = f"Keep `{planned}` manually reviewed until the approval path has cleaner outcome history."
            priority = "review_needed"
        elif status == "awaiting_slot":
            action = f"Wait for the `{planned}` slot to run before changing the weekly recommendation."
            priority = "wait"
        else:
            action = "Review the slot outcome before turning it into a strategy change."
            priority = "review_needed"
        slot_feedback_items.append(
            {
                "slot": slot,
                "calendar_label": _compact_text(item.get("calendar_label")) or None,
                "calendar_date": _compact_text(item.get("calendar_date")) or None,
                "planned_lane": planned,
                "actual_lane": actual,
                "tracking_status": status,
                "performance_label": performance,
                "priority": priority,
                "recommended_action": action,
                "evidence": _compact_text(item.get("performance_note")) or _compact_text(item.get("tracking_note")) or None,
            }
        )

    return {
        "available": True,
        "headline": _compact_text(social_plan.get("headline")) or None,
        "execution_feedback": dict(social_plan.get("execution_feedback") or {}),
        "execution_truth": dict(social_plan.get("execution_truth") or {}),
        "lane_guidance_summary": dict(social_plan.get("lane_guidance_summary") or {}),
        "lane_guidance": [
            {
                "lane": _compact_text(item.get("lane")) or None,
                "decision": _compact_text(item.get("decision")) or None,
                "title": _compact_text(item.get("title")) or None,
                "summary": _compact_text(item.get("summary")) or None,
                "recommended_action": _compact_text(item.get("recommended_action")) or None,
                "evidence": _compact_text(item.get("evidence")) or None,
                "confidence": _compact_text(item.get("confidence")) or None,
            }
            for item in (social_plan.get("lane_guidance") or [])[:5]
            if isinstance(item, dict) and _compact_text(item.get("lane"))
        ],
        "slot_outcomes": slot_outcomes[:5],
        "slot_feedback_items": slot_feedback_items[:5],
        "stable_patterns": [
            {
                "title": _compact_text(item.get("title")),
                "recommendation": _compact_text(item.get("recommendation")),
                "evidence": _compact_text(item.get("evidence")),
                "signal_type": _compact_text(item.get("signal_type")),
                "confidence": _compact_text(item.get("confidence")),
            }
            for item in stable_patterns[:4]
            if isinstance(item, dict) and _compact_text(item.get("title"))
        ],
        "experimental_ideas": [
            {
                "title": _compact_text(item.get("title")),
                "recommendation": _compact_text(item.get("recommendation")),
                "evidence": _compact_text(item.get("evidence")),
                "signal_type": _compact_text(item.get("signal_type")),
                "confidence": _compact_text(item.get("confidence")),
            }
            for item in experimental_ideas[:4]
            if isinstance(item, dict) and _compact_text(item.get("title"))
        ],
        "do_not_copy_patterns": [
            {
                "title": _compact_text(item.get("title")),
                "guidance": _compact_text(item.get("guidance")),
                "evidence": _compact_text(item.get("evidence")),
                "signal_type": _compact_text(item.get("signal_type")),
                "confidence": _compact_text(item.get("confidence")),
            }
            for item in do_not_copy_patterns[:4]
            if isinstance(item, dict) and _compact_text(item.get("title"))
        ],
        "strategy_frames": {
            key: dict(strategy_frames.get(key) or {})
            for key in ("stable_patterns", "experimental_ideas", "do_not_copy_patterns")
            if isinstance(strategy_frames.get(key), dict)
        },
    }


def _weekly_strategy_summary(feedback_payload: dict[str, Any]) -> dict[str, Any]:
    counts = feedback_payload.get("execution_feedback") if isinstance(feedback_payload.get("execution_feedback"), dict) else {}
    execution_truth = feedback_payload.get("execution_truth") if isinstance(feedback_payload.get("execution_truth"), dict) else {}
    lane_guidance_summary = (
        feedback_payload.get("lane_guidance_summary")
        if isinstance(feedback_payload.get("lane_guidance_summary"), dict)
        else {}
    )
    return {
        "weekly_strategy_feedback_available": bool(feedback_payload.get("available")),
        "weekly_strategy_recommended_lane_executed_count": int(counts.get("recommended_lane_executed") or 0),
        "weekly_strategy_alternate_lane_executed_count": int(counts.get("alternate_lane_executed") or 0),
        "weekly_strategy_different_lane_executed_count": int(counts.get("different_lane_executed") or 0),
        "weekly_strategy_awaiting_slot_count": int(counts.get("awaiting_slot") or 0),
        "weekly_strategy_no_post_observed_count": int(counts.get("no_post_observed") or 0),
        "weekly_strategy_review_slot_count": int(counts.get("review_slot") or 0),
        "weekly_strategy_execution_truth_label": _compact_text(execution_truth.get("label")) or None,
        "weekly_strategy_execution_truth_note": _compact_text(execution_truth.get("note")) or None,
        "weekly_strategy_lane_ready_to_scale_count": int(lane_guidance_summary.get("ready_to_scale") or 0),
        "weekly_strategy_lane_keep_anchor_count": int(lane_guidance_summary.get("keep_anchor") or 0),
        "weekly_strategy_lane_fallback_only_count": int(lane_guidance_summary.get("fallback_only") or 0),
        "weekly_strategy_lane_experiment_only_count": int(lane_guidance_summary.get("experiment_only") or 0),
        "weekly_strategy_lane_pull_back_count": int(lane_guidance_summary.get("pull_back") or 0),
    }


def _weekly_strategy_beliefs(feedback_payload: dict[str, Any]) -> list[dict[str, Any]]:
    beliefs: list[dict[str, Any]] = []
    execution_truth = feedback_payload.get("execution_truth") if isinstance(feedback_payload.get("execution_truth"), dict) else {}
    execution_headline = _compact_text(execution_truth.get("headline"))
    execution_note = _compact_text(execution_truth.get("note"))
    lane_guidance = [item for item in (feedback_payload.get("lane_guidance") or []) if isinstance(item, dict)]
    if execution_headline or execution_note:
        primary_guidance = next((item for item in lane_guidance if _compact_text(item.get("recommended_action"))), {})
        beliefs.append(
            {
                "headline": execution_headline or "Weekly strategy execution truth is available.",
                "confidence": "medium",
                "evidence": execution_note or "Recent slot outcomes were folded back into the current learnings surface.",
                "recommendation": _compact_text(primary_guidance.get("recommended_action"))
                or "Use the lane guidance before scaling or cutting any weekly lane.",
            }
        )
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
    for item in feedback_payload.get("stable_patterns") or []:
        title = _compact_text(item.get("title"))
        recommendation = _compact_text(item.get("recommendation"))
        evidence = _compact_text(item.get("evidence"))
        if not title or not recommendation:
            continue
        beliefs.append(
            {
                "headline": title,
                "confidence": _compact_text(item.get("confidence")) or "medium",
                "evidence": evidence or "The weekly strategy packet still considers this a stable pattern.",
                "recommendation": recommendation,
            }
        )
        break
    for item in feedback_payload.get("experimental_ideas") or []:
        title = _compact_text(item.get("title"))
        recommendation = _compact_text(item.get("recommendation"))
        evidence = _compact_text(item.get("evidence"))
        if not title or not recommendation:
            continue
        beliefs.append(
            {
                "headline": f"Active experiment: {title}",
                "confidence": _compact_text(item.get("confidence")) or "low_medium",
                "evidence": evidence or "The weekly strategy packet staged this as the next bounded experiment.",
                "recommendation": recommendation,
            }
        )
        break
    for item in feedback_payload.get("do_not_copy_patterns") or []:
        title = _compact_text(item.get("title"))
        guidance = _compact_text(item.get("guidance"))
        evidence = _compact_text(item.get("evidence"))
        if not title or not guidance:
            continue
        beliefs.append(
            {
                "headline": f"Guardrail: {title}",
                "confidence": _compact_text(item.get("confidence")) or "high",
                "evidence": evidence or "The weekly strategy packet marked this as a do-not-copy pattern.",
                "recommendation": guidance,
            }
        )
        break
    return beliefs[:4]


# 2026-06-04: Live-posts verification to suppress false-positive
# weekly_strategy_slot_missed emissions. Root cause: the weekly_
# strategy_recommendation_packet is generated from a snapshot of
# social_performance_posts.json. The current_learnings emitter
# reads slot_outcomes from the cached packet. If the packet was
# generated before social_performance_posts was refreshed for the
# day, every executed slot still reads "no_post_observed" — and
# the email goes out claiming a slot was missed even though the
# post is sitting in the live posts file.
#
# Verified incident 2026-06-04: jeepfact posted Wed 6pm (FB) +
# Wed 10:19am (IG due to the Meta IG quirk). Both in the live
# posts file. But current_learnings ran at 07:10 against a packet
# generated before social_performance_posts refresh and emitted
# "Slot 2 has no observed post yet for the planned `jeepfact`
# slot." 10 min later the packet refreshed and the same slot
# showed recommended_lane_executed. False positive in operator
# inbox.
#
# Fix shape: at change-emit time, re-read social_performance_posts
# directly and verify the missed-slot claim against the live data.
# If a matching post exists, suppress. If the live posts file is
# itself stale (so we can't verify), emit a different change kind
# (`weekly_strategy_feed_stale`) instead — operator gets told the
# observability layer is broken, not that a lane was skipped.

_LIVE_POSTS_STALE_HOURS = 24


def _live_post_matches_slot(
    *,
    posts_payload: dict[str, Any],
    target_date: str,
    suggested_lane: str,
) -> dict[str, Any] | None:
    """Returns the first live post that matches target_date + lane
    (case-insensitive on workflow), or None. Excludes future-dated
    posts; we want posts the system has actually observed going
    live, not scheduled-but-pending entries."""
    if not target_date or not suggested_lane:
        return None
    posts = posts_payload.get("posts") if isinstance(posts_payload, dict) else None
    if not isinstance(posts, list):
        return None
    suggested_norm = _compact_text(suggested_lane).lower()
    target_norm = _compact_text(target_date)
    for post in posts:
        if not isinstance(post, dict):
            continue
        if bool(post.get("is_future_post")):
            continue
        if _compact_text(post.get("published_date")) != target_norm:
            continue
        if _compact_text(post.get("workflow")).lower() == suggested_norm:
            return post
    return None


def _live_posts_feed_is_stale(
    posts_payload: dict[str, Any],
    *,
    stale_after_hours: float = _LIVE_POSTS_STALE_HOURS,
) -> bool:
    """The live posts file's generated_at is the authoritative
    freshness signal. If it hasn't been refreshed recently we can't
    trust 'no live post matches this slot' as proof the slot was
    actually missed."""
    if not isinstance(posts_payload, dict):
        return True
    generated_at = _compact_text(posts_payload.get("generated_at"))
    if not generated_at:
        return True
    hours = age_hours(generated_at)
    if hours is None:
        return True
    return hours > stale_after_hours


def _weekly_strategy_changes(
    feedback_payload: dict[str, Any],
    previous_current_learnings_payload: dict[str, Any],
    *,
    live_posts_payload: dict[str, Any] | None = None,
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
                # 2026-06-04: verify against the live posts file
                # before emitting. The cached packet's
                # tracking_status often lags the live posts file by
                # 10-20 minutes in the morning, producing false
                # positives. See _live_post_matches_slot above for
                # the incident record.
                calendar_date = _compact_text(item.get("calendar_date"))
                live_payload = live_posts_payload if isinstance(live_posts_payload, dict) else None
                matched_live = None
                feed_is_stale = False
                if live_payload is not None:
                    feed_is_stale = _live_posts_feed_is_stale(live_payload)
                    if not feed_is_stale:
                        matched_live = _live_post_matches_slot(
                            posts_payload=live_payload,
                            target_date=calendar_date,
                            suggested_lane=suggested,
                        )
                if matched_live is not None:
                    # Live posts file has a matching post — cached
                    # slot_outcomes was stale. Suppress the false
                    # positive entirely. Do NOT emit anything; the
                    # cache will catch up on the next refresh.
                    continue
                if feed_is_stale:
                    # We can't verify either way — tell the
                    # operator the observability feed is the
                    # problem, not the lane. Different change kind
                    # so it doesn't trip the "slot missed"
                    # operator-action playbook.
                    changes.append(
                        {
                            "kind": "weekly_strategy_feed_stale",
                            "headline": (
                                f"{slot} can't be verified — "
                                "social_performance_posts.json is "
                                "stale, so the cached "
                                "no_post_observed reading is "
                                "untrusted. Refresh the feed before "
                                "concluding the slot was missed."
                            ),
                        }
                    )
                else:
                    changes.append(
                        {
                            "kind": "weekly_strategy_slot_missed",
                            "headline": f"{slot} has no observed post yet for the planned `{suggested}` slot.",
                        }
                    )

    previous_execution_truth = (
        previous_feedback.get("execution_truth") if isinstance(previous_feedback.get("execution_truth"), dict) else {}
    )
    current_execution_truth = (
        feedback_payload.get("execution_truth") if isinstance(feedback_payload.get("execution_truth"), dict) else {}
    )
    previous_truth_label = _compact_text(previous_execution_truth.get("label"))
    current_truth_label = _compact_text(current_execution_truth.get("label"))
    if previous_truth_label and current_truth_label and previous_truth_label != current_truth_label:
        changes.append(
            {
                "kind": "weekly_strategy_execution_truth_changed",
                "headline": f"Weekly strategy execution truth shifted from `{previous_truth_label}` to `{current_truth_label}`.",
                "detail": _compact_text(current_execution_truth.get("note")) or None,
            }
        )

    previous_lane_guidance = {
        _compact_text(item.get("lane")): item
        for item in (previous_feedback.get("lane_guidance") or [])
        if isinstance(item, dict) and _compact_text(item.get("lane"))
    }
    for item in feedback_payload.get("lane_guidance") or []:
        if not isinstance(item, dict):
            continue
        lane = _compact_text(item.get("lane"))
        decision = _compact_text(item.get("decision"))
        if not lane or not decision:
            continue
        previous_item = previous_lane_guidance.get(lane, {})
        previous_decision = _compact_text(previous_item.get("decision"))
        if not previous_decision or previous_decision == decision:
            continue
        detail = _compact_text(item.get("summary")) or _compact_text(item.get("evidence")) or None
        if decision == "ready_to_scale":
            changes.append(
                {
                    "kind": "weekly_strategy_lane_ready_to_scale",
                    "headline": f"`{lane}` now has enough repeated clean wins to scale more confidently.",
                    "detail": detail,
                }
            )
        elif decision == "pull_back":
            changes.append(
                {
                    "kind": "weekly_strategy_lane_pull_back",
                    "headline": f"`{lane}` should not absorb more calendar volume until execution stabilizes.",
                    "detail": detail,
                }
            )

    def _titles(items: Any) -> list[str]:
        return [
            _compact_text(item.get("title"))
            for item in (items or [])
            if isinstance(item, dict) and _compact_text(item.get("title"))
        ][:3]

    previous_stable_titles = _titles(previous_feedback.get("stable_patterns"))
    current_stable_titles = _titles(feedback_payload.get("stable_patterns"))
    if current_stable_titles and current_stable_titles != previous_stable_titles:
        changes.append(
            {
                "kind": "weekly_strategy_stable_pattern_changed",
                "headline": "Weekly strategy stable patterns changed since the previous snapshot.",
                "detail": "Current stable patterns: " + "; ".join(current_stable_titles),
            }
        )

    previous_experiment_titles = _titles(previous_feedback.get("experimental_ideas"))
    current_experiment_titles = _titles(feedback_payload.get("experimental_ideas"))
    if current_experiment_titles and current_experiment_titles != previous_experiment_titles:
        changes.append(
            {
                "kind": "weekly_strategy_experiment_changed",
                "headline": "Weekly strategy experiment ideas changed since the previous snapshot.",
                "detail": "Current experiments: " + "; ".join(current_experiment_titles),
            }
        )

    previous_guardrail_titles = _titles(previous_feedback.get("do_not_copy_patterns"))
    current_guardrail_titles = _titles(feedback_payload.get("do_not_copy_patterns"))
    if current_guardrail_titles and current_guardrail_titles != previous_guardrail_titles:
        changes.append(
            {
                "kind": "weekly_strategy_guardrail_changed",
                "headline": "Weekly strategy do-not-copy guardrails changed and should be re-read.",
                "detail": "Current guardrails: " + "; ".join(current_guardrail_titles),
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


def _packet_is_stale_vs_posts(
    *,
    packet_payload: dict[str, Any],
    posts_payload: dict[str, Any],
) -> bool:
    """Returns True when the weekly_strategy_packet was generated
    BEFORE the live social posts file — meaning slot_outcomes
    reflects pre-refresh state and will read 'no_post_observed' for
    posts that the live file already has. This is the root-cause
    timing race the 2026-06-04 false positive surfaced."""
    from datetime import datetime as _dt
    packet_at = _compact_text(packet_payload.get("generated_at"))
    posts_at = _compact_text(posts_payload.get("generated_at"))
    if not packet_at or not posts_at:
        # If either timestamp is missing we can't compare; skip the
        # auto-refresh. The verify-at-emit defense-in-depth still
        # runs and will suppress any false positives.
        return False
    try:
        packet_dt = _dt.fromisoformat(packet_at)
        posts_dt = _dt.fromisoformat(posts_at)
    except ValueError:
        return False
    return packet_dt < posts_dt


def _refresh_weekly_strategy_packet_if_stale() -> dict[str, Any]:
    """Root-cause fix for the morning ordering race: if the
    weekly_strategy_packet on disk pre-dates the live social posts
    file, regenerate it inline before current_learnings reads
    slot_outcomes. Returns the (possibly re-loaded) packet payload.

    The verify-at-emit path in _weekly_strategy_changes is kept as
    defense-in-depth — both layers together mean a false positive
    requires (a) the packet to be stale AND (b) the live-posts
    verification to fail to find a matching post AND (c) the live-
    posts file to be itself fresh enough to trust. Three independent
    checks; ~impossible for the 2026-06-04 incident shape to recur."""
    packet_payload = load_json(WEEKLY_STRATEGY_PACKET_PATH, {})
    posts_payload = load_json(SOCIAL_POSTS_PATH, {})
    if not isinstance(packet_payload, dict):
        packet_payload = {}
    if not isinstance(posts_payload, dict):
        posts_payload = {}
    if not _packet_is_stale_vs_posts(
        packet_payload=packet_payload, posts_payload=posts_payload,
    ):
        return packet_payload

    # Stale. Regenerate inline. Import here to avoid any module-load
    # circularity (the packet builder reads CURRENT_LEARNINGS_PATH as
    # data, not as a Python import, but defer just in case).
    try:
        from weekly_strategy_recommendation_packet import (
            build_weekly_strategy_recommendation_packet,
        )
    except ImportError:
        # Best-effort: if the builder isn't on path (e.g. running
        # current_learnings in isolation), skip the auto-refresh and
        # let the verify-at-emit layer handle the rest.
        return packet_payload
    try:
        build_weekly_strategy_recommendation_packet()
    except Exception as exc:
        # Don't let a packet-build error block current_learnings —
        # the existing (stale) packet + verify-at-emit is still safer
        # than no emit at all. Surface the failure loudly per
        # [[feedback-swallowed-errors-lie]].
        print(
            f"[current_learnings] WARNING auto-refresh of "
            f"weekly_strategy_packet failed: {exc}. Continuing with "
            "stale packet; verify-at-emit will suppress false "
            "positives."
        )
        return packet_payload

    # Re-read the freshly-written packet.
    refreshed = load_json(WEEKLY_STRATEGY_PACKET_PATH, {})
    return refreshed if isinstance(refreshed, dict) else packet_payload


def _executed_experiments_from_receipts(
    *, window_days: int = 14,
    receipts_dir: Path | None = None,
    now_iso: str | None = None,
) -> dict[str, Any]:
    """Phase 5 Step 5: glob creative_quality_receipts/*.json, count
    receipts whose outcome_status is partial_24h or final_7d and whose
    publish.published_at falls within the last window_days, and return
    a structured summary the viewer's build_learning_inspector_payload
    can join against queued_experiments.

    Returns:
      {
        "count": int,
        "window_days": 14,
        "receipts": [
          {
            "flow": "meme",
            "run_id": "2026-06-08",
            "published_at": "2026-06-08T18:00:14-04:00",
            "post_id": "17912345",
            "platform": "instagram",
            "outcome_status": "partial_24h" | "final_7d",
            "outcomes": [{"window": "24h", "engagement_score": 47.0, ...}, ...],
            "theme": "<from receipt or post_meta>",
            "title": "<from receipt or post_meta>",
            "watch_account": None,  # never set by collector — viewer
                                     # joins against queued_experiments
                                     # by theme/title fuzz instead
          }, ...
        ],
      }

    Never raises — missing/malformed receipts are skipped silently."""
    from datetime import datetime, timedelta
    import json as _json

    rdir = receipts_dir if receipts_dir is not None else CREATIVE_QUALITY_RECEIPTS_DIR
    if not rdir.exists():
        return {"count": 0, "window_days": window_days, "receipts": []}

    if now_iso:
        try:
            now_dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
            if now_dt.tzinfo is None:
                now_dt = now_dt.astimezone()
        except ValueError:
            now_dt = datetime.now().astimezone()
    else:
        now_dt = datetime.now().astimezone()
    cutoff = now_dt - timedelta(days=window_days)

    executed: list[dict[str, Any]] = []
    for path in sorted(rdir.glob("*.json")):
        try:
            payload = _json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        status = str(payload.get("outcome_status") or "").strip()
        if status not in {"partial_24h", "final_7d"}:
            continue
        publish_block = payload.get("publish") if isinstance(payload.get("publish"), dict) else {}
        published_at_raw = str(publish_block.get("published_at") or "").strip()
        if not published_at_raw:
            continue
        try:
            published_dt = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
            if published_dt.tzinfo is None:
                published_dt = published_dt.astimezone()
        except ValueError:
            continue
        if published_dt < cutoff:
            continue
        outcomes = payload.get("outcomes") if isinstance(payload.get("outcomes"), list) else []
        executed.append({
            "flow": payload.get("flow") or path.stem.split("_")[0],
            "run_id": payload.get("run_id"),
            "receipt_path": str(path),
            "published_at": published_at_raw,
            "post_id": publish_block.get("post_id"),
            "platform": publish_block.get("platform"),
            "outcome_status": status,
            "outcomes": list(outcomes),
            # Theme + title may live on the underlying receipt or be
            # implied from the flow. Inspector promotes queued→executed
            # by fuzzy match, so we expose what's available verbatim.
            "theme": payload.get("theme") or None,
            "title": payload.get("title") or None,
        })
    return {"count": len(executed), "window_days": window_days, "receipts": executed}


def build_current_learnings_payload() -> dict[str, Any]:
    social_payload = load_json(SOCIAL_ROLLUPS_PATH, {})
    competitor_market_payload = load_json(COMPETITOR_BENCHMARK_PATH, {})
    competitor_social_payload = load_json(COMPETITOR_SOCIAL_BENCHMARK_PATH, {})
    competitor_social_snapshots_payload = load_json(COMPETITOR_SOCIAL_SNAPSHOTS_PATH, {})
    # 2026-06-05: root-cause fix for the morning ordering race.
    # current_learnings runs at ~07:10; weekly_strategy_packet
    # refresh ran at ~07:20. The 10-min lag meant slot_outcomes
    # reflected pre-refresh state. If we detect that the packet is
    # older than the live social posts file, regenerate it inline
    # so slot_outcomes is consistent with the live posts before we
    # read it. Verify-at-emit (2026-06-04) is kept as defense-in-
    # depth.
    weekly_strategy_packet_payload = _refresh_weekly_strategy_packet_if_stale()
    # 2026-06-04: live posts file consulted at emit time to verify
    # weekly_strategy_slot_missed claims against actual observed
    # posts. Suppresses false positives caused by stale packet
    # snapshots.
    live_posts_payload = load_json(SOCIAL_POSTS_PATH, {})
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
    if not isinstance(live_posts_payload, dict):
        live_posts_payload = {}
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
        live_posts_payload=live_posts_payload,
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
        # Phase 5 Step 5: executed-experiment summary the Inspector
        # uses to (a) replace its hardcoded 0 in counts, (b) promote
        # matching queued_experiments to "executed," (c) shrink the
        # "0 executed" signal_gap as receipts land.
        "executed_experiments_last_14d": _executed_experiments_from_receipts(window_days=14),
        "paths": {
            "social_rollups": str(SOCIAL_ROLLUPS_PATH),
            "social_posts": str(SOCIAL_POSTS_PATH),
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
                f"- Execution truth label: `{summary.get('weekly_strategy_execution_truth_label') or 'unknown'}`",
                f"- Lane guidance: `ready_to_scale={summary.get('weekly_strategy_lane_ready_to_scale_count') or 0}`, `keep_anchor={summary.get('weekly_strategy_lane_keep_anchor_count') or 0}`, `fallback_only={summary.get('weekly_strategy_lane_fallback_only_count') or 0}`, `experiment_only={summary.get('weekly_strategy_lane_experiment_only_count') or 0}`, `pull_back={summary.get('weekly_strategy_lane_pull_back_count') or 0}`",
                "",
            ]
        )
        if summary.get("weekly_strategy_execution_truth_note"):
            lines.append(f"- Execution truth: {summary.get('weekly_strategy_execution_truth_note')}")
            lines.append("")
        lane_guidance = weekly_strategy_feedback.get("lane_guidance") or []
        if lane_guidance:
            lines.append("### Lane Guidance")
            lines.append("")
            for item in lane_guidance[:5]:
                lines.append(
                    f"- `{item.get('lane') or 'unknown'}` | `{item.get('decision') or 'unknown'}` | {_compact_text(item.get('title')) or _compact_text(item.get('summary')) or 'No summary.'}"
                )
                if item.get("summary"):
                    lines.append(f"  Summary: {item.get('summary')}")
                if item.get("evidence"):
                    lines.append(f"  Evidence: {item.get('evidence')}")
                if item.get("recommended_action"):
                    lines.append(f"  Recommended action: {item.get('recommended_action')}")
            lines.append("")
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
        slot_feedback_items = weekly_strategy_feedback.get("slot_feedback_items") or []
        if slot_feedback_items:
            lines.append("### Slot Feedback")
            lines.append("")
            for item in slot_feedback_items:
                calendar_parts = [item.get("calendar_label"), item.get("calendar_date")]
                calendar_label = " | ".join(part for part in calendar_parts if part)
                heading = item.get("slot") or "Weekly slot"
                if calendar_label:
                    heading = f"{heading} ({calendar_label})"
                lines.append(
                    f"- `{heading}` | `{item.get('tracking_status') or 'unknown'}` | {item.get('recommended_action') or 'Review the slot outcome.'}"
                )
                if item.get("evidence"):
                    lines.append(f"  Evidence: {item.get('evidence')}")
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
