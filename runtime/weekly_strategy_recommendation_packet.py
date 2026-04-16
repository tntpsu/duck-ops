from __future__ import annotations

import argparse
from collections import Counter
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, load_json, now_local_iso, write_json, write_markdown


SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
COMPETITOR_SOCIAL_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_benchmark.json"
COMPETITOR_SOCIAL_SNAPSHOTS_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshots.json"
COMPETITOR_SOCIAL_SNAPSHOT_HISTORY_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshot_history.json"
CURRENT_LEARNINGS_PATH = DUCK_OPS_ROOT / "state" / "current_learnings.json"
PACKET_STATE_PATH = DUCK_OPS_ROOT / "state" / "weekly_strategy_recommendation_packet.json"
PACKET_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "weekly_strategy_recommendation_packet.json"
PACKET_MD_PATH = OUTPUT_OPERATOR_DIR / "weekly_strategy_recommendation_packet.md"


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _humanize_label(value: Any) -> str:
    text = _compact_text(value).replace("_", " ")
    if not text:
        return ""
    return " ".join(part if part.isupper() else part.capitalize() for part in text.split())


def _recent_snapshot_stability(history_payload: dict[str, Any], *, lookback: int = 5) -> dict[str, Any]:
    snapshots = [item for item in (history_payload.get("snapshots") or []) if isinstance(item, dict)][-lookback:]
    top_theme_counts: Counter[str] = Counter(
        _compact_text(item.get("top_theme")).lower() for item in snapshots if _compact_text(item.get("top_theme"))
    )
    top_account_counts: Counter[str] = Counter(
        _compact_text(item.get("top_account")).lower() for item in snapshots if _compact_text(item.get("top_account"))
    )
    stable_threshold = 3 if len(snapshots) >= 3 else len(snapshots)
    top_theme, top_theme_count = top_theme_counts.most_common(1)[0] if top_theme_counts else ("", 0)
    top_account, top_account_count = top_account_counts.most_common(1)[0] if top_account_counts else ("", 0)
    return {
        "recent_snapshot_count": len(snapshots),
        "stable_threshold": stable_threshold,
        "stable_top_theme": top_theme if top_theme and top_theme_count >= stable_threshold else None,
        "stable_top_theme_count": top_theme_count,
        "stable_top_account": top_account if top_account and top_account_count >= stable_threshold else None,
        "stable_top_account_count": top_account_count,
    }


def _competitor_account_labels(snapshot_payload: dict[str, Any]) -> set[str]:
    labels: set[str] = set()
    for item in snapshot_payload.get("profiles") or []:
        if not isinstance(item, dict):
            continue
        for raw in (
            item.get("account_handle"),
            item.get("display_name"),
            item.get("brand_key"),
            item.get("full_name"),
        ):
            text = _compact_text(raw).lower()
            if not text:
                continue
            labels.add(text)
            labels.add(text.replace(" ", ""))
            labels.add(text.replace("_", " "))
            labels.add(text.replace(".", ""))
    return labels


def _meaningful_theme_row(benchmark_payload: dict[str, Any], snapshot_payload: dict[str, Any]) -> dict[str, Any] | None:
    excluded_labels = _competitor_account_labels(snapshot_payload)
    for item in benchmark_payload.get("by_theme") or []:
        if not isinstance(item, dict):
            continue
        label = _compact_text(item.get("label")).lower()
        if not label:
            continue
        normalized = label.replace(" ", "").replace(".", "")
        if label in excluded_labels or normalized in excluded_labels:
            continue
        return item
    return None


def _own_signal_quality(social_payload: dict[str, Any]) -> tuple[str, str]:
    summary = social_payload.get("summary") or {}
    post_count = int(summary.get("post_count") or 0)
    coverage = _safe_float(summary.get("metrics_coverage_pct")) or 0.0
    if post_count >= 12 and coverage >= 80:
        return "medium", "Own-post coverage is broad enough to support directional weekly strategy calls."
    if post_count >= 4 and coverage >= 60:
        return "low_medium", "Own-post coverage is usable, but still narrow enough that we should avoid big changes."
    return "low", "Own-post coverage is still sparse, so recommendations should stay small and experimental."


def _competitor_signal_quality(benchmark_payload: dict[str, Any], snapshot_payload: dict[str, Any]) -> tuple[str, str]:
    benchmark_summary = benchmark_payload.get("summary") or {}
    snapshot_summary = snapshot_payload.get("summary") or {}
    post_count = int(benchmark_summary.get("post_count") or 0)
    live_accounts = int(snapshot_summary.get("live_account_count") or 0)
    cached_accounts = int(snapshot_summary.get("cached_account_count") or 0)
    hard_failures = int(snapshot_summary.get("failed_account_count") or 0)
    degraded_fetches = int(snapshot_summary.get("degraded_account_count") or 0)
    scheduled_skip_accounts = int(snapshot_summary.get("scheduled_skip_account_count") or snapshot_summary.get("scheduled_skip_count") or 0)
    profile_only_backoff_accounts = int(snapshot_summary.get("profile_only_backoff_account_count") or 0)
    live_canary_limited_accounts = int(snapshot_summary.get("live_canary_limited_account_count") or 0)
    live_canary_target_count = int(snapshot_summary.get("live_canary_target_count") or 0)
    if post_count >= 40 and live_accounts >= 4 and hard_failures == 0:
        return "medium", "Competitor social coverage is healthy enough to influence what we test next."
    if hard_failures == 0 and degraded_fetches == 0 and live_canary_limited_accounts > 0 and post_count >= 24:
        return (
            "medium",
            f"Competitor social coverage is being protected by a live canary policy: {live_canary_target_count} target(s) refreshed live while {live_canary_limited_accounts} account(s) reused cache.",
        )
    if hard_failures == 0 and degraded_fetches == 0 and profile_only_backoff_accounts == 0 and scheduled_skip_accounts > 0 and post_count >= 24:
        return "medium", "Competitor social coverage is on a staggered cadence, but the snapshot remains healthy enough for bounded weekly tests."
    if post_count >= 24 and (live_accounts >= 2 or cached_accounts >= 2):
        if profile_only_backoff_accounts > 0:
            return (
                "low_medium",
                f"Competitor social coverage is usable, but {profile_only_backoff_accounts} account(s) are on profile-only backoff because recent public refreshes still could not recover post timelines.",
            )
        return "low_medium", "Competitor social coverage is useful, but some of it is coming from cached fallback rather than fresh pulls."
    return "low", "Competitor social coverage is too degraded to drive more than one or two bounded experiments."


def _top_competitor_hook_row(competitor_social_payload: dict[str, Any]) -> dict[str, Any] | None:
    for item in competitor_social_payload.get("by_hook_family") or []:
        if not isinstance(item, dict):
            continue
        if _compact_text(item.get("label")):
            return item
    return None


def _stable_patterns(
    social_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
    snapshot_payload: dict[str, Any],
    snapshot_history_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    patterns: list[dict[str, Any]] = []
    competitor_signal = _competitor_signal_quality(competitor_social_payload, snapshot_payload)
    strongest_workflow = ((social_payload.get("rollups") or {}).get("by_workflow") or [{}])[0]
    strongest_workflow_label = _compact_text(strongest_workflow.get("label"))
    stability = _recent_snapshot_stability(snapshot_history_payload)

    best_window = ((social_payload.get("rollups") or {}).get("by_time_window") or [{}])[0]
    if _compact_text(best_window.get("label")):
        patterns.append(
            {
                "priority": "P1",
                "category": "stable_pattern",
                "signal_type": "own_timing",
                "title": f"`{best_window.get('label')}` is still the default test window",
                "recommendation": f"Keep this as the default slot for this week unless a post-specific reason forces another time.",
                "evidence": f"{best_window.get('post_count') or 0} observed posts with average score {best_window.get('avg_engagement_score') or 0}.",
                "confidence": _own_signal_quality(social_payload)[0],
            }
        )

    if strongest_workflow_label:
        patterns.append(
            {
                "priority": "P1",
                "category": "stable_pattern",
                "signal_type": "own_workflow",
                "title": f"`{strongest_workflow_label}` remains the safest anchor workflow",
                "recommendation": f"Keep one `{strongest_workflow_label}` post in this week’s mix before changing the content split broadly.",
                "evidence": f"{strongest_workflow.get('post_count') or 0} observed posts with average score {strongest_workflow.get('avg_engagement_score') or 0}.",
                "confidence": _own_signal_quality(social_payload)[0],
            }
        )

    stable_top_account = _compact_text(stability.get("stable_top_account"))
    if stable_top_account:
        patterns.append(
            {
                "priority": "P2",
                "category": "stable_pattern",
                "signal_type": "competitor_watch_account",
                "title": f"`{stable_top_account}` is the current competitor watch account",
                "recommendation": f"Use `{stable_top_account}` as the main reference account when we need fresh hook/style inspiration this week.",
                "evidence": (
                    f"`{stable_top_account}` held the top-account slot across "
                    f"{stability.get('stable_top_account_count') or 0} of the last {stability.get('recent_snapshot_count') or 0} competitor snapshots."
                ),
                "confidence": competitor_signal[0],
            }
        )

    return patterns[:4]


def _experimental_ideas(
    social_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
    snapshot_payload: dict[str, Any],
    snapshot_history_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    experiments: list[dict[str, Any]] = []
    competitor_signal = _competitor_signal_quality(competitor_social_payload, snapshot_payload)
    strongest_workflow = ((social_payload.get("rollups") or {}).get("by_workflow") or [{}])[0]
    strongest_workflow_label = _compact_text(strongest_workflow.get("label"))
    stability = _recent_snapshot_stability(snapshot_history_payload)
    stable_top_account = _compact_text(stability.get("stable_top_account"))

    if stable_top_account:
        experiments.append(
            {
                "priority": "P2",
                "category": "experimental_idea",
                "signal_type": "competitor_watch_account",
                "watch_account": stable_top_account,
                "title": f"Borrow one bounded hook from `{stable_top_account}`",
                "recommendation": (
                    f"Review the last few hooks and formats from `{stable_top_account}` before drafting one bounded post test. "
                    "Borrow structure and pacing, not exact copy."
                ),
                "evidence": (
                    f"`{stable_top_account}` held the top-account slot across "
                    f"{stability.get('stable_top_account_count') or 0} of the last {stability.get('recent_snapshot_count') or 0} competitor snapshots."
                ),
                "confidence": competitor_signal[0],
            }
        )

    meaningful_theme = _meaningful_theme_row(competitor_social_payload, snapshot_payload)
    if meaningful_theme and strongest_workflow_label:
        theme_label = _compact_text(meaningful_theme.get("label"))
        experiments.append(
            {
                "priority": "P2",
                "category": "experimental_idea",
                "signal_type": "competitor_theme",
                "theme_label": theme_label,
                "title": f"Stage one `{theme_label}`-leaning test inside `{strongest_workflow_label}`",
                "recommendation": (
                    f"Use `{theme_label}` as the concept input, but keep the execution in our existing `{strongest_workflow_label}` lane "
                    "instead of changing the whole content rhythm."
                ),
                "evidence": (
                    f"`{theme_label}` appeared in {meaningful_theme.get('post_count') or 0} competitor posts with average visible score "
                    f"{meaningful_theme.get('avg_engagement_score') or 0}."
                ),
                "confidence": competitor_signal[0],
            }
        )

    top_format = ((competitor_social_payload.get("by_format") or [{}])[0])
    total_competitor_posts = int((competitor_social_payload.get("summary") or {}).get("post_count") or 0)
    dominant_format_label = _compact_text(top_format.get("label"))
    dominant_format_count = int(top_format.get("post_count") or 0)
    if dominant_format_label and total_competitor_posts > 0 and dominant_format_count / max(1, total_competitor_posts) >= 0.5:
        experiments.append(
            {
                "priority": "P2",
                "category": "experimental_idea",
                "signal_type": "competitor_format",
                "format_label": dominant_format_label,
                "title": f"Keep one bounded `{dominant_format_label}` test on this week’s board",
                "recommendation": (
                    f"Do one small `{dominant_format_label}` experiment this week, but keep it isolated to a single post until our own signal set gets bigger."
                ),
                "evidence": (
                    f"`{dominant_format_label}` accounts for {dominant_format_count} of {total_competitor_posts} competitor posts in the current benchmark."
                ),
                "confidence": competitor_signal[0],
            }
        )

    top_hook = _top_competitor_hook_row(competitor_social_payload)
    if top_hook and _compact_text(top_hook.get("label")):
        hook_label = _compact_text(top_hook.get("label"))
        experiments.append(
            {
                "priority": "P3",
                "category": "experimental_idea",
                "signal_type": "competitor_hook",
                "hook_label": hook_label,
                "title": f"Test one `{hook_label}` hook without changing the whole caption style",
                "recommendation": f"Use `{hook_label}` as a single caption-opening experiment this week, but keep the rest of the post in our usual voice.",
                "evidence": f"`{hook_label}` appeared in {top_hook.get('post_count') or 0} competitor posts with average visible score {top_hook.get('avg_engagement_score') or 0}.",
                "confidence": competitor_signal[0],
            }
        )

    return experiments[:4]


def _do_not_copy_patterns(
    social_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
    snapshot_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    guardrails: list[dict[str, Any]] = []
    degraded_accounts = int((snapshot_payload.get("summary") or {}).get("degraded_account_count") or 0)
    hard_failures = int((snapshot_payload.get("summary") or {}).get("failed_account_count") or 0)
    profile_only_backoff_accounts = int((snapshot_payload.get("summary") or {}).get("profile_only_backoff_account_count") or 0)
    live_canary_limited_accounts = int((snapshot_payload.get("summary") or {}).get("live_canary_limited_account_count") or 0)
    if degraded_accounts or hard_failures or profile_only_backoff_accounts or live_canary_limited_accounts:
        guardrails.append(
            {
                "title": "Do not let competitor data rewrite the whole calendar this week",
                "guidance": "Keep competitor learnings as bounded tests only until live own-post coverage and competitor freshness both improve further.",
                "evidence": (
                    f"{degraded_accounts} degraded fetches, {hard_failures} hard failures, "
                    f"{profile_only_backoff_accounts} profile-only backoff accounts, and "
                    f"{live_canary_limited_accounts} canary-limited accounts in the latest snapshot."
                ),
            }
        )

    top_format = ((competitor_social_payload.get("by_format") or [{}])[0])
    total_competitor_posts = int((competitor_social_payload.get("summary") or {}).get("post_count") or 0)
    dominant_format_label = _compact_text(top_format.get("label"))
    dominant_format_count = int(top_format.get("post_count") or 0)
    if dominant_format_label and total_competitor_posts > 0 and dominant_format_count / max(1, total_competitor_posts) >= 0.5:
        guardrails.append(
            {
                "title": f"Do not pivot the whole mix to `{dominant_format_label}` just because competitors overuse it",
                "guidance": "Run one controlled format test, but keep the rest of the schedule in the workflows we already execute well.",
                "evidence": f"`{dominant_format_label}` accounts for {dominant_format_count} of {total_competitor_posts} competitor posts in the current benchmark.",
            }
        )

    top_hook = _top_competitor_hook_row(competitor_social_payload)
    if top_hook and _compact_text(top_hook.get("label")) == "statement_showcase":
        guardrails.append(
            {
                "title": "Do not copy polished competitor showcase posts beat-for-beat",
                "guidance": "Lift the pacing or framing, but keep our own tone, product mix, and merchandising boundaries intact.",
                "evidence": f"`statement_showcase` is the most repeated competitor hook family with {top_hook.get('post_count') or 0} posts.",
            }
        )

    best_window = ((social_payload.get("rollups") or {}).get("by_time_window") or [{}])[0]
    if _compact_text(best_window.get("label")) and int(best_window.get("post_count") or 0) < 3:
        guardrails.append(
            {
                "title": "Do not overfit to a tiny own-post sample",
                "guidance": "Keep this week’s plan small because our own post history is still too thin for high-confidence calendar changes.",
                "evidence": f"Current best own-post window `{best_window.get('label')}` is based on only {best_window.get('post_count') or 0} observed posts.",
            }
        )

    return guardrails[:4]


def _workflow_labels(social_payload: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    for item in ((social_payload.get("rollups") or {}).get("by_workflow") or []):
        label = _compact_text((item or {}).get("label"))
        if label and label not in labels:
            labels.append(label)
    return labels


def _preferred_slot_lane(
    *,
    signal_type: str,
    anchor_workflow: str,
    available_workflows: list[str],
    metadata: dict[str, Any],
) -> tuple[str, str, str]:
    workflow_pool = [label for label in available_workflows if label]
    alternate_story_lane = next((label for label in workflow_pool if label != anchor_workflow), anchor_workflow)
    if signal_type in {"stable_pattern", "competitor_watch_account", "competitor_hook"}:
        return anchor_workflow, anchor_workflow, "standard_lane"
    if signal_type == "competitor_theme":
        preferred = next(
            (
                label
                for label in workflow_pool
                if label in {"jeepfact", "thursday", "meme", "review_carousel"} and label != anchor_workflow
            ),
            alternate_story_lane,
        )
        content_family = _compact_text(metadata.get("theme_label")) or "theme_test"
        return preferred or anchor_workflow, content_family, "standard_lane"
    if signal_type == "competitor_format":
        format_label = _compact_text(metadata.get("format_label"))
        if format_label == "carousel" and "review_carousel" in workflow_pool:
            return "review_carousel", "carousel_test", "standard_lane"
        if format_label in {"reel", "video"}:
            return "manual_social_experiment", format_label or "format_test", "manual_test"
        return anchor_workflow, format_label or "format_test", "standard_lane"
    if signal_type == "guardrail":
        return anchor_workflow, "review_guardrail", "review"
    return anchor_workflow, anchor_workflow, "standard_lane"


def _social_plan_slots(
    *,
    anchor_window: str,
    anchor_workflow: str,
    watch_account: str | None,
    available_workflows: list[str],
    experimental_ideas: list[dict[str, Any]],
    do_not_copy_patterns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    lane, content_family, execution_mode = _preferred_slot_lane(
        signal_type="stable_pattern",
        anchor_workflow=anchor_workflow,
        available_workflows=available_workflows,
        metadata={},
    )
    slots: list[dict[str, Any]] = [
        {
            "slot": "Slot 1",
            "timing_hint": f"Early week · {anchor_window}",
            "workflow": lane,
            "suggested_lane": lane,
            "content_family": content_family,
            "execution_mode": execution_mode,
            "goal": "Anchor with the strongest proven workflow",
            "action": f"Run one `{anchor_workflow}` post in the `{anchor_window}` window to keep the week grounded in our best current signal.",
            "why": f"`{anchor_workflow}` in `{anchor_window}` is still the safest combination in our own performance data.",
            "source": "stable_pattern",
        }
    ]

    signal_to_slot = {
        "competitor_watch_account": ("Slot 2", f"Midweek · {anchor_window}", "Competitor-inspired hook test"),
        "competitor_hook": ("Slot 2", f"Midweek · {anchor_window}", "Caption-opening hook test"),
        "competitor_theme": ("Slot 3", f"Late week · {anchor_window}", "Theme experiment"),
        "competitor_format": ("Slot 4", "Weekend / bonus slot", "Format experiment"),
    }
    used_signal_types: set[str] = set()
    for idea in experimental_ideas:
        signal_type = str(idea.get("signal_type") or "").strip()
        if not signal_type or signal_type in used_signal_types:
            continue
        slot_meta = signal_to_slot.get(signal_type)
        if not slot_meta:
            continue
        slot_label, timing_hint, goal = slot_meta
        action = _compact_text(idea.get("recommendation"))
        evidence = _compact_text(idea.get("evidence"))
        if not action:
            continue
        lane, content_family, execution_mode = _preferred_slot_lane(
            signal_type=signal_type,
            anchor_workflow=anchor_workflow,
            available_workflows=available_workflows,
            metadata=idea,
        )
        slot_payload = {
            "slot": slot_label,
            "timing_hint": timing_hint,
            "workflow": lane,
            "suggested_lane": lane,
            "content_family": content_family,
            "execution_mode": execution_mode,
            "goal": goal,
            "action": action,
            "why": evidence,
            "source": signal_type,
        }
        if signal_type == "competitor_watch_account" and watch_account:
            slot_payload["watch_account"] = watch_account
        slots.append(slot_payload)
        used_signal_types.add(signal_type)

    if do_not_copy_patterns:
        first_guardrail = do_not_copy_patterns[0]
        lane, content_family, execution_mode = _preferred_slot_lane(
            signal_type="guardrail",
            anchor_workflow=anchor_workflow,
            available_workflows=available_workflows,
            metadata={},
        )
        slots.append(
            {
                "slot": "Slot 5",
                "timing_hint": "End of week review",
                "workflow": lane,
                "suggested_lane": lane,
                "content_family": content_family,
                "execution_mode": execution_mode,
                "goal": "Review results before changing the calendar",
                "action": _compact_text(first_guardrail.get("guidance")),
                "why": _compact_text(first_guardrail.get("evidence")),
                "source": "guardrail",
            }
        )

    deduped: list[dict[str, Any]] = []
    seen_slots: set[str] = set()
    for item in slots:
        slot_label = str(item.get("slot") or "").strip()
        if not slot_label or slot_label in seen_slots:
            continue
        deduped.append(item)
        seen_slots.add(slot_label)
    return deduped[:5]


def _social_plan(
    social_payload: dict[str, Any],
    stable_patterns: list[dict[str, Any]],
    experimental_ideas: list[dict[str, Any]],
    do_not_copy_patterns: list[dict[str, Any]],
) -> dict[str, Any]:
    best_window = ((social_payload.get("rollups") or {}).get("by_time_window") or [{}])[0]
    strongest_workflow = ((social_payload.get("rollups") or {}).get("by_workflow") or [{}])[0]
    available_workflows = _workflow_labels(social_payload)
    stable_account = next((item for item in stable_patterns if item.get("signal_type") == "competitor_watch_account"), None)
    anchor_window = _compact_text(best_window.get("label")) or "best available window"
    anchor_workflow = _compact_text(strongest_workflow.get("label")) or "best available workflow"
    watch_account = None
    if stable_account:
        title = _compact_text(stable_account.get("title"))
        if title.startswith("`") and "`" in title[1:]:
            watch_account = title.split("`")[1]
    slots = _social_plan_slots(
        anchor_window=anchor_window,
        anchor_workflow=anchor_workflow,
        watch_account=watch_account,
        available_workflows=available_workflows,
        experimental_ideas=experimental_ideas,
        do_not_copy_patterns=do_not_copy_patterns,
    )
    items = [item.get("action") for item in slots if _compact_text(item.get("action"))]
    return {
        "headline": f"Keep `{anchor_workflow}` anchored in `{anchor_window}`, run one or two bounded competitor-inspired tests, and avoid copying competitor styles directly.",
        "anchor_window": anchor_window,
        "anchor_workflow": anchor_workflow,
        "watch_account": watch_account,
        "slot_count": len(slots),
        "slots": slots,
        "items": items[:5],
    }


def _recommendations(
    stable_patterns: list[dict[str, Any]],
    experimental_ideas: list[dict[str, Any]],
    do_not_copy_patterns: list[dict[str, Any]],
    current_learnings_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    recommendations.extend(stable_patterns[:2])
    recommendations.extend(experimental_ideas[:3])
    if do_not_copy_patterns:
        first = do_not_copy_patterns[0]
        recommendations.append(
            {
                "priority": "P2",
                "category": "data_quality",
                "title": _compact_text(first.get("title")),
                "recommendation": _compact_text(first.get("guidance")),
                "evidence": _compact_text(first.get("evidence")),
                "confidence": "high",
            }
        )
    change_count = len(current_learnings_payload.get("changes_since_previous") or [])
    if change_count and len(recommendations) < 6:
        recommendations.append(
            {
                "priority": "P3",
                "category": "monitoring",
                "title": "Review what changed before changing the calendar",
                "recommendation": "Use the current learnings surface to compare this week’s shifts before locking in a new content rhythm.",
                "evidence": f"{change_count} cross-surface learning changes were detected since the previous run.",
                "confidence": "medium",
            }
        )
    return recommendations[:6]


def _watchouts(snapshot_payload: dict[str, Any], social_payload: dict[str, Any]) -> list[str]:
    items: list[str] = []
    profile_only_backoff_accounts = int(((snapshot_payload.get("summary") or {}).get("profile_only_backoff_account_count")) or 0)
    live_canary_limited_accounts = int(((snapshot_payload.get("summary") or {}).get("live_canary_limited_account_count")) or 0)
    if profile_only_backoff_accounts > 0:
        items.append(
            f"{profile_only_backoff_accounts} competitor account(s) are on profile-only backoff, which means some benchmark patterns are being held on older profile-only state until public timelines become recoverable again."
        )
    if live_canary_limited_accounts > 0:
        items.append(
            f"{live_canary_limited_accounts} competitor account(s) were intentionally deferred by the live canary policy, so some freshness was traded for lower rate-limit risk."
        )
    if int(((snapshot_payload.get("summary") or {}).get("cached_account_count")) or 0) > 0:
        items.append("Competitor coverage relied on cached fallback for part of the snapshot, so use it to shape tests rather than major strategy pivots.")
    if int(((social_payload.get("summary") or {}).get("post_count")) or 0) < 4:
        items.append("Own-post history is still sparse enough that the top workflow/window signals could swing quickly with a few more posts.")
    if not items:
        items.append("No major watchouts in the current strategy packet.")
    return items[:3]


def build_weekly_strategy_recommendation_packet() -> dict[str, Any]:
    social_payload = load_json(SOCIAL_ROLLUPS_PATH, {})
    competitor_social_payload = load_json(COMPETITOR_SOCIAL_BENCHMARK_PATH, {})
    snapshot_payload = load_json(COMPETITOR_SOCIAL_SNAPSHOTS_PATH, {})
    current_learnings_payload = load_json(CURRENT_LEARNINGS_PATH, {})
    snapshot_history_payload = load_json(COMPETITOR_SOCIAL_SNAPSHOT_HISTORY_PATH, {})
    if not isinstance(social_payload, dict):
        social_payload = {}
    if not isinstance(competitor_social_payload, dict):
        competitor_social_payload = {}
    if not isinstance(snapshot_payload, dict):
        snapshot_payload = {}
    if not isinstance(current_learnings_payload, dict):
        current_learnings_payload = {}
    if not isinstance(snapshot_history_payload, dict):
        snapshot_history_payload = {}

    own_signal = _own_signal_quality(social_payload)
    competitor_signal = _competitor_signal_quality(competitor_social_payload, snapshot_payload)
    stability = _recent_snapshot_stability(snapshot_history_payload)
    stable_patterns = _stable_patterns(
        social_payload,
        competitor_social_payload,
        snapshot_payload,
        snapshot_history_payload,
    )
    experimental_ideas = _experimental_ideas(
        social_payload,
        competitor_social_payload,
        snapshot_payload,
        snapshot_history_payload,
    )
    do_not_copy_patterns = _do_not_copy_patterns(
        social_payload,
        competitor_social_payload,
        snapshot_payload,
    )
    social_plan = _social_plan(
        social_payload,
        stable_patterns,
        experimental_ideas,
        do_not_copy_patterns,
    )
    stability_note = "Competitor history is still too short to call any pattern stable."
    if _compact_text(stability.get("stable_top_account")):
        stability_note = (
            f"`{_compact_text(stability.get('stable_top_account'))}` stayed on top across "
            f"{stability.get('stable_top_account_count') or 0} of the last {stability.get('recent_snapshot_count') or 0} snapshots."
        )
    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Weekly strategy packet built from own-post performance and competitor social learnings.",
            "own_signal_confidence": own_signal[0],
            "own_signal_note": own_signal[1],
            "competitor_signal_confidence": competitor_signal[0],
            "competitor_signal_note": competitor_signal[1],
            "competitor_stability_note": stability_note,
            "stable_pattern_count": len(stable_patterns),
            "experimental_idea_count": len(experimental_ideas),
            "do_not_copy_count": len(do_not_copy_patterns),
            "social_plan_item_count": len(social_plan.get("items") or []),
            "recommendation_count": 0,
            "watchout_count": 0,
        },
        "stable_patterns": stable_patterns,
        "experimental_ideas": experimental_ideas,
        "do_not_copy_patterns": do_not_copy_patterns,
        "social_plan": social_plan,
        "recommendations": _recommendations(
            stable_patterns,
            experimental_ideas,
            do_not_copy_patterns,
            current_learnings_payload,
        ),
        "watchouts": _watchouts(snapshot_payload, social_payload),
        "source_paths": {
            "social_rollups": str(SOCIAL_ROLLUPS_PATH),
            "competitor_social_benchmark": str(COMPETITOR_SOCIAL_BENCHMARK_PATH),
            "competitor_social_snapshots": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
            "competitor_social_snapshot_history": str(COMPETITOR_SOCIAL_SNAPSHOT_HISTORY_PATH),
            "current_learnings": str(CURRENT_LEARNINGS_PATH),
        },
    }
    payload["summary"]["recommendation_count"] = len(payload["recommendations"])
    payload["summary"]["watchout_count"] = len(payload["watchouts"])
    write_json(PACKET_STATE_PATH, payload)
    write_json(PACKET_OPERATOR_JSON_PATH, payload)
    write_markdown(PACKET_MD_PATH, render_weekly_strategy_recommendation_packet_markdown(payload))
    return payload


def render_weekly_strategy_recommendation_packet_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# Weekly Strategy Recommendation Packet",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Own signal confidence: `{summary.get('own_signal_confidence') or 'unknown'}`",
        f"- Competitor signal confidence: `{summary.get('competitor_signal_confidence') or 'unknown'}`",
        "",
        str(summary.get("headline") or ""),
        "",
        f"Own-signal note: {summary.get('own_signal_note') or ''}",
        "",
        f"Competitor-signal note: {summary.get('competitor_signal_note') or ''}",
        "",
        f"Competitor-stability note: {summary.get('competitor_stability_note') or ''}",
        "",
        "## This Week's Social Plan",
        "",
    ]

    social_plan = payload.get("social_plan") or {}
    if not social_plan:
        lines.append("No weekly social plan is staged yet.")
        lines.append("")
    else:
        lines.append(f"- Headline: {social_plan.get('headline')}")
        lines.append(f"- Anchor window: `{social_plan.get('anchor_window') or 'unknown'}`")
        lines.append(f"- Anchor workflow: `{social_plan.get('anchor_workflow') or 'unknown'}`")
        if social_plan.get("watch_account"):
            lines.append(f"- Watch account: `{social_plan.get('watch_account')}`")
        slots = social_plan.get("slots") or []
        if slots:
            lines.append("- Suggested slots:")
            for item in slots[:5]:
                lines.append(
                    f"  - {item.get('slot')}: {item.get('timing_hint')} | {item.get('goal')}"
                )
                lines.append(f"    Action: {item.get('action')}")
                if item.get("workflow"):
                    lines.append(f"    Workflow: `{item.get('workflow')}`")
                if item.get("suggested_lane"):
                    lines.append(f"    Lane: `{item.get('suggested_lane')}`")
                if item.get("content_family"):
                    lines.append(f"    Family: `{item.get('content_family')}`")
                if item.get("execution_mode"):
                    lines.append(f"    Mode: `{item.get('execution_mode')}`")
                if item.get("watch_account"):
                    lines.append(f"    Watch: `{item.get('watch_account')}`")
                if item.get("why"):
                    lines.append(f"    Why: {item.get('why')}")
        else:
            items = social_plan.get("items") or []
            if items:
                lines.append("- Plan items:")
                for item in items[:5]:
                    lines.append(f"  - {item}")
        lines.append("")

    lines.extend(["## Stable Competitor Patterns", ""])
    stable_patterns = payload.get("stable_patterns") or []
    if not stable_patterns:
        lines.append("No stable patterns are available yet.")
        lines.append("")
    else:
        for item in stable_patterns:
            lines.extend(
                [
                    f"### {item.get('priority')} · {item.get('title')}",
                    "",
                    f"- Signal: `{item.get('signal_type')}`",
                    f"- Confidence: `{item.get('confidence')}`",
                    f"- Keep doing: {item.get('recommendation')}",
                    f"- Evidence: {item.get('evidence')}",
                    "",
                ]
            )

    lines.extend(["## Experimental Ideas", ""])
    experiments = payload.get("experimental_ideas") or []
    if not experiments:
        lines.append("No experimental ideas are staged yet.")
        lines.append("")
    else:
        for item in experiments:
            lines.extend(
                [
                    f"### {item.get('priority')} · {item.get('title')}",
                    "",
                    f"- Signal: `{item.get('signal_type')}`",
                    f"- Confidence: `{item.get('confidence')}`",
                    f"- Test: {item.get('recommendation')}",
                    f"- Evidence: {item.get('evidence')}",
                    "",
                ]
            )

    lines.extend(["## Do Not Copy", ""])
    guardrails = payload.get("do_not_copy_patterns") or []
    if not guardrails:
        lines.append("No explicit do-not-copy guardrails are staged yet.")
        lines.append("")
    else:
        for item in guardrails:
            lines.append(f"- {item.get('title')}")
            lines.append(f"  Guidance: {item.get('guidance')}")
            lines.append(f"  Evidence: {item.get('evidence')}")
        lines.append("")

    lines.extend(["## Recommended Moves", ""])
    recommendations = payload.get("recommendations") or []
    if not recommendations:
        lines.append("No weekly strategy moves are staged yet.")
        lines.append("")
    else:
        for item in recommendations:
            lines.extend(
                [
                    f"### {item.get('priority')} · {item.get('title')}",
                    "",
                    f"- Category: `{item.get('category')}`",
                    f"- Confidence: `{item.get('confidence')}`",
                    f"- Recommendation: {item.get('recommendation')}",
                    f"- Evidence: {item.get('evidence')}",
                    "",
                ]
            )

    lines.extend(["## Watchouts", ""])
    for item in payload.get("watchouts") or []:
        lines.append(f"- {item}")
    if not (payload.get("watchouts") or []):
        lines.append("No watchouts right now.")
    lines.append("")

    lines.extend(["## Source Paths", ""])
    for label, path in (payload.get("source_paths") or {}).items():
        lines.append(f"- `{label}`: {path}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the weekly strategy recommendation packet.")
    parser.parse_args()
    payload = build_weekly_strategy_recommendation_packet()
    print(
        {
            "generated_at": payload.get("generated_at"),
            "recommendation_count": len(payload.get("recommendations") or []),
            "watchout_count": len(payload.get("watchouts") or []),
        }
    )


if __name__ == "__main__":
    main()
