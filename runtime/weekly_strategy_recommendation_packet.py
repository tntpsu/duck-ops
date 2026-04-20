from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta
import math
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, load_json, parse_iso, write_json, write_markdown


SOCIAL_POSTS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_posts.json"
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


def _now_local() -> datetime:
    return datetime.now().astimezone()


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


def _pick_workflow(
    workflow_pool: list[str],
    *,
    preferred_order: list[str] | None = None,
    exclude: set[str] | None = None,
) -> str | None:
    excluded = set(exclude or set())
    if preferred_order:
        for label in preferred_order:
            if label in workflow_pool and label not in excluded:
                return label
    return next((label for label in workflow_pool if label not in excluded), None)


def _preferred_slot_lane(
    *,
    signal_type: str,
    anchor_workflow: str,
    available_workflows: list[str],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    workflow_pool = [label for label in available_workflows if label]
    alternate_story_lane = _pick_workflow(workflow_pool, exclude={anchor_workflow}) or anchor_workflow
    narrative_lane = _pick_workflow(
        workflow_pool,
        preferred_order=["jeepfact", "thursday", "blog", "review_carousel", "meme", "gtdf", "gtdf_winner"],
        exclude={anchor_workflow},
    )
    format_label = _compact_text(metadata.get("format_label"))
    theme_label = _compact_text(metadata.get("theme_label")) or "theme test"

    def build_lane(
        suggested_lane: str,
        content_family: str,
        execution_mode: str,
        lane_fit_strength: str,
        lane_fit_reason: str,
        alternate_lane: str | None = None,
        alternate_lane_reason: str | None = None,
    ) -> dict[str, Any]:
        return {
            "workflow": suggested_lane,
            "suggested_lane": suggested_lane,
            "content_family": content_family,
            "execution_mode": execution_mode,
            "lane_fit_strength": lane_fit_strength,
            "lane_fit_reason": lane_fit_reason,
            "alternate_lane": alternate_lane,
            "alternate_lane_reason": alternate_lane_reason,
        }

    if signal_type in {"stable_pattern", "competitor_watch_account", "competitor_hook"}:
        if signal_type == "stable_pattern":
            return build_lane(
                anchor_workflow,
                anchor_workflow,
                "standard_lane",
                "strong",
                f"`{anchor_workflow}` is still our safest baseline lane, so this slot should protect the strongest own-post signal before we experiment.",
            )
        if signal_type == "competitor_watch_account":
            alternate_lane = narrative_lane if narrative_lane and narrative_lane != anchor_workflow else None
            alternate_reason = (
                f"If the borrowed account pattern needs more story than `{anchor_workflow}` can carry cleanly, move the concept into `{alternate_lane}` instead."
                if alternate_lane
                else None
            )
            return build_lane(
                anchor_workflow,
                anchor_workflow,
                "standard_lane",
                "strong",
                f"This is a bounded competitor-style borrow, so keeping it inside `{anchor_workflow}` lets us test the signal without changing the production lane.",
                alternate_lane,
                alternate_reason,
            )
        alternate_lane = narrative_lane if narrative_lane and narrative_lane != anchor_workflow else None
        alternate_reason = (
            f"If the hook needs more explanation than `{anchor_workflow}` can support, stage the same idea in `{alternate_lane}`."
            if alternate_lane
            else None
        )
        return build_lane(
            anchor_workflow,
            anchor_workflow,
            "standard_lane",
            "strong",
            f"Hook tests are safest in `{anchor_workflow}` because we can borrow the opener without changing the rest of the execution lane.",
            alternate_lane,
            alternate_reason,
        )
    if signal_type == "competitor_theme":
        preferred = narrative_lane or alternate_story_lane or anchor_workflow
        if preferred == anchor_workflow:
            alternate_lane = None
            alternate_reason = None
            if alternate_story_lane and alternate_story_lane != anchor_workflow:
                alternate_lane = alternate_story_lane
                alternate_reason = (
                    f"If the `{theme_label}` concept feels cramped in `{anchor_workflow}`, try `{alternate_lane}` as the next-best story lane."
                )
            return build_lane(
                preferred,
                theme_label,
                "standard_lane",
                "medium",
                f"`{theme_label}` is worth testing, but we do not have a cleaner supported theme lane than `{anchor_workflow}` right now, so keep it bounded.",
                alternate_lane,
                alternate_reason,
            )
        if preferred == "jeepfact":
            reason = f"`{theme_label}` reads more like a story or educational angle than a pure joke, so `jeepfact` is the cleanest supported lane."
        elif preferred == "thursday":
            reason = f"`{theme_label}` fits a feature-style spotlight better than a throwaway meme, so `thursday` is the better lane."
        elif preferred == "review_carousel":
            reason = f"`{theme_label}` will land better with a multi-panel explanation or proof pattern, so `review_carousel` is the strongest supported lane."
        else:
            reason = f"`{theme_label}` needs a slightly richer frame than the anchor slot, so `{preferred}` is the best supported lane for this test."
        alternate_lane = anchor_workflow if anchor_workflow != preferred else None
        alternate_reason = (
            f"If `{theme_label}` feels too forced in `{preferred}`, collapse it back into `{anchor_workflow}` as a lighter test."
            if alternate_lane
            else None
        )
        return build_lane(
            preferred,
            theme_label,
            "standard_lane",
            "medium",
            reason,
            alternate_lane,
            alternate_reason,
        )
    if signal_type == "competitor_format":
        if format_label == "carousel" and "review_carousel" in workflow_pool:
            alternate_lane = anchor_workflow if anchor_workflow != "review_carousel" else None
            alternate_reason = (
                f"If the carousel idea is too thin for a full review carousel, simplify it into `{anchor_workflow}`."
                if alternate_lane
                else None
            )
            return build_lane(
                "review_carousel",
                "carousel_test",
                "standard_lane",
                "medium",
                "`review_carousel` is our closest supported lane for multi-panel proof or sequence-driven ideas.",
                alternate_lane,
                alternate_reason,
            )
        if format_label in {"reel", "video"}:
            alternate_lane = anchor_workflow or alternate_story_lane
            alternate_reason = (
                f"If we want a supported test this week, translate the same hook into `{alternate_lane}` instead of trying to force a true `{format_label}` lane."
                if alternate_lane
                else None
            )
            return build_lane(
                "manual_social_experiment",
                format_label or "format_test",
                "manual_test",
                "manual",
                f"We see a `{format_label}` signal worth watching, but DuckAgent does not have a first-class `{format_label}` lane yet.",
                alternate_lane,
                alternate_reason,
            )
        return build_lane(
            anchor_workflow,
            format_label or "format_test",
            "standard_lane",
            "medium",
            f"The format signal is interesting, but our safest supported way to test it is still inside `{anchor_workflow}`.",
            alternate_story_lane if alternate_story_lane != anchor_workflow else None,
            (
                f"If the format needs a less anchor-like treatment, use `{alternate_story_lane}` as the next-best supported lane."
                if alternate_story_lane and alternate_story_lane != anchor_workflow
                else None
            ),
        )
    if signal_type == "guardrail":
        return build_lane(
            "operator_review",
            "review_guardrail",
            "review",
            "strong",
            "This slot is intentionally a review step, not a publish lane, so we can check what changed before rewriting the calendar.",
        )
    return build_lane(
        anchor_workflow,
        anchor_workflow,
        "standard_lane",
        "medium",
        f"`{anchor_workflow}` is the current default because it is the safest supported lane we have for this signal.",
    )


def _slot_execution_bridge(
    *,
    suggested_lane: str,
    execution_mode: str,
) -> dict[str, Any]:
    scheduled_lanes = {
        "meme": {
            "schedule_reference": "Monday 09:00 scheduled flow",
            "command_hint": "python src/main_agent.py --flow meme --all",
            "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
            "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
            "operator_action_label": "Run Meme Flow",
        },
        "jeepfact": {
            "schedule_reference": "Wednesday 09:00 scheduled flow",
            "command_hint": "python src/main_agent.py --flow jeepfact --all",
            "next_step": "Run the Jeep Fact flow or wait for the scheduled run, then approve the publish step normally.",
            "approval_followthrough": "Reply `publish` to the Jeep Fact review email after the pack looks right.",
            "operator_action_label": "Run Jeep Fact Flow",
        },
        "thursday": {
            "schedule_reference": "Thursday 09:00 scheduled flow",
            "command_hint": "python src/main_agent.py --flow thursday --all",
            "next_step": "Use the Thursday flow and keep the publish step inside the normal approval lane.",
            "approval_followthrough": "Reply `publish` to the Thursday review email after choosing the winner.",
            "operator_action_label": "Run Thursday Flow",
        },
        "gtdf": {
            "schedule_reference": "Thursday 20:00 scheduled flow",
            "command_hint": "python src/main_agent.py --flow gtdf --all",
            "next_step": "Use the GTDF flow and keep the publish step approval-based.",
            "approval_followthrough": "Reply `publish` to the GTDF review email after the post looks right.",
            "operator_action_label": "Run GTDF Flow",
        },
        "gtdf_winner": {
            "schedule_reference": "Sunday 08:00 scheduled flow",
            "command_hint": "python src/main_agent.py --flow gtdf_winner --all --force",
            "next_step": "Use the GTDF winner flow and keep the publish step approval-based.",
            "approval_followthrough": "Reply `publish` to the GTDF winner email after the post looks right.",
            "operator_action_label": "Run GTDF Winner Flow",
        },
        "review_carousel": {
            "schedule_reference": "Approval-driven review carousel lane",
            "command_hint": "approve review_carousel candidate, then reply publish",
            "next_step": "Stage this through the review carousel approval loop rather than a timed scheduled run.",
            "approval_followthrough": "Approve the candidate, then reply `publish` to schedule it.",
            "operator_action_label": "Use Review Carousel Lane",
        },
        "blog": {
            "schedule_reference": "Approval-driven blog social publish lane",
            "command_hint": "python src/main_agent.py --flow blog --all",
            "next_step": "Use the blog flow and keep the publish step inside the existing approval loop.",
            "approval_followthrough": "Reply `publish` to the blog review email after the post looks right.",
            "operator_action_label": "Run Blog Flow",
        },
    }
    if execution_mode == "manual_test" or suggested_lane == "manual_social_experiment":
        return {
            "execution_readiness": "manual_experiment",
            "approval_required": False,
            "schedule_reference": "No first-class recurring lane",
            "command_hint": None,
            "readiness_reason": "This is a manual experiment idea, not a supported DuckAgent publishing lane yet.",
            "next_step": "Treat this as a bounded manual social test if we choose to run it.",
            "approval_followthrough": None,
            "operator_action_label": "Run Manual Test",
        }
    if execution_mode == "review" or suggested_lane == "operator_review":
        return {
            "execution_readiness": "ready_now",
            "approval_required": False,
            "schedule_reference": "Weekly operator review block",
            "command_hint": "review current_learnings + weekly_strategy_recommendation_packet",
            "readiness_reason": "This slot is an operator review step and can be executed immediately with the current desk and learnings surfaces.",
            "next_step": "Use the desk and current learnings to review the week before changing the calendar.",
            "approval_followthrough": None,
            "operator_action_label": "Review Weekly Learnings",
        }
    if suggested_lane in scheduled_lanes:
        lane = scheduled_lanes[suggested_lane]
        return {
            "execution_readiness": "ready_with_approval",
            "approval_required": True,
            "schedule_reference": lane["schedule_reference"],
            "command_hint": lane["command_hint"],
            "readiness_reason": "This slot maps to an existing DuckAgent lane, but the normal review/publish approval boundary still applies.",
            "next_step": lane["next_step"],
            "approval_followthrough": lane["approval_followthrough"],
            "operator_action_label": lane["operator_action_label"],
        }
    return {
        "execution_readiness": "not_supported_yet",
        "approval_required": False,
        "schedule_reference": "No mapped lane",
        "command_hint": None,
        "readiness_reason": "There is not a clean current DuckAgent flow for this slot yet.",
        "next_step": "Keep this as a strategy note until we build a stronger execution bridge.",
        "approval_followthrough": None,
        "operator_action_label": "Not Supported Yet",
    }


def _ready_this_week(slots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ready: list[dict[str, Any]] = []
    for item in slots:
        readiness = str(item.get("execution_readiness") or "").strip()
        if readiness not in {"ready_now", "ready_with_approval"}:
            continue
        ready.append(
            {
                "slot": item.get("slot"),
                "calendar_label": item.get("calendar_label"),
                "suggested_lane": item.get("suggested_lane"),
                "execution_readiness": readiness,
                "approval_required": bool(item.get("approval_required")),
                "operator_action_label": item.get("operator_action_label"),
                "next_step": item.get("next_step"),
                "command_hint": item.get("command_hint"),
                "approval_followthrough": item.get("approval_followthrough"),
                "schedule_reference": item.get("schedule_reference"),
                "lane_fit_strength": item.get("lane_fit_strength"),
                "lane_fit_reason": item.get("lane_fit_reason"),
                "alternate_lane": item.get("alternate_lane"),
                "alternate_lane_reason": item.get("alternate_lane_reason"),
                "tracking_status": item.get("tracking_status"),
                "tracking_note": item.get("tracking_note"),
                "actual_lane": item.get("actual_lane"),
                "actual_platforms": list(item.get("actual_platforms") or []),
                "performance_label": item.get("performance_label"),
                "performance_note": item.get("performance_note"),
            }
        )
    return ready[:5]


def _slot_day_offset(slot_label: str) -> int:
    return {
        "Slot 1": 0,
        "Slot 2": 2,
        "Slot 3": 3,
        "Slot 4": 5,
        "Slot 5": 6,
    }.get(slot_label, 0)


def _slot_target_date(slot_label: str, *, packet_now: datetime) -> datetime.date:
    week_start = (packet_now - timedelta(days=packet_now.weekday())).date()
    return week_start + timedelta(days=_slot_day_offset(slot_label))


def _calendar_target_for_slot(
    *,
    slot_label: str,
    anchor_window: str,
    suggested_lane: str,
    execution_mode: str,
    packet_now: datetime,
) -> dict[str, str]:
    day_by_slot = {
        "Slot 1": "Monday",
        "Slot 2": "Wednesday",
        "Slot 3": "Thursday",
        "Slot 4": "Saturday",
        "Slot 5": "Sunday",
    }
    target_day = day_by_slot.get(slot_label, "This week")
    if execution_mode == "review":
        target_window = "review block"
    else:
        target_window = anchor_window or "best available window"
    calendar_label = f"{target_day} {target_window}".strip()

    if slot_label == "Slot 1" and suggested_lane == "meme":
        cadence_reason = "This lines up with the recurring Meme Monday lane while keeping the stronger evening window in view."
    elif slot_label == "Slot 2":
        cadence_reason = "This is the midweek test slot, so it should not steal focus from the Monday anchor post."
    elif slot_label == "Slot 3":
        cadence_reason = "Late-week tests fit best after the anchor and midweek experiment have already landed."
    elif slot_label == "Slot 4" and execution_mode == "manual_test":
        cadence_reason = "Higher-risk format experiments are safer as bonus weekend tests than as core weekday commitments."
    elif slot_label == "Slot 5":
        cadence_reason = "End the week with a review block before changing the calendar or the lane mix."
    else:
        cadence_reason = "Use this as the suggested weekly rhythm unless a stronger operator reason changes the day."
    calendar_date = _slot_target_date(slot_label, packet_now=packet_now).isoformat()

    return {
        "target_day": target_day,
        "target_window": target_window,
        "calendar_date": calendar_date,
        "calendar_label": calendar_label,
        "cadence_reason": cadence_reason,
    }


def _post_group_summary(workflow: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    representative = max(items, key=lambda item: _safe_float(item.get("engagement_score")) or 0.0)
    timestamps = [parsed for parsed in (parse_iso(item.get("published_at")) for item in items) if parsed is not None]
    return {
        "workflow": workflow,
        "platforms": sorted({str(item.get("platform") or "").strip() for item in items if str(item.get("platform") or "").strip()}),
        "post_count": len(items),
        "published_at": min(timestamps).isoformat() if timestamps else representative.get("published_at"),
        "time_window": _compact_text(representative.get("time_window")) or None,
        "post_id": representative.get("post_id"),
        "title": representative.get("title"),
        "theme": representative.get("theme"),
        "url": representative.get("url"),
        "engagement_score": _safe_float(representative.get("engagement_score")),
        "engagement_rate": _safe_float(representative.get("engagement_rate")),
    }


def _performance_signal(post_group: dict[str, Any], observed_posts: list[dict[str, Any]]) -> dict[str, Any]:
    scored_posts = [
        item
        for item in observed_posts
        if not bool(item.get("is_future_post")) and _safe_float(item.get("engagement_score")) is not None
    ]
    if not scored_posts:
        return {
            "performance_label": "pending",
            "performance_note": "No scored social posts are available yet, so outcome quality is still pending.",
            "performance_rank": None,
            "performance_sample_size": 0,
        }

    target_post_id = str(post_group.get("post_id") or "").strip()
    sorted_posts = sorted(
        scored_posts,
        key=lambda item: (-float(_safe_float(item.get("engagement_score")) or 0.0), str(item.get("published_at") or "")),
    )
    rank = next(
        (
            index
            for index, item in enumerate(sorted_posts, start=1)
            if str(item.get("post_id") or "").strip() == target_post_id
        ),
        None,
    )
    sample_size = len(sorted_posts)
    if rank is None:
        return {
            "performance_label": "pending",
            "performance_note": "The matched post could not be ranked against the current social window yet.",
            "performance_rank": None,
            "performance_sample_size": sample_size,
        }

    if sample_size < 3:
        label = "limited"
        note = (
            f"Only {sample_size} scored post(s) are in the current window, so this result is still directional rather than trustworthy."
        )
    else:
        top_band = max(1, math.ceil(sample_size / 3))
        if rank <= top_band:
            label = "strong"
            note = f"This landed in the top third of the current social window at rank {rank} of {sample_size} observed posts."
        elif rank > sample_size - top_band:
            label = "weak"
            note = f"This landed in the bottom third of the current social window at rank {rank} of {sample_size} observed posts."
        else:
            label = "watch"
            note = f"This landed in the middle of the current social window at rank {rank} of {sample_size} observed posts."

    score = _safe_float(post_group.get("engagement_score"))
    rate = _safe_float(post_group.get("engagement_rate"))
    evidence = []
    if score is not None:
        evidence.append(f"score {round(score, 2)}")
    if rate is not None:
        evidence.append(f"rate {round(rate, 4)}")
    if evidence:
        note = f"{note} Observed {' | '.join(evidence)}."

    return {
        "performance_label": label,
        "performance_note": note,
        "performance_rank": rank,
        "performance_sample_size": sample_size,
    }


def _slot_execution_feedback(
    slot: dict[str, Any],
    *,
    social_posts_payload: dict[str, Any],
    packet_now: datetime,
) -> dict[str, Any]:
    slot_label = str(slot.get("slot") or "").strip()
    target_date = _slot_target_date(slot_label, packet_now=packet_now)
    target_date_text = target_date.isoformat()
    suggested_lane = str(slot.get("suggested_lane") or "").strip()
    alternate_lane = str(slot.get("alternate_lane") or "").strip()
    execution_mode = str(slot.get("execution_mode") or "").strip()

    if execution_mode == "review":
        return {
            "calendar_date": target_date_text,
            "tracking_status": "review_slot",
            "tracking_note": "This slot is intentionally a review checkpoint, so there is no social post to match.",
            "actual_lane": None,
            "actual_platforms": [],
            "actual_post_id": None,
            "actual_published_at": None,
            "actual_time_window": None,
            "actual_post_url": None,
            "performance_label": None,
            "performance_note": None,
            "performance_rank": None,
            "performance_sample_size": 0,
        }

    if target_date > packet_now.date():
        return {
            "calendar_date": target_date_text,
            "tracking_status": "awaiting_slot",
            "tracking_note": f"This slot is scheduled for `{target_date_text}`, so there is no post outcome to evaluate yet.",
            "actual_lane": None,
            "actual_platforms": [],
            "actual_post_id": None,
            "actual_published_at": None,
            "actual_time_window": None,
            "actual_post_url": None,
            "performance_label": None,
            "performance_note": None,
            "performance_rank": None,
            "performance_sample_size": 0,
        }

    posts = [item for item in (social_posts_payload.get("posts") or []) if isinstance(item, dict)]
    observed_same_day = [
        item
        for item in posts
        if str(item.get("published_date") or "").strip() == target_date_text and not bool(item.get("is_future_post"))
    ]
    if not observed_same_day:
        return {
            "calendar_date": target_date_text,
            "tracking_status": "no_post_observed",
            "tracking_note": f"No observed social post was found for the `{target_date_text}` target date yet.",
            "actual_lane": None,
            "actual_platforms": [],
            "actual_post_id": None,
            "actual_published_at": None,
            "actual_time_window": None,
            "actual_post_url": None,
            "performance_label": None,
            "performance_note": None,
            "performance_rank": None,
            "performance_sample_size": 0,
        }

    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in observed_same_day:
        workflow = _compact_text(item.get("workflow"))
        if not workflow:
            continue
        grouped.setdefault(workflow, []).append(item)
    group_summaries = {workflow: _post_group_summary(workflow, items) for workflow, items in grouped.items()}
    if suggested_lane and suggested_lane in group_summaries:
        status = "recommended_lane_executed"
        matched = group_summaries[suggested_lane]
        note = f"The recommended lane `{suggested_lane}` was observed on `{target_date_text}`."
    elif alternate_lane and alternate_lane in group_summaries:
        status = "alternate_lane_executed"
        matched = group_summaries[alternate_lane]
        note = (
            f"The primary lane `{suggested_lane}` did not land, but the planned fallback `{alternate_lane}` was observed on `{target_date_text}`."
        )
    else:
        matched = max(
            group_summaries.values(),
            key=lambda item: (item.get("post_count") or 0, _safe_float(item.get("engagement_score")) or 0.0, str(item.get("published_at") or "")),
        )
        status = "different_lane_executed"
        note = (
            f"A different lane `{matched.get('workflow')}` was observed on `{target_date_text}` instead of `{suggested_lane or 'the planned lane'}`."
        )

    performance = _performance_signal(matched, posts)
    if matched.get("time_window") and str(matched.get("time_window")) != str(slot.get("target_window") or ""):
        note = f"{note} It landed in `{matched.get('time_window')}` instead of `{slot.get('target_window')}`."

    return {
        "calendar_date": target_date_text,
        "tracking_status": status,
        "tracking_note": note,
        "actual_lane": matched.get("workflow"),
        "actual_platforms": list(matched.get("platforms") or []),
        "actual_post_id": matched.get("post_id"),
        "actual_published_at": matched.get("published_at"),
        "actual_time_window": matched.get("time_window"),
        "actual_post_url": matched.get("url"),
        **performance,
    }


def _execution_feedback_summary(slots: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "recommended_lane_executed": sum(1 for item in slots if str(item.get("tracking_status") or "") == "recommended_lane_executed"),
        "alternate_lane_executed": sum(1 for item in slots if str(item.get("tracking_status") or "") == "alternate_lane_executed"),
        "different_lane_executed": sum(1 for item in slots if str(item.get("tracking_status") or "") == "different_lane_executed"),
        "awaiting_slot": sum(1 for item in slots if str(item.get("tracking_status") or "") == "awaiting_slot"),
        "no_post_observed": sum(1 for item in slots if str(item.get("tracking_status") or "") == "no_post_observed"),
        "review_slot": sum(1 for item in slots if str(item.get("tracking_status") or "") == "review_slot"),
    }


def _social_plan_slots(
    *,
    anchor_window: str,
    anchor_workflow: str,
    watch_account: str | None,
    available_workflows: list[str],
    experimental_ideas: list[dict[str, Any]],
    do_not_copy_patterns: list[dict[str, Any]],
    social_posts_payload: dict[str, Any],
    packet_now: datetime,
) -> list[dict[str, Any]]:
    lane_choice = _preferred_slot_lane(
        signal_type="stable_pattern",
        anchor_workflow=anchor_workflow,
        available_workflows=available_workflows,
        metadata={},
    )
    slots: list[dict[str, Any]] = [
        {
            "slot": "Slot 1",
            "timing_hint": f"Early week · {anchor_window}",
            "goal": "Anchor with the strongest proven workflow",
            "action": f"Run one `{anchor_workflow}` post in the `{anchor_window}` window to keep the week grounded in our best current signal.",
            "why": f"`{anchor_workflow}` in `{anchor_window}` is still the safest combination in our own performance data.",
            "source": "stable_pattern",
        }
    ]
    slots[0].update(lane_choice)
    slots[0].update(
        _calendar_target_for_slot(
            slot_label="Slot 1",
            anchor_window=anchor_window,
            suggested_lane=str(lane_choice.get("suggested_lane") or ""),
            execution_mode=str(lane_choice.get("execution_mode") or ""),
            packet_now=packet_now,
        )
    )
    slots[0].update(
        _slot_execution_bridge(
            suggested_lane=str(lane_choice.get("suggested_lane") or ""),
            execution_mode=str(lane_choice.get("execution_mode") or ""),
        )
    )

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
        lane_choice = _preferred_slot_lane(
            signal_type=signal_type,
            anchor_workflow=anchor_workflow,
            available_workflows=available_workflows,
            metadata=idea,
        )
        slot_payload = {
            "slot": slot_label,
            "timing_hint": timing_hint,
            "goal": goal,
            "action": action,
            "why": evidence,
            "source": signal_type,
        }
        slot_payload.update(lane_choice)
        slot_payload.update(
            _calendar_target_for_slot(
                slot_label=slot_label,
                anchor_window=anchor_window,
                suggested_lane=str(lane_choice.get("suggested_lane") or ""),
                execution_mode=str(lane_choice.get("execution_mode") or ""),
                packet_now=packet_now,
            )
        )
        slot_payload.update(
            _slot_execution_bridge(
                suggested_lane=str(lane_choice.get("suggested_lane") or ""),
                execution_mode=str(lane_choice.get("execution_mode") or ""),
            )
        )
        if signal_type == "competitor_watch_account" and watch_account:
            slot_payload["watch_account"] = watch_account
        slots.append(slot_payload)
        used_signal_types.add(signal_type)

    if do_not_copy_patterns:
        first_guardrail = do_not_copy_patterns[0]
        lane_choice = _preferred_slot_lane(
            signal_type="guardrail",
            anchor_workflow=anchor_workflow,
            available_workflows=available_workflows,
            metadata={},
        )
        slots.append(
            {
                "slot": "Slot 5",
                "timing_hint": "End of week review",
                "goal": "Review results before changing the calendar",
                "action": _compact_text(first_guardrail.get("guidance")),
                "why": _compact_text(first_guardrail.get("evidence")),
                "source": "guardrail",
            }
        )
        slots[-1].update(lane_choice)
        slots[-1].update(
            _calendar_target_for_slot(
                slot_label="Slot 5",
                anchor_window=anchor_window,
                suggested_lane=str(lane_choice.get("suggested_lane") or ""),
                execution_mode=str(lane_choice.get("execution_mode") or ""),
                packet_now=packet_now,
            )
        )
        slots[-1].update(
            _slot_execution_bridge(
                suggested_lane=str(lane_choice.get("suggested_lane") or ""),
                execution_mode=str(lane_choice.get("execution_mode") or ""),
            )
        )

    deduped: list[dict[str, Any]] = []
    seen_slots: set[str] = set()
    for item in slots:
        slot_label = str(item.get("slot") or "").strip()
        if not slot_label or slot_label in seen_slots:
            continue
        deduped.append(item)
        seen_slots.add(slot_label)
    deduped = deduped[:5]
    for item in deduped:
        item.update(_slot_execution_feedback(item, social_posts_payload=social_posts_payload, packet_now=packet_now))
    return deduped


def _social_plan(
    social_payload: dict[str, Any],
    social_posts_payload: dict[str, Any],
    stable_patterns: list[dict[str, Any]],
    experimental_ideas: list[dict[str, Any]],
    do_not_copy_patterns: list[dict[str, Any]],
    *,
    packet_now: datetime,
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
        social_posts_payload=social_posts_payload,
        packet_now=packet_now,
    )
    items = [item.get("action") for item in slots if _compact_text(item.get("action"))]
    ready_this_week = _ready_this_week(slots)
    execution_feedback = _execution_feedback_summary(slots)
    readiness_counts = {
        "ready_now": sum(1 for item in slots if str(item.get("execution_readiness") or "") == "ready_now"),
        "ready_with_approval": sum(1 for item in slots if str(item.get("execution_readiness") or "") == "ready_with_approval"),
        "manual_experiment": sum(1 for item in slots if str(item.get("execution_readiness") or "") == "manual_experiment"),
        "not_supported_yet": sum(1 for item in slots if str(item.get("execution_readiness") or "") == "not_supported_yet"),
    }
    return {
        "headline": f"Keep `{anchor_workflow}` anchored in `{anchor_window}`, run one or two bounded competitor-inspired tests, and avoid copying competitor styles directly.",
        "anchor_window": anchor_window,
        "anchor_workflow": anchor_workflow,
        "watch_account": watch_account,
        "slot_count": len(slots),
        "readiness_counts": readiness_counts,
        "execution_feedback": execution_feedback,
        "ready_this_week": ready_this_week,
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


def _change_focus(current_learnings_payload: dict[str, Any]) -> list[dict[str, Any]]:
    notifier = (
        current_learnings_payload.get("change_notifier")
        if isinstance(current_learnings_payload.get("change_notifier"), dict)
        else {}
    )
    items: list[dict[str, Any]] = []
    for item in list(notifier.get("items") or [])[:4]:
        if not isinstance(item, dict):
            continue
        headline = _compact_text(item.get("headline"))
        if not headline:
            continue
        items.append(
            {
                "urgency": _compact_text(item.get("urgency")) or "opportunity",
                "source": _compact_text(item.get("source")) or "learning",
                "kind": _compact_text(item.get("kind")) or None,
                "headline": headline,
                "detail": _compact_text(item.get("detail")) or None,
            }
        )
    return items


def build_weekly_strategy_recommendation_packet() -> dict[str, Any]:
    packet_now = _now_local()
    generated_at = packet_now.isoformat()
    social_posts_payload = load_json(SOCIAL_POSTS_PATH, {})
    social_payload = load_json(SOCIAL_ROLLUPS_PATH, {})
    competitor_social_payload = load_json(COMPETITOR_SOCIAL_BENCHMARK_PATH, {})
    snapshot_payload = load_json(COMPETITOR_SOCIAL_SNAPSHOTS_PATH, {})
    current_learnings_payload = load_json(CURRENT_LEARNINGS_PATH, {})
    snapshot_history_payload = load_json(COMPETITOR_SOCIAL_SNAPSHOT_HISTORY_PATH, {})
    if not isinstance(social_posts_payload, dict):
        social_posts_payload = {}
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
        social_posts_payload,
        stable_patterns,
        experimental_ideas,
        do_not_copy_patterns,
        packet_now=packet_now,
    )
    stability_note = "Competitor history is still too short to call any pattern stable."
    if _compact_text(stability.get("stable_top_account")):
        stability_note = (
            f"`{_compact_text(stability.get('stable_top_account'))}` stayed on top across "
            f"{stability.get('stable_top_account_count') or 0} of the last {stability.get('recent_snapshot_count') or 0} snapshots."
        )
    payload = {
        "generated_at": generated_at,
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
            "change_focus_count": 0,
        },
        "stable_patterns": stable_patterns,
        "experimental_ideas": experimental_ideas,
        "do_not_copy_patterns": do_not_copy_patterns,
        "change_focus": _change_focus(current_learnings_payload),
        "social_plan": social_plan,
        "recommendations": _recommendations(
            stable_patterns,
            experimental_ideas,
            do_not_copy_patterns,
            current_learnings_payload,
        ),
        "watchouts": _watchouts(snapshot_payload, social_payload),
        "source_paths": {
            "social_posts": str(SOCIAL_POSTS_PATH),
            "social_rollups": str(SOCIAL_ROLLUPS_PATH),
            "competitor_social_benchmark": str(COMPETITOR_SOCIAL_BENCHMARK_PATH),
            "competitor_social_snapshots": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
            "competitor_social_snapshot_history": str(COMPETITOR_SOCIAL_SNAPSHOT_HISTORY_PATH),
            "current_learnings": str(CURRENT_LEARNINGS_PATH),
        },
    }
    payload["summary"]["recommendation_count"] = len(payload["recommendations"])
    payload["summary"]["watchout_count"] = len(payload["watchouts"])
    payload["summary"]["change_focus_count"] = len(payload.get("change_focus") or [])
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
        f"- Learning changes carried in: `{summary.get('change_focus_count') or 0}`",
        "",
        str(summary.get("headline") or ""),
        "",
        f"Own-signal note: {summary.get('own_signal_note') or ''}",
        "",
        f"Competitor-signal note: {summary.get('competitor_signal_note') or ''}",
        "",
        f"Competitor-stability note: {summary.get('competitor_stability_note') or ''}",
        "",
        "## What Changed Since The Last Learning Snapshot",
        "",
    ]

    change_focus = payload.get("change_focus") or []
    if not change_focus:
        lines.append("No material learning changes need to be folded into this week’s plan right now.")
        lines.append("")
    else:
        for item in change_focus[:4]:
            lines.append(f"- `{item.get('urgency') or 'opportunity'}` · {item.get('headline')}")
            if item.get("detail"):
                lines.append(f"  Detail: {item.get('detail')}")
        lines.append("")

    lines.extend([
        "## This Week's Social Plan",
        "",
    ])

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
        readiness_counts = social_plan.get("readiness_counts") or {}
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
                if item.get("calendar_date"):
                    lines.append(f"    Date: `{item.get('calendar_date')}`")
                if item.get("calendar_label"):
                    lines.append(f"    Calendar: `{item.get('calendar_label')}`")
                if item.get("cadence_reason"):
                    lines.append(f"    Cadence: {item.get('cadence_reason')}")
                if item.get("lane_fit_strength"):
                    lines.append(f"    Fit: `{item.get('lane_fit_strength')}`")
                if item.get("lane_fit_reason"):
                    lines.append(f"    Lane reason: {item.get('lane_fit_reason')}")
                if item.get("execution_readiness"):
                    lines.append(f"    Readiness: `{item.get('execution_readiness')}`")
                if item.get("schedule_reference"):
                    lines.append(f"    Schedule: {item.get('schedule_reference')}")
                if item.get("operator_action_label"):
                    lines.append(f"    Use: {item.get('operator_action_label')}")
                if item.get("command_hint"):
                    lines.append(f"    Hint: `{item.get('command_hint')}`")
                if item.get("approval_followthrough"):
                    lines.append(f"    Then: {item.get('approval_followthrough')}")
                if item.get("next_step"):
                    lines.append(f"    Next: {item.get('next_step')}")
                if item.get("watch_account"):
                    lines.append(f"    Watch: `{item.get('watch_account')}`")
                if item.get("alternate_lane"):
                    lines.append(f"    Alternate: `{item.get('alternate_lane')}`")
                if item.get("alternate_lane_reason"):
                    lines.append(f"    Alternate reason: {item.get('alternate_lane_reason')}")
                if item.get("tracking_status"):
                    lines.append(f"    Outcome: `{item.get('tracking_status')}`")
                if item.get("tracking_note"):
                    lines.append(f"    Outcome note: {item.get('tracking_note')}")
                if item.get("actual_lane"):
                    lines.append(f"    Actual lane: `{item.get('actual_lane')}`")
                if item.get("actual_platforms"):
                    lines.append(f"    Platforms: `{', '.join(item.get('actual_platforms') or [])}`")
                if item.get("performance_label"):
                    lines.append(f"    Performance: `{item.get('performance_label')}`")
                if item.get("performance_note"):
                    lines.append(f"    Performance note: {item.get('performance_note')}")
                if item.get("why"):
                    lines.append(f"    Why: {item.get('why')}")
        else:
            items = social_plan.get("items") or []
            if items:
                lines.append("- Plan items:")
                for item in items[:5]:
                    lines.append(f"  - {item}")
        lines.append("")

    lines.extend(["## Ready This Week", ""])
    ready_this_week = social_plan.get("ready_this_week") or []
    if not ready_this_week:
        lines.append("No slots map cleanly to current executable lanes this week.")
        lines.append("")
    else:
        for item in ready_this_week[:5]:
            lines.append(
                f"- {item.get('slot')}: `{item.get('calendar_label') or 'this week'}` | `{item.get('suggested_lane') or 'unknown'}` | `{item.get('execution_readiness')}`"
            )
            if item.get("schedule_reference"):
                lines.append(f"  Schedule: {item.get('schedule_reference')}")
            if item.get("operator_action_label"):
                lines.append(f"  Use: {item.get('operator_action_label')}")
            if item.get("next_step"):
                lines.append(f"  Next: {item.get('next_step')}")
            if item.get("command_hint"):
                lines.append(f"  Hint: `{item.get('command_hint')}`")
            if item.get("approval_followthrough"):
                lines.append(f"  Then: {item.get('approval_followthrough')}")
            if item.get("lane_fit_strength"):
                lines.append(f"  Fit: `{item.get('lane_fit_strength')}`")
            if item.get("lane_fit_reason"):
                lines.append(f"  Lane reason: {item.get('lane_fit_reason')}")
            if item.get("alternate_lane"):
                lines.append(f"  Alternate: `{item.get('alternate_lane')}`")
            if item.get("alternate_lane_reason"):
                lines.append(f"  Alternate reason: {item.get('alternate_lane_reason')}")
            if item.get("tracking_status"):
                lines.append(f"  Outcome: `{item.get('tracking_status')}`")
            if item.get("tracking_note"):
                lines.append(f"  Outcome note: {item.get('tracking_note')}")
            if item.get("actual_lane"):
                lines.append(f"  Actual lane: `{item.get('actual_lane')}`")
            if item.get("performance_label"):
                lines.append(f"  Performance: `{item.get('performance_label')}`")
            if item.get("performance_note"):
                lines.append(f"  Performance note: {item.get('performance_note')}")
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
