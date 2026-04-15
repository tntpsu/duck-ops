from __future__ import annotations

import argparse
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, load_json, now_local_iso, write_json, write_markdown


SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
COMPETITOR_SOCIAL_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_benchmark.json"
COMPETITOR_SOCIAL_SNAPSHOTS_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshots.json"
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
    if post_count >= 40 and live_accounts >= 4 and hard_failures == 0:
        return "medium", "Competitor social coverage is healthy enough to influence what we test next."
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


def _recommendations(
    social_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
    snapshot_payload: dict[str, Any],
    current_learnings_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []

    best_window = ((social_payload.get("rollups") or {}).get("by_time_window") or [{}])[0]
    if _compact_text(best_window.get("label")):
        recommendations.append(
            {
                "priority": "P1",
                "category": "timing",
                "title": f"Keep testing the `{best_window.get('label')}` posting window",
                "recommendation": f"Schedule at least one post in `{best_window.get('label')}` this week before changing the posting calendar broadly.",
                "evidence": f"{best_window.get('post_count') or 0} observed posts with average score {best_window.get('avg_engagement_score') or 0}.",
                "confidence": _own_signal_quality(social_payload)[0],
            }
        )

    strongest_workflow = ((social_payload.get("rollups") or {}).get("by_workflow") or [{}])[0]
    if _compact_text(strongest_workflow.get("label")):
        recommendations.append(
            {
                "priority": "P1",
                "category": "workflow",
                "title": f"Keep `{strongest_workflow.get('label')}` in the mix",
                "recommendation": f"Use `{strongest_workflow.get('label')}` as one of this week’s scheduled posts while receipt coverage is still growing.",
                "evidence": f"{strongest_workflow.get('post_count') or 0} observed posts with average score {strongest_workflow.get('avg_engagement_score') or 0}.",
                "confidence": _own_signal_quality(social_payload)[0],
            }
        )

    competitor_ideas = list(competitor_social_payload.get("ideas_to_test") or [])
    for idea in competitor_ideas[:2]:
        recommendations.append(
            {
                "priority": "P2",
                "category": "competitor_test",
                "title": "Run one bounded competitor-inspired test",
                "recommendation": str(idea),
                "evidence": _compact_text(((competitor_social_payload.get("current_learnings") or [{}])[0]).get("evidence")) or "Competitor benchmark shows repeated patterns worth testing.",
                "confidence": _competitor_signal_quality(competitor_social_payload, snapshot_payload)[0],
            }
        )

    degraded_accounts = int((snapshot_payload.get("summary") or {}).get("degraded_account_count") or 0)
    hard_failures = int((snapshot_payload.get("summary") or {}).get("failed_account_count") or 0)
    profile_only_backoff_accounts = int((snapshot_payload.get("summary") or {}).get("profile_only_backoff_account_count") or 0)
    if degraded_accounts or hard_failures or profile_only_backoff_accounts:
        recommendations.append(
            {
                "priority": "P2",
                "category": "data_quality",
                "title": "Treat competitor learnings as directional this week",
                "recommendation": "Use competitor social patterns to guide small tests only; do not make big strategy changes until fresh live pulls improve again.",
                "evidence": (
                    f"{degraded_accounts} degraded competitor account fetches, {hard_failures} hard failures, and "
                    f"{profile_only_backoff_accounts} profile-only backoff account(s) in the latest snapshot."
                ),
                "confidence": "high",
            }
        )

    change_count = len(current_learnings_payload.get("changes_since_previous") or [])
    if change_count:
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
    if profile_only_backoff_accounts > 0:
        items.append(
            f"{profile_only_backoff_accounts} competitor account(s) are on profile-only backoff, which means some benchmark patterns are being held on older profile-only state until public timelines become recoverable again."
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
    if not isinstance(social_payload, dict):
        social_payload = {}
    if not isinstance(competitor_social_payload, dict):
        competitor_social_payload = {}
    if not isinstance(snapshot_payload, dict):
        snapshot_payload = {}
    if not isinstance(current_learnings_payload, dict):
        current_learnings_payload = {}

    own_signal = _own_signal_quality(social_payload)
    competitor_signal = _competitor_signal_quality(competitor_social_payload, snapshot_payload)
    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Weekly strategy packet built from own-post performance and competitor social learnings.",
            "own_signal_confidence": own_signal[0],
            "own_signal_note": own_signal[1],
            "competitor_signal_confidence": competitor_signal[0],
            "competitor_signal_note": competitor_signal[1],
            "recommendation_count": 0,
            "watchout_count": 0,
        },
        "recommendations": _recommendations(
            social_payload,
            competitor_social_payload,
            snapshot_payload,
            current_learnings_payload,
        ),
        "watchouts": _watchouts(snapshot_payload, social_payload),
        "source_paths": {
            "social_rollups": str(SOCIAL_ROLLUPS_PATH),
            "competitor_social_benchmark": str(COMPETITOR_SOCIAL_BENCHMARK_PATH),
            "competitor_social_snapshots": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
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
        "## Recommended Moves",
        "",
    ]

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
