from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, age_hours, load_json, now_local_iso, write_json, write_markdown


SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
COMPETITOR_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "social_competitor_benchmark.json"
COMPETITOR_SOCIAL_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_benchmark.json"
COMPETITOR_SOCIAL_SNAPSHOTS_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshots.json"
CURRENT_LEARNINGS_STATE_PATH = DUCK_OPS_ROOT / "state" / "current_learnings.json"
CURRENT_LEARNINGS_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.json"
CURRENT_LEARNINGS_MD_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.md"


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
    elif cached_account_count > 0 or degraded_account_count > 0:
        if degraded_account_count > 0:
            freshness_label = "cached"
            freshness_note = (
                f"Cached fallback truth: {cached_account_count} account(s) used cached data and "
                f"{degraded_account_count} account(s) had degraded fetches."
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
        "competitor_social_active_refresh_target_count": active_refresh_target_count,
        "competitor_social_freshness_label": freshness_label,
        "competitor_social_freshness_note": freshness_note,
    }


def _current_beliefs(
    social_payload: dict[str, Any],
    competitor_market_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    beliefs: list[dict[str, Any]] = []
    for item in social_payload.get("current_learnings") or []:
        if isinstance(item, dict):
            beliefs.append({"source": "own_social", **item})
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
    competitor_market_payload: dict[str, Any],
    competitor_social_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in social_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "own_social", **item})
    for item in competitor_market_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "competitor_market", **item})
    for item in competitor_social_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "competitor_social", **item})
    return changes


def build_current_learnings_payload() -> dict[str, Any]:
    social_payload = load_json(SOCIAL_ROLLUPS_PATH, {})
    competitor_market_payload = load_json(COMPETITOR_BENCHMARK_PATH, {})
    competitor_social_payload = load_json(COMPETITOR_SOCIAL_BENCHMARK_PATH, {})
    competitor_social_snapshots_payload = load_json(COMPETITOR_SOCIAL_SNAPSHOTS_PATH, {})
    if not isinstance(social_payload, dict):
        social_payload = {}
    if not isinstance(competitor_market_payload, dict):
        competitor_market_payload = {}
    if not isinstance(competitor_social_payload, dict):
        competitor_social_payload = {}
    if not isinstance(competitor_social_snapshots_payload, dict):
        competitor_social_snapshots_payload = {}

    competitor_social_freshness = _competitor_social_freshness(competitor_social_snapshots_payload)

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Current learnings across our own social results, competitor market signals, and competitor social snapshots.",
            "social_post_count": int(((social_payload.get("summary") or {}).get("post_count")) or 0),
            "social_metrics_coverage_pct": float(((social_payload.get("summary") or {}).get("metrics_coverage_pct")) or 0.0),
            "competitor_observation_days": int(((competitor_market_payload.get("summary") or {}).get("observation_days")) or 0),
            "competitor_social_post_count": int(((competitor_social_payload.get("summary") or {}).get("post_count")) or 0),
            **competitor_social_freshness,
            "data_quality_note": _compact_text((social_payload.get("summary") or {}).get("data_quality_note"))
            or _compact_text((competitor_social_payload.get("summary") or {}).get("data_quality_note"))
            or _compact_text((competitor_social_snapshots_payload.get("summary") or {}).get("data_quality_note"))
            or _compact_text((competitor_market_payload.get("summary") or {}).get("data_quality_note")),
        },
        "current_beliefs": _current_beliefs(social_payload, competitor_market_payload, competitor_social_payload),
        "changes_since_previous": _changes(social_payload, competitor_market_payload, competitor_social_payload),
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
        },
    }
    return payload


def render_current_learnings_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
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
        f"- Active refresh targets: `{summary.get('competitor_social_active_refresh_target_count') or 0}`",
        f"- Truth: {summary.get('competitor_social_freshness_note') or 'No competitor social snapshot is available yet.'}",
        "",
        "## What Changed",
        "",
    ]

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
