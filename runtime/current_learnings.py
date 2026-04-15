from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, load_json, now_local_iso, write_json, write_markdown


SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
COMPETITOR_BENCHMARK_PATH = DUCK_OPS_ROOT / "state" / "social_competitor_benchmark.json"
CURRENT_LEARNINGS_STATE_PATH = DUCK_OPS_ROOT / "state" / "current_learnings.json"
CURRENT_LEARNINGS_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.json"
CURRENT_LEARNINGS_MD_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.md"


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _current_beliefs(social_payload: dict[str, Any], competitor_payload: dict[str, Any]) -> list[dict[str, Any]]:
    beliefs: list[dict[str, Any]] = []
    for item in social_payload.get("current_learnings") or []:
        if isinstance(item, dict):
            beliefs.append({"source": "own_social", **item})
    for item in competitor_payload.get("market_learnings") or []:
        if isinstance(item, dict):
            beliefs.append({"source": "competitor_market", **item})
    return beliefs[:8]


def _best_windows(social_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return list(((social_payload.get("rollups") or {}).get("by_time_window") or []))[:5]


def _strongest_workflows(social_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return list(((social_payload.get("rollups") or {}).get("by_workflow") or []))[:5]


def _changes(social_payload: dict[str, Any], competitor_payload: dict[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in social_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "own_social", **item})
    for item in competitor_payload.get("changes_since_previous") or []:
        if isinstance(item, dict):
            changes.append({"source": "competitor_market", **item})
    return changes


def build_current_learnings_payload() -> dict[str, Any]:
    social_payload = load_json(SOCIAL_ROLLUPS_PATH, {})
    competitor_payload = load_json(COMPETITOR_BENCHMARK_PATH, {})
    if not isinstance(social_payload, dict):
        social_payload = {}
    if not isinstance(competitor_payload, dict):
        competitor_payload = {}

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Current learnings across our own social results and competitor market signals.",
            "social_post_count": int(((social_payload.get("summary") or {}).get("post_count")) or 0),
            "social_metrics_coverage_pct": float(((social_payload.get("summary") or {}).get("metrics_coverage_pct")) or 0.0),
            "competitor_observation_days": int(((competitor_payload.get("summary") or {}).get("observation_days")) or 0),
            "data_quality_note": _compact_text((social_payload.get("summary") or {}).get("data_quality_note"))
            or _compact_text((competitor_payload.get("summary") or {}).get("data_quality_note")),
        },
        "current_beliefs": _current_beliefs(social_payload, competitor_payload),
        "changes_since_previous": _changes(social_payload, competitor_payload),
        "best_windows": _best_windows(social_payload),
        "strongest_workflows": _strongest_workflows(social_payload),
        "top_posts": list(social_payload.get("top_posts") or [])[:5],
        "competitor_motifs": list(competitor_payload.get("emergent_motifs") or [])[:8],
        "ideas_to_test": list(competitor_payload.get("ideas_to_test") or [])[:6],
        "paths": {
            "social_rollups": str(SOCIAL_ROLLUPS_PATH),
            "competitor_benchmark": str(COMPETITOR_BENCHMARK_PATH),
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
        "",
        str(summary.get("headline") or ""),
        "",
        str(summary.get("data_quality_note") or ""),
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
            lines.append(f"- `{item.get('keyword')}` | score `{item.get('score')}` | listings `{item.get('listing_count')}`")
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
