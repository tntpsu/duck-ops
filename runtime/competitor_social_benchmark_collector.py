from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, load_json, now_local_iso, write_json, write_markdown


SNAPSHOT_STATE_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshots.json"
BENCHMARK_STATE_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_benchmark.json"
BENCHMARK_HISTORY_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_benchmark_history.json"
BENCHMARK_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "competitor_social_benchmark.json"
BENCHMARK_OUTPUT_MD_PATH = OUTPUT_OPERATOR_DIR / "competitor_social_benchmark.md"
SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _history_snapshots() -> list[dict[str, Any]]:
    payload = load_json(BENCHMARK_HISTORY_PATH, {})
    if isinstance(payload, dict):
        items = payload.get("snapshots")
        return list(items) if isinstance(items, list) else []
    if isinstance(payload, list):
        return payload
    return []


def _save_history(snapshots: list[dict[str, Any]]) -> None:
    write_json(
        BENCHMARK_HISTORY_PATH,
        {
            "generated_at": now_local_iso(),
            "snapshot_count": len(snapshots),
            "snapshots": snapshots,
        },
    )


def _counter_rows(counter: Counter[str], *, limit: int = 8) -> list[dict[str, Any]]:
    return [{"label": label, "count": count} for label, count in counter.most_common(limit) if label]


def _load_own_dimensions() -> dict[str, set[str]]:
    payload = load_json(SOCIAL_ROLLUPS_PATH, {})
    if not isinstance(payload, dict):
        return {"themes": set(), "formats": set(), "time_windows": set(), "workflows": set()}
    rollups = payload.get("rollups") or {}
    return {
        "themes": {
            _compact_text(item.get("label")).lower()
            for item in (rollups.get("by_theme") or [])
            if _compact_text(item.get("label")) and _compact_text(item.get("label")) != "(unknown)"
        },
        "formats": {
            _compact_text(item.get("label")).lower()
            for item in (rollups.get("by_content_type") or [])
            if _compact_text(item.get("label")) and _compact_text(item.get("label")) != "(unknown)"
        },
        "time_windows": {
            _compact_text(item.get("label")).lower()
            for item in (rollups.get("by_time_window") or [])
            if _compact_text(item.get("label")) and _compact_text(item.get("label")) != "(unknown)"
        },
        "workflows": {
            _compact_text(item.get("label")).lower()
            for item in (rollups.get("by_workflow") or [])
            if _compact_text(item.get("label")) and _compact_text(item.get("label")) != "(unknown)"
        },
    }


def _top_accounts(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in posts:
        handle = _compact_text(row.get("account_handle")) or "(unknown)"
        grouped.setdefault(handle, []).append(row)
    rows: list[dict[str, Any]] = []
    for handle, items in grouped.items():
        avg_score = round(sum(float(item.get("engagement_score") or 0.0) for item in items) / max(1, len(items)), 2)
        rows.append({"account_handle": handle, "post_count": len(items), "avg_engagement_score": avg_score})
    rows.sort(key=lambda item: (-float(item.get("avg_engagement_score") or 0.0), -int(item.get("post_count") or 0), str(item.get("account_handle") or "")))
    return rows[:8]


def _top_dimensions(posts: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    score_totals: dict[str, float] = {}
    for row in posts:
        label = _compact_text(row.get(key)).lower()
        if not label:
            continue
        counter[label] += 1
        score_totals[label] = score_totals.get(label, 0.0) + float(row.get("engagement_score") or 0.0)
    rows = []
    for label, count in counter.most_common(8):
        rows.append(
            {
                "label": label,
                "post_count": count,
                "avg_engagement_score": round(score_totals.get(label, 0.0) / max(1, count), 2),
            }
        )
    return rows


def _current_learnings(posts: list[dict[str, Any]], own_dimensions: dict[str, set[str]]) -> list[dict[str, Any]]:
    learnings: list[dict[str, Any]] = []
    top_accounts = _top_accounts(posts)
    top_formats = _top_dimensions(posts, "post_format")
    top_themes = _top_dimensions(posts, "theme")
    top_hooks = _top_dimensions(posts, "hook_family")

    if top_accounts:
        item = top_accounts[0]
        learnings.append(
            {
                "headline": f"`{item.get('account_handle')}` is the strongest visible competitor account in the current snapshot.",
                "confidence": "medium",
                "evidence": f"{item.get('post_count')} posts observed with average visible score {item.get('avg_engagement_score')}.",
                "recommendation": "Inspect this account’s recent hooks and formats before planning the next social experiments.",
            }
        )
    if top_formats:
        item = top_formats[0]
        learnings.append(
            {
                "headline": f"`{item.get('label')}` is the most repeated competitor format right now.",
                "confidence": "medium",
                "evidence": f"{item.get('post_count')} competitor posts in this format, average visible score {item.get('avg_engagement_score')}.",
                "recommendation": "Use this as a test input, not a mandate; compare against our own best-performing format before changing cadence.",
            }
        )
    if top_themes:
        item = top_themes[0]
        theme = _compact_text(item.get("label")).lower()
        recommendation = "This theme already appears in our own social history, so compare execution quality rather than just topic coverage."
        if theme not in own_dimensions.get("themes", set()):
            recommendation = "We look underrepresented in this theme; stage a low-risk test instead of copying exact posts."
        learnings.append(
            {
                "headline": f"`{item.get('label')}` is the strongest repeated competitor theme in the snapshot.",
                "confidence": "medium",
                "evidence": f"{item.get('post_count')} posts observed with average visible score {item.get('avg_engagement_score')}.",
                "recommendation": recommendation,
            }
        )
    if top_hooks:
        item = top_hooks[0]
        learnings.append(
            {
                "headline": f"`{item.get('label')}` is the most common competitor hook family right now.",
                "confidence": "low_medium",
                "evidence": f"{item.get('post_count')} posts share this hook pattern.",
                "recommendation": "Use this as experiment input for caption/hook tests, but keep brand voice distinct.",
            }
        )
    return learnings[:6]


def _ideas_to_test(posts: list[dict[str, Any]], own_dimensions: dict[str, set[str]]) -> list[str]:
    ideas: list[str] = []
    top_themes = _top_dimensions(posts, "theme")
    top_formats = _top_dimensions(posts, "post_format")
    top_hooks = _top_dimensions(posts, "hook_family")

    for row in top_themes[:3]:
        label = _compact_text(row.get("label")).lower()
        if label and label not in own_dimensions.get("themes", set()):
            ideas.append(f"Test one `{label}`-themed post in a format we already execute well.")
    for row in top_formats[:2]:
        label = _compact_text(row.get("label")).lower()
        if label and label not in own_dimensions.get("formats", set()):
            ideas.append(f"Validate whether `{label}` is worth adding to our mix with one bounded experiment.")
    for row in top_hooks[:2]:
        label = _compact_text(row.get("label")).lower()
        if label:
            ideas.append(f"Try one `{label}` caption opening while keeping product/theme constant.")
    deduped: list[str] = []
    seen: set[str] = set()
    for item in ideas:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped[:6]


def _changes_since_previous(current: dict[str, Any], previous: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not previous:
        return []
    changes: list[dict[str, Any]] = []
    current_top_account = ((current.get("top_accounts") or [{}])[0]).get("account_handle")
    prev_top_account = previous.get("top_account")
    if current_top_account and current_top_account != prev_top_account:
        changes.append({"headline": f"Top competitor account changed from `{prev_top_account}` to `{current_top_account}`."})
    current_top_theme = ((current.get("by_theme") or [{}])[0]).get("label")
    prev_top_theme = previous.get("top_theme")
    if current_top_theme and current_top_theme != prev_top_theme:
        changes.append({"headline": f"Top competitor theme changed from `{prev_top_theme}` to `{current_top_theme}`."})
    current_post_count = int(((current.get("summary") or {}).get("post_count")) or 0)
    prev_post_count = int(previous.get("post_count") or 0)
    if current_post_count != prev_post_count:
        changes.append({"headline": f"Observed competitor social post count changed by `{current_post_count - prev_post_count}`."})
    return changes


def build_competitor_social_benchmark() -> dict[str, Any]:
    snapshots = load_json(SNAPSHOT_STATE_PATH, {})
    posts = list((snapshots.get("posts") or [])) if isinstance(snapshots, dict) else []
    own_dimensions = _load_own_dimensions()

    top_accounts = _top_accounts(posts)
    by_theme = _top_dimensions(posts, "theme")
    by_format = _top_dimensions(posts, "post_format")
    by_hook = _top_dimensions(posts, "hook_family")
    by_time = _top_dimensions(posts, "hour_bucket")

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Compare competitor social patterns against our own post history without mixing first-party and competitor truth.",
            "post_count": len(posts),
            "account_count": len({str((row or {}).get('account_handle') or '') for row in posts if isinstance(row, dict)}),
            "data_quality_note": "Competitor data is based on public visible signals and should be treated as directional.",
        },
        "top_accounts": top_accounts,
        "by_theme": by_theme,
        "by_format": by_format,
        "by_hook_family": by_hook,
        "by_time_window": by_time,
        "current_learnings": _current_learnings(posts, own_dimensions),
        "ideas_to_test": _ideas_to_test(posts, own_dimensions),
        "paths": {
            "snapshots": str(SNAPSHOT_STATE_PATH),
            "own_rollups": str(SOCIAL_ROLLUPS_PATH),
        },
    }

    history = _history_snapshots()
    previous = history[-1] if history else None
    payload["changes_since_previous"] = _changes_since_previous(payload, previous)
    history.append(
        {
            "generated_at": payload.get("generated_at"),
            "top_account": (top_accounts[0] if top_accounts else {}).get("account_handle"),
            "top_theme": (by_theme[0] if by_theme else {}).get("label"),
            "post_count": len(posts),
        }
    )
    history = history[-12:]

    write_json(BENCHMARK_STATE_PATH, payload)
    write_json(BENCHMARK_OPERATOR_JSON_PATH, payload)
    write_markdown(BENCHMARK_OUTPUT_MD_PATH, render_competitor_social_benchmark_markdown(payload))
    _save_history(history)
    return payload


def render_competitor_social_benchmark_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# Competitor Social Benchmark",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Competitor posts observed: `{summary.get('post_count') or 0}`",
        f"- Competitor accounts observed: `{summary.get('account_count') or 0}`",
        "",
        str(summary.get("headline") or ""),
        "",
        str(summary.get("data_quality_note") or ""),
        "",
    ]

    for heading, rows_key in [
        ("Top Accounts", "top_accounts"),
        ("Top Themes", "by_theme"),
        ("Top Formats", "by_format"),
        ("Top Hook Families", "by_hook_family"),
        ("Top Time Windows", "by_time_window"),
    ]:
        lines.extend([f"## {heading}", ""])
        rows = payload.get(rows_key) or []
        if not rows:
            lines.append("No rows yet.")
        else:
            for row in rows:
                label = row.get("account_handle") or row.get("label")
                lines.append(
                    f"- `{label}`: `{row.get('post_count')}` posts | avg visible score `{row.get('avg_engagement_score')}`"
                )
        lines.append("")

    lines.extend(["## Current Learnings", ""])
    learnings = payload.get("current_learnings") or []
    if not learnings:
        lines.append("No learnings yet.")
        lines.append("")
    else:
        for item in learnings:
            lines.extend(
                [
                    f"### {item.get('headline')}",
                    "",
                    f"- Confidence: `{item.get('confidence')}`",
                    f"- Evidence: {item.get('evidence')}",
                    f"- Recommendation: {item.get('recommendation')}",
                    "",
                ]
            )

    lines.extend(["## Ideas Worth Testing", ""])
    ideas = payload.get("ideas_to_test") or []
    if not ideas:
        lines.append("No ideas staged yet.")
    else:
        for idea in ideas:
            lines.append(f"- {idea}")
    lines.append("")

    lines.extend(["## What Changed", ""])
    changes = payload.get("changes_since_previous") or []
    if not changes:
        lines.append("No major benchmark changes detected yet.")
    else:
        for item in changes:
            lines.append(f"- {item.get('headline')}")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the competitor social benchmark from public snapshot state.")
    parser.parse_args()
    payload = build_competitor_social_benchmark()
    print(
        {
            "generated_at": payload.get("generated_at"),
            "post_count": ((payload.get("summary") or {}).get("post_count")),
            "idea_count": len(payload.get("ideas_to_test") or []),
        }
    )


if __name__ == "__main__":
    main()
