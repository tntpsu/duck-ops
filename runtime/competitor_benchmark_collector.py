from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_AGENT_ROOT, DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, now_local_iso, write_json, write_markdown


COMPETITOR_STATE_PATH = DUCK_OPS_ROOT / "state" / "social_competitor_benchmark.json"
COMPETITOR_HISTORY_PATH = DUCK_OPS_ROOT / "state" / "social_competitor_benchmark_history.json"
COMPETITOR_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "competitor_benchmark.json"
COMPETITOR_OUTPUT_MD_PATH = OUTPUT_OPERATOR_DIR / "competitor_benchmark.md"
SOCIAL_ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
COMPETITOR_RUNS_GLOB = str(DUCK_AGENT_ROOT / "runs" / "*" / "state_competitor.json")
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9']+")

STOPWORDS = {
    "3d",
    "printed",
    "print",
    "duck",
    "ducks",
    "ducking",
    "figurine",
    "figurines",
    "rubber",
    "cute",
    "gift",
    "gifts",
    "lover",
    "lovers",
    "novelty",
    "toy",
    "toys",
    "jeep",
    "jeeps",
    "duckduckjeep",
    "printedduck",
    "collectible",
    "collectibles",
    "funny",
    "custom",
    "with",
    "for",
    "and",
    "the",
    "your",
    "from",
    "that",
    "this",
    "you",
    "pla",
    "plastic",
}


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _parse_date(value: Any) -> date | None:
    text = _compact_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_dt(value: Any) -> datetime | None:
    text = _compact_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.astimezone()


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _history_snapshots() -> list[dict[str, Any]]:
    payload = _load_json(COMPETITOR_HISTORY_PATH, {})
    if isinstance(payload, dict):
        items = payload.get("snapshots")
        return list(items) if isinstance(items, list) else []
    if isinstance(payload, list):
        return payload
    return []


def _save_history(snapshots: list[dict[str, Any]]) -> None:
    write_json(
        COMPETITOR_HISTORY_PATH,
        {
            "generated_at": now_local_iso(),
            "snapshot_count": len(snapshots),
            "snapshots": snapshots,
        },
    )


def _competitor_state_paths() -> list[Path]:
    return sorted(Path(path) for path in Path(DUCK_AGENT_ROOT / "runs").glob("*/state_competitor.json"))


def _load_competitor_reports(*, window_days: int, now: datetime | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    current = now or datetime.now().astimezone()
    cutoff = current.date() - timedelta(days=window_days)
    reports: list[dict[str, Any]] = []
    malformed: list[str] = []
    scanned = 0

    for path in _competitor_state_paths():
        scanned += 1
        payload = _load_json(path, {})
        if not isinstance(payload, dict):
            malformed.append(str(path))
            continue
        report = payload.get("competitor_report")
        if not isinstance(report, dict):
            continue
        report_date = _parse_date(report.get("report_date"))
        if report_date is None or report_date < cutoff:
            continue
        reports.append(
            {
                "path": str(path),
                "run_id": path.parent.name,
                "report_date": report_date.isoformat(),
                "report": report,
            }
        )

    reports.sort(key=lambda item: str(item.get("report_date") or ""))
    summary = {
        "generated_at": current.isoformat(),
        "window_days": window_days,
        "observation_days": len(reports),
        "scanned_run_count": scanned,
        "malformed_count": len(malformed),
        "malformed_paths": malformed[:20],
    }
    return reports, summary


def _title_tokens(title: str) -> list[str]:
    tokens: list[str] = []
    for raw in TOKEN_PATTERN.findall(title.lower()):
        token = raw.strip("'")
        if len(token) < 3 or token in STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _weighted_motif_rows(listing_snapshots: list[dict[str, Any]], *, current_date: date) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    listing_counts: Counter[str] = Counter()
    recent_cutoff = current_date - timedelta(days=7)
    for item in listing_snapshots:
        if not isinstance(item, dict):
            continue
        created_at = _parse_dt(item.get("created_ts"))
        created_date = created_at.date() if created_at else current_date
        if created_date < recent_cutoff:
            continue
        title = _compact_text(item.get("title"))
        tokens = set(_title_tokens(title))
        tags = {
            token
            for raw_tag in (item.get("tags") or [])
            for token in _title_tokens(str(raw_tag))
        }
        merged = tokens | tags
        if not merged:
            continue
        views = _safe_int(item.get("views")) or 0
        favorites = _safe_int(item.get("num_favorers")) or 0
        weight = 1 + min(views // 250, 10) + min(favorites // 25, 10)
        for token in merged:
            counter[token] += max(1, weight)
            listing_counts[token] += 1

    rows = [
        {"keyword": token, "score": score, "listing_count": listing_counts[token]}
        for token, score in counter.items()
    ]
    rows.sort(key=lambda item: (-int(item.get("score") or 0), -int(item.get("listing_count") or 0), str(item.get("keyword") or "")))
    return rows[:12]


def _shop_rows(shop_snapshots: list[dict[str, Any]], *, sort_key: str) -> list[dict[str, Any]]:
    rows = []
    for item in shop_snapshots:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "shop_name": _compact_text(item.get("shop_name")) or "(unknown)",
                "shop_id": _compact_text(item.get("shop_id")) or None,
                "momentum_score": round(_safe_float(item.get("momentum_score")) or 0.0, 2),
                "growth_rate": round(_safe_float(item.get("growth_rate")) or 0.0, 2),
                "listing_active_count": _safe_int(item.get("listing_active_count")) or 0,
                "transaction_sold_count": _safe_int(item.get("transaction_sold_count")) or 0,
            }
        )
    rows.sort(key=lambda item: (-float(item.get(sort_key) or 0), -int(item.get("listing_active_count") or 0), str(item.get("shop_name") or "")))
    return rows[:6]


def _load_social_theme_context() -> set[str]:
    payload = _load_json(SOCIAL_ROLLUPS_PATH, {})
    seen: set[str] = set()
    if not isinstance(payload, dict):
        return seen
    for row in ((payload.get("rollups") or {}).get("by_theme") or []):
        label = _compact_text((row or {}).get("label")).lower()
        if label and label != "(unknown)":
            seen.add(label)
            seen.update(_title_tokens(label))
    for row in payload.get("top_posts") or []:
        seen.update(_title_tokens(_compact_text((row or {}).get("title"))))
        seen.update(_title_tokens(_compact_text((row or {}).get("theme"))))
    return seen


def _ideas_to_test(motif_rows: list[dict[str, Any]]) -> list[str]:
    own_terms = _load_social_theme_context()
    ideas: list[str] = []
    for row in motif_rows:
        keyword = _compact_text(row.get("keyword")).lower()
        if not keyword or keyword in own_terms:
            continue
        ideas.append(
            f"Test a `{keyword}`-led duck or post angle; competitors are surfacing it across `{int(row.get('listing_count') or 0)}` recent listings."
        )
        if len(ideas) >= 4:
            break
    return ideas


def _current_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary") or {}
    top_shop = ((payload.get("top_momentum_shops") or [{}])[0] or {})
    top_motif = ((payload.get("emergent_motifs") or [{}])[0] or {})
    return {
        "generated_at": payload.get("generated_at"),
        "latest_report_date": summary.get("latest_report_date"),
        "observation_days": summary.get("observation_days"),
        "latest_total_competitor_listings": summary.get("latest_total_competitor_listings"),
        "latest_new_competitor_listings": summary.get("latest_new_competitor_listings"),
        "top_shop": top_shop.get("shop_name"),
        "top_motif": top_motif.get("keyword"),
    }


def _changes_since_previous(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[dict[str, Any]]:
    if not previous:
        return []
    changes: list[dict[str, Any]] = []
    if previous.get("top_shop") != current.get("top_shop"):
        changes.append({"kind": "top_shop_changed", "headline": f"Top competitor momentum shop moved from `{previous.get('top_shop')}` to `{current.get('top_shop')}`."})
    if previous.get("top_motif") != current.get("top_motif"):
        changes.append({"kind": "top_motif_changed", "headline": f"Leading competitor motif moved from `{previous.get('top_motif')}` to `{current.get('top_motif')}`."})
    previous_new = _safe_int(previous.get("latest_new_competitor_listings")) or 0
    current_new = _safe_int(current.get("latest_new_competitor_listings")) or 0
    if previous_new != current_new:
        delta = current_new - previous_new
        direction = "up" if delta > 0 else "down"
        changes.append({"kind": "new_listing_delta", "headline": f"New competitor listings are {direction} by `{abs(delta)}` versus the previous snapshot."})
    return changes


def build_competitor_benchmark_payload(*, window_days: int = 30) -> dict[str, Any]:
    current = datetime.now().astimezone()
    reports, report_summary = _load_competitor_reports(window_days=window_days, now=current)
    if not reports:
        payload = {
            "generated_at": now_local_iso(),
            "summary": {
                **report_summary,
                "latest_report_date": None,
                "data_quality_note": "No competitor observation runs were available in the requested window.",
            },
            "market_learnings": [],
            "top_momentum_shops": [],
            "fastest_growing_shops": [],
            "emergent_motifs": [],
            "ideas_to_test": [],
            "changes_since_previous": [],
            "history": {"snapshot_count": len(_history_snapshots()), "recent_snapshots": _history_snapshots()[-8:]},
        }
        return payload

    latest = reports[-1]
    previous = reports[-2] if len(reports) >= 2 else None
    latest_report = latest.get("report") or {}
    latest_date = _parse_date(latest.get("report_date")) or current.date()
    shop_snapshots = latest_report.get("shop_snapshots") if isinstance(latest_report.get("shop_snapshots"), list) else []
    listing_snapshots = latest_report.get("listing_snapshots") if isinstance(latest_report.get("listing_snapshots"), list) else []
    top_momentum = _shop_rows(shop_snapshots, sort_key="momentum_score")
    fastest_growth = _shop_rows(shop_snapshots, sort_key="growth_rate")
    motifs = _weighted_motif_rows(listing_snapshots, current_date=latest_date)
    ideas = _ideas_to_test(motifs)

    market_learnings: list[dict[str, Any]] = []
    if top_momentum:
        top = top_momentum[0]
        market_learnings.append(
            {
                "key": "top_competitor_shop",
                "headline": f"{top.get('shop_name')} has the strongest current competitor momentum.",
                "confidence": "medium",
                "evidence": f"Momentum `{top.get('momentum_score')}` with `{top.get('listing_active_count')}` active listings.",
                "recommendation": "Check what that shop is launching and how quickly new motifs are appearing.",
            }
        )
    if motifs:
        top = motifs[0]
        market_learnings.append(
            {
                "key": "top_competitor_motif",
                "headline": f"`{top.get('keyword')}` is the strongest current competitor motif.",
                "confidence": "medium" if int(top.get("listing_count") or 0) >= 3 else "low",
                "evidence": f"Appears across `{top.get('listing_count')}` recent listings with weighted score `{top.get('score')}`.",
                "recommendation": "Use it as a theme candidate for product or post testing, not as a blind copy target.",
            }
        )
    if ideas:
        market_learnings.append(
            {
                "key": "ideas_to_test",
                "headline": "Competitor market motifs are surfacing test ideas we are not visibly covering yet.",
                "confidence": "low",
                "evidence": f"{min(len(ideas), 4)} competitor-inspired tests were inferred from recent motif gaps.",
                "recommendation": "Pick one or two ideas to test in content first before treating them as product priorities.",
            }
        )

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            **report_summary,
            "latest_report_date": latest.get("report_date"),
            "latest_total_competitor_shops": _safe_int(latest_report.get("total_competitor_shops")) or 0,
            "latest_total_competitor_listings": _safe_int(latest_report.get("total_competitor_listings")) or 0,
            "latest_new_competitor_listings": _safe_int(latest_report.get("new_competitor_listings")) or 0,
            "data_quality_note": "This is a competitor market/listing benchmark from Etsy competitor intelligence, not a direct competitor social-post feed.",
        },
        "market_learnings": market_learnings,
        "top_momentum_shops": top_momentum,
        "fastest_growing_shops": fastest_growth,
        "emergent_motifs": motifs,
        "ideas_to_test": ideas,
        "source_reports": [{"report_date": item.get("report_date"), "path": item.get("path"), "run_id": item.get("run_id")} for item in reports[-10:]],
    }

    history = _history_snapshots()
    current_snapshot = _current_snapshot(payload)
    previous_snapshot = history[-1] if history else None
    history.append(current_snapshot)
    history = history[-60:]
    _save_history(history)
    payload["changes_since_previous"] = _changes_since_previous(previous_snapshot, current_snapshot)
    payload["history"] = {
        "snapshot_count": len(history),
        "latest_snapshot": current_snapshot,
        "previous_snapshot": previous_snapshot,
        "recent_snapshots": history[-8:],
    }
    return payload


def render_competitor_benchmark_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# Competitor Benchmark",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Window: last `{summary.get('window_days') or 0}` days",
        f"- Observation days: `{summary.get('observation_days') or 0}`",
        f"- Latest report date: `{summary.get('latest_report_date') or 'n/a'}`",
        f"- Latest competitor listings: `{summary.get('latest_total_competitor_listings') or 0}`",
        f"- Latest new competitor listings: `{summary.get('latest_new_competitor_listings') or 0}`",
        f"- Snapshot history: `{((payload.get('history') or {}).get('snapshot_count')) or 0}` runs",
        "",
        str(summary.get("data_quality_note") or ""),
        "",
        "## What Changed",
        "",
    ]

    changes = payload.get("changes_since_previous") or []
    if not changes:
        lines.append("No major competitor benchmark change was detected since the previous snapshot.")
        lines.append("")
    else:
        for item in changes:
            lines.append(f"- {item.get('headline')}")
        lines.append("")

    lines.extend(["## Current Market Learnings", ""])
    learnings = payload.get("market_learnings") or []
    if not learnings:
        lines.append("No competitor market learnings are available yet.")
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

    lines.extend(["## Top Momentum Shops", ""])
    for item in payload.get("top_momentum_shops") or []:
        lines.append(
            f"- `{item.get('shop_name')}` | momentum `{item.get('momentum_score')}` | growth `{item.get('growth_rate')}` | active listings `{item.get('listing_active_count')}`"
        )
    lines.append("")

    lines.extend(["## Emerging Motifs", ""])
    motifs = payload.get("emergent_motifs") or []
    if not motifs:
        lines.append("No emerging competitor motifs were found in the current window.")
        lines.append("")
    else:
        for item in motifs[:10]:
            lines.append(f"- `{item.get('keyword')}` | score `{item.get('score')}` | listings `{item.get('listing_count')}`")
        lines.append("")

    lines.extend(["## Ideas Worth Testing", ""])
    ideas = payload.get("ideas_to_test") or []
    if not ideas:
        lines.append("No clean competitor-gap ideas were inferred yet.")
        lines.append("")
    else:
        for item in ideas:
            lines.append(f"- {item}")
        lines.append("")

    return "\n".join(lines)


def build_competitor_benchmark(*, window_days: int = 30) -> dict[str, Any]:
    payload = build_competitor_benchmark_payload(window_days=window_days)
    write_json(COMPETITOR_STATE_PATH, payload)
    write_json(COMPETITOR_OPERATOR_JSON_PATH, payload)
    write_markdown(COMPETITOR_OUTPUT_MD_PATH, render_competitor_benchmark_markdown(payload))
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the observe-only competitor benchmark collector.")
    parser.add_argument("--window-days", type=int, default=30, help="Number of trailing days to include.")
    args = parser.parse_args()
    payload = build_competitor_benchmark(window_days=max(1, args.window_days))
    print(
        {
            "generated_at": payload.get("generated_at"),
            "observation_days": (payload.get("summary") or {}).get("observation_days"),
            "idea_count": len(payload.get("ideas_to_test") or []),
        }
    )


if __name__ == "__main__":
    main()
