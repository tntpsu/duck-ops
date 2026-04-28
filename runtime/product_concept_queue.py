#!/usr/bin/env python3
"""
Product concept queue for turning observed market signals into design-brief input.

Duck Ops owns this as an observe/review surface. DuckAgent owns the later creative
execution once an operator approves a design brief.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from governance_review_common import (
    OUTPUT_OPERATOR_DIR,
    STATE_DIR,
    load_json,
    now_local_iso,
    write_json,
    write_markdown,
)


TREND_CANDIDATES_PATH = STATE_DIR / "normalized" / "trend_candidates.json"
CURRENT_LEARNINGS_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.json"
COMPETITOR_SOCIAL_BENCHMARK_PATH = STATE_DIR / "competitor_social_benchmark.json"
PRODUCT_CONCEPT_QUEUE_PATH = STATE_DIR / "product_concept_queue.json"
PRODUCT_CONCEPT_QUEUE_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "product_concept_queue.json"
PRODUCT_CONCEPT_QUEUE_MD_PATH = OUTPUT_OPERATOR_DIR / "product_concept_queue.md"
PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH = STATE_DIR / "product_concept_queue_design_brief_input.json"

SURFACE_VERSION = 1
DEFAULT_MAX_ITEMS = 12
DEFAULT_DESIGN_BRIEF_LIMIT = 3

NOISE_PHRASES = (
    "rubber duck figurine",
    "rubber ducky",
    "rubber duck",
    "jeep duck",
    "desk decor",
    "car decor",
    "dashboard",
    "collectible",
    "figurine",
    "fidget toy",
    "fidget",
    "gift",
    "toy",
    "duck",
)

IP_SENSITIVE_TOKENS = {
    "delta gamma": "Greek-letter organization themes need manual abstraction before concepting.",
    "gamma delta": "Greek-letter organization themes need manual abstraction before concepting.",
    "sorority": "Sorority/fraternity references need manual abstraction before concepting.",
    "fraternity": "Sorority/fraternity references need manual abstraction before concepting.",
    "gcu": "School/team references need manual abstraction before concepting.",
    "lopes": "School/team nickname should not become a logo or trademark concept.",
    "wildcats": "Mascot/team references need manual abstraction before concepting.",
    "vols": "College/team nickname should not become a logo or trademark concept.",
    "tennessee vols": "College/team nickname should not become a logo or trademark concept.",
    "chicago football": "City-plus-sport themes are likely team-adjacent and need manual abstraction.",
    "football": "Sport themes need a generic public-safe direction before concepting.",
    "hockey": "Sport themes need a generic public-safe direction before concepting.",
    "logo": "Avoid logo-driven concepts unless a public-safe abstraction is defined.",
    "nfl": "Professional sports league references need explicit abstraction before concepting.",
    "nba": "Professional sports league references need explicit abstraction before concepting.",
    "mlb": "Professional sports league references need explicit abstraction before concepting.",
    "disney": "Named entertainment IP needs explicit abstraction before concepting.",
    "marvel": "Named entertainment IP needs explicit abstraction before concepting.",
    "pokemon": "Named entertainment IP needs explicit abstraction before concepting.",
}


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None or isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _slugify(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return text or "concept"


def _stable_id(prefix: str, theme: str, source: str) -> str:
    digest = hashlib.sha1(f"{prefix}:{source}:{theme}".encode("utf-8")).hexdigest()[:10]
    return f"{prefix}::{_slugify(theme)[:48]}::{digest}"


def _clean_theme(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    for phrase in NOISE_PHRASES:
        text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -")
    if not text:
        text = str(value or "fresh duck concept").strip()
    words = [part for part in text.split() if part]
    if not words:
        return "Fresh Duck Concept"
    return " ".join(word.capitalize() for word in words)


def _candidate_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        items = payload.get("items")
        if items is None:
            items = payload.get("candidates")
    else:
        items = payload
    return [item for item in list(items or []) if isinstance(item, dict)]


def _catalog_status(item: dict[str, Any]) -> str:
    catalog_match = item.get("catalog_match")
    if isinstance(catalog_match, dict):
        return str(catalog_match.get("status") or "unknown").strip() or "unknown"
    return "unknown"


def _source_ref_count(item: dict[str, Any]) -> int:
    refs = item.get("source_refs")
    return len(refs) if isinstance(refs, list) else 0


def _ip_guardrails(theme: str) -> list[str]:
    text = f" {theme.lower()} "
    guardrails: list[str] = []
    for token, message in IP_SENSITIVE_TOKENS.items():
        if f" {token} " in text:
            guardrails.append(message)
    return guardrails


def _trend_candidate_to_queue_item(item: dict[str, Any]) -> dict[str, Any]:
    raw_theme = str(item.get("theme") or "fresh duck concept").strip()
    theme = _clean_theme(raw_theme)
    signal_summary = item.get("signal_summary") if isinstance(item.get("signal_summary"), dict) else {}
    trending_score = _as_float(signal_summary.get("trending_score"))
    sold_7d = _as_int(signal_summary.get("sold_last_7d"))
    revenue_7d = _as_float(signal_summary.get("revenue_last_7d"))
    catalog_status = _catalog_status(item)
    source_ref_count = _source_ref_count(item)
    guardrails = [
        "public_concept_allowed",
        "Do not copy competitor artwork, exact wording, photos, tags, or listing structure.",
        "Keep this as a duck-first design, not a loose prop or non-duck object.",
        "Avoid readable logos, team marks, brand names, copyrighted characters, and customer-specific text.",
    ]
    guardrails.extend(_ip_guardrails(raw_theme))

    confidence_cap = _as_float(item.get("input_confidence_cap"), 0.65) or 0.65
    evidence_bonus = min(0.2, source_ref_count * 0.04)
    commercial_bonus = min(0.2, (sold_7d * 0.04) + (revenue_7d / 500.0))
    confidence = min(confidence_cap, 0.45 + evidence_bonus + commercial_bonus)
    score = min(1.0, 0.15 + (trending_score / 1000.0) + (sold_7d * 0.04) + (revenue_7d / 500.0))

    if len(guardrails) > 4:
        queue_state = "blocked_by_guardrail"
        recommended_next_step = "Review abstraction manually before sending this to DuckAgent for design briefs."
    elif catalog_status == "gap" and confidence >= 0.52:
        queue_state = "ready_for_brief_review"
        recommended_next_step = "Send this candidate to DuckAgent design_brief_queue for operator review."
    else:
        queue_state = "watch"
        recommended_next_step = "Keep watching until the signal has stronger commercial evidence or a clearer catalog gap."

    evidence = [
        f"Trend theme: {raw_theme}",
        f"Catalog status: {catalog_status}",
        f"Trending score: {trending_score:.1f}",
        f"7d sold: {sold_7d}",
        f"7d revenue: ${revenue_7d:.2f}",
        f"Source refs: {source_ref_count}",
    ]

    return {
        "concept_id": _stable_id("trend", theme, str(item.get("artifact_id") or raw_theme)),
        "source_type": "trend_candidate",
        "source_artifact_id": item.get("artifact_id"),
        "theme": theme,
        "raw_theme": raw_theme,
        "catalog_status": catalog_status,
        "queue_state": queue_state,
        "score": round(score, 3),
        "confidence": round(confidence, 3),
        "evidence": evidence,
        "guardrails": guardrails,
        "recommended_next_step": recommended_next_step,
        "duckagent_task": "design_brief_queue",
        "source_refs": list(item.get("source_refs") or [])[:3],
    }


def _learning_motif_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    motifs = payload.get("competitor_motifs") if isinstance(payload, dict) else []
    items: list[dict[str, Any]] = []
    for motif in list(motifs or []):
        if not isinstance(motif, dict):
            continue
        label = str(motif.get("label") or "").strip()
        if not label:
            continue
        theme = _clean_theme(label)
        engagement = _as_float(motif.get("avg_engagement_score"))
        post_count = _as_int(motif.get("post_count"))
        score = min(0.76, 0.25 + min(0.25, post_count * 0.03) + min(0.26, engagement / 1500.0))
        items.append(
            {
                "concept_id": _stable_id("motif", theme, "current_learnings"),
                "source_type": "competitor_motif",
                "source_artifact_id": "current_learnings.competitor_motifs",
                "theme": theme,
                "raw_theme": label,
                "catalog_status": "unknown",
                "queue_state": "watch",
                "score": round(score, 3),
                "confidence": round(min(0.68, 0.35 + post_count * 0.04), 3),
                "evidence": [
                    f"Competitor motif `{label}` appeared in {post_count} post(s).",
                    f"Average engagement score: {engagement:.2f}",
                ],
                "guardrails": [
                    "public_concept_allowed",
                    "Use as market vocabulary only; do not copy a competitor post or product.",
                    "Convert the motif into a duck-first design before image generation.",
                ],
                "recommended_next_step": "Use this as supporting evidence when a trend candidate with catalog gap appears.",
                "duckagent_task": "design_brief_queue",
                "source_refs": [],
            }
        )
    return items


def _strategy_idea_items(current_learnings: dict[str, Any], benchmark: dict[str, Any]) -> list[dict[str, Any]]:
    ideas: list[str] = []
    for payload in (current_learnings, benchmark):
        if not isinstance(payload, dict):
            continue
        for value in list(payload.get("ideas_to_test") or [])[:6]:
            text = str(value or "").strip()
            if text and text not in ideas:
                ideas.append(text)

    items: list[dict[str, Any]] = []
    for idea in ideas[:6]:
        theme = _clean_theme(re.sub(r"^test one\s+", "", idea, flags=re.IGNORECASE))
        items.append(
            {
                "concept_id": _stable_id("strategy", theme, idea),
                "source_type": "strategy_idea",
                "source_artifact_id": "social_strategy.ideas_to_test",
                "theme": theme,
                "raw_theme": idea,
                "catalog_status": "unknown",
                "queue_state": "watch",
                "score": 0.38,
                "confidence": 0.42,
                "evidence": [idea],
                "guardrails": [
                    "public_concept_allowed",
                    "Treat social strategy ideas as weak product signals until commerce evidence appears.",
                ],
                "recommended_next_step": "Keep as an idea-bank signal, not a product concept approval yet.",
                "duckagent_task": "design_brief_queue",
                "source_refs": [],
            }
        )
    return items


def _merge_duplicate_themes(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _slugify(item.get("theme"))
        existing = merged.get(key)
        if not existing:
            merged[key] = item
            continue
        if float(item.get("score") or 0.0) > float(existing.get("score") or 0.0):
            item["evidence"] = list(existing.get("evidence") or [])[:3] + list(item.get("evidence") or [])[:4]
            item["guardrails"] = list(dict.fromkeys(list(existing.get("guardrails") or []) + list(item.get("guardrails") or [])))
            merged[key] = item
        else:
            existing["evidence"] = list(existing.get("evidence") or [])[:4] + list(item.get("evidence") or [])[:3]
            existing["guardrails"] = list(dict.fromkeys(list(existing.get("guardrails") or []) + list(item.get("guardrails") or [])))
    return list(merged.values())


def _queue_rank(item: dict[str, Any]) -> tuple[int, float, float, str]:
    state_rank = {"ready_for_brief_review": 0, "watch": 1, "blocked_by_guardrail": 2}
    return (
        state_rank.get(str(item.get("queue_state") or "watch"), 9),
        -float(item.get("score") or 0.0),
        -float(item.get("confidence") or 0.0),
        str(item.get("theme") or "").lower(),
    )


def _design_brief_signal(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": str(item.get("concept_id") or ""),
        "source": "duck-ops.product_concept_queue",
        "signal_type": str(item.get("source_type") or "product_concept"),
        "theme": str(item.get("theme") or "Fresh Duck Concept"),
        "evidence": [str(value) for value in list(item.get("evidence") or []) if str(value).strip()][:5],
        "confidence": float(item.get("confidence") or 0.0),
        "score": float(item.get("score") or 0.0),
        "guardrails": [str(value) for value in list(item.get("guardrails") or []) if str(value).strip()][:8],
    }


def _build_design_brief_input(items: list[dict[str, Any]], *, limit: int) -> dict[str, Any]:
    ready = [item for item in items if str(item.get("queue_state") or "") == "ready_for_brief_review"]
    candidates = ready[:limit]
    return {
        "channel": "product_concept",
        "goal": (
            "Turn the strongest public-safe product concept signals into 2 to 3 duck design briefs "
            "for operator review before any image generation or listing work."
        ),
        "time_window": "latest Duck Ops product concept queue",
        "max_candidates": min(limit, max(1, len(candidates))) if candidates else limit,
        "operator_notes": (
            "Duck Ops curated these from market, trend, and competitor-learning signals. "
            "Keep concepts duck-first, public-safe, printable, and distinct from competitor work."
        ),
        "candidate_signals": [_design_brief_signal(item) for item in candidates],
    }


def build_product_concept_queue(
    *,
    trend_candidates: dict[str, Any] | list[dict[str, Any]] | None = None,
    current_learnings: dict[str, Any] | None = None,
    competitor_social_benchmark: dict[str, Any] | None = None,
    max_items: int = DEFAULT_MAX_ITEMS,
    design_brief_limit: int = DEFAULT_DESIGN_BRIEF_LIMIT,
    write_outputs: bool = True,
) -> dict[str, Any]:
    trend_payload = trend_candidates if trend_candidates is not None else load_json(TREND_CANDIDATES_PATH, {"items": []})
    learning_payload = current_learnings if current_learnings is not None else load_json(CURRENT_LEARNINGS_PATH, {})
    benchmark_payload = (
        competitor_social_benchmark
        if competitor_social_benchmark is not None
        else load_json(COMPETITOR_SOCIAL_BENCHMARK_PATH, {})
    )

    items = [_trend_candidate_to_queue_item(item) for item in _candidate_items(trend_payload)]
    if isinstance(learning_payload, dict):
        items.extend(_learning_motif_items(learning_payload))
    if isinstance(learning_payload, dict) and isinstance(benchmark_payload, dict):
        items.extend(_strategy_idea_items(learning_payload, benchmark_payload))

    all_items = _merge_duplicate_themes(items)
    all_items.sort(key=_queue_rank)

    ready_items = [item for item in all_items if item.get("queue_state") == "ready_for_brief_review"]
    watch_items = [item for item in all_items if item.get("queue_state") == "watch"]
    blocked_items = [item for item in all_items if item.get("queue_state") == "blocked_by_guardrail"]
    blocked_slot_count = min(3, len(blocked_items))
    watch_slot_count = min(2, len(watch_items))
    ready_slot_count = max(0, max_items - blocked_slot_count - watch_slot_count)
    items = ready_items[:ready_slot_count] + watch_items[:watch_slot_count] + blocked_items[:blocked_slot_count]

    ready_count = len(ready_items)
    watch_count = len(watch_items)
    blocked_count = len(blocked_items)
    design_brief_input = _build_design_brief_input(ready_items, limit=design_brief_limit)

    if ready_count:
        status = "ready_for_brief_review"
        headline = f"{ready_count} product concept candidate(s) are ready for design-brief review."
        recommended_action = "Run DuckAgent `design_brief_queue` with the generated input, then approve or revise the strongest brief."
    elif watch_count:
        status = "watch"
        headline = f"{watch_count} product concept signal(s) are worth watching, but none are ready for brief review."
        recommended_action = "Keep observing until a public-safe catalog gap has stronger evidence."
    elif blocked_count:
        status = "blocked_by_guardrail"
        headline = f"{blocked_count} product concept candidate(s) need manual abstraction before brief review."
        recommended_action = "Review the guardrails and rewrite any risky theme into a public-safe, duck-first abstraction."
    else:
        status = "clear"
        headline = "No product concept candidates are staged right now."
        recommended_action = "No product concept action needed."

    payload = {
        "generated_at": now_local_iso(),
        "surface_version": SURFACE_VERSION,
        "status": status,
        "headline": headline,
        "recommended_action": recommended_action,
        "source": "trend_candidates_plus_social_learnings",
        "source_paths": {
            "trend_candidates": str(TREND_CANDIDATES_PATH),
            "current_learnings": str(CURRENT_LEARNINGS_PATH),
            "competitor_social_benchmark": str(COMPETITOR_SOCIAL_BENCHMARK_PATH),
            "design_brief_input": str(PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH),
        },
        "summary": {
            "candidate_count": len(all_items),
            "ready_for_brief_review_count": ready_count,
            "watch_count": watch_count,
            "blocked_by_guardrail_count": blocked_count,
            "design_brief_signal_count": len(design_brief_input.get("candidate_signals") or []),
        },
        "design_brief_input": design_brief_input,
        "items": items,
    }

    if write_outputs:
        write_json(PRODUCT_CONCEPT_QUEUE_PATH, payload)
        write_json(PRODUCT_CONCEPT_QUEUE_OPERATOR_JSON_PATH, payload)
        write_json(PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH, design_brief_input)
        write_markdown(PRODUCT_CONCEPT_QUEUE_MD_PATH, render_product_concept_queue_markdown(payload))
    return payload


def render_product_concept_queue_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    items = [item for item in list(payload.get("items") or []) if isinstance(item, dict)]
    source_paths = payload.get("source_paths") if isinstance(payload.get("source_paths"), dict) else {}
    lines = [
        "# Product Concept Queue",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Status: `{payload.get('status') or 'unknown'}`",
        f"- Candidates: `{summary.get('candidate_count', len(items))}`",
        f"- Ready for brief review: `{summary.get('ready_for_brief_review_count', 0)}`",
        f"- Watch: `{summary.get('watch_count', 0)}`",
        f"- Blocked by guardrail: `{summary.get('blocked_by_guardrail_count', 0)}`",
        f"- Design-brief signals: `{summary.get('design_brief_signal_count', 0)}`",
        f"- Design-brief input: `{source_paths.get('design_brief_input') or PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH}`",
        f"- Headline: {payload.get('headline')}",
        f"- Recommended action: {payload.get('recommended_action')}",
        "",
        "## Candidates",
        "",
    ]
    if not items:
        lines.append("No product concept candidates are staged right now.")
    for item in items:
        lines.append(
            f"- {item.get('theme') or 'Unknown concept'} | `{item.get('queue_state') or 'unknown'}` | "
            f"`{item.get('source_type') or 'unknown'}` | score `{item.get('score', 0)}` | confidence `{item.get('confidence', 0)}`"
        )
        lines.append(f"  Next: {item.get('recommended_next_step')}")
        evidence = [str(value) for value in list(item.get("evidence") or []) if str(value).strip()]
        if evidence:
            lines.append(f"  Evidence: {'; '.join(evidence[:3])}")
        guardrails = [str(value) for value in list(item.get("guardrails") or []) if str(value).strip()]
        if guardrails:
            lines.append(f"  Guardrails: {'; '.join(guardrails[:3])}")
    return "\n".join(lines) + "\n"


def main() -> None:
    build_product_concept_queue(write_outputs=True)


if __name__ == "__main__":
    main()
