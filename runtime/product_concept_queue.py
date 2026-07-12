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
from concept_name_quality import concept_name_quality
from product_concept_brief import (
    build_concept_design_brief,
    clean_product_theme,
    evaluate_trend_quality,
)


TREND_CANDIDATES_PATH = STATE_DIR / "normalized" / "trend_candidates.json"
CURRENT_LEARNINGS_PATH = OUTPUT_OPERATOR_DIR / "current_learnings.json"
COMPETITOR_SOCIAL_BENCHMARK_PATH = STATE_DIR / "competitor_social_benchmark.json"
PRODUCT_CONCEPT_QUEUE_PATH = STATE_DIR / "product_concept_queue.json"
PRODUCT_CONCEPT_QUEUE_OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "product_concept_queue.json"
PRODUCT_CONCEPT_QUEUE_MD_PATH = OUTPUT_OPERATOR_DIR / "product_concept_queue.md"
PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH = STATE_DIR / "product_concept_queue_design_brief_input.json"
PRODUCT_CONCEPT_FEEDBACK_PATH = STATE_DIR / "product_concept_feedback.json"
# Surface 16: operator "build this next" promotions from the Build-Next
# portal page. The viewer appends here; this queue ingests them so a
# promoted concept becomes a design brief for review — credits are still
# spent only when the operator approves the brief in DuckAgent's Studio.
BUILD_NEXT_PROMOTIONS_PATH = STATE_DIR / "build_next_promotions.json"
CUSTOMER_ASK_CANDIDATES_PATH = STATE_DIR / "customer_ask_candidates.json"  # Surface 58

SURFACE_VERSION = 1
DEFAULT_MAX_ITEMS = 12
DEFAULT_DESIGN_BRIEF_LIMIT = 3
SUPPRESSING_FEEDBACK_RESOLUTIONS = {
    "abandoned",
    "approved",
    "approved_to_build",
    "approved_to_generate_concept_image",
    "discard",
    "discarded",
    "duplicate",
    "needs_changes",
    "needs_reframe",
    "not_interested",
    "rejected",
    "revision_requested",
    "skip",
    "skip_this_cycle",
    "skipped",
    "skipped_this_cycle",
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


# Manufacturing/marketing boilerplate stripped from a theme before cross-scout
# dedup. Mirrors duckAgent's semantic_dedupe boilerplate list. Two scouts
# proposing the same duck under differently-cleaned themes ("chef" vs
# "3d chef pla plastic") only slug-matched if identical — keyword-stuffed
# competitor titles never did, so the merge missed and a duplicate concept
# slipped through (2026-06-30, TESTS.md Surface 49). Stripping the boilerplate
# first makes the merge key the core SUBJECT. Deliberately excludes thematic
# words (adventure/rustic/western/…): those still distinguish concepts.
_MERGE_BOILERPLATE: frozenset[str] = frozenset({
    "3d", "printed", "print", "printing", "pla", "plastic", "resin",
    "duck", "ducks", "ducky", "ducking", "figurine", "figure",
    "collectible", "collectibles", "collectable", "gift", "gifts",
    "car", "cars", "dashboard", "dash", "decor", "decoration",
    "accessory", "accessories", "cruise", "cruises", "vehicle",
    "jeep", "jeeper", "jeeping", "buddy", "toy", "toys", "rubber",
    "novelty", "custom", "personalized", "quirky", "funny", "unique",
    "cute", "mini", "small", "style", "styles", "based", "for", "the",
    "and", "with", "inspired", "lover", "lovers", "edition", "version",
    "design", "designs", "handmade", "perfect", "great",
})


def _theme_merge_key(theme: Any) -> str:
    """Cross-scout dedup key: the theme reduced to its core-subject tokens
    (boilerplate + short words stripped, order-independent). Falls back to the
    raw slug when nothing survives, so two all-boilerplate themes stay distinct
    instead of collapsing to an empty key."""
    toks = sorted(
        t for t in re.split(r"[^a-z0-9]+", str(theme or "").lower())
        if len(t) > 2 and t not in _MERGE_BOILERPLATE
    )
    return "-".join(toks) if toks else _slugify(theme)


def _concept_feedback_key(value: Any) -> str:
    text = re.sub(r"\bducks?\b", " ", str(value or "").lower())
    text = text.replace("3dprinting", "3d printing")
    return _slugify(text)


def _concept_feedback_keys_for_item(item: dict[str, Any]) -> set[str]:
    concept_brief = item.get("concept_design_brief") if isinstance(item.get("concept_design_brief"), dict) else {}
    quality_gate = item.get("trend_quality_gate") if isinstance(item.get("trend_quality_gate"), dict) else {}
    values: list[Any] = [
        item.get("concept_id"),
        item.get("source_artifact_id"),
        item.get("theme"),
        item.get("raw_theme"),
        concept_brief.get("concept_title"),
        concept_brief.get("raw_theme"),
        concept_brief.get("semantic_identity"),
        quality_gate.get("normalized_concept_title"),
    ]
    keys = {_concept_feedback_key(value) for value in values if str(value or "").strip()}
    return {key for key in keys if key and key != "concept"}


def _load_concept_feedback() -> dict[str, Any]:
    payload = load_json(PRODUCT_CONCEPT_FEEDBACK_PATH, {"concepts": {}})
    if not isinstance(payload, dict):
        return {"concepts": {}}
    if not isinstance(payload.get("concepts"), dict):
        payload["concepts"] = {}
    return payload


def _feedback_for_item(item: dict[str, Any], feedback: dict[str, Any]) -> dict[str, Any]:
    concepts = feedback.get("concepts") if isinstance(feedback.get("concepts"), dict) else {}
    matches: list[dict[str, Any]] = []
    item_keys = _concept_feedback_keys_for_item(item)
    for key, record in concepts.items():
        if not isinstance(record, dict):
            continue
        record_keys = {
            _concept_feedback_key(key),
            *[_concept_feedback_key(value) for value in (record.get("aliases") or []) if str(value or "").strip()],
            *[_concept_feedback_key(value) for value in (record.get("concept_keys") or []) if str(value or "").strip()],
        }
        if item_keys.intersection(record_keys):
            matches.append(record)
    if not matches:
        return {}
    return sorted(matches, key=lambda record: str(record.get("updated_at") or record.get("recorded_at") or ""), reverse=True)[0]


def _feedback_suppresses_concept(record: dict[str, Any]) -> bool:
    resolution = str(record.get("latest_resolution") or record.get("resolution") or "").strip().lower()
    return resolution in SUPPRESSING_FEEDBACK_RESOLUTIONS


def _stable_id(prefix: str, theme: str, source: str) -> str:
    digest = hashlib.sha1(f"{prefix}:{source}:{theme}".encode("utf-8")).hexdigest()[:10]
    return f"{prefix}::{_slugify(theme)[:48]}::{digest}"


def _clean_theme(value: Any) -> str:
    return clean_product_theme(value)


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


def _trend_candidate_to_queue_item(item: dict[str, Any]) -> dict[str, Any]:
    raw_theme = str(item.get("theme") or "fresh duck concept").strip()
    name_quality = concept_name_quality(raw_theme)
    signal_summary = item.get("signal_summary") if isinstance(item.get("signal_summary"), dict) else {}
    trending_score = _as_float(signal_summary.get("trending_score"))
    sold_7d = _as_int(signal_summary.get("sold_last_7d"))
    revenue_7d = _as_float(signal_summary.get("revenue_last_7d"))
    catalog_status = _catalog_status(item)
    source_ref_count = _source_ref_count(item)
    source_refs = list(item.get("source_refs") or [])[:10]
    trend_quality_gate = evaluate_trend_quality(
        raw_theme=raw_theme,
        signal_summary=signal_summary,
        source_refs=source_refs,
        catalog_status=catalog_status,
        latest_observed_at=item.get("observed_at"),
    )
    concept_design_brief = build_concept_design_brief(
        raw_theme=raw_theme,
        signal_summary=signal_summary,
        source_refs=source_refs,
        catalog_status=catalog_status,
        latest_observed_at=item.get("observed_at"),
        review_status=str(item.get("review_status") or ""),
        confidence=None,
        trend_quality_gate=trend_quality_gate,
        brief_source="duck_ops_product_concept_queue",
    )
    theme = _clean_theme(concept_design_brief.get("concept_title") or raw_theme)
    guardrails = [
        "public_concept_allowed",
        "Do not copy competitor artwork, exact wording, photos, tags, or listing structure.",
        "Keep this as a duck-first design, not a loose prop or non-duck object.",
        "Avoid readable logos, team marks, brand names, copyrighted characters, and customer-specific text.",
    ]
    guardrails.extend(str(issue) for issue in name_quality.get("issues") or [])
    guardrails.extend(str(issue) for issue in trend_quality_gate.get("issues") or [])
    guardrails.extend(str(warning) for warning in trend_quality_gate.get("warnings") or [])

    confidence_cap = _as_float(item.get("input_confidence_cap"), 0.65) or 0.65
    evidence_bonus = min(0.2, source_ref_count * 0.04)
    commercial_bonus = min(0.2, (sold_7d * 0.04) + (revenue_7d / 500.0))
    confidence = min(confidence_cap, 0.45 + evidence_bonus + commercial_bonus)
    score = min(1.0, 0.15 + (trending_score / 1000.0) + (sold_7d * 0.04) + (revenue_7d / 500.0))

    if trend_quality_gate.get("status") == "blocked_by_policy" or name_quality.get("status") != "product_ready":
        queue_state = "blocked_by_guardrail"
        recommended_next_step = "Reframe this raw trend into a concrete product-ready duck concept before design briefs."
    elif trend_quality_gate.get("status") == "needs_refresh" or int(trend_quality_gate.get("staleness_days") or 0) > 21:
        queue_state = "watch"
        recommended_next_step = "Refresh the market evidence before sending this concept to DuckAgent for design briefs."
    elif catalog_status == "gap" and confidence >= 0.52:
        queue_state = "ready_for_brief_review"
        if trend_quality_gate.get("status") == "needs_reframe":
            recommended_next_step = "Use the cleaned concept brief before image generation; do not use the raw marketplace title."
        else:
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
        "name_quality": name_quality,
        "trend_quality_gate": trend_quality_gate,
        "concept_design_brief": concept_design_brief,
        "evidence": evidence,
        "guardrails": guardrails,
        "recommended_next_step": recommended_next_step,
        "duckagent_task": "design_brief_queue",
        "source_refs": source_refs[:3],
    }


def _build_next_promotion_items(payload: Any = None) -> list[dict[str, Any]]:
    """Surface 16: turn operator Build-Next promotions into queue items.

    Reuses the same brief + name/trend quality machinery as trend
    candidates (so an off-policy or unprintable name still fails closed),
    but defaults to ready_for_brief_review because the operator already
    vetted demand/margin/gap on the Build-Next page. brief_source is
    'build_next' so the lineage is visible downstream."""
    data = payload if payload is not None else load_json(BUILD_NEXT_PROMOTIONS_PATH, {"promotions": []})
    promotions = data.get("promotions") if isinstance(data, dict) else data
    items: list[dict[str, Any]] = []
    for promo in list(promotions or []):
        if not isinstance(promo, dict):
            continue
        raw_theme = str(promo.get("title") or "").strip()
        if not raw_theme:
            continue
        name_quality = concept_name_quality(raw_theme)
        trend_quality_gate = evaluate_trend_quality(
            raw_theme=raw_theme,
            signal_summary={},
            source_refs=[],
            catalog_status="gap",
            latest_observed_at=promo.get("promoted_at"),
        )
        concept_design_brief = build_concept_design_brief(
            raw_theme=raw_theme,
            signal_summary={},
            source_refs=[],
            catalog_status="gap",
            latest_observed_at=promo.get("promoted_at"),
            review_status="operator_promoted",
            confidence=0.7,
            trend_quality_gate=trend_quality_gate,
            brief_source="build_next",
        )
        theme = _clean_theme(concept_design_brief.get("concept_title") or raw_theme)
        # Fail closed: an off-policy or non-product-ready name drops to the
        # guardrail lane even though the operator promoted it.
        if trend_quality_gate.get("status") == "blocked_by_policy" or name_quality.get("status") != "product_ready":
            queue_state = "blocked_by_guardrail"
            next_step = "Reframe this promoted concept into a product-ready, public-safe duck before briefing."
        else:
            queue_state = "ready_for_brief_review"
            next_step = "Operator promoted this from Build-Next; send to DuckAgent design_brief_queue for brief approval."
        guardrails = [
            "public_concept_allowed",
            "Do not copy competitor artwork, wording, photos, tags, or listing structure.",
            "Keep this duck-first; avoid readable logos, team marks, brand names, copyrighted characters.",
        ]
        guardrails.extend(str(i) for i in name_quality.get("issues") or [])
        guardrails.extend(str(i) for i in trend_quality_gate.get("issues") or [])
        items.append({
            "concept_id": _stable_id("buildnext", theme, str(promo.get("concept_key") or raw_theme)),
            "source_type": "build_next_promotion",
            "source_artifact_id": promo.get("concept_key"),
            "theme": theme,
            "raw_theme": raw_theme,
            "catalog_status": "gap",
            "queue_state": queue_state,
            "score": 0.8,
            "confidence": 0.7,
            "name_quality": name_quality,
            "trend_quality_gate": trend_quality_gate,
            "concept_design_brief": concept_design_brief,
            "evidence": [
                f"Operator promoted `{raw_theme}` from the Build-Next ranking.",
                f"Competitor listing: {promo.get('listing_id') or 'n/a'}",
            ],
            "guardrails": guardrails,
            "recommended_next_step": next_step,
            "duckagent_task": "design_brief_queue",
            "source_refs": [],
        })
    return items


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


def _customer_ask_items(payload: Any = None) -> list[dict[str, Any]]:
    """Surface 58: turn customer-ask scout candidates (mined from inbox + review
    text by customer_ask_scout.py) into queue items. Mirrors
    _build_next_promotion_items — same name/trend/brief machinery + fail-closed
    guardrail — but lineage is source_type/brief_source='customer_ask' and the
    evidence is the distinct-requester count, so the operator sees WHO asked. The
    subject is normalized through _clean_theme so a customer ask for 'corgi'
    collapses with a trend/competitor 'corgi' via _merge_duplicate_themes."""
    data = payload if payload is not None else load_json(CUSTOMER_ASK_CANDIDATES_PATH, {"candidates": []})
    candidates = data.get("candidates") if isinstance(data, dict) else data
    items: list[dict[str, Any]] = []
    for cand in list(candidates or []):
        if not isinstance(cand, dict):
            continue
        raw_theme = str(cand.get("subject") or "").strip()
        if not raw_theme:
            continue
        requesters = int(cand.get("distinct_requesters") or 1)
        name_quality = concept_name_quality(raw_theme)
        trend_quality_gate = evaluate_trend_quality(
            raw_theme=raw_theme,
            signal_summary={},
            source_refs=[],
            catalog_status="gap",
            latest_observed_at=None,
        )
        concept_design_brief = build_concept_design_brief(
            raw_theme=raw_theme,
            signal_summary={},
            source_refs=[],
            catalog_status="gap",
            latest_observed_at=None,
            review_status="customer_requested",
            confidence=0.6,
            trend_quality_gate=trend_quality_gate,
            brief_source="customer_ask",
        )
        theme = _clean_theme(concept_design_brief.get("concept_title") or raw_theme)
        if trend_quality_gate.get("status") == "blocked_by_policy" or name_quality.get("status") != "product_ready":
            queue_state = "blocked_by_guardrail"
            next_step = "Reframe this customer-requested concept into a product-ready, public-safe duck before briefing."
        else:
            queue_state = "ready_for_brief_review"
            next_step = "Customers asked for this; send to DuckAgent design_brief_queue for brief approval."
        guardrails = [
            "public_concept_allowed",
            "Do not copy competitor artwork, wording, photos, tags, or listing structure.",
            "Keep this duck-first; avoid readable logos, team marks, brand names, copyrighted characters.",
        ]
        guardrails.extend(str(i) for i in name_quality.get("issues") or [])
        guardrails.extend(str(i) for i in trend_quality_gate.get("issues") or [])
        quotes = [str(q) for q in (cand.get("sample_quotes") or [])][:2]
        source_ids = [a for a in (cand.get("source_artifact_ids") or []) if a]
        # Frequency-weighted: 1 requester -> 0.7, 2 -> 0.8, 3+ -> capped 0.9.
        score = min(0.9, 0.6 + 0.1 * max(1, requesters))
        items.append({
            "concept_id": _stable_id("customerask", theme, raw_theme),
            "source_type": "customer_ask",
            "source_artifact_id": source_ids[0] if source_ids else None,
            "theme": theme,
            "raw_theme": raw_theme,
            "catalog_status": "gap",
            "queue_state": queue_state,
            "score": score,
            "confidence": 0.6,
            "name_quality": name_quality,
            "trend_quality_gate": trend_quality_gate,
            "concept_design_brief": concept_design_brief,
            "evidence": [
                f"{requesters} distinct customer(s) asked for this in reviews/inbox.",
                *[f'"{q}"' for q in quotes],
            ],
            "guardrails": guardrails,
            "recommended_next_step": next_step,
            "duckagent_task": "design_brief_queue",
            "source_refs": [{"kind": "customer_ask", "artifact_id": a} for a in source_ids[:5]],
        })
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
        key = _theme_merge_key(item.get("theme"))
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
    state_rank = {"ready_for_brief_review": 0, "watch": 1, "blocked_by_guardrail": 2, "suppressed_by_operator": 3}
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
        "trend_quality_gate": item.get("trend_quality_gate") or {},
        "concept_design_brief": item.get("concept_design_brief") or {},
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
        "source_contract": "duck-ops.product_concept_queue",
        "source_path": str(PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH),
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
    items.extend(_build_next_promotion_items())
    items.extend(_customer_ask_items())  # Surface 58: 5th scout
    if isinstance(learning_payload, dict):
        items.extend(_learning_motif_items(learning_payload))
    if isinstance(learning_payload, dict) and isinstance(benchmark_payload, dict):
        items.extend(_strategy_idea_items(learning_payload, benchmark_payload))

    all_items = _merge_duplicate_themes(items)
    feedback = _load_concept_feedback()
    for item in all_items:
        record = _feedback_for_item(item, feedback)
        if not record:
            continue
        item["operator_feedback"] = {
            "latest_resolution": record.get("latest_resolution") or record.get("resolution"),
            "reason": record.get("reason") or record.get("latest_reason"),
            "recorded_at": record.get("updated_at") or record.get("recorded_at"),
            "source": record.get("source"),
        }
        if _feedback_suppresses_concept(record):
            item["queue_state"] = "suppressed_by_operator"
            item["recommended_next_step"] = "No action needed; operator feedback already resolved or suppressed this concept."
    all_items.sort(key=_queue_rank)

    ready_items = [item for item in all_items if item.get("queue_state") == "ready_for_brief_review"]
    watch_items = [item for item in all_items if item.get("queue_state") == "watch"]
    blocked_items = [item for item in all_items if item.get("queue_state") == "blocked_by_guardrail"]
    suppressed_items = [item for item in all_items if item.get("queue_state") == "suppressed_by_operator"]
    blocked_slot_count = min(3, len(blocked_items))
    watch_slot_count = min(2, len(watch_items))
    ready_slot_count = max(0, max_items - blocked_slot_count - watch_slot_count)
    items = ready_items[:ready_slot_count] + watch_items[:watch_slot_count] + blocked_items[:blocked_slot_count]

    ready_count = len(ready_items)
    watch_count = len(watch_items)
    blocked_count = len(blocked_items)
    suppressed_count = len(suppressed_items)
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
            "concept_feedback": str(PRODUCT_CONCEPT_FEEDBACK_PATH),
            "design_brief_input": str(PRODUCT_CONCEPT_DESIGN_BRIEF_INPUT_PATH),
        },
        "summary": {
            "candidate_count": len(all_items),
            "ready_for_brief_review_count": ready_count,
            "watch_count": watch_count,
            "blocked_by_guardrail_count": blocked_count,
            "suppressed_by_operator_count": suppressed_count,
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
        f"- Suppressed by operator feedback: `{summary.get('suppressed_by_operator_count', 0)}`",
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
        concept_brief = item.get("concept_design_brief") if isinstance(item.get("concept_design_brief"), dict) else {}
        if concept_brief:
            lines.append(f"  Brief: {concept_brief.get('concept_title')} — {concept_brief.get('operator_summary')}")
        trend_quality = item.get("trend_quality_gate") if isinstance(item.get("trend_quality_gate"), dict) else {}
        if trend_quality:
            issue_text = "; ".join([str(value) for value in (trend_quality.get("issues") or [])[:2]])
            warning_text = "; ".join([str(value) for value in (trend_quality.get("warnings") or [])[:2]])
            lines.append(
                f"  Quality: `{trend_quality.get('status') or 'unknown'}`"
                f"{' | Issues: ' + issue_text if issue_text else ''}"
                f"{' | Warnings: ' + warning_text if warning_text else ''}"
            )
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
