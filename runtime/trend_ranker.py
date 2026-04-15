#!/usr/bin/env python3
"""
Phase 3 daily trend ranker for passive OpenClaw evaluation.

This evaluator:
- reads normalized trend candidates
- groups them by theme across days/sources
- scores each theme using the roadmap trend-ranker ruleset
- writes auditable JSON/Markdown decision artifacts
- appends deduped decision history
- writes a daily trend digest for notifier delivery
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from decision_writer import ensure_parent, load_output_patterns, render_pattern, write_decision
from workflow_control import record_workflow_transition


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
NORMALIZED_DIR = STATE_DIR / "normalized"
OUTPUT_DIR = ROOT / "output"

TREND_CANDIDATES_PATH = NORMALIZED_DIR / "trend_candidates.json"
TREND_RANKER_STATE_PATH = STATE_DIR / "trend_ranker_state.json"
DECISION_HISTORY_PATH = STATE_DIR / "decision_history.jsonl"
EVALUATOR_VERSION = 3
GENERIC_THEMES = {"duck", "ducks", "rubber duck", "rubber ducks"}
GENERIC_OPERATOR_THEMES = GENERIC_THEMES | {"jeep duck", "jeep ducks"}
THEME_NOISE_TOKENS = {"duck", "ducks", "rubber", "jeep", "collectible"}
THEME_VARIANT_TOKENS = {
    "pink",
    "black",
    "white",
    "red",
    "blue",
    "green",
    "yellow",
    "purple",
    "orange",
    "gray",
    "grey",
    "brown",
    "tan",
    "gold",
    "silver",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def canonical_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def source_ref_key(ref: dict[str, Any]) -> tuple[Any, ...]:
    return (
        ref.get("path"),
        ref.get("source_type"),
        ref.get("run_id"),
        ref.get("listing_id"),
        ref.get("section"),
    )


def confidence_cap(aggregate: dict[str, Any]) -> float:
    caps = [1.0, 0.75]  # no real outcome loop yet
    source_types = aggregate.get("source_types") or []
    if len(source_types) <= 1:
        caps.append(0.60)
    catalog_status = aggregate.get("catalog_match", {}).get("status")
    if catalog_status == "unknown":
        caps.append(0.70)
    input_cap = aggregate.get("input_confidence_cap")
    if input_cap is not None:
        caps.append(float(input_cap))
    return min(caps)


def normalized_match_keys(decision: dict[str, Any]) -> tuple[str, ...]:
    metadata = decision.get("trend_metadata") or {}
    matches = metadata.get("matching_products") or []
    keys = []
    for item in matches[:3]:
        key = str(item.get("handle") or item.get("product_id") or item.get("title") or "").strip().lower()
        if key:
            keys.append(key)
    return tuple(sorted(keys))


def normalized_theme_tokens(value: str | None) -> tuple[str, ...]:
    raw = (value or "").lower()
    cleaned = []
    token = []
    for char in raw:
        if char.isalnum():
            token.append(char)
            continue
        if token:
            cleaned.append("".join(token))
            token = []
    if token:
        cleaned.append("".join(token))
    return tuple(cleaned)


def theme_family_key(decision: dict[str, Any]) -> tuple[str, ...]:
    tokens = [
        token
        for token in normalized_theme_tokens(decision.get("theme") or decision.get("title") or "")
        if token not in THEME_NOISE_TOKENS and token not in THEME_VARIANT_TOKENS
    ]
    if not tokens:
        tokens = [
            token
            for token in normalized_theme_tokens(decision.get("theme") or decision.get("title") or "")
            if token not in THEME_NOISE_TOKENS
        ]
    return tuple(tokens)


def concept_theme_key(decision: dict[str, Any]) -> tuple[str, ...]:
    concept_noise_tokens = {"duck", "ducks", "rubber", "collectible"}
    tokens = [
        token
        for token in normalized_theme_tokens(decision.get("theme") or decision.get("title") or "")
        if token not in concept_noise_tokens and token not in THEME_VARIANT_TOKENS
    ]
    if not tokens:
        tokens = [
            token
            for token in normalized_theme_tokens(decision.get("theme") or decision.get("title") or "")
            if token not in concept_noise_tokens
        ]
    return tuple(tokens)


def reviewed_equivalence_key(decision: dict[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    return (
        normalized_match_keys(decision),
        theme_family_key(decision),
        str((decision.get("trend_metadata") or {}).get("catalog_status") or ""),
    )


def is_material_change(previous_decision: dict[str, Any], new_decision: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    prev_meta = previous_decision.get("trend_metadata") or {}
    new_meta = new_decision.get("trend_metadata") or {}

    if previous_decision.get("decision") != new_decision.get("decision"):
        reasons.append("trend status changed")
    if previous_decision.get("action_frame") != new_decision.get("action_frame"):
        reasons.append("recommended action changed")
    if prev_meta.get("catalog_status") != new_meta.get("catalog_status"):
        reasons.append("catalog status changed")
    if normalized_match_keys(previous_decision) != normalized_match_keys(new_decision):
        reasons.append("catalog match changed")

    prev_score = float(previous_decision.get("score") or 0)
    new_score = float(new_decision.get("score") or 0)
    if abs(prev_score - new_score) >= 8:
        reasons.append("score changed materially")

    prev_conf = float(previous_decision.get("confidence") or 0)
    new_conf = float(new_decision.get("confidence") or 0)
    if abs(prev_conf - new_conf) >= 0.08:
        reasons.append("confidence changed materially")

    return bool(reasons), reasons


def carry_forward_review(previous: dict[str, Any] | None, new_decision: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if not previous:
        return new_decision, False
    previous_decision = previous.get("decision") or {}
    previous_review = previous_decision.get("human_review")
    if not previous_review:
        return new_decision, False

    material_change, reasons = is_material_change(previous_decision, new_decision)
    if material_change:
        new_decision["review_status"] = "pending"
        new_decision["previous_human_review"] = previous_review
        suggestions = new_decision.setdefault("improvement_suggestions", [])
        summary = "Previously reviewed, but resurfaced because " + ", ".join(reasons[:3]) + "."
        if summary not in suggestions:
            suggestions.insert(0, summary)
        return new_decision, False

    new_decision["human_review"] = previous_review
    new_decision["review_status"] = previous_decision.get("review_status") or "approved"
    if previous_decision.get("reviewed_at"):
        new_decision["reviewed_at"] = previous_decision.get("reviewed_at")
    return new_decision, True


def normalize_operator_theme(value: str) -> str:
    return " ".join(str(value or "").lower().split())


def should_surface_trend_for_operator(decision: dict[str, Any]) -> bool:
    if decision.get("review_status") != "pending":
        return False
    if decision.get("decision") == "ignore":
        return False
    normalized_theme = normalize_operator_theme(decision.get("theme") or decision.get("title") or "")
    if normalized_theme in GENERIC_OPERATOR_THEMES:
        return False
    action = str(decision.get("action_frame") or "wait")
    if action in {"build", "promote"}:
        return True
    if action != "wait":
        return False
    return bool(
        decision.get("manual_review_requested")
        or decision.get("previous_human_review")
        or decision.get("human_review")
    )


def is_background_watch_trend(decision: dict[str, Any]) -> bool:
    return (
        decision.get("review_status") == "pending"
        and decision.get("decision") != "ignore"
        and not should_surface_trend_for_operator(decision)
    )


def priority_rank(priority: str | None) -> int:
    return {"urgent": 3, "high": 2, "medium": 1, "low": 0}.get(str(priority or "low"), 0)


def decision_latest_marker(decision: dict[str, Any]) -> str:
    metadata = decision.get("trend_metadata") or {}
    return str(
        metadata.get("latest_observed_at")
        or metadata.get("first_seen_at")
        or decision.get("created_at")
        or decision.get("date")
        or ""
    )


def concept_identity(decision: dict[str, Any]) -> tuple[str, ...]:
    family = concept_theme_key(decision)
    if family:
        return ("family", *family)
    match_keys = normalized_match_keys(decision)
    if match_keys:
        return ("catalog", *match_keys)
    tokens = normalized_theme_tokens(decision.get("theme") or decision.get("title") or "")
    if tokens:
        return ("theme", *tokens)
    return ("theme", "unknown")


def slugify_token(value: str) -> str:
    cleaned = []
    for char in str(value or "").lower():
        if char.isalnum():
            cleaned.append(char)
        elif cleaned and cleaned[-1] != "-":
            cleaned.append("-")
    return "".join(cleaned).strip("-") or "unknown"


def concept_id_for_decision(decision: dict[str, Any]) -> str:
    parts = [slugify_token(part) for part in concept_identity(decision)]
    return "trend-concept::" + "--".join(part for part in parts if part)


def reviewed_timestamp(record: dict[str, Any]) -> str:
    decision = record.get("decision") or {}
    human_review = decision.get("human_review") or {}
    return str(human_review.get("recorded_at") or record.get("reviewed_at") or decision_latest_marker(decision))


def concept_candidate_priority(record: dict[str, Any]) -> tuple[int, int, str, int, float, int]:
    decision = record.get("decision") or {}
    return (
        1 if should_surface_trend_for_operator(decision) else 0,
        1 if decision.get("manual_review_requested") else 0,
        decision_latest_marker(decision),
        int(decision.get("score") or 0),
        float(decision.get("confidence") or 0.0),
        priority_rank(decision.get("priority")),
    )


def reviewed_baseline_priority(record: dict[str, Any]) -> tuple[str, int, float, int]:
    decision = record.get("decision") or {}
    return (
        reviewed_timestamp(record),
        int(decision.get("score") or 0),
        float(decision.get("confidence") or 0.0),
        priority_rank(decision.get("priority")),
    )


def concept_aliases(records: list[dict[str, Any]], representative_record: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    preferred = str((representative_record.get("decision") or {}).get("title") or "").strip()
    if preferred:
        aliases.append(preferred)
    for record in sorted(records, key=concept_candidate_priority, reverse=True):
        decision = record.get("decision") or {}
        for value in (decision.get("title"), decision.get("theme")):
            alias = str(value or "").strip()
            if alias and alias not in aliases:
                aliases.append(alias)
    return aliases[:6]


def build_trend_concepts(
    records: dict[str, dict[str, Any]] | list[dict[str, Any]],
    *,
    new_artifact_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    iterable = records.values() if isinstance(records, dict) else records
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in iterable:
        if not isinstance(record, dict):
            continue
        decision = record.get("decision") or {}
        if decision.get("artifact_type") != "trend":
            continue
        concept_id = concept_id_for_decision(decision)
        grouped.setdefault(concept_id, []).append(record)

    concepts: list[dict[str, Any]] = []
    new_artifact_ids = new_artifact_ids or set()
    for concept_id, concept_records in grouped.items():
        reviewed_records = [
            record for record in concept_records if (record.get("decision") or {}).get("human_review")
        ]
        baseline_record = max(reviewed_records, key=reviewed_baseline_priority) if reviewed_records else None

        changed_candidates: list[dict[str, Any]] = []
        if baseline_record is not None:
            baseline_decision = baseline_record.get("decision") or {}
            for record in concept_records:
                if record is baseline_record:
                    continue
                decision = record.get("decision") or {}
                if decision.get("review_status") != "pending":
                    continue
                if decision.get("decision") == "ignore":
                    continue
                material_change, _ = is_material_change(baseline_decision, decision)
                if material_change or decision.get("manual_review_requested"):
                    changed_candidates.append(record)

        if changed_candidates:
            representative_record = max(changed_candidates, key=concept_candidate_priority)
            carried_review = False
        elif baseline_record is not None:
            representative_record = baseline_record
            carried_review = True
        else:
            eligible_records = [
                record
                for record in concept_records
                if (record.get("decision") or {}).get("decision") != "ignore"
            ]
            if not eligible_records:
                eligible_records = list(concept_records)
            representative_record = max(eligible_records, key=concept_candidate_priority)
            carried_review = False

        representative_decision = dict(representative_record.get("decision") or {})
        representative_decision["concept_id"] = concept_id
        representative_decision["concept_aliases"] = concept_aliases(concept_records, representative_record)
        if carried_review and baseline_record is not None:
            representative_decision["carried_concept_review"] = True
            representative_decision["carried_forward_from_artifact_id"] = baseline_record.get("artifact_id")

        operator_surface = should_surface_trend_for_operator(representative_decision)
        if carried_review and baseline_record is not None:
            operator_surface = False
        background_watch = (
            representative_decision.get("review_status") == "pending"
            and representative_decision.get("decision") != "ignore"
            and not operator_surface
        )
        aliases = representative_decision.get("concept_aliases") or []
        concept_title = aliases[0] if aliases else (representative_decision.get("title") or representative_decision.get("theme") or concept_id)
        concepts.append(
            {
                "concept_id": concept_id,
                "artifact_id": representative_record.get("artifact_id"),
                "artifact_type": "trend",
                "title": concept_title,
                "theme": representative_decision.get("theme"),
                "decision": representative_decision.get("decision"),
                "action_frame": representative_decision.get("action_frame"),
                "review_status": representative_decision.get("review_status"),
                "score": representative_decision.get("score"),
                "confidence": representative_decision.get("confidence"),
                "priority": representative_decision.get("priority"),
                "created_at": representative_decision.get("created_at"),
                "reasoning": representative_decision.get("reasoning") or [],
                "improvement_suggestions": representative_decision.get("improvement_suggestions") or [],
                "evidence_refs": representative_decision.get("evidence_refs") or [],
                "trend_metadata": representative_decision.get("trend_metadata") or {},
                "human_review": representative_decision.get("human_review"),
                "previous_human_review": representative_decision.get("previous_human_review"),
                "manual_review_requested": representative_decision.get("manual_review_requested"),
                "concept_aliases": aliases,
                "related_artifact_ids": sorted({str(record.get("artifact_id")) for record in concept_records if record.get("artifact_id")}),
                "operator_surface": operator_surface,
                "background_watch": background_watch,
                "carried_review": carried_review,
                "carried_forward_from_artifact_id": representative_decision.get("carried_forward_from_artifact_id"),
                "output_paths": representative_record.get("output_paths", {}),
                "new_in_run": any(str(record.get("artifact_id")) in new_artifact_ids for record in concept_records),
            }
        )

    concepts.sort(
        key=lambda concept: (
            1 if concept.get("operator_surface") else 0,
            1 if concept.get("manual_review_requested") else 0,
            decision_latest_marker(concept),
            int(concept.get("score") or 0),
            float(concept.get("confidence") or 0.0),
            priority_rank(concept.get("priority")),
        ),
        reverse=True,
    )
    return concepts


def find_review_equivalent(
    artifacts: dict[str, dict[str, Any]],
    new_decision: dict[str, Any],
) -> tuple[str, dict[str, Any]] | None:
    target_key = reviewed_equivalence_key(new_decision)
    if not target_key[0] or not target_key[1]:
        return None

    best: tuple[str, dict[str, Any]] | None = None
    best_reviewed_at = ""
    for artifact_id, record in artifacts.items():
        previous_decision = (record or {}).get("decision") or {}
        previous_review = previous_decision.get("human_review")
        if not previous_review:
            continue
        if previous_decision.get("artifact_type") != "trend":
            continue
        if artifact_id == new_decision.get("artifact_id"):
            continue
        if reviewed_equivalence_key(previous_decision) != target_key:
            continue
        reviewed_at = str(previous_review.get("recorded_at") or record.get("reviewed_at") or "")
        if reviewed_at >= best_reviewed_at:
            best = (artifact_id, record)
            best_reviewed_at = reviewed_at
    return best


def aggregate_trend_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in items:
        theme = (item.get("theme") or item.get("artifact_id") or "unknown").strip()
        key = theme.lower()
        existing = grouped.get(key)
        observed_at = item.get("observed_at")
        first_seen_at = item.get("first_seen_at") or observed_at
        source_refs = item.get("source_refs") or []
        signal_summary = item.get("signal_summary") or {}
        catalog_match = item.get("catalog_match") or {}

        if not existing:
            grouped[key] = {
                "artifact_id": item.get("artifact_id"),
                "artifact_type": "trend",
                "theme": theme,
                "first_seen_at": first_seen_at,
                "latest_observed_at": observed_at,
                "source_refs": list(source_refs),
                "source_types": sorted({ref.get("source_type") for ref in source_refs if ref.get("source_type")}),
                "competitor_run_ids": sorted({ref.get("run_id") for ref in source_refs if ref.get("source_type") == "state_competitor" and ref.get("run_id")}),
                "observed_dates": sorted({(observed_at or "")[:10]} if observed_at else []),
                "signal_summaries": [signal_summary],
                "catalog_match": catalog_match,
                "input_confidence_cap": item.get("input_confidence_cap"),
            }
            continue

        first_dt = parse_dt(existing.get("first_seen_at"))
        item_first_dt = parse_dt(first_seen_at)
        if item_first_dt and (first_dt is None or item_first_dt < first_dt):
            existing["first_seen_at"] = first_seen_at
            existing["artifact_id"] = item.get("artifact_id")

        latest_dt = parse_dt(existing.get("latest_observed_at"))
        item_latest_dt = parse_dt(observed_at)
        if item_latest_dt and (latest_dt is None or item_latest_dt > latest_dt):
            existing["latest_observed_at"] = observed_at
            if catalog_match.get("status") and catalog_match.get("status") != "unknown":
                existing["catalog_match"] = catalog_match

        merged_refs = {source_ref_key(ref): ref for ref in existing.get("source_refs") or []}
        for ref in source_refs:
            merged_refs[source_ref_key(ref)] = ref
        existing["source_refs"] = list(merged_refs.values())
        existing["source_types"] = sorted({ref.get("source_type") for ref in existing["source_refs"] if ref.get("source_type")})
        existing["competitor_run_ids"] = sorted({ref.get("run_id") for ref in existing["source_refs"] if ref.get("source_type") == "state_competitor" and ref.get("run_id")})
        if observed_at:
            existing["observed_dates"] = sorted(set(existing.get("observed_dates") or []).union({observed_at[:10]}))
        existing.setdefault("signal_summaries", []).append(signal_summary)

        input_cap = item.get("input_confidence_cap")
        if input_cap is not None:
            previous_cap = existing.get("input_confidence_cap")
            existing["input_confidence_cap"] = min(previous_cap, input_cap) if previous_cap is not None else input_cap

        if existing.get("catalog_match", {}).get("status") == "unknown" and catalog_match.get("status") != "unknown":
            existing["catalog_match"] = catalog_match

    return sorted(grouped.values(), key=lambda item: (item.get("theme") or "", item.get("first_seen_at") or ""))


def best_numeric(aggregate: dict[str, Any], key: str) -> float:
    values = [safe_number(summary.get(key)) for summary in aggregate.get("signal_summaries") or []]
    cleaned = [value for value in values if value is not None]
    return max(cleaned) if cleaned else 0.0


def quantity_drop(aggregate: dict[str, Any]) -> float:
    best = 0.0
    for summary in aggregate.get("signal_summaries") or []:
        qty = safe_number(summary.get("quantity"))
        prev = safe_number(summary.get("previous_quantity"))
        if qty is None or prev is None:
            continue
        best = max(best, prev - qty)
    return max(0.0, best)


def publication_signals(aggregate: dict[str, Any]) -> tuple[int, bool]:
    coverage = (aggregate.get("catalog_match") or {}).get("publication_coverage") or []
    total_publications = 0
    has_publishable = False
    for item in coverage:
        pubs = item.get("publications") or []
        total_publications += len(pubs)
        if item.get("tiktok_publishable"):
            has_publishable = True
    return total_publications, has_publishable


def choose_action_frame(decision: str, aggregate: dict[str, Any], limited_coverage: bool) -> str:
    if decision == "ignore":
        return "ignore"
    if decision == "watch":
        return "wait"

    status = (aggregate.get("catalog_match") or {}).get("status")
    if status == "gap":
        return "build"
    if status == "partial":
        return "build" if not limited_coverage else "promote"
    if status == "covered":
        return "promote"
    return "wait"


def evaluate_trend(aggregate: dict[str, Any]) -> dict[str, Any]:
    theme = aggregate.get("theme") or "unknown"
    normalized_theme = " ".join(str(theme).lower().split())
    if normalized_theme in GENERIC_THEMES:
        return {
            "artifact_id": aggregate["artifact_id"],
            "artifact_type": "trend",
            "theme": theme,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "decision": "ignore",
            "score": 0,
            "confidence": 0.25,
            "priority": "low",
            "reasoning": ["Theme is too generic to review meaningfully."],
            "improvement_suggestions": ["Ignore generic trend names like `duck` unless a more specific concept appears."],
            "evidence_refs": [],
            "review_status": "approved",
            "created_at": now_iso(),
            "action_frame": "ignore",
            "title": theme.title(),
            "trend_metadata": {
                "first_seen_at": aggregate.get("first_seen_at"),
                "latest_observed_at": aggregate.get("latest_observed_at"),
                "source_types": aggregate.get("source_types") or [],
                "distinct_days": len(aggregate.get("observed_dates") or []),
                "competitor_days": len(aggregate.get("competitor_run_ids") or []),
                "catalog_status": "unknown",
                "matching_products": [],
                "component_scores": {},
                "signal_summary": {},
                "confidence_cap": 0.25,
                "fail_closed": ["Theme is too generic to evaluate."],
            },
        }
    source_types = aggregate.get("source_types") or []
    competitor_days = len(aggregate.get("competitor_run_ids") or [])
    observed_dates = aggregate.get("observed_dates") or []
    first_seen = parse_dt(aggregate.get("first_seen_at"))
    latest_seen = parse_dt(aggregate.get("latest_observed_at"))
    span_days = max(0, (latest_seen - first_seen).days) if first_seen and latest_seen else 0
    catalog_match = aggregate.get("catalog_match") or {}
    catalog_status = catalog_match.get("status") or "unknown"
    matching_products = catalog_match.get("matching_products") or []
    total_publications, has_publishable = publication_signals(aggregate)
    limited_coverage = bool(matching_products) and (total_publications == 0 or not has_publishable)

    sold_7d = best_numeric(aggregate, "sold_last_7d") or best_numeric(aggregate, "sales_7d")
    sold_30d = best_numeric(aggregate, "sold_last_30d") or best_numeric(aggregate, "sales_30d")
    qty_drop = quantity_drop(aggregate)
    trending_score = best_numeric(aggregate, "trending_score")
    engagement_delta = best_numeric(aggregate, "engagement_delta_7d")
    views_delta = best_numeric(aggregate, "views_delta_7d")
    favorites_delta = best_numeric(aggregate, "favorites_delta_7d")

    reasoning: list[str] = []
    suggestions: list[str] = []
    fail_closed: list[str] = []

    # 1. Commercial signal strength (30)
    commercial = 0
    if sold_7d >= 5:
        commercial += 12
    elif sold_7d >= 3:
        commercial += 9
    elif sold_7d >= 1:
        commercial += 5

    if sold_30d >= 10:
        commercial += 6
    elif sold_30d >= 5:
        commercial += 4
    elif sold_30d >= 1:
        commercial += 2

    if qty_drop >= 5:
        commercial += 7
    elif qty_drop >= 2:
        commercial += 5
    elif qty_drop >= 1:
        commercial += 3

    if competitor_days >= 3:
        commercial += 3
    elif competitor_days >= 2:
        commercial += 2

    if trending_score >= 1000:
        commercial += 2
    elif trending_score >= 700:
        commercial += 1

    if commercial == 0 and (engagement_delta > 0 or views_delta > 0 or favorites_delta > 0):
        commercial = min(8, 2 + int(engagement_delta >= 50) + int(views_delta >= 50) + int(favorites_delta >= 5))
    commercial = int(clamp(commercial, 0, 30))
    reasoning.append(
        f"Commercial signal {commercial}/30 from sold 7d `{int(sold_7d) if sold_7d else 0}`, sold 30d `{int(sold_30d) if sold_30d else 0}`, quantity drop `{int(qty_drop)}`, and {competitor_days} competitor observation day(s)."
    )

    # 2. Persistence (20)
    distinct_days = len(observed_dates)
    persistence = 2
    if distinct_days >= 5:
        persistence = 16
    elif distinct_days == 4:
        persistence = 14
    elif distinct_days == 3:
        persistence = 10
    elif distinct_days == 2:
        persistence = 6
    if span_days >= 7:
        persistence += 4
    elif span_days >= 3:
        persistence += 2
    if sold_7d > 0 and sold_30d > 0:
        persistence += 2
    persistence = int(clamp(persistence, 0, 20))
    reasoning.append(
        f"Persistence {persistence}/20 from {distinct_days} observed day(s) spanning {span_days} day(s)."
    )

    # 3. Corroboration (15)
    corroboration = 0
    if "state_competitor" in source_types:
        corroboration += 5
    if "weekly_insights" in source_types:
        corroboration += 4
    if "product_recommendations" in source_types:
        corroboration += 4
    if "reddit_signal_history" in source_types:
        corroboration += 2
    corroboration = int(clamp(corroboration, 0, 15))
    reasoning.append(
        f"Corroboration {corroboration}/15 from sources: {', '.join(source_types) if source_types else 'none'}."
    )

    # 4. Catalog gap or coverage clarity (15)
    if catalog_status == "gap":
        catalog_score = 15
    elif catalog_status == "partial":
        catalog_score = 11
    elif catalog_status == "covered":
        catalog_score = 9 if limited_coverage else 6
    else:
        catalog_score = 4
    catalog_score = int(clamp(catalog_score, 0, 15))
    reasoning.append(
        f"Catalog clarity {catalog_score}/15 with status `{catalog_status}` and {len(matching_products)} matching product(s)."
    )

    # 5. Execution feasibility (10)
    estimated_effort_values = {(summary.get("estimated_effort") or "").lower() for summary in aggregate.get("signal_summaries") or []}
    expected_impact_values = {(summary.get("expected_impact") or "").lower() for summary in aggregate.get("signal_summaries") or []}
    execution = 8 if catalog_status == "covered" else 7 if catalog_status == "partial" else 6
    lowered_theme = theme.lower()
    if any(term in lowered_theme for term in ("custom", "personalized", "occasion", "eco friendly", "eco-friendly")):
        execution -= 2
    if "low" in estimated_effort_values:
        execution += 2
    elif "medium" in estimated_effort_values:
        execution += 1
    elif "high" in estimated_effort_values:
        execution -= 1
    if "high" in expected_impact_values:
        execution += 1
    execution = int(clamp(execution, 0, 10))
    reasoning.append(
        f"Execution feasibility {execution}/10 from catalog status `{catalog_status}` and effort hints `{', '.join(sorted(v for v in estimated_effort_values if v)) or 'none'}`."
    )

    # 6. Historical hit rate (10)
    historical = 4
    if "weekly_insights" in source_types:
        historical += 3
    if "product_recommendations" in source_types:
        historical += 2
    if matching_products:
        historical += 1
    historical = int(clamp(historical, 0, 10))
    reasoning.append(
        f"Historical fit {historical}/10 from DuckAgent recommendation/insight overlap and existing catalog evidence."
    )

    score = int(clamp(commercial + persistence + corroboration + catalog_score + execution + historical, 0, 100))

    meaningful_commercial = sold_7d >= 2 or sold_30d >= 5 or qty_drop >= 2 or (competitor_days >= 2 and trending_score >= 700)
    if not meaningful_commercial:
        fail_closed.append("No meaningful commercial signal backs this theme yet.")
    if distinct_days <= 1 and len(source_types) <= 1:
        fail_closed.append("Theme appears only once with no corroboration.")
    if catalog_status == "unknown" and not meaningful_commercial:
        fail_closed.append("Catalog match is unknown and the signal is still weak.")

    if fail_closed:
        decision = "ignore" if score < 60 else "watch"
    elif score >= 75:
        decision = "worth_acting_on"
    elif score >= 45:
        decision = "watch"
    else:
        decision = "ignore"

    priority = "urgent" if score >= 85 and qty_drop >= 3 and span_days <= 7 else "high" if score >= 75 else "medium" if score >= 55 else "low"
    action_frame = choose_action_frame(decision, aggregate, limited_coverage)

    if catalog_status == "covered":
        match_titles = ", ".join((item.get("title") or item.get("handle") or "existing product") for item in matching_products[:2])
        suggestions.append(f"You already have coverage here; prefer promotion over building. Closest match: {match_titles}.")
        if limited_coverage:
            suggestions.append("Check publication coverage and channel readiness, because the matching duck may be under-published compared with the competitor.")
        else:
            suggestions.append("Review why competitor listings are outperforming yours: title fit, freshness, social proof, and promotion cadence.")
    elif catalog_status == "partial":
        suggestions.append("You have adjacent coverage but not a clean direct match; validate whether a tighter variant or refreshed promotion would close the gap faster.")
    elif catalog_status == "gap":
        suggestions.append("This looks like a real catalog gap. If the signal persists, queue it as a build candidate rather than waiting for another weekly cycle.")
    else:
        suggestions.append("Catalog coverage is unclear. Verify whether you already have a close variant before treating this as a true gap.")

    if not meaningful_commercial:
        suggestions.append("Wait for another day of sales or inventory-drop evidence before treating this as a build-or-promote decision.")

    raw_confidence = 0.42
    raw_confidence += min(0.10, 0.04 * max(0, len(source_types) - 1))
    raw_confidence += min(0.08, 0.02 * max(0, distinct_days - 1))
    raw_confidence += 0.05 if meaningful_commercial else 0.0
    raw_confidence += 0.03 if catalog_status != "unknown" else 0.0
    confidence = round(clamp(raw_confidence, 0.25, confidence_cap(aggregate)), 2)

    if fail_closed:
        reasoning.extend(f"Fail-closed trigger: {message}" for message in fail_closed)

    evidence_refs = []
    for ref in aggregate.get("source_refs") or []:
        path = ref.get("path")
        if path and path not in evidence_refs:
            evidence_refs.append(path)
    for ref in (aggregate.get("source_refs") or [])[:5]:
        listing_id = ref.get("listing_id")
        if listing_id:
            evidence_refs.append(f"competitor_listing:{listing_id}")
    evidence_refs = evidence_refs[:10]

    return {
        "artifact_id": aggregate["artifact_id"],
        "artifact_type": "trend",
        "theme": theme,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "decision": decision,
        "score": score,
        "confidence": confidence,
        "priority": priority,
        "reasoning": reasoning,
        "improvement_suggestions": suggestions[:4],
        "evidence_refs": evidence_refs,
        "review_status": "pending",
        "created_at": now_iso(),
        "action_frame": action_frame,
        "title": theme.title(),
        "trend_metadata": {
            "first_seen_at": aggregate.get("first_seen_at"),
            "latest_observed_at": aggregate.get("latest_observed_at"),
            "source_types": source_types,
            "distinct_days": distinct_days,
            "competitor_days": competitor_days,
            "catalog_status": catalog_status,
            "matching_products": [
                {
                    "product_id": item.get("product_id"),
                    "title": item.get("title"),
                    "handle": item.get("handle"),
                    "on_sale": item.get("on_sale"),
                    "tiktok_publishable": item.get("tiktok_publishable"),
                }
                for item in matching_products[:3]
            ],
            "component_scores": {
                "commercial_signal_strength": commercial,
                "persistence": persistence,
                "corroboration": corroboration,
                "catalog_gap_or_coverage_clarity": catalog_score,
                "execution_feasibility": execution,
                "historical_hit_rate": historical,
            },
            "signal_summary": {
                "sold_last_7d": sold_7d,
                "sold_last_30d": sold_30d,
                "quantity_drop": qty_drop,
                "engagement_delta_7d": engagement_delta,
                "views_delta_7d": views_delta,
                "favorites_delta_7d": favorites_delta,
                "trending_score": trending_score,
            },
            "confidence_cap": confidence_cap(aggregate),
            "fail_closed": fail_closed,
        },
    }


def load_state() -> dict[str, Any]:
    return load_json(TREND_RANKER_STATE_PATH, {"artifacts": {}, "last_digest_date": None})


def save_state(state: dict[str, Any]) -> None:
    state = sync_trend_ranker_control(state)
    write_json(TREND_RANKER_STATE_PATH, state)


def sync_trend_ranker_control(state: dict[str, Any]) -> dict[str, Any]:
    concepts = state.get("concepts") or {}
    artifacts = state.get("artifacts") or {}
    concept_records = build_trend_concepts(artifacts)
    operator_surface_count = 0
    pending_review_count = 0
    actionable_pending_review_count = 0
    backlog_pending_review_count = 0
    new_in_run_count = 0
    latest_concept_dt: datetime | None = None

    trend_queue_iterable: list[dict[str, Any]]
    if concept_records:
        trend_queue_iterable = concept_records
    else:
        trend_queue_iterable = [concept for concept in concepts.values() if isinstance(concept, dict)]

    for concept in trend_queue_iterable:
        is_pending = str(concept.get("review_status") or "").strip().lower() == "pending"
        if is_pending:
            pending_review_count += 1
        if concept_records:
            if concept.get("operator_surface"):
                actionable_pending_review_count += 1
                operator_surface_count += 1
            elif concept.get("background_watch"):
                backlog_pending_review_count += 1
        else:
            if should_surface_trend_for_operator(concept):
                actionable_pending_review_count += 1
                operator_surface_count += 1
            elif is_pending:
                backlog_pending_review_count += 1
        if concept.get("new_in_run"):
            new_in_run_count += 1
        concept_dt = parse_dt(concept.get("created_at"))
        if concept_dt and (latest_concept_dt is None or concept_dt > latest_concept_dt):
            latest_concept_dt = concept_dt

    digest_dt = parse_dt(state.get("last_digest_date"))
    reference_dt = latest_concept_dt if latest_concept_dt and (digest_dt is None or latest_concept_dt >= digest_dt) else digest_dt
    age_hours = None
    if reference_dt is not None:
        age_hours = round((datetime.now(timezone.utc).astimezone() - reference_dt).total_seconds() / 3600.0, 2)

    if age_hours is not None and age_hours >= 72:
        control_state = "blocked"
        reason = "stale_input"
        next_action = "Refresh the trend ranker so operator-facing signals match the current catalog and search data."
    elif actionable_pending_review_count:
        control_state = "observed"
        reason = "pending_review"
        next_action = "Review the actionable trend concepts in the surfaced queue and either approve, revise, or archive them."
    elif backlog_pending_review_count:
        control_state = "verified"
        reason = "backlog_outside_operator_queue"
        next_action = "No operator action is needed right now; the remaining pending trend backlog is outside the surfaced review queue."
    elif new_in_run_count:
        control_state = "observed"
        reason = "background_refresh"
        next_action = "Trend monitoring is refreshing in the background; no operator action is required yet."
    elif concepts or artifacts:
        control_state = "verified"
        reason = "ranked_ready"
        next_action = "Use the trend ranker output as the current signal summary."
    else:
        control_state = "observed"
        reason = "idle"
        next_action = "No trend concepts are available yet."

    control = record_workflow_transition(
        workflow_id="trend_ranker",
        lane="trend_ranker",
        display_label="Trend Ranker",
        entity_id="trend_ranker",
        state=control_state,
        state_reason=reason,
        input_freshness={
            "source": str(TREND_RANKER_STATE_PATH),
            "age_hours": age_hours,
        },
        next_action=next_action,
        metadata={
            "concept_count": len(concepts),
            "artifact_count": len(artifacts),
            "operator_surface_count": operator_surface_count,
            "pending_review_count": pending_review_count,
            "actionable_pending_review_count": actionable_pending_review_count,
            "backlog_pending_review_count": backlog_pending_review_count,
            "new_in_run_count": new_in_run_count,
        },
        receipt_kind="state_sync",
        receipt_payload={
            "concept_count": len(concepts),
            "artifact_count": len(artifacts),
            "operator_surface_count": operator_surface_count,
            "pending_review_count": pending_review_count,
            "actionable_pending_review_count": actionable_pending_review_count,
            "backlog_pending_review_count": backlog_pending_review_count,
            "new_in_run_count": new_in_run_count,
        },
        history_summary=reason.replace("_", " "),
    )
    state["workflow_control"] = {
        "state": control_state,
        "state_reason": reason,
        "age_hours": age_hours,
        "path": str((control or {}).get("latest_receipt", {}).get("path") or ""),
    }
    return state


def write_daily_digest(latest_records: dict[str, dict[str, Any]], new_decisions: list[dict[str, Any]]) -> dict[str, str] | None:
    concepts = build_trend_concepts(latest_records, new_artifact_ids={str(item.get("artifact_id")) for item in new_decisions})
    active_decisions = [concept.get("decision") for concept in concepts]
    active_counts = Counter(active_decisions)
    pending_items = [concept for concept in concepts if concept.get("operator_surface")]
    pending_items.sort(key=lambda item: (item.get("score") or 0, item.get("confidence") or 0), reverse=True)
    background_watch_items = [concept for concept in concepts if concept.get("background_watch")]
    background_watch_items.sort(key=lambda item: (item.get("score") or 0, item.get("confidence") or 0), reverse=True)
    new_items = [concept for concept in concepts if concept.get("operator_surface") and concept.get("new_in_run")]
    new_items.sort(key=lambda item: (item.get("score") or 0, item.get("confidence") or 0), reverse=True)
    new_background_watch_items = [concept for concept in concepts if concept.get("background_watch") and concept.get("new_in_run")]
    new_background_watch_items.sort(key=lambda item: (item.get("score") or 0, item.get("confidence") or 0), reverse=True)
    if not new_items and not pending_items and not background_watch_items:
        return None
    payload = {
        "generated_at": now_iso(),
        "type": "trend_ranker_digest",
        "new_decision_count": len(new_items),
        "new_background_watch_count": len(new_background_watch_items),
        "active_counts": dict(active_counts),
        "pending_review_count": len(pending_items),
        "background_watch_count": len(background_watch_items),
        "new_items": new_items[:15],
        "pending_items": pending_items[:15],
        "background_watch_items": background_watch_items[:15],
        "items": pending_items[:15],
    }

    patterns = load_output_patterns()
    current = datetime.now()
    replacements = {"YYYY-MM-DD": current.strftime("%Y-%m-%d")}
    json_path = render_pattern(patterns["trend_digest_json"], replacements)
    md_path = render_pattern(patterns["trend_digest_md"], replacements)
    ensure_parent(json_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Trend Ranking Daily Digest",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- New operator-facing decisions this run: `{payload['new_decision_count']}`",
        f"- Still pending operator review: `{payload['pending_review_count']}`",
        f"- Silent background watches: `{payload['background_watch_count']}`",
        f"- Worth acting on: `{active_counts.get('worth_acting_on', 0)}`",
        f"- Watch: `{active_counts.get('watch', 0)}`",
        f"- Ignore: `{active_counts.get('ignore', 0)}`",
        "",
        "Silent background watches stay out of WhatsApp unless they become actionable or materially change.",
        "Reviewed or archived trend items are not listed below.",
        "",
    ]
    lines.append("## New Operator Decisions This Run")
    lines.append("")
    if not new_items:
        lines.append("No new operator-facing trend decisions this run.")
    else:
        for item in new_items[:10]:
            lines.append(
                f"- `{item['decision']}` | `{item.get('action_frame')}` | score `{item['score']}` | confidence `{item['confidence']}` | `{item['theme']}`"
            )
            reason = (item.get("reasoning") or ["No reasoning captured."])[0]
            lines.append(f"  Reason: {reason}")
    lines.extend(["", "## Still Pending Operator Review", ""])
    if not pending_items:
        lines.append("No pending operator-facing trend review items.")
    else:
        for item in pending_items[:10]:
            lines.append(
                f"- `{item['decision']}` | `{item.get('action_frame')}` | score `{item['score']}` | confidence `{item['confidence']}` | `{item['theme']}`"
            )
            reason = (item.get("reasoning") or ["No reasoning captured."])[0]
            lines.append(f"  Reason: {reason}")
    lines.extend(["", "## Silent Background Watches", ""])
    if not background_watch_items:
        lines.append("No silent background watches right now.")
    else:
        lines.append(
            f"`{len(background_watch_items)}` trend(s) are being monitored silently because they are still `wait` calls with no new operator decision needed."
        )
        if new_background_watch_items:
            lines.append(
                f"`{len(new_background_watch_items)}` of those were newly observed in this run, but they were kept out of the operator queue."
            )
    ensure_parent(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def main() -> int:
    payload = load_json(TREND_CANDIDATES_PATH, {"items": []})
    raw_items = payload.get("items") or []
    aggregates = aggregate_trend_candidates(raw_items)
    state = load_state()

    history_rows: list[dict[str, Any]] = []
    new_decisions: list[dict[str, Any]] = []
    latest_records: dict[str, dict[str, Any]] = {}

    for aggregate in aggregates:
        decision = evaluate_trend(aggregate)
        decision["concept_id"] = concept_id_for_decision(decision)
        previous = (state.get("artifacts") or {}).get(decision["artifact_id"])
        carry_source_artifact_id = decision["artifact_id"]
        equivalent = None
        previous_decision = (previous or {}).get("decision") or {}
        if previous is None or not previous_decision.get("human_review"):
            equivalent = find_review_equivalent(state.get("artifacts") or {}, decision)
        source_record = previous if previous is not None else (equivalent[1] if equivalent else None)
        if equivalent and (previous is None or not previous_decision.get("human_review")):
            source_record = equivalent[1]
        if equivalent:
            carry_source_artifact_id = equivalent[0]

        decision, carried_forward = carry_forward_review(source_record, decision)
        if equivalent and carried_forward:
            decision["carried_forward_from_artifact_id"] = carry_source_artifact_id
            suggestions = decision.setdefault("improvement_suggestions", [])
            summary = f"Suppressed repeat review because this matches the previously reviewed trend `{carry_source_artifact_id}`."
            if summary not in suggestions:
                suggestions.insert(0, summary)
        elif equivalent and decision.get("previous_human_review"):
            decision["previous_human_review"]["carried_forward_from_artifact_id"] = carry_source_artifact_id
        input_hash = canonical_hash(
            {
                "aggregate": aggregate,
                "evaluator_version": EVALUATOR_VERSION,
            }
        )
        decision_changed = canonical_hash(decision) != canonical_hash((previous or {}).get("decision") or {})
        if (
            previous
            and previous.get("input_hash") == input_hash
            and previous.get("evaluator_version") == EVALUATOR_VERSION
            and not decision_changed
        ):
            latest_records[decision["artifact_id"]] = previous
            continue

        output_paths = write_decision(decision)
        record = {
            "artifact_id": decision["artifact_id"],
            "input_hash": input_hash,
            "evaluated_at": now_iso(),
            "evaluator_version": EVALUATOR_VERSION,
            "decision": decision,
            "output_paths": output_paths,
        }
        state.setdefault("artifacts", {})[decision["artifact_id"]] = record
        latest_records[decision["artifact_id"]] = record
        if not carried_forward:
            new_decisions.append(decision)
            history_rows.append(
                {
                    "evaluator": "trend_ranker",
                    "artifact_id": decision["artifact_id"],
                    "theme": decision.get("theme"),
                    "decision": decision.get("decision"),
                    "action_frame": decision.get("action_frame"),
                    "score": decision.get("score"),
                    "confidence": decision.get("confidence"),
                    "priority": decision.get("priority"),
                    "input_hash": input_hash,
                    "evaluated_at": record["evaluated_at"],
                }
            )

    for artifact_id, record in (state.get("artifacts") or {}).items():
        latest_records.setdefault(artifact_id, record)

    state["concepts"] = {
        concept["concept_id"]: concept
        for concept in build_trend_concepts(latest_records, new_artifact_ids={str(item.get("artifact_id")) for item in new_decisions})
    }
    write_daily_digest(latest_records, new_decisions)
    append_jsonl(DECISION_HISTORY_PATH, history_rows)
    save_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
