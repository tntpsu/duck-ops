#!/usr/bin/env python3
"""Shared product-concept quality gate and design brief builder."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


BRIEF_SCHEMA_VERSION = "duck.product_concept_brief.v1"
QUALITY_SCHEMA_VERSION = "duck.trend_quality_gate.v1"

MARKETPLACE_RESIDUE_PHRASES = (
    "rubber duck figurine",
    "rubber ducky",
    "rubber duck",
    "jeep ducking",
    "jeep duck",
    "ducks for jeeps",
    "desk decor",
    "car decor",
    "dashboard buddy",
    "dashboard",
    "collectibles",
    "collectible",
    "figurine",
    "fidget toy",
    "fidget",
    "gift idea",
    "gift",
    "gifts",
    "party favor",
    "travel souvenir",
    "ship decoration",
    "3d printed",
    "3d print",
)

IP_SENSITIVE_PHRASES = {
    "alabama": "School/team references need manual abstraction before concepting.",
    "canes": "School/team nickname should not become a logo or trademark concept.",
    "delta gamma": "Greek-letter organization themes need manual abstraction before concepting.",
    "disney": "Named entertainment IP needs explicit abstraction before concepting.",
    "gcu": "School/team references need manual abstraction before concepting.",
    "gamma delta": "Greek-letter organization themes need manual abstraction before concepting.",
    "lopes": "School/team nickname should not become a logo or trademark concept.",
    "marvel": "Named entertainment IP needs explicit abstraction before concepting.",
    "nfl": "Professional sports league references need explicit abstraction before concepting.",
    "nba": "Professional sports league references need explicit abstraction before concepting.",
    "mlb": "Professional sports league references need explicit abstraction before concepting.",
    "pokemon": "Named entertainment IP needs explicit abstraction before concepting.",
    "tennessee vols": "College/team nickname should not become a logo or trademark concept.",
    "vols": "College/team nickname should not become a logo or trademark concept.",
    "wildcats": "Mascot/team references need manual abstraction before concepting.",
    "chicago football": "City-plus-sport themes are likely team-adjacent and need manual abstraction.",
}

GENERIC_TOKENS = {
    "duck",
    "ducks",
    "rubber",
    "jeep",
    "collectible",
    "collectibles",
    "figurine",
    "toy",
    "gift",
    "gifts",
    "decor",
    "dashboard",
    "desk",
    "car",
    "printed",
    "print",
    "3d",
}

ABSTRACT_RELATION_TOKENS = {
    "child",
    "children",
    "maternal",
    "mother",
    "mom",
    "motherhood",
    "love",
    "family",
    "uncle",
    "bond",
    "relationship",
    "relationships",
}

DEMOGRAPHIC_QUALIFIERS = {"female", "male", "girl", "boy"}

LISTING_FRAGMENT_TOKENS = {
    "inch",
    "inches",
    "mm",
    "cm",
    "magnetic",
    "tall",
    "wide",
    "size",
}

COLOR_TOKENS = {
    "black",
    "blue",
    "brown",
    "gray",
    "green",
    "grey",
    "orange",
    "pink",
    "purple",
    "red",
    "tan",
    "white",
    "yellow",
}

SPORT_TOKENS = {"baseball", "basketball", "football", "hockey"}

TEAM_ADJACENT_MODIFIERS = {
    "alabama",
    "anaheim",
    "canes",
    "chicago",
    "desert",
    "diamond",
    "gcu",
    "heartland",
    "high",
    "lopes",
    "michigan",
    "mile",
    "northwoods",
    "tennessee",
    "vols",
    "wildcats",
}

THEME_CUE_LIBRARY = {
    "greyhound": {
        "semantic_identity": "a greyhound dog-breed themed duck collectible",
        "theme_category": "animal",
        "visual_cues": [
            "sleek hound-inspired face markings",
            "folded or floppy attached ear shapes",
            "lean racing-dog markings translated into broad duck body panels",
            "small attached collar detail",
        ],
        "must_avoid": [
            "plain gray duck with no dog-breed cues",
            "realistic dog body replacing the duck body",
            "separate paws, leash, thin tail, or fragile detached props",
        ],
    },
    "german shorthaired pointer": {
        "semantic_identity": "a German Shorthaired Pointer dog-breed themed duck collectible",
        "theme_category": "animal",
        "visual_cues": [
            "solid liver-brown head with floppy attached ear shapes",
            "white duck body with liver roan/ticking speckles plus a few broad liver patches",
            "simple attached collar detail",
            "athletic pointer-dog markings translated into chunky duck body panels without changing the duck silhouette",
        ],
        "must_avoid": [
            "plain brown or spotted duck with no dog-breed cues",
            "mostly large cow-print blotches with little or no German Shorthaired Pointer roan/ticking",
            "realistic dog body replacing the duck body",
            "separate paws, thin tail, hunting weapon, or fragile detached field props",
            "generic paw-print decorations that read like clipart instead of breed-specific markings",
        ],
    },
    "tuxedo cat": {
        "semantic_identity": "a tuxedo-cat themed duck collectible",
        "theme_category": "animal",
        "visual_cues": [
            "bold black-and-white tuxedo cat markings",
            "chunky attached cat-ear or cheek-fur cues only if they keep the duck silhouette",
            "clear white chest and face contrast",
        ],
        "must_avoid": [
            "loose paws or separate feet",
            "a realistic cat body replacing the duck body",
            "tiny whiskers or fragile detached cat parts",
        ],
    },
    "highland cow": {
        "semantic_identity": "a highland-cow themed duck collectible",
        "theme_category": "animal",
        "visual_cues": [
            "chunky shaggy hair/fringe translated into broad printable shapes",
            "rounded attached horn shapes",
            "warm tan or rust color blocking that still leaves the beak and duck face readable",
        ],
        "must_avoid": [
            "thin horn tips or dangling hair strands",
            "realistic cow body replacing the duck body",
        ],
    },
    "nurse": {
        "semantic_identity": "a nurse-themed duck collectible",
        "theme_category": "occupation",
        "visual_cues": [
            "simple nurse cap or scrub color cue",
            "clean medical-color palette with attached chunky details",
            "friendly caregiver expression while keeping the duck identity obvious",
        ],
        "must_avoid": [
            "gendered labeling as the main concept",
            "tiny medical props or readable medical brand logos",
        ],
    },
    "cowgirl": {
        "semantic_identity": "a western cowgirl-themed duck collectible",
        "theme_category": "occupation",
        "visual_cues": [
            "chunky attached western hat shape",
            "simple bandana or vest color blocking",
            "warm western palette while keeping the beak and duck face clear",
        ],
        "must_avoid": [
            "thin lasso loops or fragile detached props",
            "realistic human body or separate boots replacing the duck body",
        ],
    },
}


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def token_list(value: Any) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(value or "").lower())


def _contains_phrase(text: str, phrase: str) -> bool:
    return f" {phrase} " in f" {text} "


def _title_case_phrase(value: str) -> str:
    return " ".join(token.capitalize() if not token.isupper() else token for token in value.split())


def clean_product_theme(value: Any) -> str:
    """Return a product-facing theme without marketplace keyword residue."""
    text = normalize_text(value)
    for phrase in MARKETPLACE_RESIDUE_PHRASES:
        text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text)
    tokens = [token for token in text.split() if token and token not in {"duck", "ducks"}]
    if len(tokens) > 1 and tokens[0] in {"female", "male"}:
        tokens = tokens[1:]
    if not tokens:
        tokens = [token for token in normalize_text(value).split() if token not in GENERIC_TOKENS]
    if not tokens:
        tokens = ["fresh", "duck", "concept"]
    return _title_case_phrase(" ".join(tokens)).strip()


def concept_title(value: Any) -> str:
    theme = clean_product_theme(value)
    if theme.lower().endswith(" duck"):
        return theme
    return f"{theme} Duck"


def _matched_theme_key(theme: str) -> str | None:
    text = normalize_text(theme)
    for key in sorted(THEME_CUE_LIBRARY, key=len, reverse=True):
        if _contains_phrase(text, key):
            return key
    return None


def _quality_status(blockers: list[str], warnings: list[str], stale_days: int | None) -> str:
    if blockers:
        return "blocked_by_policy"
    if stale_days is not None and stale_days > 45:
        return "needs_refresh"
    if warnings:
        return "needs_reframe"
    return "ready"


def _staleness_days(latest_observed_at: Any, *, now: datetime | None = None) -> int | None:
    if not latest_observed_at:
        return None
    try:
        observed = datetime.fromisoformat(str(latest_observed_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc).astimezone()
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return max(0, (reference - observed.astimezone(reference.tzinfo)).days)


def evaluate_trend_quality(
    *,
    raw_theme: Any,
    signal_summary: dict[str, Any] | None = None,
    source_refs: list[dict[str, Any]] | None = None,
    catalog_status: str | None = None,
    latest_observed_at: Any = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify whether a market signal is ready to become a buildable concept."""
    text = normalize_text(raw_theme)
    tokens = token_list(raw_theme)
    signal_summary = signal_summary or {}
    source_refs = source_refs or []
    blockers: list[str] = []
    warnings: list[str] = []

    meaningful = [token for token in tokens if token not in GENERIC_TOKENS]
    abstract = [token for token in meaningful if token in ABSTRACT_RELATION_TOKENS]
    concrete = [token for token in meaningful if token not in ABSTRACT_RELATION_TOKENS and token not in DEMOGRAPHIC_QUALIFIERS]

    if not meaningful:
        blockers.append("Theme is too generic to become a product concept.")
    if len(abstract) >= 2 and not concrete:
        blockers.append("Theme reads like relationship/search wording, not a product-ready visual concept.")

    for phrase, message in IP_SENSITIVE_PHRASES.items():
        if _contains_phrase(text, phrase):
            blockers.append(message)
    if any(token in SPORT_TOKENS for token in meaningful) and any(token in TEAM_ADJACENT_MODIFIERS for token in meaningful):
        blockers.append("City, school, mascot, or region-plus-sport themes need a public-safe abstraction before concepting.")

    if any(_contains_phrase(text, phrase) for phrase in MARKETPLACE_RESIDUE_PHRASES):
        warnings.append("Theme contains marketplace keyword residue; use the cleaned concept title before generation.")
    if any(token.isdigit() for token in meaningful) and any(token in LISTING_FRAGMENT_TOKENS for token in meaningful):
        blockers.append("Theme appears to contain listing-size, SKU, or product-feature fragments instead of a product concept.")
    elif any(token.isdigit() for token in meaningful):
        warnings.append("Theme contains numeric listing language; confirm the concept meaning before generation.")
    if tokens and tokens[0] in {"female", "male"}:
        warnings.append("Gender qualifier should not carry the product concept unless the operator explicitly wants it.")
    if any(token in ABSTRACT_RELATION_TOKENS for token in meaningful):
        warnings.append("Relationship or persona wording should be reframed into a clear visual product concept.")
    non_duck = [token for token in meaningful if token not in DEMOGRAPHIC_QUALIFIERS]
    if non_duck and all(token in COLOR_TOKENS for token in non_duck):
        warnings.append("Theme is color-only and needs a concrete subject before generation.")

    sold_7d = _as_float(signal_summary.get("sold_last_7d"))
    sold_30d = _as_float(signal_summary.get("sold_last_30d"))
    if sold_7d is not None and sold_30d is not None and sold_30d > 0 and sold_7d > sold_30d:
        warnings.append("Sales evidence is internally inconsistent: 7-day sales exceed 30-day sales.")

    stale_days = _staleness_days(latest_observed_at, now=now)
    if stale_days is not None and stale_days > 21:
        warnings.append(f"Latest market observation is {stale_days} day(s) old; refresh before trusting urgency.")

    if not source_refs:
        warnings.append("No source references are attached to the trend signal.")

    status = _quality_status(blockers, warnings, stale_days)
    return {
        "schema_version": QUALITY_SCHEMA_VERSION,
        "status": status,
        "generation_ready": status in {"ready", "needs_reframe"},
        "normalized_concept_title": concept_title(raw_theme),
        "issues": blockers,
        "warnings": warnings,
        "catalog_status": catalog_status or "unknown",
        "source_ref_count": len(source_refs),
        "latest_observed_at": str(latest_observed_at or ""),
        "staleness_days": stale_days,
        "checked_at": now_iso(),
    }


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _brief_library(theme: str) -> dict[str, Any]:
    key = _matched_theme_key(theme)
    if key:
        return dict(THEME_CUE_LIBRARY[key])
    clean = clean_product_theme(theme).lower()
    return {
        "semantic_identity": f"a {clean} themed duck collectible",
        "theme_category": "novelty",
        "visual_cues": [
            f"two or three chunky attached cues that make `{clean}` obvious at a glance",
            "bold color blocking with clear separation between body, face, and key details",
            "simple printable shapes instead of thin props",
        ],
        "must_avoid": [],
    }


def build_concept_design_brief(
    *,
    raw_theme: Any,
    signal_summary: dict[str, Any] | None = None,
    source_refs: list[dict[str, Any]] | None = None,
    catalog_status: str | None = None,
    latest_observed_at: Any = None,
    review_status: str | None = None,
    confidence: float | None = None,
    trend_quality_gate: dict[str, Any] | None = None,
    brief_source: str = "duck_ops_trend_ranker",
) -> dict[str, Any]:
    signal_summary = signal_summary or {}
    source_refs = source_refs or []
    gate = trend_quality_gate or evaluate_trend_quality(
        raw_theme=raw_theme,
        signal_summary=signal_summary,
        source_refs=source_refs,
        catalog_status=catalog_status,
        latest_observed_at=latest_observed_at,
    )
    title = str(gate.get("normalized_concept_title") or concept_title(raw_theme))
    library = _brief_library(title)

    must_avoid = [
        "plain duck with only a color change",
        "realistic animal or human body replacing the duck body",
        "protected logos, brand text, team marks, or competitor-specific artwork",
        "thin detached props, extra limbs, separate paws, or fragile parts",
    ]
    must_avoid.extend(str(item) for item in library.get("must_avoid") or [])
    must_avoid.extend(str(item) for item in (gate.get("issues") or [])[:3])

    evidence_summary = [
        f"Catalog status: {catalog_status or 'unknown'}",
        f"Source refs: {len(source_refs)}",
    ]
    sold_7d = signal_summary.get("sold_last_7d")
    sold_30d = signal_summary.get("sold_last_30d")
    trending_score = signal_summary.get("trending_score")
    if sold_7d is not None:
        evidence_summary.append(f"7d sold: {sold_7d}")
    if sold_30d is not None:
        evidence_summary.append(f"30d sold: {sold_30d}")
    if trending_score is not None:
        evidence_summary.append(f"Trending score: {trending_score}")

    return {
        "schema_version": BRIEF_SCHEMA_VERSION,
        "brief_source": brief_source,
        "generated_at": now_iso(),
        "concept_title": title,
        "raw_theme": str(raw_theme or ""),
        "semantic_identity": str(library.get("semantic_identity") or title),
        "theme_category": str(library.get("theme_category") or "novelty"),
        "operator_summary": (
            f"Build {title} as a readable MyJeepDuck-style product concept. "
            "The theme should be clear from the image without relying on the title."
        ),
        "visual_cues": [str(item) for item in (library.get("visual_cues") or [])],
        "must_preserve": [
            "flat-bottom duck body",
            "visible beak and friendly duck face",
            "MyJeepDuck bright, chunky, playful, printable style",
            "centered product-style composition",
        ],
        "must_avoid": list(dict.fromkeys(must_avoid)),
        "printability_guardrails": [
            "single-piece squat duck body",
            "theme details are attached and broad",
            "no thin dangling parts or loose accessories",
        ],
        "ip_copy_risks": [
            "do not copy competitor art, photos, tags, exact wording, or listing structure",
            "avoid protected logos, team marks, brand names, or copyrighted characters",
        ],
        "style_reference_policy": {
            "use_style_memory": True,
            "reference_examples": [],
            "do_not_copy_examples": True,
        },
        "evidence_summary": evidence_summary,
        "source_refs": source_refs[:10],
        "confidence": confidence,
        "review_status": review_status or "",
        "trend_quality_gate": gate,
    }
