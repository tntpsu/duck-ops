"""Surface 16 producer: rank "what duck should we build next?" into one
weekly decision queue, writing state/build_next_queue.json.

The score fuses four signals, each normalized 0..1 with a reasons[] trail
(same transparency contract as the occasion selector):

  score = demand x margin x catalog_gap x occasion_fit

  - demand       competitor pull, ranked on recent MOMENTUM (trending_score
                 = sold_7d-primary + 30d-rate ballast, computed by the
                 competitor engine), max-normalized across the candidate
                 pool; falls back to all-time engagement only when a row
                 carries no momentum signal
  - margin       profit_per_product margin_pct for the nearest confident
                 title match; a neutral estimate (flagged) when no match
  - catalog_gap  1 - overlap with our existing catalog (already-made ->
                 low gap -> suppressed); deterministic token overlap
                 against catalog_index core_terms/title, NO LLM
  - occasion_fit boost when the concept hits an ACTIVE occasion window
                 (read from occasion_intel.json), neutral when evergreen

Everything is deterministic and reads only EXISTING state — no new data
collection, no LLM in the scorer, no eval-gate machinery (convention #5
applies only to LLM-output surfaces). Promote is a separate, approval-
gated step that routes into product_concept_queue; this producer never
spends image/3D credits.

Producer-on-schedule + cheap-reader: weekly (Sunday 07:00, after the
06:30 competitor weekly analysis); readers parse the small state file in
~2ms.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

from governance_review_common import DUCK_AGENT_ROOT, DUCK_OPS_ROOT, load_json, now_local_iso
from workflow_control import TestModeRefusalError
# Reuse the concept-queue feedback contract so a rejected concept stays
# suppressed here too (no parallel, drifting suppression list).
from product_concept_queue import (
    PRODUCT_CONCEPT_FEEDBACK_PATH,
    SUPPRESSING_FEEDBACK_RESOLUTIONS,
    _concept_feedback_key,
)

CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"
OCCASION_INTEL_PATH = DUCK_OPS_ROOT / "state" / "occasion_intel.json"
PROFIT_PER_PRODUCT_PATH = DUCK_OPS_ROOT / "state" / "profit_per_product.json"
COMPETITOR_REPORTS_DIR = DUCK_AGENT_ROOT / "cache" / "competitor" / "reports"
BUILD_NEXT_QUEUE_PATH = DUCK_OPS_ROOT / "state" / "build_next_queue.json"
# Operator one-click dupe rulings on the 0.43-0.72 soft-flag band:
# {"decisions": {concept_key: {"decision": "duplicate"|"distinct", ...}}}.
# "duplicate" -> suppress next build; "distinct" -> stop flagging. Kept separate
# from the concept-feedback store so a candidate can be ruled on BEFORE promotion.
BUILD_NEXT_DUPE_DECISIONS_PATH = DUCK_OPS_ROOT / "state" / "build_next_dupe_decisions.json"
_VALID_DUPE_DECISIONS = {"duplicate", "distinct"}

# Comparison anchor for the DUCK_TEST_MODE write guard (architectural
# convention 4): captured at import, while the guard reads the LIVE module
# constant at call time so test monkeypatching keeps working.
_FROZEN_PRODUCTION_BUILD_NEXT_QUEUE_PATH = BUILD_NEXT_QUEUE_PATH.resolve()
_FROZEN_PRODUCTION_BUILD_NEXT_DUPE_DECISIONS_PATH = BUILD_NEXT_DUPE_DECISIONS_PATH.resolve()

SURFACE_VERSION = 1
TOP_N = 12
# Token overlap (candidate vs an existing catalog item) at/above this is
# treated as "we already make this" -> suppressed as a near-duplicate.
ALREADY_MADE_OVERLAP = 0.6
# Neutral factors for soft signals so a missing input degrades the score
# instead of zeroing it. demand and catalog_gap are the hard drivers.
NEUTRAL_MARGIN = 0.6
NEUTRAL_OCCASION = 0.6
# Surface 38: first-party search demand (Google Search Console). Soft factor —
# a candidate whose tokens hit a hot real search term scores above neutral;
# absence (no GSC data / no match) is uniform neutral so RANKING is unchanged
# until the signal is live. Read-only here; the producer (gsc_search_demand.py)
# owns the write + isolation guard.
NEUTRAL_SEARCH = 0.6
GSC_SEARCH_DEMAND_PATH = DUCK_OPS_ROOT / "state" / "gsc_search_demand.json"

# Tokens that carry no discriminating signal for duck concepts.
# 2026-06-15: the car-accessory boilerplate (car/decor/decoration/vehicle/
# accessory/cruise/holder/buddy/decal/dash) is just as generic in this niche
# as "duck"/"dashboard" — leaving it in made our "Owl Duck" {owl,car,decor}
# falsely match ANY competitor with car+decor at 67% (18/20 bogus
# suppressions). Strip it so the token fallback isn't broken either.
_STOPWORDS = {
    "duck", "ducks", "ducky", "3d", "3dprinted", "printed", "printing", "print",
    "figure", "figurine", "figurines", "toy", "gift", "gifts", "for", "the", "and", "with",
    "a", "an", "of", "to", "dashboard", "dash", "ducking", "jeep", "rubber", "collectible",
    "collectibles", "collectable", "novelty", "custom", "personalized", "cute", "mini", "small",
    "car", "cars", "vehicle", "decor", "decoration", "decorations", "accessory", "accessories",
    "cruise", "holder", "buddy", "decal", "ornament", "lover", "fans", "fan", "collector", "collectors",
}


def _tokens(text: Any) -> set[str]:
    raw = re.split(r"[^a-z0-9]+", str(text or "").lower())
    return {t for t in raw if len(t) > 2 and t not in _STOPWORDS}


def _overlap(a: set[str], b: set[str]) -> float:
    """Coverage of the smaller token set — robust to one title being much
    longer than the other (competitor titles are keyword-stuffed)."""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


# --------------------------------------------------------------------------
# Candidate assembly (competitor demand)
# --------------------------------------------------------------------------

_REPORT_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_competitor_report\.json$")


def _is_snapshot_only_report(report: dict[str, Any]) -> bool:
    """A daily snapshot (2026-06-12 cadence split) carries no LLM analysis —
    period='daily_snapshot', empty ducks_to_build/trending_products. Only the
    weekly analysis has build candidates; snapshot-only reports must be
    skipped or Build-Next reads an empty file and produces 0."""
    if str(report.get("period") or "").strip() == "daily_snapshot":
        return True
    ai = report.get("ai_insights")
    if isinstance(ai, dict) and ai.get("_snapshot_only"):
        return True
    return False


def latest_competitor_report(reports_dir: Path = COMPETITOR_REPORTS_DIR) -> tuple[dict[str, Any], str | None]:
    """Newest dated <YYYY-MM-DD>_competitor_report.json that actually carries
    build candidates (i.e. a WEEKLY analysis, not a daily snapshot). Returns
    ({}, None) when none exist — a missing demand source degrades the surface,
    never crashes it. Only date-prefixed filenames qualify (a dev/test report
    like test-foo_competitor_report.json must never be chosen).

    2026-06-15: the competitor cadence split (06-12) made daily runs write
    snapshot-only reports with empty ducks_to_build to the SAME filename, so
    the newest file is usually an empty snapshot. Walk newest-first and skip
    snapshot-only reports so this reads the latest real analysis."""
    try:
        files = sorted((p for p in Path(reports_dir).glob("*_competitor_report.json")
                        if _REPORT_DATE_RE.match(p.name)), reverse=True)
    except OSError:
        files = []
    for path in files:
        report = load_json(path, {})
        if not isinstance(report, dict):
            continue
        if _is_snapshot_only_report(report):
            continue
        return report, path.name
    return {}, None


# Surface 47: a competitor report older than this is too stale to drive demand
# ranking — its 7d/30d flow is no longer "recent". Mirrors the GSC/GA4 producer
# guard (seo_demand_context.STALE_MAX_DAYS). Demand DEGRADES on a stale report
# (rows fall to momentum/all-time, today's behavior), it never crashes.
COMPETITOR_REPORT_STALE_MAX_DAYS = 21


def _competitor_report_is_stale(report: dict[str, Any], report_name: str | None,
                                *, now: datetime | None = None,
                                max_days: int = COMPETITOR_REPORT_STALE_MAX_DAYS) -> bool:
    """True when the report's date is more than max_days old. Lenient: an
    empty/missing report or an unparseable date returns False (let the missing-
    report path handle absence; never disable demand on a guess)."""
    if not report:
        return False
    stamp = report.get("report_date") or report.get("generated_at")
    date_str = stamp[:10] if isinstance(stamp, str) and len(stamp) >= 10 else None
    if date_str is None and report_name and len(report_name) >= 10:
        date_str = report_name[:10]
    if not date_str:
        return False
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return False
    return ((now or datetime.now()).date() - d).days > max_days


def assemble_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Union ducks_to_build + trending_products, deduped by listing_id.
    ducks_to_build wins on conflict (it carries the priority field)."""
    by_id: dict[str, dict[str, Any]] = {}
    for key in ("trending_products", "ducks_to_build"):
        for row in report.get(key) or []:
            if not isinstance(row, dict):
                continue
            lid = str(row.get("listing_id") or row.get("title") or "")
            if not lid:
                continue
            merged = {**by_id.get(lid, {}), **row}
            merged["_sources"] = sorted(set(merged.get("_sources", []) + [key]))
            by_id[lid] = merged
    return list(by_id.values())


# --------------------------------------------------------------------------
# Factor scorers — each returns (value in 0..1, reason string, extras dict)
# --------------------------------------------------------------------------

def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


# All-time-only rows (no recent-sales signal) are capped at this fraction of the
# demand scale so a cooled-off past hit can NEVER outrank a current mover. The
# two signals live on different scales (trending_score ~tens–thousands of sales-
# weighted points vs engagement_score ~hundreds–thousands of lifetime views), so
# they must be normalized in SEPARATE pools, not one shared pool_max — otherwise
# a big all-time number swamps genuine momentum (the Duckpool inversion).
FALLBACK_DEMAND_CEILING = 0.5


def _has_snapshot_momentum(row: dict[str, Any]) -> bool:
    """True when a row's trending_score reflects REAL between-snapshot movement,
    not a new_listing lifetime proxy. delta_source is authoritative; when it's
    absent (older reports) infer from a concrete sold delta."""
    delta_source = row.get("delta_source")
    if delta_source == "new_listing":
        return False
    if delta_source == "snapshot":
        return True
    return _is_num(row.get("sold_last_7d")) or _is_num(row.get("sold_last_30d"))


# Surface 47 demand-signal classes, strongest first. `demand` = the competitor
# engine's 7d/30d demand score (favorites-velocity + sales-proxy x exact-sales
# credibility — Surface 46); `momentum` = snapshot trending_score; `alltime` =
# capped lifetime fallback. Each normalizes in its OWN pool — the three live on
# different magnitude scales and mixing them re-creates the Duckpool inversion.
_DEMAND_CLASS_DEMAND = "demand"
_DEMAND_CLASS_MOMENTUM = "momentum"
_DEMAND_CLASS_ALLTIME = "alltime"


def _demand_basis(row: dict[str, Any], allow_demand: bool = True) -> tuple[float, str]:
    """Return (raw_strength, signal_class).

    Rank on recent FLOW, not lifetime popularity. Order of preference:
    1. `demand_7d` (Surface 46 competitor demand) when present and allowed — the
       richest signal (it already folds favorites-velocity + sales proxy +
       exact-shop-sales credibility). A `demand_30d` term rides as ballast.
    2. snapshot `trending_score` (momentum).
    3. all-time `engagement_score` / views+favorites (capped fallback).
    `allow_demand=False` (a stale competitor report) skips class 1 so 3-week-old
    demand never drives ranking — the row degrades to today's momentum/all-time
    behavior. demand_7d == None (new-to-tracking gap duck) also falls through.

    CRITICAL: trending_score is only real momentum when delta_source ==
    'snapshot'. For a 'new_listing' row the engine sets trending_score =
    favorites*3 + views*0.5 — a LIFETIME proxy wearing a momentum label, which
    let a freshly-discovered listing outrank a 25-sold/7d mover (Duckpool,
    2026-06-22). So a new_listing is forced to the capped all-time pool."""
    if allow_demand:
        d7 = row.get("demand_7d")
        if _is_num(d7):
            d30 = row.get("demand_30d")
            ballast = 0.25 * float(d30) if _is_num(d30) else 0.0
            return float(d7) + ballast, _DEMAND_CLASS_DEMAND
    if _has_snapshot_momentum(row):
        trending = row.get("trending_score")
        if _is_num(trending) and trending > 0:
            return float(trending), _DEMAND_CLASS_MOMENTUM
    eng = row.get("engagement_score")
    if _is_num(eng):
        return float(eng), _DEMAND_CLASS_ALLTIME
    views = row.get("views") or 0
    favorites = row.get("favorites") or 0
    try:
        return float(views) + 5.0 * float(favorites), _DEMAND_CLASS_ALLTIME
    except (TypeError, ValueError):
        return 0.0, _DEMAND_CLASS_ALLTIME


def _demand_strength(row: dict[str, Any]) -> float:
    """Raw demand magnitude (demand-preferred). See _demand_basis."""
    return _demand_basis(row)[0]


def _pool_maxes(bases: list[tuple[float, str]]) -> dict[str, float]:
    """Per-class max for separate-pool normalization (see _demand_basis)."""
    return {
        cls: max((s for s, c in bases if c == cls), default=0.0)
        for cls in (_DEMAND_CLASS_DEMAND, _DEMAND_CLASS_MOMENTUM, _DEMAND_CLASS_ALLTIME)
    }


def score_demand(row: dict[str, Any], momentum_max: float = 0.0,
                 alltime_max: float = 0.0, *, demand_max: float = 0.0,
                 allow_demand: bool = True) -> tuple[float, str]:
    """Demand 0..1 with each signal class normalized in its own pool.

    A `demand`- or `momentum`-backed row scores up to 1.0; an `alltime`-only row
    is capped at FALLBACK_DEMAND_CEILING, so a current mover always outranks a
    cooled-off past hit regardless of how large its lifetime numbers are."""
    strength, cls = _demand_basis(row, allow_demand=allow_demand)
    if cls == _DEMAND_CLASS_DEMAND:
        if demand_max <= 0:
            return 0.0, "no demand signal in competitor pool"
        value = max(0.0, min(1.0, strength / demand_max))
        bd = row.get("demand_breakdown") if isinstance(row.get("demand_breakdown"), dict) else {}
        return value, (
            f"demand {int(strength)} (favΔ {bd.get('fav_velocity')}, "
            f"sold~ {bd.get('sales_proxy')}, cred {bd.get('credibility')}; 7d/30d flow)"
        )
    if cls == _DEMAND_CLASS_MOMENTUM:
        if momentum_max <= 0:
            return 0.0, "no momentum signal in competitor pool"
        value = max(0.0, min(1.0, strength / momentum_max))
        sold_7d = row.get("sold_last_7d")
        sold_disp = f"{int(sold_7d)} sold/7d" if _is_num(sold_7d) else "no 7d delta"
        return value, f"momentum {int(strength)} ({sold_disp}, trending_score)"
    # No demand/momentum signal — ranked on lifetime popularity, capped below the
    # momentum band and flagged so a reviewer knows it's NOT backed by sales.
    if alltime_max <= 0:
        return 0.0, "no demand signal in competitor pool"
    value = FALLBACK_DEMAND_CEILING * max(0.0, min(1.0, strength / alltime_max))
    return value, (
        f"all-time engagement {int(strength)} ({int(row.get('views') or 0)} views, "
        f"{int(row.get('favorites') or 0)} favs) — no recent-momentum signal (capped)"
    )


def _margin_index(profit: dict[str, Any]) -> tuple[list[tuple[set[str], float]], float | None]:
    """Build (title_tokens, margin_pct) pairs for confident-margin products
    plus a global median margin for the no-match fallback."""
    pairs: list[tuple[set[str], float]] = []
    margins: list[float] = []
    for p in profit.get("products") or []:
        if not isinstance(p, dict):
            continue
        margin = p.get("margin_pct")
        if not isinstance(margin, (int, float)) or isinstance(margin, bool):
            continue
        margins.append(float(margin))
        if not p.get("is_confident_margin"):
            continue
        toks: set[str] = set()
        for variant in p.get("title_variants") or [p.get("label")]:
            toks |= _tokens(variant)
        if toks:
            pairs.append((toks, float(margin)))
    return pairs, (median(margins) if margins else None)


def score_margin(cand_tokens: set[str],
                 margin_pairs: list[tuple[set[str], float]],
                 median_margin: float | None) -> tuple[float, str, bool]:
    best_margin = None
    best_ov = 0.0
    for toks, margin in margin_pairs:
        ov = _overlap(cand_tokens, toks)
        if ov > best_ov:
            best_ov, best_margin = ov, margin
    if best_margin is not None and best_ov >= ALREADY_MADE_OVERLAP:
        return max(0.0, min(1.0, best_margin / 100.0)), f"matched product margin {best_margin:.0f}%", False
    # No confident match: neutral estimate, FLAGGED (never a silent default).
    if median_margin is not None:
        return max(0.0, min(1.0, median_margin / 100.0)), \
            f"estimated from catalog median margin {median_margin:.0f}%", True
    return NEUTRAL_MARGIN, "no margin data; neutral estimate", True


def _catalog_token_sets(catalog_items: dict[str, Any]) -> list[tuple[str, set[str]]]:
    sets: list[tuple[str, set[str]]] = []
    for item in catalog_items.values():
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "") not in ("", "active"):
            continue
        toks = _tokens(item.get("title")) | _tokens(item.get("core_terms"))
        if toks:
            sets.append((str(item.get("title") or item.get("handle") or "?"), toks))
    return sets


def score_catalog_gap(cand_tokens: set[str],
                      catalog_sets: list[tuple[str, set[str]]]) -> tuple[float, str, str | None]:
    best_title, best_ov = None, 0.0
    for title, toks in catalog_sets:
        ov = _overlap(cand_tokens, toks)
        if ov > best_ov:
            best_ov, best_title = ov, title
    gap = max(0.0, 1.0 - best_ov)
    if best_ov >= ALREADY_MADE_OVERLAP:
        return gap, f"already make a similar duck ({best_ov:.0%} overlap: {best_title})", best_title
    if best_title:
        return gap, f"closest existing duck {best_ov:.0%} overlap", None
    return 1.0, "no catalog overlap (open gap)", None


def score_occasion_fit(cand_tokens: set[str],
                       active_occasions: list[dict[str, Any]]) -> tuple[float, str]:
    best = None
    for occ in active_occasions:
        kw_tokens: set[str] = set()
        for kw in occ.get("keywords") or []:
            kw_tokens |= _tokens(kw)
        if cand_tokens & kw_tokens:
            days = occ.get("days_until_peak")
            cand = (occ.get("id"), days if isinstance(days, int) else 999, occ.get("name"))
            if best is None or cand[1] < best[1]:
                best = cand
    if best is not None:
        return 1.0, f"fits ACTIVE occasion {best[2]} (peak in {best[1]}d)"
    return NEUTRAL_OCCASION, "evergreen (no active occasion match)"


def _load_search_demand_terms(path: Any = None) -> dict[str, float]:
    """{token -> 0..1 normalized GSC demand weight} from the producer's state
    file. Returns {} when the file is missing or the producer wrote
    available:false (no live data yet), so the factor stays neutral."""
    data = load_json(path or GSC_SEARCH_DEMAND_PATH, {})
    if not isinstance(data, dict) or not data.get("available"):
        return {}
    terms = data.get("term_scores")
    return {str(k): float(v) for k, v in terms.items()
            if isinstance(v, (int, float)) and not isinstance(v, bool)} if isinstance(terms, dict) else {}


def score_search_demand(cand_tokens: set[str],
                        term_scores: dict[str, float]) -> tuple[float, str]:
    """First-party demand boost: a candidate whose tokens hit a hot real GSC
    search term scores above neutral, mapped into [NEUTRAL_SEARCH, 1.0] so a
    match only ever BOOSTS. No data / no match → NEUTRAL_SEARCH (uniform, so
    ranking is unchanged until the signal is live)."""
    if not term_scores or not cand_tokens:
        return NEUTRAL_SEARCH, "no first-party search signal"
    best_term, best = "", 0.0
    for tok in cand_tokens:
        s = term_scores.get(tok, 0.0)
        if s > best:
            best, best_term = s, tok
    if best <= 0:
        return NEUTRAL_SEARCH, "no first-party search match"
    value = NEUTRAL_SEARCH + (1.0 - NEUTRAL_SEARCH) * max(0.0, min(1.0, best))
    return round(value, 3), f"search demand: '{best_term}' ({best:.2f} GSC weight)"


# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------

def _feedback_suppressed_keys(feedback: dict[str, Any]) -> set[str]:
    concepts = feedback.get("concepts") if isinstance(feedback.get("concepts"), dict) else {}
    keys: set[str] = set()
    for key, record in concepts.items():
        if not isinstance(record, dict):
            continue
        resolution = str(record.get("latest_resolution") or record.get("resolution") or "").strip().lower()
        if resolution not in SUPPRESSING_FEEDBACK_RESOLUTIONS:
            continue
        keys.add(_concept_feedback_key(key))
        for alias in (record.get("aliases") or []) + (record.get("concept_keys") or []):
            if str(alias or "").strip():
                keys.add(_concept_feedback_key(alias))
    return keys


# --------------------------------------------------------------------------
# Semantic catalog matching (2026-06-15) — primary "already made" + margin
# signal, reusing duckAgent's semantic_dedupe embeddings. The token method
# above is the deterministic fallback when embeddings are unavailable.
# --------------------------------------------------------------------------

_SEMANTIC_HARD_BAND = "already_made"
# The 0.43-0.72 band: kept in the queue but flagged so a near-dup can't rank as
# a clean top candidate (the Dachshund-ranked-#1 case). NOT auto-suppressed —
# the band is fuzzy enough that hiding it drops distinct ducks (e.g. Labrador
# vs Golden Retriever at 0.721). The operator confirms/dismisses per concept.
_SEMANTIC_SOFT_BAND = "possible_dupe"
_ON_BRAND_TOKENS = {"duck", "ducks", "ducky", "dashboard", "dash", "jeep"}


def _is_on_brand(title: str) -> bool:
    """A trending competitor product is on-brand only if it reads as a
    duck/dashboard item. ducks_to_build candidates are pre-vetted; this gates
    the unfiltered trending_products union so generic high-engagement junk
    (a Pizza Fidget Toy) can't top the ranking."""
    low = str(title or "").lower()
    return any(tok in re.split(r"[^a-z0-9]+", low) for tok in _ON_BRAND_TOKENS)


def _semantic_classify(candidate_titles: list[str], catalog_titles: list[str],
                       *, embed_fn=None) -> dict[str, dict[str, Any]]:
    """{title: {match, score, band}} via duckAgent semantic_dedupe, or {} on
    any failure (graceful — caller degrades to the token method). One cached
    embedding call. Self-loads duckAgent/.env so the OpenAI key is present
    even when invoked from a duck-ops producer."""
    if not candidate_titles or not catalog_titles:
        return {}
    import sys as _sys
    helpers_dir = str(DUCK_AGENT_ROOT)
    if helpers_dir not in _sys.path:
        _sys.path.insert(0, helpers_dir)
    try:
        if embed_fn is None and not os.getenv("OPENAI_API_KEY"):
            try:
                from dotenv import load_dotenv  # type: ignore
                load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
            except Exception:
                pass
        from helpers import semantic_dedupe as _sd  # type: ignore
        cfg = _sd.load_config()
        kw = {"embed_fn": embed_fn} if embed_fn is not None else {}
        catalog_vectors = _sd.build_catalog_vectors(catalog_titles, cfg, **kw)
        return _sd.classify_competitors(candidate_titles, catalog_vectors, cfg, **kw)
    except Exception:
        return {}


def _catalog_margin_map(catalog_items: dict[str, Any], profit: dict[str, Any]) -> dict[str, float]:
    """{normalized catalog title -> margin_pct} for confident-margin products,
    joining profit_per_product `title_variants` to catalog titles. Lets the
    semantic catalog-match yield a real per-candidate margin instead of the
    flat global-median fallback."""
    cat_norm = {}
    for v in catalog_items.values():
        if isinstance(v, dict) and v.get("title"):
            cat_norm[_norm_title(v["title"])] = v["title"]
    # Keyed by NORMALIZED catalog title so the semantic match (which returns
    # one exact title string) joins even across punctuation/variant differences.
    out: dict[str, float] = {}
    for p in profit.get("products") or []:
        if not isinstance(p, dict) or not p.get("is_confident_margin"):
            continue
        margin = p.get("margin_pct")
        if not isinstance(margin, (int, float)) or isinstance(margin, bool):
            continue
        for tv in (p.get("title_variants") or [p.get("label")]):
            n = _norm_title(tv)
            if n in cat_norm:
                out[n] = float(margin)
                break
    return out


def _norm_title(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()


def build_build_next_queue(*,
                           report: dict[str, Any],
                           report_name: str | None,
                           catalog: dict[str, Any],
                           profit: dict[str, Any],
                           occasion_intel: dict[str, Any],
                           feedback: dict[str, Any],
                           top_n: int = TOP_N,
                           semantic_map: dict[str, dict[str, Any]] | None = None,
                           search_demand_terms: dict[str, float] | None = None,
                           embed_fn=None) -> dict[str, Any]:
    catalog_items = catalog.get("items") if isinstance(catalog.get("items"), dict) else {}
    catalog_sets = _catalog_token_sets(catalog_items)
    margin_pairs, median_margin = _margin_index(profit)
    catalog_margin = _catalog_margin_map(catalog_items, profit)
    active_occasions = [o for o in occasion_intel.get("active_occasions") or [] if isinstance(o, dict)]
    suppressed_keys = _feedback_suppressed_keys(feedback)
    dupe_decisions = _load_dupe_decisions()
    # First-party search demand (Surface 38). None → load from the producer's
    # state file; pass {} explicitly in tests to skip the read.
    if search_demand_terms is None:
        search_demand_terms = _load_search_demand_terms()

    candidates = assemble_candidates(report)
    # Surface 47: rank on the competitor demand signal (7d/30d flow) when fresh.
    # A stale report (>21d) degrades demand to absent so 3-week-old flow never
    # drives ranking (rows fall to momentum/all-time — today's behavior).
    report_stale = _competitor_report_is_stale(report, report_name)
    allow_demand = not report_stale
    # Separate normalization pools: demand / momentum / all-time live on
    # different magnitude scales (see _demand_basis), so each normalizes within
    # its own pool max — one shared max would re-create the Duckpool inversion.
    _bases = [_demand_basis(c, allow_demand=allow_demand) for c in candidates]
    pools = _pool_maxes(_bases)

    # Semantic catalog match (primary). Computed once (one cached embedding
    # call); {} when embeddings are unavailable, in which case every lookup
    # below falls through to the deterministic token method.
    if semantic_map is None:
        catalog_titles = [v["title"] for v in catalog_items.values()
                          if isinstance(v, dict) and v.get("title")]
        cand_titles = [str(c.get("title") or "").strip() for c in candidates if c.get("title")]
        semantic_map = _semantic_classify(cand_titles, catalog_titles, embed_fn=embed_fn)
    classifier_coverage = sum(
        1 for p in catalog_items.values()
        if isinstance(p, dict) and isinstance(p.get("theme_classification"), dict)
    )

    queue: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    for cand in candidates:
        title = str(cand.get("title") or "").strip()
        cand_tokens = _tokens(title) | _tokens(" ".join(str(t) for t in cand.get("tags") or []))
        if not cand_tokens:
            continue

        sem = semantic_map.get(title) if semantic_map else None
        sources = cand.get("_sources") or []

        demand, demand_reason = score_demand(
            cand, pools["momentum"], pools["alltime"],
            demand_max=pools["demand"], allow_demand=allow_demand)
        gap, gap_reason, already_title = score_catalog_gap(cand_tokens, catalog_sets)
        occasion, occasion_reason = score_occasion_fit(cand_tokens, active_occasions)
        search, search_reason = score_search_demand(cand_tokens, search_demand_terms)

        # Margin: prefer the semantic catalog-match's real margin (joined by
        # normalized title); otherwise the token method (now fixed) / median.
        sem_match_norm = _norm_title(sem.get("match")) if sem else ""
        if sem_match_norm and sem_match_norm in catalog_margin:
            real = catalog_margin[sem_match_norm]
            margin = max(0.0, min(1.0, real / 100.0))
            margin_reason = f"margin {real:.0f}% from nearest catalog match '{sem.get('match')}'"
            margin_estimated = True
        else:
            margin, margin_reason, margin_estimated = score_margin(cand_tokens, margin_pairs, median_margin)

        # Suppression: semantic band is primary; token gap is the fallback.
        if sem and str(sem.get("band")) == _SEMANTIC_HARD_BAND:
            sem_suppressed, sem_reason = True, f"already made — semantic match '{sem.get('match')}' (cosine {sem.get('score')})"
        elif sem:
            sem_suppressed, sem_reason = False, ""
            gap = max(gap, 1.0 - float(sem.get("score") or 0.0))  # semantic distance widens the gap
        else:
            sem_suppressed, sem_reason = (gap < (1.0 - ALREADY_MADE_OVERLAP)), f"already made — {gap_reason}"

        # Soft-flag (kept, not suppressed) unless the operator already ruled on it.
        dupe_decision = _dupe_decision_for(dupe_decisions, title)
        possible_dupe = bool(sem and str(sem.get("band")) == _SEMANTIC_SOFT_BAND
                             and dupe_decision != "distinct")

        entry = {
            "title": title,
            "listing_id": cand.get("listing_id"),
            "shop_name": cand.get("shop_name"),
            "sources": sources,
            "priority": cand.get("priority"),
            "factors": {
                "demand": round(demand, 3),
                "margin": round(margin, 3),
                "catalog_gap": round(gap, 3),
                "occasion_fit": round(occasion, 3),
                "search_demand": round(search, 3),
            },
            "margin_estimated": margin_estimated,
            "demand_7d": cand.get("demand_7d"),
            "demand_30d": cand.get("demand_30d"),
            "demand_breakdown": cand.get("demand_breakdown"),
            "semantic_match": (sem or {}).get("match") if sem else None,
            "possible_dupe": possible_dupe,
            "dupe_score": float(sem.get("score")) if (possible_dupe and sem and sem.get("score") is not None) else None,
            "reasons": [demand_reason, margin_reason, gap_reason, occasion_reason, search_reason],
        }

        # Operator-confirmed duplicate → suppress (one-click "Confirm duplicate").
        if dupe_decision == "duplicate":
            entry["suppressed_reason"] = (
                f"operator confirmed duplicate of '{(sem or {}).get('match') or 'existing duck'}'"
            )
            suppressed.append(entry)
            continue

        # Off-brand gate: a trending-only candidate must read as a duck/
        # dashboard item (ducks_to_build candidates are always kept).
        if sources == ["trending_products"] and not _is_on_brand(title):
            entry["suppressed_reason"] = "off-brand — trending competitor product, not a duck/dashboard concept"
            suppressed.append(entry)
            continue

        fb_key = _concept_feedback_key(title)
        if fb_key in suppressed_keys:
            entry["suppressed_reason"] = "operator feedback already resolved/rejected this concept"
            suppressed.append(entry)
            continue
        if sem_suppressed:
            entry["suppressed_reason"] = sem_reason
            suppressed.append(entry)
            continue

        entry["score"] = round(demand * margin * gap * occasion * search, 4)
        queue.append(entry)

    queue.sort(key=lambda e: (-e["score"], str(e["title"])))

    return {
        "surface_version": SURFACE_VERSION,
        "generated_at": now_local_iso(),
        "sources": {
            "competitor_report": report_name,
            "competitor_report_stale": report_stale,
            "competitor_candidates": len(candidates),
            "catalog_products": len(catalog_items),
            "classifier_coverage": classifier_coverage,
            "confident_margin_products": len(margin_pairs),
            "catalog_margin_joined": len(catalog_margin),
            "semantic_classified": len(semantic_map or {}),
            "active_occasions": [o.get("id") for o in active_occasions],
        },
        "queue": queue[:top_n],
        "suppressed": suppressed,
        "queue_count": len(queue),
        "suppressed_count": len(suppressed),
    }


# --------------------------------------------------------------------------
# Write (three-layer isolation: conftest redirect + this guard + audit test)
# --------------------------------------------------------------------------

def _refusing_test_mode_prod_write() -> bool:
    if os.environ.get("DUCK_TEST_MODE") != "1":
        return False
    return Path(BUILD_NEXT_QUEUE_PATH).resolve() == _FROZEN_PRODUCTION_BUILD_NEXT_QUEUE_PATH


# --- dupe decisions: read (in the scorer) + write (from the portal action) ----

def _load_dupe_decisions() -> dict[str, Any]:
    data = load_json(BUILD_NEXT_DUPE_DECISIONS_PATH, {})
    decisions = data.get("decisions") if isinstance(data, dict) else None
    return decisions if isinstance(decisions, dict) else {}


def _dupe_decision_for(decisions: dict[str, Any], title: str) -> str | None:
    """'duplicate' | 'distinct' | None for a candidate title (by concept key)."""
    rec = decisions.get(_concept_feedback_key(title)) if decisions else None
    if isinstance(rec, dict) and rec.get("decision") in _VALID_DUPE_DECISIONS:
        return rec["decision"]
    return None


def record_dupe_decision(title: str, decision: str, *, matched: str | None = None) -> dict[str, Any]:
    """Record a one-click dupe ruling. decision in {duplicate, distinct}.
    duplicate -> suppressed next build; distinct -> stops the possible-dupe flag."""
    if decision not in _VALID_DUPE_DECISIONS:
        raise ValueError(f"decision must be one of {sorted(_VALID_DUPE_DECISIONS)}, got {decision!r}")
    if not str(title or "").strip():
        raise ValueError("title is required")
    if os.environ.get("DUCK_TEST_MODE") == "1" and \
            Path(BUILD_NEXT_DUPE_DECISIONS_PATH).resolve() == _FROZEN_PRODUCTION_BUILD_NEXT_DUPE_DECISIONS_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but BUILD_NEXT_DUPE_DECISIONS_PATH still points at the "
            "production state file. Monkeypatch it to a tmp path."
        )
    path = Path(BUILD_NEXT_DUPE_DECISIONS_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = load_json(path, {})
    if not isinstance(data, dict):
        data = {}
    decisions = data.setdefault("decisions", {})
    if not isinstance(decisions, dict):
        decisions = data["decisions"] = {}
    record = {"title": title, "decision": decision, "matched": matched, "at": now_local_iso()}
    decisions[_concept_feedback_key(title)] = record
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return record


def write_build_next_queue(payload: dict[str, Any]) -> Path:
    if _refusing_test_mode_prod_write():
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but BUILD_NEXT_QUEUE_PATH still points at the "
            "production state file — a test is about to pollute prod. "
            "Monkeypatch build_next_engine.BUILD_NEXT_QUEUE_PATH to a tmp path."
        )
    path = Path(BUILD_NEXT_QUEUE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Rank what duck to build next")
    parser.add_argument("--dry-run", action="store_true", help="Print summary, don't write")
    args = parser.parse_args()

    report, report_name = latest_competitor_report()
    payload = build_build_next_queue(
        report=report,
        report_name=report_name,
        catalog=load_json(CATALOG_INDEX_PATH, {}),
        profit=load_json(PROFIT_PER_PRODUCT_PATH, {}),
        occasion_intel=load_json(OCCASION_INTEL_PATH, {}),
        feedback=load_json(PRODUCT_CONCEPT_FEEDBACK_PATH, {"concepts": {}}),
    )

    if not args.dry_run:
        path = write_build_next_queue(payload)
        print(f"[build-next] {payload['queue_count']} ranked, "
              f"{payload['suppressed_count']} suppressed -> {path}")
    else:
        print(f"[build-next] (dry-run) {payload['queue_count']} ranked, "
              f"{payload['suppressed_count']} suppressed")
    for entry in payload["queue"][:5]:
        print(f"  - {entry['score']:.3f}  {entry['title'][:60]}  "
              f"[{', '.join(entry['reasons'][:1])}]")
    if report_name is None:
        print("  [warn] no competitor report found — demand signal absent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
