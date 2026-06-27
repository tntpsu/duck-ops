"""Surface 40: expose first-party demand (GSC search queries + GA4 listing
performance) as context for the Shopify SEO generator, so generated titles are
built from REAL shopper search intent instead of guessed keywords.

Read-only and fail-soft: if either state file is missing or available:false, the
context is EMPTY and the SEO generator behaves exactly as it did before this
surface (per the plausible-fallbacks-mask-failure discipline — the empty path is
the tested, safe default, never silent garbage).
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, load_json
# Reuse Build-Next's tokenizer/normalizer so product↔query matching uses the
# SAME stopwords/normalization as the rest of the demand stack (no drift).
from build_next_engine import _tokens, _norm_title

GSC_SEARCH_DEMAND_PATH = DUCK_OPS_ROOT / "state" / "gsc_search_demand.json"
LISTING_PERFORMANCE_PATH = DUCK_OPS_ROOT / "state" / "listing_performance.json"

DEFAULT_QUERY_LIMIT = 5
DEFAULT_TOP_TERMS = 15
# Token-coverage of the smaller set required to call a GA4 listing the same
# product as a Shopify resource (titles differ: GA4 carries an Etsy suffix).
_MATCH_MIN_OVERLAP = 0.5
# Demand older than this is ignored (fail-soft to empty) — a stalled producer
# must NOT keep driving titles off weeks-old search data. The producer isn't on
# launchd yet, so this is the safety net until it is.
STALE_MAX_DAYS = 21
_CHANNEL_SUFFIX = re.compile(r"\s*[-–|]\s*(etsy|myjeepduck.*)\s*$", re.IGNORECASE)
# Search-MODIFIER words that ride along in queries but are not product
# descriptors — they pollute the global term list (e.g. the viral "help i
# accidentally built a jeep" query). Dropped from top_search_terms only; full
# per-product matched query phrases are kept intact.
_SEARCH_MODIFIER_STOPWORDS = {
    "help", "accidentally", "built", "build", "building", "details", "detail",
    "near", "best", "cheap", "online", "review", "reviews", "ideas", "idea",
    "how", "what", "why", "where", "when", "much", "does", "did", "get", "vs",
    "meaning", "history", "facts", "fact",
}


def _strip_channel(title: Any) -> str:
    return _CHANNEL_SUFFIX.sub("", str(title or "")).strip()


def _is_stale(payload: dict[str, Any]) -> bool:
    """True only when generated_at is parseable AND older than STALE_MAX_DAYS.
    Unparseable/absent timestamps are treated as fresh (lenient) so fixtures and
    older payload shapes still work."""
    raw = payload.get("generated_at")
    if not raw:
        return False
    try:
        ts = datetime.fromisoformat(str(raw))
    except ValueError:
        return False
    now = datetime.now(ts.tzinfo) if ts.tzinfo else datetime.now()
    return (now - ts).days > STALE_MAX_DAYS


class SeoDemandContext:
    """Per-product lookups over the demand state. Construct via
    load_seo_demand_context(); an empty instance is a safe no-op."""

    def __init__(self, *, queries: list[dict[str, Any]], term_scores: dict[str, float],
                 listings: list[dict[str, Any]]):
        # (query_row, token_set) for relevance matching
        self._query_tokens = [(q, _tokens(q.get("query"))) for q in queries
                              if isinstance(q, dict) and q.get("query")]
        self._listings = [(_tokens(_strip_channel(l.get("title"))), l) for l in listings
                          if isinstance(l, dict) and l.get("title")]
        self.top_search_terms = [
            t for t, _ in sorted(term_scores.items(), key=lambda kv: kv[1], reverse=True)
            if t not in _SEARCH_MODIFIER_STOPWORDS
        ][:DEFAULT_TOP_TERMS]

    @property
    def is_empty(self) -> bool:
        return not (self._query_tokens or self._listings or self.top_search_terms)

    def relevant_queries(self, title: Any, *, limit: int = DEFAULT_QUERY_LIMIT) -> list[dict[str, Any]]:
        """Real search queries whose tokens overlap this product, newest-demand
        first (by impressions), deduped by query string."""
        toks = _tokens(title)
        if not toks:
            return []
        hits = [q for q, qt in self._query_tokens if qt & toks]
        hits.sort(key=lambda r: r.get("impressions") or 0, reverse=True)
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for q in hits:
            name = str(q.get("query"))
            if name in seen:
                continue
            seen.add(name)
            out.append({"query": name, "impressions": int(q.get("impressions") or 0),
                        "trend": q.get("trend")})
            if len(out) >= limit:
                break
        return out

    def listing_signal(self, title: Any) -> dict[str, Any] | None:
        """GA4 verdict/engagement/trend for the listing that best matches this
        product title (channel suffix stripped); None if no confident match."""
        toks = _tokens(_strip_channel(title))
        if not toks:
            return None
        best, best_ov, best_shared = None, 0.0, 0
        for ltoks, listing in self._listings:
            if not ltoks:
                continue
            shared = len(ltoks & toks)
            ov = shared / min(len(ltoks), len(toks))
            if ov > best_ov:
                best_ov, best, best_shared = ov, listing, shared
        # Guard against a single common token matching two long titles: a
        # 1-token overlap only counts when it FULLY covers the shorter title
        # (genuinely short names like "Pirate Duck"); otherwise need 2+ shared.
        if best is None or best_ov < _MATCH_MIN_OVERLAP:
            return None
        if best_shared < 2 and best_ov < 1.0:
            return None
        return {
            "verdict": best.get("verdict"),
            "engagement_rate": best.get("engagement_rate"),
            "trend": best.get("trend"),
            "channel": best.get("channel"),
        }


def load_seo_demand_context(*, gsc_path: Any = None,
                            listing_path: Any = None) -> SeoDemandContext:
    gsc = load_json(gsc_path or GSC_SEARCH_DEMAND_PATH, {})
    lp = load_json(listing_path or LISTING_PERFORMANCE_PATH, {})
    # Available AND fresh — a stalled producer's weeks-old data is ignored.
    gsc_ok = isinstance(gsc, dict) and gsc.get("available") and not _is_stale(gsc)
    lp_ok = isinstance(lp, dict) and lp.get("available") and not _is_stale(lp)
    queries = (list(gsc.get("top_queries") or []) + list(gsc.get("gap_queries") or [])) if gsc_ok else []
    term_scores = gsc.get("term_scores") if gsc_ok and isinstance(gsc.get("term_scores"), dict) else {}
    term_scores = {str(k): float(v) for k, v in term_scores.items()
                   if isinstance(v, (int, float)) and not isinstance(v, bool)}
    listings = list(lp.get("listings") or []) if lp_ok else []
    return SeoDemandContext(queries=queries, term_scores=term_scores, listings=listings)
