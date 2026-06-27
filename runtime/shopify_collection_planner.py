"""Surface 41 Stage A: gap query → DRAFT Shopify collection proposals for
operator review. READ-ONLY / planning only — Stage A produces proposals and the
review payload; it creates NOTHING in Shopify (that is Stage B, gated by an
operator email reply).

Scope rule (the defining distinction): a gap query is a collection candidate
only when >= MIN_COLLECTION_MEMBERS existing ACTIVE catalog products are
relevant. 0 relevant → Build-Next's job; 1 → listing SEO (Surface 40); ≥N → a
collection (here).

Fail-soft + staleness reuse seo_demand_context. Deterministic copy in Stage A;
LLM title/SEO polish + its convention-#5 eval land in Stage B alongside create.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, load_json, now_local_iso
from workflow_control import TestModeRefusalError
from build_next_engine import _tokens
from seo_demand_context import _is_stale, _SEARCH_MODIFIER_STOPWORDS

GSC_SEARCH_DEMAND_PATH = DUCK_OPS_ROOT / "state" / "gsc_search_demand.json"
CATALOG_INDEX_PATH = DUCK_OPS_ROOT / "state" / "normalized" / "catalog_index.json"
# Existing collections come from the SEO audit (it already enumerates them) —
# used for dedup so we never propose a collection that exists.
SEO_AUDIT_PATH = DUCK_OPS_ROOT / "state" / "shopify_seo_audit.json"
COLLECTION_PLAN_PATH = DUCK_OPS_ROOT / "state" / "shopify_collection_review" / "latest.json"
_FROZEN_PRODUCTION_COLLECTION_PLAN_PATH = COLLECTION_PLAN_PATH.resolve()

MIN_COLLECTION_MEMBERS = 3
# A product must contain ALL of the query's distinctive tokens (subset match).
# 0.5 let "baby yoda duck" grab anything sharing the generic "baby"; requiring
# the full set means a collection only groups products that genuinely match the
# searched theme. A 1-token theme ("dog") can still span many products.
MIN_QUERY_COVERAGE = 1.0
# 1 DISTINCTIVE shared token suffices: the tokenizer already strips the niche
# generics ("jeep"/"duck"/"dashboard") as stopwords, so most gap queries reduce
# to a single distinctive token ("wrangler") — requiring 2 would reject every
# real candidate. Precision comes from MIN_QUERY_COVERAGE, not raw shared count.
MIN_SHARED_TOKENS = 1
MAX_MEMBERS = 60
_TITLE_DUP_OVERLAP = 0.8


def _slug(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(text or "").lower()).strip("-")


def _query_tokens(query: Any) -> set[str]:
    return {t for t in _tokens(query) if t not in _SEARCH_MODIFIER_STOPWORDS}


def _member_tokens(item: dict[str, Any]) -> set[str]:
    toks = _tokens(item.get("title")) | _tokens(item.get("core_terms"))
    toks |= _tokens(" ".join(str(t) for t in item.get("tags") or []))
    return toks


def _is_active(item: dict[str, Any]) -> bool:
    return str(item.get("status") or "active").lower() == "active"


def match_products(query: Any, catalog_items: dict[str, Any]) -> list[dict[str, Any]]:
    """Active catalog products relevant to the query: ≥2 shared tokens AND the
    shared set covers ≥MIN_QUERY_COVERAGE of the query's tokens."""
    qt = _query_tokens(query)
    if not qt:
        return []
    members: list[dict[str, Any]] = []
    for cid, item in catalog_items.items():
        if not isinstance(item, dict) or not _is_active(item):
            continue
        shared = qt & _member_tokens(item)
        if len(shared) >= MIN_SHARED_TOKENS and len(shared) / len(qt) >= MIN_QUERY_COVERAGE:
            members.append({"id": str(cid), "title": item.get("title"), "shared": sorted(shared)})
    members.sort(key=lambda m: str(m["title"] or ""))
    return members


def _existing_collections(audit: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in audit.get("resources") or []:
        if not isinstance(r, dict) or r.get("kind") != "collection":
            continue
        title = r.get("title") or ""
        handle = r.get("handle") or _slug((r.get("resource_url") or "").rstrip("/").split("/")[-1])
        out.append({"title": title, "tokens": _tokens(title), "handle": handle})
    return out


def is_duplicate(query_tokens: set[str], proposed_title: str, proposed_handle: str,
                 existing: list[dict[str, Any]]) -> bool:
    pt = _tokens(proposed_title)
    for c in existing:
        if c["handle"] and c["handle"] == proposed_handle:
            return True
        ct = c["tokens"]
        if not ct:
            continue
        if pt and len(pt & ct) / len(pt | ct) >= _TITLE_DUP_OVERLAP:
            return True
        if query_tokens and query_tokens <= ct:  # query is a subset of an existing collection
            return True
    return False


def _collection_title(query: Any) -> str:
    words = str(query or "").split()
    title = " ".join(w if (w.isupper() or any(ch.isdigit() for ch in w)) else w.capitalize()
                     for w in words)
    if "duck" not in title.lower():
        title = (title + " Ducks").strip()
    return title


def _seo_copy(title: str, member_count: int) -> tuple[str, str]:
    seo_title = title if len(title) >= 45 else f"{title} | MyJeepDuck Collectible Ducks"
    seo_title = seo_title[:70]
    desc = (f"Shop {title} at MyJeepDuck — {member_count} collectible 3D-printed ducks "
            f"for Jeep fans and gift shoppers. Quick-ship favorites.")
    return seo_title, desc[:160]


def build_collection_candidates(gsc: dict[str, Any], catalog: dict[str, Any],
                                audit: dict[str, Any], *,
                                min_members: int = MIN_COLLECTION_MEMBERS) -> list[dict[str, Any]]:
    """Pure: gap_queries → ranked collection candidates. Empty when demand is
    unavailable/stale (fail-soft)."""
    if not (isinstance(gsc, dict) and gsc.get("available") and not _is_stale(gsc)):
        return []
    items = catalog.get("items") if isinstance(catalog.get("items"), dict) else {}
    existing = _existing_collections(audit if isinstance(audit, dict) else {})
    out: list[dict[str, Any]] = []
    seen_handles: set[str] = set()
    # Source from ALL real queries, not gap_queries: a gap is BY DEFINITION a
    # query with no catalog match (GSC's gap detector), so it can never reach the
    # ≥N members a collection needs. Collections group products we already sell —
    # i.e. queries that match MANY products but have no page. Dedup by query,
    # keeping the higher-impression row.
    by_query: dict[str, dict[str, Any]] = {}
    for q in (list(gsc.get("top_queries") or []) + list(gsc.get("gap_queries") or [])):
        if isinstance(q, dict) and q.get("query"):
            cur = by_query.get(q["query"])
            if cur is None or (q.get("impressions") or 0) > (cur.get("impressions") or 0):
                by_query[q["query"]] = q
    for gq in by_query.values():
        if not isinstance(gq, dict):
            continue
        query = gq.get("query")
        qt = _query_tokens(query)
        if not qt:
            continue
        members = match_products(query, items)[:MAX_MEMBERS]
        if len(members) < min_members:  # 0 / 1 / <N → not a collection
            continue
        title = _collection_title(query)
        handle = _slug(title)
        if not handle or handle in seen_handles:
            continue
        if is_duplicate(qt, title, handle, existing):
            continue
        seen_handles.add(handle)
        seo_title, seo_desc = _seo_copy(title, len(members))
        out.append({
            "source_query": query,
            "impressions": int(gq.get("impressions") or 0),
            "trend": gq.get("trend"),
            "impressions_by_window": gq.get("impressions_by_window"),
            "proposed_title": title,
            "proposed_handle": handle,
            "proposed_seo_title": seo_title,
            "proposed_seo_description": seo_desc,
            "member_count": len(members),
            "member_product_ids": [m["id"] for m in members],
            "member_titles": [m["title"] for m in members],
            "match_rationale": f"{len(members)} active products share {sorted(qt)} with the query",
        })
    out.sort(key=lambda c: c["impressions"], reverse=True)
    return out


def build_collection_plan(gsc: dict[str, Any], catalog: dict[str, Any],
                          audit: dict[str, Any], *,
                          min_members: int = MIN_COLLECTION_MEMBERS) -> dict[str, Any]:
    candidates = build_collection_candidates(gsc, catalog, audit, min_members=min_members)
    available = bool(isinstance(gsc, dict) and gsc.get("available") and not _is_stale(gsc))
    return {
        "generated_at": now_local_iso(),
        "available": available,
        "status": "awaiting_review",
        "stage": "A_preview",  # Stage A produces proposals only; no Shopify create exists yet
        "min_members": min_members,
        "candidate_count": len(candidates),
        "candidates": candidates,
    }


def write_collection_plan(payload: dict[str, Any], path: Any = None):
    from pathlib import Path
    out = Path(path or COLLECTION_PLAN_PATH)
    if os.environ.get("DUCK_TEST_MODE") == "1" and \
            out.resolve() == _FROZEN_PRODUCTION_COLLECTION_PLAN_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but COLLECTION_PLAN_PATH still points at production. "
            "Monkeypatch shopify_collection_planner.COLLECTION_PLAN_PATH to a tmp path.")
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage A: draft Shopify collection proposals from GSC gap queries (no create).")
    parser.add_argument("--dry-run", action="store_true", help="Print proposals, don't write")
    args = parser.parse_args()

    plan = build_collection_plan(
        load_json(GSC_SEARCH_DEMAND_PATH, {}),
        load_json(CATALOG_INDEX_PATH, {}),
        load_json(SEO_AUDIT_PATH, {}),
    )
    print(f"[collection-planner] available={plan['available']} candidates={plan['candidate_count']}")
    for c in plan["candidates"][:10]:
        print(f"\n  '{c['proposed_title']}'  (/{c['proposed_handle']})  "
              f"[{c['trend']}, {c['impressions']} impr]  ← \"{c['source_query']}\"")
        print(f"    {c['member_count']} members: " + ", ".join(t for t in c["member_titles"][:6])
              + (" …" if c["member_count"] > 6 else ""))
    if not args.dry_run:
        out = write_collection_plan(plan)
        print(f"\n  -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
