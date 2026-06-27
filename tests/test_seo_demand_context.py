"""Surface 40: seo_demand_context reader + the SEO generator prompt enrichment.

Read-only, fail-soft: missing/unavailable state → empty context → the generator
behaves exactly as before. Covers product↔query relevance, GA4 listing matching
across the Etsy channel suffix, and that the enriched prompt actually carries the
real queries (openai_json mocked to capture the prompt)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import seo_demand_context as sdc  # noqa: E402


def _write_gsc(path, *, available=True):
    path.write_text(json.dumps({
        "available": available,
        "term_scores": {"wrangler": 1.0, "pirate": 0.6, "gladiator": 0.4},
        "top_queries": [
            {"query": "jeep wrangler duck", "impressions": 300, "trend": "rising"},
            {"query": "pirate duck gift", "impressions": 120, "trend": "steady"},
        ],
        "gap_queries": [
            {"query": "jeep gladiator duck", "impressions": 80, "trend": "rising"},
        ],
    }), encoding="utf-8")


def _write_lp(path, *, available=True):
    path.write_text(json.dumps({
        "available": available,
        "listings": [
            {"title": "Wrangler Duck Dashboard Buddy - Etsy", "channel": "etsy",
             "verdict": "fix", "engagement_rate": 0.18, "trend": "steady"},
            {"title": "Pirate Duck", "channel": "shopify",
             "verdict": "promote", "engagement_rate": 0.71, "trend": "rising"},
        ],
    }), encoding="utf-8")


# ---- fail-soft -------------------------------------------------------------

def test_missing_files_yield_empty_context(tmp_path):
    ctx = sdc.load_seo_demand_context(gsc_path=tmp_path / "none.json",
                                      listing_path=tmp_path / "none2.json")
    assert ctx.is_empty
    assert ctx.relevant_queries("Wrangler Duck") == []
    assert ctx.listing_signal("Wrangler Duck") is None
    assert ctx.top_search_terms == []


def test_unavailable_payload_is_empty(tmp_path):
    g, l = tmp_path / "g.json", tmp_path / "l.json"
    _write_gsc(g, available=False)
    _write_lp(l, available=False)
    ctx = sdc.load_seo_demand_context(gsc_path=g, listing_path=l)
    assert ctx.is_empty


# ---- relevance + matching --------------------------------------------------

def _ctx(tmp_path):
    g, l = tmp_path / "g.json", tmp_path / "l.json"
    _write_gsc(g)
    _write_lp(l)
    return sdc.load_seo_demand_context(gsc_path=g, listing_path=l)


def test_relevant_queries_match_product_tokens(tmp_path):
    ctx = _ctx(tmp_path)
    qs = [q["query"] for q in ctx.relevant_queries("Jeep Wrangler Duck")]
    assert "jeep wrangler duck" in qs and "pirate duck gift" not in qs

    qs2 = [q["query"] for q in ctx.relevant_queries("Gladiator Duck")]
    assert "jeep gladiator duck" in qs2  # gap queries are matchable too

def test_relevant_queries_sorted_by_impressions(tmp_path):
    ctx = _ctx(tmp_path)
    # "duck" is a stopword; "jeep" is too — match on wrangler/pirate/gladiator
    out = ctx.relevant_queries("Wrangler Pirate Gladiator Duck", limit=5)
    assert out and out[0]["impressions"] >= out[-1]["impressions"]

def test_no_token_overlap_returns_empty(tmp_path):
    ctx = _ctx(tmp_path)
    assert ctx.relevant_queries("Unicorn Rainbow Duck") == []


def test_listing_signal_matches_across_channel_suffix(tmp_path):
    ctx = _ctx(tmp_path)
    sig = ctx.listing_signal("Wrangler Duck Dashboard Buddy")  # no " - Etsy" suffix
    assert sig and sig["verdict"] == "fix" and sig["channel"] == "etsy"

def test_listing_signal_no_match_returns_none(tmp_path):
    ctx = _ctx(tmp_path)
    assert ctx.listing_signal("Completely Unrelated Widget") is None

def test_top_search_terms_ordered(tmp_path):
    ctx = _ctx(tmp_path)
    assert ctx.top_search_terms[0] == "wrangler"  # highest term_score


# ---- hardening (failure-mode review) ---------------------------------------

def test_modifier_words_filtered_from_top_terms(tmp_path):
    """Viral-phrase fragments (help/accidentally/built) must NOT pollute the
    global term list — observed live from 'help i accidentally built a jeep'."""
    g, l = tmp_path / "g.json", tmp_path / "l.json"
    g.write_text(json.dumps({"available": True, "top_queries": [], "gap_queries": [],
                             "term_scores": {"help": 0.99, "accidentally": 0.98,
                                             "built": 0.97, "wrangler": 0.5}}), encoding="utf-8")
    _write_lp(l)
    ctx = sdc.load_seo_demand_context(gsc_path=g, listing_path=l)
    assert "wrangler" in ctx.top_search_terms
    assert not ({"help", "accidentally", "built"} & set(ctx.top_search_terms))


def test_stale_demand_is_ignored(tmp_path):
    """A stalled producer's weeks-old data must fail-soft to empty, not keep
    driving titles."""
    g, l = tmp_path / "g.json", tmp_path / "l.json"
    _write_gsc(g)
    _write_lp(l)
    payload = json.loads(g.read_text())
    payload["generated_at"] = "2020-01-01T00:00:00"  # ancient
    g.write_text(json.dumps(payload), encoding="utf-8")
    ctx = sdc.load_seo_demand_context(gsc_path=g, listing_path=l)
    assert ctx.relevant_queries("Jeep Wrangler Duck") == []  # GSC dropped as stale
    # listing payload is fresh, so its signal still resolves
    assert ctx.listing_signal("Wrangler Duck Dashboard Buddy") is not None


def test_single_shared_token_across_long_titles_not_matched(tmp_path):
    """A lone shared token must not falsely equate two different products."""
    g, l = tmp_path / "g.json", tmp_path / "l.json"
    _write_gsc(g)
    l.write_text(json.dumps({"available": True, "listings": [
        {"title": "Wrangler Gladiator Pirate Cup", "channel": "shopify",
         "verdict": "promote", "engagement_rate": 0.7}]}), encoding="utf-8")
    ctx = sdc.load_seo_demand_context(gsc_path=g, listing_path=l)
    # "Wrangler Mount" shares only {wrangler} (1 of 2) with the listing → no match
    assert ctx.listing_signal("Wrangler Mount") is None


# ---- generator wiring ------------------------------------------------------

def test_generator_prompt_includes_demand_when_present(tmp_path, monkeypatch):
    import shopify_seo_review as ssr
    captured = {}

    def fake_openai_json(system, user, **kw):
        captured["user"] = user
        return {"items": [{"id": "1", "seo_title": "Jeep Wrangler Duck Dashboard Buddy Gift",
                           "seo_description": "x" * 155, "rationale": "ok"}]}

    monkeypatch.setattr(ssr, "_ensure_duckagent_imports", lambda: (fake_openai_json, None))
    ctx = _ctx(tmp_path)
    resources = [{"id": "1", "kind": "product", "title": "Jeep Wrangler Duck",
                  "resource_url": "/p/1", "seo_title": "", "seo_description": "", "issues": []}]
    ssr._generate_proposals(resources, demand_context=ctx)
    assert "STORE_TOP_SEARCH_TERMS" in captured["user"]
    assert "jeep wrangler duck" in captured["user"]  # the real query was injected
    assert "high_intent_searches" in captured["user"]
    assert "conversion" not in captured["user"] or "fix" in captured["user"].lower()


def test_generator_prompt_unchanged_when_demand_empty(tmp_path, monkeypatch):
    import shopify_seo_review as ssr
    captured = {}

    def fake_openai_json(system, user, **kw):
        captured["user"] = user
        return {"items": [{"id": "1", "seo_title": "A" * 50, "seo_description": "x" * 155, "rationale": "ok"}]}

    monkeypatch.setattr(ssr, "_ensure_duckagent_imports", lambda: (fake_openai_json, None))
    empty = sdc.load_seo_demand_context(gsc_path=tmp_path / "none.json",
                                        listing_path=tmp_path / "none2.json")
    resources = [{"id": "1", "kind": "product", "title": "Plain Duck",
                  "resource_url": "/p/1", "seo_title": "", "seo_description": "", "issues": []}]
    ssr._generate_proposals(resources, demand_context=empty)
    assert "STORE_TOP_SEARCH_TERMS" not in captured["user"]
    assert "high_intent_searches" not in captured["user"]
