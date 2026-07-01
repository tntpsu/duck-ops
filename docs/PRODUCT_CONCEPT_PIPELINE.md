# Product-Concept Pipeline — how a new duck gets decided and built

**One-liner:** several *scouts* discover product ideas; they all feed **one
funnel** (the product concept queue); the operator approves at two gates; only
then does Studio spend credits to build the duck.

This doc is the map. For field-level contract, see
[`contracts/product_concept_queue.md`](../contracts/product_concept_queue.md).

## The flow

```
  SCOUTS (discover ideas)                 FUNNEL (decide)            BUILD (make)
  ─────────────────────────              ───────────────           ─────────────
  ① Trend candidates ─────┐
     (morning observer)   │
  ② Build-Next promote ───┤   gate 1     PRODUCT CONCEPT QUEUE      gate 2
     (competitor demand)  ├──"Promote"──▶ product_concept_queue ──"approve──▶ STUDIO
  ③ Competitor motif ─────┤   (no        - vets name/policy         brief"     (/portal/studio)
     (learning motifs)    │   credits)   - writes a design brief    (credits   - generates the
  ④ Strategy idea ────────┘              - holds approval state      spent      actual duck
     (weekly strategy)                   - dedups across scouts      here)      image → 3D → listing
```

- **Two deliberate yeses gate every build.** Gate 1 = **Promote** (idea → queue,
  no credits). Gate 2 = **approve the design brief in Studio** (credits spent).
  Nothing is generated without both.
- **The concept queue is the single source of truth** for "products in flight."
  The scouts are recommenders *into* it; Studio is the builder *from* it.

## The four scouts (feeders)

Each writes queue items tagged with a `source_type`. Code: `runtime/product_concept_queue.py`.

| # | Scout | `source_type` | Input file |
|---|---|---|---|
| ① | Trend-candidate pipeline (morning observer / trend ranker) | `trend_candidate` | `state/normalized/trend_candidates.json` |
| ② | **Build-Next "Promote"** (competitor-demand ranking) | `build_next_promotion` | `state/build_next_promotions.json` |
| ③ | Competitor-motif detector (learning motifs) | `competitor_motif` | `output/operator/current_learnings.json` |
| ④ | Weekly strategy ideas | `strategy_idea` | `current_learnings.json` + `state/competitor_social_benchmark.json` |

> Thursday, occasion tags, memes, jeepfacts, reviews, etc. are **content-publish
> lanes**, not product-concept scouts — they do **not** feed this queue.

## Where Build-Next fits

**Build-Next is scout ②** — a specialized recommender (the *competitor-demand
lens*). It scans the whole competitor catalog weekly and ranks ~40 products by
`demand × margin × catalog-gap × occasion`. Most you'll never act on; it's a
scouting list. Its only write-action, **Promote**, hands one idea to the funnel.
Page: `/portal/intel/build-next`. Producer: `runtime/build_next_engine.py`.

Build-Next is **not** a second decision system. Merging it into the queue would
bury the handful of vetted concepts under dozens of weekly scouting rows.

## How things are deduped (three layers)

1. **Inside Build-Next — vs your live catalog + your rulings.**
   Token overlap (≥`ALREADY_MADE_OVERLAP` 0.6) + semantic embeddings
   (≥0.72 hard-suppress; 0.43–0.72 soft band, split at `CONFIDENT_DUPE_FLOOR`
   0.55 into *improve-existing* vs a *weak-match* build-new hint) + operator
   dupe rulings (`build_next_dupe_decisions.json`) + rejected-concept feedback.
   This only compares against **shipped products**, not the pending queue.

2. **Inside the funnel — across scouts, by theme slug.**
   `_merge_duplicate_themes` collapses items from all four scouts that share
   `_slugify(theme)` into one (highest score wins). This is the cross-scout dedup.

3. **Build-Next funnel awareness — vs the pending queue (2026-06-30).**
   Build-Next reads the queue and flags any candidate already in it
   (`in_concept_queue`), so the page badges it and **disables Promote** — you
   can't re-propose what's already in flight.

### Known fragility (why layer 3 exists)

Layer 2 keys on an **exact `_slugify(theme)` match**, but scouts derive `theme`
differently. A trend "chef" → slug `chef`; a Build-Next promote of the raw
competitor title "3D Chef Duck: PLA Plastic Figurine" cleans only to
`3d chef pla plastic` → slug `3d-chef-pla-plastic`. The slugs differ, the merge
**misses**, and a duplicate concept slips through. Competitor titles are
SEO-keyword-stuffed, so this is the common case, not the edge case.

Layer 3 mitigates it at the **operator-facing** surface: Build-Next's own
awareness uses robust **token coverage** (`_concept_queue_match`, the clean
queued theme must be ≥`_CONCEPT_QUEUE_COVERAGE_MIN` 0.8 present in the
candidate), which is immune to keyword-stuffing — so you're never *offered* a
duplicate Promote.

**Follow-up (not yet done):** normalize each scout's `theme` to a clean subject
before layer-2's slug-merge, so two *non-Build-Next* scouts proposing the same
duck also collapse. Tracked in `TESTS.md` Surface 49.

## Queue states (what "in flight" means)

`ready_for_brief_review` · `watch` · `blocked_by_guardrail` · `suppressed_by_operator`.
Only non-suppressed items count as "in flight" for Build-Next funnel awareness.

## Code pointers

- Funnel builder + feeders: `duck-ops/runtime/product_concept_queue.py`
- Build-Next producer (scout ②): `duck-ops/runtime/build_next_engine.py`
- Build-Next page: `duckAgent/creative_agent/runtime/src/duck_creative_agent/build_next_intel_page.py`
- Promote endpoint: `duckAgent/…/viewer.py` → `/api/build-next/promote`
- Contract: `duck-ops/contracts/product_concept_queue.md`
- Coverage: `duck-ops/TESTS.md` Surfaces 16, 47, 48, 49
