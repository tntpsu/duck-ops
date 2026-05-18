# Contract: `product_concept_queue`

## Purpose

Represent product concept candidates Duck Ops can safely surface before DuckAgent generates design briefs, images, models, or listings.

## Owner Boundary

- Duck Ops owns the queue, evidence, guardrails, and Business Desk visibility.
- DuckAgent owns `design_brief_queue`, concept-image generation, model conversion, and listing mutation after operator approval.
- This queue must not publish, schedule, generate images, upload listings, or mutate external systems.

## Required Surface Fields

- `generated_at`
- `surface_version`
- `status`
- `headline`
- `recommended_action`
- `source_paths`
- `summary`
- `design_brief_input`
- `items`

## Required Item Fields

- `concept_id`
- `source_type`
- `theme`
- `raw_theme`
- `catalog_status`
- `queue_state`
- `score`
- `confidence`
- `evidence`
- `guardrails`
- `recommended_next_step`
- `duckagent_task`
- `trend_quality_gate`
- `concept_design_brief`

## Queue States

- `ready_for_brief_review`: public-safe catalog gap with enough evidence to send to DuckAgent `design_brief_queue`.
- `watch`: useful signal, but too weak or too broad for concept approval.
- `blocked_by_guardrail`: likely IP, logo, competitor-copy, or printability issue that needs manual abstraction first.

## DuckAgent Handoff

The queue writes `state/product_concept_queue_design_brief_input.json` in DuckAgent-compatible `DesignBriefQueueInput` shape:

- `channel`
- `goal`
- `time_window`
- `max_candidates`
- `operator_notes`
- `candidate_signals`

Each `candidate_signals` entry must include `public_concept_allowed` in its guardrails so DuckAgent can distinguish public market signals from private customer-custom signals.

## Concept Design Brief

Duck Ops attaches a `trend_quality_gate` and `concept_design_brief` before DuckAgent spends image or 3D credits.

Purpose:

- preserve what the operator is actually approving
- prevent title-only concept generation
- prevent shallow color-only interpretations
- keep IP/copy and printability guardrails attached to the idea
- let DuckAgent Studio, semantic QA, and future model-building share one concept contract

Minimum shape:

`trend_quality_gate`:

- `schema_version`: `duck.trend_quality_gate.v1`
- `status`: `ready`, `needs_reframe`, `needs_refresh`, or `blocked_by_policy`
- `generation_ready`
- `normalized_concept_title`
- `issues`
- `warnings`
- `catalog_status`
- `source_ref_count`
- `latest_observed_at`
- `staleness_days`
- `checked_at`

`concept_design_brief`:

- `schema_version`: `duck.product_concept_brief.v1`
- `brief_source`
- `generated_at`
- `concept_title`
- `raw_theme`
- `semantic_identity`
- `theme_category`
- `operator_summary`
- `visual_cues`
- `must_preserve`
- `must_avoid`
- `printability_guardrails`
- `ip_copy_risks`
- `style_reference_policy`
- `evidence_summary`
- `source_refs`
- `confidence`
- `review_status`

DuckAgent may generate deterministic fallback briefs for older approved artifacts, but Duck Ops is the long-term writer so review, approval, Studio generation, and QA use the same meaning.

Implementation plan:
- [PRODUCT_CONCEPT_BRIEF_CONTRACT_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/PRODUCT_CONCEPT_BRIEF_CONTRACT_PLAN.md)

## Minimum High-Confidence Evidence

- Public market, trend, or social-learning evidence is attached.
- Catalog fit or gap status is explicit.
- Guardrails explain what must not be copied or generated.
- The recommended next step is review-first, not auto-generation.
