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

## Minimum High-Confidence Evidence

- Public market, trend, or social-learning evidence is attached.
- Catalog fit or gap status is explicit.
- Guardrails explain what must not be copied or generated.
- The recommended next step is review-first, not auto-generation.
