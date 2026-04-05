# Contract: `customer_signal`

## Purpose

Represent one customer issue or reply opportunity so OpenClaw can assess risk and recommend a safe next step.

## Phase 2 sources

- `state_reviews.json`
- review-related DuckAgent emails
- inbound customer or platform emails already visible in the mailbox

## Required fields

- `artifact_id`
- `artifact_type`
- `channel`
- `source_refs`
- `customer_event`
- `business_context`

## Decision labels

- `reply_now`
- `watch`
- `escalate`
- `needs_human_context`

## Key normalization rules

- separate customer text from business context
- infer issue type conservatively
- preserve DuckAgent-generated follow-up suggestions as evidence, not final truth

## Safety rule

If customer text or order context is too thin, prefer `needs_human_context` or `escalate` over a strong reply recommendation.
