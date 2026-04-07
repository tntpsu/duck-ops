# Contract: `customer_case`

## Purpose

Represent one operator-facing customer issue case after raw `customer_signal` normalization.

This is the first business-ready layer above `customer_signal`.

It should help answer:

- what happened?
- how risky is it?
- should we reply, refund, resend, escalate, or wait for context?

## Sources

- normalized `customer_signal`
- Etsy review records
- mailbox support emails already visible to Duck Ops

## Required fields

- `artifact_id`
- `artifact_type`
- `source_signal_id`
- `channel`
- `case_type`
- `priority`
- `recommended_action`
- `recommended_recovery_action`
- `customer_summary`
- `source_refs`

## Recommended supporting fields

- `context_state`
- `response_recommendation`
- `recovery_recommendation`
- `missing_context`
- `order_enrichment`
- `tracking_enrichment`
- `resolution_enrichment`
- `operator_decision`
- `approved_recovery_action`

## Example action labels

- `watch`
- `reply_recommended`
- `reply_with_context`
- `refund_review`
- `replacement_review`
- `refund_or_replacement_review`
- `escalate`

## Safety rules

- if customer context is thin, prefer `reply_with_context` or `escalate`
- do not treat a refund or resend recommendation as executable by default
- preserve prior-resolution evidence when available:
  - refund already issued
  - public review reply already posted
  - multiple shipments already recorded on the order
- preserve the original customer text or review text as evidence
- keep response and recovery recommendations staged until an operator approves the path
