# Contract: `print_queue_candidate`

## Purpose

Represent one operator-facing recommendation for what to print or restock next.

This is an intelligence artifact, not a printer command.

## Sources

- DuckAgent weekly ops signals
- inventory alerts
- later Shopify / Etsy stock snapshots
- later printer and filament context

## Required fields

- `artifact_id`
- `artifact_type`
- `source_refs`
- `product_title`
- `priority`
- `recommended_next_action`
- `why_now`

## Typical supporting fields

- `product_id`
- `recent_demand`
- `lifetime_demand`
- `inventory_signal`
- `channel_scope`
- `confidence`

## Safety rules

- if we only have demand evidence and not real stock evidence, say that explicitly
- do not convert a print recommendation into printer execution automatically in the first slice
- preserve the source signal used to justify the queue candidate
