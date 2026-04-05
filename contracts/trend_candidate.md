# Contract: `trend_candidate`

## Purpose

Represent one trend DuckAgent already found so OpenClaw can rank it and decide whether it is worth acting on.

## Required fields

- `artifact_id`
- `artifact_type`
- `theme`
- `source_refs`
- `observed_at`
- `first_seen_at`
- `signal_summary`
- `catalog_match`

## Decision labels

- `worth_acting_on`
- `watch`
- `ignore`

## Action frame

- `promote`
- `build`
- `wait`
- `ignore`

## Key normalization rules

- merge equivalent themes
- prefer commercial evidence over engagement-only spikes
- keep source references auditable
- use `unknown` instead of inventing catalog gap status

## Minimum high-confidence evidence

- at least two supporting DuckAgent signals
- at least one strong commercial signal
- known catalog fit
