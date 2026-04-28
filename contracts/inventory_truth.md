# Contract: `inventory_truth`

## Purpose

Represent the evidence quality behind stock-watch and print-queue recommendations.

This is the layer that distinguishes demand-only leads from candidates that have cached or live inventory evidence.

## Canonical writer

- `/Users/philtullai/ai-agents/duck-ops/runtime/inventory_truth.py`

## Canonical artifacts

- JSON: `/Users/philtullai/ai-agents/duck-ops/state/inventory_truth.json`
- Markdown: `/Users/philtullai/ai-agents/duck-ops/output/operator/inventory_truth.md`

## Inputs

- `/Users/philtullai/ai-agents/duck-ops/state/normalized/print_queue_candidates.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/products_cache.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/weekly_report.json`

## Required item fields

- `product_id`
- `product_title`
- `priority`
- `recent_demand`
- `lifetime_demand`
- `inventory_evidence_level`
- `stock_evidence`
- `live_inventory_available`
- `confidence`
- `evidence_summary`
- `recommended_action`

## Evidence levels

- `demand_only`: demand exists, but no stock quantity is available.
- `cached_inventory`: cached inventory fields exist, but live verification is still recommended.
- `confirmed_low_stock`: cached inventory is at or below the configured threshold and needs live verification before printing.

## Safety rules

- `demand_only` must never be treated as permission to print.
- Business Desk wording should say "verify live stock" unless inventory is confirmed low.
- Future Shopify live-inventory integration should update this contract instead of adding a competing stock truth file.
