# Duck Phase 2 Source Map

## Purpose

This file defines the Phase 1 observation surfaces for OpenClaw.

OpenClaw should read these sources in read-only mode only.
It should never write back into DuckAgent paths.

## File sources

### DuckAgent cache

Path:

- `/Users/philtullai/ai-agents/duckAgent/cache`

Priority files:

- `weekly_insights.json`
- `product_recommendations.json`
- `products_cache.json`
- `publication_cache.json`
- `reddit_signal_history.json`

Primary uses:

- trend ranking
- publish context
- outcome attribution

### DuckAgent runs

Path:

- `/Users/philtullai/ai-agents/duckAgent/runs`

Priority artifacts:

- latest `state_competitor.json`
- latest `state_reviews.json`
- `state_newduck.json` if present later
- `state_weekly.json` if present later
- draft assets and image metadata under run folders

Primary uses:

- trend ranking
- customer intelligence
- publish candidate context

### DuckAgent logs

Path:

- `/Users/philtullai/ai-agents/duckAgent/logs`

Primary uses:

- low-priority run detection
- debugging observer gaps

## Mailbox source

Recommended default:

- read-only IMAP access to the same mailbox DuckAgent already uses

Fallback:

- mirrored Maildir or message export

Relevant message classes:

- DuckAgent approval emails
- DuckAgent review summary emails
- inbound customer or platform notification emails already arriving in the mailbox

## Observation policy

- Read-only only
- Poll for new or changed artifacts
- Hash content to avoid duplicate evaluation
- Preserve message IDs, subjects, and file paths for auditability

## Pilot source subset

For the Phase 2 pilot, prioritize only:

- `newduck` approval emails
- `weekly sale playbook` review emails
- `weekly_insights.json`
- `product_recommendations.json`
- `products_cache.json`
- `publication_cache.json`
- latest `state_competitor.json`
