# Duck Stack Bootstrap

This file explains the current repo and runtime boundaries for the duck stack.

## Repos

### `/Users/philtullai/ai-agents/duckAgent`

Business system repo.

What belongs here:

- DuckAgent flows and helpers
- creative agent runtime and browser console
- catalog, review, social, weekly, and newduck business logic

What does not belong here:

- local launchd jobs
- machine-specific logs
- browser session state

### `/Users/philtullai/ai-agents/duck-ops`

Business-specific OpenClaw workspace repo.

What belongs here:

- operator runtime
- queue/review logic
- contracts
- shared compact interface-contract helpers for dashboards/widgets/companion readers
- duck operator config
- roadmaps, checklists, and operator docs

What is intentionally ignored here:

- `output/`
- `state/`
- caches and Python bytecode

### `/Users/philtullai/ai-agents/openclaw`

Engine repo only.

What belongs here:

- upstream OpenClaw app/engine code
- engine-level extensions
- shared platform behavior

What does not belong here:

- duck business workspace source
- duck business runtime output/state
- machine-specific config or browser auth state

Current local safety rules:

- the working branch is `codex/openclaw-local`
- `origin` now points to your fork and `upstream` points to the public OpenClaw repo

Current customer-thread refresh pattern:

- mailbox/file observation stays lightweight and frequent
- Etsy inbox truth-sync runs as a separate `duck-ops` runtime lane
- launchd should schedule the Etsy inbox sync locally; do not commit machine-specific plist files
- recommended cadence is every 2 hours during the day/evening plus one guaranteed `6:30 PM` pass

Current local morning observe/review pattern:

- morning Duck Ops observer jobs use `/Users/philtullai/ai-agents/duckAgent_runtime/run_duck_ops_observe_review.sh`
- the wrapper is the standard local pattern because it adds:
  - lock protection
  - stale-lock cleanup
  - timeout enforcement
  - local stdout/stderr log files
- the observe-only engineering review loop now includes:
  - nightly `tech_debt_triage.py`
  - weekly `reliability_review.py`
  - weekly `data_model_governance_review.py`
  - weekly `documentation_governance_review.py`
- `shopify_seo_kickoff.py` is now installed locally in launchd for a `7:35 AM` pass
- Etsy Playwright scheduling now belongs to the dedicated Etsy batch planner/checker wrappers in
  `/Users/philtullai/ai-agents/openclaw_runtime`, not ad hoc launchd intervals
- lightweight reader payloads such as the Even/Pulse widget should read
  `runtime/operator_interface_contracts.py` instead of rebuilding Duck Ops state summaries on their own
- local plist files stay in `~/Library/LaunchAgents`; do not commit them into repo

## Local-Only Runtime Paths

These stay local on this Mac and should not be treated as git homes:

- `/Users/philtullai/ai-agents/duckAgent_runtime`
- `/Users/philtullai/ai-agents/openclaw_runtime`
- `~/Library/LaunchAgents`
- local logs, locks, and browser auth/session state

## Legacy Paths

- `/Users/philtullai/ai-agents/openclaw/workspace` is now a legacy compatibility/historical area
- `/Users/philtullai/ai-agents/openclaw/workspace/.git` was disabled and renamed to
  `.git.legacy-disabled` so that legacy folder no longer behaves like an active duck workspace repo
- active launchd jobs and runtime scripts now point directly at `/Users/philtullai/ai-agents/duck-ops`

## Later Cleanup

- after you are confident nothing useful remains in the old historical workspace, archive or delete
  `/Users/philtullai/ai-agents/openclaw/workspace`
- before deleting it, double-check whether `duck_phase1/` or the old markdown notes still contain
  anything you want to preserve elsewhere

## Practical Rule Of Thumb

- if the change is about ducks, operator decisions, review execution, or duck-specific config, it
  belongs in `duck-ops`
- if the change is about DuckAgent business automation or creative generation, it belongs in
  `duckAgent`
- if the change is about the underlying OpenClaw engine/platform, it belongs in `openclaw`
- if the file is machine-specific or generated while running, it belongs in the local runtime layer,
  not git
