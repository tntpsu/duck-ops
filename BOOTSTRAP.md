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
