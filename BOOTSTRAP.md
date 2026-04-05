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
- the old `origin` remote was renamed to `upstream` so we do not accidentally push custom work to
  the public upstream repo

## Local-Only Runtime Paths

These stay local on this Mac and should not be treated as git homes:

- `/Users/philtullai/ai-agents/duckAgent_runtime`
- `/Users/philtullai/ai-agents/openclaw_runtime`
- `~/Library/LaunchAgents`
- local logs, locks, and browser auth/session state

## Compatibility Paths

- `/Users/philtullai/ai-agents/openclaw/workspace/duck_phase2` is a compatibility symlink that now
  points to `/Users/philtullai/ai-agents/duck-ops`
- `/Users/philtullai/ai-agents/openclaw/workspace/.git` was disabled and renamed to
  `.git.legacy-disabled` so that legacy folder no longer behaves like the active duck workspace repo

## Practical Rule Of Thumb

- if the change is about ducks, operator decisions, review execution, or duck-specific config, it
  belongs in `duck-ops`
- if the change is about DuckAgent business automation or creative generation, it belongs in
  `duckAgent`
- if the change is about the underlying OpenClaw engine/platform, it belongs in `openclaw`
- if the file is machine-specific or generated while running, it belongs in the local runtime layer,
  not git
