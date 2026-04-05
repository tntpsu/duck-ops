# duck-ops

This is the canonical home for the duck business-specific OpenClaw operator workspace.

What lives here:

- `runtime/`
- `config/`
- `contracts/`
- operator docs and roadmaps
- stack/bootstrap docs

What does not belong in git:

- `output/`
- `state/`
- Python cache files
- local browser auth/session state
- local launchd/runtime helpers

Current goal:

- use `duck-ops` as the clean working home
- keep `openclaw` focused on engine code only

Current status:

- active local launchd jobs now run directly from `duck-ops`
- `duck-ops` is the only active git home for duck operator work
- `/Users/philtullai/ai-agents/openclaw/workspace` is now a legacy/historical area, not an active
  workspace root

See `BOOTSTRAP.md` for the current repo layout and runtime boundaries.
