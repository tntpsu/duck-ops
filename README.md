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

Compatibility:

- existing hooks that still reference `/Users/philtullai/ai-agents/openclaw/workspace/duck_phase2`
  continue to work through a compatibility symlink that now points at this folder.
- `/Users/philtullai/ai-agents/openclaw/workspace` is now a legacy compatibility area, not the
  active git home for duck operator work.

Current goal:

- use `duck-ops` as the clean working home
- keep the old `duck_phase2` path alive only as a compatibility alias until all callers are migrated
- keep `openclaw` focused on engine code only

See `BOOTSTRAP.md` for the current repo layout and runtime boundaries.
