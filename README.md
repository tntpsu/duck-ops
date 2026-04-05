# duck-ops

This is the canonical home for the duck business-specific OpenClaw operator workspace.

What lives here:

- `runtime/`
- `config/`
- `contracts/`
- operator docs and roadmaps

What does not belong in git:

- `output/`
- `state/`
- Python cache files

Compatibility:

- existing hooks that still reference `/Users/philtullai/ai-agents/openclaw/workspace/duck_phase2`
  continue to work through a compatibility symlink that now points at this folder.

Current goal:

- use `duck-ops` as the clean working home
- keep the old `duck_phase2` path alive only as a compatibility alias until all callers are migrated
