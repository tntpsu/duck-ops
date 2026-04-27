# Duck Ops Agent Instructions

This file is the canonical shared instruction file for AI assistants and human contributors working in Duck Ops.

## Read First

Before making non-trivial changes, read:

- `README.md`
- `BOOTSTRAP.md`
- `OPENCLAW_PHASE2_ROADMAP.md`
- `OPENCLAW_PHASE2_IMPLEMENTATION_CHECKLIST.md`
- the relevant `contracts/` or `config/` file for the lane you are changing
- `/Users/philtullai/ai-agents/duckAgent/docs/current_system/AGENT_GOVERNANCE_POLICY.md` when changing automation power, approval, browser, or scheduler behavior

## Repo Boundaries

Duck Ops owns:

- operator state and review queues
- Business Desk and morning operator summaries
- policy checks and promotion-readiness evidence
- customer and review execution control surfaces
- compact interface contracts for dashboards/widgets
- Duck-specific OpenClaw workspace configuration

Duck Ops does not own:

- DuckAgent flow implementation
- OpenClaw engine/platform source
- `paint-to-print-3d` conversion implementation
- local launchd plists, browser auth profiles, secrets, logs, or generated caches

## Safety Boundaries

- Do not run or enable Etsy Playwright/browser automation unless explicitly requested and current safety policy allows it.
- Keep Etsy API reads conceptually separate from Etsy browser sessions.
- Do not make live customer-facing changes unless the lane has an explicit policy, target identity, approval state, and receipt path.
- Fail closed when review target, customer target, publish state, or credentials are uncertain.
- Do not promote a lane to auto-action without operator-visible evidence and an explicit approval boundary.

## Preferred Commands

Run the test suite with DuckAgent's virtual environment:

```bash
cd /Users/philtullai/ai-agents/duck-ops
/Users/philtullai/ai-agents/duckAgent/.venv/bin/python -m pytest tests -q
```

Compile runtime files:

```bash
cd /Users/philtullai/ai-agents/duck-ops
python3 - <<'PY'
from pathlib import Path
import py_compile

for path in sorted(Path("runtime").glob("*.py")):
    py_compile.compile(str(path), doraise=True)
print("compiled runtime files")
PY
```

Inspect Business Desk status:

```bash
cd /Users/philtullai/ai-agents/duck-ops
python3 runtime/review_loop.py handle --text 'desk status'
```

## Documentation Rules

- Root `README.md` owns onboarding and routing only.
- `BOOTSTRAP.md` owns repo boundaries and local runtime layout.
- `OPENCLAW_PHASE2_ROADMAP.md` owns Duck Ops roadmap context.
- `OPENCLAW_PHASE2_IMPLEMENTATION_CHECKLIST.md` owns detailed implementation status.
- `contracts/` own payload definitions.
- `config/` owns policies and evaluator rules.
- `/Users/philtullai/ai-agents/duck-ops/output/operator/master_roadmap.md` owns cross-repo roadmap status and completed-work history, even though most other `output/` files stay generated/ignored.

When changing a payload shape, update the contract, the producer, the reader, and the tests together.

## Guard Skills

Use the Duck skills when the change fits:

- `duck-change-planner` for major lanes or cross-repo changes.
- `duck-architecture-guard` for boundary or architecture drift.
- `duck-automation-safety` for browser, scheduler, approval, and live-mutation changes.
- `duck-data-model-governance` for state, schema, latest artifacts, and operator-output changes.
- `duck-documentation-governance` for roadmap, policy, schedule, or docs-structure changes.
- `duck-reliability-review` for health, stale data, scheduled jobs, or incident follow-up.
- `duck-ship-review` before meaningful commits or pushes.

## Practical Defaults

- Use `rg` for searches.
- Use `apply_patch` for manual edits.
- Keep generated files out of git unless explicitly approved.
- Prefer thin adapters around `runtime/operator_interface_contracts.py` over duplicate dashboard readers.
- Surface actionable recommendations in email/Business Desk, not duplicated across every channel.
