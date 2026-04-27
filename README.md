# duck-ops

Duck Ops is the operator workspace for the myJeepDuck automation system. It owns review queues, policy checks, Business Desk surfaces, promotion readiness, customer/operator state, and the local control layer around DuckAgent and OpenClaw.

DuckAgent executes business workflows. Duck Ops decides what should be reviewed, surfaced, reconciled, promoted, or held.

## Start Here

Read these first:

1. `AGENTS.md` - repo rules for AI assistants and human contributors.
2. `BOOTSTRAP.md` - repo boundaries and local runtime layout.
3. `OPENCLAW_PHASE2_ROADMAP.md` - Duck Ops/OpenClaw roadmap context.
4. `OPENCLAW_PHASE2_IMPLEMENTATION_CHECKLIST.md` - current implementation status and acceptance checks.
5. `/Users/philtullai/ai-agents/duck-ops/output/operator/master_roadmap.md` - cross-repo roadmap status, completed work, and highest-value next items.
6. `/Users/philtullai/ai-agents/duckAgent/docs/current_system/AGENT_GOVERNANCE_POLICY.md` - shared automation authority and approval policy.

## What This Repo Owns

- `runtime/` - operator logic, review loop, Business Desk builders, health checks, queue handling, and policy helpers.
- `contracts/` - compact data contracts for customer cases, trends, publish candidates, print queues, and operator payloads.
- `config/` - brand guardrails, evaluator rules, customer reply policy, and source definitions.
- `OPENCLAW_PHASE2_*` docs - Duck Ops implementation roadmap and checklist.
- `WHATSAPP_OPERATOR.md` - short-message operator mode for review/trend approvals only.
- `state/` and `output/` - generated local state and operator reports, normally ignored by git.

Duck Ops does not own:

- DuckAgent flow implementation.
- OpenClaw engine/platform source.
- live credentials, browser profiles, or local `launchd` plists.
- generated caches, logs, or one-off runtime artifacts.

## Quick Start

Use DuckAgent's virtual environment when dependencies overlap:

```bash
cd /Users/philtullai/ai-agents/duck-ops
/Users/philtullai/ai-agents/duckAgent/.venv/bin/python -m pytest tests -q
```

Compile runtime files quickly:

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

## Common Commands

Business Desk commands run through the main review loop:

```bash
python3 runtime/review_loop.py handle --text 'desk status'
python3 runtime/review_loop.py handle --text 'desk next'
python3 runtime/review_loop.py handle --text 'desk show customer'
python3 runtime/review_loop.py handle --text 'desk show builds'
python3 runtime/review_loop.py handle --text 'desk show packing'
python3 runtime/review_loop.py handle --text 'desk show stock'
python3 runtime/review_loop.py handle --text 'desk show reviews'
python3 runtime/review_loop.py handle --text 'desk show roi'
python3 runtime/review_loop.py handle --text 'desk show freshness'
python3 runtime/review_loop.py handle --text 'status all'
```

Customer recovery decisions can be staged with:

```bash
python3 runtime/customer_recovery_decisions.py record --receipt-id ... --resolution replacement|refund|wait|reply_only --note "..."
python3 runtime/customer_operator.py status
python3 runtime/customer_operator.py handle --text 'replacement C301 because ...'
```

## Operating Model

Duck Ops reads DuckAgent outputs, mailbox-derived signals, platform snapshots, and local state. It turns those into:

- review items
- policy decisions
- promotion-readiness candidates
- Business Desk summaries
- health and freshness signals
- deterministic execution queues where a lane has explicit approval

The default posture is observe, recommend, and require approval. Auto-action requires a policy, evidence, a promotion gate, and operator-visible receipts.

## Etsy Browser Policy

Etsy Playwright/browser automation is constrained and should not run ad hoc.

Current design:

- exactly three Etsy Playwright windows per day
- one morning, one afternoon, and one evening window with jitter
- customer read: up to 2 threads per session
- review reply: up to 2 approved replies per session
- relist: up to 1 listing in one randomly chosen session per day
- notifier and other sidecars should stay read-only and skip Etsy browser preflight

Etsy API reads are a different lane from browser automation. Do not blur those two in docs, code, or health checks.

## Shared Interface Contracts

Shared compact operator interfaces live in:

- `runtime/operator_interface_contracts.py`

The Business Desk, Even/Pulse widget API, and future companion readers should use that module instead of recomputing counts from normalized state in multiple places.

## Documentation Map

- `BOOTSTRAP.md` - repo layout, runtime boundaries, and local-only paths.
- `OPENCLAW_PHASE2_ROADMAP.md` - operator-system roadmap.
- `OPENCLAW_PHASE2_IMPLEMENTATION_CHECKLIST.md` - phase checklist and acceptance criteria.
- `CUSTOMER_INTERACTION_AGENT_PLAN.md` - customer interaction design and roadmap.
- `contracts/` - shape and meaning of operator payloads.
- `config/` - policies and evaluator rules.
- `/Users/philtullai/ai-agents/duckAgent/docs/current_system/README.md` - cross-repo current-system docs hub.

## Generated Files

These normally do not belong in git:

- `output/`
- `state/`
- `.playwright-cli/`
- `.pytest_cache/`
- local browser auth/session state
- local launchd/runtime helpers
- logs and lock files

Only commit generated files when they are intentional fixtures, docs examples, or explicitly approved canonical outputs.

## Current Sharp Edges

- Several useful operator reports live under ignored `output/`; they are operational truth locally but not stable repo source.
- Browser automation safety depends on local wrappers and schedules outside git.
- WhatsApp is intentionally narrow now: review/trend approvals only. Email is the primary channel for social posts, sales, and Business Desk summaries.
- The master roadmap is generated under `output/operator/` but is treated as a canonical operator doc; keep that exception explicit.
