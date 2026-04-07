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

Planning docs:

- `OPENCLAW_PHASE2_ROADMAP.md`
- `OPENCLAW_PHASE2_IMPLEMENTATION_CHECKLIST.md`
- `CUSTOMER_INTERACTION_AGENT_PLAN.md`

Current staged customer/work outputs:

- `state/customer_interaction_queue.json`
- `output/operator/customer_interaction_queue.md`
- `state/customer_recovery_decisions.jsonl`
- `state/customer_action_packets.json`
- `output/operator/customer_action_packets.md`
- `output/operator/current_customer_action.md`
- `output/operator/customer_queue.md`
- `state/nightly_action_summary.json`
- `output/operator/nightly_action_summary.md`
- `state/normalized/etsy_open_orders_snapshot.json`
- `state/normalized/shopify_open_orders_snapshot.json`
- `state/normalized/packing_summary.json`
- `state/normalized/usps_tracking_snapshot.json`
- `state/google_tasks_custom_design_tasks.json`

Customer recovery decisions can now be staged with:

- `runtime/customer_recovery_decisions.py record --receipt-id ... --resolution replacement|refund|wait|reply_only --note "..."`
- `runtime/customer_operator.py status`
- `runtime/customer_operator.py handle --text 'replacement C301 because ...'`
