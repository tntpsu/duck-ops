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
- `state/business_operator_desk.json`
- `output/operator/business_operator_desk.md`
- `state/custom_build_task_candidates.json`
- `output/operator/custom_build_task_candidates.md`
- `state/etsy_conversation_browser_sync.json`
- `output/operator/etsy_conversation_browser_sync.md`
- `state/normalized/etsy_open_orders_snapshot.json`
- `state/normalized/shopify_open_orders_snapshot.json`
- `state/normalized/packing_summary.json`
- `state/normalized/print_queue_candidates.json`
- `state/normalized/usps_tracking_snapshot.json`
- `state/google_tasks_custom_design_tasks.json`

Customer recovery decisions can now be staged with:

- `runtime/customer_recovery_decisions.py record --receipt-id ... --resolution replacement|refund|wait|reply_only --note "..."`
- `runtime/customer_operator.py status`
- `runtime/customer_operator.py handle --text 'replacement C301 because ...'`

Unified desk commands now run through the main operator loop too:

- `python3 runtime/review_loop.py handle --text 'desk status'`
- `python3 runtime/review_loop.py handle --text 'desk next'`
- `python3 runtime/review_loop.py handle --text 'desk show customer'`
- `python3 runtime/review_loop.py handle --text 'desk show builds'`
- `python3 runtime/review_loop.py handle --text 'desk show packing'`
- `python3 runtime/review_loop.py handle --text 'desk show stock'`
- `python3 runtime/review_loop.py handle --text 'desk show reviews'`
- `python3 runtime/review_loop.py handle --text 'status all'`

Shared operator interface contracts now live in:

- `runtime/operator_interface_contracts.py`

Use that module as the canonical compact surface for lightweight readers like the
Business Desk, Even/Pulse widget API, and any future companion app payloads.
Keep those readers as thin adapters instead of recomputing counts from
normalized state in multiple places.

Etsy inbox truth-sync is now available as a dedicated OpenClaw lane:

- `python3 runtime/customer_inbox_refresh.py --limit 8 --skip-outside-hours --start-hour 7 --start-minute 30 --end-hour 23 --end-minute 59`

Recommended local scheduling for that lane:

- plan exactly 3 Etsy Playwright windows per day
- use one morning, one afternoon, and one evening window with a jittered run time inside each window
- cap each session instead of draining the full backlog:
  - customer read: up to 2 threads
  - review reply: up to 2 approved replies
  - relist: up to 1 listing in one randomly chosen session per day
- keep `notifier.py` read-only by skipping Etsy customer refresh preflight from the sidecar

This lane is intentionally read-only against Etsy:

- it reuses the trusted Etsy seller session
- verifies direct `/messages/<id>` links when safe
- updates customer thread truth and operator outputs
- never types or sends customer replies

Publish review reconciliation is now fail-closed in Duck Ops:

- if DuckAgent already shows a `newduck` listing as published to Shopify/Etsy, or a weekly sale as already published, OpenClaw reconciles the pending review state back to handled instead of resurfacing the old review item
