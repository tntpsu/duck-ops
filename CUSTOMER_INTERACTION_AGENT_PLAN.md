# Duck Ops Customer Interaction Agent Plan

## Goal

Build a business-facing Duck Ops agent that helps run the parts of the duck business that sit between customer demand, operator judgment, design work, inventory state, and manufacturing follow-through.

This is not just a "customer support bot."

From the business side, the eventual agent needs to help with:

- replying to reviews
- spotting customer issues and recommending the right recovery path
- collecting and structuring custom design requests
- tracking manual design work in Google Tasks
- reviewing DuckAgent social posts and sending them back for rework or approval
- learning from social post performance
- tracking which ducks exist and what stock is available
- deciding what to print next across Shopify and Etsy demand
- later, helping coordinate printers, filament, and replenishment

## Core Boundary

The first versions should remain operator-led and fail closed.

That means:

- do not let Duck Ops send customer emails automatically in the first slice
- do not let Duck Ops refund, resend, cancel, or modify orders automatically in the first slice
- do not let Duck Ops start printers automatically in the first slice
- do let Duck Ops normalize, score, recommend, queue, and track work

## Relationship To Existing Systems

- `duckAgent` remains the creative and business-capability runtime
- `duck-ops` remains the evaluator, operator, and business-orchestration workspace
- OpenClaw review intelligence, customer intelligence, and operator messaging should feed this plan rather than being replaced by it

So the customer interaction agent is best thought of as:

- a new Duck Ops operating lane
- built on top of existing review, approval, queue, and notification surfaces
- eventually spanning customer, creative, inventory, and printer decisions

## Operating Lanes

### Lane 1. Customer issue and review handling

This lane covers:

- Etsy public review replies
- negative or confused inbound customer messages
- damage, delay, wrong-item, or quality complaints
- operator guidance on whether to:
  - reply
  - refund
  - resend
  - escalate
  - wait for missing context

Primary outputs:

- `customer_case`
- `response_recommendation`
- `recovery_recommendation`
- operator-ready summary for WhatsApp / email

### Lane 2. Custom design intake

This lane covers:

- customers asking for a custom duck
- customers refining a custom request over multiple messages
- capturing the actual design brief instead of a scattered message thread

Primary outputs:

- `custom_design_case`
- structured design brief
- open questions
- proposed next operator response
- optional Google Task creation for manual design work

### Lane 3. Creative review and approval operations

This lane covers:

- reviewing DuckAgent social draft outputs
- deciding:
  - approve
  - send back for rework
  - schedule
  - discard
- later learning from social performance and feeding that back upstream

Primary outputs:

- `creative_review_case`
- approval / revision decision
- revision reasons
- later performance feedback summary

### Lane 4. Inventory and print queue intelligence

This lane covers:

- low-stock / no-stock detection across Shopify and Etsy
- deciding what should be printed next
- combining:
  - active orders
  - low inventory
  - trend momentum
  - recent sales
  - future stock goals

Primary outputs:

- `print_queue_candidate`
- `replenishment_priority`
- `label_queue`
- `inventory_risk`

### Lane 5. Printer and filament orchestration

This is later-phase work, not the first slice.

This lane would cover:

- which printer is done
- which printer is idle or blocked
- what colors are currently loaded
- which jobs fit which machines
- when filament needs to be reordered

Primary outputs:

- `printer_state`
- `print_job_assignment`
- `filament_risk`
- `purchase_recommendation`

## Canonical Artifacts To Add

The first real implementation should stop inventing ad hoc JSON for each lane and introduce stable Duck Ops artifacts.

### 1. `customer_case`

Represents one customer-facing issue or reply opportunity.

Should capture:

- source channel
- customer text
- order / listing context when available
- issue type
- sentiment / urgency
- recommended next action
- recommended recovery action
- missing context

### 2. `custom_design_case`

Represents one custom design request.

Should capture:

- customer name / handle when known
- source messages
- normalized design brief
- requested motif / colors / timing
- open questions
- design complexity
- whether it is ready for manual design work
- linked Google Task id when created

### 3. `creative_review_case`

Represents one social or creative artifact needing business approval.

Should capture:

- source run / asset
- platform intent
- why DuckAgent thinks it is worth posting
- operator concerns
- revision requests
- final approval / rejection

### 4. `print_queue_candidate`

Represents one duck or SKU that might need printing or restocking.

Should capture:

- product identity
- current stock by channel
- recent sales signal
- low-stock urgency
- demand-confidence estimate
- recommended print quantity
- why now

### 5. `printer_state`

Later-phase artifact for printer orchestration.

Should capture:

- printer id
- current job
- current material / color
- job completion state
- empty / blocked / needs operator

## Phased Build

### Phase A. Unified operating inbox

Goal:

- stop scattering business work across isolated flows

Build:

- normalize review cases, customer cases, creative review cases, and print-queue candidates into one Duck Ops queue model
- surface them through the operator lane with clear action meaning
- keep execution read-only where possible

Acceptance:

- the operator can tell what the item is
- the operator can tell what approving it will do
- duplicate or stale items are suppressed

### Phase B. Customer issue and review guidance

Goal:

- make Duck Ops useful on real customer-facing decisions without letting it act unsafely

Build:

- extend current review reply and customer-intelligence work into a richer `customer_case`
- add recommended recovery action categories:
  - reply only
  - refund
  - resend
  - refund + resend review
  - escalate
  - wait for context
- make operator cards explain whether the issue is:
  - insufficient context
  - high risk
  - clear recovery recommendation

Acceptance:

- customer cases feel clearer than raw emails or review text
- recommendations are easy to override
- operator can approve or rewrite the response path cleanly

Concrete implementation checklist for the next builds:

1. Stage Etsy order enrichment on `customer_case`
   - match Etsy reviews by `transaction_id`
   - match Etsy conversations by `order #...`
   - attach:
     - product title
     - buyer name
     - receipt/order id
     - payment / shipment status
2. Stage tracking enrichment
   - carry tracking number and carrier when Etsy receipt data already has it
   - add a carrier-specific status lookup adapter later, starting with USPS when credentials exist
   - keep tracking checks read-only
3. Stage prior-resolution memory
   - carry `resolution_enrichment` on `customer_case`
   - detect:
     - refund already issued
     - public Etsy review reply already posted
     - multiple shipments already recorded on the Etsy receipt
   - suppress or downgrade staged packets so Duck Ops does not keep asking for actions you already took
4. Stage recovery packets
   - `reply packet`
   - `refund packet`
   - `replacement packet`
   - `wait for tracking packet`
5. Surface those packets in the operator lane with explicit approval meaning
6. Only after the staged path feels trustworthy:
   - add browser/operator shortcuts for Etsy refund or resend workflows
   - keep those still operator-confirmed

Detailed Phase B implementation sequence:

### Phase B1. Resolution memory and stale-action suppression

Build:

- enrich `customer_case` with prior-resolution signals from:
  - Etsy order status
  - Etsy shipment history
  - Duck Ops review-reply execution history
- suppress clearly stale action packets such as:
  - already-refunded refund/replacement recommendations
  - already-posted public review replies
- downgrade softer signals such as possible prior resend into a watch/confirm state instead of a fresh action

Acceptance:

- Duck Ops stops re-suggesting actions that were already completed
- nightly summaries stop surfacing clearly stale customer work
- possible prior resend history stays visible without falsely triggering `buy_label_now`

### Phase B2. Live tracking and wait-state guidance

Build:

- add read-only USPS tracking lookup using tracking numbers already staged from Etsy or Shopify
- distinguish:
  - moving normally
  - stalled
  - delivered
  - exception / failed delivery
- upgrade `wait_for_tracking` packets and nightly watch items with real carrier state

Acceptance:

- shipping-delay cases stop feeling like guesswork
- Duck Ops can explain when to wait versus when to consider refund/resend review

### Phase B3. Explicit operator recovery decisions

Build:

- persist explicit operator decisions such as:
  - `approve refund`
  - `approve replacement`
  - `approve wait`
  - `approve reply only`
- use those decisions to unlock:
  - `buy_label_now`
  - refund-ready packets
  - safer nightly label-buy reminders

Acceptance:

- replacement labels are only requested after a real operator choice
- Duck Ops can distinguish unresolved cases from already-approved recoveries

### Phase B4. Custom design tasking and customer handoff

Build:

- create Google Tasks API task records for ready `custom_design_case` items
- keep blocked custom requests in a clarification-needed state
- later hand ready briefs into the concept-builder workflow

Acceptance:

- custom design work stops living only in inbox threads
- ready custom work is visible as a tracked manual task

### Phase C. Custom design intake and tasking

Goal:

- turn custom design conversations into structured work

Build:

- normalize custom-design messages into `custom_design_case`
- generate a structured design brief
- surface missing questions
- create or update a Google Task for manual design work when the case is ready

Acceptance:

- a scattered design conversation turns into one coherent brief
- manual design tasks stop being lost in message threads

### Phase D. Creative review and learning loop

Goal:

- make Duck Ops the operator review surface for social and creative outputs

Build:

- unify creative review cases with the existing viewer / review-email flows
- add performance summaries back into Duck Ops so it can say:
  - what posts worked
  - what did not
  - what themes or hooks keep winning

Acceptance:

- social review becomes a routine business lane, not a side task
- useful performance feedback starts shaping future creative drafts

### Phase E. Inventory and print queue intelligence

Goal:

- decide what to print next using business signal, not only manual instinct

Build:

- merge Shopify and Etsy stock signals
- detect low-stock / no-stock states
- combine with active orders, trend demand, and recent sales
- create `print_queue_candidate` recommendations with quantity guidance

Acceptance:

- Duck Ops can explain why one duck should be printed before another
- low-stock items stop being purely manual discovery

### Phase F. Printer and filament orchestration

Goal:

- coordinate actual manufacturing operations

Build:

- ingest printer state
- record loaded materials / colors
- track job completion
- recommend which printer should take the next job
- track filament depletion and reorder timing

Acceptance:

- Duck Ops can recommend the next print job with real machine context
- filament reorder timing becomes proactive instead of reactive

## Immediate Next Implementation Slice

Implemented on April 6, 2026:

- `customer_case`, `custom_design_case`, and `print_queue_candidate` contracts are formalized
- current review, mailbox, and weekly-insight data are mapped into those contracts
- an operator-facing customer interaction queue is written to:
  - `state/customer_interaction_queue.json`
  - `output/operator/customer_interaction_queue.md`
- low-signal customer cases are suppressed, and Etsy conversation emails are collapsed into one queue item per customer thread
- `customer_case` now carries explicit `context_state`, `response_recommendation`, and `recovery_recommendation`
- first-pass Etsy order and tracking enrichment is staged on `customer_case`:
  - Etsy order-email index
  - cached Etsy receipt snapshot
  - product / receipt / shipment / tracking context when matched
- first-pass prior-resolution memory is staged on `customer_case`:
  - refund history from Etsy order status
  - public review-reply history from Duck Ops execution sessions
  - possible prior resend signal from multiple Etsy shipments
- stale customer packets are now suppressed when Duck Ops can tell the refund or public review reply already happened
- explicit operator recovery decisions are now supported via:
  - `state/customer_recovery_decisions.jsonl`
  - `runtime/customer_recovery_decisions.py`
- customer cases, action packets, and nightly summaries now honor persisted decisions such as:
  - `replacement`
  - `refund`
  - `wait`
  - `reply_only`
- the customer lane now has a lightweight operator surface:
  - `output/operator/current_customer_action.md`
  - `output/operator/customer_queue.md`
  - `runtime/customer_operator.py`
- `review_loop.py handle` now delegates `customer status`, `customer next`, and customer decisions like `replacement C301 because ...` into that lane
- staged customer action packets now exist:
  - `state/customer_action_packets.json`
  - `output/operator/customer_action_packets.md`
  - reply / refund / replacement packet shaping is explicit and still fail-closed
- Etsy and Shopify open-order snapshots now exist for nightly operations:
  - `state/normalized/etsy_open_orders_snapshot.json`
  - `state/normalized/shopify_open_orders_snapshot.json`
  - `state/normalized/packing_summary.json`
- a nightly action summary preview is now written to:
  - `state/nightly_action_summary.json`
  - `output/operator/nightly_action_summary.md`
- the nightly action digest is now designed to email after 7 PM local with sections for:
  - customer issues needing reply
  - buy replacement labels now
  - orders to pack
  - custom / novel ducks to make
  - watch list
- read-only USPS tracking and Google Tasks bridges now exist as fail-closed adapters:
  - `runtime/usps_tracking.py`
  - `runtime/google_tasks_bridge.py`
  - they currently report `credentials_missing` until real auth/config is present
- all actions remain staged or operator-approved

Next implementation slice:

1. add real USPS credentials / endpoint config so live carrier lookups can start
2. add real Google Tasks credentials / task-list config so ready custom briefs can create tasks
3. expose customer packets more proactively in the operator push / WhatsApp flow instead of only the manual `customer status` lane
4. keep all customer-facing and manufacturing actions manual in the first pass

Why this is first:

- it helps multiple business lanes immediately
- it reuses the queue and operator surfaces already built
- it avoids jumping straight to printer automation before the work queue is trustworthy

## Data Sources To Plan For

### Already realistic soon

- Etsy reviews and review replies
- inbound mailbox messages already landing in monitored email
- DuckAgent creative artifacts and review bundles
- Shopify and Etsy inventory snapshots
- Google Tasks for manual design work

### Later

- social performance metrics
- printer telemetry
- filament inventory
- label-printing state

## Safety Rules

- customer-facing actions should start as recommendations, not autonomous sends
- refund or resend should require explicit operator confirmation
- print-queue recommendations can be automatic, but printer execution should be staged first
- every operator card must explicitly say what approval will do

## Success Criteria

Duck Ops should eventually feel like the operations layer for the business, not only a review scorer.

That means it should help answer:

- what customer issue needs attention first?
- should I reply, refund, resend, or wait?
- what custom duck design work is open?
- what creative should I approve or rework?
- what duck should I print next?
- what printer is ready and what material do I need soon?
