# Contract: `custom_design_case`

## Purpose

Represent one customer custom-design request as a structured design-work case instead of a scattered message thread.

## Sources

- mailbox emails already visible to Duck Ops
- later customer-support threads or platform exports

## Required fields

- `artifact_id`
- `artifact_type`
- `channel`
- `source_refs`
- `customer_name`
- `request_summary`
- `normalized_brief`
- `open_questions`
- `ready_for_manual_design`

## Typical normalized brief fields

- `theme_or_character`
- `requested_colors`
- `requested_deadline`
- `recipient_or_occasion`
- `design_constraints`

## Safety rules

- if the request is too vague, keep `ready_for_manual_design = false`
- do not assume a design brief is complete just because the customer said "custom"
- preserve the original wording as evidence for later human review
