# `custom_build_task_candidate`

Represents one paid, unfulfilled custom-order line that should become tracked manual build work.

This artifact exists before Google Tasks credentials are present, so Duck Ops can stage and review the work even while task creation is still blocked.

## Required fields

- `artifact_id`
- `artifact_type`
- `buyer_name`
- `channel`
- `order_ref`
- `product_title`
- `quantity`
- `custom_design_summary`
- `ready_for_task`
- `google_task_status`
- `source_refs`

## Optional fields

- `transaction_ids`
- `custom_type`
- `personalization`
- `created_at`
- `google_task_id`
- `google_task_web_view_link`

## Behavior notes

- `ready_for_task = true` means Duck Ops has enough order detail to create a manual build task when Google Tasks auth is available.
- `google_task_status` should remain fail-closed:
  - `not_created`
  - `credentials_missing`
  - `tasklist_unavailable`
  - `created`
  - `create_failed`
