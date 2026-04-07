# `etsy_conversation_thread`

Represents one Etsy conversation thread that should be reviewed in the Etsy browser surface when email notifications are too weak to act on directly.

This artifact is a staging and tracking record, not a browser automation result.

## Required fields

- `artifact_id`
- `artifact_type`
- `conversation_thread_key`
- `conversation_contact`
- `browser_review_status`
- `latest_message_preview`
- `browser_url_candidates`
- `source_artifact_id`
- `source_refs`

## Optional fields

- `grouped_message_count`
- `order_enrichment`
- `source_artifact_ids`
- `open_in_browser_hint`

## Behavior notes

- `browser_review_status = needs_browser_review` means Duck Ops needs an operator or later browser lane to open the Etsy thread and capture the real message context.
- `browser_url_candidates` can include a direct message URL when the notification email exposes one, but should always include a safe Etsy inbox fallback.
