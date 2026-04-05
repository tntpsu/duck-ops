# Contract: `publish_candidate`

## Purpose

Represent one pre-publish artifact DuckAgent already generated so OpenClaw can decide whether it is ready, weak, or should be discarded.

## Pilot sources

- `newduck` approval emails
- `weekly sale playbook` review emails

## Required fields

- `artifact_id`
- `artifact_type`
- `flow`
- `run_id`
- `source_refs`
- `candidate_summary`
- `supporting_context`

## Decision labels

- `publish_ready`
- `needs_revision`
- `discard`

## Key normalization rules

- parse email artifacts into structured summaries
- keep original subject or message reference as evidence
- attach trend and catalog context whenever available
- preserve multiple platform variants inside one candidate when needed

## Fail-closed rule

If the artifact is materially incomplete, weakly supported, unclear, or unjustifiably duplicative, it must not be marked `publish_ready`.
