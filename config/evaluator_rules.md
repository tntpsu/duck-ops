# Evaluator Rules Reference

## Purpose

This file is the implementation-facing summary of the roadmap rules.

Use it as the compact reference for Phase 1 and Phase 2 evaluator behavior.

## Global rules

- All evaluators must emit:
  - score
  - confidence
  - priority
  - decision
  - evidence bullets
- Confidence caps apply before final output.
- Weak evidence must fail closed.
- Missing context should reduce certainty, not be silently invented.

## Confidence caps

- `0.60` max if decision is based on one source only
- `0.70` max if catalog match is unknown
- `0.70` max if artifact is parsed from partial email only
- `0.75` max if no relevant outcome history exists

Use the lowest applicable cap.

## Trend ranker

Weights:

- commercial signal strength: 30
- persistence: 20
- corroboration: 15
- catalog gap or coverage clarity: 15
- execution feasibility: 10
- historical hit rate: 10

Thresholds:

- `worth_acting_on`: 75+
- `watch`: 45-74
- `ignore`: below 45

Action frame:

- `promote`
- `build`
- `wait`
- `ignore`

## Quality gate

Weights:

- trend or campaign support: 20
- brand fit: 20
- clarity and specificity: 15
- differentiation: 15
- likely conversion quality: 15
- timing fit: 10
- risk penalties: 5

Thresholds:

- `publish_ready`: 82+
- `needs_revision`: 55-81
- `discard`: below 55

Fail-closed conditions:

- materially incomplete artifact
- weak support
- unclear copy or framing
- unjustified duplication
- wrong timing for likely performance

## Customer intelligence

Weights:

- dissatisfaction severity: 30
- urgency: 20
- business impact: 15
- recoverability: 15
- policy clarity: 10
- context completeness: 10

Thresholds:

- `reply_now`: 70+ and safe to draft
- `watch`: 40-69
- `escalate`: 70+ with ambiguity or high risk
- `needs_human_context`: missing key context

Safety rule:

- If there is not enough context to draft safely, do not draft confidently.
