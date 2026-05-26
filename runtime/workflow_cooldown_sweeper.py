"""
Auto-clear stale cooldown-style workflow_control failures.

The April 24 → May 26 stuck-state story: customer_inbox_refresh and
etsy_browser_batch hit the 15-min preemptive cooldown, wrote
state_reason="refresh_failed" / "browser_batch_failed", then sat
there for 33 days because nothing re-attempted after the cooldown
expired.

This sweeper catches that pattern early: any workflow_control lane
whose state_reason matches a known cooldown-style failure AND whose
updated_at is >4h old gets reset to observed/cooldown_expired so
the next scheduled run is eligible.

Conservative by design: only sweeps a strict whitelist. State reasons
that require human/external action (auth_blocked, execution_failed,
blocked_by_upstream, manual_intervention_required, policy_blocked,
stale_input) are explicitly NOT in the whitelist. The sweeper never
clears a lane that genuinely needs the operator.

Intended invocation: as an early step in the sidecar bash, before
the lanes themselves run, so each cycle starts with a clean
workflow_control state.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKFLOW_CONTROL_DIR = DUCK_OPS_ROOT / "state" / "workflow_control"

# State reasons that are *probably* stale cooldown side-effects.
# Each started life as a transient automation event; if nothing's
# refreshed the lane in stale_hours, the originating cooldown is
# long past and the lane should be re-eligible.
CLEARABLE_STATE_REASONS: frozenset[str] = frozenset({
    "refresh_failed",
    "browser_batch_failed",
    "cooldown_active",
    "etsy_browser_in_cooldown",
})

# Conservative threshold: 4h is 16× the 15-min preemptive cooldown,
# and a full safety margin past any Etsy auth retry delay
# (auth_block_retry_delay_seconds = 1800s = 30min).
STALE_THRESHOLD_HOURS = 4.0


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone()
    except ValueError:
        return None


def sweep_stale_cooldowns(
    workflow_control_dir: Path = DEFAULT_WORKFLOW_CONTROL_DIR,
    *,
    now: datetime | None = None,
    stale_hours: float = STALE_THRESHOLD_HOURS,
    clearable_reasons: frozenset[str] = CLEARABLE_STATE_REASONS,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Sweep workflow_control/*.json for stale cooldown-style failures.

    Returns a list of records describing each lane that was cleared
    (or would be cleared if dry_run=True). Each record has lane,
    prior_state, prior_reason, hours_stale, dry_run.
    """
    now = now or datetime.now().astimezone()
    cleared: list[dict[str, Any]] = []
    if not workflow_control_dir.exists():
        return cleared
    for state_file in sorted(workflow_control_dir.glob("*.json")):
        try:
            payload = json.loads(state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        state_reason = str(payload.get("state_reason") or "").strip()
        if state_reason not in clearable_reasons:
            continue
        updated_at = _parse_iso(payload.get("updated_at"))
        if updated_at is None:
            continue
        hours_stale = (now - updated_at).total_seconds() / 3600.0
        if hours_stale < stale_hours:
            continue
        record = {
            "lane": state_file.stem,
            "prior_state": payload.get("state"),
            "prior_reason": state_reason,
            "hours_stale": round(hours_stale, 1),
            "dry_run": dry_run,
        }
        if not dry_run:
            history = payload.get("history") or []
            history.append({
                "state": "observed",
                "state_reason": "cooldown_expired",
                "at": now.isoformat(),
                "summary": (
                    f"Auto-cleared by workflow_cooldown_sweeper: "
                    f"prior {state_reason} sat {hours_stale:.1f}h with no update; "
                    f"the originating cooldown is long expired."
                ),
                "receipt_id": f"cooldown-sweeper-{now.strftime('%Y%m%d%H%M%S')}",
            })
            payload["state"] = "observed"
            payload["state_reason"] = "cooldown_expired"
            payload["updated_at"] = now.isoformat()
            payload["history"] = history
            state_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        cleared.append(record)
    return cleared


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Auto-clear stale cooldown-style workflow_control failures.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be cleared without writing.")
    parser.add_argument("--workflow-control-dir",
                        default=str(DEFAULT_WORKFLOW_CONTROL_DIR))
    parser.add_argument("--stale-hours", type=float,
                        default=STALE_THRESHOLD_HOURS)
    args = parser.parse_args(argv)
    cleared = sweep_stale_cooldowns(
        Path(args.workflow_control_dir),
        stale_hours=args.stale_hours,
        dry_run=args.dry_run,
    )
    print(json.dumps(
        {
            "cleared": cleared,
            "count": len(cleared),
            "stale_threshold_hours": args.stale_hours,
            "dry_run": args.dry_run,
        },
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
