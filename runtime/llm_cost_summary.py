"""Surface 10 producer: aggregate LLM spend from state/llm_call_log.jsonl
and emit state/llm_cost_summary.json for the /portal/intel/cost reader.

Architecture matches the producer-on-schedule + cheap-reader pattern
already established for system_health, current_learnings, weekly_strategy
packet, and learning inspector. The reader (page handler) never opens
the raw 10-MB jsonl on each request — it reads this pre-computed cache
file in ~2ms.

Cadence: invoke this from launchd alongside current_learnings (07:10
daily) plus on-demand via OS-card refresh button. The aggregation is
~50ms on a 30-line log; even at 30k lines it stays under 1s.

Soft alert: if today's spend > LLM_DAILY_SPEND_ALERT_USD (default $5),
write state/llm_spend_alert_signal.json. The OS card / Desk tile reads
this file's presence + payload to surface "today's spend over alert
threshold" without polling the raw log.

Why we don't do hard ceiling: today's catastrophic-runaway probability
is low (daily/weekly crons, not 24/7 agents). A wrong ceiling that
auto-stops a Wednesday jeepfact mid-run is worse than $15 overspend.
Observability first, ceiling second — and ceiling requires real
data to pick a non-arbitrary number.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any

# Reuse existing pricing table + helpers — single source of truth so
# Scope B's instrumentation never drifts from observability rates.
from llm_call_helpers import (
    LLM_CALL_LOG_PATH,
    MODEL_PRICING_USD_PER_1M_TOKENS,
    PER_CALL_IMAGE_COST_USD,
    estimate_cost_usd,
)
from governance_review_common import DUCK_OPS_ROOT, now_local_iso, write_json


SUMMARY_PATH = DUCK_OPS_ROOT / "state" / "llm_cost_summary.json"
ALERT_SIGNAL_PATH = DUCK_OPS_ROOT / "state" / "llm_spend_alert_signal.json"

# Default alert threshold — operator can override via env. Conservative
# until we see real spend distributions. The flag is read at run time
# (not at import) so a launchd plist change takes effect on next fire.
DEFAULT_ALERT_THRESHOLD_USD = 5.00


# ─── artifact_id → flow inference ────────────────────────────────────
#
# Today's log entries carry artifact_id strings like:
#   "publish::reviews_reply_positive::2026-05-23::review-1"
#   "score::review_reply::2026-05-25::review-6"
# Future Scope B duckAgent flows will follow a similar convention. We
# parse loosely — the first segment is the "kind" (publish/score/etc),
# the second is the "flow" (reviews_reply_positive). Both feed the
# operator's "spend by flow" rollup.

_ARTIFACT_ID_PATTERN = re.compile(r"^(?P<kind>[a-z_]+)::(?P<flow>[a-z_]+)(::.*)?$")


def parse_artifact_id(artifact_id: str | None) -> tuple[str, str]:
    """Return (kind, flow). Both default to 'unknown' on parse failure
    so the producer never crashes on malformed entries."""
    if not artifact_id:
        return ("unknown", "unknown")
    match = _ARTIFACT_ID_PATTERN.match(str(artifact_id).strip().lower())
    if not match:
        return ("unknown", "unknown")
    return (match.group("kind") or "unknown", match.group("flow") or "unknown")


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _entry_cost_usd(entry: dict[str, Any]) -> tuple[float, str]:
    """Compute USD cost for one log entry. Returns (cost, source) where
    source is 'token_priced', 'per_call_image', 'unknown_model', or
    'no_pricing'. Operator-visible source helps explain underestimates
    when scope-B coverage is partial."""
    model = str(entry.get("model") or "").strip()
    # Image calls log with a per-call kind marker; the duckAgent
    # openai_helper.openai_dalle_generate_image path (Scope B) will
    # write {"kind": "image", "model": "gpt-image-1", "image_count": N}.
    if entry.get("kind") == "image" and model in PER_CALL_IMAGE_COST_USD:
        n = _safe_int(entry.get("image_count")) or 1
        return (round(PER_CALL_IMAGE_COST_USD[model] * n, 6), "per_call_image")
    prompt_tokens = _safe_int(entry.get("prompt_tokens"))
    completion_tokens = _safe_int(entry.get("completion_tokens"))
    cost = estimate_cost_usd(prompt_tokens, completion_tokens, model)
    if cost is None:
        if not model:
            return (0.0, "no_model")
        return (0.0, "unknown_model")
    return (cost, "token_priced")


def _entry_date(entry: dict[str, Any]) -> str | None:
    """Parse the 'at' timestamp to a YYYY-MM-DD string (local TZ).
    Returns None if unparseable — the producer skips that entry."""
    raw = str(entry.get("at") or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.astimezone().date().isoformat()


@dataclass
class _Bucket:
    cost_usd: float = 0.0
    call_count: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    unknown_model_count: int = 0
    no_pricing_count: int = 0


def _add_to_bucket(bucket: _Bucket, entry: dict[str, Any], cost: float, source: str) -> None:
    bucket.cost_usd += cost
    bucket.call_count += 1
    bucket.prompt_tokens += _safe_int(entry.get("prompt_tokens"))
    bucket.completion_tokens += _safe_int(entry.get("completion_tokens"))
    if source == "unknown_model":
        bucket.unknown_model_count += 1
    if source == "no_model":
        bucket.no_pricing_count += 1


def _bucket_to_dict(bucket: _Bucket) -> dict[str, Any]:
    return {
        "cost_usd": round(bucket.cost_usd, 4),
        "call_count": bucket.call_count,
        "prompt_tokens": bucket.prompt_tokens,
        "completion_tokens": bucket.completion_tokens,
        "unknown_model_count": bucket.unknown_model_count,
        "no_pricing_count": bucket.no_pricing_count,
    }


def aggregate_llm_costs(
    *,
    log_path: Path | None = None,
    window_days: int = 30,
    now_iso: str | None = None,
) -> dict[str, Any]:
    """Read llm_call_log.jsonl, return aggregated summary.

    The returned dict is the canonical shape the page consumes:
    {
      "generated_at": "...",
      "window_days": 30,
      "log_path": "...",
      "totals": {cost_usd, call_count, ...},
      "today": {date, cost_usd, call_count, ...},
      "by_day": [{date, cost_usd, call_count, ...}, ...],  # last N days
      "by_flow": [{flow, cost_usd, call_count, ...}, ...], # sorted desc
      "by_model": [...],
      "by_provider": [...],
      "data_quality": {malformed_lines, entries_without_at,
                       unknown_model_count, no_pricing_count,
                       pricing_table_models},
    }
    """
    log = log_path if log_path is not None else LLM_CALL_LOG_PATH
    if now_iso:
        try:
            now_dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
            if now_dt.tzinfo is None:
                now_dt = now_dt.astimezone()
        except ValueError:
            now_dt = datetime.now().astimezone()
    else:
        now_dt = datetime.now().astimezone()
    today_str = now_dt.date().isoformat()
    cutoff = (now_dt - timedelta(days=window_days)).date()

    by_day: dict[str, _Bucket] = defaultdict(_Bucket)
    by_flow: dict[str, _Bucket] = defaultdict(_Bucket)
    by_model: dict[str, _Bucket] = defaultdict(_Bucket)
    by_provider: dict[str, _Bucket] = defaultdict(_Bucket)
    totals = _Bucket()

    malformed_lines = 0
    entries_without_at = 0

    if log.exists():
        with log.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    malformed_lines += 1
                    continue
                if not isinstance(entry, dict):
                    malformed_lines += 1
                    continue
                date_str = _entry_date(entry)
                if date_str is None:
                    entries_without_at += 1
                    continue
                try:
                    if date.fromisoformat(date_str) < cutoff:
                        continue
                except ValueError:
                    entries_without_at += 1
                    continue

                cost, source = _entry_cost_usd(entry)
                _, flow = parse_artifact_id(entry.get("artifact_id"))
                model = str(entry.get("model") or "unknown").strip() or "unknown"
                provider = str(entry.get("provider") or "unknown").strip() or "unknown"

                _add_to_bucket(by_day[date_str], entry, cost, source)
                _add_to_bucket(by_flow[flow], entry, cost, source)
                _add_to_bucket(by_model[model], entry, cost, source)
                _add_to_bucket(by_provider[provider], entry, cost, source)
                _add_to_bucket(totals, entry, cost, source)

    today_bucket = by_day.get(today_str, _Bucket())

    by_day_rows = sorted(
        [{"date": d, **_bucket_to_dict(b)} for d, b in by_day.items()],
        key=lambda r: r["date"],
    )
    def _sort_desc(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(items, key=lambda r: -r["cost_usd"])

    return {
        "generated_at": now_local_iso(),
        "window_days": window_days,
        "log_path": str(log),
        "totals": _bucket_to_dict(totals),
        "today": {"date": today_str, **_bucket_to_dict(today_bucket)},
        "by_day": by_day_rows,
        "by_flow": _sort_desc([{"flow": f, **_bucket_to_dict(b)} for f, b in by_flow.items()]),
        "by_model": _sort_desc([{"model": m, **_bucket_to_dict(b)} for m, b in by_model.items()]),
        "by_provider": _sort_desc([{"provider": p, **_bucket_to_dict(b)} for p, b in by_provider.items()]),
        "data_quality": {
            "malformed_lines": malformed_lines,
            "entries_without_at": entries_without_at,
            "unknown_model_count": totals.unknown_model_count,
            "no_pricing_count": totals.no_pricing_count,
            "pricing_table_models": sorted(MODEL_PRICING_USD_PER_1M_TOKENS.keys()),
            "instrumentation_note": (
                "Scope A coverage: duck-ops runtime modules only "
                "(review reply, jeepfact rewriter, weekly sale rewriter, "
                "catalog dedup). Scope B will instrument duckAgent flows "
                "(meme, jeepfact, thursday, blog, profit, newduck, "
                "competitor) which today bypass llm_call_log. Until then, "
                "the totals here are a subset of real spend — reconcile "
                "vs OpenAI billing dashboard weekly."
            ),
        },
    }


def evaluate_alert(summary: dict[str, Any], *, threshold_usd: float) -> dict[str, Any] | None:
    """Return an alert payload if today's spend exceeds threshold.
    Returns None when no alert needs writing — caller deletes any
    stale signal file so the OS card auto-clears."""
    today = summary.get("today") or {}
    today_cost = float(today.get("cost_usd") or 0.0)
    if today_cost <= threshold_usd:
        return None
    return {
        "generated_at": summary.get("generated_at"),
        "today_date": today.get("date"),
        "today_cost_usd": today_cost,
        "threshold_usd": threshold_usd,
        "exceeded_by_usd": round(today_cost - threshold_usd, 4),
        "top_flow_today": (
            (summary.get("by_flow") or [{}])[0].get("flow") if (summary.get("by_flow") or []) else None
        ),
        "severity": "warn" if today_cost < threshold_usd * 2 else "red",
        "recommendation": (
            "Review /portal/intel/cost for the spike. Hard ceiling is "
            "intentionally NOT in place — observability-first per "
            "2026-06-06 scoping decision. If the spike is unexpected, "
            "manually pause the offending flow."
        ),
    }


def _resolve_threshold() -> float:
    raw = os.environ.get("LLM_DAILY_SPEND_ALERT_USD")
    if not raw:
        return DEFAULT_ALERT_THRESHOLD_USD
    try:
        return float(raw)
    except ValueError:
        return DEFAULT_ALERT_THRESHOLD_USD


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--window-days", type=int, default=30,
                        help="Aggregation window (default 30).")
    parser.add_argument("--print-json", action="store_true",
                        help="Emit the summary on stdout as well as writing the cache file.")
    parser.add_argument("--threshold-usd", type=float, default=None,
                        help="Override the alert threshold (default reads LLM_DAILY_SPEND_ALERT_USD env).")
    args = parser.parse_args()

    summary = aggregate_llm_costs(window_days=args.window_days)
    write_json(SUMMARY_PATH, summary)

    threshold = args.threshold_usd if args.threshold_usd is not None else _resolve_threshold()
    alert = evaluate_alert(summary, threshold_usd=threshold)
    if alert is None:
        try:
            ALERT_SIGNAL_PATH.unlink()
        except FileNotFoundError:
            pass
    else:
        write_json(ALERT_SIGNAL_PATH, alert)

    if args.print_json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        totals = summary["totals"]
        today = summary["today"]
        print(
            f"[llm-cost-summary] window={args.window_days}d "
            f"calls={totals['call_count']} cost=${totals['cost_usd']:.4f} "
            f"today=${today['cost_usd']:.4f} "
            f"alert={'YES' if alert else 'no'} threshold=${threshold:.2f}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
