#!/usr/bin/env python3
"""Build the Profit Intel operator surface from workflow_control receipts.

Reads `state/workflow_control/profit-<date>.json` for the last `lookback_days`
and writes `state/profit_intel.json` + `output/operator/profit_intel.md`.

This module is a thin read aggregator. It never recomputes anomaly triggers —
the anomaly verdict comes from DuckAgent's `flows/profit/anomaly.py` via the
`metadata.anomaly` block on the receipt. One source of truth.

See PROFIT_INTEL_PANEL_PLAN.md for the canonical JSON contract and field
semantics.
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = DUCK_OPS_ROOT / "state"
WORKFLOW_CONTROL_DIR = STATE_DIR / "workflow_control"
OUTPUT_OPERATOR_DIR = DUCK_OPS_ROOT / "output" / "operator"

PROFIT_INTEL_STATE_PATH = STATE_DIR / "profit_intel.json"
PROFIT_INTEL_MD_PATH = OUTPUT_OPERATOR_DIR / "profit_intel.md"

ROUTE = "/portal/intel/profit"

WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

STALE_WARN_HOURS = 6
STALE_BANNER_HOURS = 24
EMPTY_STATE_STALE_HOURS = 48

# The profit ingest cron fires once per day at 23:58 local time
# (com.philtullai.duckagent.profit.daily.plist). Before that time
# each day, "today's receipt is missing" is the expected state by
# design — the run captures the full day at end-of-day. The banner
# below should only flag a problem when the scheduled window has
# actually elapsed without a receipt landing.
PROFIT_DAILY_RUN_HOUR = 23
PROFIT_DAILY_RUN_MINUTE = 58
PROFIT_DAILY_RUN_GRACE_MINUTES = 30  # ~32min between cron + write

_DEFAULT_ANOMALY = {
    "triggered": False,
    "reasons": [],
    "sanity_blocked": False,
    "confidence": "normal",
}


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_markdown(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _parse_iso(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _now() -> datetime:
    return datetime.now().astimezone()


_PROFIT_FILE_RE = re.compile(r"^profit-(\d{4}-\d{2}-\d{2})\.json$")


def _collect_recent_receipts(*, lookback_days: int, today: date | None = None) -> list[dict[str, Any]]:
    today = today or _now().date()
    cutoff = today - timedelta(days=lookback_days)
    rows: list[dict[str, Any]] = []
    if not WORKFLOW_CONTROL_DIR.exists():
        return rows
    for path in WORKFLOW_CONTROL_DIR.iterdir():
        match = _PROFIT_FILE_RE.match(path.name)
        if not match:
            continue
        try:
            day = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        if day < cutoff or day > today:
            continue
        payload = _load_json(path)
        if not isinstance(payload, dict):
            continue
        payload["_run_date"] = match.group(1)
        rows.append(payload)
    rows.sort(key=lambda r: r["_run_date"])
    return rows


def _channel_breakdown(metadata: dict[str, Any]) -> dict[str, Any]:
    shopify_orders = metadata.get("shopify_orders")
    etsy_orders = metadata.get("etsy_orders")
    if shopify_orders is None and etsy_orders is None:
        return {}
    return {
        "shopify": shopify_orders if shopify_orders is not None else 0,
        "etsy": etsy_orders if etsy_orders is not None else 0,
    }


def _yesterday_block(receipt: dict[str, Any]) -> dict[str, Any]:
    metadata = receipt.get("metadata") or {}
    return {
        "date": receipt.get("_run_date"),
        "orders": metadata.get("total_orders"),
        "revenue": metadata.get("total_revenue"),
        "net_profit": metadata.get("total_net_profit"),
        "margin": metadata.get("overall_margin"),
        "channels": _channel_breakdown(metadata),
    }


def _delta_pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    if previous == 0:
        return None
    return round(((current - previous) / abs(previous)) * 100, 1)


def _trend_7d(receipts: list[dict[str, Any]]) -> dict[str, Any]:
    if len(receipts) < 7:
        return {"orders_delta_pct": None, "net_delta_pct": None, "margin_delta_pct": None}
    last_seven = receipts[-7:]
    prior_seven = receipts[-14:-7] if len(receipts) >= 14 else []
    if not prior_seven:
        return {"orders_delta_pct": None, "net_delta_pct": None, "margin_delta_pct": None}

    def _avg(rows: list[dict[str, Any]], key: str) -> float | None:
        values = [
            float(r.get("metadata", {}).get(key))
            for r in rows
            if r.get("metadata", {}).get(key) is not None
        ]
        if not values:
            return None
        return sum(values) / len(values)

    return {
        "orders_delta_pct": _delta_pct(_avg(last_seven, "total_orders"), _avg(prior_seven, "total_orders")),
        "net_delta_pct": _delta_pct(_avg(last_seven, "total_net_profit"), _avg(prior_seven, "total_net_profit")),
        "margin_delta_pct": _delta_pct(_avg(last_seven, "overall_margin"), _avg(prior_seven, "overall_margin")),
    }


def _per_day_row(receipt: dict[str, Any]) -> dict[str, Any]:
    metadata = receipt.get("metadata") or {}
    anomaly = metadata.get("anomaly") if isinstance(metadata.get("anomaly"), dict) else {}
    return {
        "date": receipt.get("_run_date"),
        "orders": metadata.get("total_orders"),
        "revenue": metadata.get("total_revenue"),
        "net_profit": metadata.get("total_net_profit"),
        "margin": metadata.get("overall_margin"),
        "channels": _channel_breakdown(metadata),
        "is_anomaly": bool(anomaly.get("triggered")),
        "anomaly_reasons": list(anomaly.get("reasons") or []),
    }


def _channel_mix_window(receipts: list[dict[str, Any]]) -> dict[str, Any]:
    shopify_orders = 0
    etsy_orders = 0
    shopify_revenue = 0.0
    etsy_revenue = 0.0
    for r in receipts:
        meta = r.get("metadata") or {}
        shopify_orders += int(meta.get("shopify_orders") or 0)
        etsy_orders += int(meta.get("etsy_orders") or 0)
        shopify_revenue += float(meta.get("shopify_revenue") or 0)
        etsy_revenue += float(meta.get("etsy_revenue") or 0)
    return {
        "shopify_orders": shopify_orders,
        "etsy_orders": etsy_orders,
        "shopify_revenue": round(shopify_revenue, 2),
        "etsy_revenue": round(etsy_revenue, 2),
    }


def _read_anomaly(receipt: dict[str, Any]) -> dict[str, Any]:
    metadata = receipt.get("metadata") or {}
    candidate = metadata.get("anomaly")
    if not isinstance(candidate, dict):
        return dict(_DEFAULT_ANOMALY)
    result = dict(_DEFAULT_ANOMALY)
    result.update({
        "triggered": bool(candidate.get("triggered")),
        "reasons": list(candidate.get("reasons") or []),
        "sanity_blocked": bool(candidate.get("sanity_blocked")),
        "confidence": str(candidate.get("confidence") or "normal"),
    })
    return result


def _today_action(
    receipt: dict[str, Any] | None,
    today_iso: str,
    *,
    now: datetime | None = None,
) -> str:
    if receipt is None or receipt.get("_run_date") != today_iso:
        # Distinguish "the scheduled cron hasn't fired yet — by design"
        # from "the run was attempted and is in pending state". Pre-23:58
        # the absent receipt is the expected state; downstream displays
        # (page email-status pill, Desk card PENDING badge, weekly
        # narrative) should render this as on-schedule, not as a
        # problem. 2026-05-30 operator question on the Desk card was
        # the same false-alarm pattern as the banner fix shipped earlier.
        if not _profit_run_window_elapsed(now or _now(), run_date=date.fromisoformat(today_iso)):
            return "awaiting_schedule"
        return "pending"
    state = str(receipt.get("state") or "").lower()
    reason = str(receipt.get("state_reason") or "").lower()
    last_verification = receipt.get("last_verification") or {}
    status = str(last_verification.get("status") or "").lower() if isinstance(last_verification, dict) else ""

    if reason == "weekly_operator_email_deferred":
        return "deferred"
    if reason == "report_emailed_anomaly_bypass" or status == "anomaly_bypass":
        return "anomaly_bypass"
    if state == "verified":
        return "sent"
    if reason == "profit_metrics_impossible":
        return "errored"
    if state == "blocked":
        return "blocked"
    return "blocked"


def _next_weekly_email_iso(today: date, cadence_metadata: dict[str, Any] | None) -> str | None:
    if not isinstance(cadence_metadata, dict):
        return None
    target_weekday = cadence_metadata.get("target_weekday")
    if target_weekday is None:
        target_name = (cadence_metadata.get("weekly_email_day") or "").lower()
        try:
            target_weekday = WEEKDAY_NAMES.index(target_name.capitalize())
        except ValueError:
            return None
    days_ahead = (int(target_weekday) - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (today + timedelta(days=days_ahead)).isoformat()


def _email_status(today_receipt: dict[str, Any] | None, latest_receipt: dict[str, Any] | None, today_iso: str, *, now: datetime | None = None) -> dict[str, Any] | None:
    today_action = _today_action(today_receipt, today_iso, now=now)
    cadence_source = today_receipt if today_receipt is not None else latest_receipt
    if cadence_source is None:
        return None
    last_verification = cadence_source.get("last_verification") if isinstance(cadence_source.get("last_verification"), dict) else {}
    metadata = cadence_source.get("metadata") or {}
    cadence_metadata = metadata.get("operator_email_cadence") if isinstance(metadata.get("operator_email_cadence"), dict) else None
    cadence = (cadence_metadata or last_verification or {}).get("cadence") or "daily"
    weekly_email_day = (cadence_metadata or last_verification or {}).get("weekly_email_day")
    next_email_at = _next_weekly_email_iso(date.fromisoformat(today_iso), cadence_metadata or last_verification)
    return {
        "today_action": today_action,
        "next_email_at": next_email_at,
        "cadence": cadence,
        "weekly_email_day": weekly_email_day,
    }


def _data_as_of(receipt: dict[str, Any] | None) -> str | None:
    if receipt is None:
        return None
    return receipt.get("updated_at")


def _staleness_reason(data_as_of_iso: str | None) -> str | None:
    parsed = _parse_iso(data_as_of_iso)
    if parsed is None:
        return None
    age_hours = (_now() - parsed).total_seconds() / 3600
    if age_hours > EMPTY_STATE_STALE_HOURS:
        return "stale_data"
    return None


def _profit_run_window_elapsed(now: datetime, *, run_date: date) -> bool:
    """True iff `run_date`'s scheduled profit run + grace window is past.

    Caller passes the date whose receipt is being checked (the
    business "today" for the page) — NOT now's calendar date. After
    midnight rollover, `now.date()` advances but the prior day's
    23:58 run is what we're looking for, so the scheduled time must
    anchor to `run_date`, not `now`.

    Returns True when `now >= run_date 23:58 + grace`. Before then,
    "no receipt yet" is the designed state and the banner stays
    silent."""
    scheduled = datetime(
        run_date.year,
        run_date.month,
        run_date.day,
        PROFIT_DAILY_RUN_HOUR,
        PROFIT_DAILY_RUN_MINUTE,
        tzinfo=now.tzinfo,
    )
    return now >= scheduled + timedelta(minutes=PROFIT_DAILY_RUN_GRACE_MINUTES)


def build_profit_intel(
    *,
    lookback_days: int = 30,
    today: date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now_dt = now if now is not None else _now()
    today = today or now_dt.date()
    today_iso = today.isoformat()
    generated_at = now_dt.isoformat()

    receipts = _collect_recent_receipts(lookback_days=lookback_days, today=today)
    if not receipts:
        return {
            "available": False,
            "reason": "no_profit_history",
            "generated_at": generated_at,
            "data_as_of": None,
            "route": ROUTE,
            "details": "No profit workflow_control receipts found in the lookback window.",
        }

    latest = receipts[-1]
    data_as_of = _data_as_of(latest)
    stale_reason = _staleness_reason(data_as_of)
    if stale_reason:
        return {
            "available": False,
            "reason": stale_reason,
            "generated_at": generated_at,
            "data_as_of": data_as_of,
            "route": ROUTE,
            "details": f"Most recent profit receipt is older than {EMPTY_STATE_STALE_HOURS}h ({data_as_of}).",
        }

    today_receipt = latest if latest.get("_run_date") == today_iso else None
    payload: dict[str, Any] = {
        "available": True,
        "generated_at": generated_at,
        "data_as_of": data_as_of,
        "yesterday": _yesterday_block(latest),
        "trend_7d": _trend_7d(receipts),
        "anomaly": _read_anomaly(latest),
        "email_status": _email_status(today_receipt, latest, today_iso, now=now_dt),
        "per_day": [_per_day_row(r) for r in receipts],
        "channel_mix_7d": _channel_mix_window(receipts[-7:]),
        "channel_mix_30d": _channel_mix_window(receipts[-30:]),
        "route": ROUTE,
    }
    if today_receipt is None and _profit_run_window_elapsed(now_dt, run_date=today):
        # The 23:58 cron + grace has elapsed and we still don't have
        # today's receipt. That's a genuine miss worth surfacing. Before
        # the window elapses, "no receipt yet" is the by-design state
        # and we stay silent. 2026-05-30 operator question: prior to
        # this gate the banner fired 23h58m of every day, making it
        # both untrue (run not yet expected) and misleading ("check
        # Scheduler Health" while Scheduler Health was green).
        payload["banner"] = "Today's 23:58 profit run is overdue — check Scheduler Health"
    return payload


def _format_money(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _format_pct(value: Any, *, suffix: str = "%") -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.1f}{suffix}"
    except (TypeError, ValueError):
        return "—"


def _format_delta(value: Any) -> str:
    if value is None:
        return "—"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return "—"
    arrow = "↑" if n > 0 else ("↓" if n < 0 else "→")
    return f"{arrow} {abs(n):.0f}%"


def _format_age(data_as_of_iso: str | None) -> str:
    parsed = _parse_iso(data_as_of_iso)
    if parsed is None:
        return "unknown"
    delta = _now() - parsed
    hours = delta.total_seconds() / 3600
    if hours < 1:
        return f"{int(delta.total_seconds() // 60)}m ago"
    if hours < 48:
        return f"{int(hours)}h ago"
    return f"{int(hours // 24)}d ago"


def render_profit_intel_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = ["## Profit Intel"]
    if not payload.get("available"):
        reason = payload.get("reason")
        details = payload.get("details") or "Profit intel unavailable."
        humanized = {
            "no_profit_history": "No profit data yet.",
            "no_today_run": "Today's profit run hasn't produced state.",
            "stale_data": "Data is stale.",
        }.get(reason, "Profit intel unavailable.")
        lines.append(f"- {humanized}")
        lines.append(f"- {details}")
        lines.append(f"- [Open full report →]({payload.get('route', ROUTE)})")
        return "\n".join(lines) + "\n"

    yesterday = payload.get("yesterday") or {}
    trend = payload.get("trend_7d") or {}
    anomaly = payload.get("anomaly") or _DEFAULT_ANOMALY
    email_status = payload.get("email_status") or {}
    channels = yesterday.get("channels") or {}

    if anomaly.get("triggered"):
        lines[0] = "## Profit Intel  ⚠️ ANOMALY"

    if payload.get("banner"):
        lines.append(f"- {payload['banner']}")

    lines.append(
        "- Yesterday ({date}): {orders} orders | {rev} rev | {net} net | {margin} margin".format(
            date=yesterday.get("date") or "—",
            orders=yesterday.get("orders") if yesterday.get("orders") is not None else "—",
            rev=_format_money(yesterday.get("revenue")),
            net=_format_money(yesterday.get("net_profit")),
            margin=_format_pct(yesterday.get("margin")),
        )
    )
    if trend.get("orders_delta_pct") is not None or trend.get("net_delta_pct") is not None:
        lines.append(
            "- 7-day trend: orders {o} | net {n} | margin {m}".format(
                o=_format_delta(trend.get("orders_delta_pct")),
                n=_format_delta(trend.get("net_delta_pct")),
                m=_format_delta(trend.get("margin_delta_pct")),
            )
        )
    else:
        lines.append("- 7-day trend: building (need ≥14 days of history)")

    if channels:
        lines.append(
            f"- Channel mix: Shopify {channels.get('shopify', 0)} / Etsy {channels.get('etsy', 0)}"
        )

    if anomaly.get("triggered"):
        reasons = ", ".join(anomaly.get("reasons") or []) or "unspecified"
        lines.append(f"- Trigger: {reasons}")
    elif anomaly.get("sanity_blocked"):
        lines.append("- Sanity floor tripped — metrics impossible; see Scheduler Health")
    else:
        lines.append("- Anomalies: none")

    today_action = email_status.get("today_action") or "unknown"
    if today_action == "sent":
        status_line = "📨 email sent today"
    elif today_action == "deferred":
        weekly_day = email_status.get("weekly_email_day") or "Monday"
        status_line = f"⏭️ deferred — next email {weekly_day} ({email_status.get('next_email_at') or '—'})"
    elif today_action == "anomaly_bypass":
        status_line = "⚡ anomaly bypass — email sent immediately"
    elif today_action == "errored":
        status_line = "❌ blocked — see Scheduler Health"
    elif today_action == "awaiting_schedule":
        status_line = "⏰ today's run scheduled for 23:58"
    elif today_action == "pending":
        status_line = "⌛ today's run hasn't fired yet"
    elif today_action == "blocked":
        status_line = "❌ blocked — see Scheduler Health"
    else:
        status_line = "❓ no run today"
    lines.append(f"- Status: {status_line}")

    age_label = _format_age(payload.get("data_as_of"))
    lines.append(f"- Data as of: {payload.get('data_as_of') or '—'} ({age_label})")

    send_now = f"{ROUTE}/send-now"
    lines.append(f"- [Open full report →]({payload.get('route', ROUTE)}) | [Send today's report now →]({send_now})")
    return "\n".join(lines) + "\n"


def write_profit_intel(*, lookback_days: int = 30) -> dict[str, Any]:
    payload = build_profit_intel(lookback_days=lookback_days)
    _write_json(PROFIT_INTEL_STATE_PATH, payload)
    _write_markdown(PROFIT_INTEL_MD_PATH, render_profit_intel_markdown(payload))
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Profit Intel surface")
    parser.add_argument("--lookback-days", type=int, default=30)
    args = parser.parse_args()
    payload = write_profit_intel(lookback_days=args.lookback_days)
    print(json.dumps({"available": payload.get("available"), "today_action": (payload.get("email_status") or {}).get("today_action")}, indent=2))


if __name__ == "__main__":
    main()
