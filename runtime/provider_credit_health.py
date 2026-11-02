#!/usr/bin/env python3
"""Grade AI-provider credit health and email the operator daily while it is bad.

Origin, 2026-11-02: Gemini's prepaid credits ran out. Every Gemini-backed
surface fell through to a path that still produced *presentable* output -- Meme
Monday posted a studio-template render instead of the AI one -- and all 57 OS
cards stayed green. The operator found out by asking a question that had nothing
to do with it. ([[feedback_plausible_fallbacks_mask_failure]])

Two facts the operator asked to be told, daily:

    "we are out of $"              -- a provider refused a call for billing
    "we are using the fallback"    -- a surface ran its degraded path

**Out of credits is a STATE, not an event.** Grading a 24-hour window alone gets
this wrong in the most expensive direction: Gemini is mostly exercised by the
Monday meme, so by Wednesday a window-only grader sees no 402s, reports green,
and stops emailing -- while the account is still empty and every render all week
is quietly degraded. So the verdict per provider is the latest DECISIVE event in
the whole ledger: a `credits_exhausted` failure, or an `ok` heartbeat. Exhausted
holds until a call actually succeeds, which is also what makes a top-up clear
the card by itself with nothing for the operator to acknowledge.

The 24-hour window is still read, but only to describe recent activity.

Absence is never health. A ledger with nothing in it grades `unknown`, not
green: it means the provider was not exercised, or the instrumentation broke,
and both are different from "working"
([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"

PROVIDER_HEALTH_LOG_PATH = DUCK_OPS_ROOT / "state" / "provider_health_log.jsonl"
STATE_PATH = DUCK_OPS_ROOT / "state" / "provider_credit_health.json"

#: Architectural convention #4, layer 2.
_FROZEN_PRODUCTION_STATE_PATH = STATE_PATH.resolve()
_TEST_MODE_ENV = "DUCK_TEST_MODE"

DEFAULT_WINDOW_HOURS = 24

CREDITS_EXHAUSTED = "credits_exhausted"

#: Kinds that mean "the provider refused us", each needing a different action.
_ACTIONS = {
    CREDITS_EXHAUSTED: "Top up the prepaid balance, then rerun the degraded surface.",
    "auth_blocked": "Check the provider API key and account access.",
    "rate_limited": "Wait for the rate-limit window; no payment needed.",
    "upstream_unavailable": "Provider-side outage; retry later.",
    "request_failed": "Inspect the recorded detail before retrying.",
}

_TOPUP_URL = {
    "gemini": "https://aistudio.google.com/app/apikey",
    "openai": "https://platform.openai.com/settings/organization/billing",
    "photoroom": "https://app.photoroom.com/api-dashboard",
}


class TestModeRefusalError(RuntimeError):
    """Raised when the producer would overwrite the real state file under pytest."""


def _parse_at(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.astimezone()


def read_ledger(path: Path | None = None) -> list[dict[str, Any]]:
    """Read the append-only ledger, skipping anything unreadable.

    A malformed line must not blind the alarm, so bad lines are counted and
    reported rather than raised ([[feedback_swallowed_errors_lie]] -- the count
    surfaces in the payload's data_quality block).
    """
    target = Path(path) if path is not None else PROVIDER_HEALTH_LOG_PATH
    records: list[dict[str, Any]] = []
    malformed = 0
    try:
        with target.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    malformed += 1
                    continue
                if isinstance(entry, dict):
                    records.append(entry)
    except FileNotFoundError:
        return []
    except Exception:
        return []
    if malformed:
        records.append({"__malformed_count__": malformed})
    return records


def grade(
    records: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> dict[str, Any]:
    """Turn the ledger into a verdict. Pure -- no I/O, no clock of its own."""
    moment = now or datetime.now().astimezone()
    cutoff = moment - timedelta(hours=window_hours)

    malformed = sum(int(r.get("__malformed_count__") or 0) for r in records)
    entries = [r for r in records if "__malformed_count__" not in r]

    providers: dict[str, dict[str, Any]] = {}
    fallbacks_in_window: list[dict[str, Any]] = []

    for entry in entries:
        at = _parse_at(entry.get("at"))
        event = str(entry.get("event") or "")
        provider = str(entry.get("provider") or "").strip() or "unknown"

        if event == "fallback":
            if at is not None and at >= cutoff:
                fallbacks_in_window.append(
                    {
                        "at": entry.get("at"),
                        "surface": entry.get("surface"),
                        "reason": entry.get("reason"),
                        "provider": entry.get("provider"),
                    }
                )
            continue

        if event not in {"failure", "ok"}:
            continue

        slot = providers.setdefault(
            provider,
            {
                "provider": provider,
                "state": "unknown",
                "since": None,
                "last_ok_at": None,
                "last_failure_at": None,
                "last_failure_kind": None,
                "failures_in_window": 0,
                "last_detail": "",
            },
        )

        if event == "ok":
            slot["last_ok_at"] = entry.get("at")
        else:
            kind = str(entry.get("kind") or "request_failed")
            slot["last_failure_at"] = entry.get("at")
            slot["last_failure_kind"] = kind
            slot["last_detail"] = str(entry.get("detail") or "")[:300]
            if at is not None and at >= cutoff:
                slot["failures_in_window"] = int(slot["failures_in_window"]) + 1

    # The decisive question per provider: which came last, the refusal or a
    # success? Ordering is by the ledger's own timestamps, not file order,
    # because two repos append to it concurrently.
    for slot in providers.values():
        last_ok = _parse_at(slot.get("last_ok_at"))
        last_fail = _parse_at(slot.get("last_failure_at"))
        if last_fail is not None and (last_ok is None or last_fail > last_ok):
            slot["state"] = (
                "credits_exhausted"
                if slot.get("last_failure_kind") == CREDITS_EXHAUSTED
                else "failing"
            )
            slot["since"] = slot.get("last_failure_at")
        elif last_ok is not None:
            slot["state"] = "ok"
            slot["since"] = slot.get("last_ok_at")

    broke = sorted(
        [p for p in providers.values() if p["state"] == "credits_exhausted"],
        key=lambda p: str(p.get("since") or ""),
    )
    failing = [p for p in providers.values() if p["state"] == "failing"]

    if broke:
        status = "red"
        names = ", ".join(p["provider"] for p in broke)
        reason = (
            f"{names} refused calls for billing and has not succeeded since "
            f"{broke[0].get('since')}. Paid generation is running on the fallback path."
        )
    elif failing or fallbacks_in_window:
        status = "yellow"
        if failing:
            reason = (
                f"{', '.join(p['provider'] for p in failing)} is failing "
                f"({failing[0].get('last_failure_kind')}), not a billing problem."
            )
        else:
            surfaces = sorted({str(f.get("surface") or "?") for f in fallbacks_in_window})
            reason = (
                f"{len(fallbacks_in_window)} fallback run(s) in the last {window_hours}h "
                f"({', '.join(surfaces)}) with no provider refusal recorded."
            )
    elif any(p["state"] == "ok" for p in providers.values()):
        status = "green"
        reason = (
            f"{', '.join(sorted(p['provider'] for p in providers.values() if p['state'] == 'ok'))} "
            f"succeeded most recently; no fallback runs in the last {window_hours}h."
        )
    else:
        # Deliberately not green. See the module docstring.
        status = "yellow"
        reason = (
            "No provider calls recorded yet, so there is no evidence either way. "
            "Either nothing has exercised an AI provider, or the recording is not wired up."
        )

    return {
        "schema_version": "duck_ops.provider_credit_health.v1",
        "generated_at": moment.isoformat(),
        "window_hours": window_hours,
        "status": status,
        "status_reason": reason,
        "out_of_credits": [p["provider"] for p in broke],
        "providers": sorted(providers.values(), key=lambda p: p["provider"]),
        "fallback_runs_in_window": fallbacks_in_window,
        "fallback_count_in_window": len(fallbacks_in_window),
        "recommended_action": (
            _ACTIONS[CREDITS_EXHAUSTED] if broke
            else _ACTIONS.get(str(failing[0].get("last_failure_kind")), "") if failing
            else ""
        ),
        "topup_urls": {p["provider"]: _TOPUP_URL.get(p["provider"], "") for p in broke},
        "data_quality": {"malformed_ledger_lines": malformed},
        "ledger_path": str(PROVIDER_HEALTH_LOG_PATH),
    }


def should_email(payload: dict[str, Any], *, last_sent_date: str | None, today: str) -> bool:
    """Email once a day while the operator's two conditions hold.

    Not on `unknown` (the no-evidence yellow): a daily email that fires because
    nothing ran would train the operator to ignore the one that matters
    ([[feedback_red_ci_is_no_ci]]).
    """
    if last_sent_date == today:
        return False
    if payload.get("status") == "red":
        return True
    return bool(payload.get("fallback_count_in_window"))


def render_email(payload: dict[str, Any]) -> tuple[str, str, str]:
    broke = payload.get("out_of_credits") or []
    fallbacks = payload.get("fallback_runs_in_window") or []
    if broke:
        subject = f"MJD: [provider_credit_health] {', '.join(broke)} is out of credits — AI path is on fallback"
    else:
        subject = f"MJD: [provider_credit_health] {len(fallbacks)} fallback run(s) in the last {payload.get('window_hours')}h"

    lines = [subject.split("] ", 1)[-1], ""]
    for provider in payload.get("providers") or []:
        lines.append(
            f"- {provider['provider']}: {provider['state']}"
            f" (since {provider.get('since') or 'unknown'};"
            f" last success {provider.get('last_ok_at') or 'never recorded'})"
        )
    if fallbacks:
        lines += ["", f"Fallback runs in the last {payload.get('window_hours')}h:"]
        for run in fallbacks[:20]:
            lines.append(f"- {run.get('surface')}: {run.get('reason')} ({run.get('at')})")
    if payload.get("recommended_action"):
        lines += ["", f"Action: {payload['recommended_action']}"]
    for provider, url in (payload.get("topup_urls") or {}).items():
        if url:
            lines.append(f"Top up {provider}: {url}")
    text = "\n".join(lines)

    rows = "".join(
        f"<tr><td style='padding:4px 10px 4px 0'><b>{p['provider']}</b></td>"
        f"<td style='padding:4px 10px 4px 0'>{p['state']}</td>"
        f"<td style='padding:4px 0'>last success: {p.get('last_ok_at') or 'never recorded'}</td></tr>"
        for p in (payload.get("providers") or [])
    )
    fallback_html = "".join(
        f"<li>{run.get('surface')} — {run.get('reason')} <small>({run.get('at')})</small></li>"
        for run in fallbacks[:20]
    )
    topup_html = "".join(
        f"<p><a href='{url}'>Top up {provider}</a></p>"
        for provider, url in (payload.get("topup_urls") or {}).items()
        if url
    )
    html = (
        f"<div style=\"font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif\">"
        f"<h2 style='margin:0 0 6px'>AI provider credit health</h2>"
        f"<p style='margin:0 0 12px'>{payload.get('status_reason')}</p>"
        f"<table style='border-collapse:collapse;font-size:14px'>{rows}</table>"
        + (f"<h3>Fallback runs</h3><ul>{fallback_html}</ul>" if fallback_html else "")
        + (f"<p><b>Action:</b> {payload['recommended_action']}</p>" if payload.get("recommended_action") else "")
        + topup_html
        + "</div>"
    )
    return subject, text, html


def _send(payload: dict[str, Any]) -> bool:
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
    except Exception:
        pass
    from helpers.email_helper import send_email  # type: ignore

    subject, text, html = render_email(payload)
    send_email(subject, html, text)
    return True


def write_state(payload: dict[str, Any], path: Path | None = None) -> None:
    target = Path(path) if path is not None else STATE_PATH
    if os.getenv(_TEST_MODE_ENV) == "1" and target.resolve() == _FROZEN_PRODUCTION_STATE_PATH:
        raise TestModeRefusalError(
            f"Refusing to write the production provider_credit_health state ({target}) "
            f"while {_TEST_MODE_ENV}=1."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, target)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--window-hours", type=int, default=DEFAULT_WINDOW_HOURS)
    parser.add_argument("--print-json", action="store_true")
    parser.add_argument("--no-email", action="store_true", help="Grade and write state without sending.")
    args = parser.parse_args()

    payload = grade(read_ledger(), window_hours=args.window_hours)

    previous: dict[str, Any] = {}
    try:
        previous = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        previous = {}
    last_sent = str(previous.get("last_email_sent_date") or "") or None
    today = datetime.now().astimezone().date().isoformat()

    emailed = False
    if not args.no_email and should_email(payload, last_sent_date=last_sent, today=today):
        try:
            _send(payload)
            emailed = True
        except Exception as exc:  # noqa: BLE001
            # Loud, and recorded in the payload: a silent email failure would
            # leave the operator believing no news is good news.
            payload["email_error"] = str(exc)
            print(f"[provider-credit-health] EMAIL FAILED: {exc}", file=sys.stderr)

    payload["last_email_sent_date"] = today if emailed else last_sent
    payload["emailed_this_run"] = emailed
    write_state(payload)

    if args.print_json:
        print(json.dumps(payload, indent=2))
    else:
        print(
            f"[provider-credit-health] status={payload['status']} "
            f"out_of_credits={payload['out_of_credits'] or '-'} "
            f"fallbacks_{args.window_hours}h={payload['fallback_count_in_window']} "
            f"emailed={'YES' if emailed else 'no'}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
