"""SEO outcome loop (Surface 63): per-page GSC before/after for Shopify SEO applies.

Joins shopify_seo_writeback receipts (the proof an edit was applied, anchored at
`verified_at`) against Google Search Console per-page metrics in 28-day windows,
producing deterministic verdicts: did the edit move impressions/position?

Scope: GSC covers only the URL-prefix property https://www.myjeepduck.com/ —
this measures the SHOPIFY SEO lane only. Etsy listings are invisible to GSC
(Etsy traffic lives in GA4) and are explicitly out of scope.

Honesty rules (low-traffic site, ~8 Google clicks/day):
- impressions % and position drive verdicts; clicks are reported, never judged
- an incomplete after-window is `pending` with days_remaining, never "no effect"
- thin volume is `low_data`, not a fake verdict
- with no GSC token the FULL roster still ships as `unmeasured` with live
  window countdowns, so the portal page is useful before re-auth

Fills the `traffic_signal: available=false` stub in shopify_seo_outcomes.py
(Phase 2 reads this intel by resource_id).
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from gsc_search_demand import (
    fetch_gsc_access_token,
    gsc_config,
    query_search_analytics,
)
from workflow_control import TestModeRefusalError

DUCK_OPS_ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = DUCK_OPS_ROOT / "state"
SEO_OUTCOME_INTEL_PATH = STATE_DIR / "seo_outcome_intel.json"
_FROZEN_PRODUCTION_SEO_OUTCOME_INTEL_PATH = SEO_OUTCOME_INTEL_PATH.resolve()
SEO_WRITEBACK_RECEIPT_DIR = STATE_DIR / "shopify_seo_writeback" / "receipts"

WINDOW_DAYS = 28
SETTLE_GAP_DAYS = 3
GSC_LAG_DAYS = 3
MIN_IMPRESSIONS = 20          # below this across both windows -> low_data
NEW_VISIBILITY_MIN = 10       # 0 before -> >=10 after counts as improved (new visibility)
IMPR_PCT_THRESHOLD = 0.25
POSITION_ABS_THRESHOLD = 3.0
CONFIDENCE_MIN_IMPRESSIONS = 100
MAX_COHORT_AGE_DAYS = 400     # GSC retains ~16 months

VERDICT_RULES = {
    "impressions_pct_threshold": IMPR_PCT_THRESHOLD,
    "position_abs_threshold": POSITION_ABS_THRESHOLD,
    "min_impressions": MIN_IMPRESSIONS,
    "new_visibility_min": NEW_VISIBILITY_MIN,
    "note": "clicks are reported but too sparse (~8/day sitewide) to drive verdicts",
}

SCOPE_NOTE = "Shopify domain only; Etsy is invisible to GSC (GA4 covers Etsy)."


def _parse_dt(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _load_receipts(receipt_dir: Path | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Latest receipt per (resource_kind, resource_id) = the measurement anchor.
    Returns (tracked, unjoinable, superseded_count). A re-edit resets the clock;
    earlier receipts are superseded. Receipts without a resource_url or a
    parseable verified_at are unjoinable — loud, not swallowed."""
    root = Path(receipt_dir or SEO_WRITEBACK_RECEIPT_DIR)
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    unjoinable: list[dict[str, Any]] = []
    superseded = 0
    if not root.exists():
        return [], [], 0
    for path in sorted(root.glob("*.json")):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(receipt, dict):
            continue
        rid = str(receipt.get("resource_id") or "").strip()
        kind = str(receipt.get("resource_kind") or "").strip()
        url = str(receipt.get("resource_url") or "").strip()
        anchor = _parse_dt(receipt.get("verified_at"))
        if not rid or not url or anchor is None:
            unjoinable.append({
                "receipt_id": receipt.get("receipt_id") or path.name,
                "reason": "missing resource_url" if not url else (
                    "missing resource_id" if not rid else "unparseable verified_at"),
            })
            continue
        key = (kind, rid)
        prior = latest.get(key)
        if prior is None or anchor > prior["_anchor"]:
            if prior is not None:
                superseded += 1
            latest[key] = {**receipt, "_anchor": anchor, "_receipt_count": (prior or {}).get("_receipt_count", 0) + 1}
        else:
            superseded += 1
            latest[key]["_receipt_count"] = latest[key].get("_receipt_count", 1) + 1
    return list(latest.values()), unjoinable, superseded


def _windows(anchor: date, today: date) -> dict[str, Any]:
    before_start = anchor - timedelta(days=WINDOW_DAYS)
    before_end = anchor - timedelta(days=1)
    after_start = anchor + timedelta(days=SETTLE_GAP_DAYS)
    after_end = after_start + timedelta(days=WINDOW_DAYS - 1)
    complete = today >= after_end + timedelta(days=GSC_LAG_DAYS)
    started = today >= after_start
    days_elapsed = max(0, min((today - after_start).days + 1, WINDOW_DAYS)) if started else 0
    days_remaining = max(0, (after_end + timedelta(days=GSC_LAG_DAYS) - today).days) if not complete else 0
    return {
        "before": (before_start.isoformat(), before_end.isoformat()),
        "after": (after_start.isoformat(), after_end.isoformat()),
        "complete": complete,
        "started": started,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
    }


def _verdict(before: dict[str, Any] | None, after: dict[str, Any] | None, *,
             complete: bool, measured: bool) -> tuple[str, str, str]:
    """Returns (verdict, reason, confidence)."""
    if not measured:
        return "unmeasured", "GSC unavailable this run — roster tracked, graded after re-auth", "low"
    if not complete:
        return "pending", "after window has day(s) left before it can be judged", "low"
    b_impr = int((before or {}).get("impressions") or 0)
    a_impr = int((after or {}).get("impressions") or 0)
    peak = max(b_impr, a_impr)
    confidence = "medium" if peak >= CONFIDENCE_MIN_IMPRESSIONS else "low"
    if b_impr == 0 and a_impr == 0:
        return "no_data", "no impressions in either window (possibly a renamed handle)", confidence
    if b_impr == 0 and a_impr >= NEW_VISIBILITY_MIN:
        return "improved", f"new visibility: 0 -> {a_impr} impressions", confidence
    if peak < MIN_IMPRESSIONS:
        return "low_data", f"too little traffic to judge (peak {peak} impressions over {WINDOW_DAYS}d)", confidence
    impr_pct = (a_impr - b_impr) / b_impr if b_impr else 0.0
    b_pos = float((before or {}).get("position") or 0.0)
    a_pos = float((after or {}).get("position") or 0.0)
    pos_delta = (a_pos - b_pos) if (b_pos and a_pos) else 0.0
    if impr_pct >= IMPR_PCT_THRESHOLD or (pos_delta <= -POSITION_ABS_THRESHOLD and b_pos):
        return "improved", f"impressions {impr_pct:+.0%}, position {pos_delta:+.1f}", confidence
    if impr_pct <= -IMPR_PCT_THRESHOLD or (pos_delta >= POSITION_ABS_THRESHOLD and b_pos):
        return "declined", f"impressions {impr_pct:+.0%}, position {pos_delta:+.1f}", confidence
    return "flat", f"impressions {impr_pct:+.0%}, position {pos_delta:+.1f}", confidence


def _window_metrics(rows_by_page: dict[str, dict[str, Any]], page_url: str) -> dict[str, Any]:
    row = rows_by_page.get(page_url) or {}
    return {
        "clicks": int(row.get("clicks") or 0),
        "impressions": int(row.get("impressions") or 0),
        "ctr": round(float(row.get("ctr") or 0.0), 4),
        "position": round(float(row.get("position") or 0.0), 1),
    }


def collect(config: dict[str, Any] | None = None, *, today: str | None = None,
            receipts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Build the full outcome payload. Deterministic roster first; GSC second —
    so a dead token still ships the tracked roster as `unmeasured`."""
    config = config or gsc_config()
    today_d = date.fromisoformat(today) if today else date.today()
    site_url = str(config.get("site_url") or "").rstrip("/")

    if receipts is None:
        tracked, unjoinable, superseded = _load_receipts()
    else:
        cooked: list[dict[str, Any]] = []
        for r in receipts:
            anchor = _parse_dt(r.get("verified_at"))
            if anchor is not None:
                cooked.append({**r, "_anchor": anchor, "_receipt_count": r.get("_receipt_count", 1)})
        tracked, unjoinable, superseded = cooked, [], 0

    # Cohorts by anchor date; skip beyond-GSC-retention cohorts loudly.
    cohorts: dict[date, list[dict[str, Any]]] = {}
    for receipt in tracked:
        anchor_date = receipt["_anchor"].date()
        if (today_d - anchor_date).days > MAX_COHORT_AGE_DAYS:
            unjoinable.append({"receipt_id": receipt.get("receipt_id"),
                               "reason": f"anchor older than {MAX_COHORT_AGE_DAYS}d (beyond GSC retention)"})
            continue
        cohorts.setdefault(anchor_date, []).append(receipt)

    # GSC: 1 token call + <=2 page-dimension calls per cohort.
    error: str | None = None
    token: str | None = None
    api_calls = 0
    if config.get("credentials_ready"):
        try:
            token, _meta = fetch_gsc_access_token(config)
            api_calls += 1
            if not token:
                error = str((_meta or {}).get("error") or "token_unavailable")
        except Exception as exc:
            error = f"token_error: {exc}"
    else:
        error = "credentials_missing"

    cohort_rows: dict[date, dict[str, dict[str, dict[str, Any]]]] = {}
    if token:
        for anchor_date in sorted(cohorts):
            win = _windows(anchor_date, today_d)
            windows_needed = [("before", win["before"])]
            if win["started"]:
                windows_needed.append(("after", win["after"]))
            per_window: dict[str, dict[str, dict[str, Any]]] = {}
            for label, (start, end) in windows_needed:
                # query_search_analytics returns (rows, meta) and never raises on
                # HTTP errors — failure lives in meta. Each row's dimension key
                # (the page URL, for dimensions=("page",)) is under "query".
                rows, meta = query_search_analytics(
                    token, config["site_url"], start, end,
                    dimensions=("page",), row_limit=1000,
                )
                api_calls += 1
                if not (meta or {}).get("ok"):
                    per_window[label] = {}
                    per_window[f"_{label}_error"] = str((meta or {}).get("error") or "query_failed")  # type: ignore[assignment]
                    continue
                per_window[label] = {str(r.get("query") or ""): r for r in rows or []}
            cohort_rows[anchor_date] = per_window

    pages: list[dict[str, Any]] = []
    summary = {k: 0 for k in ("tracked", "improved", "flat", "declined", "pending",
                              "low_data", "no_data", "unmeasured")}
    summary["superseded"] = superseded
    summary["unjoinable"] = len(unjoinable)

    for anchor_date in sorted(cohorts, reverse=True):
        win = _windows(anchor_date, today_d)
        per_window = cohort_rows.get(anchor_date) or {}
        cohort_measured = bool(token) and "_before_error" not in per_window
        for receipt in cohorts[anchor_date]:
            page_url = site_url + str(receipt.get("resource_url") or "")
            before = _window_metrics(per_window.get("before") or {}, page_url) if cohort_measured else None
            after = (_window_metrics(per_window.get("after") or {}, page_url)
                     if cohort_measured and win["started"] else None)
            verdict, reason, confidence = _verdict(
                before, after, complete=win["complete"], measured=cohort_measured)
            if verdict == "pending":
                reason = f"after window has {win['days_remaining']} day(s) left before it can be judged"
            delta = None
            if before is not None and after is not None:
                b_impr = before["impressions"]
                delta = {
                    "impressions_abs": after["impressions"] - b_impr,
                    "impressions_pct": round((after["impressions"] - b_impr) / b_impr, 3) if b_impr else None,
                    "clicks_abs": after["clicks"] - before["clicks"],
                    "position_abs": round(after["position"] - before["position"], 1)
                    if (before["position"] and after["position"]) else None,
                }
            pages.append({
                "receipt_id": receipt.get("receipt_id"),
                "resource_id": receipt.get("resource_id"),
                "resource_kind": receipt.get("resource_kind"),
                "resource_url": receipt.get("resource_url"),
                "page_url": page_url,
                "title": receipt.get("title"),
                "lane": receipt.get("lane"),
                "applied_at": str(receipt.get("verified_at")),
                "applied_fields": receipt.get("applied_fields") or [],
                "cohort_date": anchor_date.isoformat(),
                "receipt_count": receipt.get("_receipt_count", 1),
                "before": None if before is None else {
                    "start": win["before"][0], "end": win["before"][1], **before},
                "after": None if after is None else {
                    "start": win["after"][0], "end": win["after"][1],
                    "complete": win["complete"], "days_elapsed": win["days_elapsed"],
                    "days_remaining": win["days_remaining"], **after},
                "delta": delta,
                "verdict": verdict,
                "verdict_reason": reason,
                "confidence": confidence,
            })
            summary["tracked"] += 1
            summary[verdict] = summary.get(verdict, 0) + 1

    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "available": bool(token),
        "error": error,
        "source": "google_search_console",
        "site_url": config.get("site_url"),
        "scope_note": SCOPE_NOTE,
        "window_days": WINDOW_DAYS,
        "settle_gap_days": SETTLE_GAP_DAYS,
        "gsc_lag_days": GSC_LAG_DAYS,
        "verdict_rules": VERDICT_RULES,
        "summary": summary,
        "pages": pages,
        "unjoinable": unjoinable,
        "api_calls": api_calls,
        "cohort_count": len(cohorts),
    }


def write_seo_outcome_intel(payload: dict[str, Any], path: Any = None) -> Path:
    out = Path(path or SEO_OUTCOME_INTEL_PATH)
    if os.environ.get("DUCK_TEST_MODE") == "1" and \
            out.resolve() == _FROZEN_PRODUCTION_SEO_OUTCOME_INTEL_PATH:
        raise TestModeRefusalError(
            "DUCK_TEST_MODE=1 but SEO_OUTCOME_INTEL_PATH still points at the "
            "production state file. Monkeypatch seo_outcome_intel.SEO_OUTCOME_INTEL_PATH "
            "to a tmp path.")
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


def main() -> int:
    # Credentials live in duckAgent/.env (env-in-dotenv convention); load it so
    # a launchd-run job sees GSC_REFRESH_TOKEN / GSC_SITE_URL.
    if not os.environ.get("GSC_REFRESH_TOKEN"):
        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=str(DUCK_OPS_ROOT.parent / "duckAgent" / ".env"), override=False)
        except Exception:
            pass
    dry_run = "--dry-run" in sys.argv
    payload = collect()
    s = payload["summary"]
    line = (f"[seo_outcome_intel] tracked={s['tracked']} improved={s['improved']} "
            f"declined={s['declined']} pending={s['pending']} unmeasured={s['unmeasured']} "
            f"available={payload['available']} api_calls={payload['api_calls']}")
    if dry_run:
        print(line)
        print(json.dumps(payload["summary"], indent=2))
        return 0
    out = write_seo_outcome_intel(payload)
    print(f"{line} -> {out}")
    if not payload["available"]:
        print(f"  [note] GSC unavailable ({payload['error']}) — roster written as unmeasured; "
              "verdicts appear after GSC_REFRESH_TOKEN re-auth.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
