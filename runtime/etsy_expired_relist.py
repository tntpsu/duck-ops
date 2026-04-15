#!/usr/bin/env python3
"""
Safely renew eligible expired Etsy listings.

This lane is intentionally conservative:
- it discovers expired listings through the Etsy API
- it only considers listings with at least one recorded Etsy sale
- it enforces a maximum number of successful renewals per local day
- it uses the trusted Etsy seller Playwright session for the actual renew click
- it can run in dry-run mode without clicking anything
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from review_reply_discovery import navigate_within_session, parse_eval_json, run_pw_command
from review_reply_executor import choose_session, ensure_authenticated_session
from etsy_browser_guard import blocked_status as etsy_browser_blocked_status
from workflow_control import record_workflow_transition


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
OUTPUT_DIR = ROOT / "output" / "operator"
DUCK_AGENT_ROOT = ROOT.parent / "duckAgent"
DUCK_AGENT_VENV_PYTHON = DUCK_AGENT_ROOT / ".venv" / "bin" / "python"

RELIST_STATE_PATH = STATE_DIR / "etsy_expired_relist.json"
RELIST_HISTORY_PATH = STATE_DIR / "etsy_expired_relist_history.jsonl"
RELIST_SALES_CACHE_PATH = STATE_DIR / "etsy_listing_sales_counts.json"
RELIST_OPERATOR_JSON_PATH = OUTPUT_DIR / "etsy_expired_relist.json"
RELIST_OPERATOR_MD_PATH = OUTPUT_DIR / "etsy_expired_relist.md"

WORKFLOW_ID = "etsy_expired_relist"
WORKFLOW_LANE = "etsy_expired_relist"
WORKFLOW_LABEL = "Etsy Expired Relist"

DEFAULT_DAILY_LIMIT = 3
DEFAULT_PREVIEW_LIMIT = 3
SALES_CACHE_TTL_HOURS = 24
RECENT_ETSY_TRANSACTIONS_PATH = ROOT / "state" / "normalized" / "etsy_transactions_snapshot.json"
WEEKLY_INSIGHTS_PATH = DUCK_AGENT_ROOT / "cache" / "weekly_insights.json"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def _load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def _run_duckagent_json(script: str, *, timeout_seconds: int = 240) -> dict[str, Any]:
    proc = subprocess.run(
        [str(DUCK_AGENT_VENV_PYTHON), "-c", script],
        cwd=str(DUCK_AGENT_ROOT),
        env={**os.environ, **_load_env_file(DUCK_AGENT_ROOT / ".env")},
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"duckAgent helper exited with code {proc.returncode}")
    stdout = proc.stdout or ""
    raw_lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    raw = raw_lines[-1] if raw_lines else ""
    if not raw:
        raise RuntimeError("duckAgent helper returned no JSON payload")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"duckAgent helper did not end with valid JSON. stdout tail: {stdout[-500:]}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("duckAgent helper returned a non-object payload")
    return payload


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone()
    except ValueError:
        return None


def _format_local_time(value: Any) -> str | None:
    parsed = _parse_iso(value)
    if not parsed:
        return None
    return parsed.strftime("%b %-d, %-I:%M %p")


def _format_local_date_from_ts(value: Any) -> str | None:
    try:
        parsed = datetime.fromtimestamp(int(value)).astimezone()
    except (TypeError, ValueError, OSError):
        return None
    return parsed.strftime("%b %-d, %Y")


def _normalize_title(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("’", "'").split()).strip()


def _age_hours(value: Any) -> float | None:
    parsed = _parse_iso(value)
    if not parsed:
        return None
    return max(0.0, (datetime.now().astimezone() - parsed).total_seconds() / 3600.0)


def _editor_url(listing_id: Any) -> str:
    return f"https://www.etsy.com/your/shops/me/listing-editor/edit/{listing_id}"


def _load_history_entries() -> list[dict[str, Any]]:
    if not RELIST_HISTORY_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in RELIST_HISTORY_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _renewed_today_listing_ids(history_entries: list[dict[str, Any]]) -> set[str]:
    today = datetime.now().astimezone().date().isoformat()
    return {
        str(entry.get("listing_id") or "").strip()
        for entry in history_entries
        if str(entry.get("status") or "").strip() == "renewed"
        and str(entry.get("date_local") or "").strip() == today
        and str(entry.get("listing_id") or "").strip()
    }


def _renewed_today_count(history_entries: list[dict[str, Any]]) -> int:
    return len(_renewed_today_listing_ids(history_entries))


def fetch_expired_listings() -> dict[str, Any]:
    script = textwrap.dedent(
        """
        import json
        import os
        from datetime import datetime, timezone
        from helpers.etsy_api_wrapper import EtsyAPIWrapper

        shop_id = os.getenv("ETSY_SHOP_ID")
        api = EtsyAPIWrapper()

        offset = 0
        limit = 100
        rows = []
        while True:
            response = api.get(
                f"/application/shops/{shop_id}/listings",
                params={"state": "expired", "limit": str(limit), "offset": str(offset)},
            )
            results = (response or {}).get("results") or []
            if not results:
                break
            for item in results:
                rows.append(
                    {
                        "listing_id": item.get("listing_id"),
                        "title": item.get("title"),
                        "state": item.get("state"),
                        "quantity": item.get("quantity"),
                        "url": item.get("url"),
                        "num_favorers": item.get("num_favorers"),
                        "ending_timestamp": item.get("ending_timestamp"),
                        "last_modified_timestamp": item.get("last_modified_timestamp"),
                        "should_auto_renew": item.get("should_auto_renew"),
                        "has_variations": item.get("has_variations"),
                        "is_customizable": item.get("is_customizable"),
                        "price": item.get("price"),
                        "tags": item.get("tags") or [],
                    }
                )
            if len(results) < limit:
                break
            offset += limit

        print(json.dumps({"generated_at": datetime.now(timezone.utc).astimezone().isoformat(), "items": rows}))
        """
    ).strip()
    return _run_duckagent_json(script, timeout_seconds=180)


def refresh_listing_sales_counts(*, force: bool = False) -> dict[str, Any]:
    cached = load_json(RELIST_SALES_CACHE_PATH, {})
    age = _age_hours(cached.get("generated_at")) if isinstance(cached, dict) else None
    if not force and isinstance(cached, dict) and cached.get("counts") and age is not None and age <= SALES_CACHE_TTL_HOURS:
        return cached

    if not force:
        counts: dict[str, int] = {}
        titles: dict[str, str] = {}
        title_counts: dict[str, int] = {}

        recent_transactions = load_json(RECENT_ETSY_TRANSACTIONS_PATH, {})
        for tx in list(recent_transactions.get("items") or []):
            listing_id = str(tx.get("listing_id") or "").strip()
            if not listing_id:
                continue
            try:
                quantity = int(tx.get("quantity") or 1)
            except (TypeError, ValueError):
                quantity = 1
            counts[listing_id] = counts.get(listing_id, 0) + max(quantity, 1)
            title = str(tx.get("title") or "").strip()
            if title:
                titles[listing_id] = title
                normalized_title = _normalize_title(title)
                if normalized_title:
                    title_counts[normalized_title] = max(title_counts.get(normalized_title, 0), counts[listing_id])

        weekly_insights = load_json(WEEKLY_INSIGHTS_PATH, {})
        for key in ("top_performers", "top_performers_30d", "top_performers_7d"):
            for item in list(weekly_insights.get(key) or []):
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
                normalized_title = _normalize_title(title)
                try:
                    lifetime_sales = int(item.get("lifetime_sales") or 0)
                except (TypeError, ValueError):
                    lifetime_sales = 0
                try:
                    recent_sales = int(item.get("recent_sales") or 0)
                except (TypeError, ValueError):
                    recent_sales = 0
                title_counts[normalized_title] = max(title_counts.get(normalized_title, 0), lifetime_sales, recent_sales)

        payload = {
            "generated_at": now_iso(),
            "counts": counts,
            "titles": titles,
            "title_counts": title_counts,
            "source": "local_signals",
        }
        write_json(RELIST_SALES_CACHE_PATH, payload)
        return payload

    script = textwrap.dedent(
        """
        import json
        import os
        import time
        from datetime import datetime, timezone
        from helpers.etsy_helper import etsy_get_shop_transactions

        shop_id = os.getenv("ETSY_SHOP_ID")
        offset = 0
        limit = 100
        counts = {}
        titles = {}
        title_counts = {}
        page_count = 0

        while True:
            response = etsy_get_shop_transactions(shop_id, limit=limit, offset=offset)
            results = (response or {}).get("results") or []
            if not results:
                break
            for tx in results:
                listing_id = str(tx.get("listing_id") or "").strip()
                if not listing_id:
                    continue
                try:
                    quantity = int(tx.get("quantity") or 1)
                except (TypeError, ValueError):
                    quantity = 1
                counts[listing_id] = counts.get(listing_id, 0) + max(quantity, 1)
                title = str(tx.get("title") or "").strip()
                if title and listing_id not in titles:
                    titles[listing_id] = title
                normalized_title = " ".join(title.lower().replace("’", "'").split()).strip()
                if normalized_title:
                    title_counts[normalized_title] = max(title_counts.get(normalized_title, 0), counts[listing_id])
            page_count += 1
            if len(results) < limit:
                break
            offset += limit
            time.sleep(0.05)

        print(
            json.dumps(
                {
                    "generated_at": datetime.now(timezone.utc).astimezone().isoformat(),
                    "counts": counts,
                    "titles": titles,
                    "title_counts": title_counts,
                    "pages": page_count,
                    "listing_count": len(counts),
                    "source": "etsy_transactions_all_time",
                }
            )
        )
        """
    ).strip()
    payload = _run_duckagent_json(script, timeout_seconds=300)
    write_json(RELIST_SALES_CACHE_PATH, payload)
    return payload


def _sales_count_for_listing(listing: dict[str, Any], sales_counts: dict[str, Any]) -> int:
    counts = sales_counts.get("counts") or {}
    title_counts = sales_counts.get("title_counts") or {}
    listing_id = str(listing.get("listing_id") or "").strip()
    try:
        exact_count = int(counts.get(listing_id) or 0)
    except (TypeError, ValueError):
        exact_count = 0
    if exact_count > 0:
        return exact_count
    normalized_title = _normalize_title(listing.get("title"))
    try:
        return int(title_counts.get(normalized_title) or 0)
    except (TypeError, ValueError):
        return 0


def _eligible_sort_key(item: dict[str, Any]) -> tuple[int, int, int, str]:
    return (
        -int(item.get("sales_count") or 0),
        -int(item.get("num_favorers") or 0),
        -int(item.get("ending_timestamp") or 0),
        str(item.get("title") or "").lower(),
    )


def build_relist_queue(
    expired_payload: dict[str, Any],
    sales_counts: dict[str, Any],
    history_entries: list[dict[str, Any]],
    *,
    daily_limit: int,
) -> dict[str, Any]:
    renewed_today_ids = _renewed_today_listing_ids(history_entries)
    renewed_today = len(renewed_today_ids)
    remaining_today = max(0, daily_limit - renewed_today)
    items: list[dict[str, Any]] = []
    for raw in list(expired_payload.get("items") or []):
        listing = dict(raw)
        listing_id = str(listing.get("listing_id") or "").strip()
        sales_count = _sales_count_for_listing(listing, sales_counts)
        renewed_today_flag = listing_id in renewed_today_ids
        eligible = sales_count >= 1 and not renewed_today_flag and remaining_today > 0
        if renewed_today_flag:
            reason = "already_renewed_today"
        elif sales_count < 1:
            reason = "no_recorded_sales"
        elif remaining_today <= 0:
            reason = "daily_limit_reached"
        else:
            reason = "eligible"
        items.append(
            {
                **listing,
                "listing_id": listing_id,
                "sales_count": sales_count,
                "eligible": eligible,
                "eligibility_reason": reason,
                "renewed_today": renewed_today_flag,
                "edit_url": _editor_url(listing_id),
                "expires_on": _format_local_date_from_ts(listing.get("ending_timestamp")),
            }
        )
    eligible_items = sorted((item for item in items if item.get("eligible")), key=_eligible_sort_key)
    skipped_items = sorted((item for item in items if not item.get("eligible")), key=_eligible_sort_key)
    return {
        "generated_at": now_iso(),
        "daily_limit": daily_limit,
        "renewed_today": renewed_today,
        "remaining_today": remaining_today,
        "expired_count": len(items),
        "eligible_count": len(eligible_items),
        "sales_counts_generated_at": sales_counts.get("generated_at"),
        "expired_generated_at": expired_payload.get("generated_at"),
        "eligible_items": eligible_items,
        "skipped_items": skipped_items,
        "items": eligible_items + skipped_items,
    }


def _inspect_editor_state(session_name: str) -> dict[str, Any]:
    output = run_pw_command(
        session_name,
        "eval",
        textwrap.dedent(
            """
            (() => {
              const normalize = value => (value || '').replace(/\\s+/g, ' ').trim();
              const buttons = Array.from(document.querySelectorAll('button'));
              const renewButton = buttons.find(btn => /renew with changes/i.test(normalize(btn.innerText)));
              const statusNode = Array.from(document.querySelectorAll('*')).find(node => normalize(node.innerText) === 'Expired');
              return JSON.stringify({
                currentUrl: window.location.href,
                pageTitle: document.title,
                expiredVisible: Boolean(statusNode),
                renewButtonFound: Boolean(renewButton),
                renewButtonText: renewButton ? normalize(renewButton.innerText) : null,
              });
            })()
            """
        ).strip(),
    )
    payload = parse_eval_json(output)
    return payload if isinstance(payload, dict) else {}


def preview_listing(listing: dict[str, Any]) -> dict[str, Any]:
    session_name, start_url = choose_session()
    ensure_authenticated_session(session_name, start_url)
    landed_url, page_title = navigate_within_session(session_name, listing["edit_url"], wait_seconds=2.0)
    inspection = _inspect_editor_state(session_name)
    return {
        "listing_id": listing.get("listing_id"),
        "title": listing.get("title"),
        "sales_count": listing.get("sales_count"),
        "edit_url": listing.get("edit_url"),
        "landed_url": landed_url,
        "page_title": page_title,
        "expired_visible": bool(inspection.get("expiredVisible")),
        "renew_button_found": bool(inspection.get("renewButtonFound")),
        "renew_button_text": inspection.get("renewButtonText"),
        "previewed_at": now_iso(),
    }


def execute_relist(listing: dict[str, Any]) -> dict[str, Any]:
    preview = preview_listing(listing)
    session_name, _ = choose_session()
    if not preview.get("renew_button_found"):
        return {
            **preview,
            "status": "failed",
            "reason": "renew_button_not_found",
        }
    output = run_pw_command(
        session_name,
        "eval",
        textwrap.dedent(
            """
            (() => {
              const normalize = value => (value || '').replace(/\\s+/g, ' ').trim();
              const renewButton = Array.from(document.querySelectorAll('button')).find(btn => /renew with changes/i.test(normalize(btn.innerText)));
              if (!renewButton) {
                return JSON.stringify({ clicked: false, reason: 'renew_button_not_found' });
              }
              renewButton.scrollIntoView({ block: 'center' });
              renewButton.click();
              return JSON.stringify({ clicked: true, label: normalize(renewButton.innerText) });
            })()
            """
        ).strip(),
    )
    click_result = parse_eval_json(output)
    time.sleep(3)
    inspection = _inspect_editor_state(session_name)
    renewed = bool(click_result.get("clicked")) and not bool(inspection.get("expiredVisible"))
    return {
        **preview,
        "status": "renewed" if renewed else "failed",
        "reason": None if renewed else "post_click_verification_failed",
        "click_result": click_result,
        "post_click": inspection,
        "renewed_at": now_iso() if renewed else None,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Etsy Expired Relist",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Expired listings found: `{payload.get('expired_count')}`",
        f"- Eligible to renew today: `{payload.get('eligible_count')}`",
        f"- Daily renewal cap: `{payload.get('daily_limit')}`",
        f"- Renewed today: `{payload.get('renewed_today')}`",
        f"- Remaining renewal slots today: `{payload.get('remaining_today')}`",
        f"- Sales counts refreshed: `{payload.get('sales_counts_generated_at')}`",
        "",
        "## Eligible Today",
        "",
    ]
    eligible_items = payload.get("eligible_items") or []
    if not eligible_items:
        lines.append("No eligible expired ducks are ready to renew right now.")
    else:
        for item in eligible_items[:10]:
            lines.extend(
                [
                    f"- {item.get('title')}",
                    f"  - Listing ID: `{item.get('listing_id')}`",
                    f"  - Sales: `{item.get('sales_count')}`",
                    f"  - Favorers: `{item.get('num_favorers') or 0}`",
                    f"  - Expired: `{item.get('expires_on') or 'unknown'}`",
                    f"  - Edit URL: `{item.get('edit_url')}`",
                ]
            )
    skipped_items = payload.get("skipped_items") or []
    if skipped_items:
        lines.extend(["", "## Skipped", ""])
        for item in skipped_items[:10]:
            lines.append(
                f"- {item.get('title')} | `{item.get('eligibility_reason')}` | sales `{item.get('sales_count')}`"
            )
    last_run = payload.get("last_run") or {}
    if last_run:
        lines.extend(["", "## Last Run", ""])
        lines.append(f"- Mode: `{last_run.get('mode')}`")
        lines.append(f"- At: `{last_run.get('at')}`")
        for result in last_run.get("results") or []:
            lines.append(
                f"- {result.get('title') or result.get('listing_id')} | `{result.get('status') or 'previewed'}`"
            )
    return "\n".join(lines) + "\n"


def write_outputs(payload: dict[str, Any]) -> None:
    write_json(RELIST_STATE_PATH, payload)
    write_json(RELIST_OPERATOR_JSON_PATH, payload)
    RELIST_OPERATOR_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    RELIST_OPERATOR_MD_PATH.write_text(render_markdown(payload), encoding="utf-8")


def sync_control(payload: dict[str, Any], *, results: list[dict[str, Any]] | None = None) -> None:
    if (payload.get("eligible_count") or 0) > 0 and (payload.get("remaining_today") or 0) > 0:
        state = "proposed"
        reason = "eligible_candidates_ready"
        next_action = "Preview or renew up to three eligible expired Etsy listings."
    elif (payload.get("remaining_today") or 0) <= 0:
        state = "blocked"
        reason = "daily_limit_reached"
        next_action = "Wait until tomorrow before renewing any more expired Etsy listings."
    else:
        state = "resolved"
        reason = "no_eligible_expired_listings"
        next_action = "No action needed until more expired listings with proven sales appear."
    if results and any(str(result.get("status") or "") == "renewed" for result in results):
        state = "verified"
        reason = "renewed"
        next_action = "Monitor the renewed listing batch and wait for tomorrow's renewal slots before renewing more."
    record_workflow_transition(
        workflow_id=WORKFLOW_ID,
        lane=WORKFLOW_LANE,
        display_label=WORKFLOW_LABEL,
        entity_id=WORKFLOW_ID,
        run_id=datetime.now().astimezone().date().isoformat(),
        state=state,
        state_reason=reason,
        requires_confirmation=False,
        next_action=next_action,
        metadata={
            "expired_count": payload.get("expired_count"),
            "eligible_count": payload.get("eligible_count"),
            "renewed_today": payload.get("renewed_today"),
        },
        receipt_kind="state_sync",
        receipt_payload={
            "expired_count": payload.get("expired_count"),
            "eligible_count": payload.get("eligible_count"),
            "remaining_today": payload.get("remaining_today"),
            "results": results or [],
        },
        history_summary=reason.replace("_", " "),
    )


def refresh_payload(*, daily_limit: int, force_sales_refresh: bool = False) -> dict[str, Any]:
    expired_payload = fetch_expired_listings()
    sales_counts = refresh_listing_sales_counts(force=force_sales_refresh)
    history_entries = _load_history_entries()
    payload = build_relist_queue(expired_payload, sales_counts, history_entries, daily_limit=daily_limit)
    write_outputs(payload)
    sync_control(payload)
    return payload


def _find_target_items(payload: dict[str, Any], *, listing_id: str | None, limit: int) -> list[dict[str, Any]]:
    eligible_items = list(payload.get("eligible_items") or [])
    if listing_id:
        target = str(listing_id).strip()
        return [item for item in eligible_items if str(item.get("listing_id") or "").strip() == target][:1]
    return eligible_items[: max(limit, 0)]


def command_preview(args: argparse.Namespace) -> int:
    blocked = etsy_browser_blocked_status()
    if blocked.get("blocked"):
        print(
            json.dumps(
                {
                    "mode": "preview",
                    "status": "blocked",
                    "reason": blocked.get("block_reason"),
                    "blocked_until": blocked.get("blocked_until"),
                    "results": [],
                },
                indent=2,
            )
        )
        return 0
    payload = refresh_payload(daily_limit=args.max_per_day, force_sales_refresh=args.force_sales_refresh)
    targets = _find_target_items(payload, listing_id=args.listing_id, limit=args.limit)
    results = [preview_listing(item) for item in targets]
    payload["last_run"] = {"mode": "preview", "at": now_iso(), "results": results}
    write_outputs(payload)
    sync_control(payload, results=results)
    print(json.dumps({"mode": "preview", "results": results}, indent=2))
    return 0


def command_renew(args: argparse.Namespace) -> int:
    blocked = etsy_browser_blocked_status()
    if blocked.get("blocked"):
        print(
            json.dumps(
                {
                    "mode": "renew" if args.execute else "renew_dry_run",
                    "status": "blocked",
                    "reason": blocked.get("block_reason"),
                    "blocked_until": blocked.get("blocked_until"),
                    "results": [],
                },
                indent=2,
            )
        )
        return 0
    payload = refresh_payload(daily_limit=args.max_per_day, force_sales_refresh=args.force_sales_refresh)
    if (payload.get("remaining_today") or 0) <= 0:
        payload["last_run"] = {"mode": "renew", "at": now_iso(), "results": []}
        write_outputs(payload)
        sync_control(payload, results=[])
        print(json.dumps({"mode": "renew", "results": [], "message": "Daily limit already reached."}, indent=2))
        return 0
    targets = _find_target_items(
        payload,
        listing_id=args.listing_id,
        limit=min(args.limit, int(payload.get("remaining_today") or 0)),
    )
    results: list[dict[str, Any]] = []
    for item in targets:
        if args.execute:
            result = execute_relist(item)
            if str(result.get("status") or "") == "renewed":
                append_jsonl(
                    RELIST_HISTORY_PATH,
                    {
                        "at": now_iso(),
                        "date_local": datetime.now().astimezone().date().isoformat(),
                        "listing_id": item.get("listing_id"),
                        "title": item.get("title"),
                        "sales_count": item.get("sales_count"),
                        "status": "renewed",
                    },
                )
        else:
            result = {
                **preview_listing(item),
                "status": "dry_run_ready",
                "reason": "execute_flag_required",
            }
        results.append(result)
    payload = refresh_payload(daily_limit=args.max_per_day, force_sales_refresh=False)
    payload["last_run"] = {
        "mode": "renew_execute" if args.execute else "renew_dry_run",
        "at": now_iso(),
        "results": results,
    }
    write_outputs(payload)
    sync_control(payload, results=results)
    print(json.dumps({"mode": payload["last_run"]["mode"], "results": results}, indent=2))
    return 0


def command_refresh(args: argparse.Namespace) -> int:
    payload = refresh_payload(daily_limit=args.max_per_day, force_sales_refresh=args.force_sales_refresh)
    print(json.dumps(payload, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preview and renew eligible expired Etsy listings.")
    subparsers = parser.add_subparsers(dest="command")

    refresh_parser = subparsers.add_parser("refresh", help="Refresh expired-listing eligibility and write operator outputs.")
    refresh_parser.add_argument("--max-per-day", type=int, default=DEFAULT_DAILY_LIMIT)
    refresh_parser.add_argument("--force-sales-refresh", action="store_true")
    refresh_parser.set_defaults(func=command_refresh)

    preview_parser = subparsers.add_parser("preview", help="Open and verify the next eligible expired listing without renewing it.")
    preview_parser.add_argument("--listing-id")
    preview_parser.add_argument("--limit", type=int, default=1)
    preview_parser.add_argument("--max-per-day", type=int, default=DEFAULT_DAILY_LIMIT)
    preview_parser.add_argument("--force-sales-refresh", action="store_true")
    preview_parser.set_defaults(func=command_preview)

    renew_parser = subparsers.add_parser("renew", help="Renew up to the allowed number of eligible expired listings.")
    renew_parser.add_argument("--listing-id")
    renew_parser.add_argument("--limit", type=int, default=DEFAULT_PREVIEW_LIMIT)
    renew_parser.add_argument("--max-per-day", type=int, default=DEFAULT_DAILY_LIMIT)
    renew_parser.add_argument("--force-sales-refresh", action="store_true")
    renew_parser.add_argument("--execute", action="store_true", help="Actually click the Etsy renew button.")
    renew_parser.set_defaults(func=command_renew)

    parser.set_defaults(func=command_refresh)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
