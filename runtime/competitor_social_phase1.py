from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, load_json, now_local_iso, write_json, write_markdown


CONFIG_PATH = DUCK_OPS_ROOT / "config" / "competitor_social_sources.json"
STATE_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_phase1.json"
OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "competitor_social_phase1.json"
OUTPUT_MD_PATH = OUTPUT_OPERATOR_DIR / "competitor_social_phase1.md"


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _load_config() -> dict[str, Any]:
    payload = load_json(CONFIG_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _verification_items(seed_accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for account in seed_accounts:
        verification_status = _compact_text(account.get("verification_status"))
        handle = _compact_text(account.get("instagram_handle"))
        if verification_status == "confirmed" and handle:
            continue
        items.append(
            {
                "brand_key": _compact_text(account.get("brand_key")),
                "display_name": _compact_text(account.get("display_name")),
                "current_handle": handle or None,
                "verification_status": verification_status or "needs_review",
                "reason": _compact_text(account.get("reason")),
            }
        )
    return items


def build_competitor_social_phase1_payload() -> dict[str, Any]:
    config = _load_config()
    seed_accounts = [item for item in (config.get("seed_accounts") or []) if isinstance(item, dict)]
    collection_boundary = config.get("collection_boundary") or {}
    snapshot_schema = config.get("snapshot_schema") or {}
    verification_items = _verification_items(seed_accounts)
    confirmed_handles = sum(
        1
        for item in seed_accounts
        if _compact_text(item.get("verification_status")) == "confirmed" and _compact_text(item.get("instagram_handle"))
    )
    payload = {
        "generated_at": now_local_iso(),
        "version": int(config.get("version") or 1),
        "summary": {
            "headline": "Phase 1 competitor social foundation locks the seed account universe, collection boundary, and snapshot contract before collector work starts.",
            "platform_scope": list(config.get("platform_scope") or []),
            "seed_account_count": len(seed_accounts),
            "confirmed_handle_count": confirmed_handles,
            "verification_needed_count": len(verification_items),
            "latest_posts_per_account": int(collection_boundary.get("latest_posts_per_account") or 0),
        },
        "collection_boundary": collection_boundary,
        "snapshot_schema": snapshot_schema,
        "seed_accounts": seed_accounts,
        "open_verification_items": verification_items,
        "recommended_next_step": {
            "title": "Build the bounded competitor Instagram snapshot collector.",
            "notes": [
                "Use only confirmed handles in the first collector pass.",
                "Leave unresolved accounts visible in the phase-one surface instead of guessing handles.",
                "Keep the first collector Instagram-only and public-observe-only.",
            ],
        },
        "paths": {
            "config": str(CONFIG_PATH),
            "state": str(STATE_PATH),
            "operator_markdown": str(OUTPUT_MD_PATH),
        },
    }
    return payload


def render_competitor_social_phase1_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# Competitor Social Phase 1",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Seed accounts: `{summary.get('seed_account_count') or 0}`",
        f"- Confirmed handles: `{summary.get('confirmed_handle_count') or 0}`",
        f"- Handle cleanups remaining: `{summary.get('verification_needed_count') or 0}`",
        f"- Latest posts per account target: `{summary.get('latest_posts_per_account') or 0}`",
        "",
        str(summary.get("headline") or ""),
        "",
        "## Collection Boundary",
        "",
        f"- Mode: `{(payload.get('collection_boundary') or {}).get('mode') or ''}`",
        f"- Login allowed: `{(payload.get('collection_boundary') or {}).get('login_allowed')}`",
        f"- Interaction allowed: `{(payload.get('collection_boundary') or {}).get('interaction_allowed')}`",
        f"- Bounded scroll only: `{(payload.get('collection_boundary') or {}).get('bounded_scroll_only')}`",
        "",
        "## Seed Accounts",
        "",
    ]

    for account in payload.get("seed_accounts") or []:
        handle = _compact_text(account.get("instagram_handle")) or "(needs exact handle)"
        lines.extend(
            [
                f"### {account.get('display_name')}",
                "",
                f"- Handle: `{handle}`",
                f"- Verification: `{account.get('verification_status')}`",
                f"- Confidence: `{account.get('confidence')}`",
                f"- Category: `{account.get('category')}`",
                f"- Why: {account.get('reason')}",
                "",
            ]
        )

    lines.extend(["## Snapshot Schema", ""])
    schema = payload.get("snapshot_schema") or {}
    lines.append("- Required fields:")
    for field in schema.get("required_fields") or []:
        lines.append(f"  - `{field}`")
    lines.append("- Optional fields:")
    for field in schema.get("optional_fields") or []:
        lines.append(f"  - `{field}`")
    lines.append("- Comparison dimensions:")
    for field in schema.get("comparison_dimensions") or []:
        lines.append(f"  - `{field}`")
    lines.append("")

    lines.extend(["## Remaining Account Cleanups", ""])
    verification_items = payload.get("open_verification_items") or []
    if not verification_items:
        lines.append("No account cleanup items remain before the first collector pass.")
        lines.append("")
    else:
        for item in verification_items:
            lines.append(
                f"- `{item.get('display_name')}`: `{item.get('verification_status')}`"
                + (f" | current `{item.get('current_handle')}`" if item.get("current_handle") else "")
            )
        lines.append("")

    next_step = payload.get("recommended_next_step") or {}
    lines.extend(["## Recommended Next Step", ""])
    lines.append(f"- {next_step.get('title')}")
    for item in next_step.get("notes") or []:
        lines.append(f"  - {item}")
    lines.append("")

    return "\n".join(lines)


def build_competitor_social_phase1() -> dict[str, Any]:
    payload = build_competitor_social_phase1_payload()
    write_json(STATE_PATH, payload)
    write_json(OPERATOR_JSON_PATH, payload)
    write_markdown(OUTPUT_MD_PATH, render_competitor_social_phase1_markdown(payload))
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the competitor social phase-1 foundation artifact.")
    parser.parse_args()
    payload = build_competitor_social_phase1()
    print(
        {
            "generated_at": payload.get("generated_at"),
            "seed_account_count": ((payload.get("summary") or {}).get("seed_account_count")),
            "verification_needed_count": ((payload.get("summary") or {}).get("verification_needed_count")),
        }
    )


if __name__ == "__main__":
    main()
