#!/usr/bin/env python3
"""
Monitor active weekly sale items so Duck Ops can learn what is working and
feed that back into future sale and marketing choices.
"""

from __future__ import annotations

import json
import os
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workflow_control import record_workflow_transition

ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = ROOT.parent / "duckAgent"
ACTIVE_SALES_PATH = DUCK_AGENT_ROOT / "cache" / "active_sales.json"
SALES_CACHE_PATH = DUCK_AGENT_ROOT / "cache" / "sales_cache.json"
WEEKLY_INSIGHTS_PATH = DUCK_AGENT_ROOT / "cache" / "weekly_insights.json"
DUCK_AGENT_ENV_PATH = DUCK_AGENT_ROOT / ".env"
SALE_COLLECTION_TITLE = "On Sale Duck Collection – 3D-Printed Ducks at Discounted Prices"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _now_local() -> datetime:
    return datetime.now(timezone.utc).astimezone()


def _normalize_title(value: str | None) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_percent(value: Any) -> float:
    text = str(value or "").strip().replace("%", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _hours_since(value: str | None, now_local: datetime) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round(max(0.0, (now_local - parsed.astimezone()).total_seconds() / 3600.0), 1)


def _load_env_defaults(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _shopify_rest_get(endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    _load_env_defaults(DUCK_AGENT_ENV_PATH)
    domain = os.getenv("SHOPIFY_DOMAIN")
    token = os.getenv("SHOPIFY_TOKEN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2025-01")
    if not domain or not token:
        raise RuntimeError("Shopify credentials are not available for live sale refresh.")
    query = f"?{urlencode(params or {})}" if params else ""
    url = f"https://{domain}/admin/api/{api_version}/{endpoint}{query}"
    request = Request(url, headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"})
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _discount_from_variants(variants: list[dict[str, Any]]) -> str:
    best_discount = 0
    for variant in variants:
        try:
            price = float(variant.get("price") or 0)
            compare_at_price = float(variant.get("compare_at_price") or 0)
        except (TypeError, ValueError):
            continue
        if compare_at_price > price > 0:
            discount = round(((compare_at_price - price) / compare_at_price) * 100)
            best_discount = max(best_discount, int(discount))
    return f"{best_discount}%" if best_discount > 0 else ""


def _fetch_live_active_sales_payload(now_local: datetime) -> dict[str, Any] | None:
    collections_payload = _shopify_rest_get("custom_collections.json", {"limit": 250})
    collection_id = None
    for collection in list(collections_payload.get("custom_collections") or []):
        if str(collection.get("title") or "").strip() == SALE_COLLECTION_TITLE:
            collection_id = str(collection.get("id") or "").strip()
            break
    if not collection_id:
        return None

    products_payload = _shopify_rest_get(
        "products.json",
        {
            "collection_id": collection_id,
            "fields": "id,title,variants",
            "limit": 250,
        },
    )
    items: list[dict[str, Any]] = []
    for product in list(products_payload.get("products") or []):
        variants = list(product.get("variants") or [])
        discount = _discount_from_variants(variants)
        if not discount:
            continue
        items.append(
            {
                "id": str(product.get("id") or "").strip(),
                "title": str(product.get("title") or "").strip(),
                "discount": discount,
            }
        )
    return {
        "shopify": items,
        "timestamp": now_local.isoformat(),
        "source": "shopify_live_collection",
        "collection_id": collection_id,
    }


def _maybe_refresh_active_sales_payload(active_sales_payload: dict[str, Any], now_local: datetime) -> dict[str, Any]:
    age = _hours_since(active_sales_payload.get("timestamp"), now_local)
    active_items = list(active_sales_payload.get("shopify") or [])
    if age is not None and age < 30 and active_items:
        return active_sales_payload
    try:
        refreshed = _fetch_live_active_sales_payload(now_local)
    except Exception:
        return active_sales_payload
    if not refreshed:
        return active_sales_payload
    ACTIVE_SALES_PATH.write_text(json.dumps(refreshed, indent=2), encoding="utf-8")
    return refreshed


def _marketing_recommendation(effectiveness: str) -> str:
    if effectiveness == "strong":
        return "Feature this sale in marketing. Use it in the email hero, social post, and supporting story slots."
    if effectiveness == "working":
        return "Keep it live and give it supportive social placement rather than a full campaign rewrite."
    if effectiveness == "watch":
        return "Leave it running for now, but do not center the next campaign around it until the numbers improve."
    return "Rotate this out or rewrite the sale hook next cycle instead of giving it more prime exposure."


def _effectiveness_bucket(sales_7d: int, sales_30d: int, lifetime_sales: int) -> tuple[str, str]:
    if sales_7d >= 8 or sales_30d >= 25:
        return "strong", "keep_and_feature"
    if sales_7d >= 3 or sales_30d >= 10:
        return "working", "keep_and_promote"
    if sales_30d >= 4 or lifetime_sales >= 150:
        return "watch", "keep_but_retest"
    return "weak", "rotate_or_rewrite"


def build_weekly_sale_monitor(
    active_sales_payload: dict[str, Any] | None = None,
    sales_cache_payload: dict[str, Any] | None = None,
    weekly_insights_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_local = _now_local()
    active_sales_payload = active_sales_payload or load_json(ACTIVE_SALES_PATH, {"shopify": []})
    if isinstance(active_sales_payload, dict):
        active_sales_payload = _maybe_refresh_active_sales_payload(active_sales_payload, now_local)
    sales_cache_payload = sales_cache_payload or load_json(SALES_CACHE_PATH, {"lifetime": {}, "last_30d": {}})
    weekly_insights_payload = weekly_insights_payload or load_json(WEEKLY_INSIGHTS_PATH, {})

    top_7d = {
        _normalize_title(item.get("title")): item
        for item in list(weekly_insights_payload.get("top_performers_7d") or [])
        if isinstance(item, dict)
    }
    top_30d = {
        _normalize_title(item.get("title")): item
        for item in list(weekly_insights_payload.get("top_performers_30d") or [])
        if isinstance(item, dict)
    }
    top_all = {
        _normalize_title(item.get("title")): item
        for item in list(weekly_insights_payload.get("top_performers") or [])
        if isinstance(item, dict)
    }

    items: list[dict[str, Any]] = []
    active_shopify = list(active_sales_payload.get("shopify") or [])
    lifetime_sales_map = sales_cache_payload.get("lifetime") or {}
    sales_30d_map = sales_cache_payload.get("last_30d") or {}

    for sale in active_shopify:
        product_id = str(sale.get("id") or "").strip()
        title = str(sale.get("title") or "").strip()
        norm = _normalize_title(title)
        recent_7d = int((top_7d.get(norm) or {}).get("sales_7d") or 0)
        recent_30d = int(
            sales_30d_map.get(product_id)
            or (top_30d.get(norm) or {}).get("sales_30d")
            or (top_all.get(norm) or {}).get("recent_sales")
            or 0
        )
        lifetime_sales = int(
            lifetime_sales_map.get(product_id)
            or (top_all.get(norm) or {}).get("lifetime_sales")
            or (top_30d.get(norm) or {}).get("lifetime_sales")
            or 0
        )
        discount = str(sale.get("discount") or "").strip()
        effectiveness, recommendation = _effectiveness_bucket(recent_7d, recent_30d, lifetime_sales)
        reasons: list[str] = []
        if recent_7d:
            reasons.append(f"{recent_7d} sale(s) in the last 7 days.")
        if recent_30d:
            reasons.append(f"{recent_30d} sale(s) in the last 30 days.")
        else:
            reasons.append("No recent sale velocity detected in the current 30-day snapshot.")
        if lifetime_sales:
            reasons.append(f"{lifetime_sales} lifetime sale(s) overall.")
        if _parse_percent(discount) >= 20 and effectiveness in {"watch", "weak"}:
            reasons.append("This is already a fairly deep discount, so weak results suggest the hook or placement needs work.")
        items.append(
            {
                "product_id": product_id,
                "product_title": title,
                "discount": discount,
                "discount_percent": _parse_percent(discount),
                "sales_7d": recent_7d,
                "sales_30d": recent_30d,
                "lifetime_sales": lifetime_sales,
                "effectiveness": effectiveness,
                "recommendation": recommendation,
                "reasons": reasons,
                "marketing_recommendation": _marketing_recommendation(effectiveness),
            }
        )

    rank = {"strong": 0, "working": 1, "watch": 2, "weak": 3}
    items.sort(
        key=lambda item: (
            rank.get(str(item.get("effectiveness") or "weak").lower(), 9),
            -int(item.get("sales_7d") or 0),
            -int(item.get("sales_30d") or 0),
            str(item.get("product_title") or "").lower(),
        )
    )

    counts = {
        "active_sale_items": len(items),
        "strong": sum(1 for item in items if item.get("effectiveness") == "strong"),
        "working": sum(1 for item in items if item.get("effectiveness") == "working"),
        "watch": sum(1 for item in items if item.get("effectiveness") == "watch"),
        "weak": sum(1 for item in items if item.get("effectiveness") == "weak"),
    }

    return {
        "generated_at": now_local.isoformat(),
        "source_timestamps": {
            "active_sales": active_sales_payload.get("timestamp"),
            "sales_cache": sales_cache_payload.get("last_sync"),
        },
        "source_freshness_hours": {
            "active_sales": _hours_since(active_sales_payload.get("timestamp"), now_local),
            "sales_cache": _hours_since(sales_cache_payload.get("last_sync"), now_local),
        },
        "counts": counts,
        "summary": {
            "headline": (
                "Weekly sales with real traction should stay in the marketing mix. "
                "Weak performers should trigger a rewrite or rotation instead of more exposure."
            ),
            "top_keep_titles": [item.get("product_title") for item in items if item.get("effectiveness") in {"strong", "working"}][:3],
            "top_rotate_titles": [item.get("product_title") for item in items if item.get("effectiveness") == "weak"][:3],
        },
        "items": items,
    }


def render_weekly_sale_monitor_markdown(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") or {}
    freshness = payload.get("source_freshness_hours") or {}
    lines = [
        "# Weekly Sale Monitor",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Active sale items: `{counts.get('active_sale_items', 0)}`",
        f"- Strong performers: `{counts.get('strong', 0)}`",
        f"- Working performers: `{counts.get('working', 0)}`",
        f"- Watch closely: `{counts.get('watch', 0)}`",
        f"- Weak performers: `{counts.get('weak', 0)}`",
        f"- Active-sales snapshot age: `{freshness.get('active_sales')}` hour(s)",
        f"- Sales-cache snapshot age: `{freshness.get('sales_cache')}` hour(s)",
        "",
        "## Read",
        "",
        str((payload.get("summary") or {}).get("headline") or "No summary available."),
        "",
        "## Active Sale Items",
        "",
    ]
    items = payload.get("items") or []
    if not items:
        lines.append("No active sale items were found.")
    else:
        for item in items:
            lines.append(
                f"- {item.get('product_title')} | {item.get('discount')} | {item.get('effectiveness')} | 7d {item.get('sales_7d', 0)} | 30d {item.get('sales_30d', 0)} | lifetime {item.get('lifetime_sales', 0)}"
            )
            lines.append(f"  Recommendation: {item.get('recommendation')}")
            lines.append(f"  Marketing: {item.get('marketing_recommendation')}")
            for reason in list(item.get("reasons") or [])[:3]:
                lines.append(f"  Why: {reason}")
    return "\n".join(lines) + "\n"


def sync_weekly_sale_monitor_control(payload: dict[str, Any]) -> dict[str, Any]:
    counts = payload.get("counts") or {}
    freshness = payload.get("source_freshness_hours") or {}
    active_sales_age = freshness.get("active_sales")
    sales_cache_age = freshness.get("sales_cache")
    active_items = int(counts.get("active_sale_items") or 0)
    weak_items = int(counts.get("weak") or 0)

    max_age = max(
        float(active_sales_age) if active_sales_age is not None else 0.0,
        float(sales_cache_age) if sales_cache_age is not None else 0.0,
    )
    if max_age >= 30:
        state = "blocked"
        state_reason = "stale_input"
        next_action = (
            "This sale monitor snapshot is stale right now. The next weekly sale or campaign refresh should rebuild it "
            "automatically, so only force a refresh if you need to make a sale decision before then."
        )
    elif active_items <= 0:
        state = "observed"
        state_reason = "no_active_sales"
        next_action = "Confirm there are no active sale items or refresh the source feeds before planning the next sale push."
    elif weak_items > 0:
        state = "observed"
        state_reason = "weak_items_present"
        next_action = "Rewrite or rotate the weak sale items before centering the next campaign on them."
    else:
        state = "verified"
        state_reason = "sale_monitor_ready"
        next_action = "Use this fresh sale monitor to guide the next weekly campaign and sale choices."

    control = record_workflow_transition(
        workflow_id="weekly_sale_monitor",
        lane="weekly_sale_monitor",
        display_label="Weekly Sale Monitor",
        entity_id="weekly_sale_monitor",
        state=state,
        state_reason=state_reason,
        requires_confirmation=False,
        input_freshness={
            "active_sales_hours": active_sales_age,
            "sales_cache_hours": sales_cache_age,
        },
        last_verification={
            "active_sale_items": active_items,
            "strong": int(counts.get("strong") or 0),
            "working": int(counts.get("working") or 0),
            "watch": int(counts.get("watch") or 0),
            "weak": weak_items,
        },
        next_action=next_action,
        metadata={
            "generated_at": payload.get("generated_at"),
            "top_keep_titles": list((payload.get("summary") or {}).get("top_keep_titles") or []),
            "top_rotate_titles": list((payload.get("summary") or {}).get("top_rotate_titles") or []),
        },
        receipt_kind="snapshot",
        receipt_payload={
            "generated_at": payload.get("generated_at"),
            "counts": counts,
        },
    )
    payload["workflow_control"] = {
        "state": control.get("state"),
        "state_reason": control.get("state_reason"),
        "updated_at": control.get("updated_at"),
        "next_action": control.get("next_action"),
    }
    return payload
