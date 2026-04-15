from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
DUCK_AGENT_VENV_PY = DUCK_AGENT_ROOT / ".venv" / "bin" / "python3"
REVIEW_STATE_DIR = DUCK_OPS_ROOT / "state" / "shopify_draft_activation_review"
REVIEW_RUN_DIR = REVIEW_STATE_DIR / "runs"
REVIEW_OUTPUT_MD = DUCK_OPS_ROOT / "output" / "operator" / "shopify_draft_activation_review.md"
NEW_DUCK_ARRIVALS_COLLECTION_TITLE = "New Duck Arrivals"


def _ensure_duckagent_python() -> None:
    if os.environ.get("SHOPIFY_DRAFT_ACTIVATION_VENV_READY") == "1":
        return
    current_python = Path(sys.executable).resolve()
    if current_python == DUCK_AGENT_VENV_PY or not DUCK_AGENT_VENV_PY.exists():
        return
    os.environ["SHOPIFY_DRAFT_ACTIVATION_VENV_READY"] = "1"
    os.execv(str(DUCK_AGENT_VENV_PY), [str(DUCK_AGENT_VENV_PY), str(Path(__file__).resolve()), *sys.argv[1:]])


def _ensure_duckagent_imports():
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        load_dotenv = None

    env_path = DUCK_AGENT_ROOT / ".env"
    if load_dotenv is not None:
        load_dotenv(env_path, override=False)
    elif env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)

    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    from helpers.email_helper import send_email  # type: ignore
    from helpers.report_email_helper import render_report_email, report_badge, report_card, report_link  # type: ignore
    from helpers.shopify_helper import _rest_get_paginated, product_id_to_gid, shopify_graphql  # type: ignore

    return send_email, render_report_email, report_badge, report_card, report_link, _rest_get_paginated, product_id_to_gid, shopify_graphql


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _review_run_path(run_id: str) -> Path:
    return REVIEW_RUN_DIR / f"{run_id}.json"


def _latest_path() -> Path:
    return REVIEW_STATE_DIR / "latest.json"


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _blank_html(value: Any) -> bool:
    text = str(value or "")
    text = text.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
    while "<" in text and ">" in text:
        start = text.find("<")
        end = text.find(">", start)
        if end == -1:
            break
        text = text[:start] + " " + text[end + 1 :]
    return not _normalize_text(text)


def _html_to_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
    while "<" in text and ">" in text:
        start = text.find("<")
        end = text.find(">", start)
        if end == -1:
            break
        text = text[:start] + " " + text[end + 1 :]
    return _normalize_text(text)


def _tag_list(tags_csv: Any) -> list[str]:
    return [tag.strip() for tag in str(tags_csv or "").split(",") if tag.strip()]


def _tag_quality_suggestions(tag_list: list[str], *, title: str) -> list[str]:
    if not tag_list:
        return []
    normalized = [" ".join(tag.lower().split()) for tag in tag_list if tag.strip()]
    unique_count = len(set(normalized))
    duplicate_count = len(normalized) - unique_count
    single_word_count = sum(1 for tag in normalized if len(tag.split()) <= 1)
    generic_tags = [
        tag
        for tag in normalized
        if tag in {"duck", "ducks", "collectible", "gift", "dashboard decor", "3d printed"}
    ]
    suggestions: list[str] = []
    if len(tag_list) < 10:
        suggestions.append(f"Only {len(tag_list)} Shopify tags; consider expanding to more distinct browse phrases.")
    if duplicate_count > 0:
        suggestions.append(f"Tag list has {duplicate_count} overlapping or duplicate phrase(s); keep the strongest distinct tags.")
    if single_word_count > max(6, len(tag_list) // 2):
        suggestions.append("A lot of the Shopify tags are single-word terms; prefer more 2-3 word browse phrases.")
    if len(tag_list) > 30 and (duplicate_count > 0 or len(generic_tags) > 2):
        suggestions.append("The tag set is broad but overlapping; trim weak generic tags instead of maximizing quantity.")
    title_words = {part for part in _normalize_text(title).lower().split() if part}
    title_overlap = sum(1 for tag in normalized if title_words and set(tag.split()).issubset(title_words))
    if title_overlap > max(4, len(tag_list) // 2):
        suggestions.append("Many Shopify tags just restate the product title; add more use-case, gift, or style phrases.")
    return suggestions


def _shopify_admin_product_url(product_id: Any) -> str:
    store_slug = str(os.getenv("SHOPIFY_ADMIN_STORE_SLUG", "") or "").strip()
    if not store_slug or not product_id:
        return ""
    return f"https://admin.shopify.com/store/{store_slug}/products/{product_id}"


def _fetch_draft_products(_rest_get_paginated) -> list[dict[str, Any]]:
    params = {
        "status": "draft",
        "limit": 250,
        "fields": "id,title,handle,status,body_html,vendor,product_type,tags,updated_at,images,variants",
    }
    items: list[dict[str, Any]] = []
    for page in _rest_get_paginated("products.json", params=params):
        items.extend(page.get("products", []) or [])
    return [item for item in items if isinstance(item, dict)]


def _fetch_graphql_product_details(product_id: int | str, product_id_to_gid, shopify_graphql) -> dict[str, Any]:
    query = """
    query DraftActivationAudit($id: ID!) {
      product(id: $id) {
        id
        status
        seo {
          title
          description
        }
        category {
          id
          fullName
        }
        collections(first: 20) {
          nodes {
            title
          }
        }
      }
    }
    """
    payload = shopify_graphql(query, {"id": product_id_to_gid(product_id)})
    product = ((payload.get("data") or {}).get("product")) or {}
    seo = product.get("seo") if isinstance(product.get("seo"), dict) else {}
    category = product.get("category") if isinstance(product.get("category"), dict) else {}
    return {
        "status": _normalize_text(product.get("status")).lower(),
        "seo_title": _normalize_text(seo.get("title")),
        "seo_description": _normalize_text(seo.get("description")),
        "category_id": _normalize_text(category.get("id")),
        "category_name": _normalize_text(category.get("fullName")),
        "collection_titles": [
            _normalize_text(node.get("title"))
            for node in ((((product.get("collections") or {}).get("nodes")) or []))
            if isinstance(node, dict) and _normalize_text(node.get("title"))
        ],
    }


def _variant_checks(product: dict[str, Any]) -> tuple[bool, list[str]]:
    variants = product.get("variants") if isinstance(product.get("variants"), list) else []
    if not variants:
        return False, ["No variants found."]
    details: list[str] = []
    ready = True
    primary = variants[0] if isinstance(variants[0], dict) else {}
    sku = _normalize_text(primary.get("sku"))
    price = float(primary.get("price") or 0)
    inventory_quantity = primary.get("inventory_quantity")
    if not sku:
        ready = False
        details.append("Primary variant SKU is missing.")
    if price <= 0:
        ready = False
        details.append("Primary variant price is missing or zero.")
    if inventory_quantity is not None:
        try:
            qty = int(inventory_quantity)
            if qty <= 0:
                ready = False
                details.append("Primary variant inventory is zero.")
        except Exception:
            pass
    if ready:
        detail = f"SKU {sku or '(missing)'} | Price {primary.get('price') or '(missing)'}"
        if inventory_quantity is not None:
            detail += f" | Qty {inventory_quantity}"
        details.append(detail)
    return ready, details


def _audit_product(product: dict[str, Any], gql_details: dict[str, Any]) -> dict[str, Any]:
    title = _normalize_text(product.get("title"))
    tags = _normalize_text(product.get("tags"))
    vendor = _normalize_text(product.get("vendor"))
    product_type = _normalize_text(product.get("product_type"))
    images = product.get("images") if isinstance(product.get("images"), list) else []
    description_text = _html_to_text(product.get("body_html"))
    tag_list = _tag_list(tags)
    variant_ready, variant_details = _variant_checks(product)
    collection_titles = set(gql_details.get("collection_titles") or [])
    image_alt_missing_count = sum(1 for image in images if isinstance(image, dict) and not _normalize_text(image.get("alt")))
    checks = [
        {
            "label": "Status is draft",
            "ok": gql_details.get("status") in {"draft", "active"},
            "detail": f"Current Shopify status: {gql_details.get('status') or 'unknown'}",
        },
        {
            "label": "Title present",
            "ok": bool(title),
            "detail": title or "Title is missing.",
        },
        {
            "label": "Description present",
            "ok": not _blank_html(product.get("body_html")),
            "detail": "Description present." if not _blank_html(product.get("body_html")) else "Body HTML is blank.",
        },
        {
            "label": "Images present",
            "ok": len(images) > 0,
            "detail": f"{len(images)} image(s)",
        },
        {
            "label": "Variant ready",
            "ok": variant_ready,
            "detail": "; ".join(variant_details),
        },
        {
            "label": "Tags present",
            "ok": bool(tags),
            "detail": tags or "Tags are missing.",
        },
        {
            "label": "Vendor and type present",
            "ok": bool(vendor) and bool(product_type),
            "detail": f"Vendor {vendor or '(missing)'} | Product type {product_type or '(missing)'}",
        },
        {
            "label": "SEO present",
            "ok": bool(gql_details.get("seo_title")) and bool(gql_details.get("seo_description")),
            "detail": (
                "SEO title and description present."
                if gql_details.get("seo_title") and gql_details.get("seo_description")
                else "SEO title or description is missing."
            ),
        },
        {
            "label": "Category present",
            "ok": bool(gql_details.get("category_id")),
            "detail": gql_details.get("category_name") or "Category is missing.",
        },
        {
            "label": "New Duck Arrivals collection",
            "ok": NEW_DUCK_ARRIVALS_COLLECTION_TITLE in collection_titles,
            "detail": (
                f"In {NEW_DUCK_ARRIVALS_COLLECTION_TITLE}"
                if NEW_DUCK_ARRIVALS_COLLECTION_TITLE in collection_titles
                else f"Not currently in {NEW_DUCK_ARRIVALS_COLLECTION_TITLE}."
            ),
        },
    ]
    blocking_labels = {
        "Title present",
        "Description present",
        "Images present",
        "Variant ready",
        "Tags present",
        "Vendor and type present",
        "SEO present",
        "Category present",
    }
    blocking_issues = [f"{item['label']}: {item['detail']}" for item in checks if item["label"] in blocking_labels and not item["ok"]]
    quality_suggestions: list[str] = []
    if 0 < len(images) < 3:
        quality_suggestions.append(f"Only {len(images)} image(s); consider adding at least 3 polished product shots.")
    if description_text and len(description_text) < 140:
        quality_suggestions.append("Description is short; consider adding a little more buyer-facing detail before activation.")
    if not _normalize_text(product.get("handle")):
        quality_suggestions.append("Handle is missing or blank; confirm Shopify generated a clean product handle.")
    if image_alt_missing_count:
        quality_suggestions.append(f"{image_alt_missing_count} product image(s) are missing alt text.")
    quality_suggestions.extend(_tag_quality_suggestions(tag_list, title=title))
    if NEW_DUCK_ARRIVALS_COLLECTION_TITLE not in collection_titles and product_type.lower() == "collectible duck":
        quality_suggestions.append(f"Add this draft to {NEW_DUCK_ARRIVALS_COLLECTION_TITLE} so the newest ducks rotate through the launch collection.")
    seo_title = str(gql_details.get("seo_title") or "")
    seo_description = str(gql_details.get("seo_description") or "")
    if seo_title and not 45 <= len(seo_title) <= 70:
        quality_suggestions.append(f"SEO title is {len(seo_title)} characters; aim for roughly 45-70 for cleaner search display.")
    if seo_description and not 140 <= len(seo_description) <= 160:
        quality_suggestions.append(f"SEO description is {len(seo_description)} characters; aim for roughly 140-160 for search snippets.")
    ready_for_activation = not blocking_issues and gql_details.get("status") in {"draft", "active"}
    return {
        "legacy_product_id": int(product.get("id") or 0),
        "id": str(product.get("id") or ""),
        "kind": "product",
        "title": title,
        "handle": _normalize_text(product.get("handle")),
        "status": gql_details.get("status") or _normalize_text(product.get("status")).lower(),
        "admin_url": _shopify_admin_product_url(product.get("id")),
        "updated_at": _normalize_text(product.get("updated_at")),
        "image_count": len(images),
        "tags": tags,
        "seo_title": gql_details.get("seo_title") or "",
        "seo_description": gql_details.get("seo_description") or "",
        "category_name": gql_details.get("category_name") or "",
        "checks": checks,
        "blocking_issues": blocking_issues,
        "quality_suggestions": quality_suggestions,
        "ready_for_activation": ready_for_activation,
    }


def render_shopify_draft_activation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Shopify Draft Activation Review",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Draft products found: `{payload.get('item_count', 0)}`",
        f"- Ready to activate: `{payload.get('ready_count', 0)}`",
        f"- Needs work: `{payload.get('blocked_count', 0)}`",
        f"- Ready with suggestions: `{payload.get('suggestion_count', 0)}`",
        "",
    ]
    for item in payload.get("items") or []:
        lines.append(f"## {item.get('title') or item.get('id')}")
        lines.append("")
        lines.append(f"- Product ID: `{item.get('legacy_product_id')}`")
        lines.append(f"- Status: `{item.get('status')}`")
        lines.append(f"- Ready: `{bool(item.get('ready_for_activation'))}`")
        if item.get("admin_url"):
            lines.append(f"- Admin: {item.get('admin_url')}")
        for check in item.get("checks") or []:
            marker = "PASS" if check.get("ok") else "CHECK"
            lines.append(f"- {marker}: {check.get('label')} — {check.get('detail')}")
        for suggestion in item.get("quality_suggestions") or []:
            lines.append(f"- NOTE: {suggestion}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_shopify_draft_activation_email(payload: dict[str, Any], *, render_report_email, report_badge, report_card, report_link) -> tuple[str, str, str]:
    ready_items = [item for item in (payload.get("items") or []) if item.get("ready_for_activation")]
    blocked_items = [item for item in (payload.get("items") or []) if not item.get("ready_for_activation")]
    preview_ready = ready_items[:10]
    preview_blocked = blocked_items[:10]
    ready_cards = []
    for item in preview_ready:
        admin_html = report_link(item["admin_url"], "Open draft") if item.get("admin_url") else f"Product ID {item.get('legacy_product_id')}"
        suggestions = "".join(f"<li>{issue}</li>" for issue in (item.get("quality_suggestions") or [])[:4])
        suggestions_html = (
            "<div style=\"margin-top:10px;color:#6b7280;\">Quality suggestions (advisory only):"
            f"<ul style=\"margin:6px 0 0;padding-left:20px;color:#4b5563;\">{suggestions}</ul></div>"
            if suggestions
            else ""
        )
        ready_cards.append(
            report_card(
                item.get("title") or str(item.get("legacy_product_id") or "Draft product"),
                (
                    f"<div style=\"margin-bottom:8px;\">{report_badge('READY', 'green')}</div>"
                    f"<div style=\"margin-bottom:8px;\">{admin_html}</div>"
                    f"<div style=\"color:#374151;\">Status: {item.get('status') or 'draft'} | Images: {item.get('image_count', 0)} | Category: {item.get('category_name') or 'missing'}</div>"
                    f"{suggestions_html}"
                ),
                eyebrow="Ready To Activate",
            )
        )
    blocked_cards = []
    for item in preview_blocked:
        admin_html = report_link(item["admin_url"], "Open draft") if item.get("admin_url") else f"Product ID {item.get('legacy_product_id')}"
        issues = "".join(f"<li>{issue}</li>" for issue in (item.get("blocking_issues") or [])[:5])
        blocked_cards.append(
            report_card(
                item.get("title") or str(item.get("legacy_product_id") or "Draft product"),
                (
                    f"<div style=\"margin-bottom:8px;\">{report_badge('NEEDS WORK', 'amber')}</div>"
                    f"<div style=\"margin-bottom:8px;\">{admin_html}</div>"
                    f"<ul style=\"margin:0;padding-left:20px;color:#374151;\">{issues}</ul>"
                ),
                eyebrow="Needs Work",
            )
        )
    body_html = "".join(
        [
            report_card(
                "Weekly Shopify Draft Review",
                (
                    "<div style=\"color:#374151;margin-bottom:8px;\">DuckAgent checked every current Shopify draft product for listing completeness.</div>"
                    "<div style=\"color:#374151;margin-bottom:8px;\">Reply <strong>\"publish\"</strong> or <strong>\"apply\"</strong> to activate all drafts in the <strong>ready</strong> bucket. Blocked drafts will be left alone.</div>"
                    "<div style=\"color:#6b7280;\">Quality suggestions are advisory only. They help us tighten the listing, but they do not block activation.</div>"
                    "<div style=\"color:#6b7280;margin-top:6px;\">Shopify tag advice is about distinct browse phrases and overlap cleanup, not simply maximizing the raw tag count.</div>"
                ),
                eyebrow="Summary",
            ),
            "".join(ready_cards) or report_card("No ready drafts", "<div style=\"color:#6b7280;\">No Shopify drafts passed the readiness checklist this week.</div>", eyebrow="Ready To Activate"),
            "".join(blocked_cards) if blocked_cards else "",
        ]
    )
    html = render_report_email(
        label="Duck Ops Shopify",
        title="Weekly Shopify Draft Review",
        subtitle="Review draft products before activating Shopify listings",
        body_html=body_html,
        stats=[
            ("Drafts", payload.get("item_count", 0)),
            ("Ready", payload.get("ready_count", 0)),
            ("Blocked", payload.get("blocked_count", 0)),
            ("Suggestions", payload.get("suggestion_count", 0)),
        ],
        footer_note="Duck Ops Shopify draft activation review",
    )
    text_lines = [
        "Weekly Shopify draft review",
        "",
        f"Draft products: {payload.get('item_count', 0)}",
        f"Ready to activate: {payload.get('ready_count', 0)}",
        f"Needs work: {payload.get('blocked_count', 0)}",
        f"Ready with suggestions: {payload.get('suggestion_count', 0)}",
        "",
        'Reply "publish" or "apply" to activate all ready Shopify drafts. Blocked drafts will be left alone.',
        "Quality suggestions are advisory only and do not block activation.",
        "Shopify tag suggestions are about better phrase coverage and less overlap, not just using the maximum number of tags.",
        "",
    ]
    if preview_ready:
        text_lines.append("Ready to activate:")
        for item in preview_ready:
            line = f"- {item.get('title')} (product {item.get('legacy_product_id')})"
            suggestions = item.get("quality_suggestions") or []
            if suggestions:
                line += f" | Suggestion: {suggestions[0]}"
            text_lines.append(line)
        text_lines.append("")
    if preview_blocked:
        text_lines.append("Needs work:")
        for item in preview_blocked:
            issues = item.get("blocking_issues") or []
            summary = issues[0] if issues else "See email for checklist."
            text_lines.append(f"- {item.get('title')}: {summary}")
    subject = f"MJD: [shopify_draft_activation] Weekly Shopify draft review | FLOW:shopify_draft_activation | RUN:{payload.get('run_id')} | ACTION:review"
    return subject, "\n".join(text_lines).strip(), html


def build_shopify_draft_activation_review() -> dict[str, Any]:
    (
        _send_email,
        _render_report_email,
        _report_badge,
        _report_card,
        _report_link,
        _rest_get_paginated,
        product_id_to_gid,
        shopify_graphql,
    ) = _ensure_duckagent_imports()
    generated_at = datetime.now().astimezone().isoformat()
    run_id = datetime.now().astimezone().strftime("shopify_draft_activation_%Y%m%d_%H%M%S")
    raw_products = _fetch_draft_products(_rest_get_paginated)
    items: list[dict[str, Any]] = []
    for product in raw_products:
        legacy_id = product.get("id")
        if not legacy_id:
            continue
        gql_details = _fetch_graphql_product_details(legacy_id, product_id_to_gid, shopify_graphql)
        items.append(_audit_product(product, gql_details))
    items.sort(key=lambda item: (not bool(item.get("ready_for_activation")), str(item.get("title") or "").lower()))
    payload = {
        "run_id": run_id,
        "generated_at": generated_at,
        "status": "awaiting_review",
        "item_count": len(items),
        "ready_count": sum(1 for item in items if item.get("ready_for_activation")),
        "blocked_count": sum(1 for item in items if not item.get("ready_for_activation")),
        "suggestion_count": sum(1 for item in items if item.get("quality_suggestions")),
        "items": items,
    }
    _write_json(_review_run_path(run_id), payload)
    _write_json(_latest_path(), payload)
    REVIEW_OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    REVIEW_OUTPUT_MD.write_text(render_shopify_draft_activation_markdown(payload), encoding="utf-8")
    return payload


def send_shopify_draft_activation_review_email() -> dict[str, Any] | None:
    (
        send_email,
        render_report_email,
        report_badge,
        report_card,
        report_link,
        _rest_get_paginated,
        product_id_to_gid,
        shopify_graphql,
    ) = _ensure_duckagent_imports()
    payload = build_shopify_draft_activation_review()
    if int(payload.get("item_count") or 0) <= 0:
        return None
    subject, text_body, html_body = render_shopify_draft_activation_email(
        payload,
        render_report_email=render_report_email,
        report_badge=report_badge,
        report_card=report_card,
        report_link=report_link,
    )
    send_email(subject, html_body, text_body)
    payload["status"] = "emailed"
    payload["email_subject"] = subject
    _write_json(_review_run_path(str(payload.get("run_id") or "")), payload)
    _write_json(_latest_path(), payload)
    REVIEW_OUTPUT_MD.write_text(render_shopify_draft_activation_markdown(payload), encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> int:
    _ensure_duckagent_python()
    parser = argparse.ArgumentParser(description="Build or email the weekly Shopify draft activation review.")
    parser.add_argument("--send-email", action="store_true", help="Send the review email after building the run.")
    args = parser.parse_args(argv)
    payload = send_shopify_draft_activation_review_email() if args.send_email else build_shopify_draft_activation_review()
    if payload is None:
        print("No Shopify draft products found.")
        return 0
    print(json.dumps({"run_id": payload.get("run_id"), "item_count": payload.get("item_count"), "ready_count": payload.get("ready_count")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
