from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from shopify_seo_audit import build_shopify_seo_audit


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
DUCK_AGENT_VENV_PY = DUCK_AGENT_ROOT / ".venv" / "bin" / "python3"
REVIEW_STATE_DIR = DUCK_OPS_ROOT / "state" / "shopify_seo_review"
REVIEW_RUN_DIR = REVIEW_STATE_DIR / "runs"
REVIEW_OUTPUT_MD = DUCK_OPS_ROOT / "output" / "operator" / "shopify_seo_review.md"

SEO_REVIEW_CATEGORY_SPECS: dict[str, dict[str, Any]] = {
    "missing_title": {
        "label": "Missing SEO titles",
        "issue_codes": {"missing_seo_title"},
        "apply_title": True,
        "apply_description": False,
    },
    "missing_description": {
        "label": "Missing SEO descriptions",
        "issue_codes": {"missing_seo_description"},
        "apply_title": False,
        "apply_description": True,
    },
    "long_title": {
        "label": "SEO titles too long",
        "issue_codes": {"long_seo_title"},
        "apply_title": True,
        "apply_description": False,
    },
    "long_description": {
        "label": "SEO descriptions too long",
        "issue_codes": {"long_seo_description"},
        "apply_title": False,
        "apply_description": True,
    },
    "short_title": {
        "label": "SEO titles too short",
        "issue_codes": {"short_seo_title"},
        "apply_title": True,
        "apply_description": False,
    },
    "duplicate_title": {
        "label": "Duplicate SEO titles",
        "issue_codes": {"duplicate_seo_title"},
        "apply_title": True,
        "apply_description": False,
    },
    "near_duplicate_title": {
        "label": "Near-duplicate SEO titles",
        "issue_codes": {"near_duplicate_seo_title"},
        "apply_title": True,
        "apply_description": False,
    },
    "weak_title": {
        "label": "Weak or raw-match SEO titles",
        "issue_codes": {"seo_title_matches_raw_title", "weak_generic_seo_title"},
        "apply_title": True,
        "apply_description": False,
    },
    "weak_description": {
        "label": "Weak or generic SEO descriptions",
        "issue_codes": {"low_value_seo_copy", "weak_generic_seo_description"},
        "apply_title": False,
        "apply_description": True,
    },
}
SEO_REVIEW_CATEGORY_ORDER = [
    "missing_title",
    "missing_description",
    "long_title",
    "long_description",
    "short_title",
    "duplicate_title",
    "near_duplicate_title",
    "weak_title",
    "weak_description",
]


def _ensure_duckagent_python() -> None:
    if os.environ.get("SHOPIFY_SEO_REVIEW_VENV_READY") == "1":
        return
    current_python = Path(sys.executable).resolve()
    if current_python == DUCK_AGENT_VENV_PY or not DUCK_AGENT_VENV_PY.exists():
        return
    os.environ["SHOPIFY_SEO_REVIEW_VENV_READY"] = "1"
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
    from helpers.openai_helper import openai_json  # type: ignore

    return openai_json, send_email


def _review_run_path(run_id: str) -> Path:
    return REVIEW_RUN_DIR / f"{run_id}.json"


def _latest_path() -> Path:
    return REVIEW_STATE_DIR / "latest.json"


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _supersede_open_category_reviews(
    *,
    review_type: str,
    issue_category: str | None,
    replacement_run_id: str,
    superseded_at: str,
) -> None:
    normalized_category = _normalize_text(issue_category).lower()
    if review_type != "issue_category_batch" or not normalized_category or not REVIEW_RUN_DIR.exists():
        return
    for path in REVIEW_RUN_DIR.glob("*.json"):
        payload = _load_json(path, {})
        if not isinstance(payload, dict):
            continue
        if str(payload.get("run_id") or "").strip() == replacement_run_id:
            continue
        if _normalize_text(payload.get("review_type")).lower() != "issue_category_batch":
            continue
        if _normalize_text(payload.get("seo_category")).lower() != normalized_category:
            continue
        if _normalize_text(payload.get("status")).lower() != "awaiting_review":
            continue
        payload["status"] = "superseded"
        payload["superseded_at"] = superseded_at
        payload["superseded_by_run_id"] = replacement_run_id
        payload["superseded_reason"] = "A newer Shopify SEO review batch replaced this category email."
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _trim_to_range(text: str, *, min_len: int, max_len: int, extras: list[str]) -> str:
    value = _normalize_text(text)
    if len(value) > max_len:
        value = value[: max_len + 1].rsplit(" ", 1)[0].strip() or value[:max_len].strip()
    for extra in extras:
        if len(value) >= min_len:
            break
        candidate = _normalize_text(f"{value} {extra}")
        if len(candidate) <= max_len:
            value = candidate
    if len(value) < min_len:
        room = max_len - len(value) - 1
        if room > 0:
            padding = _normalize_text("collectible duck gift idea")
            padding = padding[: room + 1].rsplit(" ", 1)[0].strip() or padding[:room].strip()
            if padding:
                value = _normalize_text(f"{value} {padding}")
    return value


def _finalize_sentence(text: str, *, max_len: int) -> str:
    value = _normalize_text(text).rstrip(" ,;:-|")
    if len(value) > max_len:
        value = value[: max_len + 1].rsplit(" ", 1)[0].strip() or value[:max_len].strip()
        value = value.rstrip(" ,;:-|")
    trailing_stopwords = {"and", "or", "the", "for", "with", "from"}
    while value:
        bare = value.rstrip(".!?").rstrip(" ,;:-|")
        if not bare:
            break
        last_word = bare.split()[-1].lower()
        if last_word not in trailing_stopwords:
            value = bare
            break
        trimmed = " ".join(bare.split()[:-1]).rstrip(" ,;:-|")
        if not trimmed:
            value = bare
            break
        value = trimmed
    if value and value[-1] not in ".!?":
        if len(value) < max_len:
            value = f"{value}."
        else:
            trimmed = value[: max_len - 1].rsplit(" ", 1)[0].strip() or value[: max_len - 1].strip()
            value = trimmed.rstrip(" ,;:-|")
            while value:
                last_word = value.split()[-1].lower()
                if last_word not in trailing_stopwords:
                    break
                value = " ".join(value.split()[:-1]).rstrip(" ,;:-|")
            if value:
                value = f"{value}."
    return value


def _clean_product_title(title: str) -> str:
    value = _normalize_text(title)
    value = value.replace("–", "-")
    value = re.sub(r"\s*\|\s*", " | ", value)
    value = re.sub(
        r"(?i)\s*-\s*3d printed(?: [a-z]+)? collectible ducks?\b.*$",
        "",
        value,
    )
    value = re.sub(r"(?i)\s*-\s*collectible ducks?\b.*$", "", value)
    value = re.sub(r"(?i)\bweiner doc\b", "Wiener Dog", value)
    value = re.sub(r"(?i)\b3d printed(?: [a-z]+)? collectible ducks?\b", "", value)
    value = re.sub(r"(?i)\bcollectible ducks?\b", "", value)
    value = re.sub(r"\s{2,}", " ", value).strip(" -|")
    if " | " in value:
        value = value.split(" | ", 1)[0].strip()
    if len(value) > 40 and " - " in value:
        parts = [part.strip() for part in value.split(" - ") if part.strip()]
        shortened = parts[0]
        for part in parts[1:]:
            candidate = f"{shortened} {part}"
            if len(candidate) > 40:
                break
            shortened = candidate
        value = shortened
    if len(value) > 40:
        value = value[:41].rsplit(" ", 1)[0].strip() or value[:40].strip()
    value = value.replace(" - ", " ")
    return _normalize_text(value)


def _ensure_duck_keyword(value: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        return "Duck"
    if re.search(r"(?i)\bduck\b", normalized):
        return normalized
    return _normalize_text(f"{normalized} Duck")


def _title_with_brand(base: str, suffixes: list[str], *, min_len: int = 45, max_len: int = 70) -> str:
    base = _normalize_text(base).rstrip(" ,;:-|")
    for suffix in suffixes:
        candidate = f"{base} {suffix}".strip()
        if len(candidate) > max_len:
            room = max_len - len(suffix) - 1
            if room > 0:
                trimmed_base = base[: room + 1].rsplit(" ", 1)[0].strip() or base[:room].strip()
                candidate = f"{trimmed_base} {suffix}".strip()
        if min_len <= len(candidate) <= max_len:
            return candidate.rstrip(" ,;:-|")
    fallback_suffix = suffixes[-1]
    room = max_len - len(fallback_suffix) - 1
    trimmed_base = base[: room + 1].rsplit(" ", 1)[0].strip() or base[:room].strip()
    return f"{trimmed_base} {fallback_suffix}".strip().rstrip(" ,;:-|")


def _clean_collection_title(title: str) -> str:
    value = _normalize_text(title)
    if " - " in value:
        parts = [part.strip() for part in value.split(" - ") if part.strip()]
        if parts and len(parts[-1]) >= 6:
            return parts[-1]
    return value


def _default_rationale_for_resource(resource: dict[str, Any]) -> str:
    kind = str(resource.get("kind") or "")
    issue_codes = {
        str(issue.get("code") or "")
        for issue in (resource.get("issues") or [])
        if isinstance(issue, dict)
    }
    if "missing_seo_title" in issue_codes and "missing_seo_description" in issue_codes:
        return f"Adds missing search metadata to this {kind}."
    if "missing_seo_title" in issue_codes:
        return f"Adds a missing SEO title for this {kind}."
    if "missing_seo_description" in issue_codes:
        return f"Adds a missing SEO description for this {kind}."
    if "duplicate_seo_title" in issue_codes:
        return f"Helps this {kind} stand apart in search results."
    if "near_duplicate_seo_title" in issue_codes:
        return f"Helps this {kind} avoid blending into another similar search result."
    if "seo_title_matches_raw_title" in issue_codes:
        return f"Turns the raw {kind} name into a stronger search snippet."
    if "weak_generic_seo_title" in issue_codes:
        return f"Makes the SEO title for this {kind} more specific and click-worthy."
    if "weak_generic_seo_description" in issue_codes or "low_value_seo_copy" in issue_codes:
        return f"Makes the SEO description for this {kind} more specific to what shoppers will find."
    return f"Tightens weak SEO copy for this {kind}."


def _default_title_for_resource(resource: dict[str, Any]) -> str:
    title = _normalize_text(resource.get("title"))
    kind = str(resource.get("kind") or "")
    lowered = title.lower()
    resource_url = _normalize_text(resource.get("resource_url")).lower()
    if kind == "page":
        if "data-sharing-opt-out" in resource_url or "privacy choices" in lowered:
            base = "Your Privacy Choices | MyJeepDuck Data Sharing Opt-Out"
        elif "about" in lowered:
            base = "About MyJeepDuck Collectible Ducks and Gift Ideas"
        elif "contact" in lowered:
            base = "Contact MyJeepDuck for Collectible Duck Order Help"
        elif "privacy" in lowered:
            base = "MyJeepDuck Privacy Policy for Collectible Duck Orders"
        elif "terms" in lowered:
            base = "MyJeepDuck Terms of Service for Collectible Duck Orders"
        elif "bundle" in lowered:
            base = "Duck Gift Bundles and Flock Sets | MyJeepDuck"
        else:
            base = f"{title} | MyJeepDuck Collectible Ducks"
    elif kind == "collection":
        clean_title = _clean_collection_title(title)
        base = _title_with_brand(
            f"Shop {clean_title}",
            ["| MyJeepDuck", "| MyJeepDuck Collectible Ducks"],
        )
    elif kind == "article":
        base = f"{title} | MyJeepDuck Blog"
    else:
        clean_name = _ensure_duck_keyword(_clean_product_title(title))
        base = _title_with_brand(
            clean_name,
            [
                "| MyJeepDuck Dashboard Decor Duck Gift",
                "| MyJeepDuck Collectible Duck Gift",
                "| MyJeepDuck Duck Gift Idea",
                "| MyJeepDuck",
            ],
        )
    return _trim_to_range(base, min_len=45, max_len=70, extras=["gift idea"])


def _default_description_for_resource(resource: dict[str, Any]) -> str:
    title = _normalize_text(resource.get("title"))
    kind = str(resource.get("kind") or "")
    lowered = title.lower()
    resource_url = _normalize_text(resource.get("resource_url")).lower()
    if kind == "product":
        clean_name = _clean_product_title(title)
        base = (
            f"Shop {clean_name} at MyJeepDuck for dashboard decor, gift-ready ducking fun, and "
            "playful collectible style that helps your flock stand out"
        )
    elif kind == "collection":
        clean_title = _clean_collection_title(title)
        base = (
            f"Browse {clean_title} at MyJeepDuck for collectible ducks, gift-ready favorites, and playful "
            "flock picks for dashboards, swaps, gifts, and easy gifting today."
        )
    elif kind == "page" and "about" in lowered:
        base = (
            "Learn how MyJeepDuck creates collectible dashboard ducks, custom gift ideas, and flock "
            "favorites for ducking fans, gift shoppers, and playful collectors."
        )
    elif kind == "page" and "contact" in lowered:
        base = (
            "Contact MyJeepDuck for help with collectible ducks, custom ideas, order questions, and "
            "gift guidance for ducking fans building a standout flock for any dashboard or desk."
        )
    elif kind == "page" and "privacy" in lowered:
        base = (
            "Review the MyJeepDuck privacy policy to learn how collectible duck orders, customer details, "
            "and store communications are handled and protected for shoppers and gift buyers."
        )
    elif kind == "page" and ("data-sharing-opt-out" in resource_url or "privacy choices" in lowered):
        base = (
            "Manage your MyJeepDuck privacy choices and data-sharing preferences, including opt-out options "
            "for ads, customer personalization, and store communications."
        )
    elif kind == "page" and "terms" in lowered:
        base = (
            "Read the MyJeepDuck terms of service for collectible duck orders, store policies, and the key "
            "details shoppers should know before purchasing gifts, customs, or ready-to-ship flock favorites."
        )
    elif kind == "page" and "bundle" in lowered:
        base = (
            "Explore MyJeepDuck bundle sets for collectible ducks, gift-ready mixes, and curated flock picks "
            "that make birthdays, swaps, and ducking surprises easy to shop."
        )
    else:
        base = (
            f"Explore {title} at MyJeepDuck for collectible ducks, custom gift ideas, and playful flock "
            "favorites built for dashboard displays, ducking fans, and standout gift shopping."
        )
    extras = ["anywhere"] if kind == "product" else ["Shop MyJeepDuck for more collectible ducks and gift-ready favorites."]
    return _finalize_sentence(
        _trim_to_range(
            base,
            min_len=150,
            max_len=160,
            extras=extras,
        ),
        max_len=160,
    )


def _title_needs_fallback(value: str) -> bool:
    text = _normalize_text(value).lower()
    return (not text) or ("collectible flock favorite" in text) or len(text) < 45 or len(text) > 70


def _description_needs_fallback(value: str) -> bool:
    text = _normalize_text(value).lower()
    return (not text) or len(text) < 150 or len(text) > 160 or text.endswith(("and", "or", "the", "for", "with", "from", "|"))


def _issue_codes(resource: dict[str, Any]) -> set[str]:
    return {
        str(issue.get("code") or "")
        for issue in (resource.get("issues") or [])
        if isinstance(issue, dict)
    }


def _priority_sort_key(resource: dict[str, Any]) -> tuple[int, int, str]:
    issues = resource.get("issues") if isinstance(resource.get("issues"), list) else []
    high = sum(1 for issue in issues if isinstance(issue, dict) and issue.get("severity") == "high")
    medium = sum(1 for issue in issues if isinstance(issue, dict) and issue.get("severity") == "medium")
    return (-high, -medium, str(resource.get("title") or "").lower())


def _select_review_candidates(audit_payload: dict[str, Any], *, limit: int = 10) -> list[dict[str, Any]]:
    top_actions = audit_payload.get("top_actions") if isinstance(audit_payload.get("top_actions"), list) else []
    return [dict(item) for item in top_actions[:limit] if isinstance(item, dict)]


def _select_missing_only_candidates(audit_payload: dict[str, Any], *, limit: int | None = None) -> list[dict[str, Any]]:
    resources = audit_payload.get("resources") if isinstance(audit_payload.get("resources"), list) else []
    selected: list[dict[str, Any]] = []
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        issue_codes = _issue_codes(resource)
        if not issue_codes:
            continue
        if issue_codes.issubset({"missing_seo_title", "missing_seo_description"}):
            selected.append(dict(resource))
    selected.sort(key=_priority_sort_key)
    if limit is None or limit <= 0:
        return selected
    return selected[:limit]


def _category_spec(category: str | None) -> dict[str, Any] | None:
    if not category:
        return None
    return SEO_REVIEW_CATEGORY_SPECS.get(str(category))


def _category_label(category: str | None) -> str:
    spec = _category_spec(category)
    return str(spec.get("label") or category or "SEO category") if spec else str(category or "SEO category")


def _select_issue_category_candidates(
    audit_payload: dict[str, Any],
    *,
    issue_category: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    spec = _category_spec(issue_category)
    if not spec:
        return []
    issue_codes = set(spec.get("issue_codes") or set())
    resources = audit_payload.get("resources") if isinstance(audit_payload.get("resources"), list) else []
    selected: list[dict[str, Any]] = []
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        codes = _issue_codes(resource)
        if codes & issue_codes:
            selected.append(dict(resource))
    selected.sort(key=_priority_sort_key)
    if limit is None or limit <= 0:
        return selected
    return selected[:limit]


def _next_issue_category(after_category: str | None, audit_payload: dict[str, Any]) -> str | None:
    start_index = -1
    if after_category in SEO_REVIEW_CATEGORY_ORDER:
        start_index = SEO_REVIEW_CATEGORY_ORDER.index(str(after_category))
    for category in SEO_REVIEW_CATEGORY_ORDER[start_index + 1 :]:
        if _select_issue_category_candidates(audit_payload, issue_category=category, limit=1):
            return category
    return None


def _generate_proposals(resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    openai_json, _ = _ensure_duckagent_imports()
    prompt_resources = []
    for resource in resources:
        prompt_resources.append(
            {
                "id": resource.get("id"),
                "kind": resource.get("kind"),
                "title": resource.get("title"),
                "path": resource.get("resource_url"),
                "current_seo_title": resource.get("seo_title"),
                "current_seo_description": resource.get("seo_description"),
                "issues": [issue.get("code") for issue in resource.get("issues") or [] if isinstance(issue, dict)],
            }
        )

    system = (
        "You write Shopify SEO metadata for a collectible duck store. "
        "Return strict JSON only. "
        "Keep titles clear and clickable without keyword stuffing. "
        "Keep descriptions plain text, shopper-friendly, and accurate."
    )
    user = (
        "Create proposed replacement SEO for each Shopify resource below.\n"
        "Rules:\n"
        "- Return one item per input id.\n"
        "- seo_title must be 45-70 characters.\n"
        "- seo_description must be 150-160 characters.\n"
        "- Do not invent pricing, shipping promises, or trademark claims.\n"
        "- Keep titles/descriptions plain text only.\n"
        "- rationale should be 4-18 words.\n\n"
        f"RESOURCES_JSON:\n{json.dumps(prompt_resources, ensure_ascii=False)}\n\n"
        'Return JSON as {"items":[{"id":"...","seo_title":"...","seo_description":"...","rationale":"..."}]}.'
    )
    response = openai_json(system, user, max_tokens=2200, temperature=0.3, model="gpt-4o-mini")
    items = response.get("items") if isinstance(response.get("items"), list) else []
    proposals: list[dict[str, Any]] = []
    by_id = {str(item.get("id") or ""): item for item in items if isinstance(item, dict)}
    for resource in resources:
        rid = str(resource.get("id") or "")
        proposal = by_id.get(rid) or {}
        seo_title = _trim_to_range(
            proposal.get("seo_title") or resource.get("title") or "",
            min_len=45,
            max_len=70,
            extras=["MyJeepDuck", "gift ideas"],
        )
        seo_description = _trim_to_range(
            proposal.get("seo_description") or f"Shop {resource.get('title') or 'this MyJeepDuck favorite'} at MyJeepDuck.",
            min_len=150,
            max_len=160,
            extras=[
                "Explore collectible ducks, custom gifts, and quick-ship favorites from MyJeepDuck.",
                "Built for ducking fans, gift shoppers, and standout dashboard displays.",
            ],
        )
        if _title_needs_fallback(seo_title):
            seo_title = _default_title_for_resource(resource)
        if _description_needs_fallback(seo_description):
            seo_description = _default_description_for_resource(resource)
        proposals.append(
            {
                "id": rid,
                "seo_title": seo_title,
                "seo_description": seo_description,
                "rationale": _normalize_text(proposal.get("rationale") or "Improves clarity, missing metadata coverage, and click appeal."),
            }
        )
    return proposals


def _review_subject_label(review_type: str, issue_category: str | None = None) -> str:
    if review_type == "issue_category_batch":
        return _category_label(issue_category)
    if review_type == "missing_only_bulk":
        return "Missing SEO backfill review"
    return "Top 10 SEO fixes"


def _approval_action(review_type: str, item_count: int, *, issue_category: str | None = None, auto_send_next_category: bool = False) -> str:
    if review_type == "issue_category_batch":
        action = f"Reply apply to update all {item_count} items in the {_category_label(issue_category).lower()} batch."
        if auto_send_next_category:
            action += " If this applies cleanly, DuckAgent will email the next remaining SEO category automatically."
        return action
    if review_type == "missing_only_bulk":
        return (
            f"Reply apply to backfill all missing SEO fields in this run ({item_count} items). "
            "Existing SEO metadata will be left unchanged."
        )
    return f"Reply apply to update all {item_count} proposed SEO fixes."


def _kind_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        kind = str(item.get("kind") or "unknown")
        counts[kind] = counts.get(kind, 0) + 1
    return counts


def _display_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    preview_limit = payload.get("preview_limit")
    if isinstance(preview_limit, int) and preview_limit > 0:
        return items[:preview_limit]
    return items


def build_shopify_seo_review(
    *,
    limit: int = 10,
    force_audit: bool = False,
    review_type: str = "top_actions",
    issue_category: str | None = None,
    auto_send_next_category: bool = False,
) -> dict[str, Any]:
    audit_payload = build_shopify_seo_audit() if force_audit else _load_json(DUCK_OPS_ROOT / "state" / "shopify_seo_audit.json", {})
    if not isinstance(audit_payload, dict) or not audit_payload:
        audit_payload = build_shopify_seo_audit()

    if review_type == "missing_only_bulk":
        selected = _select_missing_only_candidates(audit_payload, limit=limit if limit > 0 else None)
        proposals: list[dict[str, Any]] = []
    elif review_type == "issue_category_batch":
        selected = _select_issue_category_candidates(
            audit_payload,
            issue_category=str(issue_category or ""),
            limit=limit if limit > 0 else None,
        )
        proposals = []
    else:
        selected = _select_review_candidates(audit_payload, limit=limit)
        proposals = _generate_proposals(selected)
    proposal_map = {str(item.get("id") or ""): item for item in proposals}

    if review_type == "missing_only_bulk":
        run_prefix = "shopify_seo_missing"
    elif review_type == "issue_category_batch":
        run_prefix = f"shopify_seo_{issue_category or 'category'}"
    else:
        run_prefix = "shopify_seo"
    run_id = f"{run_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    generated_at = datetime.now().astimezone().isoformat()
    items: list[dict[str, Any]] = []
    category_spec = _category_spec(issue_category)
    for resource in selected:
        proposal = proposal_map.get(str(resource.get("id") or ""), {})
        issue_codes = _issue_codes(resource)
        if review_type == "missing_only_bulk":
            apply_seo_title = "missing_seo_title" in issue_codes
            apply_seo_description = "missing_seo_description" in issue_codes
        elif review_type == "issue_category_batch":
            apply_seo_title = bool(category_spec and category_spec.get("apply_title"))
            apply_seo_description = bool(category_spec and category_spec.get("apply_description"))
        else:
            apply_seo_title = True
            apply_seo_description = True
        proposed_title = (
            proposal.get("seo_title")
            or _default_title_for_resource(resource)
            if apply_seo_title
            else resource.get("seo_title") or ""
        )
        proposed_description = (
            proposal.get("seo_description")
            or _default_description_for_resource(resource)
            if apply_seo_description
            else resource.get("seo_description") or ""
        )
        if apply_seo_title and _title_needs_fallback(proposed_title):
            proposed_title = _default_title_for_resource(resource)
        if apply_seo_description and _description_needs_fallback(proposed_description):
            proposed_description = _default_description_for_resource(resource)
        items.append(
            {
                "id": str(resource.get("id") or ""),
                "kind": resource.get("kind"),
                "title": resource.get("title"),
                "resource_url": resource.get("resource_url"),
                "current_seo_title": resource.get("seo_title"),
                "current_seo_description": resource.get("seo_description"),
                "issues": resource.get("issues") or [],
                "proposed_seo_title": proposed_title,
                "proposed_seo_description": proposed_description,
                "rationale": _normalize_text(proposal.get("rationale") or _default_rationale_for_resource(resource)),
                "apply_seo_title": apply_seo_title,
                "apply_seo_description": apply_seo_description,
            }
        )

    preview_limit = 15 if review_type == "missing_only_bulk" and len(items) > 15 else 0
    if review_type == "issue_category_batch" and len(items) > 15:
        preview_limit = 15
    payload = {
        "run_id": run_id,
        "review_type": review_type,
        "seo_category": issue_category,
        "category_label": _category_label(issue_category) if issue_category else "",
        "auto_send_next_category": auto_send_next_category,
        "generated_at": generated_at,
        "shopify_domain": audit_payload.get("shopify_domain") or "",
        "audit_generated_at": audit_payload.get("generated_at"),
        "item_count": len(items),
        "preview_limit": preview_limit,
        "kind_counts": _kind_counts(items),
        "approval_action": _approval_action(
            review_type,
            len(items),
            issue_category=issue_category,
            auto_send_next_category=auto_send_next_category,
        ),
        "items": items,
        "status": "awaiting_review",
    }

    REVIEW_RUN_DIR.mkdir(parents=True, exist_ok=True)
    _review_run_path(run_id).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    _supersede_open_category_reviews(
        review_type=review_type,
        issue_category=issue_category,
        replacement_run_id=run_id,
        superseded_at=generated_at,
    )
    _latest_path().write_text(json.dumps(payload, indent=2), encoding="utf-8")
    REVIEW_OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    REVIEW_OUTPUT_MD.write_text(render_shopify_seo_review_markdown(payload), encoding="utf-8")
    return payload


def render_shopify_seo_review_markdown(payload: dict[str, Any]) -> str:
    review_type = str(payload.get("review_type") or "top_actions")
    item_count = int(payload.get("item_count", 0) or 0) or len(payload.get("items") or [])
    display_items = _display_items(payload)
    hidden_count = max(0, item_count - len(display_items))
    kind_counts = payload.get("kind_counts") if isinstance(payload.get("kind_counts"), dict) else {}
    issue_category = str(payload.get("seo_category") or "")
    auto_send_next_category = bool(payload.get("auto_send_next_category"))
    approval_action = str(payload.get("approval_action") or _approval_action(review_type, item_count, issue_category=issue_category, auto_send_next_category=auto_send_next_category))
    lines = [
        "# Shopify SEO Review",
        "",
        f"- Generated at: `{payload.get('generated_at', '')}`",
        f"- Store: `{payload.get('shopify_domain', '')}`",
        f"- Review type: `{review_type}`",
        f"- Category: `{payload.get('category_label', '')}`" if payload.get("category_label") else "",
        f"- Items: `{item_count}`",
        f"- Approval action: `{approval_action}`",
        "",
    ]
    lines = [line for line in lines if line != "" or (lines and lines[-1] != "")]
    if kind_counts:
        lines.append("## Scope")
        lines.append("")
        for kind, count in sorted(kind_counts.items()):
            lines.append(f"- {kind.title()}s: `{count}`")
        lines.append("")
    if hidden_count > 0:
        lines.append(f"_Showing the first {len(display_items)} items. {hidden_count} more will be applied if you reply apply._")
        lines.append("")
    for idx, item in enumerate(display_items, start=1):
        lines.append(f"## {idx}. {item.get('title') or '(untitled)'}")
        lines.append("")
        lines.append(f"- Kind: `{item.get('kind', '')}`")
        lines.append(f"- Path: `{item.get('resource_url', '')}`")
        lines.append(f"- Why: `{item.get('rationale', '')}`")
        if item.get("apply_seo_title", True):
            lines.append(f"- Current SEO title: `{item.get('current_seo_title', '')}`")
            lines.append(f"- Proposed SEO title: `{item.get('proposed_seo_title', '')}`")
        else:
            lines.append(f"- Current SEO title: `{item.get('current_seo_title', '')}`")
            lines.append("- SEO title action: `keep current`")
        if item.get("apply_seo_description", True):
            lines.append(f"- Current SEO description: `{item.get('current_seo_description', '')}`")
            lines.append(f"- Proposed SEO description: `{item.get('proposed_seo_description', '')}`")
        else:
            lines.append(f"- Current SEO description: `{item.get('current_seo_description', '')}`")
            lines.append("- SEO description action: `keep current`")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_shopify_seo_review_email(payload: dict[str, Any]) -> tuple[str, str, str]:
    review_type = str(payload.get("review_type") or "top_actions")
    item_count = int(payload.get("item_count", 0) or 0) or len(payload.get("items") or [])
    display_items = _display_items(payload)
    hidden_count = max(0, item_count - len(display_items))
    kind_counts = payload.get("kind_counts") if isinstance(payload.get("kind_counts"), dict) else {}
    issue_category = str(payload.get("seo_category") or "")
    auto_send_next_category = bool(payload.get("auto_send_next_category"))
    approval_action = str(
        payload.get("approval_action")
        or _approval_action(
            review_type,
            item_count,
            issue_category=issue_category,
            auto_send_next_category=auto_send_next_category,
        )
    )
    approval_action_html = approval_action.replace("Reply apply", "Reply <code>apply</code>")
    category_html = f"<strong>Category:</strong> {payload.get('category_label', '')}<br/>" if payload.get("category_label") else ""
    subject = f"MJD: [shopify_seo] {_review_subject_label(review_type, issue_category)} | FLOW:shopify_seo | RUN:{payload['run_id']} | ACTION:review"
    text_lines = [
        "Shopify SEO review ready",
        "",
        f"Store: {payload.get('shopify_domain', '')}",
        f"Generated: {payload.get('generated_at', '')}",
        f"Review type: {review_type}",
        *([f"Category: {payload.get('category_label', '')}"] if payload.get("category_label") else []),
        f"Items in run: {item_count}",
        "",
        f"Approval action: {approval_action}",
        "",
    ]
    html_parts = [
        "<html><body style='font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;'>",
        "<h2>Shopify SEO review ready</h2>",
        (
            f"<p><strong>Store:</strong> {payload.get('shopify_domain', '')}<br/>"
            f"<strong>Generated:</strong> {payload.get('generated_at', '')}<br/>"
            f"<strong>Review type:</strong> {review_type}<br/>"
            f"{category_html}"
            f"<strong>Items in run:</strong> {item_count}</p>"
        ),
        f"<p><strong>Approval action:</strong> {approval_action_html}</p>",
    ]
    if kind_counts:
        scope_text = " | ".join(f"{kind.title()}s: {count}" for kind, count in sorted(kind_counts.items()))
        text_lines.extend([f"Scope: {scope_text}", ""])
        html_parts.append(f"<p><strong>Scope:</strong> {scope_text}</p>")
    if hidden_count > 0:
        text_lines.extend([f"Showing the first {len(display_items)} items below. {hidden_count} more are included in this run.", ""])
        html_parts.append(
            f"<p><em>Showing the first {len(display_items)} items below. {hidden_count} more are included in this run.</em></p>"
        )
    for idx, item in enumerate(display_items, start=1):
        issues = item.get("issues") if isinstance(item.get("issues"), list) else []
        issue_text = ", ".join(
            str(issue.get("message") or issue.get("code") or "")
            for issue in issues
            if isinstance(issue, dict)
        )
        title_action = item.get("proposed_seo_title", "") if item.get("apply_seo_title", True) else "(keep current)"
        description_action = item.get("proposed_seo_description", "") if item.get("apply_seo_description", True) else "(keep current)"
        text_lines.extend(
            [
                f"{idx}. {item.get('title') or '(untitled)'} [{item.get('kind', '')}]",
                f"Path: {item.get('resource_url', '')}",
                f"Issues: {issue_text}",
                f"Current SEO title: {item.get('current_seo_title', '') or '(missing)'}",
                f"SEO title action: {title_action}",
                f"Current SEO description: {item.get('current_seo_description', '') or '(missing)'}",
                f"SEO description action: {description_action}",
                f"Why: {item.get('rationale', '')}",
                "",
            ]
        )
        html_parts.extend(
            [
                "<div style='border:1px solid #d9dee5;border-radius:10px;padding:14px;margin:14px 0;'>",
                f"<h3 style='margin:0 0 8px 0;'>{idx}. {item.get('title') or '(untitled)'}</h3>",
                f"<p style='margin:0 0 8px 0;'><strong>Kind:</strong> {item.get('kind', '')}<br/>"
                f"<strong>Path:</strong> <code>{item.get('resource_url', '')}</code></p>",
                f"<p style='margin:0 0 8px 0;'><strong>Why this matters:</strong> {item.get('rationale', '')}</p>",
                f"<p style='margin:0 0 4px 0;'><strong>Current SEO title:</strong> {item.get('current_seo_title', '') or '(missing)'}</p>",
                f"<p style='margin:0 0 8px 0;'><strong>SEO title action:</strong> {title_action}</p>",
                f"<p style='margin:0 0 4px 0;'><strong>Current SEO description:</strong> {item.get('current_seo_description', '') or '(missing)'}</p>",
                f"<p style='margin:0;'><strong>SEO description action:</strong> {description_action}</p>",
                "</div>",
            ]
        )
    html_parts.append("</body></html>")
    return subject, "\n".join(text_lines).strip() + "\n", "".join(html_parts)


def send_shopify_seo_review_email(
    *,
    limit: int = 10,
    force_audit: bool = False,
    review_type: str = "top_actions",
    issue_category: str | None = None,
    auto_send_next_category: bool = False,
) -> dict[str, Any]:
    payload = build_shopify_seo_review(
        limit=limit,
        force_audit=force_audit,
        review_type=review_type,
        issue_category=issue_category,
        auto_send_next_category=auto_send_next_category,
    )
    _, send_email = _ensure_duckagent_imports()
    subject, text_body, html_body = render_shopify_seo_review_email(payload)
    send_email(subject, html_body, text_body)
    payload["email_subject"] = subject
    payload["status"] = "awaiting_review"
    _review_run_path(payload["run_id"]).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    _latest_path().write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def send_next_shopify_seo_category_review_email(*, after_category: str | None = None, force_audit: bool = False) -> dict[str, Any] | None:
    audit_payload = build_shopify_seo_audit() if force_audit else _load_json(DUCK_OPS_ROOT / "state" / "shopify_seo_audit.json", {})
    if not isinstance(audit_payload, dict) or not audit_payload:
        audit_payload = build_shopify_seo_audit()
    next_category = _next_issue_category(after_category, audit_payload)
    if not next_category:
        return None
    return send_shopify_seo_review_email(
        limit=0,
        force_audit=False,
        review_type="issue_category_batch",
        issue_category=next_category,
        auto_send_next_category=True,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate and email the Shopify SEO review queue.")
    parser.add_argument("--send-email", action="store_true", help="Send the Shopify SEO review email.")
    parser.add_argument("--limit", type=int, default=10, help="Number of SEO fixes to include.")
    parser.add_argument("--force-audit", action="store_true", help="Rebuild the SEO audit before selecting review items.")
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Build a missing-only bulk backfill review that only fills blank SEO fields.",
    )
    parser.add_argument(
        "--send-next-category",
        action="store_true",
        help="Send the next remaining Shopify SEO category batch email.",
    )
    parser.add_argument(
        "--after-category",
        default="",
        help="When sending the next category, start after this category key.",
    )
    parser.add_argument(
        "--issue-category",
        default="",
        help="Build a specific Shopify SEO issue category batch.",
    )
    args = parser.parse_args(argv or sys.argv[1:])

    _ensure_duckagent_python()
    if args.send_next_category:
        payload = send_next_shopify_seo_category_review_email(after_category=args.after_category or None, force_audit=args.force_audit)
        if not payload:
            print(json.dumps({"status": "no_remaining_categories"}, indent=2))
            return 0
    else:
        if args.missing_only:
            review_type = "missing_only_bulk"
        elif args.issue_category:
            review_type = "issue_category_batch"
        else:
            review_type = "top_actions"
        payload = (
            send_shopify_seo_review_email(
                limit=args.limit,
                force_audit=args.force_audit,
                review_type=review_type,
                issue_category=args.issue_category or None,
                auto_send_next_category=review_type == "issue_category_batch",
            )
            if args.send_email
            else build_shopify_seo_review(
                limit=args.limit,
                force_audit=args.force_audit,
                review_type=review_type,
                issue_category=args.issue_category or None,
                auto_send_next_category=review_type == "issue_category_batch",
            )
        )
    print(json.dumps({"run_id": payload["run_id"], "item_count": payload["item_count"], "status": payload["status"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
