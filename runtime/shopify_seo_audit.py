from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
DUCK_AGENT_VENV_PY = DUCK_AGENT_ROOT / ".venv" / "bin" / "python3"
STATE_PATH = DUCK_OPS_ROOT / "state" / "shopify_seo_audit.json"
OUTPUT_MD_PATH = DUCK_OPS_ROOT / "output" / "operator" / "shopify_seo_audit.md"

PRODUCT_FIELDS = """
id
title
handle
status
updatedAt
seo {
  title
  description
}
"""

COLLECTION_FIELDS = """
id
title
handle
updatedAt
seo {
  title
  description
}
"""

PAGE_FIELDS = """
id
title
handle
updatedAt
titleTag: metafield(namespace: "global", key: "title_tag") {
  value
}
descriptionTag: metafield(namespace: "global", key: "description_tag") {
  value
}
"""

ARTICLE_FIELDS = """
id
title
handle
updatedAt
blog {
  title
  handle
}
titleTag: metafield(namespace: "global", key: "title_tag") {
  value
}
descriptionTag: metafield(namespace: "global", key: "description_tag") {
  value
}
"""


def _ensure_shopify_imports():
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        load_dotenv = None

    if load_dotenv is not None:
        load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
    else:
        env_path = DUCK_AGENT_ROOT / ".env"
        if env_path.exists():
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
    from helpers.shopify_helper import shopify_graphql  # type: ignore

    return shopify_graphql


def _ensure_duckagent_python() -> None:
    if os.environ.get("SHOPIFY_SEO_AUDIT_VENV_READY") == "1":
        return
    try:
        current_python = Path(sys.executable).resolve()
    except Exception:
        current_python = Path(sys.executable)
    target_python = DUCK_AGENT_VENV_PY
    if current_python == target_python:
        return
    if not target_python.exists():
        return
    os.environ["SHOPIFY_SEO_AUDIT_VENV_READY"] = "1"
    os.execv(str(target_python), [str(target_python), str(Path(__file__).resolve()), *sys.argv[1:]])


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _normalized_key(value: Any) -> str:
    return _normalize_text(value).lower()


def _seo_values(kind: str, node: dict[str, Any]) -> tuple[str, str, bool, bool]:
    if kind in {"product", "collection"}:
        seo = node.get("seo") if isinstance(node.get("seo"), dict) else {}
        title = _normalize_text(seo.get("title"))
        description = _normalize_text(seo.get("description"))
        return title, description, bool(title), bool(description)

    title_tag = node.get("titleTag") if isinstance(node.get("titleTag"), dict) else {}
    description_tag = node.get("descriptionTag") if isinstance(node.get("descriptionTag"), dict) else {}
    title = _normalize_text(title_tag.get("value"))
    description = _normalize_text(description_tag.get("value"))
    return title, description, bool(title), bool(description)


def _resource_url(kind: str, node: dict[str, Any]) -> str:
    handle = str(node.get("handle") or "").strip()
    if not handle:
        return ""
    if kind == "product":
        return f"/products/{handle}"
    if kind == "collection":
        return f"/collections/{handle}"
    if kind == "page":
        return f"/pages/{handle}"
    if kind == "article":
        blog = node.get("blog") if isinstance(node.get("blog"), dict) else {}
        blog_handle = str(blog.get("handle") or "").strip()
        return f"/blogs/{blog_handle}/{handle}" if blog_handle else f"/blogs/news/{handle}"
    return ""


def _issues_for_resource(kind: str, node: dict[str, Any]) -> list[dict[str, Any]]:
    seo_title, seo_description, has_explicit_title, has_explicit_description = _seo_values(kind, node)
    issues: list[dict[str, Any]] = []

    if not has_explicit_title:
        issues.append({"code": "missing_seo_title", "severity": "high", "message": "Missing SEO title."})
    elif len(seo_title) < 35:
        issues.append({"code": "short_seo_title", "severity": "medium", "message": f"SEO title is short ({len(seo_title)} chars)."})
    elif len(seo_title) > 70:
        issues.append({"code": "long_seo_title", "severity": "medium", "message": f"SEO title is long ({len(seo_title)} chars)."})

    if not has_explicit_description:
        issues.append({"code": "missing_seo_description", "severity": "high", "message": "Missing SEO description."})
    elif len(seo_description) < 70:
        issues.append(
            {
                "code": "short_seo_description",
                "severity": "medium",
                "message": f"SEO description is short ({len(seo_description)} chars).",
            }
        )
    elif len(seo_description) > 165:
        issues.append(
            {
                "code": "long_seo_description",
                "severity": "medium",
                "message": f"SEO description is long ({len(seo_description)} chars).",
            }
        )

    return issues


def _decorate_duplicates(resources: list[dict[str, Any]]) -> None:
    title_map: dict[str, list[str]] = defaultdict(list)
    for resource in resources:
        title_key = _normalized_key(resource.get("seo_title"))
        if title_key:
            title_map[title_key].append(resource["id"])

    for resource in resources:
        title_key = _normalized_key(resource.get("seo_title"))
        if not title_key:
            continue
        duplicates = [rid for rid in title_map[title_key] if rid != resource["id"]]
        if not duplicates:
            continue
        resource["issues"].append(
            {
                "code": "duplicate_seo_title",
                "severity": "medium",
                "message": f"SEO title duplicates {len(duplicates)} other resource(s).",
                "duplicates": duplicates,
            }
        )


def _severity_rank(value: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(value, 3)


def _priority_score(resource: dict[str, Any]) -> tuple[int, int, str]:
    issues = resource.get("issues") if isinstance(resource.get("issues"), list) else []
    high = sum(1 for issue in issues if isinstance(issue, dict) and issue.get("severity") == "high")
    medium = sum(1 for issue in issues if isinstance(issue, dict) and issue.get("severity") == "medium")
    return (-high, -medium, str(resource.get("title") or "").lower())


def _paginate_connection(shopify_graphql, connection_name: str, node_fields: str, *, page_size: int = 100) -> list[dict[str, Any]]:
    query = f"""
    query ShopifySeoAudit($cursor: String) {{
      {connection_name}(first: {page_size}, after: $cursor) {{
        edges {{
          cursor
          node {{
            {node_fields}
          }}
        }}
        pageInfo {{
          hasNextPage
        }}
      }}
    }}
    """
    cursor: str | None = None
    items: list[dict[str, Any]] = []
    while True:
        payload = shopify_graphql(query, {"cursor": cursor})
        connection = (((payload.get("data") or {}).get(connection_name)) or {})
        edges = connection.get("edges") if isinstance(connection.get("edges"), list) else []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if isinstance(node, dict):
                items.append(node)
        page_info = connection.get("pageInfo") if isinstance(connection.get("pageInfo"), dict) else {}
        if not page_info.get("hasNextPage") or not edges:
            break
        cursor = str(edges[-1].get("cursor") or "").strip() or None
        if not cursor:
            break
    return items


def build_shopify_seo_audit() -> dict[str, Any]:
    shopify_graphql = _ensure_shopify_imports()
    generated_at = datetime.now().astimezone().isoformat()

    raw_resources = {
        "product": _paginate_connection(shopify_graphql, "products", PRODUCT_FIELDS),
        "collection": _paginate_connection(shopify_graphql, "collections", COLLECTION_FIELDS),
        "page": _paginate_connection(shopify_graphql, "pages", PAGE_FIELDS),
        "article": _paginate_connection(shopify_graphql, "articles", ARTICLE_FIELDS),
    }

    resources: list[dict[str, Any]] = []
    for kind, nodes in raw_resources.items():
        for node in nodes:
            seo_title, seo_description, _, _ = _seo_values(kind, node)
            resource = {
                "id": str(node.get("id") or ""),
                "kind": kind,
                "title": _normalize_text(node.get("title")),
                "handle": str(node.get("handle") or "").strip(),
                "updated_at": str(node.get("updatedAt") or "").strip(),
                "seo_title": seo_title,
                "seo_description": seo_description,
                "resource_url": _resource_url(kind, node),
                "issues": _issues_for_resource(kind, node),
            }
            if kind == "article":
                blog = node.get("blog") if isinstance(node.get("blog"), dict) else {}
                resource["blog_title"] = str(blog.get("title") or "").strip()
            resources.append(resource)

    _decorate_duplicates(resources)

    actionable = [resource for resource in resources if resource.get("issues")]
    actionable.sort(key=_priority_score)

    by_kind: dict[str, dict[str, int]] = {}
    for kind in ("product", "collection", "page", "article"):
        subset = [resource for resource in resources if resource["kind"] == kind]
        subset_actionable = [resource for resource in subset if resource.get("issues")]
        by_kind[kind] = {
            "total": len(subset),
            "actionable": len(subset_actionable),
            "missing_title": sum(
                1
                for resource in subset
                if any(issue.get("code") == "missing_seo_title" for issue in resource.get("issues", []))
            ),
            "missing_description": sum(
                1
                for resource in subset
                if any(issue.get("code") == "missing_seo_description" for issue in resource.get("issues", []))
            ),
        }

    payload = {
        "generated_at": generated_at,
        "shopify_domain": str(os.getenv("SHOPIFY_DOMAIN") or ""),
        "summary": {
            "total_resources": len(resources),
            "actionable_resources": len(actionable),
            "high_severity_resources": sum(
                1
                for resource in actionable
                if any(issue.get("severity") == "high" for issue in resource.get("issues", []))
            ),
        },
        "by_kind": by_kind,
        "top_actions": actionable[:25],
        "resources": resources,
    }

    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    OUTPUT_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_MD_PATH.write_text(render_shopify_seo_audit_markdown(payload), encoding="utf-8")
    return payload


def render_shopify_seo_audit_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    by_kind = payload.get("by_kind") if isinstance(payload.get("by_kind"), dict) else {}
    top_actions = payload.get("top_actions") if isinstance(payload.get("top_actions"), list) else []

    lines = [
        "# Shopify SEO Audit",
        "",
        f"- Generated at: `{payload.get('generated_at', '')}`",
        f"- Store: `{payload.get('shopify_domain', '')}`",
        f"- Resources scanned: `{summary.get('total_resources', 0)}`",
        f"- Actionable resources: `{summary.get('actionable_resources', 0)}`",
        f"- High-severity resources: `{summary.get('high_severity_resources', 0)}`",
        "",
        "## Coverage",
        "",
    ]

    for kind in ("product", "collection", "page", "article"):
        stats = by_kind.get(kind) if isinstance(by_kind.get(kind), dict) else {}
        lines.append(
            f"- {kind.title()}s: total `{stats.get('total', 0)}` | actionable `{stats.get('actionable', 0)}` | missing title `{stats.get('missing_title', 0)}` | missing description `{stats.get('missing_description', 0)}`"
        )

    lines.extend(["", "## Top Actions", ""])
    if not top_actions:
        lines.append("No actionable SEO issues found.")
        return "\n".join(lines).strip() + "\n"

    for resource in top_actions[:15]:
        title = resource.get("title") or "(untitled)"
        kind = str(resource.get("kind") or "").title()
        path = resource.get("resource_url") or ""
        lines.append(f"- {kind}: {title}")
        if path:
            lines.append(f"  Path: `{path}`")
        issues = resource.get("issues") if isinstance(resource.get("issues"), list) else []
        for issue in sorted(
            [issue for issue in issues if isinstance(issue, dict)],
            key=lambda issue: (_severity_rank(str(issue.get("severity") or "")), str(issue.get("code") or "")),
        ):
            lines.append(f"  - {issue.get('severity', 'unknown')}: {issue.get('message', '')}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main(argv: list[str] | None = None) -> int:
    _ = argv or sys.argv[1:]
    _ensure_duckagent_python()
    payload = build_shopify_seo_audit()
    print(json.dumps({"generated_at": payload["generated_at"], "actionable_resources": payload["summary"]["actionable_resources"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
