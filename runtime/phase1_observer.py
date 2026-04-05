#!/usr/bin/env python3
"""
Phase 1 passive observer for the DuckAgent/OpenClaw sidecar design.

This script:
- scans configured DuckAgent file sources
- connects to the DuckAgent mailbox in read-only mode
- hashes and registers observed artifacts
- builds lightweight normalized records for trend, publish, and customer artifacts
- writes an observation summary to state and digest output

It intentionally does not:
- write into DuckAgent
- send email
- emit evaluator decisions
"""

from __future__ import annotations

import hashlib
import imaplib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.parser import BytesParser
from email.policy import default as email_policy
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
STATE_DIR = ROOT / "state"
OUTPUT_DIR = ROOT / "output"
NORMALIZED_DIR = STATE_DIR / "normalized"
CACHE_ALIAS_PATH = STATE_DIR / "catalog_aliases.json"
DUCKAGENT_RUNS_DIR = Path("/Users/philtullai/ai-agents/duckAgent/runs")

SOURCE_CONFIG_PATH = CONFIG_DIR / "source_observer.json"
REGISTRY_PATH = STATE_DIR / "artifact_registry.jsonl"
OBSERVATION_SUMMARY_PATH = STATE_DIR / "observation_summary.json"
MAILBOX_OBSERVATIONS_PATH = NORMALIZED_DIR / "mailbox_observations.json"

PILOT_PUBLISH_FLOWS = {"newduck", "weekly_sale"}
INITIAL_MAILBOX_BOOTSTRAP_LIMIT = 75
SECONDARY_FOLDER_BOOTSTRAP_LIMIT = 25

RICH_SUBJECT_PATTERN = re.compile(
    r"^MJD:\s*\[(?P<label>[^\]]+)\]\s*(?P<title>.*?)\s*\|\s*FLOW:(?P<flow>[^|]+?)\s*\|\s*RUN:(?P<run_id>[^|]+?)\s*\|\s*ACTION:(?P<action>[^|]+?)\s*$",
    re.IGNORECASE,
)
REVIEW_SUMMARY_SUBJECT_PATTERN = re.compile(
    r"Daily Etsy Review Summary - (?P<run_id>\d{4}-\d{2}-\d{2}) \((?P<story_status>[^)]+)\)",
    re.IGNORECASE,
)
REVIEW_SUMMARY_STAT_PATTERN = re.compile(
    r"- (?P<label>Total Reviews|Average Rating|5-Star Reviews|4-Star Reviews|3-Star Reviews|Low Rating Reviews \(≤2\)):\s*(?P<value>[0-9.]+)",
    re.IGNORECASE,
)
THANK_YOU_MESSAGE_PATTERN = re.compile(
    r"Review\s+(?P<index>\d+):\s+Customer Review:\s+\"(?P<review>.*?)\"\s+Generated Response:\s+(?P<response>.*?)\s+Date:\s+(?P<date>\d{4}-\d{2}-\d{2} [0-9:]+)",
    re.DOTALL,
)
PRIVATE_REVIEW_PATTERN = re.compile(
    r"REVIEW\s+(?P<index>\d+):\s*(?P<rating>\d+)/5 STARS - (?P<date>[0-9:\-\s]+)\s+=+\s+Customer Review:\s+\"(?P<review>.*?)\"\s+📩 PRIVATE MESSAGE TO SEND:\s+-+\s+(?P<response>.*?)\s+-+\s+💡 NEXT STEPS:\s+(?P<next_steps>.*?)(?:\n\nREVIEW BREAKDOWN:|\Z)",
    re.DOTALL,
)
TRANSACTION_ID_PATTERN = re.compile(r"transaction ID:\s*(?P<tx>\d+)", re.IGNORECASE)
RECENT_REVIEW_WINDOW_DAYS = 7


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return text or "unknown"


def trim_text(value: str | None, limit: int = 2000) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=False))
            handle.write("\n")


def load_existing_mailbox_items() -> dict[str, dict[str, Any]]:
    if not MAILBOX_OBSERVATIONS_PATH.exists():
        return {}
    payload = load_json(MAILBOX_OBSERVATIONS_PATH)
    items = payload.get("items", [])
    return {item["registry_key"]: item for item in items if item.get("registry_key")}


def sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def load_registry() -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    if not REGISTRY_PATH.exists():
        return registry
    for line in REGISTRY_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        registry[item["path"]] = item
    return registry


def save_registry(registry: dict[str, dict[str, Any]]) -> None:
    rows = [registry[key] for key in sorted(registry)]
    write_jsonl(REGISTRY_PATH, rows)


def discover_files(config: dict[str, Any]) -> dict[str, list[Path]]:
    discovered: dict[str, list[Path]] = {}
    for source in config.get("sources", []):
        if source.get("type") != "directory":
            continue
        base = Path(source["path"])
        items: list[Path] = []
        for name in source.get("include", []):
            candidate = base / name
            if candidate.exists() and candidate.is_file():
                items.append(candidate)
        for pattern in source.get("include_globs", []):
            items.extend(path for path in base.glob(pattern) if path.is_file())
        discovered[source["id"]] = sorted({path.resolve() for path in items})
    return discovered


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        env[key] = value
    return env


def first_present(env: dict[str, str], keys: list[str]) -> str | None:
    for key in keys:
        value = env.get(key)
        if value:
            return value
    return None


def parse_rich_subject(subject: str) -> dict[str, str]:
    cleaned = " ".join((subject or "").split())
    match = RICH_SUBJECT_PATTERN.match(cleaned)
    if match:
        return {
            "label": match.group("label").strip(),
            "title": match.group("title").strip(),
            "flow": match.group("flow").strip().lower(),
            "run_id": match.group("run_id").strip(),
            "action": match.group("action").strip().lower(),
        }

    data: dict[str, str] = {}
    label_match = re.search(r"\[([^\]]+)\]", cleaned)
    flow_match = re.search(r"FLOW:([^|]+)", cleaned, re.IGNORECASE)
    run_match = re.search(r"RUN:([^|]+)", cleaned, re.IGNORECASE)
    action_match = re.search(r"ACTION:([^|]+)", cleaned, re.IGNORECASE)
    title = cleaned
    if label_match:
        data["label"] = label_match.group(1).strip()
    if flow_match:
        data["flow"] = flow_match.group(1).strip().lower()
    if run_match:
        data["run_id"] = run_match.group(1).strip()
    if action_match:
        data["action"] = action_match.group(1).strip().lower()
    if "| FLOW:" in cleaned:
        title = cleaned.split("| FLOW:", 1)[0]
    if "]" in title:
        title = title.split("]", 1)[1]
    data["title"] = title.replace("MJD:", "").strip()
    return data


def parse_review_summary_subject(subject: str) -> dict[str, Any] | None:
    cleaned = " ".join((subject or "").split())
    match = REVIEW_SUMMARY_SUBJECT_PATTERN.search(cleaned)
    if not match:
        return None
    story_status = match.group("story_status").strip()
    return {
        "run_id": match.group("run_id").strip(),
        "story_status": story_status,
        "story_ready": "story ready" in story_status.lower(),
    }


def resolve_mailbox_folders(config: dict[str, Any], env: dict[str, str]) -> list[str]:
    mailbox_cfg = config.get("mailbox", {})
    raw_folders = mailbox_cfg.get("default_folders") or ["$IMAP_FOLDER", "INBOX"]
    resolved: list[str] = []
    for raw in raw_folders:
        folder = raw
        if raw.startswith("$"):
            folder = env.get(raw[1:], "")
        folder = folder.strip()
        if folder and folder not in resolved:
            resolved.append(folder)
    return resolved


def load_mailbox_settings(config: dict[str, Any]) -> dict[str, Any]:
    mailbox_cfg = config.get("mailbox", {})
    env_file = Path(mailbox_cfg.get("env_file", "/Users/philtullai/ai-agents/duckAgent/.env"))
    env = load_env_file(env_file)
    host = first_present(env, mailbox_cfg.get("host_env_precedence", ["IMAP_HOST", "SMTP_HOST"]))
    user = first_present(env, mailbox_cfg.get("user_env_precedence", ["IMAP_USER", "SMTP_USER"]))
    password = first_present(env, mailbox_cfg.get("password_env_precedence", ["IMAP_PASS", "SMTP_PASS"]))
    port_value = first_present(env, mailbox_cfg.get("port_env_precedence", ["IMAP_PORT"]))
    try:
        ssl_port = int(port_value) if port_value else int(mailbox_cfg.get("default_ssl_port", 993))
    except ValueError:
        ssl_port = 993
    starttls_port = int(mailbox_cfg.get("default_starttls_port", 143))
    folders = resolve_mailbox_folders(config, env)
    bootstrap_limit = int(mailbox_cfg.get("bootstrap_message_limit", INITIAL_MAILBOX_BOOTSTRAP_LIMIT))
    settings = {
        "env_file": str(env_file),
        "host": host,
        "user": user,
        "password": password,
        "ssl_port": ssl_port,
        "starttls_port": starttls_port,
        "folders": folders,
        "bootstrap_limit": bootstrap_limit,
        "enabled": bool(host and user and password and folders),
    }
    if not settings["enabled"]:
        settings["error"] = "missing host, user, password, or folders"
    return settings


def connect_imap(settings: dict[str, Any]) -> tuple[imaplib.IMAP4 | None, str | None, str | None]:
    host = settings.get("host")
    user = settings.get("user")
    password = settings.get("password")
    if not (host and user and password):
        return None, None, settings.get("error") or "mailbox credentials missing"

    attempts = [
        ("imap_ssl", settings.get("ssl_port", 993)),
        ("imap_starttls", settings.get("starttls_port", 143)),
    ]
    errors: list[str] = []
    for mode, port in attempts:
        try:
            if mode == "imap_ssl":
                client = imaplib.IMAP4_SSL(host, port)
            else:
                client = imaplib.IMAP4(host, port)
                if hasattr(client, "starttls"):
                    client.starttls()
            client.login(user, password)
            return client, mode, None
        except Exception as exc:  # pragma: no cover - network variability
            errors.append(f"{mode}@{port}: {exc}")
    return None, None, "; ".join(errors)


def get_uidvalidity(client: imaplib.IMAP4) -> str:
    try:
        response = client.response("UIDVALIDITY")
        if response and response[1]:
            value = response[1][0]
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="ignore")
            return str(value)
    except Exception:
        pass
    return "unknown"


def extract_raw_from_fetch(data: list[Any]) -> bytes:
    for item in data:
        if isinstance(item, tuple) and len(item) >= 2 and isinstance(item[1], (bytes, bytearray)):
            return bytes(item[1])
    return b""


def strip_html(value: str) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\s+", " ", text).strip()


def likely_support_message(subject: str, from_line: str) -> bool:
    haystack = normalize_text(f"{subject} {from_line}")
    signals = {
        "refund",
        "replacement",
        "damaged",
        "broken",
        "shipping",
        "delivery",
        "order",
        "etsy",
        "shopify",
        "support",
        "customer",
    }
    return any(signal in haystack for signal in signals)


def parse_email_bytes(raw_bytes: bytes) -> dict[str, Any]:
    msg = BytesParser(policy=email_policy).parsebytes(raw_bytes)
    body_text = ""
    body_html = ""
    attachments: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            disposition = part.get_content_disposition()
            filename = part.get_filename()
            if disposition == "attachment" and filename:
                attachments.append(filename)
                continue
            content_type = part.get_content_type()
            try:
                content = part.get_content()
            except Exception:
                continue
            if content_type == "text/plain" and not body_text:
                body_text = content
            elif content_type == "text/html" and not body_html:
                body_html = content
    else:
        content_type = msg.get_content_type()
        try:
            content = msg.get_content()
        except Exception:
            content = ""
        if content_type == "text/plain":
            body_text = content
        elif content_type == "text/html":
            body_html = content
    return {
        "message_id": (msg.get("Message-ID") or "").strip(),
        "subject": (msg.get("Subject") or "").strip(),
        "from": (msg.get("From") or "").strip(),
        "to": (msg.get("To") or "").strip(),
        "date": (msg.get("Date") or "").strip(),
        "body_text": (body_text or strip_html(body_html)).strip(),
        "body_html": body_html.strip(),
        "attachments": attachments,
        "raw_size_bytes": len(raw_bytes),
    }


def should_fetch_full_message(parsed: dict[str, Any], subject_data: dict[str, str]) -> bool:
    flow = subject_data.get("flow")
    action = subject_data.get("action")
    if flow in PILOT_PUBLISH_FLOWS and action == "review":
        return True
    if parse_review_summary_subject(parsed.get("subject") or ""):
        return True
    if likely_support_message(parsed.get("subject") or "", parsed.get("from") or ""):
        return True
    return False


def mailbox_registry_key(folder: str, uidvalidity: str, uid: int) -> str:
    safe_folder = slugify(folder.replace("/", "-"))
    return f"mailbox://{safe_folder}/{uidvalidity}/{uid}"


def last_seen_uid_for_folder(registry: dict[str, dict[str, Any]], folder: str, uidvalidity: str) -> int | None:
    matching: list[int] = []
    for item in registry.values():
        if item.get("artifact_type") != "email":
            continue
        if item.get("folder") != folder:
            continue
        if str(item.get("uidvalidity")) != str(uidvalidity):
            continue
        uid = item.get("uid")
        if isinstance(uid, int):
            matching.append(uid)
    if not matching:
        return None
    return max(matching)


@dataclass
class CatalogMatch:
    status: str
    matching_products: list[dict[str, Any]]
    publication_coverage: list[dict[str, Any]]


def normalize_text(value: str) -> str:
    value = value.lower()
    value = re.sub(r"\bwhite\s+tailed\b", "whitetail", value)
    value = re.sub(r"\bwhite\s+tail\b", "whitetail", value)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


STOPWORDS = {
    "the",
    "only",
    "officially",
    "licensed",
    "dashboard",
    "ducking",
    "figure",
    "collectible",
    "collectibles",
    "ornament",
    "vehicle",
    "jeep",
    "style",
    "gift",
    "for",
    "with",
    "and",
    "inspired",
    "inspiredby",
    "inspired-by",
    "3d",
    "printed",
    "rubber",
    "mini",
    "toy",
}
GENERIC_THEME_VALUES = {
    "duck",
    "ducks",
    "rubber duck",
    "rubber ducks",
    "jeep duck",
    "jeep ducks",
}


def extract_theme(raw_title: str) -> str:
    title = normalize_text(raw_title)
    if not title:
        return "unknown"
    tokens = title.split()
    duck_positions = [i for i, token in enumerate(tokens) if token in {"duck", "ducks"}]
    if duck_positions:
        idx = duck_positions[0]
        theme_tokens = []
        for token in tokens[max(0, idx - 3) : idx]:
            if token not in STOPWORDS:
                theme_tokens.append(token)
        if not theme_tokens and idx > 0 and tokens[idx - 1] not in STOPWORDS:
            theme_tokens = [tokens[idx - 1]]
        return " ".join(theme_tokens + ["duck"]).strip()
    filtered = [token for token in tokens[:4] if token not in STOPWORDS]
    return " ".join(filtered[:3]) or title


def is_meaningful_theme(theme: str) -> bool:
    normalized = normalize_text(theme)
    if not normalized or normalized in GENERIC_THEME_VALUES:
        return False
    non_duck_tokens = [token for token in normalized.split() if token not in {"duck", "ducks"}]
    return len(non_duck_tokens) >= 1


def is_duckish(title: str, tags: list[str] | None = None) -> bool:
    title_norm = normalize_text(title)
    if " duck " in f" {title_norm} " or title_norm.endswith(" duck") or " ducks " in f" {title_norm} ":
        return True
    for tag in tags or []:
        tag_norm = normalize_text(tag)
        if "duck" in tag_norm:
            return True
    return False


def load_products_index() -> dict[str, dict[str, Any]]:
    path = Path("/Users/philtullai/ai-agents/duckAgent/cache/products_cache.json")
    if not path.exists():
        return {}
    raw = load_json(path)
    items = raw.get("items", {})
    if not isinstance(items, dict):
        return {}
    alias_payload = load_json(CACHE_ALIAS_PATH)
    alias_records = alias_payload.get("aliases", []) if isinstance(alias_payload, dict) else []
    alias_by_key: dict[str, list[str]] = {}
    for record in alias_records:
        if not isinstance(record, dict):
            continue
        alias_text = str(record.get("theme") or "").strip()
        if not alias_text:
            continue
        product_id = str(record.get("product_id") or "").strip()
        product_handle = str(record.get("product_handle") or "").strip()
        if product_id:
            alias_by_key.setdefault(product_id, []).append(alias_text)
        if product_handle:
            alias_by_key.setdefault(product_handle, []).append(alias_text)
    products: dict[str, dict[str, Any]] = {}
    for pid, item in items.items():
        tags = item.get("tags") or []
        if isinstance(tags, list):
            tags_text = ", ".join(str(tag) for tag in tags)
        else:
            tags_text = str(tags)
        core_terms = item.get("core_terms") or []
        if isinstance(core_terms, list):
            core_terms_text = ", ".join(str(term) for term in core_terms)
        else:
            core_terms_text = str(core_terms)
        concept_variations = item.get("concept_variations") or []
        if isinstance(concept_variations, list):
            concept_variations_text = ", ".join(str(variation) for variation in concept_variations)
        else:
            concept_variations_text = str(concept_variations)
        manual_aliases = alias_by_key.get(str(pid), []) + alias_by_key.get(str(item.get("handle") or ""), [])
        products[str(pid)] = {
            "id": item.get("id"),
            "handle": item.get("handle"),
            "title": item.get("title"),
            "status": item.get("status"),
            "on_sale": item.get("on_sale"),
            "tiktok_publishable": item.get("tiktok_publishable"),
            "category": item.get("category"),
            "ai_theme_category": item.get("ai_theme_category"),
            "tags": tags_text,
            "core_terms": core_terms_text,
            "concept_variations": concept_variations_text,
            "manual_aliases": ", ".join(dict.fromkeys(manual_aliases)),
            "image_src": (item.get("image") or {}).get("src"),
        }
    write_json(NORMALIZED_DIR / "catalog_index.json", {"generated_at": now_iso(), "items": products})
    return products


def load_publications_index() -> dict[str, dict[str, Any]]:
    path = Path("/Users/philtullai/ai-agents/duckAgent/cache/publication_cache.json")
    if not path.exists():
        return {}
    raw = load_json(path)
    publications: dict[str, dict[str, Any]] = {}
    for pid, item in raw.items():
        if not isinstance(item, dict):
            continue
        publications[str(pid)] = {
            "tiktok_publishable": item.get("tiktok_publishable"),
            "publications": item.get("publications", []),
            "cached_at": item.get("cached_at"),
        }
    write_json(NORMALIZED_DIR / "publication_index.json", {"generated_at": now_iso(), "items": publications})
    return publications


def match_catalog(
    theme: str,
    products: dict[str, dict[str, Any]],
    publications: dict[str, dict[str, Any]],
) -> CatalogMatch:
    if not products:
        return CatalogMatch("unknown", [], [])
    theme_tokens = [token for token in normalize_text(theme).split() if token != "duck"]
    if not theme_tokens:
        return CatalogMatch("unknown", [], [])

    exact_matches: list[dict[str, Any]] = []
    partial_matches: list[dict[str, Any]] = []
    for pid, item in products.items():
        haystack = normalize_text(
            " ".join(
                filter(
                    None,
                    [
                        item.get("title") or "",
                        item.get("handle") or "",
                        item.get("category") or "",
                        item.get("ai_theme_category") or "",
                        item.get("tags") or "",
                        item.get("core_terms") or "",
                        item.get("concept_variations") or "",
                        item.get("manual_aliases") or "",
                    ],
                )
            )
        )
        present = [token for token in theme_tokens if token in haystack]
        if len(present) == len(theme_tokens):
            exact_matches.append({"product_id": pid, **item})
        elif len(present) >= max(1, min(2, len(theme_tokens))):
            partial_matches.append({"product_id": pid, **item})

    chosen = exact_matches[:5] or partial_matches[:5]
    coverage = [
        {
            "product_id": item["product_id"],
            "publications": publications.get(item["product_id"], {}).get("publications", []),
            "tiktok_publishable": publications.get(item["product_id"], {}).get("tiktok_publishable"),
        }
        for item in chosen
    ]
    if exact_matches:
        status = "covered"
    elif partial_matches:
        status = "partial"
    else:
        status = "gap"
    return CatalogMatch(status, chosen, coverage)


def merge_trend_candidate(
    candidates: dict[str, dict[str, Any]],
    theme: str,
    source_ref: dict[str, Any],
    signal_summary: dict[str, Any],
    catalog_match: CatalogMatch,
    observed_at: str,
) -> None:
    slug = slugify(theme)
    first_seen_date = observed_at[:10]
    artifact_id = f"trend::{slug}::{first_seen_date}"
    existing = candidates.get(artifact_id)
    if not existing:
        candidates[artifact_id] = {
            "artifact_id": artifact_id,
            "artifact_type": "trend",
            "theme": theme,
            "source_refs": [source_ref],
            "observed_at": observed_at,
            "first_seen_at": observed_at,
            "signal_summary": signal_summary,
            "catalog_match": {
                "status": catalog_match.status,
                "matching_products": catalog_match.matching_products,
                "publication_coverage": catalog_match.publication_coverage,
            },
            "input_confidence_cap": 0.75,
        }
        return

    existing["source_refs"].append(source_ref)
    existing["observed_at"] = max(existing["observed_at"], observed_at)
    source_count = len(existing["source_refs"])
    existing["input_confidence_cap"] = 0.75 if source_count <= 1 else 0.85
    for key, value in signal_summary.items():
        if value is None:
            continue
        old = existing["signal_summary"].get(key)
        if old is None or (isinstance(value, (int, float)) and isinstance(old, (int, float)) and value > old):
            existing["signal_summary"][key] = value
    if existing["catalog_match"]["status"] == "unknown" and catalog_match.status != "unknown":
        existing["catalog_match"] = {
            "status": catalog_match.status,
            "matching_products": catalog_match.matching_products,
            "publication_coverage": catalog_match.publication_coverage,
        }


def normalize_trends(
    products: dict[str, dict[str, Any]],
    publications: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}

    competitor_paths = sorted(Path("/Users/philtullai/ai-agents/duckAgent/runs").glob("*/state_competitor.json"))
    for path in competitor_paths:
        payload = load_json(path)
        report = payload.get("competitor_report", {})
        report_date = report.get("report_date") or path.parent.name
        observed_at = f"{report_date}T00:00:00-04:00"
        for item in report.get("trending_products", []):
            title = item.get("title") or ""
            tags = item.get("tags") or []
            if not is_duckish(title, tags):
                continue
            theme = extract_theme(title)
            if not is_meaningful_theme(theme):
                continue
            catalog_match = match_catalog(theme, products, publications)
            merge_trend_candidate(
                candidates,
                theme=theme,
                source_ref={
                    "path": str(path),
                    "source_type": "state_competitor",
                    "run_id": path.parent.name,
                    "listing_id": item.get("listing_id"),
                },
                signal_summary={
                    "sold_last_7d": item.get("sold_last_7d"),
                    "sold_last_30d": item.get("sold_last_30d"),
                    "engagement_delta_7d": item.get("engagement_delta_7d"),
                    "views_delta_7d": item.get("views_delta_7d"),
                    "favorites_delta_7d": item.get("favorites_delta_7d"),
                    "quantity": item.get("quantity"),
                    "previous_quantity": item.get("previous_quantity"),
                    "delta_source": item.get("delta_source"),
                    "trending_score": item.get("trending_score"),
                },
                catalog_match=catalog_match,
                observed_at=observed_at,
            )

    weekly_path = Path("/Users/philtullai/ai-agents/duckAgent/cache/weekly_insights.json")
    if weekly_path.exists():
        weekly = load_json(weekly_path)
        observed_at = weekly.get("generated_at") or now_iso()
        for section_name in ("top_performers_7d", "top_performers_30d"):
            for item in weekly.get(section_name, [])[:15]:
                title = item.get("title") or ""
                if not is_duckish(title):
                    continue
                theme = extract_theme(title)
                if not is_meaningful_theme(theme):
                    continue
                catalog_match = match_catalog(theme, products, publications)
                merge_trend_candidate(
                    candidates,
                    theme=theme,
                    source_ref={
                        "path": str(weekly_path),
                        "source_type": "weekly_insights",
                        "section": section_name,
                    },
                    signal_summary={
                        "sales_7d": item.get("sales_7d"),
                        "sales_30d": item.get("sales_30d"),
                        "lifetime_sales": item.get("lifetime_sales"),
                    },
                    catalog_match=catalog_match,
                    observed_at=observed_at,
                )

    recs_path = Path("/Users/philtullai/ai-agents/duckAgent/cache/product_recommendations.json")
    if recs_path.exists():
        recs = load_json(recs_path)
        observed_at = recs.get("generated_at") or now_iso()
        for section_name in ("new_product_recommendations", "promotion_opportunities"):
            for item in recs.get(section_name, [])[:15]:
                raw_name = item.get("specific_product") or item.get("product_name") or item.get("product_title") or ""
                if not raw_name:
                    continue
                theme = extract_theme(raw_name)
                if not is_meaningful_theme(theme):
                    continue
                catalog_match = match_catalog(theme, products, publications)
                merge_trend_candidate(
                    candidates,
                    theme=theme,
                    source_ref={
                        "path": str(recs_path),
                        "source_type": "product_recommendations",
                        "section": section_name,
                    },
                    signal_summary={
                        "expected_impact": item.get("expected_impact"),
                        "estimated_effort": item.get("estimated_effort"),
                    },
                    catalog_match=catalog_match,
                    observed_at=observed_at,
                )

    rows = sorted(candidates.values(), key=lambda item: (item["theme"], item["first_seen_at"]))
    write_json(NORMALIZED_DIR / "trend_candidates.json", {"generated_at": now_iso(), "items": rows})
    return rows


def match_related_trends(theme: str, trend_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    theme_tokens = {token for token in normalize_text(theme).split() if token and token != "duck"}
    matches: list[dict[str, Any]] = []
    for candidate in trend_candidates:
        candidate_tokens = {token for token in normalize_text(candidate.get("theme") or "").split() if token and token != "duck"}
        if theme_tokens and theme_tokens.issubset(candidate_tokens):
            matches.append(
                {
                    "artifact_id": candidate.get("artifact_id"),
                    "theme": candidate.get("theme"),
                    "catalog_status": ((candidate.get("catalog_match") or {}).get("status")),
                }
            )
            continue
        if theme_tokens and candidate_tokens and theme_tokens.intersection(candidate_tokens):
            matches.append(
                {
                    "artifact_id": candidate.get("artifact_id"),
                    "theme": candidate.get("theme"),
                    "catalog_status": ((candidate.get("catalog_match") or {}).get("status")),
                }
            )
    return matches[:5]


def build_newduck_candidate_from_email(
    email_item: dict[str, Any],
    products: dict[str, dict[str, Any]],
    publications: dict[str, dict[str, Any]],
    trend_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    subject_data = email_item.get("subject_metadata", {})
    duck_name = subject_data.get("title") or "Unknown Duck"
    theme = extract_theme(duck_name)
    catalog_match = match_catalog(theme, products, publications)
    plain_body = email_item.get("body_text") or ""
    rich_body = email_item.get("body_html_excerpt") or plain_body
    body_text = rich_body if len(plain_body.strip()) < 80 else plain_body
    title_matches = [match.strip() for match in re.findall(r"Title:\s*(.+)", body_text)]
    shopify_title = title_matches[0] if len(title_matches) >= 1 else duck_name
    etsy_title = title_matches[1] if len(title_matches) >= 2 else duck_name
    return {
        "artifact_id": f"publish::newduck::{subject_data.get('run_id', 'unknown')}::{slugify(duck_name)}",
        "artifact_type": "listing",
        "flow": "newduck",
        "run_id": subject_data.get("run_id") or "unknown",
        "source_refs": [
            {
                "path": email_item["registry_key"],
                "source_type": "mailbox_email",
                "folder": email_item.get("folder"),
                "uid": email_item.get("uid"),
                "message_id": email_item.get("message_id"),
                "subject": email_item.get("subject"),
            }
        ],
        "candidate_summary": {
            "title": duck_name,
            "body": trim_text(body_text, 4000),
            "images": email_item.get("attachments", []),
            "platform_targets": ["shopify", "etsy"],
            "platform_variants": {
                "shopify": {
                    "title": shopify_title,
                    "body_excerpt": trim_text(body_text, 1000),
                },
                "etsy": {
                    "title": etsy_title,
                    "description_excerpt": trim_text(body_text, 1000),
                },
            },
            "email_subject": email_item.get("subject"),
        },
        "supporting_context": {
            "brand_family": "duck",
            "catalog_overlap": catalog_match.matching_products,
            "publication_coverage": catalog_match.publication_coverage,
            "trend_refs": match_related_trends(theme, trend_candidates),
        },
        "normalization_notes": {
            "source_mode": "approval_email",
            "completeness": "partial_email",
            "input_confidence_cap": 0.70,
        },
    }


def build_weekly_sale_candidate_from_email(
    email_item: dict[str, Any],
    trend_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    subject_data = email_item.get("subject_metadata", {})
    body_text = email_item.get("body_text") or ""
    publish_token_match = re.search(r"\[PUBLISH:([^\]]+)\]", body_text)
    promotion_refs = [
        {"artifact_id": item["artifact_id"], "theme": item["theme"]}
        for item in trend_candidates[:5]
    ]
    return {
        "artifact_id": f"publish::weekly_sale::{subject_data.get('run_id', 'unknown')}::sale-playbook",
        "artifact_type": "promotion",
        "flow": "weekly_sale",
        "run_id": subject_data.get("run_id") or "unknown",
        "source_refs": [
            {
                "path": email_item["registry_key"],
                "source_type": "mailbox_email",
                "folder": email_item.get("folder"),
                "uid": email_item.get("uid"),
                "message_id": email_item.get("message_id"),
                "subject": email_item.get("subject"),
            }
        ],
        "candidate_summary": {
            "title": "Weekly Sale Playbook",
            "body": trim_text(body_text, 4000),
            "images": email_item.get("attachments", []),
            "platform_targets": ["shopify", "etsy"],
            "publish_token": publish_token_match.group(1) if publish_token_match else None,
            "email_subject": email_item.get("subject"),
        },
        "supporting_context": {
            "brand_family": "sale_playbook",
            "catalog_overlap": [],
            "publication_coverage": [],
            "trend_refs": promotion_refs,
        },
        "normalization_notes": {
            "source_mode": "approval_email",
            "completeness": "partial_email",
            "input_confidence_cap": 0.70,
        },
    }


def parse_review_summary_stats(body_text: str) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    label_map = {
        "Total Reviews": "total_reviews",
        "Average Rating": "average_rating",
        "5-Star Reviews": "five_star_reviews",
        "4-Star Reviews": "four_star_reviews",
        "3-Star Reviews": "three_star_reviews",
        "Low Rating Reviews (≤2)": "low_rating_reviews",
    }
    for match in REVIEW_SUMMARY_STAT_PATTERN.finditer(body_text or ""):
        label = match.group("label")
        key = label_map.get(label)
        if not key:
            continue
        raw = match.group("value")
        try:
            value: Any = float(raw) if "." in raw else int(raw)
        except ValueError:
            value = raw
        stats[key] = value
    return stats


def recent_review_summary_emails(mailbox_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = datetime.now().date() - timedelta(days=RECENT_REVIEW_WINDOW_DAYS)
    by_run_id: dict[str, dict[str, Any]] = {}
    for email_item in mailbox_items:
        review_meta = parse_review_summary_subject(email_item.get("subject") or "")
        if not review_meta:
            continue
        try:
            run_date = datetime.strptime(review_meta["run_id"], "%Y-%m-%d").date()
        except ValueError:
            continue
        if run_date < cutoff:
            continue
        candidate = {**email_item, "review_summary_metadata": review_meta}
        existing = by_run_id.get(review_meta["run_id"])
        current_uid = int(candidate.get("uid") or 0)
        existing_uid = int((existing or {}).get("uid") or 0)
        if existing is None or current_uid > existing_uid:
            by_run_id[review_meta["run_id"]] = candidate
    return [by_run_id[key] for key in sorted(by_run_id)]


def parse_review_story_from_summary(body_text: str) -> dict[str, Any] | None:
    review_match = re.search(r"Selected Review:\s*(?P<review>.+)", body_text or "")
    score_match = re.search(r"AI Score:\s*(?P<score>\d+)/10", body_text or "")
    template_match = re.search(r"Template:\s*(?P<template>[^\n]+)", body_text or "")
    image_match = re.search(r"Image URL:\s*(?P<image>\S+)", body_text or "")
    if not review_match:
        return None
    try:
        ai_score = int(score_match.group("score")) if score_match else None
    except ValueError:
        ai_score = None
    return {
        "selected_review": review_match.group("review").strip(),
        "ai_score": ai_score,
        "template_id": template_match.group("template").strip() if template_match else None,
        "image_url": image_match.group("image").strip() if image_match else None,
    }


def parse_positive_review_replies(body_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for match in THANK_YOU_MESSAGE_PATTERN.finditer(body_text or ""):
        rows.append(
            {
                "index": int(match.group("index")),
                "customer_review": match.group("review").strip(),
                "generated_response": match.group("response").strip(),
                "review_date": match.group("date").strip(),
            }
        )
    return rows


def parse_private_review_replies(body_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for match in PRIVATE_REVIEW_PATTERN.finditer(body_text or ""):
        next_steps = match.group("next_steps").strip()
        tx_match = TRANSACTION_ID_PATTERN.search(next_steps)
        rows.append(
            {
                "index": int(match.group("index")),
                "rating": int(match.group("rating")),
                "review_date": match.group("date").strip(),
                "customer_review": match.group("review").strip(),
                "generated_response": match.group("response").strip(),
                "next_steps": next_steps,
                "transaction_id": tx_match.group("tx") if tx_match else None,
            }
        )
    return rows


def parse_review_summary_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), pattern)
        except ValueError:
            continue
    return None


def review_state_path(run_id: str) -> Path:
    return DUCKAGENT_RUNS_DIR / run_id / "state_reviews.json"


def load_reviews_state(run_id: str) -> dict[str, Any] | None:
    path = review_state_path(run_id)
    if not path.exists():
        return None
    try:
        payload = load_json(path)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def review_datetime_string(review: dict[str, Any]) -> str | None:
    timestamp = review.get("create_timestamp") or review.get("created_timestamp")
    if not timestamp:
        return None
    try:
        return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def resolve_review_target(run_id: str, reply: dict[str, Any], artifact_slug: str) -> dict[str, Any]:
    target = {
        "shop_id": None,
        "review_key": artifact_slug,
        "review_id": None,
        "transaction_id": reply.get("transaction_id"),
        "listing_id": None,
        "review_url": None,
        "buyer_user_id": None,
        "review_text": trim_text(reply.get("customer_review"), 400),
        "review_date": reply.get("review_date"),
        "match_quality": "missing",
        "source_state_path": str(review_state_path(run_id)),
    }

    state = load_reviews_state(run_id)
    if not state:
        return target

    reviews = state.get("reviews_data") or []
    reply_text = normalize_text(reply.get("customer_review") or "")
    reply_date = parse_review_summary_datetime(reply.get("review_date"))
    reply_rating = reply.get("rating", 5)

    best_review: dict[str, Any] | None = None
    best_score = -1
    best_quality = "missing"

    for review in reviews:
        score = 0
        review_text = normalize_text(review.get("review") or "")
        if reply_text and review_text == reply_text:
            score += 4
        elif reply_text and reply_text in review_text:
            score += 2

        if reply_rating is not None and review.get("rating") == reply_rating:
            score += 1

        if target["transaction_id"] and str(review.get("transaction_id") or "") == str(target["transaction_id"]):
            score += 4

        review_date_str = review_datetime_string(review)
        if reply.get("review_date") and review_date_str == reply.get("review_date"):
            score += 3
        elif reply_date is not None and review_date_str:
            review_dt = parse_review_summary_datetime(review_date_str)
            if review_dt and abs((review_dt - reply_date).total_seconds()) <= 120:
                score += 2

        if score > best_score:
            best_review = review
            best_score = score

    if best_review is None or best_score < 3:
        return target

    if best_score >= 8:
        best_quality = "exact"
    elif best_score >= 5:
        best_quality = "strong"
    else:
        best_quality = "weak"

    target.update(
        {
            "shop_id": best_review.get("shop_id"),
            "transaction_id": best_review.get("transaction_id") or target.get("transaction_id"),
            "listing_id": best_review.get("listing_id"),
            "buyer_user_id": best_review.get("buyer_user_id"),
            "review_text": trim_text(best_review.get("review"), 400),
            "review_date": review_datetime_string(best_review) or target.get("review_date"),
            "match_quality": best_quality,
        }
    )
    return target


def build_reviews_story_candidate_from_email(email_item: dict[str, Any]) -> dict[str, Any] | None:
    review_meta = email_item.get("review_summary_metadata") or parse_review_summary_subject(email_item.get("subject") or "")
    if not review_meta or not review_meta.get("story_ready"):
        return None
    body_text = email_item.get("body_text") or ""
    story = parse_review_story_from_summary(body_text)
    if not story:
        return None
    stats = parse_review_summary_stats(body_text)
    return {
        "artifact_id": f"publish::reviews_story::{review_meta['run_id']}::review-story",
        "artifact_type": "social_post",
        "flow": "reviews_story",
        "run_id": review_meta["run_id"],
        "source_refs": [
            {
                "path": email_item["registry_key"],
                "source_type": "mailbox_email",
                "folder": email_item.get("folder"),
                "uid": email_item.get("uid"),
                "message_id": email_item.get("message_id"),
                "subject": email_item.get("subject"),
            }
        ],
        "candidate_summary": {
            "title": f"Etsy Review Story {review_meta['run_id']}",
            "body": trim_text(story["selected_review"], 1500),
            "images": [story["image_url"]] if story.get("image_url") else [],
            "platform_targets": ["instagram_story", "facebook_story"],
            "email_subject": email_item.get("subject"),
            "selected_review": story["selected_review"],
            "story_ai_score": story.get("ai_score"),
            "template_id": story.get("template_id"),
            "story_status": review_meta.get("story_status"),
        },
        "supporting_context": {
            "brand_family": "reviews_story",
            "catalog_overlap": [],
            "publication_coverage": [],
            "trend_refs": [],
            "review_stats": stats,
        },
        "normalization_notes": {
            "source_mode": "review_summary_email",
            "completeness": "medium",
            "input_confidence_cap": 0.75,
        },
    }


def build_review_reply_candidate_from_email(
    email_item: dict[str, Any],
    run_id: str,
    reply: dict[str, Any],
    flow: str,
    artifact_slug: str,
    platform_target: str,
    response_kind: str,
    confidence_cap: float,
) -> dict[str, Any]:
    body_text = email_item.get("body_text") or ""
    stats = parse_review_summary_stats(body_text)
    review_target = resolve_review_target(run_id, reply, artifact_slug)
    return {
        "artifact_id": f"publish::{flow}::{run_id}::{artifact_slug}",
        "artifact_type": "review_reply",
        "flow": flow,
        "run_id": run_id,
        "review_target": review_target,
        "source_refs": [
            {
                "path": email_item["registry_key"],
                "source_type": "mailbox_email",
                "folder": email_item.get("folder"),
                "uid": email_item.get("uid"),
                "message_id": email_item.get("message_id"),
                "subject": email_item.get("subject"),
            }
        ],
        "candidate_summary": {
            "title": f"Etsy Review Reply {run_id} #{reply['index']}",
            "body": trim_text(reply.get("generated_response"), 2500),
            "images": [],
            "platform_targets": [platform_target],
            "email_subject": email_item.get("subject"),
            "customer_review": trim_text(reply.get("customer_review"), 1000),
            "review_date": reply.get("review_date"),
            "review_rating": reply.get("rating", 5 if flow == "reviews_reply_positive" else None),
            "response_kind": response_kind,
            "transaction_id": reply.get("transaction_id"),
            "next_steps": trim_text(reply.get("next_steps"), 800) if reply.get("next_steps") else None,
        },
        "supporting_context": {
            "brand_family": "reviews_reply",
            "catalog_overlap": [],
            "publication_coverage": [],
            "trend_refs": [],
            "review_stats": stats,
        },
        "normalization_notes": {
            "source_mode": "review_summary_email",
            "completeness": "medium",
            "input_confidence_cap": confidence_cap,
            "review_target_match_quality": review_target.get("match_quality"),
        },
    }


def merge_publish_candidate(candidates: dict[str, dict[str, Any]], candidate: dict[str, Any]) -> None:
    existing = candidates.get(candidate["artifact_id"])
    if not existing:
        candidates[candidate["artifact_id"]] = candidate
        return

    existing["source_refs"].extend(candidate.get("source_refs", []))
    existing_notes = existing.setdefault("normalization_notes", {})
    candidate_notes = candidate.get("normalization_notes", {})
    if existing_notes.get("source_mode") == "approval_email" and candidate_notes.get("source_mode") == "state_file":
        existing["candidate_summary"] = candidate["candidate_summary"]
        existing["supporting_context"] = candidate["supporting_context"]
        existing["normalization_notes"] = candidate_notes
    elif not existing.get("candidate_summary", {}).get("body") and candidate.get("candidate_summary", {}).get("body"):
        existing["candidate_summary"] = candidate["candidate_summary"]


def normalize_publish_candidates(
    mailbox_items: list[dict[str, Any]],
    trend_candidates: list[dict[str, Any]],
    products: dict[str, dict[str, Any]],
    publications: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}

    for path in sorted(Path("/Users/philtullai/ai-agents/duckAgent/runs").glob("*/state_newduck.json")):
        payload = load_json(path)
        data = payload.get("newduck") or {}
        if not data:
            continue
        duck_name = data.get("duck_name") or path.parent.name
        theme = extract_theme(duck_name)
        catalog_match = match_catalog(theme, products, publications)
        merge_publish_candidate(
            candidates,
            {
                "artifact_id": f"publish::newduck::{path.parent.name}::{slugify(duck_name)}",
                "artifact_type": "listing",
                "flow": "newduck",
                "run_id": path.parent.name,
                "source_refs": [{"path": str(path), "source_type": "state_newduck"}],
                "candidate_summary": {
                    "title": ((data.get("copy") or {}).get("shopify") or {}).get("title"),
                    "body": ((data.get("copy") or {}).get("shopify") or {}).get("body_html"),
                    "images": [image.get("path") for image in data.get("images", [])],
                    "platform_targets": ["shopify", "etsy"],
                    "platform_variants": {
                        "shopify": (data.get("copy") or {}).get("shopify"),
                        "etsy": (data.get("copy") or {}).get("etsy"),
                    },
                },
                "supporting_context": {
                    "brand_family": "duck",
                    "catalog_overlap": catalog_match.matching_products,
                    "publication_coverage": catalog_match.publication_coverage,
                    "trend_refs": match_related_trends(theme, trend_candidates),
                },
                "normalization_notes": {
                    "source_mode": "state_file",
                    "completeness": "high",
                    "input_confidence_cap": 0.85,
                },
            },
        )

    for path in sorted(Path("/Users/philtullai/ai-agents/duckAgent/runs").glob("*/state_weekly.json")):
        payload = load_json(path)
        playbook = payload.get("sale_playbook") or {}
        if not playbook:
            continue
        merge_publish_candidate(
            candidates,
            {
                "artifact_id": f"publish::weekly_sale::{path.parent.name}::sale-playbook",
                "artifact_type": "promotion",
                "flow": "weekly_sale",
                "run_id": path.parent.name,
                "source_refs": [{"path": str(path), "source_type": "state_weekly"}],
                "candidate_summary": {
                    "title": "Weekly Sale Playbook",
                    "body": playbook.get("strategic_summary"),
                    "images": [],
                    "platform_targets": ["shopify", "etsy"],
                    "publish_token": path.parent.name,
                },
                "supporting_context": {
                    "brand_family": "sale_playbook",
                    "catalog_overlap": [],
                    "publication_coverage": [],
                    "trend_refs": [
                        {"artifact_id": item["artifact_id"], "theme": item["theme"]}
                        for item in trend_candidates[:5]
                    ],
                },
                "normalization_notes": {
                    "source_mode": "state_file",
                    "completeness": "high",
                    "input_confidence_cap": 0.85,
                },
            },
        )

    for email_item in mailbox_items:
        subject_data = email_item.get("subject_metadata", {})
        flow = subject_data.get("flow")
        action = subject_data.get("action")
        if flow not in PILOT_PUBLISH_FLOWS or action != "review":
            continue
        if flow == "newduck":
            merge_publish_candidate(
                candidates,
                build_newduck_candidate_from_email(email_item, products, publications, trend_candidates),
            )
        elif flow == "weekly_sale":
            merge_publish_candidate(
                candidates,
                build_weekly_sale_candidate_from_email(email_item, trend_candidates),
            )

    for email_item in recent_review_summary_emails(mailbox_items):
        review_meta = email_item.get("review_summary_metadata") or {}
        run_id = review_meta.get("run_id")
        if not run_id:
            continue
        story_candidate = build_reviews_story_candidate_from_email(email_item)
        if story_candidate:
            merge_publish_candidate(candidates, story_candidate)

        body_text = email_item.get("body_text") or ""
        for reply in parse_positive_review_replies(body_text):
            merge_publish_candidate(
                candidates,
                build_review_reply_candidate_from_email(
                    email_item=email_item,
                    run_id=run_id,
                    reply=reply,
                    flow="reviews_reply_positive",
                    artifact_slug=f"review-{reply['index']}",
                    platform_target="etsy_public_review",
                    response_kind="public_thank_you",
                    confidence_cap=0.76,
                ),
            )
        for reply in parse_private_review_replies(body_text):
            slug = f"transaction-{reply['transaction_id']}" if reply.get("transaction_id") else f"review-{reply['index']}"
            merge_publish_candidate(
                candidates,
                build_review_reply_candidate_from_email(
                    email_item=email_item,
                    run_id=run_id,
                    reply=reply,
                    flow="reviews_reply_private",
                    artifact_slug=slug,
                    platform_target="etsy_private_message",
                    response_kind="private_recovery",
                    confidence_cap=0.72,
                ),
            )

    rows = sorted(candidates.values(), key=lambda item: (item["flow"], item["run_id"], item["artifact_id"]))
    write_json(NORMALIZED_DIR / "publish_candidates.json", {"generated_at": now_iso(), "items": rows})
    return rows


def infer_issue_type(review: dict[str, Any]) -> str:
    rating = review.get("rating")
    text = normalize_text(review.get("review") or "")
    if rating is not None and rating <= 2 and any(term in text for term in ("late", "shipping", "arrived", "delivery")):
        return "shipping"
    if rating is not None and rating <= 2 and any(term in text for term in ("broken", "chip", "damaged", "quality")):
        return "quality"
    if rating is not None and rating <= 2:
        return "refund_request"
    return "unknown"


def normalize_customer_signals(mailbox_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(Path("/Users/philtullai/ai-agents/duckAgent/runs").glob("*/state_reviews.json")):
        payload = load_json(path)
        for review in payload.get("reviews_data", []):
            tx = review.get("transaction_id") or review.get("listing_id") or review.get("create_timestamp")
            artifact_id = f"customer::etsy_review::{tx}"
            rating = review.get("rating")
            sentiment = "negative" if rating is not None and rating <= 3 else "positive"
            event_time = review.get("create_timestamp")
            iso_event = (
                datetime.fromtimestamp(event_time, tz=timezone.utc).astimezone().isoformat()
                if event_time
                else None
            )
            rows.append(
                {
                    "artifact_id": artifact_id,
                    "artifact_type": "customer",
                    "channel": "etsy_review",
                    "source_refs": [{"path": str(path), "run_id": path.parent.name}],
                    "customer_event": {
                        "event_type": "review",
                        "rating": rating,
                        "sentiment": sentiment,
                        "customer_text": review.get("review"),
                        "event_time": iso_event,
                    },
                    "business_context": {
                        "order_id": review.get("transaction_id"),
                        "product_title": None,
                        "issue_type": infer_issue_type(review),
                        "allowed_remedies": ["refund", "replacement", "apology"],
                    },
                    "normalization_notes": {
                        "source_mode": "state_reviews",
                        "input_confidence_cap": 0.85,
                    },
                }
            )

    for email_item in mailbox_items:
        subject_data = email_item.get("subject_metadata", {})
        if subject_data.get("flow"):
            continue
        body_text = normalize_text(email_item.get("body_text") or "")
        subject = email_item.get("subject") or ""
        from_line = (email_item.get("from") or "").lower()
        support_signals = [
            "refund",
            "replacement",
            "damaged",
            "broken",
            "late",
            "shipping",
            "delivery",
            "etsy",
            "shopify",
            "order",
        ]
        if not any(token in body_text or token in subject.lower() or token in from_line for token in support_signals):
            continue
        artifact_id = f"customer::mail::{email_item['uid']}"
        rows.append(
            {
                "artifact_id": artifact_id,
                "artifact_type": "customer",
                "channel": "mailbox_email",
                "source_refs": [
                    {
                        "path": email_item["registry_key"],
                        "folder": email_item.get("folder"),
                        "uid": email_item.get("uid"),
                        "message_id": email_item.get("message_id"),
                    }
                ],
                "customer_event": {
                    "event_type": "email",
                    "rating": None,
                    "sentiment": "unknown",
                    "customer_text": trim_text(email_item.get("body_text"), 1500),
                    "event_time": email_item.get("date"),
                },
                "business_context": {
                    "order_id": None,
                    "product_title": None,
                    "issue_type": "email_support",
                    "allowed_remedies": ["reply", "refund", "replacement", "escalation"],
                },
                "normalization_notes": {
                    "source_mode": "mailbox_email",
                    "input_confidence_cap": 0.70,
                },
            }
        )

    write_json(NORMALIZED_DIR / "customer_signals.json", {"generated_at": now_iso(), "items": rows})
    return rows


def observe_mailbox(
    config: dict[str, Any],
    registry: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    settings = load_mailbox_settings(config)
    existing_items = load_existing_mailbox_items()
    merged_items = dict(existing_items)
    mailbox_summary: dict[str, Any] = {
        "status": "not_configured",
        "folders_requested": settings.get("folders", []),
        "artifacts_total": 0,
        "new": 0,
        "changed": 0,
        "unchanged": 0,
        "errors": [],
        "bootstrap_message_limit": settings.get("bootstrap_limit"),
    }
    if not settings.get("enabled"):
        mailbox_summary["error"] = settings.get("error")
        return [], mailbox_summary

    client, transport, error = connect_imap(settings)
    if client is None:
        mailbox_summary["status"] = "connection_failed"
        mailbox_summary["error"] = error
        return [], mailbox_summary

    force_bootstrap = not existing_items
    folder_counts: dict[str, int] = {}
    try:
        for folder in settings["folders"]:
            try:
                typ, _ = client.select(folder, readonly=True)
                if typ != "OK":
                    mailbox_summary["errors"].append(f"select failed for {folder}")
                    continue
                uidvalidity = get_uidvalidity(client)
                prior_last_uid = last_seen_uid_for_folder(registry, folder, uidvalidity)
                typ, data = client.uid("search", None, "ALL")
                if typ != "OK":
                    mailbox_summary["errors"].append(f"search failed for {folder}")
                    continue
                all_uids = [int(uid) for uid in data[0].split() if uid]
                if prior_last_uid is None or force_bootstrap:
                    bootstrap_limit = settings["bootstrap_limit"]
                    if folder.upper() == "INBOX" and len(settings["folders"]) > 1:
                        bootstrap_limit = min(bootstrap_limit, SECONDARY_FOLDER_BOOTSTRAP_LIMIT)
                    candidate_uids = all_uids[-bootstrap_limit:]
                else:
                    candidate_uids = [uid for uid in all_uids if uid > prior_last_uid]
                folder_counts[folder] = len(candidate_uids)
                for uid in candidate_uids:
                    typ, fetched = client.uid(
                        "fetch",
                        str(uid),
                        "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID SUBJECT FROM TO DATE)])",
                    )
                    if typ != "OK":
                        mailbox_summary["errors"].append(f"fetch failed for {folder}:{uid}")
                        continue
                    header_raw = extract_raw_from_fetch(fetched)
                    if not header_raw:
                        continue
                    parsed = parse_email_bytes(header_raw)
                    subject_data = parse_rich_subject(parsed.get("subject") or "")
                    raw = header_raw
                    payload_mode = "header_only"
                    if should_fetch_full_message(parsed, subject_data):
                        # Keep mailbox observation read-only on providers that may
                        # treat RFC822 fetches as a read. BODY.PEEK[] returns the
                        # full message content without intentionally setting \Seen.
                        typ_full, fetched_full = client.uid("fetch", str(uid), "(BODY.PEEK[])")
                        if typ_full == "OK":
                            full_raw = extract_raw_from_fetch(fetched_full)
                            if full_raw:
                                raw = full_raw
                                parsed = parse_email_bytes(full_raw)
                                payload_mode = "full_message"
                    content_hash = sha256_bytes(raw)
                    path_key = mailbox_registry_key(folder, uidvalidity, uid)
                    previous = registry.get(path_key)
                    if previous is None:
                        status = "new"
                        first_seen_at = now_iso()
                    elif previous.get("content_hash") != content_hash:
                        status = "changed"
                        first_seen_at = previous.get("first_seen_at") or now_iso()
                    else:
                        status = "unchanged"
                        first_seen_at = previous.get("first_seen_at") or now_iso()
                    registry[path_key] = {
                        "path": path_key,
                        "source_id": "duckagent_mailbox",
                        "artifact_type": "email",
                        "folder": folder,
                        "uid": uid,
                        "uidvalidity": uidvalidity,
                        "content_hash": content_hash,
                        "message_id": parsed.get("message_id"),
                        "subject": parsed.get("subject"),
                        "payload_mode": payload_mode,
                        "size_bytes": parsed.get("raw_size_bytes"),
                        "first_seen_at": first_seen_at,
                        "last_seen_at": now_iso(),
                        "last_status": status,
                    }
                    mailbox_summary[status] += 1
                    merged_items[path_key] = {
                        "registry_key": path_key,
                        "source_id": "duckagent_mailbox",
                        "folder": folder,
                        "uid": uid,
                        "uidvalidity": uidvalidity,
                        "message_id": parsed.get("message_id"),
                        "subject": parsed.get("subject"),
                        "subject_metadata": subject_data,
                        "from": parsed.get("from"),
                        "to": parsed.get("to"),
                        "date": parsed.get("date"),
                        "payload_mode": payload_mode,
                        "body_text": parsed.get("body_text"),
                        "body_html_excerpt": trim_text(strip_html(parsed.get("body_html")), 1200),
                        "attachments": parsed.get("attachments", []),
                        "registry_status": status,
                    }
            except Exception as exc:  # pragma: no cover - network variability
                mailbox_summary["errors"].append(f"{folder}: {exc}")
    finally:
        try:
            client.logout()
        except Exception:
            pass

    mailbox_summary["status"] = "ok"
    mailbox_summary["transport"] = transport
    items = sorted(
        merged_items.values(),
        key=lambda item: (
            item.get("folder") or "",
            int(item.get("uid") or 0),
        ),
    )
    mailbox_summary["artifacts_total"] = len(items)
    mailbox_summary["folder_counts"] = folder_counts
    write_json(
        MAILBOX_OBSERVATIONS_PATH,
        {
            "generated_at": now_iso(),
            "mailbox_status": mailbox_summary["status"],
            "folders": settings["folders"],
            "items": items,
        },
    )
    return items, mailbox_summary


def write_observation_digest(summary: dict[str, Any]) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%dT%H%M%S")
    json_path = OUTPUT_DIR / "digests" / f"observation_summary__{timestamp}.json"
    md_path = OUTPUT_DIR / "digests" / f"observation_summary__{timestamp}.md"
    write_json(json_path, summary)
    md = [
        "# Phase 1 Observation Summary",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Observed file artifacts: `{summary['artifact_counts']['files_total']}`",
        f"- New files: `{summary['artifact_counts']['files_new']}`",
        f"- Changed files: `{summary['artifact_counts']['files_changed']}`",
        f"- Unchanged files: `{summary['artifact_counts']['files_unchanged']}`",
        f"- Observed mailbox artifacts: `{summary['artifact_counts']['emails_total']}`",
        f"- New emails: `{summary['artifact_counts']['emails_new']}`",
        f"- Changed emails: `{summary['artifact_counts']['emails_changed']}`",
        f"- Unchanged emails: `{summary['artifact_counts']['emails_unchanged']}`",
        f"- Trend candidates: `{summary['normalized_counts']['trend_candidates']}`",
        f"- Publish candidates: `{summary['normalized_counts']['publish_candidates']}`",
        f"- Customer signals: `{summary['normalized_counts']['customer_signals']}`",
        f"- Mailbox status: `{summary['mailbox_status']}`",
    ]
    if summary.get("mailbox", {}).get("transport"):
        md.append(f"- Mailbox transport: `{summary['mailbox']['transport']}`")
    folder_counts = summary.get("mailbox", {}).get("folder_counts", {})
    if folder_counts:
        md.append("- Mailbox folder counts:")
        for folder, count in folder_counts.items():
            md.append(f"  - `{folder}`: `{count}` fetched")
    if summary.get("mailbox", {}).get("errors"):
        md.append("- Mailbox errors:")
        for error in summary["mailbox"]["errors"]:
            md.append(f"  - {error}")
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")


def main() -> int:
    NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)
    config = load_json(SOURCE_CONFIG_PATH)
    registry = load_registry()
    discovered = discover_files(config)

    file_changed_counts = defaultdict(int)
    for source_id, files in discovered.items():
        for path in files:
            stat = path.stat()
            path_key = str(path)
            content_hash = sha256_file(path)
            previous = registry.get(path_key)
            if previous is None:
                status = "new"
                first_seen_at = now_iso()
            elif previous.get("content_hash") != content_hash:
                status = "changed"
                first_seen_at = previous.get("first_seen_at") or now_iso()
            else:
                status = "unchanged"
                first_seen_at = previous.get("first_seen_at") or now_iso()

            registry[path_key] = {
                "path": path_key,
                "source_id": source_id,
                "artifact_type": "file",
                "size_bytes": stat.st_size,
                "mtime_epoch": stat.st_mtime,
                "content_hash": content_hash,
                "first_seen_at": first_seen_at,
                "last_seen_at": now_iso(),
                "last_status": status,
            }
            file_changed_counts[status] += 1

    mailbox_items, mailbox_summary = observe_mailbox(config, registry)
    save_registry(registry)

    products = load_products_index()
    publications = load_publications_index()
    trend_candidates = normalize_trends(products, publications)
    publish_candidates = normalize_publish_candidates(mailbox_items, trend_candidates, products, publications)
    customer_signals = normalize_customer_signals(mailbox_items)

    summary = {
        "generated_at": now_iso(),
        "mailbox_status": mailbox_summary["status"],
        "artifact_counts": {
            "files_total": sum(len(paths) for paths in discovered.values()),
            "files_new": file_changed_counts["new"],
            "files_changed": file_changed_counts["changed"],
            "files_unchanged": file_changed_counts["unchanged"],
            "emails_total": mailbox_summary.get("artifacts_total", 0),
            "emails_new": mailbox_summary.get("new", 0),
            "emails_changed": mailbox_summary.get("changed", 0),
            "emails_unchanged": mailbox_summary.get("unchanged", 0),
        },
        "source_counts": {
            **{source_id: len(paths) for source_id, paths in discovered.items()},
            "duckagent_mailbox": mailbox_summary.get("artifacts_total", 0),
        },
        "mailbox": mailbox_summary,
        "normalized_counts": {
            "trend_candidates": len(trend_candidates),
            "publish_candidates": len(publish_candidates),
            "customer_signals": len(customer_signals),
        },
    }
    write_json(OBSERVATION_SUMMARY_PATH, summary)
    write_observation_digest(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
