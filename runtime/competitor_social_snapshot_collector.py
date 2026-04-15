from __future__ import annotations

import argparse
import html
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, load_json, now_local_iso, write_json, write_markdown


CONFIG_PATH = DUCK_OPS_ROOT / "config" / "competitor_social_sources.json"
STATE_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshots.json"
HISTORY_PATH = DUCK_OPS_ROOT / "state" / "competitor_social_snapshot_history.json"
OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "competitor_social_snapshots.json"
OUTPUT_MD_PATH = OUTPUT_OPERATOR_DIR / "competitor_social_snapshots.md"

IG_APP_ID = "936619743392459"
REQUEST_TIMEOUT = 20
REQUEST_RETRY_DELAYS = (0.4, 1.0)
RETRYABLE_HTTP_CODES = {401, 408, 409, 425, 429, 500, 502, 503, 504}
INTER_ACCOUNT_DELAY_SECONDS = 0.5
PROFILE_RATE_LIMIT_CIRCUIT_BREAKER_THRESHOLD = 2

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9']+")
HASHTAG_PATTERN = re.compile(r"(?<!\w)#([A-Za-z0-9_]+)")
DUCK_FAMILY_PATTERN = re.compile(r"\b([A-Za-z0-9'&-]+(?:\s+[A-Za-z0-9'&-]+){0,2})\s+duck\b", re.IGNORECASE)
META_CONTENT_RE = re.compile(r'<meta[^>]+content="([^"]+)"[^>]+(?:name|property)="([^"]+)"', re.IGNORECASE)
DESCRIPTION_COUNTS_RE = re.compile(
    r"(?P<followers>[\d.,]+[KMBkmb]?)\s+Followers,\s+"
    r"(?P<following>[\d.,]+[KMBkmb]?)\s+Following,\s+"
    r"(?P<posts>[\d.,]+[KMBkmb]?)\s+Posts",
    re.IGNORECASE,
)
DESCRIPTION_NAME_RE = re.compile(r"Posts\s+-\s+(?P<name>.*?)\s+\(@", re.IGNORECASE)
TITLE_NAME_RE = re.compile(r"^(?P<name>.*?)\s+\(@", re.IGNORECASE)

STOPWORDS = {
    "3d",
    "printed",
    "print",
    "duck",
    "ducks",
    "ducking",
    "with",
    "from",
    "that",
    "this",
    "your",
    "they",
    "them",
    "just",
    "into",
    "have",
    "has",
    "more",
    "best",
    "shop",
    "etsy",
    "link",
    "bio",
    "gift",
    "gifts",
    "handmade",
    "collectible",
    "collectibles",
    "custom",
    "modern",
    "keepsakes",
    "hearts",
    "fun",
}

THEME_KEYWORDS = {
    "wedding": {"wedding", "bride", "groom", "bridal", "engagement"},
    "music": {"music", "showgirl", "era", "concert", "pop", "singer"},
    "cruise": {"cruise", "cruising", "ship", "vacation"},
    "offroad": {"offroad", "trail", "mud", "jeepish", "4x4"},
    "decor": {"decor", "shelf", "display", "home"},
    "dashboard": {"dashboard", "dash", "car", "auto"},
    "holiday": {"christmas", "holiday", "easter", "halloween", "spooky"},
    "space": {"space", "astronaut", "galaxy", "rocket", "planet"},
    "giftable": {"gift", "giftable", "present"},
}


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def _safe_count_int(value: Any) -> int | None:
    text = _compact_text(value)
    if not text:
        return None
    normalized = text.replace(",", "")
    multiplier = 1
    if normalized[-1:].lower() == "k":
        multiplier = 1_000
        normalized = normalized[:-1]
    elif normalized[-1:].lower() == "m":
        multiplier = 1_000_000
        normalized = normalized[:-1]
    elif normalized[-1:].lower() == "b":
        multiplier = 1_000_000_000
        normalized = normalized[:-1]
    try:
        return int(float(normalized) * multiplier)
    except Exception:
        return None


def _history_snapshots() -> list[dict[str, Any]]:
    payload = load_json(HISTORY_PATH, {})
    if isinstance(payload, dict):
        items = payload.get("snapshots")
        return list(items) if isinstance(items, list) else []
    if isinstance(payload, list):
        return payload
    return []


def _save_history(snapshots: list[dict[str, Any]]) -> None:
    write_json(
        HISTORY_PATH,
        {
            "generated_at": now_local_iso(),
            "snapshot_count": len(snapshots),
            "snapshots": snapshots,
        },
    )


def _load_existing_snapshot() -> dict[str, Any]:
    payload = load_json(STATE_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _load_config() -> dict[str, Any]:
    payload = load_json(CONFIG_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _request_json(url: str, *, referer_handle: str | None = None) -> dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-IG-App-ID": IG_APP_ID,
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer_handle:
        headers["Referer"] = f"https://www.instagram.com/{referer_handle}/"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8", "ignore"))


def _request_text(url: str, *, referer_handle: str | None = None) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
    }
    if referer_handle:
        headers["Referer"] = f"https://www.instagram.com/{referer_handle}/"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return response.read().decode("utf-8", "ignore")


def _request_json_with_retries(url: str, *, referer_handle: str | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    max_attempts = len(REQUEST_RETRY_DELAYS) + 1
    last_error: Exception | None = None
    for attempt_no in range(1, max_attempts + 1):
        try:
            payload = _request_json(url, referer_handle=referer_handle)
            attempts.append({"attempt": attempt_no, "outcome": "success"})
            return payload, attempts
        except urllib.error.HTTPError as exc:
            attempts.append({"attempt": attempt_no, "outcome": "http_error", "code": exc.code})
            last_error = exc
            if exc.code not in RETRYABLE_HTTP_CODES or attempt_no >= max_attempts:
                setattr(exc, "codex_attempts", list(attempts))
                raise
        except Exception as exc:
            attempts.append({"attempt": attempt_no, "outcome": "error", "error": str(exc)})
            last_error = exc
            if attempt_no >= max_attempts:
                setattr(exc, "codex_attempts", list(attempts))
                raise
        time.sleep(REQUEST_RETRY_DELAYS[attempt_no - 1])
    if last_error:
        raise last_error
    raise RuntimeError(f"Request failed without returning a payload: {url}")


def _request_text_with_retries(url: str, *, referer_handle: str | None = None) -> tuple[str, list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    max_attempts = len(REQUEST_RETRY_DELAYS) + 1
    last_error: Exception | None = None
    for attempt_no in range(1, max_attempts + 1):
        try:
            payload = _request_text(url, referer_handle=referer_handle)
            attempts.append({"attempt": attempt_no, "outcome": "success"})
            return payload, attempts
        except urllib.error.HTTPError as exc:
            attempts.append({"attempt": attempt_no, "outcome": "http_error", "code": exc.code})
            last_error = exc
            if exc.code not in RETRYABLE_HTTP_CODES or attempt_no >= max_attempts:
                setattr(exc, "codex_attempts", list(attempts))
                raise
        except Exception as exc:
            attempts.append({"attempt": attempt_no, "outcome": "error", "error": str(exc)})
            last_error = exc
            if attempt_no >= max_attempts:
                setattr(exc, "codex_attempts", list(attempts))
                raise
        time.sleep(REQUEST_RETRY_DELAYS[attempt_no - 1])
    if last_error:
        raise last_error
    raise RuntimeError(f"Request failed without returning text: {url}")


def _profile_info_url(handle: str) -> str:
    return f"https://www.instagram.com/api/v1/users/web_profile_info/?username={urllib.parse.quote(handle)}"


def _timeline_url(handle: str, *, count: int) -> str:
    return f"https://www.instagram.com/api/v1/feed/user/{urllib.parse.quote(handle)}/username/?count={count}"


def _profile_html_url(handle: str) -> str:
    return f"https://www.instagram.com/{urllib.parse.quote(handle)}/"


def _extract_hashtags(text: str) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for match in HASHTAG_PATTERN.finditer(text):
        tag = _compact_text(match.group(1))
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(tag)
    return results


def _caption_text(item: dict[str, Any]) -> str:
    caption = item.get("caption")
    if isinstance(caption, dict):
        return _compact_text(caption.get("text"))
    return _compact_text(item.get("caption_text"))


def _visible_hook(text: str) -> str | None:
    if not text:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if lines:
        return lines[0][:180]
    return text[:180]


def _hook_family(text: str) -> str:
    lowered = text.lower()
    if "?" in text or "would you" in lowered or "which one" in lowered:
        return "engagement_prompt"
    if any(token in lowered for token in ["new", "ready for", "debut", "just dropped", "now available"]):
        return "launch_reveal"
    if any(token in lowered for token in ["perfect gift", "shop", "link in bio", "limited edition"]):
        return "cta_merchandising"
    if any(token in lowered for token in ["inspired by", "story", "because", "made for"]):
        return "storytelling"
    return "statement_showcase"


def _tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for raw in TOKEN_PATTERN.findall(text.lower()):
        token = raw.strip("'")
        if len(token) < 3 or token in STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _theme_from_text(text: str, hashtags: list[str]) -> str | None:
    tokens = set(_tokens(text))
    tokens.update(tag.lower() for tag in hashtags)
    for theme, keywords in THEME_KEYWORDS.items():
        if tokens & keywords:
            return theme
    if hashtags:
        return hashtags[0].lower()
    token_rows = _tokens(text)
    return token_rows[0] if token_rows else None


def _duck_family_from_text(text: str) -> str | None:
    match = DUCK_FAMILY_PATTERN.search(text)
    if not match:
        return None
    return _compact_text(match.group(1)).title()


def _repeated_motif(text: str, hashtags: list[str]) -> str | None:
    candidates = [tag.lower() for tag in hashtags if tag]
    if candidates:
        return candidates[0]
    token_rows = _tokens(text)
    return token_rows[0] if token_rows else None


def _post_format(item: dict[str, Any]) -> str:
    media_type = _safe_int(item.get("media_type"))
    product_type = _compact_text(item.get("product_type")).lower()
    if product_type == "clips" or media_type == 2:
        return "reel"
    if media_type == 8:
        return "carousel"
    return "image"


def _hour_bucket(timestamp: int | None) -> str | None:
    if not timestamp:
        return None
    dt = datetime.fromtimestamp(timestamp).astimezone()
    hour = dt.hour
    if 5 <= hour < 8:
        return "early_morning"
    if 8 <= hour < 11:
        return "morning"
    if 11 <= hour < 14:
        return "midday"
    if 14 <= hour < 17:
        return "afternoon"
    if 17 <= hour < 21:
        return "evening"
    return "late_night"


def _engagement_visible(item: dict[str, Any]) -> dict[str, int | None]:
    return {
        "likes": _safe_int(item.get("like_count")),
        "comments": _safe_int(item.get("comment_count")),
        "plays": _safe_int(item.get("play_count")),
        "views": _safe_int(item.get("view_count")) or _safe_int(item.get("ig_play_count")),
    }


def _engagement_score(row: dict[str, Any]) -> float:
    visible = row.get("engagement_visible") or {}
    likes = _safe_int(visible.get("likes")) or 0
    comments = _safe_int(visible.get("comments")) or 0
    plays = _safe_int(visible.get("plays")) or 0
    views = _safe_int(visible.get("views")) or 0
    return round(likes + comments * 4 + plays * 0.05 + views * 0.03, 2)


def _clone_cached_profile(profile: dict[str, Any], *, snapshot_source: str, refreshed_at: str) -> dict[str, Any]:
    cloned = dict(profile or {})
    cloned["snapshot_source"] = snapshot_source
    cloned["cache_refreshed_at"] = refreshed_at
    cloned["original_observed_at"] = _compact_text(cloned.get("observed_at")) or None
    return cloned


def _clone_cached_posts(posts: list[dict[str, Any]], *, refreshed_at: str, limit: int) -> list[dict[str, Any]]:
    cloned_rows: list[dict[str, Any]] = []
    for row in list(posts or [])[:limit]:
        if not isinstance(row, dict):
            continue
        cloned = dict(row)
        cloned["snapshot_source"] = "cached"
        cloned["cache_refreshed_at"] = refreshed_at
        cloned["original_observed_at"] = _compact_text(cloned.get("observed_at")) or None
        cloned_rows.append(cloned)
    return cloned_rows


def _html_meta_content(html_text: str, key: str) -> str | None:
    for content, attr in META_CONTENT_RE.findall(html_text):
        if _compact_text(attr).lower() == key.lower():
            return html.unescape(content)
    return None


def _profile_summary_from_html(seed: dict[str, Any], html_text: str) -> dict[str, Any]:
    description = _html_meta_content(html_text, "description") or _html_meta_content(html_text, "og:description") or ""
    title = _html_meta_content(html_text, "og:title") or ""
    counts_match = DESCRIPTION_COUNTS_RE.search(description)
    name_match = DESCRIPTION_NAME_RE.search(description) or TITLE_NAME_RE.search(title)
    bio = None
    if ' on Instagram: "' in description:
        bio = description.split(' on Instagram: "', 1)[1].rsplit('"', 1)[0]
    return {
        "brand_key": _compact_text(seed.get("brand_key")),
        "display_name": _compact_text(seed.get("display_name")),
        "account_handle": _compact_text(seed.get("instagram_handle")),
        "platform": "instagram",
        "full_name": _compact_text((name_match.group("name") if name_match else None)) or _compact_text(seed.get("display_name")),
        "biography": _compact_text(bio),
        "external_url": None,
        "follower_count": _safe_count_int(counts_match.group("followers")) if counts_match else None,
        "following_count": _safe_count_int(counts_match.group("following")) if counts_match else None,
        "media_count": _safe_count_int(counts_match.group("posts")) if counts_match else None,
        "is_private": False,
        "is_verified": None,
        "profile_pic_url": None,
        "eimu_id": None,
        "confidence": _compact_text(seed.get("confidence")) or None,
        "category": _compact_text(seed.get("category")) or None,
        "reason": _compact_text(seed.get("reason")) or None,
        "snapshot_source": "html_profile",
    }


def _profile_summary(profile_payload: dict[str, Any], seed: dict[str, Any]) -> dict[str, Any]:
    user = (((profile_payload.get("data") or {}).get("user")) or {}) if isinstance(profile_payload, dict) else {}
    return {
        "brand_key": _compact_text(seed.get("brand_key")),
        "display_name": _compact_text(seed.get("display_name")),
        "account_handle": _compact_text(seed.get("instagram_handle")),
        "platform": "instagram",
        "full_name": _compact_text(user.get("full_name")) or _compact_text(seed.get("display_name")),
        "biography": _compact_text(user.get("biography")),
        "external_url": _compact_text(user.get("external_url")) or None,
        "follower_count": _safe_int((user.get("edge_followed_by") or {}).get("count")) or _safe_int(user.get("follower_count")),
        "following_count": _safe_int((user.get("edge_follow") or {}).get("count")) or _safe_int(user.get("following_count")),
        "media_count": _safe_int((user.get("edge_owner_to_timeline_media") or {}).get("count")) or _safe_int(user.get("media_count")),
        "is_private": bool(user.get("is_private")),
        "is_verified": bool(user.get("is_verified")),
        "profile_pic_url": _compact_text(user.get("profile_pic_url_hd")) or _compact_text(user.get("profile_pic_url")) or None,
        "eimu_id": _compact_text(user.get("eimu_id")) or None,
        "confidence": _compact_text(seed.get("confidence")) or None,
        "category": _compact_text(seed.get("category")) or None,
        "reason": _compact_text(seed.get("reason")) or None,
        "snapshot_source": "live",
    }


def _normalized_posts(seed: dict[str, Any], timeline_payload: dict[str, Any], *, observed_at: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    items = timeline_payload.get("items") if isinstance(timeline_payload, dict) else []
    if not isinstance(items, list):
        return results
    for item in items:
        if not isinstance(item, dict):
            continue
        caption_text = _caption_text(item)
        hashtags = _extract_hashtags(caption_text)
        taken_at = _safe_int(item.get("taken_at")) or _safe_int(item.get("device_timestamp"))
        post_dt = datetime.fromtimestamp(taken_at).astimezone() if taken_at else None
        row = {
            "brand_key": _compact_text(seed.get("brand_key")),
            "account_name": _compact_text(seed.get("display_name")),
            "platform": "instagram",
            "account_handle": _compact_text(seed.get("instagram_handle")),
            "post_id": _compact_text(item.get("pk")) or None,
            "post_code": _compact_text(item.get("code")) or None,
            "post_url": f"https://www.instagram.com/p/{_compact_text(item.get('code'))}/" if _compact_text(item.get("code")) else None,
            "observed_at": observed_at,
            "post_date": post_dt.date().isoformat() if post_dt else None,
            "published_at": post_dt.isoformat() if post_dt else None,
            "hour_bucket": _hour_bucket(taken_at),
            "post_format": _post_format(item),
            "hook_family": _hook_family(caption_text),
            "visible_hook": _visible_hook(caption_text),
            "theme": _theme_from_text(caption_text, hashtags),
            "duck_family": _duck_family_from_text(caption_text),
            "caption_excerpt": caption_text[:240] if caption_text else None,
            "hashtags": hashtags,
            "engagement_visible": _engagement_visible(item),
            "engagement_score": None,
            "repeated_motif": _repeated_motif(caption_text, hashtags),
            "notes": None,
            "confidence": "public_api",
            "source_url": f"https://www.instagram.com/{_compact_text(seed.get('instagram_handle'))}/",
            "snapshot_source": "live",
        }
        row["engagement_score"] = _engagement_score(row)
        results.append(row)
    return results


def _collect_account(
    seed: dict[str, Any],
    *,
    latest_posts_per_account: int,
    force_html_profile_only: bool = False,
    cached_profile: dict[str, Any] | None = None,
    cached_posts: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[str, Any] | None]:
    handle = _compact_text(seed.get("instagram_handle"))
    if not handle:
        return None, [], {"account_handle": None, "failure_class": "missing_handle", "message": "Seed account has no confirmed Instagram handle."}

    observed_at = now_local_iso()
    profile_payload: dict[str, Any] | None = None
    profile_attempts: list[dict[str, Any]] = []
    if not force_html_profile_only:
        if cached_profile or cached_posts:
            pass
        try:
            profile_payload, profile_attempts = _request_json_with_retries(_profile_info_url(handle), referer_handle=handle)
        except urllib.error.HTTPError as exc:
            profile_attempts = list(getattr(exc, "codex_attempts", []))
            profile_payload = None
            profile_error: Exception | None = exc
        except Exception as exc:
            profile_attempts = list(getattr(exc, "codex_attempts", []))
            profile_payload = None
            profile_error = exc
        else:
            profile_error = None
    else:
        profile_error = RuntimeError("profile_api_skipped_after_rate_limit")

    if profile_payload is None:
        try:
            html_text, html_attempts = _request_text_with_retries(_profile_html_url(handle), referer_handle=handle)
            html_profile = _profile_summary_from_html(seed, html_text)
            html_profile["snapshot_source"] = "html_profile_cached_posts" if cached_posts else "html_profile"
            if cached_posts:
                return (
                    html_profile,
                    _clone_cached_posts(cached_posts, refreshed_at=observed_at, limit=latest_posts_per_account),
                    {
                        "account_handle": handle,
                        "failure_class": "profile_api_rate_limited_html_profile_cached_posts" if force_html_profile_only or isinstance(profile_error, urllib.error.HTTPError) else "profile_fetch_failed_html_profile_cached_posts",
                        "message": "Profile API path was unavailable; HTML profile fallback succeeded and cached posts were reused.",
                        "fallback_used": True,
                        "attempts": {"profile_api": profile_attempts, "profile_html": html_attempts},
                    },
                )
            return (
                html_profile,
                [],
                {
                    "account_handle": handle,
                    "failure_class": "profile_api_rate_limited_html_profile_only" if force_html_profile_only or isinstance(profile_error, urllib.error.HTTPError) else "profile_fetch_failed_html_profile_only",
                    "message": "Profile API path was unavailable; HTML profile fallback succeeded but no cached posts were available yet.",
                    "fallback_used": True,
                    "attempts": {"profile_api": profile_attempts, "profile_html": html_attempts},
                },
            )
        except Exception as html_exc:
            html_attempts = list(getattr(html_exc, "codex_attempts", []))
            if cached_profile or cached_posts:
                fallback_profile = _clone_cached_profile(cached_profile or _profile_summary({}, seed), snapshot_source="cached_profile_and_posts", refreshed_at=observed_at)
                fallback_posts = _clone_cached_posts(cached_posts or [], refreshed_at=observed_at, limit=latest_posts_per_account)
                return (
                    fallback_profile,
                    fallback_posts,
                    {
                        "account_handle": handle,
                        "failure_class": "profile_http_error" if isinstance(profile_error, urllib.error.HTTPError) else "profile_fetch_failed",
                        "message": f"Profile API path failed and HTML fallback failed ({html_exc}); cached snapshot reused.",
                        "fallback_used": True,
                        "attempts": {"profile_api": profile_attempts, "profile_html": html_attempts},
                    },
                )
            return (
                None,
                [],
                {
                    "account_handle": handle,
                    "failure_class": "profile_http_error" if isinstance(profile_error, urllib.error.HTTPError) else "profile_fetch_failed",
                    "message": f"Profile API path failed and HTML fallback failed ({html_exc}).",
                    "attempts": {"profile_api": profile_attempts, "profile_html": html_attempts},
                },
            )

    try:
        timeline_payload, timeline_attempts = _request_json_with_retries(_timeline_url(handle, count=latest_posts_per_account), referer_handle=handle)
    except urllib.error.HTTPError as exc:
        live_profile = _profile_summary(profile_payload, seed)
        if cached_posts:
            live_profile["snapshot_source"] = "live_profile_cached_posts"
            return (
                live_profile,
                _clone_cached_posts(cached_posts, refreshed_at=observed_at, limit=latest_posts_per_account),
                {
                    "account_handle": handle,
                    "failure_class": "timeline_http_error",
                    "message": f"Timeline endpoint returned HTTP {exc.code}; cached posts reused.",
                    "fallback_used": True,
                    "attempts": list(getattr(exc, "codex_attempts", [])),
                },
            )
        return live_profile, [], {"account_handle": handle, "failure_class": "timeline_http_error", "message": f"Timeline endpoint returned HTTP {exc.code}.", "attempts": list(getattr(exc, "codex_attempts", []))}
    except Exception as exc:
        live_profile = _profile_summary(profile_payload, seed)
        if cached_posts:
            live_profile["snapshot_source"] = "live_profile_cached_posts"
            return (
                live_profile,
                _clone_cached_posts(cached_posts, refreshed_at=observed_at, limit=latest_posts_per_account),
                {
                    "account_handle": handle,
                    "failure_class": "timeline_fetch_failed",
                    "message": f"{exc}; cached posts reused.",
                    "fallback_used": True,
                    "attempts": list(getattr(exc, "codex_attempts", [])),
                },
            )
        return live_profile, [], {"account_handle": handle, "failure_class": "timeline_fetch_failed", "message": str(exc), "attempts": list(getattr(exc, "codex_attempts", []))}

    profile = _profile_summary(profile_payload, seed)
    posts = _normalized_posts(seed, timeline_payload, observed_at=observed_at)
    return profile, posts, None


def _history_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary") or {}
    return {
        "generated_at": payload.get("generated_at"),
        "seed_account_count": int(summary.get("seed_account_count") or 0),
        "collected_account_count": int(summary.get("collected_account_count") or 0),
        "failed_account_count": int(summary.get("failed_account_count") or 0),
        "post_count": int(summary.get("post_count") or 0),
        "top_account": ((payload.get("rollups") or {}).get("top_accounts") or [{}])[0].get("account_handle"),
        "top_theme": ((payload.get("rollups") or {}).get("top_themes") or [{}])[0].get("label"),
    }


def _load_history() -> list[dict[str, Any]]:
    return _history_snapshots()


def _changes_since_previous(current: dict[str, Any], previous: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not previous:
        return []
    changes: list[dict[str, Any]] = []
    current_top_account = ((current.get("rollups") or {}).get("top_accounts") or [{}])[0].get("account_handle")
    prev_top_account = previous.get("top_account")
    if current_top_account and current_top_account != prev_top_account:
        changes.append({"headline": f"Top competitor account shifted from `{prev_top_account}` to `{current_top_account}`."})
    current_top_theme = ((current.get("rollups") or {}).get("top_themes") or [{}])[0].get("label")
    prev_top_theme = previous.get("top_theme")
    if current_top_theme and current_top_theme != prev_top_theme:
        changes.append({"headline": f"Most repeated competitor theme shifted from `{prev_top_theme}` to `{current_top_theme}`."})
    post_delta = int(((current.get("summary") or {}).get("post_count")) or 0) - int(previous.get("post_count") or 0)
    if post_delta:
        changes.append({"headline": f"Observed competitor post count changed by `{post_delta}`."})
    return changes


def _counter_rows(counter: Counter[str], *, key_name: str = "label", limit: int = 8) -> list[dict[str, Any]]:
    rows = [{key_name: label, "count": count} for label, count in counter.most_common(limit) if label]
    return rows


def _account_rollups(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in posts:
        handle = _compact_text(row.get("account_handle")) or "(unknown)"
        grouped.setdefault(handle, []).append(row)
    rows = []
    for handle, items in grouped.items():
        avg_score = round(sum(float(item.get("engagement_score") or 0.0) for item in items) / max(1, len(items)), 2)
        rows.append({"account_handle": handle, "post_count": len(items), "avg_engagement_score": avg_score})
    rows.sort(key=lambda item: (-float(item.get("avg_engagement_score") or 0.0), -int(item.get("post_count") or 0), str(item.get("account_handle") or "")))
    return rows[:8]


def build_competitor_social_snapshots(*, latest_posts_per_account: int | None = None) -> dict[str, Any]:
    config = _load_config()
    existing_snapshot = _load_existing_snapshot()
    cached_profiles_by_handle = {
        _compact_text(item.get("account_handle")): item
        for item in (existing_snapshot.get("profiles") or [])
        if isinstance(item, dict) and _compact_text(item.get("account_handle"))
    }
    cached_posts_by_handle: dict[str, list[dict[str, Any]]] = {}
    for row in (existing_snapshot.get("posts") or []):
        if not isinstance(row, dict):
            continue
        handle = _compact_text(row.get("account_handle"))
        if not handle:
            continue
        cached_posts_by_handle.setdefault(handle, []).append(row)
    seed_accounts = [item for item in (config.get("seed_accounts") or []) if isinstance(item, dict)]
    target_count = latest_posts_per_account or int(((config.get("collection_boundary") or {}).get("latest_posts_per_account")) or 12)

    profiles: list[dict[str, Any]] = []
    posts: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    consecutive_profile_rate_limits = 0
    rate_limit_circuit_breaker_used = False

    for seed in seed_accounts[: int(((config.get("collection_boundary") or {}).get("max_accounts_per_run")) or 10)]:
        handle = _compact_text(seed.get("instagram_handle"))
        profile, account_posts, failure = _collect_account(
            seed,
            latest_posts_per_account=target_count,
            force_html_profile_only=consecutive_profile_rate_limits >= PROFILE_RATE_LIMIT_CIRCUIT_BREAKER_THRESHOLD,
            cached_profile=cached_profiles_by_handle.get(handle),
            cached_posts=cached_posts_by_handle.get(handle),
        )
        if profile:
            profiles.append(profile)
        posts.extend(account_posts)
        if failure:
            failures.append(failure)
            failure_class = _compact_text(failure.get("failure_class"))
            if "profile_http_error" in failure_class or "profile_api_rate_limited" in failure_class:
                consecutive_profile_rate_limits += 1
                if consecutive_profile_rate_limits >= PROFILE_RATE_LIMIT_CIRCUIT_BREAKER_THRESHOLD:
                    rate_limit_circuit_breaker_used = True
            else:
                consecutive_profile_rate_limits = 0
        else:
            consecutive_profile_rate_limits = 0
        time.sleep(INTER_ACCOUNT_DELAY_SECONDS)

    theme_counter: Counter[str] = Counter(_compact_text(row.get("theme")).lower() for row in posts if _compact_text(row.get("theme")))
    format_counter: Counter[str] = Counter(_compact_text(row.get("post_format")).lower() for row in posts if _compact_text(row.get("post_format")))
    hook_counter: Counter[str] = Counter(_compact_text(row.get("hook_family")).lower() for row in posts if _compact_text(row.get("hook_family")))
    motif_counter: Counter[str] = Counter(_compact_text(row.get("repeated_motif")).lower() for row in posts if _compact_text(row.get("repeated_motif")))

    cached_account_count = sum(
        1
        for profile in profiles
        if _compact_text(profile.get("snapshot_source")) not in {"", "live"}
    )
    html_profile_account_count = sum(
        1
        for profile in profiles
        if "html_profile" in _compact_text(profile.get("snapshot_source"))
    )
    profile_only_account_count = sum(
        1
        for profile in profiles
        if "html_profile" in _compact_text(profile.get("snapshot_source"))
        and not any(_compact_text(row.get("account_handle")) == _compact_text(profile.get("account_handle")) for row in posts)
    )
    degraded_account_count = len(failures)
    hard_failure_count = sum(1 for item in failures if not bool((item or {}).get("fallback_used")))

    payload = {
        "generated_at": now_local_iso(),
        "summary": {
            "headline": "Public Instagram competitor snapshots collected through bounded observe-only reads.",
            "seed_account_count": len(seed_accounts),
            "collected_account_count": len(profiles),
            "failed_account_count": hard_failure_count,
            "degraded_account_count": degraded_account_count,
            "cached_account_count": cached_account_count,
            "html_profile_account_count": html_profile_account_count,
            "profile_only_account_count": profile_only_account_count,
            "live_account_count": max(0, len(profiles) - cached_account_count),
            "post_count": len(posts),
            "latest_posts_per_account": target_count,
            "rate_limit_circuit_breaker_used": rate_limit_circuit_breaker_used,
            "data_quality_note": "Visible metrics vary by post type and account. Treat this as directional benchmark data, not first-party truth.",
        },
        "collection_boundary": config.get("collection_boundary") or {},
        "profiles": profiles,
        "posts": posts,
        "failures": failures,
        "rollups": {
            "top_accounts": _account_rollups(posts),
            "top_themes": _counter_rows(theme_counter),
            "top_formats": _counter_rows(format_counter),
            "top_hook_families": _counter_rows(hook_counter),
            "top_motifs": _counter_rows(motif_counter),
        },
    }

    history = _load_history()
    previous = history[-1] if history else None
    payload["changes_since_previous"] = _changes_since_previous(payload, previous)
    history.append(_history_snapshot(payload))
    history = history[-12:]

    write_json(STATE_PATH, payload)
    write_json(OPERATOR_JSON_PATH, payload)
    write_markdown(OUTPUT_MD_PATH, render_competitor_social_snapshots_markdown(payload))
    _save_history(history)
    return payload


def render_competitor_social_snapshots_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# Competitor Social Snapshots",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Seed accounts: `{summary.get('seed_account_count') or 0}`",
        f"- Accounts collected: `{summary.get('collected_account_count') or 0}`",
        f"- Accounts with degraded fetches: `{summary.get('degraded_account_count') or 0}`",
        f"- Accounts hard failed: `{summary.get('failed_account_count') or 0}`",
        f"- Accounts using cached fallback: `{summary.get('cached_account_count') or 0}`",
        f"- Accounts recovered from HTML profile fallback: `{summary.get('html_profile_account_count') or 0}`",
        f"- Accounts with profile-only fallback: `{summary.get('profile_only_account_count') or 0}`",
        f"- Posts observed: `{summary.get('post_count') or 0}`",
        f"- Rate-limit circuit breaker used: `{bool(summary.get('rate_limit_circuit_breaker_used'))}`",
        "",
        str(summary.get("headline") or ""),
        "",
        str(summary.get("data_quality_note") or ""),
        "",
        "## Top Accounts",
        "",
    ]
    top_accounts = ((payload.get("rollups") or {}).get("top_accounts") or [])
    if top_accounts:
        for item in top_accounts:
            lines.append(
                f"- `{item.get('account_handle')}`: `{item.get('post_count')}` posts | avg visible score `{item.get('avg_engagement_score')}`"
            )
    else:
        lines.append("No competitor accounts were collected.")
    lines.append("")

    for heading, key in [
        ("Top Themes", "top_themes"),
        ("Top Formats", "top_formats"),
        ("Top Hook Families", "top_hook_families"),
        ("Top Motifs", "top_motifs"),
    ]:
        lines.extend([f"## {heading}", ""])
        rows = ((payload.get("rollups") or {}).get(key) or [])
        if not rows:
            lines.append("No rows yet.")
        else:
            for item in rows:
                lines.append(f"- `{item.get('label')}`: `{item.get('count')}`")
        lines.append("")

    lines.extend(["## Failures", ""])
    failures = payload.get("failures") or []
    if not failures:
        lines.append("No collector failures.")
        lines.append("")
    else:
        for item in failures:
            lines.append(f"- `{item.get('account_handle')}`: `{item.get('failure_class')}` | {item.get('message')}")
        lines.append("")

    lines.extend(["## What Changed", ""])
    changes = payload.get("changes_since_previous") or []
    if not changes:
        lines.append("No major snapshot changes detected yet.")
    else:
        for item in changes:
            lines.append(f"- {item.get('headline')}")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect bounded public Instagram competitor snapshots.")
    parser.add_argument("--latest-posts", type=int, default=None, help="Override the per-account snapshot depth.")
    args = parser.parse_args()
    payload = build_competitor_social_snapshots(latest_posts_per_account=args.latest_posts)
    print(
        {
            "generated_at": payload.get("generated_at"),
            "collected_account_count": ((payload.get("summary") or {}).get("collected_account_count")),
            "post_count": ((payload.get("summary") or {}).get("post_count")),
            "failed_account_count": ((payload.get("summary") or {}).get("failed_account_count")),
        }
    )


if __name__ == "__main__":
    main()
