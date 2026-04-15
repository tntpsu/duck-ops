from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from governance_review_common import (
    DUCK_AGENT_ROOT,
    DUCK_OPS_ROOT,
    OUTPUT_OPERATOR_DIR,
    now_local_iso,
    write_json,
    write_markdown,
)


DUCK_AGENT_VENV_PY = DUCK_AGENT_ROOT / ".venv" / "bin" / "python3"
DUCK_AGENT_RUNS_DIR = DUCK_AGENT_ROOT / "runs"
STATE_PATH = DUCK_OPS_ROOT / "state" / "social_performance_posts.json"
ROLLUPS_PATH = DUCK_OPS_ROOT / "state" / "social_performance_rollups.json"
HISTORY_PATH = DUCK_OPS_ROOT / "state" / "social_performance_history.json"
OPERATOR_JSON_PATH = OUTPUT_OPERATOR_DIR / "social_insights.json"
OUTPUT_MD_PATH = OUTPUT_OPERATOR_DIR / "social_insights.md"
HASHTAG_PATTERN = re.compile(r"(?<!\w)#([A-Za-z0-9_]+)")


def _ensure_duckagent_python() -> None:
    if os.environ.get("SOCIAL_PERFORMANCE_VENV_READY") == "1":
        return
    try:
        current_python = Path(sys.executable).resolve()
    except Exception:
        current_python = Path(sys.executable)
    if current_python == DUCK_AGENT_VENV_PY or not DUCK_AGENT_VENV_PY.exists():
        return
    os.environ["SOCIAL_PERFORMANCE_VENV_READY"] = "1"
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
            os.environ.setdefault(key, value.strip().strip('"').strip("'"))

    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    from helpers.meta_token_manager import get_meta_token_manager  # type: ignore

    return get_meta_token_manager


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.astimezone()


def _compact_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _time_window(dt: datetime | None) -> str | None:
    if dt is None:
        return None
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


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _extract_hashtags(caption: Any) -> list[str]:
    text = str(caption or "")
    seen: set[str] = set()
    results: list[str] = []
    for match in HASHTAG_PATTERN.finditer(text):
        tag = str(match.group(1) or "").strip()
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(tag)
    return results


def _receipt_paths() -> list[Path]:
    return sorted(DUCK_AGENT_RUNS_DIR.glob("*/*_posts.json"))


def _normalize_receipt_post(
    receipt_path: Path,
    payload: dict[str, Any],
    post: dict[str, Any],
    *,
    now: datetime,
    cutoff: datetime,
) -> dict[str, Any] | None:
    workflow = _compact_text(payload.get("workflow"))
    run_id = _compact_text(payload.get("run_id"))
    platform = _compact_text(post.get("platform")).lower()
    post_id = _compact_text(post.get("post_id"))
    if not workflow or not run_id or not platform or not post_id:
        return None

    meta = post.get("meta_data") if isinstance(post.get("meta_data"), dict) else {}
    scheduled_time = _parse_iso(post.get("scheduled_time"))
    saved_at = _parse_iso(post.get("saved_at"))
    observed_time = scheduled_time or saved_at
    if observed_time is None or observed_time < cutoff:
        return None

    row = {
        "workflow": workflow,
        "run_id": run_id,
        "platform": platform,
        "post_id": post_id,
        "status": _compact_text(post.get("status")).lower() or "unknown",
        "url": _compact_text(post.get("url")) or None,
        "shortcode": _compact_text(post.get("shortcode")) or None,
        "media_id": _compact_text(post.get("media_id")) or None,
        "receipt_path": str(receipt_path),
        "receipt_saved_at": saved_at.isoformat() if saved_at else None,
        "scheduled_time": scheduled_time.isoformat() if scheduled_time else None,
        "published_at": observed_time.isoformat(),
        "published_date": observed_time.date().isoformat(),
        "weekday": observed_time.strftime("%A"),
        "hour_local": observed_time.hour,
        "time_window": _time_window(observed_time),
        "is_future_post": observed_time > now,
        "content_type": _compact_text(meta.get("content_type")) or None,
        "title": _compact_text(meta.get("title")) or None,
        "theme": _compact_text(meta.get("theme")) or None,
        "duck_family": _compact_text(meta.get("duck_family")) or None,
        "caption": _compact_text(meta.get("caption")) or None,
        "hashtags": [
            str(tag).strip()
            for tag in ((meta.get("hashtags") or []) or _extract_hashtags(meta.get("caption")))
            if str(tag).strip()
        ],
        "asset_url": _compact_text(meta.get("asset_url")) or None,
        "link_url": _compact_text(meta.get("link_url")) or None,
        "media_count": _safe_int(meta.get("media_count")),
        "meta_data": meta,
        "receipt_contract_version": _safe_int(meta.get("receipt_contract_version")) or 0,
    }
    return row


def _load_normalized_posts(*, window_days: int, now: datetime | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    current = now or datetime.now().astimezone()
    cutoff = current - timedelta(days=window_days)
    posts: list[dict[str, Any]] = []
    malformed_receipts: list[str] = []
    scanned_receipts = 0

    for receipt_path in _receipt_paths():
        scanned_receipts += 1
        try:
            payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        except Exception:
            malformed_receipts.append(str(receipt_path))
            continue
        if not isinstance(payload, dict):
            malformed_receipts.append(str(receipt_path))
            continue
        receipt_posts = payload.get("posts")
        if not isinstance(receipt_posts, list):
            malformed_receipts.append(str(receipt_path))
            continue
        for post in receipt_posts:
            if not isinstance(post, dict):
                continue
            normalized = _normalize_receipt_post(receipt_path, payload, post, now=current, cutoff=cutoff)
            if normalized is not None:
                posts.append(normalized)

    posts.sort(key=lambda item: (str(item.get("published_at") or ""), str(item.get("workflow") or ""), str(item.get("platform") or "")))
    summary = {
        "generated_at": current.isoformat(),
        "window_days": window_days,
        "window_start": cutoff.isoformat(),
        "window_end": current.isoformat(),
        "scanned_receipt_count": scanned_receipts,
        "normalized_post_count": len(posts),
        "malformed_receipt_count": len(malformed_receipts),
        "malformed_receipts": malformed_receipts[:20],
    }
    return posts, summary


def _facebook_object_fields() -> str:
    return ",".join(
        [
            "id",
            "created_time",
            "permalink_url",
            "message",
            "shares",
            "reactions.summary(total_count).limit(0)",
            "comments.summary(total_count).limit(0)",
        ]
    )


def _parse_insights_payload(payload: dict[str, Any]) -> dict[str, int]:
    results: dict[str, int] = {}
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        name = _compact_text(item.get("name"))
        values = item.get("values") if isinstance(item.get("values"), list) else []
        if not name or not values:
            continue
        first = values[0] if isinstance(values[0], dict) else {}
        value = first.get("value")
        if isinstance(value, dict):
            continue
        parsed = _safe_int(value)
        if parsed is not None:
            results[name] = parsed
    return results


def _fetch_instagram_metrics(post: dict[str, Any], token_manager) -> dict[str, Any]:
    post_id = str(post.get("post_id") or "").strip()
    if not post_id:
        return {"status": "missing_id", "metrics": {}, "errors": ["Missing Instagram post id."]}

    object_response = token_manager.make_request(
        "GET",
        f"https://graph.facebook.com/v19.0/{post_id}",
        params={"fields": "id,media_type,media_product_type,permalink,timestamp,comments_count,like_count"},
    )
    if object_response.status_code >= 400:
        return {
            "status": "fetch_failed",
            "metrics": {},
            "errors": [object_response.text[:300]],
        }

    object_payload = object_response.json()
    metrics: dict[str, Any] = {
        "like_count": _safe_int(object_payload.get("like_count")),
        "comments_count": _safe_int(object_payload.get("comments_count")),
        "permalink": _compact_text(object_payload.get("permalink")) or None,
        "timestamp": _compact_text(object_payload.get("timestamp")) or None,
        "media_type": _compact_text(object_payload.get("media_type")) or None,
        "media_product_type": _compact_text(object_payload.get("media_product_type")) or None,
    }

    errors: list[str] = []
    insight_metrics: dict[str, int] = {}
    media_type = str(metrics.get("media_type") or "").upper()
    metrics_to_try = ["reach", "saved"]
    if media_type in {"VIDEO", "REELS"}:
        metrics_to_try.append("video_views")
    for metric in metrics_to_try:
        insight_response = token_manager.make_request(
            "GET",
            f"https://graph.facebook.com/v19.0/{post_id}/insights",
            params={"metric": metric},
        )
        if insight_response.status_code >= 400:
            errors.append(f"{metric}:{insight_response.text[:160]}")
            continue
        insight_metrics.update(_parse_insights_payload(insight_response.json()))

    metrics.update(insight_metrics)
    metric_values = [metrics.get(key) for key in ("like_count", "comments_count", "saved", "reach", "impressions")]
    status = "ok" if any(value not in (None, 0) for value in metric_values) else ("partial" if metrics else "empty")
    if errors and status == "ok":
        status = "partial"
    return {"status": status, "metrics": metrics, "errors": errors}


def _fetch_facebook_metrics(post: dict[str, Any], token_manager) -> dict[str, Any]:
    post_id = str(post.get("post_id") or "").strip()
    if not post_id:
        return {"status": "missing_id", "metrics": {}, "errors": ["Missing Facebook post id."]}

    token_override = token_manager.get_facebook_page_token()
    object_response = token_manager.make_request(
        "GET",
        f"https://graph.facebook.com/v19.0/{post_id}",
        params={"fields": _facebook_object_fields()},
        token_override=token_override,
    )
    if object_response.status_code >= 400:
        return {
            "status": "fetch_failed",
            "metrics": {},
            "errors": [object_response.text[:300]],
        }

    object_payload = object_response.json()
    reactions = object_payload.get("reactions") if isinstance(object_payload.get("reactions"), dict) else {}
    comments = object_payload.get("comments") if isinstance(object_payload.get("comments"), dict) else {}
    metrics: dict[str, Any] = {
        "reaction_count": _safe_int(((reactions.get("summary") or {}).get("total_count"))),
        "comment_count": _safe_int(((comments.get("summary") or {}).get("total_count"))),
        "share_count": _safe_int(((object_payload.get("shares") or {}).get("count"))),
        "permalink": _compact_text(object_payload.get("permalink_url")) or None,
        "timestamp": _compact_text(object_payload.get("created_time")) or None,
    }

    errors: list[str] = []
    insight_metrics: dict[str, int] = {}
    for metric in ["post_impressions", "post_impressions_unique", "post_engaged_users"]:
        insight_response = token_manager.make_request(
            "GET",
            f"https://graph.facebook.com/v19.0/{post_id}/insights",
            params={"metric": metric},
            token_override=token_override,
        )
        if insight_response.status_code >= 400:
            errors.append(f"{metric}:{insight_response.text[:160]}")
            continue
        insight_metrics.update(_parse_insights_payload(insight_response.json()))

    metrics.update(insight_metrics)
    metric_values = [metrics.get(key) for key in ("reaction_count", "comment_count", "share_count", "post_impressions_unique")]
    status = "ok" if any(value not in (None, 0) for value in metric_values) else ("partial" if metrics else "empty")
    if errors and status == "ok":
        status = "partial"
    return {"status": status, "metrics": metrics, "errors": errors}


def fetch_post_metrics(post: dict[str, Any]) -> dict[str, Any]:
    if post.get("is_future_post"):
        return {"status": "scheduled_future", "metrics": {}, "errors": []}

    get_meta_token_manager = _ensure_duckagent_imports()
    token_manager = get_meta_token_manager()
    platform = str(post.get("platform") or "").lower()
    if platform == "instagram":
        return _fetch_instagram_metrics(post, token_manager)
    if platform == "facebook":
        return _fetch_facebook_metrics(post, token_manager)
    return {"status": "unsupported_platform", "metrics": {}, "errors": [f"Unsupported platform: {platform}"]}


def _engagement_score(post: dict[str, Any]) -> float:
    metrics = post.get("metrics") if isinstance(post.get("metrics"), dict) else {}
    if str(post.get("platform") or "").lower() == "facebook":
        values = [metrics.get("reaction_count"), metrics.get("comment_count"), metrics.get("share_count")]
    else:
        values = [metrics.get("like_count"), metrics.get("comments_count"), metrics.get("saved")]
    return float(sum(_safe_float(value) or 0.0 for value in values))


def _engagement_rate(post: dict[str, Any]) -> float | None:
    metrics = post.get("metrics") if isinstance(post.get("metrics"), dict) else {}
    reach = _safe_float(metrics.get("reach") or metrics.get("post_impressions_unique"))
    if not reach or reach <= 0:
        return None
    return round(_engagement_score(post) / reach, 4)


def _rollup_rows(posts: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for post in posts:
        key = _compact_text(post.get(field)) or "(unknown)"
        grouped[key].append(post)

    rows: list[dict[str, Any]] = []
    for key, items in grouped.items():
        scores = [_engagement_score(item) for item in items]
        avg_score = round(sum(scores) / len(scores), 2) if scores else 0.0
        avg_rate_values = [rate for rate in (_engagement_rate(item) for item in items) if rate is not None]
        avg_rate = round(sum(avg_rate_values) / len(avg_rate_values), 4) if avg_rate_values else None
        rows.append(
            {
                "label": key,
                "post_count": len(items),
                "avg_engagement_score": avg_score,
                "avg_engagement_rate": avg_rate,
                "top_post_id": max(items, key=_engagement_score).get("post_id"),
            }
        )
    rows.sort(key=lambda item: (-float(item.get("avg_engagement_score") or 0), -int(item.get("post_count") or 0), str(item.get("label") or "")))
    return rows


@dataclass
class Learning:
    key: str
    headline: str
    confidence: str
    evidence: str
    recommendation: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "headline": self.headline,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "recommendation": self.recommendation,
        }


def _social_snapshot(rollup_payload: dict[str, Any]) -> dict[str, Any]:
    rollups = rollup_payload.get("rollups") or {}
    top_workflow = ((rollups.get("by_workflow") or [{}])[0] or {})
    top_window = ((rollups.get("by_time_window") or [{}])[0] or {})
    top_theme = ((rollups.get("by_theme") or [{}])[0] or {})
    top_post = ((rollup_payload.get("top_posts") or [{}])[0] or {})
    summary = rollup_payload.get("summary") or {}
    return {
        "generated_at": rollup_payload.get("generated_at"),
        "window_days": rollup_payload.get("window_days"),
        "post_count": summary.get("post_count"),
        "metrics_coverage_pct": summary.get("metrics_coverage_pct"),
        "top_workflow": top_workflow.get("label"),
        "top_time_window": top_window.get("label"),
        "top_theme": top_theme.get("label"),
        "top_post_id": top_post.get("post_id"),
    }


def _load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        items = payload.get("snapshots")
        return list(items) if isinstance(items, list) else []
    if isinstance(payload, list):
        return payload
    return []


def _save_history(path: Path, snapshots: list[dict[str, Any]]) -> None:
    write_json(path, {"generated_at": now_local_iso(), "snapshot_count": len(snapshots), "snapshots": snapshots})


def _social_changes(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[dict[str, Any]]:
    if not previous:
        return []
    changes: list[dict[str, Any]] = []
    if previous.get("top_workflow") != current.get("top_workflow"):
        changes.append(
            {
                "kind": "top_workflow_changed",
                "headline": f"Top workflow moved from `{previous.get('top_workflow')}` to `{current.get('top_workflow')}`.",
            }
        )
    if previous.get("top_time_window") != current.get("top_time_window"):
        changes.append(
            {
                "kind": "top_time_window_changed",
                "headline": f"Best posting window moved from `{previous.get('top_time_window')}` to `{current.get('top_time_window')}`.",
            }
        )
    previous_count = _safe_int(previous.get("post_count")) or 0
    current_count = _safe_int(current.get("post_count")) or 0
    if previous_count != current_count:
        delta = current_count - previous_count
        direction = "up" if delta > 0 else "down"
        changes.append(
            {
                "kind": "post_count_changed",
                "headline": f"Observed post count is {direction} by `{abs(delta)}` since the previous snapshot.",
            }
        )
    previous_coverage = _safe_float(previous.get("metrics_coverage_pct")) or 0.0
    current_coverage = _safe_float(current.get("metrics_coverage_pct")) or 0.0
    if round(previous_coverage, 1) != round(current_coverage, 1):
        delta = round(current_coverage - previous_coverage, 1)
        direction = "up" if delta > 0 else "down"
        changes.append(
            {
                "kind": "coverage_changed",
                "headline": f"Metrics coverage is {direction} by `{abs(delta)}` point(s) since the previous snapshot.",
            }
        )
    return changes


def _derive_learnings(posts: list[dict[str, Any]], rollups: dict[str, list[dict[str, Any]]]) -> list[Learning]:
    learnings: list[Learning] = []
    if not posts:
        return learnings

    workflow_rows = rollups.get("by_workflow") or []
    if workflow_rows:
        top = workflow_rows[0]
        confidence = "medium" if int(top.get("post_count") or 0) >= 3 else "low"
        learnings.append(
            Learning(
                key="top_workflow",
                headline=f"{top.get('label')} is the current strongest workflow in the observed window.",
                confidence=confidence,
                evidence=f"{top.get('post_count')} posts with average engagement score {top.get('avg_engagement_score')}.",
                recommendation="Keep this workflow in the weekly content mix while receipt coverage grows.",
            )
        )

    window_rows = [row for row in (rollups.get("by_time_window") or []) if row.get("label") != "(unknown)"]
    if window_rows:
        top = window_rows[0]
        confidence = "medium" if int(top.get("post_count") or 0) >= 2 else "low"
        learnings.append(
            Learning(
                key="best_time_window",
                headline=f"{top.get('label').replace('_', ' ').title()} is the current best-performing posting window.",
                confidence=confidence,
                evidence=f"{top.get('post_count')} observed posts with average engagement score {top.get('avg_engagement_score')}.",
                recommendation="Use this as the default test window until we have enough weekly data to split by weekday.",
            )
        )

    theme_rows = [row for row in (rollups.get("by_theme") or []) if row.get("label") != "(unknown)"]
    if theme_rows:
        top = theme_rows[0]
        confidence = "medium" if int(top.get("post_count") or 0) >= 2 else "low"
        learnings.append(
            Learning(
                key="top_theme",
                headline=f"{top.get('label')} is the strongest current social theme.",
                confidence=confidence,
                evidence=f"{top.get('post_count')} posts with average engagement score {top.get('avg_engagement_score')}.",
                recommendation="Reuse this visual/caption family in new content tests before broadening into lower-signal themes.",
            )
        )

    top_post = max(posts, key=_engagement_score)
    top_title = _compact_text(top_post.get("title")) or _compact_text(top_post.get("theme")) or f"{top_post.get('workflow')} post"
    learnings.append(
        Learning(
            key="top_post",
            headline=f"{top_title} is the current top observed post.",
            confidence="low" if len(posts) < 5 else "medium",
            evidence=f"{top_post.get('platform')} post {top_post.get('post_id')} has engagement score {round(_engagement_score(top_post), 2)}.",
            recommendation="Review its caption, asset choice, and time slot as a template for the next iteration.",
        )
    )
    return learnings[:4]


def build_social_performance_payload(*, window_days: int = 30, fetch_metrics: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
    now = datetime.now().astimezone()
    posts, summary = _load_normalized_posts(window_days=window_days, now=now)
    statuses = Counter()

    for post in posts:
        fetch_result = fetch_post_metrics(post) if fetch_metrics else {"status": "skipped", "metrics": {}, "errors": []}
        post["metric_status"] = fetch_result.get("status")
        post["metrics"] = fetch_result.get("metrics") or {}
        post["metric_errors"] = fetch_result.get("errors") or []
        post["engagement_score"] = round(_engagement_score(post), 2)
        post["engagement_rate"] = _engagement_rate(post)
        statuses[str(post.get("metric_status") or "unknown")] += 1

    posts.sort(key=lambda item: (-float(item.get("engagement_score") or 0), str(item.get("published_at") or "")), reverse=False)
    posts = sorted(posts, key=lambda item: (-float(item.get("engagement_score") or 0), str(item.get("published_at") or "")))

    post_payload = {
        "generated_at": now_local_iso(),
        "summary": {
            **summary,
            "metric_status_counts": dict(statuses),
            "platform_counts": dict(Counter(str(item.get("platform") or "") for item in posts)),
            "workflow_counts": dict(Counter(str(item.get("workflow") or "") for item in posts)),
        },
        "posts": posts,
    }

    rollups = {
        "by_workflow": _rollup_rows(posts, "workflow"),
        "by_platform": _rollup_rows(posts, "platform"),
        "by_time_window": _rollup_rows(posts, "time_window"),
        "by_theme": _rollup_rows(posts, "theme"),
        "by_duck_family": _rollup_rows(posts, "duck_family"),
    }
    learnings = _derive_learnings(posts, rollups)
    top_posts = [
        {
            "workflow": item.get("workflow"),
            "platform": item.get("platform"),
            "post_id": item.get("post_id"),
            "title": item.get("title"),
            "theme": item.get("theme"),
            "url": item.get("url") or ((item.get("metrics") or {}).get("permalink")),
            "engagement_score": item.get("engagement_score"),
            "engagement_rate": item.get("engagement_rate"),
        }
        for item in posts[:5]
    ]
    rollup_payload = {
        "generated_at": now_local_iso(),
        "window_days": window_days,
        "summary": {
            "post_count": len(posts),
            "platforms_observed": len({str(item.get("platform") or "") for item in posts}),
            "workflows_observed": len({str(item.get("workflow") or "") for item in posts}),
            "metrics_coverage_pct": round(
                (sum(1 for item in posts if str(item.get("metric_status") or "") in {"ok", "partial"}) / len(posts)) * 100,
                1,
            )
            if posts
            else 0.0,
            "data_quality_note": "Receipt history is still sparse, so recommendations should be treated as directional, not final."
            if len(posts) < 10
            else "Coverage is broad enough to start trusting directionally strong differences.",
        },
        "current_learnings": [item.as_dict() for item in learnings],
        "top_posts": top_posts,
        "rollups": rollups,
    }
    history = _load_history(HISTORY_PATH)
    current_snapshot = _social_snapshot(rollup_payload)
    previous_snapshot = history[-1] if history else None
    history.append(current_snapshot)
    history = history[-60:]
    _save_history(HISTORY_PATH, history)
    rollup_payload["changes_since_previous"] = _social_changes(previous_snapshot, current_snapshot)
    rollup_payload["history"] = {
        "snapshot_count": len(history),
        "latest_snapshot": current_snapshot,
        "previous_snapshot": previous_snapshot,
        "recent_snapshots": history[-8:],
    }
    return post_payload, rollup_payload


def render_social_insights_markdown(post_payload: dict[str, Any], rollup_payload: dict[str, Any]) -> str:
    summary = rollup_payload.get("summary") or {}
    lines = [
        "# Social Insights",
        "",
        f"- Generated: `{rollup_payload.get('generated_at') or ''}`",
        f"- Window: last `{rollup_payload.get('window_days') or 0}` days",
        f"- Posts analyzed: `{summary.get('post_count') or 0}`",
        f"- Metrics coverage: `{summary.get('metrics_coverage_pct') or 0}%`",
        f"- Snapshot history: `{((rollup_payload.get('history') or {}).get('snapshot_count')) or 0}` runs",
        "",
        str(summary.get("data_quality_note") or ""),
        "",
        "## What Changed",
        "",
    ]

    changes = rollup_payload.get("changes_since_previous") or []
    if not changes:
        lines.append("No major learning change was detected since the previous snapshot.")
        lines.append("")
    else:
        for item in changes:
            lines.append(f"- {item.get('headline')}")
        lines.append("")

    lines.extend([
        "## Current Learnings",
        "",
    ])

    learnings = rollup_payload.get("current_learnings") or []
    if not learnings:
        lines.append("No social learnings are available yet because there are no normalized posts in the current window.")
        lines.append("")
    else:
        for item in learnings:
            lines.extend(
                [
                    f"### {item.get('headline')}",
                    "",
                    f"- Confidence: `{item.get('confidence')}`",
                    f"- Evidence: {item.get('evidence')}",
                    f"- Recommendation: {item.get('recommendation')}",
                    "",
                ]
            )

    lines.extend(["## Top Posts", ""])
    top_posts = rollup_payload.get("top_posts") or []
    if not top_posts:
        lines.append("No observed posts were available for the current window.")
        lines.append("")
    else:
        for item in top_posts:
            label = _compact_text(item.get("title")) or _compact_text(item.get("theme")) or f"{item.get('workflow')} post"
            lines.extend(
                [
                    f"- `{item.get('workflow')}` / `{item.get('platform')}`: {label}",
                    f"  score `{item.get('engagement_score')}` | rate `{item.get('engagement_rate')}` | post `{item.get('post_id')}`",
                    f"  {item.get('url') or '(no url)'}",
                ]
            )
        lines.append("")

    for title, key in [
        ("Workflow Rollup", "by_workflow"),
        ("Platform Rollup", "by_platform"),
        ("Time Window Rollup", "by_time_window"),
        ("Theme Rollup", "by_theme"),
    ]:
        lines.extend([f"## {title}", ""])
        rows = ((rollup_payload.get("rollups") or {}).get(key)) or []
        if not rows:
            lines.append("No rows available.")
            lines.append("")
            continue
        for row in rows[:8]:
            lines.append(
                f"- `{row.get('label')}`: `{row.get('post_count')}` posts | avg score `{row.get('avg_engagement_score')}` | avg rate `{row.get('avg_engagement_rate')}`"
            )
        lines.append("")

    lines.extend(["## Data Notes", ""])
    post_summary = post_payload.get("summary") or {}
    for status, count in sorted((post_summary.get("metric_status_counts") or {}).items()):
        lines.append(f"- Metric status `{status}`: `{count}`")
    malformed = int(post_summary.get("malformed_receipt_count") or 0)
    if malformed:
        lines.append(f"- Malformed receipts skipped: `{malformed}`")
    lines.append("")
    return "\n".join(lines)


def build_social_performance(*, window_days: int = 30, fetch_metrics: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
    post_payload, rollup_payload = build_social_performance_payload(window_days=window_days, fetch_metrics=fetch_metrics)
    write_json(STATE_PATH, post_payload)
    write_json(ROLLUPS_PATH, rollup_payload)
    write_json(OPERATOR_JSON_PATH, rollup_payload)
    write_markdown(OUTPUT_MD_PATH, render_social_insights_markdown(post_payload, rollup_payload))
    return post_payload, rollup_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the observe-only social performance collector.")
    parser.add_argument("--window-days", type=int, default=30, help="Number of trailing days to include.")
    parser.add_argument("--skip-fetch", action="store_true", help="Build from receipts only without calling Meta.")
    args = parser.parse_args()

    _ensure_duckagent_python()
    post_payload, rollup_payload = build_social_performance(window_days=max(1, args.window_days), fetch_metrics=not args.skip_fetch)
    print(
        {
            "generated_at": post_payload.get("generated_at"),
            "post_count": (post_payload.get("summary") or {}).get("normalized_post_count"),
            "metrics_coverage_pct": (rollup_payload.get("summary") or {}).get("metrics_coverage_pct"),
        }
    )


if __name__ == "__main__":
    main()
