"""Monday business digest (Surface 15.5, rebuilt Surface 66.3): one email that
composes the weekly info-surfaces (profit, reviews, recommendations, learnings,
competitors, ...) instead of ~8 separate Monday emails.

When DUCK_EMAIL_DIGEST_MODE=1, the individual surfaces fold into this digest
via email_cadence_gate (their anomaly bypasses still fire same-day). This
producer reads the cheap state files each surface already writes
(producer/reader convention) and sends ONE gated email.

Fold contract: every surface in email_cadence_gate.DIGEST_FOLDED_SURFACES has
a builder in DIGEST_SECTION_BUILDERS. A fold reason naming a destination with
no consumer is a silent drop, not a defer — that is how the 23-item Shopify
SEO batch vanished twice. test_business_monday_digest pins the two sets equal.

Fail-soft per section: a missing/malformed state file yields a "no data"
section, never a crash — and the section's `_status` surfaces loudly so an
empty section doesn't masquerade as healthy [[plausible-fallbacks-mask-failure]].
"""
from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from email_cadence_gate import DIGEST_FOLDED_SURFACES, log_cadence_decision, should_send_email
from governance_review_common import DUCK_OPS_ROOT, now_local_iso

DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
STATE_DIR = DUCK_OPS_ROOT / "state"
OUTPUT_DIGESTS_DIR = DUCK_OPS_ROOT / "output" / "digests"
COMPETITOR_REPORTS_DIR = DUCK_AGENT_ROOT / "cache" / "competitor" / "reports"
DUCK_AGENT_RUNS_DIR = DUCK_AGENT_ROOT / "runs"
PORTAL_DECISIONS_URL = "http://127.0.0.1:8765/portal/decisions"

# A weekly_monday deferral older than one cadence window means the batch was
# never surfaced anywhere — red, not "waiting".
DEFERRED_BATCH_STALE_DAYS = 7
GENERIC_THEME_TOKENS = {"duck", "ducks", "the", "a", "an", "of", "and", "for", "3d", "printed", "figurine"}


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _parse_dt(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed


def _age_days(value: Any, *, now: datetime | None = None) -> float | None:
    stamp = _parse_dt(value)
    if stamp is None:
        return None
    current = now or datetime.now().astimezone()
    return (current - stamp).total_seconds() / 86400.0


def _pct(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "?"
    return f"{number:+.1f}%"


def _money(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "$?"


def _section(title: str, builder: Callable[[], tuple[str, str]], *, omit_when_empty: bool = False) -> dict[str, Any]:
    """builder returns (status, body). Any exception → degraded section."""
    try:
        status, body = builder()
    except Exception as exc:  # fail-soft per section
        return {"title": title, "status": "error", "body": f"section failed: {exc}", "omit_when_empty": omit_when_empty}
    return {"title": title, "status": status, "body": body or "No data this week.", "omit_when_empty": omit_when_empty}


# ── Section builders ────────────────────────────────────────────────


def build_profit_section() -> tuple[str, str]:
    d = _load_json(STATE_DIR / "profit_intel.json")
    if not d:
        return "empty", "No profit intel available."
    y = d.get("yesterday") or {}
    t = d.get("trend_7d") or {}
    lines = [
        f"Yesterday: {y.get('orders', '?')} orders, {_money(y.get('revenue'))} revenue, "
        f"{round(float(y.get('margin') or 0), 1)}% margin."
    ]
    per_day = [row for row in (d.get("per_day") or []) if isinstance(row, dict)]
    per_day.sort(key=lambda row: str(row.get("date") or ""))
    if len(per_day) >= 14:
        this_week = per_day[-7:]
        last_week = per_day[-14:-7]
        rev_now = sum(float(row.get("revenue") or 0) for row in this_week)
        rev_prev = sum(float(row.get("revenue") or 0) for row in last_week)
        orders_now = sum(int(row.get("orders") or 0) for row in this_week)
        orders_prev = sum(int(row.get("orders") or 0) for row in last_week)
        rev_delta = ((rev_now - rev_prev) / rev_prev * 100.0) if rev_prev else None
        lines.append(
            f"Last 7 days: {orders_now} orders / {_money(rev_now)} vs {orders_prev} orders / {_money(rev_prev)} the week before"
            + (f" ({_pct(rev_delta)} revenue)." if rev_delta is not None else ".")
        )
    else:
        lines.append(
            f"7-day trend: orders {_pct(t.get('orders_delta_pct'))}, net {_pct(t.get('net_delta_pct'))}, "
            f"margin {_pct(t.get('margin_delta_pct'))}."
        )
    mix = d.get("channel_mix_7d") or {}
    if isinstance(mix, dict) and mix:
        channel_bits = []
        for channel in ("shopify", "etsy"):
            orders = mix.get(f"{channel}_orders")
            revenue = mix.get(f"{channel}_revenue")
            if orders is None and revenue is None:
                continue
            channel_bits.append(f"{channel.title()} {int(orders or 0)} orders / {_money(revenue or 0)}")
        if channel_bits:
            lines.append("Channel mix (7d): " + ", ".join(channel_bits) + ".")
    anomaly = d.get("anomaly") or {}
    if anomaly.get("triggered"):
        reasons = "; ".join(str(r) for r in (anomaly.get("reasons") or [])[:2])
        lines.append(f"⚠️ Anomaly flagged: {reasons or 'see /portal/intel/profit'}.")
    return "ok", " ".join(lines)


def _seo_decision_lines(now: datetime) -> tuple[list[str], bool]:
    latest = _load_json(STATE_DIR / "shopify_seo_review" / "latest.json")
    if not latest:
        return [], False
    status = str(latest.get("status") or "")
    label = str(latest.get("category_label") or latest.get("seo_category") or "Shopify SEO")
    count = int(latest.get("item_count") or 0)
    subject = str(latest.get("email_subject") or "")
    if status == "awaiting_review":
        age = _age_days(latest.get("emailed_at") or latest.get("generated_at"), now=now)
        age_text = f", waiting {age:.0f}d" if age is not None else ""
        return [
            f"Shopify SEO — {label}: {count} item(s){age_text}. Reply `apply` to the email "
            f"\"{subject or 'MJD: [shopify_seo] …'}\" (approving chains to the next category automatically)."
        ], False
    if status == "deferred_by_cadence":
        age = _age_days(latest.get("deferred_since") or latest.get("generated_at"), now=now)
        stale = age is not None and age > DEFERRED_BATCH_STALE_DAYS
        age_text = f"{age:.0f}d" if age is not None else "unknown age"
        if stale:
            return [
                f"🔴 Shopify SEO — {label}: {count} item(s) deferred {age_text} and never emailed — "
                f"the kickoff should resend it; if this line persists next Monday the lane is stuck."
            ], True
        return [f"Shopify SEO — {label}: {count} item(s) deferred {age_text}; sends with the next Monday kickoff."], False
    return [], False


def _quality_gate_decision_lines(now: datetime) -> list[str]:
    state = _load_json(STATE_DIR / "quality_gate_state.json") or {}
    artifacts = (state.get("artifacts") or {}).values()
    pending: list[dict[str, Any]] = []
    for record in artifacts:
        decision = (record or {}).get("decision") or {}
        if str(decision.get("review_status") or "") == "pending":
            pending.append(decision)
    if not pending:
        return []
    pending.sort(key=lambda d: str(d.get("created_at") or ""))
    samples = []
    for decision in pending[:3]:
        age = _age_days(decision.get("created_at"), now=now)
        flow = str(decision.get("flow") or decision.get("artifact_type") or "item").replace("_", " ")
        samples.append(f"{flow} ({age:.0f}d)" if age is not None else flow)
    return [f"Review inbox: {len(pending)} item(s) waiting — {', '.join(samples)} → {PORTAL_DECISIONS_URL}"]


def _trend_decision_lines() -> list[str]:
    queue = _load_json(STATE_DIR / "review_queue.json") or {}
    count = int(queue.get("pending_count") or 0)
    if not count:
        return []
    return [f"Trend candidates: {count} surfaced for a build/promote call → {PORTAL_DECISIONS_URL}"]


def build_decisions_section() -> tuple[str, str]:
    now = datetime.now().astimezone()
    seo_lines, seo_stale = _seo_decision_lines(now)
    lines = seo_lines + _quality_gate_decision_lines(now) + _trend_decision_lines()
    if not lines:
        return "ok", "Nothing is waiting on you this week."
    status = "error" if seo_stale else "attention"
    return status, "\n".join(f"• {line}" for line in lines)


def build_recommendations_section() -> tuple[str, str]:
    d = _load_json(STATE_DIR / "weekly_strategy_recommendation_packet.json")
    if not d:
        return "empty", "No strategy packet available."
    s = d.get("summary") or {}
    recs = [r for r in (d.get("recommendations") or []) if isinstance(r, dict)]
    occ = d.get("occasion_nominations") or []
    watchouts = [w for w in (d.get("watchouts") or []) if isinstance(w, (dict, str))]
    order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    recs.sort(key=lambda r: order.get(str(r.get("priority") or "P9"), 9))
    lines = [
        f"{s.get('recommendation_count', len(recs))} recommendations, {s.get('watchout_count', len(watchouts))} watchouts, "
        f"{len(occ)} active occasion window(s)."
    ]
    for rec in recs[:2]:
        title = str(rec.get("title") or "").replace("`", "").strip()
        action = str(rec.get("recommendation") or "").strip()
        if title:
            lines.append(f"• {rec.get('priority', '')} {title}: {action}".strip())
    for watch in watchouts[:1]:
        text = watch if isinstance(watch, str) else str(watch.get("title") or watch.get("watchout") or watch.get("recommendation") or "")
        if text:
            lines.append(f"• Watchout: {text}")
    return "ok", "\n".join(lines)


def build_learnings_section() -> tuple[str, str]:
    d = _load_json(STATE_DIR / "current_learnings.json")
    if not d:
        return "empty", "No learnings available."
    notifier = d.get("change_notifier") or {}
    changes = [c for c in (d.get("changes_since_previous") or []) if isinstance(c, dict)]
    attention = int(notifier.get("attention_change_count") or 0)
    if not changes and not attention:
        return "empty", "No learning changes this week."
    lines = [str(notifier.get("headline") or f"{len(changes)} learning change(s) since last week.")]
    for change in changes[:2]:
        headline = str(change.get("headline") or "").strip()
        if headline:
            lines.append(f"• {headline}")
    return "ok", "\n".join(lines)


def _latest_full_competitor_report() -> dict[str, Any] | None:
    """Newest report that carries a real analysis — the daily runs write
    `_snapshot_only` stubs with empty trending/rising lists."""
    if not COMPETITOR_REPORTS_DIR.exists():
        return None
    paths = sorted(COMPETITOR_REPORTS_DIR.glob("*_competitor_report.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in paths:
        data = _load_json(path)
        if not data or (data.get("ai_insights") or {}).get("_snapshot_only"):
            continue
        if data.get("trending_products") or data.get("rising_competitors"):
            return data
    return None


def build_competitors_section() -> tuple[str, str]:
    report = _latest_full_competitor_report()
    benchmark = _load_json(STATE_DIR / "competitor_social_benchmark.json")
    if not report and not benchmark:
        return "empty", "No competitor benchmark available."
    lines: list[str] = []
    if report:
        rising = [r for r in (report.get("rising_competitors") or []) if isinstance(r, dict)]
        rising.sort(key=lambda r: float(r.get("exact_sold_7d") or 0), reverse=True)
        if rising:
            top = rising[0]
            mine = report.get("my_shop_velocity") or {}
            my_actual = mine.get("actual_sold_7d") or mine.get("sold_units_7d_actual")
            mine_text = f" (you: {int(my_actual)})" if my_actual not in (None, "") else ""
            lines.append(
                f"Fastest competitor: {top.get('shop_name')} sold {int(top.get('exact_sold_7d') or 0)} units in 7d{mine_text}, "
                f"momentum {float(top.get('momentum_score') or 0):.0f}."
            )
        ranks = []
        for metric in report.get("comparison_metrics") or []:
            if not isinstance(metric, dict) or not metric.get("my_rank"):
                continue
            ranks.append(
                f"{str(metric.get('keyword_category') or '').replace('_', ' ')} #{metric.get('my_rank')}/{metric.get('total_competitors')}"
            )
        if ranks:
            lines.append("Your rank by category: " + ", ".join(ranks[:4]) + ".")
        gaps = [g for g in (report.get("category_gaps") or []) if isinstance(g, dict)]
        gaps.sort(key=lambda g: int(g.get("competitor_listings") or 0) - int(g.get("my_listings") or 0), reverse=True)
        if gaps:
            gap = gaps[0]
            lines.append(
                f"Biggest catalog gap: {str(gap.get('category') or '').replace('_', ' ')} — "
                f"competitors {gap.get('competitor_listings')} listings vs your {gap.get('my_listings')}."
            )
    if benchmark:
        summary = benchmark.get("summary") or {}
        if summary.get("post_count"):
            lines.append(f"Social benchmark: {summary.get('post_count')} competitor posts across {summary.get('account_count')} accounts refreshed.")
    return ("ok", "\n".join(lines)) if lines else ("empty", "Competitor report has no analysis this week.")


_SOLD_RE = re.compile(r"sold 7d `(?P<s7>\d+)`(?:, sold 30d `(?P<s30>\d+)`)?")


def _theme_core_tokens(theme: str) -> set[str]:
    return {tok for tok in re.findall(r"[a-z0-9]+", str(theme or "").lower()) if tok not in GENERIC_THEME_TOKENS}


def _competitor_corroboration(theme: str, report: dict[str, Any] | None) -> str:
    if not report:
        return ""
    core = _theme_core_tokens(theme)
    if not core:
        return ""
    pools = [
        ("trending", report.get("trending_products") or []),
        ("build list", report.get("ducks_to_build") or []),
    ]
    for label, pool in pools:
        for product in pool:
            if not isinstance(product, dict):
                continue
            title_tokens = _theme_core_tokens(str(product.get("title") or ""))
            if core <= title_tokens:
                return f"also on {product.get('shop_name')}'s {label} (\"{str(product.get('title') or '')[:50]}\")"
    return ""


def build_build_next_section() -> tuple[str, str]:
    paths = sorted(glob.glob(str(OUTPUT_DIGESTS_DIR / "trend_digest__*.json")))
    digest = _load_json(Path(paths[-1])) if paths else None
    if not digest:
        return "empty", "No trend digest available."
    items = [i for i in (digest.get("items") or []) if isinstance(i, dict)]
    builds = [i for i in items if str(i.get("action_frame") or "") in {"build", "promote"}]
    if not builds:
        return "empty", "No build-worthy trend surfaced this week."
    report = _latest_full_competitor_report()
    lines = []
    for item in builds[:3]:
        theme = str(item.get("theme") or item.get("title") or "")
        reasoning = " ".join(str(r) for r in (item.get("reasoning") or []))
        match = _SOLD_RE.search(reasoning)
        evidence = ""
        if match:
            evidence = f"competitors sold {match.group('s7')} in 7d"
            if match.group("s30"):
                evidence += f" / {match.group('s30')} in 30d"
        metadata = item.get("trend_metadata") if isinstance(item.get("trend_metadata"), dict) else {}
        catalog = str(metadata.get("catalog_status") or "")
        corroboration = _competitor_corroboration(theme, report)
        parts = [p for p in (evidence, f"catalog {catalog}" if catalog else "", corroboration) if p]
        lines.append(f"• {item.get('action_frame')}: {theme} — " + ("; ".join(parts) if parts else "see trend digest"))
    return "ok", "\n".join(lines)


def build_reviews_section() -> tuple[str, str]:
    now = datetime.now().astimezone()
    window_start = now - timedelta(days=7)
    if not DUCK_AGENT_RUNS_DIR.exists():
        return "empty", "No review runs available."
    seen: dict[str, dict[str, Any]] = {}
    replies_drafted = 0
    day_dirs = sorted(DUCK_AGENT_RUNS_DIR.glob("20??-??-??"))[-9:]
    for day_dir in day_dirs:
        state = _load_json(day_dir / "state_reviews.json")
        if not state:
            continue
        for review in state.get("reviews_data") or []:
            if not isinstance(review, dict):
                continue
            try:
                created = datetime.fromtimestamp(int(review.get("create_timestamp") or 0)).astimezone()
            except (TypeError, ValueError, OSError, OverflowError):
                continue
            if created < window_start:
                continue
            key = str(review.get("transaction_id") or review.get("review_id") or id(review))
            seen[key] = review
        handoff = state.get("reviews_reply_handoff") or {}
        if _age_days(handoff.get("generated_at"), now=now) is not None and _age_days(handoff.get("generated_at"), now=now) <= 7:
            replies_drafted += len(handoff.get("replies") or [])
    if not seen:
        return "empty", "No new Etsy reviews in the last 7 days."
    ratings = [int(r.get("rating") or 0) for r in seen.values()]
    low = sum(1 for r in ratings if 0 < r <= 2)
    avg = sum(ratings) / len(ratings) if ratings else 0.0
    line = f"{len(seen)} new review(s) this week, average {avg:.1f}★; {replies_drafted} public thank-you reply(ies) drafted for auto-posting."
    if low:
        line += f" ⚠️ {low} low rating(s) — handled privately (see the same-day alert email)."
    return ("attention" if low else "ok"), line


def build_engineering_governance_section() -> tuple[str, str]:
    d = _load_json(STATE_DIR / "engineering_governance_digest.json")
    if not d:
        return "empty", "No engineering governance digest available."
    health = d.get("health_summary") or {}
    recs = d.get("review_recommendation_summary") or {}
    label = str(health.get("overall_label") or health.get("overall_status") or "").strip()
    if not label and not recs.get("count"):
        return "empty", "No engineering findings this week."
    status = "attention" if str(health.get("overall_status") or "") in {"bad", "degraded"} else "ok"
    return status, f"Machinery: {label or 'unknown'}. {int(recs.get('count') or 0)} review recommendation(s), {int(recs.get('top_priority_count') or 0)} top-priority."


def build_business_intelligence_section() -> tuple[str, str]:
    d = _load_json(DUCK_AGENT_ROOT / "cache" / "weekly_insights.json")
    if not d:
        return "empty", "No weekly insights available."
    items = d.get("action_items") or d.get("insights") or d.get("items") or []
    if isinstance(d, dict) and not items:
        return "empty", "No weekly insight action items."
    lines = []
    for item in list(items)[:2]:
        text = item if isinstance(item, str) else str((item or {}).get("title") or (item or {}).get("insight") or (item or {}).get("action") or "")
        if text:
            lines.append(f"• {text[:160]}")
    return ("ok", "\n".join(lines)) if lines else ("empty", "No weekly insight action items.")


# Surface name → (title, builder, omit_when_empty). Every DIGEST_FOLDED_SURFACES
# member MUST appear here (test-pinned); extra digest-only sections are fine.
DIGEST_SECTION_BUILDERS: dict[str, tuple[str, Callable[[], tuple[str, str]], bool]] = {
    "profit": ("Profit", build_profit_section, False),
    "decisions": ("Decisions waiting on you", build_decisions_section, False),
    "build_next": ("Build next (evidence)", build_build_next_section, False),
    "competitors": ("Competitors", build_competitors_section, False),
    "reviews": ("Reviews", build_reviews_section, False),
    "recommendations": ("Strategy & Recommendations", build_recommendations_section, False),
    "learnings": ("Learnings", build_learnings_section, True),
    "engineering_governance": ("Machinery health", build_engineering_governance_section, True),
    "business_intelligence": ("Weekly insights", build_business_intelligence_section, True),
}


def fold_contract_gaps() -> set[str]:
    """Folded surfaces with no digest section — must be empty (test-pinned)."""
    return set(DIGEST_FOLDED_SURFACES) - set(DIGEST_SECTION_BUILDERS)


def build_digest_sections() -> list[dict[str, Any]]:
    gaps = fold_contract_gaps()
    if gaps:
        print(f"[digest] FOLD CONTRACT BROKEN: folded surfaces with no digest section: {sorted(gaps)}", file=sys.stderr)
    sections: list[dict[str, Any]] = []
    for title, builder, omit_when_empty in DIGEST_SECTION_BUILDERS.values():
        sections.append(_section(title, builder, omit_when_empty=omit_when_empty))
    return sections


def render_digest_html(sections: list[dict[str, Any]]) -> tuple[str, str]:
    import html as html_lib
    rows = []
    text_lines = ["myJeepDuck — Monday Business Digest", ""]
    badge_for = {"ok": "🟢", "attention": "🟡", "empty": "🟡", "error": "🔴"}
    for sec in sections:
        if sec.get("omit_when_empty") and sec["status"] == "empty":
            continue
        badge = badge_for.get(sec["status"], "⚪")
        body_html = "<br>".join(html_lib.escape(line) for line in str(sec["body"]).splitlines()) or html_lib.escape(str(sec["body"]))
        rows.append(
            f"<div style='margin:0 0 14px;'><h3 style='margin:0 0 4px;'>"
            f"{badge} {html_lib.escape(sec['title'])}</h3>"
            f"<p style='margin:0;color:#333;line-height:1.45;'>{body_html}</p></div>"
        )
        text_lines.append(f"{badge} {sec['title']}:")
        text_lines.extend(f"  {line}" for line in str(sec["body"]).splitlines())
        text_lines.append("")
    html = (f"<div style='font-family:-apple-system,sans-serif;max-width:640px;'>"
            f"<h2>Monday Business Digest</h2>{''.join(rows)}"
            f"<p style='color:#999;font-size:12px;'>Folds the weekly info-emails into one. "
            f"Approval lanes (SEO, design briefs, videos) still send their own reply-to-approve emails; "
            f"anomalies and low ratings still send same-day.</p></div>")
    return html, "\n".join(text_lines).rstrip() + "\n"


def _ensure_send_email():
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
    except Exception:
        pass
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    from helpers.email_helper import send_email  # type: ignore
    return send_email


def main() -> int:
    parser = argparse.ArgumentParser(description="Monday business digest")
    parser.add_argument("--send-email", action="store_true")
    parser.add_argument("--force", action="store_true", help="ignore the Monday cadence gate")
    args = parser.parse_args()

    sections = build_digest_sections()
    html, text = render_digest_html(sections)
    summary = {"generated_at": now_local_iso(),
               "section_count": len(sections),
               "empty_sections": [s["title"] for s in sections if s["status"] != "ok"]}
    print(f"[digest] built {len(sections)} sections; degraded: {summary['empty_sections']}")

    if not args.send_email:
        print(text)
        return 0

    decision = should_send_email("business_digest", summary)
    # decision already carries surface_name; log_cadence_decision takes the
    # CadenceDecision as its only positional arg (passing a 2nd crashed the
    # job every Monday — it never sent since Surface 15.5 shipped).
    log_cadence_decision(decision)
    if not (decision.should_send or args.force):
        print(f"[digest] cadence gate deferred: {decision.reason}")
        return 0

    send_email = _ensure_send_email()
    send_email("myJeepDuck — Monday Business Digest", html, text)
    print("[digest] sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
