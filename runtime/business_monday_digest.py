"""Monday business digest (Surface 15.5): one email that composes the
weekly info-surfaces (profit, reviews, recommendations, learnings,
competitors, ...) instead of ~8 separate Monday emails.

When DUCK_EMAIL_DIGEST_MODE=1, the individual surfaces fold into this digest
via email_cadence_gate (their anomaly bypasses still fire same-day). This
producer reads the cheap state files each surface already writes
(producer/reader convention) and sends ONE gated email.

Fail-soft per section: a missing/malformed state file yields a "no data"
section, never a crash — and the section's `_status` surfaces loudly so an
empty section doesn't masquerade as healthy [[plausible-fallbacks-mask-failure]].
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from email_cadence_gate import log_cadence_decision, should_send_email
from governance_review_common import DUCK_OPS_ROOT, now_local_iso

DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
STATE_DIR = DUCK_OPS_ROOT / "state"


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        import json
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _section(title: str, builder: Callable[[], tuple[str, str]]) -> dict[str, Any]:
    """builder returns (status, html_body). Any exception → degraded section."""
    try:
        status, body = builder()
    except Exception as exc:  # fail-soft per section
        return {"title": title, "status": "error", "body": f"section failed: {exc}"}
    return {"title": title, "status": status, "body": body or "No data this week."}


def build_digest_sections() -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    def profit() -> tuple[str, str]:
        d = _load_json(STATE_DIR / "profit_intel.json")
        if not d:
            return "empty", "No profit intel available."
        y = d.get("yesterday") or {}
        t = d.get("trend_7d") or {}
        return "ok", (f"Yesterday: {y.get('orders','?')} orders, "
                      f"${y.get('revenue','?')} revenue, {round(y.get('margin',0),1)}% margin. "
                      f"7-day net delta: {t.get('net_delta_pct','?')}%.")

    def recommendations() -> tuple[str, str]:
        d = _load_json(STATE_DIR / "weekly_strategy_recommendation_packet.json")
        if not d:
            return "empty", "No strategy packet available."
        s = d.get("summary") or {}
        recs = d.get("recommendations") or []
        occ = d.get("occasion_nominations") or []
        return "ok", (f"{s.get('recommendation_count', len(recs))} recommendations, "
                      f"{s.get('watchout_count', 0)} watchouts, "
                      f"{len(occ)} active occasion window(s).")

    def learnings() -> tuple[str, str]:
        d = _load_json(STATE_DIR / "current_learnings.json")
        if not d:
            return "empty", "No learnings available."
        items = d.get("learnings") or d.get("items") or []
        return "ok", f"{len(items)} current learnings tracked."

    def competitors() -> tuple[str, str]:
        d = _load_json(STATE_DIR / "competitor_social_benchmark.json")
        if not d:
            return "empty", "No competitor benchmark available."
        return "ok", "Competitor social benchmark refreshed this week."

    sections.append(_section("Profit", profit))
    sections.append(_section("Strategy & Recommendations", recommendations))
    sections.append(_section("Learnings", learnings))
    sections.append(_section("Competitors", competitors))
    return sections


def render_digest_html(sections: list[dict[str, Any]]) -> tuple[str, str]:
    import html as html_lib
    rows = []
    text_lines = ["myJeepDuck — Monday Business Digest", ""]
    for sec in sections:
        badge = {"ok": "🟢", "empty": "🟡", "error": "🔴"}.get(sec["status"], "⚪")
        rows.append(
            f"<div style='margin:0 0 14px;'><h3 style='margin:0 0 4px;'>"
            f"{badge} {html_lib.escape(sec['title'])}</h3>"
            f"<p style='margin:0;color:#333;'>{html_lib.escape(sec['body'])}</p></div>"
        )
        text_lines.append(f"{badge} {sec['title']}: {sec['body']}")
    html = (f"<div style='font-family:-apple-system,sans-serif;max-width:640px;'>"
            f"<h2>Monday Business Digest</h2>{''.join(rows)}"
            f"<p style='color:#999;font-size:12px;'>Folds the weekly info-emails into one. "
            f"Anomalies still send same-day.</p></div>")
    return html, "\n".join(text_lines)


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
        print(html[:400])
        return 0

    decision = should_send_email("business_digest", summary)
    log_cadence_decision("business_digest", decision)
    if not (decision.should_send or args.force):
        print(f"[digest] cadence gate deferred: {decision.reason}")
        return 0

    send_email = _ensure_send_email()
    send_email("myJeepDuck — Monday Business Digest", html, text)
    print("[digest] sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
