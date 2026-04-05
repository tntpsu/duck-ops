#!/usr/bin/env python3
"""
Read-only Etsy review-reply discovery packet generator.

This script does not submit anything. It:
- selects a review-reply quality-gate artifact
- writes a discovery packet with the exact reply text and target metadata
- optionally opens Etsy in a headed Playwright session
- captures screenshot/snapshot/body-text evidence without typing or submitting
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from decision_writer import ensure_parent, load_output_patterns, render_pattern, slugify


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
STATE_DIR = ROOT / "state"
QUALITY_GATE_STATE_PATH = STATE_DIR / "quality_gate_state.json"
DISCOVERY_SESSION_STATE_PATH = STATE_DIR / "review_reply_discovery_sessions.json"
DISCOVERY_CONFIG_PATH = CONFIG_DIR / "review_reply_discovery.json"
DEFAULT_ETSY_REVIEWS_URL = "https://www.etsy.com/your/shops/me/dashboard"
PWCLI_PATH = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills" / "playwright" / "scripts" / "playwright_cli.sh"


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def load_discovery_config() -> dict[str, Any]:
    return load_json(
        DISCOVERY_CONFIG_PATH,
        {
            "version": 1,
            "entry_points": [
                {
                    "id": "seller_dashboard",
                    "url": DEFAULT_ETSY_REVIEWS_URL,
                    "notes": "Preferred signed-in seller entry point.",
                }
            ],
            "signals": {
                "sign_in_url_fragments": ["/signin"],
                "not_found_text_fragments": ["uh oh!", "page you were looking for was not found"],
                "review_text_fragments": ["reviews", "shop reviews", "feedback"],
            },
        },
    )


def save_json(path: Path, payload: Any) -> None:
    ensure_parent(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_session_state() -> dict[str, Any]:
    return load_json(DISCOVERY_SESSION_STATE_PATH, {"sessions": {}})


def save_session_state(payload: dict[str, Any]) -> None:
    save_json(DISCOVERY_SESSION_STATE_PATH, payload)


def select_record(state: dict[str, Any], artifact_id: str | None) -> dict[str, Any]:
    artifacts = state.get("artifacts") or {}
    if artifact_id:
        record = artifacts.get(artifact_id)
        if not record:
            raise SystemExit(f"Unknown artifact_id: {artifact_id}")
        return record

    candidates = []
    for record in artifacts.values():
        decision = record.get("decision") or {}
        if decision.get("flow") != "reviews_reply_positive":
            continue
        candidates.append(record)
    if not candidates:
        raise SystemExit("No public review-reply artifacts found.")
    candidates.sort(
        key=lambda item: (
            (item.get("decision") or {}).get("run_id") or "",
            (item.get("decision") or {}).get("created_at") or "",
        ),
        reverse=True,
    )
    return candidates[0]


def write_packet(packet: dict[str, Any]) -> dict[str, str]:
    patterns = load_output_patterns()
    replacements = {
        "run_id": packet.get("run_id") or "unknown",
        "artifact_slug": packet.get("artifact_slug") or slugify(packet.get("artifact_id") or "artifact"),
    }
    json_path = render_pattern(patterns["discovery_json"], replacements)
    md_path = render_pattern(patterns["discovery_md"], replacements)

    ensure_parent(json_path).write_text(json.dumps(packet, indent=2), encoding="utf-8")

    lines = [
        "# Review Reply Discovery Packet",
        "",
        f"- Generated at: `{packet['generated_at']}`",
        f"- Artifact: `{packet['artifact_id']}`",
        f"- Decision: `{packet['decision']}`",
        f"- Review status: `{packet.get('review_status', 'pending')}`",
        f"- Discovery mode: `{packet['discovery_mode']}`",
        f"- Submit guard: `{packet['submit_guard']}`",
        "",
        "## Review Target",
        "",
    ]
    target = packet.get("review_target") or {}
    for label, key in (
        ("Shop ID", "shop_id"),
        ("Review key", "review_key"),
        ("Review ID", "review_id"),
        ("Transaction ID", "transaction_id"),
        ("Listing ID", "listing_id"),
        ("Review URL", "review_url"),
        ("Match quality", "match_quality"),
        ("Review date", "review_date"),
    ):
        lines.append(f"- {label}: `{target.get(key)}`")

    lines.extend(
        [
            "",
            "## Review Context",
            "",
            f"- Customer review: {packet.get('customer_review') or 'n/a'}",
            f"- Approved reply text: {packet.get('approved_reply_text') or 'n/a'}",
            "",
            "## Browser Evidence",
            "",
        ]
    )
    browser = packet.get("browser_capture") or {}
    lines.append(f"- Start URL: `{browser.get('start_url')}`")
    lines.append(f"- Landed URL: `{browser.get('landed_url')}`")
    lines.append(f"- Page title: `{browser.get('page_title')}`")
    lines.append(f"- Targeting strategy: `{browser.get('targeting_strategy')}`")
    lines.append(f"- Review text visible: `{browser.get('review_text_visible')}`")
    lines.append(f"- Exact review located: `{browser.get('exact_review_located')}`")
    lines.append(f"- Reply box visible: `{browser.get('reply_box_visible')}`")
    lines.append(f"- Reply textarea placeholder: `{browser.get('reply_textarea_placeholder')}`")
    lines.append(f"- Matched transaction ID: `{browser.get('matched_transaction_id')}`")
    lines.append(f"- Transaction ID verified: `{browser.get('transaction_id_verified')}`")
    lines.append(f"- Matched listing ID: `{browser.get('matched_listing_id')}`")
    lines.append(f"- Matched listing title: `{browser.get('matched_listing_title')}`")
    lines.append(f"- Listing ID verified: `{browser.get('listing_id_verified')}`")
    lines.append(f"- Candidate blocks containing the review text: `{browser.get('candidate_count')}`")
    if browser.get("screenshot_path"):
        lines.append(f"- Screenshot: `{browser['screenshot_path']}`")
    if browser.get("review_block_excerpt"):
        lines.extend(["", "### Exact Review Block", "", "```text", browser["review_block_excerpt"], "```"])
    if browser.get("reply_controls"):
        lines.extend(["", "### Nearby Reply Controls", ""])
        for control in browser["reply_controls"][:12]:
            lines.append(f"- `{control.get('text') or control.get('ariaLabel')}`")
    if browser.get("snapshot_excerpt"):
        snapshot_excerpt = browser["snapshot_excerpt"].replace("```yaml", "").replace("```", "").strip()
        lines.extend(["", "### Snapshot Excerpt", "", "```yaml", snapshot_excerpt, "```"])
    if browser.get("notes"):
        lines.extend(["", "### Notes", ""])
        lines.extend(f"- {note}" for note in browser["notes"])
    probes = packet.get("path_probes") or []
    if probes:
        lines.extend(["", "## Path Probes", ""])
        for probe in probes:
            lines.append(f"- `{probe.get('id')}` -> `{probe.get('landed_url') or probe.get('start_url')}`")
            lines.append(
                f"  sign_in_required=`{probe.get('sign_in_required')}` not_found=`{probe.get('not_found')}` review_signal_visible=`{probe.get('review_signal_visible')}`"
            )

    ensure_parent(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def run_pw_command(session: str, *args: str) -> str:
    if not PWCLI_PATH.exists():
        raise SystemExit(f"Playwright wrapper not found at {PWCLI_PATH}")
    cmd = [str(PWCLI_PATH), "--session", session, *args]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def session_is_open(session: str) -> bool:
    try:
        run_pw_command(session, "snapshot")
        return True
    except subprocess.CalledProcessError as exc:
        combined = f"{exc.stdout}\n{exc.stderr}".lower()
        if "is not open" in combined:
            return False
        return False


def launch_browser_session(session: str, url: str) -> dict[str, Any]:
    log_dir = ROOT / "output" / "discovery" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{session}.log"
    handle = log_path.open("ab")
    process = subprocess.Popen(
        [str(PWCLI_PATH), "--session", session, "open", url, "--headed"],
        stdout=handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        cwd=str(Path.cwd()),
    )

    deadline = time.time() + 20
    last_error = None
    while time.time() < deadline:
        try:
            snapshot = run_pw_command(session, "snapshot")
            return {
                "session_name": session,
                "url": url,
                "pid": process.pid,
                "launched_at": now_iso(),
                "log_path": str(log_path),
                "ready": True,
                "snapshot_excerpt": "\n".join(snapshot.splitlines()[:20]),
            }
        except subprocess.CalledProcessError as exc:
            last_error = f"{exc.stdout}\n{exc.stderr}".strip()
            time.sleep(1)

    return {
        "session_name": session,
        "url": url,
        "pid": process.pid,
        "launched_at": now_iso(),
        "log_path": str(log_path),
        "ready": False,
        "error": last_error or "Timed out waiting for browser session to become ready.",
    }


def parse_page_metadata(snapshot_output: str) -> tuple[str | None, str | None]:
    url_match = re.search(r"Page URL:\s*(?P<url>\S+)", snapshot_output)
    title_match = re.search(r"Page Title:\s*(?P<title>.+)", snapshot_output)
    return (
        url_match.group("url").strip() if url_match else None,
        title_match.group("title").strip() if title_match else None,
    )


def parse_screenshot_path(output: str) -> str | None:
    match = re.search(r"\[Screenshot of viewport\]\((?P<path>[^)]+)\)", output)
    if not match:
        return None
    return match.group("path").strip()


def copy_relative_artifact(relative_path: str | None, destination_dir: Path) -> str | None:
    if not relative_path:
        return None
    source_path = Path(relative_path)
    if not source_path.is_absolute():
        source_path = Path.cwd() / source_path
    if not source_path.exists():
        return None
    destination = destination_dir / source_path.name
    ensure_parent(destination).write_bytes(source_path.read_bytes())
    return str(destination)


def latest_existing_screenshot(destination_dir: Path) -> str | None:
    if not destination_dir.exists():
        return None
    candidates = sorted(destination_dir.glob("*.png"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidates:
        return None
    return str(candidates[0])


def extract_eval_result(output: str) -> str:
    match = re.search(r"### Result\s*(?P<body>.*?)(?:\n### Ran Playwright code|\Z)", output, re.DOTALL)
    if not match:
        return output.strip()
    return match.group("body").strip()


def parse_eval_json(output: str) -> Any:
    body = extract_eval_result(output)
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return body


def review_surface_url(current_url: str | None) -> str | None:
    if not current_url:
        return None
    if "/shop/" in current_url and "/reviews" in current_url:
        return current_url
    if "/shop/" in current_url and "#reviews" in current_url:
        return current_url
    if "/shop/" in current_url:
        return current_url.split("#", 1)[0] + "#reviews"
    return None


def navigate_within_session(session: str, url: str, wait_seconds: float = 1.0) -> tuple[str | None, str | None]:
    run_pw_command(
        session,
        "eval",
        f"(() => {{ window.location.assign({json.dumps(url)}); return 'navigating'; }})()",
    )
    time.sleep(wait_seconds)
    snapshot_output = run_pw_command(session, "snapshot")
    return parse_page_metadata(snapshot_output)


def navigate_to_reviews_surface(session: str) -> dict[str, Any]:
    snapshot_output = run_pw_command(session, "snapshot")
    current_url, page_title = parse_page_metadata(snapshot_output)
    current_surface = review_surface_url(current_url)
    if current_surface:
        return {
            "strategy": "already_on_shop_reviews_surface",
            "start_url": current_url,
            "landed_url": current_surface,
            "page_title": page_title,
        }

    href_output = run_pw_command(
        session,
        "eval",
        (
            "(() => { "
            "const links = Array.from(document.querySelectorAll('a[href]')).map(node => node.href).filter(Boolean); "
            "const pagedReviews = links.find(href => href.includes('/shop/') && href.includes('/reviews')); "
            "const direct = links.find(href => href.includes('/shop/') && href.includes('#reviews')); "
            "const shop = links.find(href => href.includes('/shop/')); "
            "return pagedReviews || direct || (shop ? shop.split('#')[0] + '/reviews?ref=pagination&page=1' : null); "
            "})()"
        ),
    )
    href = parse_eval_json(href_output)
    if not isinstance(href, str) or not href.strip():
        return {
            "strategy": "no_review_surface_link_found",
            "start_url": current_url,
            "landed_url": current_url,
            "page_title": page_title,
        }

    landed_url, landed_title = navigate_within_session(session, href)
    return {
        "strategy": "navigate_via_shop_reviews_page" if "/reviews" in href and "#reviews" not in href else "navigate_via_shop_reviews_anchor",
        "start_url": current_url,
        "navigated_to": href,
        "landed_url": landed_url,
        "page_title": landed_title,
    }


def locate_review_block(
    session: str,
    customer_review: str,
    expected_listing_id: str | None = None,
    expected_transaction_id: str | None = None,
) -> dict[str, Any]:
    if not customer_review:
        return {"found": False, "reason": "missing_customer_review"}
    result = run_pw_command(
        session,
        "eval",
        (
            "(() => { "
            f"const needle = {json.dumps(customer_review)}; "
            f"const expectedListingId = {json.dumps(str(expected_listing_id) if expected_listing_id else None)}; "
            f"const expectedTransactionId = {json.dumps(str(expected_transaction_id) if expected_transaction_id else None)}; "
            "const byTransaction = expectedTransactionId "
            "  ? document.querySelector(`li[data-review-region=\"${expectedTransactionId}\"]`) "
            "    || document.querySelector(`[data-transaction-id=\"${expectedTransactionId}\"]`)?.closest('li, article, section, [data-review-id], [data-review]') "
            "  : null; "
            "const candidates = Array.from(document.querySelectorAll('body *')).filter(node => (node.innerText || '').includes(needle)); "
            "if (!candidates.length && !byTransaction) return {found:false}; "
            "const listingInfoFor = scope => { "
            "  const link = Array.from(scope.querySelectorAll('a[href]')).find(node => /\\/listing\\/(\\d+)/.test(node.href || '')); "
            "  const href = link ? (link.href || null) : null; "
            "  const match = href ? href.match(/\\/listing\\/(\\d+)/) : null; "
            "  return { "
            "    href, "
            "    listingId: match ? match[1] : null, "
            "    listingTitle: link ? ((link.innerText || '').trim() || (link.getAttribute('aria-label') || '').trim() || null) : null "
            "  }; "
            "}; "
            "const ranked = candidates.map(node => { "
            "  const scope = node.closest('li, article, section, [data-review-id], [data-review], .review-card') || node.parentElement || node; "
            "  const listing = listingInfoFor(scope); "
            "  const scopeText = (scope.innerText || '').trim(); "
            "  const reviewText = (node.innerText || '').trim(); "
            "  const reviewRegion = scope.getAttribute('data-review-region') || null; "
            "  return { "
            "    node, "
            "    scope, "
            "    scopeLen: scopeText.length, "
            "    reviewText, "
            "    listing, "
            "    reviewRegion, "
            "    transactionMatch: expectedTransactionId ? reviewRegion === expectedTransactionId : false, "
            "    exactText: reviewText === needle || scopeText.includes(needle), "
            "    listingMatch: expectedListingId ? listing.listingId === expectedListingId : false "
            "  }; "
            "}).sort((a, b) => "
            "  (Number(b.transactionMatch) - Number(a.transactionMatch)) || "
            "  (Number(b.listingMatch) - Number(a.listingMatch)) || "
            "  (Number(b.exactText) - Number(a.exactText)) || "
            "  (a.scopeLen - b.scopeLen)"
            "); "
            "const chosen = byTransaction ? (() => { "
            "  const scope = byTransaction.closest('li, article, section, [data-review-id], [data-review], .review-card') || byTransaction; "
            "  const listing = listingInfoFor(scope); "
            "  return { "
            "    node: scope, "
            "    scope, "
            "    scopeLen: ((scope.innerText || '').trim().length), "
            "    reviewText: (scope.innerText || '').trim(), "
            "    listing, "
            "    reviewRegion: scope.getAttribute('data-review-region') || null, "
            "    transactionMatch: true, "
            "    exactText: needle ? (scope.innerText || '').includes(needle) : false, "
            "    listingMatch: expectedListingId ? listing.listingId === expectedListingId : false "
            "  }; "
            "})() : ranked[0]; "
            "const el = chosen.node; "
            "const scope = chosen.scope; "
            "scope.scrollIntoView({block: 'center', inline: 'nearest'}); "
            "scope.setAttribute('data-openclaw-target-review', '1'); "
            "scope.style.outline = '3px solid #ff6a00'; "
            "scope.style.outlineOffset = '4px'; "
            "const fields = Array.from(scope.querySelectorAll('textarea,input')).map(node => ({"
            "tag: node.tagName,"
            "type: node.getAttribute('type'),"
            "placeholder: node.getAttribute('placeholder'),"
            "ariaLabel: node.getAttribute('aria-label'),"
            "valueLength: (node.value || '').length"
            "})); "
            "const controls = Array.from(scope.querySelectorAll('button,a')).map(node => ({"
            "text: (node.innerText || '').trim(),"
            "ariaLabel: node.getAttribute('aria-label')"
            "})).filter(item => item.text || item.ariaLabel); "
            "return {"
            "found: true,"
            "reviewText: (el.innerText || '').trim(),"
            "contextText: (scope.innerText || '').trim().slice(0, 1500),"
            "fields,"
            "controls,"
            "candidateCount: ranked.length,"
            "matchedTransactionId: chosen.reviewRegion,"
            "transactionIdVerified: expectedTransactionId ? chosen.reviewRegion === expectedTransactionId : null,"
            "matchedListingHref: chosen.listing.href,"
            "matchedListingId: chosen.listing.listingId,"
            "matchedListingTitle: chosen.listing.listingTitle,"
            "listingIdVerified: expectedListingId ? chosen.listing.listingId === expectedListingId : null,"
            "replyBoxVisible: fields.some(field => field.tag === 'TEXTAREA'),"
            "replyTextareaPlaceholder: (fields.find(field => field.tag === 'TEXTAREA') || {}).placeholder || null,"
            "submitVisible: controls.some(control => /post a public response/i.test(control.text || control.ariaLabel || '')),"
            "cancelVisible: controls.some(control => /cancel/i.test(control.text || control.ariaLabel || '')),"
            "contactBuyerVisible: controls.some(control => /contact buyer/i.test(control.text || control.ariaLabel || ''))"
            "}; "
            "})()"
        ),
    )
    parsed = parse_eval_json(result)
    return parsed if isinstance(parsed, dict) else {"found": False, "raw_result": parsed}


def capture_target_review_screenshot(session: str, destination_dir: Path) -> str | None:
    destination = destination_dir / f"target-review-{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%S-%fZ')}.png"
    run_pw_command(
        session,
        "run-code",
        (
            "const locator = page.locator('[data-openclaw-target-review=\"1\"]').first(); "
            "await locator.waitFor({ state: 'visible', timeout: 3000 }); "
            f"await locator.screenshot({{ path: {json.dumps(str(destination))} }}); "
            f"return {json.dumps(str(destination))};"
        ),
    )
    return str(destination) if destination.exists() else None


def score_probe(probe: dict[str, Any]) -> int:
    if probe.get("exact_review_located"):
        return 6
    if probe.get("review_text_visible"):
        return 5
    if probe.get("review_signal_visible"):
        return 4
    if probe.get("sign_in_required"):
        return 3
    if not probe.get("not_found"):
        return 2
    return 0


def build_probe(
    packet: dict[str, Any],
    entry: dict[str, Any],
    signals: dict[str, Any],
    destination_dir: Path,
    keep_browser_open: bool = False,
    session_name_override: str | None = None,
) -> dict[str, Any]:
    session_hash = hashlib.sha1(f"{packet['artifact_id']}::{entry['id']}".encode("utf-8")).hexdigest()[:4]
    session = session_name_override or entry.get("session_name") or f"r{session_hash}"
    probe: dict[str, Any] = {
        "id": entry.get("id"),
        "label": entry.get("notes"),
        "attempted": True,
        "session_name": session,
        "start_url": entry.get("url"),
        "landed_url": None,
        "page_title": None,
        "sign_in_required": False,
        "not_found": False,
        "review_signal_visible": False,
        "review_text_visible": False,
        "exact_review_located": False,
        "reply_box_visible": False,
        "reply_textarea_placeholder": None,
        "reply_controls": [],
        "review_block_excerpt": None,
        "matched_listing_id": None,
        "matched_listing_title": None,
        "matched_listing_href": None,
        "listing_id_verified": None,
        "candidate_count": None,
        "targeting_strategy": None,
        "body_text_excerpt": None,
        "snapshot_excerpt": None,
        "screenshot_path": None,
        "notes": [],
    }
    destination_dir = ROOT / "output" / "discovery" / "assets" / slugify(packet["artifact_id"])
    destination_dir.mkdir(parents=True, exist_ok=True)

    customer_review = (packet.get("customer_review") or "").strip()
    should_close_session = not keep_browser_open and not session_name_override
    try:
        reuse_current_session = bool(session_name_override and session_is_open(session))
        if reuse_current_session:
            snapshot_output = run_pw_command(session, "snapshot")
            current_url, current_title = parse_page_metadata(snapshot_output)
            probe["start_url"] = current_url
            probe["notes"].append("Reused the existing authenticated Etsy browser page instead of forcing a fresh open.")
        else:
            run_pw_command(session, "open", entry.get("url") or DEFAULT_ETSY_REVIEWS_URL, "--headed")
            snapshot_output = run_pw_command(session, "snapshot")
            current_url, current_title = parse_page_metadata(snapshot_output)
        landed_url, page_title = parse_page_metadata(snapshot_output)
        probe["landed_url"] = landed_url
        probe["page_title"] = page_title
        probe["snapshot_excerpt"] = "\n".join(snapshot_output.splitlines()[:40])

        body_output = run_pw_command(session, "eval", "document.body.innerText")
        body_text = extract_eval_result(body_output)
        lowered = body_text.lower()
        review_visible = customer_review.lower() in lowered if customer_review else False
        probe["review_text_visible"] = review_visible
        probe["sign_in_required"] = any(fragment in (landed_url or "").lower() for fragment in signals.get("sign_in_url_fragments", []))
        probe["not_found"] = any(fragment in lowered for fragment in signals.get("not_found_text_fragments", []))
        probe["review_signal_visible"] = any(fragment in lowered for fragment in signals.get("review_text_fragments", []))
        probe["body_text_excerpt"] = body_text[:1500]

        if not probe["sign_in_required"] and not probe["not_found"] and not review_visible:
            navigation = navigate_to_reviews_surface(session)
            probe["targeting_strategy"] = navigation.get("strategy")
            probe["landed_url"] = navigation.get("landed_url") or probe["landed_url"]
            probe["page_title"] = navigation.get("page_title") or probe["page_title"]
            snapshot_output = run_pw_command(session, "snapshot")
            probe["snapshot_excerpt"] = "\n".join(snapshot_output.splitlines()[:40])
            body_output = run_pw_command(session, "eval", "document.body.innerText")
            body_text = extract_eval_result(body_output)
            lowered = body_text.lower()
            review_visible = customer_review.lower() in lowered if customer_review else False
            probe["review_text_visible"] = review_visible
            probe["review_signal_visible"] = any(fragment in lowered for fragment in signals.get("review_text_fragments", []))
            probe["body_text_excerpt"] = body_text[:1500]
        elif not probe["targeting_strategy"]:
            probe["targeting_strategy"] = (
                "reuse_current_authenticated_page" if reuse_current_session else "landed_directly_on_target_surface"
            )

        expected_listing_id = str(((packet.get("review_target") or {}).get("listing_id") or "")).strip() or None
        expected_transaction_id = str(((packet.get("review_target") or {}).get("transaction_id") or "")).strip() or None
        if not probe["sign_in_required"] and not probe["not_found"] and probe["review_text_visible"]:
            review_block = locate_review_block(
                session,
                customer_review,
                expected_listing_id=expected_listing_id,
                expected_transaction_id=expected_transaction_id,
            )
            probe["matched_transaction_id"] = review_block.get("matchedTransactionId")
            probe["transaction_id_verified"] = review_block.get("transactionIdVerified")
            probe["matched_listing_id"] = review_block.get("matchedListingId")
            probe["matched_listing_title"] = review_block.get("matchedListingTitle")
            probe["matched_listing_href"] = review_block.get("matchedListingHref")
            probe["listing_id_verified"] = review_block.get("listingIdVerified")
            probe["candidate_count"] = review_block.get("candidateCount")
            probe["exact_review_located"] = bool(
                review_block.get("found")
                and (
                    expected_transaction_id is None
                    or str(review_block.get("matchedTransactionId") or "") == expected_transaction_id
                )
                and (
                    expected_listing_id is None
                    or str(review_block.get("matchedListingId") or "") == expected_listing_id
                )
            )
            probe["reply_box_visible"] = bool(review_block.get("replyBoxVisible"))
            probe["reply_textarea_placeholder"] = review_block.get("replyTextareaPlaceholder")
            probe["reply_controls"] = review_block.get("controls") or []
            probe["review_block_excerpt"] = review_block.get("contextText")
            if probe["exact_review_located"]:
                probe["notes"].append(
                    "Exact target review block was located by Etsy transaction ID, then verified against the expected listing ID and review text."
                )
            elif review_block.get("found"):
                probe["notes"].append(
                    "A nearby review block was found, but its transaction or listing identifiers did not match the expected target. Discovery is failing closed."
                )
            if probe["reply_box_visible"]:
                probe["notes"].append("Reply textarea is visible in dry-run mode; no typing or submit action was performed.")
            if probe["matched_listing_title"]:
                probe["notes"].append(f"Matched listing title: {probe['matched_listing_title']}")
            if probe["candidate_count"] and probe["candidate_count"] > 1:
                probe["notes"].append(
                    f"Found {probe['candidate_count']} DOM candidates containing the review text; the smallest matching block was selected."
                )
            if review_block.get("submitVisible"):
                probe["notes"].append("The public-response submit control is visible, which confirms the page is actionable once an executor exists.")
            if review_block.get("cancelVisible"):
                probe["notes"].append("Cancel control is visible alongside the reply box, supporting a safe dry-run stop before submit.")

        if probe["exact_review_located"]:
            try:
                probe["screenshot_path"] = capture_target_review_screenshot(session, destination_dir)
                if probe["screenshot_path"]:
                    probe["notes"].append("Read-only discovery captured an element screenshot of the highlighted target review block.")
            except subprocess.CalledProcessError as exc:
                probe["notes"].append(
                    f"Target review screenshot capture failed, falling back to the generic viewport screenshot: {exc.stderr.strip() or exc.stdout.strip()}"
                )

        if not probe["screenshot_path"]:
            try:
                screenshot_output = run_pw_command(session, "screenshot")
                screenshot_rel = parse_screenshot_path(screenshot_output)
                probe["screenshot_path"] = copy_relative_artifact(screenshot_rel, destination_dir)
            except subprocess.CalledProcessError as exc:
                probe["notes"].append(f"Generic screenshot capture failed: {exc.stderr.strip() or exc.stdout.strip()}")

        if not probe["screenshot_path"]:
            fallback = latest_existing_screenshot(destination_dir)
            if fallback:
                probe["screenshot_path"] = fallback
                probe["notes"].append("Fresh screenshot capture failed, so discovery is reusing the latest successful screenshot for this same review target.")

        probe["notes"].append("Read-only discovery captured without typing or clicking submit.")
        if probe["sign_in_required"]:
            probe["notes"].append("Seller authentication is required before deeper Etsy review discovery can continue.")
        if probe["not_found"]:
            probe["notes"].append("This entry path lands on a not-found page and should not be used for live execution.")
        if not review_visible:
            probe["notes"].append(
                "Target review text was not visible on the landing page. Discovery should not proceed to live submit."
            )
    except subprocess.CalledProcessError as exc:
        probe["notes"].append(f"Playwright discovery failed: {exc.stderr.strip() or exc.stdout.strip()}")
    finally:
        if should_close_session:
            try:
                run_pw_command(session, "close")
            except Exception:
                pass

    return probe


def run_browser_discovery(
    packet: dict[str, Any],
    start_url: str,
    keep_browser_open: bool = False,
    probe_id: str | None = None,
    session_name_override: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = load_discovery_config()
    entries = config.get("entry_points") or [{"id": "manual", "url": start_url, "notes": "Manual start URL"}]
    if probe_id:
        entries = [entry for entry in entries if entry.get("id") == probe_id]
        if not entries:
            raise SystemExit(f"Unknown probe id: {probe_id}")
    signals = config.get("signals") or {}
    destination_dir = ROOT / "output" / "discovery" / "assets" / slugify(packet["artifact_id"])
    destination_dir.mkdir(parents=True, exist_ok=True)

    probes = []
    for entry in entries:
        probes.append(
            build_probe(
                packet,
                entry,
                signals,
                destination_dir,
                keep_browser_open=keep_browser_open,
                session_name_override=session_name_override,
            )
        )

    best_probe = max(probes, key=score_probe) if probes else {
        "attempted": False,
        "start_url": start_url,
        "landed_url": None,
        "page_title": None,
        "review_text_visible": False,
        "snapshot_excerpt": None,
        "screenshot_path": None,
        "notes": ["No discovery probes were configured."],
    }
    return best_probe, probes


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a read-only Etsy review discovery packet.")
    parser.add_argument("--artifact-id", help="Specific review-reply artifact to inspect.")
    parser.add_argument(
        "--etsy-reviews-url",
        default=os.environ.get("ETSY_REVIEW_DISCOVERY_URL") or DEFAULT_ETSY_REVIEWS_URL,
        help="Read-only landing URL for Etsy reviews discovery.",
    )
    parser.add_argument(
        "--capture-browser",
        action="store_true",
        help="Open Etsy in a headed Playwright session and capture read-only evidence.",
    )
    parser.add_argument(
        "--keep-browser-open",
        action="store_true",
        help="Keep Playwright probe browser sessions open after capture for manual inspection.",
    )
    parser.add_argument(
        "--probe-id",
        help="Limit browser discovery to one configured entry point such as seller_dashboard.",
    )
    parser.add_argument(
        "--session-name",
        help="Override the Playwright session name so discovery can reuse a specific signed-in browser.",
    )
    parser.add_argument(
        "--launch-auth-browser",
        action="store_true",
        help="Launch a persistent Etsy seller browser session for manual sign-in and exit.",
    )
    args = parser.parse_args()

    discovery_config = load_discovery_config()
    configured_entries = discovery_config.get("entry_points") or []
    entry_by_id = {entry.get("id"): entry for entry in configured_entries if entry.get("id")}

    if args.launch_auth_browser:
        probe_id = args.probe_id or "seller_dashboard"
        entry = entry_by_id.get(probe_id)
        if not entry:
            raise SystemExit(f"Unknown probe id for auth launch: {probe_id}")
        session_name = args.session_name or entry.get("session_name") or probe_id
        state = load_session_state()
        if session_is_open(session_name):
            result = {
                "session_name": session_name,
                "url": entry.get("url") or DEFAULT_ETSY_REVIEWS_URL,
                "already_open": True,
                "launched_at": now_iso(),
            }
        else:
            result = launch_browser_session(session_name, entry.get("url") or DEFAULT_ETSY_REVIEWS_URL)
        state.setdefault("sessions", {})[session_name] = result
        save_session_state(state)
        print(json.dumps(result, indent=2))
        return 0

    state = load_json(QUALITY_GATE_STATE_PATH, {"artifacts": {}})
    record = select_record(state, args.artifact_id)
    decision = record.get("decision") or {}
    if decision.get("flow") != "reviews_reply_positive":
        raise SystemExit("Discovery currently supports public Etsy review replies only.")

    review_target = decision.get("review_target") or {}
    packet = {
        "generated_at": now_iso(),
        "artifact_id": decision.get("artifact_id"),
        "artifact_slug": decision.get("artifact_slug") or slugify(decision.get("artifact_id") or "artifact"),
        "run_id": decision.get("run_id"),
        "decision": decision.get("decision"),
        "review_status": decision.get("review_status"),
        "discovery_mode": "read_only",
        "submit_guard": "submit_disabled",
        "customer_review": ((decision.get("preview") or {}).get("context_text")),
        "approved_reply_text": decision.get("approved_reply_text") or ((decision.get("preview") or {}).get("proposed_text")),
        "review_target": review_target,
        "quality_gate_metadata": decision.get("quality_gate_metadata") or {},
        "browser_capture": {
            "attempted": False,
            "start_url": args.etsy_reviews_url,
            "landed_url": None,
            "page_title": None,
            "review_text_visible": False,
            "snapshot_excerpt": None,
            "screenshot_path": None,
            "notes": [
                "No browser capture was run yet.",
                "Submit is intentionally disabled in discovery mode.",
            ],
        },
        "path_probes": [],
        "required_operator_checks": [
            "Confirm the landed Etsy page is the expected reviews management surface.",
            "Confirm the target review text matches the packet before any future typing is allowed.",
            "Confirm the packet shows no submit action was performed.",
            "Reject live execution if the review is not clearly targetable from this path.",
        ],
    }

    if args.capture_browser:
        browser_capture, probes = run_browser_discovery(
            packet,
            args.etsy_reviews_url,
            keep_browser_open=args.keep_browser_open,
            probe_id=args.probe_id,
            session_name_override=args.session_name,
        )
        packet["browser_capture"] = browser_capture
        packet["path_probes"] = probes

    paths = write_packet(packet)
    print(json.dumps({"artifact_id": packet["artifact_id"], "paths": paths}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
