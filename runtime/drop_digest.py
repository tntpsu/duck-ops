"""Tell the operator a drop is waiting, without telling them six times a day.

The drop scan runs 4-hourly. A digest that mailed on every scan with anything
pending would send six identical emails a day and be filtered inside a week,
which is the failure this is written to avoid rather than discover
([[feedback_cadence_bypass_keys_must_be_rare]] -- a bypass key that is almost
always true collapses a cadence).

So it sends when the SITUATION CHANGES, not when the state is non-empty:

  * something new is waiting that was not in the last email, or
  * nothing new, but the oldest item has crossed a day boundary we have not
    mentioned yet (1, 3, 7 days), or
  * nothing at all is waiting and the last email said something was -- the
    all-clear, so a resolved queue does not look like a dead job.

Everything else is silence, and silence here is correct: the OS card already
shows the count for anyone who looks.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parent
REPO_ROOT = RUNTIME_DIR.parent
DUCK_AGENT_ROOT = REPO_ROOT.parent / "duckAgent"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402
import drop_operator  # noqa: E402

STATE_PATH = REPO_ROOT / "state" / "drop_digest.json"
_FROZEN_PRODUCTION_STATE_PATH = (REPO_ROOT / "state" / "drop_digest.json").resolve()
_TEST_MODE_ENV = "DUCK_TEST_MODE"

#: Ages we will re-mail about, in days. Deliberately sparse: a nag every day
#: is a nag nobody reads, and the card carries the continuous signal.
AGE_MILESTONES = (1, 3, 7)


class TestModeRefusalError(RuntimeError):
    pass


def _write_state(payload: dict, path: Path | None = None) -> None:
    target = Path(path) if path is not None else STATE_PATH
    if (os.getenv(_TEST_MODE_ENV) == "1"
            and target.resolve() == _FROZEN_PRODUCTION_STATE_PATH):
        raise TestModeRefusalError(
            f"Refusing to write the production drop digest state ({target}) "
            f"under {_TEST_MODE_ENV}=1.")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, target)


def _read_state(path: Path | None = None) -> dict:
    target = Path(path) if path is not None else STATE_PATH
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _oldest_age_days(items: list[dict], now: datetime) -> float:
    ages = []
    for entry in items:
        try:
            stamp = datetime.fromisoformat(str(entry.get("received_at")))
            if stamp.tzinfo is None:
                stamp = stamp.astimezone()
            ages.append((now - stamp).total_seconds() / 86400.0)
        except Exception:
            continue
    return max(ages) if ages else 0.0


def _milestone(age_days: float) -> int:
    crossed = [m for m in AGE_MILESTONES if age_days >= m]
    return max(crossed) if crossed else 0


def decide(now: datetime | None = None, state_path: Path | None = None) -> dict:
    """Should we send, and why? Pure enough to test without a mailbox."""
    now = now or datetime.now().astimezone()
    proposals = drop_intake._read_ledger(drop_intake.DROP_PROPOSALS_PATH)
    pending = [dict(e, stem=s) for s, e in drop_operator._pending(proposals)]
    notes = drop_operator._unanswered_notes()
    stems = sorted(e["stem"] for e in pending)
    note_files = sorted(str(n.get("file")) for n in notes)

    prior = _read_state(state_path)
    seen = set(prior.get("stems") or []) | set(prior.get("note_files") or [])
    current = set(stems) | set(note_files)
    new_items = sorted(current - seen)

    age = _oldest_age_days(pending, now)
    milestone = _milestone(age)
    prior_milestone = int(prior.get("milestone") or 0)

    if not current:
        if prior.get("stems") or prior.get("note_files"):
            reason = "all_clear"
        else:
            reason = "nothing_waiting"
        return {"send": reason == "all_clear", "reason": reason,
                "stems": stems, "note_files": note_files,
                "new_items": [], "milestone": 0, "oldest_age_days": 0.0,
                "pending": pending, "notes": notes}

    if new_items:
        reason = "new_items"
    elif milestone > prior_milestone:
        reason = f"aged_past_{milestone}d"
    else:
        reason = "no_change"

    return {"send": reason != "no_change", "reason": reason,
            "stems": stems, "note_files": note_files,
            "new_items": new_items, "milestone": milestone,
            "oldest_age_days": round(age, 2),
            "pending": pending, "notes": notes}


#: Wide enough to judge a frame on a phone, small enough that three of them
#: do not make a 10 MB email.
PREVIEW_WIDTH = 520


def _downscale(src: Path, dest: Path) -> Path | None:
    from PIL import Image  # type: ignore

    with Image.open(src) as im:
        im = im.convert("RGB")
        ratio = PREVIEW_WIDTH / max(im.width, 1)
        if ratio < 1:
            im = im.resize((PREVIEW_WIDTH, max(int(im.height * ratio), 1)))
        im.save(dest, format="JPEG", quality=82)
    return dest


def build_previews(pending: list, out_dir: Path | None = None) -> list[dict]:
    """Stills of the FINISHED post for each waiting drop.

    Two things this learned the hard way, both from the operator looking at a
    real email rather than from any test:

    1. Show the post, not the source. The first version pulled a bare frame
       and the reply was "I don't see memes, just photos". It now burns the
       caption in exactly as it will publish.
    2. One still cannot represent a two-beat clip. When a drop carries a
       `closing_line`, the punchline appears partway through, so a single
       frame shows half the joke -- which is the half the operator already
       read in the caption. Two frames, one per beat.

    A still is still a still: `watch_hint` says so, because approving a video
    off a photo is approving the thing you did not look at.
    """
    out_dir = Path(out_dir or (REPO_ROOT / "state" / "drop_previews"))
    out_dir.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    previews: list[dict] = []

    for index, entry in enumerate(pending, start=1):
        stem = entry.get("stem") or f"drop{index}"
        asset = Path(str(entry.get("asset_path") or ""))
        record: dict = {"stem": stem, "frames": [], "note": None,
                        "is_video": str(entry.get("media_type")) == "video",
                        "asset_path": str(asset)}
        try:
            if not asset.is_file():
                record["note"] = "the file is gone"
                previews.append(record)
                continue

            if not record["is_video"]:
                out = out_dir / f"{stem}.preview.jpg"
                _downscale(asset, out)
                record["frames"] = [{"cid": f"drop{index}a",
                                     "path": str(out), "label": None}]
                previews.append(record)
                continue

            from helpers import duck_video_helper, image_lane_reel  # type: ignore

            closing = str(entry.get("closing_line") or "").strip()
            closing_at = float(entry.get("closing_at") or 0.0)
            built = image_lane_reel.caption_and_poster(
                asset, out_dir / stem,
                hook_text=str(entry.get("on_screen_hook_text") or ""),
                closing_line=closing or None, closing_at=closing_at,
                run_id="preview", lane=str(entry.get("lane") or "drop"))

            if built.get("status") != "ready":
                # A hook that will not fit is itself worth seeing, so fall
                # back to a bare frame rather than showing nothing -- and say
                # why, because a preview that quietly differs from the others
                # reads as a glitch ("I only saw one with the meme on it").
                record["note"] = str(built.get("error") or "")[:160]
                frame = out_dir / f"{stem}.raw.png"
                grab = duck_video_helper.extract_poster_frame(
                    asset, frame, at_seconds=2.0)
                if grab.get("poster_path"):
                    out = out_dir / f"{stem}.preview.jpg"
                    _downscale(Path(grab["poster_path"]), out)
                    record["frames"] = [{"cid": f"drop{index}a",
                                         "path": str(out),
                                         "label": "caption NOT applied"}]
                previews.append(record)
                continue

            playable = Path(built["video_path"])
            duration = float(built.get("clip_seconds") or 0.0)
            # One frame per beat. Before the closing card, and after it.
            if closing and closing_at > 0:
                stops = [(max(closing_at / 2, 0.5), "first beat"),
                         (min(closing_at + 1.5, max(duration - 0.3, closing_at + 0.2)),
                          "second beat")]
            else:
                stops = [(min(2.0, max(duration - 0.3, 0.5)), None)]

            for n, (at, label) in enumerate(stops):
                raw = out_dir / f"{stem}.f{n}.png"
                grab = duck_video_helper.extract_poster_frame(
                    playable, raw, at_seconds=at)
                if not grab.get("poster_path"):
                    continue
                out = out_dir / f"{stem}.f{n}.jpg"
                _downscale(Path(grab["poster_path"]), out)
                record["frames"].append({"cid": f"drop{index}{chr(97 + n)}",
                                         "path": str(out), "label": label})
            if not record["frames"]:
                record["note"] = "could not read a frame from it"
            previews.append(record)
        except Exception as exc:  # noqa: BLE001
            print(f"[drop-digest] preview failed for {stem}: {exc}",
                  file=sys.stderr)
            record["note"] = f"preview failed: {type(exc).__name__}"
            previews.append(record)
    return previews


def _esc(text: object) -> str:
    return (str(text or "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


#: The house approval-email palette, lifted from
#: `email_reply_action_helper.render_reply_action_buttons` and
#: `email_helper.email_summary` rather than invented. The first version of
#: this email had its own stylesheet and its own colours, which is one more
#: look for the operator to learn for no benefit -- "we shouldn't reinvent
#: the wheel from our other approval emails".
_FONT = "-apple-system,system-ui,Segoe UI,Roboto,Arial"
_TEXT = "#374151"
_MUTED = "#6b7280"
_RULE = "#d1d5db"


def _load_duckagent_env() -> None:
    """Credentials and APPROVAL_REPLY_TO live in duckAgent/.env
    ([[feedback_env_in_dotenv]]). Loaded before rendering, not only before
    sending, because the buttons need the reply-to address too.

    REFUSED under DUCK_TEST_MODE, and that refusal is the whole point. The
    first version loaded unconditionally and so pulled the entire production
    environment into any test process that rendered an email -- which broke
    six unrelated cadence tests via DUCK_EMAIL_DIGEST_MODE, and, far worse,
    populated live SMTP credentials in a suite that is supposed to be unable
    to reach anyone ([[feedback_test_runs_reach_the_outside_world]]). Tests
    pass `reply_to` explicitly instead.
    """
    if str(os.environ.get(_TEST_MODE_ENV) or "").strip() in {"1", "true", "TRUE", "yes"}:
        return
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
    except Exception:
        pass


def _reply_buttons(stem: str, subject: str, run_id: str,
                   to: str | None = None, *, can_approve: bool = True,
                   siblings: int = 0) -> str:
    """The SAME mailto buttons every other approval email uses.

    They open a pre-filled draft reply, which on a phone is one tap instead of
    typing a 44-character stem -- the thing that made the first version
    unusable where it is actually read.
    """
    _load_duckagent_env()
    try:
        from helpers.email_reply_action_helper import (  # type: ignore
            ReplyAction, approval_reply_to, render_reply_action_buttons)
    except Exception as exc:  # pragma: no cover - duckAgent absent
        print(f"[drop-digest] WARNING no reply buttons: {exc}", file=sys.stderr)
        return ""
    recipient = (to or approval_reply_to() or "").strip()
    if not recipient:
        # render_reply_action_buttons returns "" for an empty recipient, which
        # would ship an approval email with nothing to approve WITH and no
        # error anywhere ([[feedback_swallowed_errors_lie]]).
        print("[drop-digest] WARNING APPROVAL_REPLY_TO/SMTP_USER unset — "
              "sending an approval email with no buttons", file=sys.stderr)
        return ""
    # Decline is offered ALWAYS, Approve only when the agent declared the
    # drop finished. A not-ready drop is precisely the one you want to clear
    # out, and the first version gated the whole button block on post_ready --
    # so the only drop that could never be published was also the only one
    # that could not be dismissed, and it would have aged through every
    # digest milestone forever.
    actions = []
    if can_approve:
        actions.append(ReplyAction(label="Approve",
                                   reply_text=f"drop approve {stem}",
                                   action="approve"))
        if siblings:
            # One tap for the normal case: a corrected re-send supersedes its
            # predecessors, and clearing them one at a time meant one email
            # each -- "I don't want to send 4 emails".
            actions.append(ReplyAction(
                # No ampersand: render_reply_action_buttons escapes the
                # label, so "&amp;" would ship as "&amp;amp;".
                label=f"Approve only this (clears {siblings})",
                reply_text=f"drop approve {stem} only",
                action="approve"))
    actions.append(ReplyAction(label="Decline",
                               reply_text=f"drop decline {stem}",
                               action="decline"))
    return render_reply_action_buttons(
        subject=subject, flow="drops", run_id=run_id, to=recipient,
        actions=actions,
    )


def render_email(decision: dict, now: datetime | None = None,
                 previews: list[dict] | None = None,
                 reply_to: str | None = None) -> tuple[str, str, str]:
    now = now or datetime.now().astimezone()
    run_id = now.strftime("%Y-%m-%d")
    pv = {p["stem"]: p for p in (previews or [])}
    n = len(decision["stems"])
    notes = decision.get("notes") or []

    if decision["reason"] == "all_clear":
        subject = (f"MJD: [DROPS] queue clear | FLOW:drops | RUN:{run_id} "
                   "| ACTION:notify")
        text = ("Nothing from an agent is waiting on you any more.\n\n"
                "This is the all-clear, so a resolved queue does not look "
                "like a dead job.")
        html = (f"<div style='font-family:{_FONT};line-height:1.5;"
                f"color:{_TEXT};max-width:620px;'>"
                "<h2 style='margin:0 0 8px 0;'>Drop queue is clear</h2>"
                "<p>Nothing from an agent is waiting on you.</p>"
                f"<p style='color:{_MUTED};font-size:12px;'>You get this once "
                "when the queue empties, so a resolved queue does not look "
                "like a dead job.</p></div>")
        return subject, text, html

    bits = []
    if n:
        bits.append(f"{n} drop{'s' if n != 1 else ''}")
    if notes:
        bits.append(f"{len(notes)} note{'s' if len(notes) != 1 else ''}")
    subject = (f"MJD: [DROPS] {' and '.join(bits)} waiting | FLOW:drops "
               f"| RUN:{run_id} | ACTION:approve")

    parts = [f"<div style='font-family:{_FONT};line-height:1.5;color:{_TEXT};"
             "max-width:620px;'>",
             f"<h2 style='margin:0 0 8px 0;'>{' and '.join(bits)} waiting on "
             "you</h2>",
             f"<p style='margin:0 0 18px 0;color:{_MUTED};font-size:13px;'>"
             "Each preview below is the finished post, caption burned in, "
             "exactly as it would publish.</p>"]

    for entry in decision["pending"]:
        stem = entry.get("stem")
        preview = pv.get(stem) or {}
        parts.append(
            f"<div style='border:1px solid {_RULE};border-radius:10px;"
            "overflow:hidden;margin:0 0 18px 0;'>")
        for frame in preview.get("frames") or []:
            parts.append(f"<img src='cid:{frame['cid']}' alt='preview' "
                         "width='520' style='display:block;width:100%;"
                         "height:auto;background:#f3f4f6;'/>")
            if frame.get("label"):
                parts.append(
                    f"<div style='padding:6px 16px;font-size:11px;"
                    f"color:{_MUTED};background:#fafafa;'>"
                    f"{_esc(frame['label'])}</div>")
        parts.append("<div style='padding:14px 16px 16px 16px;'>")
        parts.append(
            f"<div style='font-size:12px;color:{_MUTED};margin:0 0 8px 0;'>"
            f"<strong>{_esc(entry.get('lane'))}</strong> &middot; from "
            f"{_esc(entry.get('source_agent'))}</div>")
        if preview.get("is_video"):
            # Say it plainly. Approving a video off a photo is approving the
            # thing you did not actually watch.
            frames = len(preview.get("frames") or [])
            parts.append(
                f"<div style='font-size:12px;color:{_MUTED};margin:0 0 8px 0;'>"
                f"{'Still frames' if frames > 1 else 'Still frame'} from the "
                "video &mdash; the clip itself is in "
                f"<code>DuckDrop/accepted/</code>.</div>")
        warn = preview.get("note")
        if warn:
            parts.append(f"<div style='font-size:12px;color:#8a5300;"
                         f"margin:0 0 8px 0;'>{_esc(warn)}</div>")
        caption = str(entry.get("caption") or "").strip()
        if caption:
            parts.append(f"<p style='margin:0 0 10px 0;font-size:15px;'>"
                         f"{_esc(caption)}</p>")
        ready = bool(entry.get("post_ready"))
        if not ready:
            parts.append(
                f"<div style='margin-top:12px;color:{_MUTED};font-size:12px;'>"
                "The agent has not declared this finished "
                "(<code>post_ready:false</code>), so there is nothing to "
                "approve &mdash; but you can still clear it.</div>")
        siblings = sum(1 for o in decision["pending"]
                       if o.get("stem") != stem
                       and o.get("lane") == entry.get("lane"))
        parts.append(_reply_buttons(str(stem), subject, run_id, to=reply_to,
                                    can_approve=ready, siblings=siblings))
        parts.append("</div></div>")

    for note in notes:
        parts.append(
            f"<div style='border:1px solid {_RULE};border-radius:10px;"
            "padding:14px 16px;margin:0 0 14px 0;background:#fbfbfc;'>"
            f"<div style='font-size:12px;color:{_MUTED};margin:0 0 6px 0;'>"
            "<strong>note from the agent</strong></div>"
            f"<p style='margin:0 0 8px 0;font-size:15px;'>"
            f"{_esc(note.get('subject') or note.get('file'))}</p>"
            f"<p style='margin:0 0 8px 0;color:{_TEXT};font-size:13px;'>"
            f"{_esc(str(note.get('body') or '')[:400])}</p>"
            f"<div style='color:{_MUTED};font-size:12px;'>Reply "
            f"<code>drop answer {_esc(note.get('file'))} &lt;your answer&gt;"
            "</code></div></div>")

    parts.append(f"<p style='color:{_MUTED};font-size:12px;margin-top:18px;'>"
                 f"Oldest item: {decision['oldest_age_days']} day(s). Sent "
                 f"because: {_esc(decision['reason'])}. You only hear from "
                 "this when something changes.</p>")
    parts.append("</div>")
    html = "\n".join(parts)

    text_body = drop_operator.format_waiting(decision["pending"], notes)
    text = (f"{text_body}\n\nReply to this email with one of the lines "
            f"above.\n\nOldest item: {decision['oldest_age_days']} day(s). "
            f"Sent because: {decision['reason']}.")
    return subject, text, html


def send(decision: dict, now: datetime | None = None) -> bool:
    _load_duckagent_env()
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    from helpers.email_helper import send_email  # type: ignore

    previews = build_previews(decision["pending"])
    subject, text, html = render_email(decision, now=now, previews=previews)
    attachments = [{"path": f["path"], "cid": f["cid"]}
                   for p in previews for f in (p.get("frames") or [])
                   if f.get("cid") and f.get("path")]
    send_email(subject, html, text, attachments=attachments or None)
    return True


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="print the decision and the email, send nothing")
    ap.add_argument("--force", action="store_true",
                    help="send even when nothing changed (operator asking for "
                         "the current state, or verifying the format)")
    args = ap.parse_args(argv)

    now = datetime.now().astimezone()
    decision = decide(now=now)
    print(f"[drop-digest] send={decision['send']} reason={decision['reason']} "
          f"drops={len(decision['stems'])} notes={len(decision['note_files'])}")

    if args.dry_run:
        previews = build_previews(decision["pending"])
        subject, text, html = render_email(decision, now=now, previews=previews)
        print(f"\nSubject: {subject}\n\n{text}")
        out = REPO_ROOT / "state" / "drop_previews" / "digest_preview.html"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(html, encoding="utf-8")
        print(f"\n[drop-digest] html written to {out} "
              f"({len(html)} bytes, {len(previews)} preview(s))")
        return 0

    if not decision["send"] and not args.force:
        return 0
    if args.force and not decision["send"]:
        decision = {**decision, "reason": "asked for it"}

    send(decision, now=now)
    _write_state({
        "last_sent_at": now.isoformat(),
        "reason": decision["reason"],
        "stems": decision["stems"],
        "note_files": decision["note_files"],
        "milestone": decision["milestone"],
    })
    print("[drop-digest] sent")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
