#!/usr/bin/env python3
"""Accept finished posts that an outside agent drops into a synced folder.

The mirror (Surface 119) lets business data OUT so an external agent can read
it. This is the return leg: the agent writes a finished image or video plus a
sidecar describing it, Drive syncs it down, and this producer decides whether
it may become a post PROPOSAL.

Three properties carry the surface, and the first one is the whole point:

1. **The drop folder is an UNTRUSTED input channel.** Anything that can write
   to it -- a confused agent, a shared Drive link, a compromised account --
   is reaching for the operator's public feed. So nothing here auto-publishes.
   An accepted drop becomes a proposal the operator approves by reply, exactly
   like every other lane, and the house rule against silently promoting a lane
   to auto-action is the reason ([[feedback_auto_manual_queue_contract]]).

2. **It is a SEPARATE sync root from the mirror.** `drive_mirror.release()`
   publishes with `rsync -a --delete` into the mirror root, so a file dropped
   there is deleted on the next run with no error and no trace. ~/DuckDrop is
   its own Drive folder for that reason, and `_refuse_if_inside_mirror` makes
   the mistake loud rather than silent if anyone ever repoints it.

3. **A file is not read until it has stopped changing.** Drive writes a
   download progressively, so a file picked up mid-sync is a truncated file
   that passes every content check applied to the bytes that arrived. Each
   candidate must be QUIESCENT -- unchanged in size and mtime across a
   settling window -- before anything parses it.

Rejection is never deletion. A refused drop moves to rejected/ beside a
`.reason.txt`, which Drive syncs back up, so the agent that wrote it can see
why without anyone relaying the message by hand.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

def _default_drop_root() -> Path:
    """The factory-default drop directory, env overrides excluded.

    A folder inside the mounted My Drive, for the same reason the mirror moved
    there on 2026-11-13 (Surface 129): a Drive-for-desktop *mirror root*
    uploads to `Computers -> <machine>`, which an external agent cannot write
    to. The inbound lane needs the reverse of what the mirror needs -- the
    agent must be able to PUT files where this process reads them -- so the
    mirror-root approach could never have worked here at all. The registration
    this folder used to depend on had in fact silently vanished from Drive's
    `roots` table, so nothing was syncing and the lane was dead from the day
    it shipped until 2026-11-16.

    Derived through drive_mirror rather than re-derived here, for the reason
    recorded directly below in `_mirror_root`: a second copy of a path is a
    copy that goes stale the moment the first one moves.
    """
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import drive_mirror
        my_drive = drive_mirror._my_drive_dir()
    except Exception:  # pragma: no cover - defensive
        my_drive = None
    return (my_drive or Path.home()) / "DuckDrop"


#: Where Drive syncs the agent's output down. A folder in My Drive, never a
#: subdirectory of the mirror -- see property 2 above.
DROP_ROOT = Path(os.environ.get("DUCK_DROP_ROOT") or _default_drop_root())
INBOX_DIR = "inbox"
ACCEPTED_DIR = "accepted"
REJECTED_DIR = "rejected"

#: The return channel. Until now this was a one-way contract: we published a
#: README and the agent published files, and the only thing it could learn was
#: whether one file was refused. It could not ask what a field meant, say a
#: rule was ambiguous, or find out whether anything it sent was ever posted --
#: so the same misunderstanding could repeat weekly with nobody able to name
#: it. These three close the loop.
FEEDBACK_DIR = "feedback"      # agent -> us: questions, problems, suggestions
ANSWERS_DIR = "answers"        # us -> agent: replies, and changes we made
FEEDBACK_READ_DIR = "feedback/read"
STATUS_FILE = "status.json"    # what happened to every drop, machine-readable

#: Feedback is deliberately NOT validated the way a drop is. A refused drop is
#: safe; a refused question is a channel nobody uses. Anything readable is
#: accepted and surfaced -- the worst case is a note we cannot parse, which is
#: itself worth seeing.
FEEDBACK_EXTS = {".json", ".md", ".txt"}
MAX_FEEDBACK_BYTES = 256 * 1024

def _mirror_root() -> Path:
    """The mirror's published root. Used only to refuse a catastrophic misconfig.

    Read from drive_mirror rather than re-derived here. This file carried its
    own copy of the default until 2026-11-13, and when the mirror moved into the
    mounted My Drive the copy stayed pointed at `~/DuckData` -- so the refusal
    below would have been guarding a directory the mirror no longer publishes
    to, and a drop root placed inside the real one would have sailed through.
    Resolved lazily so importing this module never depends on drive_mirror
    importing cleanly, and so a test redirecting the root is honoured.
    """
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import drive_mirror
        return drive_mirror.DRIVE_MIRROR_ROOT
    except Exception:
        return Path(
            os.environ.get("DRIVE_MIRROR_ROOT") or (Path.home() / "DuckData")
        )

DROP_INTAKE_STATE_PATH = REPO_ROOT / "state" / "drop_intake.json"

#: Durable record of every accepted drop, and the only thing a flow may claim
#: from. `drop_intake.json` is the LAST SCAN's manifest and is rewritten whole
#: on every run, so a proposal accepted on Monday is gone from it the moment
#: Tuesday's scan finds an empty inbox. A claim has to outlive the scan that
#: created it, so it lives here instead.
DROP_PROPOSALS_PATH = REPO_ROOT / "state" / "drop_proposals.json"
#: Feedback goes in a durable ledger for the same reason proposals do: the
#: per-scan manifest is rewritten whole, so a note would vanish on the next
#: empty scan and nobody would ever answer it.
DROP_FEEDBACK_PATH = REPO_ROOT / "state" / "drop_feedback.json"
_FROZEN_PRODUCTION_FEEDBACK_PATH = (
    REPO_ROOT / "state" / "drop_feedback.json").resolve()

_FROZEN_PRODUCTION_PROPOSALS_PATH = (
    REPO_ROOT / "state" / "drop_proposals.json"
).resolve()
_TEST_MODE_REFUSAL_ENV = "DUCK_TEST_MODE"


class TestModeRefusalError(RuntimeError):
    """Raised when a run under DUCK_TEST_MODE=1 would write the real ledger."""


def _refuse_test_mode_prod_write(path: Path) -> None:
    """Fixture rows in this ledger are worse than fixture rows elsewhere: a
    claimable proposal is a thing a scheduled flow will PUBLISH."""
    flag = str(os.environ.get(_TEST_MODE_REFUSAL_ENV) or "").strip()
    if flag not in {"1", "true", "TRUE", "yes"}:
        return
    try:
        resolved = Path(path).resolve()
    except Exception:  # pragma: no cover - defensive
        return
    frozen = {
        _FROZEN_PRODUCTION_PROPOSALS_PATH: "proposal ledger",
        _FROZEN_PRODUCTION_FEEDBACK_PATH: "feedback ledger",
    }
    which = frozen.get(resolved)
    if which:
        raise TestModeRefusalError(
            f"drop_intake refused to write {resolved}: "
            f"{_TEST_MODE_REFUSAL_ENV}=1 and this is the frozen production "
            f"{which}."
        )

#: The standalone lane. Every other lane answers "which weekly slot is this
#: for?"; this one answers "when do you want it out?" and belongs to no slot.
#: A drop naming it is rendered and scheduled by the express runner on the
#: ten-minute `social_publish_due` cadence, within minutes of the operator's
#: approval, instead of waiting for a weekly launchd job.
EXPRESS_LANE = "express"

#: `publish_at` value meaning "as soon as you can after I approve it".
PUBLISH_AT_ASAP = "asap"
#: Spellings of it we accept, because an agent writing "now" means asap and
#: refusing it teaches nothing. The STORED value is always PUBLISH_AT_ASAP.
_ASAP_WORDS = frozenset({"asap", "now", "immediately", "immediate"})

#: A scheduled time must be at least this far out. Two independent floors
#: land on the same number: Meta refuses a Facebook `scheduled_publish_time`
#: under 10 minutes ahead, and our own IG queue is drained by a sidecar that
#: fires every 10 minutes, so a nearer time could not be honoured by either
#: platform. Anything under this should be sent as `asap` instead, which takes
#: a different publish path entirely (immediate, not scheduled).
MIN_PUBLISH_LEAD_SECONDS = 600
#: And no further out than this. OURS, not a platform limit -- Facebook allows
#: months. A post scheduled further ahead than we can remember approving sits
#: in an untrusted channel with nobody able to recall what it says, and the
#: ledger row that authorises it is a row nobody will read again.
MAX_PUBLISH_LEAD_DAYS = 60

#: The zone a wall-clock `publish_at` is read in. Same env var and same
#: default as `scheduler_health.SCHEDULER_TZ`, and a test pins that the two
#: agree -- "18:00" from an agent means 18:00 where the shop is, and resolving
#: it in a different zone than every other schedule in this system would put
#: a post out hours early twice a year.
#:
#: A naive stamp is localised by ATTACHING this zone, never by adding today's
#: offset: ZoneInfo recomputes the offset from the resulting wall time, so a
#: time on the far side of a DST change is still the hour the agent asked for
#: ([[feedback_schedule_math_needs_a_zone_not_an_offset]]).
def _publish_tz_name() -> str:
    return os.environ.get("DUCK_SCHEDULER_TZ") or "America/New_York"


def _publish_tz():
    from zoneinfo import ZoneInfo
    return ZoneInfo(_publish_tz_name())


def resolve_publish_at(raw: Any, *, now: datetime | None = None) -> tuple[str, datetime | None]:
    """Normalise a sidecar `publish_at` into ("asap", None) or ("at", dt).

    Raises DropRefused with a message written for the agent that sent it.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise DropRefused(
            f'"publish_at" is required on an "{EXPRESS_LANE}" drop: either '
            f'"{PUBLISH_AT_ASAP}" or an ISO-8601 time like '
            '"2026-11-22T18:00". An express post has no weekly slot to fall '
            "back on, so without it there is no answer to when it goes out.")
    text = raw.strip()
    if text.lower() in _ASAP_WORDS:
        return PUBLISH_AT_ASAP, None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise DropRefused(
            f'"publish_at": {text!r} is not "{PUBLISH_AT_ASAP}" and does not '
            "parse as an ISO-8601 time. Use e.g. \"2026-11-22T18:00\" "
            f"(read in {_publish_tz_name()}) or \"2026-11-22T18:00-05:00\"."
        ) from None
    if parsed.tzinfo is None:
        # Wall clock in the shop's zone, with the offset computed for THAT
        # date rather than today's.
        parsed = parsed.replace(tzinfo=_publish_tz())

    now = now or datetime.now().astimezone()
    lead = (parsed - now).total_seconds()
    if lead < 0:
        raise DropRefused(
            f'"publish_at": {parsed.isoformat()} is already in the past '
            f"(it is now {now.isoformat()}). Send a future time, or "
            f'"{PUBLISH_AT_ASAP}" to go out as soon as it is approved.')
    if lead < MIN_PUBLISH_LEAD_SECONDS:
        raise DropRefused(
            f'"publish_at": {parsed.isoformat()} is only '
            f"{lead / 60:.0f} minute(s) away. Neither platform can honour "
            f"that -- Facebook refuses a scheduled time under "
            f"{MIN_PUBLISH_LEAD_SECONDS // 60} minutes and our Instagram "
            f"queue is drained every {MIN_PUBLISH_LEAD_SECONDS // 60} "
            f'minutes. Use "{PUBLISH_AT_ASAP}" for something this soon; it '
            "publishes immediately instead of scheduling.")
    if lead > MAX_PUBLISH_LEAD_DAYS * 86400:
        raise DropRefused(
            f'"publish_at": {parsed.isoformat()} is {lead / 86400:.0f} days '
            f"out; the limit is {MAX_PUBLISH_LEAD_DAYS}. That is our rule, "
            "not the platform's: nobody will remember approving a post that "
            "far ahead.")
    return "at", parsed


#: Lanes a drop may name, from the flow registry rather than a second list
#: that drifts. Resolved lazily so importing this module never needs them.
_FALLBACK_LANES = frozenset({
    "meme", "jeepfact", "thursday", "duckvideo", "gtdf", "reviews_story",
    # Not a flow. `express` has no flows/express/steps.py and never will: it
    # is a standalone post with no generation step, no review email of its
    # own and no weekly slot, so it cannot come from the flow registry and
    # has to be named here.
    EXPRESS_LANE,
})

#: Extension -> the magic bytes a real file of that type starts with. The
#: extension is a claim; these are the check. A .png full of shell script is
#: the whole reason this mapping exists rather than a suffix allowlist.
_MAGIC = {
    ".png": [b"\x89PNG\r\n\x1a\n"],
    ".jpg": [b"\xff\xd8\xff"],
    ".jpeg": [b"\xff\xd8\xff"],
    ".mp4": [b"ftyp"],   # at offset 4
    ".mov": [b"ftyp"],   # at offset 4
}
_IMAGE_EXTS = frozenset({".png", ".jpg", ".jpeg"})
_VIDEO_EXTS = frozenset({".mp4", ".mov"})

MAX_IMAGE_BYTES = 25 * 1024 * 1024
MAX_VIDEO_BYTES = 300 * 1024 * 1024
MAX_CAPTION_CHARS = 2200          # Instagram's own ceiling
#: A hook is composited over the frame, so the ceiling is legibility on a
#: phone, not a platform limit. 180 was a guess and roughly double the truth:
#: measured against the renderer, four lines hold ~84 characters on a 720-wide
#: clip and ~100 on a 1080-wide one. This is a generous SANITY bound, not the
#: real gate -- how much fits depends on the clip's width, which is unknowable
#: here, so `build_lane_reel_from_video` refuses a hook that would be cut off
#: and that refusal is what actually decides.
MAX_HOOK_CHARS = 120
MAX_SIDECAR_BYTES = 64 * 1024

#: A candidate must be unchanged for this long before it is read, so a file
#: still arriving from Drive is never parsed as a complete one.
QUIESCENT_SECONDS = 45

#: How long a sidecar may sit with no asset beside it before it is refused.
#: An agent writes the two halves separately, and a video is the slow half, so
#: for a while "the asset is not here" means "it is still uploading" -- but
#: only for a while. Measured 2026-11-19: Gemini wrote a sidecar for a post
#: whose .mp4 never arrived, and eleven hours later nothing anywhere said so,
#: because the scan skips .json files on the (correct) grounds that a sidecar
#: is found through its asset. An orphan was therefore invisible to the
#: manifest, to `rejected/`, and to the agent's own status file: it believed it
#: had dropped a post, we believed nothing had arrived, and neither could see
#: the other ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]] --
#: zero findings describes which files the gate OPENED).
ORPHAN_SIDECAR_GRACE_SECONDS = 30 * 60


class DropRefused(RuntimeError):
    """A drop that may not become a proposal. Carries an operator-readable
    reason, which is written beside the file so the agent can see it."""


@dataclass
class DropRecord:
    stem: str
    asset: str
    status: str
    reason: str = ""
    lane: str = ""
    media_type: str = ""
    bytes: int = 0
    caption_chars: int = 0
    source_agent: str = ""
    received_at: str = ""


#: Lanes that actually CONSUME a drop -- i.e. whose scheduled run calls the
#: claim and publishes the dropped file in place of its own work.
#:
#: This is separate from `allowed_lanes()` on purpose, and the gap between
#: them was a real defect: the contract accepted eleven lanes and exactly one
#: of them could use a drop, so a jeepfact post would validate, enter the
#: ledger, be approved by the operator -- and then nothing would ever claim
#: it. It sat forever while the lane generated its own post as usual. The
#: README promised the opposite in as many words.
#:
#: That is the shape of a gate whose reason names a destination with no
#: rendering consumer ([[feedback_fold_defer_is_a_handoff_contract]]): nothing
#: errors, nothing logs, and the operator believes the post is scheduled.
#:
#: Adding a name here is a CLAIM about duckAgent's code, so
#: tests/test_drop_lane_consumers.py checks it against the flows themselves
#: and fails if the two drift apart in either direction.
DROP_CONSUMING_LANES = frozenset({"meme", "jeepfact", "duckvideo", EXPRESS_LANE})


#: What each consuming lane actually wants, written for the agent that has to
#: produce it. A generic contract was not enough: the lanes differ on the day
#: they publish, the media they accept, and -- the one that matters most --
#: whether the operator's approval ALONE sends the post or a second reply is
#: still required. An agent cannot guess any of that, and guessing wrong means
#: a Wednesday post published on Monday.
#:
#: `tests/test_drop_lane_consumers.py` fails if a consuming lane has no entry
#: here, so wiring a lane forces documenting it in the same change.
LANE_DROP_SPECS: dict[str, dict[str, Any]] = {
    "meme": {
        "what": "Meme Monday — one joke panel, or one clip.",
        "day": "Mondays (MEME_DOW, default MON)",
        "media_types": ("image", "video"),
        "publishes": ("the operator's approval is the LAST human step; the "
                      "Monday run schedules it to Facebook and Instagram "
                      "with no further review"),
        "consumer": "flows/meme/steps.py",
    },
    "jeepfact": {
        "what": "Jeep Fact Wednesday — normally a 5-slide deck; a drop is one panel or clip.",
        "day": "Wednesdays (JEEPFACT_DOW, default WED)",
        "media_types": ("image", "video"),
        "publishes": ("the operator's approval is the LAST human step; the "
                      "Wednesday run schedules it with no further review"),
        "consumer": "flows/jeepfact/steps.py",
    },
    "duckvideo": {
        "what": "The weekly duck reel.",
        "day": "Fridays (DUCKVIDEO_DOW, default FRI)",
        "media_types": ("video",),
        "publishes": ("approval is NOT enough — the operator must also reply "
                      "to the review email before anything is posted, so a "
                      "drop here passes two human gates"),
        "consumer": "flows/duckvideo/steps.py",
    },
    EXPRESS_LANE: {
        "what": ("A standalone post. Belongs to no weekly slot and replaces "
                 "nothing -- Meme Monday still runs as usual."),
        "day": "whenever you ask: set \"publish_at\"",
        "media_types": ("image", "video"),
        "publishes": ("the operator's approval is the LAST human step. Within "
                      "about ten minutes of it the post is rendered and then "
                      "either published immediately (publish_at: asap) or "
                      "scheduled for the time you asked for"),
        "consumer": "helpers/express_drop_publisher.py",
    },
}


def accepted_media_types(lane: str) -> tuple[str, ...]:
    """What this lane can actually publish. Empty tuple = unknown lane."""
    spec = LANE_DROP_SPECS.get(lane) or {}
    return tuple(spec.get("media_types") or ())


def _media_phrase(types: tuple[str, ...]) -> str:
    """Prose DERIVED from the enforced tuple, never written beside it -- the
    README claiming "video only" while the validator accepted images is the
    same shape of lie this whole surface was just fixed for."""
    if not types:
        return "unknown"
    if len(types) == 1:
        return f"{types[0]} only"
    return " or ".join(types)


def lane_spec_block() -> str:
    """Render LANE_DROP_SPECS for the lanes that can actually take a drop."""
    out = []
    for lane in sorted(consuming_lanes()):
        spec = LANE_DROP_SPECS.get(lane)
        if not spec:  # pragma: no cover - the contract test forbids this
            out.append(f"    - {lane}\n        (undocumented — ask before using)")
            continue
        out.append(
            f"    - {lane}\n"
            f"        {spec['what']}\n"
            f"        posts: {spec['day']}\n"
            f"        accepts: {_media_phrase(tuple(spec['media_types']))}\n"
            f"        after approval: {spec['publishes']}")
    return "\n".join(out)


def consuming_lanes() -> frozenset[str]:
    """Lanes a drop can actually be used by, which is what the agent needs."""
    return DROP_CONSUMING_LANES


def allowed_lanes() -> frozenset[str]:
    """Every lane name this system publishes. Read from the flow registry when
    it is importable so this cannot drift into a private second list.

    NOT the set a drop may name -- see DROP_CONSUMING_LANES. This stays wide
    so an unconsumable lane gets the specific "cannot take a drop yet" refusal
    rather than a misleading "not a known lane"."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import duck_flows  # noqa: PLC0415
        names = {str(k) for k in getattr(duck_flows, "FLOWS", {}).keys()}
        return frozenset(names | _FALLBACK_LANES) if names else _FALLBACK_LANES
    except Exception:
        return _FALLBACK_LANES


def _refuse_if_inside_mirror(drop_root: Path) -> None:
    """The mirror publishes with --delete. A drop root underneath it would be
    erased on the next mirror run, so this is a hard refusal rather than a
    warning nobody reads."""
    try:
        resolved = drop_root.resolve()
        mirror = _mirror_root().resolve()
    except Exception:
        return
    if resolved == mirror or mirror in resolved.parents:
        raise DropRefused(
            f"drop root {resolved} is inside the mirror root {mirror}, which "
            f"is published with `rsync --delete`. Everything dropped there "
            f"would be deleted by the next mirror run."
        )


def _refuse_if_symlinked(drop_root: Path) -> None:
    """A Drive *shortcut* is mounted as a symlink into
    `.shortcut-targets-by-id/`, and its target can sit anywhere -- including
    the read-only Computers backup. Accepting one would mean moving the
    operator's files into a directory nobody chose. There is no case where the
    drop root is legitimately a symlink. (Mirror side: Surface 129.)
    """
    if drop_root.is_symlink():
        raise DropRefused(
            f"drop root {drop_root} is a symlink (-> {os.readlink(drop_root)}). "
            f"Delete the Drive shortcut and use a real folder in My Drive."
        )


def _is_quiescent(path: Path, *, now: float | None = None,
                  settle: int = QUIESCENT_SECONDS) -> bool:
    try:
        stat = path.lstat()
    except OSError:
        return False
    now = now if now is not None else time.time()
    return (now - stat.st_mtime) >= settle


def _magic_matches(path: Path, suffix: str) -> bool:
    signatures = _MAGIC.get(suffix)
    if not signatures:
        return False
    try:
        head = path.open("rb").read(16)
    except OSError:
        return False
    for sig in signatures:
        if suffix in _VIDEO_EXTS:
            # ISO base media: 4-byte size then 'ftyp'.
            if head[4:8] == sig:
                return True
        elif head.startswith(sig):
            return True
    return False


def validate_drop(asset: Path, sidecar: Path, *,
                  lanes: frozenset[str] | None = None) -> dict:
    """Decide whether one asset+sidecar pair may become a proposal.

    Raises DropRefused with a reason meant to be read by whoever wrote the
    file. Returns the proposal fields on success.
    """
    lanes = lanes if lanes is not None else allowed_lanes()

    # A symlink is an invitation to read something outside the folder.
    if asset.is_symlink() or sidecar.is_symlink():
        raise DropRefused("symlinks are not accepted; drop the real file")
    if not asset.is_file():
        raise DropRefused(f"{asset.name} is not a regular file")
    if not sidecar.is_file():
        raise DropRefused(
            f"no sidecar: expected {sidecar.name} describing {asset.name}")

    suffix = asset.suffix.lower()
    if suffix not in _IMAGE_EXTS | _VIDEO_EXTS:
        raise DropRefused(
            f"{suffix or 'no extension'} is not an accepted type "
            f"({', '.join(sorted(_IMAGE_EXTS | _VIDEO_EXTS))})")

    size = asset.stat().st_size
    if size == 0:
        raise DropRefused("the file is empty")
    cap = MAX_IMAGE_BYTES if suffix in _IMAGE_EXTS else MAX_VIDEO_BYTES
    if size > cap:
        raise DropRefused(
            f"{size / 2**20:.0f} MB exceeds the {cap // 2**20} MB limit "
            f"for {'images' if suffix in _IMAGE_EXTS else 'video'}")

    # The extension is a claim; the bytes are the evidence.
    if not _magic_matches(asset, suffix):
        raise DropRefused(
            f"the contents are not a real {suffix.lstrip('.')} file — the "
            f"extension and the data disagree")

    if sidecar.stat().st_size > MAX_SIDECAR_BYTES:
        raise DropRefused("the sidecar json is implausibly large")
    try:
        meta = json.loads(sidecar.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DropRefused(f"the sidecar json does not parse: {exc}") from None
    if not isinstance(meta, dict):
        raise DropRefused("the sidecar json must be an object")

    caption = meta.get("caption")
    if not isinstance(caption, str) or not caption.strip():
        raise DropRefused("the sidecar needs a non-empty \"caption\"")
    # Control characters other than newline/tab have no place in a caption and
    # are how a terminal-rendered surface gets hijacked.
    cleaned = "".join(ch for ch in caption if ch in "\n\t" or ch >= " ")
    if len(cleaned) > MAX_CAPTION_CHARS:
        raise DropRefused(
            f"the caption is {len(cleaned)} characters; the limit is "
            f"{MAX_CAPTION_CHARS}")

    lane = str(meta.get("lane") or "").strip()
    if lane not in lanes:
        raise DropRefused(
            f"\"lane\": {lane or '(missing)'} is not a known lane "
            f"({', '.join(sorted(lanes))})")
    media_type = "image" if suffix in _IMAGE_EXTS else "video"
    accepted = accepted_media_types(lane)
    if accepted and media_type not in accepted:
        raise DropRefused(
            f"the \"{lane}\" lane publishes {_media_phrase(accepted)}, and "
            f"this is {'an' if media_type == 'image' else 'a'} {media_type}. "
            "It would be accepted here and then fail when the lane tried to "
            "use it.")
    consuming = consuming_lanes()
    if lane not in consuming:
        # Refused rather than accepted, because accepting is the worse
        # failure: the operator approves it and believes it is scheduled,
        # and it never runs. Say what to do instead of just saying no.
        raise DropRefused(
            f"the \"{lane}\" lane cannot take a drop yet -- nothing in it "
            "claims one, so this file would be approved and then never "
            "published. Lanes that can take a drop today: "
            f"{', '.join(sorted(consuming))}. Ask for \"{lane}\" to be "
            "wired if you need it.")

    source_agent = str(meta.get("source_agent") or "").strip()
    if not source_agent:
        raise DropRefused("the sidecar needs \"source_agent\" naming who made this")

    # Defaults FALSE, and a non-boolean is refused rather than coerced.
    # This is the field that decides whether a scheduled flow may publish the
    # drop in place of its own work, so "absent" must never read as "ready" --
    # and the string "false" must never read as True, which is exactly what
    # bool() would have done with it.
    post_ready = meta.get("post_ready", False)
    if not isinstance(post_ready, bool):
        raise DropRefused(
            '"post_ready" must be true or false (a JSON boolean), not '
            f'{type(post_ready).__name__}'
        )

    # The text burned INTO the video, as distinct from the caption posted
    # beside it. A dropped clip arrives without it: the agent writes the words
    # and the lane composites them, because a reel with no on-screen text reads
    # as an unfinished repost. Optional -- a clip whose maker already rendered
    # text into the frames should leave this out rather than get two copies.
    hook = meta.get("on_screen_hook_text", "")
    if not isinstance(hook, str):
        raise DropRefused(
            '"on_screen_hook_text" must be a string, not '
            f'{type(hook).__name__}')
    hook = "".join(ch for ch in hook if ch >= " ").strip()
    if len(hook) > MAX_HOOK_CHARS:
        raise DropRefused(
            f"\"on_screen_hook_text\" is {len(hook)} characters; the limit is "
            f"{MAX_HOOK_CHARS} because it has to stay legible burned over a "
            "phone-sized frame")

    # The second beat. `overlay_hook_line` can composite a SECOND card along
    # the bottom from `closing_at` seconds onward, and that is the right home
    # for a punchline: the first drop's hook was one sentence with a payoff
    # ("...turns green (and your holiday dash crew isn't having it)") and at
    # 132 characters it did not fit the frame as one card. Trimming it to fit
    # cost the joke. Two beats fit where one does not.
    closing = meta.get("closing_line", "")
    if not isinstance(closing, str):
        raise DropRefused(
            f'"closing_line" must be a string, not {type(closing).__name__}')
    closing = "".join(ch for ch in closing if ch >= " ").strip()
    if len(closing) > MAX_HOOK_CHARS:
        raise DropRefused(
            f'"closing_line" is {len(closing)} characters; the limit is '
            f"{MAX_HOOK_CHARS}")
    closing_at = meta.get("closing_at", 0.0)
    if not isinstance(closing_at, (int, float)) or isinstance(closing_at, bool):
        raise DropRefused(
            f'"closing_at" must be a number of seconds, not '
            f"{type(closing_at).__name__}")
    if closing_at < 0:
        raise DropRefused('"closing_at" cannot be negative')
    if closing and not closing_at:
        raise DropRefused(
            '"closing_line" needs "closing_at" -- the second card appears '
            "from that many seconds in, and 0 would stack it on the hook")

    # WHEN it goes out. Only the express lane has this question to answer --
    # every other lane's time comes from its own weekly window, so a
    # `publish_at` there would be silently ignored, which is the exact shape
    # of lie this contract keeps having to fix. Refused instead, naming the
    # lane that does honour it.
    publish_at_raw = meta.get("publish_at")
    if lane == EXPRESS_LANE:
        kind, when = resolve_publish_at(publish_at_raw)
        publish_at = PUBLISH_AT_ASAP if kind == PUBLISH_AT_ASAP else when.isoformat()
    else:
        if publish_at_raw is not None:
            raise DropRefused(
                f'"publish_at" has no meaning on the "{lane}" lane -- that '
                f"lane publishes on its own weekly day, so the time would be "
                f'ignored. Use "lane": "{EXPRESS_LANE}" for a post that goes '
                "out at a time you choose, or drop the field.")
        publish_at = ""

    return {
        "lane": lane,
        "publish_at": publish_at,
        "caption": cleaned.strip(),
        "closing_line": closing,
        "closing_at": float(closing_at),
        "media_type": "image" if suffix in _IMAGE_EXTS else "video",
        "source_agent": source_agent,
        "notes": str(meta.get("notes") or "")[:2000],
        "on_screen_hook_text": hook,
        "bytes": size,
        "post_ready": post_ready,
    }


def _move(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    if dest.exists():
        stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
        dest = dest_dir / f"{src.stem}.{stamp}{src.suffix}"
    shutil.move(str(src), str(dest))
    return dest


def scan_inbox(drop_root: Path | None = None, *,
               now: float | None = None,
               settle: int = QUIESCENT_SECONDS) -> dict:
    """Process every settled candidate in the inbox. Never raises on one bad
    drop: a refusal is an outcome, not an error."""
    drop_root = Path(drop_root) if drop_root is not None else DROP_ROOT
    _refuse_if_inside_mirror(drop_root)
    _refuse_if_symlinked(drop_root)

    inbox = drop_root / INBOX_DIR
    manifest: dict = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "drop_root": str(drop_root),
        "available": inbox.is_dir(),
        "accepted": [],
        "rejected": [],
        "waiting": [],
        "proposals": [],
    }
    if not inbox.is_dir():
        manifest["detail"] = (
            f"{inbox} does not exist yet; run with --init or add the folder "
            f"to Drive for desktop")
        return manifest

    lanes = allowed_lanes()
    for asset in sorted(inbox.iterdir()):
        if asset.name.startswith(".") or asset.is_dir():
            continue
        if asset.suffix.lower() == ".json":
            continue  # sidecars are found via their asset, never alone

        sidecar = asset.with_suffix(asset.suffix + ".json")
        if not sidecar.exists():
            sidecar = asset.with_suffix(".json")

        record = DropRecord(
            stem=asset.stem, asset=asset.name, status="rejected",
            received_at=datetime.now().astimezone().isoformat(),
        )
        try:
            # Ahead of the settle gate on purpose. A symlink is never
            # acceptable, and one pointing at a file that keeps changing
            # would otherwise sit in `waiting` forever instead of being
            # refused.
            if asset.is_symlink() or (sidecar.exists() and sidecar.is_symlink()):
                raise DropRefused("symlinks are not accepted; drop the real file")

            # Both halves must have settled: a sidecar that is still arriving
            # parses as truncated json and reads as a malformed drop.
            if not _is_quiescent(asset, now=now, settle=settle) or (
                sidecar.exists()
                and not _is_quiescent(sidecar, now=now, settle=settle)
            ):
                manifest["waiting"].append(asset.name)
                continue

            fields = validate_drop(asset, sidecar, lanes=lanes)
        except DropRefused as exc:
            record.reason = str(exc)
            rejected_dir = drop_root / REJECTED_DIR
            moved = _move(asset, rejected_dir)
            if sidecar.exists():
                _move(sidecar, rejected_dir)
            # Written beside the file so Drive carries the reason back up to
            # whoever dropped it, instead of it living only in our logs.
            (rejected_dir / f"{moved.stem}.reason.txt").write_text(
                f"Refused {record.received_at}\n\n{exc}\n", encoding="utf-8")
            manifest["rejected"].append(record.__dict__)
            continue

        record.status = "accepted"
        record.lane = fields["lane"]
        record.media_type = fields["media_type"]
        record.bytes = fields["bytes"]
        record.caption_chars = len(fields["caption"])
        record.source_agent = fields["source_agent"]

        accepted_dir = drop_root / ACCEPTED_DIR
        moved = _move(asset, accepted_dir)
        if sidecar.exists():
            _move(sidecar, accepted_dir)
        manifest["accepted"].append(record.__dict__)
        manifest["proposals"].append({
            **fields,
            "asset_path": str(moved),
            "stem": record.stem,
            "received_at": record.received_at,
            # Explicit, so no later reader can mistake this for approved work.
            # ONE field carries the decision. There used to be a `status:
            # "awaiting_operator"` beside it, written here and never updated
            # again, so every declined or approved drop in the ledger still
            # read "awaiting_operator" forever -- the two-fields-one-meaning
            # trap `STATUS_CONTRACT.md` exists to prevent. Nothing had come
            # to depend on it yet, which is the only reason removing it was
            # cheap; a week later it would have had a reader.
            "approval": "operator_required",
        })

    # Sidecars with no asset. The loop above cannot see these by design -- it
    # finds a sidecar THROUGH its asset -- so they need their own pass or they
    # are reported nowhere at all.
    manifest["incomplete"] = _collect_orphan_sidecars(drop_root, inbox, now=now)

    manifest["accepted_count"] = len(manifest["accepted"])
    manifest["rejected_count"] = len(manifest["rejected"])
    manifest["waiting_count"] = len(manifest["waiting"])
    manifest["incomplete_count"] = len(manifest["incomplete"])
    return manifest


def _asset_for_sidecar(sidecar: Path) -> tuple[str, bool]:
    """(what asset this sidecar is waiting for, whether any such file exists).

    Two spellings are accepted for a sidecar, so both are checked here:
    `post.mp4.json` names its asset exactly, while a bare `post.json` matches
    any `post.*` that is not itself a sidecar.
    """
    inner = Path(sidecar.stem)            # post.mp4.json -> post.mp4
    if inner.suffix:                       # the exact-name spelling
        return inner.name, (sidecar.parent / inner.name).is_file()
    for sibling in sidecar.parent.iterdir():  # the bare spelling
        if sibling == sidecar or sibling.is_dir():
            continue
        if sibling.suffix.lower() == ".json":
            continue
        if sibling.stem == sidecar.stem:
            return sibling.name, True
    return f"{sidecar.stem}.<ext>", False


def _collect_orphan_sidecars(drop_root: Path, inbox: Path, *,
                             now: float | None = None) -> list[dict]:
    """Surface every sidecar whose asset is missing, and refuse the stale ones.

    Inside the grace window this only REPORTS: the asset may still be on its
    way up from Drive, and refusing a good drop because its video is large
    would be worse than waiting. Past the window the sidecar is moved to
    `rejected/` with a reason written beside it, which is the only channel
    that reaches the agent that wrote it.
    """
    current = time.time() if now is None else now
    out: list[dict] = []
    for sidecar in sorted(inbox.iterdir()):
        if sidecar.is_dir() or sidecar.name.startswith("."):
            continue
        if sidecar.suffix.lower() != ".json":
            continue
        expected, present = _asset_for_sidecar(sidecar)
        if present:
            continue  # the loop above owns it
        try:
            age = current - sidecar.stat().st_mtime
        except OSError:  # pragma: no cover - vanished mid-scan
            continue
        declared: dict = {}
        try:
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                declared = {k: payload.get(k) for k in
                            ("lane", "caption", "source_agent", "publish_at")}
        except Exception:
            declared = {}
        row = {
            "sidecar": sidecar.name,
            "stem": Path(sidecar.stem).stem,
            "expected_asset": expected,
            "age_seconds": int(age),
            "declared": declared,
            "state": "waiting_for_asset",
        }
        if age > ORPHAN_SIDECAR_GRACE_SECONDS:
            reason = (
                f"the sidecar arrived without its asset. Nothing named "
                f"\"{expected}\" is in {INBOX_DIR}/, and it has been "
                f"{int(age // 60)} minutes. Both halves must land: upload the "
                f"asset first, then the sidecar, so the post is never read "
                f"before its file is there. Re-send both.\n\n"
                f"NOTE: the rest of this sidecar has not been checked. Every "
                f"other rule -- the caption, the lane, source_agent, "
                f"post_ready, publish_at -- is checked only when both halves "
                f"are present, by one validator, so there is no second "
                f"opinion here that could disagree with it. A re-send can "
                f"therefore still be refused for a different reason.")
            rejected_dir = drop_root / REJECTED_DIR
            moved = _move(sidecar, rejected_dir)
            (rejected_dir / f"{moved.stem}.reason.txt").write_text(
                f"Refused {datetime.now().astimezone().isoformat()}\n\n"
                f"{reason}\n", encoding="utf-8")
            row["state"] = "refused"
            row["reason"] = reason
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# The proposal ledger
# ---------------------------------------------------------------------------
#
# Three conditions gate a claim, and all three exist for a reason the review
# of this lane kept coming back to: the meme lane AUTO-PUBLISHES. On 2026-11-09
# it scheduled a real Facebook post at 09:00 with no human in the loop. So a
# file an external agent writes into a Drive folder is one wiring step away
# from a public post, and "never silently promote a lane from manual approval
# to auto-action" is the house rule that forbids closing that gap by default.
#
#   1. the sidecar declared `post_ready: true`   -- the AGENT says it is done
#   2. the operator approved it                  -- a HUMAN says it may go out
#   3. nothing has claimed it yet                -- it goes out at most once
#
# Condition 2 is the one that keeps the lane honest. 1 and 3 are correctness;
# 2 is governance, and it is why a drop cannot reach the public by itself.


def _read_json_file(path: Path) -> dict:
    """Whole-document read. `_read_ledger` returns only the proposals map, so
    the feedback ledger -- which has a different shape -- needs its own."""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_ledger(path: Path) -> dict:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload.get("proposals", {}) if isinstance(payload, dict) else {}


def _write_ledger(path: Path, proposals: dict) -> None:
    _refuse_test_mode_prod_write(path)
    _write_json_atomic(Path(path), {
        "generated_at": datetime.now().astimezone().isoformat(),
        "proposals": proposals,
    })


def record_proposals(manifest: dict, ledger_path: Path | None = None) -> dict:
    """Merge a scan's proposals into the durable ledger.

    Existing entries are never overwritten: a stem already in the ledger keeps
    its approval and claim state, because re-running a scan must not quietly
    un-approve or un-claim anything. `_move` already makes stems unique on
    collision, so a genuinely new file cannot land on an old stem.
    """
    path = Path(ledger_path) if ledger_path is not None else DROP_PROPOSALS_PATH
    proposals = _read_ledger(path)
    added = []
    for prop in manifest.get("proposals") or []:
        stem = prop.get("stem")
        if not stem or stem in proposals:
            continue
        proposals[stem] = {
            **prop,
            "approval": "operator_required",
            "approved_at": None,
            "approved_by": None,
            "claimed_by": None,
            "claimed_at": None,
        }
        added.append(stem)
    if added:
        _write_ledger(path, proposals)
    manifest["ledger_added"] = added
    return proposals


def record_feedback(notes: list[dict], *,
                    ledger_path: Path | None = None) -> dict:
    """Append notes to the durable feedback ledger. Never drops an older one."""
    path = Path(ledger_path) if ledger_path is not None else DROP_FEEDBACK_PATH
    _refuse_test_mode_prod_write(path)
    existing = _read_json_file(path) if path.exists() else {}
    items = existing.get("notes")
    items = items if isinstance(items, list) else []
    seen = {(n.get("file"), n.get("received_at")) for n in items
            if isinstance(n, dict)}
    for note in notes:
        if (note.get("file"), note.get("received_at")) in seen:
            continue
        items.append({**note, "answered_at": None, "answer": None})
    payload = {"generated_at": datetime.now().astimezone().isoformat(),
               "note_count": len(items), "notes": items}
    _write_json_atomic(path, payload)
    return payload


def answer_feedback(file_name: str, answer: str, *,
                    drop_root: Path | None = None,
                    ledger_path: Path | None = None) -> dict:
    """Record an answer AND publish it where the agent will actually see it.

    Writing it only into our ledger would be the same one-way failure this
    channel exists to fix, so the reply is also written into the synced
    answers/ folder.
    """
    path = Path(ledger_path) if ledger_path is not None else DROP_FEEDBACK_PATH
    _refuse_test_mode_prod_write(path)
    payload = _read_json_file(path) if path.exists() else {}
    items = payload.get("notes")
    items = items if isinstance(items, list) else []
    match = next((n for n in items if n.get("file") == file_name), None)
    if match is None:
        raise DropRefused(f"no feedback note named {file_name!r}")
    stamp = datetime.now().astimezone().isoformat()
    match["answered_at"] = stamp
    match["answer"] = answer
    payload.update({"generated_at": stamp, "note_count": len(items),
                    "notes": items})
    _write_json_atomic(path, payload)

    root = Path(drop_root) if drop_root is not None else DROP_ROOT
    out = root / ANSWERS_DIR
    try:
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{Path(file_name).stem}.answer.md").write_text(
            f"# Re: {match.get('subject') or file_name}\n\n"
            f"Answered {stamp}\n\n{answer}\n", encoding="utf-8")
    except OSError as exc:  # pragma: no cover
        print(f"[drop-intake] answer not published to {out}: {exc}",
              file=sys.stderr)
    return match


def set_approval(stem: str, approved: bool, *, by: str = "operator",
                 publish_now: bool = False,
                 ledger_path: Path | None = None) -> dict:
    """Record the operator's decision on one proposal.

    `publish_now` rewrites `publish_at` to `asap` in the same write. It is the
    operator's override for a drop whose requested time has passed: the agent
    cannot set it (nothing in the drop folder reaches this function), and
    without it a missed window would cost a full round trip through Drive for
    a post that is otherwise fine and still wanted.
    """
    path = Path(ledger_path) if ledger_path is not None else DROP_PROPOSALS_PATH
    proposals = _read_ledger(path)
    entry = proposals.get(stem)
    if entry is None:
        raise DropRefused(f"no proposal named {stem}")
    if entry.get("claimed_by"):
        raise DropRefused(
            f"{stem} was already used by {entry['claimed_by']}; approving it "
            f"now would change nothing"
        )
    entry["approval"] = "approved" if approved else "declined"
    entry["approved_at"] = datetime.now().astimezone().isoformat()
    entry["approved_by"] = by
    if publish_now and approved:
        if entry.get("lane") != EXPRESS_LANE:
            raise DropRefused(
                f"{stem} is on the \"{entry.get('lane')}\" lane, which "
                f"publishes on its own weekly day. Only an "
                f"\"{EXPRESS_LANE}\" drop can be released immediately.")
        entry["publish_at"] = PUBLISH_AT_ASAP
        entry["publish_at_overridden_by"] = by
    _write_ledger(path, proposals)
    return entry


def publish_window(entry: dict, *, now: datetime | None = None) -> tuple[str, datetime | None]:
    """Where one ledger entry sits relative to its requested publish time.

    One function, read by the claim filter, the agent's status file and the
    operator's approve message, so none of the three can disagree about
    whether a post is still sendable.

    Returns one of:
      ("none", None)       no requested time -- a weekly lane
      ("asap", None)       go as soon as it is approved
      ("scheduled", dt)    the time is still ahead
      ("missed", dt)       the time has passed
      ("unreadable", None) a stored value that no longer parses
    """
    raw = str(entry.get("publish_at") or "").strip()
    if not raw:
        return "none", None
    if raw.lower() in _ASAP_WORDS:
        return PUBLISH_AT_ASAP, None
    try:
        when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        # Stored by an older writer, or edited by hand. Reported, never
        # treated as "fine" and never treated as "go now".
        return "unreadable", None
    if when.tzinfo is None:
        when = when.replace(tzinfo=_publish_tz())
    now = now or datetime.now().astimezone()
    return ("scheduled", when) if when > now else ("missed", when)


def claimable_for_lane(lane: str, ledger_path: Path | None = None) -> list[dict]:
    """Every proposal a flow could take for `lane`, oldest first.

    Read-only. A caller that wants to know WHY nothing is claimable should
    compare this against the full ledger rather than re-deriving the rules.
    """
    path = Path(ledger_path) if ledger_path is not None else DROP_PROPOSALS_PATH
    out = []
    for stem, entry in _read_ledger(path).items():
        if entry.get("lane") != lane:
            continue
        if entry.get("approval") != "approved":
            continue
        if not entry.get("post_ready"):
            continue
        if entry.get("claimed_by"):
            continue
        if not Path(str(entry.get("asset_path") or "")).is_file():
            continue
        # The fourth condition, and the only one that can go from true to
        # false while nothing changes: a requested time passes. Publishing a
        # dated post after its moment is worse than not publishing it -- a
        # Black Friday joke on the following Tuesday -- so this fails CLOSED
        # and `_drop_state` reports `missed_window` to the agent while
        # `drop approve <stem> now` is the operator's one-word override.
        state, _when = publish_window(entry)
        if state in {"missed", "unreadable"}:
            continue
        out.append({**entry, "stem": stem})
    # FIFO, so a backlog drains in the order it arrived instead of the newest
    # drop starving everything behind it.
    return sorted(out, key=lambda e: str(e.get("received_at") or ""))


def claim_for_lane(lane: str, *, claimed_by: str,
                   ledger_path: Path | None = None) -> dict | None:
    """Take the oldest claimable proposal for `lane`, or None.

    The claim is written before the caller does anything with the result, so a
    flow that crashes mid-publish does not leave the drop claimable by next
    week's run. Re-publishing something that may already be public is the worse
    of the two failures.
    """
    path = Path(ledger_path) if ledger_path is not None else DROP_PROPOSALS_PATH
    candidates = claimable_for_lane(lane, ledger_path=path)
    if not candidates:
        return None
    stem = candidates[0]["stem"]
    proposals = _read_ledger(path)
    entry = proposals.get(stem)
    if entry is None:  # pragma: no cover - lost a race with another writer
        return None
    entry["claimed_by"] = claimed_by
    entry["claimed_at"] = datetime.now().astimezone().isoformat()
    _write_ledger(path, proposals)
    return {**entry, "stem": stem}


# What happened when the lane actually tried to send it. A FOURTH axis, kept
# separate from the three already on the entry on purpose
# ([[feedback_status_fields_one_meaning]]): `approval` says whether it may go,
# `claimed_by` says who took it, `post_ready` says whether its producer
# finished it, and this says what the platforms did. Folding the outcome into
# any of the others would make "taken" and "sent" the same fact, which is
# exactly the confusion this exists to end.
PUBLISH_OUTCOME_STATES = frozenset({
    "published_now",  # live on every platform now
    "scheduled",      # accepted by every platform, holding for its time
    "partial",        # at least one platform took it, at least one refused
    "failed",         # nothing took it
})


def record_publish_outcome(stem: str, *, status: str,
                           platforms: Any = (),
                           error: str = "",
                           run_id: str = "",
                           at: datetime | None = None,
                           ledger_path: Path | None = None) -> dict:
    """Record what the platforms did with an already-claimed drop.

    Until this existed the ledger's story stopped at the claim, so a drop that
    was rendered, uploaded and then refused by both platforms was
    indistinguishable from one that went live -- `_drop_state` told the
    producing agent "a scheduled run took it; it is on the lane" either way,
    and that status file is the ONLY channel that agent has. The operator had
    no better view: the failure lived for ten minutes in a sidecar heartbeat
    that the next pass overwrote, and in a log nobody reads.

    Never changes `approval` or `claimed_by`. A failed publish must NOT make
    the drop claimable again: re-publishing something that may already be
    public is the worse of the two failures, which is the same reason the
    claim is written before the publish is attempted. Releasing it is the
    operator's call, by approving a fresh drop.

    Raises DropRefused for an unknown status or an unknown stem -- a
    plausible-looking default here would write a green outcome over a real
    failure ([[feedback_plausible_fallbacks_mask_failure]]).
    """
    if status not in PUBLISH_OUTCOME_STATES:
        raise DropRefused(
            f"{status!r} is not a publish outcome; expected one of "
            f"{', '.join(sorted(PUBLISH_OUTCOME_STATES))}")
    path = Path(ledger_path) if ledger_path is not None else DROP_PROPOSALS_PATH
    proposals = _read_ledger(path)
    entry = proposals.get(stem)
    if entry is None:
        raise DropRefused(f"no proposal named {stem}")
    entry["publish_status"] = status
    entry["publish_outcome_at"] = (at or datetime.now().astimezone()).isoformat()
    entry["publish_platforms"] = [str(p) for p in (platforms or [])]
    entry["publish_error"] = str(error or "")
    entry["publish_run_id"] = str(run_id or "")
    _write_ledger(path, proposals)
    return {**entry, "stem": stem}


MIRROR_DROP_README = """# myJeepDuck drop folder

Finished posts go here. An agent writes TWO files with the same stem into
`inbox/`:

    inbox/duck_fact_2026-11-14.mp4
    inbox/duck_fact_2026-11-14.mp4.json

The sidecar:

    {
      "caption": "the post text, up to 2200 characters",
      "lane": "express",
      "publish_at": "asap",
      "source_agent": "who or what made this",
      "post_ready": false,
      "on_screen_hook_text": "optional, up to 120 chars -- see below",
      "closing_line": "optional second beat, shown from closing_at seconds",
      "closing_at": 5.0,
      "notes": "optional, for the operator"
    }

`post_ready` is the important one. Set it to `true` ONLY if this is finished
CONTENT: the caption is exactly what should be posted, and the on-screen text
is settled -- either already burned into the frames, or written in
`on_screen_hook_text` as the exact words to composite. It defaults to `false`,
and `false` is the right answer whenever you are unsure.

`on_screen_hook_text` is the text that goes INTO the video, as distinct from
the `caption` posted beside it. Leave it out if the frames already carry their
text; filling in both gets two copies on screen.

**Keep it SHORT -- around 80 characters is the practical ceiling.** 120 is the
hard limit here, but what actually fits depends on how wide your clip is: the
text is composited onto a card and centred, and four lines hold roughly 84
characters on a 720-wide clip and 100 on a 1080-wide one. A hook that does not
fit is REFUSED at render time rather than cut off mid-sentence, so an
over-long hook costs you the post, not just the tail of the sentence. If you
have a two-beat line, send the first beat only.

Why this is a field and not a line in `notes`: until 2026-11-16 the rule here
was that `post_ready: true` required the pixels to be final, because overlay
text in `notes` was an instruction with nothing to execute it -- prose a human
had to read and act on. A structured field is executable, so the lane now
composites it with the same ffmpeg pass duckvideo uses. `notes` is still prose
for the operator and nothing reads it as an instruction.

Audio is not yours to set. A reel gets its track from the approved Instagram
audio catalog at publish time, or a local bed if nothing fits; do not count on
music baked into the file surviving.

A drop with `post_ready: true` that the operator then approves can be taken by
that lane's scheduled run INSTEAD of the post it would have generated itself.
That is the whole point of the field, which is why it is a deliberate claim
rather than a default.

## Sending a post that is not tied to a weekly slot

Most lanes here answer "which weekly slot is this for?" — a meme drop waits
for Monday, a jeepfact drop for Wednesday. If what you have is just a post,
use the `express` lane instead and say when you want it out:

    "lane": "express",
    "publish_at": "asap"

    "lane": "express",
    "publish_at": "2026-11-22T18:00"

`publish_at` is REQUIRED on an express drop and REFUSED on every other lane.
Required here because an express post has no weekly slot to fall back on, so
without it there is no answer to when it goes out. Refused elsewhere because
those lanes publish on their own day and the field would be silently ignored,
which is the single failure mode this whole contract is written to avoid.

**`"asap"`** means "as soon as the operator approves it". The runner fires
every ten minutes, so that is the real resolution: expect the post within
about ten minutes of approval, not instantly. `"now"` and `"immediately"` are
accepted as the same thing.

**A time** is ISO-8601. Written without an offset — `"2026-11-22T18:00"` — it
is read as wall-clock time in the shop's own zone (America/New_York), with the
offset computed for THAT DATE, so a time on the far side of a daylight-saving
change is still the hour you asked for. Write the offset explicitly
(`"2026-11-22T18:00-05:00"`) if you would rather be exact. Either way
`status.json` echoes back the time we resolved it to — check it, because an
agent that meant UTC finds out there rather than from a post five hours early.

Two bounds on a time, and the first one is not ours:

  * **at least 10 minutes out.** Facebook refuses a scheduled time nearer than
    that, and our Instagram queue is drained every 10 minutes, so neither
    platform could honour it. Something sooner should be `"asap"`, which takes
    a different path entirely — published immediately rather than scheduled.
  * **at most 60 days out.** That one IS ours. Nobody will remember approving
    a post further ahead than that, and the approval is what authorises it.

**The operator still approves it.** `publish_at` says WHEN, never WHETHER. An
express drop sits in the ledger exactly like any other until a human approves
it, and nothing you can write into this folder changes that.

**A time that passes before approval holds the post.** If you ask for Saturday
18:00 and the operator does not get to it until Sunday, nothing is published:
`status.json` shows `missed_window` and the operator is told the same thing,
with a one-word reply that releases it immediately if they still want it. It
fails that way on purpose — a dated post sent after its moment is worse than
one that never went out. If a post is time-critical, say so in `notes`.

**What an express post actually becomes.** An image publishes as a Facebook
photo and an Instagram feed image. A video publishes as an Instagram reel and
a Facebook PHOTO OF ITS POSTER FRAME — the same thing the meme lane already
does with a dropped clip, said here so you do not discover it from the feed.
There is no Facebook video path.

An express post replaces nothing. Meme Monday still runs as usual; this is an
extra post, not a substitute for a scheduled one.


`lane` must be one of the lanes that can actually TAKE a drop. That is a
smaller set than the lanes this system publishes, and the difference matters:
a lane with no claim wired into it would accept your file, let the operator
approve it, and then never publish it -- so a drop naming one is refused, with
a message saying which lane you named and asking for it to be wired.

The set is read from the live code when this file is written, so it cannot
drift out of date:

{lanes}

If the lane you want is not listed, say so rather than picking one that is.
A meme-lane drop that was really meant for jeepfact publishes to the wrong
audience on the wrong day, which is worse than not publishing.

Read "after approval" above carefully. On two of these lanes the operator's
approval is the LAST human step and the post then goes out unseen, so
`post_ready: true` on an unfinished file is not a draft -- it is a published
mistake.


## Making the file

These are the rules that actually bit us, in the order they bit:

**Portrait, and tell us nothing about size -- we measure it.** The on-screen
hook is composited onto a card built at YOUR clip's width. The first real
drop was 720x1280 against a renderer that assumed 1080, and a third of every
caption line fell off the right edge of the frame while every status said
"ready". That is fixed, but it means an unusual aspect ratio is still your
clip's problem to get right: send something that looks like a phone video.

**A punchline goes in `closing_line`, not in the hook.** A second card can
appear along the bottom of the frame from `closing_at` seconds in, which is
where a payoff belongs. The first drop we ever took wrote one 132-character
sentence with the joke at the end; it did not fit as a single card, and
trimming it to fit cost the joke. Two beats fit where one does not:

    "on_screen_hook_text": "POV: a stock crossover honks the instant it turns green",
    "closing_line": "and the holiday dash crew is not having it",
    "closing_at": 5.0

`closing_at` is required whenever `closing_line` is set -- at 0 the second
card would stack on top of the first.

**Keep `on_screen_hook_text` to about 80 characters.** 120 is the hard limit
but what FITS depends on the clip's width -- four lines hold roughly 84
characters at 720 wide. A hook that does not fit is refused at render time
rather than cut off mid-sentence, so an over-long hook costs the whole post,
not just the tail. A two-beat line should send the first beat only.

**Do not count on audio you baked in.** A reel takes its track at publish
time from an operator-approved catalog, or a local bed when nothing fits. If
the sound is essential to the joke, say so in `notes` and expect a human to
handle it.

**Write the caption as it should appear, hashtags included.** It is posted
verbatim. Nothing is appended, so a caption with no hashtags gets none.

**One asset per drop.** A drop is a single panel or a single clip, never a
deck or a carousel.


## What tends to work

Measured on our own posts, with the confidence stated so you can weigh it.
Treat these as priors, not rules -- and if your own reasoning says otherwise,
say so in `feedback/` rather than quietly complying.

**Ask an A-or-B question, not an open one.** MEDIUM confidence, and the
strongest copy finding we have: six open-question posts earned zero comments
between them, while two that named two options earned five. 6.58 comments per
100 reach against 0.00. Naming the two things is what does the work -- "Santa
duck or the doll duck?" beats "what do you think?".

**Keep the duck the subject.** The ducks are the operator's own 3D prints,
photographed in their own vehicle. The product is the duck, not the scenery,
the vehicle or the weather.

**No hashtags in `on_screen_hook_text`.** They would be burned into the
frame. Hashtags belong in the caption.

**Do not claim things we cannot stand behind** -- no invented specs, prices,
review counts, shipping promises or superlatives.

**What we do NOT know**, stated so you do not optimise for it: whether reels
outperform images here. "reel > image > carousel" was retracted on
2026-11-08 because it does not reproduce -- our reel sample was a single post,
and the surface that reports it refuses to rank a format with fewer than three.
What our own data supports is only image > carousel on median reach, 50.0 over
5 posts against 25.0 over 7. Reels leading across 72 competitor posts is real
evidence and a fair reason to try the format, but it is THEIR evidence. Do not
treat "make it a reel" as a known win.


## Talking back

This is not a one-way contract. Three channels run the other direction, and
using them is how this gets better instead of repeating the same
misunderstanding every week.

**`status.json`** — read this first, every time. It is rewritten on every
scan and says what happened to every file you have sent:

    waiting_for_asset     your sidecar is here and its asset is not, so
                          this is not a post yet. Listed under
                          `incomplete_this_scan`, refused after 30 minutes
    waiting_for_a_human   accepted, the operator has not decided yet
    approved              approved, waiting for its lane's run (or, on
                          express, for the next ten-minute pass)
    scheduled             taken and handed to the platforms, holding until
                          the publish_at you asked for
    published             a run took it and the platforms accepted it.
                          `published_on` lists which ones
    publish_failed        a run took it and EVERY platform refused it.
                          Nothing was posted, it will not be retried, and
                          `publish_error` says why. Fix what the error
                          names and send a new version
    declined              the operator said no; it will never be posted
    not_ready             accepted, but post_ready was false, so no run
                          will ever take it
    missed_window         express only: approved, but the publish_at you
                          asked for has passed, so nothing will send it.
                          The operator can release it immediately; if they
                          do not, send a new version with a future time

If a file you sent is not listed at all, it was refused before it became a
proposal: look in `rejected/` for a `.reason.txt` with the same name. Those
reasons are written for you, not for us -- they say what to change.

**Upload the asset BEFORE the sidecar.** A sidecar whose asset is missing is
not a post: it appears under `incomplete_this_scan` in `status.json` as
`waiting_for_asset` while we assume the file is still coming up from Drive,
and after 30 minutes it is refused with a reason in `rejected/`. Sending the
two halves in that order means a drop is never read before its file is there,
and it means an upload that silently failed shows up as a refusal you can act
on rather than as a post that never happens. Measured 2026-11-19: a sidecar
arrived whose `.mp4` never did, and for eleven hours nothing said so.

Each drop also echoes `publish_at` as we resolved it, which is the field to
check when a time did not land where you expected.

Once a run has tried, three more fields appear: `publish_status`,
`published_on` and `publish_error`. A `published` drop with only one entry in
`published_on` went out on that platform and was refused by the other, so
`publish_error` is worth reading even on a success.

A run of `declined`, or a pile of `not_ready`, is the signal to ask rather
than to send more of the same. A `publish_failed` is the strongest such
signal, because the operator had already said yes: whatever `publish_error`
names was ours or yours, and asking in `feedback/` beats sending the same
post again. So is a `missed_window`: it means the gap
between your sending and the operator approving was longer than the lead time
you left, so leave more of it or send `asap` and let the approval set the
time.

**`feedback/`** — write a `.json`, `.md` or `.txt` file here to ask a
question, say a rule is ambiguous, report that something did not work, or
propose a change. For JSON we read `from`, `subject`, `about` and `body`;
anything else is read as prose. It is deliberately NOT validated the way a
drop is -- a refused drop is safe, a refused question is a channel nobody
uses twice. Say what you actually think, including that a rule here is wrong.

Worth sending: a field whose meaning you had to guess, a refusal you did not
understand, a constraint that made you produce something worse, or a pattern
you noticed in what gets approved.

**`answers/`** — replies land here as `<your-file>.answer.md`. When an answer
changes a rule, this README changes too, so re-read it when an answer says so.

The source data these posts should be built from is the separate `DuckData`
sync folder. Read its README.md first: most of that tree is a current
snapshot, not a history, and its Retention section says which parts are which.

Accepted files move to `accepted/`. Refused files move to `rejected/` beside
a `.reason.txt` saying why -- read that rather than guessing.

Accepted is NOT published. Every drop becomes a proposal the operator
approves by hand, the same as every other post in this system. Approval is a
human action taken here, not something a sidecar can grant itself: a drop that
is `post_ready` but unapproved sits in the ledger untouched forever. That is
as true of an express drop as of any other -- `publish_at` chooses the time,
never the permission.

Types: .png .jpg .jpeg (max 25 MB), .mp4 .mov (max 300 MB). The contents
must actually be that type; the extension alone is not enough.

Do not write into the `DuckData` folder. It is published with `rsync --delete`
and anything left there is erased on the next run.
"""


def _drop_state(entry: dict) -> tuple[str, str]:
    """(state, detail) for one ledger entry, in the agent's terms.

    Deliberately phrased for the producer, not for us: "waiting for a human"
    tells an agent something it can act on, while `approval=operator_required`
    tells it only that we have fields.
    """
    if entry.get("claimed_by"):
        # The claim is not the outcome. Before `publish_status` existed this
        # branch answered "published" for every claimed drop, including one
        # whose render succeeded and whose platforms both refused it -- a flat
        # lie to the only channel the producing agent has, and the one failure
        # nothing else could see.
        outcome = str(entry.get("publish_status") or "")
        err = str(entry.get("publish_error") or "").strip()
        took = ", ".join(str(p) for p in (entry.get("publish_platforms") or []))
        if outcome == "failed":
            return ("publish_failed",
                    "a run took it and every platform refused it"
                    + (f": {err}" if err else "")
                    + ". It will NOT be retried -- re-send a new version. "
                      "Nothing was posted.")
        if outcome == "partial":
            return ("published",
                    f"a run took it and {took or 'some platforms'} accepted it, "
                    f"while the rest refused it"
                    + (f": {err}" if err else "")
                    + ". Partly public -- do not re-send the same post without "
                      "checking what is already live.")
        if outcome == "scheduled":
            return ("scheduled",
                    f"taken and accepted by {took or 'the platforms'}, holding "
                    f"until {entry.get('publish_at')}")
        if outcome == "published_now":
            return ("published", f"live on {took or 'the platforms'}")
        state, when = publish_window(entry)
        if state == "scheduled" and when is not None:
            return ("scheduled",
                    f"taken and scheduled to publish at {when.isoformat()}")
        # Claimed with no outcome recorded. Honest about the gap rather than
        # guessing: a run that dies between the claim and the publish lands
        # here, and so does a drop claimed by a lane that predates the
        # outcome write-back.
        return ("published", "a scheduled run took it; it is on the lane")
    approval = str(entry.get("approval") or "")
    if approval == "declined":
        return ("declined", "the operator declined it; it will never be posted")
    # Ahead of the approved branch on purpose. `claimable_for_lane` requires
    # post_ready, so an approved drop whose sidecar says post_ready:false can
    # never be taken by anything -- and the old order answered "approved and
    # waiting for its lane's next run", telling the producing agent its post
    # was on the way. This file is the ONLY channel that agent has. The CLI
    # already warns the operator about this exact combination at approval
    # time; the agent was the one left with the wrong answer.
    if not entry.get("post_ready"):
        detail = "accepted, but the sidecar said post_ready:false, so no run will take it"
        if approval == "approved":
            detail += " -- the operator approved it anyway, which changed nothing"
        return ("not_ready", detail)
    if approval == "approved":
        # Ahead of the plain "approved" answer: an express drop whose time has
        # passed is approved, post_ready and still never going out, and
        # "waiting for its lane's next run" would be a flat lie -- the runner
        # fires every ten minutes and skips it every time.
        window, when = publish_window(entry)
        if window == "missed" and when is not None:
            return ("missed_window",
                    f"approved, but the requested publish_at "
                    f"({when.isoformat()}) has passed, so nothing will send "
                    "it. The operator can release it immediately, or send a "
                    "new version with a future time.")
        if window == "unreadable":
            return ("missed_window",
                    "approved, but the stored publish_at no longer parses, so "
                    "nothing will send it. Re-send with a valid publish_at.")
        if window == "scheduled" and when is not None:
            return ("approved",
                    f"approved and waiting to be scheduled for "
                    f"{when.isoformat()}")
        if window == PUBLISH_AT_ASAP:
            return ("approved",
                    "approved and waiting on the express runner, which fires "
                    "every ten minutes")
        return ("approved", "approved and waiting for its lane's next run")
    return ("waiting_for_a_human", "accepted and waiting on the operator")


def write_status_file(drop_root: Path, proposals: dict,
                      *, refused: list[dict] | None = None,
                      incomplete: list[dict] | None = None,
                      now: datetime | None = None) -> Path:
    """Publish what happened to every drop, where the agent can read it.

    This is the half of the loop that was missing. The agent could learn that
    ONE file was refused, by finding a .reason.txt it had to know to look for,
    and nothing at all about whether a file it sent was ever posted. So it had
    no way to tell a good drop from an ignored one, and no reason to change
    anything.
    """
    now = now or datetime.now().astimezone()
    drops = {}
    for stem, entry in (proposals or {}).items():
        if not isinstance(entry, dict):
            continue
        state, detail = _drop_state(entry)
        drops[stem] = {
            "lane": entry.get("lane"),
            "state": state,
            "detail": detail,
            "received_at": entry.get("received_at"),
            "claimed_by": entry.get("claimed_by"),
            "claimed_at": entry.get("claimed_at"),
            # Echoed back so the agent can see what we READ its publish_at
            # as, not just what it wrote: a naive "2026-11-22T18:00" is
            # resolved in the shop's zone, and an agent that meant UTC finds
            # out here rather than from a post five hours early.
            "publish_at": entry.get("publish_at") or None,
            "publish_at_overridden_by": entry.get("publish_at_overridden_by"),
            # What the platforms did, once a run has tried. Null until then.
            # Without this an agent could see that its post was TAKEN and
            # never that it was refused, so a drop that failed for a reason
            # the agent controls -- a caption the platform rejected, an asset
            # it could not render -- taught it nothing and came back the same
            # way next time.
            "publish_status": entry.get("publish_status") or None,
            "publish_error": entry.get("publish_error") or None,
            "published_on": entry.get("publish_platforms") or None,
        }
    payload = {
        "generated_at": now.isoformat(),
        "doc": ("What happened to each file you dropped. Rewritten every scan. "
                "`state` is one of: waiting_for_a_human, approved, scheduled, "
                "published, publish_failed, declined, not_ready, "
                "missed_window. `incomplete_this_scan` lists sidecars whose "
                "asset never arrived -- those are not posts yet and will be "
                "refused if the file does not land. "
                "`publish_failed` means a run took it and "
                "every platform refused it: nothing was posted, it will not "
                "be retried, and `publish_error` says why. If something "
                "you sent is not listed at all it was refused before it "
                "became a proposal -- look in "
                f"{REJECTED_DIR}/ for a .reason.txt with the same name."),
        "lanes_open_for_drops": sorted(consuming_lanes()),
        "drops": drops,
        "refused_this_scan": [
            {"asset": r.get("asset"), "reason": r.get("reason")}
            for r in (refused or []) if isinstance(r, dict)
        ],
        # A sidecar whose asset never arrived. Reported here because it can
        # appear nowhere else: it never becomes a proposal, so it is absent
        # from `drops` above, and until it is refused it is absent from
        # `refused_this_scan` too. An agent that wrote one and saw nothing
        # had no way to tell a lost upload from a post we simply had not got
        # to yet.
        "incomplete_this_scan": [
            {"sidecar": r.get("sidecar"),
             "expected_asset": r.get("expected_asset"),
             "state": r.get("state"),
             "minutes_waiting": int((r.get("age_seconds") or 0) // 60),
             "reason": r.get("reason")}
            for r in (incomplete or []) if isinstance(r, dict)
        ],
        "how_to_reply": (
            f"Write a .json, .md or .txt file into {FEEDBACK_DIR}/ to ask a "
            f"question or flag a problem. Answers appear in {ANSWERS_DIR}/."),
    }
    path = Path(drop_root) / STATUS_FILE
    _write_json_atomic(path, payload)
    return path


def collect_feedback(drop_root: Path, *, now: datetime | None = None) -> list[dict]:
    """Pick up notes the agent left for us, and file them.

    Not validated the way a drop is, on purpose: a refused drop is safe, a
    refused question is a channel nobody uses twice. Anything readable is
    taken. A note we cannot parse is still surfaced, because an unparseable
    note is itself a message about the contract.
    """
    now = now or datetime.now().astimezone()
    inbox = Path(drop_root) / FEEDBACK_DIR
    read_dir = Path(drop_root) / FEEDBACK_READ_DIR
    if not inbox.is_dir():
        return []
    collected: list[dict] = []
    for path in sorted(inbox.iterdir()):
        if path.is_dir() or path.name.startswith("."):
            continue
        if path.suffix.lower() not in FEEDBACK_EXTS:
            continue
        try:
            if path.stat().st_size > MAX_FEEDBACK_BYTES:
                body, parsed = "(note too large to read)", None
            else:
                body = path.read_text(encoding="utf-8", errors="replace")
                parsed = None
                if path.suffix.lower() == ".json":
                    try:
                        parsed = json.loads(body)
                    except Exception:
                        parsed = None
        except OSError as exc:  # pragma: no cover - unreadable file
            body, parsed = f"(could not read: {exc})", None
        note = {
            "file": path.name,
            "received_at": now.isoformat(),
            "from": (parsed or {}).get("from") if isinstance(parsed, dict) else None,
            "subject": (parsed or {}).get("subject") if isinstance(parsed, dict) else None,
            "about": (parsed or {}).get("about") if isinstance(parsed, dict) else None,
            "body": ((parsed or {}).get("body") if isinstance(parsed, dict) else None)
                    or body[:MAX_FEEDBACK_BYTES],
            "parsed": isinstance(parsed, dict),
        }
        collected.append(note)
        try:
            read_dir.mkdir(parents=True, exist_ok=True)
            os.replace(path, _unique_dest(read_dir, path.name))
        except OSError as exc:  # pragma: no cover
            print(f"[drop-intake] could not file feedback {path.name}: {exc}",
                  file=sys.stderr)
    return collected


def _unique_dest(dest_dir: Path, name: str) -> Path:
    dest = dest_dir / name
    n = 2
    while dest.exists():
        dest = dest_dir / f"{Path(name).stem}_{n}{Path(name).suffix}"
        n += 1
    return dest


def init_drop_root(drop_root: Path | None = None) -> Path:
    drop_root = Path(drop_root) if drop_root is not None else DROP_ROOT
    _refuse_if_inside_mirror(drop_root)
    _refuse_if_symlinked(drop_root)
    for sub in (INBOX_DIR, ACCEPTED_DIR, REJECTED_DIR,
                FEEDBACK_DIR, FEEDBACK_READ_DIR, ANSWERS_DIR):
        (drop_root / sub).mkdir(parents=True, exist_ok=True)
    # The lane list is rendered from the registry rather than typed into the
    # template: a lane added upstream would otherwise be refused here while
    # the contract an agent reads still said it was invalid.
    write_contract_readme(drop_root)
    return drop_root


def write_contract_readme(drop_root: Path) -> Path:
    """Publish the contract where the agent reads it. Idempotent.

    Called on every scan, not only on `--init`. Until 2026-11-19 it was
    init-only, so the README in Drive was whatever the contract looked like
    the last time someone ran that flag by hand -- and a rule added to the
    validator reached the agent only as an unexplained refusal. A contract
    that drifts from the code enforcing it is the one failure this whole lane
    is built around ([[feedback_fold_defer_is_a_handoff_contract]]), and the
    lane list is already rendered from the live registry for exactly that
    reason, which init-only writing quietly undid.
    """
    lanes = lane_spec_block()
    path = Path(drop_root) / "README.md"
    text = MIRROR_DROP_README.replace("{lanes}", lanes)
    # Rewritten only on a real change: this folder is synced, and touching an
    # unchanged file six times a day would upload it six times a day.
    try:
        if path.read_text(encoding="utf-8") == text:
            return path
    except OSError:
        pass
    path.write_text(text, encoding="utf-8")
    return path


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--drop-root", default=None)
    ap.add_argument("--state-path", default=None)
    ap.add_argument("--init", action="store_true",
                    help="create the folder structure and write the README")
    ap.add_argument("--settle-seconds", type=int, default=QUIESCENT_SECONDS)
    ap.add_argument("--ledger-path", default=None)
    ap.add_argument("--list", action="store_true",
                    help="show the proposal ledger and why each is or is not claimable")
    ap.add_argument("--approve", metavar="STEM",
                    help="approve a proposal so a flow may publish it")
    ap.add_argument("--decline", metavar="STEM",
                    help="decline a proposal so no flow ever takes it")
    ap.add_argument("--feedback", action="store_true",
                    help="show notes the agent has left for us")
    ap.add_argument("--answer", nargs=2, metavar=("FILE", "TEXT"),
                    help="answer a feedback note; the reply is published "
                         "into the synced answers/ folder")
    args = ap.parse_args(argv)

    if args.feedback:
        payload = _read_json_file(DROP_FEEDBACK_PATH) if DROP_FEEDBACK_PATH.exists() else {}
        notes = payload.get("notes") or []
        if not notes:
            print("[drop-intake] no feedback")
            return 0
        for n in notes:
            mark = "ANSWERED" if n.get("answered_at") else "UNANSWERED"
            print(f"  [{mark}] {n.get('file')} — {n.get('subject') or '(no subject)'}"
                  f" (from {n.get('from') or 'unknown'})")
            body = str(n.get("body") or "").strip().splitlines()
            for line in body[:6]:
                print(f"      {line}")
            if len(body) > 6:
                print(f"      ... {len(body) - 6} more line(s)")
        return 0

    if args.answer:
        try:
            note = answer_feedback(args.answer[0], args.answer[1])
        except DropRefused as exc:
            print(f"[drop-intake] {exc}", file=sys.stderr)
            return 1
        print(f"[drop-intake] answered {note.get('file')}; "
              f"reply published to {ANSWERS_DIR}/")
        return 0

    ledger_path = Path(args.ledger_path) if args.ledger_path else DROP_PROPOSALS_PATH

    if args.approve or args.decline:
        stem = args.approve or args.decline
        try:
            entry = set_approval(stem, bool(args.approve), ledger_path=ledger_path)
        except DropRefused as exc:
            print(f"[drop-intake] {exc}", file=sys.stderr)
            return 1
        print(f"[drop-intake] {stem} -> {entry['approval']}")
        if entry["approval"] == "approved" and not entry.get("post_ready"):
            print("  NOTE: the sidecar says post_ready=false, so no flow will "
                  "claim this even though you approved it.", file=sys.stderr)
        return 0

    if args.list:
        proposals = _read_ledger(ledger_path)
        if not proposals:
            print("[drop-intake] no proposals")
            return 0
        # Derived by `_drop_state`, the same function the agent-facing status
        # file uses, rather than a second copy of the rules. The old copy here
        # printed "awaiting operator (declined)" for every declined drop --
        # wrong twice over: nothing is awaited, and the operator already
        # answered.
        for stem, e in sorted(proposals.items()):
            state, detail = _drop_state(e)
            if state == "approved":
                if not Path(str(e.get("asset_path") or "")).is_file():
                    state, detail = ("asset_missing", "approved, but the asset file is gone")
                else:
                    state, detail = ("CLAIMABLE", detail)
            print(f"  {stem}\n      lane={e.get('lane')} from={e.get('source_agent')}"
                  f" -> {state}: {detail}")
        return 0

    drop_root = Path(args.drop_root) if args.drop_root else DROP_ROOT
    if args.init:
        created = init_drop_root(drop_root)
        print(f"[drop-intake] ready at {created}")
        return 0

    manifest = scan_inbox(drop_root, settle=args.settle_seconds)
    proposals = record_proposals(manifest, ledger_path=ledger_path)
    state_path = Path(args.state_path) if args.state_path else DROP_INTAKE_STATE_PATH
    _write_json_atomic(state_path, manifest)

    # Keep the contract the agent reads in step with the code enforcing it.
    # Loud, never fatal: a scan that refused to run because a README could
    # not be written would stop accepting posts over a documentation file.
    try:
        write_contract_readme(drop_root)
    except OSError as exc:
        print(f"[drop-intake] WARNING contract README not refreshed: {exc}",
              file=sys.stderr)

    notes = collect_feedback(drop_root)
    if notes:
        record_feedback(notes)

    # Publish the outcome of every drop where the agent can read it. Written
    # on every scan, including an empty one, because "nothing changed" is also
    # an answer and an agent with no answer assumes the worst or repeats
    # itself.
    write_status_file(drop_root, proposals,
                      refused=manifest.get("rejected") or [],
                      incomplete=manifest.get("incomplete") or [])

    # Loud about an orphan, because it is the one outcome that used to be
    # reported nowhere ([[feedback_swallowed_errors_lie]]).
    for row in manifest.get("incomplete") or []:
        print(f"[drop-intake] INCOMPLETE {row.get('sidecar')}: no "
              f"{row.get('expected_asset')} after "
              f"{int((row.get('age_seconds') or 0) // 60)} min "
              f"({row.get('state')})")

    print(f"[drop-intake] accepted={manifest.get('accepted_count', 0)} "
          f"rejected={manifest.get('rejected_count', 0)} "
          f"waiting={manifest.get('waiting_count', 0)} "
          f"feedback={len(notes)}")
    for note in notes:
        print(f"  FEEDBACK from {note.get('from') or 'an agent'}: "
              f"{note.get('subject') or note.get('file')}")
    for rec in manifest.get("rejected", []):
        print(f"  REFUSED {rec['asset']}: {rec['reason']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
