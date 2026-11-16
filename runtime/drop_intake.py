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
    if resolved == _FROZEN_PRODUCTION_PROPOSALS_PATH:
        raise TestModeRefusalError(
            f"drop_intake refused to write {resolved}: "
            f"{_TEST_MODE_REFUSAL_ENV}=1 and this is the frozen production "
            f"proposal ledger."
        )

#: Lanes a drop may name, from the flow registry rather than a second list
#: that drifts. Resolved lazily so importing this module never needs them.
_FALLBACK_LANES = frozenset({
    "meme", "jeepfact", "thursday", "duckvideo", "gtdf", "reviews_story",
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
MAX_SIDECAR_BYTES = 64 * 1024

#: A candidate must be unchanged for this long before it is read, so a file
#: still arriving from Drive is never parsed as a complete one.
QUIESCENT_SECONDS = 45


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


def allowed_lanes() -> frozenset[str]:
    """Lane names a drop may claim. Read from the flow registry when it is
    importable so this cannot drift into a private second list."""
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

    return {
        "lane": lane,
        "caption": cleaned.strip(),
        "media_type": "image" if suffix in _IMAGE_EXTS else "video",
        "source_agent": source_agent,
        "notes": str(meta.get("notes") or "")[:2000],
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
            "approval": "operator_required",
            "status": "awaiting_operator",
        })

    manifest["accepted_count"] = len(manifest["accepted"])
    manifest["rejected_count"] = len(manifest["rejected"])
    manifest["waiting_count"] = len(manifest["waiting"])
    return manifest


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


def set_approval(stem: str, approved: bool, *, by: str = "operator",
                 ledger_path: Path | None = None) -> dict:
    """Record the operator's decision on one proposal."""
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
    _write_ledger(path, proposals)
    return entry


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


MIRROR_DROP_README = """# myJeepDuck drop folder

Finished posts go here. An agent writes TWO files with the same stem into
`inbox/`:

    inbox/duck_fact_2026-11-14.mp4
    inbox/duck_fact_2026-11-14.mp4.json

The sidecar:

    {
      "caption": "the post text, up to 2200 characters",
      "lane": "meme",
      "source_agent": "who or what made this",
      "post_ready": false,
      "notes": "optional, for the operator"
    }

`post_ready` is the important one. Set it to `true` ONLY if the file is
finished exactly as it should appear publicly -- for a video that means any
on-screen text is ALREADY BURNED INTO THE FRAMES and the audio track is the
one that should play. Overlay text written in `notes` is an instruction, not a
rendered video, and a drop like that is a proposal for the operator to finish
rather than something a scheduled flow may post. It defaults to `false`, and
`false` is the right answer whenever you are unsure.

A drop with `post_ready: true` that the operator then approves can be taken by
that lane's scheduled run INSTEAD of the post it would have generated itself.
That is the whole point of the field, which is why it is a deliberate claim
rather than a default.

`lane` must be one of the lanes this system actually publishes. An
unrecognised lane is refused, so the valid set is listed below rather than
left to guess -- it is read from the live flow registry when this file is
written, so it cannot drift out of date:

{lanes}

The source data these posts should be built from is the separate `DuckData`
sync folder. Read its README.md first: most of that tree is a current
snapshot, not a history, and its Retention section says which parts are which.

Accepted files move to `accepted/`. Refused files move to `rejected/` beside
a `.reason.txt` saying why -- read that rather than guessing.

Accepted is NOT published. Every drop becomes a proposal the operator
approves by hand, the same as every other post in this system. Approval is a
human action taken here, not something a sidecar can grant itself: a drop that
is `post_ready` but unapproved sits in the ledger untouched forever.

Types: .png .jpg .jpeg (max 25 MB), .mp4 .mov (max 300 MB). The contents
must actually be that type; the extension alone is not enough.

Do not write into the `DuckData` folder. It is published with `rsync --delete`
and anything left there is erased on the next run.
"""


def init_drop_root(drop_root: Path | None = None) -> Path:
    drop_root = Path(drop_root) if drop_root is not None else DROP_ROOT
    _refuse_if_inside_mirror(drop_root)
    _refuse_if_symlinked(drop_root)
    for sub in (INBOX_DIR, ACCEPTED_DIR, REJECTED_DIR):
        (drop_root / sub).mkdir(parents=True, exist_ok=True)
    # The lane list is rendered from the registry rather than typed into the
    # template: a lane added upstream would otherwise be refused here while
    # the contract an agent reads still said it was invalid.
    lanes = "\n".join(f"    - {lane}" for lane in sorted(allowed_lanes()))
    (drop_root / "README.md").write_text(
        MIRROR_DROP_README.replace("{lanes}", lanes), encoding="utf-8"
    )
    return drop_root


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
    args = ap.parse_args(argv)

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
        for stem, e in sorted(proposals.items()):
            if e.get("claimed_by"):
                why = f"used by {e['claimed_by']}"
            elif e.get("approval") != "approved":
                why = f"awaiting operator ({e.get('approval')})"
            elif not e.get("post_ready"):
                why = "sidecar says post_ready=false"
            elif not Path(str(e.get("asset_path") or "")).is_file():
                why = "asset file is gone"
            else:
                why = "CLAIMABLE"
            print(f"  {stem}\n      lane={e.get('lane')} from={e.get('source_agent')} -> {why}")
        return 0

    drop_root = Path(args.drop_root) if args.drop_root else DROP_ROOT
    if args.init:
        created = init_drop_root(drop_root)
        print(f"[drop-intake] ready at {created}")
        return 0

    manifest = scan_inbox(drop_root, settle=args.settle_seconds)
    record_proposals(manifest, ledger_path=ledger_path)
    state_path = Path(args.state_path) if args.state_path else DROP_INTAKE_STATE_PATH
    _write_json_atomic(state_path, manifest)
    print(f"[drop-intake] accepted={manifest.get('accepted_count', 0)} "
          f"rejected={manifest.get('rejected_count', 0)} "
          f"waiting={manifest.get('waiting_count', 0)}")
    for rec in manifest.get("rejected", []):
        print(f"  REFUSED {rec['asset']}: {rec['reason']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
