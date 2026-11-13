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
   publishes with `rsync -a --delete` into ~/DuckData, so a file dropped
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

#: Where Drive syncs the agent's output down. A SECOND sync root, never a
#: subdirectory of the mirror -- see property 2 above.
DROP_ROOT = Path(os.environ.get("DUCK_DROP_ROOT") or (Path.home() / "DuckDrop"))
INBOX_DIR = "inbox"
ACCEPTED_DIR = "accepted"
REJECTED_DIR = "rejected"

#: The mirror's published root. Used only to refuse a catastrophic misconfig.
_MIRROR_ROOT = Path(
    os.environ.get("DRIVE_MIRROR_ROOT") or (Path.home() / "DuckData")
)

DROP_INTAKE_STATE_PATH = REPO_ROOT / "state" / "drop_intake.json"

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
        mirror = _MIRROR_ROOT.resolve()
    except Exception:
        return
    if resolved == mirror or mirror in resolved.parents:
        raise DropRefused(
            f"drop root {resolved} is inside the mirror root {mirror}, which "
            f"is published with `rsync --delete`. Everything dropped there "
            f"would be deleted by the next mirror run."
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

    return {
        "lane": lane,
        "caption": cleaned.strip(),
        "media_type": "image" if suffix in _IMAGE_EXTS else "video",
        "source_agent": source_agent,
        "notes": str(meta.get("notes") or "")[:2000],
        "bytes": size,
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
      "notes": "optional, for the operator"
    }

Accepted files move to `accepted/`. Refused files move to `rejected/` beside
a `.reason.txt` saying why -- read that rather than guessing.

Accepted is NOT published. Every drop becomes a proposal the operator
approves by hand, the same as every other post in this system.

Types: .png .jpg .jpeg (max 25 MB), .mp4 .mov (max 300 MB). The contents
must actually be that type; the extension alone is not enough.

Do not write into ~/DuckData. That folder is published with `rsync --delete`
and anything left there is erased on the next run.
"""


def init_drop_root(drop_root: Path | None = None) -> Path:
    drop_root = Path(drop_root) if drop_root is not None else DROP_ROOT
    _refuse_if_inside_mirror(drop_root)
    for sub in (INBOX_DIR, ACCEPTED_DIR, REJECTED_DIR):
        (drop_root / sub).mkdir(parents=True, exist_ok=True)
    (drop_root / "README.md").write_text(MIRROR_DROP_README, encoding="utf-8")
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
    args = ap.parse_args(argv)

    drop_root = Path(args.drop_root) if args.drop_root else DROP_ROOT
    if args.init:
        created = init_drop_root(drop_root)
        print(f"[drop-intake] ready at {created}")
        return 0

    manifest = scan_inbox(drop_root, settle=args.settle_seconds)
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
