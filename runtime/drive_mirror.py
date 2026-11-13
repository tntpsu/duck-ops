#!/usr/bin/env python3
"""Mirror the chosen slice of business data into one directory Google Drive syncs.

This is the first producer in the stack whose SUCCESS condition is that data
leaves the machine. Every other guard here exists to keep things in, which
inverts the failure analysis: the dangerous bug is not "the mirror is empty",
it is "the mirror contains something nobody chose to publish".

Three properties carry the whole surface:

1. **Allowlist, never a denylist.** `WHOLE_TREES` and `CREATIVE_LANES` name
   what goes. A denylist would behave identically today and leak the first
   file a future producer invents.

2. **Nothing reaches the published directory before the gate passes.** Work is
   staged in `~/.DuckData.staging/`, scanned there, and only then released.
   Drive uploads a file the instant it appears, so a file that appears is a
   file that is published.

3. **The gate must prove it looked.** `scripts/secret_scan.py` would have
   passed this folder without reading a byte -- it walks `git ls-files` and
   returns 0 on a non-git directory, and `~/DuckData/` is not a repo. So the
   gate here walks the staged filesystem itself and refuses to release unless
   every eligible file is accounted for. A scanner that scanned nothing must
   not be mistakable for a scanner that found nothing.
   ([[feedback_a_failed_probe_is_a_fact_about_the_probe]])

Release is a delta rsync out of the gated staging tree, NOT a directory
rename. A rename would present Drive with ~178,000 deletions and ~178,000
creations on every run and re-upload the full ~10.3 GB nightly. The safety
property is unchanged -- every byte in the published directory has passed the
gate -- and rsync renames each file into place individually, so Drive never
sees a half-written file.
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

REPO_ROOT = Path(__file__).resolve().parents[1]
DUCK_OPS_ROOT = REPO_ROOT
DUCKAGENT_ROOT = Path(
    os.environ.get("DUCKAGENT_ROOT") or (REPO_ROOT.parent / "duckAgent")
)

#: Where Drive for desktop watches. Overridable for operators with a different
#: Drive layout; the test-mode guard below keys on the factory default.
DRIVE_MIRROR_ROOT = Path(
    os.environ.get("DRIVE_MIRROR_ROOT") or (Path.home() / "DuckData")
)
#: Staging lives OUTSIDE the mirror root on purpose. A staging directory nested
#: inside the synced folder would upload every ungated intermediate.
DRIVE_MIRROR_STAGING_ROOT = Path(
    os.environ.get("DRIVE_MIRROR_STAGING_ROOT")
    or (Path.home() / ".DuckData.staging")
)
#: State the grader and the OS card read.
DRIVE_MIRROR_STATE_PATH = REPO_ROOT / "state" / "drive_mirror.json"
#: Which (path, size, mtime) tuples have already cleared the gate, so a nightly
#: run scans the delta instead of re-reading 5 GB of JSON.
DRIVE_MIRROR_GATE_LEDGER_PATH = REPO_ROOT / "state" / "drive_mirror_gate_ledger.json"
#: One compact row per run. The manifest holds only the LATEST run, which is
#: enough to see a lane at zero and blind to a lane that merely collapsed --
#: 1320 product photos becoming 12 is the same selection bug with no symptom a
#: single-run view can show. The selection-sanity card reads this for a
#: baseline; without it the card can only ever grade against itself.
DRIVE_MIRROR_RUNS_PATH = REPO_ROOT / "state" / "drive_mirror_runs.jsonl"
#: Rows retained. Enough for a baseline that survives a bad week, small enough
#: that the card's read stays a cheap read.
DRIVE_MIRROR_RUNS_KEPT = 60
#: Drive for desktop's private state. Machine-local and not a contract Google
#: owes us, which is why every read of it is allowed to fail.
DRIVEFS_SUPPORT_ROOT = (
    Path.home() / "Library" / "Application Support" / "Google" / "DriveFS"
)

# Frozen at import: the FACTORY-DEFAULT published path, never moved by
# monkeypatch. The guard compares the LIVE value against this, so a test whose
# fixture redirects the root sails through while a test that bypasses isolation
# is refused.
_FROZEN_PRODUCTION_MIRROR_ROOT = (Path.home() / "DuckData").resolve()
#: The run history is a second production state file, so it needs the same
#: frozen-path guard as the mirror root. A test that reaches this would
#: append fixture rows to the baseline the selection card grades against,
#: which is how a shrink detector learns that nine files is normal.
_FROZEN_PRODUCTION_RUNS_PATH = (
    Path(__file__).resolve().parents[1] / "state" / "drive_mirror_runs.jsonl"
).resolve()
_TEST_MODE_REFUSAL_ENV = "DUCK_TEST_MODE"


class TestModeRefusalError(RuntimeError):
    """Raised when a mirror run under DUCK_TEST_MODE=1 would write to the real
    published directory. Everywhere else in the stack this guard protects a
    state file; here it protects ~10 GB of rsync into a folder that uploads to
    Google."""


# ---------------------------------------------------------------------------
# The allowlist
# ---------------------------------------------------------------------------

#: (published subdirectory, source directory). Copied whole.
WHOLE_TREES: tuple[tuple[str, Path], ...] = (
    ("duck-ops/output", DUCK_OPS_ROOT / "output"),
    ("duck-ops/state", DUCK_OPS_ROOT / "state"),
    ("duckAgent/data", DUCKAGENT_ROOT / "data"),
    ("duckAgent/config", DUCKAGENT_ROOT / "config"),
    ("duckAgent/state", DUCKAGENT_ROOT / "state"),
)

#: Creative output selected out of duckAgent/runs by filename. Pattern
#: selection is the weak joint of this design: a lane that renames its output
#: stops syncing and the only symptom is a count going to zero, which is why
#: every lane is counted separately and a lane collapsing to zero is fatal
#: rather than quiet. ([[feedback_no_silent_caps]])
CREATIVE_LANES: dict[str, str] = {
    "meme": "meme_*.png",
    "jeepfact": "jeepfact*.png",
    "video": "*.mp4",
    "weekly_hero": "weekly_hero*",
    "post_receipts": "*_posts.json",
    "product_photos": "newduck*.png",
}
CREATIVE_SOURCE = DUCKAGENT_ROOT / "runs"
CREATIVE_DEST = "duckAgent/runs"

#: Filenames that never leave, even when an include pattern matches them.
#: Exclusion is evaluated AFTER inclusion and wins: `meme_*.png` matches 37
#: `meme_*_subject.png` intermediates and `jeepfact*.png` matches 232 more, so
#: an include-only implementation would publish 269 files this list rejects.
EXCLUDE_NAME_PATTERNS: tuple[str, ...] = (
    ".env",
    ".env.*",
    "*.env",
    "etsy_tokens.json",
    # NOT `*token*.json`. That wildcard reads as obviously safe and silently
    # drops 402 legitimate files: duck-ops/state/workflow_control/
    # etsy-token-auth-*.json are workflow HEALTH records ("auth_healthy",
    # "etsy_auth_probe"), scanned clean, and exactly the operational history an
    # external model would want. Measured 2026-11-12.
    # ([[feedback_no_silent_caps]])
    "*oauth_token*.json",
    "*_credentials.json",
    "client_secret*",
    "*.pem",
    "*.key",
    "*_subject.png",
    "template_render.png",
    "background_scene.png",
    "*_lab_option*",
    "state_newduck.json",
    # Internal bookkeeping that happens to live in a published tree: one entry
    # per scanned file, meaningless to a reader of the mirror, and rewritten
    # every run so it would re-upload nightly for nothing.
    "drive_mirror_gate_ledger.json",
)
#: Directories never walked. `.git` and `.venv` carry credentials and churn;
#: the rest are caches.
EXCLUDE_DIR_NAMES: frozenset[str] = frozenset(
    {".git", ".venv", "__pycache__", "node_modules", ".pytest_cache", ".mypy_cache"}
)

#: Headroom required over the measured staging size before anything is copied.
#: The mirror holds a full second copy by design, so a run that would fill the
#: disk must refuse rather than discover it at 98%.
DISK_HEADROOM_FACTOR = 1.25


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------

#: Bumped whenever RETENTION_RULES changes what the mirror keeps. The
#: selection card builds its shrink baseline only from history rows carrying
#: the CURRENT version, so a deliberate thinning resets that baseline by
#: construction rather than by loosening a threshold. Thinning drops
#: `duck-ops/output` by 96%, which is precisely what that card exists to
#: catch -- it has to stay able to catch the NEXT one.
RETENTION_VERSION = 2

#: Names of the two filename conventions retention knows how to read. A rule
#: declares which one its directory uses; anything else is left alone.
SPLIT_TRAILING_DATE = "trailing_date"
SPLIT_LEADING_TIMESTAMP = "leading_timestamp"


def _split_trailing_date(name: str) -> tuple[str, str] | None:
    r"""`trend__wolf-duck__2026-11-01.json` -> (`trend__wolf-duck.json`, `2026-11-01`).

    Splits on the LAST `__`, which is the convention declared in
    config/output_naming.json (`trend__<theme>__<date>`,
    `customer_action_packets__<date>`, `nightly_action_summary__YYYY-MM-DD`,
    `phase_readiness__YYYY-WW`).

    The split is deliberately NOT "collapse digit runs to a placeholder",
    which is the obvious implementation and silently destroys data: seven
    trend concepts carry digits in their own names
    (`2026-graduation-duck`, `2026-custom-graduation-duck`,
    `10-000-lakes-duck`, `39-s-magnetic-duck`, `2-quot-duck`,
    `blazing-420-duck`, `sister-39-s-duck`), so a `\d+ -> N` rule merges the
    first two into one group and then deletes one of the two concepts.
    Splitting on the separator the producer actually writes keeps every digit
    that belongs to a name on the name side.
    ([[feedback_pin_the_rule_not_the_reading]])

    `urgent__<date>__<artifact_slug>` therefore groups per artifact rather
    than per report, so those files are effectively never pruned. That is
    correct-but-inert, visible in the rule's `groups` count, and left alone:
    there are four of them.
    """
    stem, dot, ext = name.rpartition(".")
    if not dot:
        return None
    head, sep, version = stem.rpartition("__")
    if not sep or not head or not version:
        return None
    return f"{head}.{ext}", version


def _split_leading_timestamp(name: str) -> tuple[str, str] | None:
    """`202604121531590852790400-state-sync.json` -> (`state-sync.json`, `2026...`)."""
    version, sep, rest = name.partition("-")
    if not sep or not version.isdigit() or not rest:
        return None
    return rest, version


_SPLITTERS = {
    SPLIT_TRAILING_DATE: _split_trailing_date,
    SPLIT_LEADING_TIMESTAMP: _split_leading_timestamp,
}

#: Per-directory retention, applied to the STAGED copy and nothing else. The
#: source trees under duck-ops/ are never touched -- thinning what Google
#: Drive uploads must not delete the operator's own data.
#:
#: Measured 2026-11-13 against the live mirror: Drive had uploaded 4,938 of
#: 178,741 files and then stalled with an empty operation queue and no errors.
#: 88% of that file count was two directories re-dumping unchanged reports.
#: The byte count was never the problem; 157,815 upload round trips to move
#: 840 MB was. Applying these four rules takes the mirror to 19,515 files /
#: 4.90 GB, of which duckAgent/runs -- the product photos and videos, the data
#: an external model is actually here to read -- is 95% of the bytes.
RETENTION_RULES: tuple[dict, ...] = (
    # 338 concepts x 209 daily snapshots. Two consecutive days differ in the
    # timestamp and one derived counter, nothing else; the files state their
    # own market observation is 161 days old. ~2,700 files/day while the
    # producer runs, so this is load-bearing and not a one-off cleanup.
    {
        "path": "duck-ops/output/trend_rankings",
        "keep": 1,
        "split": SPLIT_TRAILING_DATE,
    },
    # Every file is a COMPLETE re-dump of the same two reports (432 and 475
    # items). 4.48 GB of history for 4.3 MB of current state -- and this is
    # the customer PII, so holding less of it in the cloud is a second win.
    {
        "path": "duck-ops/output/customer_intelligence",
        "keep": 1,
        "split": SPLIT_TRAILING_DATE,
    },
    # 47,624 files whose entire payload is one integer (`review_pending`).
    # ~400 bytes of content each, one 4 KB disk block each. This one is not
    # really a retention problem -- it should have been a single JSONL -- but
    # capping it is the cheap half of that fix.
    {
        "path": "duck-ops/state/workflow_receipts/ops",
        "keep": 30,
        "split": SPLIT_LEADING_TIMESTAMP,
    },
    # Ten report shapes re-dumped daily, `nightly_action_summary` at 2.25 MB
    # a copy. Note `phase_readiness__2026-15` is YYYY-WW rather than a date,
    # which is another reason the group key comes from the separator split and
    # not from a date regex that would fail to match it.
    {
        "path": "duck-ops/output/digests",
        "keep": 30,
        "split": SPLIT_TRAILING_DATE,
    },
)


def prune_staged_retention(staging_root: Path) -> list[dict]:
    """Apply RETENTION_RULES to the staged tree: newest `keep` per group.

    Five properties this function has to keep, each one a way to lose data:

    1. It prunes STAGING ONLY. A source path here would delete duck-ops data.
    2. A name the splitter cannot read is KEPT and counted in `unparsed`.
       Deleting on a parse failure turns a producer renaming its output into
       silent data loss; keeping turns it into a number that can be alarmed on.
    3. A configured directory that is ABSENT is recorded as `path_missing`
       rather than skipped quietly, because retention silently not applying is
       how the mirror reached 178,741 files in the first place.
    4. Ordering comes from the version token in the NAME, not from mtime. An
       rsync into a fresh staging root can restamp mtimes, which would quietly
       redefine "newest" as "most recently copied". Verified 2026-11-13: the
       two orders agree in every one of the 697 live groups, so this choice
       costs nothing today and holds when staging changes.
    5. `keep` is >= 1 per group, so a directory that had files cannot come out
       empty. If it does anyway, that is `emptied` and the run must see it.
    """
    results: list[dict] = []
    for rule in RETENTION_RULES:
        entry = {
            "path": rule["path"],
            "keep": rule["keep"],
            "files_before": 0,
            "files_after": 0,
            "bytes_before": 0,
            "bytes_after": 0,
            "deleted": 0,
            "unparsed": 0,
            "groups": 0,
            "status": "ok",
        }
        base = staging_root / rule["path"]
        if not base.is_dir():
            entry["status"] = "path_missing"
            results.append(entry)
            continue
        splitter = _SPLITTERS[rule["split"]]
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIR_NAMES]
            here = Path(dirpath)
            groups: dict[str, list[tuple[str, Path, int]]] = {}
            for name in filenames:
                path = here / name
                try:
                    size = path.stat().st_size
                except OSError:
                    continue
                entry["files_before"] += 1
                entry["bytes_before"] += size
                split = splitter(name)
                if split is None:
                    entry["unparsed"] += 1
                    entry["files_after"] += 1
                    entry["bytes_after"] += size
                    continue
                groups.setdefault(split[0], []).append((split[1], path, size))
            entry["groups"] += len(groups)
            for items in groups.values():
                items.sort(key=lambda it: it[0], reverse=True)
                for _, _path, size in items[: rule["keep"]]:
                    entry["files_after"] += 1
                    entry["bytes_after"] += size
                for _, path, size in items[rule["keep"] :]:
                    try:
                        path.unlink()
                        entry["deleted"] += 1
                    except OSError as exc:
                        entry["status"] = "delete_failed"
                        entry["error"] = str(exc)[:200]
                        entry["files_after"] += 1
                        entry["bytes_after"] += size
        if entry["files_before"] and not entry["files_after"]:
            entry["status"] = "emptied"
        results.append(entry)
    return results

# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

# Patterns and redaction come from scripts/secret_scan.py, which stays the
# authority for WHAT a credential looks like. What is NOT reused is its
# traversal: scan_repo walks `git ls-files` and main() returns 0 on a
# non-git directory, so pointing the CLI at the mirror would report clean on a
# folder it never opened.
def _load_scanner():
    scripts_dir = REPO_ROOT / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    import secret_scan

    return secret_scan


_SCANNER = _load_scanner()
PATTERNS = _SCANNER.PATTERNS
_is_scannable = _SCANNER._is_scannable
_redact = _SCANNER._redact

# One alternation over every credential shape, so the common case (no secret)
# costs a single pass per file instead of nineteen. A hit re-reads that one
# file line by line to name the pattern and locate it.
#
# A leading `(?i)` is a GLOBAL flag and is a syntax error anywhere but the
# start of a pattern, so the Bearer/Authorization rule has to be rewritten as
# the scoped `(?i:...)` to survive being nested in the alternation.
def _as_union_branch(pattern: re.Pattern) -> str:
    text = pattern.pattern
    if text.startswith("(?i)"):
        return f"(?i:{text[4:]})"
    return f"(?:{text})"


_UNION_PATTERN = re.compile(
    "|".join(_as_union_branch(pattern) for pattern, _ in PATTERNS)
)


@dataclass
class GateResult:
    """Why the gate did or did not release. `released` is never inferred from
    an empty findings list -- a scan that read nothing also finds nothing."""

    released: bool
    reason: str
    files_eligible: int = 0
    files_scanned: int = 0
    files_carried: int = 0
    files_skipped_binary: int = 0
    findings: list[dict] = field(default_factory=list)


def _gate_key(path: Path, rel: str) -> str:
    st = path.stat()
    return f"{rel}|{st.st_size}|{st.st_mtime_ns}"


def _scan_one(path: Path, rel: str) -> list[dict]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        # An unreadable file is NOT a clean file. Surface it as a finding so a
        # permissions problem cannot look like a pass.
        return [{"path": rel, "line": 0, "pattern": "unreadable", "redacted_excerpt": str(exc)[:120]}]
    if not _UNION_PATTERN.search(text):
        return []
    findings = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for pattern, name in PATTERNS:
            match = pattern.search(line)
            if match:
                findings.append({
                    "path": rel,
                    "line": lineno,
                    "pattern": name,
                    "redacted_excerpt": _redact(match.group(0)),
                })
    return findings


def gate_staged_tree(
    staging_root: Path,
    ledger: dict | None = None,
) -> tuple[GateResult, dict]:
    """Scan the staged tree and decide whether it may be released.

    Release requires all three of: at least one eligible file exists, every
    eligible file is accounted for (read now, or proven identical to one that
    passed before), and no findings. The middle condition is the one that makes
    a broken scanner loud -- `findings == []` on its own is exactly what a
    scanner that never opened a file reports.
    """
    ledger = dict(ledger or {})
    fresh_ledger: dict[str, str] = {}
    eligible = scanned = carried = skipped_binary = 0
    findings: list[dict] = []

    if not staging_root.is_dir():
        return GateResult(False, f"staging tree missing: {staging_root}"), fresh_ledger

    for dirpath, dirnames, filenames in os.walk(staging_root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIR_NAMES]
        for name in filenames:
            path = Path(dirpath) / name
            rel = str(path.relative_to(staging_root))
            if not _is_scannable(path):
                skipped_binary += 1
                continue
            eligible += 1
            try:
                key = _gate_key(path, rel)
            except OSError:
                findings.append({"path": rel, "line": 0, "pattern": "unstattable",
                                 "redacted_excerpt": ""})
                continue
            fresh_ledger[rel] = key
            if ledger.get(rel) == key:
                carried += 1
                continue
            scanned += 1
            findings.extend(_scan_one(path, rel))

    accounted = scanned + carried
    if eligible == 0:
        return GateResult(
            False,
            "refusing to release: zero scannable files in the staged tree, so "
            "the gate has no evidence it scanned anything",
            eligible, scanned, carried, skipped_binary, findings,
        ), fresh_ledger
    if accounted != eligible:
        return GateResult(
            False,
            f"refusing to release: {eligible} eligible file(s) but only "
            f"{accounted} accounted for ({scanned} scanned, {carried} carried)",
            eligible, scanned, carried, skipped_binary, findings,
        ), fresh_ledger
    if findings:
        return GateResult(
            False,
            f"refusing to release: {len(findings)} credential finding(s) in the "
            f"staged tree",
            eligible, scanned, carried, skipped_binary, findings,
        ), fresh_ledger
    return GateResult(
        True,
        f"clean: {eligible} eligible file(s) accounted for "
        f"({scanned} scanned, {carried} unchanged since the last pass)",
        eligible, scanned, carried, skipped_binary, findings,
    ), fresh_ledger


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def is_excluded(name: str) -> bool:
    """True if this filename never leaves, whatever matched it."""
    return any(fnmatch.fnmatch(name, pat) for pat in EXCLUDE_NAME_PATTERNS)


def select_creative_files(source: Path | None = None) -> dict[str, list[Path]]:
    """Per-lane selection out of the runs tree, exclusions already applied.

    A file matching two lanes is counted under both -- the counts answer "is
    this lane still producing?", which a de-duplicated total cannot.
    """
    source = Path(source) if source is not None else CREATIVE_SOURCE
    selected: dict[str, list[Path]] = {lane: [] for lane in CREATIVE_LANES}
    if not source.is_dir():
        return selected
    for dirpath, dirnames, filenames in os.walk(source):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIR_NAMES]
        for name in filenames:
            if is_excluded(name):
                continue
            for lane, pattern in CREATIVE_LANES.items():
                if fnmatch.fnmatch(name, pattern):
                    selected[lane].append(Path(dirpath) / name)
    return selected


def _tree_size_bytes(paths: list[Path]) -> int:
    total = 0
    for p in paths:
        try:
            total += p.stat().st_size
        except OSError:
            continue
    return total


def _dir_size_bytes(root: Path) -> tuple[int, int]:
    total = count = 0
    if not root.is_dir():
        return 0, 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIR_NAMES]
        for name in filenames:
            try:
                total += (Path(dirpath) / name).stat().st_size
            except OSError:
                continue
            count += 1
    return total, count


# ---------------------------------------------------------------------------
# Copying
# ---------------------------------------------------------------------------

def _rsync(args: list[str]) -> tuple[int, str]:
    proc = subprocess.run(
        ["rsync", *args], capture_output=True, text=True, check=False
    )
    return proc.returncode, (proc.stderr or proc.stdout or "").strip()[:400]


def _exclusion_args() -> list[str]:
    args = []
    for pat in EXCLUDE_NAME_PATTERNS:
        args += ["--exclude", pat]
    for name in sorted(EXCLUDE_DIR_NAMES):
        args += ["--exclude", f"{name}/"]
    return args


def stage_whole_trees(staging_root: Path) -> list[dict]:
    """rsync each allowlisted tree into staging. A missing source tree is
    recorded and skipped, never fatal -- one absent directory must not stop the
    other four from syncing, and must not look like a clean empty mirror
    either."""
    results = []
    for subdir, source in WHOLE_TREES:
        dest = staging_root / subdir
        entry = {"tree": subdir, "source": str(source)}
        if not source.is_dir():
            entry.update(status="source_missing", files=0, bytes=0)
            results.append(entry)
            continue
        dest.mkdir(parents=True, exist_ok=True)
        code, err = _rsync([
            "-a", "--delete", *_exclusion_args(),
            f"{source}/", f"{dest}/",
        ])
        nbytes, nfiles = _dir_size_bytes(dest)
        entry.update(
            status="ok" if code == 0 else "rsync_failed",
            files=nfiles,
            bytes=nbytes,
            rsync_exit=code,
        )
        if code != 0:
            entry["error"] = err
        results.append(entry)
    return results


def stage_creative_slice(
    staging_root: Path,
    source: Path | None = None,
) -> tuple[dict[str, int], dict]:
    """rsync the pattern-selected creative files, preserving run-relative paths
    so provenance (which run produced which image) survives the mirror."""
    source = Path(source) if source is not None else CREATIVE_SOURCE
    selected = select_creative_files(source)
    counts = {lane: len(paths) for lane, paths in selected.items()}
    dest = staging_root / CREATIVE_DEST
    info: dict = {"source": str(source), "dest": CREATIVE_DEST, "lane_counts": counts}

    rel_paths = sorted({
        str(p.relative_to(source)) for paths in selected.values() for p in paths
    })
    info["files"] = len(rel_paths)
    info["bytes"] = _tree_size_bytes(
        [source / r for r in rel_paths]
    )
    if not rel_paths:
        info["status"] = "nothing_selected"
        return counts, info

    dest.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".files-from", delete=False) as fh:
        fh.write("\n".join(rel_paths) + "\n")
        list_path = fh.name
    try:
        code, err = _rsync([
            "-a", f"--files-from={list_path}", f"{source}/", f"{dest}/",
        ])
    finally:
        os.unlink(list_path)
    info["status"] = "ok" if code == 0 else "rsync_failed"
    info["rsync_exit"] = code
    if code != 0:
        info["error"] = err
        return counts, info

    # Prune staged creative files no longer selected. This runs only after the
    # zero-lane check in run(), because a lane that renamed its output selects
    # nothing and this loop would otherwise delete the lane's whole history.
    expected = set(rel_paths)
    pruned = 0
    for dirpath, dirnames, filenames in os.walk(dest):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIR_NAMES]
        for name in filenames:
            p = Path(dirpath) / name
            if str(p.relative_to(dest)) not in expected:
                try:
                    p.unlink()
                    pruned += 1
                except OSError:
                    continue
    info["pruned"] = pruned
    return counts, info


#: Travels with the data by design. An external model reading this corpus will
#: over-read it -- the social sample is 27 posts, and a format ranking drawn
#: from it was retracted on 2026-11-08 -- so the caveats have to be IN the
#: mirror, not in a document the reader never sees.
MIRROR_README = """# myJeepDuck data mirror

Generated automatically by duck-ops/runtime/drive_mirror.py. Do not edit:
every file here is overwritten from the source repositories on each run, and
anything added by hand is deleted on the next one. To hand something BACK to
this system, write it to the separate `~/DuckDrop/` sync folder, never here.

## What this is

A selected slice of the myJeepDuck automation system's business data, mirrored
so an external model can read sales, learnings, social performance and
creative history.

**Counts live in `MANIFEST.json`, not in this file.** Per-tree file counts,
per-lane counts and the retention caps actually applied are written there on
every run. This README carries only things that stay true between runs --
earlier versions quoted figures inline and two of them were off by three
orders of magnitude within a day of a retention change.

## Where things are

| Path | What it holds |
|---|---|
| `duckAgent/runs/` | **The creative archive, and most of the bytes.** Per-run folders plus `studio_assets/`. Product photographs of printed ducks, meme images, jeepfact images, generated videos, weekly hero images, and `*_posts.json` publish receipts. |
| `duck-ops/output/operator/` | Operator-facing reports: the review queue, per-review detail, the weekly strategy packet, sale monitoring, social insights. Mostly one file per topic or per customer. |
| `duck-ops/output/digests/` | Dated digest reports: nightly action summaries, observation summaries, trend digests, review queues, weekly phase readiness. |
| `duck-ops/output/trend_rankings/` | Per-concept trend scoring, one `.json` + `.md` per duck concept. |
| `duck-ops/output/customer_intelligence/` | The customer interaction queue and action packets. **Real customer PII** -- see Privacy. |
| `duck-ops/output/quality_gates/` | Per-publish gate decisions: what was allowed out and why. |
| `duck-ops/output/execution/` + `discovery/` | Etsy review-reply attempts and the discovery passes behind them. |
| `duck-ops/state/` | The machine's working state: ~120 JSON files at the top level (build-next queue, business desk, calibration, SEO review and writeback, social performance, competitor snapshots) plus `workflow_control/` and `workflow_receipts/`. |
| `duckAgent/config/` | Shipped configuration: taxonomies, style libraries, scoring rules, category policy. The system's declared rules rather than its observations. |
| `duckAgent/data/` | Style memory, the creative feedback ledger, quality receipts. |
| `duckAgent/state/` | Small liveness and drift files. |

Start with `duck-ops/state/social_performance_posts.json` and
`social_performance_rollups.json` for post outcomes,
`duck-ops/output/operator/` for what the operator is being asked to decide,
and `duckAgent/runs/` for the images and video.

## Retention -- read this before treating anything as a time series

Most of this tree is a current snapshot, not a history. As of 2026-11-13 the
mirror publishes only the newest files per report, because publishing every
historical re-dump meant 178,741 files for 9.89 GB and Google Drive could not
finish uploading it. `MANIFEST.json` lists the exact caps under `retention`.

What that means for a reader:

- **`trend_rankings/` holds ONE snapshot per concept, not a series.** Do not
  compute a trend from it. The dated filename is when that snapshot was taken,
  and consecutive historical snapshots differed only in their timestamp and a
  staleness counter anyway -- the underlying market observation was 161 days
  old when this cap was introduced.
- **`customer_intelligence/` holds the newest complete pair of reports.** Each
  file is a FULL re-dump of the whole queue, so one file is the whole picture,
  and the item counts inside it are the real sample size.
- **`digests/` holds roughly the last 30 of each report shape**, so it supports
  a month of comparison and no more.
- **`workflow_receipts/ops/` holds the last 30 records only.** The rest of
  `workflow_receipts/` is per-run and complete.
- Everything not named above is published in full.

## Reading this honestly

- **Sample sizes are small.** The social performance data covers tens of posts,
  not thousands. A ranking drawn from it is a hypothesis, not a finding. One
  such ranking was retracted on 2026-11-08 after it failed to reproduce.
- **`confidence` and `evidence` fields are load-bearing.** Where a rollup
  carries them, they qualify the number next to them. Do not strip or ignore
  them for tidiness.
- **A count and a statistic here may have different denominators.** Check what
  each one is computed over before putting them in the same sentence.
- **A timestamped filename is when the file was written, not when the event
  happened.** Several producers re-dump unchanged content on a schedule.

## Privacy

This mirror contains real customer data by explicit decision of the shop
operator: names, Etsy handles, message previews, draft replies and order
details, in `output/customer_intelligence/` and in `output/operator/` (where
most files are per-customer). Treat it as personal data. Do not redistribute
it, and do not use it to contact anyone.

## Not in here

Credentials of any kind. Every staged file is scanned before publication and
the run refuses to publish if anything matching a credential is found, or if
the scanner cannot prove it read the files it was asked to read.
"""


def write_mirror_readme(staging_root: Path, manifest: dict) -> None:
    """Drop the README and the selection manifest into staging, where the gate
    will scan them before anything is published."""
    staging_root.mkdir(parents=True, exist_ok=True)
    # Write the README only when it actually differs. An unconditional write
    # bumps its mtime every run, which re-scans it at the gate and makes Drive
    # re-upload an identical file nightly.
    readme = staging_root / "README.md"
    if not readme.is_file() or readme.read_text() != MIRROR_README:
        readme.write_text(MIRROR_README)
    (staging_root / "MANIFEST.json").write_text(json.dumps({
        "generated_at": manifest.get("generated_at"),
        "trees": [
            {k: v for k, v in tree.items() if k in {"tree", "status", "files", "bytes"}}
            for tree in manifest.get("trees", [])
        ],
        "creative_lane_counts": manifest.get("lane_counts", {}),
        "creative_files": manifest.get("creative", {}).get("files"),
        # The caps a reader needs in order to know what is a snapshot and what
        # is a series. The README explains what they MEAN; these are the
        # numbers, here rather than inline so they cannot go stale.
        "retention_version": manifest.get("retention_version"),
        "retention": [
            {
                "path": r.get("path"),
                "keep_newest_per_report": r.get("keep"),
                "files_published": r.get("files_after"),
                "files_withheld": r.get("deleted"),
                "status": r.get("status"),
            }
            for r in (manifest.get("retention") or [])
            if isinstance(r, dict)
        ],
        "note": "Counts describe what this run selected. See README.md before "
                "drawing conclusions from any of it, especially the Retention "
                "section: most of this tree is a snapshot, not a time series.",
    }, indent=2, sort_keys=True))


def release(staging_root: Path, mirror_root: Path) -> tuple[bool, str]:
    """Publish the gated staging tree with a delta rsync.

    Deliberately not a directory rename: a rename shows Drive the entire
    mirror as deleted-and-recreated and re-uploads ~10 GB every run. Each file
    rsync writes lands via a rename inside the destination, so Drive never
    observes a partially written file, and nothing enters here that the gate
    has not already cleared.
    """
    if _refusing_test_mode_prod_write(mirror_root):
        raise TestModeRefusalError(
            f"drive_mirror refused to publish: {_TEST_MODE_REFUSAL_ENV}=1 and the "
            f"destination is the frozen production mirror "
            f"({_FROZEN_PRODUCTION_MIRROR_ROOT}). A test that reaches this would "
            f"rsync the staged tree into a folder that uploads to Google Drive."
        )
    mirror_root.mkdir(parents=True, exist_ok=True)
    code, err = _rsync([
        "-a", "--delete", f"{staging_root}/", f"{mirror_root}/",
    ])
    if code != 0:
        return False, f"release rsync failed (exit {code}): {err}"
    return True, "released"


def _refusing_test_mode_runs_write(runs_path: Path | None) -> bool:
    flag = str(os.environ.get(_TEST_MODE_REFUSAL_ENV) or "").strip()
    if flag not in {"1", "true", "TRUE", "yes"}:
        return False
    try:
        resolved = Path(runs_path or DRIVE_MIRROR_RUNS_PATH).resolve()
    except Exception:  # pragma: no cover - defensive
        return False
    return resolved == _FROZEN_PRODUCTION_RUNS_PATH


def _refusing_test_mode_prod_write(mirror_root: Path | None) -> bool:
    flag = str(os.environ.get(_TEST_MODE_REFUSAL_ENV) or "").strip()
    if flag not in {"1", "true", "TRUE", "yes"}:
        return False
    resolved = Path(mirror_root or DRIVE_MIRROR_ROOT).resolve()
    return resolved == _FROZEN_PRODUCTION_MIRROR_ROOT


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    os.replace(tmp, path)


def _free_bytes(path: Path) -> int:
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    return shutil.disk_usage(probe).free


def _probe_drive_upload_state(mirror_root: Path) -> dict:
    """How much of the released mirror has Drive actually uploaded?

    The producer's own success condition stops at "the bytes are in the mirror
    root". Whether they LEFT the machine is Drive's business, and on
    2026-11-13 that difference was the whole story: both of Drive's upload
    queues read zero while 96% of the run was still undiscovered, because
    Drive enqueues in batches and an empty queue is also exactly what a
    mid-scan lull looks like. So the signal here is the count of files
    carrying a cloud checksum, never the queue depth -- a depth of zero cannot
    distinguish finished from barely started.

    Reads Drive's private sqlite through a copy, read-only. Every failure is
    reported as `readable: false` and this NEVER raises: evidence capture must
    not be able to fail the action it documents
    ([[feedback_receipts_never_load_bearing]]).
    """
    out: dict = {
        "readable": False,
        "error": None,
        "uploaded": None,
        "local_files": None,
        "queued": None,
        "pending": None,
    }
    try:
        local = 0
        for dirpath, dirnames, filenames in os.walk(mirror_root):
            # Drive's own in-flight scratch dir is not published content.
            dirnames[:] = [d for d in dirnames if d != ".tmp.driveupload"]
            local += len(filenames)
        out["local_files"] = local
    except Exception as exc:  # pragma: no cover - defensive
        out["error"] = f"could not count mirror files: {exc}"
        return out

    try:
        if not DRIVEFS_SUPPORT_ROOT.is_dir():
            out["error"] = "Drive for desktop is not installed on this machine"
            return out
        db = None
        for account in sorted(DRIVEFS_SUPPORT_ROOT.iterdir()):
            candidate = account / "mirror_sqlite.db"
            if account.is_dir() and candidate.exists():
                db = candidate
                break
        if db is None:
            out["error"] = "no Drive mirror database found (is a folder synced?)"
            return out

        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp) / "mirror_sqlite.db"
            shutil.copy2(db, copy)
            # Without the write-ahead log the copy reports a stale snapshot.
            for suffix in ("-wal", "-shm"):
                side = db.with_name(db.name + suffix)
                if side.exists():
                    shutil.copy2(side, copy.with_name(copy.name + suffix))
            con = sqlite3.connect(f"file:{copy}?mode=ro", uri=True)
            try:
                cur = con.cursor()
                out["uploaded"] = cur.execute(
                    "select count(*) from mirror_item where "
                    "cloud_md5_checksum is not null and cloud_md5_checksum != ''"
                ).fetchone()[0]
                out["queued"] = cur.execute(
                    "select count(*) from queued_uploads"
                ).fetchone()[0]
                out["pending"] = cur.execute(
                    "select count(*) from pending_uploads"
                ).fetchone()[0]
            finally:
                con.close()
        out["readable"] = True
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _append_run_history(
    path: Path,
    manifest: dict,
    *,
    kept: int = DRIVE_MIRROR_RUNS_KEPT,
) -> None:
    """Append one compact row per run and trim to the retention cap.

    A REFUSED run is recorded too. The throughput card's question is "is work
    completing", and a run that refused every night for a week is the exact
    shape that question exists to catch.

    Never raises EXCEPT on the test-mode guard below, which is a programming
    error rather than an I/O failure and must be loud.
    """
    if _refusing_test_mode_runs_write(path):
        raise TestModeRefusalError(
            f"drive_mirror refused to append run history: "
            f"{_TEST_MODE_REFUSAL_ENV}=1 and the destination is the frozen "
            f"production history ({_FROZEN_PRODUCTION_RUNS_PATH}). Fixture rows "
            f"here poison the baseline the selection card grades against."
        )
    row = {
        "generated_at": manifest.get("generated_at"),
        "status": manifest.get("status"),
        "ok": bool(manifest.get("ok")),
        "staged_files": manifest.get("staged_files"),
        "staged_bytes": manifest.get("staged_bytes"),
        "lane_counts": dict(manifest.get("lane_counts") or {}),
        "tree_files": {
            str(tree.get("tree")): tree.get("files")
            for tree in (manifest.get("trees") or [])
            if isinstance(tree, dict)
        },
        "failure_count": len(manifest.get("failures") or []),
        "upload": dict(manifest.get("upload") or {}),
        # The baseline the selection card grades shrink against is computed
        # only over rows matching the CURRENT retention version, so a
        # deliberate thinning resets it instead of reading as an outage.
        "retention_version": manifest.get("retention_version"),
        "retention": [
            {
                "path": r.get("path"),
                "status": r.get("status"),
                "deleted": r.get("deleted"),
                "unparsed": r.get("unparsed"),
                "files_after": r.get("files_after"),
            }
            for r in (manifest.get("retention") or [])
            if isinstance(r, dict)
        ],
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []
        if path.exists():
            lines = [
                line
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        lines.append(json.dumps(row, sort_keys=True))
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text("\n".join(lines[-kept:]) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    except Exception:  # pragma: no cover - defensive
        return


def run(
    mirror_root: Path | None = None,
    staging_root: Path | None = None,
    state_path: Path | None = None,
    ledger_path: Path | None = None,
    runs_path: Path | None = None,
    creative_source: Path | None = None,
    allow_zero_lanes: bool = False,
) -> dict:
    """Stage, gate, release, and write the manifest. Returns the manifest; the
    caller turns `ok` into an exit code."""
    started = time.time()
    mirror_root = Path(mirror_root) if mirror_root is not None else DRIVE_MIRROR_ROOT
    staging_root = (
        Path(staging_root) if staging_root is not None else DRIVE_MIRROR_STAGING_ROOT
    )
    state_path = Path(state_path) if state_path is not None else DRIVE_MIRROR_STATE_PATH
    ledger_path = (
        Path(ledger_path) if ledger_path is not None else DRIVE_MIRROR_GATE_LEDGER_PATH
    )
    runs_path = Path(runs_path) if runs_path is not None else DRIVE_MIRROR_RUNS_PATH

    manifest: dict = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "mirror_root": str(mirror_root),
        "staging_root": str(staging_root),
        "ok": False,
        "status": "unknown",
        "failures": [],
    }

    # Refuse before any copying, not after staging 10 GB.
    if _refusing_test_mode_prod_write(mirror_root):
        raise TestModeRefusalError(
            f"drive_mirror refused to run: {_TEST_MODE_REFUSAL_ENV}=1 and the "
            f"destination is the frozen production mirror "
            f"({_FROZEN_PRODUCTION_MIRROR_ROOT})."
        )

    staging_root.mkdir(parents=True, exist_ok=True)

    manifest["trees"] = stage_whole_trees(staging_root)
    lane_counts, creative = stage_creative_slice(staging_root, creative_source)
    manifest["creative"] = creative
    manifest["lane_counts"] = lane_counts

    for tree in manifest["trees"]:
        if tree["status"] == "rsync_failed":
            manifest["failures"].append(f"tree {tree['tree']}: rsync failed")
        elif tree["status"] == "source_missing":
            manifest["failures"].append(f"tree {tree['tree']}: source directory missing")
    if creative.get("status") == "rsync_failed":
        manifest["failures"].append("creative slice: rsync failed")

    # A lane at zero is the signature of a renamed output, and it is fatal
    # rather than quiet: the only other symptom is a count silently dropping.
    empty_lanes = sorted(lane for lane, n in lane_counts.items() if n == 0)
    manifest["empty_lanes"] = empty_lanes
    if empty_lanes and not allow_zero_lanes:
        manifest["failures"].append(
            "lane(s) selected zero files (renamed output?): " + ", ".join(empty_lanes)
        )

    # Retention runs BEFORE the gate and before the counts are taken, so what
    # the gate scans and what the manifest reports are both the tree that is
    # actually published. Pruning after the gate would publish a count for
    # files that were then deleted.
    manifest["retention_version"] = RETENTION_VERSION
    manifest["retention"] = prune_staged_retention(staging_root)
    for pruned in manifest["retention"]:
        # `path_missing` is deliberately NOT a failure. Nothing wrong is
        # published when a cap does not apply -- the mirror is merely fatter
        # than intended -- and refusing the whole publish over it would hold
        # 4.9 GB of good data hostage to a renamed directory. It still has to
        # be seen, because the only other symptom is the mirror growing back
        # by ~2,700 files a day, so the selection card grades it instead.
        # (`delete_failed` and `emptied` DO mean the published tree is wrong,
        # and stay fatal.)
        if pruned["status"] in ("delete_failed", "emptied"):
            manifest["failures"].append(
                f"retention {pruned['path']}: {pruned['status']}"
                + (f" ({pruned['error']})" if pruned.get("error") else "")
            )

    # stage_whole_trees counted the tree as rsynced, which is before pruning.
    # Re-measure so `tree_files` in the manifest and in the run history is
    # what was published, not what was copied -- the selection card grades
    # that number and would otherwise never see the thinning at all.
    for tree in manifest["trees"]:
        dest = staging_root / str(tree.get("tree"))
        if dest.is_dir():
            tbytes, tfiles = _dir_size_bytes(dest)
            tree["bytes"], tree["files"] = tbytes, tfiles

    # Before the gate, so the README and manifest are scanned and published
    # like every other file instead of appearing in the mirror ungated.
    write_mirror_readme(staging_root, manifest)

    staged_bytes, staged_files = _dir_size_bytes(staging_root)
    manifest["staged_bytes"] = staged_bytes
    manifest["staged_files"] = staged_files
    free = _free_bytes(mirror_root)
    manifest["free_bytes_at_destination"] = free
    needed = int(staged_bytes * DISK_HEADROOM_FACTOR)
    if free < needed:
        manifest["failures"].append(
            f"insufficient free space: need ~{needed} bytes "
            f"({DISK_HEADROOM_FACTOR}x staged), have {free}"
        )

    ledger = _load_json(ledger_path)
    gate, fresh_ledger = gate_staged_tree(staging_root, ledger.get("entries"))
    manifest["gate"] = {
        "released": gate.released,
        "reason": gate.reason,
        "files_eligible": gate.files_eligible,
        "files_scanned": gate.files_scanned,
        "files_carried": gate.files_carried,
        "files_skipped_binary": gate.files_skipped_binary,
        "finding_count": len(gate.findings),
        "findings": gate.findings[:25],
    }
    if not gate.released:
        manifest["failures"].append(f"gate: {gate.reason}")

    if manifest["failures"]:
        manifest["status"] = "refused"
        manifest["duration_seconds"] = round(time.time() - started, 2)
        manifest["upload"] = _probe_drive_upload_state(mirror_root)
        _append_run_history(runs_path, manifest)
        _write_json_atomic(state_path, manifest)
        return manifest

    ok, reason = release(staging_root, mirror_root)
    manifest["release"] = {"ok": ok, "reason": reason}
    if not ok:
        manifest["failures"].append(reason)
        manifest["status"] = "refused"
    else:
        manifest["ok"] = True
        manifest["status"] = "released"
        # The ledger records only what was published. Persisting it on a
        # refused run would let the next run "carry" a file that never cleared.
        _write_json_atomic(
            ledger_path,
            {"generated_at": manifest["generated_at"], "entries": fresh_ledger},
        )

    manifest["duration_seconds"] = round(time.time() - started, 2)
    manifest["upload"] = _probe_drive_upload_state(mirror_root)
    _append_run_history(runs_path, manifest)
    _write_json_atomic(state_path, manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mirror-root", default=None)
    ap.add_argument("--staging-root", default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="select and report counts without copying anything")
    ap.add_argument("--allow-zero-lanes", action="store_true",
                    help="do not fail when a creative lane selects zero files")
    args = ap.parse_args(argv)

    if args.dry_run:
        selected = select_creative_files()
        total = 0
        for lane, paths in sorted(selected.items()):
            size = _tree_size_bytes(paths)
            total += size
            print(f"{lane:16} {len(paths):6} files  {size / 2**30:7.2f} GB")
        print(f"{'CREATIVE SLICE':16} "
              f"{sum(len(p) for p in selected.values()):6} files  "
              f"{total / 2**30:7.2f} GB")
        for subdir, source in WHOLE_TREES:
            nbytes, nfiles = _dir_size_bytes(source)
            state = "ok" if source.is_dir() else "MISSING"
            print(f"{subdir:22} {nfiles:7} files  {nbytes / 2**30:7.2f} GB  {state}")
        return 0

    manifest = run(
        mirror_root=args.mirror_root,
        staging_root=args.staging_root,
        allow_zero_lanes=args.allow_zero_lanes,
    )
    print(f"status={manifest['status']} "
          f"staged_files={manifest.get('staged_files')} "
          f"gate={manifest.get('gate', {}).get('reason')}")
    for failure in manifest["failures"]:
        print(f"  FAILURE: {failure}", file=sys.stderr)
    return 0 if manifest["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
