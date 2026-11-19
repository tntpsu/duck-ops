"""Per-resource memory of what SEO copy was written, and under which rules.

WHY THIS EXISTS
---------------
The Shopify SEO review lane had no per-resource memory at all. Selection
(`_select_issue_category_candidates`) filtered purely on today's audit issue
codes, and a round-robin cursor walked nine issue categories one batch per day.
After about nine days it wrapped and proposed the same categories again. 176
writeback receipts recorded exactly what had been written and verified, and
nothing in the selection path ever read one.

So the lane could not answer the only question that decides whether a resource
needs work: "has this resource already been rewritten under the current rules,
and is that copy still live?"

THE THREE TRIGGERS
------------------
This ledger replaces the daily sweep with three triggers, in the order they
fire:

  1. NEW or DRIFTED -- no row for the resource, or the live value no longer
     hashes to what we wrote. Hash drift is what keeps the ledger honest: edit
     a title in the Shopify admin and the row stops matching, so the resource
     re-enters the queue instead of the ledger quietly lying about it. This is
     the normal path and is usually 0-5 items.
  2. STALE RULES -- the row's `rules_version` predates the config's. This is
     the deliberate re-sweep after a new insight or a new platform, bounded and
     explicit, and it is the ONLY trigger that can produce a large batch.
  3. NEW ISSUE -- the auditor found a code on a resource that is otherwise
     current. A genuine finding rather than churn.

A resource that is current, unmodified and clean matches no trigger and costs
nothing. That is what makes a spend cap unnecessary rather than merely
deferred: in steady state the lane generates nothing.

WHAT A ROW IS NOT
-----------------
It is not proof the copy is good -- only that it was written deliberately under
a known ruleset and is still live. Quality is the auditor's question, and a
resource can be current in this ledger and still carry an issue code (trigger
3).
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from seo_rules_config import seo_rules_version  # noqa: E402

SEO_COPY_LEDGER_PATH = DUCK_OPS_ROOT / "state" / "seo_copy_ledger.json"
SEO_WRITEBACK_RECEIPTS_DIR = DUCK_OPS_ROOT / "state" / "shopify_seo_writeback" / "receipts"

# Frozen at import: the FACTORY-DEFAULT production path, never monkeypatched.
# The guard compares the LIVE module constant against this, so a conftest that
# swaps the live value for a tmp path sails through while a test that bypasses
# isolation is refused ([[feedback_test_isolation_from_production_state]]).
_FROZEN_PRODUCTION_LEDGER_PATH = SEO_COPY_LEDGER_PATH.resolve()
_TEST_MODE_ENV = "DUCK_TEST_MODE"

LEDGER_FIELDS = ("seo_title", "seo_description")

# A trigger names WHY a resource is in front of the operator. The auditor
# decides whether there is anything to fix at all; the ledger decides whether
# we have already tried. Making "no ledger row" a trigger on its own was the
# first implementation here and it classified all 355 live resources as needing
# work -- a catalogue-wide re-propose, the exact outcome the backfill exists to
# prevent. Absence of a row is not a defect in the copy.
TRIGGER_ISSUE_UNSEEN = "issue_unseen"
TRIGGER_ISSUE_PERSISTS = "issue_persists"
TRIGGER_DRIFT_WITH_ISSUE = "drift_with_issue"
TRIGGER_RESWEEP = "rules_version_resweep"
TRIGGER_NONE = ""

# Drift with no issue code attached is not work, but it is not nothing either:
# the row's hash is stale and must be refreshed or the ledger lies about what
# is live. Reported separately so a silent hash update never masquerades as a
# clean resource.
OBSERVATION_DRIFT_CLEAN = "drift_no_issue"


class TestModeRefusalError(RuntimeError):
    """A ledger write would land in the factory-default production state file
    while DUCK_TEST_MODE=1.

    Loud rather than a silent no-op: a no-op would mask test failures that
    depend on the write having happened ([[feedback_swallowed_errors_lie]]).
    """


def _refusing_test_mode_prod_write(path: Path | None) -> bool:
    if str(os.environ.get(_TEST_MODE_ENV) or "").strip() not in {"1", "true", "TRUE", "yes"}:
        return False
    resolved = Path(path).resolve() if path is not None else SEO_COPY_LEDGER_PATH.resolve()
    return resolved == _FROZEN_PRODUCTION_LEDGER_PATH


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def value_hash(value: Any) -> str:
    """Hash of a live copy value, normalised the way the auditor reads it.

    Whitespace-normalised so a cosmetic re-save in the Shopify admin does not
    read as an operator edit, but otherwise exact -- a one-word change must
    show up as drift.
    """
    normalized = " ".join(str(value or "").strip().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]


def row_key(platform: str, resource_id: Any) -> str:
    return f"{platform}:{resource_id}"


def load_ledger(path: Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else SEO_COPY_LEDGER_PATH
    if not target.exists():
        return {"ledger_version": 1, "updated_at": None, "rows": {}}
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        # A corrupt ledger must not take the lane down, but it must not read as
        # "nothing was ever written" either -- that would re-propose the whole
        # catalogue. The caller sees the marker and can refuse to batch.
        return {"ledger_version": 1, "updated_at": None, "rows": {}, "load_error": True}
    if not isinstance(data, dict):
        return {"ledger_version": 1, "updated_at": None, "rows": {}, "load_error": True}
    data.setdefault("rows", {})
    return data


def save_ledger(ledger: dict[str, Any], path: Path | None = None) -> Path:
    target = Path(path) if path is not None else SEO_COPY_LEDGER_PATH
    if _refusing_test_mode_prod_write(path):
        raise TestModeRefusalError(
            f"{_TEST_MODE_ENV}=1 and the ledger write would land in the "
            f"factory-default production path {target}. Patch "
            "SEO_COPY_LEDGER_PATH or pass path=tmp_path."
        )
    payload = dict(ledger)
    payload["updated_at"] = now_iso()
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, target)
    return target


def record_write(
    ledger: dict[str, Any],
    *,
    platform: str = "shopify",
    resource_id: Any,
    fields: dict[str, Any],
    source: str,
    applied_run_id: str = "",
    verified_at: str = "",
    rules_version: int | None = None,
) -> dict[str, Any]:
    """Record that `fields` were written for one resource.

    `fields` maps field name -> the value that is now LIVE, so the row stores
    the hash of what the store actually holds, not of what was proposed. A
    proposal that failed to apply must never leave a row claiming it did.
    """
    key = row_key(platform, resource_id)
    row = dict((ledger.get("rows") or {}).get(key) or {})
    row["platform"] = platform
    row["resource_id"] = str(resource_id)
    row["rules_version"] = int(
        rules_version if rules_version is not None else seo_rules_version()
    )
    row["source"] = source
    row["written_at"] = now_iso()
    if applied_run_id:
        row["applied_run_id"] = applied_run_id
    if verified_at:
        row["verified_at"] = verified_at
    hashes = dict(row.get("live_value_hash") or {})
    for field, value in (fields or {}).items():
        if field not in LEDGER_FIELDS:
            continue
        hashes[field] = value_hash(value)
    row["live_value_hash"] = hashes
    ledger.setdefault("rows", {})[key] = row
    return row


def backfill_from_receipts(
    ledger: dict[str, Any],
    *,
    receipts_dir: Path | None = None,
    platform: str = "shopify",
) -> dict[str, Any]:
    """Seed the ledger from the writeback receipts that already exist.

    Starting empty would make every resource look new and re-propose the whole
    catalogue on the first run -- the opposite of the point. The receipts are
    the authoritative record of what was applied and verified, and they carry
    the OBSERVED value (what the store held on read-back), which is exactly
    what the row's hash must be built from.

    Receipts are processed oldest-first so the newest write wins.
    """
    source_dir = Path(receipts_dir) if receipts_dir is not None else SEO_WRITEBACK_RECEIPTS_DIR
    stats = {"receipts_seen": 0, "receipts_used": 0, "rows_written": 0, "skipped_unverified": 0}
    if not source_dir.exists():
        return stats
    for receipt_path in sorted(source_dir.glob("*.json")):
        stats["receipts_seen"] += 1
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(receipt, dict):
            continue
        resource_id = receipt.get("resource_id")
        if not resource_id:
            continue
        # Only a verified receipt proves the value is live. An attempted or
        # failed write must not leave a row claiming the resource is current
        # ([[feedback_verify_against_the_marketplace]]).
        if str(receipt.get("status") or "").strip().lower() != "verified":
            stats["skipped_unverified"] += 1
            continue
        observed = receipt.get("observed") if isinstance(receipt.get("observed"), dict) else {}
        applied = [str(f) for f in (receipt.get("applied_fields") or [])]
        fields = {
            field: observed.get(field)
            for field in applied
            if field in LEDGER_FIELDS and observed.get(field) is not None
        }
        if not fields:
            continue
        stats["receipts_used"] += 1
        record_write(
            ledger,
            platform=platform,
            resource_id=resource_id,
            fields=fields,
            source="backfill_receipt",
            applied_run_id=str(receipt.get("run_id") or ""),
            verified_at=str(receipt.get("verified_at") or ""),
            # Receipts written before seo_rules_version existed carry no key
            # and read as 0 -- below any real version, so every pre-config
            # resource is eligible for the first deliberate re-sweep without
            # pretending it was written under rules that did not exist yet.
            # Receipts written since carry the real version.
            rules_version=int(receipt.get("seo_rules_version") or 0),
        )
    stats["rows_written"] = len(ledger.get("rows") or {})
    return stats


def classify_resource(
    row: dict[str, Any] | None,
    *,
    live_values: dict[str, Any],
    issue_codes: Iterable[str] = (),
    current_rules_version: int | None = None,
    resweep: bool = False,
) -> dict[str, Any]:
    """Decide whether one resource needs work, and say why.

    `resweep=True` is the deliberate post-version-bump sweep: it pulls in
    resources whose row predates the current rules even when the auditor is
    happy with them. It is off by default, because a version bump that
    automatically re-proposed every old row would make the bump unbounded --
    the thing the operator must be able to decide the size of.
    """
    version = int(
        current_rules_version if current_rules_version is not None else seo_rules_version()
    )
    codes = sorted({str(c) for c in (issue_codes or []) if str(c)})

    hashes = dict((row or {}).get("live_value_hash") or {})
    drifted = sorted(
        field
        for field, value in (live_values or {}).items()
        if field in LEDGER_FIELDS and field in hashes and value_hash(value) != hashes[field]
    )
    row_version = int((row or {}).get("rules_version") or 0)

    # The auditor is the gate. No issue code means there is nothing to fix,
    # whatever the ledger says -- except in an explicit re-sweep.
    if not codes:
        if drifted:
            return {
                "trigger": TRIGGER_NONE,
                "observation": OBSERVATION_DRIFT_CLEAN,
                "needs_work": False,
                "reason": (
                    "live value changed outside this lane but the auditor is "
                    f"content: {', '.join(drifted)} -- refresh the row's hash"
                ),
                "drifted_fields": drifted,
                "issue_codes": [],
            }
        if resweep and row and row_version < version:
            return {
                "trigger": TRIGGER_RESWEEP,
                "needs_work": True,
                "reason": f"re-sweep: written under seo_rules_version {row_version}, current is {version}",
                "row_rules_version": row_version,
                "issue_codes": [],
            }
        return {"trigger": TRIGGER_NONE, "needs_work": False, "reason": "no issue codes", "issue_codes": []}

    # From here the auditor has flagged something, so there IS work. The only
    # question left is what the ledger knows about our previous attempt.
    if drifted:
        return {
            "trigger": TRIGGER_DRIFT_WITH_ISSUE,
            "needs_work": True,
            "reason": (
                f"live value changed outside this lane ({', '.join(drifted)}) and "
                f"the auditor flags {', '.join(codes)}"
            ),
            "drifted_fields": drifted,
            "issue_codes": codes,
        }

    if row and row_version >= version:
        # We already rewrote this resource under the current rules and it still
        # fails the auditor. Regenerating with the same generator and the same
        # rules would most likely produce the same copy, so this is surfaced as
        # a rules problem rather than queued as routine work
        # ([[feedback_llm_detection_vs_generation]]).
        return {
            "trigger": TRIGGER_ISSUE_PERSISTS,
            "needs_work": False,
            "reason": (
                f"already written under seo_rules_version {row_version} and the "
                f"auditor still flags {', '.join(codes)} -- the rules or the "
                "generator need a change, not another pass"
            ),
            "row_rules_version": row_version,
            "issue_codes": codes,
        }

    return {
        "trigger": TRIGGER_ISSUE_UNSEEN,
        "needs_work": True,
        "reason": (
            f"auditor flags {', '.join(codes)} and this lane has not written it "
            f"under seo_rules_version {version}"
        ),
        "row_rules_version": row_version,
        "issue_codes": codes,
    }


def classify_audit_resources(
    resources: Iterable[dict[str, Any]],
    *,
    ledger: dict[str, Any] | None = None,
    platform: str = "shopify",
    current_rules_version: int | None = None,
    resweep: bool = False,
    include_observations: bool = False,
) -> list[dict[str, Any]]:
    """Classify a whole audit snapshot.

    Returns the resources that need work. With `include_observations=True` it
    also returns the no-work rows that carry an observation (drift the auditor
    is content with), so a caller can refresh their hashes.
    """
    book = ledger if ledger is not None else load_ledger()
    rows = book.get("rows") or {}
    out: list[dict[str, Any]] = []
    for resource in resources or []:
        resource_id = resource.get("resource_id") or resource.get("id")
        if not resource_id:
            continue
        live = {
            "seo_title": resource.get("seo_title"),
            "seo_description": resource.get("seo_description"),
        }
        codes = [str(i.get("code")) for i in (resource.get("issues") or []) if i.get("code")]
        verdict = classify_resource(
            rows.get(row_key(platform, resource_id)),
            live_values=live,
            issue_codes=codes,
            current_rules_version=current_rules_version,
            resweep=resweep,
        )
        keep = verdict.get("needs_work") or (
            include_observations and verdict.get("observation")
        )
        if not keep:
            continue
        out.append(
            {
                "resource_id": str(resource_id),
                "kind": resource.get("kind"),
                "title": resource.get("title"),
                **verdict,
            }
        )
    return out




def trigger_summary(classified: Iterable[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for item in classified or []:
        key = item.get("trigger") or item.get("observation") or TRIGGER_NONE
        summary[key] = summary.get(key, 0) + 1
    return summary


def rebuild_ledger(
    *,
    receipts_dir: Path | None = None,
    path: Path | None = None,
    platform: str = "shopify",
) -> dict[str, Any]:
    """Rebuild the ledger from the receipts and persist it.

    The ledger is a DERIVED INDEX, not a second source of truth: the receipts
    are the authoritative record of what was applied and verified, and this
    rebuilds the index over them. That is why there is no dual write at apply
    time -- two records of the same fact drift, and the one nobody reads is
    always the one that is wrong.

    Safe to run repeatedly; the newest receipt per resource wins.
    """
    ledger: dict[str, Any] = {"ledger_version": 1, "rows": {}}
    stats = backfill_from_receipts(ledger, receipts_dir=receipts_dir, platform=platform)
    ledger["rebuilt_at"] = now_iso()
    ledger["source"] = "shopify_seo_writeback_receipts"
    ledger["stats"] = stats
    save_ledger(ledger, path=path)
    return {"path": str(path or SEO_COPY_LEDGER_PATH), **stats}


if __name__ == "__main__":  # pragma: no cover - operator entry point
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rebuild", action="store_true", help="rebuild the ledger from the writeback receipts"
    )
    args = parser.parse_args()
    if args.rebuild:
        print(json.dumps(rebuild_ledger(), indent=2))
    else:
        book = load_ledger()
        print(
            json.dumps(
                {
                    "path": str(SEO_COPY_LEDGER_PATH),
                    "rows": len(book.get("rows") or {}),
                    "rebuilt_at": book.get("rebuilt_at"),
                    "seo_rules_version": seo_rules_version(),
                },
                indent=2,
            )
        )
