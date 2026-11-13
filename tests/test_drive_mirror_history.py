"""Run history and the upload probe -- the evidence the mirror's OS cards read.

Surface 123. The manifest records only the LATEST run, which is enough to see
a lane at exactly zero and blind to one that merely collapsed. 1320 product
photos becoming 12 is the same selection bug and has no symptom a single-run
view can show, so the selection-sanity card needs a baseline and the producer
has to keep one.

The upload probe answers the other half: the mirror's success condition is
that data LEAVES the machine, and the producer's own view stops at the local
directory. Both pieces are EVIDENCE, so the rule that governs them is that
neither may ever fail the run it documents
([[feedback_receipts_never_load_bearing]]).
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drive_mirror  # noqa: E402


def _manifest(**kw) -> dict:
    out = {
        "generated_at": "2026-11-13T11:11:32-05:00",
        "status": "released",
        "ok": True,
        "staged_files": 178731,
        "staged_bytes": 10617329606,
        "lane_counts": {"meme": 131, "product_photos": 1320},
        "trees": [
            {"tree": "duck-ops/output", "files": 116508, "status": "ok"},
            {"tree": "duck-ops/state", "files": 59767, "status": "ok"},
        ],
        "failures": [],
        "upload": {"readable": True, "uploaded": 178731, "local_files": 178731},
    }
    out.update(kw)
    return out


class RunHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = TemporaryDirectory()
        self.path = Path(self.ctx.name) / "state" / "drive_mirror_runs.jsonl"

    def tearDown(self) -> None:
        self.ctx.cleanup()

    def _rows(self) -> list[dict]:
        return [json.loads(line)
                for line in self.path.read_text(encoding="utf-8").splitlines()
                if line.strip()]

    def test_each_run_appends_one_row(self) -> None:
        for _ in range(3):
            drive_mirror._append_run_history(self.path, _manifest())
        self.assertEqual(len(self._rows()), 3)

    def test_a_row_carries_the_counts_the_baseline_is_built_from(self) -> None:
        drive_mirror._append_run_history(self.path, _manifest())
        row = self._rows()[0]
        self.assertEqual(row["lane_counts"]["product_photos"], 1320)
        self.assertEqual(row["tree_files"]["duck-ops/output"], 116508)
        self.assertEqual(row["staged_files"], 178731)

    def test_a_refused_run_is_recorded_too(self) -> None:
        # The throughput card asks "is work completing". A week of refusals is
        # exactly the shape that question exists to catch, so a refused run
        # must not be absent from the evidence.
        drive_mirror._append_run_history(
            self.path, _manifest(status="refused", ok=False, failures=["gate: nope"]))
        row = self._rows()[0]
        self.assertEqual(row["status"], "refused")
        self.assertFalse(row["ok"])
        self.assertEqual(row["failure_count"], 1)

    def test_history_is_capped_and_keeps_the_newest(self) -> None:
        for i in range(10):
            drive_mirror._append_run_history(
                self.path, _manifest(staged_files=i), kept=4)
        rows = self._rows()
        self.assertEqual(len(rows), 4)
        self.assertEqual([r["staged_files"] for r in rows], [6, 7, 8, 9])

    def test_an_unwritable_history_never_fails_the_run(self) -> None:
        # Evidence capture must not be able to break the thing it documents.
        blocked = Path(self.ctx.name) / "blocked"
        blocked.write_text("not a directory", encoding="utf-8")
        drive_mirror._append_run_history(blocked / "runs.jsonl", _manifest())

    def test_a_corrupt_existing_file_does_not_lose_the_new_row(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("{garbage\n", encoding="utf-8")
        drive_mirror._append_run_history(self.path, _manifest())
        text = self.path.read_text(encoding="utf-8")
        self.assertIn("178731", text)


class UploadProbeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = TemporaryDirectory()
        self.root = Path(self.ctx.name) / "mirror"
        (self.root / "duck-ops").mkdir(parents=True)
        (self.root / "duck-ops" / "a.json").write_text("{}", encoding="utf-8")
        (self.root / "duck-ops" / "b.json").write_text("{}", encoding="utf-8")

    def tearDown(self) -> None:
        self.ctx.cleanup()

    def test_a_missing_drive_install_is_reported_not_raised(self) -> None:
        original = drive_mirror.DRIVEFS_SUPPORT_ROOT
        drive_mirror.DRIVEFS_SUPPORT_ROOT = Path(self.ctx.name) / "no-such-drive"
        try:
            out = drive_mirror._probe_drive_upload_state(self.root)
        finally:
            drive_mirror.DRIVEFS_SUPPORT_ROOT = original
        self.assertFalse(out["readable"])
        self.assertIn("not installed", str(out["error"]))
        # The local half is still measured even when the remote half is not.
        self.assertEqual(out["local_files"], 2)

    def test_drive_s_own_scratch_directory_is_not_counted_as_content(self) -> None:
        scratch = self.root / ".tmp.driveupload"
        scratch.mkdir()
        (scratch / "in-flight").write_text("x", encoding="utf-8")
        original = drive_mirror.DRIVEFS_SUPPORT_ROOT
        drive_mirror.DRIVEFS_SUPPORT_ROOT = Path(self.ctx.name) / "no-such-drive"
        try:
            out = drive_mirror._probe_drive_upload_state(self.root)
        finally:
            drive_mirror.DRIVEFS_SUPPORT_ROOT = original
        self.assertEqual(out["local_files"], 2)

    def test_an_unreadable_database_is_reported_not_raised(self) -> None:
        fake = Path(self.ctx.name) / "DriveFS" / "1234567890"
        fake.mkdir(parents=True)
        (fake / "mirror_sqlite.db").write_text("this is not a database", encoding="utf-8")
        original = drive_mirror.DRIVEFS_SUPPORT_ROOT
        drive_mirror.DRIVEFS_SUPPORT_ROOT = fake.parent
        try:
            out = drive_mirror._probe_drive_upload_state(self.root)
        finally:
            drive_mirror.DRIVEFS_SUPPORT_ROOT = original
        self.assertFalse(out["readable"])
        self.assertIsNotNone(out["error"])

    def test_a_real_database_is_read_through_a_copy(self) -> None:
        import sqlite3
        fake = Path(self.ctx.name) / "DriveFS" / "1234567890"
        fake.mkdir(parents=True)
        db = fake / "mirror_sqlite.db"
        con = sqlite3.connect(db)
        con.executescript(
            "create table mirror_item (cloud_md5_checksum text);"
            "create table queued_uploads (stable_id integer);"
            "create table pending_uploads (local_stable_id integer);"
            "insert into mirror_item values ('abc'), ('def'), (''), (null);"
            "insert into queued_uploads values (1), (2);"
        )
        con.commit()
        con.close()
        before = db.stat().st_mtime_ns
        original = drive_mirror.DRIVEFS_SUPPORT_ROOT
        drive_mirror.DRIVEFS_SUPPORT_ROOT = fake.parent
        try:
            out = drive_mirror._probe_drive_upload_state(self.root)
        finally:
            drive_mirror.DRIVEFS_SUPPORT_ROOT = original
        self.assertTrue(out["readable"], out["error"])
        # Only the rows carrying a real checksum count as uploaded.
        self.assertEqual(out["uploaded"], 2)
        self.assertEqual(out["queued"], 2)
        self.assertEqual(out["pending"], 0)
        # Drive owns this file; the probe reads a copy and leaves it alone.
        self.assertEqual(db.stat().st_mtime_ns, before)


if __name__ == "__main__":
    unittest.main()
