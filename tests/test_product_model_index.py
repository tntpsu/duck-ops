"""Surface 64 Phase 1: product→model index producer.

Fixture tree mirrors the real source layouts: paint-to-print-3d job dirs
(with/without handoff manifest) + Studio 3d_lab delivery dirs. Fixture
markers ("unit-test-fixture-duck", 9999xxxxx ids) are what the layer-3
pollution audit greps the LIVE index for — keep them in sync.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import product_model_index as pmi
from workflow_control import TestModeRefusalError


def _write(path: Path, content: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _paint_job(root: Path, name: str, *, object_name: str | None = None,
               ready: bool = True, with_manifest: bool = True) -> Path:
    job = root / name
    _write(job / "selected" / "region_materials.obj", "o unit-test-fixture-duck\n")
    _write(job / "selected" / "region_materials.mtl", "newmtl region_0\n")
    if with_manifest:
        manifest = {
            "schema_version": "duckagent.paint_to_print_handoff.v1",
            "ready_for_duckagent_handoff": ready,
            "source": {"object_name": object_name or ""},
        }
        _write(job / "handoff_manifest.json", json.dumps(manifest))
    return job


def _studio_dir(root: Path, slug: str, *, glb_name: str = "3dai_single_model.glb") -> Path:
    run = root / f"portal_studio_3d_20260801_120000_000001_{slug}"
    _write(run / glb_name, "glb-bytes")
    return run


@pytest.fixture()
def catalog(tmp_path):
    items = {
        "999900000001": {"handle": "highland-cow-duck-fluffy-collectible",
                         "title": "Highland Cow Duck – Fluffy Collectible",
                         "status": "active", "tags": "cow, highland, farm"},
        "999900000002": {"handle": "captain-duck-nautical-collectible",
                         "title": "Captain Duck – Nautical Collectible",
                         "status": "active", "tags": "captain, sailor"},
        "999900000003": {"handle": "plain-yellow-duck",
                         "title": "Plain Yellow Duck",
                         "status": "active", "tags": "classic"},
        "999900000004": {"handle": "archived-duck", "title": "Archived Duck",
                         "status": "archived", "tags": ""},
    }
    path = tmp_path / "catalog_index.json"
    path.write_text(json.dumps({"generated_at": "2026-08-01", "items": items}))
    return path


def _run(tmp_path, catalog, **kwargs):
    paint = tmp_path / "paint_outputs"
    studio = tmp_path / "studio_lab"
    paint.mkdir(exist_ok=True)
    studio.mkdir(exist_ok=True)
    return pmi.run_once(
        paint_outputs_dir=paint, studio_lab_dir=studio,
        catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
        index_path=tmp_path / "index_out.json", **kwargs,
    ), paint, studio


class TestScanAndJoin:
    def test_manifest_object_name_joins_to_product(self, tmp_path, catalog):
        paint = tmp_path / "paint_outputs"
        _paint_job(paint, "highland_cow_duck_handoff_v1", object_name="Highland Cow Duck")
        payload = pmi.run_once(
            paint_outputs_dir=paint, studio_lab_dir=tmp_path / "none",
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=tmp_path / "index_out.json",
        )
        item = payload["items"]["999900000001"]
        assert item["model"]["kind"] == "obj_mtl"
        assert item["model"]["handoff_ready"] is True
        assert item["needs_review"] is False
        assert item["matched_by"] == "full_token_coverage"

    def test_dir_name_fallback_when_no_manifest(self, tmp_path, catalog):
        paint = tmp_path / "paint_outputs"
        _paint_job(paint, "captain_duck_fixed_conversion", with_manifest=False)
        payload = pmi.run_once(
            paint_outputs_dir=paint, studio_lab_dir=tmp_path / "none",
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=tmp_path / "index_out.json",
        )
        # "fixed"/"conversion" are noise tokens; "captain" remains → single-token
        # low-confidence match must be flagged, never silently trusted.
        item = payload["items"]["999900000002"]
        assert item["needs_review"] is True

    def test_studio_glb_scanned_with_slug_text(self, tmp_path, catalog):
        studio = tmp_path / "studio_lab"
        _studio_dir(studio, "highland-cow-duck")
        payload = pmi.run_once(
            paint_outputs_dir=tmp_path / "none", studio_lab_dir=studio,
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=tmp_path / "index_out.json",
        )
        item = payload["items"]["999900000001"]
        assert item["model"]["kind"] == "glb"
        assert item["model"]["glb"].endswith("3dai_single_model.glb")

    def test_obj_mtl_beats_glb_for_same_product(self, tmp_path, catalog):
        paint = tmp_path / "paint_outputs"
        studio = tmp_path / "studio_lab"
        _studio_dir(studio, "highland-cow-duck")
        _paint_job(paint, "highland_cow_duck_handoff_v1", object_name="Highland Cow Duck")
        payload = pmi.run_once(
            paint_outputs_dir=paint, studio_lab_dir=studio,
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=tmp_path / "index_out.json",
        )
        assert payload["items"]["999900000001"]["model"]["kind"] == "obj_mtl"

    def test_unmatched_model_lands_in_bucket_not_a_guess(self, tmp_path, catalog):
        paint = tmp_path / "paint_outputs"
        _paint_job(paint, "zebra_stripes_thing_v1", object_name="Zebra Stripes Thing")
        payload = pmi.run_once(
            paint_outputs_dir=paint, studio_lab_dir=tmp_path / "none",
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=tmp_path / "index_out.json",
        )
        assert payload["items"] == {}
        assert len(payload["unmatched_models"]) == 1
        assert payload["unmatched_models"][0]["reason"] == "no_confident_match"

    def test_ambiguous_full_coverage_needs_review(self, tmp_path):
        items = {
            "999900000010": {"handle": "cow-duck-brown", "title": "Cow Duck Brown",
                             "status": "active", "tags": ""},
            "999900000011": {"handle": "cow-duck-white", "title": "Cow Duck White",
                             "status": "active", "tags": ""},
        }
        catalog = tmp_path / "catalog_index.json"
        catalog.write_text(json.dumps({"items": items}))
        paint = tmp_path / "paint_outputs"
        _paint_job(paint, "cow_duck_v1", object_name="Cow Duck")
        payload = pmi.run_once(
            paint_outputs_dir=paint, studio_lab_dir=tmp_path / "none",
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=tmp_path / "index_out.json",
        )
        matched = [v for v in payload["items"].values()]
        assert len(matched) == 1
        assert matched[0]["needs_review"] is True
        assert matched[0]["matched_by"] == "ambiguous_full_coverage"

    def test_products_without_model_lists_active_only(self, tmp_path, catalog):
        payload, *_ = _run(tmp_path, catalog)
        assert "999900000003" in payload["products_without_model"]
        assert "999900000004" not in payload["products_without_model"]  # archived

    def test_not_ready_manifest_still_scanned_but_marked(self, tmp_path, catalog):
        paint = tmp_path / "paint_outputs"
        _paint_job(paint, "highland_cow_duck_wip", object_name="Highland Cow Duck", ready=False)
        payload = pmi.run_once(
            paint_outputs_dir=paint, studio_lab_dir=tmp_path / "none",
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=tmp_path / "index_out.json",
        )
        assert payload["items"]["999900000001"]["model"]["handoff_ready"] is False


class TestWriteGuardAndOutput:
    def test_dry_run_writes_nothing(self, tmp_path, catalog):
        out = tmp_path / "index_out.json"
        pmi.run_once(
            dry_run=True,
            paint_outputs_dir=tmp_path / "none", studio_lab_dir=tmp_path / "none",
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=out,
        )
        assert not out.exists()

    def test_write_is_atomic_json(self, tmp_path, catalog):
        out = tmp_path / "index_out.json"
        payload = pmi.run_once(
            paint_outputs_dir=tmp_path / "none", studio_lab_dir=tmp_path / "none",
            catalog_index_path=catalog, aliases_path=tmp_path / "no_aliases.json",
            index_path=out,
        )
        on_disk = json.loads(out.read_text())
        assert on_disk["products_without_model"] == payload["products_without_model"]
        assert "generated_at" in on_disk

    def test_frozen_prod_path_refused_under_test_mode(self, tmp_path):
        assert os.environ.get("DUCK_TEST_MODE") == "1"
        with pytest.raises(TestModeRefusalError):
            pmi.write_product_model_index(
                {"items": {}},
                path=pmi._FROZEN_PRODUCTION_PRODUCT_MODEL_INDEX_PATH,
            )
