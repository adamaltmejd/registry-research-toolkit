"""Opening a pinned SCB snapshot: pins, clean checkout, optional files, quick reads."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    omit_scb_snapshot_file,
    repin_input_bundle,
    sparsify_scb_values,
    write_input_bundle,
    write_scb_input,
    write_scb_snapshot,
)
from reg_meta_build.input_snapshot import (
    CatalogBundleSelection,
    ScbSnapshotSelection,
    SnapshotError,
    open_input_bundle,
    open_scb_snapshot,
    verify_input_bundle,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_selected_snapshot_requires_exact_commit_manifest_and_clean_checkout(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    selection = write_scb_snapshot(tmp_path / "snapshot-fixture", source_dir)

    reader = open_scb_snapshot(selection)
    with reader.open_csv("Registerinformation.csv") as (_header, rows):
        assert sum(1 for _row in rows) > 0

    with pytest.raises(SnapshotError, match="input commit pin mismatch"):
        open_scb_snapshot(
            type(selection)(selection.path, "0" * 40, selection.manifest_sha256)
        )
    with pytest.raises(SnapshotError, match="manifest pin mismatch"):
        open_scb_snapshot(
            type(selection)(selection.path, selection.input_commit, "0" * 64)
        )

    (selection.path / "manifest.json").write_bytes(b"dirty")
    with pytest.raises(SnapshotError, match="must be clean"):
        open_scb_snapshot(selection)


def test_selected_snapshot_preserves_optional_absence_and_rejects_value_pairing(
    tmp_path: Path,
) -> None:
    partial_source = write_scb_input(
        tmp_path / "partial-source", include=("registerinformation",)
    )
    partial = write_scb_snapshot(tmp_path / "partial-snapshot", partial_source)
    reader = open_scb_snapshot(partial)
    assert reader.has_file("Registerinformation.csv")
    assert not reader.has_file("Identifierare.csv")

    source_dir = write_scb_input(tmp_path / "paired-source")
    selection = write_scb_snapshot(tmp_path / "paired-snapshot", source_dir)
    unpaired = omit_scb_snapshot_file(selection, "VardemangderValidDates.csv")
    with pytest.raises(SnapshotError, match="requires VardemangderValidDates.csv"):
        open_scb_snapshot(unpaired)


def _damage_committed_record_same_size(selection: CatalogBundleSelection) -> None:
    """Flip one byte of a normalized Timeseries record without changing its size."""
    bundle_manifest = json.loads(
        (selection.path / "catalog-bundle.json").read_text(encoding="utf-8")
    )
    snapshot = selection.path.parent / bundle_manifest["scb_snapshot_path"]
    manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    item = next(item for item in manifest["files"] if item["name"] == "Timeseries.csv")
    record = snapshot / item["records"][0]["path"]
    payload = bytearray(record.read_bytes())
    payload[0] = ord("A") if payload[0] != ord("A") else ord("B")
    record.write_bytes(payload)


@pytest.mark.parametrize("sparse", (False, True), ids=("complete", "sparse"))
def test_quick_selection_accepts_same_size_damage_that_explicit_verify_rejects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sparse: bool
) -> None:
    # Ordinary selection checks pins, cleanliness and the size inventory only:
    # it neither hashes normalized payloads nor streams committed blobs, so
    # same-size damage is accepted there and caught only by explicit verify.
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    _damage_committed_record_same_size(selection)
    changed = repin_input_bundle(selection, "same-size snapshot damage")
    if sparse:
        sparsify_scb_values(changed)
    trace = tmp_path / "git-trace.log"
    monkeypatch.setenv("GIT_TRACE", str(trace))

    bundle = open_input_bundle(changed)
    reader = open_scb_snapshot(
        ScbSnapshotSelection(
            bundle.snapshot.root,
            changed.input_commit,
            bundle.manifest.scb_manifest_sha256,
        )
    )
    with reader.open_csv("Registerinformation.csv") as (_header, rows):
        assert sum(1 for _row in rows) > 0
    assert reader.vardemangder_materialized is not sparse
    assert "cat-file --batch" not in trace.read_text(encoding="utf-8")

    monkeypatch.delenv("GIT_TRACE")
    with pytest.raises(SnapshotError):
        verify_input_bundle(changed)
