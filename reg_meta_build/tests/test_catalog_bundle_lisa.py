"""LISA workbook selection, capture and verification in catalog input bundles."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    repin_input_bundle,
    sparsify_scb_values,
    write_input_bundle,
    write_scb_input,
    write_scb_snapshot,
)
from _lisa_fixtures import write_lisa_workbook
from reg_meta_build.input_snapshot import (
    LISA_BUNDLE_PATH,
    LISA_DATASET_ID,
    LisaWorkbookSelection,
    SnapshotError,
    open_input_bundle,
    prepare_input_bundle,
    verify_input_bundle,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_catalog_bundle_declares_unselected_lisa_without_documentation_fallback(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    loose_workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")

    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    bundle = open_input_bundle(selection)

    dataset = bundle.manifest.supplemental_datasets[0]
    assert dataset.dataset == LISA_DATASET_ID
    assert dataset.selected is False
    assert dataset.required is False
    assert dataset.exclusion_reason == "not selected during bundle preparation"
    artifact = next(
        item for item in bundle.manifest.files if item.path == LISA_BUNDLE_PATH
    )
    assert artifact.present is False
    assert not (bundle.root / LISA_BUNDLE_PATH).exists()
    assert loose_workbook.is_file()
    with pytest.raises(SnapshotError, match="was not selected"):
        bundle.require_supplemental_dataset(LISA_DATASET_ID)


def test_catalog_bundle_captures_exact_selected_lisa_bytes_and_identity(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    source_bytes = workbook.read_bytes()
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()

    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=source_sha256,
        ),
    )
    workbook.write_bytes(b"changed after capture")
    bundle = open_input_bundle(selection)
    dataset, artifact, captured = bundle.require_supplemental_dataset(LISA_DATASET_ID)

    assert dataset.publisher == "SCB"
    assert dataset.purpose
    assert dataset.reader == "lisa-variable-workbook-v1"
    assert dataset.layout == "lisa-variable-list-2024-2025-v1"
    assert dataset.upstream_revision == "2024-2025"
    assert dataset.selected is True
    assert dataset.required is True
    assert artifact.sha256 == source_sha256
    assert artifact.size == len(source_bytes)
    assert captured.read_bytes() == source_bytes


def test_catalog_bundle_captures_lisa_selected_outside_catalog_input_root(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    workbook = write_lisa_workbook(tmp_path / "official-source" / "lisa.xlsx")
    source_bytes = workbook.read_bytes()

    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(source_bytes).hexdigest(),
        ),
    )

    assert (selection.path / LISA_BUNDLE_PATH).read_bytes() == source_bytes


def test_selected_lisa_ordinary_open_is_quick_and_explicit_verify_hashes(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )
    captured = selection.path / LISA_BUNDLE_PATH
    payload = bytearray(captured.read_bytes())
    payload[-1] ^= 1
    captured.write_bytes(payload)
    changed = repin_input_bundle(selection, "same-size LISA damage")

    # Routine reads retain the existing manifest/inventory size boundary.
    open_input_bundle(changed)
    with pytest.raises(SnapshotError, match="hash mismatch"):
        verify_input_bundle(changed)


def test_selected_lisa_missing_from_pinned_bundle_has_no_fallback(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    selection = write_input_bundle(
        tmp_path / "accepted",
        input_dir,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )
    (selection.path / LISA_BUNDLE_PATH).unlink()
    missing = repin_input_bundle(selection, "missing selected LISA")

    with pytest.raises(SnapshotError, match="inventory mismatch"):
        open_input_bundle(missing)


@pytest.mark.parametrize("failure", ("missing", "wrong-hash", "invalid-workbook"))
def test_catalog_bundle_rejects_missing_wrong_or_invalid_selected_lisa(
    tmp_path: Path, failure: str
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    workbook = input_dir / "docs" / "lisa.xlsx"
    if failure == "missing":
        selected_hash = "0" * 64
        expected = "selected LISA workbook not found"
    elif failure == "wrong-hash":
        write_lisa_workbook(workbook)
        selected_hash = "0" * 64
        expected = "does not match --lisa-workbook-sha256"
    else:
        workbook.parent.mkdir(parents=True)
        workbook.write_bytes(b"not a workbook")
        selected_hash = hashlib.sha256(workbook.read_bytes()).hexdigest()
        expected = "cannot open selected LISA workbook"
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    output = snapshot.path.parent / "bundle"

    with pytest.raises(SnapshotError, match=expected):
        prepare_input_bundle(
            input_dir,
            snapshot,
            output,
            lisa_workbook=LisaWorkbookSelection(
                path=workbook,
                upstream_revision="2024-2025",
                sha256=selected_hash,
            ),
        )

    assert not output.exists()


def test_lisa_bundle_preparation_uses_small_source_validation_with_sparse_values(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    # With the value role sparse, exhaustively verifying the snapshot or opening
    # Vardemangder would fail: LISA capture must do neither.
    sparsify_scb_values(snapshot)
    output = snapshot.path.parent / "bundle"
    prepare_input_bundle(
        input_dir,
        snapshot,
        output,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )

    assert (output / LISA_BUNDLE_PATH).read_bytes() == workbook.read_bytes()
