"""Catalog input bundles: preparation, quick open and explicit verification."""

from __future__ import annotations

import hashlib
import json
import shutil
from typing import TYPE_CHECKING
from zipfile import BadZipFile

import pytest
from _csv_fixtures import (
    repin_input_bundle,
    write_input_bundle,
    write_input_bundle_from_snapshot,
    write_scb_input,
    write_scb_snapshot,
)
from _snapshot_fixtures import git
from reg_meta_build.input_snapshot import (
    CatalogBundleSelection,
    SnapshotError,
    open_input_bundle,
    prepare_input_bundle,
    verify_input_bundle,
)
from reg_meta_build.sources.curated_records import CuratedSourceError
from reg_meta_build.sources.scb_reference_records import ScbReferenceSourceError

if TYPE_CHECKING:
    from pathlib import Path


def test_catalog_bundle_captures_complete_small_inventory_and_selects_quickly(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    auxiliary = input_dir / "SCB" / "Tabelldefinitioner.sql"
    auxiliary.write_bytes(
        b"-- exact source fact\r\n"
        b"CREATE TABLE [dbo].[Example]([A] [int] NULL) ON [PRIMARY]\r\nGO\r\n"
    )
    selection = write_input_bundle(tmp_path / "accepted", input_dir)

    bundle = open_input_bundle(selection)

    items = {item.path: item for item in bundle.manifest.files}
    assert items["catalog/SCB/Tabelldefinitioner.sql"].present
    assert not items["catalog/SCB/ID-kolumner.xlsx"].present
    assert (
        bundle.input_dir / "SCB" / "Tabelldefinitioner.sql"
    ).read_bytes() == auxiliary.read_bytes()
    manifest_text = (selection.path / "catalog-bundle.json").read_text(encoding="utf-8")
    assert str(tmp_path) not in manifest_text
    assert bundle.provenance == {
        "input_repository_commit": selection.input_commit,
        "bundle_manifest_path": "bundle/catalog-bundle.json",
        "bundle_manifest_sha256": selection.manifest_sha256,
    }


def test_catalog_bundle_explicit_verify_and_unlisted_file_rejection(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    assert verify_input_bundle(selection).schema_version == 2

    unlisted = selection.path / "catalog" / "SCB" / "new-optional-input.xlsx"
    unlisted.parent.mkdir(parents=True, exist_ok=True)
    unlisted.write_bytes(b"unlisted")
    repo = selection.path.parent
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "unlisted input")
    changed_commit = git(repo, "rev-parse", "HEAD")
    changed = CatalogBundleSelection(
        selection.path, changed_commit, selection.manifest_sha256
    )

    with pytest.raises(SnapshotError, match="inventory mismatch"):
        open_input_bundle(changed)


def test_catalog_bundle_manifest_changes_with_meaningful_auxiliary_input(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    auxiliary = input_dir / "SCB" / "Tabelldefinitioner.sql"
    auxiliary.write_text(
        "CREATE TABLE [dbo].[First]([A] [int] NULL) ON [PRIMARY]\nGO\n",
        encoding="utf-8",
    )
    first = write_input_bundle(tmp_path / "accepted-a", input_dir)

    auxiliary.write_text(
        "CREATE TABLE [dbo].[Second]([A] [int] NULL) ON [PRIMARY]\nGO\n",
        encoding="utf-8",
    )
    second = write_input_bundle(tmp_path / "accepted-b", input_dir)

    assert first.manifest_sha256 != second.manifest_sha256


def test_catalog_bundle_carries_classification_books_but_no_curation_or_naming(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from reg_meta_build.cli import run

    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    books = input_dir / "classifications"
    (books / "sos").mkdir(parents=True)
    (books / "kon.csv").write_text("code,label\n1,Man\n2,Kvinna\n", encoding="utf-8")
    (books / "sos" / "kva.csv").write_text("code,label\nA,Alfa\n", encoding="utf-8")
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    output = snapshot.path.parent / "bundle"

    exit_code = run(
        [
            "prepare-input-bundle",
            "--input-dir",
            str(input_dir),
            "--scb-snapshot",
            str(snapshot.path),
            "--scb-input-commit",
            snapshot.input_commit,
            "--scb-manifest-sha256",
            snapshot.manifest_sha256,
            "--output-dir",
            str(output),
        ]
    )
    assert exit_code == 0, capsys.readouterr().out

    written = {
        path.relative_to(output).as_posix()
        for path in output.rglob("*")
        if path.is_file()
    }
    assert {
        "catalog/classifications/kon.csv",
        "catalog/classifications/sos/kva.csv",
    } <= written
    assert not {
        path for path in written if path.startswith(("curation/", "fqid_slugs/"))
    }


@pytest.mark.parametrize(
    "introduced_relative",
    ("SCB/Tabelldefinitioner.sql", "Socialstyrelsen/introduced.xlsx"),
)
def test_catalog_bundle_preparation_rejects_new_source_membership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    introduced_relative: str,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    books = input_dir / "classifications"
    books.mkdir()
    (books / "kon.csv").write_text("code,label\n1,Man\n", encoding="utf-8")
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    output = snapshot.path.parent / "bundle"
    copy2 = shutil.copy2

    # Introduce the new source while the bundle's inputs are being copied.
    def copy_then_introduce(source, destination, **kwargs):
        copied = copy2(source, destination, **kwargs)
        introduced = input_dir / introduced_relative
        if not introduced.exists():
            introduced.parent.mkdir(parents=True, exist_ok=True)
            introduced.write_bytes(b"introduced during capture")
        return copied

    monkeypatch.setattr(shutil, "copy2", copy_then_introduce)
    with pytest.raises(SnapshotError, match="changed during bundle preparation"):
        prepare_input_bundle(input_dir, snapshot, output)

    assert not output.exists()
    assert not git(
        snapshot.path.parent, "status", "--porcelain=v1", "--untracked-files=all"
    )


def test_catalog_bundle_requires_exact_clean_selection_and_never_overwrites(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)

    with pytest.raises(SnapshotError, match="commit pin mismatch"):
        open_input_bundle(
            CatalogBundleSelection(selection.path, "0" * 40, selection.manifest_sha256)
        )
    with pytest.raises(SnapshotError, match="manifest pin mismatch"):
        open_input_bundle(
            CatalogBundleSelection(selection.path, selection.input_commit, "0" * 64)
        )
    with pytest.raises(SnapshotError, match="directory not found"):
        open_input_bundle(
            CatalogBundleSelection(
                tmp_path / "missing", selection.input_commit, selection.manifest_sha256
            )
        )

    (selection.path / "catalog-bundle.json").write_bytes(b"dirty")
    with pytest.raises(SnapshotError, match="must be clean"):
        open_input_bundle(selection)

    snapshot = write_scb_snapshot(tmp_path / "other-accepted", input_dir / "SCB")
    existing = snapshot.path.parent / "bundle"
    existing.mkdir()
    marker = existing / "keep"
    marker.write_text("accepted", encoding="utf-8")
    with pytest.raises(SnapshotError, match="will not be overwritten"):
        prepare_input_bundle(
            input_dir,
            snapshot,
            existing,
        )
    assert marker.read_text(encoding="utf-8") == "accepted"


def test_catalog_bundle_rejects_unsupported_manifest_before_build(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    manifest_path = selection.path / "catalog-bundle.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = 999
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    changed = repin_input_bundle(selection, "unsupported bundle schema")

    with pytest.raises(SnapshotError, match="unsupported catalog bundle schema"):
        open_input_bundle(changed)


def test_bundle_preparation_keeps_source_conflicts_for_common_resolution(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    canonical = input_dir / "scb_canonical"
    canonical.mkdir()
    (canonical / "scb_canonical.toml").write_text(
        '[[register]]\nkey = "example"\nname = "Example"\n'
        '[[register.variable]]\nname = "Code"\ncolumn = "Code"\n'
        'classification = "Unresolved source declaration"\nvalue_set = "codes"\n',
        encoding="utf-8",
    )
    codes = canonical / "codes.csv"
    codes.write_text("code,label\n01,First\n01,Conflicting\n", encoding="utf-8")

    selection = write_input_bundle(tmp_path / "accepted", input_dir)

    assert (
        selection.path / "catalog/scb_canonical/codes.csv"
    ).read_bytes() == codes.read_bytes()
    verify_input_bundle(selection)


@pytest.mark.parametrize("name", ("../outside", "/outside", "dir\\outside"))
def test_bundle_preparation_rejects_escaping_canonical_code_list(
    tmp_path: Path, name: str
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    canonical = input_dir / "scb_canonical"
    canonical.mkdir()
    (canonical / "scb_canonical.toml").write_text(
        '[[register]]\nkey = "example"\nname = "Example"\n'
        '[[register.variable]]\nname = "Code"\ncolumn = "Code"\n'
        f"value_set = {json.dumps(name)}\n",
        encoding="utf-8",
    )
    with pytest.raises(SnapshotError, match="local filename"):
        write_input_bundle(tmp_path / "accepted", input_dir)


def test_catalog_bundle_preparation_rejects_invalid_consumed_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from reg_meta.errors import EXIT_CONFIG

    from reg_meta_build import cli as cli_module

    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    provider_dir = input_dir / "Folkhalsomyndigheten"
    provider_dir.mkdir()
    (provider_dir / "fohm.toml").write_text("not = [valid", encoding="utf-8")
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")

    with pytest.raises(CuratedSourceError, match="invalid global curated TOML"):
        prepare_input_bundle(input_dir, snapshot, snapshot.path.parent / "bundle")
    assert (
        cli_module.run(
            [
                "prepare-input-bundle",
                "--input-dir",
                str(input_dir),
                "--scb-snapshot",
                str(snapshot.path),
                "--scb-input-commit",
                snapshot.input_commit,
                "--scb-manifest-sha256",
                snapshot.manifest_sha256,
                "--output-dir",
                str(snapshot.path.parent / "bundle"),
            ]
        )
        == EXIT_CONFIG
    )
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "catalog_input_bundle_invalid"
    assert "invalid global curated TOML" in error["message"]
    assert not (snapshot.path.parent / "bundle").exists()


@pytest.mark.parametrize(
    ("relative", "payload", "expected_exception"),
    (
        ("SCB/ID-kolumner.xlsx", b"not a zip", BadZipFile),
        ("SCB/Tabelldefinitioner.sql", b"\x81", ScbReferenceSourceError),
    ),
)
def test_bundle_prepare_and_verify_reject_invalid_small_consumed_contracts(
    tmp_path: Path,
    relative: str,
    payload: bytes,
    expected_exception: type[Exception],
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    malformed = input_dir / relative
    malformed.parent.mkdir(parents=True, exist_ok=True)
    malformed.write_bytes(payload)

    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    repository = snapshot.path.parent
    snapshot_manifest = (snapshot.path / "manifest.json").read_bytes()
    output = repository / "bundle"
    with pytest.raises(expected_exception):
        prepare_input_bundle(input_dir, snapshot, output)

    assert not output.exists()
    assert (snapshot.path / "manifest.json").read_bytes() == snapshot_manifest
    assert git(repository, "rev-parse", "HEAD") == snapshot.input_commit
    assert not git(repository, "status", "--porcelain=v1", "--untracked-files=all")

    # Accept a malformed fixture by committing it into a valid bundle with a
    # matching manifest entry, so the explicit verifier is independently required
    # to exercise the same consumer.
    malformed.unlink()
    selection = write_input_bundle_from_snapshot(input_dir, snapshot)
    manifest_path = selection.path / "catalog-bundle.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = next(
        item for item in manifest["files"] if item["path"] == f"catalog/{relative}"
    )
    assert entry["present"] is False
    accepted_payload = selection.path / "catalog" / relative
    accepted_payload.parent.mkdir(parents=True, exist_ok=True)
    accepted_payload.write_bytes(payload)
    entry.update(
        present=True, size=len(payload), sha256=hashlib.sha256(payload).hexdigest()
    )
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    selection = repin_input_bundle(selection, "accept malformed small input")
    bundle_manifest = manifest_path.read_bytes()

    with pytest.raises(expected_exception):
        verify_input_bundle(selection)

    assert manifest_path.read_bytes() == bundle_manifest
    assert git(repository, "rev-parse", "HEAD") == selection.input_commit
    assert not git(repository, "status", "--porcelain=v1", "--untracked-files=all")
