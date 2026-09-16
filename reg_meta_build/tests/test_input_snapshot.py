"""Lossless-record and failure-safety tests for the input snapshot prototype."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import subprocess
from typing import TYPE_CHECKING
from zipfile import BadZipFile

import pytest
from _csv_fixtures import (
    hydrate_scb_values,
    omit_scb_snapshot_file,
    repin_input_bundle,
    repin_scb_snapshot,
    scb_interpretation_rows,
    scb_values_role,
    sparsify_scb_values,
    write_input_bundle,
    write_input_bundle_from_snapshot,
    write_scb_input,
    write_scb_snapshot,
)
from _lisa_fixtures import write_lisa_workbook
from reg_meta.errors import RegMetaError
from reg_meta_build._curation import repo_curation_path
from reg_meta_build.db import _open_scb_csv, _open_scb_csv_prepared, _open_scb_csv_raw
from reg_meta_build.input_snapshot import (
    LISA_BUNDLE_PATH,
    LISA_DATASET_ID,
    SCB_CSV_FILES,
    CatalogBundleSelection,
    LisaWorkbookSelection,
    ScbSnapshotSelection,
    SnapshotError,
    SnapshotFile,
    SnapshotMaterializationError,
    converter_source_commit,
    create_build_lock,
    load_manifest,
    measure_codec_sample,
    measure_git_history,
    open_input_bundle,
    open_scb_snapshot,
    prepare_input_bundle,
    prepare_snapshot,
    restore_snapshot,
    verify_build_lock,
    verify_input_bundle,
    verify_snapshot,
)
from reg_meta_build.source_cases import (
    HAMN_SIGNAL_CASE_FILE,
    load_hamn_signal_source_case,
)

from reg_meta_build import input_snapshot as snapshot_module

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


def _inventory(
    tmp_path: Path,
    source_dir: Path,
    *,
    extra: tuple[str, ...] = (),
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    archive = tmp_path / "retained-delivery.zip"
    archive.write_bytes(b"independently retained archive fixture")
    actual = {path.name for path in source_dir.glob("*.csv")}
    names = (*SCB_CSV_FILES, *extra)
    inventory = tmp_path / "inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "bundle_id": "scb-mikrometadata",
                "edition": "fixture-2026-09-14",
                "source_dir": str(source_dir),
                "files": [{"name": name, "required": name in actual} for name in names],
                "archives": [
                    {
                        "path": str(archive),
                        "locator": "offline/scb-fixture.zip",
                        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                        "members": sorted(actual.intersection(names)),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return inventory


def _logical_rows(path: Path) -> list[list[bytes | None]]:
    with path.open("rb") as raw:
        text = io.TextIOWrapper(raw, encoding="latin-1", newline="")
        reader = csv.reader(
            text,
            delimiter="|",
            quotechar='"',
            quoting=csv.QUOTE_NOTNULL,
            strict=True,
        )
        return [
            [None if value is None else value.encode("latin-1") for value in row]
            for row in reader
        ]


def _git(repo: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return process.stdout.strip()


def _git_index_evidence(repo: Path) -> tuple[bytes, bytes]:
    index_path = repo / _git(repo, "rev-parse", "--git-path", "index")
    index_bytes = index_path.read_bytes()
    flags = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "core.sparseCheckout=false",
            "ls-files",
            "--sparse",
            "-v",
            "--stage",
            "-z",
        ],
        check=True,
        capture_output=True,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    ).stdout
    assert index_path.read_bytes() == index_bytes
    return index_bytes, flags


def _builder_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    repo = tmp_path / "builder-repo"
    module = repo / "reg_meta_build" / "src" / "reg_meta_build" / "input_snapshot.py"
    sources = (
        module,
        module.with_name("cli.py"),
        repo / "scripts" / "prototype_scb_inputs.py",
        repo / "scripts" / "build_db_watch.py",
        repo / "uv.lock",
    )
    for source in sources:
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"fixture: {source.name}\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "builder")
    monkeypatch.setattr(snapshot_module, "__file__", str(module))
    return repo


def _write_catalog_db(
    path: Path,
    *,
    import_date: str,
    input_dir: str,
    source_checksums: str = "same normalized inputs",
    fact: str = "same catalog fact",
) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            "CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE catalog_fact (id INTEGER PRIMARY KEY, value TEXT NOT NULL);"
        )
        conn.executemany(
            "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
            (
                ("import_date", import_date),
                ("input_dir", input_dir),
                ("source_checksums", source_checksums),
            ),
        )
        conn.execute("INSERT INTO catalog_fact VALUES (1, ?)", (fact,))
        conn.commit()
    finally:
        conn.close()


def _update_db(path: Path, sql: str, value: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(sql, (value,))
        conn.commit()
    finally:
        conn.close()


def test_snapshot_round_trip_preserves_raw_fields_occurrences_and_order(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    extra = source_dir / "UnknownFields.csv"
    extra.write_bytes(
        b"dup|dup||odd|\x8f\r\n"
        b'001|0||""|head\r\n'
        b'001|0||""|head\r\n'
        b'\x8f|\xc5|NULL|"line1\r\nline2|quoted ""text"""|value\r\n'
        b' x |000|""||\xc5\r\n'
    )
    vardemangder = source_dir / "Vardemangder.csv"
    vardemangder.write_bytes(
        vardemangder.read_bytes()
        + b'X|X|007|" padded "|999999|\r\n'
        + b'X|X|007|" padded "|999999|0\r\n'
        + b'X|X|007|" padded "|999999|000\r\n'
        + b'X|X|007|" padded "|999999|\r\n'
    )
    inventory = _inventory(tmp_path, source_dir, extra=(extra.name,))

    first = tmp_path / "snapshot-a"
    second = tmp_path / "snapshot-b"
    stats = prepare_snapshot(inventory, first, converter_commit="a" * 40)
    prepare_snapshot(inventory, second, converter_commit="a" * 40)

    assert stats.raw_bytes == sum(
        path.stat().st_size for path in source_dir.glob("*.csv")
    )
    assert stats.records > 0
    assert stats.codec_sample
    assert {
        path.relative_to(first).as_posix(): path.read_bytes()
        for path in first.rglob("*")
        if path.is_file()
    } == {
        path.relative_to(second).as_posix(): path.read_bytes()
        for path in second.rglob("*")
        if path.is_file()
    }
    identical_history = measure_git_history(first, second)
    assert identical_history["changed_files"] == 0
    assert identical_history["incremental_packed_object_bytes"] == 0

    manifest = verify_snapshot(first)
    manifest_text = (first / "manifest.json").read_text(encoding="utf-8")
    assert str(tmp_path) not in manifest_text
    assert "source_dir" not in manifest_text
    assert "timestamp" not in manifest_text
    unknown = next(item for item in manifest.files if item.name == extra.name)
    assert unknown.header == (
        ("t", "dup"),
        ("t", "dup"),
        ("n",),
        ("t", "odd"),
        ("b", "jw=="),
    )
    assert unknown.groups == ()
    assert unknown.inline_positions == (0, 1, 2, 3, 4)
    assert unknown.raw_sha256 == hashlib.sha256(extra.read_bytes()).hexdigest()
    registerinformation = next(
        item for item in manifest.files if item.name == "Registerinformation.csv"
    )
    assert registerinformation.column_count == 36
    assert registerinformation.inline_positions == (31, 32, 33, 34, 35)

    restored = tmp_path / "restored"
    restore_snapshot(first, restored)
    for source in source_dir.glob("*.csv"):
        assert _logical_rows(restored / source.name) == _logical_rows(source)
    assert _logical_rows(restored / extra.name) == [
        [b"dup", b"dup", None, b"odd", b"\x8f"],
        [b"001", b"0", None, b"", b"head"],
        [b"001", b"0", None, b"", b"head"],
        [b"\x8f", b"\xc5", b"NULL", b'line1\r\nline2|quoted "text"', b"value"],
        [b" x ", b"000", b"", None, b"\xc5"],
    ]
    assert _logical_rows(restored / "Vardemangder.csv")[-4:] == [
        [b"X", b"X", b"007", b" padded ", b"999999", None],
        [b"X", b"X", b"007", b" padded ", b"999999", b"0"],
        [b"X", b"X", b"007", b" padded ", b"999999", b"000"],
        [b"X", b"X", b"007", b" padded ", b"999999", None],
    ]
    assert restored.joinpath(extra.name).read_bytes() != extra.read_bytes()


def test_selected_snapshot_stream_preserves_lossless_cells_before_scb_decoding(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    values = source_dir / "Vardemangder.csv"
    values.write_bytes(
        values.read_bytes()
        + b"X|X|007|\x8f|999999|\r\n"
        + b'X|X|007|""|999999|0\r\n'
        + b"X|X|007|NULL|999999|000\r\n"
        + b"X|X|007|\x8f|999999|\r\n"
    )
    selection = write_scb_snapshot(tmp_path / "snapshot-fixture", source_dir)
    reader = open_scb_snapshot(selection)

    with reader.open_csv("Vardemangder.csv") as (_header, rows):
        assert list(rows)[-4:] == [
            ["X", "X", "007", "\x8f", "999999", None],
            ["X", "X", "007", "", "999999", "0"],
            ["X", "X", "007", "NULL", "999999", "000"],
            ["X", "X", "007", "\x8f", "999999", None],
        ]

    synthetic_path = tmp_path / "not-restored" / "Vardemangder.csv"
    with _open_scb_csv_raw(synthetic_path, reader) as (_header, rows):
        assert list(rows)[-1][1] == ["X", "X", "007", "\x8f", "999999", ""]
    with _open_scb_csv(synthetic_path, reader) as (_header, rows):
        assert list(rows)[-1][1]["Värdebenämning"] == "Å"
    with _open_scb_csv_prepared(synthetic_path, reader) as (_header, rows):
        prepared = list(rows)[-4:]
    assert prepared[0][1]["ItemId"] == (False, None, "")
    assert prepared[1][1]["Värdebenämning"] == (True, "", "")
    assert prepared[2][1]["Värdebenämning"] == (True, "NULL", "NULL")
    assert prepared[3][1]["Värdebenämning"] == (True, "\x8f", "Å")


def test_registerinformation_raw_and_prepared_readers_share_text_but_not_presence(
    tmp_path: Path,
) -> None:
    scb_dir = write_scb_input(
        tmp_path / "source", registerinformation_rows=scb_interpretation_rows()
    )
    reader = open_scb_snapshot(write_scb_snapshot(tmp_path / "accepted", scb_dir))

    with _open_scb_csv(scb_dir / "Registerinformation.csv") as (_header, rows):
        raw_rows = [row for _row_number, row in rows]
    with _open_scb_csv(tmp_path / "unused" / "Registerinformation.csv", reader) as (
        _header,
        rows,
    ):
        snapshot_rows = [row for _row_number, row in rows]
    with _open_scb_csv_prepared(
        tmp_path / "unused" / "Registerinformation.csv", reader
    ) as (_header, rows):
        prepared_rows = [row for _row_number, row in rows]

    assert snapshot_rows == raw_rows
    assert prepared_rows[0]["Populationkommentar"] == (False, None, "")
    assert prepared_rows[3]["Populationkommentar"] == (True, "", "")
    assert prepared_rows[0]["Datatyp"] == (True, " numeric ", " numeric ")
    assert prepared_rows[0]["Datalängd"] == (True, "0", "0")


def test_selected_snapshot_requires_exact_commit_manifest_and_clean_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    selection = write_scb_snapshot(tmp_path / "snapshot-fixture", source_dir)

    def exhaustive_use(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "ordinary selection must not perform exhaustive verification"
        )

    monkeypatch.setattr(snapshot_module, "_verify_committed_snapshot", exhaustive_use)
    monkeypatch.setattr(snapshot_module, "_open_snapshot_rows", exhaustive_use)
    monkeypatch.setattr(snapshot_module, "_update_record_hash", exhaustive_use)
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


def test_catalog_bundle_captures_complete_small_inventory_and_selects_quickly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    auxiliary = input_dir / "SCB" / "Tabelldefinitioner.sql"
    auxiliary.write_bytes(b"-- exact source fact\r\nSELECT 1;\r\n")
    selection = write_input_bundle(tmp_path / "accepted", input_dir)

    def exhaustive_use(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("ordinary bundle selection must stay quick")

    monkeypatch.setattr(snapshot_module, "verify_snapshot", exhaustive_use)
    monkeypatch.setattr(snapshot_module, "_verify_committed_snapshot", exhaustive_use)
    monkeypatch.setattr(snapshot_module, "_file_sha256", exhaustive_use)
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


def test_sparse_value_role_preserves_declared_source_and_requires_hydration_for_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_dir = tmp_path / "source"
    scb_dir = write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    complete = open_input_bundle(selection)
    source_hash = complete.snapshot.raw_sha256("Vardemangder.csv")
    role = scb_values_role(selection)
    cold_paths = sorted((complete.snapshot.root / "files/Vardemangder.csv").rglob("*"))
    committed_cold = {
        path
        for path in _git(
            selection.path.parent, "ls-tree", "-r", "--name-only", "HEAD"
        ).splitlines()
        if path.startswith(f"{role}/")
    }
    assert committed_cold

    saved_directories = sparsify_scb_values(selection)
    assert all(not path.exists() for path in cold_paths)

    def exhaustive_use(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("warm sparse selection must not perform exhaustive proof")

    with monkeypatch.context() as quick_only:
        quick_only.setattr(
            snapshot_module, "_verify_committed_snapshot", exhaustive_use
        )
        quick_only.setattr(snapshot_module, "_open_snapshot_rows", exhaustive_use)
        bundle = open_input_bundle(selection)
    assert bundle.snapshot.has_file("Vardemangder.csv")
    assert bundle.snapshot.raw_sha256("Vardemangder.csv") == source_hash
    assert (
        source_hash
        == hashlib.sha256((scb_dir / "Vardemangder.csv").read_bytes()).hexdigest()
    )
    assert {
        path
        for path in _git(
            selection.path.parent, "ls-tree", "-r", "--name-only", "HEAD"
        ).splitlines()
        if path.startswith(f"{role}/")
    } == committed_cold

    for action in (
        bundle.snapshot.open_vardemangder,
        lambda: verify_input_bundle(selection),
        lambda: verify_snapshot(bundle.snapshot.root),
        lambda: restore_snapshot(bundle.snapshot.root, tmp_path / "restored"),
    ):
        with pytest.raises(SnapshotMaterializationError) as exc_info:
            action()
        assert str(selection.path.parent) in str(exc_info.value)
        assert selection.input_commit in str(exc_info.value)
        assert role in exc_info.value.hydration_action
    assert not (tmp_path / "restored").exists()

    hydrate_scb_values(selection)
    hydrated = open_input_bundle(selection)
    hydrated.snapshot.require_vardemangder_materialized()
    assert verify_snapshot(hydrated.snapshot.root) == hydrated.snapshot.manifest
    assert _git(selection.path.parent, "rev-parse", "HEAD") == selection.input_commit
    assert not _git(selection.path.parent, "status", "--porcelain=v1")

    restored_directories = sparsify_scb_values(selection)
    assert restored_directories == saved_directories
    open_input_bundle(selection)
    assert _git(selection.path.parent, "rev-parse", "HEAD") == selection.input_commit
    assert not _git(selection.path.parent, "status", "--porcelain=v1")


@pytest.mark.parametrize("index_representation", ("full", "preserved-condensed"))
def test_legitimate_sparse_reads_preserve_index_and_config_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    index_representation: str,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    complete = open_input_bundle(selection)
    snapshot_selection = ScbSnapshotSelection(
        path=complete.snapshot.root,
        input_commit=selection.input_commit,
        manifest_sha256=complete.manifest.scb_manifest_sha256,
    )
    directories = sparsify_scb_values(selection)
    repo = selection.path.parent
    index_path = repo / _git(repo, "rev-parse", "--git-path", "index")
    full_index_bytes = index_path.read_bytes()
    if index_representation == "preserved-condensed":
        subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "sparse-checkout",
                "set",
                "--cone",
                "--sparse-index",
                "--stdin",
            ],
            input="".join(f"{path}\n" for path in directories),
            check=True,
            text=True,
        )
        condensed_index_bytes = index_path.read_bytes()
        assert condensed_index_bytes != full_index_bytes
        assert _git(repo, "config", "--worktree", "--get", "index.sparse") == "true"
        _git(repo, "config", "--worktree", "index.sparse", "false")
        assert index_path.read_bytes() == condensed_index_bytes
    assert _git(repo, "config", "--worktree", "--get", "index.sparse") == "false"

    monkeypatch.delenv("GIT_OPTIONAL_LOCKS", raising=False)
    assert "GIT_OPTIONAL_LOCKS" not in os.environ
    config_path = repo / _git(repo, "rev-parse", "--git-path", "config.worktree")
    before = _git_index_evidence(repo)
    config_bytes = config_path.read_bytes()
    assert any(
        entry.startswith(b"S ") and b"Vardemangder.csv/" in entry
        for entry in before[1].split(b"\0")
    )

    assert not open_scb_snapshot(snapshot_selection).vardemangder_materialized
    assert _git_index_evidence(repo) == before
    assert config_path.read_bytes() == config_bytes
    assert not open_input_bundle(selection).snapshot.vardemangder_materialized
    assert _git_index_evidence(repo) == before
    assert config_path.read_bytes() == config_bytes
    with pytest.raises(SnapshotMaterializationError):
        verify_snapshot(snapshot_selection.path)
    assert _git_index_evidence(repo) == before
    assert config_path.read_bytes() == config_bytes


@pytest.mark.parametrize("boundary", ("snapshot", "bundle", "proof"))
@pytest.mark.parametrize(
    "invalid_state", ("warm-hot-skip", "hydrated-hot-skip", "cold-replacements")
)
def test_sparse_boundaries_reject_without_normalizing_raw_index_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    invalid_state: str,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    complete = open_input_bundle(selection)
    repo = selection.path.parent
    snapshot_selection = ScbSnapshotSelection(
        path=complete.snapshot.root,
        input_commit=selection.input_commit,
        manifest_sha256=complete.manifest.scb_manifest_sha256,
    )
    snapshot_relative = complete.snapshot.root.relative_to(repo).as_posix()
    cold = sorted(
        f"{snapshot_relative}/{path}"
        for path in snapshot_module._vardemangder_normalized_files(
            complete.snapshot.manifest
        )
    )
    cold_payloads = {path: (repo / path).read_bytes() for path in cold}
    hot_item = next(
        item
        for item in complete.snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    hot = f"{snapshot_relative}/{hot_item.records[0].path}"
    sparsify_scb_values(selection)

    if invalid_state == "hydrated-hot-skip":
        hydrate_scb_values(selection)
    if invalid_state.endswith("hot-skip"):
        _git(repo, "update-index", "--skip-worktree", hot)
        assert (repo / hot).is_file()
        expected = "outside the SCB Vardemangder role"
    else:
        for relative, payload in cold_payloads.items():
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        expected = "marked omitted by the accepted Git index"

    monkeypatch.delenv("GIT_OPTIONAL_LOCKS", raising=False)
    assert "GIT_OPTIONAL_LOCKS" not in os.environ
    before = _git_index_evidence(repo)
    expected_skips = (hot,) if invalid_state.endswith("hot-skip") else tuple(cold)
    assert all(
        any(
            entry.startswith(b"S ") and entry.endswith(b"\t" + relative.encode("utf-8"))
            for entry in before[1].split(b"\0")
        )
        for relative in expected_skips
    )
    if invalid_state.endswith("hot-skip"):

        def reject_status(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("raw index rejection must precede Git status")

        monkeypatch.setattr(snapshot_module, "clean_git_commit", reject_status)
    actions = {
        "snapshot": lambda: open_scb_snapshot(snapshot_selection),
        "bundle": lambda: open_input_bundle(selection),
        "proof": lambda: verify_snapshot(snapshot_selection.path),
    }
    with pytest.raises(SnapshotError, match=expected):
        actions[boundary]()
    assert _git_index_evidence(repo) == before


def test_sparse_bundle_verifier_cli_reports_materialization_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from reg_meta.errors import EXIT_CONFIG

    from reg_meta_build import cli as cli_module

    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    sparsify_scb_values(selection)

    exit_code = cli_module.run(
        [
            "verify-input-bundle",
            "--input-bundle",
            str(selection.path),
            "--input-commit",
            selection.input_commit,
            "--input-manifest-sha256",
            selection.manifest_sha256,
        ]
    )

    assert exit_code == EXIT_CONFIG
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "scb_snapshot_materialization_required"
    assert selection.input_commit in error["message"]
    assert "sparse-checkout add --stdin" in error["remediation"]


@pytest.mark.parametrize(
    "invalid_layout",
    (
        "partial-cold",
        "missing-hot",
        "assume-unchanged-hot",
        "loose-cold-replacement",
        "complete-loose-cold-replacement",
        "non-cone",
        "sparse-index",
        "missing-sibling",
    ),
)
def test_sparse_value_role_rejects_incorrect_index_and_rule_state(
    tmp_path: Path, invalid_layout: str
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    selection = write_input_bundle(tmp_path / "accepted", input_dir)
    repo = selection.path.parent
    if invalid_layout == "missing-sibling":
        support = repo / "support" / "evidence.txt"
        support.parent.mkdir()
        support.write_text("accepted support\n", encoding="utf-8")
        _git(repo, "add", ".")
        _git(repo, "commit", "-q", "-m", "accepted support")
        selection = CatalogBundleSelection(
            selection.path,
            _git(repo, "rev-parse", "HEAD"),
            selection.manifest_sha256,
        )
    bundle = open_input_bundle(selection)
    snapshot_relative = bundle.snapshot.root.relative_to(repo).as_posix()
    manifest = bundle.snapshot.manifest
    cold = sorted(
        f"{snapshot_relative}/{path}"
        for path in snapshot_module._vardemangder_normalized_files(manifest)
    )
    hot_item = next(
        item for item in manifest.files if item.name == "Registerinformation.csv"
    )
    hot = f"{snapshot_relative}/{hot_item.records[0].path}"

    if invalid_layout == "partial-cold":
        _git(repo, "update-index", "--skip-worktree", cold[0])
        (repo / cold[0]).unlink()
        expected = "complete SCB Vardemangder role"
    elif invalid_layout == "missing-hot":
        _git(repo, "update-index", "--skip-worktree", hot)
        (repo / hot).unlink()
        expected = "outside the SCB Vardemangder role"
    elif invalid_layout == "assume-unchanged-hot":
        _git(repo, "update-index", "--assume-unchanged", hot)
        path = repo / hot
        payload = bytearray(path.read_bytes())
        payload[0] = ord("A") if payload[0] != ord("A") else ord("B")
        path.write_bytes(payload)
        assert not _git(repo, "status", "--porcelain=v1")
        expected = "assume-unchanged"
    else:
        cold_path = repo / cold[0]
        cold_payloads = {path: (repo / path).read_bytes() for path in cold}
        directories = sparsify_scb_values(selection)
        if invalid_layout == "loose-cold-replacement":
            cold_path.parent.mkdir(parents=True, exist_ok=True)
            cold_path.write_bytes(cold_payloads[cold[0]])
            expected = "marked omitted by the accepted Git index"
        elif invalid_layout == "complete-loose-cold-replacement":
            for relative, payload in cold_payloads.items():
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(payload)
            expected = "marked omitted by the accepted Git index"
        elif invalid_layout == "non-cone":
            _git(repo, "config", "--worktree", "core.sparseCheckoutCone", "false")
            expected = "cone mode"
        elif invalid_layout == "sparse-index":
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "sparse-checkout",
                    "set",
                    "--cone",
                    "--sparse-index",
                    "--stdin",
                ],
                input="".join(f"{path}\n" for path in directories),
                check=True,
                text=True,
            )
            expected = "normal full Git index"
        else:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "sparse-checkout",
                    "set",
                    "--cone",
                    "--no-sparse-index",
                    "--stdin",
                ],
                input="".join(
                    f"{path}\n"
                    for path in directories
                    if not path.startswith("support")
                ),
                check=True,
                text=True,
            )
            expected = "outside the SCB Vardemangder role"

    with pytest.raises(SnapshotError, match=expected):
        open_input_bundle(selection)


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
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "unlisted input")
    changed_commit = _git(repo, "rev-parse", "HEAD")
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
    auxiliary.write_text("CREATE TABLE first;\n", encoding="utf-8")
    first = write_input_bundle(tmp_path / "accepted-a", input_dir)

    auxiliary.write_text("CREATE TABLE second;\n", encoding="utf-8")
    second = write_input_bundle(tmp_path / "accepted-b", input_dir)

    assert first.manifest_sha256 != second.manifest_sha256


def test_catalog_bundle_captures_and_validates_authored_hamn_case(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    curation = tmp_path / "curation"
    curation.mkdir()
    source = repo_curation_path(HAMN_SIGNAL_CASE_FILE)
    assert source is not None
    authored_bytes = source.read_bytes()
    (curation / HAMN_SIGNAL_CASE_FILE).write_bytes(authored_bytes)

    selection = write_input_bundle(
        tmp_path / "accepted", input_dir, curation_dir=curation
    )
    bundle = open_input_bundle(selection)
    item = next(
        item
        for item in bundle.manifest.files
        if item.path == f"curation/{HAMN_SIGNAL_CASE_FILE}"
    )

    assert item.present is True
    assert item.sha256 == hashlib.sha256(authored_bytes).hexdigest()
    assert (bundle.curation_dir / HAMN_SIGNAL_CASE_FILE).read_bytes() == authored_bytes
    assert (
        load_hamn_signal_source_case(
            bundle.curation_dir / HAMN_SIGNAL_CASE_FILE
        ).authored_case.case_id
        == "hamn-signal-unresolved-length"
    )
    assert verify_input_bundle(selection).schema_version == 2


def test_catalog_bundle_manifest_changes_with_authored_hamn_case_bytes(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    curation = tmp_path / "curation"
    curation.mkdir()
    source = repo_curation_path(HAMN_SIGNAL_CASE_FILE)
    assert source is not None
    case_path = curation / HAMN_SIGNAL_CASE_FILE
    case_path.write_bytes(source.read_bytes())
    first = write_input_bundle(
        tmp_path / "accepted-a", input_dir, curation_dir=curation
    )

    payload = json.loads(case_path.read_text(encoding="utf-8"))
    payload["proposal"]["rationale"] += " Reviewed again."
    case_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    second = write_input_bundle(
        tmp_path / "accepted-b", input_dir, curation_dir=curation
    )

    assert first.manifest_sha256 != second.manifest_sha256


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
    curation = tmp_path / "curation"
    slugs = tmp_path / "slugs"
    curation.mkdir()
    slugs.mkdir()
    output = snapshot.path.parent / "bundle"

    with pytest.raises(SnapshotError, match=expected):
        prepare_input_bundle(
            input_dir,
            curation,
            slugs,
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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    workbook = write_lisa_workbook(input_dir / "docs" / "lisa.xlsx")
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    sparsify_scb_values(snapshot)
    curation = tmp_path / "curation"
    slugs = tmp_path / "slugs"
    curation.mkdir()
    slugs.mkdir()

    def reject_exhaustive(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("LISA capture must not exhaustively verify the snapshot")

    def reject_values(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("LISA capture must not open Vardemangder")

    monkeypatch.setattr(snapshot_module, "verify_snapshot", reject_exhaustive)
    monkeypatch.setattr(
        snapshot_module.ScbSnapshotReader, "open_vardemangder", reject_values
    )
    output = snapshot.path.parent / "bundle"
    prepare_input_bundle(
        input_dir,
        curation,
        slugs,
        snapshot,
        output,
        lisa_workbook=LisaWorkbookSelection(
            path=workbook,
            upstream_revision="2024-2025",
            sha256=hashlib.sha256(workbook.read_bytes()).hexdigest(),
        ),
    )

    assert (output / LISA_BUNDLE_PATH).read_bytes() == workbook.read_bytes()


def test_catalog_bundle_preparation_skips_exhaustive_snapshot_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    auxiliary = input_dir / "SCB" / "Tabelldefinitioner.sql"
    auxiliary.write_text("CREATE TABLE auxiliary;\n", encoding="utf-8")
    curation = tmp_path / "curation"
    slugs = tmp_path / "slugs"
    curation.mkdir()
    slugs.mkdir()

    def exhaustive_use(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("auxiliary-only preparation must reuse the accepted proof")

    monkeypatch.setattr(snapshot_module, "verify_snapshot", exhaustive_use)
    sparsify_scb_values(snapshot)
    output = snapshot.path.parent / "bundle"
    prepare_input_bundle(input_dir, curation, slugs, snapshot, output)

    assert (
        output / "catalog" / "SCB" / "Tabelldefinitioner.sql"
    ).read_bytes() == auxiliary.read_bytes()


def test_catalog_bundle_preparation_rejects_escaping_classification_before_copy(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "authoring" / "source"
    write_scb_input(input_dir)
    escaped_source = tmp_path / "guard.csv"
    escaped_source.write_text("source bytes\n", encoding="utf-8")
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    accepted_guard = snapshot.path.parent / "guard.csv"
    accepted_guard.write_text("accepted bytes\n", encoding="utf-8")
    snapshot = repin_scb_snapshot(snapshot, "accepted guard")
    accepted_bytes = accepted_guard.read_bytes()

    curation = tmp_path / "curation"
    curation.mkdir()
    (curation / "classifications.toml").write_text(
        '[[classification]]\nshort_name = "TEST"\nname = "Test"\n'
        'valid_codes_file = "../../../guard.csv"\n',
        encoding="utf-8",
    )
    slugs = tmp_path / "slugs"
    slugs.mkdir()
    output = snapshot.path.parent / "bundle"

    with pytest.raises(RegMetaError) as exc_info:
        prepare_input_bundle(input_dir, curation, slugs, snapshot, output)

    assert exc_info.value.code == "classification_csv_invalid"
    assert accepted_guard.read_bytes() == accepted_bytes
    assert _git(snapshot.path.parent, "rev-parse", "HEAD") == snapshot.input_commit
    assert not _git(
        snapshot.path.parent, "status", "--porcelain=v1", "--untracked-files=all"
    )
    assert not output.exists()


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
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    curation = tmp_path / "curation"
    slugs = tmp_path / "slugs"
    curation.mkdir()
    slugs.mkdir()
    output = snapshot.path.parent / "bundle"
    validate_contract = snapshot_module._validate_bundle_contract

    def introduce_source(staging: Path) -> None:
        validate_contract(staging)
        introduced = input_dir / introduced_relative
        introduced.parent.mkdir(parents=True, exist_ok=True)
        introduced.write_bytes(b"introduced during validation")

    monkeypatch.setattr(snapshot_module, "_validate_bundle_contract", introduce_source)
    with pytest.raises(SnapshotError, match="changed during bundle preparation"):
        prepare_input_bundle(input_dir, curation, slugs, snapshot, output)

    assert not output.exists()
    assert not _git(
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
            tmp_path / "empty-curation",
            tmp_path / "empty-slugs",
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


def test_catalog_bundle_preparation_rejects_invalid_consumed_input(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    provider_dir = input_dir / "Folkhalsomyndigheten"
    provider_dir.mkdir()
    (provider_dir / "fohm.toml").write_text("not = [valid", encoding="utf-8")
    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    curation = tmp_path / "curation"
    slugs = tmp_path / "slugs"
    curation.mkdir()
    slugs.mkdir()

    with pytest.raises(RegMetaError) as exc_info:
        prepare_input_bundle(
            input_dir, curation, slugs, snapshot, snapshot.path.parent / "bundle"
        )
    assert exc_info.value.code == "curated_toml_invalid"
    assert not (snapshot.path.parent / "bundle").exists()


@pytest.mark.parametrize(
    ("source_root", "relative", "payload", "expected_exception"),
    (
        ("curation", "lineage.toml", b"not = [valid", RegMetaError),
        ("curation", HAMN_SIGNAL_CASE_FILE, b"{}", SnapshotError),
        ("input", "SCB/ID-kolumner.xlsx", b"not a zip", BadZipFile),
        ("input", "SCB/Tabelldefinitioner.sql", b"\x81", UnicodeDecodeError),
    ),
)
def test_bundle_prepare_and_verify_reject_invalid_small_consumed_contracts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_root: str,
    relative: str,
    payload: bytes,
    expected_exception: type[Exception],
) -> None:
    input_dir = tmp_path / "source"
    write_scb_input(input_dir)
    curation = tmp_path / "curation"
    slugs = tmp_path / "slugs"
    curation.mkdir()
    slugs.mkdir()
    root = curation if source_root == "curation" else input_dir
    malformed = root / relative
    malformed.parent.mkdir(parents=True, exist_ok=True)
    malformed.write_bytes(payload)

    snapshot = write_scb_snapshot(tmp_path / "accepted", input_dir / "SCB")
    repository = snapshot.path.parent
    snapshot_manifest = (snapshot.path / "manifest.json").read_bytes()
    output = repository / "bundle"
    with pytest.raises(expected_exception):
        prepare_input_bundle(input_dir, curation, slugs, snapshot, output)

    assert not output.exists()
    assert (snapshot.path / "manifest.json").read_bytes() == snapshot_manifest
    assert _git(repository, "rev-parse", "HEAD") == snapshot.input_commit
    assert not _git(repository, "status", "--porcelain=v1", "--untracked-files=all")

    # Build an accepted malformed fixture without the contract gate so the
    # explicit verifier is independently required to exercise the same consumer.
    with monkeypatch.context() as bypass:
        bypass.setattr(snapshot_module, "_validate_bundle_contract", lambda _root: None)
        selection = write_input_bundle_from_snapshot(
            input_dir,
            snapshot,
            curation_dir=curation,
            slug_dir=slugs,
        )
    bundle_manifest = (selection.path / "catalog-bundle.json").read_bytes()

    with pytest.raises(expected_exception):
        verify_input_bundle(selection)

    assert (selection.path / "catalog-bundle.json").read_bytes() == bundle_manifest
    assert _git(repository, "rev-parse", "HEAD") == selection.input_commit
    assert not _git(repository, "status", "--porcelain=v1", "--untracked-files=all")


def test_prepare_exhaustively_verifies_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    output = tmp_path / "snapshot"
    exhaustive_verify = snapshot_module.verify_snapshot

    def damage_then_verify(staging: Path):
        record = next(staging.glob("files/Vardemangder.csv/records/*.tsv"))
        payload = bytearray(record.read_bytes())
        payload[0] = ord("A") if payload[0] != ord("A") else ord("B")
        record.write_bytes(payload)
        return exhaustive_verify(staging)

    monkeypatch.setattr(snapshot_module, "verify_snapshot", damage_then_verify)
    with pytest.raises(SnapshotError, match="references missing value_set payload"):
        prepare_snapshot(
            _inventory(tmp_path, source_dir),
            output,
            converter_commit="b" * 40,
        )

    assert not output.exists()
    assert not list(tmp_path.glob(".snapshot.candidate-*"))


def test_snapshot_rejects_missing_dictionary_reference_even_with_updated_file_hash(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    snapshot = tmp_path / "snapshot"
    prepare_snapshot(
        _inventory(tmp_path, source_dir), snapshot, converter_commit="b" * 40
    )
    manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    value_file = next(
        item for item in manifest["files"] if item["name"] == "Vardemangder.csv"
    )
    record = value_file["records"][0]
    record_path = snapshot / record["path"]
    lines = record_path.read_text(encoding="utf-8").splitlines()
    fields = lines[0].split("\t")
    fields[0] = "A" * 22
    lines[0] = "\t".join(fields)
    record_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    record["size"] = record_path.stat().st_size
    record["sha256"] = hashlib.sha256(record_path.read_bytes()).hexdigest()
    (snapshot / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SnapshotError, match="references missing value_set payload"):
        verify_snapshot(snapshot)


def test_snapshot_fails_on_content_key_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    monkeypatch.setattr(snapshot_module, "_payload_key", lambda *_args: "A" * 22)
    with pytest.raises(SnapshotError, match="content-key collision"):
        prepare_snapshot(
            _inventory(tmp_path, source_dir),
            tmp_path / "snapshot",
            converter_commit="b" * 40,
        )


@pytest.mark.parametrize(
    "mutation",
    ("earlier-changed", "later-replaced", "optional-appeared", "optional-disappeared"),
)
def test_snapshot_rechecks_complete_source_bundle_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    inventory_path = _inventory(tmp_path, source_dir)
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if mutation == "optional-appeared":
        inventory["files"].append({"name": "Optional.csv", "required": False})
        inventory["archives"][0]["members"].append("Optional.csv")
    elif mutation == "optional-disappeared":
        optional = next(
            item for item in inventory["files"] if item["name"] == "Identifierare.csv"
        )
        optional["required"] = False
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")

    original_prepare_file = snapshot_module._prepare_file
    mutated = False

    def prepare_then_mutate(*args: object, **kwargs: object) -> SnapshotFile:
        nonlocal mutated
        prepared = original_prepare_file(*args, **kwargs)
        if mutated:
            return prepared
        mutated = True
        if mutation == "earlier-changed":
            with (source_dir / "Registerinformation.csv").open("ab") as handle:
                handle.write(b"changed after conversion\r\n")
        elif mutation == "later-replaced":
            later = source_dir / "Timeseries.csv"
            later_stat = later.stat()
            replacement = source_dir / "Timeseries.replacement"
            replacement.write_bytes(later.read_bytes())
            os.utime(
                replacement,
                ns=(later_stat.st_atime_ns, later_stat.st_mtime_ns),
            )
            replacement.replace(later)
        elif mutation == "optional-appeared":
            (source_dir / "Optional.csv").write_bytes(b"field\r\nvalue\r\n")
        else:
            (source_dir / "Identifierare.csv").unlink()
        return prepared

    monkeypatch.setattr(snapshot_module, "_prepare_file", prepare_then_mutate)
    output = tmp_path / "candidate"

    with pytest.raises(
        SnapshotError, match=r"source (?:bundle|CSV) changed during conversion"
    ):
        prepare_snapshot(inventory_path, output, converter_commit="b" * 40)

    assert not output.exists()
    assert not list(tmp_path.glob(".candidate.candidate-*"))


def test_codec_measurement_sqlite_control_covers_all_normalized_tables(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    measurement = measure_codec_sample(
        _inventory(tmp_path, source_dir),
        limits={"Registerinformation.csv": 2, "Vardemangder.csv": 3},
        codec_sample_lines=10,
    )
    assert measurement["sample_records"]["Registerinformation.csv"] == 2
    assert measurement["sample_records"]["Vardemangder.csv"] == 3
    control = measurement["sqlite_normalized_tables_control"]
    expected_table_rows = {
        f"{name}:records": records
        for name, records in measurement["sample_records"].items()
    }
    expected_field_counts: dict[str, int] = {}
    for name in measurement["sample_records"]:
        with snapshot_module.open_lossless_csv(source_dir / name) as (header, _rows):
            groups, inline = snapshot_module._group_layout(name, header)
        expected_field_counts[f"{name}:records"] = len(groups) + len(inline)
        for group_name, positions in groups:
            logical_name = f"{name}:{group_name}"
            expected_table_rows[logical_name] = measurement["codec_sample"][
                logical_name
            ]["lines"]
            expected_field_counts[logical_name] = len(positions) + 1

    assert control["bytes"] > 0
    assert control["table_rows"] == expected_table_rows
    assert control["table_field_counts"] == expected_field_counts
    assert control["logical_tables"] == len(expected_table_rows)
    assert control["logical_rows"] == sum(expected_table_rows.values())
    assert control["ordering"] == "zero-based occurrence ordinal per logical table"


def test_content_keys_stay_stable_and_git_measurement_reports_update_growth(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    inventory = _inventory(tmp_path, source_dir)
    codec_measurement = measure_codec_sample(
        inventory,
        limits={"Registerinformation.csv": 2, "Vardemangder.csv": 3},
        codec_sample_lines=10,
    )
    assert codec_measurement["population_records"]["Registerinformation.csv"] == 11
    assert codec_measurement["population_records"]["Vardemangder.csv"] == 14
    assert codec_measurement["sample_records"]["Registerinformation.csv"] == 2
    assert codec_measurement["sample_records"]["Vardemangder.csv"] == 3
    assert codec_measurement["sampling"] == {
        "method": "evenly-spaced-source-ordinals-v1",
        "source_passes": 2,
    }
    assert codec_measurement["codec_sample"]
    initial = tmp_path / "initial"
    prepare_snapshot(inventory, initial, converter_commit="9" * 40)
    initial_manifest = load_manifest(initial)
    initial_values = next(
        item for item in initial_manifest.files if item.name == "Vardemangder.csv"
    )
    initial_dictionary = next(
        group.dictionary.path
        for group in initial_values.groups
        if group.name == "value"
    )

    with (source_dir / "Vardemangder.csv").open("ab") as handle:
        handle.write(b"NEW|NEW|007|New label|999999|0007\r\n")
    update = tmp_path / "update"
    prepare_snapshot(inventory, update, converter_commit="9" * 40)
    update_manifest = load_manifest(update)
    update_values = next(
        item for item in update_manifest.files if item.name == "Vardemangder.csv"
    )
    update_dictionary = next(
        group.dictionary.path for group in update_values.groups if group.name == "value"
    )

    assert set(
        (initial / initial_dictionary).read_text(encoding="utf-8").splitlines()
    ) < set((update / update_dictionary).read_text(encoding="utf-8").splitlines())
    measurement = measure_git_history(initial, update)
    assert measurement["changed_files"] >= 3
    assert measurement["inserted_lines"] > 0
    assert (
        measurement["two_commit_packed_object_bytes"]
        >= measurement["initial_packed_object_bytes"]
    )
    assert measurement["git_gc"] == "git gc --prune=now"
    assert measurement["git_version"].startswith("git version ")
    assert measurement["git_pack_settings"] == {
        "core.compression": "9",
        "pack.compression": "9",
        "pack.depth": "50",
        "pack.threads": "1",
        "pack.useSparse": "true",
        "pack.window": "10",
        "pack.windowMemory": "0",
        "repack.useDeltaBaseOffset": "true",
        "repack.writeBitmaps": "false",
    }


def test_git_measurement_reports_effective_command_scope_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = tmp_path / "initial"
    update = tmp_path / "update"
    initial.mkdir()
    update.mkdir()
    (initial / "records.tsv").write_text("initial\n", encoding="utf-8")
    (update / "records.tsv").write_text("updated\n", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "pack.window")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "0")

    measurement = measure_git_history(initial, update)

    assert measurement["git_pack_settings"]["pack.window"] == "0"


def test_converter_commit_requires_same_clean_tracked_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "converter-repo"
    cli = repo / "scripts" / "prototype.py"
    converter = repo / "package" / "input_snapshot.py"
    cli.parent.mkdir(parents=True)
    converter.parent.mkdir(parents=True)
    cli.write_text("cli\n", encoding="utf-8")
    converter.write_text("converter\n", encoding="utf-8")
    (repo / ".gitignore").write_text("ignored.py\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "converter")
    commit = _git(repo, "rev-parse", "HEAD")
    monkeypatch.setattr(snapshot_module, "__file__", str(converter))

    assert converter_source_commit(cli) == commit

    converter.write_text("dirty converter\n", encoding="utf-8")
    with pytest.raises(SnapshotError, match="must be clean"):
        converter_source_commit(cli)
    converter.write_text("converter\n", encoding="utf-8")

    ignored = repo / "ignored.py"
    ignored.write_text("ignored\n", encoding="utf-8")
    with pytest.raises(SnapshotError, match="not a tracked HEAD blob"):
        converter_source_commit(ignored)

    other_repo = tmp_path / "unrelated-repo"
    unrelated_cli = other_repo / "prototype.py"
    other_repo.mkdir()
    unrelated_cli.write_text("unrelated\n", encoding="utf-8")
    _git(other_repo, "init", "-q")
    _git(other_repo, "config", "user.email", "test@example.invalid")
    _git(other_repo, "config", "user.name", "Test")
    _git(other_repo, "add", ".")
    _git(other_repo, "commit", "-q", "-m", "unrelated")
    with pytest.raises(SnapshotError, match="same Git checkout"):
        converter_source_commit(unrelated_cli)


def test_codec_sample_ordinals_span_the_whole_stream() -> None:
    assert tuple(snapshot_module._sample_ordinals(10, 3)) == (0, 4, 9)
    assert tuple(snapshot_module._sample_ordinals(10, 1)) == (5,)
    assert tuple(snapshot_module._sample_ordinals(3, 10)) == (0, 1, 2)

    consumed: list[int] = []

    def rows() -> Iterator[list[str]]:
        for value in range(10):
            consumed.append(value)
            yield [str(value)]

    assert list(snapshot_module._sample_rows(rows(), 10, 1)) == [["5"]]
    assert consumed == list(range(10))


def test_snapshot_rejects_unsupported_version_and_never_replaces_candidates(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    inventory = _inventory(tmp_path, source_dir)
    snapshot = tmp_path / "snapshot"
    prepare_snapshot(inventory, snapshot, converter_commit="c" * 40)
    original_manifest = (snapshot / "manifest.json").read_bytes()

    with pytest.raises(SnapshotError, match="already exists"):
        prepare_snapshot(inventory, snapshot, converter_commit="c" * 40)
    assert (snapshot / "manifest.json").read_bytes() == original_manifest

    manifest = json.loads(original_manifest)
    manifest["schema_version"] = 2
    (snapshot / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SnapshotError, match="unsupported snapshot schema version 2"):
        verify_snapshot(snapshot)

    malformed_dir = write_scb_input(tmp_path / "malformed-source")
    (malformed_dir / "Registerinformation.csv").write_bytes(b'header\r\n"unterminated')
    failed = tmp_path / "failed-candidate"
    with pytest.raises(SnapshotError, match="malformed CSV"):
        prepare_snapshot(
            _inventory(tmp_path / "malformed", malformed_dir),
            failed,
            converter_commit="c" * 40,
        )
    assert not failed.exists()


@pytest.mark.parametrize("unsafe_kind", ["absolute", "traversal"])
def test_snapshot_rejects_unsafe_source_name_without_writing_outside_restore(
    tmp_path: Path, unsafe_kind: str
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    snapshot = tmp_path / "snapshot"
    prepare_snapshot(
        _inventory(tmp_path, source_dir), snapshot, converter_commit="f" * 40
    )
    outside = tmp_path / "outside.csv"
    outside.write_bytes(b"must remain unchanged")
    unsafe_name = str(outside) if unsafe_kind == "absolute" else "../outside.csv"
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source_file = next(item for item in manifest["files"] if item["present"])
    original_name = source_file["name"]
    source_file["name"] = unsafe_name
    for archive in manifest["archives"]:
        archive["members"] = [
            unsafe_name if member == original_name else member
            for member in archive["members"]
        ]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(SnapshotError, match="plain .csv filename"):
        verify_snapshot(snapshot)
    restore_parent = tmp_path / "new-parent"
    with pytest.raises(SnapshotError, match="plain .csv filename"):
        restore_snapshot(snapshot, restore_parent / "restore")

    assert outside.read_bytes() == b"must remain unchanged"
    assert not restore_parent.exists()


def test_restore_rejects_targets_inside_snapshot_without_writing(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    snapshot = tmp_path / "snapshot"
    prepare_snapshot(
        _inventory(tmp_path, source_dir), snapshot, converter_commit="f" * 40
    )
    before = {
        path.relative_to(snapshot).as_posix(): path.read_bytes()
        for path in snapshot.rglob("*")
        if path.is_file()
    }
    nested_parent = snapshot / "restore-output"

    with pytest.raises(SnapshotError, match="outside the snapshot root"):
        restore_snapshot(snapshot, snapshot)
    with pytest.raises(SnapshotError, match="outside the snapshot root"):
        restore_snapshot(snapshot, nested_parent / "restored")

    assert not nested_parent.exists()
    assert before == {
        path.relative_to(snapshot).as_posix(): path.read_bytes()
        for path in snapshot.rglob("*")
        if path.is_file()
    }


def test_inventory_requires_complete_listing_pairing_and_archive_coverage(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(
        tmp_path / "source",
        include=("vardemangder",),
    )
    with pytest.raises(SnapshotError, match="must be present or absent together"):
        prepare_snapshot(
            _inventory(tmp_path, source_dir),
            tmp_path / "snapshot",
            converter_commit="d" * 40,
        )

    (source_dir / "Surprise.csv").write_bytes(b"x\r\n")
    inventory = _inventory(tmp_path / "second", source_dir)
    with pytest.raises(SnapshotError, match="unlisted CSV"):
        prepare_snapshot(inventory, tmp_path / "snapshot-2", converter_commit="d" * 40)


def test_build_lock_refuses_sparse_values_before_streaming_committed_blobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    selection = write_scb_snapshot(tmp_path / "accepted", source_dir)
    _builder_checkout(tmp_path, monkeypatch)
    recorded_db = tmp_path / "recorded.db"
    _write_catalog_db(
        recorded_db,
        import_date="2026-09-15T10:00:00Z",
        input_dir="/tmp/complete",
    )
    lock_path = tmp_path / "build-lock.json"
    create_build_lock(
        selection.path,
        recorded_db,
        lock_path,
        providers=("scb",),
        build_options={"validate": True},
        auxiliary_inputs={},
    )
    sparsify_scb_values(selection)

    def reject_blob_scan(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("sparse proof must fail before committed blob streaming")

    monkeypatch.setattr(snapshot_module, "_verify_committed_snapshot", reject_blob_scan)
    with pytest.raises(SnapshotMaterializationError):
        create_build_lock(
            selection.path,
            recorded_db,
            tmp_path / "second-lock.json",
            providers=("scb",),
            build_options={"validate": True},
            auxiliary_inputs={},
        )
    with pytest.raises(SnapshotMaterializationError):
        verify_build_lock(lock_path, selection.path, auxiliary_inputs={})
    assert not (tmp_path / "second-lock.json").exists()


def test_build_lock_pins_clean_repositories_auxiliary_inputs_and_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    input_repo = tmp_path / "input-repo"
    input_repo.mkdir()
    snapshot = input_repo / "snapshots" / "fixture"
    prepare_snapshot(
        _inventory(tmp_path, source_dir), snapshot, converter_commit="e" * 40
    )
    _git(input_repo, "init", "-q")
    _git(input_repo, "config", "user.email", "test@example.invalid")
    _git(input_repo, "config", "user.name", "Test")
    _git(input_repo, "add", ".")
    _git(input_repo, "commit", "-q", "-m", "snapshot")

    builder_repo = _builder_checkout(tmp_path, monkeypatch)

    auxiliary = tmp_path / "Tabelldefinitioner.sql"
    auxiliary.write_text("CREATE TABLE x (id int);\n", encoding="utf-8")
    recorded_db = tmp_path / "recorded.db"
    replay_db = tmp_path / "replay.db"
    _write_catalog_db(
        recorded_db,
        import_date="2026-09-14T10:00:00Z",
        input_dir="/tmp/first-reconstruction",
    )
    _write_catalog_db(
        replay_db,
        import_date="2026-09-14T11:00:00Z",
        input_dir="/tmp/second-reconstruction",
    )
    lock_path = tmp_path / "build-lock.json"
    lock = create_build_lock(
        snapshot,
        recorded_db,
        lock_path,
        providers=("scb",),
        build_options={"validate": True, "prestage": False},
        auxiliary_inputs={
            "Tabelldefinitioner.sql": auxiliary,
            "ID-kolumner.xlsx": None,
        },
    )
    assert lock.snapshot_path == "snapshots/fixture"
    assert load_manifest(snapshot).converter_version == lock.snapshot_converter_version
    verify_build_lock(
        lock_path,
        snapshot,
        auxiliary_inputs={
            "Tabelldefinitioner.sql": auxiliary,
            "ID-kolumner.xlsx": None,
        },
    )
    verify_build_lock(
        lock_path,
        snapshot,
        auxiliary_inputs={
            "Tabelldefinitioner.sql": auxiliary,
            "ID-kolumner.xlsx": None,
        },
        recorded_db=recorded_db,
        replay_db=replay_db,
    )
    with pytest.raises(SnapshotError, match="requires the recorded result DB"):
        verify_build_lock(
            lock_path,
            snapshot,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
            replay_db=replay_db,
        )

    _update_db(
        replay_db,
        "UPDATE import_manifest SET value = ? WHERE key = 'source_checksums'",
        "changed normalized inputs",
    )
    with pytest.raises(SnapshotError, match="replay result DB differs"):
        verify_build_lock(
            lock_path,
            snapshot,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
            recorded_db=recorded_db,
            replay_db=replay_db,
        )

    recorded_bytes = recorded_db.read_bytes()
    recorded_db.write_bytes(b"tampered recorded artifact")
    with pytest.raises(SnapshotError, match="recorded result DB hash"):
        verify_build_lock(
            lock_path,
            snapshot,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
            recorded_db=recorded_db,
        )
    recorded_db.write_bytes(recorded_bytes)
    _update_db(
        replay_db,
        "UPDATE import_manifest SET value = ? WHERE key = 'source_checksums'",
        "same normalized inputs",
    )
    _update_db(
        replay_db,
        "UPDATE catalog_fact SET value = ? WHERE id = 1",
        "changed catalog fact",
    )
    with pytest.raises(SnapshotError, match="replay result DB differs"):
        verify_build_lock(
            lock_path,
            snapshot,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
            recorded_db=recorded_db,
            replay_db=replay_db,
        )

    for field, value, message in (
        ("snapshot_schema_version", 999, "snapshot schema version"),
        ("snapshot_converter_version", -999, "snapshot converter version"),
    ):
        invalid_lock = json.loads(lock_path.read_text(encoding="utf-8"))
        invalid_lock[field] = value
        invalid_lock_path = tmp_path / f"invalid-{field}.json"
        invalid_lock_path.write_text(json.dumps(invalid_lock), encoding="utf-8")
        with pytest.raises(SnapshotError, match=f"{message} pin mismatch"):
            verify_build_lock(
                invalid_lock_path,
                snapshot,
                auxiliary_inputs={
                    "Tabelldefinitioner.sql": auxiliary,
                    "ID-kolumner.xlsx": None,
                },
            )

    auxiliary.write_text("changed\n", encoding="utf-8")
    with pytest.raises(SnapshotError, match="hash/size mismatch"):
        verify_build_lock(
            lock_path,
            snapshot,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
        )
    auxiliary.write_text("CREATE TABLE x (id int);\n", encoding="utf-8")

    (input_repo / ".gitignore").write_text(
        "/snapshots/fixture/files/\n", encoding="utf-8"
    )
    _git(input_repo, "rm", "-q", "-r", "--cached", "snapshots/fixture/files")
    _git(input_repo, "add", ".gitignore")
    _git(input_repo, "commit", "-q", "-m", "omit normalized snapshot files")
    unbacked_commit = _git(input_repo, "rev-parse", "HEAD")

    unbacked_lock_path = tmp_path / "unbacked-lock.json"
    with pytest.raises(SnapshotError, match="missing from pinned commit"):
        create_build_lock(
            snapshot,
            recorded_db,
            unbacked_lock_path,
            providers=("scb",),
            build_options={"validate": True, "prestage": False},
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
        )
    assert not unbacked_lock_path.exists()

    forged_lock = json.loads(lock_path.read_text(encoding="utf-8"))
    forged_lock["input_repository_commit"] = unbacked_commit
    forged_lock_path = tmp_path / "forged-lock.json"
    forged_lock_path.write_text(json.dumps(forged_lock), encoding="utf-8")
    with pytest.raises(SnapshotError, match="missing from pinned commit"):
        verify_build_lock(
            forged_lock_path,
            snapshot,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
        )

    (builder_repo / ".gitignore").write_text(
        "/scripts/build_db_watch.py\n", encoding="utf-8"
    )
    _git(builder_repo, "rm", "-q", "--cached", "scripts/build_db_watch.py")
    _git(builder_repo, "add", ".gitignore")
    _git(builder_repo, "commit", "-q", "-m", "omit builder entry point")
    with pytest.raises(
        SnapshotError, match="builder source is not a tracked HEAD blob"
    ):
        verify_build_lock(
            lock_path,
            snapshot,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
        )
