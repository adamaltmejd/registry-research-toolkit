"""Lossless-record and failure-safety tests for the input snapshot prototype."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import subprocess
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_scb_input
from reg_meta_build.input_snapshot import (
    SCB_CSV_FILES,
    SnapshotError,
    converter_source_commit,
    create_build_lock,
    load_manifest,
    measure_codec_sample,
    measure_git_history,
    prepare_snapshot,
    restore_snapshot,
    verify_build_lock,
    verify_snapshot,
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


def test_build_lock_pins_clean_repositories_auxiliary_inputs_and_result(
    tmp_path: Path,
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

    builder_repo = tmp_path / "builder-repo"
    builder_repo.mkdir()
    (builder_repo / "uv.lock").write_text("fixture lock\n", encoding="utf-8")
    _git(builder_repo, "init", "-q")
    _git(builder_repo, "config", "user.email", "test@example.invalid")
    _git(builder_repo, "config", "user.name", "Test")
    _git(builder_repo, "add", ".")
    _git(builder_repo, "commit", "-q", "-m", "builder")

    auxiliary = tmp_path / "Tabelldefinitioner.sql"
    auxiliary.write_text("CREATE TABLE x (id int);\n", encoding="utf-8")
    result_db = tmp_path / "reg_meta.db"
    result_db.write_bytes(b"fixture result")
    lock_path = tmp_path / "build-lock.json"
    lock = create_build_lock(
        snapshot,
        builder_repo,
        result_db,
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
        builder_repo,
        auxiliary_inputs={
            "Tabelldefinitioner.sql": auxiliary,
            "ID-kolumner.xlsx": None,
        },
        result_db=result_db,
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
                builder_repo,
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
            builder_repo,
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
            builder_repo,
            result_db,
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
            builder_repo,
            auxiliary_inputs={
                "Tabelldefinitioner.sql": auxiliary,
                "ID-kolumner.xlsx": None,
            },
        )
