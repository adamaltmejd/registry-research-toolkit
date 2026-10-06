"""Lossless-record and failure-safety tests for the input snapshot prototype."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import pathlib
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_scb_input
from _snapshot_fixtures import scb_inventory
from reg_meta_build.input_snapshot import (
    SnapshotError,
    measure_git_history,
    prepare_snapshot,
    restore_snapshot,
    verify_snapshot,
)

if TYPE_CHECKING:
    from pathlib import Path


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
    inventory = scb_inventory(tmp_path, source_dir, extra=(extra.name,))

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


def test_prepare_exhaustively_verifies_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    output = tmp_path / "snapshot"
    write_bytes = pathlib.Path.write_bytes

    # Damage a staged record (same size) right after the candidate manifest is
    # written, so only an exhaustive proof of the staged candidate can catch it.
    def write_manifest_then_damage(self: Path, data: bytes) -> int:
        written = write_bytes(self, data)
        if self.name == "manifest.json" and self.parent.name.startswith(
            ".snapshot.candidate-"
        ):
            record = next(self.parent.glob("files/Vardemangder.csv/records/*.tsv"))
            payload = bytearray(record.read_bytes())
            payload[0] = ord("A") if payload[0] != ord("A") else ord("B")
            write_bytes(record, bytes(payload))
        return written

    monkeypatch.setattr(pathlib.Path, "write_bytes", write_manifest_then_damage)
    with pytest.raises(SnapshotError, match="references missing value_set payload"):
        prepare_snapshot(
            scb_inventory(tmp_path, source_dir),
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
        scb_inventory(tmp_path, source_dir), snapshot, converter_commit="b" * 40
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
    inventory_path = scb_inventory(tmp_path, source_dir)
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

    path_open = pathlib.Path.open
    opened: list[str] = []

    # Mutate the delivery once the first source CSV has been converted, i.e. when
    # the converter opens a second distinct source CSV for reading.
    def open_then_mutate(self: Path, mode: str = "r", *args, **kwargs):
        if self.parent == source_dir and "r" in mode and self.name not in opened:
            opened.append(self.name)
            if len(opened) == 2:
                if mutation == "earlier-changed":
                    with path_open(
                        source_dir / "Registerinformation.csv", "ab"
                    ) as handle:
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
        return path_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "open", open_then_mutate)
    output = tmp_path / "candidate"

    with pytest.raises(
        SnapshotError, match=r"source (?:bundle|CSV) changed during conversion"
    ):
        prepare_snapshot(inventory_path, output, converter_commit="b" * 40)

    assert not output.exists()
    assert not list(tmp_path.glob(".candidate.candidate-*"))


def test_snapshot_rejects_unsupported_version_and_never_replaces_candidates(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    inventory = scb_inventory(tmp_path, source_dir)
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
            scb_inventory(tmp_path / "malformed", malformed_dir),
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
        scb_inventory(tmp_path, source_dir), snapshot, converter_commit="f" * 40
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
        scb_inventory(tmp_path, source_dir), snapshot, converter_commit="f" * 40
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
            scb_inventory(tmp_path, source_dir),
            tmp_path / "snapshot",
            converter_commit="d" * 40,
        )

    (source_dir / "Surprise.csv").write_bytes(b"x\r\n")
    inventory = scb_inventory(tmp_path / "second", source_dir)
    with pytest.raises(SnapshotError, match="unlisted CSV"):
        prepare_snapshot(inventory, tmp_path / "snapshot-2", converter_commit="d" * 40)
