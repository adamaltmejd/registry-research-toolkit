"""Prepared source values retain all evidence without expanding occurrence rows."""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta_build.prepared_values import (
    PreparedValueError,
    open_prepared_source_values,
    prepare_source_values,
    prepared_value_paths,
)
from reg_meta_build.source_records import DeliveredCell, RecordLocator, SourceRevision
from reg_meta_build.source_values import (
    SourceMemberHint,
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueValidity,
)

from reg_meta_build import prepared_values

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


def _revision(name: str = "values", marker: str = "a") -> SourceRevision:
    return SourceRevision.create(
        dataset=name,
        publisher="fixture",
        purpose="source value storage test",
        upstream_revision="2026-09",
        artifact_path=f"{name}.csv",
        artifact_size=100,
        artifact_sha256=marker * 64,
    )


def _association(row: int = 2, member: str | None = "001") -> SourceValueAssociation:
    return SourceValueAssociation(
        row_number=row,
        descriptor_key="list-1",
        value_key="value-1",
        source_file="values.csv",
        member_id=member,
        member_id_field="CVID",
        item_id="0007",
    )


def _prepare(root: Path, **changes):
    arguments: dict[str, Any] = {
        "revision": _revision(),
        "descriptors": (
            SourceValueDescriptor(
                "list-1", raw_cells=(" Raw\u00a0list ",), name="Raw list"
            ),
        ),
        "values": (SourceValue("value-1", "01", "Text", raw_cells=("01", " Text ")),),
        "associations": (_association(),),
    }
    arguments.update(changes)
    return prepare_source_values(root, **arguments)


def _open(root: Path, manifest):
    commit = accept_prepared(root)
    return open_prepared_source_values(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )


def _evidence():
    locator = RecordLocator(
        semantic_record_key=("Codes", "01"),
        physical_file="workbook.xlsx",
        physical_table="Codes",
        physical_record="row:19",
        physical_cells=("Codes!A19", "Codes!B19"),
    )
    cells = (
        DeliveredCell(
            name="code",
            present=True,
            raw_value="01",
            interpreted_value="01",
            raw_type="str",
            storage_type="s",
            number_format="@",
        ),
        DeliveredCell(name="blank", present=True, raw_value="", interpreted_value=""),
        DeliveredCell(
            name="absent", present=False, raw_value=None, interpreted_value=""
        ),
    )
    hint = SourceMemberHint(role="sheet_suffix", value=" name ", locator=locator)
    return locator, cells, hint


def test_roundtrip_all_evidence_duplicates_and_exact_native_tokens(tmp_path):
    root = tmp_path / "inputs" / "values"
    locator, cells, hint = _evidence()
    descriptors = (
        SourceValueDescriptor(
            "list-1",
            raw_cells=(None, "", " old\u00a0name "),
            name="old name",
            version="LA2020",
            level="x",
            member_hints=(hint,),
            locators=(locator,),
            delivered_cells=cells,
        ),
        SourceValueDescriptor("empty", raw_cells=("",)),
    )
    values = (
        SourceValue(
            "value-1",
            "01",
            "Text",
            raw_cells=("01", " Text "),
            locators=(locator,),
            delivered_cells=cells,
        ),
        SourceValue("other-key", "01", "Text", raw_cells=("01", "Text")),
    )
    first = replace(_association(), source_table="Codes")
    rows = (
        first,
        replace(first, row_number=3, member_id="1"),
        first,
        replace(
            first,
            row_number=19,
            source_file="workbook.xlsx",
            source_table=None,
            member_id=None,
            member_id_field=None,
            item_id=None,
            member_hints=(hint,),
            supplied_period=" 2020 ",
            section_period="2019–2022",
            section_locator=locator,
            delivered_cells=cells,
        ),
        replace(first, row_number=8, member_id="", item_id="", value_key="other-key"),
        replace(first, row_number=8, item_id=None),
    )
    validity = (
        SourceValueValidity(
            9,
            "0007",
            None,
            "",
            "validity.csv",
            "Scope",
            raw_cells=("0007", None, ""),
            locators=(locator,),
            delivered_cells=cells,
        ),
    ) * 2
    validity_revision = _revision("validity", "b")
    manifest = _prepare(
        root,
        descriptors=descriptors,
        values=values,
        associations=iter(rows),
        validity=iter(validity),
        validity_revision=validity_revision,
    )
    reader = _open(root, manifest)
    assert reader.manifest.revision == _revision()
    assert reader.manifest.validity_revision == validity_revision
    assert tuple(reader.descriptors()) == descriptors
    assert tuple(reader.values()) == values
    assert tuple(reader.validity()) == validity
    assert tuple(reader.associations()) == rows
    assert tuple(reader.associations()) == rows
    for member in ("001", "1", None, "", "missing"):
        assert tuple(reader.lookup_member(member)) == tuple(
            row for row in rows if row.member_id == member
        )
    assert [row.locator for row in reader.associations()] == [
        row.locator for row in rows
    ]
    assert manifest.association_count == 6
    assert manifest.member_count == 4
    assert manifest.auxiliary_count == 4


def test_regular_stream_has_fixed_compact_size_and_no_auxiliary_rows(tmp_path):
    root = tmp_path / "inputs" / "values"
    count = 20_000
    manifest = _prepare(
        root, associations=(_association(i + 2, str(i % 11)) for i in range(count))
    )
    sizes = {file.path: file.size for file in manifest.files}
    assert sizes["files/associations.bin"] == 16 * count
    assert sizes["files/member-positions.bin"] == 4 * count
    assert manifest.auxiliary_count == 0
    reader = _open(root, manifest)
    assert tuple(reader.lookup_member("4")) == tuple(
        _association(i + 2, "4") for i in range(4, count, 11)
    )
    assert sum(1 for _ in reader.associations()) == count


def test_empty_stream_and_dictionaries_are_valid(tmp_path):
    root = tmp_path / "inputs" / "values"
    manifest = _prepare(root, descriptors=(), values=(), associations=())
    reader = _open(root, manifest)
    assert manifest.association_defaults is None
    assert tuple(reader.associations()) == ()
    assert tuple(reader.lookup_member(None)) == ()
    assert (
        tuple(reader.descriptors())
        == tuple(reader.values())
        == tuple(reader.validity())
        == ()
    )


def test_preparation_is_byte_deterministic(tmp_path):
    first, second = tmp_path / "first", tmp_path / "second"
    rows = (_association(2, None), _association(4, ""), _association(2, None))
    a = _prepare(first, associations=rows)
    b = _prepare(second, associations=iter(rows))
    assert a == b
    assert [path.read_bytes() for path in prepared_value_paths(first)] == [
        path.read_bytes() for path in prepared_value_paths(second)
    ]


@pytest.mark.parametrize(
    "changes,match",
    [
        (
            {
                "associations": (
                    _association(),
                    replace(_association(), value_key="missing"),
                )
            },
            "missing dictionary",
        ),
        (
            {
                "descriptors": (
                    SourceValueDescriptor("list-1"),
                    SourceValueDescriptor("list-1"),
                )
            },
            "duplicate descriptor",
        ),
        (
            {
                "values": (
                    SourceValue("value-1", "1", "a"),
                    SourceValue("value-1", "1", "b"),
                )
            },
            "duplicate value",
        ),
        ({"associations": (replace(_association(), member_id=12),)}, "member_id"),
        ({"associations": (replace(_association(), row_number=True),)}, "row_number"),
        (
            {"values": (replace(SourceValue("value-1", "123", "a"), code=123),)},
            "cannot prepare",
        ),
    ],
)
def test_invalid_input_fails_atomically(tmp_path, changes, match):
    root = tmp_path / "values"
    with pytest.raises(PreparedValueError, match=match):
        _prepare(root, **changes)
    assert tuple(tmp_path.iterdir()) == ()


def test_validity_requires_explicit_provenance(tmp_path):
    rows = (SourceValueValidity(2, "0007", "2020", "2023", "validity.csv"),)
    with pytest.raises(PreparedValueError, match="explicit validity_revision"):
        _prepare(tmp_path / "missing", validity=rows)
    revision = _revision("validity", "b").model_copy(
        update={"artifact_sha256": "c" * 64}
    )
    with pytest.raises(PreparedValueError, match="revision identity"):
        _prepare(tmp_path / "invalid", validity=rows, validity_revision=revision)


def test_uint32_ceiling_and_existing_output_are_fail_closed(tmp_path, monkeypatch):
    root = tmp_path / "values"
    monkeypatch.setattr(prepared_values, "_UINT32_MAX", 3)
    with pytest.raises(PreparedValueError, match="uint32"):
        _prepare(root, associations=(_association(i + 2) for i in range(4)))
    assert not root.exists()
    _prepare(root)
    before = [path.read_bytes() for path in prepared_value_paths(root)]
    with pytest.raises(PreparedValueError, match="already exists"):
        _prepare(root)
    assert before == [path.read_bytes() for path in prepared_value_paths(root)]


@pytest.mark.parametrize(
    "member", ["values.sqlite", "associations.bin", "member-positions.bin"]
)
def test_new_commit_cannot_bless_same_size_member_edit_with_stale_manifest(
    tmp_path, member
):
    root = tmp_path / "inputs" / "values"
    manifest = _prepare(root)
    accept_prepared(root)
    path = root / "files" / member
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    changed_commit = accept_prepared(root)
    with pytest.raises(PreparedValueError, match="preparation proof"):
        open_prepared_source_values(
            root, expected_sha256=manifest.sha256, input_commit=changed_commit
        )


@pytest.mark.parametrize("change", ["missing", "dirty", "index-flag", "ignored-extra"])
def test_warm_selection_rejects_invalid_worktree(tmp_path, change):
    root = tmp_path / "inputs" / "values"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    path = root / "files" / "associations.bin"
    if change == "missing":
        path.unlink()
    elif change == "dirty":
        path.write_bytes(b"x" * path.stat().st_size)
    elif change == "index-flag":
        subprocess.run(
            [
                "git",
                "-C",
                str(root.parent),
                "update-index",
                "--assume-unchanged",
                "values/files/associations.bin",
            ],
            check=True,
        )
    else:
        (root.parent / ".git" / "info" / "exclude").write_text("hidden\n")
        (root / "files" / "hidden").write_text("unaccepted")
    with pytest.raises(PreparedValueError):
        open_prepared_source_values(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


def test_warm_access_does_not_rehash_or_reclean_payloads(tmp_path, monkeypatch):
    root = tmp_path / "inputs" / "values"
    rows = (
        _association(),
        replace(_association(), row_number=4, supplied_period=" 2020 "),
    )
    manifest = _prepare(root, associations=rows)
    commit = accept_prepared(root)

    def forbidden(*args, **kwargs):
        pytest.fail("warm reader re-ran preparation")

    monkeypatch.setattr(prepared_values, "_file_sha256", forbidden)
    monkeypatch.setattr(prepared_values, "_checked", forbidden)
    monkeypatch.setattr(prepared_values, "_check_association", forbidden)
    reader = open_prepared_source_values(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert tuple(reader.associations()) == rows
    assert tuple(reader.lookup_member("001")) == rows
    assert len(tuple(reader.values())) == len(tuple(reader.descriptors())) == 1


def test_manifest_and_commit_pins_reject_updates(tmp_path):
    root = tmp_path / "inputs" / "values"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    with pytest.raises(PreparedValueError, match="manifest hash"):
        open_prepared_source_values(root, expected_sha256="0" * 64, input_commit=commit)
    document = json.loads((root / "manifest.json").read_text())
    document["revision"]["purpose"] = "changed preparation"
    (root / "manifest.json").write_text(json.dumps(document))
    accept_prepared(root)
    with pytest.raises(PreparedValueError, match="commit pin"):
        open_prepared_source_values(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )
