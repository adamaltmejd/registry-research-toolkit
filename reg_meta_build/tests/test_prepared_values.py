"""Prepared source values retain all evidence without expanding occurrence rows."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta.source_evidence import DeliveredCell, RecordLocator, SourceRevision
from reg_meta_build.prepared_values import (
    PreparedValueError,
    open_prepared_source_values,
    prepare_source_values,
    prepared_value_paths,
)
from reg_meta_build.source_values import (
    SourceMemberHint,
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
    SourceValueValidity,
)

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


def test_point_lookups_return_exact_member_item_and_validity(tmp_path):
    root = tmp_path / "values"
    rows = tuple(
        replace(_association(i + 2, str(i)), item_id=str(i))
        for i in range(9990, 10_000)
    ) + (replace(_association(10_002, None), item_id=None),)
    validity = SourceValueValidity(2, "9999", "2020", "2020", "validity.csv")
    manifest = _prepare(
        root,
        associations=iter(rows),
        validity=(validity,),
        validity_revision=_revision("validity", "b"),
        join=SourceValueJoin(
            record_sources=("records",),
            member_target="native_member",
            member_format="integer",
            validity_target="item",
            missing_validity="unknown",
            rule="Fixture exact integer coordinates",
            provenance=("fixture format",),
        ),
    )
    reader = _open(root, manifest)
    assert next(reader.associations()) == rows[0]
    with reader.session() as session:
        assert tuple(session.lookup_native_member(9999)) == (rows[-2],)
        assert tuple(session.lookup_member("9999")) == (rows[-2],)
        assert tuple(session.lookup_member(None)) == (rows[-1],)
        assert tuple(session.lookup_member("absent")) == ()
        assert session.native_item_coordinate("9999") == 9999
        assert session.native_item_coordinate(None) is None
        assert session.native_item_coordinate("absent") is None
        assert session.validity_for(item_id="9999") == (validity,)


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


def test_existing_output_is_never_overwritten(tmp_path):
    root = tmp_path / "values"
    _prepare(root)
    before = [path.read_bytes() for path in prepared_value_paths(root)]
    with pytest.raises(PreparedValueError, match="already exists"):
        _prepare(root)
    assert before == [path.read_bytes() for path in prepared_value_paths(root)]


def test_manifest_count_beyond_the_uint32_format_is_rejected(tmp_path):
    # The stored streams address items with uint32; a count the format cannot hold is
    # refused wherever it appears, here in an accepted manifest.
    root = tmp_path / "inputs" / "values"
    _prepare(root)
    path = root / "manifest.json"
    document = json.loads(path.read_text())
    document["item_count"] = 2**32
    payload = json.dumps(document).encode()
    path.write_bytes(payload)
    commit = accept_prepared(root)
    with pytest.raises(PreparedValueError, match="uint32"):
        open_prepared_source_values(
            root,
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            input_commit=commit,
        )


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


def test_warm_open_returns_stored_values(tmp_path):
    root = tmp_path / "inputs" / "values"
    rows = (
        _association(),
        replace(_association(), row_number=4, supplied_period=" 2020 "),
    )
    manifest = _prepare(root, associations=rows)
    reader = _open(root, manifest)
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


# Every older version carried an interpretation this one supersedes: 2 read source
# type markers as enumerated codes, 3 read a wrapped SOS inline list as the partial
# code list its first `=` per segment produced.
def test_old_preparation_cannot_reuse_a_superseded_value_interpretation(tmp_path):
    root = tmp_path / "inputs" / "values"
    _prepare(root)
    path = root / "manifest.json"
    original = json.loads(path.read_text())
    # The current version is whatever a fresh preparation writes; every older one
    # from 2 up is superseded.
    current = original["schema_version"]
    assert current > 2
    for superseded in range(2, current):
        document = dict(original, schema_version=superseded)
        payload = json.dumps(document).encode()
        path.write_bytes(payload)
        commit = accept_prepared(root)
        with pytest.raises(ValueError, match="schema_version"):
            open_prepared_source_values(
                root,
                expected_sha256=hashlib.sha256(payload).hexdigest(),
                input_commit=commit,
            )
