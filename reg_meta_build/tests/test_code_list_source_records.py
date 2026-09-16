"""Canonical code lists retain evidence before any classification decision."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.source_records import SourceRevision
from reg_meta_build.sources.code_lists import CodeListSourceError, read_code_list

if TYPE_CHECKING:
    from pathlib import Path


def _source(tmp_path: Path, text: str) -> tuple[Path, SourceRevision]:
    path = tmp_path / "codes.csv"
    payload = text.encode("utf-8")
    path.write_bytes(payload)
    return path, SourceRevision.create(
        dataset="code-list-fixture",
        publisher="Agency",
        purpose="source fixture",
        upstream_revision="fixture-1",
        artifact_path="catalog/codes.csv",
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


@pytest.mark.parametrize("header", ["code,label", "vardekod,vardebenamning"])
def test_code_csv_preserves_zeros_duplicates_conflicts_and_blank_rows(
    tmp_path: Path, header: str
) -> None:
    path, revision = _source(
        tmp_path,
        header
        + "\n01, First label \n01, First label \n01,Another label\n1,First label\n,\n\n",
    )
    clean = read_code_list(path, revision, name="declared-codes")
    assert clean.revision == revision
    assert len(clean.associations) == 5
    assert len(clean.values) == 4
    assert [row.row_number for row in clean.associations] == [2, 3, 4, 5, 6]
    first = clean.values[clean.associations[0].value_key]
    assert first.code == "01" and first.label == "First label"
    assert first.raw_cells == ("01", " First label ")
    assert [cell.name for cell in first.delivered_cells] == header.split(",")
    assert [locator.physical_record for locator in first.locators] == ["row:2", "row:3"]
    assert clean.associations[0].value_key == clean.associations[1].value_key
    assert clean.associations[1].value_key != clean.associations[2].value_key
    assert all(row.member_id is None for row in clean.associations)
    assert clean.tables[0].rows[-1].role == "note"
    assert len(clean.tables[0].rows) == 7
    assert clean.descriptors["declared-codes"].raw_cells == tuple(header.split(","))
    blank = clean.values[clean.associations[-1].value_key]
    assert blank.normalized_content == ("", "")
    assert all(cell.present and cell.raw_value == "" for cell in blank.delivered_cells)


def test_extended_code_csv_keeps_every_declared_field_and_conflicting_scope(
    tmp_path: Path,
) -> None:
    header = "code,label,label_en,parent_code,valid_from,valid_to"
    raw = (
        " 001 ",
        " Bla\u030a\u00a0  etikett ",
        " English  label ",
        " 00 ",
        "2020",
        "",
    )
    path, revision = _source(
        tmp_path,
        "\ufeff"
        + header
        + "\r\n"
        + ",".join(raw)
        + "\r\n"
        + ",".join((*raw[:4], "2021", "unknown"))
        + "\r\n",
    )
    clean = read_code_list(path, revision, name="codes")
    assert len(clean.values) == len(clean.associations) == 2
    first, second = [clean.values[row.value_key] for row in clean.associations]
    assert (
        first.normalized_content == second.normalized_content == ("001", "Blå etikett")
    )
    assert first.payload_key != second.payload_key
    assert first.raw_cells == raw
    assert [cell.name for cell in first.delivered_cells] == header.split(",")
    assert tuple(cell.raw_value for cell in first.delivered_cells) == raw
    assert tuple(cell.interpreted_value for cell in first.delivered_cells) == raw
    assert all(
        cell.raw_type == "str" and cell.storage_type == "csv" and cell.present
        for cell in first.delivered_cells
    )
    assert second.delivered_cells[-1].raw_value == "unknown"
    assert first.locators[0].physical_file == revision.artifact_path
    assert clean.associations[0].delivered_cells == first.delivered_cells


def test_quoted_multiline_label_and_header_spelling_remain_evidence(
    tmp_path: Path,
) -> None:
    path, revision = _source(
        tmp_path, ' Code ,LABEL\n01," first, label\nsecond line "\n'
    )
    clean = read_code_list(path, revision, name=" source list ")
    descriptor = clean.descriptors["source list"]
    assert descriptor.name == " source list "
    assert descriptor.raw_cells == (" Code ", "LABEL")
    value = next(iter(clean.values.values()))
    assert value.raw_cells == ("01", " first, label\nsecond line ")
    assert value.label == "first, label second line"


@pytest.mark.parametrize(
    "text",
    [
        "",
        "code,label,extra\n1,A,B\n",
        "code,label,label_en\n1,A,English\n",
        "code;label\n1;A\n",
        "label,code\nA,1\n",
        "code,code\n1,A\n",
        "code,label\n1,A,B\n",
        "vardekod,vardebenamning\n1\n",
        "code,label,label_en,parent_code,valid_from,valid_to\n1,A,,,,,extra\n",
    ],
)
def test_unknown_contract_or_row_width_fails(tmp_path: Path, text: str) -> None:
    path, revision = _source(tmp_path, text)
    with pytest.raises(CodeListSourceError, match="layout|fields"):
        read_code_list(path, revision, name="codes")


def test_header_only_list_is_observed_without_inventing_members(tmp_path: Path) -> None:
    path, revision = _source(tmp_path, "code,label\n")
    clean = read_code_list(path, revision, name="codes")
    assert not clean.values and not clean.associations
    assert len(clean.descriptors) == len(clean.tables[0].rows) == 1


@pytest.mark.parametrize("replacement", ["code,label\n1,B\n", "code,label\n1,Longer\n"])
def test_exact_revision_is_checked_before_parsing(
    tmp_path: Path, replacement: str
) -> None:
    path, revision = _source(tmp_path, "code,label\n1,A\n")
    path.write_text(replacement)
    with pytest.raises(CodeListSourceError, match="differs from its declared revision"):
        read_code_list(path, revision, name="codes")


def test_blank_descriptor_and_malformed_csv_are_explicit_errors(tmp_path: Path) -> None:
    path, revision = _source(tmp_path, 'code,label\n1,"unterminated\n')
    with pytest.raises(CodeListSourceError, match="name must be non-empty"):
        read_code_list(path, revision, name=" \t ")
    with pytest.raises(CodeListSourceError, match="invalid code CSV"):
        read_code_list(path, revision, name="codes")
