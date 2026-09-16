"""Losslessness tests for the SCB value-record cleaning boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _csv_fixtures import write_scb_input, write_scb_snapshot
from reg_meta_build.input_snapshot import open_scb_snapshot
from reg_meta_build.normalization import canonical_value_set_content
from reg_meta_build.sources.scb_values import clean_scb_values

if TYPE_CHECKING:
    from pathlib import Path


def test_clean_scb_values_preserves_native_associations_and_validity(
    tmp_path: Path,
) -> None:
    value_rows = [
        "Version  A|Level  1|007|Alpha  label|100|",
        'Version  A|Level  1|007|Alpha label|100|""',
        "Version  A|Level  1|007|Alpha label|100|0",
        "Alternate|Level 2|008|Å|100|000",
        "Alternate|Level 2|008|DOS_LABEL|100|999",
        "Alternate|Level 2|008|DOS_LABEL|100|999",
        "Alternate|Level 2|009|Conflict A||5001",
        'Alternate|Level 2|009|Conflict B|""|5001',
        "Alternate|Level 2|000|Leading zero|0|0",
    ]
    validity_rows = [
        "5001|2019-06-30|2020-02-29",
        "5001|2019-06-30|2020-02-29",
        "999||2021-03-04",
        '0|""|9999-12-31',
    ]
    scb_dir = write_scb_input(
        tmp_path / "source",
        vardemangder_rows=value_rows,
        valid_dates_rows=validity_rows,
    )
    value_path = scb_dir / "Vardemangder.csv"
    value_path.write_bytes(value_path.read_bytes().replace(b"DOS_LABEL", b"\x8f"))
    reader = open_scb_snapshot(write_scb_snapshot(tmp_path / "accepted", scb_dir))

    cleaned = clean_scb_values(reader)
    associations = list(cleaned.associations())
    assert dict(cleaned.provenance) == reader.provenance
    assert len(cleaned.provenance["input_repository_commit"]) == 40

    assert [association.row_number for association in associations] == list(
        range(2, 11)
    )
    assert [association.locator for association in associations[-2:]] == [
        "Vardemangder.csv:row:9",
        "Vardemangder.csv:row:10",
    ]
    assert [(a.cvid, a.item_id) for a in associations] == [
        ("100", None),
        ("100", ""),
        ("100", "0"),
        ("100", "000"),
        ("100", "999"),
        ("100", "999"),
        (None, "5001"),
        ("", "5001"),
        ("0", "0"),
    ]
    assert (
        associations[4].cvid,
        associations[4].item_id,
        associations[4].descriptor_key,
        associations[4].value_key,
    ) == (
        associations[5].cvid,
        associations[5].item_id,
        associations[5].descriptor_key,
        associations[5].value_key,
    )
    assert associations[0].descriptor_key != associations[3].descriptor_key
    first_descriptor = cleaned.descriptors[associations[0].descriptor_key]
    assert first_descriptor.raw_cells == ("Version  A", "Level  1")
    assert (first_descriptor.version, first_descriptor.level) == (
        "Version A",
        "Level 1",
    )
    assert cleaned.values[associations[0].value_key].normalized_content == (
        "007",
        "Alpha label",
    )
    assert cleaned.values[associations[3].value_key].raw_cells == ("008", "Å")
    assert cleaned.values[associations[4].value_key].raw_cells == ("008", "\x8f")
    equivalent_labels = [
        cleaned.values[association.value_key].normalized_content
        for association in associations[3:6]
    ]
    assert equivalent_labels == [("008", "Å")] * 3
    conflicting_labels = [
        cleaned.values[association.value_key].normalized_content
        for association in associations[6:8]
    ]
    assert conflicting_labels == [("009", "Conflict A"), ("009", "Conflict B")]
    assert canonical_value_set_content(conflicting_labels) == (
        ("009", "Conflict A"),
        ("009", "Conflict B"),
    )
    assert cleaned.values[associations[-1].value_key].code == "000"
    assert "000" not in {record.item_id for record in cleaned.validity}

    assert [(record.locator, record.raw_cells) for record in cleaned.validity] == [
        (
            "VardemangderValidDates.csv:row:2",
            ("5001", "2019-06-30", "2020-02-29"),
        ),
        (
            "VardemangderValidDates.csv:row:3",
            ("5001", "2019-06-30", "2020-02-29"),
        ),
        ("VardemangderValidDates.csv:row:4", ("999", None, "2021-03-04")),
        ("VardemangderValidDates.csv:row:5", ("0", "", "9999-12-31")),
    ]
    assert (cleaned.validity[0].valid_from, cleaned.validity[0].valid_to) == (
        "2019-06-30",
        "2020-02-29",
    )


def test_canonical_content_compares_only_explicit_normalized_members() -> None:
    assert canonical_value_set_content(
        [
            ("007", "Alpha label"),
            ("007", "Alpha label"),
            ("009", "Conflict B"),
            ("009", "Conflict A"),
        ]
    ) == (
        ("007", "Alpha label"),
        ("009", "Conflict A"),
        ("009", "Conflict B"),
    )


def test_normalized_payloads_keep_missing_distinct_from_supplied_empty(
    tmp_path: Path,
) -> None:
    scb_dir = write_scb_input(
        tmp_path / "source",
        vardemangder_rows=["||||100|5", '""|""|""|""|100|6'],
        valid_dates_rows=["5||", '6|""|""'],
    )
    reader = open_scb_snapshot(write_scb_snapshot(tmp_path / "accepted", scb_dir))
    cleaned = clean_scb_values(reader)
    missing, empty = list(cleaned.associations())

    assert cleaned.values[missing.value_key].normalized_content == (None, None)
    assert cleaned.values[empty.value_key].normalized_content == ("", "")
    assert cleaned.descriptors[missing.descriptor_key].version is None
    assert cleaned.descriptors[empty.descriptor_key].version == ""
    assert cleaned.validity[0].valid_from is None
    assert cleaned.validity[1].valid_from == ""
