"""Regressions for complete and focused SCB observation traversal."""

from __future__ import annotations

import gzip
import hashlib
import json
from contextlib import contextmanager
from typing import TYPE_CHECKING

from _csv_fixtures import (
    REGISTERINFORMATION_HEADER,
    _var_row,
    write_input_bundle,
    write_scb_input,
)
from _lisa_fixtures import write_lisa_workbook
from reg_meta_build.db import _open_scb_csv_raw
from reg_meta_build.input_snapshot import LisaWorkbookSelection, open_input_bundle
from reg_meta_build.source_inspection import (
    CensusCompletion,
    inspect_bundle_source_records,
    write_scb_observation_census,
)

from reg_meta_build import db as db_module

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path
    from typing import Any

    import pytest


def _read_census(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        return [json.loads(line) for line in source]


def _census(tmp_path: Path, name: str, rows: list[str]) -> list[dict[str, Any]]:
    input_dir = tmp_path / name / "source"
    write_scb_input(input_dir, registerinformation_rows=rows)
    selection = write_input_bundle(tmp_path / name / "accepted", input_dir)
    destination = tmp_path / f"{name}.jsonl.gz"
    write_scb_observation_census(
        open_input_bundle(selection), destination, code_commit="c" * 40
    )
    return _read_census(destination)


def test_raw_scb_traversal_reuses_source_row_without_presence_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_row: list[str | None] = ["kept", ""]

    @contextmanager
    def open_source(
        _path: Path, _snapshot: object
    ) -> Iterator[tuple[list[str], Iterator[list[str | None]]]]:
        yield ["first", "second"], iter((source_row,))

    monkeypatch.setattr(db_module, "_open_scb_source_raw", open_source)

    with _open_scb_csv_raw(tmp_path / "Other.csv") as (_header, rows):
        row_number, fields = next(rows)

    assert row_number == 2
    assert fields is source_row
    assert type(fields) is list
    assert fields == ["kept", ""]


def test_focused_lisa_inspection_skips_unrelated_malformed_native_row(
    tmp_path: Path,
) -> None:
    unrelated = _var_row(colname="Other", cvid=1, var_id=1)
    unrelated_fields = unrelated.split("|")
    header = REGISTERINFORMATION_HEADER.split("|")
    unrelated_fields[header.index("RegVerID")] = "not-an-id"
    lisa = _var_row(
        colname="AmPolTyp",
        cvid=2,
        var_id=31619,
        year="2020",
        regver_id=200,
        register=("LISA", 34, 153),
    )
    input_dir = tmp_path / "source"
    write_scb_input(
        input_dir,
        registerinformation_rows=["|".join(unrelated_fields), lisa],
    )
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

    report = inspect_bundle_source_records(
        open_input_bundle(selection), code_commit="c" * 40
    )

    scb_records = [
        record
        for record in report.source_records
        if record.source == "scb-registerinformation"
    ]
    assert len(scb_records) == 1
    assert scb_records[0].subject.native.register_id == 34
    assert scb_records[0].locators[0].physical_record == "row:3"
    assert report.summary.scb_total_occurrences == 1


def test_context_group_keeps_members_when_one_context_has_own_alternatives(
    tmp_path: Path,
) -> None:
    def row(data_length: str, population_name: str) -> str:
        return _var_row(
            colname="Contextual",
            cvid=7001,
            var_id=7000,
            year="2004",
            regver_id=700,
            data_type="char",
            data_length=data_length,
            population_name=population_name,
        )

    rows = [
        row("10", "Population A"),
        row("11", "Population A"),
        row("10", "Population B"),
    ]

    lines = _census(tmp_path, "contexts", rows)
    same_context = [
        line for line in lines if line["type"] == "same_complete_context_alternatives"
    ]
    separated = [
        line for line in lines if line["type"] == "context_separated_alternatives"
    ]

    assert len(same_context) == 1
    assert len(separated) == 1
    assert {
        member["locator"]["physical_record"]
        for alternative in separated[0]["alternatives"]
        for member in alternative["members"]
    } == {"row:2", "row:3", "row:4"}
    assert (
        len(
            {
                member["context_fingerprint"]
                for alternative in separated[0]["alternatives"]
                for member in alternative["members"]
            }
        )
        == 2
    )
    completion = CensusCompletion.model_validate_json(
        json.dumps(lines[-1], ensure_ascii=False)
    )
    assert completion.counts.context_separated_groups == 1
    assert completion.affected_memberships.context_separated_cvids == (7001,)


def test_temporal_group_crosses_edition_specific_cvids_without_expanding_pool(
    tmp_path: Path,
) -> None:
    rows = [
        _var_row(
            colname="Temporal",
            cvid=8101,
            var_id=8100,
            year="2001",
            versionname="2001-2003",
            regver_id=801,
            data_type="char",
            data_length="10",
        ),
        _var_row(
            colname="Temporal",
            cvid=8102,
            var_id=8100,
            year="2004",
            regver_id=802,
            data_type="char",
            data_length="11",
        ),
    ]

    lines = _census(tmp_path, "temporal", rows)
    groups = [
        line for line in lines if line["type"] == "unproved_temporal_alternatives"
    ]

    assert len(groups) == 1
    assert not any(part.startswith("member:") for part in groups[0]["comparison_key"])
    members = [
        member
        for alternative in groups[0]["alternatives"]
        for member in alternative["members"]
    ]
    assert {member["native"]["member_id"] for member in members} == {8101, 8102}
    assert {member["native"]["edition_id"] for member in members} == {801, 802}
    assert len(members) == 2
    pooled = next(
        line
        for line in lines
        if line["type"] == "observation"
        and line["record"]["subject"]["native"]["member_id"] == 8101
    )
    assert pooled["record"]["edition_scope"]["kind"] == "pooled"
    assert pooled["interpretation_issue"]["kind"] == "pooled_period"
    completion = CensusCompletion.model_validate_json(
        json.dumps(lines[-1], ensure_ascii=False)
    )
    assert completion.counts.unproved_temporal_groups == 1
    assert completion.counts.unproved_temporal_cvids == 2
    assert completion.affected_memberships.unproved_temporal_cvids == (8101, 8102)
