"""Prepared source records and evidence tables round-trip losslessly and repeatably."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import (
    accept_prepared,
    prepare_records as _prepare,
    prepared_record as _record,
    prepared_revision as _revision,
)
from reg_meta.source_evidence import RecordLocator, SourceRevision
from reg_meta_build.prepared_sources import (
    PreparedSourceError,
    PreparedSourceRecords,
    open_prepared_source_records,
    prepare_source_records,
    prepared_source_paths,
)
from reg_meta_build.source_records import (
    SourceEvidenceRow,
    SourceEvidenceTable,
    SourceFields,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Literal


def test_prepared_artifact_preserves_order_duplicates_and_exact_evidence(
    tmp_path: Path,
) -> None:
    first_revision = _revision("source-a", "a")
    second_revision = _revision("source-b", "b")
    first = _record(first_revision, row=9, member="First", raw_value=" First ")
    second = _record(second_revision, row=3, member="Second", raw_value="Second")
    records = (second, first, second)
    first_path = tmp_path / "a" / "records"
    second_path = tmp_path / "b" / "records"
    manifest = prepare_source_records(
        first_path,
        records=iter(records),
        revisions=(second_revision, first_revision),
        scope="two sources",
    )
    repeated = prepare_source_records(
        second_path,
        records=iter(records),
        revisions=(first_revision, second_revision),
        scope="two sources",
    )
    assert manifest == repeated
    for left, right in zip(
        prepared_source_paths(first_path),
        prepared_source_paths(second_path),
        strict=True,
    ):
        assert left.read_bytes() == right.read_bytes()
    commit = accept_prepared(first_path)
    opened = open_prepared_source_records(
        first_path, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert len(opened) == 3
    assert tuple(opened.records) == records
    assert tuple(opened.records) == records  # Each access starts a fresh stream.
    assert tuple(
        opened.lookup(second.source, second.locators[0].semantic_record_key)
    ) == (second, second)
    assert tuple(opened.iter_records(source=first.source)) == (first,)
    assert tuple(opened.lookup(first.source, ("absent",))) == ()


def test_preparation_keeps_occurrence_order_as_evidence(tmp_path: Path) -> None:
    revision = _revision("source-a", "a")
    first = _record(revision, row=9, member="First", raw_value="First")
    second = _record(revision, row=3, member="Second", raw_value="Second")
    original = _prepare(tmp_path / "a" / "records", records=(first, second))
    reversed_manifest = _prepare(tmp_path / "b" / "records", records=(second, first))
    assert original.database_sha256 != reversed_manifest.database_sha256


def test_repeated_large_evidence_is_interned_without_eager_record_collection(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    evidence = "Long provider documentation. " * 400
    consumed = 0

    def records():
        nonlocal consumed
        for row in range(2, 302):
            consumed += 1
            yield _record(revision, row=row, member="Repeated", raw_value=evidence)

    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=records(), revisions=(revision,), scope="repeated evidence"
    )
    assert consumed == manifest.record_count == 300
    expanded = sum(len(record.model_dump_json().encode()) for record in records())
    assert manifest.database_size < expanded // 8
    commit = accept_prepared(root)
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert not isinstance(opened.records, tuple | list)
    assert next(opened.records).delivered_cells[0].raw_value == evidence


@pytest.mark.parametrize(
    "cells",
    [
        (),
        ("Sheet!A1",),
        ("A1", "B9"),
        ("source:row:234:name", "source:row:234:definition"),
        ("Årsdata!Ö2", "Årsdata!Ö2", "Årsdata!Ö3"),
    ],
)
def test_locator_factoring_preserves_every_physical_coordinate(
    tmp_path: Path, cells: tuple[str, ...]
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=2, member="First", raw_value=" First ")
    record = record.model_copy(
        update={
            "locators": (
                record.locators[0].model_copy(update={"physical_cells": cells}),
            )
        }
    )
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root, records=(record,))
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    assert tuple(opened.records) == (record,)


def test_shared_field_text_is_not_repeated_for_changed_field_combinations(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    definition = value_field("Long shared source definition. " * 800)
    records = tuple(
        _record(
            revision,
            row=row,
            member="First",
            raw_value=" First ",
            fields=SourceFields(
                definition=definition,
                operational_definition=value_field(f"edition {row}"),
            ),
        )
        for row in range(2, 102)
    )
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root, records=records)
    expanded_size = sum(len(record.model_dump_json().encode()) for record in records)
    assert manifest.database_size < expanded_size // 4
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    assert tuple(opened.records) == records


def test_field_factoring_preserves_raw_boolean_and_integer_types(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    records = tuple(
        _record(
            revision,
            row=row,
            member="Same",
            raw_value="Same",
            fields=SourceFields(name=value_field("Same", raw=raw)),
        )
        for row, raw in enumerate((True, 1, False, 0), start=2)
    )
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root, records=records)
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    # Pydantic/Python equality equates True with 1; JSON preserves the source type.
    assert tuple(record.model_dump_json() for record in opened.records) == tuple(
        record.model_dump_json() for record in records
    )


def _evidence_table(
    revision: SourceRevision, *, name: str = "Raw source sheet"
) -> SourceEvidenceTable:
    record = _record(revision, row=4, member="Item", raw_value=" Item ")
    rows = []
    roles: tuple[Literal["header", "declaration", "unparsed", "data"], ...] = (
        "header",
        "declaration",
        "unparsed",
        "data",
    )
    for position, role in enumerate(roles, start=1):
        locator = RecordLocator(
            semantic_record_key=(revision.dataset, name, role),
            physical_file=revision.artifact_path,
            physical_table=name,
            physical_record=f"row:{position}",
            physical_cells=tuple(
                f"{name}!{column}{position}" for column in ("A", "B", "C")
            ),
        )
        rows.append(
            SourceEvidenceRow(locator=locator, role=role, cells=record.delivered_cells)
        )
    rows.append(rows[-1])  # Repeated physical observations must not be deduplicated.
    return SourceEvidenceTable(
        source=revision.dataset,
        source_revision_id=revision.revision_id,
        name=name,
        rows=tuple(rows),
    )


def test_evidence_tables_stream_and_roundtrip_with_rows_roles_and_duplicates(
    tmp_path: Path,
) -> None:
    first = _revision("source-a", "a")
    second = _revision("source-b", "b")
    table = _evidence_table(first)
    empty = SourceEvidenceTable(
        source=second.dataset,
        source_revision_id=second.revision_id,
        name="Empty sheet",
        rows=(),
    )
    tables = (table, empty, table)
    root = tmp_path / "inputs" / "records"
    repeated = tmp_path / "repeated" / "records"
    manifests = tuple(
        prepare_source_records(
            path,
            records=(),
            revisions=(first, second),
            scope="evidence tables",
            tables=iter(tables),
        )
        for path in (root, repeated)
    )
    assert manifests[0] == manifests[1]
    assert manifests[0].record_count == 0
    assert manifests[0].table_count == 3
    assert manifests[0].table_row_count == 10
    for left, right in zip(
        prepared_source_paths(root), prepared_source_paths(repeated), strict=True
    ):
        assert left.read_bytes() == right.read_bytes()
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifests[0].sha256, input_commit=commit
    )
    assert tuple(reader.records) == ()
    assert tuple(reader.iter_tables()) == tables
    assert tuple(reader.iter_tables()) == tables
    assert tuple(reader.iter_tables(source=first.dataset)) == (table, table)
    assert tuple(reader.iter_tables(source=second.dataset)) == (empty,)
    assert tuple(reader.iter_tables(source="absent-source")) == ()


@pytest.mark.parametrize("fault", ["missing-revision", "wrong-source", "invalid-row"])
def test_evidence_preparation_validates_source_membership_and_row_contracts(
    tmp_path: Path, fault: str
) -> None:
    revision = _revision("source-a", "a")
    table = _evidence_table(revision)
    if fault == "missing-revision":
        table = table.model_copy(
            update={"source_revision_id": "source-a@sha256:" + "f" * 64}
        )
    elif fault == "wrong-source":
        table = table.model_copy(update={"source": "another-source"})
    else:
        table = table.model_copy(
            update={
                "rows": (table.rows[0].model_copy(update={"role": "invented-role"}),)
            }
        )
    with pytest.raises(
        PreparedSourceError, match="source table|SourceEvidenceTable contract"
    ):
        prepare_source_records(
            tmp_path / "records",
            records=(),
            revisions=(revision,),
            scope="invalid evidence",
            tables=(table,),
        )
    assert list(tmp_path.iterdir()) == []


def test_evidence_reader_decodes_only_requested_tables_and_checks_payload_refs(
    tmp_path: Path,
) -> None:
    first = _revision("source-a", "a")
    second = _revision("source-b", "b")
    first_table = _evidence_table(first)
    second_table = _evidence_table(second)
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root,
        records=(),
        revisions=(first, second),
        scope="lazy table decoder",
        tables=(first_table, second_table),
    )
    with sqlite3.connect(root / "files/records.sqlite") as conn:
        # Exercise structural decoding directly, below the public opener's
        # immutable preparation proof. A strings payload is not a DeliveredCell.
        conn.execute(
            "UPDATE evidence_cell SET payload = (SELECT cells_payload FROM evidence_row WHERE ordinal = evidence_cell.row_ordinal) WHERE row_ordinal IN (SELECT ordinal FROM evidence_row WHERE table_ordinal = 2)"
        )
    reader = PreparedSourceRecords(root=root, manifest=manifest, input_commit="a" * 40)
    assert tuple(reader.iter_tables(source=first.dataset)) == (first_table,)
    stream = reader.iter_tables()
    assert next(stream) == first_table
    with pytest.raises(PreparedSourceError, match="wrong-kind"):
        next(stream)


def test_records_and_tables_share_cells_without_losing_either_surface(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=4, member="Item", raw_value=" Item ")
    table = _evidence_table(revision)
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root,
        records=(record,),
        revisions=(revision,),
        scope="shared raw evidence",
        tables=(table,),
    )
    with sqlite3.connect(root / "files/records.sqlite") as conn:
        assert conn.execute(
            "SELECT count(*) FROM payload WHERE kind='cell'"
        ).fetchone()[0] == len(record.delivered_cells)
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert tuple(reader.records) == (record,)
    assert tuple(reader.iter_tables()) == (table,)
