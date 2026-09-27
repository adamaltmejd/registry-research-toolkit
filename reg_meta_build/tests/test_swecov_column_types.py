"""SWECOV storage schema is selected evidence, never row-level data."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import _var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from reg_meta_build._curation import data_type_class, widen_data_type_classes
from reg_meta_build.input_snapshot import _validate_bundle_contract
from reg_meta_build.pipeline import build_catalog
from reg_meta_build.prepared_catalog import (
    ReferenceEvidence,
    open_prepared_catalog_sources,
    prepare_catalog_sources,
)
from reg_meta_build.source_records import SourceRevision
from reg_meta_build.source_reference_records import SourceColumnTypeDeclaration
from reg_meta_build.sources.swecov_column_types import (
    SwecovColumnTypesError,
    index_swecov_column_types,
    infer_steward_column_type,
    read_swecov_column_types,
    storage_class,
)

if TYPE_CHECKING:
    from pathlib import Path


HEADER = (
    "table_schema",
    "table_name",
    "table_type",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "ordinal_position",
)


def _csv(rows: list[tuple[str, str, str, str]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(HEADER)
    for position, (table, column, sql_type, table_type) in enumerate(rows, start=1):
        writer.writerow(
            ("dbo", table, table_type, column, sql_type, "", "", "", position)
        )
    return buffer.getvalue().encode()


def _read(tmp_path: Path, payload: bytes):
    path = tmp_path / "swecov_column_types.csv"
    path.write_bytes(payload)
    revision = SourceRevision.create(
        dataset="swecov/derived/swecov_column_types.csv",
        publisher="SWECOV",
        purpose="storage schema",
        upstream_revision="fixture",
        artifact_path="catalog/swecov/derived/swecov_column_types.csv",
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )
    return read_swecov_column_types(path, revision)


@pytest.mark.parametrize(
    ("types", "expected"),
    [
        (("int", "bigint", "smallint", "tinyint", "bit"), "integer"),
        (("numeric", "decimal", "float", "real", "money", "smallmoney"), "decimal"),
        (("char", "varchar", "nchar", "nvarchar", "text", "ntext"), "text"),
        (("date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"), "date"),
    ],
)
def test_storage_class_boundaries(types: tuple[str, ...], expected: str) -> None:
    assert {storage_class(kind) for kind in types} == {expected}
    assert storage_class("varbinary") is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("INTEGER", "integer"),
        ("decimal", "decimal"),
        ("Text", "text"),
        ("DATE", "date"),
        ("SMALLINT", "integer"),
        ("FLOAT", "decimal"),
        ("VARCHAR", "text"),
        ("DATETIME2", "date"),
        ("uniqueidentifier", None),
        ("Datum och klockslag", None),
    ],
)
def test_data_type_class_boundaries(value: str, expected: str | None) -> None:
    assert data_type_class(value) == expected


@pytest.mark.parametrize(
    ("classes", "expected"),
    [
        ((), None),
        (("integer", "integer"), "integer"),
        (("integer", "decimal"), "decimal"),
        (("integer", "text"), "text"),
        (("integer", "date"), None),
        (("decimal", "decimal"), "decimal"),
        (("decimal", "text"), "text"),
        (("decimal", "date"), None),
        (("text", "text"), "text"),
        (("text", "date"), "text"),
        (("date", "date"), "date"),
        (("date", "integer", "text"), None),
    ],
)
def test_type_class_lattice(classes: tuple[str, ...], expected: str | None) -> None:
    assert widen_data_type_classes(classes) == expected


def test_reader_contract_and_json_roundtrip(tmp_path: Path) -> None:
    parsed = _read(tmp_path, _csv([("CIS2004", "CO11", "smallint", "BASE TABLE")]))
    (declaration,) = parsed.declarations
    assert declaration.locator.semantic_record_key == (
        "swecov_column_type",
        "CIS2004",
        "co11",
    )
    assert declaration.delivered_cells[2].raw_value == "BASE TABLE"
    evidence = ReferenceEvidence(
        revision_id=parsed.revision.revision_id, declaration=declaration
    )
    assert ReferenceEvidence.model_validate_json(evidence.model_dump_json()) == evidence


def test_selected_csv_is_prepared_as_typed_reference_evidence(tmp_path: Path) -> None:
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=[
            _var_row(cvid=1001, var_id=101, colname="VALUE", data_type="int")
        ],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        include=("registerinformation", "unika"),
    )
    csv_path = source / "swecov/derived/swecov_column_types.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_bytes(_csv([("CIS2004", "CO11", "smallint", "BASE TABLE")]))
    selection = write_input_bundle(tmp_path / "accepted", source)
    destination = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(selection, destination)
    commit = accept_prepared(destination)
    prepared = open_prepared_catalog_sources(
        destination,
        expected_sha256=manifest.sha256,
        input_commit=commit,
    )
    entry = next(
        item
        for item in manifest.inputs
        if item.path == "catalog/swecov/derived/swecov_column_types.csv"
    )
    assert entry.role == "swecov_column_types" and entry.counts.declarations == 1
    assert entry.revision is not None
    declarations = [
        item.declaration
        for item in prepared.iter_evidence()
        if isinstance(item, ReferenceEvidence)
        and isinstance(item.declaration, SourceColumnTypeDeclaration)
        and item.revision_id == entry.revision.revision_id
    ]
    assert index_swecov_column_types(declarations).keys() == {("CIS2004", "co11")}

    curation = tmp_path / "curation"
    register = curation / "registers/scb/sample.toml"
    register.parent.mkdir(parents=True)
    (curation / "classifications").mkdir()
    register.write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )
    output, report = tmp_path / "catalog.db", tmp_path / "report"
    build_catalog(
        destination,
        commit,
        manifest.sha256,
        output,
        report,
        curation_dir=curation,
        registers=("1",),
    )
    with gzip.open(report / "events.jsonl.gz", "rt", encoding="utf-8") as stream:
        events = [json.loads(line) for line in stream]
    assert not any(event["kind"] == "issue" for event in events)
    assert any(
        event["kind"] == "prepared_evidence"
        and event["revision_id"] == entry.revision.revision_id
        and event["disposition"] == "source_context"
        for event in events
    )
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM source_column_type").fetchone() == (
            0,
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"bad,header\n", "header"),
        (_csv([("CIS2004", "CO11", "smallint", "COPY")]), "table_type"),
        (
            _csv([("CIS2004", "CO11", "smallint", "VIEW")]).replace(
                b",1\r\n", b",1.5\r\n"
            ),
            "noninteger",
        ),
    ],
)
def test_bundle_validation_rejects_invalid_csv(
    tmp_path: Path, payload: bytes, message: str
) -> None:
    path = tmp_path / "catalog/swecov/derived/swecov_column_types.csv"
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    with pytest.raises(SwecovColumnTypesError, match=message):
        _validate_bundle_contract(tmp_path)


def test_widening_prefixes_and_row_order(tmp_path: Path) -> None:
    rows = [
        ("CIS2004", "CO11", "smallint", "BASE TABLE"),
        ("CIS2012", "co11", "float", "VIEW"),
        ("CIS2016", "co11", "varchar", "VIEW"),
        ("tabell_Cis2016", "co11", "int", "VIEW"),
        ("Cis2020", "co11", "date", "VIEW"),
    ]
    first = index_swecov_column_types(_read(tmp_path, _csv(rows)).declarations)
    last = index_swecov_column_types(
        _read(tmp_path, _csv(list(reversed(rows)))).declarations
    )
    assert infer_steward_column_type(
        "co11", ("CIS",), first
    ) == infer_steward_column_type("co11", ("CIS",), last)
    inferred, provenance = infer_steward_column_type("co11", ("CIS",), first)
    assert inferred == "text"
    assert provenance is not None and "tabell_Cis2016" not in provenance
    assert (
        infer_steward_column_type("co11", ("CIS2004", "CIS2012"), first)[0] == "decimal"
    )
    assert infer_steward_column_type("co11", ("Cis",), first)[0] == "date"
    assert infer_steward_column_type("co11", ("NONE",), first) == (None, None)


def test_date_numeric_mixture_retains_sorted_evidence(tmp_path: Path) -> None:
    declarations = index_swecov_column_types(
        _read(
            tmp_path,
            _csv(
                [
                    ("CIS2004", "A", "date", "BASE TABLE"),
                    ("CIS2002", "a", "int", "VIEW"),
                ]
            ),
        ).declarations
    )
    inferred, evidence = infer_steward_column_type("A", ("CIS",), declarations)
    assert inferred is None
    assert evidence is not None and evidence.endswith("CIS2002=int, CIS2004=date")


def test_date_numeric_text_mixture_stays_untyped(tmp_path: Path) -> None:
    declarations = index_swecov_column_types(
        _read(
            tmp_path,
            _csv(
                [
                    ("CIS2016", "A", "varchar", "VIEW"),
                    ("CIS2004", "a", "date", "BASE TABLE"),
                    ("CIS2002", "A", "int", "VIEW"),
                ]
            ),
        ).declarations
    )
    inferred, evidence = infer_steward_column_type("a", ("CIS",), declarations)
    assert inferred is None
    assert evidence is not None and evidence.endswith(
        "CIS2002=int, CIS2004=date, CIS2016=varchar"
    )
    assert (
        infer_steward_column_type("a", ("CIS2004", "CIS2016"), declarations)[0]
        == "text"
    )
