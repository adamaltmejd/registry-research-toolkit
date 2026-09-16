"""Lossless source-record boundary for Socialstyrelsen workbooks."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceCoordinate,
    SourceRevision,
    value_field,
)
from reg_meta_build.sources.sos import SosParseIssue, parse_register_file
from reg_meta_build.sources.sos_records import (
    clean_sos_source,
    iter_sos_variable_records,
)

if TYPE_CHECKING:
    from pathlib import Path


_CLASSIFICATION_URL = "https://example.test/classifications/ssyk"


def _write_source_workbook(path: Path) -> None:
    import openpyxl
    from openpyxl.worksheet.hyperlink import Hyperlink

    workbook = openpyxl.Workbook()
    general = workbook.active
    general.title = "Generell information"
    general.append(["", "Om datamängden version", None])
    general.append(["", "Datamängd", "Patientregistret källa"])
    general.append(["", "Version", "2026:1"])

    variables = workbook.create_sheet("Metadata - Variabelnivå")
    variables.append(
        [
            "Deldatamängdsnamn",
            "Variabelnamn",
            "Variabeletikett",
            "Variabelbeskrivning",
            "Värdemängd",
            "Länk kodverk",
            "Datatyp",
            "Data från",
            "Data till",
            "Specificera källa",
            "Eget källfält",
        ]
    )
    base = [
        "PAR_OV",
        "HDIA",
        " Huvuddiagnos ",
        "Första raden\r\nandra raden  ",
        "Se kodlista",
        _CLASSIFICATION_URL,
        "Heltal",
        2001,
        2020,
        "Patientregistret",
        "bevaras",
    ]
    variables.append(base)
    variables.append(base)
    variables.append([*base[:2], "Annan etikett", *base[3:]])
    variables.append([*base[:7], "2001", 2020, *base[9:]])
    for row_number in range(2, 6):
        variables[f"F{row_number}"].hyperlink = _CLASSIFICATION_URL
    variables.append(
        [
            "PAR_OV",
            "PARTIELL",
            "Partiell",
            None,
            None,
            None,
            "Sträng (text)",
            2010,
            None,
            None,
            None,
        ]
    )
    variables["F6"] = "Visad kodlista"
    variables["F6"].hyperlink = Hyperlink(
        ref="F6",
        location="'Kodlista_HDIA'!A1",
        display="Visad kodlista",
    )
    variables.append(
        [
            "PAR_OV",
            "MALFORMED",
            "Malformed",
            None,
            None,
            None,
            "Heltal",
            "+2001",
            "2020",
            None,
            None,
        ]
    )
    variables.append(
        [
            None,
            "UTAN_DEL",
            "Utan deldatamängd",
            None,
            None,
            None,
            "Datum",
            None,
            None,
            None,
            None,
        ]
    )

    codes = workbook.create_sheet("Kodlista_HDIA")
    codes.append(["Tidsperiod", "Kod", "Beskrivning"])
    codes.append(["2001-2020", 1, "Kod ett"])
    codes["B2"].number_format = "000"
    workbook.save(path)


def _revision(path: Path) -> SourceRevision:
    payload = path.read_bytes()
    return SourceRevision.create(
        dataset="sos-metadata",
        publisher="Socialstyrelsen",
        purpose="Socialstyrelsen source-record fixture",
        upstream_revision="2026:1",
        artifact_path=path.name,
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def test_parser_retains_xlsx_row_cells_and_code_display_provenance(
    tmp_path: Path,
) -> None:
    path = tmp_path / "Metadata Patientregistret (PAR)_webb.xlsx"
    _write_source_workbook(path)

    register = parse_register_file(path)

    evidence = register.variables[0].source_evidence
    assert evidence is not None
    assert evidence.sheet_name == "Metadata - Variabelnivå"
    assert evidence.row_number == 2
    coverage_start = next(
        cell for cell in evidence.cells if cell.field_name == "data_from"
    )
    assert (
        coverage_start.coordinate,
        coverage_start.raw_value,
        coverage_start.data_type,
        coverage_start.number_format,
        coverage_start.display_value,
    ) == ("H2", 2001, "n", "General", "2001")
    assert next(
        cell for cell in evidence.cells if cell.header == "Eget källfält"
    ).raw_value == ("bevaras")
    classification = next(
        cell for cell in evidence.cells if cell.field_name == "external_classification"
    )
    assert (
        classification.coordinate,
        classification.raw_value,
        classification.data_type,
        classification.number_format,
        classification.hyperlink_target,
    ) == ("F2", _CLASSIFICATION_URL, "s", "General", _CLASSIFICATION_URL)

    internal_link = next(
        cell
        for cell in register.variables[4].source_evidence.cells
        if cell.field_name == "external_classification"
    )
    assert (
        internal_link.raw_value,
        internal_link.hyperlink_target,
        internal_link.hyperlink_location,
    ) == ("Visad kodlista", None, "'Kodlista_HDIA'!A1")

    code = register.kodlistor[0].rows[0]
    assert code.kod == "001"
    assert code.source_evidence is not None
    code_cell = next(
        cell for cell in code.source_evidence.cells if cell.field_name == "kod"
    )
    assert (
        code_cell.coordinate,
        code_cell.raw_value,
        code_cell.data_type,
        code_cell.number_format,
        code_cell.display_value,
    ) == ("B2", 1, "n", "000", "001")


def test_source_records_keep_occurrences_conflicts_and_native_sos_coordinates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "Metadata Patientregistret (PAR)_webb.xlsx"
    _write_source_workbook(path)
    register = parse_register_file(path)

    records = tuple(iter_sos_variable_records(register, _revision(path)))

    assert len(records) == 7
    first, duplicate, conflict, storage_variant, partial, malformed, no_subset = records
    assert first.record_id == duplicate.record_id
    assert first.locators[0].physical_record == "row:2"
    assert duplicate.locators[0].physical_record == "row:3"
    assert conflict.record_id != first.record_id
    assert storage_variant.fields.model_dump(
        exclude={"coverage_from"}
    ) == first.fields.model_dump(exclude={"coverage_from"})
    assert (
        storage_variant.fields.coverage_from.value
        == first.fields.coverage_from.value
        == "2001"
    )
    assert storage_variant.fields.coverage_from.raw_value == "2001"
    assert first.fields.coverage_from.raw_value == 2001
    assert storage_variant.edition_scope == first.edition_scope
    assert storage_variant.record_id != first.record_id
    first_start = next(
        cell for cell in first.delivered_cells if cell.name == "Data från"
    )
    variant_start = next(
        cell for cell in storage_variant.delivered_cells if cell.name == "Data från"
    )
    assert first_start.raw_type == "int"
    assert variant_start.raw_type == "str"
    assert first.locators[0].semantic_record_key == (
        "register:Patientregistret källa",
        "deldatamangd:PAR_OV",
        "variable:HDIA",
    )
    assert first.locators[0].physical_file == path.name
    assert first.locators[0].physical_table == "Metadata - Variabelnivå"
    assert "Metadata - Variabelnivå!B2" in first.locators[0].physical_cells

    assert first.subject.register_name.name == "Patientregistret källa"
    assert first.subject.variant.name == "PAR_OV"
    assert first.subject.variable == SourceCoordinate(
        status="value", native_id="HDIA", name="Huvuddiagnos"
    )
    assert conflict.subject.variable.native_id == first.subject.variable.native_id
    assert conflict.subject.variable.name == "Annan etikett"
    assert first.subject.member.name == "HDIA"
    assert first.subject.native == NativeCoordinates()
    assert first.fields.column_name == value_field("HDIA")
    assert first.fields.name == value_field("Huvuddiagnos", raw=" Huvuddiagnos ")
    assert first.fields.description == value_field(
        "Första raden\nandra raden", raw="Första raden\nandra raden  "
    )
    assert first.fields.data_type == value_field("integer", raw="Heltal")
    assert first.fields.representation == value_field("Se kodlista")
    assert first.fields.source_attribution == value_field("Patientregistret")
    assert first.fields.reference_period is None
    classification_cell = next(
        cell for cell in first.delivered_cells if cell.name == "Länk kodverk"
    )
    assert (
        classification_cell.raw_type,
        classification_cell.storage_type,
        classification_cell.number_format,
        classification_cell.hyperlink_target,
    ) == ("str", "s", "General", _CLASSIFICATION_URL)
    assert first.edition_scope.kind == "intervals"
    assert first.edition_scope.intervals[0].start == "2001"
    assert first.edition_scope.intervals[0].end == "2020"
    assert first.edition_period_scope.kind == "not_applicable"

    assert partial.edition_scope.kind == "unknown"
    assert partial.edition_scope.label == "Data från=2010; Data till=<blank>"
    partial_link = next(
        cell for cell in partial.delivered_cells if cell.name == "Länk kodverk"
    )
    assert (
        partial_link.raw_value,
        partial_link.hyperlink_target,
        partial_link.hyperlink_location,
    ) == ("Visad kodlista", None, "'Kodlista_HDIA'!A1")
    assert malformed.edition_scope.kind == "unknown"
    assert malformed.edition_scope.label == "Data från=+2001; Data till=2020"
    assert no_subset.subject.variant.status == "unknown"
    assert no_subset.subject.variable.native_id == "UTAN_DEL"
    assert no_subset.locators[0].semantic_record_key[1] == "deldatamangd:<unknown>"
    assert no_subset.edition_scope.kind == "unknown"
    assert not any(
        "_default" in part for part in no_subset.locators[0].semantic_record_key
    )

    blank_subset = next(
        cell for cell in no_subset.delivered_cells if cell.name == "Deldatamängdsnamn"
    )
    assert (
        blank_subset.present,
        blank_subset.raw_value,
        blank_subset.interpreted_value,
    ) == (
        True,
        "",
        "",
    )


def _write_complete_workbook(path: Path) -> None:
    import openpyxl

    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    dcat = workbook.create_sheet("Metadata-Datamängd (DCAT-AP)")
    dcat.append(["Attribut", "Definition", "Svenska", "Engelska"])
    dcat.append(["Titel", None, "Första titeln", "First title"])
    dcat.append(["Titel", None, "Andra titeln", "Second title"])
    dcat.append(["Beskrivning", None, "  indragen rad\n    tabell  kolumn", None])
    dcat.append(["Okänt attribut", None, "Obehandlad uppgift", "Unmapped fact"])
    subsets = workbook.create_sheet("Deldatamängder")
    subsets.append(
        ["Deldatamängdsnamn", "Deldatamängdsetikett", "Data från", "Data till"]
    )
    subsets.append(["PAR_OV", "Öppenvård", 1900, 2025])
    codes = workbook["Kodlista_HDIA"]
    codes.delete_rows(1, codes.max_row)
    codes.append(["Variabelnamn", "HDIA"])
    codes.append(["Variabelnamn", "ANNAN_VAR"])
    codes.append(["Tidsperiod", "Kod", "Beskrivning (kliniknamn)"])
    codes.append(["2010-2012", None, None])
    for description in ["Klinik ett", "Klinik ett", "Annan klinik"]:
        codes.append([None, 1, description])
        codes.cell(codes.max_row, 2).number_format = "000"
    raw = workbook.create_sheet("Kodlista_RAW")
    raw.append(["Sjukhus", "Adress"])
    raw.append(["Klinik", "Gatan 1"])
    workbook.save(path)
    workbook.close()


def test_common_parent_metadata_preserves_languages_conflicts_and_raw_context(
    tmp_path: Path,
) -> None:
    path = tmp_path / "Metadata Test.xlsx"
    _write_complete_workbook(path)
    cleaned = clean_sos_source(parse_register_file(path), _revision(path))

    parents = [
        record
        for record in cleaned.records
        if record.subject.member.status == "not_applicable"
    ]
    assert all(record.subject.variable.status == "not_applicable" for record in parents)
    titles = [record for record in parents if record.language and record.fields.name]
    assert {(record.language, record.fields.name.value) for record in titles} == {
        ("sv", "Första titeln"),
        ("sv", "Andra titeln"),
        ("en", "First title"),
        ("en", "Second title"),
    }
    swedish = [record for record in titles if record.language == "sv"]
    assert (
        swedish[0].locators[0].semantic_record_key
        == swedish[1].locators[0].semantic_record_key
    )
    assert swedish[0].record_id != swedish[1].record_id
    description = next(
        record.fields.description
        for record in parents
        if record.language == "sv" and record.fields.description
    )
    assert description.value == "  indragen rad\n    tabell  kolumn"
    subset = next(
        record for record in parents if record.subject.variant.name == "PAR_OV"
    )
    assert subset.edition_scope.intervals[0].start == "1900"
    partial = next(
        record for record in cleaned.records if record.subject.member.name == "PARTIELL"
    )
    assert partial.edition_scope.kind == "unknown"
    assert partial.fields.coverage_from.value == "2010"
    assert partial.fields.coverage_to.status == "unknown"
    dcat = next(
        table
        for table in cleaned.tables
        if table.name == "Metadata-Datamängd (DCAT-AP)"
    )
    assert any(
        cell.raw_value == "Unmapped fact" for row in dcat.rows for cell in row.cells
    )


def test_common_values_keep_duplicate_associations_competing_labels_and_period_origins(
    tmp_path: Path,
) -> None:
    path = tmp_path / "Metadata Test.xlsx"
    _write_complete_workbook(path)
    cleaned = clean_sos_source(parse_register_file(path), _revision(path))

    assert len(cleaned.values) == 2
    assert {value.normalized_content for value in cleaned.values.values()} == {
        ("001", "Klinik ett"),
        ("001", "Annan klinik"),
    }
    first, duplicate, conflict = cleaned.associations
    assert first.value_key == duplicate.value_key != conflict.value_key
    assert [association.row_number for association in cleaned.associations] == [5, 6, 7]
    assert first.supplied_period is None
    assert first.section_period == "2010-2012"
    assert first.section_locator.physical_record == "row:4"
    descriptor = cleaned.descriptors["sheet:Kodlista_HDIA"]
    assert [(hint.role, hint.value) for hint in descriptor.member_hints] == [
        ("sheet_suffix", "HDIA"),
        ("list_header", "HDIA"),
        ("list_header", "ANNAN_VAR"),
    ]
    assert len(cleaned.values[first.value_key].locators) == 2
    raw = next(table for table in cleaned.tables if table.name == "Kodlista_RAW")
    assert all(row.role == "unparsed" for row in raw.rows)
    assert not any(
        association.descriptor_key == "sheet:Kodlista_RAW"
        for association in cleaned.associations
    )
    assert all(not record.code_set_references for record in cleaned.records)


def test_source_cleaning_blocks_on_parser_failure_with_original_evidence_available(
    tmp_path: Path,
) -> None:
    path = tmp_path / "Metadata Test.xlsx"
    _write_complete_workbook(path)
    parsed = parse_register_file(path)
    failed = replace(
        parsed,
        parse_issues=(
            SosParseIssue("Kodlista_HDIA", "parse_failure", "bad code layout"),
        ),
    )

    with pytest.raises(ValueError, match="Kodlista_HDIA: bad code layout"):
        clean_sos_source(failed, _revision(path))
    assert failed.source_sheets == parsed.source_sheets
