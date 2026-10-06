"""Lossless source-record boundary for Socialstyrelsen workbooks."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _sos_fixtures import (
    CLASSIFICATION_URL as _CLASSIFICATION_URL,
    source_revision as _revision,
    write_source_workbook as _write_source_workbook,
)
from reg_meta.source_evidence import DeliveredCell, RecordLocator
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.sos import SosParseError, parse_register_file
from reg_meta_build.sources.sos_records import (
    clean_sos_source,
    clean_sos_variable,
    iter_sos_variable_records,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("header", "field"),
    [
        ("Variabelnamn", "name"),
        (" variabelNAMN ", "name"),
        ("Datavynamn", "deldatamangd"),
        ("Data från", "data_from"),
        ("Datatyp", "data_type"),
    ],
)
def test_variable_headers_reject_duplicate_semantic_fields(
    tmp_path: Path, header: str, field: str
) -> None:
    import openpyxl

    path = tmp_path / "source.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    sheet = workbook["Metadata - Variabelnivå"]
    sheet["L1"], sheet["L2"] = header, "SECOND_VALUE"
    workbook.save(path)
    original = path.read_bytes()

    with pytest.raises(SosParseError, match=f"ambiguous headers for '{field}'"):
        parse_register_file(path)
    assert path.read_bytes() == original


def test_absent_formula_cache_does_not_change_existing_source_payloads() -> None:
    original = {
        "name": "Kod",
        "present": True,
        "raw_value": "001",
        "interpreted_value": "001",
        "raw_type": "str",
        "storage_type": "s",
        "number_format": "General",
        "hyperlink_target": None,
        "hyperlink_location": None,
    }
    assert DeliveredCell.model_validate(original).model_dump(mode="json") == original
    for raw_value, raw_type in [("", "str"), ("", "none"), (" ", "str")]:
        with_cache = {
            **original,
            "cached_raw_value": raw_value,
            "cached_raw_type": raw_type,
        }
        assert (
            DeliveredCell.model_validate(with_cache).model_dump(mode="json")
            == with_cache
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

    linked_evidence = register.variables[4].source_evidence
    assert linked_evidence is not None
    internal_link = next(
        cell
        for cell in linked_evidence.cells
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
    assert storage_variant.fields.coverage_from is not None
    assert first.fields.coverage_from is not None
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

    assert partial.edition_scope.kind == "intervals"
    assert tuple((i.start, i.end) for i in partial.edition_scope.intervals) == (
        ("2010", None),
    )
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


def test_sos_workbook_flags_claim_identifier_from_kopplingsvariabel(
    tmp_path: Path,
) -> None:
    import openpyxl

    path = tmp_path / "Metadata Patientregistret (PAR)_webb.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    # Column L is Kopplingsvariabel: row 2 stays a delivered blank while row 6
    # carries a linkage marker.
    workbook["Metadata - Variabelnivå"]["L6"] = "X"
    workbook.save(path)

    register = parse_register_file(path)
    revision = _revision(path)
    blank = clean_sos_variable(register, register.variables[0], revision)
    marked = clean_sos_variable(
        register,
        next(
            variable for variable in register.variables if variable.name == "PARTIELL"
        ),
        revision,
    )
    # Every SOS workbook variable is explicitly sensitive health and
    # social-services microdata.
    for record in (blank, marked):
        sensitivity = record.fields.sensitivity
        assert sensitivity is not None
        assert sensitivity.status == "value"
        assert sensitivity.value is True
    # A delivered blank Kopplingsvariabel is an explicit False identifier
    # claim; a non-blank marker is an explicit True claim.
    assert blank.fields.identifier is not None
    assert blank.fields.identifier.status == "value"
    assert blank.fields.identifier.value is False
    assert marked.fields.identifier is not None
    assert marked.fields.identifier.status == "value"
    assert marked.fields.identifier.value is True
    assert marked.fields.identifier.raw_value == "X"
    # The claimed cell stays among the delivered cells, so the claim is
    # traceable to its workbook coordinate.
    blank_cell = next(
        cell for cell in blank.delivered_cells if cell.name == "Kopplingsvariabel"
    )
    assert (blank_cell.present, blank_cell.raw_value) == (True, "")
    marked_cell = next(
        cell for cell in marked.delivered_cells if cell.name == "Kopplingsvariabel"
    )
    assert (marked_cell.present, marked_cell.raw_value) == (True, "X")


def test_sos_flag_claims_form_without_unresolved_flag(tmp_path: Path) -> None:
    from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
    from reg_meta_build.source_coding import resolve_code_membership
    from reg_meta_build.source_coordinates import (
        native_column_key,
        native_variant_key,
    )
    from reg_meta_build.source_formation import form_native_variable

    path = tmp_path / "Metadata Patientregistret (PAR)_webb.xlsx"
    _write_source_workbook(path)
    register = parse_register_file(path)
    sos_record = clean_sos_variable(register, register.variables[0], _revision(path))
    flags = SourceFields(
        sensitivity=sos_record.fields.sensitivity,
        identifier=sos_record.fields.identifier,
    )
    occurrence = SourceRecord.create(
        revision=_revision(path),
        locators=(
            RecordLocator(
                semantic_record_key=("variable:4", "year:2020"),
                physical_file=path.name,
                physical_table="input",
                physical_record="row:2020",
                physical_cells=(),
            ),
        ),
        subject=SourceSubject(
            provider="sos",
            register=SourceCoordinate(status="value", native_id=1, name="Example"),
            variant=SourceCoordinate(status="value", native_id=2, name="People"),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(
                status="value", native_id=4, name="Source variable"
            ),
            member=SourceCoordinate(status="value", native_id=2020),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            availability=value_field(True),
            name=value_field("Source variable"),
            definition=value_field("Source definition"),
            column_name=value_field("VALUE"),
            data_type=value_field("integer"),
        ),
    )
    variant_key = native_variant_key(occurrence)
    assert variant_key is not None
    column_key = native_column_key(occurrence)
    assert column_key is not None
    result = form_native_variable(
        (occurrence,),
        register=ResolvedRegister(provider="sos", slug="example", name="Example"),
        variants={variant_key: ResolvedVariant(slug="people", name="People")},
        slug="value",
        provider_key="4",
        flags=flags,
        coding={column_key: resolve_code_membership(())},
    )
    assert result.variable is not None
    assert (result.variable.is_sensitive, result.variable.is_identifier) == (
        True,
        False,
    )
    assert not any(
        diagnostic.code == "unresolved_flag" for diagnostic in result.diagnostics
    )


def test_formula_and_delivered_cache_survive_without_promoting_computed_facts(
    tmp_path: Path,
) -> None:
    import xml.etree.ElementTree as ET
    import zipfile

    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Metadata - Variabelnivå"]["C2"] = '="Computed label"'
    del workbook["Kodlista_HDIA"]
    sheet = workbook.create_sheet("Kodlista_HOSPITAL")
    sheet.append(
        [
            "Variabelnamn",
            "Från",
            "Till",
            "Sjukhuskod",
            "Region",
            "Sjukhusnamn",
            "Aktuella",
            "Kommentar",
        ]
    )
    formula = '=IF(C2>1, " ","JA")'
    sheet.append(
        ["SJUKHUS", 1973, 1980, "10010", "Stockholm", "Sjukhus", formula, None]
    )
    workbook.save(path)

    # openpyxl writes formula text but cannot populate a delivered formula cache.
    namespace = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as source:
        members = {name: source.read(name) for name in source.namelist()}
    for name, payload in members.items():
        if not name.startswith("xl/worksheets/") or not name.endswith(".xml"):
            continue
        document = ET.fromstring(payload)
        for cell in document.findall(".//s:c", namespace):
            if cell.find("s:f", namespace) is None:
                continue
            cell.set("t", "str")
            cached = cell.find("s:v", namespace)
            assert cached is not None
            cached.text = " " if cell.get("r") == "G2" else "Computed label"
        members[name] = ET.tostring(document)
    with zipfile.ZipFile(path, "w") as output:
        for name, payload in members.items():
            output.writestr(name, payload)

    cleaned = clean_sos_source(parse_register_file(path), _revision(path))
    (association,) = cleaned.associations
    delivered = association.delivered_cells[6]
    assert (
        delivered.raw_value,
        delivered.storage_type,
        delivered.interpreted_value,
    ) == (formula, "f", "")
    assert (delivered.cached_raw_value, delivered.cached_raw_type) == (" ", "str")
    assert cleaned.values[association.value_key].code == "10010"
    variable = next(
        record
        for record in cleaned.records
        if record.subject.member.name == "HDIA"
        and record.locators[0].physical_record == "row:2"
    )
    assert variable.fields.name is not None
    assert variable.fields.name.status == "unknown"
    assert variable.fields.name.raw_value == '="Computed label"'
    assert variable.subject.variable.name is None
    assert variable.delivered_cells[2].cached_raw_value == "Computed label"
