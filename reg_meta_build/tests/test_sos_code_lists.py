"""Code-list and inline value-set source records from Socialstyrelsen workbooks."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _sos_fixtures import (
    BU_SPEC_LINED,
    BU_SPEC_MEMBERS,
    BU_SPEC_ONE_LINE,
    BU_SPEC_WRAPPED,
    clean_code_rows as _clean_code_rows,
    source_revision as _revision,
    write_complete_workbook as _write_complete_workbook,
    write_source_workbook as _write_source_workbook,
)
from reg_meta_build.sources.sos import SosParseIssue, parse_register_file
from reg_meta_build.sources.sos_records import (
    clean_sos_source,
)

if TYPE_CHECKING:
    from pathlib import Path


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
    assert first.section_locator is not None
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


@pytest.mark.parametrize(
    ("raw", "members", "unresolved"),
    [
        # G71: every assignment on its own line -> the complete three-member list.
        (BU_SPEC_LINED, BU_SPEC_MEMBERS, False),
        # Alignment gaps separate the remaining delivered assignments.
        (BU_SPEC_WRAPPED, BU_SPEC_MEMBERS, False),
        (BU_SPEC_ONE_LINE, BU_SPEC_MEMBERS, False),
    ],
)
def test_wrapped_inline_code_list_keeps_its_original_cell(
    tmp_path: Path, raw: str, members: list[tuple[str, str]], unresolved: bool | None
) -> None:
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Metadata - Variabelnivå"]["E2"] = raw
    workbook.save(path)

    cleaned = clean_sos_source(parse_register_file(path), _revision(path))
    record = next(
        record
        for record in cleaned.records
        if record.locators[0].physical_record == "row:2"
        and record.subject.member.name == "HDIA"
    )
    descriptor_key = f"inline:{record.record_id}"

    # Whatever the classifier decides, the original cell remains the evidence.
    assert record.fields.representation is not None
    assert record.fields.representation.raw_value == raw
    assert (
        next(
            cell for cell in record.delivered_cells if cell.name == "Värdemängd"
        ).raw_value
        == raw
    )
    assert [
        cleaned.values[association.value_key].normalized_content
        for association in cleaned.associations
        if association.descriptor_key == descriptor_key
    ] == members
    if unresolved is None:  # Free text prepares no list.
        assert descriptor_key not in cleaned.descriptors
    else:
        descriptor = cleaned.descriptors[descriptor_key]
        assert descriptor.unresolved_members is unresolved
        # The unresolved descriptor keeps the delivered cell it could not resolve.
        assert descriptor.delivered_cells == record.delivered_cells


def test_formatted_code_section_heading_is_evidence_not_a_value(tmp_path: Path) -> None:
    import openpyxl
    from openpyxl.styles import Font

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    sheet = workbook["Kodlista_HDIA"]
    sheet.append([None, "Description of the following code digits", None])
    sheet["B3"].font = Font(bold=True)
    sheet.append([None, "01-19", "Literal code range"])
    for cell in sheet[4]:
        cell.font = Font(bold=True)
    sheet.append([None, "20", None])
    workbook.save(path)

    parsed = parse_register_file(path)
    cleaned = clean_sos_source(parsed, _revision(path))

    assert [row.kod for row in parsed.kodlistor[0].rows] == ["001", "01-19", "20"]
    assert {value.code for value in cleaned.values.values()} == {"001", "01-19", "20"}
    table = next(table for table in cleaned.tables if table.name == sheet.title)
    assert table.rows[2].role == "section"
    assert (
        table.rows[2].cells[1].raw_value == "Description of the following code digits"
    )


def test_metod_header_delivers_code_labels_without_reordered_inference(
    tmp_path: Path,
) -> None:
    exact = _clean_code_rows(
        tmp_path,
        [
            ["Variabelnamn", "Tidsperiod", "Kod", "Behandlingsmetod"],
            ["HDIA", "2020-", "1", "IVF"],
        ],
    )
    assert [value.normalized_content for value in exact.values.values()] == [
        ("1", "IVF")
    ]
    reordered = _clean_code_rows(
        tmp_path,
        [["Kod", "Behandlingsmetod", "Tidsperiod"], ["1", "IVF", "2020-"]],
    )
    assert [value.normalized_content for value in reordered.values.values()] == [
        ("1", None)
    ]


def test_known_hidden_support_sheet_is_preserved_but_unknown_sheet_blocks(
    tmp_path: Path,
) -> None:
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    support = workbook.create_sheet("Ej relevant_listor")
    support.sheet_state = "veryHidden"
    support.append(["Binär", "Datatyp"])
    support.append(["Ja", "Heltal"])
    workbook.save(path)

    cleaned = clean_sos_source(parse_register_file(path), _revision(path))
    table = next(table for table in cleaned.tables if table.name == support.title)
    assert [row.role for row in table.rows] == ["unparsed", "unparsed"]
    assert [[cell.raw_value for cell in row.cells] for row in table.rows] == [
        ["Binär", "Datatyp"],
        ["Ja", "Heltal"],
    ]
    assert table.rows[1].locator.physical_cells == (
        "Ej relevant_listor!A2",
        "Ej relevant_listor!B2",
    )
    assert len(cleaned.associations) == 1

    support.title = "New source structure"
    workbook.save(path)
    parsed = parse_register_file(path)
    with pytest.raises(
        ValueError, match="New source structure: unrecognized worksheet"
    ):
        clean_sos_source(parsed, _revision(path))
    assert parsed.source_sheets[-1].rows[1].source_evidence.cells[0].raw_value == "Ja"


def test_partial_header_with_code_rows_keeps_binding_target(tmp_path: Path) -> None:
    cleaned = _clean_code_rows(
        tmp_path,
        [["Tidsperiod", "Kod", "Other"], ["2020", "1", "Retained evidence"]],
    )
    assert "sheet:Kodlista_Arbitrary" in cleaned.descriptors
    (association,) = cleaned.associations
    assert cleaned.values[association.value_key].code == "1"
    table = next(
        table for table in cleaned.tables if table.name == "Kodlista_Arbitrary"
    )
    assert table.rows[1].cells[2].raw_value == "Retained evidence"


def test_empty_standard_header_keeps_binding_target(tmp_path: Path) -> None:
    cleaned = _clean_code_rows(
        tmp_path,
        [["Variabelnamn", "HDIA"], ["Tidsperiod", "Kod", "Beskrivning"]],
    )
    descriptor = cleaned.descriptors["sheet:Kodlista_Arbitrary"]
    assert descriptor.member_references == ("HDIA",)
    assert not cleaned.associations and not cleaned.values
    table = next(
        table for table in cleaned.tables if table.name == "Kodlista_Arbitrary"
    )
    assert table.rows[1].role == "header"


@pytest.mark.parametrize(
    ("pointer", "named", "declared", "reference"),
    [
        ("Kodlista_Target!A1", "HDIA", False, False),
        ("Kodlista_Target!", "HDIA", False, False),
        ("Kodlista_Target", "SIBLING", False, False),
        ("Kodlista_Target", None, False, True),
        ("Kodlista_Target!", None, False, True),
        ("Kodlista_Target!A1", None, True, False),
        ("Kodlista_Missing!A1", None, True, False),
        ("kodlista_Target", None, True, False),
    ],
)
def test_sheet_pointer_uses_exact_target_and_named_member_guard(
    tmp_path: Path, pointer: str, named: str | None, declared: bool, reference: bool
) -> None:
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Metadata - Variabelnivå"]["F2"] = pointer
    target = workbook.create_sheet("Kodlista_Target")
    if named:
        target.append(["Variabelnamn", named])
        target.append(["Tidsperiod", "Kod", "Beskrivning"])
        target.append(["2020", "1", "One"])
    else:
        target.append(["KOD", "Beskrivning"])
        target.append(["1", "One"])
    workbook.save(path)

    cleaned = clean_sos_source(parse_register_file(path), _revision(path))
    record = next(
        record
        for record in cleaned.records
        if record.subject.member.name == "HDIA"
        and record.locators[0].physical_record == "row:2"
    )
    declaration = record.fields.classification_declared
    assert (declaration is not None) is declared
    if declaration is not None:
        assert declaration.value == pointer
    assert (
        next(
            cell for cell in record.delivered_cells if cell.name == "Länk kodverk"
        ).raw_value
        == pointer
    )
    descriptor = cleaned.descriptors["sheet:Kodlista_Target"]
    assert ("HDIA" in descriptor.member_references) is (named == "HDIA" or reference)
    pointer_hints = [
        hint for hint in descriptor.member_hints if hint.role == "variable_pointer"
    ]
    assert bool(pointer_hints) is reference
    if reference:
        assert pointer_hints[0].locator.physical_record == "row:2"


def test_sheet_pointer_record_and_descriptor_are_stable_under_sheet_order(
    tmp_path: Path,
) -> None:
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Metadata - Variabelnivå"]["F2"] = "Kodlista_Target"
    target = workbook.create_sheet("Kodlista_Target")
    target.append(["KOD", "Beskrivning"])
    target.append(["1", "One"])
    workbook.save(path)
    revision = _revision(path)
    first = clean_sos_source(parse_register_file(path), revision)
    workbook.move_sheet(target, offset=-3)
    workbook.save(path)
    second = clean_sos_source(parse_register_file(path), revision)
    first_record = next(r for r in first.records if r.subject.member.name == "HDIA")
    second_record = next(r for r in second.records if r.subject.member.name == "HDIA")
    assert first_record == second_record
    assert (
        first.descriptors["sheet:Kodlista_Target"]
        == second.descriptors["sheet:Kodlista_Target"]
    )


def test_sheet_pointer_honors_target_named_in_code_rows(tmp_path: Path) -> None:
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Metadata - Variabelnivå"]["F2"] = "Kodlista_Target!A1"
    target = workbook.create_sheet("Kodlista_Target")
    target.append(["Variabelnamn", "Tidsperiod", "Kod", "Beskrivning"])
    target.append(["HDIA", "2020", "1", "One"])
    workbook.save(path)

    cleaned = clean_sos_source(parse_register_file(path), _revision(path))
    record = next(r for r in cleaned.records if r.subject.member.name == "HDIA")
    assert record.fields.classification_declared is None
    descriptor = cleaned.descriptors["sheet:Kodlista_Target"]
    assert not any(h.role == "variable_pointer" for h in descriptor.member_hints)
    assert any(
        "HDIA" in association.member_references
        for association in cleaned.associations
        if association.descriptor_key == descriptor.payload_key
    )


def test_code_patterns_and_adjacent_legend_stay_literal_and_separate(
    tmp_path: Path,
) -> None:
    cleaned = _clean_code_rows(
        tmp_path,
        [
            ["KOD ", "Beskrivning", "Följande sympoler används", None, None],
            ["1XXXX", "missbildning i CNS", "X: valfri siffra", None, None],
            ["11xxy", "Annan kategori", "x: 1 unilateral", "Räknas ej", "Osäker"],
            [None, None, 1, "A", "K"],
        ],
    )
    assert {value.normalized_content for value in cleaned.values.values()} == {
        ("1XXXX", "missbildning i CNS"),
        ("11xxy", "Annan kategori"),
    }
    assert len(cleaned.associations) == 2
    assert "sheet:Kodlista_Arbitrary" in cleaned.descriptors
    assert all(not row.member_references for row in cleaned.associations)
    assert all(len(value.delivered_cells) == 2 for value in cleaned.values.values())
    table = next(
        table for table in cleaned.tables if table.name == "Kodlista_Arbitrary"
    )
    assert table.rows[1].cells[2].raw_value == "X: valfri siffra"
    assert table.rows[3].cells[2].raw_value == "1"
    assert table.rows[3].cells[3].raw_value == "A"
    assert not cleaned.declarations and not cleaned.validity
