"""Register and subset parent records from Socialstyrelsen workbooks."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _sos_fixtures import (
    source_revision as _revision,
    write_complete_workbook as _write_complete_workbook,
    write_source_workbook as _write_source_workbook,
)
from reg_meta_build.catalog_resolution import resolve_parents
from reg_meta_build.source_coordinates import native_parent_key, source_register_key
from reg_meta_build.source_curation import record_ref
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import (
    SourceFields,
)
from reg_meta_build.sources.sos import parse_register_file
from reg_meta_build.sources.sos_records import (
    clean_sos_source,
)

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def test_lova_repeated_subset_token_keys_labels_separately(tmp_path: Path) -> None:
    import openpyxl

    path = tmp_path / "Metadata LOVA (LOVA).xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    subsets = workbook.create_sheet("Deldatamängder och datavyer")
    subsets.append(
        [
            "Deldatamängdsetikett",
            "Deldatamängdsnamn",
            "Deldatamängdsbeskrivning",
            "Data från",
            "Data till",
            "Uppdateringsfrekvens",
            "Aggregeringsnivå",
            "Kommentar",
        ]
    )
    labels = (
        "Legitimerade omsorgs- och vårdyrkesgruppers ekonomi och arbetsmarknadssituation",
        "Legitimerade omsorgs- och vårdyrkesgruppers arbetsmarknadsstatus",
    )
    subsets.append(
        [
            labels[0],
            "LOVA",
            "Arbetsmarknadsstatus och vissa ekomoniska uppgifter",
            1995,
            None,
            None,
            "Individ",
            None,
        ]
    )
    for _ in range(12):
        subsets.append([None] * 8)
    subsets.append(
        [
            labels[1],
            "LOVA",
            "Uppgifter om innehavare",
            1995,
            None,
            None,
            "Individ",
            "Huvudtabell som samanställer uppgifter från flera källar",
        ]
    )
    workbook.save(path)
    workbook.close()

    records = clean_sos_source(parse_register_file(path), _revision(path)).records
    parents = [
        record
        for record in records
        if any(fact.kind == "variant" for fact in record.parent_facts)
    ]
    assert len(parents) == 2
    assert {record.locators[0].physical_record for record in parents} == {
        "row:2",
        "row:15",
    }
    assert len({record_ref(record) for record in parents}) == 2
    assert {record.subject.variant.name for record in parents} == {
        f"LOVA / {label}" for label in labels
    }

    names = []
    seen = set()
    for record in records:
        for parent in record.parent_facts:
            if parent.kind not in {"register", "variant"}:
                continue
            key = native_parent_key(record.source, "sos", parent)
            assert key is not None
            if key in seen:
                continue
            seen.add(key)
            kind = "register" if parent.kind == "register" else "register_variant"
            names.append(
                NamingDeclaration(
                    target=NativeNamingTarget(
                        kind=kind,
                        provider="sos",
                        source_key=key,
                        register_key=source_register_key(record)
                        if parent.kind == "variant"
                        else None,
                    ),
                    naming=SlugEntry(
                        kind=kind,
                        provider="sos",
                        source_id="1" if kind == "register" else f"1.{len(names)}",
                        slug="lova" if kind == "register" else f"view-{len(names)}",
                    ),
                    contributors=(),
                )
            )
    resolved = resolve_parents(records, tuple(names))
    assert len(resolved.variants) == 2
    assert not any(
        issue.code == "unknown_parent_name" for issue in resolved.diagnostics
    )


@pytest.mark.parametrize(
    ("code", "register_name", "sheet_name", "headers", "rows", "fields"),
    [
        (
            "PAR",
            "Patientregistret",
            "Deldatamängder och datavyer",
            ("Deldatamängdsnamn", "Deldatamängdsetikett", "Data från", "Data till"),
            (("PAR_OV", "Öppenvård", 2001, 2020), ("PAR_SV", "Slutenvård", 1987, 2020)),
            "coverage_from,coverage_to,name",
        ),
        (
            "MFR",
            "Medicinska födelseregistret",
            "Deldatamängder",
            (
                "Deldatamängdsnamn",
                "Deldatamängdsetikett",
                "Deldatamängdsbeskrivning",
                "Aggregeringsnivå",
            ),
            (("MFR_BARN", "Barn", "Uppgifter om barn", "Individ"),),
            "aggregation_level,description,name",
        ),
        (
            "DORS",
            "Dödsorsaksregistret",
            "Deldatamängder och datavyer",
            ("Deldatamängdsetikett", "Deldatamängdsnamn", "Uppdateringsfrekvens"),
            (
                ("Dödsorsaker", "DORS", "Årligen"),
                ("Covid-19 Hermes", "COV_DORS_HERMES", "Månadsvis"),
            ),
            "name,update_frequency",
        ),
    ],
)
def test_other_sos_workbooks_keep_token_only_subset_keys(
    tmp_path: Path,
    code: str,
    register_name: str,
    sheet_name: str,
    headers: tuple[str, ...],
    rows: tuple[tuple[str | int, ...], ...],
    fields: str,
) -> None:
    import openpyxl

    path = tmp_path / f"Metadata {code} ({code}).xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Generell information"]["C2"] = register_name
    subsets = workbook.create_sheet(sheet_name)
    subsets.append(headers)
    for row in rows:
        subsets.append(row)
    workbook.save(path)
    workbook.close()

    records = clean_sos_source(parse_register_file(path), _revision(path)).records
    parents = [
        record
        for record in records
        if any(fact.kind == "variant" for fact in record.parent_facts)
    ]
    name_index = headers.index("Deldatamängdsnamn")
    tokens = tuple(str(row[name_index]) for row in rows)
    assert len(parents) == len(tokens)
    assert {record_ref(parent).semantic_record_key for parent in parents} == {
        (
            f"register:{register_name}",
            "metadata:subsets",
            f"subset:{token}",
            f"fields:{fields}",
            "language:<not declared>",
        )
        for token in tokens
    }
    assert {
        native_parent_key(parent.source, "sos", parent.parent_facts[0])
        for parent in parents
    } == {
        (
            "sos-metadata",
            "sos",
            "register",
            "name",
            register_name,
            "variant",
            "name",
            token,
        )
        for token in tokens
    }


def test_styrtabell_parent_keeps_both_lookup_signals(tmp_path: Path) -> None:
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_complete_workbook(path)
    workbook = openpyxl.load_workbook(path)
    sheet = workbook["Deldatamängder"]
    sheet["B2"] = "Styrtabell för diagnoser"
    sheet["E1"] = "Aggregeringsnivå"
    sheet["E2"] = "Ej relevant"
    workbook.save(path)
    workbook.close()
    cleaned = clean_sos_source(parse_register_file(path), _revision(path))
    parent = next(
        fact
        for record in cleaned.records
        for fact in record.parent_facts
        if fact.kind == "variant"
        and fact.variant is not None
        and fact.variant.name == "PAR_OV"
    )
    assert parent.fields.name is not None
    assert parent.fields.aggregation_level is not None
    assert parent.fields.name.value == "Styrtabell för diagnoser"
    assert parent.fields.aggregation_level.value == "Ej relevant"


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
    assert all(record.fields.name is None for record in parents)
    titles = [
        record
        for record in parents
        if record.language and record.parent_facts[0].fields.name
    ]
    observed_titles = set()
    for record in titles:
        name = record.parent_facts[0].fields.name
        assert name is not None
        observed_titles.add((record.language, name.value))
    assert observed_titles == {
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
        record.parent_facts[0].fields.description
        for record in parents
        if record.language == "sv" and record.parent_facts[0].fields.description
    )
    assert description.value == "  indragen rad\n    tabell  kolumn"
    subset = next(
        record for record in parents if record.subject.variant.name == "PAR_OV"
    )
    assert subset.edition_scope.intervals[0].start == "1900"
    assert subset.parent_facts[0].kind == "variant"
    assert subset.parent_field_locators(0, "coverage_from")[0].physical_cells == (
        "Deldatamängder!C2",
    )
    assert swedish[0].parent_field_locators(0, "name")[0].physical_cells == (
        "Metadata-Datamängd (DCAT-AP)!C2",
    )
    partial = next(
        record for record in cleaned.records if record.subject.member.name == "PARTIELL"
    )
    assert partial.edition_scope.kind == "intervals"
    assert tuple((i.start, i.end) for i in partial.edition_scope.intervals) == (
        ("2010", None),
    )
    assert partial.fields.coverage_from is not None
    assert partial.fields.coverage_to is not None
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


def _register_parent_records(cleaned) -> list:
    return [
        record
        for record in cleaned.records
        if record.subject.member.status == "not_applicable"
        and record.parent_facts[0].kind == "register"
    ]


def test_sos_contact_cells_remain_evidence_without_parent_field(tmp_path: Path) -> None:
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    _write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Generell information"].append([None, "E-post", "Rela@x.se"])
    dcat = workbook.create_sheet("Metadata-Datamängd (DCAT-AP)")
    dcat.append(["Attribut", "Definition", "Svenska", "Engelska"])
    dcat.append(["Titel", None, "Patientregistret", None])
    dcat.append(["Kontaktuppgift", None, "ReLa@x.se", None])
    workbook.save(path)

    cleaned = clean_sos_source(parse_register_file(path), _revision(path))
    assert "contact" not in SourceFields.model_fields
    for table_name, raw in (
        ("Generell information", "Rela@x.se"),
        ("Metadata-Datamängd (DCAT-AP)", "ReLa@x.se"),
    ):
        table = next(table for table in cleaned.tables if table.name == table_name)
        assert any(cell.raw_value == raw for row in table.rows for cell in row.cells)


def test_sos_register_name_prefers_dcat_title_over_dataset_label(
    tmp_path: Path,
) -> None:
    # _write_complete_workbook delivers a general Datamängd
    # ("Patientregistret källa") that differs from the DCAT-AP Titel rows, the
    # LSS/HSL/SOL shape that used to raise conflicting_parent_metadata.
    path = tmp_path / "Metadata Test.xlsx"
    _write_complete_workbook(path)
    cleaned = clean_sos_source(parse_register_file(path), _revision(path))

    registers = _register_parent_records(cleaned)
    general = [record for record in registers if record.language is None]
    assert general
    # No general-sheet observation competes for the register name anymore.
    assert all(record.parent_facts[0].fields.name is None for record in general)
    dataset = next(
        record
        for record in general
        if record.parent_facts[0].fields.dataset_label is not None
    )
    assert dataset.parent_facts[0].fields.dataset_label is not None
    assert dataset.parent_facts[0].fields.dataset_label.value == (
        "Patientregistret källa"
    )
    assert dataset.parent_field_locators(0, "dataset_label")[0].physical_cells == (
        "Generell information!C2",
    )
    titles = {
        record.parent_facts[0].fields.name.value
        for record in registers
        if record.language == "sv" and record.parent_facts[0].fields.name is not None
    }
    assert titles == {"Första titeln", "Andra titeln"}


def test_sos_register_name_falls_back_to_dataset_without_dcat_sheet(
    tmp_path: Path,
) -> None:
    # _write_source_workbook has no DCAT sheet (the LOVA shape): Datamängd
    # remains the only name observation and keeps resolving as before.
    path = tmp_path / "Metadata Patientregistret (PAR)_webb.xlsx"
    _write_source_workbook(path)
    parsed = parse_register_file(path)
    assert not any(sheet.kind == "dcat" for sheet in parsed.source_sheets)
    cleaned = clean_sos_source(parsed, _revision(path))

    registers = _register_parent_records(cleaned)
    dataset = next(
        record
        for record in registers
        if record.language is None and record.parent_facts[0].fields.name is not None
    )
    assert dataset.parent_facts[0].fields.name.value == "Patientregistret källa"
    assert dataset.parent_facts[0].fields.dataset_label is None
