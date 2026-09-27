"""LISA workbook sensitivity declarations and person-table defaults."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from _lisa_fixtures import write_lisa_workbook
from openpyxl import load_workbook
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    SourceFields,
    SourceRecord,
    SourceRevision,
    value_field,
)
from reg_meta_build.sources.lisa import read_lisa_source

if TYPE_CHECKING:
    from pathlib import Path


def _read(path: Path) -> tuple[SourceRecord, ...]:
    payload = path.read_bytes()
    revision = SourceRevision.create(
        dataset="lisa-fixture",
        publisher="SCB",
        purpose="sensitivity test",
        upstream_revision="fixture",
        artifact_path=path.name,
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )
    return read_lisa_source(path, revision).records


def test_lisa_sensitivity_declarations_keep_raw_evidence(tmp_path: Path) -> None:
    records = _read(write_lisa_workbook(tmp_path / "lisa.xlsx"))
    by_column = {
        record.fields.column_name.value: record
        for record in records
        if record.locators[0].physical_table == "Individ"
    }

    assert by_column["KU2YrkStalln"].fields.sensitivity == value_field(
        True, raw="I vissa fall"
    )
    assert by_column["RekrBidr"].fields.sensitivity == value_field(True, raw="Ja")
    assert by_column["AmPolTyp"].fields.sensitivity == value_field(False, raw="Nej")

    conditional = by_column["KU2YrkStalln"].model_copy(
        update={
            "fields": by_column["KU2YrkStalln"].fields.model_copy(
                update={"name": value_field("Yrkesställning")}
            )
        }
    )
    occurrence = source_occurrence(conditional)
    assert occurrence.variant_key is not None
    assert occurrence.column_key is not None
    result = form_native_variable(
        (conditional,),
        register=ResolvedRegister(provider="scb", slug="lisa", name="LISA"),
        variants={
            occurrence.variant_key: ResolvedVariant(slug="individual", name="Individ")
        },
        slug="ku2yrkstalln",
        provider_key="KU2YrkStalln",
        flags=SourceFields(
            sensitivity=conditional.fields.sensitivity,
            identifier=value_field(False),
        ),
        coding={occurrence.column_key: resolve_code_membership(())},
    )
    assert result.variable is not None
    assert result.variable.is_sensitive is True


def test_lisa_undeclared_sensitivity_defaults_only_for_person_tables(
    tmp_path: Path,
) -> None:
    path = write_lisa_workbook(tmp_path / "lisa.xlsx")
    workbook = load_workbook(path)
    workbook["Individ"]["E600"] = None
    workbook.save(path)
    workbook.close()

    records = _read(path)
    by_table = {
        table: [
            record
            for record in records
            if record.locators[0].physical_table == table
        ]
        for table in (
            "Individ",
            "Individ årsoberoende",
            "Företag",
            "Arbetsställe",
        )
    }
    assert next(
        record
        for record in by_table["Individ"]
        if record.fields.column_name.value == "AmPolTyp"
    ).fields.sensitivity == value_field(True)
    assert all(
        record.fields.sensitivity == value_field(True)
        for record in by_table["Individ årsoberoende"]
    )
    assert all(
        record.fields.sensitivity is None
        for table in ("Företag", "Arbetsställe")
        for record in by_table[table]
    )
