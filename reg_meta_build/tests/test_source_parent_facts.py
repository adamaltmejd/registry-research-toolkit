"""Common parent claims retain exact evidence without expanding source rows."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from reg_meta.source_evidence import SourceField, SourceRevision
from reg_meta_build.source_curation import (
    parent_fact_projection,
)
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from reg_meta_build.source_records import SourceRecord

_REVISION = SourceRevision.create(
    dataset="scb-parent-fixture",
    publisher="SCB",
    purpose="parent evidence test",
    upstream_revision="1",
    artifact_path="Registerinformation.csv",
    artifact_size=0,
    artifact_sha256="0" * 64,
)


def _record(row: int = 2, **changes: str | None) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    values = var_row(colname="Example", cvid=1001, var_id=101).split("|")
    raw = dict(zip(header, values, strict=True)) | changes
    cells = {
        name: (value is not None, value, value or "") for name, value in raw.items()
    }
    return clean_scb_row(header, row, cells, _REVISION).record


def _field(value: SourceField | None) -> SourceField:
    assert value is not None
    return value


def test_parent_facts_have_neutral_scope_and_exact_field_cells() -> None:
    record = _record(
        Registerversionmätinformation="  A paragraph\n\n  detail  ",
        Registerversion_ForstaGodkannandeDatum="2019-02-29",
        Registerversion_DocStaus=" Published ",
    )
    register, variant, edition, population, object_type = record.parent_facts
    assert [parent.kind for parent in record.parent_facts] == [
        "register",
        "variant",
        "edition",
        "population",
        "object_type",
    ]
    assert _field(register.fields.purpose).value == "Testning"
    assert _field(variant.fields.description).value == "Alla individer"
    assert edition.coordinate.native_id == record.subject.native.edition_id
    assert _field(edition.fields.first_approved_at).value == "2019-02-29"
    assert _field(edition.fields.documentation_status).value == "Published"
    assert (
        _field(edition.fields.measurement_information).value
        == "  A paragraph\n\n  detail"
    )
    assert population.edition == edition.coordinate
    assert _field(population.fields.population_definition).value == "Alla personer"
    assert _field(object_type.fields.definition).value == "Fysisk person"
    assert record.fields.population_definition is None
    assert record.fields.purpose is None and record.fields.documentation_status is None
    assert record.parent_field_locators(2, "first_approved_at")[0].physical_cells == (
        "Registerinformation.csv:row:2:Registerversion_ForstaGodkannandeDatum",
    )
    with pytest.raises(KeyError):
        record.parent_field_locators(0, "unobserved")


def test_blank_parent_facts_preserve_unknown_and_raw_representation() -> None:
    null, blank = _record(Registersyfte=None), _record(Registersyfte="  ")
    assert (
        _field(null.parent_facts[0].fields.purpose).status
        == _field(blank.parent_facts[0].fields.purpose).status
        == "unknown"
    )
    assert _field(null.parent_facts[0].fields.purpose).raw_value is None
    assert _field(blank.parent_facts[0].fields.purpose).raw_value == "  "
    assert null.record_id != blank.record_id
    assert null.fields == blank.fields
    assert parent_fact_projection(null.parent_facts[0]) == parent_fact_projection(
        blank.parent_facts[0]
    )
