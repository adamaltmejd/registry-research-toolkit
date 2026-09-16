"""Common parent claims retain exact evidence without expanding source rows."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from _prepared_fixtures import accept_prepared
from reg_meta_build.prepared_sources import (
    open_prepared_source_records,
    prepare_source_records,
)
from reg_meta_build.source_curation import (
    BoundedUnresolvedDecision,
    CurationCase,
    FieldExpectation,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
    evaluate_case,
    parent_fact_projection,
)
from reg_meta_build.source_records import SourceField, SourceRecord, SourceRevision
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path


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
    values = _var_row(colname="Example", cvid=1001, var_id=101).split("|")
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


def test_parent_conflicts_and_duplicate_rows_remain_distinct_occurrences(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, duplicate, conflict = (
        _record(),
        _record(row=9),
        _record(Registersyfte="Another purpose"),
    )
    assert first.record_id == duplicate.record_id != conflict.record_id
    assert first.fields == conflict.fields
    output = tmp_path / "prepared" / "records"
    manifest = prepare_source_records(
        output,
        records=(first, duplicate, conflict),
        revisions=(_REVISION,),
        scope="parent fixture",
    )
    assert manifest.record_count == 3
    with sqlite3.connect(output / "files/records.sqlite") as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM payload WHERE kind='parent'").fetchone()[
                0
            ]
            == 6
        )
        assert conn.execute("SELECT COUNT(*) FROM occurrence").fetchone()[0] == 3
    commit = accept_prepared(output)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("accepted decoding must not recalculate source identity")

    monkeypatch.setattr(SourceRecord, "_record_id", forbidden)
    restored = tuple(
        open_prepared_source_records(
            output, expected_sha256=manifest.sha256, input_commit=commit
        ).records
    )
    assert [record.model_dump(mode="json") for record in restored] == [
        record.model_dump(mode="json") for record in (first, duplicate, conflict)
    ]
    assert restored[1].parent_field_locators(0, "purpose")[0].physical_record == "row:9"


@pytest.mark.parametrize(
    "fault", ["out_of_bounds", "missing_field", "duplicate_field", "wrong_scope"]
)
def test_malformed_parent_contracts_fail_before_preparation(
    tmp_path: Path, fault: str
) -> None:
    record = _record()
    document = record.model_dump(mode="json")
    parent = document["parent_facts"][0]
    if fault == "out_of_bounds":
        parent["field_cells"][0]["positions"] = [len(record.delivered_cells)]
    elif fault == "missing_field":
        parent["field_cells"].pop()
    elif fault == "duplicate_field":
        parent["field_cells"].append(parent["field_cells"][0])
    else:
        parent["variant"] = document["subject"]["variant"]
    with pytest.raises(ValueError):
        SourceRecord.model_validate_json(json.dumps(document))
    # The preparation boundary must revalidate even a forged model instance.
    forged = record.model_copy(
        update={
            "parent_facts": (
                record.parent_facts[0].model_copy(update={"field_cells": ()}),
            )
        }
    )
    destination = tmp_path / "records"
    with pytest.raises(ValueError, match="parent facts"):
        prepare_source_records(
            destination,
            records=(forged,),
            revisions=(_REVISION,),
            scope="invalid fixture",
        )
    assert not destination.exists()


def test_parent_guards_track_semantics_without_changing_variable_guards() -> None:
    first, whitespace, changed = (
        _record(),
        _record(row=12, Registersyfte="Testning "),
        _record(Registersyfte="Changed"),
    )
    reference = SourceRecordRef(
        source=first.source, semantic_record_key=first.locators[0].semantic_record_key
    )
    decision = BoundedUnresolvedDecision(
        reviewed=True,
        withheld_aspects=("identity",),
        reason="Synthetic finite source decision",
    )
    parents = CurationCase(
        case_id="parent-facts",
        targets=(
            RecordExpectation(
                ref=reference,
                alternatives=(
                    RecordProjection(
                        parent_facts=tuple(
                            parent_fact_projection(parent)
                            for parent in first.parent_facts
                        )
                    ),
                ),
            ),
        ),
        decision=decision,
    )
    variable = CurationCase(
        case_id="variable-facts",
        targets=(
            RecordExpectation(
                ref=reference,
                alternatives=(
                    RecordProjection(
                        fields=(
                            FieldExpectation(
                                name="column_name", status="value", value="Example"
                            ),
                        )
                    ),
                ),
            ),
        ),
        decision=decision,
    )
    assert evaluate_case(parents, (whitespace,)).status == "applicable"
    assert evaluate_case(parents, (changed,)).status == "stale"
    assert evaluate_case(variable, (changed,)).status == "applicable"
