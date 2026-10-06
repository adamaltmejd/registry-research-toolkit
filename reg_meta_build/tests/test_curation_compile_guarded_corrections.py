"""Classification-reference, unit and name corrections and guarded column owners compile with their guards."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _curation_compile_support import (
    checked_correction_fixture as _checked_correction_fixture,
    errata_record as _errata_record,
    partition_scope as _partition_scope,
    run_checked_correction as _run_checked_correction,
    scb_partition_tree as _scb_partition_tree,
)
from reg_meta.source_evidence import (
    SourceRevision,
)
from reg_meta_build.curation_compile import (
    compile_coding_register,
    convert_column_partitions,
)
from reg_meta_build.curation_tree import (
    ErrataFieldEntry,
    load_curation_tree,
)
from reg_meta_build.source_curation import (
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_classification_reference_corrections_preserve_shared_ref_other_windows(
    tmp_path,
):
    tree, scope, original, _, base = _checked_correction_fixture(tmp_path)
    original = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={
                    "classification_declared": value_field("Incorrect municipal list"),
                    "representation": value_field("Six-digit district code"),
                    "coverage_from": value_field("2009"),
                    "coverage_to": value_field("2009"),
                }
            )
        }
    )
    negative = original.model_copy(
        update={
            "record_id": original.record_id + "-other-window",
            "fields": original.fields.model_copy(
                update={
                    "classification_declared": value_field("District reference"),
                    "coverage_from": value_field("2017"),
                    "coverage_to": value_field("2017"),
                }
            ),
            "edition_scope": TemporalScope(
                kind="intervals", intervals=(ScopeInterval(start="2017", end="2017"),)
            ),
        }
    )
    assert record_ref(original) == record_ref(negative)
    entry = ErrataFieldEntry(
        **{
            **base.model_dump(exclude={"field", "value", "expected_fields"}),
            "expected_scope": original.edition_scope,
            "expected_fields": list(
                capture_expectations(
                    (original,),
                    fields=(
                        "name",
                        "definition",
                        "description",
                        "operational_definition",
                        "classification_declared",
                        "representation",
                        "data_type",
                        "coverage_from",
                        "coverage_to",
                    ),
                )[0]
                .alternatives[0]
                .fields
            ),
        },
        field="classification_declared",
        value="District reference",
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    result = apply_occurrence_cases((original, negative), cases[scope.source, None])
    assert not result.diagnostics
    changed, untouched = result.occurrences
    assert changed.source_records == (original,)
    assert changed.fields.classification_declared.value == "District reference"
    assert changed.edition_scope == original.edition_scope
    assert untouched == source_occurrence(negative)
    for field in ("classification_declared", "representation", "coverage_from"):
        changed_source = original.model_copy(
            update={
                "fields": original.fields.model_copy(
                    update={field: value_field("Changed source assertion")}
                )
            }
        )
        _, stale, _ = _run_checked_correction(tree, scope, (changed_source, negative))
        assert stale and stale[0].code == "stale_curation_entry"


def test_classification_reference_corrections_require_complete_metadata_guards(
    tmp_path,
):
    _, _, _, _, base = _checked_correction_fixture(tmp_path)
    with pytest.raises(ValueError, match="classification corrections require"):
        ErrataFieldEntry(
            **base.model_dump(exclude={"field", "value"}),
            field="classification_declared",
            value="District reference",
        )


def test_measurement_unit_correction_guards_shared_ref_physical_assertions(tmp_path):
    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    original = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"measurement_unit": value_field("Antal")}
            )
        }
    )
    negative = original.model_copy(
        update={
            "record_id": original.record_id + "-other-unit",
            "fields": original.fields.model_copy(
                update={"measurement_unit": value_field("Kronor (SEK)")}
            ),
        }
    )
    fields = (
        "name",
        "definition",
        "description",
        "operational_definition",
        "classification_declared",
        "representation",
        "data_type",
        "coverage_from",
        "coverage_to",
        "measurement_unit",
    )
    guards = list(
        capture_expectations((original,), fields=fields)[0].alternatives[0].fields
    )
    entry = ErrataFieldEntry(
        **{
            **base.model_dump(exclude={"field", "value", "expected_fields"}),
            "expected_fields": guards,
        },
        field="measurement_unit",
        value="Antal personer",
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    result = apply_occurrence_cases((original, negative), cases[scope.source, None])
    assert not result.diagnostics
    changed, untouched = result.occurrences
    assert changed.fields.measurement_unit.value == "Antal personer"
    assert changed.source_records == (original,)
    assert changed.edition_scope == original.edition_scope
    assert untouched == source_occurrence(negative)
    for field in ("measurement_unit", "definition", "representation"):
        altered = original.model_copy(
            update={
                "fields": original.fields.model_copy(
                    update={field: value_field("Changed supplied assertion")}
                )
            }
        )
        _, stale, _ = _run_checked_correction(tree, scope, (altered, negative))
        assert stale and stale[0].code == "stale_curation_entry"
    for missing in ("measurement_unit", "representation", "coverage_from"):
        with pytest.raises(ValueError):
            entry.model_validate(
                {
                    **entry.model_dump(),
                    "expected_fields": [
                        g.model_dump() for g in guards if g.name != missing
                    ],
                }
            )


def test_checked_name_correction_keeps_same_ref_other_source_scope(tmp_path):
    tree, scope, original, _, _ = _checked_correction_fixture(tmp_path)
    other = original.model_copy(
        update={
            "record_id": original.record_id + "-monthly",
            "edition_scope": TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start="2008-01-01", end="2008-12-31"),),
            ),
            "fields": original.fields.model_copy(
                update={"name": value_field("Monthly observation")}
            ),
        }
    )
    assert record_ref(original) == record_ref(other)
    cases, issues, _ = _run_checked_correction(tree, scope, (original, other))
    assert not issues
    result = apply_occurrence_cases((original, other), cases[scope.source, None])
    assert not result.diagnostics
    assert result.occurrences[0].fields.name.value == "Reviewed label"
    assert result.occurrences[1] == source_occurrence(other)
    drifted = original.model_copy(update={"edition_scope": other.edition_scope})
    _, stale, _ = _run_checked_correction(tree, scope, (drifted, other))
    assert stale and stale[0].code == "stale_curation_entry"
    replay = apply_occurrence_cases((drifted, other), cases[scope.source, None])
    assert replay.diagnostics


@pytest.mark.parametrize(
    "guards,editions",
    [
        ('[{ name = "measurement_unit", status = "value", value = "SEK" }]', "[]"),
        (
            '[{ name = "measurement_unit", status = "value", value = "SEK" }, { name = "measurement_unit", status = "value", value = "SEK" }]',
            '["2020"]',
        ),
    ],
)
def test_guarded_column_owner_requires_finite_unique_fields(guards, editions):
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    with pytest.raises(ValidationError):
        IdentityColumnOwnerEntry.model_validate(
            {
                "variable": "1.5",
                "variant": "1.2",
                "column": "ANSWER",
                "owner": "1.5.amount",
                "ref": "role",
                "source_editions": json.loads(editions),
                "expected_fields": json.loads(
                    guards.replace("name = ", '"name": ')
                    .replace("status = ", '"status": ')
                    .replace("value = ", '"value": ')
                ),
            }
        )


def test_guarded_column_owner_allows_operation_only_guard():
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    entry = IdentityColumnOwnerEntry.model_validate(
        {
            "variable": "1.5",
            "variant": "1.2",
            "column": "ANSWER",
            "owner": "1.5.amount",
            "ref": "role",
            "source_editions": ["2020"],
            "expected_fields": [
                {"name": "operational_definition", "status": "value", "value": "Amount"}
            ],
        }
    )
    assert entry.expected_fields[0].value == "Amount"


@pytest.mark.parametrize("end", ["2020-12-31", None])
def test_documented_source_scope_has_reportable_window_when_source_missing(
    tmp_path: Path, end
):
    from reg_meta_build.curation_compile import coding_entry_windows
    from reg_meta_build.curation_tree import (
        CodingDocumentedEntry,
        PreparedCodingAuthority,
    )

    root = tmp_path / "curation"
    _scb_partition_tree(root, "")
    record = _errata_record(column="ANSWER", year="2020", member=20)
    revision = SourceRevision.create(
        dataset=record.source,
        publisher="SCB",
        purpose="coding fixture",
        upstream_revision="1",
        artifact_path="records.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    authority = PreparedCodingAuthority(
        revision=revision,
        source_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020-01-01", end=end),)
        ),
        locators=list(record.locators),
        records=list(
            capture_expectations(
                (record,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        codings=["b" * 64],
    )
    entry = CodingDocumentedEntry(
        variable="1.999",
        variant="1.2",
        column="MISSING",
        reason="Exact supplied source domain",
        source="fixture",
        members=(("J", "Yes"), ("N", "No")),
        version_label="Source list",
        source_authority=authority,
    )
    assert entry.periods == []
    assert coding_entry_windows(entry) == (("2020-01-01", end or "9999-12-31"),)
    tree = load_curation_tree(root)
    register = tree.registers[0]
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"documented": [entry]})}
    )
    cases, diagnostics = compile_coding_register(
        register,
        _partition_scope((record,)),
        originals=(record,),
        columns={},
        column_scopes={},
        coding={},
    )
    assert not cases
    assert len(diagnostics) == 1
    assert diagnostics[0].code == "stale_curation_entry"
    assert diagnostics[0].case_id.endswith("coding.documented/1/period/1")
    assert (diagnostics[0].valid_from, diagnostics[0].valid_to) == coding_entry_windows(
        entry
    )[0]


@pytest.mark.parametrize("unlisted_peer", [False, True])
def test_finite_scoped_owner_closes_only_fully_covered_unassigned_literal(
    unlisted_peer,
):
    records = tuple(
        _errata_record(column="ANSWER", year=year, member=20 + i)
        for i, year in enumerate(("2020", "2021"))
    )
    owners = {(record_ref(record), "ANSWER"): "1.5.answer" for record in records}
    records += (_errata_record(column="KNOWN", year="1999", member=19),)
    if unlisted_peer:
        records += (_errata_record(column="ANSWER", year="2022", member=22),)
    result = convert_column_partitions(
        records,
        source_id="1.5",
        split_ids=("1.5.answer",),
        declared_columns={"ANSWER": None, "KNOWN": "1.5.answer"},
        declaration_reference="Explicitly withheld except finite reviewed owners",
        scoped_owners=owners,
    )
    assert bool(result.diagnostics) is unlisted_peer
    assert result.case is not None
    after = apply_occurrence_cases(records, (result.case,))
    assert not after.diagnostics
    for occurrence in after.occurrences:
        record = occurrence.source_records[0]
        if (
            record.fields.column_name.value == "KNOWN"
            or (record_ref(record), "ANSWER") in owners
        ):
            assert occurrence.variable_key[-1] == "1.5.answer"
        else:
            assert occurrence.variable_key == source_occurrence(record).variable_key
