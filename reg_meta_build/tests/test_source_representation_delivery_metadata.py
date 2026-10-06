"""Accepted parallel columns: checked delivery metadata retain literals, coverage and field permissions."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from _source_representation_support import coding_claim as _claim
from catalog_manifest import synthetic_manifest
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.catalog_dependencies import (
    check_delivery_coverage,
)
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_representations import (
    resolve_representation_cases,
)
from reg_meta_build.sources.scb_records import clean_scb_row


def _unit_fixture(*, overlap=False):
    from reg_meta_build.source_curation import (
        DeliveryMetadataColumn,
        DeliveryMetadataDecision,
    )

    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="units.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    records = []
    for index, (year, unit) in enumerate(
        (("2020", "100-tal kronor"), ("2020" if overlap else "2021", "Kronor (SEK)"))
    ):
        values = var_row(
            colname="VALUE",
            var_id=1,
            cvid=100 + index,
            varname="Income " + year,
            vardef="Source supplied income definition",
            unit=unit,
            year=year,
        ).split("|")
        records.append(
            clean_scb_row(
                header,
                index + 1,
                {
                    name: (True, value, value)
                    for name, value in zip(header, values, strict=True)
                },
                revision,
            ).record
        )
    records = tuple(records)
    first = source_occurrence(records[0])
    assert first.variable_key is not None and first.variant_key is not None
    expectations = capture_expectations(
        records, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    case = CurationCase(
        case_id="literal-delivery-units",
        targets=expectations,
        peer_guards=(
            PeerGuard(
                guard_id="full-unit-family",
                source=records[0].source,
                native=NativeCoordinates(
                    register_id=1, register_variant_id=10, variable_id=1
                ),
                expected_members=tuple(e.ref for e in expectations),
            ),
        ),
        decision=DeliveryMetadataDecision(
            fields=("name",),
            reviewed=True,
            variable_key=first.variable_key,
            columns=(
                DeliveryMetadataColumn(
                    variant_key=first.variant_key,
                    column="VALUE",
                    valid_from="2020-01-01",
                    valid_to="2020-12-31" if overlap else "2021-12-31",
                    expected_codings=(),
                ),
            ),
            reason="Retain literal source units",
            provenance="Source encoding only; no value conversion",
        ),
    )
    return (
        records,
        case,
        {first.variant_key: ResolvedVariant(slug="people", name="People")},
        {first.column_key: resolve_code_membership(())},
    )


def _form_units(fixture, *, cases=None, records=None):
    original, case, variants, coding = fixture
    records = original if records is None else records
    resolution = resolve_representation_cases(
        records, (case,) if cases is None else cases, coding=coding
    )
    formed = form_native_variable(
        tuple(source_occurrence(record) for record in records),
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants=variants,
        slug="income",
        provider_key="1",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=resolution.cases,
    )
    return resolution, formed


def test_checked_delivery_metadata_retain_literals_and_coverage(tmp_path):
    fixture = _unit_fixture()
    records, _, _, _ = fixture
    _, baseline = _form_units(fixture, cases=())
    assert any(
        issue.code == "conflicting_variable_fact" and issue.fields == ("name",)
        for issue in baseline.diagnostics
    )
    resolution, formed = _form_units(fixture)
    assert not resolution.diagnostics
    assert formed.variable is not None and formed.variable.measurement_unit is None
    assert not any(issue.severity == "error" for issue in formed.diagnostics)
    assert {state.measurement_unit for state in formed.variable.states} == {
        "100-tal kronor",
        "Kronor (SEK)",
    }
    assert tuple(record.fields.measurement_unit.value for record in records) == (
        "100-tal kronor",
        "Kronor (SEK)",
    )
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})
    wrong = formed.variable.model_copy(
        update={
            "states": tuple(
                state.model_copy(update={"measurement_unit": "Kronor (SEK)"})
                for state in formed.variable.states
            )
        }
    )
    with pytest.raises(ValueError, match="literal delivery unit changed"):
        check_delivery_coverage((wrong,), formed.coverage, withheld={})
    write_resolved_catalog(
        (formed.variable,), tmp_path / "reg_meta.db", manifest=synthetic_manifest()
    )
    with closing(open_built_db(tmp_path / "reg_meta.db")) as conn:
        assert {
            row[0]
            for row in conn.execute("SELECT measurement_unit FROM variable_state")
        } == {"100-tal kronor", "Kronor (SEK)"}


@pytest.mark.parametrize("drift", ["missing", "changed", "new", "partial", "owner"])
def test_checked_delivery_metadata_fail_closed(drift):
    fixture = _unit_fixture()
    records, case, variants, coding = fixture
    if drift == "missing":
        records = records[:-1]
    elif drift == "changed":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"measurement_unit": value_field("other unit")}
                    )
                }
            ),
        )
    elif drift == "new":
        records = (
            *records,
            records[1].model_copy(
                update={
                    "locators": (
                        records[1]
                        .locators[0]
                        .model_copy(
                            update={
                                "semantic_record_key": (
                                    *records[1].locators[0].semantic_record_key,
                                    "new-peer",
                                )
                            }
                        ),
                    )
                }
            ),
        )
    elif drift == "partial":
        case = case.model_copy(update={"targets": case.targets[:-1]})
    elif drift == "owner":
        case = case.model_copy(
            update={
                "decision": case.decision.model_copy(
                    update={"variable_key": ("different", "owner")}
                )
            }
        )
    fixture = (fixture[0], case, variants, coding)
    if drift == "owner":
        with pytest.raises(ValueError, match="unconverted column coding"):
            _form_units(fixture, records=records)
        return
    resolution, formed = _form_units(fixture, records=records)
    if drift in {"missing", "changed", "new"}:
        assert resolution.diagnostics and not resolution.cases
    else:
        assert any(
            issue.code == "conflicting_variable_fact" for issue in formed.diagnostics
        )


def test_checked_delivery_metadata_do_not_resolve_same_column_conflicts():
    _, formed = _form_units(_unit_fixture(overlap=True))
    assert any(
        issue.severity == "error" and "measurement_unit" in issue.fields
        for issue in formed.diagnostics
    )


def test_checked_delivery_metadata_reject_changed_coding():
    fixture = _unit_fixture()
    records, case, variants, coding = fixture
    key = next(iter(coding))
    changed_coding = {key: resolve_code_membership((_claim("new supplied list", "1"),))}
    resolution, _ = _form_units((records, case, variants, changed_coding))
    assert not resolution.cases
    assert any(
        issue.code == "stale_representation_coding" for issue in resolution.diagnostics
    )


@pytest.mark.parametrize("name_permission", [False, True])
def test_checked_delivery_text_keeps_source_names_without_common_winner(
    name_permission,
):
    records, case, variants, coding = _unit_fixture()
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={
                        "name": value_field("Visit" if index == 0 else "Admission"),
                        "description": value_field(
                            "Visited facility" if index == 0 else "Admitting facility"
                        ),
                        "measurement_unit": value_field("Kronor (SEK)"),
                    }
                )
            }
        )
        for index, record in enumerate(records)
    )
    decision = case.decision.model_copy(
        update={
            "fields": ("name", "description") if name_permission else ("description",)
        }
    )
    case = case.model_copy(
        update={
            "decision": decision,
            "targets": capture_expectations(
                records,
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            ),
        }
    )
    resolution, formed = _form_units((records, case, variants, coding))
    assert resolution.cases == (case,)
    if not name_permission:
        assert formed.variable is None
        assert any(d.code == "unresolved_variable_name" for d in formed.diagnostics)
        return
    variable = formed.variable
    assert variable is not None
    assert variable.name is None and variable.description is None
    assert [(s.name, s.description) for s in variable.states] == [
        ("Visit", "Visited facility"),
        ("Admission", "Admitting facility"),
    ]
    assert not any(d.severity == "error" for d in formed.diagnostics)
    check_delivery_coverage((variable,), formed.coverage, withheld={})
    changed = variable.model_copy(
        update={
            "states": (
                variable.states[0].model_copy(
                    update={"description": "Borrowed sibling description"}
                ),
                *variable.states[1:],
            )
        }
    )
    with pytest.raises(ValueError, match="literal delivery description changed"):
        check_delivery_coverage((changed,), formed.coverage, withheld={})
    unknown = records[0].model_copy(
        update={"fields": records[0].fields.model_copy(update={"name": None})}
    )
    unknown_case = case.model_copy(
        update={
            "targets": capture_expectations(
                (unknown, records[1]),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        }
    )
    with pytest.raises(ValueError, match="positive permitted source facts"):
        resolve_representation_cases(
            (unknown, records[1]), (unknown_case,), coding=coding
        )


def test_delivery_metadata_permissions_do_not_cover_an_enlarged_effective_scope():
    records, case, variants, coding = _unit_fixture()
    column = case.decision.columns[0].model_copy(update={"valid_to": "2020-12-31"})
    case = case.model_copy(
        update={"decision": case.decision.model_copy(update={"columns": (column,)})}
    )
    resolution, formed = _form_units((records, case, variants, coding))
    assert resolution.cases == (case,)
    assert any(
        d.code == "conflicting_variable_fact" and d.fields == ("name",)
        for d in formed.diagnostics
    )


def test_delivery_metadata_requires_unique_explicit_field_permissions():
    _, case, _, _ = _unit_fixture()
    decision = type(case.decision)
    for fields in ((), ("name", "name"), ("data_type",)):
        with pytest.raises(ValueError):
            decision.model_validate({**case.decision.model_dump(), "fields": fields})


def test_delivery_metadata_exact_open_scope_is_not_a_finite_window_exemption():
    from reg_meta_build.source_curation import DeliveryMetadataColumn, SourceEvidence

    original, case, variants, coding = _unit_fixture()
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020", end=None),)
    )
    record = original[0].model_copy(
        update={
            "edition_scope": scope,
            "edition_period_scope": TemporalScope(kind="not_applicable"),
        }
    )
    column = DeliveryMetadataColumn(
        variant_key=case.decision.columns[0].variant_key,
        column="VALUE",
        valid_from="2020-01-01",
        valid_to="9999-12-31",
        expected_codings=(),
        source_scope=scope,
    )
    case = case.model_copy(
        update={
            "targets": capture_expectations(
                (record,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            ),
            "peer_guards": tuple(
                g.model_copy(update={"expected_members": (record_ref(record),)})
                for g in case.peer_guards
            ),
            "decision": case.decision.model_copy(update={"columns": (column,)}),
        }
    )
    resolution, formed = _form_units(((record,), case, variants, coding))
    assert resolution.cases == (case,) and formed.variable is not None
    assert formed.variable.states[0].valid_to == "9999-12-31"
    assert record.edition_scope.intervals[0].end is None
    for bad in (
        {"source_scope": None},
        {"valid_to": "2021-12-31"},
        {"source_scope": TemporalScope(kind="unknown", label="unknown supplied scope")},
        {"source_scope": TemporalScope(kind="year_independent")},
    ):
        with pytest.raises(ValueError):
            DeliveryMetadataColumn.model_validate({**column.model_dump(), **bad})
    # Equal derived dates still do not imply the same supplied source scope.
    other_scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020-01-01", end=None),)
    )
    changed_column = column.model_copy(update={"source_scope": other_scope})
    changed_case = case.model_copy(
        update={
            "decision": case.decision.model_copy(update={"columns": (changed_column,)})
        }
    )
    rejected = resolve_representation_cases((record,), (changed_case,), coding=coding)
    assert not rejected.cases
    assert [d.code for d in rejected.diagnostics] == ["stale_delivery_metadata_scope"]
    for changed_scope in (
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2019", end=None),)
        ),
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end="2021"),)
        ),
    ):
        effective = replace(source_occurrence(record), edition_scope=changed_scope)
        evidence = SourceEvidence((record,), effective_occurrences=(effective,))
        rejected = resolve_representation_cases(evidence, (case,), coding=coding)
        assert not rejected.cases
        assert any(
            d.code == "stale_delivery_metadata_scope" for d in rejected.diagnostics
        )


@pytest.mark.parametrize("changed_field", ["name", "measurement_unit"])
def test_delivery_metadata_keeps_unrelated_checked_field_corrections(changed_field):
    from reg_meta_build.source_curation import SourceEvidence

    records, metadata, variants, coding = _unit_fixture()
    correction = metadata.model_copy(
        update={
            "case_id": "checked-source-field",
            "decision": OccurrenceCorrectionDecision(
                reviewed=True,
                effects=tuple(
                    CheckedFieldChange(
                        ref=record_ref(record),
                        replacement=FieldExpectation(
                            name=changed_field,
                            status="value",
                            value="Checked source wording",
                        ),
                    )
                    for record in records[:1]
                ),
                reason="Independent supplied source correction",
                provenance="exact fixture",
            ),
        }
    )
    corrected = apply_occurrence_cases(records, (correction,))
    assert not corrected.diagnostics
    evidence = SourceEvidence(records, effective_occurrences=corrected.occurrences)
    representations = resolve_representation_cases(evidence, (metadata,), coding=coding)
    formed = form_native_variable(
        corrected.occurrences,
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants=variants,
        slug="income",
        provider_key="1",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=representations.cases,
    )
    unit_conflicts = [
        d
        for d in formed.diagnostics
        if d.code == "conflicting_variable_fact" and d.fields == ("measurement_unit",)
    ]
    assert not unit_conflicts
    assert any(d.code == "delivery_units_vary" for d in formed.diagnostics)
    if changed_field == "name":
        # The checked text permission cannot waive an independent field correction.
        assert any(
            d.code == "conflicting_variable_fact" and d.fields == ("name",)
            for d in formed.diagnostics
        )
        assert corrected.occurrences[0].fields.name is not None
        assert corrected.occurrences[0].fields.name.value == "Checked source wording"
        assert corrected.occurrences[0].source_records == (records[0],)
