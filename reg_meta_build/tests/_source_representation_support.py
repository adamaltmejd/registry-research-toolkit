"""Shared synthetic parallel-column records, setups and formation entry for the representation tests."""

from __future__ import annotations

from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import column_identity
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CheckedIdentityChange,
    ColumnRepresentation,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RepresentationDecision,
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

KEY = ("accepted", "fixture", "income")


def representation_records(
    *, vardef="A generic family label", varopdef="", data_type="int"
):
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    result = []
    for i, column in enumerate(("First", "Second")):
        values = var_row(
            colname=column,
            cvid=100 + i,
            var_id=i + 1,
            varname=column,
            year="2020",
            vardef=vardef if i else "A generic family label",
            varopdef=varopdef if i else "",
            data_type=data_type if i else "int",
        ).split("|")
        result.append(
            clean_scb_row(
                header,
                i + 1,
                {
                    name: (True, value, value)
                    for name, value in zip(header, values, strict=True)
                },
                revision,
            ).record
        )
    return tuple(result)


def representation_setup(records=None, claims=None):
    records = representation_records() if records is None else records
    variant_key = source_occurrence(records[0]).variant_key
    assert variant_key is not None
    variant = ResolvedVariant(slug="people", name="People")
    coding = {
        column_identity(KEY, variant_key, column): resolve_code_membership(
            tuple((claims or {}).get(column, ()))
        )
        for column in ("First", "Second")
    }
    expectations = capture_expectations(
        records,
        fields=(
            "column_name",
            "name",
            "description",
            "definition",
            "measurement_unit",
            "data_type",
            "data_length",
            "operational_definition",
            "source_attribution",
        ),
        coding=True,
    )
    guard = PeerGuard(
        guard_id="exact-family",
        source=records[0].source,
        native=NativeCoordinates(register_id=1, register_variant_id=10),
        edition_scopes=(records[0].edition_scope,),
        expected_members=tuple(e.ref for e in expectations),
    )
    identity = CurationCase(
        case_id="identity",
        targets=expectations,
        peer_guards=(guard,),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                e
                for r in records
                for e in (
                    CheckedIdentityChange(ref=record_ref(r), variable_key=KEY),
                    CheckedFieldChange(
                        ref=record_ref(r),
                        replacement=FieldExpectation(
                            name="name", status="value", value="Income"
                        ),
                    ),
                )
            ),
            reason="Existing parallel family identity and label",
            provenance="accepted family",
        ),
    )
    effects = apply_occurrence_cases(records, (identity,))
    assert effects.diagnostics == ()
    representation = CurationCase(
        case_id="representations",
        targets=expectations,
        peer_guards=(guard,),
        decision=RepresentationDecision(
            reviewed=True,
            variable_key=KEY,
            variant_key=variant_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            columns=tuple(
                ColumnRepresentation(
                    column=column,
                    valid_from=start,
                    valid_to=end,
                )
                for column, start, end in (
                    ("First", "2020-01-01", "2020-06-30"),
                    ("Second", "2020-07-01", "2020-12-31"),
                )
            ),
            reason="Existing accepted precise period representations",
            provenance="accepted family",
        ),
    )
    return records, effects.occurrences, representation, {variant_key: variant}, coding


def form_parallel_columns(setup, cases=None):
    records, occurrences, case, variants, coding = setup
    resolution = resolve_representation_cases(
        records, (case,) if cases is None else cases, coding=coding
    )
    formed = form_native_variable(
        occurrences,
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants=variants,
        slug="income",
        provider_key="family",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=resolution.cases,
    )
    return formed, resolution


def column_storage_setup(
    first_type, first_length, second_type, second_length, *, claims=None
):
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={
                        "data_type": value_field(dtype) if dtype is not None else None,
                        "data_length": value_field(length)
                        if length is not None
                        else None,
                    }
                )
            }
        )
        for record, dtype, length in zip(
            representation_records(),
            (first_type, second_type),
            (first_length, second_length),
            strict=True,
        )
    )
    setup = representation_setup(records, claims=claims)
    decision = setup[2].decision
    assert isinstance(decision, RepresentationDecision)
    case = setup[2].model_copy(
        update={
            "decision": decision.model_copy(
                update={
                    "column_metadata": "per_column",
                    "columns": tuple(
                        column.model_copy(
                            update={
                                "valid_from": decision.valid_from,
                                "valid_to": decision.valid_to,
                            }
                        )
                        for column in decision.columns
                    ),
                }
            )
        }
    )
    return (*setup[:2], case, *setup[3:])


def coding_claim(name, code, year=2020):
    return CodeListClaim(
        name,
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=str(year), end=str(year)),)
        ),
        (CodeMembershipClaim(code, "Label", TemporalScope(kind="year_independent")),),
    )
