"""Delivery metadata: the two checks no build reaches (delivery coverage of literal
units and texts, and the open source-scope comparison). The reachable behavior is the
build case `representation-delivery-metadata-keeps-literal-units-and-texts` and the
`representation-delivery-metadata-*` loader cases."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.catalog_dependencies import (
    check_delivery_coverage,
)
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
)
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_curation import (
    CurationCase,
    PeerGuard,
    capture_expectations,
)
from reg_meta_build.source_effects import (
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


def _unit_fixture():
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
        (("2020", "100-tal kronor"), ("2021", "Kronor (SEK)"))
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
                    valid_to="2021-12-31",
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


def _form_units(fixture):
    records, case, variants, coding = fixture
    resolution = resolve_representation_cases(records, (case,), coding=coding)
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


def test_delivery_coverage_refuses_a_changed_literal_unit_or_description():
    """Delivery coverage refuses a written state whose literal unit or text moved.

    Input: the two-edition fixture (2020 '100-tal kronor', 2021 'Kronor (SEK)') formed
    with its name permission, then each built state rewritten with the 2021 unit, or one
    state given another description. Expected: `check_delivery_coverage` accepts the
    formed variable and refuses each rewrite ("literal delivery unit changed" /
    "literal delivery description changed"). No build reaches this: formation writes the
    claimed literal, so only a defect between formation and the writer could move it;
    the build cases `representation-delivery-metadata-keeps-literal-units-and-texts`
    show the literal values that reach the artifact. Fails if `check_delivery_coverage`
    stops comparing a claimed unit or description with the written state.
    """
    fixture = _unit_fixture()
    _, formed = _form_units(fixture)
    assert formed.variable is not None
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})
    wrong_unit = formed.variable.model_copy(
        update={
            "states": tuple(
                state.model_copy(update={"measurement_unit": "Kronor (SEK)"})
                for state in formed.variable.states
            )
        }
    )
    with pytest.raises(ValueError, match="literal delivery unit changed"):
        check_delivery_coverage((wrong_unit,), formed.coverage, withheld={})
    records, case, variants, coding = fixture
    texts = tuple(
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
    text_case = case.model_copy(
        update={
            "decision": case.decision.model_copy(
                update={"fields": ("name", "description")}
            ),
            "targets": capture_expectations(
                texts,
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            ),
        }
    )
    _, formed = _form_units((texts, text_case, variants, coding))
    assert formed.variable is not None
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})
    borrowed = formed.variable.model_copy(
        update={
            "states": (
                formed.variable.states[0].model_copy(
                    update={"description": "Borrowed sibling description"}
                ),
                *formed.variable.states[1:],
            )
        }
    )
    with pytest.raises(ValueError, match="literal delivery description changed"):
        check_delivery_coverage((borrowed,), formed.coverage, withheld={})


def test_delivery_metadata_exact_open_scope_is_not_a_finite_window_exemption():
    """An open supplied source scope (2020-) keeps its open end; a changed scope stales.

    Input: one SCB record whose edition scope is the open interval `2020-`, and a
    delivery-metadata column over 2020-01-01..9999-12-31 that names that scope. Expected:
    the column applies and the state ends 9999-12-31; the same column naming another open
    scope (2020-01-01-), or the record's effective scope changed to 2019- or 2020..2021,
    is refused as `stale_delivery_metadata_scope`. No build reaches this: SCB delivers no
    open edition scope, and `compile_delivery_metadata` refuses a changed source scope
    before resolution (stale_curation_entry), so this runtime check is defense in depth.
    The column-model refusals are the loader cases
    `representation-delivery-metadata-*-source-scope-refused` and
    `...-open-ended-window-without-source-scope-refused`.
    Fails if `resolve_representation_cases` stops comparing a column's `source_scope`
    with the effective occurrence scopes it covers.
    """
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
