"""Native formation legs no build case can observe: year-independent states and the
coverage claims formation mints for the delivery-coverage gate.

Only the LISA reader delivers a year-independent edition (`sources/lisa.py`), and the
`cases/build` runner has no LISA source (the same reason `test_catalog_lineage.py`
keeps its year-independent legs). The delivery-coverage gate is load-bearing, but no
build reaches its refusal arms (`test_delivery_coverage_obligations.py` keeps those as
direct tests with hand-built claims), so the claims formation hands it are pinned here.
The other formation behaviors this file pinned are legs of
`cases/build/formation-native-families-keep-literal-deliveries-or-stay-unresolved` or
build cases that already asserted them.
"""

from __future__ import annotations

from dataclasses import replace

from _csv_fixtures import SCB_REVISION
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_evidence import RecordLocator
from reg_meta_build.source_formation import VariableFormation, form_native_variable
from reg_meta_build.source_occurrences import (
    AppliedCorrection,
    EffectiveOccurrence,
    effective_occurrence,
    source_occurrence,
)
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


def _record(edition_scope: TemporalScope, period_scope: TemporalScope) -> SourceRecord:
    return SourceRecord.create(
        revision=SCB_REVISION,
        locators=(
            RecordLocator(
                semantic_record_key=("variable:4", "year:2020"),
                physical_file="input.csv",
                physical_table="input.csv",
                physical_record="row:2020",
                physical_cells=(),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="value", native_id=1, name="Example"),
            variant=SourceCoordinate(status="value", native_id=2, name="People"),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(
                status="value", native_id=4, name="Source variable"
            ),
            member=SourceCoordinate(status="value", native_id=2020),
            native=NativeCoordinates(),
        ),
        edition_scope=edition_scope,
        edition_period_scope=period_scope,
        fields=SourceFields(
            availability=value_field(True),
            name=value_field("Source variable"),
            definition=value_field("Source definition"),
            column_name=value_field("VALUE"),
            data_type=value_field("integer"),
        ),
    )


def _form(
    occurrence: SourceRecord | EffectiveOccurrence, claims: tuple[CodeListClaim, ...]
) -> VariableFormation:
    effective = effective_occurrence(occurrence)
    assert effective.variant_key is not None and effective.column_key is not None
    return form_native_variable(
        (occurrence,),
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants={effective.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="value",
        provider_key="4",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={effective.column_key: resolve_code_membership(claims)},
    )


def test_independent_delivery_forms_without_calendar_dates_and_retains_source():
    """Fails if formation dates a year-independent delivery, marks it pooled, drops
    its year-independent list, or owes the catalog a dated coverage claim for it."""
    scope = TemporalScope(kind="year_independent")
    claim = CodeListClaim(
        "native-list",
        scope,
        (CodeMembershipClaim("02", "EU25 utom Norden", scope),),
        "EU25",
    )
    formed = _form(_record(scope, scope), (claim,))
    assert formed.diagnostics == ()
    assert formed.variable is not None
    (state,) = formed.variable.states
    assert state.period_scope == "year_independent"
    assert state.valid_from is state.valid_to is None
    assert state.pooled is False
    assert state.value_set is not None
    assert state.value_set.members == (("02", "EU25 utom Norden"),)
    (obligation,) = formed.coverage
    assert obligation.period_scope == "year_independent"
    assert obligation.valid_from is obligation.valid_to is None
    assert obligation.coding_claim == (state.value_set, "EU25")


def test_formation_coverage_claims_carry_the_delivered_type_and_attributions():
    """The claims the delivery-coverage gate checks a written state against.

    Fails if formation stops minting the delivered type claim or the correction
    attributions, so the gate (whose refusals `test_delivery_coverage_obligations.py`
    pins against hand-built claims) silently accepts a retyped state or one that lost
    its correction's provenance."""
    edition = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
    )
    corrected = replace(
        source_occurrence(_record(edition, TemporalScope(kind="not_applicable"))),
        corrections=(
            AppliedCorrection(
                case_id="fix-one", effect_index=0, provenance="fixture:fix-one"
            ),
        ),
    )
    formed = _form(corrected, ())
    assert formed.variable is not None
    assert formed.variable.states[0].provenance == "fixture:fix-one"
    (obligation,) = formed.coverage
    assert (obligation.variant, obligation.column) == ("people", "VALUE")
    assert (obligation.valid_from, obligation.valid_to) == ("2020-01-01", "2020-12-31")
    assert obligation.data_type_claim == ("value", "integer")
    assert obligation.attributions == ("fixture:fix-one",)
