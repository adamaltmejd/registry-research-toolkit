"""Year-independent native formation: the one formation leg no build case reaches.

Only the LISA reader delivers a year-independent edition (`sources/lisa.py`), and the
`cases/build` runner has no LISA source, so this stays a direct formation test (the
same reason `test_catalog_lineage.py` keeps its year-independent legs). The other
formation behaviors this file pinned are legs of
`cases/build/formation-native-families-keep-literal-deliveries-or-stay-unresolved` or
build cases that already asserted them.
"""

from __future__ import annotations

from _csv_fixtures import SCB_REVISION
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_evidence import RecordLocator
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import effective_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)


def test_independent_delivery_forms_without_calendar_dates_and_retains_source():
    """Fails if formation dates a year-independent delivery, marks it pooled, drops
    its year-independent list, or owes the catalog a dated coverage claim for it."""
    scope = TemporalScope(kind="year_independent")
    record = SourceRecord.create(
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
        edition_scope=scope,
        edition_period_scope=scope,
        fields=SourceFields(
            availability=value_field(True),
            name=value_field("Source variable"),
            definition=value_field("Source definition"),
            column_name=value_field("VALUE"),
            data_type=value_field("integer"),
        ),
    )
    claim = CodeListClaim(
        "native-list",
        scope,
        (CodeMembershipClaim("02", "EU25 utom Norden", scope),),
        "EU25",
    )
    occurrence = effective_occurrence(record)
    assert occurrence.variant_key is not None and occurrence.column_key is not None
    formed = form_native_variable(
        (record,),
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants={
            occurrence.variant_key: ResolvedVariant(slug="people", name="People")
        },
        slug="value",
        provider_key="4",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={occurrence.column_key: resolve_code_membership((claim,))},
    )
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
