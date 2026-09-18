"""Ordinary native families form catalog content without whole-variable cases."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta.db import open_db
from reg_meta_build.catalog_dependencies import check_delivery_coverage
from reg_meta_build.convert_identity import convert_declared_partitions
from reg_meta_build.prepared_sources import (
    open_prepared_source_records,
    prepare_source_records,
)
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_effects import apply_occurrence_cases, record_ref
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import (
    AppliedCorrection,
    EffectiveOccurrence,
    effective_occurrence,
    source_occurrence,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)

from reg_meta_build.fqid_slugs import load_provider_toml

if TYPE_CHECKING:
    from pathlib import Path


_REVISION = SourceRevision.create(
    dataset="fixture",
    publisher="SCB",
    purpose="Formation fixture",
    upstream_revision="1",
    artifact_path="input.csv",
    artifact_size=1,
    artifact_sha256="a" * 64,
)
_REGISTER = ResolvedRegister(provider="scb", slug="example", name="Example")
# Keyed by the fixture's native variant id, the last native variant key element.
_VARIANTS: dict[str | int, ResolvedVariant] = {
    2: ResolvedVariant(slug="people", name="People"),
    3: ResolvedVariant(slug="households", name="Households"),
}
_VARIANT = _VARIANTS[2]
_FLAGS = SourceFields(sensitivity=value_field(False), identifier=value_field(False))


def _record(
    year: int,
    *,
    column: str = "VALUE",
    native_id: int | str = 4,
    definition: str = "Source definition",
    variant: int = 2,
    operational_definition: str | None = None,
    source_attribution: str | None = None,
    row: str = "",
) -> SourceRecord:
    return SourceRecord.create(
        revision=_REVISION,
        locators=(
            RecordLocator(
                semantic_record_key=("variable:4", f"year:{year}{row}"),
                physical_file="input.csv",
                physical_table="input.csv",
                physical_record=f"row:{year}{row}",
                physical_cells=(),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="value", native_id=1, name="Example"),
            variant=SourceCoordinate(status="value", native_id=variant, name="People"),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(
                status="value", native_id=native_id, name="Source variable"
            ),
            member=SourceCoordinate(status="value", native_id=year),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=str(year), end=str(year)),)
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            availability=value_field(True),
            name=value_field("Source variable"),
            definition=value_field(definition),
            operational_definition=value_field(operational_definition)
            if operational_definition
            else None,
            source_attribution=value_field(source_attribution)
            if source_attribution
            else None,
            column_name=value_field(column),
            data_type=value_field("integer"),
        ),
    )


def _form(
    records: tuple[SourceRecord | EffectiveOccurrence, ...],
    *,
    flags: SourceFields = _FLAGS,
    claims: tuple[CodeListClaim, ...] = (),
):
    variants = {}
    coding = {}
    for record in records:
        occurrence = effective_occurrence(record)
        if (key := occurrence.variant_key) is not None:
            variants[key] = _VARIANTS[key[-1]]
        if (key := occurrence.column_key) is not None:
            coding[key] = resolve_code_membership(claims)
    return form_native_variable(
        records,
        register=_REGISTER,
        variants=variants,
        slug="value",
        provider_key="4",
        flags=flags,
        coding=coding,
    )


def test_explicit_open_ended_coverage_survives_formation_and_storage(
    tmp_path: Path,
) -> None:
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2004-01-01", end=None),)
    )
    record = _record(2004).model_copy(update={"edition_period_scope": scope})
    claim = CodeListClaim(
        claim_id="accepted-list",
        scope=scope,
        members=(
            CodeMembershipClaim("01", "One", TemporalScope(kind="year_independent")),
        ),
    )
    result = _form((record,), claims=(claim,))
    assert result.variable is not None and result.diagnostics == ()
    state = result.variable.states[0]
    assert (state.valid_from, state.valid_to) == ("2004-01-01", "9999-12-31")
    output = tmp_path / "catalog.db"
    write_resolved_catalog((result.variable,), output, manifest={})
    with closing(open_db(output)) as conn:
        assert tuple(
            conn.execute("SELECT valid_from, valid_to FROM variable_state").fetchone()
        ) == ("2004-01-01", "9999-12-31")


def test_prepared_native_family_forms_and_writes_without_handwritten_cases(
    tmp_path: Path,
) -> None:
    records = (_record(2020), _record(2022))
    artifact = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        artifact,
        records=records,
        revisions=(_REVISION,),
        scope="complete synthetic source",
    )
    commit = accept_prepared(artifact)
    prepared = open_prepared_source_records(
        artifact, expected_sha256=manifest.sha256, input_commit=commit
    )
    result = _form(tuple(prepared.records))
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.occurrences == records
    output = tmp_path / "catalog" / "reg_meta.db"
    write_resolved_catalog(
        (result.variable,), output, manifest={"source": manifest.sha256}
    )
    with closing(open_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT provider_key, slug, name, definition FROM variable"
            ).fetchone()
        ) == ("4", "value", "Source variable", "Source definition")
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT valid_from, valid_to, delivery_column_name, value_set_id FROM variable_state ORDER BY valid_from"
            )
        ] == [
            ("2020-01-01", "2020-12-31", "VALUE", None),
            ("2022-01-01", "2022-12-31", "VALUE", None),
        ]


def test_native_identity_is_scoped_and_preserves_native_primitive_type() -> None:
    integer, string, distinct = (
        _record(2020),
        _record(2020, native_id="4"),
        _record(2020, native_id=5),
    )
    assert len({native_variable_key(r) for r in (integer, string, distinct)}) == 3
    assert native_variable_key(integer) == native_variable_key(_record(2021, variant=3))
    assert native_column_key(integer) != native_column_key(distinct)
    with pytest.raises(ValueError, match="one complete native"):
        _form((integer, string))


def test_source_native_multiple_columns_require_explicit_partition_or_alias_decision() -> (
    None
):
    records = (_record(2020, column="OLD"), _record(2021, column="NEW"))
    result = _form(records)
    assert result.variable is None
    assert [d.code for d in result.diagnostics] == ["unresolved_native_identity"]
    assert result.occurrences == records
    assert len(result.diagnostics[0].refs) == 2


def _fdb_record(year: int, *, column: str, variant: int) -> SourceRecord:
    return _record(year, column=column, native_id=830, variant=variant)


def test_fdb_two_spelling_ownership_forms_both_partitions(tmp_path: Path) -> None:
    """Y-167: the tracked 1.830 ownership lets both partitions form.

    Without the declaration the two spellings withhold as one unresolved native
    identity (strict behavior); with it, each partition forms with its exact
    literal delivery columns (diagnostic cleared by curation, not by folding).
    """
    pair = (
        _fdb_record(1999, column="GatuRest", variant=424),
        _fdb_record(2005, column="Gaturest", variant=424),
    )
    pair_variant = native_variant_key(pair[0])
    assert pair_variant is not None
    variants = {
        pair_variant: ResolvedVariant(
            slug="arbetsstalleenheter", name="Arbetsställeenheter"
        )
    }
    pair_columns = [native_column_key(record) for record in pair]
    assert len(pair_columns) == 2 and None not in pair_columns
    withheld = form_native_variable(
        pair,
        register=_REGISTER,
        variants=variants,
        slug="gaturest",
        provider_key="830.gaturest",
        flags=_FLAGS,
        coding={
            key: resolve_code_membership(()) for key in pair_columns if key is not None
        },
    )
    assert withheld.variable is None
    assert [d.code for d in withheld.diagnostics] == ["unresolved_native_identity"]
    # The tracked declaration (mirrors fqid_slugs/scb.toml Y-167 entry) feeds
    # the production entry point, never hand-plumbed.
    declaration = tmp_path / "scb.toml"
    declaration.write_text(
        '[variable."1.830.gaturest"]\n'
        'columns = { GatuRest = "1.830.gaturest", Gaturest = "1.830.gaturest", '
        'PGaturest = "1.830.pgaturest" }\n'
        'columns_ref = "Y-167 fixture reference"\n'
        '[variable."1.830.pgaturest"]\nslug = "pgaturest"\n',
        encoding="utf-8",
    )
    records = (
        *pair,
        _fdb_record(2010, column="GatuRest", variant=427),
        _fdb_record(2015, column="Gaturest", variant=427),
        _fdb_record(2020, column="PGaturest", variant=424),
        _fdb_record(2025, column="PGaturest", variant=427),
    )
    converted = convert_declared_partitions(
        records,
        entries=load_provider_toml(declaration),
        provider="scb",
        source_id="1.830",
        split_ids=("1.830.gaturest", "1.830.pgaturest"),
    )
    assert converted.case is not None and converted.diagnostics == ()
    applied = apply_occurrence_cases(records, (converted.case,))
    assert applied.diagnostics == ()
    slugs = {"1.830.gaturest": "gaturest", "1.830.pgaturest": "pgaturest"}
    formed = {}
    for binding in converted.bindings:
        members = tuple(
            o
            for o in applied.occurrences
            if o.variable_key == binding.target.source_key
        )
        assert members
        group_variants = {}
        coding = {}
        for occurrence in members:
            assert occurrence.variant_key is not None
            last = occurrence.variant_key[-1]
            group_variants[occurrence.variant_key] = ResolvedVariant(
                slug="arbetsstalleenheter" if last == 424 else "juridiska-enheter",
                name="Arbetsställeenheter" if last == 424 else "Juridiska enheter",
            )
            assert occurrence.column_key is not None
            coding[occurrence.column_key] = resolve_code_membership(())
        result = form_native_variable(
            members,
            register=_REGISTER,
            variants=group_variants,
            slug=slugs[binding.source_id],
            provider_key=binding.source_id,
            flags=_FLAGS,
            coding=coding,
        )
        assert result.variable is not None and result.diagnostics == ()
        formed[binding.source_id] = result.variable
    assert {
        state.delivery_column_name for state in formed["1.830.gaturest"].states
    } == {"GatuRest", "Gaturest"}
    assert {
        state.delivery_column_name for state in formed["1.830.pgaturest"].states
    } == {"PGaturest"}
    assert sorted(
        (state.valid_from, state.valid_to) for state in formed["1.830.gaturest"].states
    ) == [
        ("1999-01-01", "1999-12-31"),
        ("2005-01-01", "2005-12-31"),
        ("2010-01-01", "2010-12-31"),
        ("2015-01-01", "2015-12-31"),
    ]


def test_canonical_text_conflict_preserves_states_and_withholds_only_that_fact() -> (
    None
):
    result = _form((_record(2020), _record(2021, definition="Conflicting definition")))
    assert result.variable is not None
    assert result.variable.definition is None
    assert len(result.variable.states) == 2
    assert [(d.code, d.fields, d.withheld_output) for d in result.diagnostics] == [
        ("conflicting_variable_fact", ("definition",), ("variable.definition",))
    ]


def test_state_grain_texts_vary_by_period_without_a_variable_fact_conflict(
    tmp_path: Path,
) -> None:
    records = (
        _record(
            2020,
            operational_definition="Introductory question text",
            source_attribution="Fr171",
        ),
        _record(
            2021,
            operational_definition="Derived-variable definition",
            source_attribution="Fr128e",
        ),
    )
    result = _form(records)
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.variable.operational_definition is None
    assert result.variable.source_register_text is None
    output = tmp_path / "catalog.db"
    write_resolved_catalog((result.variable,), output, manifest={})
    with closing(open_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT operational_definition, source_register_text FROM variable"
            ).fetchone()
        ) == (None, None)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT valid_from, operational_definition, source_register_text "
                "FROM variable_state ORDER BY valid_from"
            )
        ] == [
            ("2020-01-01", "Introductory question text", "Fr171"),
            ("2021-01-01", "Derived-variable definition", "Fr128e"),
        ]


def test_state_grain_texts_vary_by_variant_without_a_variable_fact_conflict() -> None:
    result = _form(
        (
            _record(
                2020,
                variant=2,
                operational_definition="Person wording",
                source_attribution="Fr171",
            ),
            _record(
                2020,
                variant=3,
                operational_definition="Household wording",
                source_attribution="Fr128e",
            ),
        )
    )
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.variable.operational_definition is None
    assert result.variable.source_register_text is None
    assert sorted(
        (state.variant.slug, state.operational_definition, state.source_register_text)
        for state in result.variable.states
    ) == [
        ("households", "Household wording", "Fr128e"),
        ("people", "Person wording", "Fr171"),
    ]


def test_stable_state_grain_texts_still_summarize_the_variable() -> None:
    result = _form(
        tuple(
            _record(
                year,
                operational_definition="One definition",
                source_attribution="Fr171",
            )
            for year in (2020, 2021)
        )
    )
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.variable.operational_definition == "One definition"
    assert result.variable.source_register_text == "Fr171"
    assert {
        (state.operational_definition, state.source_register_text)
        for state in result.variable.states
    } == {("One definition", "Fr171")}


@pytest.mark.parametrize("field", ["operational_definition", "source_attribution"])
def test_competing_same_period_texts_still_withhold_that_occurrence_fact(
    field: str,
) -> None:
    def competing(text: str, row: str) -> SourceRecord:
        return _record(
            2020,
            row=row,
            operational_definition=text if field == "operational_definition" else None,
            source_attribution=text if field == "source_attribution" else None,
        )

    first, second = competing("First text", ""), competing("Second text", "b")
    result = _form((first, second))
    assert result.variable is not None
    assert [
        (d.code, d.fields, d.valid_from, d.valid_to, d.refs) for d in result.diagnostics
    ] == [
        (
            "conflicting_occurrence_facts",
            (field,),
            "2020-01-01",
            "2020-12-31",
            (record_ref(first), record_ref(second)),
        )
    ]
    assert [
        (state.operational_definition, state.source_register_text)
        for state in result.variable.states
    ] == [(None, None)]


def test_checked_identity_does_not_choose_a_parallel_column_representation() -> None:
    records = (
        replace(source_occurrence(_record(2020, column="OLD")), identity_checked=True),
        replace(source_occurrence(_record(2020, column="NEW")), identity_checked=True),
        replace(source_occurrence(_record(2021, column="NEW")), identity_checked=True),
    )
    result = _form(records)
    assert result.variable is not None
    assert [
        (state.delivery_column_name, state.valid_from, state.valid_to)
        for state in result.variable.states
    ] == [("NEW", "2021-01-01", "2021-12-31")]
    assert [
        (issue.code, issue.valid_from, issue.valid_to, issue.withheld_output)
        for issue in result.diagnostics
    ] == [("unresolved_column_representation", "2020-01-01", "2020-12-31", ("state",))]
    assert result.occurrences == records


def test_unknown_flags_withhold_unsupported_entity_without_defaulting_false() -> None:
    result = _form((_record(2020),), flags=SourceFields())
    assert result.variable is None
    assert result.diagnostics[0].code == "unresolved_flag"
    assert result.diagnostics[0].fields == ("is_sensitive", "is_identifier")
    assert len(result.intervals[0].segments) == 1


@pytest.mark.parametrize("sensitivity", [False, True])
def test_conditional_sensitivity_cannot_be_reduced_to_a_boolean(
    sensitivity: bool,
) -> None:
    result = _form(
        (_record(2020),),
        flags=SourceFields(
            sensitivity=value_field(sensitivity),
            identifier=value_field(False),
            conditional_sensitivity=value_field(True),
        ),
    )
    assert result.variable is None
    assert result.diagnostics[0].code == "unresolved_flag"
    assert result.diagnostics[0].fields == ("is_sensitive",)


def test_bound_code_validity_splits_ordinary_state_and_retains_version_label() -> None:
    claim = CodeListClaim(
        "list",
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
        ),
        (
            CodeMembershipClaim("0", "No", TemporalScope(kind="not_applicable")),
            CodeMembershipClaim(
                "1",
                "Yes",
                TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2020-04-02", end="2020-09-01"),),
                ),
            ),
        ),
        version_label="Supplied list label",
    )
    result = _form((_record(2020),), claims=(claim,))
    assert result.variable is not None
    assert result.diagnostics == ()
    assert [
        (
            s.valid_from,
            s.valid_to,
            len(s.value_set.members) if s.value_set else 0,
            s.value_set_version_label,
        )
        for s in result.variable.states
    ] == [
        ("2020-01-01", "2020-04-01", 1, "Supplied list label"),
        ("2020-04-02", "2020-09-01", 2, "Supplied list label"),
        ("2020-09-02", "2020-12-31", 1, "Supplied list label"),
    ]


def test_coding_conflict_preserves_variable_and_period_but_withholds_membership() -> (
    None
):
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
    )
    claims = tuple(
        CodeListClaim(
            identity,
            scope,
            (CodeMembershipClaim(code, "Label", TemporalScope(kind="not_applicable")),),
        )
        for identity, code in (("a", "0"), ("b", "1"))
    )
    result = _form((_record(2020),), claims=claims)
    assert result.variable is not None
    assert result.variable.states[0].value_set is None
    assert result.diagnostics[0].code == "conflicting_code_memberships"
    assert (result.diagnostics[0].valid_from, result.diagnostics[0].valid_to) == (
        "2020-01-01",
        "2020-12-31",
    )
    assert result.coding[0].claims == claims


@pytest.mark.parametrize("missing", ["variant", "coding"])
def test_missing_implementation_mapping_is_fatal_not_curation_backlog(
    missing: str,
) -> None:
    record = _record(2020)
    variant_key, column_key = native_variant_key(record), native_column_key(record)
    assert variant_key is not None and column_key is not None
    with pytest.raises(ValueError, match="missing"):
        form_native_variable(
            (record,),
            register=_REGISTER,
            variants={} if missing == "variant" else {variant_key: _VARIANT},
            slug="value",
            provider_key="4",
            flags=_FLAGS,
            coding={}
            if missing == "coding"
            else {column_key: resolve_code_membership(())},
        )


def test_formation_coverage_carries_type_and_refuses_silent_retype() -> None:
    result = _form((_record(2020),))
    assert result.variable is not None
    (obligation,) = result.coverage
    assert (obligation.variant, obligation.column) == ("people", "VALUE")
    assert (obligation.valid_from, obligation.valid_to) == (
        "2020-01-01",
        "2020-12-31",
    )
    assert obligation.data_type_claim == ("value", "integer")
    assert obligation.attributions == ()
    check_delivery_coverage((result.variable,), result.coverage, withheld={})
    damaged = result.variable.model_copy(
        update={
            "states": (
                result.variable.states[0].model_copy(update={"data_type": "text"}),
            )
        }
    )
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        check_delivery_coverage((damaged,), result.coverage, withheld={})
    assert "claimed data_type='integer' written 'text'" in str(failure.value)


def test_formation_coverage_carries_correction_attributions() -> None:
    from dataclasses import replace as _replace

    occurrence = _replace(
        source_occurrence(_record(2020)),
        corrections=(
            AppliedCorrection(
                case_id="fix-one", effect_index=0, provenance="fixture:fix-one"
            ),
        ),
    )
    result = _form((occurrence,))
    assert result.variable is not None
    (obligation,) = result.coverage
    assert obligation.attributions == ("fixture:fix-one",)
    assert result.variable.states[0].provenance == "fixture:fix-one"
    check_delivery_coverage((result.variable,), result.coverage, withheld={})
    stripped = result.variable.model_copy(
        update={
            "states": (
                result.variable.states[0].model_copy(update={"provenance": None}),
            )
        }
    )
    with pytest.raises(ValueError, match="claimed attributions"):
        check_delivery_coverage((stripped,), result.coverage, withheld={})
