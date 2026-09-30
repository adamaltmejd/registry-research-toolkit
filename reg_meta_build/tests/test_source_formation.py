"""Ordinary native families form catalog content without whole-variable cases."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _curation_fixtures import write_fdb_partition_curation
from _prepared_fixtures import accept_prepared
from reg_meta.db import open_db
from reg_meta.source_evidence import RecordLocator, SourceField, SourceRevision
from reg_meta_build.catalog_dependencies import check_delivery_coverage
from reg_meta_build.curation_compile import convert_column_partitions
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
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import (
    AppliedCorrection,
    EffectiveOccurrence,
    effective_occurrence,
    source_occurrence,
)
from reg_meta_build.source_periods import source_scopes
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
from reg_meta_build.sources.swecov_column_types import StewardColumnStorage

from reg_meta_build.fqid_slugs import declared_column_ownership, load_provider_toml

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
    storage: dict[str, StewardColumnStorage] | None = None,
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
        storage=storage,
    )


def test_widened_segment_records_documented_and_storage_provenance() -> None:
    records = tuple(
        _record(2020, row=label).model_copy(
            update={
                "fields": _record(2020, row=label).fields.model_copy(
                    update={
                        "data_type": value_field(kind),
                        "data_length": value_field(length),
                    }
                )
            }
        )
        for label, kind, length in (("a", "varchar", "18"), ("b", "float", "53"))
    )
    storage = {
        "value": StewardColumnStorage(
            classes=frozenset({"integer", "decimal"}),
            provenance="SWECOV storage csv: CIS2018=int, CIS2020=float",
        )
    }
    result = _form(records, storage=storage)
    assert result.variable is not None
    (state,) = result.variable.states
    assert (state.data_type, state.data_length) == ("decimal", None)
    assert state.provenance is not None
    assert "Documented Datatyp: float, varchar" in state.provenance
    assert "CIS2018=int, CIS2020=float" in state.provenance
    assert not any(
        item.code in {"unknown_data_type", "conflicting_occurrence_facts"}
        for item in result.diagnostics
    )


def test_incompatible_documented_types_retain_detail_and_conflict() -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"data_type": value_field(kind)}
                )
            }
        )
        for record, kind in zip(
            (_record(2020, row="a"), _record(2020, row="b")),
            ("date", "integer"),
        )
    )
    result = _form(
        records,
        storage={
            "value": StewardColumnStorage(
                classes=frozenset({"integer"}),
                provenance="SWECOV storage csv: CIS2020=int",
            )
        },
    )
    (unknown,) = (
        item for item in result.diagnostics if item.code == "unknown_data_type"
    )
    assert "Documented Datatyp: date, integer" in unknown.detail
    assert "CIS2020=int" in unknown.detail
    assert any(
        item.code == "conflicting_occurrence_facts" for item in result.diagnostics
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


def test_pooled_only_edition_forms_one_marked_state(tmp_path: Path) -> None:
    """Y-202: a genuinely pooled multi-year edition forms ONE marked state.

    The whole pooled range lands in the DB with `pooled = 1` — no annual
    inference inside the range, no unsupported-occurrence error."""
    edition, period, issue = source_scopes("2012 - 2014")
    assert issue == "pooled_period"
    record = _record(2012, row="-pooled").model_copy(
        update={"edition_scope": edition, "edition_period_scope": period}
    )
    result = _form((record,))
    assert result.variable is not None and result.diagnostics == ()
    (state,) = result.variable.states
    assert (state.valid_from, state.valid_to) == ("2012-01-01", "2014-12-31")
    assert state.pooled is True
    output = tmp_path / "catalog.db"
    write_resolved_catalog((result.variable,), output, manifest={})
    with closing(open_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT valid_from, valid_to, pooled FROM variable_state"
            ).fetchone()
        ) == ("2012-01-01", "2014-12-31", 1)


def test_pooled_edition_state_carries_its_value_set() -> None:
    """Y-207: a pooled variable's one marked state carries its bound codes.

    The coding resolves over the whole pooled range, so formation finds the
    membership for the occurrence period — no `missing_coding_period`."""
    edition, period, issue = source_scopes("2020 - 2022")
    assert issue == "pooled_period"
    record = _record(2020, row="-pooled").model_copy(
        update={"edition_scope": edition, "edition_period_scope": period}
    )
    claim = CodeListClaim(
        claim_id="pooled-list",
        scope=period,
        members=(
            CodeMembershipClaim("01", "One", TemporalScope(kind="year_independent")),
        ),
    )
    result = _form((record,), claims=(claim,))
    assert result.variable is not None
    assert result.diagnostics == ()
    (state,) = result.variable.states
    assert (state.valid_from, state.valid_to) == ("2020-01-01", "2022-12-31")
    assert state.pooled is True
    assert state.value_set is not None
    assert state.value_set.members == (("01", "One"),)


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


def test_case_only_column_twins_fold_to_one_variable() -> None:
    """Y-210: SRU `v0115`/`V0115` is one physical column needing no curation."""
    records = (
        _record(2004, column="v0115"),
        _record(2005, column="V0115"),
        _record(2006, column="V0115"),
    )
    result = _form(records)
    assert result.variable is not None
    assert not any(d.severity == "error" for d in result.diagnostics)
    assert [(s.valid_from, s.delivery_column_name) for s in result.variable.states] == [
        ("2004-01-01", "V0115"),
        ("2005-01-01", "V0115"),
        ("2006-01-01", "V0115"),
    ]
    (warning,) = [d for d in result.diagnostics if d.code == "column_spelling_folded"]
    assert warning.severity == "warning"
    assert (
        "v0115" in warning.detail and "most recent spelling (V0115)" in warning.detail
    )
    assert {r.fields.column_name.value for r in result.occurrences} == {
        "v0115",
        "V0115",
    }


def test_diacritic_column_twins_fold_to_one_variable() -> None:
    """Y-210: `Kön`/`Kon` fold by the shared column-identity key."""
    result = _form((_record(2020, column="Kön"), _record(2021, column="Kon")))
    assert result.variable is not None
    assert not any(d.severity == "error" for d in result.diagnostics)
    assert {s.delivery_column_name for s in result.variable.states} == {"Kon"}
    (warning,) = [d for d in result.diagnostics if d.code == "column_spelling_folded"]
    assert "Kön" in warning.detail and "most recent spelling (Kon)" in warning.detail


def test_co_delivered_twins_keep_the_error_and_form_nothing() -> None:
    """Y-220: `Niva`/`Nivå` side by side in one edition are two columns.

    Sequential twins keep folding; one edition co-delivering two spellings of
    one fold key keeps the pre-Y-210 `unresolved_native_identity` error and
    forms nothing."""
    probe = effective_occurrence(_record(2001, column="Niva"))
    assert probe.variant_key is not None
    edition_key = (*probe.variant_key, "edition", "native-int", 14946)

    def _edition_record(column: str, data_length: str) -> EffectiveOccurrence:
        record = _record(2001, column=column)
        return replace(
            effective_occurrence(
                record.model_copy(
                    update={
                        "fields": record.fields.model_copy(
                            update={"data_length": value_field(data_length)}
                        )
                    }
                )
            ),
            edition_key=edition_key,
        )

    result = _form((_edition_record("Niva", "3"), _edition_record("Nivå", "1")))
    assert result.variable is None
    assert result.coverage == ()
    assert [d.code for d in result.diagnostics] == ["unresolved_native_identity"]
    assert result.diagnostics[0].severity == "error"
    distinct = _form((_record(2020, column="OLD"), _record(2021, column="NEW")))
    assert result.diagnostics[0].detail == distinct.diagnostics[0].detail
    assert result.diagnostics[0].fields == distinct.diagnostics[0].fields


def test_unfolded_columns_still_require_partition_or_alias() -> None:
    """Y-210: distinct folds keep the error; no separator rule is added."""
    for old, new in (("BLK", "BLKFTG"), ("H56", "H5_6")):
        result = _form((_record(2020, column=old), _record(2021, column=new)))
        assert result.variable is None
        assert [d.code for d in result.diagnostics] == ["unresolved_native_identity"]


def test_checked_column_keeps_its_literal_when_twins_fold() -> None:
    """Y-210 a1: folding never rewrites an explicit partition/alias decision.

    The checked 2002 `V0115` state keeps its literal while the unchecked twins
    take the most recent spelling (`v0115` from 2005); coverage follows each
    output's own origin. Origin is tracked explicitly, never by literal."""
    checked = replace(
        effective_occurrence(_record(2002, column="V0115")), identity_checked=True
    )
    result = _form(
        (checked, _record(2004, column="V0115"), _record(2005, column="v0115"))
    )
    assert result.variable is not None
    assert not any(d.severity == "error" for d in result.diagnostics)
    assert [(s.valid_from, s.delivery_column_name) for s in result.variable.states] == [
        ("2002-01-01", "V0115"),
        ("2004-01-01", "v0115"),
        ("2005-01-01", "v0115"),
    ]
    (warning,) = [d for d in result.diagnostics if d.code == "column_spelling_folded"]
    assert "most recent spelling (v0115)" in warning.detail
    assert sorted((c.valid_from, c.column) for c in result.coverage) == [
        ("2002-01-01", "V0115"),
        ("2004-01-01", "v0115"),
        ("2005-01-01", "v0115"),
    ]


def _fdb_record(year: int, *, column: str, variant: int) -> SourceRecord:
    return _record(year, column=column, native_id=830, variant=variant)


def test_fdb_two_spelling_ownership_forms_both_partitions(tmp_path: Path) -> None:
    """Y-167: the tracked 1.830 ownership lets both partitions form.

    Y-210: without the declaration the case-only twins fold to one column and
    form with the most recent spelling (diagnostic by folding, not by curation);
    with it, each partition forms with its exact literal delivery columns.
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
    folded = form_native_variable(
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
    assert folded.variable is not None
    assert [(s.valid_from, s.delivery_column_name) for s in folded.variable.states] == [
        ("1999-01-01", "Gaturest"),
        ("2005-01-01", "Gaturest"),
    ]
    (warning,) = [d for d in folded.diagnostics if d.code == "column_spelling_folded"]
    assert warning.severity == "warning"
    assert "GatuRest" in warning.detail and "Gaturest" in warning.detail
    assert not any(d.severity == "error" for d in folded.diagnostics)
    # The register-scoped declaration feeds the production entry point.
    declaration = tmp_path / "scb.toml"
    declaration.write_text(
        '[register."1"]\nslug = "fdb"\n'
        '[variable."1.830.gaturest"]\nslug = "gaturest"\n'
        '[variable."1.830.pgaturest"]\nslug = "pgaturest"\n',
        encoding="utf-8",
    )
    curation_dir = write_fdb_partition_curation(tmp_path / "curation")
    records = (
        *pair,
        _fdb_record(2010, column="GatuRest", variant=427),
        _fdb_record(2015, column="Gaturest", variant=427),
        _fdb_record(2020, column="PGaturest", variant=424),
        _fdb_record(2025, column="PGaturest", variant=427),
    )
    ownership = declared_column_ownership(
        load_provider_toml(declaration),
        provider="scb",
        source_id="1.830",
        curation_dir=curation_dir,
    )
    converted = convert_column_partitions(
        records,
        source_id="1.830",
        split_ids=("1.830.gaturest", "1.830.pgaturest"),
        declared_columns=dict(ownership.declared_columns),
        declaration_reference=ownership.declaration_reference,
    )
    assert converted.case is not None and converted.diagnostics == ()
    assert converted.case.decision.provenance.endswith(
        "column ownership: Y-167 fixture reference"
    )
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


@pytest.mark.parametrize(
    "field", ["name", "definition", "description", "measurement_unit"]
)
def test_variable_grain_canonical_conflict_fields_are_unchanged(field: str) -> None:
    first = _record(2020)
    second = _record(2021)
    first = first.model_copy(
        update={
            "fields": first.fields.model_copy(update={field: value_field("First text")})
        }
    )
    second = second.model_copy(
        update={
            "fields": second.fields.model_copy(
                update={field: value_field("Conflicting text")}
            )
        }
    )
    result = _form((first, second))
    assert [
        (d.fields, d.withheld_output)
        for d in result.diagnostics
        if d.code == "conflicting_variable_fact"
    ] == [((field,), (f"variable.{field}",))]


@pytest.mark.parametrize(
    ("published", "other"),
    [
        ("Kronor", "kronor"),
        ("Kronor (SEK)", "kronor"),
        ("Antal månader", "Månader"),
        ("Månader", "Månad"),
        ("Antal veckor", "Veckor"),
        ("Antal minuter", "Minuter"),
        ("Antal barn", "Antal"),
        ("Dagar", "Antal"),
        ("Årtal", "År"),
    ],
)
def test_exact_unit_pair_publishes_variable_unit_in_either_order(
    published: str, other: str
) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"measurement_unit": value_field(unit)}
                )
            }
        )
        for record, unit in ((_record(2020), published), (_record(2021), other))
    )
    for ordered in (records, records[::-1]):
        result = _form(ordered)
        assert result.variable is not None
        assert result.variable.measurement_unit == published
        assert not any(
            diagnostic.code == "conflicting_variable_fact"
            for diagnostic in result.diagnostics
        )


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
def test_competing_same_period_texts_resolve_to_unknown_without_diagnostic(
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
    assert result.diagnostics == ()
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


@pytest.mark.parametrize(
    ("sensitivity", "conditional", "expected"),
    [
        (False, True, True),
        (True, True, True),
        (None, True, True),
        (False, False, False),
        (True, False, True),
    ],
)
def test_conditional_sensitivity_ratchets_up_without_changing_plain_values(
    sensitivity: bool | None, conditional: bool, expected: bool
) -> None:
    declaration = value_field(sensitivity) if sensitivity is not None else None
    result = _form(
        (_record(2020),),
        flags=SourceFields(
            sensitivity=declaration,
            identifier=value_field(False),
            conditional_sensitivity=value_field(conditional),
        ),
    )
    assert result.variable is not None
    assert result.variable.is_sensitive is expected
    assert result.diagnostics == ()


def test_conditional_false_alone_does_not_supply_sensitivity() -> None:
    result = _form(
        (_record(2020),),
        flags=SourceFields(
            identifier=value_field(False), conditional_sensitivity=value_field(False)
        ),
    )
    assert result.variable is None
    assert result.diagnostics[0].code == "unresolved_flag"
    assert result.diagnostics[0].fields == ("is_sensitive",)


def test_unknown_conditional_declaration_ratchets_to_sensitive() -> None:
    result = _form(
        (_record(2020),),
        flags=SourceFields(
            sensitivity=value_field(False),
            identifier=value_field(False),
            conditional_sensitivity=SourceField(status="unknown", raw_value="unclear"),
        ),
    )
    assert result.variable is not None
    assert result.variable.is_sensitive is True


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


def test_mixed_columnless_and_unresolvable_occurrence_stays_an_error() -> None:
    base = _record(2020)
    columnless = base.model_copy(
        update={
            "fields": base.fields.model_copy(
                update={
                    "column_name": SourceField(status="negative", raw_value=""),
                }
            )
        }
    )
    periodless = _record(2021).model_copy(
        update={
            "edition_scope": TemporalScope(kind="unknown", label="okänd"),
        }
    )
    formed = _form((columnless, periodless))
    assert formed.variable is None
    (omitted,) = [
        d for d in formed.diagnostics if d.code == "omitted_columnless_occurrence"
    ]
    assert omitted.severity == "warning"
    (terminal,) = [d for d in formed.diagnostics if d.code == "no_supported_states"]
    assert terminal.severity == "error"


def test_independent_delivery_forms_without_calendar_dates_and_retains_source():
    scope = TemporalScope(kind="year_independent")
    record = _record(2020).model_copy(
        update={"edition_scope": scope, "edition_period_scope": scope}
    )
    claim = CodeListClaim(
        "native-list",
        scope,
        (CodeMembershipClaim("02", "EU25 utom Norden", scope),),
        "EU25",
    )
    formed = _form((record,), claims=(claim,))
    assert formed.diagnostics == ()
    assert formed.occurrences == (record,)
    (state,) = formed.variable.states
    assert state.period_scope == "year_independent"
    assert state.valid_from is state.valid_to is None
    assert state.pooled is False
    assert state.value_set.members == (("02", "EU25 utom Norden"),)
    (obligation,) = formed.coverage
    assert obligation.period_scope == "year_independent"
    assert obligation.valid_from is obligation.valid_to is None
