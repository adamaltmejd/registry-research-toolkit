"""Ordinary native families form catalog content without whole-variable cases."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta.db import open_db
from reg_meta_build.prepared_sources import (
    open_prepared_source_records,
    prepare_source_records,
)
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_coding import CodeListClaim, CodeMembershipClaim
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_formation import form_native_variable
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
_VARIANT = ResolvedVariant(slug="people", name="People")
_FLAGS = SourceFields(sensitivity=value_field(False), identifier=value_field(False))


def _record(
    year: int,
    *,
    column: str = "VALUE",
    native_id: int | str = 4,
    definition: str = "Source definition",
    variant: int = 2,
) -> SourceRecord:
    return SourceRecord.create(
        revision=_REVISION,
        locators=(
            RecordLocator(
                semantic_record_key=("variable:4", f"year:{year}"),
                physical_file="input.csv",
                physical_table="input.csv",
                physical_record=f"row:{year}",
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
            column_name=value_field(column),
            data_type=value_field("integer"),
        ),
    )


def _form(
    records: tuple[SourceRecord, ...],
    *,
    flags: SourceFields = _FLAGS,
    claims: tuple[CodeListClaim, ...] = (),
):
    variants = {}
    coding = {}
    for record in records:
        if (key := native_variant_key(record)) is not None:
            variants[key] = _VARIANT
        if (key := native_column_key(record)) is not None:
            coding[key] = claims
    return form_native_variable(
        records,
        register=_REGISTER,
        variants=variants,
        slug="value",
        provider_key="4",
        flags=flags,
        coding=coding,
    )


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


def test_unknown_flags_withhold_unsupported_entity_without_defaulting_false() -> None:
    result = _form((_record(2020),), flags=SourceFields())
    assert result.variable is None
    assert result.diagnostics[0].code == "unresolved_flag"
    assert result.diagnostics[0].fields == ("is_sensitive", "is_identifier")
    assert len(result.intervals[0].segments) == 1


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
            coding={} if missing == "coding" else {column_key: ()},
        )
