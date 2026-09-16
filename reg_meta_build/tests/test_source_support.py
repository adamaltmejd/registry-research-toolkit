"""Prepared source relations bind flags without fuzzy identity or source priority."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from reg_meta_build.source_intervals import reconcile_source_fields
from reg_meta_build.source_records import (
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_support import SourceSupportBindings, SourceSupportJoin
from reg_meta_build.sources.scb_auxiliary import scb_support_joins


def _record(
    source: str,
    *,
    row: int = 1,
    register: int = 1,
    variable: int = 5,
    column: str | None = "VALUE",
    identifier: bool | None = None,
) -> SourceRecord:
    revision = SourceRevision.create(
        dataset=source,
        publisher="SCB",
        purpose="support fixture",
        upstream_revision="1",
        artifact_path=f"{source}.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    return SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=(f"row:{row}",),
                physical_file=f"{source}.csv",
                physical_table="Sheet",
                physical_record=str(row),
                physical_cells=(),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(
                status="value", native_id=register, name="Register"
            ),
            variant=SourceCoordinate(status="value", native_id=2, name="People"),
            variable=SourceCoordinate(
                status="value", native_id=variable, name="Variable"
            ),
            member=SourceCoordinate(status="value", native_id=row),
            population=SourceCoordinate(status="unknown"),
            native=NativeCoordinates(register_id=register, variable_id=variable),
        ),
        fields=SourceFields(
            column_name=value_field(column) if column is not None else None,
            identifier=value_field(identifier) if identifier is not None else None,
            description=value_field(f"{source} description"),
        ),
        edition_scope=TemporalScope(kind="unknown", label="No period supplied"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
    )


def _joins() -> tuple[SourceSupportJoin, ...]:
    return scb_support_joins(
        {
            "Registerinformation.csv": "delivery",
            "UnikaRegisterOchVariabler.csv": "summary",
            "Identifierare.csv": "ids",
        }
    )


def test_literal_summary_match_supplies_only_declared_fields_and_keeps_duplicates() -> (
    None
):
    source = (
        _record("summary", identifier=False),
        _record("summary", row=2, identifier=False),
    )
    target = _record("delivery")
    index = SourceSupportBindings(_joins(), source)
    index.observe(target)
    index.observe(_record("delivery", row=2))
    index.seal()
    matches = index.bind(target)
    assert len(matches) == 2 and index.diagnostics == ()
    assert [m.record for m in matches] == list(source)
    assert all(m.fields.description is None for m in matches)
    fields, conflicts = reconcile_source_fields(
        (target,), support=tuple(m.fields for m in matches)
    )
    assert conflicts == ()
    assert fields.identifier is not None and fields.identifier.value is False
    assert (
        fields.description is not None
        and fields.description.value == "delivery description"
    )
    assert len(index.accounting) == 2
    assert all(
        item.disposition == "bound_support" and len(item.targets) == 1
        for item in index.accounting
    )


def test_ambiguous_names_cannot_attach_flags_to_either_native_variable() -> None:
    target = _record("delivery")
    other = _record("delivery", row=2, variable=6)
    index = SourceSupportBindings(_joins(), (_record("summary", identifier=True),))
    for record in (target, other):
        index.observe(record)
    index.seal()
    assert index.bind(target) == index.bind(other) == ()
    assert index.diagnostics[0].code == "ambiguous_support_target"
    assert index.accounting[0].disposition == "ambiguous_target"
    assert len(index.accounting[0].targets) == 2


def test_native_identifier_declaration_has_its_explicit_source_wide_scope() -> None:
    first = _record("delivery")
    second = _record("delivery", register=9, column="OTHER")
    foreign = _record("other-source")
    source = _record("ids", identifier=True)
    index = SourceSupportBindings(_joins(), (source,))
    for record in (first, second, foreign):
        index.observe(record)
    index.seal()
    assert index.bind(first)[0].record == index.bind(second)[0].record == source
    assert index.bind(foreign) == ()
    assert len(index.accounting[0].targets) == 2 and index.diagnostics == ()


def test_unknown_and_unmatched_source_keys_never_join_to_unknown_targets() -> None:
    index = SourceSupportBindings(
        _joins(),
        (_record("summary", column=None), _record("summary", row=2, column="ABSENT")),
    )
    target = _record("delivery", column=None)
    index.observe(target)
    index.seal()
    assert index.bind(target) == ()
    assert {i.code: i.severity for i in index.diagnostics} == {
        "unknown_support_key": "error",
        "unreferenced_support_record": "warning",
    }
    assert {a.disposition for a in index.accounting} == {
        "unknown_key",
        "unreferenced_support",
    }


def test_conflicting_flag_declarations_are_preserved_without_boolean_or() -> None:
    target = _record("delivery")
    records = (
        _record("summary", identifier=False),
        _record("summary", row=2, identifier=True),
    )
    index = SourceSupportBindings(_joins(), records)
    index.observe(target)
    index.seal()
    matches = index.bind(target)
    fields, conflicts = reconcile_source_fields(
        (target,), support=tuple(m.fields for m in matches)
    )
    assert conflicts == ("identifier",)
    assert fields.identifier is not None and fields.identifier.status == "unknown"
    assert tuple(m.record for m in matches) == records


def test_incomplete_scan_missing_adapter_and_unobserved_variable_are_contract_errors() -> (
    None
):
    target = _record("delivery")
    index = SourceSupportBindings(_joins(), (_record("summary"),))
    with pytest.raises(ValueError, match="before binding"):
        index.bind(target)
    index.observe(target)
    index.seal()
    with pytest.raises(ValueError, match="already complete"):
        index.observe(target)
    with pytest.raises(ValueError, match="unobserved target"):
        index.bind(_record("delivery", variable=8))
    with pytest.raises(ValueError, match="missing support relationship"):
        SourceSupportBindings((), (_record("summary"),))


def test_relationship_contract_and_source_absence_are_explicit() -> None:
    assert scb_support_joins({}) == ()
    assert scb_support_joins({"Registerinformation.csv": "delivery"}) == ()
    join = _joins()[0]
    assert SourceSupportJoin.model_validate_json(join.model_dump_json()) == join
    for update in (
        {"keys": ()},
        {"fields": ("availability",)},
        {"target_sources": ("summary",)},
    ):
        with pytest.raises(ValidationError):
            SourceSupportJoin.model_validate(join.model_dump() | update)
