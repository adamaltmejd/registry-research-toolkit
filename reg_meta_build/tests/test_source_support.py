"""Prepared source relations bind flags without fuzzy identity or source priority."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from reg_meta_build.source_coordinates import native_variable_key
from reg_meta_build.source_effects import record_ref
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
    register: int | None = 1,
    variable: int = 5,
    column: str | None = "VALUE",
    identifier: bool | None = None,
    edition: str | None = None,
    coverage: tuple[str, str] | None = None,
) -> SourceRecord:
    """One support or delivery row. ``edition`` is the target's own original
    version label; ``coverage`` the endpoints a summary row declares."""
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
            )
            if register is not None
            else SourceCoordinate(status="unknown"),
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
            coverage_from=value_field(coverage[0]) if coverage is not None else None,
            coverage_to=value_field(coverage[1]) if coverage is not None else None,
        ),
        edition_scope=TemporalScope(kind="unknown", label="No period supplied"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        original_period_text=edition,
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
    # One candidate family: its endpoints name no edition this target reports,
    # and the literal key alone still binds both rows.
    source = (
        _record("summary", identifier=False, coverage=("1997", "2001")),
        _record("summary", row=2, identifier=False, coverage=("2003", "2003")),
    )
    target = _record("delivery", edition="2020")
    index = SourceSupportBindings(_joins(), source)
    index.observe(target)
    index.observe(_record("delivery", row=2, edition="2021"))
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
    # An empty endpoint and an absent one are both incomplete, so neither row
    # reaches the discriminator even though each target names its own edition.
    target = _record("delivery", edition="2003")
    other = _record("delivery", row=2, variable=6, edition="2020")
    index = SourceSupportBindings(
        _joins(),
        (
            _record("summary", identifier=True, coverage=("2003", "")),
            _record("summary", row=2, identifier=True),
        ),
    )
    for record in (target, other):
        index.observe(record)
    index.seal()
    assert index.bind(target) == index.bind(other) == ()
    assert [i.code for i in index.diagnostics] == ["ambiguous_support_target"]
    assert {a.disposition for a in index.accounting} == {"ambiguous_target"}
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
    # Both rows state the same endpoints, so the discriminator binds them to one
    # renumbered candidate; their disagreement remains a conflict, not an OR.
    target = _record("delivery", edition="2003")
    other = _record("delivery", row=2, variable=6, edition="2020")
    records = (
        _record("summary", identifier=False, coverage=("2003", "2003")),
        _record("summary", row=2, identifier=True, coverage=("2003", "2003")),
    )
    index = SourceSupportBindings(_joins(), records)
    index.observe(target)
    index.observe(other)
    index.seal()
    assert index.bind(other) == ()
    matches = index.bind(target)
    fields, conflicts = reconcile_source_fields(
        (target,), support=tuple(m.fields for m in matches)
    )
    assert conflicts == ("identifier",)
    assert fields.identifier is not None and fields.identifier.status == "unknown"
    assert tuple(m.record for m in matches) == records


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [(True, False, True), (False, False, False), ("conditional", False, True)],
)
def test_unika_sensitivity_declarations_ratchet_up_and_keep_evidence(
    first: bool | str, second: bool, expected: bool
) -> None:
    target = _record("delivery", edition="2003")
    source = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"sensitivity": value_field(sensitivity)}
                )
            }
        )
        for record, sensitivity in (
            (_record("summary", coverage=("1990", "2009")), first),
            (_record("summary", row=2, coverage=("2010", "2023")), second),
        )
    )
    index = SourceSupportBindings(_joins(), source)
    index.observe(target)
    index.seal()
    matches = index.bind(target)
    fields, conflicts = reconcile_source_fields(
        (target,), support=tuple(match.fields for match in matches)
    )

    assert fields.sensitivity is not None
    assert fields.sensitivity.status == "value"
    assert fields.sensitivity.value is expected
    assert conflicts == ()
    assert tuple(match.record for match in matches) == source
    reverse, reverse_conflicts = reconcile_source_fields(
        (target,), support=tuple(match.fields for match in reversed(matches))
    )
    assert reverse == fields
    assert reverse_conflicts == conflicts


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
    join, identifiers = _joins()
    assert join.discriminator == ("coverage_from", "coverage_to")
    assert identifiers.discriminator == ()
    assert SourceSupportJoin.model_validate_json(join.model_dump_json()) == join
    for update in (
        {"keys": ()},
        {"fields": ("availability",)},
        {"target_sources": ("summary",)},
        {"discriminator": ("coverage_from", "coverage_from")},
        {"unique_variable": False},
    ):
        with pytest.raises(ValidationError):
            SourceSupportJoin.model_validate(join.model_dump() | update)


def test_renumbered_native_variables_take_only_their_own_declared_endpoints() -> None:
    # SCB renumbered one variable, so the four literal names cover two native
    # identities. Each summary row states the endpoints of its own edition set.
    old = _record("delivery", variable=5, edition="1989")
    old_again = _record("delivery", row=2, variable=5, edition="1989_old")
    new = _record("delivery", row=3, variable=6, edition="2003")
    source = (
        _record("summary", identifier=False, coverage=("1989", "1989_old")),
        _record("summary", row=2, identifier=False, coverage=("1989", "1989_old")),
        _record("summary", row=3, identifier=True, coverage=("2003", "2003")),
    )
    index = SourceSupportBindings(_joins(), source)
    for record in (old, old_again, new):
        index.observe(record)
    index.seal()
    assert index.diagnostics == ()
    # The repeated equal rows both stay with the candidate that carries them.
    assert [m.record for m in index.bind(old)] == list(source[:2])
    assert [m.record for m in index.bind(old_again)] == list(source[:2])
    assert [m.record for m in index.bind(new)] == [source[2]]
    assert [(a.disposition, a.targets) for a in index.accounting] == [
        ("bound_support", (native_variable_key(old),)),
        ("bound_support", (native_variable_key(old),)),
        ("bound_support", (native_variable_key(new),)),
    ]
    fields, conflicts = reconcile_source_fields(
        (new,), support=tuple(m.fields for m in index.bind(new))
    )
    assert conflicts == ()
    assert fields.identifier is not None and fields.identifier.value is True


def test_endpoints_both_candidates_carry_bind_to_neither_of_them() -> None:
    targets = (
        _record("delivery", variable=5, edition="1997"),
        _record("delivery", row=2, variable=5, edition="2001"),
        _record("delivery", row=3, variable=6, edition="1997"),
        _record("delivery", row=4, variable=6, edition="2001"),
    )
    index = SourceSupportBindings(
        _joins(), (_record("summary", identifier=False, coverage=("1997", "2001")),)
    )
    for record in targets:
        index.observe(record)
    index.seal()
    assert all(index.bind(record) == () for record in targets)
    assert [i.code for i in index.diagnostics] == ["ambiguous_support_target"]
    (item,) = index.accounting
    assert item.disposition == "ambiguous_target" and len(item.targets) == 2


def test_an_endpoint_no_candidate_carries_leaves_only_that_row_ambiguous() -> None:
    old = _record("delivery", variable=5, edition="1997")
    new = _record("delivery", row=2, variable=6, edition="2003")
    # 1997 and 1999 belong to no single candidate: naming 1997 does not make the
    # first candidate the owner of a 1999 that nobody observed.
    unlisted = _record("summary", identifier=False, coverage=("1997", "1999"))
    bound = _record("summary", row=2, identifier=False, coverage=("2003", "2003"))
    index = SourceSupportBindings(_joins(), (unlisted, bound))
    for record in (old, new):
        index.observe(record)
    index.seal()
    assert index.bind(old) == ()
    assert [m.record for m in index.bind(new)] == [bound]
    (issue,) = index.diagnostics
    assert issue.code == "ambiguous_support_target"
    assert issue.severity == "error"
    assert issue.refs == (record_ref(unlisted),)
    assert {a.record.record_id: a.disposition for a in index.accounting} == {
        unlisted.record_id: "ambiguous_target",
        bound.record_id: "bound_support",
    }


def test_a_candidate_without_native_identity_keeps_its_key_unresolved() -> None:
    join = SourceSupportJoin(
        source="summary",
        target_sources=("delivery",),
        keys=("variable_id",),
        fields=("identifier",),
        unique_variable=True,
        discriminator=("coverage_from", "coverage_to"),
        rule="native variable id, separated by declared endpoints",
        provenance=("fixture",),
    )
    known = _record("delivery", edition="2003")
    unidentified = _record("delivery", row=2, register=None, edition="2003")
    index = SourceSupportBindings(
        (join,), (_record("summary", identifier=True, coverage=("2003", "2003")),)
    )
    for record in (known, unidentified):
        index.observe(record)
    index.seal()
    assert index.bind(known) == index.bind(unidentified) == ()
    assert [i.code for i in index.diagnostics] == ["ambiguous_support_target"]
    (item,) = index.accounting
    assert item.disposition == "ambiguous_target"
    assert item.targets == (native_variable_key(known),)
