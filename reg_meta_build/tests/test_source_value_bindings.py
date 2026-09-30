"""Typed source joins preserve evidence while supplying bounded coding claims."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Literal

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta.source_evidence import RecordLocator, SourceField, SourceRevision
from reg_meta_build.prepared_values import (
    open_prepared_source_values,
    prepare_source_values,
)
from reg_meta_build.source_coding import (
    coding_content_sha256,
    coding_observation_fingerprints,
    copied_coding_fingerprints,
    resolve_code_membership,
)
from reg_meta_build.source_occurrences import source_occurrence
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
from reg_meta_build.source_support import SourceSupportBindings
from reg_meta_build.source_value_bindings import (
    bind_code_lists,
    bind_occurrence_code_lists,
    open_value_bindings,
)
from reg_meta_build.source_value_periods import value_period, value_window
from reg_meta_build.source_values import (
    SourceMemberHint,
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
    SourceValueValidity,
    SourceValueWindow,
)
from reg_meta_build.sources.scb_auxiliary import (
    clean_identifier_row,
    scb_support_joins,
)

from reg_meta_build import prepared_values, source_value_bindings

if TYPE_CHECKING:
    from pathlib import Path


def _revision(source: str) -> SourceRevision:
    return SourceRevision.create(
        dataset=source,
        publisher="Fixture",
        purpose="test",
        upstream_revision="1",
        artifact_path=source,
        artifact_size=0,
        artifact_sha256="0" * 64,
    )


def _record(
    *,
    source: str = "records",
    member: int | str = 1001,
    declared: str | None = None,
    description: str = "first",
    identifier: SourceField | None = None,
    provider: str = "test",
) -> SourceRecord:
    return SourceRecord.create(
        revision=_revision(source),
        locators=(
            RecordLocator(
                semantic_record_key=(str(member),),
                physical_file=source,
                physical_table="records",
                physical_record="row:2",
                physical_cells=("A2",),
            ),
        ),
        subject=SourceSubject(
            provider=provider,
            register=SourceCoordinate(status="value", native_id=1),
            variant=SourceCoordinate(status="value", native_id=2),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(status="value", native_id=3),
            member=SourceCoordinate(
                status="value",
                native_id=member if type(member) is int else None,
                name=member if isinstance(member, str) else None,
            ),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            column_name=value_field("column"),
            description=value_field(description),
            value_set_declared=value_field(declared) if declared else None,
            identifier=identifier,
        ),
    )


def _join(
    kind: Literal["native_member", "member_name", "declared_list"] = "native_member",
    *,
    source: str = "records",
    missing: Literal["unknown", "unrestricted"] = "unrestricted",
) -> SourceValueJoin:
    return SourceValueJoin(
        record_sources=(source,),
        member_target=kind,
        member_format="integer" if kind == "native_member" else "none",
        validity_target="item" if kind == "native_member" else "row",
        missing_validity=missing,
        rule="Fixture explicit structural relation",
        provenance=("fixture format",),
    )


def _prepare(
    path: Path,
    *,
    join: SourceValueJoin,
    descriptors: tuple[SourceValueDescriptor, ...] = (
        SourceValueDescriptor("list", version="v1"),
    ),
    values: tuple[SourceValue, ...] = (
        SourceValue("a", "01", "One"),
        SourceValue("b", "", "Blank"),
    ),
    rows: tuple[SourceValueAssociation, ...] | None = None,
    validity: tuple[SourceValueValidity, ...] = (),
    validity_present: bool = True,
):
    if rows is None:
        rows = (
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
        )
    manifest = prepare_source_values(
        path,
        revision=_revision("values"),
        descriptors=descriptors,
        values=values,
        associations=rows,
        validity=validity,
        validity_revision=_revision("validity") if validity_present else None,
        join=join,
    )
    commit = accept_prepared(path)
    return open_prepared_source_values(
        path, expected_sha256=manifest.sha256, input_commit=commit
    )


def test_occurrence_binding_distinguishes_coding_evidence_from_metadata_support(
    tmp_path: Path,
) -> None:
    values = _prepare(tmp_path / "values", join=_join())
    record = _record()
    original = source_occurrence(record)
    declared = replace(
        original,
        source_records=(),
        support_records=(record,),
        occurrence_key="added",
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2021", end="2021"),)
        ),
    )
    with open_value_bindings((values,)) as sessions:
        native = bind_occurrence_code_lists(original, sessions)
        assert native.claims and native.bindings
        metadata_only = bind_occurrence_code_lists(declared, sessions)
        assert metadata_only.claims == metadata_only.bindings == ()
        copied = bind_occurrence_code_lists(
            replace(declared, coding_records=(record, record)), sessions
        )
    assert len(copied.bindings) == 1
    assert {claim.scope for claim in copied.claims} == {declared.edition_scope}
    assert copied.bindings[0].record_locators == record.locators


@pytest.mark.parametrize("kind", ("pooled", "unknown"))
@pytest.mark.parametrize("constraint", ("item", "row", "section"))
def test_undated_copied_coding_pins_original_validity_constraints(
    tmp_path: Path, kind, constraint
) -> None:
    record = _record()
    copied = replace(
        source_occurrence(record),
        source_records=(),
        coding_records=(record,),
        support_records=(record,),
        occurrence_key="undated-copy",
        edition_scope=TemporalScope(kind=kind, label="Original unresolved coverage"),
    )
    fingerprints = []
    for year in (2019, 2020):
        window = SourceValueWindow("known", f"{year}-01-01", f"{year}-12-31")
        association = SourceValueAssociation(
            2,
            "list",
            "a",
            "values",
            member_id="1001",
            item_id="1",
            supplied_window=window if constraint == "row" else None,
            section_window=window if constraint == "section" else None,
        )
        validity = (
            (
                SourceValueValidity(
                    2, "1", window.start, window.end, "validity", window=window
                ),
            )
            if constraint == "item"
            else ()
        )
        source = _prepare(
            tmp_path / str(year) / "values",
            join=_join(),
            rows=(association,),
            validity=validity,
        )
        with open_value_bindings((source,)) as sessions:
            bound = bind_occurrence_code_lists(copied, sessions)
        fingerprints.append(coding_observation_fingerprints(bound.claims))
        assert bound.claims[0].members[0].associations == (association,)
        assert resolve_code_membership(bound.claims).segments == ()
    assert fingerprints[0] != fingerprints[1]


def test_finite_copy_pins_changed_validity_even_when_both_intersections_are_unknown(
    tmp_path: Path,
) -> None:
    association = SourceValueAssociation(
        2,
        "list",
        "a",
        "values",
        member_id="1001",
        item_id="1",
        supplied_window=SourceValueWindow("known", "2020-01-01", "2020-03-31"),
    )
    observations, copies = [], []
    for start, end in (("2020-07-01", "2020-09-30"), ("2020-10-01", "2020-12-31")):
        window = SourceValueWindow("known", start, end)
        source = _prepare(
            tmp_path / start / "values",
            join=_join(),
            rows=(association,),
            validity=(
                SourceValueValidity(2, "1", start, end, "validity", window=window),
            ),
        )
        with open_value_bindings((source,)) as sessions:
            bound = bind_code_lists(_record(), sessions)
        assert [issue.code for issue in bound.issues] == ["conflicting_code_validity"]
        assert bound.claims[0].members[0].scope.kind == "unknown"
        observations.append(coding_observation_fingerprints(bound.claims))
        copies.append(copied_coding_fingerprints(bound.claims))
    # Copy guards preserve the conflicting assertions while existing finite coding
    # decision fingerprints keep their scoped observation contract.
    assert observations[0] == observations[1]
    assert copies[0] != copies[1]


@pytest.mark.parametrize("mixed", [False, True])
def test_source_type_evidence_does_not_create_coding_or_require_item_validity(
    tmp_path: Path, mixed: bool
) -> None:
    rows = (SourceValueAssociation(2, "list", "marker", "values", member_id="1001"),)
    if mixed:
        rows += (
            SourceValueAssociation(
                3, "list", "code", "values", member_id="1001", item_id="1"
            ),
        )
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        descriptors=(SourceValueDescriptor("list", non_membership_codes=("TYPE",)),),
        values=(
            SourceValue("marker", "TYPE", "Numeric"),
            SourceValue("code", "01", "One"),
        ),
        rows=rows,
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(_record(), sessions)
    assert result.issues == ()
    assert result.bindings[0].non_membership_associations == rows[:1]
    assert result.bindings[0].item_validity_set_aside == ()
    assert result.bindings[0].association_count == len(rows)
    assert result.bindings[0].inactive_associations == ()
    if mixed:
        assert [member.code for member in result.claims[0].members] == ["01"]
        assert result.bindings[0].claim_id == result.claims[0].claim_id
    else:
        assert result.claims == ()
        assert result.bindings[0].claim_id is None


def test_native_join_preserves_raw_tokens_uses_one_session_and_exact_validity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = (
        SourceValueAssociation(
            2, "list", "a", "values", member_id="01001", item_id="01"
        ),
        SourceValueAssociation(3, "list", "b", "values", member_id="1001", item_id="2"),
        SourceValueAssociation(
            4, "list", "a", "values", member_id="broken", item_id="1"
        ),
    )
    validity = (
        SourceValueValidity(
            2,
            "1",
            "2020-07-01",
            None,
            "validity",
            window=value_window("2020-07-01", None),
        ),
    )
    source = _prepare(tmp_path / "values", join=_join(), rows=rows, validity=validity)
    calls = []
    real = prepared_values._readonly

    def tracked(path):
        calls.append(path)
        return real(path)

    monkeypatch.setattr(prepared_values, "_readonly", tracked)
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(_record(), sessions)
        opened = len(calls)
        for _ in range(10):
            assert bind_code_lists(_record(), sessions) == result
        assert len(calls) == opened
        assert next(iter(sessions[0].source_issues())).raw_member_tokens == ("broken",)
        assert result.issues == ()
        assert [
            member.associations[0].member_id for member in result.claims[0].members
        ] == ["01001", "1001"]
        resolved = resolve_code_membership(result.claims)
        assert [
            (s.valid_from, s.valid_to, s.code_set.members if s.code_set else None)
            for s in resolved.segments
        ] == [
            ("2020-01-01", "2020-12-31", (("", "Blank"), ("01", "One"))),
        ]
        assert result.bindings[0].association_count == 2
    assert tuple(source.associations()) == rows
    assert source.manifest.auxiliary_count == 0


def test_edition_list_sets_aside_global_item_dates_for_explicit_membership(
    tmp_path: Path,
) -> None:
    rows = (
        SourceValueAssociation(2, "list", "a", "values", member_id="1001", item_id="1"),
        SourceValueAssociation(3, "list", "b", "values", member_id="1001", item_id="2"),
    )
    validity = tuple(
        SourceValueValidity(
            index + 2,
            str(index + 1),
            "2008-11-19",
            None,
            "validity",
            window=value_window("2008-11-19", None),
        )
        for index in range(2)
    )
    source = _prepare(tmp_path / "values", join=_join(), rows=rows, validity=validity)
    with open_value_bindings((source,)) as sessions:
        old = TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="1990", end="1990"),)
        )
        bound = bind_code_lists(_record(), sessions, scope=old)
        assert bound.issues == ()
        assert len(bound.claims) == len(bound.bindings) == 1
        assert bound.bindings[0].claim_id == bound.claims[0].claim_id
        assert bound.bindings[0].item_validity_set_aside == rows
        assert bound.bindings[0].inactive_associations == ()
        assert [member.validity for member in bound.claims[0].members] == [
            (validity[0],),
            (validity[1],),
        ]
        assert [member.scope.kind for member in bound.claims[0].members] == [
            "year_independent",
            "year_independent",
        ]
        assert [
            issue.code for issue in resolve_code_membership(bound.claims).issues
        ] == []

    partial_validity = (
        validity[0],
        replace(
            validity[1],
            valid_from="2009-01-01",
            window=value_window("2009-01-01", None),
        ),
    )
    partial_source = _prepare(
        tmp_path / "partial", join=_join(), rows=rows, validity=partial_validity
    )
    partial = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2008", end="2008"),)
    )
    with open_value_bindings((partial_source,)) as sessions:
        bound = bind_code_lists(_record(), sessions, scope=partial)
    assert bound.bindings[0].item_validity_set_aside == rows
    assert [member.code for member in bound.claims[0].members] == ["01", ""]
    assert resolve_code_membership(bound.claims).issues == ()
    assert [member.validity for member in bound.claims[0].members] == [
        (partial_validity[0],),
        (partial_validity[1],),
    ]

    pooled = TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start="1971", end="2024"),),
    )
    pooled_validity = tuple(
        replace(
            item,
            valid_from="2012-01-04",
            window=value_window("2012-01-04", None),
        )
        for item in validity
    )
    pooled_source = _prepare(
        tmp_path / "pooled", join=_join(), rows=rows, validity=pooled_validity
    )
    with open_value_bindings((pooled_source,)) as sessions:
        bound = bind_code_lists(_record(), sessions, scope=pooled)
    assert bound.bindings[0].item_validity_set_aside == rows
    assert all(
        member.scope.kind == "year_independent" for member in bound.claims[0].members
    )
    assert resolve_code_membership(bound.claims).issues == ()


@pytest.mark.parametrize(
    "supplied_start,supplied_end,expected_issues",
    (
        ("1990-01-01", "1994-12-31", ("conflicting_code_validity",)),
        ("2001-01-01", "2004-12-31", ()),
    ),
)
def test_explicit_member_windows_remain_restrictions_on_association_fallback(
    tmp_path: Path,
    supplied_start: str,
    supplied_end: str,
    expected_issues: tuple[str, ...],
) -> None:
    rows = (
        SourceValueAssociation(
            2,
            "list",
            "a",
            "values",
            member_id="1001",
            item_id="1",
            supplied_window=value_window(supplied_start, supplied_end),
        ),
        SourceValueAssociation(3, "list", "b", "values", member_id="1001", item_id="2"),
    )
    validity = tuple(
        SourceValueValidity(
            index + 2,
            str(index + 1),
            start,
            None,
            "validity",
            window=value_window(start, None),
        )
        for index, start in enumerate(("1995-01-01", "2008-01-01"))
    )
    source = _prepare(tmp_path / "values", join=_join(), rows=rows, validity=validity)
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="1990", end="2000"),)
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(), sessions, scope=scope)
    assert tuple(issue.code for issue in bound.issues) == expected_issues
    assert bound.bindings[0].item_validity_set_aside == rows[1:]
    assert bound.bindings[0].inactive_associations == (
        rows[:1] if not expected_issues else ()
    )
    assert rows[1] in bound.claims[0].members[-1].associations
    if expected_issues:
        assert resolve_code_membership(bound.claims).issues
    else:
        assert all(
            rows[0] not in member.associations for member in bound.claims[0].members
        )


def test_item_validity_set_aside_is_independent_of_association_order(
    tmp_path: Path,
) -> None:
    rows = (
        SourceValueAssociation(2, "list", "a", "values", member_id="1001", item_id="1"),
        SourceValueAssociation(3, "list", "b", "values", member_id="1001", item_id="2"),
    )
    validity = tuple(
        SourceValueValidity(
            index + 2,
            str(index + 1),
            "2008-11-19",
            None,
            "validity",
            window=value_window("2008-11-19", None),
        )
        for index in range(2)
    )
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="1990", end="1990"),)
    )
    results = []
    for index, order in enumerate((rows, rows[::-1])):
        source = _prepare(
            tmp_path / str(index), join=_join(), rows=order, validity=validity
        )
        with open_value_bindings((source,)) as sessions:
            results.append(bind_code_lists(_record(), sessions, scope=scope))
    assert results[0] == results[1]


def test_item_validity_set_aside_keeps_section_excluded_association_inactive(
    tmp_path: Path,
) -> None:
    rows = (
        SourceValueAssociation(
            2,
            "list",
            "a",
            "values",
            member_id="1001",
            item_id="1",
            section_window=value_window("2010-01-01", "2010-12-31"),
        ),
        SourceValueAssociation(3, "list", "b", "values", member_id="1001", item_id="2"),
    )
    validity = tuple(
        SourceValueValidity(
            index + 2,
            str(index + 1),
            "2008-11-19",
            None,
            "validity",
            window=value_window("2008-11-19", None),
        )
        for index in range(2)
    )
    source = _prepare(tmp_path / "values", join=_join(), rows=rows, validity=validity)
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="1990", end="1990"),)
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(), sessions, scope=scope)
    assert bound.issues == ()
    assert bound.bindings[0].inactive_associations == rows[:1]
    assert bound.bindings[0].item_validity_set_aside == rows[1:]
    assert [member.code for member in bound.claims[0].members] == [""]


@pytest.mark.parametrize("obstacle", ("unknown", "type_marker", "section", "row"))
def test_item_validity_set_aside_respects_join_and_evidence_guards(
    tmp_path: Path, obstacle: str
) -> None:
    rows = (
        SourceValueAssociation(
            2,
            "list",
            "a",
            "values",
            member_id="1001",
            item_id="1",
            section_window=value_window("2010-01-01", "2010-12-31")
            if obstacle == "section"
            else None,
        ),
        SourceValueAssociation(
            3,
            "list",
            "b",
            "values",
            member_id="1001",
            item_id="2",
            section_window=value_window("2010-01-01", "2010-12-31")
            if obstacle == "section"
            else None,
        ),
    )
    if obstacle == "type_marker":
        rows = (replace(rows[0], value_key="marker"), rows[1])
    validity = tuple(
        SourceValueValidity(
            index + 2,
            str(index + 1),
            "2008-11-19",
            None,
            "values" if obstacle == "row" else "validity",
            window=SourceValueWindow("unknown")
            if obstacle == "unknown" and index == 1
            else value_window("2008-11-19", None),
        )
        for index in range(2)
    )
    source = _prepare(
        tmp_path / "values",
        join=_join("declared_list") if obstacle == "row" else _join(),
        descriptors=(
            SourceValueDescriptor(
                "list",
                name="Codes",
                non_membership_codes=("TYPE",) if obstacle == "type_marker" else (),
            ),
        ),
        values=(
            SourceValue("a", "01", "One"),
            SourceValue("b", "02", "Two"),
            SourceValue("marker", "TYPE", "Numeric"),
        ),
        rows=rows,
        validity=validity,
    )
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="1990", end="1990"),)
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(
            _record(declared="Codes" if obstacle == "row" else None),
            sessions,
            scope=scope,
        )
    if obstacle in {"unknown", "type_marker"}:
        assert bound.bindings[0].item_validity_set_aside == (
            rows[:1] if obstacle == "unknown" else rows[1:]
        )
    else:
        assert bound.bindings[0].item_validity_set_aside == ()
    if obstacle == "unknown":
        assert [issue.code for issue in bound.issues] == ["unknown_code_validity"]
        assert resolve_code_membership(bound.claims).issues
    elif obstacle == "type_marker":
        assert bound.issues == ()
        assert bound.bindings[0].non_membership_associations == rows[:1]
        assert [member.code for member in bound.claims[0].members] == ["02"]
    else:
        assert bound.issues == ()
        assert bound.claims[0].members == ()


def test_checked_effective_scope_preserves_source_validity_and_evidence(
    tmp_path: Path,
) -> None:
    row = SourceValueAssociation(
        2, "list", "a", "values", member_id="1001", item_id="1"
    )
    validity = SourceValueValidity(
        2, "1", "2020-07-01", None, "validity", window=value_window("2020-07-01", None)
    )
    source = _prepare(
        tmp_path / "values", join=_join(), rows=(row,), validity=(validity,)
    )
    record = _record()
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2021-01-01", end=None),)
    )
    with open_value_bindings((source,)) as sessions:
        original = bind_code_lists(record, sessions)
        corrected = bind_code_lists(record, sessions, scope=scope)
    assert corrected.bindings[0].record_id == record.record_id
    assert corrected.bindings[0].record_locators == record.locators
    assert corrected.claims[0].claim_id != original.claims[0].claim_id
    assert corrected.claims[0].members[0].associations == (row,)
    assert corrected.claims[0].members[0].validity == (validity,)
    resolved = resolve_code_membership(corrected.claims)
    assert resolved.issues == ()
    assert [(s.valid_from, s.valid_to) for s in resolved.segments] == [
        ("2021-01-01", "9999-12-31")
    ]


def test_equal_membership_periods_keep_distinct_association_and_validity_evidence(
    tmp_path: Path,
) -> None:
    window = value_window("2020-07-01", None)
    rows = tuple(
        SourceValueAssociation(
            index + 2, "list", code, "values", member_id="1001", item_id=str(index)
        )
        for index, code in enumerate(("a", "b"))
    )
    validity = tuple(
        SourceValueValidity(
            index + 2, str(index), "2020-07-01", None, "validity", window=window
        )
        for index in range(2)
    )
    source = _prepare(tmp_path / "values", join=_join(), rows=rows, validity=validity)
    with open_value_bindings((source,)) as sessions:
        members = bind_code_lists(_record(), sessions).claims[0].members
        assert [member.scope for member in members] == [
            TemporalScope(kind="year_independent")
        ] * 2
        assert [member.associations for member in members] == [(rows[0],), (rows[1],)]
        assert [member.validity for member in members] == [
            (validity[0],),
            (validity[1],),
        ]


def test_native_list_reuse_keeps_each_record_binding_and_checks_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _prepare(tmp_path / "values", join=_join())
    first, duplicate = _record(), _record(description="Different parent/prose evidence")
    assert first.record_id != duplicate.record_id
    with open_value_bindings((source,)) as sessions:
        session = sessions[0].session
        lookups = []
        lookup = session.lookup_native_member

        def tracked(member):
            lookups.append(member)
            return lookup(member)

        monkeypatch.setattr(session, "lookup_native_member", tracked)
        a = bind_code_lists(first, sessions)
        b = bind_code_lists(duplicate, sessions)
        assert len(lookups) == 1
        assert a.claims == b.claims
        assert a.bindings[0].record_id == first.record_id
        assert b.bindings[0].record_id == duplicate.record_id
        later = TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2021", end="2021"),)
        )
        c = bind_code_lists(first, sessions, scope=later)
        assert len(lookups) == 2
        assert c.claims[0].claim_id != a.claims[0].claim_id
        bind_code_lists(_record(member=1002), sessions)
        assert lookups == [1001, 1001, 1002]
    resolved = resolve_code_membership((*a.claims, *b.claims))
    assert resolved.segments == resolve_code_membership(a.claims).segments


@pytest.mark.parametrize(
    "missing,window,expected",
    [
        ("unknown", None, "unknown_code_validity"),
        ("unrestricted", SourceValueWindow("unknown"), "unknown_code_validity"),
        ("unrestricted", None, None),
    ],
)
def test_missing_file_unknown_window_and_unlisted_item_are_distinct(
    tmp_path: Path,
    missing: Literal["unknown", "unrestricted"],
    window: SourceValueWindow | None,
    expected: str | None,
) -> None:
    validity = (
        (SourceValueValidity(2, "1", "bad", "bad", "validity", window=window),)
        if window is not None
        else ()
    )
    source = _prepare(
        tmp_path / "values",
        join=_join(missing=missing),
        validity=validity,
        validity_present=missing != "unknown",
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(_record(), sessions)
    assert [issue.code for issue in result.issues] == ([expected] if expected else [])
    resolved = resolve_code_membership(result.claims)
    assert (resolved.segments[0].code_set is None) == (expected is not None)


@pytest.mark.parametrize(
    "identifier",
    (value_field(False), SourceField(status="unknown"), None),
)
def test_unresolved_non_identifier_list_keeps_both_errors(
    tmp_path: Path, identifier: SourceField | None
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(missing="unknown"),
        validity_present=False,
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=identifier), sessions)
    assert len(bound.claims) == 1
    assert [issue.code for issue in bound.issues] == ["unknown_code_validity"]
    assert [issue.code for issue in resolve_code_membership(bound.claims).issues] == [
        "unknown_code_membership"
    ]


def test_declared_identifier_drops_unresolvable_list_but_keeps_clean_list(
    tmp_path: Path,
) -> None:
    unresolved = _prepare(
        tmp_path / "unresolved",
        join=_join(missing="unknown"),
        validity_present=False,
    )
    clean = _prepare(tmp_path / "clean", join=_join())
    record = _record(identifier=value_field(True))
    with open_value_bindings((unresolved, clean)) as sessions:
        bound = bind_code_lists(record, sessions)
    assert len(bound.claims) == 1
    assert bound.issues == ()
    assert len(bound.bindings) == 2
    assert {binding.claim_id for binding in bound.bindings} == {
        None,
        bound.claims[0].claim_id,
    }
    resolved = resolve_code_membership(bound.claims)
    assert resolved.issues == ()
    assert resolved.segments[0].code_set is not None
    assert resolved.segments[0].code_set.members == (("01", "One"),)


def test_identifierare_join_drops_unresolvable_scb_record_coding(
    tmp_path: Path,
) -> None:
    record = _record(source="scb-registerinformation", provider="scb")
    assert record.fields.identifier is None
    cells = {
        "VarID": (True, "3", "3"),
        "Variabelnamn": (True, "Variable", "Variable"),
        "Variabeldefinition": (True, "Definition", "Definition"),
    }
    identifier = clean_identifier_row(
        tuple(cells), 2, cells, _revision("scb-identifierare")
    )
    support = SourceSupportBindings(
        scb_support_joins(
            {
                "Registerinformation.csv": "scb-registerinformation",
                "Identifierare.csv": "scb-identifierare",
            }
        ),
        (identifier,),
    )
    support.observe(record)
    support.seal()
    assert support.bind(record)[0].fields.identifier == value_field(True)
    source = _prepare(
        tmp_path / "values",
        join=_join(source="scb-registerinformation", missing="unknown"),
        validity_present=False,
    )
    with open_value_bindings((source,)) as sessions:
        unjoined = bind_occurrence_code_lists(source_occurrence(record), sessions)
        joined = bind_occurrence_code_lists(
            source_occurrence(record), sessions, support=support
        )
    assert len(unjoined.claims) == 1
    assert [issue.code for issue in unjoined.issues] == ["unknown_code_validity"]
    assert [
        issue.code for issue in resolve_code_membership(unjoined.claims).issues
    ] == ["unknown_code_membership"]
    assert joined.claims == joined.issues == ()
    assert joined.bindings[0].claim_id is None


@pytest.mark.parametrize("code,label", ((None, "One"), ("01", None)))
def test_declared_identifier_drops_unknown_membership(
    tmp_path: Path, code: str | None, label: str | None
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        values=(SourceValue("a", code, label),),
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=value_field(True)), sessions)
    assert bound.claims == bound.issues == ()
    assert bound.bindings[0].claim_id is None


def test_declared_identifier_drops_only_bad_membership_period(
    tmp_path: Path,
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        values=(SourceValue("a", "01", "One"), SourceValue("b", None, "Bad")),
        rows=(
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
            SourceValueAssociation(
                3,
                "list",
                "b",
                "values",
                member_id="1001",
                item_id="2",
                supplied_window=value_window("2020-07-01", "2020-07-31"),
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=value_field(True)), sessions)
    assert len(bound.claims) == 1
    assert bound.issues == ()
    assert bound.claims[0].drop_unknown_membership
    assert coding_content_sha256(bound.claims[0]) is None
    resolved = resolve_code_membership(bound.claims)
    assert resolved.issues == ()
    assert [
        (
            segment.valid_from,
            segment.valid_to,
            segment.code_set.members if segment.code_set else None,
        )
        for segment in resolved.segments
    ] == [
        ("2020-01-01", "2020-06-30", (("01", "One"),)),
        ("2020-07-01", "2020-07-31", None),
        ("2020-08-01", "2020-12-31", (("01", "One"),)),
    ]


def test_declared_identifier_drops_only_bad_validity_period(tmp_path: Path) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(missing="unknown"),
        rows=(
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
            SourceValueAssociation(
                3,
                "list",
                "b",
                "values",
                member_id="1001",
                item_id="2",
                supplied_window=value_window("2020-07-01", "2020-07-31"),
            ),
        ),
        validity=(
            SourceValueValidity(
                2, "1", None, None, "validity", window=SourceValueWindow("known")
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=value_field(True)), sessions)
    assert len(bound.claims) == 1
    assert bound.issues == ()
    resolved = resolve_code_membership(bound.claims)
    assert resolved.issues == ()
    assert [
        (
            segment.valid_from,
            segment.valid_to,
            segment.code_set.members if segment.code_set else None,
        )
        for segment in resolved.segments
    ] == [
        ("2020-01-01", "2020-06-30", (("01", "One"),)),
        ("2020-07-01", "2020-07-31", None),
        ("2020-08-01", "2020-12-31", (("01", "One"),)),
    ]


def test_declared_identifier_drops_validity_error_with_complete_membership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _prepare(tmp_path / "values", join=_join())
    monkeypatch.setattr(
        source_value_bindings,
        "_member_scope",
        lambda *_args, **_kwargs: (
            TemporalScope(kind="year_independent"),
            "unknown_code_validity",
        ),
    )
    with open_value_bindings((source,)) as sessions:
        declared = bind_code_lists(_record(identifier=value_field(True)), sessions)
        undeclared = bind_code_lists(_record(identifier=value_field(False)), sessions)
    assert declared.claims == declared.issues == ()
    assert declared.bindings[0].claim_id is None
    assert len(undeclared.claims) == 1
    assert [issue.code for issue in undeclared.issues] == ["unknown_code_validity"]
    assert resolve_code_membership(undeclared.claims).issues == ()


def test_mixed_identifier_declarations_affect_only_their_own_records(
    tmp_path: Path,
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(missing="unknown"),
        validity_present=False,
    )
    with open_value_bindings((source,)) as sessions:
        declared = bind_code_lists(
            _record(member=1001, identifier=value_field(True)), sessions
        )
        undeclared = bind_code_lists(
            _record(member=1001, identifier=value_field(False)), sessions
        )
    assert declared.claims == declared.issues == ()
    assert declared.bindings[0].claim_id is None
    assert len(undeclared.claims) == 1
    assert [issue.code for issue in undeclared.issues] == ["unknown_code_validity"]
    assert [
        issue.code for issue in resolve_code_membership(undeclared.claims).issues
    ] == ["unknown_code_membership"]


def test_occurrence_identifier_is_decided_per_source_record(tmp_path: Path) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        values=(SourceValue("a", None, "Unknown"),),
        rows=(
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
            SourceValueAssociation(
                3, "list", "a", "values", member_id="1002", item_id="2"
            ),
        ),
    )
    declared = _record(member=1001, identifier=value_field(True))
    other = _record(member=1002, identifier=value_field(False))
    occurrence = replace(source_occurrence(declared), source_records=(declared, other))
    assert occurrence.fields.identifier == value_field(True)
    with open_value_bindings((source,)) as sessions:
        bound = bind_occurrence_code_lists(occurrence, sessions)
    assert len(bound.claims) == 1
    assert [issue.code for issue in resolve_code_membership(bound.claims).issues] == [
        "unknown_code_membership"
    ]
    assert {binding.record_id: binding.claim_id for binding in bound.bindings} == {
        declared.record_id: None,
        other.record_id: bound.claims[0].claim_id,
    }


def test_repeated_binding_errors_share_context_but_keep_all_original_associations(
    tmp_path: Path,
) -> None:
    rows = (
        SourceValueAssociation(
            2, "first", "a", "values", member_id="1001", item_id="1"
        ),
        SourceValueAssociation(
            3, "first", "a", "values", member_id="1001", item_id="1"
        ),
        SourceValueAssociation(
            4, "second", "b", "values", member_id="1001", item_id="2"
        ),
    )
    source = _prepare(
        tmp_path / "values",
        join=_join(missing="unknown"),
        descriptors=(SourceValueDescriptor("first"), SourceValueDescriptor("second")),
        rows=rows,
        validity_present=False,
    )
    first, duplicate = _record(), _record(description="other physical observation")
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(first, sessions)
        repeated = bind_code_lists(duplicate, sessions)
    assert [(i.code, i.descriptor_key, i.occurrence_count) for i in result.issues] == [
        ("unknown_code_validity", "first", 2),
        ("unknown_code_validity", "second", 1),
    ]
    assert result.issues[0].association_locators == (rows[0].locator, rows[1].locator)
    assert result.issues[1].association_locators == (rows[2].locator,)
    assert sum(len(c.members) for c in result.claims) == len(rows)
    assert sum(b.association_count for b in result.bindings) == len(rows)
    assert all(i.record_id == first.record_id for i in result.issues)
    assert all(i.record_id == duplicate.record_id for i in repeated.issues)
    assert result.claims == repeated.claims
    assert all(
        s.code_set is None for s in resolve_code_membership(result.claims).segments
    )


@pytest.mark.parametrize("outside_dates", (False, True))
def test_normalized_member_collision_keeps_conflicting_complete_lists(
    tmp_path: Path,
    outside_dates: bool,
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        descriptors=(SourceValueDescriptor("first"), SourceValueDescriptor("second")),
        validity=tuple(
            SourceValueValidity(
                index + 2,
                str(index + 1),
                "2021-01-01",
                None,
                "validity",
                window=value_window("2021-01-01", None),
            )
            for index in range(2)
        )
        if outside_dates
        else (),
        rows=(
            SourceValueAssociation(
                2, "first", "a", "values", member_id="01001", item_id="1"
            ),
            SourceValueAssociation(
                3, "second", "b", "values", member_id="1001", item_id="2"
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(_record(), sessions)
    assert len(result.claims) == 2
    assert [issue.code for issue in resolve_code_membership(result.claims).issues] == [
        "conflicting_code_memberships"
    ]


def test_unresolved_member_list_is_reported_instead_of_a_partial_claim(
    tmp_path: Path,
) -> None:
    record = _record(member="EXACT")
    resolved = _record(member="EXACT", description="resolved")
    source = _prepare(
        tmp_path / "values",
        join=_join("member_name"),
        descriptors=(
            SourceValueDescriptor(
                "unresolved", record_ids=(record.record_id,), unresolved_members=True
            ),
            SourceValueDescriptor("resolved", record_ids=(resolved.record_id,)),
        ),
        rows=(SourceValueAssociation(2, "resolved", "a", "values"),),
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(record, sessions)
        other = bind_code_lists(resolved, sessions)

    # A delivered list whose members the source does not separate states no code
    # membership, and says so against its own record: silence would read as a
    # variable with no coding declaration at all.
    assert not result.claims
    assert not result.bindings
    assert [
        (issue.code, issue.descriptor_key, issue.record_id) for issue in result.issues
    ] == [("unresolved_member_list", "unresolved", record.record_id)]
    # The flag is per descriptor: every other list on the source still binds.
    assert [binding.descriptor_key for binding in other.bindings] == ["resolved"]
    assert other.claims and not other.issues


def test_named_and_inline_references_do_not_cross_sources_or_record_occurrences(
    tmp_path: Path,
) -> None:
    record = _record(member="EXACT")
    descriptors = (
        SourceValueDescriptor("explicit", member_references=("EXACT",)),
        SourceValueDescriptor(
            "hint", member_hints=(SourceMemberHint("sheet_suffix", "EXACT"),)
        ),
        SourceValueDescriptor("inline", record_ids=(record.record_id,)),
    )
    rows = tuple(
        SourceValueAssociation(i, key, "a", "values")
        for i, key in enumerate(("explicit", "hint", "inline"), start=2)
    )
    source = _prepare(
        tmp_path / "values",
        join=_join("member_name"),
        descriptors=descriptors,
        rows=rows,
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(record, sessions)
        changed = bind_code_lists(
            _record(member="EXACT", description="changed"), sessions
        )
        other = bind_code_lists(_record(member="EXACT", source="other"), sessions)
        hints = list(sessions[0].source_issues())
    assert {binding.descriptor_key for binding in result.bindings} == {
        "explicit",
        "inline",
    }
    assert [binding.descriptor_key for binding in changed.bindings] == ["explicit"]
    assert not other.claims
    assert [(issue.code, issue.descriptor_key) for issue in hints] == [
        ("unresolved_list_reference", "hint")
    ]


def test_shared_explicit_pointers_bind_only_their_exact_record_occurrences(
    tmp_path: Path,
) -> None:
    first, second = _record(member="FIRST"), _record(member="SECOND")
    descriptor = SourceValueDescriptor(
        "shared",
        member_references=("FIRST", "SECOND"),
        member_hints=(
            SourceMemberHint("sheet_suffix", "FIRST"),
            SourceMemberHint("variable_pointer", "FIRST", first.locators[0]),
            SourceMemberHint("variable_pointer", "SECOND", second.locators[0]),
        ),
    )
    source = _prepare(
        tmp_path / "values",
        join=_join("member_name"),
        descriptors=(
            descriptor,
            SourceValueDescriptor("inline", record_ids=(first.record_id,)),
        ),
        rows=(
            SourceValueAssociation(2, "shared", "a", "values"),
            SourceValueAssociation(3, "inline", "b", "values"),
        ),
    )
    elsewhere = SourceRecord.create(
        revision=_revision("records"),
        locators=(
            first.locators[0].model_copy(
                update={
                    "semantic_record_key": ("FIRST", "other"),
                    "physical_record": "row:99",
                }
            ),
        ),
        subject=first.subject,
        edition_scope=first.edition_scope,
        edition_period_scope=first.edition_period_scope,
        fields=first.fields,
    )
    with open_value_bindings((source,)) as sessions:
        bound_first = bind_code_lists(first, sessions)
        bound_second = bind_code_lists(second, sessions)
        same_name_elsewhere = bind_code_lists(elsewhere, sessions)
        outsider = bind_code_lists(_record(member="OUTSIDER"), sessions)
        other_source = bind_code_lists(
            _record(member="FIRST", source="other"), sessions
        )
    assert {binding.descriptor_key for binding in bound_first.bindings} == {
        "shared",
        "inline",
    }
    assert [binding.descriptor_key for binding in bound_second.bindings] == ["shared"]
    assert all(not result.issues for result in (bound_first, bound_second))
    assert all(
        not result.claims and not result.bindings
        for result in (same_name_elsewhere, outsider, other_source)
    )


@pytest.mark.parametrize("missing", ["pointer", "locator", "header", "row", "source"])
def test_incomplete_or_competing_shared_pointer_evidence_stays_ambiguous(
    tmp_path: Path, missing: str
) -> None:
    first, second = _record(member="FIRST"), _record(member="SECOND")
    pointer = SourceMemberHint("variable_pointer", "FIRST", first.locators[0])
    second_locator: RecordLocator | None = second.locators[0]
    if missing == "locator":
        second_locator = None
    elif missing == "source":
        second_locator = second_locator.model_copy(update={"physical_file": "other"})
    other_pointer = SourceMemberHint("variable_pointer", "SECOND", second_locator)
    hints = (pointer,) if missing == "pointer" else (pointer, other_pointer)
    if missing == "header":
        hints += (SourceMemberHint("list_header", "FIRST"),)
    source = _prepare(
        tmp_path / "values",
        join=_join("member_name"),
        descriptors=(
            SourceValueDescriptor(
                "shared", member_references=("FIRST", "SECOND"), member_hints=hints
            ),
        ),
        rows=(
            SourceValueAssociation(
                2,
                "shared",
                "a",
                "values",
                member_references=("FIRST",) if missing == "row" else (),
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(first, sessions)
    assert "ambiguous_list_member_references" in [issue.code for issue in result.issues]
    assert result.bindings[0].association_count == 1
    assert resolve_code_membership(result.claims).segments[0].code_set is None


def test_conflicting_named_references_withhold_and_outside_codes_stay_accounted(
    tmp_path: Path,
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join("member_name"),
        descriptors=(
            SourceValueDescriptor("list", member_references=("EXACT",)),
            SourceValueDescriptor("old", member_references=("EXACT",)),
        ),
        rows=(
            SourceValueAssociation(
                2, "list", "a", "values", member_references=("OTHER",)
            ),
            SourceValueAssociation(
                3, "old", "b", "values", supplied_window=value_period("2001-2002")
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(_record(member="EXACT"), sessions)
    assert [issue.code for issue in result.issues] == [
        "conflicting_list_member_references"
    ]
    old = next(
        binding for binding in result.bindings if binding.descriptor_key == "old"
    )
    assert len(old.inactive_associations) == old.association_count == 1
    assert (
        next(claim for claim in result.claims if claim.claim_id == old.claim_id).members
        == ()
    )
    assert resolve_code_membership(result.claims).segments[0].code_set is None


def test_declared_list_name_is_exact_and_scoped_and_absence_reported_once(
    tmp_path: Path,
) -> None:
    first = _prepare(
        tmp_path / "first",
        join=_join("declared_list"),
        descriptors=(SourceValueDescriptor("list", name="Named"),),
    )
    second = _prepare(
        tmp_path / "second",
        join=_join("declared_list"),
        descriptors=(SourceValueDescriptor("list", name="Other"),),
    )
    with open_value_bindings((first, second)) as sessions:
        result = bind_code_lists(_record(declared="Named"), sessions)
        missing = bind_code_lists(_record(declared="Missing"), sessions)
    assert len(result.claims) == 1 and not result.issues
    assert [issue.code for issue in missing.issues] == ["declared_value_list_not_found"]


@pytest.mark.parametrize(
    "first,last,compact,expected",
    [
        ("2020-02-29", "2020-03-01", False, ("2020-02-29", "2020-03-01")),
        ("202002", "202002", True, ("2020-02-01", "2020-02-29")),
        ("2019-02-29", None, False, None),
        ("2021", "2020", True, None),
        (None, "", False, (None, None)),
    ],
)
def test_value_bounds_keep_day_precision_and_invalid_bounds_unknown(
    first: str | None,
    last: str | None,
    compact: bool,
    expected: tuple[str | None, str | None] | None,
) -> None:
    window = value_window(first, last, compact_dates=compact)
    assert window.status == ("known" if expected is not None else "unknown")
    if expected is not None:
        assert (window.start, window.end) == expected


def test_absent_validity_cannot_be_mislabeled_unrestricted(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="absent item-validity"):
        _prepare(tmp_path / "values", join=_join(), validity_present=False)
    assert not (tmp_path / "values").exists()


def test_pooled_edition_binds_coding_over_the_whole_pooled_range(
    tmp_path: Path,
) -> None:
    """Y-207: a Vardemangder membership on a pooled edition binds its range.

    The pooled occurrence "2020 - 2022" carries 2020-01-01..2022-12-31; the
    membership window is clamped to exactly that range — never annual slices.
    """
    edition, period, issue = source_scopes("2020 - 2022")
    assert issue == "pooled_period"
    record = _record().model_copy(
        update={"edition_scope": edition, "edition_period_scope": period}
    )
    # Validity wider than the pooled range: binding clamps it to the range,
    # exactly as it clamps to an annual occurrence today.
    window = value_window("2019-01-01", "2023-12-31")
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        validity=(
            SourceValueValidity(
                2, "1", "2019-01-01", "2023-12-31", "validity", window=window
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(record, sessions)
    assert result.issues == ()
    assert len(result.claims) == 1
    assert result.claims[0].scope == period
    (member,) = result.claims[0].members
    assert (member.code, member.label) == ("01", "One")
    assert member.scope == TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start="2020-01-01", end="2022-12-31"),),
    )
    resolved = resolve_code_membership(result.claims)
    assert resolved.issues == ()
    assert [(s.valid_from, s.valid_to) for s in resolved.segments] == [
        ("2020-01-01", "2022-12-31")
    ]
    assert resolved.segments[0].code_set is not None
    assert resolved.segments[0].code_set.members == (("01", "One"),)


@pytest.mark.parametrize("kind", ("pooled", "unknown"))
def test_rangeless_pooled_and_unknown_scopes_stay_unsupported(
    tmp_path: Path, kind
) -> None:
    """Y-207: a pooled scope without a range, and an unknown scope, still
    yield `unsupported_coding_scope` — the range is carried evidence only."""
    scope = TemporalScope(kind=kind, label="Original unresolved coverage")
    record = _record().model_copy(
        update={"edition_scope": scope, "edition_period_scope": scope}
    )
    # Validity is present so binding reaches the occurrence-window check
    # instead of the year-independent shortcut.
    window = value_window("2019-01-01", "2023-12-31")
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        validity=(
            SourceValueValidity(
                2, "1", "2019-01-01", "2023-12-31", "validity", window=window
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(record, sessions)
    assert [issue.code for issue in result.issues] == ["unsupported_coding_scope"]
    resolved = resolve_code_membership(result.claims)
    assert resolved.segments == ()
    assert [issue.code for issue in resolved.issues] == ["unsupported_coding_scope"]
