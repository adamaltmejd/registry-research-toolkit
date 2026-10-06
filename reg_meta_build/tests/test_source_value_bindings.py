"""Typed source joins preserve evidence while supplying bounded coding claims: validity, membership windows and association fallback."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.source_coding import (
    coding_observation_fingerprints,
    copied_coding_fingerprints,
    resolve_code_membership,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
)
from reg_meta_build.source_value_bindings import (
    bind_code_lists,
    bind_occurrence_code_lists,
    open_value_bindings,
)
from reg_meta_build.source_value_periods import value_window
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueValidity,
    SourceValueWindow,
)

if TYPE_CHECKING:
    from pathlib import Path
from _source_value_bindings_support import (
    join_bindings as _join,
    prepare_value_sources as _prepare,
    value_record as _record,
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
