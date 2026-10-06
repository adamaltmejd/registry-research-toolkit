"""Typed source joins: binding errors, named and shared pointers, value bounds and pooled editions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_periods import source_scopes
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceRecord,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_value_bindings import (
    bind_code_lists,
    open_value_bindings,
)
from reg_meta_build.source_value_periods import value_period, value_window
from reg_meta_build.source_values import (
    SourceMemberHint,
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueValidity,
)

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta.source_evidence import RecordLocator
from _source_value_bindings_support import (
    join_bindings as _join,
    prepare_value_sources as _prepare,
    value_record as _record,
    value_revision as _revision,
)


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


def test_identifier_inline_enumeration_retains_incomplete_membership(
    tmp_path: Path,
) -> None:
    record = _record(member="EXACT", identifier=value_field(True))
    source = _prepare(
        tmp_path / "values",
        join=_join("member_name"),
        descriptors=(SourceValueDescriptor("inline", record_ids=(record.record_id,)),),
        rows=(SourceValueAssociation(2, "inline", "a", "values"),),
        values=(SourceValue("a", "1", None),),
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(record, sessions)
    assert len(bound.claims) == 1
    assert bound.claims[0].members[0].code == "1"
    assert bound.claims[0].members[0].label is None
    assert bound.bindings[0].association_count == 1
    assert bound.bindings[0].claim_id == bound.claims[0].claim_id
    assert [issue.code for issue in resolve_code_membership(bound.claims).issues] == [
        "unknown_code_membership"
    ]


@pytest.mark.parametrize(
    "drift",
    (
        None,
        "pointer",
        "case_pointer",
        "sheet_locator",
        "file",
        "header",
        "headers",
        "row",
        "column",
        "anchor",
    ),
)
def test_explicit_workbook_pointer_preserves_named_header_case(
    tmp_path: Path, drift: str | None
) -> None:
    record = _record(member="CIVIL")
    record = record.model_copy(
        update={
            "fields": record.fields.model_copy(
                update={
                    "column_name": value_field(
                        "CIVIL" if drift != "column" else "OTHER"
                    ),
                    "representation": value_field(
                        {
                            "pointer": "Se Kodlista_Other",
                            "case_pointer": "Se kodlista_Civil",
                            "anchor": "Se Kodlista_Civil!A1",
                        }.get(drift, "Se Kodlista_Civil")
                    ),
                }
            )
        }
    )
    locator = record.locators[0].model_copy(
        update={
            "physical_file": "other.xlsx"
            if drift == "file"
            else record.locators[0].physical_file,
            "physical_table": "OTHER" if drift == "sheet_locator" else "Kodlista_Civil",
            "physical_record": "row:1",
        }
    )
    header = "OTHER" if drift == "header" else "Civil"
    hints = (SourceMemberHint("list_header", header, locator),)
    refs = (header,)
    if drift == "headers":
        hints += (SourceMemberHint("list_header", "SIBLING", locator),)
        refs += ("SIBLING",)
    descriptor = SourceValueDescriptor(
        "list",
        name="Kodlista_Civil",
        member_hints=hints,
        member_references=refs,
        locators=(locator,),
    )
    rows = (
        SourceValueAssociation(
            2,
            "list",
            "a",
            "values",
            member_references=("SIBLING",) if drift == "row" else (),
        ),
    )
    source = _prepare(
        tmp_path / "values",
        join=_join("member_name"),
        descriptors=(descriptor,),
        rows=rows,
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(record, sessions)
    assert bool(result.claims) is (drift is None)
    if drift is None:
        assert [(m.code, m.label) for m in result.claims[0].members] == [("01", "One")]
        assert result.claims[0].members[0].associations == rows
        assert source.manifest.descriptor_count == 1
