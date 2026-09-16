"""Typed source joins preserve evidence while supplying bounded coding claims."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta_build.prepared_values import (
    open_prepared_source_values,
    prepare_source_values,
)
from reg_meta_build.source_coding import resolve_code_membership
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
from reg_meta_build.source_value_bindings import bind_code_lists, open_value_bindings
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

from reg_meta_build import prepared_values

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
            provider="test",
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
            ("2020-01-01", "2020-06-30", (("", "Blank"),)),
            ("2020-07-01", "2020-12-31", (("", "Blank"), ("01", "One"))),
        ]
        assert result.bindings[0].association_count == 2
    assert tuple(source.associations()) == rows
    assert source.manifest.auxiliary_count == 0


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
    tmp_path: Path, missing: str, window: SourceValueWindow | None, expected: str | None
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


def test_normalized_member_collision_keeps_conflicting_complete_lists(
    tmp_path: Path,
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        descriptors=(SourceValueDescriptor("first"), SourceValueDescriptor("second")),
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
