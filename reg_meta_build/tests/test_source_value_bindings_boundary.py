"""Native-member code-list binding observed through the public binding results.

Every case prepares a synthetic accepted value source with ``prepare_source_values``
and asserts on the documented public return models: ``bind_code_lists`` →
``ValueBindingResult``, ``ValueBindingSession.source_issues`` and
``resolve_code_membership`` → ``CodingResolution``. Fixture values are synthetic and
taken from the literals of the tests these cases replace.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta.source_evidence import RecordLocator, SourceRevision
from reg_meta_build.prepared_values import (
    open_prepared_source_values,
    prepare_source_values,
)
from reg_meta_build.source_coding import resolve_code_membership
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
from reg_meta_build.source_value_bindings import bind_code_lists, open_value_bindings
from reg_meta_build.source_value_periods import value_window
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
    SourceValueValidity,
)

if TYPE_CHECKING:
    from pathlib import Path

YEAR_2020 = TemporalScope(
    kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
)
YEAR_2021 = TemporalScope(
    kind="intervals", intervals=(ScopeInterval(start="2021", end="2021"),)
)


def revision(source: str) -> SourceRevision:
    return SourceRevision.create(
        dataset=source,
        publisher="Fixture",
        purpose="test",
        upstream_revision="1",
        artifact_path=source,
        artifact_size=0,
        artifact_sha256="0" * 64,
    )


def native_record(member: int | str = 1001, *, description: str = "first"):
    """A synthetic record of source ``records`` whose member is a native ID."""
    return SourceRecord.create(
        revision=revision("records"),
        locators=(
            RecordLocator(
                semantic_record_key=(str(member),),
                physical_file="records",
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
        edition_scope=YEAR_2020,
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            column_name=value_field("column"), description=value_field(description)
        ),
    )


def native_values(
    path: Path,
    rows: tuple[SourceValueAssociation, ...],
    validity: tuple[SourceValueValidity, ...] = (),
):
    """Prepare, accept and open one native-member value source (item validity)."""
    manifest = prepare_source_values(
        path,
        revision=revision("values"),
        descriptors=(SourceValueDescriptor("list", version="v1"),),
        values=(SourceValue("a", "01", "One"), SourceValue("b", "", "Blank")),
        associations=rows,
        validity=validity,
        validity_revision=revision("validity"),
        join=SourceValueJoin(
            record_sources=("records",),
            member_target="native_member",
            member_format="integer",
            validity_target="item",
            missing_validity="unrestricted",
            rule="Fixture explicit structural relation",
            provenance=("fixture format",),
        ),
    )
    commit = accept_prepared(path)
    return open_prepared_source_values(
        path, expected_sha256=manifest.sha256, input_commit=commit
    )


def segments(claims):
    return [
        (s.valid_from, s.valid_to, s.code_set.members if s.code_set else None)
        for s in resolve_code_membership(claims).segments
    ]


def test_native_member_tokens_bind_by_number_and_keep_their_raw_spelling(
    tmp_path: Path,
) -> None:
    """Zero-padded and plain tokens of one native member bind to one list.

    Each membership keeps the token as delivered, an unparseable token is reported
    once with its raw spelling, and the source's dated item validity (item "1",
    delivered for item "01") stays on the membership as evidence but is set aside
    by the explicit edition membership (the documented rule in
    ``ValueBindingSession.bind``; the set-aside list was confirmed by running it).
    """
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
    source = native_values(tmp_path / "values", rows, validity)
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(native_record(), sessions)
        assert bind_code_lists(native_record(), sessions) == result
        assert [
            (issue.code, issue.raw_member_tokens, issue.occurrence_count)
            for issue in sessions[0].source_issues()
        ] == [("unknown_native_member_token", ("broken",), 1)]
    assert result.issues == ()
    assert [m.associations[0].member_id for m in result.claims[0].members] == [
        "01001",
        "1001",
    ]
    assert result.bindings[0].association_count == 2
    assert [m.validity for m in result.claims[0].members] == [validity, ()]
    assert [a.member_id for a in result.bindings[0].item_validity_set_aside] == [
        "01001"
    ]
    assert segments(result.claims) == [
        ("2020-01-01", "2020-12-31", (("", "Blank"), ("01", "One")))
    ]
    assert tuple(source.associations()) == rows


def test_repeated_native_record_shares_claims_but_keeps_its_own_binding(
    tmp_path: Path,
) -> None:
    """A parent/prose duplicate of one native record reuses the same code list.

    Each binding still names its own physical record; a later effective scope gets
    a distinct claim identity; the shared claims resolve as one list.
    """
    rows = (
        SourceValueAssociation(2, "list", "a", "values", member_id="1001", item_id="1"),
    )
    source = native_values(tmp_path / "values", rows)
    first = native_record()
    duplicate = native_record(description="Different parent/prose evidence")
    assert first.record_id != duplicate.record_id
    with open_value_bindings((source,)) as sessions:
        a = bind_code_lists(first, sessions)
        b = bind_code_lists(duplicate, sessions)
        later = bind_code_lists(first, sessions, scope=YEAR_2021)
        other = bind_code_lists(native_record(member=1002), sessions)
    assert a.claims == b.claims
    assert [binding.record_id for binding in a.bindings] == [first.record_id]
    assert [binding.record_id for binding in b.bindings] == [duplicate.record_id]
    assert [binding.record_locators for binding in b.bindings] == [duplicate.locators]
    assert later.claims[0].claim_id != a.claims[0].claim_id
    assert later.claims[0].scope == YEAR_2021
    assert other.claims == other.bindings == other.issues == ()
    assert segments((*a.claims, *b.claims)) == segments(a.claims)


@pytest.mark.parametrize("member", [1001, "not-native"], ids=["empty", "named"])
def test_native_record_without_eligible_membership_states_no_claim(
    tmp_path: Path, member: int | str
) -> None:
    """An empty source binds nothing; a record without a native member ID is reported."""
    source = native_values(tmp_path / "values", ())
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(native_record(member), sessions)
    assert bound.claims == bound.bindings == ()
    assert [issue.code for issue in bound.issues] == (
        ["unknown_record_member"] if isinstance(member, str) else []
    )
