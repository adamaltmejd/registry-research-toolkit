"""Code membership resolution never chooses a largest or latest competing list."""

from __future__ import annotations

from dataclasses import replace

import pytest
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    coding_content_sha256,
    coding_observation_fingerprints,
    resolve_code_membership,
)
from reg_meta_build.source_records import ScopeInterval, TemporalScope
from reg_meta_build.source_values import SourceValueAssociation


def _scope(start: str = "2020-01-01", end: str = "2020-12-31") -> TemporalScope:
    return TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
    )


def _member(
    code: str | None, label: str | None = "Label", *, scope: TemporalScope | None = None
) -> CodeMembershipClaim:
    return CodeMembershipClaim(
        code, label, scope or TemporalScope(kind="not_applicable")
    )


def _claim(
    identity: str, *members: CodeMembershipClaim, scope: TemporalScope | None = None
) -> CodeListClaim:
    return CodeListClaim(identity, scope or _scope(), members)


def test_exact_codes_labels_duplicates_and_occurrence_evidence_survive() -> None:
    association = SourceValueAssociation(
        row_number=2, descriptor_key="list", value_key="001", source_file="values.csv"
    )
    member = CodeMembershipClaim(
        "001",
        "Original label",
        TemporalScope(kind="not_applicable"),
        (association, association),
    )
    claim = _claim(
        "a", member, member, _member("001", "Another label"), _member("", "Empty code")
    )
    result = resolve_code_membership((claim,))
    assert result.issues == ()
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert segment.code_set is not None
    assert segment.code_set.members == (
        ("", "Empty code"),
        ("001", "Another label"),
        ("001", "Original label"),
    )
    assert result.claims == (claim,)
    assert result.claims[0].members[0].associations == (association, association)


def test_code_validity_splits_real_calendar_periods_without_annual_widening() -> None:
    result = resolve_code_membership(
        (
            _claim(
                "a",
                _member("0"),
                _member("1", scope=_scope("2020-02-29", "2020-03-02")),
            ),
        )
    )
    assert result.issues == ()
    assert [
        (s.valid_from, s.valid_to, s.code_set.members if s.code_set else None)
        for s in result.segments
    ] == [
        ("2020-01-01", "2020-02-28", (("0", "Label"),)),
        ("2020-02-29", "2020-03-02", (("0", "Label"), ("1", "Label"))),
        ("2020-03-03", "2020-12-31", (("0", "Label"),)),
    ]


def test_code_constraints_clip_to_actual_occurrence_and_preserve_disjoint_gaps() -> (
    None
):
    scope = TemporalScope(
        kind="intervals",
        intervals=(
            ScopeInterval(start="2020-02-01", end="2020-02-03"),
            ScopeInterval(start="2020-02-10", end="2020-02-12"),
        ),
    )
    result = resolve_code_membership(
        (_claim("a", _member("001", scope=_scope()), scope=scope),)
    )
    assert result.issues == ()
    assert [(s.valid_from, s.valid_to) for s in result.segments] == [
        ("2020-02-01", "2020-02-03"),
        ("2020-02-10", "2020-02-12"),
    ]


def test_equivalent_independent_lists_agree_without_losing_their_references() -> None:
    a, b = (
        _claim("a", _member("1"), _member("2")),
        _claim("b", _member("2"), _member("1"), _member("1")),
    )
    result = resolve_code_membership((b, a))
    assert result.issues == ()
    assert result.segments[0].claim_ids == ("a", "b")
    assert result.segments == resolve_code_membership((a, b)).segments


def test_coding_expectation_ignores_storage_and_equivalent_interval_partition() -> None:
    whole = _claim("original", _member("01"), _member("02"))
    split = _claim(
        "new revision and physical identity",
        _member("02"),
        _member("01", scope=_scope("2020-01-01", "2020-06-30")),
        _member("01", scope=_scope("2020-07-01", "2020-12-31")),
        _member("02"),
        scope=_scope("2020", "2020"),
    )
    assert coding_content_sha256(whole) is not None
    assert coding_content_sha256(whole) == coding_content_sha256(split)


def test_coding_expectation_changes_with_meaning_and_rejects_unknown_membership() -> (
    None
):
    original = _claim("original", _member("01"))
    variants = (
        _claim("code", _member("1")),
        _claim("label", _member("01", "Changed")),
        _claim("period", _member("01"), scope=_scope("2020-02-01")),
        replace(original, version_label="New classification vintage"),
    )
    assert all(
        coding_content_sha256(variant) != coding_content_sha256(original)
        for variant in variants
    )
    assert coding_content_sha256(_claim("missing", _member(None))) is None
    assert coding_content_sha256(_claim("empty")) is None


@pytest.mark.parametrize(
    "rival",
    [(_member("2"),), (_member("1", "Other label"),), (_member("1"), _member("2"))],
)
def test_conflicting_lists_withhold_only_the_contested_coding_period(
    rival: tuple[CodeMembershipClaim, ...],
) -> None:
    a = _claim("a", _member("1"))
    b = _claim("b", *rival, scope=_scope("2020-03-15", "2020-04-02"))
    result = resolve_code_membership((a, b))
    assert [
        (s.valid_from, s.valid_to, s.code_set is None) for s in result.segments
    ] == [
        ("2020-01-01", "2020-03-14", False),
        ("2020-03-15", "2020-04-02", True),
        ("2020-04-03", "2020-12-31", False),
    ]
    assert [(i.code, i.claim_ids) for i in result.issues] == [
        ("conflicting_code_memberships", ("a", "b"))
    ]


@pytest.mark.parametrize("code,label", [(None, "Label"), ("001", None)])
def test_missing_code_or_label_withholds_complete_list_only_where_member_applies(
    code: str | None, label: str | None
) -> None:
    result = resolve_code_membership(
        (
            _claim(
                "a",
                _member("0"),
                _member(code, label, scope=_scope("2020-05-01", "2020-05-02")),
            ),
        )
    )
    assert [s.code_set is None for s in result.segments] == [False, True, False]
    assert result.issues[0].code == "unknown_code_membership"
    assert result.issues[0].member_positions == (("a", 1),)
    assert (result.issues[0].valid_from, result.issues[0].valid_to) == (
        "2020-05-01",
        "2020-05-02",
    )


@pytest.mark.parametrize("kind", ["unknown", "pooled"])
def test_unknown_or_pooled_code_scope_cannot_be_filled_from_occurrence_year(
    kind: str,
) -> None:
    scope = TemporalScope.model_validate({"kind": kind, "label": "2010–2020"})
    result = resolve_code_membership(
        (_claim("a", _member("0"), _member("1", scope=scope)),)
    )
    assert result.segments[0].code_set is None
    assert result.issues[0].code == "unknown_code_membership"
    result = resolve_code_membership((_claim("a", _member("1"), scope=scope),))
    assert result.segments == ()
    assert result.issues[0].code == "unsupported_coding_scope"
    assert result.claims[0].scope == scope


def test_no_active_members_reports_missing_coding_and_keeps_occurrence_coverage() -> (
    None
):
    result = resolve_code_membership(
        (_claim("a", _member("1", scope=_scope("2020-03-01", "2020-12-31"))),)
    )
    assert [
        (s.valid_from, s.valid_to, s.code_set is None) for s in result.segments
    ] == [("2020-01-01", "2020-02-29", True), ("2020-03-01", "2020-12-31", False)]
    assert result.issues[0].code == "empty_active_coding"


def test_repeated_identical_claims_keep_accounting_without_repeating_events() -> None:
    claim = _claim("a", _member("1"))
    result = resolve_code_membership((claim, claim))
    assert result.claims == (claim, claim)
    assert result.segments == resolve_code_membership((claim,)).segments
    assert result.issues == ()
    with pytest.raises(ValueError, match="conflicting content"):
        resolve_code_membership((claim, _claim("a", _member("2"))))


def test_conflicting_labels_preserve_agreed_codes_without_selecting_a_label() -> None:
    scope = _scope()
    members = (_member("001"),)
    first = CodeListClaim("first", scope, members, version_label="First edition")
    second = CodeListClaim("second", scope, members, version_label="Second edition")
    result = resolve_code_membership((first, second))
    assert result.segments[0].code_set is not None
    assert result.segments[0].code_set.members == (("001", "Label"),)
    assert result.segments[0].version_label == ""
    assert result.issues[0].code == "conflicting_coding_labels"
    assert result.issues[0].withheld == "coding_label"


@pytest.mark.parametrize("kind", ("pooled", "unknown"))
def test_undated_copy_fingerprint_pins_evidence_without_inventing_periods(kind):
    scope = TemporalScope(kind=kind, label="Original undated evidence")
    claim = _claim("original", _member("1"), _member("2"), scope=scope)
    identical = replace(claim, claim_id="moved", members=tuple(reversed(claim.members)))
    assert coding_observation_fingerprints(
        (claim, claim)
    ) == coding_observation_fingerprints((identical,))
    assert coding_observation_fingerprints((claim,)) != coding_observation_fingerprints(
        (_claim("changed", _member("1"), scope=scope),)
    )
    assert resolve_code_membership((claim,)).segments == ()
