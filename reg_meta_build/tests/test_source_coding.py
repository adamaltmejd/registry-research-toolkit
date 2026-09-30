"""Code membership resolution never chooses a largest or latest competing list."""

from __future__ import annotations

from dataclasses import replace

import pytest
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    coding_content_sha256,
    coding_observation_fingerprints,
    copied_coding_fingerprints,
    has_unknown_code_membership,
    resolve_code_membership,
)
from reg_meta_build.source_records import ScopeInterval, TemporalScope
from reg_meta_build.source_value_periods import value_period
from reg_meta_build.source_values import SourceValueAssociation, SourceValueWindow


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


def test_butsatt_source_blocks_withhold_the_1982_to_1988_overlap() -> None:
    rows = (
        ("1973-1988", "1", "Till hemmet"),
        ("1973-1988", "2", "Till barnklinik"),
        ("1973-1988", "3", "Annan klinik"),
        ("1973-1988", "4", "Annan adress"),
        ("1982/1983-1989/1990", "1", "Till hemmet"),
        ("1982/1983-1989/1990", "2", "Till annan adress"),
        ("1990/1991-1998", "1", "Till hemmet"),
        ("1990/1991-1998", "2", "Till annan adress"),
        ("1990/1991-1998", "3", "Ej utskriven vid 28 dygn"),
        ("1999-", "1", "Ej hemskriven vid 28 dagar"),
    )
    members = []
    for row_number, (period, code, label) in enumerate(rows, start=2):
        window = value_period(period)
        assert window is not None and window.status == "known"
        assert window.start is not None
        members.append(
            replace(
                _member(
                    code,
                    label,
                    scope=_scope(window.start, window.end or "2000-12-31"),
                ),
                associations=(
                    SourceValueAssociation(
                        row_number,
                        "sheet:Kodlista_butsatt",
                        str(row_number),
                        "mfr.xlsx",
                        "Kodlista_butsatt",
                        supplied_period=period,
                    ),
                ),
            )
        )
    claim = _claim("butsatt", *members, scope=_scope("1973-01-01", "2000-12-31"))
    result = resolve_code_membership((claim,))
    assert [
        (issue.code, issue.valid_from, issue.valid_to) for issue in result.issues
    ] == [("conflicting_code_memberships", "1982-01-01", "1988-12-31")]
    segment_1990 = next(
        segment for segment in result.segments if segment.valid_from == "1990-01-01"
    )
    assert segment_1990.valid_to == "1990-12-31"
    assert segment_1990.code_set is not None
    assert segment_1990.code_set.members == (
        ("1", "Till hemmet"),
        ("2", "Till annan adress"),
        ("3", "Ej utskriven vid 28 dygn"),
    )


def test_adjacent_dated_labels_for_one_code_keep_both_periods() -> None:
    first = SourceValueAssociation(
        2, "sheet:Kodlista_butsatt", "first", "mfr.xlsx", "Kodlista_butsatt"
    )
    second = SourceValueAssociation(
        3, "sheet:Kodlista_butsatt", "second", "mfr.xlsx", "Kodlista_butsatt"
    )
    claim = _claim(
        "adjacent",
        replace(
            _member("2", "Till barnklinik", scope=_scope("1982-01-01", "1989-12-31")),
            associations=(first,),
        ),
        replace(
            _member("2", "Till annan adress", scope=_scope("1990-01-01", "1998-12-31")),
            associations=(second,),
        ),
        scope=_scope("1982-01-01", "1998-12-31"),
    )
    result = resolve_code_membership((claim,))
    assert result.issues == ()
    assert [
        (segment.valid_from, segment.valid_to, segment.code_set.members)
        for segment in result.segments
        if segment.code_set is not None
    ] == [
        ("1982-01-01", "1989-12-31", (("2", "Till barnklinik"),)),
        ("1990-01-01", "1998-12-31", (("2", "Till annan adress"),)),
    ]


@pytest.mark.parametrize("second_scope", ["year_independent", "intervals"])
def test_same_sheet_year_independent_conflicting_labels_withhold_coding(
    second_scope: str,
) -> None:
    first = SourceValueAssociation(
        21, "sheet:Kodlista_bdiag_bk", "first", "mfr.xlsx", "Kodlista_bdiag_bk"
    )
    second = SourceValueAssociation(
        23, "sheet:Kodlista_bdiag_bk", "second", "mfr.xlsx", "Kodlista_bdiag_bk"
    )
    scope = (
        TemporalScope(kind="year_independent")
        if second_scope == "year_independent"
        else _scope("2020-06-01", "2020-06-30")
    )
    claim = _claim(
        "shared",
        replace(
            _member(
                "16310",
                "pakygyri i cerebrala cortex",
                scope=TemporalScope(kind="year_independent"),
            ),
            associations=(first,),
        ),
        replace(_member("16310", "micropolygyri", scope=scope), associations=(second,)),
    )
    result = resolve_code_membership((claim,))
    assert result.claims == (claim,)
    contested = (
        result.segments if second_scope == "year_independent" else result.segments[1:2]
    )
    assert all(segment.code_set is None for segment in contested)
    assert [issue.code for issue in result.issues] == ["conflicting_code_memberships"]
    assert result.issues[0].valid_from == (
        "2020-01-01" if second_scope == "year_independent" else "2020-06-01"
    )
    assert result.issues[0].valid_to == (
        "2020-12-31" if second_scope == "year_independent" else "2020-06-30"
    )
    if second_scope == "intervals":
        assert result.segments[0].code_set is not None
        assert result.segments[2].code_set is not None


def test_same_sheet_identical_year_independent_duplicates_are_harmless() -> None:
    associations = tuple(
        SourceValueAssociation(row, "sheet:Codes", str(row), "dors.xlsx", "Codes")
        for row in (2, 3)
    )
    claim = _claim(
        "duplicate",
        *(
            replace(
                _member("01", "Same", scope=TemporalScope(kind="year_independent")),
                associations=(association,),
            )
            for association in associations
        ),
    )
    result = resolve_code_membership((claim,))
    assert result.issues == ()
    assert result.segments[0].code_set is not None
    assert result.segments[0].code_set.members == (("01", "Same"),)
    assert result.claims[0].members[0].associations == (associations[0],)
    assert result.claims[0].members[1].associations == (associations[1],)


@pytest.mark.parametrize(
    ("other_file", "other_sheet"),
    [("edition-2.xlsx", "Kodlista_codes"), ("edition-1.xlsx", "Kodlista_other")],
)
@pytest.mark.parametrize("scope_kind", ["intervals", "year_independent"])
def test_dated_label_history_across_files_or_descriptors_is_preserved(
    other_file: str, other_sheet: str, scope_kind: str
) -> None:
    first = SourceValueAssociation(
        2, "sheet:Kodlista_codes", "first", "edition-1.xlsx", "Kodlista_codes"
    )
    second = SourceValueAssociation(
        3, f"sheet:{other_sheet}", "second", other_file, other_sheet
    )
    member_scope = (
        _scope()
        if scope_kind == "intervals"
        else TemporalScope(kind="year_independent")
    )
    claim = _claim(
        "label-history",
        replace(_member("2", "Old", scope=member_scope), associations=(first,)),
        replace(_member("2", "New", scope=member_scope), associations=(second,)),
    )
    result = resolve_code_membership((claim,))
    assert result.issues == ()
    assert result.segments[0].code_set is not None
    assert result.segments[0].code_set.members == (("2", "New"), ("2", "Old"))


def test_non_sheet_annual_label_boundary_keeps_both_labels() -> None:
    first = SourceValueAssociation(
        2, "scb-values", "first", "Vardemangder.csv", "values"
    )
    second = SourceValueAssociation(
        3, "scb-values", "second", "Vardemangder.csv", "values"
    )
    claim = _claim(
        "annual-label-history",
        replace(
            _member("2", "Old", scope=_scope("1990-01-01", "1990-12-31")),
            associations=(first,),
        ),
        replace(
            _member("2", "New", scope=_scope("1990-12-31", "1991-12-31")),
            associations=(second,),
        ),
        scope=_scope("1990-01-01", "1991-12-31"),
    )
    result = resolve_code_membership((claim,))
    assert result.issues == ()
    boundary = next(
        segment
        for segment in result.segments
        if segment.valid_from == segment.valid_to == "1990-12-31"
    )
    assert boundary.code_set is not None
    assert boundary.code_set.members == (("2", "New"), ("2", "Old"))


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


def test_unknown_membership_check_matches_missing_values_and_scopes() -> None:
    assert has_unknown_code_membership(_claim("missing-code", _member(None)))
    assert has_unknown_code_membership(_claim("missing-label", _member("1", None)))
    assert has_unknown_code_membership(
        _claim(
            "unknown-scope",
            _member("1", scope=TemporalScope(kind="unknown", label="undated")),
        )
    )
    assert not has_unknown_code_membership(_claim("clean", _member("1")))


def test_bounded_unknown_validity_keeps_copy_evidence_distinct() -> None:
    def claim(start: str) -> CodeListClaim:
        association = SourceValueAssociation(
            2,
            "list",
            "a",
            "values",
            supplied_window=SourceValueWindow("known", start, "2020-07-31"),
        )
        return _claim(
            "a",
            CodeMembershipClaim(
                "01",
                "One",
                _scope("2020-07-01", "2020-07-31"),
                (association,),
                unknown_validity=True,
            ),
        )

    first, second = claim("2020-07-01"), claim("2020-07-02")
    assert coding_content_sha256(first) is None
    assert copied_coding_fingerprints((first,)) != copied_coding_fingerprints((second,))


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


def test_independent_lists_preserve_members_and_have_no_calendar_window():
    scope = TemporalScope(kind="year_independent")
    claim = _claim(
        "source",
        _member("00", "SVERIGE"),
        _member("02", "EU25 utom Norden"),
        scope=scope,
    )
    resolution = resolve_code_membership((claim, claim))
    assert resolution.issues == ()
    assert resolution.claims == (claim, claim)
    (segment,) = resolution.segments
    assert segment.period_scope == "year_independent"
    assert segment.valid_from is segment.valid_to is None
    assert segment.code_set.members == (("00", "SVERIGE"), ("02", "EU25 utom Norden"))


@pytest.mark.parametrize(
    "restriction", [_scope(), TemporalScope(kind="unknown", label="unresolved source")]
)
def test_independent_members_reject_finite_or_unknown_validity(restriction):
    claim = _claim(
        "source",
        _member("02", scope=restriction),
        scope=TemporalScope(kind="year_independent"),
    )
    resolution = resolve_code_membership((claim,))
    assert [issue.code for issue in resolution.issues] == ["unknown_code_membership"]
    assert resolution.segments[0].code_set is None


@pytest.mark.parametrize("other", ["dated", "unknown", "conflicting", "empty"])
def test_independent_coding_refuses_competing_or_absent_membership(other):
    scope = TemporalScope(kind="year_independent")
    first = _claim("source", _member("02", "EU25"), scope=scope)
    second = {
        "dated": _claim("other", _member("02", "EU25")),
        "unknown": _claim(
            "other",
            _member("02", "EU25"),
            scope=TemporalScope(kind="unknown", label="unresolved source"),
        ),
        "conflicting": _claim("other", _member("02", "EU28"), scope=scope),
        "empty": _claim("other", scope=scope),
    }[other]
    resolution = resolve_code_membership((first, second))
    assert resolution.issues
    assert not resolution.segments or resolution.segments[0].code_set is None
    assert resolution.claims == (first, second)
