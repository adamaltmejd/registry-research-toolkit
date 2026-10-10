"""Code-membership folds and the raw coding digest that no build case can reach.

The fold's reachable shapes (dated SOS Kodlista periods, SCB Vardemangder lists) are
build cases: `cases/build/value-code-lists-withhold-only-contested-or-unknown-periods`.
What stays here is LISA-only interval algebra (year-independent occurrences and
occurrences of disjoint intervals: no build-case source delivers LISA), the fold's
fail-fast identity guard, and the content the committed `raw_codings` digests pin.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_evidence import (
    DeliveredCell,
    RecordLocator,
    evidence_sha256,
)
from reg_meta_build.source_records import ScopeInterval, TemporalScope
from reg_meta_build.source_values import (
    SourceMemberHint,
    SourceValueAssociation,
    SourceValueValidity,
    SourceValueWindow,
)

INDEPENDENT = TemporalScope(kind="year_independent")


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


def test_coding_evidence_digest_pins_content_and_duplicates_not_row_positions() -> None:
    """Input: a claim with duplicated associations, cached cells and validity rows.
    Output: its `evidence_sha256` (the committed `raw_codings` digests,
    `curation/registers/sos/dors.toml`, `scb/lisa.toml`, ...) is unchanged when the
    same rows arrive re-sorted, at other row numbers, under another claim id, and it
    changes when a duplicate is dropped or a delivered cell changes.

    No boundary reaches the nested association, hint and validity shapes: build cases
    render their digests with this same function, and `test_committed_curation.py`
    only loads the committed literals. Expected values are relations between two
    inputs, not this function's output.

    Fails if the digest pins row order, row numbers, physical cells or the
    revision-bearing claim id (any new delivery would stale every raw coding pin), or
    stops pinning duplicates and delivered content.
    """
    locator = RecordLocator(
        semantic_record_key=("member:01",),
        physical_file="värden.xlsx",
        physical_table="Lista",
        physical_record="2",
        physical_cells=("A2", "B2"),
    )
    cell = DeliveredCell(
        name="Kod", present=True, raw_value="001", interpreted_value="001"
    )
    association = SourceValueAssociation(
        row_number=2,
        descriptor_key="lista",
        value_key="001",
        source_file="värden.xlsx",
        source_table="Lista",
        member_id="01",
        item_id="",
        member_hints=(SourceMemberHint("row", "Åäö", locator),),
        member_references=("", "01"),
        supplied_period="2020–2021",
        supplied_window=SourceValueWindow("known", "2020-01-01", "2021-12-31"),
        section_window=SourceValueWindow("unknown"),
        section_locator=locator,
        delivered_cells=(cell, cell),
    )
    validity = SourceValueValidity(
        row_number=3,
        item_id="",
        valid_from="Original date",
        valid_to=None,
        source_file="värden.xlsx",
        raw_cells=(None, "", "001"),
        locators=(locator,),
        delivered_cells=(
            cell.model_copy(
                update={"cached_raw_value": "001", "cached_raw_type": "string"}
            ),
        ),
        window=SourceValueWindow("unknown"),
    )
    member = CodeMembershipClaim(
        "001", "Åäö", _scope(), (association, association), (validity,), True
    )
    claim = replace(
        _claim("claim", member, _member("", ""), _member(None, None), member),
        version_label="Original version",
        drop_unknown_membership=True,
    )
    moved_locator = locator.model_copy(
        update={"physical_record": "7", "physical_cells": ("A7", "B7")}
    )
    moved_association = replace(
        association,
        row_number=7,
        member_hints=(SourceMemberHint("row", "Åäö", moved_locator),),
        section_locator=moved_locator,
    )
    moved_member = replace(
        member,
        associations=(moved_association, moved_association),
        validity=(replace(validity, row_number=9, locators=(moved_locator,)),),
    )
    resorted = replace(
        claim,
        claim_id="claim-from-another-delivery",
        members=(moved_member, *claim.members[1:3], moved_member)[::-1],
    )
    assert evidence_sha256(moved_association) == evidence_sha256(association)
    assert evidence_sha256(resorted) == evidence_sha256(claim)
    deduplicated = replace(claim, members=claim.members[:-1])
    assert evidence_sha256(deduplicated) != evidence_sha256(claim)
    recoded = replace(
        association,
        delivered_cells=(cell, cell.model_copy(update={"raw_value": "002"})),
    )
    assert evidence_sha256(recoded) != evidence_sha256(association)


def test_member_validity_splits_at_exact_days_across_a_leap_day() -> None:
    """Input: a 2020 list with `0` all year and `1` valid only 2020-02-29..2020-03-02.
    Output: three segments, cut at those exact days: `0` to 2020-02-28, `0` and `1`
    from the leap day to 2020-03-02, `0` again from 2020-03-03.

    No boundary reaches it: SCB sets aside item validity narrower than the item's
    edition (`item_validity_set_aside`) and SOS Kodlista periods are whole years, so
    no delivery cuts a member inside a year
    (`cases/build/value-code-lists-withhold-only-contested-or-unknown-periods` pins
    the year-grain form).

    Fails if the fold rounds member scopes to calendar years (`1` then covers all of
    2020) or mishandles an interior day boundary (an off-by-one segment end, or a
    lost leap day).
    """
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
    """Input: one list whose occurrence is two disjoint February windows and whose
    member is valid all year. Output: two coded segments, exactly those windows; the
    gap between them gets no segment.

    No boundary reaches it: only the LISA reader builds a multi-interval occurrence
    scope (`sources/lisa.py`), and no build-case source delivers LISA.

    Fails if the fold spans the gap with one segment or widens to the member's year.
    """
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


def test_one_claim_identity_with_conflicting_content_fails_fast() -> None:
    """Input: two claims sharing the identity `a` with different members. Output:
    `ValueError` "conflicting content"; the fold never picks one of them.

    No boundary reaches it: a claim id hashes the revision, native member or record,
    descriptor and scope, and one CVID's prose duplicates share one cached binding.
    Probed: two Registerinformation rows of one CVID whose Unika identifier flags
    differ are withheld as `unresolved_variable_name` before coding.

    Fails if the identity check is dropped and one claim's content silently wins.
    """
    with pytest.raises(ValueError, match="conflicting content"):
        resolve_code_membership((_claim("a", _member("1")), _claim("a", _member("2"))))


def test_independent_lists_preserve_members_and_have_no_calendar_window():
    """Input: a year-independent list, passed twice. Output: one year-independent
    segment without dates holding both members, no issue, both claims accounted.

    No boundary reaches it: only the LISA reader yields year-independent occurrences
    (`sources/lisa.py`); an undated SOS variable is `unsupported_occurrence` before
    coding (probed), and no build-case source delivers LISA.

    Fails if an independent list gets a calendar window or repeated claims conflict.
    """
    claim = _claim(
        "source",
        _member("00", "SVERIGE"),
        _member("02", "EU25 utom Norden"),
        scope=INDEPENDENT,
    )
    resolution = resolve_code_membership((claim, claim))
    assert resolution.issues == ()
    assert resolution.claims == (claim, claim)
    (segment,) = resolution.segments
    assert segment.period_scope == "year_independent"
    assert segment.valid_from is segment.valid_to is None
    assert segment.code_set is not None
    assert segment.code_set.members == (("00", "SVERIGE"), ("02", "EU25 utom Norden"))


@pytest.mark.parametrize(
    "restriction", [_scope(), TemporalScope(kind="unknown", label="unresolved source")]
)
def test_independent_members_reject_finite_or_unknown_validity(restriction):
    """Input: a year-independent list whose member is dated or of unknown validity.
    Output: `unknown_code_membership` and no published code set.

    No boundary reaches it: year-independent occurrences are LISA-only (see above).

    Fails if a dated member is accepted into an undated list.
    """
    claim = _claim("source", _member("02", scope=restriction), scope=INDEPENDENT)
    resolution = resolve_code_membership((claim,))
    assert [issue.code for issue in resolution.issues] == ["unknown_code_membership"]
    assert resolution.segments[0].code_set is None


@pytest.mark.parametrize(
    ("other", "issue"),
    [
        (
            _claim("other", _member("02", "EU28"), scope=INDEPENDENT),
            "conflicting_code_memberships",
        ),
        (_claim("other", scope=INDEPENDENT), "empty_active_coding"),
    ],
    ids=["conflicting", "empty"],
)
def test_independent_coding_refuses_competing_or_absent_membership(other, issue):
    """Input: a year-independent list `02 EU25` beside a second independent list
    that relabels 02, or states no member. Output: the named issue and no published
    code set; both claims stay accounted.

    No boundary reaches it: year-independent occurrences are LISA-only (see above).
    A dated or unknown-scope rival cannot occur: every claim of one occurrence is
    bound at that occurrence's scope.

    Fails if the fold publishes one of the independent lists, or the union.
    """
    first = _claim("source", _member("02", "EU25"), scope=INDEPENDENT)
    resolution = resolve_code_membership((first, other))
    assert [i.code for i in resolution.issues] == [issue]
    assert resolution.segments[0].code_set is None
    assert resolution.claims == (first, other)
