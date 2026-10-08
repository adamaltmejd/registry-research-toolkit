"""Coding application refuses a decision it cannot check against original evidence.

Kept as a unit test by maintainer decision (unreachable fail-closed guards). The
file's other claims moved to the build case
`cases/build/coding-entries-apply-or-go-stale-per-register` or were dropped as seams
(stored-case replays, internal fingerprints); the Stage 8a PR body maps each one.
"""

from __future__ import annotations

import pytest
from _csv_fixtures import scb_record
from reg_meta_build.curation_tree import CodingChoiceEntry
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    coding_content_sha256,
)
from reg_meta_build.source_coding_choices import (
    apply_coding_choices,
    compile_coding_selection,
)
from reg_meta_build.source_curation import (
    CodingDecision,
    CodingSelection,
    CurationCase,
    PeerGuard,
    capture_expectations,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import ScopeInterval, TemporalScope


def test_missing_binding_or_guard_is_fatal_not_a_content_waiver() -> None:
    """Input: a reviewed choice whose column has no converted coding binding, and the
    same choice with no peer guard over its target. Expected: `ValueError`
    ("unconverted column binding", "guarded original membership"), never an applied
    or merely stale choice.

    No boundary reaches either: `compile_coding_register` always emits a choice for a
    column of the same build's coding map and always guards its targets, so a build
    cannot hand `apply_coding_choices` an unbound or unguarded decision.

    Fails if `apply_coding_choices` drops either check, so a decision without a
    binding or a guard is applied or silently skipped instead of failing fast.
    """
    record = scb_record(cvid=2020, var_id=5, colname="VALUE")
    column = source_occurrence(record).column_key
    assert column is not None
    claim = CodeListClaim(
        "first",
        TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2020-01-01", end="2020-12-31"),),
        ),
        (CodeMembershipClaim("01", "Label", TemporalScope(kind="year_independent")),),
        version_label="first",
    )
    digest = coding_content_sha256(claim)
    assert digest is not None
    window = {"valid_from": "2020-01-01", "valid_to": "2020-12-31"}
    case = CurationCase(
        case_id="accepted",
        targets=capture_expectations((record,), fields=("column_name",)),
        peer_guards=(
            PeerGuard(
                guard_id="accepted",
                source=record.source,
                effective_column=column,
                expected_members=(record_ref(record),),
            ),
        ),
        decision=CodingDecision(
            reviewed=True,
            column_key=column,
            expected_codings=(digest,),
            selection=CodingSelection(
                **window, expected_codings=(digest,), selected_coding=digest
            ),
            reason="Reviewed",
            provenance="fixture",
            **window,
        ),
    )
    with pytest.raises(ValueError, match="unconverted column"):
        apply_coding_choices((record,), (case,), coding={})
    with pytest.raises(ValueError, match="guarded original membership"):
        apply_coding_choices(
            (record,),
            (case.model_copy(update={"peer_guards": ()}),),
            coding={column: (claim,)},
        )


def test_choice_keeps_a_documented_blank_sentinel_member() -> None:
    """Input: two complete 2020 lists both labelled `same label`: `1 Label`, and the
    same plus a labelled blank code `"" Not applicable`. A `[[coding.choice]]` keeps
    `same label` with `keep_members = [["1", "Label"], ["", "Not applicable"]]`.
    Expected: the choice compiles to the superset, applies, and publishes both
    members, the blank one included; once the kept list gains a member `2`, the same
    decision is stale.

    Maintainer decision (2026-10-08): a blank code is a real member when the source
    list documents it with a label; an unlabelled blank stays unknown membership.
    No build reaches this yet: the SCB Vardemangder adapter binds a labelled blank
    vardekod as a missing code (`unknown_code_membership`), so the list is never
    complete. It becomes a register of
    `cases/build/coding-entries-apply-or-go-stale-per-register` once Stage 10's
    adapter keeps labelled blanks.

    Fails if member matching or publishing drops the `""` code (the choice goes stale,
    or the published set loses `Not applicable`), or if `keep_members` stops
    checking the complete member set.
    """
    record = scb_record(cvid=2020, var_id=5, colname="VALUE")
    column = source_occurrence(record).column_key
    assert column is not None
    year = TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start="2020-01-01", end="2020-12-31"),),
    )
    independent = TemporalScope(kind="year_independent")
    ordinary = CodeListClaim(
        "ordinary",
        year,
        (CodeMembershipClaim("1", "Label", independent),),
        version_label="same label",
    )
    superset = CodeListClaim(
        "superset",
        year,
        (
            CodeMembershipClaim("1", "Label", independent),
            CodeMembershipClaim("", "Not applicable", independent),
        ),
        version_label="same label",
    )
    entry = CodingChoiceEntry.model_validate(
        {
            "variable": "1.5",
            "variant": "people",
            "column": "VALUE",
            "periods": [["2020-01-01", "2020-12-31"]],
            "keep": "same label",
            "keep_members": [["1", "Label"], ["", "Not applicable"]],
            "over": ["same label"],
            "reason": "Reviewed",
            "source": "fixture",
        }
    )
    selection, status, problem = compile_coding_selection(
        entry, "choice", (ordinary, superset), "2020-01-01", "2020-12-31"
    )
    assert (status, problem) == ("matched", "")
    assert isinstance(selection, CodingSelection)
    window = {"valid_from": "2020-01-01", "valid_to": "2020-12-31"}
    case = CurationCase(
        case_id="accepted",
        targets=capture_expectations((record,), fields=("column_name",)),
        peer_guards=(
            PeerGuard(
                guard_id="accepted",
                source=record.source,
                effective_column=column,
                expected_members=(record_ref(record),),
            ),
        ),
        decision=CodingDecision(
            reviewed=True,
            column_key=column,
            expected_codings=selection.expected_codings,
            selection=selection,
            reason="Reviewed",
            provenance="fixture",
            **window,
        ),
    )
    applied = apply_coding_choices(
        (record,), (case,), coding={column: (ordinary, superset)}
    )
    assert [item.status for item in applied.accounting] == ["applied"]
    (segment,) = applied.coding[column].segments
    assert segment.code_set is not None
    assert set(segment.code_set.members) == {("1", "Label"), ("", "Not applicable")}
    grown = CodeListClaim(
        "superset",
        year,
        (*superset.members, CodeMembershipClaim("2", "Label", independent)),
        version_label="same label",
    )
    drifted = apply_coding_choices(
        (record,), (case,), coding={column: (ordinary, grown)}
    )
    assert [item.status for item in drifted.accounting] == ["stale"]
