"""Checked codebook bindings: scoped sentinels affect only their window and require complete original projections."""

from dataclasses import replace

import pytest
from _source_classification_bindings_support import (
    apply_bindings as _apply,
    binding_setup as _setup,
    sole_classification as _sole_classification,
    sole_conformance as _sole_conformance,
)
from reg_meta_build.source_classification_bindings import (
    apply_classification_cases,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coding_choices import coding_expectations
from reg_meta_build.source_curation import (
    PeerGuard,
    capture_expectations,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)


def _scoped_sentinel_case(setup, *, start="2020-01-01", end="2020-12-31"):
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.source_coding import copied_coding_fingerprints

    record, case, coding, books = setup
    decision = case.decision
    claims = coding[decision.column_key].claims
    return case.model_copy(
        update={
            "case_id": "scoped-sentinel",
            "targets": capture_expectations(
                (record,), fields=tuple(SourceFields.model_fields), coding=True
            ),
            "decision": decision.model_copy(
                update={
                    "valid_from": start,
                    "valid_to": end,
                    "expected_codings": coding_expectations(claims, start, end),
                    "expected_source_codings": copied_coding_fingerprints(claims),
                    "expected_classification": canonical_sha256(
                        books["fixture"].model_dump(mode="json")
                    ),
                    "sentinel_members": (("99", "Source label"),),
                    "binding_scope": "inline_coding",
                }
            ),
        }
    )


# The build compiles a `[[coding.sentinel]]` and applies it from the same build's code
# lists and books, so the build reaches only the compile-time member/label/book match
# (case `coding-checked-entries-apply-then-go-stale-on-drift`). The tests below keep the
# application-time sentinel guards no build reaches (maintainer decision, as for the
# Stage 7b delivery-coverage arms).


@pytest.mark.parametrize("change", ["member", "outside-window", "book"])
def test_scoped_sentinel_rejects_changed_coding_and_codebook(change):
    """A sentinel case reviewed against one code list and book, applied to a changed
    one (a new in-window member, a new list outside the window, or a renamed book),
    refuses with `classification_evidence_changed` and binds no window.

    No boundary reaches it: `compile_coding_register` recomputes `expected_codings`,
    `expected_source_codings` and `expected_classification` from the same build it
    applies to, so they never disagree. Fails if `apply_classification_cases` stops
    comparing a scoped sentinel's pinned codings, source fingerprints or book digest
    with the evidence it binds.
    """
    setup = _setup(code="99")
    case = _scoped_sentinel_case(setup, start="2020-07-01")
    key = case.decision.column_key
    claims = setup[2][key].claims
    books = setup[3]
    if change == "book":
        books = {"fixture": books["fixture"].model_copy(update={"name": "Changed"})}
    elif change == "outside-window":
        claims = (
            *claims,
            CodeListClaim(
                "additional",
                TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2019", end="2019"),),
                ),
                (
                    CodeMembershipClaim(
                        "02", "Other", TemporalScope(kind="year_independent")
                    ),
                ),
            ),
        )
    else:
        members = (*claims[0].members, replace(claims[0].members[0], code="02"))
        claims = (replace(claims[0], members=members),)
    result = _apply(
        setup,
        (setup[1], case),
        coding={key: resolve_code_membership(claims)},
        classifications=books,
    )
    assert "classification_evidence_changed" in {d.code for d in result.diagnostics}
    assert all(
        _sole_classification(s)
        == (None if s.valid_from.startswith("2019") else "fixture")
        for s in result.coding[key].segments
    )
    assert all(
        not _sole_conformance(s).sentinel_members
        for s in result.coding[key].segments
        if _sole_conformance(s)
    )


def test_scoped_sentinel_guard_uses_accepted_owner_and_complete_effective_peers():
    """A sentinel case on an accepted partition owner's column binds the book; applied
    to evidence with a new peer record, no effective occurrence, or a narrowed
    occurrence period, it binds no window and reports a diagnostic.

    No boundary reaches the refusal: the peer guard is compiled from the same build's
    effective columns it is applied to. Fails if `apply_classification_cases` stops
    checking a sentinel case's `peer_guards` against the effective column's members
    and period, or reads the original column key instead of the accepted owner's.
    """
    from reg_meta_build.source_curation import SourceEvidence

    setup = _setup(code="99")
    record = setup[0]
    original = source_occurrence(record)
    accepted = replace(
        original,
        variable_key=(*original.variable_key, "accepted-partition", "1.1.accepted"),
    )
    key = accepted.column_key
    assert key is not None
    case = _scoped_sentinel_case(setup)
    case = case.model_copy(
        update={
            "decision": case.decision.model_copy(update={"column_key": key}),
            "peer_guards": (
                PeerGuard(
                    guard_id="accepted-members",
                    source=record.source,
                    effective_column=key,
                    expected_members=tuple(t.ref for t in case.targets),
                ),
            ),
        }
    )
    coding = {key: setup[2][original.column_key]}
    arguments = {"coding": coding, "classifications": setup[3]}
    evidence = SourceEvidence((record,), effective_occurrences=(accepted,))
    good = apply_classification_cases(evidence, (case,), **arguments)
    assert _sole_classification(good.coding[key].segments[0]) == "fixture"
    peer = record.model_copy(
        update={
            "record_id": "new-peer",
            "locators": tuple(
                loc.model_copy(
                    update={
                        "semantic_record_key": (
                            *loc.semantic_record_key[:-1],
                            "member:101",
                        )
                    }
                )
                for loc in record.locators
            ),
        }
    )
    extra = replace(accepted, source_records=(peer,))
    for changed in (
        SourceEvidence((record, peer), effective_occurrences=(accepted, extra)),
        SourceEvidence((record,), effective_occurrences=()),
        SourceEvidence(
            (record,),
            effective_occurrences=(
                replace(
                    accepted,
                    edition_period_scope=TemporalScope(
                        kind="intervals",
                        intervals=(
                            ScopeInterval(start="2020-07-01", end="2020-12-31"),
                        ),
                    ),
                ),
            ),
        ),
    ):
        result = apply_classification_cases(changed, (case,), **arguments)
        assert result.diagnostics and all(
            _sole_classification(s) is None for s in result.coding[key].segments
        )


def test_scoped_sentinel_requires_every_shared_ref_original_projection():
    """A sentinel case whose target is one record projected as two physical columns
    binds the book while both projections are delivered; applied with one projection
    missing, it is not applicable and binds nothing.

    No boundary reaches the refusal: the build captures the targets from the same
    records it applies the case to. Fails if `apply_classification_cases` compares a
    target's ref instead of every captured projection alternative.
    """
    from reg_meta_build.source_curation import SourceEvidence

    setup = _setup(code="99")
    record = setup[0]
    twin = record.model_copy(
        update={
            "record_id": "shared-ref-twin",
            "fields": record.fields.model_copy(
                update={"column_name": value_field("Sibling")}
            ),
        }
    )
    first = source_occurrence(record)
    second = replace(source_occurrence(twin), fields=first.fields)
    key = first.column_key
    assert key is not None
    case = _scoped_sentinel_case(setup)
    targets = capture_expectations(
        (record, twin), fields=tuple(SourceFields.model_fields), coding=True
    )
    assert len(targets) == 1 and len(targets[0].alternatives) == 2
    case = case.model_copy(
        update={
            "targets": targets,
            "peer_guards": (
                PeerGuard(
                    guard_id="complete-twins",
                    source=record.source,
                    effective_column=key,
                    expected_members=tuple(t.ref for t in targets),
                ),
            ),
        }
    )
    good = apply_classification_cases(
        SourceEvidence((record, twin), effective_occurrences=(first, second)),
        (case,),
        coding=setup[2],
        classifications=setup[3],
    )
    assert _sole_classification(good.coding[key].segments[0]) == "fixture"
    missing = apply_classification_cases(
        SourceEvidence((record,), effective_occurrences=(first,)),
        (case,),
        coding=setup[2],
        classifications=setup[3],
    )
    assert missing.evaluations[0].status != "applicable"
    assert _sole_classification(missing.coding[key].segments[0]) is None
