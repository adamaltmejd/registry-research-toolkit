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


def test_scoped_sentinel_preserves_source_list_and_only_affects_its_window():
    setup = _setup(code="99")
    case = _scoped_sentinel_case(setup, start="2020-07-01")
    result = _apply(setup, (setup[1], case))
    segments = result.coding[case.decision.column_key].segments
    assert [(s.valid_from, s.valid_to, _sole_classification(s)) for s in segments] == [
        ("2020-01-01", "2020-06-30", "fixture"),
        ("2020-07-01", "2020-12-31", "fixture"),
    ]
    assert segments[0].code_set == segments[1].code_set
    assert (
        result.coding[case.decision.column_key].claims
        == setup[2][case.decision.column_key].claims
    )
    assert _sole_conformance(segments[1]).sentinel_members == (("99", "Source label"),)
    certificate = _sole_conformance(segments[1]).scoped_sentinels[0]
    assert certificate.members == _sole_conformance(segments[1]).sentinel_members
    assert certificate.classification_sha256 == case.decision.expected_classification
    assert certificate.source_fingerprints == case.decision.expected_source_codings
    assert certificate.valid_from == "2020-07-01"
    assert certificate.delivery_column_name == case.decision.column_key[-1]
    assert "scoped-sentinel" in certificate.provenance
    assert setup[3]["fixture"].sentinel_codes == ()
    assert [d.code for d in result.diagnostics] == [
        "nonconforming_classification_codes",
        "sentinel_classification_codes",
    ]


@pytest.mark.parametrize("change", ["label", "member", "outside-window", "book"])
def test_scoped_sentinel_rejects_changed_coding_and_codebook(change):
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
        members = claims[0].members
        members = (
            (replace(members[0], label="Substantive industry"),)
            if change == "label"
            else (*members, replace(members[0], code="02"))
        )
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


def test_scoped_sentinel_leaves_substantive_same_literal_in_another_window():
    setup = _setup(code="99")
    key = setup[1].decision.column_key
    current = setup[2][key].claims[0]
    older = replace(
        current,
        claim_id="older",
        scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2019", end="2019"),)
        ),
        members=(replace(current.members[0], label="Substantive industry"),),
    )
    coding = {key: resolve_code_membership((older, current))}
    setup = (*setup[:2], coding, setup[3])
    case = _scoped_sentinel_case(setup)
    declared = setup[1].model_copy(
        update={
            "decision": setup[1].decision.model_copy(
                update={"valid_from": "2019-01-01"}
            )
        }
    )
    result = _apply(setup, (declared, case))
    old, new = result.coding[key].segments
    assert (
        _sole_classification(old) == "fixture"
        and _sole_conformance(old).sentinel_members == ()
    )
    assert old.code_set.members == (("99", "Substantive industry"),)
    assert _sole_classification(new) == "fixture"
    assert _sole_conformance(new).sentinel_members == (("99", "Source label"),)
    assert result.coding[key].claims == (older, current)


def test_scoped_sentinel_requires_every_shared_ref_original_projection():
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
