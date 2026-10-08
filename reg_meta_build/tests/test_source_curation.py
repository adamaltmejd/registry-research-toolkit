"""Illustrative applicability checks for the first shared curation boundary: projections, peers, batch guards and acknowledgements.

The cases mirror retained SCB and Socialstyrelsen source examples.  They are test
fixtures, not accepted curation decisions.
"""

from __future__ import annotations

from _source_curation_support import (
    curation_decision as _decision,
    curation_record as _record,
    expectation as _expectation,
    field_value as _field,
    projection as _projection,
    scope_interval as _interval,
    source_ref as _ref,
)
from reg_meta_build.source_curation import (
    CurationCase,
    RecordExpectation,
    SourceEvidence,
    evaluate_case,
    evaluate_cases,
)
from reg_meta_build.source_records import (
    SourceFields,
    value_field,
)


def test_repeated_guard_cache_preserves_issues_and_physical_alternatives() -> None:
    original = _record(
        source="fixture",
        key=("member:1",),
        register_name="Register",
        member_name="Variable",
        fields=SourceFields(name=value_field("First")),
        edition_scope=_interval("2020", "2020"),
    )
    second = original.model_copy(
        update={"fields": SourceFields(name=value_field("Second"))}
    )
    expectations = (
        _expectation(original, _field("name", "value", "First")),
        RecordExpectation(
            ref=_ref(original),
            alternatives=(
                _projection(
                    _field("name", "value", "First"),
                    edition_scope=original.edition_scope,
                    subject=original.subject,
                ),
                _projection(
                    _field("name", "value", "Second"),
                    edition_scope=original.edition_scope,
                    subject=original.subject,
                ),
            ),
        ),
    )
    cases = tuple(
        CurationCase.model_validate_json(
            CurationCase(
                case_id=f"repeated:{i}",
                targets=(expectations[i % 2],),
                decision=_decision(),
            ).model_dump_json()
        )
        for i in range(20)
    )
    records = (original, second, original)
    shared = evaluate_cases(cases, SourceEvidence(records))
    fresh = tuple(evaluate_case(case, records) for case in cases)
    assert tuple(result.model_dump_json() for result in shared) == tuple(
        result.model_dump_json() for result in fresh
    )
    assert {result.status for result in shared} == {"applicable", "stale"}
    assert all(
        result.issues[0].code == "target_projection_changed"
        for result in shared
        if result.status == "stale"
    )
    assert evaluate_cases(cases, SourceEvidence((original,)))[1].status == "stale"
