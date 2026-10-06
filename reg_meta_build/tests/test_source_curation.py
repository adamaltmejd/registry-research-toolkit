"""Illustrative applicability checks for the first shared curation boundary: projections, peers, batch guards and acknowledgements.

The cases mirror retained SCB and Socialstyrelsen source examples.  They are test
fixtures, not accepted curation decisions.
"""

from __future__ import annotations

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row as _var_row
from _source_curation_support import (
    curation_decision as _decision,
    curation_record as _record,
    curation_revision as _revision,
    expectation as _expectation,
    expected_field as _expected_field,
    field_value as _field,
    projection as _projection,
    scope_interval as _interval,
    source_ref as _ref,
)
from pydantic import ValidationError
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CurationCase,
    FieldExpectation,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SourceEvidence,
    SourceRecordRef,
    evaluate_case,
    evaluate_cases,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row


def test_native_identity_projection_ignores_unselected_metadata() -> None:
    original = _record(
        source="scb-source",
        key=("member",),
        register_name="Register",
        member_name="Variable",
        fields=SourceFields(data_type=value_field("text")),
        edition_scope=_interval("2020", "2020"),
        native=NativeCoordinates(register_id=1, variable_id=5),
    )
    case = CurationCase(
        case_id="register-name",
        decision=_decision(),
        targets=(
            RecordExpectation(
                ref=_ref(original),
                alternatives=(
                    RecordProjection(native=NativeCoordinates(register_id=1)),
                ),
            ),
        ),
    )
    changed = original.model_copy(
        update={"fields": SourceFields(data_type=value_field("integer"))}
    )
    assert evaluate_case(case, (changed,)).status == "applicable"
    changed = original.model_copy(
        update={
            "subject": original.subject.model_copy(
                update={"native": NativeCoordinates(register_id=2, variable_id=5)}
            )
        }
    )
    assert evaluate_case(case, (changed,)).status == "stale"
    with pytest.raises(ValidationError, match="select at least one coordinate"):
        RecordProjection(native=NativeCoordinates())


def test_reused_evidence_keeps_distinct_projection_shapes_and_new_source_changes() -> (
    None
):
    original = _record(
        source="scb-source",
        key=("member",),
        register_name="Register",
        member_name="Variable",
        fields=SourceFields(data_type=value_field("text")),
        edition_scope=_interval("2020", "2020"),
        native=NativeCoordinates(register_id=1, variable_id=5),
    )
    cases = tuple(
        CurationCase(
            case_id=f"shape-{index}",
            decision=_decision(),
            targets=(RecordExpectation(ref=_ref(original), alternatives=(shape,)),),
        )
        for index, shape in enumerate(
            (
                RecordProjection(native=NativeCoordinates(register_id=1)),
                RecordProjection(native=NativeCoordinates(variable_id=6)),
                RecordProjection(fields=(_field("data_type", "value", "text"),)),
                RecordProjection(fields=(_field("data_type", "value", "integer"),)),
            )
        )
    )
    evidence = SourceEvidence((original, original))
    expected = evaluate_cases(cases, (original, original))
    assert [result.status for result in expected] == [
        "applicable",
        "stale",
        "applicable",
        "stale",
    ]
    assert evaluate_cases(cases, evidence) == expected
    assert evaluate_cases(tuple(reversed(cases)), evidence) == tuple(reversed(expected))
    changed = original.model_copy(
        update={"fields": SourceFields(data_type=value_field("integer"))}
    )
    assert evaluate_case(cases[2], SourceEvidence((changed,))).status == "stale"


def test_peer_coordinates_match_native_identity_without_using_its_label() -> None:
    record = _record(
        source="provider-source",
        key=("member",),
        register_name="Register",
        member_name="Variable",
        fields=SourceFields(data_type=value_field("text")),
        edition_scope=_interval("2020", "2020"),
    )
    record = record.model_copy(
        update={
            "subject": record.subject.model_copy(
                update={
                    "variable": SourceCoordinate(
                        status="value", native_id="CODE", name="New label"
                    )
                }
            )
        }
    )
    expected = _expectation(record, _expected_field(record, "data_type"))
    case = CurationCase(
        case_id="native-column",
        decision=_decision(),
        targets=(expected,),
        peer_guards=(
            PeerGuard(
                guard_id="family",
                source=record.source,
                coordinates=(
                    (
                        "variable",
                        SourceCoordinate(
                            status="value", native_id="CODE", name="Old label"
                        ),
                    ),
                ),
                expected_members=(_ref(record),),
            ),
        ),
    )
    assert evaluate_case(case, (record,)).status == "applicable"
    another = record.model_copy(
        update={
            "locators": (
                record.locators[0].model_copy(
                    update={"semantic_record_key": ("new-member",)}
                ),
            )
        }
    )
    assert (
        evaluate_case(case, (record, another)).issues[0].code
        == "peer_membership_changed"
    )


@pytest.mark.parametrize(
    "selector", ["native", "coordinate", "column", "name", "field"]
)
def test_batch_guards_find_new_peers_and_keep_residual_conditions(
    selector: str,
) -> None:
    record = _record(
        source="scb-source",
        key=("original",),
        register_name="Register",
        member_name="Variable",
        fields=SourceFields(
            column_name=value_field("Col"), data_type=value_field("text")
        ),
        edition_scope=_interval("2020", "2020"),
        native=NativeCoordinates(register_id=1, variable_id=5),
    )
    guard = PeerGuard(
        guard_id="complete-peers",
        source=record.source,
        expected_members=(_ref(record),),
        fields=(FieldExpectation(name="data_type", status="value", value="text"),),
        edition_scopes=(record.edition_scope,),
        native=NativeCoordinates(variable_id=5) if selector == "native" else None,
        coordinates=(("register", record.subject.register_name),)
        if selector == "coordinate"
        else (),
        folded_column="col" if selector == "column" else None,
        register_name="Register" if selector == "name" else None,
    )
    case = CurationCase(
        case_id="checked",
        decision=_decision(),
        targets=(_expectation(record, _expected_field(record, "column_name")),),
        peer_guards=(guard,),
    )
    another = record.model_copy(
        update={
            "locators": (
                record.locators[0].model_copy(
                    update={"semantic_record_key": ("new-member",)},
                ),
            )
        }
    )
    excluded = another.model_copy(
        update={
            "fields": SourceFields(
                column_name=value_field("Col"),
                data_type=value_field("integer"),
            )
        }
    )
    cases = (case, case.model_copy(update={"case_id": "also-checked"}))
    # Physical duplicates do not add semantic peers; the selected index never
    # bypasses the remaining type, period or source predicates.
    assert all(
        e.status == "applicable"
        for e in evaluate_cases(cases, (record, record, excluded))
    )
    evidence = SourceEvidence(iter((record, excluded, another)))
    evaluations = evaluate_cases(cases, evidence)
    assert all(e.status == "stale" for e in evaluations)
    assert all(e.issues[0].added_members == (_ref(another),) for e in evaluations)
    assert evaluate_case(case, evidence) == evaluations[0]
    assert tuple(evidence) == (record, excluded, another)


def test_an_acknowledgement_names_its_issue_and_pins_no_source_members() -> None:
    ref = SourceRecordRef(source="scb-source", semantic_record_key=("member",))
    decision = AcknowledgeDecision(
        code="unresolved_catalog_identity",
        subject="('scb', 'variable', 5)",
        refs=(ref,),
        register_key=("scb-source", "scb", "register", "native-int", 1),
        reason="Accepted while the identity stays unresolved.",
        evidence="Diagnostic build ledger.",
    )
    case = CurationCase(case_id="acknowledged", targets=(), decision=decision)
    assert evaluate_case(case, ()).status == "applicable"
    with pytest.raises(ValidationError, match="not source pins"):
        CurationCase(
            case_id="pinned",
            targets=(
                RecordExpectation(
                    ref=ref,
                    alternatives=(
                        RecordProjection(native=NativeCoordinates(register_id=1)),
                    ),
                ),
            ),
            decision=decision,
        )
    with pytest.raises(ValidationError, match="not source pins"):
        CurationCase(
            case_id="fingerprinted",
            targets=(),
            decision=decision,
            expected_evidence_sha256="0" * 64,
        )
    with pytest.raises(ValidationError, match="reason and evidence"):
        AcknowledgeDecision.model_validate({**decision.model_dump(), "evidence": " "})


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


def test_raw_registerinformation_whitespace_does_not_stale_clean_projection() -> None:
    header = REGISTERINFORMATION_HEADER.split("|")

    def clean(register_name: str, population_name: str) -> SourceRecord:
        row = _var_row(
            colname="Example",
            cvid=1001,
            var_id=101,
            register=(register_name, 1, 10),
            population_name=population_name,
        ).split("|")
        cells: dict[str, tuple[bool, str | None, str]] = {
            name: (True, value, value) for name, value in zip(header, row, strict=True)
        }
        return clean_scb_row(
            header,
            2,
            cells,
            _revision("scb-registerinformation"),
        ).record

    expected = clean("TESTREG", "Hela befolkningen")
    whitespace_changed = clean("  TESTREG  ", "  Hela befolkningen  ")
    case = CurationCase(
        case_id="illustrative-clean-whitespace",
        targets=(
            _expectation(
                expected,
                _expected_field(expected, "column_name"),
                _expected_field(expected, "data_type"),
            ),
        ),
        decision=_decision(),
    )

    result = evaluate_case(case, [whitespace_changed])

    assert expected.context != whitespace_changed.context
    assert expected.subject == whitespace_changed.subject
    assert result.status == "applicable"
