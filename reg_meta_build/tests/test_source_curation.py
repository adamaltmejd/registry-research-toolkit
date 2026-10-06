"""Illustrative applicability checks for the first shared curation boundary.

The cases mirror retained SCB and Socialstyrelsen source examples.  They are test
fixtures, not accepted curation decisions.
"""

from __future__ import annotations

import hashlib
import json
from typing import Literal

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import TypeAdapter, ValidationError
from reg_meta.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceField,
    SourceRevision,
)
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CodingDecision,
    CurationCase,
    FieldExpectation,
    GuardValidationContext,
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
    ScopeInterval,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build import source_curation


def _revision(source: str, marker: str = "accepted") -> SourceRevision:
    digest = hashlib.sha256(marker.encode()).hexdigest()
    return SourceRevision.create(
        dataset=source,
        publisher="fixture publisher",
        purpose="illustrative source-curation test only",
        upstream_revision=marker,
        artifact_path=f"{marker}.source",
        artifact_size=len(marker),
        artifact_sha256=digest,
    )


def _interval(start: str, end: str) -> TemporalScope:
    return TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start=start, end=end),),
    )


def _record(
    *,
    source: str,
    key: tuple[str, ...],
    register_name: str,
    member_name: str,
    fields: SourceFields,
    edition_scope: TemporalScope,
    native: NativeCoordinates | None = None,
    variant: SourceCoordinate | None = None,
    population: SourceCoordinate | None = None,
    marker: str = "accepted",
    row: int = 1,
    context: tuple[str, ...] = (),
    raw_note: str = "original",
) -> SourceRecord:
    revision = _revision(source, marker)
    return SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=key,
                physical_file=revision.artifact_path,
                physical_table="fixture",
                physical_record=f"row:{row}",
                physical_cells=(f"fixture!A{row}",),
            ),
        ),
        subject=SourceSubject(
            provider=source.split("-", maxsplit=1)[0],
            register=SourceCoordinate(status="value", name=register_name),
            variant=variant or SourceCoordinate(status="unknown"),
            population=population or SourceCoordinate(status="unknown"),
            member=SourceCoordinate(status="value", name=member_name),
            native=native or NativeCoordinates(),
        ),
        edition_scope=edition_scope,
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=fields,
        context=context,
        delivered_cells=(
            DeliveredCell(
                name="fixture_note",
                present=True,
                raw_value=raw_note,
                interpreted_value=raw_note,
            ),
        ),
    )


def _ref(record: SourceRecord) -> SourceRecordRef:
    return SourceRecordRef(
        source=record.source,
        semantic_record_key=record.locators[0].semantic_record_key,
    )


def _field(
    name: str,
    status: Literal["absent", "value", "unknown", "negative"],
    value: str | None = None,
) -> FieldExpectation:
    return FieldExpectation(name=name, status=status, value=value)


def _expected_field(record: SourceRecord, name: str) -> FieldExpectation:
    field = getattr(record.fields, name)
    assert field is not None
    return FieldExpectation(name=name, status=field.status, value=field.value)


def _projection(
    *fields: FieldExpectation,
    edition_scope: TemporalScope,
    subject: SourceSubject | None = None,
) -> RecordProjection:
    return RecordProjection(
        fields=fields,
        edition_scope=edition_scope,
        subject=subject,
    )


def _expectation(
    record: SourceRecord,
    *fields: FieldExpectation,
) -> RecordExpectation:
    return RecordExpectation(
        ref=_ref(record),
        alternatives=(
            _projection(
                *fields,
                edition_scope=record.edition_scope,
                subject=record.subject,
            ),
        ),
    )


def _decision() -> CodingDecision:
    """Any decision: these cases check applicability, never application."""
    return CodingDecision(
        reviewed=True,
        column_key=("fixture-column",),
        valid_from="2020-01-01",
        valid_to="2020-12-31",
        expected_codings=(),
        selection="omit_state",
        reason="Retained source evidence does not justify a semantic winner.",
        provenance="illustrative fixture",
    )


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


def test_projection_order_and_value_rules_reuse_the_common_source_contract() -> None:
    projection = RecordProjection(
        fields=(
            _field("representation", "value", "YYYY"),
            _field("data_type", "value", "integer"),
        )
    )

    assert tuple(field.name for field in projection.fields) == (
        "data_type",
        "representation",
    )
    with pytest.raises(ValueError, match="availability values must be true"):
        FieldExpectation(name="availability", status="value", value=False)
    with pytest.raises(ValueError, match="name must carry a string value"):
        FieldExpectation(name="name", status="value", value=True)


@pytest.mark.parametrize(
    ("name", "status", "value"),
    [
        ("availability", "value", 1),
        ("sensitivity", "value", 1),
        ("sensitivity", "value", "true"),
        ("identifier", "value", 0),
        ("conditional_sensitivity", "value", "true"),
        ("name", "value", True),
        ("measurement_unit", "negative", None),
        ("identifier", "negative", None),
        ("name", "unknown", "unsupplied"),
        ("name", "value", None),
        ("unrecognized", "value", "text"),
    ],
)
def test_projection_and_source_json_reject_the_same_invalid_named_field(
    name: str, status: str, value: str | int | bool | None
) -> None:
    with pytest.raises(ValidationError):
        FieldExpectation.model_validate_json(
            json.dumps({"name": name, "status": status, "value": value})
        )
    with pytest.raises(ValidationError):
        SourceFields.model_validate_json(
            json.dumps({name: {"status": status, "value": value}})
        )


@pytest.mark.parametrize("intern", [False, True])
@pytest.mark.parametrize(
    "alternatives",
    [
        [{"fields": [{"name": "name", "status": "value", "value": 1}]}],
        [{"edition_scope": {"kind": "unknown"}}],
        [{}],
        [
            {"fields": [{"name": "name", "status": "value", "value": "text"}]},
            {"fields": [{"name": "name", "status": "value", "value": "text"}]},
        ],
        [
            {"fields": [{"name": "name", "status": "value", "value": "text"}]},
            {"fields": [{"name": "definition", "status": "value", "value": "text"}]},
        ],
    ],
    ids=["singleton-field", "singleton-scope", "singleton-empty", "duplicate", "shape"],
)
def test_record_expectation_json_keeps_nested_and_multiple_alternative_guards(
    alternatives: list[dict[str, object]],
    intern: bool,
) -> None:
    with pytest.raises(ValidationError):
        RecordExpectation.model_validate_json(
            json.dumps(
                {
                    "ref": {"source": "fixture", "semantic_record_key": ["member:1"]},
                    "alternatives": alternatives,
                }
            ),
            context=GuardValidationContext() if intern else None,
        )


def test_json_guard_interning_keeps_full_values_and_scope_isolation() -> None:
    from reg_meta_build.source_curation import CodeSetExpectation, ParentFactProjection

    field = _field("name", "value", "Literal")
    parent = ParentFactProjection(
        kind="register",
        coordinate=SourceCoordinate(status="value", native_id=1),
        register=SourceCoordinate(status="value", native_id=1),
        fields=(field,),
    )
    projection = RecordProjection(
        fields=(field,),
        native=NativeCoordinates(variable_id=1),
        edition_scope=_interval("2020", "2020"),
        parent_facts=(parent,),
        code_set_references=(
            CodeSetExpectation(reference_id="book", content_sha256="a" * 64),
        ),
    )
    expected = RecordExpectation(
        ref=SourceRecordRef(source="fixture", semantic_record_key=("member:1",)),
        alternatives=(projection,),
    )
    variants = (
        expected.model_copy(
            update={"ref": expected.ref.model_copy(update={"source": "other"})}
        ),
        *(
            expected.model_copy(
                update={"alternatives": (projection.model_copy(update=update),)}
            )
            for update in (
                {"fields": (_field("name", "value", "Other"),)},
                {"fields": (_field("definition", "value", "Literal"),)},
                {"edition_scope": _interval("2021", "2021")},
                {"native": NativeCoordinates(variable_id=2)},
                {"parent_facts": ()},
                {"code_set_references": ()},
            )
        ),
    )
    adapter = TypeAdapter(tuple[RecordExpectation, ...])
    payload = adapter.dump_json((expected, expected, *variants))
    context = GuardValidationContext()
    parsed = adapter.validate_json(payload, context=context)
    assert adapter.dump_json(parsed) == payload
    assert parsed[0] is parsed[1]
    assert parsed[0].alternatives[0] is parsed[1].alternatives[0]
    assert all(item is not parsed[0] for item in parsed[2:])
    assert parsed[2].alternatives[0] is parsed[0].alternatives[0]
    assert all(
        item.alternatives[0] is not parsed[0].alternatives[0] for item in parsed[3:]
    )
    fresh = adapter.validate_json(payload, context=GuardValidationContext())
    assert fresh[0] is not parsed[0]
    assert fresh[0].alternatives[0] is not parsed[0].alternatives[0]
    assert adapter.dump_json(fresh) == payload


def test_json_guard_interning_preserves_reference_and_alternative_order() -> None:
    ref = SourceRecordRef(source="källa", semantic_record_key=("å", "b"))
    alternatives = tuple(
        RecordProjection(fields=(_field("name", "value", value),))
        for value in ("Första", "Andra")
    )
    expected = RecordExpectation(ref=ref, alternatives=alternatives)
    variants = (
        expected,
        expected,
        expected.model_copy(update={"alternatives": tuple(reversed(alternatives))}),
        expected.model_copy(
            update={"ref": ref.model_copy(update={"semantic_record_key": ("å/b",)})}
        ),
        expected.model_copy(update={"ref": ref.model_copy(update={"source": "KÄLLA"})}),
    )
    adapter = TypeAdapter(tuple[RecordExpectation, ...])
    payload = adapter.dump_json(variants)
    baseline = adapter.validate_json(payload)
    parsed = adapter.validate_json(payload, context=GuardValidationContext())
    assert adapter.dump_json(parsed) == adapter.dump_json(baseline) == payload
    assert parsed[0] is parsed[1]
    assert all(item is not parsed[0] for item in parsed[2:])
    assert parsed[2].alternatives == tuple(reversed(parsed[0].alternatives))
    assert parsed[2].alternatives[0] is parsed[0].alternatives[1]
    assert parsed[3].alternatives[0] is parsed[0].alternatives[0]
    invalid = json.loads(expected.model_dump_json())
    invalid["ref"]["semantic_record_key"] = [1]
    with pytest.raises(ValidationError):
        adapter.validate_json(
            json.dumps([json.loads(expected.model_dump_json()), invalid]),
            context=GuardValidationContext(),
        )


def test_json_guard_interning_keeps_complete_subclass_facts() -> None:
    class MarkedExpectation(RecordExpectation):
        marker: str

    first = MarkedExpectation(
        ref=SourceRecordRef(source="fixture", semantic_record_key=("member:1",)),
        alternatives=(RecordProjection(fields=(_field("name", "value", "Name"),)),),
        marker="First",
    )
    second = first.model_copy(update={"marker": "Second"})
    adapter = TypeAdapter(tuple[MarkedExpectation, ...])
    payload = adapter.dump_json((first, second, first))
    parsed = adapter.validate_json(payload, context=GuardValidationContext())
    assert adapter.dump_json(parsed) == payload
    assert parsed[0] is parsed[2]
    assert parsed[0] is not parsed[1]
    assert parsed[0].alternatives[0] is parsed[1].alternatives[0]


def test_expected_projection_tokens_share_json_clones_but_keep_complete_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = _record(
        source="fixture",
        key=("member:1",),
        register_name="Register",
        member_name="Variable",
        fields=SourceFields(name=value_field("Literal")),
        edition_scope=_interval("2020", "2020"),
    )
    expected = _expectation(original, _field("name", "value", "Literal"))
    cloned = RecordExpectation.model_validate_json(expected.model_dump_json())
    projection = expected.alternatives[0]
    different = (
        RecordProjection(fields=(_field("name", "value", "Other"),)),
        RecordProjection(fields=(_field("definition", "value", "Literal"),)),
        RecordProjection(
            fields=(FieldExpectation(name="sensitivity", status="value", value=True),)
        ),
        RecordProjection(
            fields=(FieldExpectation(name="sensitivity", status="value", value=False),)
        ),
        RecordProjection(
            subject=original.subject.model_copy(
                update={"register_name": SourceCoordinate(status="value", native_id=1)}
            )
        ),
        RecordProjection(
            subject=original.subject.model_copy(
                update={
                    "register_name": SourceCoordinate(status="value", native_id="1")
                }
            )
        ),
    )
    token = source_curation._model_token
    calls: list[RecordProjection] = []

    def counted(value: RecordProjection) -> str:
        calls.append(value)
        return token(value)

    monkeypatch.setattr(source_curation, "_model_token", counted)
    evidence = SourceEvidence((original,))
    first = evidence.expected_projections(expected)
    assert evidence.expected_projections(cloned) == first
    assert calls == [projection]
    for alternative in different:
        differing = RecordExpectation(ref=expected.ref, alternatives=(alternative,))
        assert evidence.expected_projections(differing) == {
            token(alternative): alternative
        }
    assert len(calls) == 1 + len(different)
    assert len(evidence.expected_tokens) == 1 + len(different)
    fresh = SourceEvidence((original,))
    assert fresh.expected_projections(cloned) == first
    assert len(calls) == 2 + len(different)


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


def _replacement(
    record: SourceRecord,
    *,
    fields: SourceFields | None = None,
    subject: SourceSubject | None = None,
    marker: str = "replacement",
    row: int = 999,
    raw_note: str = "replacement raw cell",
) -> SourceRecord:
    revision = _revision(record.source, marker)
    locator = record.locators[0]
    return SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=locator.semantic_record_key,
                physical_file=revision.artifact_path,
                physical_table=locator.physical_table,
                physical_record=f"row:{row}",
                physical_cells=(f"fixture!A{row}",),
            ),
        ),
        subject=subject or record.subject,
        edition_scope=record.edition_scope,
        edition_period_scope=record.edition_period_scope,
        fields=fields or record.fields,
        language=record.language,
        code_set_references=record.code_set_references,
        original_period_text=record.original_period_text,
        context=record.context,
        delivered_cells=(
            DeliveredCell(
                name="fixture_note",
                present=True,
                raw_value=raw_note,
                interpreted_value=raw_note,
            ),
        ),
    )


def _lisa_records() -> tuple[SourceRecord, SourceRecord]:
    source = "scb-registerinformation"
    native_2010 = NativeCoordinates(
        register_id=34,
        register_variant_id=153,
        edition_id=8652,
        variable_id=31619,
        member_id=421800,
    )
    native_2017 = NativeCoordinates(
        register_id=34,
        register_variant_id=153,
        edition_id=13286,
        variable_id=31619,
        member_id=590946,
    )
    known = _record(
        source=source,
        key=(
            "register:34",
            "variant:153",
            "edition:8652",
            "variable:31619",
            "member:421800",
        ),
        register_name="Longitudinell integrationsdatabas (LISA)",
        member_name=(
            "Ersättning i samband med arbetsmarknadspolitisk åtgärd, förekomst av"
        ),
        fields=SourceFields(
            availability=value_field(True),
            column_name=value_field("AmPolTyp", raw="AmPolTyp"),
            data_type=value_field("integer", raw="int"),
            data_length=value_field("0", raw="0"),
            description=value_field("unrelated description"),
        ),
        edition_scope=_interval("2010", "2010"),
        native=native_2010,
        variant=SourceCoordinate(
            status="value",
            native_id=153,
            name="Individer, 15 år och äldre",
        ),
        population=SourceCoordinate(
            status="value",
            name="Folkbokförda personer i Sverige 15 år och äldre",
        ),
        row=456481,
    )
    blank = _record(
        source=source,
        key=(
            "register:34",
            "variant:153",
            "edition:13286",
            "variable:31619",
            "member:590946",
        ),
        register_name="Longitudinell integrationsdatabas (LISA)",
        member_name=(
            "Ersättning i samband med arbetsmarknadspolitisk åtgärd, förekomst av"
        ),
        fields=SourceFields(
            availability=value_field(True),
            column_name=SourceField(status="unknown", raw_value=""),
            data_type=SourceField(status="unknown", raw_value=""),
            data_length=SourceField(status="unknown", raw_value=""),
            description=value_field("another unrelated description"),
        ),
        edition_scope=_interval("2017", "2017"),
        native=native_2017,
        variant=SourceCoordinate(
            status="value",
            native_id=153,
            name="Individer, 15 år och äldre",
        ),
        population=SourceCoordinate(
            status="value",
            name="Folkbokförda personer i Sverige 15 år och äldre",
        ),
        row=248061,
    )
    return known, blank


def _lisa_case(known: SourceRecord, blank: SourceRecord) -> CurationCase:
    fields_known = (
        _field("column_name", "value", "AmPolTyp"),
        _field("data_type", "value", "integer"),
        _field("data_length", "value", "0"),
    )
    fields_blank = (
        _field("column_name", "unknown"),
        _field("data_type", "unknown"),
        _field("data_length", "unknown"),
    )
    return CurationCase(
        case_id="illustrative-lisa-ampoltyp-unresolved",
        targets=(_expectation(blank, *fields_blank),),
        support=(_expectation(known, *fields_known),),
        peer_guards=(
            PeerGuard(
                guard_id="lisa-ampoltyp-native-peers",
                source=known.source,
                native=NativeCoordinates(
                    register_id=34,
                    register_variant_id=153,
                    variable_id=31619,
                ),
                expected_members=(_ref(known), _ref(blank)),
            ),
        ),
        decision=_decision(),
    )


def test_lisa_unresolved_case_ignores_layout_raw_and_unprojected_changes() -> None:
    known, blank = _lisa_records()
    case = _lisa_case(known, blank)
    changed_delivery = _replacement(
        blank,
        fields=blank.fields.model_copy(
            update={"description": value_field("new unrelated description")}
        ),
        marker="new-artifact",
        row=99,
        raw_note="different raw cell",
    )

    result = evaluate_case(case, (record for record in (changed_delivery, known)))

    assert result.status == "applicable"
    assert result.decision == case.decision


@pytest.mark.parametrize(
    "change",
    (
        "target",
        "target_subject",
        "target_missing",
        "support",
        "support_missing",
        "peer",
    ),
)
def test_lisa_unresolved_case_blocks_relevant_source_changes(change: str) -> None:
    known, blank = _lisa_records()
    case = _lisa_case(known, blank)
    records = [known, blank]
    if change == "target":
        records[1] = _replacement(
            blank,
            fields=blank.fields.model_copy(
                update={"column_name": value_field("AmPolTyp")}
            ),
        )
    elif change == "target_subject":
        records[1] = _replacement(
            blank,
            subject=blank.subject.model_copy(
                update={
                    "population": SourceCoordinate(
                        status="value",
                        name="Changed population",
                    )
                }
            ),
        )
    elif change == "target_missing":
        records.pop()
    elif change == "support":
        records[0] = _replacement(
            known,
            fields=known.fields.model_copy(update={"data_type": value_field("text")}),
        )
    elif change == "support_missing":
        records.pop(0)
    else:
        records.append(
            _record(
                source=known.source,
                key=(
                    "register:34",
                    "variant:153",
                    "edition:9999",
                    "variable:31619",
                    "member:9999",
                ),
                register_name=known.subject.register_name.name or "",
                member_name=known.subject.member.name or "",
                fields=known.fields,
                edition_scope=_interval("2011", "2011"),
                native=NativeCoordinates(
                    register_id=34,
                    register_variant_id=153,
                    edition_id=9999,
                    variable_id=31619,
                    member_id=9999,
                ),
            )
        )

    result = evaluate_case(case, records)

    assert result.status == "stale"
    assert result.decision is None
    expected_code = {
        "target": "target_projection_changed",
        "target_subject": "target_projection_changed",
        "target_missing": "target_missing",
        "support": "support_projection_changed",
        "support_missing": "support_missing",
        "peer": "peer_membership_changed",
    }[change]
    issue = next(issue for issue in result.issues if issue.code == expected_code)
    if change in {"target", "target_subject", "support"}:
        assert len(issue.missing_projections) == len(issue.added_projections) == 1
    elif change in {"target_missing", "support_missing"}:
        assert len(issue.missing_members) == 1
        assert not issue.added_members
    else:
        assert not issue.missing_members
        assert len(issue.added_members) == 1
        assert issue.added_members[0].semantic_record_key[-1] == "member:9999"


def _lova_record(
    subset: str,
    *,
    label: str,
    data_type: str,
    scope: TemporalScope,
    coverage_from: str,
    coverage_to: str | None,
    row: int,
) -> SourceRecord:
    return _record(
        source="sos-metadata",
        key=("register:LOVA", f"deldatamangd:{subset}", "variable:EXAMAR"),
        register_name="LOVA",
        member_name="EXAMAR",
        fields=SourceFields(
            availability=value_field(True),
            column_name=value_field("EXAMAR"),
            name=value_field(label),
            data_type=value_field(data_type),
            coverage_from=value_field(coverage_from),
            coverage_to=(
                value_field(coverage_to)
                if coverage_to is not None
                else SourceField(status="unknown", raw_value=None)
            ),
            representation=value_field("YYYY"),
            source_attribution=value_field("SCB-HREG"),
        ),
        edition_scope=scope,
        variant=SourceCoordinate(status="value", name=subset),
        row=row,
    )


def _lova_case(records: tuple[SourceRecord, ...]) -> CurationCase:
    expectations = tuple(
        _expectation(
            record,
            _expected_field(record, "name"),
            _expected_field(record, "data_type"),
            _expected_field(record, "coverage_from"),
            _expected_field(record, "coverage_to"),
            _field("representation", "value", "YYYY"),
        )
        for record in records
    )
    return CurationCase(
        case_id="illustrative-lova-examar-unresolved",
        targets=expectations,
        peer_guards=(
            PeerGuard(
                guard_id="lova-examar-name-peers",
                source="sos-metadata",
                register_name="LOVA",
                fields=(_field("column_name", "value", "EXAMAR"),),
                expected_members=tuple(_ref(record) for record in records),
            ),
        ),
        # Selective hyperlink expectations are outside this first projection. The
        # decision therefore withholds binding but does not claim link-change coverage.
        decision=_decision(),
    )


def test_lova_case_preserves_three_occurrences_and_unknown_open_scopes() -> None:
    records = (
        _lova_record(
            "A_LOVA",
            label="Examensår",
            data_type="integer",
            scope=_interval("1995", "2022"),
            coverage_from="1995",
            coverage_to="2022",
            row=32,
        ),
        _lova_record(
            "A_LOVA_EXAMEN",
            label="Utbildningsår (avslutningsår högsta utb.)",
            data_type="text",
            scope=TemporalScope(
                kind="unknown",
                label="Data från=1977; Data till=<blank>",
            ),
            coverage_from="1977",
            coverage_to=None,
            row=33,
        ),
        _lova_record(
            "A_LOVA_HOSP",
            label="Examensår",
            data_type="integer",
            scope=TemporalScope(
                kind="unknown",
                label="Data från=1900; Data till=<blank>",
            ),
            coverage_from="1900",
            coverage_to=None,
            row=34,
        ),
    )
    case = _lova_case(records)

    result = evaluate_case(case, list(reversed(records)))

    assert result.status == "applicable"
    assert records[1].edition_scope.kind == records[2].edition_scope.kind == "unknown"
    assert result.decision == case.decision


def test_projection_sets_keep_conflicts_but_ignore_identical_duplicates() -> None:
    base = _lova_record(
        "A_LOVA_EXAMEN",
        label="Utbildningsår",
        data_type="integer",
        scope=TemporalScope(kind="unknown", label="open source window"),
        coverage_from="1977",
        coverage_to=None,
        row=33,
    )
    text = _replacement(
        base,
        fields=base.fields.model_copy(update={"data_type": value_field("text")}),
        row=334,
    )
    expected = RecordExpectation(
        ref=_ref(base),
        alternatives=(
            _projection(
                _field("data_type", "value", "integer"),
                edition_scope=base.edition_scope,
                subject=base.subject,
            ),
            _projection(
                _field("data_type", "value", "text"),
                edition_scope=base.edition_scope,
                subject=base.subject,
            ),
        ),
    )
    case = CurationCase(
        case_id="illustrative-same-key-conflict",
        targets=(expected,),
        decision=_decision(),
    )
    duplicate_integer = _replacement(
        base,
        marker="duplicate",
        row=333,
    )

    applicable = evaluate_case(case, [text, duplicate_integer, base])
    third = _replacement(
        base,
        fields=base.fields.model_copy(update={"data_type": value_field("date")}),
        row=335,
    )
    stale = evaluate_case(case, [base, text, third])

    assert applicable.status == "applicable"
    assert stale.status == "stale"
    assert {issue.code for issue in stale.issues} == {"target_projection_changed"}
    assert len(stale.issues[0].missing_projections) == 0
    assert len(stale.issues[0].added_projections) == 1
    assert stale.issues[0].added_projections[0].fields[0].value == "date"
