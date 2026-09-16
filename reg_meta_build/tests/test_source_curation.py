"""Illustrative applicability checks for the first shared curation boundary.

The cases mirror retained SCB and Socialstyrelsen source examples.  They are test
fixtures, not accepted curation decisions.
"""

from __future__ import annotations

import hashlib
from typing import Literal

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta_build.source_curation import (
    BoundedUnresolvedDecision,
    CurationCase,
    FieldExpectation,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
    UnresolvedAspect,
    evaluate_case,
)
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row


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


def _unresolved(*aspects: UnresolvedAspect) -> BoundedUnresolvedDecision:
    return BoundedUnresolvedDecision(
        reviewed=True,
        withheld_aspects=aspects,
        reason="Retained source evidence does not justify a semantic winner.",
    )


def test_unresolved_aspects_cannot_bypass_checks_by_using_an_unknown_label() -> None:
    with pytest.raises(ValidationError, match="withheld_aspects"):
        BoundedUnresolvedDecision.model_validate(
            {
                "reviewed": True,
                "withheld_aspects": ("code_set_binding",),
                "reason": "Use the explicit coding aspect and its dependency checks.",
            }
        )


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
        decision=_unresolved("identity"),
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
        decision=_unresolved(
            "column_name",
            "data_type",
            "data_length",
            "identity",
            "coding",
        ),
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
    assert result.decision.safe_behavior == "preserve_source_records"


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
        decision=_unresolved(
            "identity",
            "data_type",
            "period",
            "coding",
        ),
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
    assert result.decision is not None
    assert "identity" in result.decision.withheld_aspects
    assert "period" in result.decision.withheld_aspects


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
        decision=_unresolved("data_type"),
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
