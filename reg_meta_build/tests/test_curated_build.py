"""Prepared evidence -> checked curation -> ordinary catalog, without raw readers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta.errors import EXIT_CONFIG, EXIT_OUTPUT, EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.prepared_sources import prepare_source_records
from reg_meta_build.source_curation import (
    BoundedUnresolvedDecision,
    CodeSetExpectation,
    CurationCase,
    CurationResolutionError,
    FieldExpectation,
    FormVariableDecision,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
    evaluate_case,
    inspect_cases,
    resolve_cases,
)
from reg_meta_build.source_records import (
    CodeSetReference,
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

if TYPE_CHECKING:
    from pathlib import Path


_REVISION = SourceRevision.create(
    dataset="fixture-export",
    publisher="SCB",
    purpose="Synthetic annual column repair",
    upstream_revision="one",
    artifact_path="fixture.csv",
    artifact_size=1,
    artifact_sha256=hashlib.sha256(b"fixture").hexdigest(),
)


def _scope(year: int) -> TemporalScope:
    return TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start=str(year), end=str(year)),)
    )


def _record(
    year: int,
    *,
    member: int | None = None,
    column: str | None = None,
    description: str = "description",
    data_type: str | None = None,
    code_set_references: tuple[CodeSetReference, ...] = (),
) -> SourceRecord:
    member = member or year
    fields = SourceFields(
        availability=value_field(True),
        name=value_field("Benefit indicator"),
        definition=value_field("Occurrence of benefit"),
        description=value_field(description),
        operational_definition=value_field("Own annual definition"),
        column_name=value_field(column)
        if column
        else SourceField(status="unknown", raw_value=""),
        data_type=value_field(data_type)
        if data_type
        else SourceField(status="unknown", raw_value=""),
        data_length=SourceField(status="unknown", raw_value=""),
    )
    return SourceRecord.create(
        revision=_REVISION,
        locators=(
            RecordLocator(
                semantic_record_key=("variable:11", f"member:{member}"),
                physical_file="fixture.csv",
                physical_table="fixture.csv",
                physical_record=f"row:{member}",
                physical_cells=(),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="value", name="Study register"),
            variant=SourceCoordinate(status="value", name="Individuals"),
            member=SourceCoordinate(status="value", name="Benefit indicator"),
            population=SourceCoordinate(status="value", name="Registered individuals"),
            native=NativeCoordinates(
                register_id=1,
                register_variant_id=2,
                variable_id=11,
                member_id=member,
                edition_id=year,
            ),
        ),
        edition_scope=_scope(year),
        edition_period_scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start=f"{year}-01-01", end=f"{year}-12-31"),),
        ),
        fields=fields,
        code_set_references=code_set_references,
    )


def _ref(record: SourceRecord) -> SourceRecordRef:
    return SourceRecordRef(
        source=record.source, semantic_record_key=record.locators[0].semantic_record_key
    )


def _expect(record: SourceRecord, names: tuple[str, ...]) -> RecordExpectation:
    fields = []
    for name in names:
        field = getattr(record.fields, name)
        fields.append(
            FieldExpectation(
                name=name,
                status=field.status if field else "absent",
                value=field.value if field else None,
            )
        )
    return RecordExpectation(
        ref=_ref(record),
        alternatives=(
            RecordProjection(
                fields=tuple(fields),
                subject=record.subject,
                edition_scope=record.edition_scope,
                edition_period_scope=record.edition_period_scope,
            ),
        ),
    )


@pytest.fixture
def case_inputs() -> tuple[CurationCase, tuple[SourceRecord, ...]]:
    known = _record(2010, column="Benefit", data_type="integer")
    target = _record(2017)
    case = CurationCase(
        case_id="reviewed-benefit-2017",
        targets=(
            _expect(
                target,
                (
                    "availability",
                    "column_name",
                    "data_type",
                    "data_length",
                    "operational_definition",
                ),
            ),
        ),
        support=(
            _expect(
                known,
                (
                    "name",
                    "definition",
                    "description",
                    "operational_definition",
                    "measurement_unit",
                    "column_name",
                ),
            ),
        ),
        peer_guards=(
            PeerGuard(
                guard_id="2017-members",
                source=_REVISION.dataset,
                native=NativeCoordinates(
                    register_id=1, register_variant_id=2, variable_id=11
                ),
                edition_scopes=(_scope(2017),),
                expected_members=(_ref(target),),
            ),
        ),
        decision=FormVariableDecision(
            reviewed=True,
            register_slug="study",
            variant_slug="individuals",
            variable_slug="benefit",
            provider_key="11",
            delivery_column_name="Benefit",
            canonical_source=_ref(known),
            is_sensitive=False,
            is_identifier=False,
            reason="Synthetic reviewed evidence establishes the spelling for this exact member.",
            coding="withheld",
            coding_reason="Code evidence is outside this explicit slice.",
        ),
    )
    return case, (known, target)


def _prepare(
    tmp_path: Path, cases: tuple[CurationCase, ...], records: tuple[SourceRecord, ...]
) -> list[str]:
    input_root = tmp_path / "inputs"
    path = input_root / f"records-{len(list(input_root.glob('records-*')))}"
    manifest = prepare_source_records(
        path,
        records=records,
        revisions=(_REVISION,),
        scope="Synthetic exact annual column repair",
    )
    input_commit = accept_prepared(path)
    cases_path = tmp_path / "cases.json"
    cases_path.write_text(json.dumps([case.model_dump(mode="json") for case in cases]))
    return [
        "build-curated-db",
        "--records",
        str(path),
        "--records-sha256",
        manifest.artifact_sha256,
        "--cases",
        str(cases_path),
        "--db-path",
        str(tmp_path / "reg_meta.db"),
        "--records-commit",
        input_commit,
    ]


def test_warm_build_materializes_correction_without_raw_or_legacy_work(
    tmp_path: Path,
    case_inputs,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from reg_meta_build.sources import lisa, scb_records, sos

    from reg_meta_build import db

    case, records = case_inputs
    argv = _prepare(tmp_path, (case,), records)

    def forbidden(*args, **kwargs):
        pytest.fail("warm build called a raw source reader or legacy materializer")

    monkeypatch.setattr(lisa, "read_lisa_source", forbidden)
    monkeypatch.setattr(scb_records, "iter_scb_observations", forbidden)
    monkeypatch.setattr(sos, "parse_register_file", forbidden)
    monkeypatch.setattr(db, "materialize", forbidden)
    assert run(argv) == 0
    assert json.loads(capsys.readouterr().out)["structural_validation"] == "passed"
    with sqlite3.connect(tmp_path / "reg_meta.db") as conn:
        row = conn.execute(
            "SELECT valid_from, valid_to, delivery_column_name, data_type, data_length, value_set_id, operational_definition, provenance FROM variable_state"
        ).fetchone()
        assert row[:7] == (
            "2017-01-01",
            "2017-12-31",
            "Benefit",
            None,
            None,
            None,
            "Own annual definition",
        )
        assert "Coding withheld" in row[7]
        assert conn.execute("SELECT slug, name FROM variable").fetchone() == (
            "benefit",
            "Benefit indicator",
        )
    original = (tmp_path / "reg_meta.db").read_bytes()
    assert run(argv) == 0
    assert (tmp_path / "reg_meta.db").read_bytes() == original


def test_relevant_update_blocks_before_replacing_existing_database(
    tmp_path: Path, case_inputs, capsys
) -> None:
    case, records = case_inputs
    argv = _prepare(tmp_path, (case,), records)
    assert run(argv) == 0
    original = (tmp_path / "reg_meta.db").read_bytes()
    changed = (records[0], _record(2017, column="Different"))
    argv = _prepare(tmp_path, (case,), changed)
    assert run(argv) == EXIT_CONFIG
    assert (tmp_path / "reg_meta.db").read_bytes() == original
    captured = capsys.readouterr()
    assert "Different" in captured.err + captured.out
    assert "target_projection_changed" in captured.err + captured.out


def test_case_dependencies_ignore_unconsumed_facts_and_unrelated_years(
    case_inputs,
) -> None:
    case, records = case_inputs
    target = _record(2017, description="An unrelated source edit")
    assert (
        evaluate_case(case, (records[0], target, _record(2025))).status == "applicable"
    )
    assert resolve_cases((case,), (records[0], target)) == resolve_cases(
        (case,), records
    )
    added_peer = _record(2017, member=12345)
    result = evaluate_case(case, (*records, added_peer))
    assert result.status == "stale"
    assert result.issues[0].added_members == (_ref(added_peer),)


def test_resolution_rejects_intersecting_cases_or_unchecked_consumed_facts(
    case_inputs,
) -> None:
    case, records = case_inputs
    overlap = case.model_copy(update={"case_id": "overlapping-case"})
    with pytest.raises(CurationResolutionError, match="already assigned"):
        resolve_cases((case, overlap), records)
    target = case.targets[0]
    shape = target.alternatives[0]
    incomplete = target.model_copy(
        update={
            "alternatives": (
                shape.model_copy(
                    update={
                        "fields": tuple(
                            f for f in shape.fields if f.name != "data_type"
                        )
                    }
                ),
            )
        }
    )
    with pytest.raises(CurationResolutionError, match="checked fields"):
        resolve_cases((case.model_copy(update={"targets": (incomplete,)}),), records)


def test_cli_report_cannot_overwrite_inputs_or_database(
    tmp_path: Path, case_inputs
) -> None:
    case, records = case_inputs
    argv = _prepare(tmp_path, (case,), records)
    source = tmp_path / "inputs" / "records-0" / "manifest.json"
    original = source.read_bytes()
    assert run([*argv, "--output", str(source)]) == EXIT_USAGE
    assert source.read_bytes() == original
    assert not (tmp_path / "reg_meta.db").exists()
    assert run([*argv, "--output", str(tmp_path / "reg_meta.db")]) == EXIT_USAGE
    assert not (tmp_path / "reg_meta.db").exists()


@pytest.mark.parametrize("change", ["pooled", "negative", "rename"])
def test_even_reviewed_case_cannot_infer_annual_availability_or_rename_a_different_column(
    case_inputs, change: str
) -> None:
    case, records = case_inputs
    target = records[1]
    fields = target.fields
    scope = target.edition_scope
    if change == "pooled":
        scope = TemporalScope(kind="pooled", label="2016–2018")
    elif change == "negative":
        fields = fields.model_copy(
            update={"availability": SourceField(status="negative")}
        )
    else:
        fields = fields.model_copy(update={"column_name": value_field("Other")})
    updated = SourceRecord.create(
        revision=_REVISION,
        locators=target.locators,
        subject=target.subject,
        edition_scope=scope,
        edition_period_scope=target.edition_period_scope,
        fields=fields,
    )
    reviewed = case.model_copy(
        update={
            "targets": (
                _expect(
                    updated,
                    (
                        "availability",
                        "column_name",
                        "data_type",
                        "data_length",
                        "operational_definition",
                    ),
                ),
            ),
            "peer_guards": (),
        }
    )
    with pytest.raises(CurationResolutionError, match="annual|availability|rename"):
        resolve_cases((reviewed,), (records[0], updated))


def test_a_reviewed_spelling_still_needs_checked_source_support(case_inputs) -> None:
    case, records = case_inputs
    decision = case.decision.model_copy(update={"delivery_column_name": "Unsupported"})
    with pytest.raises(
        CurationResolutionError, match="spelling needs checked source support"
    ):
        resolve_cases((case.model_copy(update={"decision": decision}),), records)


def test_changed_exact_period_stales_case_and_cannot_be_widened(case_inputs) -> None:
    case, records = case_inputs
    target = records[1]
    updated = SourceRecord.create(
        revision=_REVISION,
        locators=target.locators,
        subject=target.subject,
        edition_scope=target.edition_scope,
        edition_period_scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2017-10-31", end="2017-10-31"),),
        ),
        fields=target.fields,
    )
    assert evaluate_case(case, (records[0], updated)).status == "stale"
    target_expectation = case.targets[0]
    shape = target_expectation.alternatives[0].model_copy(
        update={"edition_period_scope": updated.edition_period_scope}
    )
    rereviewed = case.model_copy(
        update={
            "targets": (
                target_expectation.model_copy(update={"alternatives": (shape,)}),
            )
        }
    )
    with pytest.raises(CurationResolutionError, match="exact source period"):
        resolve_cases((rereviewed,), (records[0], updated))


def test_directory_report_rejected_before_publication(
    tmp_path: Path, case_inputs
) -> None:
    case, records = case_inputs
    argv = _prepare(tmp_path, (case,), records)
    database = tmp_path / "reg_meta.db"
    database.write_bytes(b"existing database")
    assert run([*argv, "--output", str(tmp_path)]) == EXIT_USAGE
    assert database.read_bytes() == b"existing database"


def test_late_report_failure_explicitly_reports_published_database(
    tmp_path: Path, case_inputs, monkeypatch, capsys
) -> None:
    from reg_meta_build import cli

    case, records = case_inputs
    argv = _prepare(tmp_path, (case,), records)

    def failed_report(*args, **kwargs):
        raise OSError("late report failure")

    monkeypatch.setattr(cli, "write_json", failed_report)
    assert run(argv) == EXIT_OUTPUT
    receipt = json.loads(capsys.readouterr().err.splitlines()[-1])["error"]
    assert receipt["catalog_published"] is True
    assert (
        receipt["db_sha256"]
        == hashlib.sha256((tmp_path / "reg_meta.db").read_bytes()).hexdigest()
    )


def test_inspection_preserves_unknown_metadata_with_actionable_warnings(
    case_inputs,
) -> None:
    case, records = case_inputs
    inspection = inspect_cases((case,), records)

    assert inspection.buildable
    state = inspection.variables[0].states[0]
    assert state.data_type is None
    assert state.data_length is None
    warnings = {item.code: item for item in inspection.diagnostics}
    assert set(warnings) == {
        "missing_data_type",
        "missing_data_length",
        "coding_withheld",
    }
    assert all(item.severity == "warning" for item in warnings.values())
    assert all(item.refs == (_ref(records[1]),) for item in warnings.values())
    assert all(item.case_id == case.case_id for item in warnings.values())
    assert "does not yet resolve coding" in warnings["coding_withheld"].detail
    assert inspection.report()["status"] == "ready"


def test_selected_prepared_member_cannot_silently_disappear(case_inputs) -> None:
    case, records = case_inputs
    added = _record(2025)
    inspection = inspect_cases((case,), (*records, added))

    assert not inspection.buildable
    errors = [item for item in inspection.diagnostics if item.severity == "error"]
    assert [item.code for item in errors] == ["unaccounted_source_member"]
    assert errors[0].refs == (_ref(added),)
    member = next(
        item for item in inspection.source_accounting if item.ref == _ref(added)
    )
    assert member.roles == ()
    assert member.case_ids == ()
    assert member.locators == added.locators
    with pytest.raises(CurationResolutionError, match="unaccounted_source_member"):
        resolve_cases((case,), (*records, added))


def test_source_accounting_retains_identical_physical_occurrences(case_inputs) -> None:
    case, records = case_inputs
    inspection = inspect_cases((case,), (*records, records[1]))
    assert inspection.buildable
    member = next(
        item for item in inspection.source_accounting if item.ref == _ref(records[1])
    )
    assert len(member.occurrences) == 2
    assert member.occurrences[0] == member.occurrences[1]
    assert member.occurrences[0].record_id == records[1].record_id
    assert member.occurrences[0].source_revision_id == records[1].source_revision_id
    assert member.output_disposition == "resolved_target"
    assert inspection.report()["source_occurrences"] == 3


def test_unknown_flag_blocks_strict_and_is_withheld_in_diagnostic_output(
    tmp_path: Path, case_inputs, capsys
) -> None:
    case, records = case_inputs
    unknown_record = _record(2018)
    unknown_case = case.model_copy(
        update={
            "case_id": "unknown-flag",
            "peer_guards": (),
            "targets": (
                _expect(
                    unknown_record,
                    tuple(
                        field.name for field in case.targets[0].alternatives[0].fields
                    ),
                ),
            ),
            "decision": case.decision.model_copy(
                update={"variable_slug": "unknown", "is_sensitive": None}
            ),
        }
    )
    argv = _prepare(tmp_path, (case, unknown_case), (*records, unknown_record))
    assert run(argv) == EXIT_CONFIG
    strict = json.loads(capsys.readouterr().out)
    issue = next(
        item for item in strict["diagnostics"] if item["code"] == "unresolved_flag"
    )
    assert issue["severity"] == "error"
    assert issue["subject"] == "scb/study/unknown"
    assert issue["fields"] == ["is_sensitive"]
    assert issue["withheld_output"] == ["variable", "states", "dependent_edges"]
    assert not (tmp_path / "reg_meta.db").exists()
    argv[argv.index("--db-path")] = "--diagnostic-db-path"
    argv.append("--diagnostic")
    assert run(argv) == EXIT_CONFIG
    diagnostic = json.loads(capsys.readouterr().out)
    assert diagnostic["status"] == "diagnostic_complete"
    assert diagnostic["diagnostics"] == strict["diagnostics"]
    withheld = next(
        item
        for item in diagnostic["case_accounting"]
        if item["case_id"] == "unknown-flag"
    )
    assert withheld["disposition"] == "withheld"
    assert withheld["decision"]["is_sensitive"] is None
    assert withheld["output_fqid"] == "scb/study/unknown"
    source = next(
        item
        for item in diagnostic["source_accounting"]
        if item["ref"] == _ref(unknown_record).model_dump(mode="json")
    )
    assert source["output_disposition"] == "withheld_target"
    with sqlite3.connect(tmp_path / "reg_meta.db") as conn:
        assert conn.execute("SELECT slug FROM variable").fetchall() == [("benefit",)]


def test_only_explicit_expected_peer_members_are_review_context(case_inputs) -> None:
    case, records = case_inputs
    peer = _record(2017, member=12345)
    guard = case.peer_guards[0]
    reviewed = case.model_copy(
        update={
            "peer_guards": (
                guard.model_copy(
                    update={"expected_members": (*guard.expected_members, _ref(peer))}
                ),
            )
        }
    )
    inspection = inspect_cases((reviewed,), (*records, peer))

    assert inspection.buildable
    assert len(inspection.variables[0].states) == 1
    member = next(
        item for item in inspection.source_accounting if item.ref == _ref(peer)
    )
    assert member.roles == ("review_context",)
    assert member.case_ids == (case.case_id,)
    assert member.locators == peer.locators

    unexpected = inspect_cases((case,), (*records, peer))
    assert not unexpected.buildable
    assert {item.code for item in unexpected.diagnostics} >= {
        "peer_membership_changed",
        "unaccounted_source_member",
    }
    unclassified = next(
        item for item in unexpected.source_accounting if item.ref == _ref(peer)
    )
    assert unclassified.roles == ()


def _withhold(
    record: SourceRecord, *, case_id: str = "unresolved-2018"
) -> CurationCase:
    return CurationCase(
        case_id=case_id,
        targets=(_expect(record, ("data_type",)),),
        decision=BoundedUnresolvedDecision(
            reviewed=True,
            withheld_aspects=("data_type",),
            reason="The exact member awaits evidence; exclude its entire catalog content.",
        ),
    )


def test_bounded_unresolved_withholds_whole_target_but_allows_separate_safe_output(
    case_inputs,
) -> None:
    case, records = case_inputs
    withheld = _record(2018)
    unresolved = _withhold(withheld)
    inspection = inspect_cases((unresolved, case), (*records, withheld))

    assert inspection.buildable
    assert inspection.variables == resolve_cases((case,), records)
    accepted = next(
        item for item in inspection.diagnostics if item.code == "accepted_unresolved"
    )
    assert accepted.severity == "warning"
    assert accepted.case_id == unresolved.case_id
    assert accepted.refs == (_ref(withheld),)
    assert "entire target content withheld" in accepted.detail
    member = next(
        item for item in inspection.source_accounting if item.ref == _ref(withheld)
    )
    assert member.roles == ("target",)


@pytest.mark.parametrize("problem", ["stale", "overlap"])
def test_bounded_unresolved_cannot_waive_staleness_or_target_conflicts(
    case_inputs, problem: str
) -> None:
    case, records = case_inputs
    if problem == "stale":
        unresolved = _withhold(_record(2018))
        selected = (*records, _record(2018, data_type="text"))
        expected_code = "target_projection_changed"
    else:
        unresolved = _withhold(records[1])
        selected = records
        expected_code = "target_assignment_conflict"
    inspection = inspect_cases((case, unresolved), selected)

    assert not inspection.buildable
    assert any(
        item.code == expected_code and item.severity == "error"
        for item in inspection.diagnostics
    )
    assert not any(
        item.code == "accepted_unresolved" for item in inspection.diagnostics
    )
    with pytest.raises(CurationResolutionError, match=expected_code):
        resolve_cases((case, unresolved), selected)


def test_inspection_collects_independent_blockers_without_mutating_sources(
    case_inputs,
) -> None:
    case, records = case_inputs
    extra = _record(2018)
    invalid = case.model_copy(
        update={
            "case_id": "invalid-formation",
            "targets": (_expect(extra, ("data_type",)),),
            "peer_guards": (),
            "support": (),
        }
    )
    selected = (
        records[0],
        _record(2017, data_type="text"),
        extra,
        _record(2025),
    )
    before = tuple(record.model_dump_json() for record in selected)
    cases_before = (case.model_dump_json(), invalid.model_dump_json())
    inspection = inspect_cases((case, invalid), selected)

    assert not inspection.buildable
    assert {item.code for item in inspection.diagnostics} >= {
        "target_projection_changed",
        "formation_invalid",
        "unaccounted_source_member",
        "no_catalog_content",
    }
    assert tuple(record.model_dump_json() for record in selected) == before
    assert (case.model_dump_json(), invalid.model_dump_json()) == cases_before
    assert inspect_cases((invalid, case), reversed(selected)).report() == (
        inspection.report()
    )


def test_successful_inspection_report_is_input_order_independent(case_inputs) -> None:
    case, records = case_inputs
    withheld = _record(2018)
    unresolved = _withhold(withheld)
    selected = (*records, withheld)
    inspection = inspect_cases((case, unresolved), selected)

    assert inspection.buildable
    assert (
        inspection.report()
        == inspect_cases((unresolved, case), reversed(selected)).report()
    )


@pytest.mark.parametrize("selection", ["empty", "unaccounted", "withheld"])
def test_inspection_never_reports_an_empty_catalog_as_success(selection: str) -> None:
    record = _record(2018)
    records = () if selection == "empty" else (record,)
    cases = (_withhold(record),) if selection == "withheld" else ()
    inspection = inspect_cases(cases, records)

    assert not inspection.buildable
    assert inspection.variables == ()
    assert inspection.report()["status"] == "blocked"
    assert any(
        item.code == "no_catalog_content" and item.severity == "error"
        for item in inspection.diagnostics
    )
    with pytest.raises(CurationResolutionError, match="no_catalog_content"):
        resolve_cases(cases, records)


@pytest.mark.parametrize("problem", ["register", "variant", "provider"])
def test_inspection_blocks_cross_case_output_contract_failures(
    case_inputs, problem: str
) -> None:
    case, records = case_inputs
    second = _record(2018)
    field_name = "register_name" if problem == "register" else problem
    value = (
        "unsupported-provider"
        if problem == "provider"
        else SourceCoordinate(status="value", name="Inconsistent name")
    )
    second = SourceRecord.create(
        revision=_REVISION,
        locators=second.locators,
        subject=second.subject.model_copy(update={field_name: value}),
        edition_scope=second.edition_scope,
        edition_period_scope=second.edition_period_scope,
        fields=second.fields,
    )
    second_case = case.model_copy(
        update={
            "case_id": "second-variable",
            "targets": (
                _expect(
                    second,
                    tuple(
                        field.name for field in case.targets[0].alternatives[0].fields
                    ),
                ),
            ),
            "peer_guards": (),
            "decision": case.decision.model_copy(
                update={"variable_slug": "second-benefit", "provider_key": "12"}
            ),
        }
    )
    inspection = inspect_cases((case, second_case), (*records, second))

    assert not inspection.buildable
    assert inspection.report()["status"] == "blocked"
    failures = [
        item
        for item in inspection.diagnostics
        if item.code == "output_contract_invalid"
    ]
    assert len(failures) == 1
    assert failures[0].severity == "error"
    assert problem in failures[0].detail
    with pytest.raises(CurationResolutionError, match="output_contract_invalid"):
        resolve_cases((case, second_case), (*records, second))


@pytest.fixture
def coding_case_inputs(case_inputs):
    case, records = case_inputs
    reference = CodeSetReference(
        reference_id="benefit-codes",
        content_sha256=hashlib.sha256(b"0:No;1:Yes").hexdigest(),
        physical_locator="codes.csv:10-11",
    )
    coded = _record(2018, code_set_references=(reference,))
    unresolved = _withhold(coded).model_copy(
        update={
            "decision": BoundedUnresolvedDecision(
                reviewed=True,
                withheld_aspects=("coding",),
                reason="Competing source coding needs review; withhold this entire target.",
            )
        }
    )
    target = unresolved.targets[0]
    checked = target.model_copy(
        update={
            "alternatives": (
                target.alternatives[0].model_copy(
                    update={
                        "code_set_references": (
                            CodeSetExpectation(
                                reference_id=reference.reference_id,
                                content_sha256=reference.content_sha256,
                            ),
                        )
                    }
                ),
            )
        }
    )
    return (
        case,
        unresolved.model_copy(update={"targets": (checked,)}),
        (*records, coded),
    )


def test_coding_unresolved_decision_requires_checked_coding_dependencies(
    coding_case_inputs,
) -> None:
    case, unresolved, records = coding_case_inputs
    target = unresolved.targets[0]
    unchecked = target.model_copy(
        update={
            "alternatives": (
                target.alternatives[0].model_copy(update={"code_set_references": None}),
            )
        }
    )
    unresolved = unresolved.model_copy(update={"targets": (unchecked,)})
    inspection = inspect_cases((case, unresolved), records)

    assert not inspection.buildable
    assert any(
        item.code == "unresolved_dependencies_missing" and item.severity == "error"
        for item in inspection.diagnostics
    )
    assert not any(
        item.code == "accepted_unresolved" for item in inspection.diagnostics
    )


@pytest.mark.parametrize("change", ["reference", "content"])
def test_changed_coding_evidence_stales_exact_unresolved_decision(
    coding_case_inputs, change: str
) -> None:
    case, unresolved, records = coding_case_inputs
    reference = records[-1].code_set_references[0]
    update = (
        {"reference_id": "replacement-codes"}
        if change == "reference"
        else {"content_sha256": hashlib.sha256(b"1:No;2:Yes").hexdigest()}
    )
    changed = _record(2018, code_set_references=(reference.model_copy(update=update),))
    inspection = inspect_cases((case, unresolved), (*records[:-1], changed))

    assert not inspection.buildable
    stale = next(
        item
        for item in inspection.diagnostics
        if item.code == "target_projection_changed"
    )
    assert stale.case_id == unresolved.case_id
    assert stale.applicability_issue is not None
    assert stale.applicability_issue.missing_projections[0].code_set_references == (
        unresolved.targets[0].alternatives[0].code_set_references
    )
    assert not any(
        item.code == "accepted_unresolved" for item in inspection.diagnostics
    )


def test_moving_coding_evidence_without_content_change_keeps_decision_applicable(
    coding_case_inputs,
) -> None:
    case, unresolved, records = coding_case_inputs
    reference = records[-1].code_set_references[0]
    moved = _record(
        2018,
        code_set_references=(
            reference.model_copy(
                update={"physical_locator": "repacked/codes.csv:44-45"}
            ),
        ),
    )
    inspection = inspect_cases((case, unresolved), (*records[:-1], moved))

    assert inspection.buildable
    assert inspection.report() == inspect_cases((case, unresolved), records).report()
    assert any(item.code == "accepted_unresolved" for item in inspection.diagnostics)


def test_explicitly_empty_coding_projection_accepts_absence_and_stales_on_arrival(
    coding_case_inputs,
) -> None:
    case, unresolved, records = coding_case_inputs
    target = unresolved.targets[0]
    unresolved = unresolved.model_copy(
        update={
            "targets": (
                target.model_copy(
                    update={
                        "alternatives": (
                            target.alternatives[0].model_copy(
                                update={"code_set_references": ()}
                            ),
                        )
                    }
                ),
            )
        }
    )

    inspection = inspect_cases((case, unresolved), (*records[:-1], _record(2018)))
    assert inspection.buildable
    assert any(item.code == "accepted_unresolved" for item in inspection.diagnostics)
    with pytest.raises(CurationResolutionError, match="target_projection_changed"):
        resolve_cases((case, unresolved), records)
