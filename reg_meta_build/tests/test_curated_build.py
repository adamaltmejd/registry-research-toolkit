"""Prepared evidence -> checked curation -> ordinary catalog, without raw readers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, EXIT_OUTPUT, EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.prepared_sources import prepare_source_records
from reg_meta_build.source_curation import (
    CurationCase,
    CurationResolutionError,
    FieldExpectation,
    FormVariableDecision,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
    evaluate_case,
    resolve_cases,
)
from reg_meta_build.source_records import (
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
    path = tmp_path / "records.json"
    manifest = prepare_source_records(
        path,
        records=records,
        revisions=(_REVISION,),
        scope="Synthetic exact annual column repair",
    )
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
    source = tmp_path / "records.json"
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
