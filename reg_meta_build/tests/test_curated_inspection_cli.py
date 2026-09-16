"""Inspection and publication share one structured curation gate."""

from __future__ import annotations

import hashlib
import json
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta.db import get_manifest, open_db
from reg_meta.errors import EXIT_CONFIG, EXIT_INTERNAL, EXIT_OUTPUT, EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.prepared_sources import prepare_source_records
from reg_meta_build.source_curation import (
    CurationCase,
    FieldExpectation,
    FormVariableDecision,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def prepared_cli(tmp_path: Path) -> list[str]:
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="CLI contract test",
        upstream_revision="one",
        artifact_path="fixture.csv",
        artifact_size=1,
        artifact_sha256=hashlib.sha256(b"x").hexdigest(),
    )
    coordinate = SourceCoordinate(status="value", name="Fixture")
    record = SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=("member:1",),
                physical_file="fixture.csv",
                physical_table="fixture.csv",
                physical_record="row:1",
                physical_cells=(),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=coordinate,
            variant=coordinate,
            population=coordinate,
            member=coordinate,
            native=NativeCoordinates(variable_id=1),
        ),
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2017", end="2017"),)
        ),
        edition_period_scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2017-01-01", end="2017-12-31"),),
        ),
        fields=SourceFields(
            availability=value_field(True),
            name=value_field("Fixture variable"),
            column_name=value_field("Fixture"),
        ),
    )
    ref = SourceRecordRef(source=record.source, semantic_record_key=("member:1",))
    expectation = RecordExpectation(
        ref=ref,
        alternatives=(
            RecordProjection(
                fields=tuple(
                    FieldExpectation(
                        name=name,
                        status=field.status if field else "absent",
                        value=field.value if field else None,
                    )
                    for name in SourceFields.model_fields
                    for field in (getattr(record.fields, name),)
                ),
                subject=record.subject,
                edition_scope=record.edition_scope,
                edition_period_scope=record.edition_period_scope,
            ),
        ),
    )
    case = CurationCase(
        case_id="fixture-case",
        targets=(expectation,),
        support=(expectation,),
        decision=FormVariableDecision(
            reviewed=True,
            register_slug="fixture",
            variant_slug="fixture",
            variable_slug="fixture",
            provider_key="1",
            delivery_column_name="Fixture",
            canonical_source=ref,
            is_sensitive=False,
            is_identifier=False,
            reason="Exact source declaration for this CLI fixture.",
            coding="withheld",
            coding_reason="Coding intentionally outside this fixture.",
        ),
    )
    records = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        records, records=(record,), revisions=(revision,), scope="CLI fixture"
    )
    cases = tmp_path / "cases.json"
    cases.write_text(json.dumps([case.model_dump(mode="json")]))
    commit = accept_prepared(records)
    return [
        "--records",
        str(records),
        "--records-sha256",
        manifest.artifact_sha256,
        "--cases",
        str(cases),
        "--records-commit",
        commit,
    ]


def _arguments(command: str, prepared: list[str], tmp_path: Path) -> list[str]:
    argv = [command, *prepared]
    if command == "build-curated-db":
        argv.extend(("--db-path", str(tmp_path / "catalog.db")))
    return argv


def test_inspection_and_build_share_report_and_inspect_once_each(
    tmp_path: Path, prepared_cli, monkeypatch, capsys
) -> None:
    from reg_meta_build import resolved_catalog, source_curation

    calls = []
    original = source_curation.inspect_cases

    def inspected(cases, records):
        calls.append(cases)
        return original(cases, records)

    def no_database(*args, **kwargs):
        pytest.fail("inspection attempted to write a database")

    monkeypatch.setattr(source_curation, "inspect_cases", inspected)
    with monkeypatch.context() as guard:
        guard.setattr(resolved_catalog, "write_resolved_catalog", no_database)
        assert run(_arguments("inspect-curation", prepared_cli, tmp_path)) == 0
    inspection = json.loads(capsys.readouterr().out)
    assert len(calls) == 1
    assert not (tmp_path / "catalog.db").exists()
    assert inspection["status"] == "ready"
    assert inspection["warnings"] > 0
    assert "db_path" not in inspection

    assert run(_arguments("build-curated-db", prepared_cli, tmp_path)) == 0
    build = json.loads(capsys.readouterr().out)
    assert len(calls) == 2
    assert all(build[key] == value for key, value in inspection.items())
    assert build["catalog_published"] is True
    assert build["structural_validation"] == "passed"
    assert (
        build["db_sha256"]
        == hashlib.sha256((tmp_path / "catalog.db").read_bytes()).hexdigest()
    )


def test_blocked_build_preserves_catalog_and_returns_inspection_diagnostics(
    tmp_path: Path, prepared_cli, monkeypatch, capsys
) -> None:
    from reg_meta_build import resolved_catalog

    path = tmp_path / "cases.json"
    cases = json.loads(path.read_text())
    for role in ("targets", "support"):
        projection = cases[0][role][0]["alternatives"][0]
        next(field for field in projection["fields"] if field["name"] == "name")[
            "value"
        ] = "Changed expectation"
    path.write_text(json.dumps(cases))
    database = tmp_path / "catalog.db"
    database.write_bytes(b"previous catalog")

    def no_database(*args, **kwargs):
        pytest.fail("blocked curation attempted to write a database")

    monkeypatch.setattr(resolved_catalog, "write_resolved_catalog", no_database)
    assert run(_arguments("inspect-curation", prepared_cli, tmp_path)) == EXIT_CONFIG
    inspection = json.loads(capsys.readouterr().out)
    assert inspection["status"] == "blocked"
    assert len(inspection["diagnostics"]) >= 2
    assert inspection["errors"] >= 2
    assert "error" not in inspection
    report = tmp_path / "blocked-report.json"
    assert (
        run(
            [
                *_arguments("build-curated-db", prepared_cli, tmp_path),
                "--verbose",
                "--output",
                str(report),
            ]
        )
        == EXIT_CONFIG
    )
    envelope = json.loads(report.read_text())
    build = envelope["data"]
    assert envelope["request"]["command"] == "build-curated-db"
    assert all(build[key] == value for key, value in inspection.items())
    assert build["catalog_published"] is False
    assert database.read_bytes() == b"previous catalog"


def _add_stale_case(path: Path) -> None:
    cases = json.loads(path.read_text())
    missing = json.loads(json.dumps(cases[0]))
    missing["case_id"] = "missing-case"
    missing["targets"][0]["ref"]["semantic_record_key"] = ["missing:member"]
    missing["decision"]["variable_slug"] = "missing"
    cases.append(missing)
    path.write_text(json.dumps(cases))


def _diagnostic_arguments(prepared: list[str], output: Path) -> list[str]:
    return [
        "build-curated-db",
        *prepared,
        "--diagnostic",
        "--diagnostic-db-path",
        str(output),
    ]


def test_diagnostic_completes_safe_output_with_unchanged_blockers_and_accounting(
    tmp_path: Path, prepared_cli, capsys
) -> None:
    _add_stale_case(tmp_path / "cases.json")
    previous = tmp_path / "catalog.db"
    previous.write_bytes(b"previous catalog")
    assert run(_arguments("build-curated-db", prepared_cli, tmp_path)) == EXIT_CONFIG
    strict = json.loads(capsys.readouterr().out)
    diagnostic = tmp_path / "diagnostic.db"
    assert run(_diagnostic_arguments(prepared_cli, diagnostic)) == EXIT_CONFIG
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "diagnostic_complete"
    assert result["artifact_complete"] is True
    assert result["catalog_published"] is False
    assert result["publication_ready"] is False
    assert result["incomplete"] is True
    assert result["curation_status"] == "blocked"
    assert result["curation_exit_code"] == EXIT_CONFIG
    assert result["diagnostics"] == strict["diagnostics"]
    assert result["source_accounting"] == strict["source_accounting"]
    assert result["case_accounting"] == strict["case_accounting"]
    assert {
        item["case_id"]: item["disposition"] for item in result["case_accounting"]
    } == {"fixture-case": "resolved", "missing-case": "withheld"}
    assert result["source_occurrences"] == 1
    assert previous.read_bytes() == b"previous catalog"
    repeat = tmp_path / "diagnostic-repeat.db"
    assert run(_diagnostic_arguments(prepared_cli, repeat)) == EXIT_CONFIG
    assert repeat.read_bytes() == diagnostic.read_bytes()
    capsys.readouterr()
    with closing(open_db(diagnostic)) as conn:
        manifest = get_manifest(conn)
        assert manifest["catalog_artifact_kind"] == "diagnostic"
        assert manifest["catalog_publishable"] == "false"
        assert manifest["catalog_completeness"] == "incomplete"
        ledger = json.loads(manifest["diagnostic_accounting"])
        assert ledger["diagnostics"] == result["diagnostics"]
        assert ledger["case_accounting"] == result["case_accounting"]
        assert conn.execute("SELECT count(*) FROM variable").fetchone()[0] == 1


def test_diagnostic_option_requires_its_own_output_path(
    tmp_path: Path, prepared_cli, capsys
) -> None:
    assert (
        run([*_arguments("build-curated-db", prepared_cli, tmp_path), "--diagnostic"])
        == EXIT_USAGE
    )
    assert not (tmp_path / "catalog.db").exists()
    capsys.readouterr()
    assert (
        run(
            [
                "build-curated-db",
                *prepared_cli,
                "--diagnostic-db-path",
                str(tmp_path / "diagnostic.db"),
            ]
        )
        == EXIT_USAGE
    )
    assert not (tmp_path / "diagnostic.db").exists()


def test_diagnostic_report_cannot_replace_its_database(
    tmp_path: Path, prepared_cli
) -> None:
    diagnostic = tmp_path / "diagnostic.db"
    assert (
        run(
            [
                *_diagnostic_arguments(prepared_cli, diagnostic),
                "--output",
                str(diagnostic),
            ]
        )
        == EXIT_USAGE
    )
    assert not diagnostic.exists()


def test_diagnostic_report_failure_reports_completed_artifact(
    tmp_path: Path, prepared_cli, monkeypatch, capsys
) -> None:
    from reg_meta_build import cli

    _add_stale_case(tmp_path / "cases.json")
    diagnostic = tmp_path / "diagnostic.db"

    def failed_report(*args, **kwargs):
        raise OSError("late report failure")

    monkeypatch.setattr(cli, "write_json", failed_report)
    assert run(_diagnostic_arguments(prepared_cli, diagnostic)) == EXIT_OUTPUT
    receipt = json.loads(capsys.readouterr().err.splitlines()[-1])["error"]
    assert receipt["artifact_complete"] is True
    assert receipt["artifact_kind"] == "diagnostic"
    assert receipt["catalog_published"] is False
    assert receipt["curation_exit_code"] == EXIT_CONFIG
    with closing(open_db(diagnostic)) as conn:
        assert json.loads(get_manifest(conn)["diagnostic_accounting"])["errors"] > 0


def test_diagnostic_still_rejects_invalid_pin_and_resolved_contract(
    tmp_path: Path, prepared_cli, capsys
) -> None:
    diagnostic = tmp_path / "diagnostic.db"
    bad_pin = prepared_cli.copy()
    bad_pin[3] = "0" * 64
    assert run(_diagnostic_arguments(bad_pin, diagnostic)) == EXIT_CONFIG
    assert not diagnostic.exists()
    capsys.readouterr()
    cases = json.loads((tmp_path / "cases.json").read_text())
    cases[0]["decision"]["variable_slug"] = "Bad_slug"
    (tmp_path / "cases.json").write_text(json.dumps(cases))
    assert run(_diagnostic_arguments(prepared_cli, diagnostic)) == EXIT_CONFIG
    assert not diagnostic.exists()
    assert "curated_diagnostic_contract_invalid" in capsys.readouterr().out


@pytest.mark.parametrize("command", ["inspect-curation", "build-curated-db"])
@pytest.mark.parametrize(
    "protected",
    [
        "inputs/records/manifest.json",
        "inputs/records/files/records.sqlite",
        "cases.json",
    ],
)
def test_report_cannot_overwrite_inputs(
    tmp_path: Path, prepared_cli, command: str, protected: str
) -> None:
    path = tmp_path / protected
    previous = path.read_bytes()
    assert (
        run([*_arguments(command, prepared_cli, tmp_path), "--output", str(path)])
        == EXIT_USAGE
    )
    assert path.read_bytes() == previous
    assert not (tmp_path / "catalog.db").exists()


@pytest.mark.parametrize("command", ["inspect-curation", "build-curated-db"])
def test_report_cannot_add_file_inside_prepared_input(
    tmp_path: Path, prepared_cli, command: str
) -> None:
    report = tmp_path / "inputs" / "records" / "report.json"
    assert (
        run([*_arguments(command, prepared_cli, tmp_path), "--output", str(report)])
        == EXIT_USAGE
    )
    assert not report.exists()
    assert not report.with_suffix(".json.tmp").exists()
    assert not (tmp_path / "catalog.db").exists()


@pytest.mark.parametrize("command", ["inspect-curation", "build-curated-db"])
def test_global_db_is_rejected(tmp_path: Path, prepared_cli, command: str) -> None:
    assert (
        run([*_arguments(command, prepared_cli, tmp_path), "--db", str(tmp_path)])
        == EXIT_USAGE
    )
    assert not (tmp_path / "catalog.db").exists()


@pytest.mark.parametrize("command", ["inspect-curation", "build-curated-db"])
@pytest.mark.parametrize("malformed", ["cases", "pin"])
def test_invalid_input_is_contract_error_not_curation_diagnostic(
    tmp_path: Path, prepared_cli, command: str, malformed: str, capsys
) -> None:
    if malformed == "cases":
        (tmp_path / "cases.json").write_text('[{"decision": "not a decision"}]')
    else:
        prepared_cli[3] = "0" * 64
    assert run(_arguments(command, prepared_cli, tmp_path)) == EXIT_CONFIG
    error = json.loads(capsys.readouterr().out)
    assert error["error"]["code"] == "curated_input_invalid"
    assert "diagnostics" not in error
    assert not (tmp_path / "catalog.db").exists()


def test_blocked_report_failure_does_not_claim_publication(
    tmp_path: Path, prepared_cli, monkeypatch, capsys
) -> None:
    from reg_meta_build import cli

    (tmp_path / "cases.json").write_text("[]")

    def failed_report(*args, **kwargs):
        raise OSError("report destination unavailable")

    monkeypatch.setattr(cli, "write_json", failed_report)
    assert run(_arguments("build-curated-db", prepared_cli, tmp_path)) == EXIT_INTERNAL
    captured = capsys.readouterr()
    error = json.loads(captured.out)["error"]
    assert "report destination unavailable" in error["message"]
    assert "catalog_published" not in error
    assert not (tmp_path / "catalog.db").exists()


@pytest.mark.parametrize("command", ["inspect-curation", "build-curated-db"])
def test_report_temporary_file_cannot_overwrite_input(
    tmp_path: Path, prepared_cli, command: str
) -> None:
    cases = tmp_path / "report.json.tmp"
    (tmp_path / "cases.json").rename(cases)
    prepared_cli[5] = str(cases)
    previous = cases.read_bytes()
    assert (
        run(
            [
                *_arguments(command, prepared_cli, tmp_path),
                "--output",
                str(tmp_path / "report.json"),
            ]
        )
        == EXIT_USAGE
    )
    assert cases.read_bytes() == previous
    assert not (tmp_path / "catalog.db").exists()


def test_database_backup_cannot_replace_case_input(
    tmp_path: Path, prepared_cli
) -> None:
    database = tmp_path / "catalog.db"
    database.write_bytes(b"previous catalog")
    backup = tmp_path / "catalog.db.prev"
    (tmp_path / "cases.json").rename(backup)
    prepared_cli[5] = str(backup)
    evidence = backup.read_bytes()
    assert run(_arguments("build-curated-db", prepared_cli, tmp_path)) == EXIT_USAGE
    assert database.read_bytes() == b"previous catalog"
    assert backup.read_bytes() == evidence


@pytest.mark.parametrize("input_file", ["manifest.json", "files/records.sqlite"])
@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_database_backup_link_cannot_replace_prepared_input(
    tmp_path: Path, prepared_cli, input_file: str, link_kind: str
) -> None:
    database = tmp_path / "catalog.db"
    database.write_bytes(b"previous catalog")
    backup = tmp_path / "catalog.db.prev"
    evidence = tmp_path / "inputs" / "records" / input_file
    previous = evidence.read_bytes()
    if link_kind == "symlink":
        backup.symlink_to(evidence)
    else:
        backup.hardlink_to(evidence)
    assert run(_arguments("build-curated-db", prepared_cli, tmp_path)) == EXIT_USAGE
    assert database.read_bytes() == b"previous catalog"
    assert backup.read_bytes() == previous
    assert evidence.read_bytes() == previous


@pytest.mark.parametrize("temporary_link", [False, True])
def test_report_cannot_replace_database_backup(
    tmp_path: Path, prepared_cli, temporary_link: bool
) -> None:
    database = tmp_path / "catalog.db"
    database.write_bytes(b"previous catalog")
    backup = tmp_path / "catalog.db.prev"
    backup.write_bytes(b"older catalog")
    report = backup
    if temporary_link:
        report = tmp_path / "report.json"
        (tmp_path / "report.json.tmp").symlink_to(backup)
    assert (
        run(
            [
                *_arguments("build-curated-db", prepared_cli, tmp_path),
                "--output",
                str(report),
            ]
        )
        == EXIT_USAGE
    )
    assert database.read_bytes() == b"previous catalog"
    assert backup.read_bytes() == b"older catalog"


@pytest.mark.parametrize("command", ["inspect-curation", "build-curated-db"])
@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
@pytest.mark.parametrize(
    "input_file",
    [
        "inputs/records/manifest.json",
        "inputs/records/files/records.sqlite",
        "cases.json",
    ],
)
def test_report_temporary_link_cannot_overwrite_input(
    tmp_path: Path, prepared_cli, command: str, link_kind: str, input_file: str
) -> None:
    evidence = tmp_path / input_file
    previous = evidence.read_bytes()
    temporary = tmp_path / "report.json.tmp"
    if link_kind == "symlink":
        temporary.symlink_to(evidence)
    else:
        temporary.hardlink_to(evidence)
    assert (
        run(
            [
                *_arguments(command, prepared_cli, tmp_path),
                "--output",
                str(tmp_path / "report.json"),
            ]
        )
        == EXIT_USAGE
    )
    assert evidence.read_bytes() == previous
    assert not (tmp_path / "catalog.db").exists()


@pytest.mark.parametrize("directory_path", ["catalog.db", "catalog.db.prev"])
def test_database_destinations_cannot_be_directories(
    tmp_path: Path, prepared_cli, directory_path: str, monkeypatch
) -> None:
    from reg_meta_build import resolved_catalog

    directory = tmp_path / directory_path
    directory.mkdir()
    marker = directory / "preserved.txt"
    marker.write_bytes(b"original content")

    def no_database(*args, **kwargs):
        pytest.fail("invalid output path reached catalog writer")

    monkeypatch.setattr(resolved_catalog, "write_resolved_catalog", no_database)
    assert run(_arguments("build-curated-db", prepared_cli, tmp_path)) == EXIT_USAGE
    assert marker.read_bytes() == b"original content"
