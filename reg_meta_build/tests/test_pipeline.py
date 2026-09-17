"""Exercise the actual builder connection using a complete prepared fixture."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from _csv_fixtures import _var_row, timeseries_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from reg_meta.errors import EXIT_CONFIG, EXIT_OUTPUT, EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.input_snapshot import _git, input_bundle_repository
from reg_meta_build.pipeline import (
    PipelineSelection,
    ScopeDeclarations,
    ScopeFile,
    UnappliedCuration,
    build_selected_catalog,
)
from reg_meta_build.prepared_catalog import (
    open_prepared_catalog_sources,
    prepare_catalog_sources,
)
from reg_meta_build.source_coordinates import (
    native_parent_key,
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import PeerGuard
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import NativeCoordinates

from reg_meta_build.fqid_slugs import SlugEntry


@pytest.fixture
def selection(tmp_path, request):
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=[
            _var_row(
                cvid=1001,
                var_id=101,
                colname="VALUE",
                data_type="int" if getattr(request, "param", None) == "typed" else "",
            )
        ],
        timeseries_rows=[
            timeseries_row(entitet="AktuellVariabel", id1="1001", id2="404")
        ]
        if getattr(request, "param", None) == "source_event"
        else None,
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        include=("registerinformation", "unika", "identifierare", "timeseries")
        if getattr(request, "param", True)
        else ("registerinformation",),
    )
    if getattr(request, "param", None) == "unbound_values":
        write_scb_input(
            source,
            include=("vardemangder", "valid_dates"),
            vardemangder_rows=["List|1|1|One|broken|5001"],
            valid_dates_rows=["5001|2020-01-01|2020-12-31"],
        )
    curation = tmp_path / "curation"
    curation.mkdir()
    (curation / "delivery_enrichment.generated.toml").write_text(
        '[[description]]\nregister="scb/sample"\nvariable="value"\n'
        'description="Existing description"\n'
    )
    bundle = write_input_bundle(tmp_path / "inputs", source, curation_dir=curation)
    destination = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, destination)
    commit = accept_prepared(destination)
    prepared = open_prepared_catalog_sources(
        destination, input_commit=commit, expected_sha256=manifest.sha256
    )
    revision = next(
        e.revision for e in manifest.inputs if e.record_usage == "occurrence"
    )
    (record,) = tuple(prepared.records.iter_records(source=revision.dataset))
    register = source_register_key(record)
    variable = native_variable_key(record)
    targets = {("variable", variable)}
    for parent in record.parent_facts:
        if parent.kind in {"register", "variant"}:
            targets.add(
                (
                    "register" if parent.kind == "register" else "register_variant",
                    native_parent_key(record.source, "scb", parent),
                )
            )
    declarations = tuple(
        NamingDeclaration(
            target=NativeNamingTarget(
                kind=kind,
                provider="scb",
                source_key=key,
                register_key=register if kind != "register" else None,
                identity_revision=revision,
            ),
            naming=SlugEntry(
                kind=kind,
                provider="scb",
                source_id=str(key[-1])
                if kind == "register"
                else f"{register[-1]}.{key[-1]}",
                slug={
                    "register": "sample",
                    "register_variant": "people",
                    "variable": "value",
                }[kind],
            ),
            contributors=(),
        )
        for kind, key in sorted(targets, key=repr)
    )
    directory = tmp_path / "selection"
    directory.mkdir()
    scope = ScopeDeclarations(
        source=record.source,
        register_key=None,
        naming=declarations,
        provider_keys=((variable, str(variable[-1])),),
    )
    payload = gzip.compress(scope.model_dump_json().encode(), mtime=0)
    (directory / "scope.json.gz").write_bytes(payload)
    selected = PipelineSelection(
        prepared_path=str(destination),
        prepared_commit=commit,
        prepared_sha256=manifest.sha256,
        identifier_sources=tuple(
            e.revision.dataset
            for e in manifest.inputs
            if e.revision and e.path == "Identifierare.csv"
        ),
        event_sources=tuple(
            (e.revision.dataset, revision.dataset)
            for e in manifest.inputs
            if e.revision and e.path == "Timeseries.csv"
        )
        if getattr(request, "param", None) == "source_event"
        else (),
        scopes=(
            ScopeFile(
                source=record.source,
                register_key=None,
                path="scope.json.gz",
                sha256=hashlib.sha256(payload).hexdigest(),
            ),
        ),
    )
    path = directory / "selection.json"
    path.write_text(selected.model_dump_json())
    return path


@pytest.mark.parametrize("selection", ["source_event"], indirect=True)
def test_native_source_event_is_resolved_and_missing_endpoint_is_reported(
    selection, tmp_path
):
    output, report = tmp_path / "event.db", tmp_path / "report"
    result = build_selected_catalog(
        selection, output=output, report_dir=report, diagnostic=True
    )
    assert result["status"] == "diagnostic_complete"
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        issues = [
            json.loads(line)
            for line in stream
            if '"unresolved_source_event_endpoint"' in line
        ]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert "404" in issues[0]["detail"]
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0] == 0
        )


def test_real_build_command_writes_nonpublishable_full_selection(
    selection, tmp_path, capsys, monkeypatch
):
    monkeypatch.delenv("REG_META_BUILD_TIMING", raising=False)
    output, report = tmp_path / "diagnostic.db", tmp_path / "report"
    status = run(
        [
            "build-db",
            "--selection",
            str(selection),
            "--report-dir",
            str(report),
            "--diagnostic",
            "--timing",
            "--diagnostic-db-path",
            str(output),
        ]
    )
    captured = capsys.readouterr()
    assert "[timing] pipeline: total:" in captured.err
    result = json.loads(captured.out)
    assert status == EXIT_CONFIG
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert result["corpus_validation"]["passed"] is False
    assert result["corpus_validation"]["failures"]
    assert result["counts"]["physical_occurrences"] == 1
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
        flags = dict(conn.execute("SELECT key, value FROM import_manifest"))
        assert flags["catalog_publishable"] == "false"
        assert flags["catalog_completeness"] == "incomplete"
        assert (
            conn.execute("SELECT COUNT(*) FROM identifier_semantics").fetchone()[0] == 1
        )
        assert conn.execute("SELECT COUNT(*) FROM timeseries_event").fetchone()[0] > 0
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        events = [json.loads(line) for line in stream]
    assert sum(e["kind"] == "source_occurrence" for e in events) == 1
    assert any(e["kind"] == "issue" and e["severity"] == "error" for e in events)
    assert json.loads((report / "summary.json").read_text()) == result


@pytest.mark.parametrize(
    "overrides",
    [
        ["--input-dir", "raw"],
        ["--skip-slugs"],
        ["--no-validate"],
        ["--providers", "scb"],
        ["--trace-scb-cvids", "1001"],
        ["--scb-value-prestage-cache", "cache.sqlite"],
    ],
)
def test_build_command_has_no_legacy_or_validation_bypass(overrides):
    assert (
        run(
            [
                "build-db",
                "--selection",
                "selection.json",
                "--report-dir",
                "report",
                *overrides,
            ]
        )
        == EXIT_USAGE
    )


@pytest.mark.parametrize(
    "options",
    [
        ["--diagnostic"],
        ["--diagnostic-db-path", "comparison.db"],
        ["--diagnostic", "--diagnostic-db-path", "comparison.db", "--db", "active"],
    ],
)
def test_diagnostic_command_requires_its_separate_output(options):
    assert (
        run(
            [
                "build-db",
                "--selection",
                "selection.json",
                "--report-dir",
                "report",
                *options,
            ]
        )
        == EXIT_USAGE
    )


def test_cli_summary_cannot_overwrite_selected_declarations(selection, tmp_path):
    original = selection.read_bytes()
    status = run(
        [
            "--output",
            str(selection),
            "build-db",
            "--selection",
            str(selection),
            "--report-dir",
            str(tmp_path / "report"),
        ]
    )
    assert status == EXIT_USAGE
    assert selection.read_bytes() == original


@pytest.mark.parametrize("member", ["selection", "scope", "manifest", "records"])
@pytest.mark.parametrize("link", ["hard", "symbolic"])
def test_cli_summary_temporary_alias_cannot_overwrite_inputs(
    selection, tmp_path, member, link
):
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    prepared = Path(selected.prepared_path)
    target = {
        "selection": selection,
        "scope": selection.parent / selected.scopes[0].path,
        "manifest": prepared / "manifest.json",
        "records": prepared / "files/records/files/records.sqlite",
    }[member]
    original = target.read_bytes()
    summary = tmp_path / "summary.json"
    temporary = summary.with_suffix(".json.tmp")
    if link == "hard":
        temporary.hardlink_to(target)
    else:
        temporary.symlink_to(target)
    report = tmp_path / "report"
    assert (
        run(
            [
                "--output",
                str(summary),
                "build-db",
                "--selection",
                str(selection),
                "--report-dir",
                str(report),
            ]
        )
        == EXIT_USAGE
    )
    assert not report.exists()
    assert target.read_bytes() == original


@pytest.mark.parametrize("backup", [False, True])
def test_catalog_paths_cannot_alias_prepared_inputs(selection, tmp_path, backup):
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    target = Path(selected.prepared_path) / "files/records/files/records.sqlite"
    original = target.read_bytes()
    output = tmp_path / "catalog.db"
    alias = Path(str(output) + ".prev") if backup else output
    alias.hardlink_to(target)
    with pytest.raises(ValueError, match="must not alias selected inputs"):
        build_selected_catalog(selection, output, tmp_path / "report")
    assert target.read_bytes() == original
    assert not (tmp_path / "report").exists()


def test_late_cli_summary_failure_reports_completed_artifact(
    selection, tmp_path, capsys, monkeypatch
):
    from reg_meta_build import cli

    def fail(*args, **kwargs):
        raise OSError("late summary failure")

    monkeypatch.setattr(cli, "write_json", fail)
    output, report = tmp_path / "diagnostic.db", tmp_path / "report"
    assert (
        run(
            [
                "build-db",
                "--selection",
                str(selection),
                "--report-dir",
                str(report),
                "--diagnostic",
                "--diagnostic-db-path",
                str(output),
            ]
        )
        == EXIT_OUTPUT
    )
    receipt = json.loads(capsys.readouterr().err.splitlines()[-1])["error"]
    assert receipt["artifact_complete"] is True
    assert receipt["status"] == "diagnostic_complete"
    assert receipt["publication_ready"] is False
    assert receipt["curation_exit_code"] == EXIT_CONFIG
    assert receipt["database"] == str(output)
    assert json.loads((report / "summary.json").read_text())["database"] == str(output)


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_pipeline_artifact_dates_its_pinned_preparation_and_boots_backend(
    selection, tmp_path, monkeypatch
):
    from fastapi.testclient import TestClient
    from reg_webapp.app import create_app

    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    committed = _git(
        input_bundle_repository(Path(selected.prepared_path)),
        "show",
        "-s",
        "--format=%cI",
        selected.prepared_commit,
    )
    expected = datetime.fromisoformat(committed).astimezone(UTC)
    output = tmp_path / "artifact" / "reg_meta.db"
    build_selected_catalog(selection, output, tmp_path / "report", diagnostic=True)
    monkeypatch.setenv("REG_META_DB", str(output.parent))
    monkeypatch.delenv("REG_WEBAPP_STEWARD", raising=False)
    monkeypatch.delenv("REG_WEBAPP_DELIVERY_INVENTORY", raising=False)
    with TestClient(create_app()) as client:
        response = client.get("/api/context")
    assert response.status_code == 200
    import_date = response.json()["reg_meta"]["import_date"]
    assert import_date.endswith("Z")
    assert datetime.fromisoformat(import_date) == expected


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
@pytest.mark.parametrize("diagnostic", [False, True])
@pytest.mark.parametrize("failure", ["summary", "events", "summary_and_cli"])
def test_internal_report_failure_retains_completed_artifact_receipt(
    selection, tmp_path, monkeypatch, capsys, diagnostic, failure
):
    from reg_meta_build import cli, pipeline, resolved_catalog

    # A one-variable fixture cannot meet the real-corpus floors. Retain actual
    # structural validation and publication while exercising report finalization.
    validate = resolved_catalog.validate_built_db
    monkeypatch.setattr(
        resolved_catalog,
        "validate_built_db",
        lambda path, *, corpus: validate(path, corpus=False),
    )
    output, report = tmp_path / "artifact" / "reg_meta.db", tmp_path / "report"
    if not diagnostic:
        output.parent.mkdir()
        output.write_bytes(b"previous catalog")
    original_write = Path.write_text
    original_open = gzip.open

    def fail_summary(path, *args, **kwargs):
        if path == report / "summary.json":
            raise OSError("internal summary failure")
        return original_write(path, *args, **kwargs)

    @contextmanager
    def fail_events(*args, **kwargs):
        with original_open(*args, **kwargs) as stream:
            yield stream
        if Path(args[0]) == report / "events.jsonl.gz":
            raise OSError("event close failure")

    if failure.startswith("summary"):
        monkeypatch.setattr(Path, "write_text", fail_summary)
    else:
        monkeypatch.setattr(pipeline.gzip, "open", fail_events)
    if failure == "summary_and_cli":

        def fail_cli(*args, **kwargs):
            raise OSError("CLI summary failure")

        monkeypatch.setattr(cli, "write_json", fail_cli)
    options = (
        ["--diagnostic", "--diagnostic-db-path", str(output)]
        if diagnostic
        else ["--db", str(output.parent)]
    )
    assert (
        run(
            [
                "build-db",
                "--selection",
                str(selection),
                "--report-dir",
                str(report),
                *options,
            ]
        )
        == EXIT_OUTPUT
    )
    captured = capsys.readouterr()
    receipt = json.loads(
        captured.err.splitlines()[-1] if failure == "summary_and_cli" else captured.out
    )["error"]
    assert receipt["code"] == "pipeline_report_failed"
    assert receipt["artifact_complete"] is True
    assert receipt["status"] == ("diagnostic_complete" if diagnostic else "complete")
    assert receipt["publication_ready"] is (not diagnostic)
    assert receipt["database"] == str(output)
    assert receipt["curation_exit_code"] == (EXIT_CONFIG if diagnostic else 0)
    assert receipt["report_dir"] == str(report)
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
    if not diagnostic:
        assert Path(str(output) + ".prev").read_bytes() == b"previous catalog"


@pytest.mark.parametrize("selection", ["unbound_values"], indirect=True)
def test_unbindable_value_rows_are_reported_without_a_target_occurrence(
    selection, tmp_path
):
    report = tmp_path / "report"
    result = build_selected_catalog(
        selection, tmp_path / "diagnostic.db", report, diagnostic=True
    )
    assert result["status"] == "diagnostic_complete"
    assert result["counts"]["value_associations"] == 1
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        events = [json.loads(line) for line in stream]
    (problem,) = [e for e in events if e["kind"] == "value_source_issue"]
    assert problem["code"] == "unknown_native_member_token"
    assert problem["raw_member_tokens"] == ["broken"]
    assert problem["occurrence_count"] == 1
    assert any(
        e["kind"] == "issue"
        and e["code"] == problem["code"]
        and e["severity"] == "error"
        for e in events
    )


def test_strict_curation_failure_preserves_previous_catalog(selection, tmp_path):
    output = tmp_path / "active.db"
    output.write_bytes(b"previous catalog")
    result = build_selected_catalog(selection, output, tmp_path / "strict-report")
    assert result["status"] == "blocked"
    assert result["database"] is None
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_strict_corpus_failure_preserves_previous_catalog(selection, tmp_path):
    output = tmp_path / "catalog.db"
    output.write_bytes(b"previous catalog")
    report = tmp_path / "report"
    with pytest.raises(ValueError, match="resolved catalog validation failed"):
        build_selected_catalog(selection, output, report)
    summary = json.loads((report / "summary.json").read_text())
    assert summary["status"] == "engineering_failure"
    assert summary["counts"].get("error", 0) == 0
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


def test_unapplied_existing_curation_is_an_error_and_cannot_hide_a_resolved_target(
    selection, tmp_path
):
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    prepared = open_prepared_catalog_sources(
        Path(selected.prepared_path),
        input_commit=selected.prepared_commit,
        expected_sha256=selected.prepared_sha256,
    )
    file = selected.scopes[0]
    scope = ScopeDeclarations.model_validate_json(
        gzip.decompress((selection.parent / file.path).read_bytes())
    )
    records = tuple(prepared.records.iter_records(source=scope.source))
    expected = capture_expectations(records, fields=("column_name",))
    gap = UnappliedCuration(
        revision=next(
            e.revision
            for e in prepared.manifest.inputs
            if e.role == "curation" and e.revision is not None
        ),
        pointer="/description/0",
        reason="Existing input decision has unresolved target evidence.",
        targets=expected,
        peer_guards=(
            PeerGuard(
                guard_id="gap",
                source=scope.source,
                native=NativeCoordinates(variable_id=101),
                expected_members=tuple(e.ref for e in expected),
            ),
        ),
    )

    def save(gap):
        payload = gzip.compress(
            scope.model_copy(update={"unapplied_curation": (gap,)})
            .model_dump_json()
            .encode(),
            mtime=0,
        )
        (selection.parent / file.path).write_bytes(payload)
        selection.write_text(
            selected.model_copy(
                update={
                    "scopes": (
                        file.model_copy(
                            update={"sha256": hashlib.sha256(payload).hexdigest()}
                        ),
                    )
                }
            ).model_dump_json()
        )

    save(gap)
    report = tmp_path / "gap-report"
    result = build_selected_catalog(
        selection, tmp_path / "gap.db", report, diagnostic=True
    )
    assert result["status"] == "diagnostic_complete"
    assert result["counts"]["unapplied_curation"] == 1
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        assert any(
            json.loads(line).get("code") == "unapplied_existing_curation"
            for line in stream
        )
    save(gap.model_copy(update={"missing_variable": "scb/sample/value"}))
    with pytest.raises(ValueError, match="finish its conversion"):
        build_selected_catalog(
            selection, tmp_path / "bad-gap.db", tmp_path / "bad-gap", diagnostic=True
        )


def test_diagnostic_database_is_deterministic_and_create_only(selection, tmp_path):
    first, second = tmp_path / "one.db", tmp_path / "two.db"
    build_selected_catalog(selection, first, tmp_path / "one", diagnostic=True)
    build_selected_catalog(selection, second, tmp_path / "two", diagnostic=True)
    assert first.read_bytes() == second.read_bytes()
    with pytest.raises(ValueError, match="separate"):
        build_selected_catalog(selection, first, tmp_path / "retry", diagnostic=True)


@pytest.mark.parametrize("selection", [False], indirect=True)
def test_all_variables_withheld_remains_a_curation_failure(selection, tmp_path):
    output = tmp_path / "all-withheld.db"
    strict = build_selected_catalog(selection, output, tmp_path / "strict")
    assert strict["status"] == "blocked"
    assert not output.exists()
    diagnostic = build_selected_catalog(
        selection, output, tmp_path / "diagnostic", diagnostic=True
    )
    assert diagnostic["status"] == "diagnostic_complete"
    assert diagnostic["variables"] == 0
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM register").fetchone()[0] == 1
        assert (
            conn.execute(
                "SELECT value FROM import_manifest WHERE key='catalog_publishable'"
            ).fetchone()[0]
            == "false"
        )


@pytest.mark.parametrize("failure", ["unconverted", "scope", "hash", "escape"])
def test_engineering_failures_never_become_diagnostic_waivers(
    selection, tmp_path, failure
):
    raw = json.loads(selection.read_bytes())
    if failure == "unconverted":
        raw["unconverted"] = ["Existing relation requires conversion"]
    elif failure == "scope":
        raw["scopes"] = []
    elif failure == "hash":
        raw["scopes"][0]["sha256"] = "b" * 64
    else:
        raw["scopes"][0]["path"] = "../scope.json.gz"
    selection.write_text(json.dumps(raw))
    output = tmp_path / "invalid.db"
    with pytest.raises(ValueError):
        build_selected_catalog(
            selection, output, tmp_path / "invalid-report", diagnostic=True
        )
    assert not output.exists()
