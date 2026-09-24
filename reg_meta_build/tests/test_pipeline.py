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
from _sos_fixtures import (
    BU_SPEC_LINED,
    BU_SPEC_MEMBERS,
    BU_SPEC_WRAPPED,
    inline_value_set_register,
    write_sos_input,
)
from reg_meta.errors import EXIT_CONFIG, EXIT_OUTPUT, EXIT_USAGE
from reg_meta_build.classifications import load_seed
from reg_meta_build.cli import run
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.input_snapshot import _git, input_bundle_repository
from reg_meta_build.pipeline import (
    CodebookDeclaration,
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
)
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import NativeCoordinates

from reg_meta_build.fqid_slugs import SlugEntry

# The SOS `Värdemängd` cells the inline code-list classifier decides between: a
# newline-delimited list resolves to its members, the wrapped one does not.
_SOS_CELLS = {"sos_lined": BU_SPEC_LINED, "sos_wrapped": BU_SPEC_WRAPPED}
_SCB_SLUGS = {"register": "sample", "register_variant": "people", "variable": "value"}
_SOS_SLUGS = {"register": "kodregister", "register_variant": "vy-a", "variable": "spec"}

# The sentinel-build fixture: the classifications seed carries the canonical book
# (codes 2/3/4) with its curated sentinel; the SOS `SPEC` variable declares that
# book and observes one extra bulk/missing token (`9`).
_SENTINEL_SHORT_NAME = "INSATS"
_SENTINEL_SEED = (
    '[[classification]]\nshort_name = "INSATS"\nname = "Insats"\n'
    'valid_codes_file = "insats.csv"\n'
    'sentinel_codes = [{code = "9", meaning = "ej aktuellt"}]\n'
)
_SENTINEL_CSV = "code,label\n2,Miljo\n3,Beteende\n4,Bada\n"
_SENTINEL_CELL = f"{BU_SPEC_LINED}\n9 = ej aktuellt"


def _scope(records, *, slugs, revision, cases=()):
    """One whole-source scope declaring every native target its records name."""
    provider = records[0].subject.provider
    variables = sorted(
        {key for key in map(native_variable_key, records) if key is not None}, key=repr
    )
    # A renumbered native variable is a catalog variable of its own and needs its
    # own slug; a scope naming one variable keeps the plain fixture slug.
    variable_slugs = {
        key: slugs["variable"] if index == 0 else f"{slugs['variable']}-{index}"
        for index, key in enumerate(variables)
    }
    targets = {("variable", key) for key in variables}
    for record in records:
        for parent in record.parent_facts:
            if parent.kind in {"register", "variant"}:
                targets.add(
                    (
                        "register" if parent.kind == "register" else "register_variant",
                        native_parent_key(record.source, provider, parent),
                    )
                )
    # A scope may name several registers; each parent kind keeps the plain
    # fixture slug for its first target and takes a suffixed slug after that,
    # exactly like renumbered variables do above.
    parent_slugs = {
        (kind, key): slugs[kind] if index == 0 else f"{slugs[kind]}-{index}"
        for kind in ("register", "register_variant")
        for index, key in enumerate(
            sorted({key for k, key in targets if k == kind}, key=repr)
        )
    }

    def _target_register(kind, key):
        if kind == "register":
            return None
        marker = "variable" if kind == "variable" else "variant"
        return key[: key.index(marker)]

    def _source_id(kind, key):
        if kind == "register":
            return str(key[-1])
        return f"{_target_register(kind, key)[-1]}.{key[-1]}"

    return ScopeDeclarations(
        source=revision.dataset,
        register_key=None,
        cases=cases,
        naming=tuple(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind=kind,
                    provider=provider,
                    source_key=key,
                    register_key=_target_register(kind, key),
                    identity_revision=revision,
                ),
                naming=SlugEntry(
                    kind=kind,
                    provider=provider,
                    source_id=_source_id(kind, key),
                    slug=variable_slugs[key]
                    if kind == "variable"
                    else parent_slugs[(kind, key)],
                ),
                contributors=(),
            )
            for kind, key in sorted(targets, key=repr)
        ),
        provider_keys=tuple((key, str(key[-1])) for key in variables),
    )


def _sos_flag_cases(records):
    """The flags the SOS format has no column for, as the reviewed curation a
    real build carries. Nothing here touches the delivered `Värdemängd` cell."""
    fields = ("sensitivity", "identifier")
    occurrences = tuple(r for r in records if native_variable_key(r) is not None)
    return (
        CurationCase(
            case_id="sos-declared-flags",
            targets=capture_expectations(occurrences, fields=fields),
            decision=OccurrenceCorrectionDecision(
                reviewed=True,
                effects=tuple(
                    CheckedFieldChange(
                        ref=record_ref(record),
                        replacement=FieldExpectation(
                            name=name, status="value", value=False
                        ),
                    )
                    for record in occurrences
                    for name in fields
                ),
                reason="The synthetic workbook omits both catalog flags.",
                provenance="fixture declaration",
            ),
        ),
    )


@pytest.fixture
def structural_validation_only(monkeypatch):
    """Retain real structural validation and publication, minus the real-corpus
    floors no fixture this small can meet."""
    from reg_meta_build import resolved_catalog

    validate = resolved_catalog.validate_built_db
    monkeypatch.setattr(
        resolved_catalog,
        "validate_built_db",
        lambda path, *, corpus: validate(path, corpus=False),
    )


def _input_revision(manifest, role):
    """The revision of the one prepared input filling this role."""
    return next(e.revision for e in manifest.inputs if e.role == role and e.revision)


def _scope_file(directory, name, scope):
    """Write one scope payload and pin it exactly as the selection requires."""
    payload = gzip.compress(scope.model_dump_json().encode(), mtime=0)
    (directory / name).write_bytes(payload)
    return ScopeFile(
        source=scope.source,
        register_key=scope.register_key,
        path=name,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


@pytest.fixture
def selection(tmp_path, request):
    param = getattr(request, "param", None)
    source = tmp_path / "source"
    # SCB renumbered one delivered column: two native variables share the summary's
    # whole literal key and separate only on their declared version endpoints. Only
    # the later summary row declares an identifier, so the built flags say which
    # native variable each row reached.
    renumbered = param == "renumbered"
    lineage_warning = param == "lineage_warning"
    columnless = param == "columnless"
    sentinel = param == "sentinel"
    write_scb_input(
        source,
        registerinformation_rows=[
            _var_row(
                cvid=1001,
                var_id=101,
                colname="VALUE",
                data_type="int"
                if param
                in {
                    "typed",
                    "renumbered",
                    "lineage_warning",
                    "columnless",
                    "sentinel",
                    *_SOS_CELLS,
                }
                else "",
            ),
            *(
                [
                    _var_row(
                        cvid=1002,
                        var_id=102,
                        colname="VALUE",
                        data_type="int",
                        year="2021",
                        regver_id=111,
                    )
                ]
                if renumbered
                else []
            ),
            *(
                [
                    _var_row(
                        cvid=2001,
                        var_id=102,
                        # Quoted empty: a delivered blank Kolumnnamn (SCB
                        # states the member has no physical column). An
                        # unquoted empty field would read as undelivered.
                        colname='""',
                        varname="ColumnlessVar",
                        data_type="int",
                    ),
                    # The same variable in a second variant: resolution runs
                    # per variant, but the omission is one warning per variable.
                    _var_row(
                        cvid=2002,
                        var_id=102,
                        colname='""',
                        varname="ColumnlessVar",
                        data_type="int",
                        register=("TESTREG", 1, 11),
                    ),
                ]
                if columnless
                else []
            ),
            *(
                [
                    _var_row(
                        cvid=2001,
                        var_id=201,
                        colname="OTHCOL",
                        varname="OtherVar",
                        varsource="TESTREG",
                        data_type="int",
                        register=("OTHERREG", 2, 20),
                    )
                ]
                if lineage_warning
                else []
            ),
        ],
        timeseries_rows=[
            timeseries_row(entitet="AktuellVariabel", id1="1001", id2="404")
        ]
        if param == "source_event"
        else None,
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0",
            *(
                [
                    "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2021|2021|0|0|1"
                ]
                if renumbered
                else []
            ),
            *(
                [
                    "OTHERREG|Testregistret|Individer|Individer|OtherVar|OTHCOL|2020|2020|0|0|0"
                ]
                if lineage_warning
                else []
            ),
        ],
        include=("registerinformation", "unika", "timeseries")
        if lineage_warning
        else (
            ("registerinformation", "unika", "identifierare", "timeseries")
            if param is not False
            else ("registerinformation",)
        ),
    )
    if sentinel:
        write_sos_input(
            source,
            registers=(
                inline_value_set_register(
                    _SENTINEL_CELL,
                    external_classification=_SENTINEL_SHORT_NAME,
                ),
            ),
        )
    elif param in _SOS_CELLS:
        write_sos_input(
            source, registers=(inline_value_set_register(_SOS_CELLS[param]),)
        )
    if param == "unbound_values":
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
    if sentinel:
        class_dir = source / "classifications"
        class_dir.mkdir()
        (class_dir / "insats.csv").write_text(_SENTINEL_CSV, encoding="utf-8")
        (curation / "classifications.toml").write_text(_SENTINEL_SEED, encoding="utf-8")
    bundle = write_input_bundle(tmp_path / "inputs", source, curation_dir=curation)
    destination = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, destination)
    commit = accept_prepared(destination)
    prepared = open_prepared_catalog_sources(
        destination, input_commit=commit, expected_sha256=manifest.sha256
    )
    revision = _input_revision(manifest, "scb_records")
    records = tuple(prepared.records.iter_records(source=revision.dataset))
    directory = tmp_path / "selection"
    directory.mkdir()
    scopes = [
        _scope_file(
            directory,
            "scope.json.gz",
            _scope(records, slugs=_SCB_SLUGS, revision=revision),
        )
    ]
    if param in _SOS_CELLS or sentinel:
        sos_revision = _input_revision(manifest, "sos_workbook")
        sos_records = tuple(prepared.records.iter_records(source=sos_revision.dataset))
        scopes.append(
            _scope_file(
                directory,
                "sos-scope.json.gz",
                _scope(
                    sos_records,
                    slugs=_SOS_SLUGS,
                    revision=sos_revision,
                    cases=_sos_flag_cases(sos_records),
                ),
            )
        )
    classifications: tuple[CodebookDeclaration, ...] = ()
    if sentinel:
        descriptor = next(
            descriptor.payload_key
            for values in prepared.value_sources
            if values.manifest.revision is not None
            and values.manifest.revision.dataset == "classifications/insats.csv"
            for descriptor in values.descriptors()
        )
        # Pass the validated `load_seed()` entry through: the declaration must
        # accept the curated tuple form, not just rebuilt raw tables.
        seed_entry = next(
            entry
            for entry in load_seed(curation / "classifications.toml")
            if entry["short_name"] == _SENTINEL_SHORT_NAME
        )
        classifications = (
            CodebookDeclaration(
                source="classifications/insats.csv",
                descriptor=descriptor,
                metadata={
                    "slug": "insats",
                    "short_name": _SENTINEL_SHORT_NAME,
                    "name": "Insats",
                    "sentinel_codes": seed_entry["sentinel_codes"],
                },
            ),
        )
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
        if param == "source_event"
        else (),
        scopes=tuple(scopes),
        classifications=classifications,
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
    selection,
    tmp_path,
    monkeypatch,
    capsys,
    diagnostic,
    failure,
    structural_validation_only,
):
    from reg_meta_build import cli, pipeline

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


@pytest.mark.parametrize(
    ("selection", "resolved"),
    [("sos_lined", True), ("sos_wrapped", False)],
    indirect=["selection"],
)
def test_unresolved_inline_code_list_is_reported_and_refused_for_publication(
    selection, tmp_path, resolved, structural_validation_only
):
    # The delivered `Värdemängd` cell is the only difference between the
    # publishable build and the refused one.
    strict_db = tmp_path / "catalog.db"
    strict = build_selected_catalog(selection, strict_db, tmp_path / "report")
    diagnostic_db, diagnostic_report = tmp_path / "diagnostic.db", tmp_path / "diag"
    diagnostic = build_selected_catalog(
        selection, diagnostic_db, diagnostic_report, diagnostic=True
    )
    with gzip.open(diagnostic_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue" and event["code"] == "unresolved_member_list"
        ]
    with sqlite3.connect(diagnostic_db) as conn:
        codes = conn.execute(
            "SELECT code, label FROM value_code ORDER BY code"
        ).fetchall()
        variables = conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0]

    # The diagnostic scan completes either way; the wrapped cell states no
    # membership but is reported against its own source record, so a list the
    # format cannot separate never reads as an absent coding declaration.
    assert diagnostic["status"] == "diagnostic_complete"
    assert variables == 2
    if resolved:
        assert diagnostic["counts"].get("error", 0) == 0
        assert strict["status"] == "complete"
        assert strict["publication_ready"] is True
        assert strict_db.exists()
        assert not issues
        assert codes == BU_SPEC_MEMBERS
    else:
        # The delivered cell is the fixture's only defect.
        assert diagnostic["counts"]["error"] == 1
        assert strict["status"] == "blocked"
        assert strict["publication_ready"] is False
        assert strict["database"] is None
        assert not strict_db.exists()
        (issue,) = issues
        assert issue["severity"] == "error"
        assert issue["fields"] == ["coding"]
        assert issue["withheld_output"] == ["state.value_set"]
        assert "SPEC" in issue["subject"]
        assert "inline:" in issue["detail"]
        assert issue["refs"]
        assert codes == []


def test_malformed_selection_sentinels_are_refused():
    from reg_meta_build.pipeline import _selection_sentinels

    assert _selection_sentinels(None, subject="insats") == ()
    assert (
        _selection_sentinels(
            [{"code": "9", "meaning": "ej aktuellt"}], subject="insats"
        )[0].code
        == "9"
    )
    with pytest.raises(ValueError, match="more than once"):
        _selection_sentinels(
            [
                {"code": "9", "meaning": "a"},
                {"code": "9", "meaning": "b"},
            ],
            subject="insats",
        )
    with pytest.raises(ValueError, match="sentinel_codes"):
        _selection_sentinels([{"code": 9, "meaning": "ej aktuellt"}], subject="insats")


@pytest.mark.parametrize("selection", ["sentinel"], indirect=True)
def test_curated_sentinel_keeps_binding_in_strict_build(
    selection, tmp_path, structural_validation_only
):
    # The SOS `SPEC` variable declares the `INSATS` book and observes one extra
    # bulk token (`9`) the selection's `sentinel_codes` names. The binding is
    # kept with a warning, so the strict build stays publication_ready with no
    # errors; the token stays variable-local (value set, never canonical).
    strict_db, strict_report = tmp_path / "catalog.db", tmp_path / "strict-report"
    strict = build_selected_catalog(selection, strict_db, strict_report)
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    summary = json.loads((strict_report / "summary.json").read_text())
    assert summary["counts"].get("error", 0) == 0
    assert summary["counts"]["warning"] >= 1
    with gzip.open(strict_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue"
        ]
    assert not [issue for issue in issues if issue["severity"] == "error"]
    (warning,) = [
        issue for issue in issues if issue["code"] == "sentinel_classification_codes"
    ]
    assert warning["severity"] == "warning"
    assert warning["withheld_output"] == []
    assert warning["fields"] == ["coding", "classification"]
    assert "'9'" in warning["detail"] and "ej aktuellt" in warning["detail"]
    with sqlite3.connect(strict_db) as conn:
        (slug,) = conn.execute(
            "SELECT c.slug FROM variable v "
            "JOIN variable_state vs USING(variable_id) "
            "JOIN classification c ON vs.classification_id = c.id "
            "WHERE v.slug = 'spec'"
        ).fetchone()
        assert slug == "insats"
        members = conn.execute(
            "SELECT vc.code FROM variable v "
            "JOIN variable_state vs USING(variable_id) "
            "JOIN value_set_member vsm USING(value_set_id) "
            "JOIN value_code vc USING(code_id) "
            "WHERE v.slug = 'spec' ORDER BY vc.code"
        ).fetchall()
        assert [code for (code,) in members] == ["2", "3", "4", "9"]
        (sentinel_canonical,) = conn.execute(
            "SELECT COUNT(*) FROM classification_code cc "
            "JOIN classification c ON cc.classification_id = c.id "
            "JOIN value_code vc USING(code_id) "
            "WHERE c.slug = 'insats' AND vc.code = '9'"
        ).fetchone()
        assert sentinel_canonical == 0
        assert conn.execute(
            "SELECT status, checked_code_count, matched_code_count, "
            "nonconforming_code_count FROM classification_conformance"
        ).fetchall() == [("kept", 4, 4, 0)]


@pytest.mark.parametrize("selection", ["lineage_warning"], indirect=True)
def test_lineage_warning_withholds_only_the_edge(
    selection, tmp_path, structural_validation_only
):
    # One consumer state names a second fixture register with no accepted
    # same_as edge, so the only diagnostic is unresolved_lineage_no_source_state.
    # As a warning it must not count toward counts["error"] and must leave
    # the strict build complete and publishable.
    strict_db, strict_report = tmp_path / "catalog.db", tmp_path / "strict-report"
    strict = build_selected_catalog(selection, strict_db, strict_report)
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    strict_summary = json.loads((strict_report / "summary.json").read_text())
    assert strict_summary["status"] == "complete"
    assert strict_summary["publication_ready"] is True
    assert strict_summary["counts"].get("error", 0) == 0
    assert strict_summary["counts"]["warning"] >= 1
    diagnostic_db, diagnostic_report = (
        tmp_path / "diagnostic.db",
        tmp_path / "diagnostic-report",
    )
    diagnostic = build_selected_catalog(
        selection, diagnostic_db, diagnostic_report, diagnostic=True
    )
    assert diagnostic["status"] == "diagnostic_complete"
    diagnostic_summary = json.loads((diagnostic_report / "summary.json").read_text())
    assert diagnostic_summary["counts"]["warning"] >= 1
    assert diagnostic_summary["counts"].get("error", 0) == 0
    with gzip.open(diagnostic_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue"
        ]
    (issue,) = issues
    assert issue["code"] == "unresolved_lineage_no_source_state"
    assert issue["severity"] == "warning"
    assert issue["withheld_output"] == [issue["subject"] + ":lineage"]


@pytest.mark.parametrize("selection", ["columnless"], indirect=True)
def test_columnless_variable_is_omitted_as_an_explained_warning(
    selection, tmp_path, structural_validation_only
):
    # A delivered blank Kolumnnamn states the member has no physical column, so
    # the variable stays out of the catalog. The omission is a warning, so the
    # strict build still completes as publication_ready with no errors.
    strict_db, strict_report = tmp_path / "catalog.db", tmp_path / "strict-report"
    strict = build_selected_catalog(selection, strict_db, strict_report)
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    strict_summary = json.loads((strict_report / "summary.json").read_text())
    assert strict_summary["status"] == "complete"
    assert strict_summary["counts"].get("error", 0) == 0
    assert strict_summary["counts"]["warning"] >= 1
    with sqlite3.connect(strict_db) as conn:
        assert conn.execute("SELECT provider_key FROM variable").fetchall() == [
            ("101",)
        ]
    diagnostic_report = tmp_path / "diagnostic-report"
    diagnostic = build_selected_catalog(
        selection, tmp_path / "diagnostic.db", diagnostic_report, diagnostic=True
    )
    assert diagnostic["status"] == "diagnostic_complete"
    assert diagnostic["counts"].get("error", 0) == 0
    with gzip.open(diagnostic_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue"
        ]
    assert not [issue for issue in issues if issue["severity"] == "error"]
    (omitted,) = [
        issue for issue in issues if issue["code"] == "omitted_columnless_occurrence"
    ]
    assert omitted["severity"] == "warning"
    assert omitted["fields"] == ["column_name"]
    assert omitted["withheld_output"] == ["occurrence"]
    assert len(omitted["refs"]) == 2
    assert any(
        issue["code"] == "no_supported_states" and issue["severity"] == "warning"
        for issue in issues
    )


@pytest.mark.parametrize("selection", ["renumbered"], indirect=True)
def test_renumbered_variables_each_take_their_own_summary_flags(
    selection, tmp_path, structural_validation_only
):
    # Both summary rows carry the same four literal names; only their declared
    # version endpoints say which renumbered variable each one describes.
    output = tmp_path / "catalog.db"
    strict = build_selected_catalog(selection, output, tmp_path / "report")
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    with sqlite3.connect(output) as conn:
        assert conn.execute(
            "SELECT provider_key, is_sensitive, is_identifier FROM variable "
            "ORDER BY provider_key"
        ).fetchall() == [("101", 0, 0), ("102", 0, 1)]


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
        written = _scope_file(
            selection.parent,
            file.path,
            scope.model_copy(update={"unapplied_curation": (gap,)}),
        )
        selection.write_text(
            selected.model_copy(update={"scopes": (written,)}).model_dump_json()
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


def test_lost_delivery_coverage_refuses_the_build_before_any_database(
    selection, tmp_path, monkeypatch
):
    """The operator's witness: a defect inside formation truncates one supported
    2020 occurrence, nothing else reports it, and the strict build must refuse
    before any database is placed."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def truncate(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"valid_to": "2020-06-30"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", truncate)
    output, report = tmp_path / "lost.db", tmp_path / "lost-report"
    with pytest.raises(ValueError, match="delivery coverage was lost") as failure:
        build_selected_catalog(selection, output, report, diagnostic=False)
    missing = "2020-07-01..2020-12-31"
    assert missing in str(failure.value)
    assert "scb/sample/value people/VALUE" in str(failure.value)
    assert not output.exists()
    summary = json.loads((report / "summary.json").read_text())
    assert summary["status"] == "engineering_failure"
    assert missing in summary["error"]


def test_lost_delivery_coverage_diagnostic_completes_with_error_diagnostic(
    selection, tmp_path, monkeypatch, capsys
):
    """The operator's witness in diagnostic mode: the same truncation is recorded
    as an error diagnostic, and the run still completes with a nonpublishable
    database instead of failing after output is written."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def truncate(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"valid_to": "2020-06-30"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", truncate)
    output, report = tmp_path / "lost.db", tmp_path / "lost-report"
    status = run(
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
    result = json.loads(capsys.readouterr().out)
    missing = "2020-07-01..2020-12-31"
    assert status == EXIT_CONFIG
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert output.exists()
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        issues = [
            json.loads(line)
            for line in stream
            if '"unexplained_delivery_coverage_loss"' in line
        ]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert missing in issues[0]["detail"]
    assert "scb/sample/value people/VALUE" in issues[0]["detail"]
    assert json.loads((report / "summary.json").read_text()) == result


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_changed_delivery_facts_refuse_the_build_before_any_database(
    selection, tmp_path, monkeypatch
):
    """The operator's witness: a defect inside formation retypes one supported
    2020 state, nothing else reports it, and the strict build must refuse before
    any database is placed."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def retype(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"data_type": "text"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", retype)
    output, report = tmp_path / "changed.db", tmp_path / "changed-report"
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        build_selected_catalog(selection, output, report, diagnostic=False)
    assert "scb/sample/value people/VALUE" in str(failure.value)
    assert "claimed data_type=" in str(failure.value)
    assert "written 'text'" in str(failure.value)
    assert not output.exists()
    summary = json.loads((report / "summary.json").read_text())
    assert summary["status"] == "engineering_failure"
    assert "supported delivery facts changed" in summary["error"]


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_changed_delivery_facts_diagnostic_completes_with_error_diagnostic(
    selection, tmp_path, monkeypatch, capsys
):
    """The operator's witness in diagnostic mode: the same retype is recorded as
    an error diagnostic, and the run still completes with a nonpublishable
    database instead of failing after output is written."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def retype(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"data_type": "text"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", retype)
    output, report = tmp_path / "changed.db", tmp_path / "changed-report"
    status = run(
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
    result = json.loads(capsys.readouterr().out)
    assert status == EXIT_CONFIG
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert output.exists()
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        issues = [
            json.loads(line)
            for line in stream
            if '"unexplained_delivery_fact_change"' in line
        ]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert "scb/sample/value people/VALUE" in issues[0]["detail"]
    assert "claimed data_type=" in issues[0]["detail"]
    assert "written 'text'" in issues[0]["detail"]
    assert json.loads((report / "summary.json").read_text()) == result


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
