"""Exercise the actual builder connection using a complete prepared fixture."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3

import pytest
from _csv_fixtures import _var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from reg_meta.errors import EXIT_CONFIG, EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.pipeline import (
    PipelineSelection,
    ScopeDeclarations,
    ScopeFile,
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
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget

from reg_meta_build.fqid_slugs import SlugEntry


@pytest.fixture
def selection(tmp_path, request):
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=[
            _var_row(cvid=1001, var_id=101, colname="VALUE", data_type="")
        ],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        include=("registerinformation", "unika")
        if getattr(request, "param", True)
        else ("registerinformation",),
    )
    bundle = write_input_bundle(tmp_path / "inputs", source)
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


def test_real_build_command_writes_nonpublishable_full_selection(
    selection, tmp_path, capsys
):
    output, report = tmp_path / "diagnostic.db", tmp_path / "report"
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
    assert result["counts"]["physical_occurrences"] == 1
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
        flags = dict(conn.execute("SELECT key, value FROM import_manifest"))
        assert flags["catalog_publishable"] == "false"
        assert flags["catalog_completeness"] == "incomplete"
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        events = [json.loads(line) for line in stream]
    assert sum(e["kind"] == "source_occurrence" for e in events) == 1
    assert any(e["kind"] == "issue" and e["severity"] == "error" for e in events)
    assert json.loads((report / "summary.json").read_text()) == result


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


def test_strict_curation_failure_preserves_previous_catalog(selection, tmp_path):
    output = tmp_path / "active.db"
    output.write_bytes(b"previous catalog")
    result = build_selected_catalog(selection, output, tmp_path / "strict-report")
    assert result["status"] == "blocked"
    assert result["database"] is None
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


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
