"""build-db and check-curation CLI contract, output-path safety, determinism and failure preservation."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest
from _pipeline_catalog_support import (
    CatalogFixture,
    import_manifest as _manifest,
    report_issues as _issues,
)
from _resolved_catalog_support import resolved_variable
from catalog_manifest import synthetic_manifest
from reg_meta_build.cli import run
from reg_meta_build.db import DB_FILENAME, publish_db
from reg_meta_build.errors import EXIT_USAGE, RegMetaError
from reg_meta_build.pipeline import (
    build_catalog,
    check_curation,
)
from reg_meta_build.resolved_catalog import write_resolved_catalog

# The checkout's tracked curation tree and its slug sibling, the defaults that
# check-curation protects when --curation-dir is omitted.
CHECKOUT_CURATION = Path(__file__).resolve().parents[1] / "curation"
CHECKOUT_SLUGS = CHECKOUT_CURATION.parent / "fqid_slugs"
# One reviewed name correction per register of the two-register catalog, so a
# scoped build has curation cases to decide and dump.
ERRATA = (
    '\n[[errata.field]]\nvariable = "{variable}"\nvariant = "{variant}"\n'
    'column = "{column}"\nedition = "110"\nfield = "name"\n'
    'value = "{value}"\n'
    'expected_fields = [{{name = "name", status = "value", value = "{name}"}}, '
    '{{name = "definition", status = "value", value = "A generic family label"}}, '
    '{{name = "description", status = "absent"}}, '
    '{{name = "operational_definition", status = "absent"}}]\n'
    'expected_period_text = "2020"\n'
    'expected_scope = {{kind = "intervals", intervals = [{{start = "2020", end = "2020"}}]}}\n'
    'expected_period = {{kind = "intervals", '
    'intervals = [{{start = "2020-01-01", end = "2020-12-31"}}]}}\n'
    'evidence = "Reviewed fixture source label"\nnoted = "2026-10-01"\n'
)


def _with_errata(catalog: CatalogFixture) -> None:
    for name, native, column, label, value in (
        ("sample", "1", "VALUE", "GenericVar", "Reviewed value"),
        ("other", "2", "OTHER", "OtherVar", "Reviewed other"),
    ):
        path = catalog.curation / f"registers/scb/{name}.toml"
        path.write_text(
            path.read_text(encoding="utf-8")
            + ERRATA.format(
                variable=f"{native}.{native}01",
                variant=f"{native}.{native}0",
                column=column,
                name=label,
                value=value,
            ),
            encoding="utf-8",
        )


def _ledger(report: Path) -> list[bytes]:
    # Raw ledger lines, not `report_events`: the check-is-a-prefix-of-the-build
    # assertions compare bytes, which decoding would not.
    with gzip.open(report / "events.jsonl.gz", "rb") as stream:
        return stream.read().splitlines()


def _files(directory: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in directory.iterdir()}


def test_build_uses_tracked_tree_and_writes_no_selection_hash(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    output = tmp_path / "slice.db"
    result = catalog.build(output, tmp_path / "report", registers=("1",))
    assert result["status"] == "complete"
    assert result["publication_ready"] is False
    assert result["variables"] == result["states"] == 1
    assert result["counts"]["physical_occurrences"] == 1
    assert _issues(tmp_path / "report") == []
    manifest = _manifest(output)
    assert manifest["prepared_commit"] == catalog.commit
    assert manifest["prepared_manifest_sha256"] == catalog.digest
    assert manifest["curation_tree_sha256"] == result["curation_tree_sha256"]
    assert "curation_selection_sha256" not in manifest


def test_cli_requires_prepared_pins_and_rejects_selection(
    catalog: CatalogFixture, tmp_path: Path, capsys
) -> None:
    args = [
        "build-db",
        "--prepared",
        str(catalog.prepared),
        "--input-commit",
        catalog.commit,
        "--input-manifest-sha256",
        catalog.digest,
        "--report-dir",
        str(tmp_path / "report"),
        "--curation-dir",
        str(catalog.curation),
        "--registers",
        "1",
    ]
    assert run([*args, "--selection", "old.json"]) == EXIT_USAGE
    capsys.readouterr()
    assert run(["--db", str(tmp_path / "db-dir"), *args]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "complete"
    check_args = ["check-curation", *args[1:]]
    check_args[check_args.index(str(tmp_path / "report"))] = str(
        tmp_path / "check-report"
    )
    assert run(check_args) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "curation_check_complete"
    assert summary["passed"] is True
    assert summary["publication_ready"] is False
    assert summary["database"] is None
    assert summary["checks_not_run"]
    assert summary["curation_tree_sha256"]
    assert "counts" in summary and "acknowledged" in summary
    assert summary["prepared_commit"] == catalog.commit
    assert summary["prepared_manifest_sha256"] == catalog.digest
    assert list(tmp_path.rglob("*.db")) == [tmp_path / "db-dir" / "reg_meta.db"]
    for invalid in (
        ["--db", str(tmp_path / "not-a-db"), *check_args],
        [*check_args, "--diagnostic"],
        [*check_args, "--diagnostic-db-path", str(tmp_path / "bad.db")],
        check_args[:-2],
        [*check_args[:-1], "1,"],
    ):
        assert run(invalid) == EXIT_USAGE
        capsys.readouterr()


@pytest.mark.parametrize("command", ["build-db", "check-curation"])
def test_cli_summary_cannot_overwrite_prepared_manifest(
    catalog: CatalogFixture, tmp_path: Path, capsys, command: str
) -> None:
    catalog = catalog.private(tmp_path)
    manifest = catalog.prepared / "manifest.json"
    original = manifest.read_bytes()
    common = [
        command,
        "--prepared",
        str(catalog.prepared),
        "--input-commit",
        catalog.commit,
        "--input-manifest-sha256",
        catalog.digest,
        "--curation-dir",
        str(catalog.curation),
        "--report-dir",
        str(tmp_path / "report"),
        "--registers",
        "1",
    ]
    prefix = ["--db", str(tmp_path / "db-dir")] if command == "build-db" else []
    alias = tmp_path / "manifest-alias.json"
    alias.symlink_to(manifest)
    for destination in (manifest, alias):
        assert run(["--output", str(destination), *prefix, *common]) == EXIT_USAGE
        assert manifest.read_bytes() == original
        capsys.readouterr()
    if command == "check-curation":
        slug_dir = catalog.curation.parent / "fqid_slugs"
        slug_dir.mkdir()
        marker = slug_dir / "keep.json"
        marker.write_text("untouched")
        assert run(["--output", str(marker), *common]) == EXIT_USAGE
        slug_alias = tmp_path / "slug-alias"
        slug_alias.symlink_to(slug_dir, target_is_directory=True)
        assert run(["--output", str(slug_alias / marker.name), *common]) == EXIT_USAGE
        assert marker.read_text() == "untouched"
        capsys.readouterr()
    assert not (tmp_path / "db-dir").exists()
    assert not (tmp_path / "report").exists()


def test_diagnostic_and_strict_compile_identically(
    catalog: CatalogFixture, tmp_path: Path, capsys
) -> None:
    register = catalog.curation / "registers" / "scb" / "sample.toml"
    register.write_text(
        register.read_text(encoding="utf-8")
        + '[[variable]]\nnative_id = "1.999"\nslug = "missing"\n'
        + '[[coding.uncoded]]\nvariable = "1.999"\nvariant = "people"\n'
        + 'column = "VALUE"\nperiods = [["2020-01-01", "2020-12-31"]]\n'
        + 'reason = "Reviewed"\nsource = "fixture"\n',
        encoding="utf-8",
    )
    strict = catalog.build(
        tmp_path / "strict.db",
        tmp_path / "strict-report",
        registers=("1",),
        dump_decisions=tmp_path / "strict-decisions",
    )
    diagnostic = catalog.build(
        tmp_path / "diagnostic.db",
        tmp_path / "diagnostic-report",
        registers=("1",),
        diagnostic=True,
        dump_decisions=tmp_path / "diagnostic-decisions",
    )
    assert strict["status"] == "blocked"
    assert not (tmp_path / "strict.db").exists()
    assert diagnostic["status"] == "diagnostic_complete"
    assert diagnostic["publication_ready"] is False
    assert any(
        i["code"] == "stale_curation_entry" for i in _issues(tmp_path / "strict-report")
    )
    assert [i["code"] for i in _issues(tmp_path / "strict-report")] == [
        i["code"] for i in _issues(tmp_path / "diagnostic-report")
    ]
    assert {
        p.name: p.read_bytes() for p in (tmp_path / "strict-decisions").iterdir()
    } == {p.name: p.read_bytes() for p in (tmp_path / "diagnostic-decisions").iterdir()}
    assert (
        run(
            [
                "check-curation",
                "--prepared",
                str(catalog.prepared),
                "--input-commit",
                catalog.commit,
                "--input-manifest-sha256",
                catalog.digest,
                "--curation-dir",
                str(catalog.curation),
                "--registers",
                "1",
                "--report-dir",
                str(tmp_path / "check-report"),
                "--dump-decisions",
                str(tmp_path / "check-decisions"),
            ]
        )
        == 10
    )
    checked = json.loads(capsys.readouterr().out)
    assert checked["status"] == "curation_check_complete"
    assert checked["passed"] is False
    assert checked["counts"]["error"] > 0
    assert any(
        "coding.uncoded" in issue.get("case_id", "")
        for issue in _issues(tmp_path / "check-report")
    )
    assert [i["code"] for i in _issues(tmp_path / "check-report")] == [
        i["code"] for i in _issues(tmp_path / "diagnostic-report")
    ]
    assert {
        p.name: p.read_bytes() for p in (tmp_path / "check-decisions").iterdir()
    } == {p.name: p.read_bytes() for p in (tmp_path / "diagnostic-decisions").iterdir()}


def test_rerun_is_byte_identical(catalog: CatalogFixture, tmp_path: Path) -> None:
    first = tmp_path / "a.db"
    second = tmp_path / "b.db"
    catalog.build(
        first,
        tmp_path / "a-report",
        registers=("1",),
        dump_decisions=tmp_path / "a-decisions",
    )
    catalog.build(
        second,
        tmp_path / "b-report",
        registers=("1",),
        dump_decisions=tmp_path / "b-decisions",
    )
    assert (
        hashlib.sha256(first.read_bytes()).digest()
        == hashlib.sha256(second.read_bytes()).digest()
    )
    assert {p.name: p.read_bytes() for p in (tmp_path / "a-decisions").iterdir()} == {
        p.name: p.read_bytes() for p in (tmp_path / "b-decisions").iterdir()
    }
    first_ledger = (tmp_path / "a-report/events.jsonl.gz").read_bytes()
    assert first_ledger == (tmp_path / "b-report/events.jsonl.gz").read_bytes()
    assert first_ledger[4:8] == bytes(4)


def test_catalog_bytes_do_not_depend_on_writer_input_order(tmp_path: Path) -> None:
    # Fails if the writer stores registers, variables, states or manifest entries in
    # the order it receives them (drops a sort before an insert), so one resolved
    # catalog handed over in another order changes the artifact's bytes. A rebuild
    # replays one order, and the build cases' reversed-row steps compare projections,
    # which ignore order; this is the byte-level witness.
    variables = (resolved_variable(), resolved_variable("sos"))
    first, reordered = tmp_path / "first.db", tmp_path / "reordered.db"
    write_resolved_catalog(
        variables, first, manifest=synthetic_manifest() | {"z": "last", "a": "first"}
    )
    write_resolved_catalog(
        tuple(
            v.model_copy(update={"states": tuple(reversed(v.states))})
            for v in reversed(variables)
        ),
        reordered,
        manifest=synthetic_manifest() | {"a": "first", "z": "last"},
    )
    assert reordered.read_bytes() == first.read_bytes()


@pytest.mark.parametrize("where", ["prepared", "curation", "report", "dump"])
def test_outputs_cannot_alias_inputs_or_each_other(
    catalog: CatalogFixture, tmp_path: Path, where: str
) -> None:
    if where in {"prepared", "dump"}:
        catalog = catalog.private(tmp_path)
    output = tmp_path / "output.db"
    report = tmp_path / "report"
    kwargs = {"registers": ("1",)}
    if where == "prepared":
        output = catalog.prepared / "output.db"
    elif where == "curation":
        output = catalog.curation / "output.db"
    elif where == "report":
        output = report / "output.db"
    else:
        kwargs["dump_decisions"] = catalog.prepared
    with pytest.raises(ValueError, match="separate|new directory"):
        catalog.build(output, report, **kwargs)
    if where in {"prepared", "curation", "dump"}:
        check_report = (
            catalog.prepared / "report"
            if where == "prepared"
            else catalog.curation / "report"
            if where == "curation"
            else tmp_path / "check-report"
        )
        check_kwargs = {"dump_decisions": catalog.prepared} if where == "dump" else {}
        with pytest.raises(ValueError, match="separate|new directory"):
            catalog.check(check_report, **check_kwargs)
        if where == "curation":
            alias = tmp_path / "curation-alias"
            alias.symlink_to(catalog.curation, target_is_directory=True)
            with pytest.raises(ValueError, match="separate"):
                catalog.check(alias / "report")
    elif where == "report":
        with pytest.raises(ValueError, match="separate|new directory"):
            catalog.check(report, dump_decisions=report / "decisions")


@pytest.mark.parametrize(
    "where",
    (
        "output_inside",
        "output_dir_contains",
        "report_equal",
        "report_inside",
        "report_contains",
        "dump_inside",
    ),
)
def test_outputs_cannot_overlap_slug_tree(
    catalog: CatalogFixture, tmp_path: Path, where: str
) -> None:
    slug_dir = catalog.curation.parent / "fqid_slugs"
    slug_dir.mkdir()
    marker = slug_dir / "keep.toml"
    marker.write_bytes(b"tracked slug input")
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    output, report = outside / "catalog.db", outside / "report"
    kwargs = {"registers": ("1",)}
    if where == "output_inside":
        output = slug_dir / "catalog.db"
    elif where == "output_dir_contains":
        output = tmp_path / "catalog.db"
    elif where == "report_equal":
        report = slug_dir
    elif where == "report_inside":
        report = slug_dir / "report"
    elif where == "report_contains":
        report = tmp_path
    else:
        kwargs["dump_decisions"] = slug_dir / "decisions"
    with pytest.raises(ValueError, match="separate|new directory"):
        catalog.build(output, report, **kwargs)
    assert marker.read_bytes() == b"tracked slug input"
    assert not output.exists()
    if where.startswith("report") or where == "dump_inside":
        with pytest.raises(ValueError, match="separate|new directory"):
            catalog.check(
                report if where.startswith("report") else outside / "check-report",
                **(
                    {"dump_decisions": slug_dir / "decisions"}
                    if where == "dump_inside"
                    else {}
                ),
            )
        assert marker.read_bytes() == b"tracked slug input"


@pytest.mark.parametrize("local", [False, True])
def test_prepared_pins_are_checked(
    catalog: CatalogFixture, tmp_path: Path, local: bool
) -> None:
    with pytest.raises(ValueError, match="prepared input commit pin mismatch"):
        if local:
            check_curation(
                catalog.prepared,
                "0" * 40,
                catalog.digest,
                tmp_path / "report",
                curation_dir=catalog.curation,
                registers=("1",),
            )
        else:
            build_catalog(
                catalog.prepared,
                "0" * 40,
                catalog.digest,
                tmp_path / "bad.db",
                tmp_path / "report",
                curation_dir=catalog.curation,
                registers=("1",),
            )


def test_strict_curation_failure_preserves_previous_catalog(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    register = catalog.curation / "registers/scb/sample.toml"
    register.write_text(
        register.read_text(encoding="utf-8")
        + '[[variable]]\nnative_id = "1.999"\nslug = "missing"\n',
        encoding="utf-8",
    )
    output = tmp_path / "active.db"
    output.write_bytes(b"previous catalog")
    result = catalog.build(output, tmp_path / "report")
    assert result["status"] == "blocked"
    assert result["database"] is None
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


def test_strict_corpus_failure_preserves_previous_catalog(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    output = tmp_path / "active.db"
    output.write_bytes(b"previous catalog")
    report = tmp_path / "report"
    # A strict build validates against the corpus floors, which the synthetic
    # sources cannot meet.
    with pytest.raises(
        ValueError, match="resolved catalog validation failed.*corpus build"
    ):
        catalog.build(output, report)
    summary = json.loads((report / "summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "engineering_failure"
    assert summary["counts"].get("error", 0) == 0
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


# Publication over an existing catalog. build-db publishes only a strict full build,
# which the synthetic sources cannot pass (the corpus floors above), so these drive
# the writer that build calls: `write_resolved_catalog` stages in a temporary
# directory beside the output and installs it through `publish_db`.


def test_publication_keeps_exactly_the_replaced_generation(tmp_path: Path) -> None:
    # Fails if publication stops linking the live catalog aside to `.prev`, stops
    # evicting an older `.prev` (the link then raises FileExistsError), installs
    # something other than the new catalog, or leaves its staging behind.
    output = tmp_path / "reg_meta.db"
    previous = output.with_name("reg_meta.db.prev")
    output.write_bytes(b"gen-2")
    previous.write_bytes(b"gen-1-old")
    manifest = synthetic_manifest()
    write_resolved_catalog((resolved_variable(),), output, manifest=manifest)
    assert previous.read_bytes() == b"gen-2"
    assert _manifest(output)["prepared_commit"] == manifest["prepared_commit"]
    assert sorted(p.name for p in tmp_path.iterdir()) == [output.name, previous.name]


@pytest.mark.parametrize("step", ["link", "replace"])
def test_failed_publication_step_preserves_live_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, step: str
) -> None:
    # Y-52. Fails if publication moves the live catalog aside before its final replace
    # (rotate-then-rename leaves the live name absent), replaces it before the
    # backup link succeeded, or leaves its staging behind on failure. The failing
    # filesystem is the mocked process boundary. It fails only the publication call
    # (the `.prev` link, or the replace onto the live name), so no other step can
    # raise in its place.
    output = tmp_path / "reg_meta.db"
    output.write_bytes(b"live generation")
    prev = output.with_name("reg_meta.db.prev")
    target = (prev if step == "link" else output).resolve()
    real = getattr(os, step)

    def fail_publication(src, dst, *args, **kwargs):
        if Path(dst).resolve() == target:
            raise OSError(f"injected {step} failure")
        return real(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, step, fail_publication)
    with pytest.raises(OSError, match=f"injected {step} failure"):
        write_resolved_catalog(
            (resolved_variable(),), output, manifest=synthetic_manifest()
        )
    assert output.read_bytes() == b"live generation"
    # `.prev` is the weaker promise (publish_db's docstring): only the live catalog
    # must survive, and no staging may remain.
    assert {p.name for p in tmp_path.iterdir()} <= {output.name, prev.name}


def test_first_publication_keeps_no_previous_generation(tmp_path: Path) -> None:
    # Fails if publication links a `.prev` aside when no live catalog exists yet
    # (drops publish_db's `final_path.exists()` guard, so the link raises on the
    # absent catalog) or leaves its staging behind.
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (resolved_variable(),), output, manifest=synthetic_manifest()
    )
    assert sorted(p.name for p in tmp_path.iterdir()) == [output.name]


def test_publication_refuses_a_diagnostic_catalog(tmp_path: Path) -> None:
    # Fails if publish_db stops reading the staged catalog's manifest before it
    # installs it, so a diagnostic catalog could replace the live one. No command
    # reaches this guard: derive refuses a diagnostic base first, and extend-db and
    # the writer place a diagnostic catalog create-only, never through publish_db.
    live = tmp_path / "reg_meta.db"
    write_resolved_catalog((resolved_variable(),), live, manifest=synthetic_manifest())
    live_bytes = live.read_bytes()
    diagnostic = tmp_path / "diagnostic.db"
    write_resolved_catalog(
        (resolved_variable(),),
        diagnostic,
        manifest=synthetic_manifest(),
        diagnostic=True,
    )
    diagnostic_bytes = diagnostic.read_bytes()
    with pytest.raises(RegMetaError) as rejected:
        publish_db(diagnostic, live)
    assert rejected.value.code == "catalog_not_publishable"
    assert live.read_bytes() == live_bytes
    assert diagnostic.read_bytes() == diagnostic_bytes
    assert sorted(p.name for p in tmp_path.iterdir()) == [diagnostic.name, live.name]


@pytest.mark.parametrize("destination", ["existing", "active"])
def test_diagnostic_build_never_writes_an_existing_or_active_catalog(
    catalog: CatalogFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    destination: str,
) -> None:
    # Fails if a diagnostic build may place its catalog over an existing file (drops
    # the build's `not publishable and output.exists()` check before compiling) or at
    # the active catalog path `REG_META_DB` names (drops the writer's
    # `default_db_dir()` check), or creates anything there before refusing.
    active = tmp_path / "absent-active"
    monkeypatch.setenv("REG_META_DB", str(active))
    if destination == "existing":
        output = tmp_path / "reg_meta.db"
        output.write_bytes(b"live catalog")
        message = "separate from build inputs and each other"
    else:
        output = active / DB_FILENAME
        message = "new explicit path separate from the active catalog"
    with pytest.raises(ValueError, match=message):
        catalog.build(output, tmp_path / "report", registers=("1",), diagnostic=True)
    if destination == "existing":
        assert output.read_bytes() == b"live catalog"
        assert not output.with_name("reg_meta.db.prev").exists()
    else:
        assert not active.exists()


@pytest.mark.parametrize(
    ("catalog", "spec", "code", "message"),
    [
        (True, "1,3", "pipeline_registers_unknown", "names no selected scope: ['3']"),
        (
            "thin_twin",
            "1",
            "pipeline_registers_ambiguous",
            "qualify as SOURCE:ID: ['1']",
        ),
    ],
    indirect=["catalog"],
    ids=["unknown", "ambiguous"],
)
@pytest.mark.parametrize("command", ["build-db", "check-curation"])
def test_unresolvable_register_scope_is_refused(
    catalog: CatalogFixture,
    tmp_path: Path,
    capsys,
    command: str,
    spec: str,
    code: str,
    message: str,
) -> None:
    # Fails if an unknown --registers spec, or a bare id that SCB register 1 and the
    # FK thin register keyed 1 both expose, stops being a usage refusal with its own
    # code (e.g. the CLI rewraps it as pipeline_build_failed, or `_selected_scopes`
    # drops its ambiguity check and selects both scopes) or writes any output.
    prefix = ["--db", str(tmp_path / "db-dir")] if command == "build-db" else []
    args = [
        command,
        *prefix,
        "--prepared",
        str(catalog.prepared),
        "--input-commit",
        catalog.commit,
        "--input-manifest-sha256",
        catalog.digest,
        "--curation-dir",
        str(catalog.curation),
        "--report-dir",
        str(tmp_path / "report"),
        "--registers",
        spec,
    ]
    assert run(args) == EXIT_USAGE
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == code
    assert message in error["message"]
    assert not (tmp_path / "db-dir").exists()
    assert not (tmp_path / "report").exists()


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_local_check_matches_scoped_build_decisions_and_leading_ledger(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    _with_errata(catalog)
    built = catalog.build(
        tmp_path / "slice.db",
        tmp_path / "build-report",
        registers=("1",),
        diagnostic=True,
        dump_decisions=tmp_path / "build-decisions",
    )
    checked = catalog.check(
        tmp_path / "check-report",
        registers=("1",),
        dump_decisions=tmp_path / "check-decisions",
    )
    assert built["counts"].get("error", 0) == 0
    assert checked["passed"] is True
    assert checked["counts"]["scopes"] == 1
    assert checked["counts"]["physical_occurrences"] == 1
    assert "variables" not in checked and "states" not in checked
    assert _files(tmp_path / "check-decisions") == _files(tmp_path / "build-decisions")
    local = _ledger(tmp_path / "check-report")
    assert _ledger(tmp_path / "build-report")[: len(local)] == local


@pytest.mark.parametrize("catalog", [True], indirect=True)
@pytest.mark.parametrize("registers", [("1", "2"), ()])
def test_decision_dump_does_not_change_catalog_or_ledger(
    catalog: CatalogFixture, tmp_path: Path, registers: tuple[str, ...]
) -> None:
    _with_errata(catalog)
    kwargs = {"registers": registers, "diagnostic": not registers}
    catalog.build(
        tmp_path / "dumped.db",
        tmp_path / "dumped-report",
        dump_decisions=tmp_path / "decisions",
        **kwargs,
    )
    catalog.build(tmp_path / "plain.db", tmp_path / "plain-report", **kwargs)
    assert (tmp_path / "dumped.db").read_bytes() == (tmp_path / "plain.db").read_bytes()
    assert (tmp_path / "dumped-report/events.jsonl.gz").read_bytes() == (
        tmp_path / "plain-report/events.jsonl.gz"
    ).read_bytes()
    with sqlite3.connect(tmp_path / "plain.db") as conn:
        assert sorted(conn.execute("SELECT name FROM variable")) == [
            ("Reviewed other",),
            ("Reviewed value",),
        ]


def _check_args(catalog: CatalogFixture, report: Path) -> list[str]:
    return [
        "check-curation",
        "--prepared",
        str(catalog.prepared),
        "--input-commit",
        catalog.commit,
        "--input-manifest-sha256",
        catalog.digest,
        "--registers",
        "1",
        "--report-dir",
        str(report),
    ]


def test_check_output_cannot_replace_default_catalog(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default catalog location comes from ``REG_META_DB``."""
    db_dir = tmp_path / "active"
    db_dir.mkdir()
    active = db_dir / DB_FILENAME
    previous = active.with_name(active.name + ".prev")
    active.write_bytes(b"active catalog sentinel")
    previous.write_bytes(b"previous catalog sentinel")
    monkeypatch.setenv("REG_META_DB", str(db_dir))
    alias = tmp_path / "catalog-alias.json"
    alias.symlink_to(active)
    summary = tmp_path / "summary.json"
    # The summary is written through <output>.tmp; a hard link there is the catalog.
    summary.with_suffix(".json.tmp").hardlink_to(active)
    report = tmp_path / "report"
    args = [*_check_args(catalog, report), "--curation-dir", str(catalog.curation)]
    for destination in (active, previous, alias, summary):
        assert run(["--output", str(destination), *args]) == EXIT_USAGE
        assert active.read_bytes() == b"active catalog sentinel"
        assert previous.read_bytes() == b"previous catalog sentinel"
    assert not summary.exists()
    assert not report.exists()


@pytest.mark.parametrize("tree", ["curation", "fqid_slugs"])
def test_check_output_cannot_enter_default_curation_tree(
    catalog: CatalogFixture, tmp_path: Path, tree: str
) -> None:
    """Without ``--curation-dir`` check-curation reads the checkout's own tree."""
    root = CHECKOUT_CURATION if tree == "curation" else CHECKOUT_SLUGS
    assert root.is_dir()
    # Aim below an existing tracked file: the path lies inside the default tree,
    # but a regressed guard cannot create it, so the checkout stays clean.
    tracked = min(root.rglob("*.toml"))
    destination = tracked / "summary.json"
    report = tmp_path / "report"
    original = tracked.read_bytes()
    args = ["--output", str(destination), *_check_args(catalog, report)]
    assert run(args) == EXIT_USAGE
    assert tracked.read_bytes() == original
    assert not report.exists()
