"""Exercise build-db from accepted prepared sources and tracked curation."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import weakref
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from _csv_fixtures import _var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from _sos_fixtures import DEFAULT_REGISTERS, write_sos_input
from reg_meta.documentary import SourceCodeCrosswalkDeclaration
from reg_meta.errors import EXIT_USAGE
from reg_meta.source_evidence import RecordLocator, SourceRevision
from reg_meta_build.catalog_dependencies import CatalogDependencyError
from reg_meta_build.cli import run
from reg_meta_build.curation_tree import (
    ClassificationFamilies,
    ClassificationFamily,
    ClassificationMetadata,
    CuratedClassification,
)
from reg_meta_build.pipeline import (
    _classification_references,
    build_catalog,
    check_curation,
)
from reg_meta_build.prepared_catalog import (
    PreparedCatalogSources,
    ReferenceEvidence,
    prepare_catalog_sources,
)
from reg_meta_build.prepared_sources import PreparedSourceRecords
from reg_meta_build.resolved_catalog import ResolvedCodeSet
from reg_meta_build.source_naming import authored_naming_id
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.curation_tree import CurationTree


@dataclass(frozen=True)
class CatalogFixture:
    prepared: Path
    commit: str
    digest: str
    curation: Path

    def build(self, output: Path, report: Path, **kwargs):
        return build_catalog(
            self.prepared,
            self.commit,
            self.digest,
            output,
            report,
            curation_dir=self.curation,
            **kwargs,
        )

    def check(self, report: Path, **kwargs):
        registers = kwargs.pop("registers", ("1",))
        return check_curation(
            self.prepared,
            self.commit,
            self.digest,
            report,
            curation_dir=self.curation,
            registers=registers,
            **kwargs,
        )


@pytest.fixture
def catalog(tmp_path: Path, request) -> CatalogFixture:
    mode = getattr(request, "param", False)
    second = mode is True or mode == "unknown_support"
    thin = mode in {"thin", "thin_two"}
    source = tmp_path / "source"
    records = [_var_row(cvid=1001, var_id=101, colname="VALUE", data_type="int")]
    summaries = [
        "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
    ]
    if second:
        records.append(
            _var_row(
                cvid=2001,
                var_id=201,
                colname="OTHER",
                varname="OtherVar",
                data_type="int",
                register=("OTHERREG", 2, 20),
            )
        )
        summaries.append(
            "OTHERREG|Testregistret|Individer|Individer|OtherVar|OTHER|2020|2020|0|0|0"
        )
    if mode == "unknown_support":
        summaries = [
            "|".join((*row.split("|")[:5], "", *row.split("|")[6:]))
            for row in summaries
        ]
    write_scb_input(
        source,
        registerinformation_rows=records,
        unika_rows=summaries,
        include=("registerinformation", "unika"),
    )
    if thin:
        thin_source = source / "Forsakringskassan"
        thin_source.mkdir()
        (thin_source / "fk.toml").write_text(
            '[[register]]\nkey = "remote"\nname = "Remote"\n'
            'valid_from = "2020-01-01"\nvalid_to = "2020-12-31"\n'
            '[[register.variable]]\nname = "Amount"\ncolumn = "AMOUNT"\n'
            'data_type = "int"\n'
            + (
                '[[register]]\nkey = "aktivitetsstod"\nname = "Aktivitetsstöd"\n'
                'valid_from = "2020-01-01"\nvalid_to = "2020-12-31"\n'
                '[[register.variable]]\nname = "Benefit"\ncolumn = "BENEFIT"\n'
                'data_type = "int"\n'
                if mode == "thin_two"
                else ""
            ),
            encoding="utf-8",
        )
    if mode == "sos_whole":
        from openpyxl import load_workbook

        sos_dir = write_sos_input(source, registers=DEFAULT_REGISTERS[1:2])
        workbook_path = next(sos_dir.glob("*.xlsx"))
        workbook = load_workbook(workbook_path)
        workbook["Generell information"]["C4"] = None
        workbook.save(workbook_path)
        workbook.close()
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    commit = accept_prepared(prepared)
    curation = tmp_path / "curation"
    registers = curation / "registers" / "scb"
    registers.mkdir(parents=True)
    (curation / "classifications").mkdir()
    (registers / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )
    if second:
        (registers / "other.toml").write_text(
            '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "2"\n'
            '[[variant]]\nnative_id = "2.20"\nslug = "people"\n'
            '[[variable]]\nnative_id = "2.201"\nslug = "value"\n',
            encoding="utf-8",
        )
    if thin:
        thin_curation = curation / "registers" / "fk"
        thin_curation.mkdir()
        register_id = authored_naming_id(
            "register", provider="fk", register_key="remote"
        )
        variant_id = authored_naming_id(
            "register_variant",
            provider="fk",
            register_key="remote",
            member_key="_default",
        )
        variable_id = authored_naming_id(
            "variable", provider="fk", register_key="remote", member_key="AMOUNT"
        )
        (thin_curation / "remote.toml").write_text(
            '[register]\nprovider = "fk"\nslug = "remote"\n'
            f'native_id = "{register_id}"\n'
            '[[variant]]\nslug = "default"\n'
            f'native_id = "{variant_id}"\n'
            '[[variable]]\nslug = "amount"\n'
            f'native_id = "{variable_id}"\n',
            encoding="utf-8",
        )
        if mode == "thin_two":
            register_id = authored_naming_id(
                "register", provider="fk", register_key="aktivitetsstod"
            )
            variant_id = authored_naming_id(
                "register_variant",
                provider="fk",
                register_key="aktivitetsstod",
                member_key="_default",
            )
            variable_id = authored_naming_id(
                "variable",
                provider="fk",
                register_key="aktivitetsstod",
                member_key="BENEFIT",
            )
            (thin_curation / "aktivitetsstod.toml").write_text(
                '[register]\nprovider = "fk"\nslug = "aktivitetsstod"\n'
                f'native_id = "{register_id}"\n'
                '[[variant]]\nslug = "default"\n'
                f'native_id = "{variant_id}"\n'
                '[[variable]]\nslug = "benefit"\n'
                f'native_id = "{variable_id}"\n',
                encoding="utf-8",
            )
    return CatalogFixture(prepared, commit, manifest.sha256, curation)


def _issues(report: Path) -> list[dict]:
    with gzip.open(report / "events.jsonl.gz", "rt", encoding="utf-8") as stream:
        return [row for line in stream if (row := json.loads(line))["kind"] == "issue"]


def _manifest(path: Path) -> dict[str, str]:
    with sqlite3.connect(path) as conn:
        return dict(conn.execute("SELECT key, value FROM import_manifest"))


def test_exact_classification_aliases_and_family_references() -> None:
    def book(
        name: str, slug: str, aliases: tuple[str, ...] = ()
    ) -> CuratedClassification:
        return CuratedClassification(
            classification=ClassificationMetadata(
                short_name=name,
                slug=slug,
                name=name,
                codes_file=f"{slug}.csv",
                aliases=aliases,
            )
        )

    first = book("A", "a", ("Source A",))
    second = book("B", "b")

    def references(
        books: tuple[CuratedClassification, ...], alias: str
    ) -> tuple[dict[str, str], dict[str, tuple[str, ...]]]:
        tree = cast(
            "CurationTree",
            SimpleNamespace(
                classifications=books,
                classification_families=ClassificationFamilies(
                    family=(
                        ClassificationFamily(
                            key="pair", members=("a", "b"), aliases=(alias,)
                        ),
                    )
                ),
            ),
        )
        return _classification_references(tree)

    assert references((first, second), "Source pair") == (
        {"A": "a", "Source A": "a", "B": "b"},
        {"Source pair": ("a", "b")},
    )
    with pytest.raises(ValueError, match="duplicate classification reference"):
        references((first, book("B", "b", ("Source A",))), "Source pair")
    with pytest.raises(
        ValueError, match="duplicate classification reference and family alias"
    ):
        references((first, second), "A")


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


def test_check_cli_output_cannot_replace_default_catalog(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta.db import DB_FILENAME

    from reg_meta_build import cli

    db_dir = tmp_path / "active"
    db_dir.mkdir()
    active = db_dir / DB_FILENAME
    previous = active.with_name(active.name + ".prev")
    active.write_bytes(b"active catalog sentinel")
    previous.write_bytes(b"previous catalog sentinel")
    monkeypatch.setattr(cli, "default_db_dir", lambda: db_dir)
    alias = tmp_path / "catalog-alias.json"
    alias.symlink_to(active)
    summary = tmp_path / "summary.json"
    temporary = summary.with_suffix(".json.tmp")
    temporary.hardlink_to(active)
    args = [
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
        str(tmp_path / "report"),
    ]
    for destination in (active, previous, alias, summary):
        assert run(["--output", str(destination), *args]) == EXIT_USAGE
        assert active.read_bytes() == b"active catalog sentinel"
        assert previous.read_bytes() == b"previous catalog sentinel"
    assert temporary.read_bytes() == b"active catalog sentinel"
    assert not summary.exists()
    assert not (tmp_path / "report").exists()


def test_check_cli_output_protects_default_curation_tree(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import _curation

    monkeypatch.setattr(_curation, "repo_curation_dir", lambda: catalog.curation)
    curated = catalog.curation / "registers/scb/sample.toml"
    slug_dir = catalog.curation.parent / "fqid_slugs"
    slug_dir.mkdir()
    slug = slug_dir / "keep.toml"
    slug.write_bytes(b"slug sentinel")
    original = curated.read_bytes()
    args = [
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
        str(tmp_path / "report"),
    ]
    for destination in (curated, slug):
        assert run(["--output", str(destination), *args]) == EXIT_USAGE
    assert curated.read_bytes() == original
    assert slug.read_bytes() == b"slug sentinel"
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


@pytest.mark.parametrize("catalog", [True], indirect=True)
@pytest.mark.parametrize("dump", [False, True])
def test_completed_scope_contracts_are_released(
    catalog: CatalogFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dump: bool,
) -> None:
    from reg_meta_build import pipeline

    register = catalog.curation / "registers/scb/sample.toml"
    register.write_text(
        register.read_text(encoding="utf-8")
        + '\n[[errata.field]]\nvariable = "1.101"\nvariant = "1.10"\n'
        'column = "VALUE"\nedition = "110"\nfield = "name"\n'
        'value = "Reviewed value"\n'
        'expected_fields = [{name = "name", status = "value", value = "GenericVar"}, '
        '{name = "definition", status = "value", value = "A generic family label"}, '
        '{name = "description", status = "absent"}, '
        '{name = "operational_definition", status = "absent"}]\n'
        'expected_period_text = "2020"\n'
        'expected_scope = {kind = "intervals", intervals = [{start = "2020", end = "2020"}]}\n'
        'expected_period = {kind = "intervals", '
        'intervals = [{start = "2020-01-01", end = "2020-12-31"}]}\n'
        'evidence = "Reviewed fixture source label"\nnoted = "2026-10-01"\n',
        encoding="utf-8",
    )
    other = catalog.curation / "registers/scb/other.toml"
    correction = register.read_text(encoding="utf-8").split("\n[[errata.field]]", 1)[1]
    other.write_text(
        other.read_text(encoding="utf-8")
        + "\n[[errata.field]]"
        + correction.replace('"1.101"', '"2.201"')
        .replace('"1.10"', '"2.20"')
        .replace('"VALUE"', '"OTHER"')
        .replace('"GenericVar"', '"OtherVar"')
        .replace('"Reviewed value"', '"Reviewed other"'),
        encoding="utf-8",
    )
    catalog.build(
        tmp_path / "reference.db",
        tmp_path / "reference-report",
        registers=("1", "2"),
        dump_decisions=tmp_path / "reference-decisions",
    )
    compile_tree = pipeline.compile_curation
    compile_scope = pipeline._compiled_scope
    finalize = pipeline.finalize_classification_bindings
    scopes = []
    scope_keys = []
    guarded_cases = []
    raw_scope_keys = []

    def observed_compile(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        cases = [case for group in compiled.cases.values() for case in group]
        assert len(cases) == 2
        assert all(case.targets and case.peer_guards for case in cases)
        assert all(case.decision.kind == "correct_occurrences" for case in cases)
        guarded_cases.extend(case.case_id for case in cases)
        raw_scope_keys[:] = [
            set(mapping or {})
            for mapping in (
                compiled.cases,
                compiled.source_diagnostics,
                compiled.naming,
                compiled.naming_ambiguities,
                compiled.provider_keys,
                compiled.variants,
            )
        ]
        return compiled

    def observe_scope(key, compiled):
        for mapping, original_keys in zip(
            (
                compiled.cases,
                compiled.source_diagnostics,
                compiled.naming,
                compiled.naming_ambiguities,
                compiled.provider_keys,
                compiled.variants,
            ),
            raw_scope_keys,
            strict=True,
        ):
            if mapping is not None:
                assert all(
                    (prior in mapping) == (dump and prior in original_keys)
                    for prior in scope_keys
                )
        scope = compile_scope(key, compiled)
        scopes.append(weakref.ref(scope))
        scope_keys.append(key)
        return scope

    def observed_finalize(compiled, *args, **kwargs):
        assert len(scopes) == 2
        assert all(ref() is None for ref in scopes)
        assert not compiled.fields
        cases = [case for group in compiled.cases.values() for case in group]
        assert [case.case_id for case in cases] == (guarded_cases if dump else [])
        return finalize(compiled, *args, **kwargs)

    monkeypatch.setattr(pipeline, "compile_curation", observed_compile)
    monkeypatch.setattr(pipeline, "_compiled_scope", observe_scope)
    monkeypatch.setattr(pipeline, "finalize_classification_bindings", observed_finalize)
    catalog.build(
        tmp_path / "released.db",
        tmp_path / "report",
        registers=("1", "2"),
        dump_decisions=tmp_path / "decisions" if dump else None,
    )
    with sqlite3.connect(tmp_path / "released.db") as conn:
        assert conn.execute(
            "SELECT name FROM variable WHERE slug = 'value' AND name = 'Reviewed value'"
        ).fetchone() == ("Reviewed value",)
    assert (tmp_path / "reference.db").read_bytes() == (
        tmp_path / "released.db"
    ).read_bytes()
    assert (tmp_path / "reference-report/events.jsonl.gz").read_bytes() == (
        tmp_path / "report/events.jsonl.gz"
    ).read_bytes()
    if dump:
        assert {p.name: p.read_bytes() for p in (tmp_path / "decisions").iterdir()} == {
            p.name: p.read_bytes() for p in (tmp_path / "reference-decisions").iterdir()
        }

    def invalid_compile(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        key, cases = next(
            (key, cases) for key, cases in compiled.cases.items() if cases
        )
        invalid = cases[0].model_copy(update={"targets": ()})
        return replace(compiled, cases={**compiled.cases, key: (invalid, *cases[1:])})

    scope_keys.clear()
    monkeypatch.setattr(pipeline, "compile_curation", invalid_compile)
    with pytest.raises(ValueError, match="curation case needs exact targets"):
        catalog.build(
            tmp_path / "invalid.db",
            tmp_path / "invalid-report",
            registers=("1", "2"),
            dump_decisions=tmp_path / "invalid-decisions" if dump else None,
        )
    assert not (tmp_path / "invalid.db").exists()


@pytest.mark.parametrize("catalog", [True], indirect=True)
@pytest.mark.parametrize("cache_limit", [2, 8192])
def test_decoded_record_eviction_preserves_build_outputs(
    catalog: CatalogFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cache_limit: int,
) -> None:
    from reg_meta_build import prepared_sources

    (catalog.curation / "relations.toml").write_text(
        '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\n'
        'b = "scb/other/value"\n'
        '[[edge]]\ntype = "replaced_by"\nfrom = "scb/sample/value"\n'
        'to = "scb/other/value"\neffective_year = 2021\n',
        encoding="utf-8",
    )
    clear = PreparedSourceRecords.clear_decoded_records
    monkeypatch.setattr(
        PreparedSourceRecords, "clear_decoded_records", lambda self: None
    )
    catalog.build(
        tmp_path / "retained.db",
        tmp_path / "retained-report",
        registers=("1", "2"),
        dump_decisions=tmp_path / "retained-decisions",
    )
    sizes: list[int] = []

    def observed_clear(self: PreparedSourceRecords) -> None:
        sizes.append(len(self._record_cache))
        clear(self)
        assert not self._record_cache

    monkeypatch.setattr(PreparedSourceRecords, "clear_decoded_records", observed_clear)
    monkeypatch.setattr(prepared_sources, "_DECODED_RECORD_CACHE_LIMIT", cache_limit)
    catalog.build(
        tmp_path / "released.db",
        tmp_path / "released-report",
        registers=("1", "2"),
        dump_decisions=tmp_path / "released-decisions",
    )
    assert len(sizes) >= 3  # Compilation and both completed register scopes.
    with sqlite3.connect(tmp_path / "released.db") as conn:
        for table, count in (("variable_same_as", 2), ("variable_replaced_by", 1)):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (count,)
    assert (tmp_path / "retained.db").read_bytes() == (
        tmp_path / "released.db"
    ).read_bytes()
    assert (tmp_path / "retained-report/events.jsonl.gz").read_bytes() == (
        tmp_path / "released-report/events.jsonl.gz"
    ).read_bytes()
    assert {
        p.name: p.read_bytes() for p in (tmp_path / "retained-decisions").iterdir()
    } == {p.name: p.read_bytes() for p in (tmp_path / "released-decisions").iterdir()}


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_subset_resolves_only_named_scope(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build.source_support import SourceSupportBindings

    observe = SourceSupportBindings.observe_target
    targets = []

    def record_target(self, target):
        targets.append(target)
        return observe(self, target)

    monkeypatch.setattr(SourceSupportBindings, "observe_target", record_target)
    result = catalog.build(tmp_path / "slice.db", tmp_path / "report", registers=("1",))
    full_support_count = len(targets)
    assert result["counts"]["physical_occurrences"] == 1
    assert result["variables"] == 1
    with sqlite3.connect(tmp_path / "slice.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM register").fetchone() == (1,)
    local = catalog.check(tmp_path / "local-report")
    assert len(targets) == 2 * full_support_count
    assert full_support_count >= 2
    assert local["counts"]["physical_occurrences"] == 1
    assert local["counts"]["scopes"] == 1
    assert "variables" not in local and "states" not in local
    assert "skipped_curation" not in local["counts"]
    with pytest.raises(ValueError, match="names no selected scope"):
        catalog.build(tmp_path / "bad.db", tmp_path / "bad-report", registers=("3",))
    with pytest.raises(ValueError, match="names no selected scope"):
        catalog.check(tmp_path / "bad-check-report", registers=("3",))


@pytest.mark.parametrize("catalog", ["thin_two"], indirect=True)
def test_thin_source_has_register_scopes_selected_by_name_or_source_id(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    from reg_meta_build.prepared_catalog import open_prepared_catalog_sources

    prepared = open_prepared_catalog_sources(
        catalog.prepared, expected_sha256=catalog.digest, input_commit=catalog.commit
    )
    source = "Forsakringskassan/fk.toml"
    coordinates = prepared.records.register_coordinates(source)
    assert {register[-1] for register, _, _ in coordinates if register} == {
        "remote",
        "aktivitetsstod",
    }
    assert len(coordinates) == 2
    for index, selector in enumerate(("aktivitetsstod", f"{source}:aktivitetsstod")):
        output, report = tmp_path / f"thin-{index}.db", tmp_path / f"thin-{index}"
        result = catalog.build(output, report, registers=(selector,), diagnostic=True)
        assert result["status"] == "diagnostic_complete"
        assert result["counts"]["scopes"] == 1
        assert result["counts"]["physical_occurrences"] == 2
        assert result["variables"] == 1
    full = catalog.build(
        tmp_path / "full.db", tmp_path / "full-report", diagnostic=True
    )
    assert full["status"] == "diagnostic_complete"
    assert full["counts"]["scopes"] == 3
    assert full["variables"] == 3


@pytest.mark.parametrize("catalog", ["sos_whole"], indirect=True)
def test_sos_workbook_without_register_coordinate_stays_whole_source(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    from reg_meta_build.prepared_catalog import open_prepared_catalog_sources

    prepared = open_prepared_catalog_sources(
        catalog.prepared, expected_sha256=catalog.digest, input_commit=catalog.commit
    )
    source = next(
        entry.revision.dataset
        for entry in prepared.manifest.inputs
        if entry.role == "sos_workbook" and entry.revision is not None
    )
    coordinates = prepared.records.register_coordinates(source)
    assert coordinates and {register for register, _, _ in coordinates} == {None}
    result = catalog.build(
        tmp_path / "sos.db",
        tmp_path / "sos-report",
        registers=(source,),
        diagnostic=True,
    )
    assert result["status"] == "diagnostic_complete"
    assert result["counts"]["scopes"] == 1
    assert result["counts"]["physical_occurrences"] == sum(
        count for _, _, count in coordinates
    )


def test_source_cannot_mix_whole_and_register_scopes(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build.prepared_sources import PreparedSourceRecords

    original = PreparedSourceRecords.register_coordinates

    def mixed(self, source):
        coordinates = original(self, source)
        if source == "scb-registerinformation":
            return (*coordinates, (None, coordinates[0][1], 1))
        return coordinates

    monkeypatch.setattr(PreparedSourceRecords, "register_coordinates", mixed)
    with pytest.raises(ValueError, match="both whole-source and register scopes"):
        catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_full_build_compiles_every_prepared_scope(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    result = catalog.build(tmp_path / "full.db", tmp_path / "report", diagnostic=True)
    assert result["status"] == "diagnostic_complete"
    assert result["counts"]["physical_occurrences"] == 2
    assert result["counts"]["scopes"] == 2
    assert result["variables"] == 2
    assert not [
        issue for issue in _issues(tmp_path / "report") if issue["severity"] == "error"
    ]


def test_strict_full_build_can_publish_clean_compiled_tree(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import resolved_catalog

    validate = resolved_catalog.validate_built_db
    monkeypatch.setattr(
        resolved_catalog,
        "validate_built_db",
        lambda path, *, corpus: validate(path, corpus=False),
    )
    result = catalog.build(tmp_path / "full.db", tmp_path / "report")
    assert result["status"] == "complete"
    assert result["publication_ready"] is True
    assert _manifest(tmp_path / "full.db")["catalog_publishable"] == "true"


def test_native_compiler_respects_tracked_slug_freeze_state(
    catalog: CatalogFixture,
) -> None:
    from reg_meta_build.curation_compile import compile_native_naming
    from reg_meta_build.curation_tree import load_curation_tree
    from reg_meta_build.pipeline import CompiledScope
    from reg_meta_build.prepared_catalog import open_prepared_catalog_sources

    register = catalog.curation / "registers/scb/sample.toml"
    register.write_text(
        register.read_text(encoding="utf-8").split("[[variable]]", 1)[0],
        encoding="utf-8",
    )
    register.with_name("sample.auto.toml").write_text(
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )
    prepared = open_prepared_catalog_sources(
        catalog.prepared, expected_sha256=catalog.digest, input_commit=catalog.commit
    )
    source = "scb-registerinformation"
    register_key = prepared.records.register_coordinates(source)[0][0]
    scope = CompiledScope(source=source, register_key=register_key)

    def variable_slugs() -> list[str | None]:
        naming, _, _, _, _ = compile_native_naming(
            load_curation_tree(catalog.curation), prepared, (scope,), subset=True
        )
        return [
            declaration.naming.slug
            for declaration in naming[source, register_key]
            if declaration.target.kind == "variable"
        ]

    assert variable_slugs() == []
    (catalog.curation / "slug_state.toml").write_text(
        'scb = "curating"\n', encoding="utf-8"
    )
    assert variable_slugs() == ["value"]


@pytest.mark.parametrize("where", ["prepared", "curation", "report", "dump"])
def test_outputs_cannot_alias_inputs_or_each_other(
    catalog: CatalogFixture, tmp_path: Path, where: str
) -> None:
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
    with pytest.raises(ValueError):
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


@pytest.mark.parametrize("local", [False, True])
def test_source_occurrence_accounting_cannot_drop_a_record(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, local: bool
) -> None:
    from reg_meta_build import pipeline

    resolve = pipeline.resolve_source_scope

    def drop_occurrence(*args, **kwargs):
        result = resolve(*args, **kwargs)
        return replace(result, corrections=replace(result.corrections, occurrences=()))

    monkeypatch.setattr(pipeline, "resolve_source_scope", drop_occurrence)
    with pytest.raises(ValueError, match="physical source occurrence accounting"):
        if local:
            catalog.check(tmp_path / "report")
        else:
            catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


def test_compiled_global_contract_rejects_unknown_field(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def invalid(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        return replace(compiled, fields={**compiled.fields, "unknown": True})

    monkeypatch.setattr(pipeline, "compile_curation", invalid)
    with pytest.raises(ValueError, match="unknown"):
        catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize("local", [False, True])
def test_compiled_global_contract_revalidates_serialized_nested_field(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, local: bool
) -> None:
    from reg_meta_build.curation_compile import CompiledCodebook

    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def invalid(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        book = CompiledCodebook("invalid", "invalid", {"broken": ["invalid"]})  # ty: ignore[invalid-argument-type]
        return replace(compiled, fields={**compiled.fields, "classifications": (book,)})

    monkeypatch.setattr(pipeline, "compile_curation", invalid)
    with pytest.raises(ValueError, match="classifications.0.metadata.broken"):
        if local:
            catalog.check(tmp_path / "report")
        else:
            catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize("local", [False, True])
def test_compiled_scope_contract_revalidates_serialized_naming(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, local: bool
) -> None:
    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def invalid(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        key, declarations = next(iter((compiled.naming or {}).items()))
        declaration = declarations[0]
        target = declaration.target.model_copy(update={"source_key": ()})
        naming = declaration.model_copy(update={"target": target})
        return replace(
            compiled,
            naming={**(compiled.naming or {}), key: (naming, *declarations[1:])},
        )

    monkeypatch.setattr(pipeline, "compile_curation", invalid)
    with pytest.raises(ValueError, match="nonempty exact source key"):
        if local:
            catalog.check(tmp_path / "report")
        else:
            catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize("local", [False, True])
def test_late_coding_contract_revalidates_serialized_nested_decision(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, local: bool
) -> None:
    from reg_meta_build import source_scope

    register = catalog.curation / "registers/scb/sample.toml"
    register.write_text(
        register.read_text(encoding="utf-8")
        + '\n[[coding.uncoded]]\nvariable = "1.101"\nvariant = "people"\n'
        'column = "VALUE"\nperiods = [["2020-01-01", "2020-12-31"]]\n'
        'reason = "Reviewed"\nsource = "fixture"\n',
        encoding="utf-8",
    )
    compile_coding = source_scope.compile_coding_register

    def invalid(*args, **kwargs):
        cases, diagnostics = compile_coding(*args, **kwargs)
        assert cases
        decision = cases[0].decision.model_copy(update={"reason": " "})
        case = cases[0].model_copy(update={"decision": decision})
        return (case, *cases[1:]), diagnostics

    monkeypatch.setattr(source_scope, "compile_coding_register", invalid)
    with pytest.raises(ValueError, match="rationale and provenance"):
        if local:
            catalog.check(tmp_path / "report")
        else:
            catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


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
    with pytest.raises(ValueError, match="resolved catalog validation failed"):
        catalog.build(output, report)
    summary = json.loads((report / "summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "engineering_failure"
    assert summary["counts"].get("error", 0) == 0
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


@pytest.mark.parametrize("diagnostic", [False, True])
def test_lost_delivery_coverage_is_never_published(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, diagnostic: bool
) -> None:
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def truncate(segment, variant, coding, subject):
        states, issues, withheld = original(segment, variant, coding, subject)
        return (
            [state.model_copy(update={"valid_to": "2020-06-30"}) for state in states],
            issues,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", truncate)
    output, report = tmp_path / "lost.db", tmp_path / "report"
    missing = "2020-07-01..2020-12-31"
    if diagnostic:
        result = catalog.build(output, report, diagnostic=True)
        assert result["status"] == "diagnostic_complete"
        assert result["publication_ready"] is False
        assert output.exists()
        issues = [
            issue
            for issue in _issues(report)
            if issue["code"] == "unexplained_delivery_coverage_loss"
        ]
        assert len(issues) == 1 and issues[0]["severity"] == "error"
        assert missing in issues[0]["detail"]
        assert "scb/sample/value people/VALUE" in issues[0]["detail"]
    else:
        with pytest.raises(ValueError, match="delivery coverage was lost") as failure:
            catalog.build(output, report)
        assert missing in str(failure.value)
        assert "scb/sample/value people/VALUE" in str(failure.value)
        assert not output.exists()
        assert json.loads((report / "summary.json").read_text())["status"] == (
            "engineering_failure"
        )


@pytest.mark.parametrize("diagnostic", [False, True])
def test_changed_delivery_facts_are_never_published(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, diagnostic: bool
) -> None:
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def retype(segment, variant, coding, subject):
        states, issues, withheld = original(segment, variant, coding, subject)
        return (
            [state.model_copy(update={"data_type": "text"}) for state in states],
            issues,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", retype)
    output, report = tmp_path / "changed.db", tmp_path / "report"
    if diagnostic:
        result = catalog.build(output, report, diagnostic=True)
        assert result["status"] == "diagnostic_complete"
        assert result["publication_ready"] is False
        assert output.exists()
        issues = [
            issue
            for issue in _issues(report)
            if issue["code"] == "unexplained_delivery_fact_change"
        ]
        assert len(issues) == 1 and issues[0]["severity"] == "error"
        detail = issues[0]["detail"]
    else:
        with pytest.raises(
            ValueError, match="supported delivery facts changed"
        ) as failure:
            catalog.build(output, report)
        detail = str(failure.value)
        assert not output.exists()
        assert json.loads((report / "summary.json").read_text())["status"] == (
            "engineering_failure"
        )
    assert "scb/sample/value people/VALUE" in detail
    assert "claimed data_type=" in detail
    assert "written 'text'" in detail


_COLUMN_OVERLAPS = {
    "overlapping_distinct_value_sets": (
        {
            "value_set_version_label": "a",
            "value_set": ResolvedCodeSet(members=(("1", "One"),)),
        },
        {
            "value_set_version_label": "b",
            "value_set": ResolvedCodeSet(members=(("2", "Two"),)),
        },
    ),
    "overlapping_codeless_codebearing_states": (
        {},
        {
            "value_set_version_label": "a",
            "value_set": ResolvedCodeSet(members=(("1", "One"),)),
        },
    ),
    "overlapping_pooled_explicit_states": (
        {},
        {"pooled": True, "value_set_version_label": "p"},
    ),
}


@pytest.mark.parametrize("code", _COLUMN_OVERLAPS)
def test_overlapping_states_withhold_only_variable_in_diagnostic(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, code: str
) -> None:
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def overlap(segment, variant, coding, subject):
        states, issues, withheld = original(segment, variant, coding, subject)
        return (
            [
                state.model_copy(update=update)
                for state in states
                for update in _COLUMN_OVERLAPS[code]
            ],
            issues,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", overlap)
    output, strict_report = tmp_path / "overlap.db", tmp_path / "strict"
    with pytest.raises(ValueError) as failure:
        catalog.build(output, strict_report)
    assert "scb/sample/value people/VALUE" in str(failure.value)
    assert not output.exists()
    assert json.loads((strict_report / "summary.json").read_text())["status"] == (
        "engineering_failure"
    )
    report = tmp_path / "diagnostic"
    result = catalog.build(output, report, diagnostic=True)
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert result["variables"] == 0
    issues = [issue for issue in _issues(report) if issue["code"] == code]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert issues[0]["detail"] == str(failure.value)
    assert issues[0]["withheld_output"] == ["scb/sample/value"]
    assert validate_built_db(output, corpus=False).passed


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_references_into_unselected_registers_are_deferred(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import resolved_catalog

    validate = resolved_catalog.validate_built_db
    monkeypatch.setattr(
        resolved_catalog,
        "validate_built_db",
        lambda path, *, corpus: validate(path, corpus=False),
    )
    (catalog.curation / "relations.toml").write_text(
        '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\n'
        'b = "scb/other/value"\n'
        '[[edge]]\ntype = "replaced_by"\nfrom = "scb/sample/value"\n'
        'to = "scb/other/value"\neffective_year = 2021\n',
        encoding="utf-8",
    )
    full = tmp_path / "full.db"
    result = catalog.build(full, tmp_path / "full-report")
    assert result["publication_ready"] is True
    assert "deferred_references" not in result["counts"]
    with sqlite3.connect(full) as conn:
        for table in ("variable_same_as", "variable_replaced_by"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone() != (0,)
    for register in ("1", "2"):
        output, report = tmp_path / f"{register}.db", tmp_path / f"report-{register}"
        result = catalog.build(output, report, diagnostic=True, registers=(register,))
        assert result["status"] == "diagnostic_complete"
        assert result["publication_ready"] is False
        assert result["counts"].get("error", 0) == 0
        issues = _issues(report)
        assert issues
        assert {(issue["code"], issue["severity"]) for issue in issues} == {
            ("deferred_out_of_slice_reference", "warning")
        }
        assert result["counts"]["deferred_references"] == len(issues)
        with sqlite3.connect(output) as conn:
            for table in ("variable_same_as", "variable_replaced_by"):
                assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (0,)


@pytest.mark.parametrize("catalog", ["thin"], indirect=True)
def test_reference_into_unselected_thin_provider_is_deferred(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    (catalog.curation / "relations.toml").write_text(
        '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\nb = "fk/remote/amount"\n',
        encoding="utf-8",
    )
    output, report = tmp_path / "slice.db", tmp_path / "report"
    result = catalog.build(output, report, registers=("1",), diagnostic=True)
    assert result["status"] == "diagnostic_complete"
    assert result["counts"].get("error", 0) == 0
    assert result["counts"]["deferred_references"] == 1
    assert [(issue["code"], issue["severity"]) for issue in _issues(report)] == [
        ("deferred_out_of_slice_reference", "warning")
    ]
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable_same_as").fetchone() == (0,)


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_unselected_additional_family_name_uses_naming_stages(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import curation_compile

    (catalog.curation / "relations.toml").write_text(
        '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\n'
        'b = "scb/other/calendar"\n',
        encoding="utf-8",
    )
    compile_matrix = curation_compile.compile_matrix_repr

    def with_matrix_name(tree, prepared, scopes, naming):
        cases, matrix_names, keys, diagnostics = compile_matrix(
            tree, prepared, scopes, naming
        )
        if len(scopes) != 1:
            return cases, matrix_names, keys, diagnostics
        key = scopes[0].source, scopes[0].register_key
        declarations = naming.get(key, ())
        if not declarations or key[1] is None or key[1][-1] != 2:
            return cases, matrix_names, keys, diagnostics
        native = next(item for item in declarations if item.target.kind == "variable")
        # The period-family compiler adds names after native naming and partitions.
        matrix = native.model_copy(
            update={
                "target": native.target.model_copy(
                    update={
                        "source_key": (
                            "curation",
                            "period-family",
                            "scb",
                            "other",
                            "calendar",
                        )
                    }
                ),
                "naming": replace(
                    native.naming, source_id="matrix:calendar", slug="calendar"
                ),
            }
        )
        return (
            cases,
            {**matrix_names, key: (*matrix_names.get(key, ()), matrix)},
            keys,
            diagnostics,
        )

    monkeypatch.setattr(curation_compile, "compile_matrix_repr", with_matrix_name)
    output, report = tmp_path / "slice.db", tmp_path / "report"
    result = catalog.build(output, report, registers=("1",), diagnostic=True)
    assert result["counts"].get("error", 0) == 0
    assert result["counts"]["deferred_references"] == 1
    assert [(issue["code"], issue["severity"]) for issue in _issues(report)] == [
        ("deferred_out_of_slice_reference", "warning")
    ]


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_scoped_build_compiles_full_curation_only_for_selected_scope(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation
    compiled_registers = []

    def record_scopes(tree, prepared, scopes, *, subset, storage_columns):
        compiled_registers.append(tuple(scope.register_key[-1] for scope in scopes))
        return compile_tree(
            tree, prepared, scopes, subset=subset, storage_columns=storage_columns
        )

    monkeypatch.setattr(pipeline, "compile_curation", record_scopes)
    result = catalog.build(
        tmp_path / "slice.db",
        tmp_path / "report",
        registers=("1",),
        diagnostic=True,
        dump_decisions=tmp_path / "build-decisions",
    )
    assert result["counts"].get("error", 0) == 0
    assert compiled_registers == [(1,)]
    for name in (
        "compile_deferred_naming",
        "resolve_panel_dependencies",
        "write_resolved_catalog",
        "validate_built_db",
    ):
        monkeypatch.setattr(
            pipeline, name, lambda *a, **kw: pytest.fail("catalog tail ran")
        )
    checked = catalog.check(
        tmp_path / "check-report", dump_decisions=tmp_path / "check-decisions"
    )
    assert checked["passed"] is True
    assert compiled_registers == [(1,), (1,)]
    assert {
        p.name: p.read_bytes() for p in (tmp_path / "build-decisions").iterdir()
    } == {p.name: p.read_bytes() for p in (tmp_path / "check-decisions").iterdir()}
    with gzip.open(tmp_path / "report/events.jsonl.gz", "rb") as stream:
        full_events = stream.read().splitlines()
    with gzip.open(tmp_path / "check-report/events.jsonl.gz", "rb") as stream:
        local_events = stream.read().splitlines()
    assert full_events[: len(local_events)] == local_events


def test_check_curation_keeps_late_classification_diagnostics(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build.source_curation import ResolutionDiagnostic

    from reg_meta_build import pipeline

    finalize = pipeline.finalize_classification_bindings

    def late_binding(*args, **kwargs):
        return (
            *finalize(*args, **kwargs),
            ResolutionDiagnostic(
                code="late_classification_check",
                severity="error",
                subject="test",
                detail="late binding reached",
            ),
        )

    monkeypatch.setattr(pipeline, "finalize_classification_bindings", late_binding)
    result = catalog.check(tmp_path / "report", dump_decisions=tmp_path / "decisions")
    assert result["status"] == "curation_check_complete"
    assert result["passed"] is False
    assert result["counts"]["error"] == 1
    assert [issue["code"] for issue in _issues(tmp_path / "report")] == [
        "late_classification_check"
    ]
    assert (tmp_path / "decisions/compile-report.json").exists()


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_scoped_build_indexes_swecov_columns_once(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import curation_compile

    original = curation_compile.index_swecov_column_types
    calls = []

    def tracked(declarations):
        calls.append(True)
        return original(declarations)

    monkeypatch.setattr(curation_compile, "index_swecov_column_types", tracked)
    catalog.build(
        tmp_path / "slice.db", tmp_path / "report", registers=("1",), diagnostic=True
    )
    assert len(calls) == 1


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_unselected_declared_variable_name_is_deferred(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    other = catalog.curation / "registers/scb/other.toml"
    other.write_text(
        other.read_text(encoding="utf-8").replace('slug = "value"', 'slug = "curated"'),
        encoding="utf-8",
    )
    (catalog.curation / "relations.toml").write_text(
        '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\nb = "scb/other/curated"\n',
        encoding="utf-8",
    )
    report = tmp_path / "report"
    result = catalog.build(
        tmp_path / "slice.db", report, registers=("1",), diagnostic=True
    )
    assert result["counts"].get("error", 0) == 0
    assert result["counts"]["deferred_references"] == 1
    assert [(issue["code"], issue["severity"]) for issue in _issues(report)] == [
        ("deferred_out_of_slice_reference", "warning")
    ]


@pytest.mark.parametrize("catalog", [True], indirect=True)
@pytest.mark.parametrize("target", ["scb/nosuch/value", "scb/other/missing"])
def test_undeclared_reference_is_fatal_even_when_register_is_unselected(
    catalog: CatalogFixture, tmp_path: Path, target: str
) -> None:
    (catalog.curation / "relations.toml").write_text(
        f'[[edge]]\ntype = "same_as"\na = "scb/sample/value"\nb = "{target}"\n',
        encoding="utf-8",
    )
    for name, registers in (("full", ()), ("scoped", ("1",))):
        with pytest.raises(CatalogDependencyError, match=target):
            catalog.build(
                tmp_path / f"{name}.db",
                tmp_path / f"{name}-report",
                diagnostic=True,
                registers=registers,
            )
    checked = catalog.check(tmp_path / "check-report")
    assert checked["status"] == "curation_check_complete"
    assert checked["passed"] is True
    assert "variable_edge_groups" in checked["checks_not_run"]
    assert "delivery_coverage" in checked["checks_not_run"]
    assert "deferred_references" not in checked["counts"]


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_curation_wholly_outside_slice_is_skipped_without_claiming_proof(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    target = "scb/nosuch/value"
    (catalog.curation / "relations.toml").write_text(
        f'[[edge]]\ntype = "same_as"\na = "scb/other/value"\nb = "{target}"\n',
        encoding="utf-8",
    )
    with pytest.raises(CatalogDependencyError, match=target):
        catalog.build(tmp_path / "full.db", tmp_path / "full-report", diagnostic=True)
    report = tmp_path / "slice-report"
    result = catalog.build(tmp_path / "slice.db", report, registers=("1",))
    assert result["status"] == "complete"
    assert result["counts"].get("error", 0) == 0
    assert result["counts"]["skipped_curation"] == 1
    assert not any(
        issue["code"] == "deferred_out_of_slice_reference" for issue in _issues(report)
    )


def _unowned_crosswalk(source: str) -> SourceCodeCrosswalkDeclaration:
    revision = SourceRevision.create(
        dataset=source,
        publisher="fixture",
        purpose="relationship selection regression",
        upstream_revision="1",
        artifact_path="crosswalk.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    return SourceCodeCrosswalkDeclaration(
        revision=revision,
        locator=RecordLocator(
            semantic_record_key=("crosswalk",),
            physical_file="crosswalk.csv",
            physical_table="crosswalk",
            physical_record="row:1",
            physical_cells=("row:1",),
        ),
        delivered_cells=(),
        member_name=None,
        supplied_period=None,
        section_period=None,
        section_locator=None,
        description=None,
        operands=(),
    )


@pytest.mark.parametrize("catalog", ["thin"], indirect=True)
@pytest.mark.parametrize(
    "source", ["scb-registerinformation", "Forsakringskassan/fk.toml", "unknown"]
)
def test_unbound_relationship_defers_only_known_unselected_occurrence_source(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, source: str
) -> None:
    declaration = _unowned_crosswalk(source)
    revision = declaration.revision
    evidence = ReferenceEvidence(
        revision_id=revision.revision_id, declaration=declaration
    )
    original = PreparedCatalogSources.iter_evidence

    def with_crosswalk(prepared):
        yield from original(prepared)
        yield evidence

    monkeypatch.setattr(PreparedCatalogSources, "iter_evidence", with_crosswalk)
    for label, registers in (("slice", ("1",)), ("full", ())):
        report = tmp_path / f"{label}-report"
        catalog.build(
            tmp_path / f"{label}.db", report, registers=registers, diagnostic=True
        )
        deferred = label == "slice" and source == "Forsakringskassan/fk.toml"
        assert [(i["code"], i["severity"]) for i in _issues(report)] == [
            (
                "deferred_out_of_slice_reference"
                if deferred
                else "unbound_source_relationship",
                "warning" if deferred else "error",
            )
        ]
        with gzip.open(report / "events.jsonl.gz", "rt") as stream:
            retained = [
                item
                for line in stream
                if (item := json.loads(line)).get("revision_id") == revision.revision_id
            ]
        assert len(retained) == 1
        assert retained[0]["disposition"] == (
            "out_of_slice_relationship" if deferred else "unbound_relationship"
        )


def test_support_only_register_reaches_coding_compiler_and_report(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    register = catalog.curation / "registers/scb/sample.toml"
    register.write_text(
        register.read_text()
        + '\n[[coding.support]]\nvariable = "1.999"\nvariant = "people"\n'
        'column = "VALUE"\nperiods = [["2020-01-01", "2020-12-31"]]\n'
        'code = "1"\nlabel = "erroneous"\nassociation = "row:bad"\n'
        'expected_association = "' + "a" * 64 + '"\n'
        'authority_code = "2"\nauthority_label = "correct"\n'
        'authority_association = "row:correct"\n'
        'expected_authority_association = "' + "b" * 64 + '"\n'
        'expected_source_codings = ["' + "c" * 64 + '"]\n'
        'reason = "Reviewed exact association"\nsource = "fixture"\n'
    )
    report = tmp_path / "report"
    decisions = tmp_path / "decisions"
    result = catalog.build(
        tmp_path / "catalog.db",
        report,
        registers=("1",),
        diagnostic=True,
        dump_decisions=decisions,
    )
    assert result["status"] == "diagnostic_complete"
    issues = _issues(report)
    assert any(
        issue["code"] == "stale_curation_entry"
        and "coding.support/1/period/1" in issue.get("case_id", "")
        for issue in issues
    )
    compiled = json.loads((decisions / "compile-report.json").read_text())
    assert any(
        "coding.support/1/period/1" in case_id
        for case_id in compiled["scb/sample"]["entries_read"]
    )


def test_unbound_value_descriptor_identity_changes_with_exact_source_evidence():
    from reg_meta_build.pipeline import _value_source_issue
    from reg_meta_build.source_value_bindings import ValueBindingIssue
    from reg_meta_build.source_values import SourceValueDescriptor

    descriptor = SourceValueDescriptor(
        payload_key="sheet:DIAGNOS", raw_cells=("DIAGNOS",)
    )
    revision = SimpleNamespace(revision_id="named@sha256:" + ("a" * 64))
    rows = []
    session = SimpleNamespace(
        source="named",
        descriptors=(descriptor,),
        session=SimpleNamespace(
            source=SimpleNamespace(manifest=SimpleNamespace(revision=revision)),
            lookup_descriptor=lambda key: iter(rows),
        ),
    )
    problem = ValueBindingIssue(
        "unresolved_list_reference", "named", descriptor.payload_key
    )
    original, evidence = _value_source_issue(session, problem)
    assert original.refs == () and original.severity == "error"
    assert evidence["physical_associations"] == 0 and evidence["descriptor"][
        "raw_cells"
    ] == ["DIAGNOS"]
    revision.revision_id = "named@sha256:" + ("b" * 64)
    changed, _ = _value_source_issue(session, problem)
    assert changed.subject != original.subject
    revision.revision_id = "named@sha256:" + ("a" * 64)
    session.descriptors = (replace(descriptor, raw_cells=("changed source cell",)),)
    changed, _ = _value_source_issue(session, problem)
    assert changed.subject != original.subject
    session.descriptors = (descriptor,)
    rows.append(object())
    changed, evidence = _value_source_issue(session, problem)
    assert (
        changed.subject != original.subject and evidence["physical_associations"] == 1
    )


@pytest.mark.parametrize("catalog", ["unknown_support"], indirect=True)
def test_pipeline_routes_real_support_errors_without_dropping_source_event(
    catalog, tmp_path
):
    for label, registers in (("slice", ("1",)), ("full", ())):
        report = tmp_path / label
        catalog.build(
            tmp_path / f"{label}.db", report, registers=registers, diagnostic=True
        )
        relevant = [
            i
            for i in _issues(report)
            if i["code"] in {"unknown_support_key", "deferred_out_of_slice_reference"}
        ]
        assert sorted((i["code"], i["severity"]) for i in relevant) == (
            [
                ("deferred_out_of_slice_reference", "warning"),
                ("unknown_support_key", "error"),
            ]
            if registers
            else [("unknown_support_key", "error"), ("unknown_support_key", "error")]
        )
        with gzip.open(report / "events.jsonl.gz", "rt") as stream:
            raw = [
                v
                for line in stream
                if (v := json.loads(line))["kind"] == "support_source_issue"
            ]
        assert (
            len(raw) == 1 and len(raw[0]["refs"]) == 2 and raw[0]["severity"] == "error"
        )
        assert {json.dumps(r, sort_keys=True) for i in relevant for r in i["refs"]} == {
            json.dumps(r, sort_keys=True) for r in raw[0]["refs"]
        }


def test_retained_unattached_relationship_persists_warning_and_literal_only(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.source_records import SourceEvidenceRow, SourceEvidenceTable

    source = "scb-registerinformation"
    declaration = _unowned_crosswalk(source)
    table = SourceEvidenceTable(
        source=source,
        source_revision_id=declaration.revision.revision_id,
        name="crosswalk",
        rows=(
            SourceEvidenceRow(
                locator=declaration.locator,
                role="declaration",
                cells=declaration.delivered_cells,
            ),
        ),
    )
    original_evidence = PreparedCatalogSources.iter_evidence
    original_tables = PreparedSourceRecords.iter_tables

    def with_crosswalk(prepared):
        yield from original_evidence(prepared)
        yield ReferenceEvidence(
            revision_id=declaration.revision.revision_id, declaration=declaration
        )

    def with_table(records, **kwargs):
        yield from original_tables(records, **kwargs)
        if kwargs.get("source") in (None, source):
            yield table

    monkeypatch.setattr(PreparedCatalogSources, "iter_evidence", with_crosswalk)
    monkeypatch.setattr(PreparedSourceRecords, "iter_tables", with_table)
    register = catalog.curation / "registers/scb/sample.toml"
    register.write_text(
        register.read_text() + "\n[[documentary.retained]]\n"
        'source = "scb-registerinformation"\ntable = "crosswalk"\nrow = "row:1"\n'
        f'payload_sha256 = "{canonical_sha256(declaration.model_dump(mode="json"))}"\n'
        f'table_sha256 = "{canonical_sha256(table.model_dump(mode="json"))}"\n'
        'reason = "No supplied variable or encoding endpoint; retain literal codes unattached."\n'
        'evidence = "Complete exact table reviewed."\nnoted = "2026-10-02"\n'
    )
    out, report = tmp_path / "retained.db", tmp_path / "retained-report"
    catalog.build(out, report, diagnostic=True)
    assert [(i["code"], i["severity"]) for i in _issues(report)] == [
        ("retained_unattached_source_relationship", "warning")
    ]
    with sqlite3.connect(out) as conn:
        owner, status, raw = conn.execute(
            "SELECT owner_variable_id,binding_status,declaration_json FROM source_relationship"
        ).fetchone()
        assert owner is None and status == "retained_unattached"
        assert SourceCodeCrosswalkDeclaration.model_validate_json(raw) == declaration
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM source_relationship_variable"
            ).fetchone()[0]
            == 0
        )
        warning = conn.execute(
            "SELECT register_id, variable_id, json_extract(warning_json, '$.detail') FROM data_warning"
        ).fetchone()
        assert warning[0] is not None and warning[1] is None
        assert (
            warning[2]
            == "No supplied variable or encoding endpoint; retain literal codes unattached."
        )
        assert not conn.execute("pragma foreign_key_check").fetchall()
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        events = [json.loads(line) for line in stream]
    assert [
        e["disposition"]
        for e in events
        if e.get("revision_id") == declaration.revision.revision_id
    ] == ["retained_unattached"]


@pytest.mark.parametrize("changed", [False, True])
def test_compiled_retained_source_finding_reaches_exact_ack(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, changed: bool
) -> None:
    from reg_meta_build.source_coordinates import source_register_key
    from reg_meta_build.source_curation import (
        AcknowledgeDecision,
        CurationCase,
        ResolutionDiagnostic,
    )
    from reg_meta_build.source_effects import record_ref

    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def retained(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        prepared = args[1]
        item = next(prepared.records.iter_records(source="scb-registerinformation"))
        register_key = source_register_key(item)
        assert register_key is not None
        key = (item.source, register_key)
        problem = ResolutionDiagnostic(
            code="unassigned_original_columns",
            severity="error",
            subject="1.101",
            refs=(record_ref(item),),
            fields=("identity", "column_name"),
            detail="Retained unassigned source column.",
            withheld_output=("1.101",),
        )
        ack = CurationCase(
            case_id="accepted-partial-owner",
            targets=(),
            decision=AcknowledgeDecision(
                register_key=register_key,
                code=problem.code,
                subject=problem.subject,
                refs=problem.refs,
                fields=problem.fields,
                reason="Preserve the unknown original column.",
                evidence="Exact reviewed source finding.",
            ),
        )
        if changed:
            problem = problem.model_copy(update={"refs": ()})
        return replace(
            compiled,
            cases={**compiled.cases, key: (*compiled.cases[key], ack)},
            source_diagnostics={key: ((register_key, problem),)},
        )

    monkeypatch.setattr(pipeline, "compile_curation", retained)
    catalog.build(
        tmp_path / "catalog.db", tmp_path / "report", registers=("1",), diagnostic=True
    )
    with gzip.open(tmp_path / "report/events.jsonl.gz", "rt") as ledger:
        issues = [json.loads(line) for line in ledger if '"kind": "issue"' in line]
    findings = [i for i in issues if i["code"] == "unassigned_original_columns"]
    assert len(findings) == 1
    assert findings[0]["severity"] == ("error" if changed else "warning")
    if changed:
        assert any(i["code"] == "stale_curation_entry" for i in issues)
    else:
        assert findings[0]["acknowledged_by"] == "accepted-partial-owner"


def test_warning_only_register_is_compiled_and_warning_roundtrips(catalog, tmp_path):
    from reg_meta_build.prepared_catalog import open_prepared_catalog_sources
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    prepared = open_prepared_catalog_sources(
        catalog.prepared, input_commit=catalog.commit, expected_sha256=catalog.digest
    )
    originals = tuple(
        r for r in prepared.records.records if r.source == "scb-registerinformation"
    )
    guard = acknowledgement_evidence_sha256(originals)
    curated = catalog.curation / "registers/scb/sample.toml"
    with curated.open("a") as handle:
        handle.write(f'''
[[coding.warning]]
variable = "1.101"
variant = "people"
column = "VALUE"
periods = [["2020-01-01", "2020-12-31"]]
fields = ["data_type", "coding"]
expected_evidence_sha256 = "{guard}"
data_warning = "Retained source metadata conflict"
reason = "Preserve source type and own coding"
source = "Exact source rows"
''')
    checked = catalog.check(tmp_path / "check-report")
    assert checked["passed"] is True
    db = tmp_path / "catalog.db"
    result = catalog.build(db, tmp_path / "report", registers=("1",), diagnostic=True)
    assert result["counts"].get("error", 0) == 0
    with sqlite3.connect(db) as conn:
        warning = conn.execute("SELECT warning_json FROM data_warning").fetchone()
        assert warning is not None
        payload = json.loads(warning[0])
        assert payload["code"] == "source_metadata_conflict"
        assert payload["variant"] == "people"
        assert payload["delivery_column_name"] == "VALUE"
        assert payload["valid_from"] == "2020-01-01"
        assert payload["valid_to"] == "2020-12-31"
        assert payload["refs"][0]["semantic_record_key"][-1] == "member:1001"
        assert conn.execute("SELECT data_type FROM variable_state").fetchone() == (
            "integer",
        )
