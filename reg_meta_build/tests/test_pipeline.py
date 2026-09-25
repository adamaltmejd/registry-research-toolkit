"""Exercise build-db from accepted prepared sources and tracked curation."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import _var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from reg_meta.errors import EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.pipeline import build_catalog
from reg_meta_build.prepared_catalog import prepare_catalog_sources

if TYPE_CHECKING:
    from pathlib import Path


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


@pytest.fixture
def catalog(tmp_path: Path, request) -> CatalogFixture:
    second = getattr(request, "param", False)
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
    write_scb_input(
        source,
        registerinformation_rows=records,
        unika_rows=summaries,
        include=("registerinformation", "unika"),
    )
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
    return CatalogFixture(prepared, commit, manifest.sha256, curation)


def _issues(report: Path) -> list[dict]:
    with gzip.open(report / "events.jsonl.gz", "rt", encoding="utf-8") as stream:
        return [row for line in stream if (row := json.loads(line))["kind"] == "issue"]


def _manifest(path: Path) -> dict[str, str]:
    with sqlite3.connect(path) as conn:
        return dict(conn.execute("SELECT key, value FROM import_manifest"))


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


def test_cli_summary_cannot_overwrite_prepared_manifest(
    catalog: CatalogFixture, tmp_path: Path, capsys
) -> None:
    manifest = catalog.prepared / "manifest.json"
    original = manifest.read_bytes()
    status = run(
        [
            "--output",
            str(manifest),
            "--db",
            str(tmp_path / "db-dir"),
            "build-db",
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
    )
    assert status == EXIT_USAGE
    assert manifest.read_bytes() == original
    assert not (tmp_path / "db-dir").exists()
    capsys.readouterr()


def test_diagnostic_and_strict_compile_identically(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    register = catalog.curation / "registers" / "scb" / "sample.toml"
    register.write_text(
        register.read_text(encoding="utf-8")
        + '[[variable]]\nnative_id = "1.999"\nslug = "missing"\n',
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


@pytest.mark.parametrize("catalog", [True], indirect=True)
def test_subset_resolves_only_named_scope(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    result = catalog.build(tmp_path / "slice.db", tmp_path / "report", registers=("1",))
    assert result["counts"]["physical_occurrences"] == 1
    assert result["variables"] == 1
    with sqlite3.connect(tmp_path / "slice.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM register").fetchone() == (1,)
    with pytest.raises(ValueError, match="names no selected scope"):
        catalog.build(tmp_path / "bad.db", tmp_path / "bad-report", registers=("3",))


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


def test_prepared_pins_are_checked(catalog: CatalogFixture, tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        build_catalog(
            catalog.prepared,
            "0" * 40,
            catalog.digest,
            tmp_path / "bad.db",
            tmp_path / "report",
            curation_dir=catalog.curation,
            registers=("1",),
        )


def test_source_occurrence_accounting_cannot_drop_a_record(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import pipeline

    resolve = pipeline.resolve_source_scope

    def drop_occurrence(*args, **kwargs):
        result = resolve(*args, **kwargs)
        return replace(result, corrections=replace(result.corrections, occurrences=()))

    monkeypatch.setattr(pipeline, "resolve_source_scope", drop_occurrence)
    with pytest.raises(ValueError, match="physical source occurrence accounting"):
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
