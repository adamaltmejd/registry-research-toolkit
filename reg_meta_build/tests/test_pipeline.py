"""Exercise build-db from accepted prepared sources and tracked curation."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from _csv_fixtures import _var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from _sos_fixtures import DEFAULT_REGISTERS, write_sos_input
from reg_meta.errors import EXIT_USAGE
from reg_meta_build.catalog_dependencies import CatalogDependencyError
from reg_meta_build.cli import run
from reg_meta_build.curation_tree import (
    ClassificationFamilies,
    ClassificationFamily,
    ClassificationMetadata,
    CuratedClassification,
)
from reg_meta_build.pipeline import _classification_references, build_catalog
from reg_meta_build.prepared_catalog import prepare_catalog_sources
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


@pytest.fixture
def catalog(tmp_path: Path, request) -> CatalogFixture:
    mode = getattr(request, "param", False)
    second = mode is True
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


def test_compiled_global_contract_revalidates_serialized_nested_field(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build.curation_compile import CompiledCodebook

    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def invalid(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        book = CompiledCodebook("invalid", "invalid", {"broken": ["invalid"]})
        return replace(compiled, fields={**compiled.fields, "classifications": (book,)})

    monkeypatch.setattr(pipeline, "compile_curation", invalid)
    with pytest.raises(ValueError, match="classifications.0.metadata.broken"):
        catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


def test_compiled_scope_contract_revalidates_serialized_naming(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
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

    def record_scopes(tree, prepared, scopes, *, subset):
        compiled_registers.append(tuple(scope.register_key[-1] for scope in scopes))
        return compile_tree(tree, prepared, scopes, subset=subset)

    monkeypatch.setattr(pipeline, "compile_curation", record_scopes)
    result = catalog.build(
        tmp_path / "slice.db", tmp_path / "report", registers=("1",), diagnostic=True
    )
    assert result["counts"].get("error", 0) == 0
    assert compiled_registers == [(1,)]


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
        '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\n'
        'b = "scb/other/curated"\n',
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
