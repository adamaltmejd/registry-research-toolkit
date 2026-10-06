"""build-db register-scope selection, out-of-slice references and curation reaching the scope compilers."""

from __future__ import annotations

import gzip
import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _pipeline_catalog_support import (
    CatalogFixture,
    report_issues as _issues,
)
from reg_meta_build.catalog_dependencies import CatalogDependencyError

if TYPE_CHECKING:
    from pathlib import Path


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
