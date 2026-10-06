"""build-db and check-curation CLI contract, output-path safety, determinism and failure preservation."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

import pytest
from _pipeline_catalog_support import (
    CatalogFixture,
    import_manifest as _manifest,
    report_issues as _issues,
)
from reg_meta.errors import EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.pipeline import (
    build_catalog,
    check_curation,
)

if TYPE_CHECKING:
    from pathlib import Path


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
