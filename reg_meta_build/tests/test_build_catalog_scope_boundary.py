"""Register-scoped builds, local checks and decision dumps at the build boundary.

Every case builds a synthetic two-register SCB catalog from readable CSV and
curation TOML through `build_catalog` / `check_curation`, then asserts the
result dict, the event ledger, the decision dump or the built SQLite rows.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _pipeline_catalog_support import report_issues

if TYPE_CHECKING:
    from pathlib import Path

    from _pipeline_catalog_support import CatalogFixture

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
RELATIONS = (
    '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\nb = "scb/other/value"\n'
    '[[edge]]\ntype = "replaced_by"\nfrom = "scb/sample/value"\n'
    'to = "scb/other/value"\neffective_year = 2021\n'
)


# Registers 1 (scb/sample) and 2 (scb/other), one variable each.
pytestmark = pytest.mark.parametrize("catalog", [True], indirect=True)


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


def _count(db: Path, table: str) -> int:
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_slice_build_holds_only_the_selected_register(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    output = tmp_path / "slice.db"
    result = catalog.build(output, tmp_path / "report", registers=("2",))
    assert result["status"] == "complete"
    assert result["counts"]["scopes"] == 1
    assert result["counts"]["physical_occurrences"] == 1
    assert result["variables"] == 1
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT slug FROM register").fetchall() == [("other",)]


@pytest.mark.parametrize("command", ["build", "check"])
def test_unknown_register_scope_is_refused(
    catalog: CatalogFixture, tmp_path: Path, command: str
) -> None:
    output = tmp_path / "bad.db"
    with pytest.raises(ValueError, match=r"names no selected scope: \['3'\]"):
        if command == "build":
            catalog.build(output, tmp_path / "report", registers=("1", "3"))
        else:
            catalog.check(tmp_path / "report", registers=("1", "3"))
    assert not output.exists()
    assert not (tmp_path / "report").exists()


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


def test_decision_dump_lists_each_scope_cases_and_naming(
    catalog: CatalogFixture, tmp_path: Path
) -> None:
    _with_errata(catalog)
    decisions = tmp_path / "decisions"
    catalog.build(
        tmp_path / "slice.db",
        tmp_path / "report",
        registers=("1", "2"),
        dump_decisions=decisions,
    )
    scopes = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(decisions.glob("scope-*.json"))
    ]
    assert len(scopes) == 2
    for scope, name in zip(
        sorted(scopes, key=lambda item: repr(item["register_key"])),
        ("sample", "other"),
        strict=True,
    ):
        assert [case["decision"]["kind"] for case in scope["cases"]] == [
            "correct_occurrences"
        ]
        assert all(f"registers/scb/{name}.toml" in c["case_id"] for c in scope["cases"])
        slugs = {
            item["target"]["kind"]: item["naming"]["slug"] for item in scope["naming"]
        }
        assert slugs["register"] == name
        assert slugs["variable"] == "value"


@pytest.mark.parametrize("selected", ["1", "2"])
def test_relations_into_unselected_register_are_deferred(
    catalog: CatalogFixture, tmp_path: Path, selected: str
) -> None:
    (catalog.curation / "relations.toml").write_text(RELATIONS, encoding="utf-8")
    full = tmp_path / "full.db"
    result = catalog.build(full, tmp_path / "full-report", diagnostic=True)
    assert result["counts"].get("error", 0) == 0
    assert "deferred_references" not in result["counts"]
    assert _count(full, "variable_same_as") > 0
    assert _count(full, "variable_replaced_by") > 0
    output, report = tmp_path / "slice.db", tmp_path / "slice-report"
    result = catalog.build(output, report, diagnostic=True, registers=(selected,))
    assert result["status"] == "diagnostic_complete"
    assert result["counts"].get("error", 0) == 0
    issues = report_issues(report)
    assert [(i["code"], i["severity"]) for i in issues] == [
        ("deferred_out_of_slice_reference", "warning")
    ] * len(issues)
    assert len(issues) == result["counts"]["deferred_references"] == 2
    assert _count(output, "variable_same_as") == 0
    assert _count(output, "variable_replaced_by") == 0
