"""Concept groups on the catalog/CLI surfaces (#322 / #325).

`get groups` lists a register's families with member facets, `get schema`
annotates member columns inline, and the CLI renders group rows in its
`--format json` envelope and `--format list` text.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reader_artifacts import build_reader_artifact
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path


# ── CLI list text over a readable source ─────────────────────────────────────


@pytest.fixture(scope="module")
def group_cli_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_reader_artifact(
        tmp_path_factory.mktemp("concept-group-cli"),
        "reader/concept-group-cli",
        "catalog",
    ).parent


class TestCliListDisplay:
    def test_search_group_rows_render_with_counts(
        self, group_cli_db: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        capsys.readouterr()
        code = run(
            [
                "--db",
                str(group_cli_db),
                "--format",
                "list",
                "search",
                "--query",
                "Lönesumma",
                "--field",
                "varname",
            ]
        )
        assert code == 0
        # Pure-group results use the dedicated column set: identity + counts.
        assert capsys.readouterr().out == (
            "  group_key      payroll\n"
            "  group_label    Lönesumma per månad\n"
            "  source         curated\n"
            "  register_name  Example\n"
            "  matched        3\n"
            "  members        3\n"
        )

    def test_search_group_row_projects_match_label_in_mixed_results(
        self, group_cli_db: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        capsys.readouterr()
        code = run(
            [
                "--db",
                str(group_cli_db),
                "--format",
                "list",
                "search",
                "--query",
                "Lön",
                "--field",
                "varname",
            ]
        )
        assert code == 0
        # Mixed with a leaf, the group row shares the leaf columns and projects its
        # match count into the name column.
        assert capsys.readouterr().out == (
            "  type           group\n"
            "  register_name  Example\n"
            "  var_id         \n"
            "  variable_name  Lönesumma per månad (3/3 members matched)\n"
            "  group          payroll\n"
            "\n"
            "  type           varname\n"
            "  register_name  Example\n"
            "  var_id         60\n"
            "  variable_name  Lön totalt\n"
            "  group          \n"
        )

    def test_get_groups_list_renders_one_record_per_group(
        self, group_cli_db: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        capsys.readouterr()
        code = run(
            [
                "--db",
                str(group_cli_db),
                "--format",
                "list",
                "get",
                "groups",
                "scb/example",
            ]
        )
        assert code == 0
        # #819: the axes column shows the authored axis LABEL ('månad'), not the
        # stable match key ('month').
        assert capsys.readouterr().out == (
            "  register   Example\n"
            "  group_key  payroll\n"
            "  label      Lönesumma per månad\n"
            "  source     curated\n"
            "  axes       månad\n"
            "  members    3\n"
        )
