"""CLI output formats: envelope, human-readable rendering and display limits."""

from __future__ import annotations

import pytest
from cli_test_support import build_cli_source
from reg_meta.cli import run


@pytest.fixture(scope="module")
def limits_db(tmp_path_factory: pytest.TempPathFactory) -> str:
    return build_cli_source(tmp_path_factory.mktemp("limits"), "cli-display-limits")


@pytest.fixture(scope="module")
def periods_db(tmp_path_factory: pytest.TempPathFactory) -> str:
    return build_cli_source(tmp_path_factory.mktemp("periods"), "cli-periods")


def test_default_format_lists_few_rows_and_tabulates_many(
    limits_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    get = ["--db", limits_db, "get"]
    assert run([*get, "varinfo", "Category", "--register", "Wide"]) == 0
    one_row = capsys.readouterr().out
    assert "  register_id  " in one_row
    assert "---" not in one_row
    assert (
        run([*get, "values", "Category", "--register", "Wide", "--year", "2020"]) == 0
    )
    many_rows = capsys.readouterr().out.splitlines()
    assert many_rows[0].split() == ["code", "label"]
    assert set(many_rows[1]) <= {"-", " "}


def test_table_rows_past_display_cap_are_cut_with_hint(
    limits_db: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COLUMNS", "200")
    argv = ["--db", limits_db, "--format", "table", "get", "values", "Category"]
    assert run([*argv, "--register", "Wide", "--year", "2020"]) == 0
    captured = capsys.readouterr()
    rows = captured.out.splitlines()[2:]
    assert [row.split()[0] for row in rows] == [f"{i:03d}" for i in range(1, 101)]
    assert "Table view truncated 1 rows" in captured.err


@pytest.mark.parametrize(
    "argv",
    [["get", "schema", "--register", "Terms"], ["get", "varinfo", "Grade"]],
    ids=["schema", "varinfo"],
)
def test_period_column_renders_each_window_at_its_coarsest_token(
    periods_db: str, argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(["--db", periods_db, "--format", "list", *argv]) == 0
    periods = [
        line.split()[1]
        for line in capsys.readouterr().out.splitlines()
        if line.split()[:1] == ["period"]
    ]
    assert periods == [
        "VT2015",
        "HT2015",
        "2018-Q2",
        "2019-03",
        "2020",
        "2021-01-01..9999-12-31",
    ]
