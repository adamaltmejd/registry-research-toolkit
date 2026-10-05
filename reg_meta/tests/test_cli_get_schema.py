"""CLI `get schema`: editions, year filters, column filters and display modes."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cli_test_support import build_cli_source
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_open_year_bounds_keep_every_window_on_their_side(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = build_cli_source(tmp_path, "cli-periods")
    argv = ["--db", db, "--format", "json", "get", "schema", "--register", "Terms"]

    def windows(years: str) -> list[list[str]]:
        assert run([*argv, "--years", years]) == 0
        output = json.loads(capsys.readouterr().out)
        return [
            [version["valid_from"], version["valid_to"]]
            for variant in output["variants"]
            for version in variant["versions"]
        ]

    assert windows("-2019") == [
        ["2015-01-01", "2015-06-30"],
        ["2015-07-01", "2015-12-31"],
        ["2018-04-01", "2018-06-30"],
        ["2019-03-01", "2019-03-31"],
    ]
    assert windows("2019-") == [
        ["2019-03-01", "2019-03-31"],
        ["2020-01-01", "2020-12-31"],
        ["2021-01-01", "9999-12-31"],
    ]
