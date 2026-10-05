"""CLI `search`: result types, filters, pagination and code matching."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cli_test_support import build_cli_source
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_code_matching_needs_a_digit_and_three_characters(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Codes ZQX9 and Q71 have labels unrelated to their code text."""
    db = build_cli_source(tmp_path, "cli-code-shape")

    def codes(query: str) -> list[str]:
        assert run(["--db", db, "--format", "json", "search", "--query", query]) == 0
        results = json.loads(capsys.readouterr().out)["results"]
        return [row["code"] for row in results if row["type"] == "code"]

    assert codes("ZQX9") == ["ZQX9"]
    assert codes("Q71") == ["Q71"]
    assert codes(" ZQX9 ") == ["ZQX9"]
    assert codes("ZQX") == []
    assert codes("Q7") == []
    assert codes(" Q7 ") == []
