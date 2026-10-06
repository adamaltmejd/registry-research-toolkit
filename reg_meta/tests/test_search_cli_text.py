"""`reg-meta search --format list` text: which columns a result set renders and
how classification, register and succession rows fill them.

The source registers `Utbildningsregistret` (purpose naming codes `S20` and
`Y10`), classification `sun2020` (codes `S20`, `Z100`) and the succession chain
`ea..ed -> ec` (codes `Y10`, `Z10`), all surfaced through code containment.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reader_artifacts import build_reader_artifact
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="module")
def db_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("classification-editions")
    return build_reader_artifact(
        directory, "reader/classification-editions", "catalog"
    ).parent


def _records(db_dir: Path, capsys: pytest.CaptureFixture[str], *argv: str):
    """Run one list-format search and parse its records (blank-line separated
    `column  value` lines; the trailing hint lines are not records)."""
    assert run(["--db", str(db_dir), "--format", "list", "search", *argv]) == 0
    records = []
    for block in capsys.readouterr().out.strip().split("\n\n"):
        lines = [line.strip() for line in block.splitlines()]
        if lines and not lines[0].startswith("hint:"):
            records.append(
                {
                    key: value.strip()
                    for key, _, value in (l.partition(" ") for l in lines)
                }
            )
    return records


def test_register_row_fills_the_register_column(db_dir, capsys) -> None:
    [record] = _records(db_dir, capsys, "--query", "Utbildningsregistret")

    assert record["type"] == "register"
    assert record["register_name"] == "Utbildningsregistret"


def test_classification_row_fills_the_generic_columns_in_a_mixed_table(
    db_dir, capsys
) -> None:
    records = _records(db_dir, capsys, "--query", "S20", "--field", "description")

    assert {r["type"] for r in records} == {"register", "classification"}
    [row] = [r for r in records if r["type"] == "classification"]
    assert row["register_name"] == "SUN2020"
    assert row["variable_name"] == "Svensk utbildningsnomenklatur"


def test_succession_row_shows_its_edition_count_in_a_mixed_table(
    db_dir, capsys
) -> None:
    records = _records(db_dir, capsys, "--query", "Y10", "--field", "description")

    [row] = [r for r in records if r["type"] == "classification_succession"]
    assert row["register_name"] == "EC"
    assert row["variable_name"] == "Yrkesgrupp edition C (4 editions)"
    assert row["n_editions"] == "4"


def test_pure_succession_table_shows_the_fqid_and_edition_count(db_dir, capsys) -> None:
    [record] = _records(db_dir, capsys, "--query", "Y10", "--type", "classification")

    assert record == {
        "short_name": "EC",
        "classification_name": "Yrkesgrupp edition C",
        "fqid": "class/ec",
        "n_editions": "4",
    }


def test_classification_and_succession_table_shows_both_fqids(db_dir, capsys) -> None:
    records = _records(db_dir, capsys, "--query", "Z10", "--type", "classification")

    assert [r["fqid"] for r in records] == ["class/ec", "class/sun2020"]
    assert [r["n_editions"] for r in records] == ["4", ""]
