"""CLI tests for the maintainer search-eval runner (``scripts/run_search_eval.py``).

The runner reads its eval set from the hard-wired ``<backend>/search_eval.toml``, so
each test copies the script into a tmp ``<root>/scripts/`` layout next to a
test-written ``<root>/search_eval.toml`` and runs it as a subprocess against the
synthetic catalog DB, asserting on exit code, stdout and stderr.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_search_eval.py"

_HEADER = ["query", "group", "intended", "expect", "rank", "returned", "has_more"]


def _case(query: str, group: str, intended: str = "scb/lisa") -> str:
    return (
        f'[[case]]\nquery = "{query}"\ngroup = "{group}"\n'
        f'intended = "{intended}"\nexpect = "hit"\n\n'
    )


def _run(
    tmp_path: Path, cases: str, *args: str, home: Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a tmp copy of the runner over ``cases``; REG_META_DB is unset so only
    ``--db`` (or the platform default) can locate the catalog."""
    root = tmp_path / "runner"
    (root / "scripts").mkdir(parents=True, exist_ok=True)
    shutil.copy(_SCRIPT, root / "scripts" / _SCRIPT.name)
    (root / "search_eval.toml").write_text(cases, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "REG_META_DB"}
    if home is not None:
        env["HOME"] = str(home)
    return subprocess.run(
        [sys.executable, str(root / "scripts" / _SCRIPT.name), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def _rows(stdout: str) -> list[list[str]]:
    """The report table's header and data rows (up to the blank line), split on
    whitespace; the separator row is dropped."""
    table = stdout.split("\n\n", 1)[0].splitlines()
    return [table[0].split(), *(line.split() for line in table[2:])]


def test_db_file_path_is_used_directly(tmp_path: Path, catalog_db: Path) -> None:
    """``--db <file>`` reads that file and prints the report."""
    result = _run(tmp_path, _case("lisa", "register"), "--db", str(catalog_db))
    assert result.returncode == 0, result.stderr
    assert _rows(result.stdout)[1][:2] == ["lisa", "register"]


def test_db_directory_resolves_reg_meta_db_inside(
    tmp_path: Path, catalog_db: Path
) -> None:
    """``--db <dir>`` resolves ``<dir>/reg_meta.db``; a dir without one exits 2
    naming the path it looked for."""
    empty = tmp_path / "empty"
    empty.mkdir()
    missing = _run(tmp_path, _case("lisa", "register"), "--db", str(empty))
    assert missing.returncode == 2
    assert str(empty / "reg_meta.db") in missing.stderr

    found = _run(tmp_path, _case("lisa", "register"), "--db", str(catalog_db.parent))
    assert found.returncode == 0, found.stderr


def test_db_tilde_file_path_is_expanded(tmp_path: Path, catalog_db: Path) -> None:
    """A ``~``-prefixed file path resolves against ``$HOME``."""
    result = _run(
        tmp_path,
        _case("lisa", "register"),
        "--db",
        "~/reg_meta.db",
        home=catalog_db.parent,
    )
    assert result.returncode == 0, result.stderr


def test_supported_groups_each_report_a_row(tmp_path: Path, catalog_db: Path) -> None:
    """register, variable and classification cases all run, one row each."""
    cases = (
        _case("lisa", "register")
        + _case("kön", "variable")
        + _case("svensk", "classification")
    )
    result = _run(tmp_path, cases, "--db", str(catalog_db))
    assert result.returncode == 0, result.stderr
    assert [row[:2] for row in _rows(result.stdout)[1:]] == [
        ["lisa", "register"],
        ["kön", "variable"],
        ["svensk", "classification"],
    ]


def test_value_group_fails_fast(tmp_path: Path, catalog_db: Path) -> None:
    """A ``value`` case is rejected: code rows carry no FQID, so it could never hit."""
    result = _run(tmp_path, _case("kvinna", "value"), "--db", str(catalog_db))
    assert result.returncode != 0
    assert "unsupported eval group 'value'" in result.stderr


def test_unknown_group_names_supported_set(tmp_path: Path, catalog_db: Path) -> None:
    result = _run(tmp_path, _case("lisa", "bogus"), "--db", str(catalog_db))
    assert result.returncode != 0
    assert "'bogus'" in result.stderr
    assert "register | variable | classification" in result.stderr


def test_report_shows_page_size_and_has_more_never_total(
    tmp_path: Path, catalog_db: Path
) -> None:
    """With more hits than ``--limit``, the row reports the returned page size and
    ``has_more = true``; no exact total is printed."""
    result = _run(
        tmp_path,
        _case("svensk", "classification"),
        "--db",
        str(catalog_db),
        "--limit",
        "1",
    )
    assert result.returncode == 0, result.stderr
    assert "total" not in result.stdout
    header, row = _rows(result.stdout)
    assert header == [*_HEADER, "status"]
    returned, has_more = row[_HEADER.index("returned")], row[_HEADER.index("has_more")]
    assert (returned, has_more) == ("1", "true")
