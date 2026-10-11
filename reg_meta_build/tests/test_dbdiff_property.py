"""Property cases for `dbdiff`'s catalog id projection, asserted on its CLI (exit
code and `--json`): a catalog whose ids are renumbered without a content change
compares identical, and moving any one reference to another existing row is a
difference.

The regression these pin is a false "identical": a changed relationship that the
id projection hides. The catalog is the `_classified` CLI artifact, built from its
readable source (`cases/cli/_classified/`); Hypothesis permutes its ids and moves
its references on copies. Both follow the artifact's own FOREIGN KEY declarations
(plus variable_search_text's two undeclared references), not dbdiff's id map, so
the two must agree.
"""

from __future__ import annotations

import functools
import io
import json
import shutil
import sqlite3
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from reg_meta_build.dbdiff import main
from test_cli_cases import CASES, Artifacts

if TYPE_CHECKING:
    from _build_case_runner import PreparedCache

# Declared without a FOREIGN KEY clause (db.py's search table).
_UNDECLARED = {
    ("variable_search_text", "variable_id"): ("variable", "variable_id"),
    ("variable_search_text", "register_id"): ("register", "register_id"),
}
# Added to every id while a permutation is applied, so no step collides.
_OFFSET = 1 << 40


@pytest.fixture(scope="session")
def classified(prepared_cache: PreparedCache) -> Path:
    """The `_classified` artifact's catalog, built once per session."""
    artifact = Artifacts(prepared_cache).get(CASES / "_classified", None)
    return artifact.db_dir / "reg_meta.db"


@functools.cache
def _ids(db: Path) -> dict[tuple[str, str], tuple[tuple[str, str], ...]]:
    """Each id column of ``db`` -> every column holding its ids, the id column
    first, read from the schema: a single-column INTEGER primary key that is no
    foreign key, or an INTEGER column a foreign key targets, and the foreign keys
    (followed through composite ones) that reach it."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        tables = [
            name
            for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts_%' "
                "AND sql NOT LIKE 'CREATE VIRTUAL%'"
            )
        ]
        types = {
            (table, c[1]): c[2]
            for table in tables
            for c in conn.execute(f'PRAGMA table_info("{table}")')
        }
        edges = dict(_UNDECLARED)
        for table in tables:
            for fk in conn.execute(f'PRAGMA foreign_key_list("{table}")'):
                assert fk[4] is not None, fk  # the catalog names its targets
                edges[table, fk[3]] = (fk[2], fk[4])
        owners: set[tuple[str, str]] = set()
        for table in tables:
            pk = [c for c in conn.execute(f'PRAGMA table_info("{table}")') if c[5]]
            if (
                len(pk) == 1
                and pk[0][2] == "INTEGER"
                and (table, pk[0][1]) not in edges
            ):
                owners.add((table, pk[0][1]))
        owners |= {
            target
            for target in edges.values()
            if target not in edges and types[target] == "INTEGER"
        }
        holders = {owner: [owner] for owner in owners}
        for columns in holders.values():
            grown = True
            while grown:
                grown = False
                for source, target in sorted(edges.items()):
                    if target in columns and source not in columns:
                        columns.append(source)
                        grown = True
        return {owner: tuple(columns) for owner, columns in holders.items()}
    finally:
        conn.close()


@functools.cache
def _facts(db: Path) -> tuple[dict, list]:
    """``db``'s id values per id column, and every reference cell a move may
    change: (table, row key columns, row key, column, current value, id column)."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        values = {
            owner: sorted(
                v for (v,) in conn.execute(f'SELECT "{owner[1]}" FROM "{owner[0]}"')
            )
            for owner in _ids(db)
        }
        cells = []
        for owner, columns in sorted(_ids(db).items()):
            for table, column in columns[1:]:
                try:  # a WITHOUT ROWID table has none; its primary key names a row
                    conn.execute(f'SELECT rowid FROM "{table}" LIMIT 0')
                    key: tuple[str, ...] = ("rowid",)
                except sqlite3.OperationalError:
                    key = tuple(
                        c[1]
                        for c in sorted(
                            conn.execute(f'PRAGMA table_info("{table}")'),
                            key=lambda c: c[5],
                        )
                        if c[5]
                    )
                select = ", ".join(f'"{k}"' for k in key)
                for *row_key, value in conn.execute(
                    f'SELECT {select}, "{column}" FROM "{table}"'
                ):
                    cells.append((table, key, tuple(row_key), column, value, owner))
        return values, cells
    finally:
        conn.close()


@st.composite
def _changes(draw: st.DrawFn, db: Path) -> dict:
    values, cells = _facts(db)
    renumber = {owner: draw(st.permutations(ids)) for owner, ids in values.items()}
    table, key, row_key, column, current, owner = draw(st.sampled_from(cells))
    new = draw(st.sampled_from(values[owner]).filter(lambda v: v != current))
    return {"renumber": renumber, "move": (table, key, row_key, column, new)}


def _renumber(db: Path, renumber: dict) -> None:
    """Permute each id column's values in ``db``, every column holding them
    following; a value no row holds stays as it is."""
    conn = sqlite3.connect(db)
    try:
        for owner, order in renumber.items():
            conn.execute("CREATE TEMP TABLE m (old INTEGER PRIMARY KEY, new INTEGER)")
            conn.executemany(
                "INSERT INTO m VALUES (?, ?)", zip(sorted(order), order, strict=True)
            )
            for table, column in _ids(db)[owner]:
                conn.execute(
                    f'UPDATE "{table}" SET "{column}" = "{column}" + {_OFFSET}'
                )
            for table, column in _ids(db)[owner]:
                conn.execute(
                    f'UPDATE "{table}" SET "{column}" = COALESCE('
                    f'(SELECT new FROM m WHERE old = "{column}" - {_OFFSET}), '
                    f'"{column}" - {_OFFSET})'
                )
            conn.execute("DROP TABLE m")
        conn.commit()
    finally:
        conn.close()


def _move(db: Path, move: tuple) -> None:
    table, key, row_key, column, new = move
    where = " AND ".join(f'"{k}" = ?' for k in key)
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            f'UPDATE "{table}" SET "{column}" = ? WHERE {where}', (new, *row_key)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        assume(False)  # a unique or CHECK constraint refuses the move
    finally:
        conn.close()


def _refuse_constant(name: str) -> object:
    raise ValueError(f"--json printed {name}, which is not JSON")


def _dbdiff(a: Path, b: Path) -> tuple[int, dict | None, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = main(["--json", str(a), str(b)])
    if code == 2:
        return code, None, err.getvalue()
    assert code in (0, 1), (code, err.getvalue())
    return code, json.loads(out.getvalue(), parse_constant=_refuse_constant), ""


def test_renumbering_is_identical_and_any_moved_reference_differs(
    classified: Path,
) -> None:
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(_changes(classified))
    def check(change: dict) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            renumbered, moved = Path(tmp) / "renumbered.db", Path(tmp) / "moved.db"
            shutil.copyfile(classified, renumbered)
            shutil.copyfile(classified, moved)
            _renumber(renumbered, change["renumber"])
            _move(moved, change["move"])

            code, report, error = _dbdiff(classified, renumbered)
            assert code == 0 and report is not None, (code, error, report)
            assert report["natural_keys"], report
            code, report, error = _dbdiff(classified, moved)
            # A move that breaks a natural key is refused (exit 2), never hidden.
            assume(not (code == 2 and "is not unique" in error))
            assert code == 1, (change["move"], code, error, report)

    check()
