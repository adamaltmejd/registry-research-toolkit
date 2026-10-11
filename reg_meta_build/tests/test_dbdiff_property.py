"""Property cases for `dbdiff`'s catalog id projection, asserted on its CLI (exit
code and `--json`): a catalog whose ids are renumbered without a content change
compares identical, and moving any one reference to another existing row is a
difference.

The regression these pin is a false "identical": a changed relationship that the
id projection hides. The databases carry the real catalog DDL (`db.DDL`, less the
build-only tables) with a few small tables filled: nested natural keys (variable
→ register → provider), a primary key that is a reference
(variable_search_text.variable_id), NULL and dangling references, and keys made
of text equal to the reference tags. Renumbering and moves follow the schema's
own FOREIGN KEY declarations (plus variable_search_text's two undeclared
references), not dbdiff's id map, so the two must agree.
"""

from __future__ import annotations

import functools
import io
import json
import shutil
import sqlite3
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

from hypothesis import assume, given, settings, strategies as st
from reg_meta_build.db import DDL
from reg_meta_build.dbdiff import main

# The tables filled, each with its own id column.
_IDS = {
    "provider": "provider_id",
    "register": "register_id",
    "register_variant": "register_variant_id",
    "variable": "variable_id",
    "value_code": "code_id",
}
_TABLES = (*_IDS, "code_variable_map", "variable_alias", "variable_search_text")
# Declared without a FOREIGN KEY clause (db.py's search table).
_UNDECLARED = {
    ("variable_search_text", "variable_id"): "variable",
    ("variable_search_text", "register_id"): "register",
}
# Text equal to the reference tags and to numeric spellings of an id.
_TEXT = ("k", "d", "dangling id", '["k",1]', "1", "01", "", "scb")


@functools.cache
def _empty_catalog() -> Path:
    """A catalog with the shipped schema and no rows, made once per process."""
    path = Path(tempfile.mkdtemp(prefix="dbdiff-property-")) / "empty.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(DDL)
        for table in (
            "variable_alias_build",
            "variable_instance",
            "classification_candidate",
            "unika_summary",
        ):
            conn.execute(f"DROP TABLE {table}")
        conn.commit()
    finally:
        conn.close()
    return path


@functools.cache
def _references() -> dict[tuple[str, str], str]:
    """(table, column) -> referenced table, for the filled tables, read from the
    schema's FOREIGN KEY declarations."""
    conn = sqlite3.connect(_empty_catalog())
    try:
        refs = dict(_UNDECLARED)
        for table in _TABLES:
            for fk in conn.execute(f"PRAGMA foreign_key_list({table})"):
                assert fk[4] == _IDS[fk[2]], fk  # each points at the parent's id
                refs[table, fk[3]] = fk[2]
        return refs
    finally:
        conn.close()


@st.composite
def _catalogs(draw: st.DrawFn) -> dict:
    # Slugs unique across the catalog keep every natural key unique, so no move
    # can turn a difference into a refusal; one may be NULL.
    slugs = iter(
        draw(
            st.lists(
                st.sampled_from(_TEXT) | st.text(max_size=3),
                unique=True,
                min_size=12,
                max_size=12,
            )
        )
    )
    ids = {table: list(range(1, draw(st.integers(1, 3)) + 1)) for table in _IDS}

    def ref(table: str, dangling: bool = False) -> st.SearchStrategy:
        choices = st.sampled_from(ids[table])
        return choices | st.just(max(ids[table]) + 1) if dangling else choices

    rows: dict[str, list[dict]] = {
        "provider": [
            {"provider_id": i, "slug": next(slugs), "name": "p"}
            for i in ids["provider"]
        ],
        "register": [
            {
                "register_id": i,
                "provider_id": draw(ref("provider")),
                "name": "r",
                "slug": next(slugs),
            }
            for i in ids["register"]
        ],
        "register_variant": [
            {
                "register_variant_id": i,
                "register_id": draw(ref("register")),
                "slug": None if i == 1 and draw(st.booleans()) else next(slugs),
            }
            for i in ids["register_variant"]
        ],
        "variable": [
            {
                "variable_id": i,
                "register_id": draw(ref("register")),
                "provider_key": "v",
                "slug": next(slugs),
                "source_register_id": draw(st.none() | ref("register", True)),
            }
            for i in ids["variable"]
        ],
        "value_code": [
            {"code_id": i, "code": code, "label": label}
            for i, (code, label) in zip(
                ids["value_code"],
                draw(
                    st.lists(
                        st.tuples(st.sampled_from(_TEXT), st.sampled_from(_TEXT)),
                        unique=True,
                        min_size=len(ids["value_code"]),
                        max_size=len(ids["value_code"]),
                    )
                ),
                strict=True,
            )
        ],
        "code_variable_map": [
            {"code_id": c, "variable_id": v}
            for c, v in draw(
                st.lists(
                    st.tuples(ref("value_code"), ref("variable")),
                    unique=True,
                    max_size=4,
                )
            )
        ],
        "variable_alias": [
            {"variable_id": v, "register_variant_id": rv, "delivery_column_name": n}
            for v, rv, n in draw(
                st.lists(
                    st.tuples(
                        ref("variable"),
                        ref("register_variant"),
                        st.sampled_from(_TEXT),
                    ),
                    unique=True,
                    max_size=4,
                )
            )
        ],
        "variable_search_text": [
            {
                "variable_id": v,
                "register_id": draw(ref("register", True)),
                "provider_key": "v",
                "delivery_column_names": "[]",
            }
            for v in draw(st.lists(ref("variable"), unique=True, max_size=3))
        ],
    }
    renumber = {table: draw(st.permutations(ids[table])) for table in _IDS}
    moves = [
        (table, index, column, new)
        for (table, column), parent in sorted(_references().items())
        for index, row in enumerate(rows[table])
        for new in ids[parent]
        if new != row[column]
    ]
    move = draw(st.sampled_from(moves)) if moves else None
    return {"rows": rows, "renumber": renumber, "move": move}


def _write(path: Path, rows: dict[str, list[dict]]) -> None:
    """``rows`` into a copy of the empty catalog; False when a unique constraint
    refuses them (a move onto a row's twin), which the example then discards."""
    shutil.copyfile(_empty_catalog(), path)
    conn = sqlite3.connect(path)
    try:
        for table in _TABLES:
            for row in rows[table]:
                cols = ", ".join(row)
                marks = ", ".join("?" * len(row))
                conn.execute(
                    f"INSERT INTO {table} ({cols}) VALUES ({marks})",
                    tuple(row.values()),
                )
        conn.commit()
    except sqlite3.IntegrityError:
        assume(False)
    finally:
        conn.close()


def _renumbered(rows: dict[str, list[dict]], renumber: dict) -> dict:
    """``rows`` with each id table's ids permuted, and every declared reference
    following; a dangling id is no row's and stays as it is."""
    maps = {
        table: dict(zip(sorted(order), order, strict=True))
        for table, order in renumber.items()
    }
    owners = {(table, column): table for table, column in _IDS.items()}
    owners |= _references()
    out = {}
    for table, table_rows in rows.items():
        out[table] = [
            {
                column: maps[owners[table, column]].get(value, value)
                if (table, column) in owners and value is not None
                else value
                for column, value in row.items()
            }
            for row in table_rows
        ]
    return out


def _moved(rows: dict[str, list[dict]], move: tuple) -> dict:
    table, index, column, new = move
    out = {name: [dict(row) for row in table_rows] for name, table_rows in rows.items()}
    out[table][index][column] = new
    return out


def _refuse_constant(name: str) -> object:
    raise ValueError(f"--json printed {name}, which is not JSON")


def _dbdiff(a: Path, b: Path) -> tuple[int, dict]:
    out = io.StringIO()
    with redirect_stdout(out):
        code = main(["--json", str(a), str(b)])
    assert code in (0, 1), (code, out.getvalue())
    return code, json.loads(out.getvalue(), parse_constant=_refuse_constant)


@settings(deadline=None)
@given(_catalogs())
def test_renumbering_is_identical_and_any_moved_reference_differs(spec: dict) -> None:
    assume(spec["move"] is not None)
    with tempfile.TemporaryDirectory() as tmp:
        base, renumbered, moved = (Path(tmp) / f"{n}.db" for n in "abc")
        _write(base, spec["rows"])
        _write(renumbered, _renumbered(spec["rows"], spec["renumber"]))
        _write(moved, _moved(spec["rows"], spec["move"]))

        code, report = _dbdiff(base, renumbered)
        assert report["natural_keys"], report
        assert code == 0, report
        code, report = _dbdiff(base, moved)
        assert code == 1, (spec["move"], report)
