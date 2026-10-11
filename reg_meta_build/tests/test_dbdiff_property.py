"""Property cases for `dbdiff`'s natural-key projection, asserted on its CLI (exit
code and `--json`): ids reassigned without a content change compare identical,
and moving any one reference to another existing row is a difference.

The regression these pin is a false "identical": a changed relationship that the
id projection hides. Small parent/child databases mix the shapes that have hidden
one or broken the report: NULL and dangling references, parents equal but for
their id, a primary key that is a foreign key, a nullable one-column key, text
equal to the reference tags, and infinite REALs inside a key.
"""

from __future__ import annotations

import io
import json
import math
import sqlite3
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

from hypothesis import assume, example, given, settings, strategies as st
from reg_meta_build.dbdiff import main

_SCHEMA = """
CREATE TABLE parent (id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
CREATE TABLE measure (id INTEGER PRIMARY KEY, w REAL);
-- A nullable one-column natural key that is itself a reference.
CREATE TABLE child2 (id INTEGER PRIMARY KEY, p INTEGER UNIQUE REFERENCES parent(id));
-- A primary key that is a foreign key.
CREATE TABLE child (id INTEGER PRIMARY KEY REFERENCES parent(id), note TEXT);
CREATE TABLE grand (
  child2_id INTEGER REFERENCES child2(id),
  measure_id INTEGER REFERENCES measure(id),
  parent_id INTEGER REFERENCES parent(id),
  label TEXT
);
"""

# Where each reference points: (table, column) -> referenced table.
_LINKS = {
    ("child2", "p"): "parent",
    ("child", "id"): "parent",
    ("grand", "child2_id"): "child2",
    ("grand", "measure_id"): "measure",
    ("grand", "parent_id"): "parent",
}
# Columns whose values must stay unique, so a move needs a free target.
_UNIQUE = {("child2", "p"), ("child", "id")}
_COLUMNS = {
    "parent": ("id", "a", "b"),
    "measure": ("id", "w"),
    "child2": ("id", "p"),
    "child": ("id", "note"),
    "grand": ("child2_id", "measure_id", "parent_id", "label"),
}

# Text equal to the reference tags, and to the dangling marker dbdiff once used.
_TEXT = st.one_of(
    st.sampled_from(["k", "r", "d", "dangling id", '["k",1]', "", "x"]),
    st.text(max_size=3),
)
_REAL = st.sampled_from([1.0, 2.5, math.inf, -math.inf])


@st.composite
def _catalogs(draw: st.DrawFn) -> dict:
    n_parent = draw(st.integers(1, 4))
    parent_ids = list(range(1, n_parent + 1))
    # Ids no row holds: a reference to one is dangling.
    dangling = [n_parent + 1, n_parent + 2]
    parent = [
        (i, draw(st.none() | _TEXT), draw(st.none() | st.integers(0, 7)))
        for i in parent_ids
    ]
    measure_ids = list(range(1, draw(st.integers(1, 3)) + 1))
    measure = [(i, draw(st.none() | _REAL)) for i in measure_ids]
    # Distinct parents, then at most one NULL: two NULLs leave no unique key.
    p_values: list[int | None] = draw(
        st.lists(st.sampled_from(parent_ids), unique=True, max_size=3)
    )
    if draw(st.booleans()):
        p_values.append(None)
    p_values = draw(st.permutations(p_values))
    child2 = [(i + 1, p) for i, p in enumerate(p_values)]
    child2_ids = [row[0] for row in child2]
    child_ids = draw(st.lists(st.sampled_from(parent_ids), unique=True, max_size=3))
    child = [(i, draw(_TEXT)) for i in child_ids]

    def ref(ids: list[int]) -> st.SearchStrategy:
        return st.none() | st.sampled_from(ids + dangling)

    grand = [
        (
            draw(ref(child2_ids)),
            draw(ref(measure_ids)),
            draw(ref(parent_ids)),
            draw(_TEXT),
        )
        for _ in range(draw(st.integers(1, 4)))
    ]
    tables = {
        "parent": parent,
        "measure": measure,
        "child2": child2,
        "child": child,
        "grand": grand,
    }
    ids = {"parent": parent_ids, "measure": measure_ids, "child2": child2_ids}
    renumber = {name: draw(st.permutations(ids[name])) for name in ids}
    # One reference cell and another existing row it could point to instead.
    moves = []
    for (table, column), target in _LINKS.items():
        position = _COLUMNS[table].index(column)
        taken = {row[position] for row in tables[table]}
        for index, row in enumerate(tables[table]):
            for new in ids[target]:
                free = (table, column) not in _UNIQUE or new not in taken
                if new != row[position] and free:
                    moves.append((table, index, column, new))
    move = draw(st.sampled_from(moves)) if moves else None
    return {"tables": tables, "renumber": renumber, "move": move}


def _write(path: Path, tables: dict) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_SCHEMA)
        for name, rows in tables.items():
            marks = ", ".join("?" * len(_COLUMNS[name]))
            conn.executemany(f"INSERT INTO {name} VALUES ({marks})", rows)
        conn.commit()
    finally:
        conn.close()


def _renumbered(tables: dict, renumber: dict) -> dict:
    """``tables`` with each id table's ids permuted and every reference to them
    following; a dangling id is no row's and stays as it is."""
    maps = {
        name: dict(zip(sorted(order), order, strict=True))
        for name, order in renumber.items()
    }
    out = {}
    for table, rows in tables.items():
        new_rows = []
        for row in rows:
            values = list(row)
            for position, column in enumerate(_COLUMNS[table]):
                target = _LINKS.get((table, column))
                if target is None and column == "id" and table in maps:
                    target = table
                if target is not None and values[position] in maps[target]:
                    values[position] = maps[target][values[position]]
            new_rows.append(tuple(values))
        out[table] = new_rows
    return out


def _moved(tables: dict, move: tuple) -> dict:
    table, index, column, new = move
    out = {name: list(rows) for name, rows in tables.items()}
    row = list(out[table][index])
    row[_COLUMNS[table].index(column)] = new
    out[table][index] = tuple(row)
    return out


def _refuse_constant(name: str) -> object:
    raise ValueError(f"--json printed {name}, which is not JSON")


def _dbdiff(a: Path, b: Path) -> tuple[int, dict]:
    out = io.StringIO()
    with redirect_stdout(out):
        code = main(["--json", str(a), str(b)])
    assert code in (0, 1), (code, out.getvalue())
    return code, json.loads(out.getvalue(), parse_constant=_refuse_constant)


_DANGLING_LOOKALIKE = {
    "tables": {
        "parent": [(1, "dangling id", 3)],
        "measure": [(1, 1.0)],
        "child2": [],
        "child": [],
        "grand": [(None, None, 3, "x")],
    },
    "renumber": {"parent": [1], "measure": [1], "child2": []},
    "move": ("grand", 0, "parent_id", 1),
}
_NULL_ONE_COLUMN_KEY = {
    "tables": {
        "parent": [(1, "x", 0)],
        "measure": [(1, 1.0)],
        "child2": [(1, None)],
        "child": [],
        "grand": [(1, None, None, "x")],
    },
    "renumber": {"parent": [1], "measure": [1], "child2": [1]},
    "move": ("grand", 0, "parent_id", 1),
}
_INFINITE_KEY = {
    "tables": {
        "parent": [(1, "x", 0)],
        "measure": [(1, math.inf)],
        "child2": [],
        "child": [],
        "grand": [(None, 1, None, "x")],
    },
    "renumber": {"parent": [1], "measure": [1], "child2": []},
    "move": ("grand", 0, "parent_id", 1),
}


@settings(deadline=None)
# A dangling reference to id 3 must not equal a parent whose key is
# ('dangling id', 3): moving the reference onto that parent read identical.
@example(_DANGLING_LOOKALIKE)
# A one-column key whose only column is a NULL reference: the key was SQL NULL,
# not JSON null, and identical databases exited 2.
@example(_NULL_ONE_COLUMN_KEY)
# An infinite REAL inside a reported key crashed `--json`.
@example(_INFINITE_KEY)
@given(_catalogs())
def test_renumbering_is_identical_and_any_moved_reference_differs(spec: dict) -> None:
    assume(spec["move"] is not None)
    with tempfile.TemporaryDirectory() as tmp:
        base, renumbered, moved = (Path(tmp) / f"{n}.db" for n in "abc")
        _write(base, spec["tables"])
        _write(renumbered, _renumbered(spec["tables"], spec["renumber"]))
        _write(moved, _moved(spec["tables"], spec["move"]))

        code, report = _dbdiff(base, renumbered)
        # Raw-id tables compare their ids, so renumbering them may differ.
        if not report["raw_id_tables"]:
            assert code == 0, report
        code, report = _dbdiff(base, moved)
        assert code == 1, (spec["move"], report)
