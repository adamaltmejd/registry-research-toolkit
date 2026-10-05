"""Concept-group candidate fixtures shared by the generator and worklist tests.

The helpers keep their original bodies; the test modules import them under their
original private names so the moved tests stay byte-identical.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_variable, build_slugged_db

if TYPE_CHECKING:
    import sqlite3


def base_db() -> sqlite3.Connection:
    """An scb/lisa register with no variables and no curated classification — the
    blank canvas each test seeds with `add_variable`."""
    return build_slugged_db(variable=None, version=None, classification=None)


def add_family(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    stem: str,
    suffixes: list[int],
    name: str,
    var_id_base: int,
) -> None:
    """Add a digit-suffixed slug family (`<stem><suffix>`) all sharing one `name`
    (a strong, foldable family). `var_id` is unique per member."""
    for i, suffix in enumerate(suffixes):
        add_variable(
            conn,
            register_id=register_id,
            var_id=var_id_base + i,
            name=name,
            slug=f"{stem}{suffix}",
        )
