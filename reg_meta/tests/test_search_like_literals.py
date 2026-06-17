from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_binding, add_variable, build_slugged_db
from reg_meta.queries import search

if TYPE_CHECKING:
    import sqlite3


def _add_search_var(
    conn: sqlite3.Connection,
    *,
    var_id: int,
    name: str,
    slug: str,
    delivery_column_name: str,
) -> None:
    add_variable(conn, register_id=1, var_id=var_id, name=name, slug=slug)
    add_binding(
        conn,
        cvid=var_id,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=var_id,
        delivery_column_name=delivery_column_name,
    )


def test_varname_like_metacharacters_match_literally() -> None:
    conn = build_slugged_db(variable=None)
    _add_search_var(
        conn,
        var_id=900,
        name="Literal 12_5",
        slug="literal-underscore",
        delivery_column_name="Col12_5",
    )
    _add_search_var(
        conn,
        var_id=901,
        name="Plain 120",
        slug="plain-120",
        delivery_column_name="Col120",
    )
    _add_search_var(
        conn,
        var_id=902,
        name="Literal 99%5",
        slug="literal-percent",
        delivery_column_name="Col99%5",
    )
    _add_search_var(
        conn,
        var_id=903,
        name="Plain 994",
        slug="plain-994",
        delivery_column_name="Col994",
    )

    underscore = search(conn, "12_", field="varname", fold_groups=False)["results"]
    assert {r["variable_name"] for r in underscore} == {"Literal 12_5"}

    percent = search(conn, "99%", field="varname", fold_groups=False)["results"]
    assert {r["variable_name"] for r in percent} == {"Literal 99%5"}


def test_datacolumn_like_metacharacters_match_literally() -> None:
    conn = build_slugged_db(variable=None)
    _add_search_var(
        conn,
        var_id=900,
        name="Literal underscore",
        slug="literal-underscore",
        delivery_column_name="Col12_5",
    )
    _add_search_var(
        conn,
        var_id=901,
        name="Plain digits",
        slug="plain-120",
        delivery_column_name="Col120",
    )
    _add_search_var(
        conn,
        var_id=902,
        name="Literal percent",
        slug="literal-percent",
        delivery_column_name="Col99%5",
    )
    _add_search_var(
        conn,
        var_id=903,
        name="Plain percent wildcard candidate",
        slug="plain-994",
        delivery_column_name="Col994",
    )

    underscore = search(conn, "12_", field="datacolumn", fold_groups=False)["results"]
    assert {r["datacolumn"] for r in underscore} == {"Col12_5"}

    percent = search(conn, "99%", field="datacolumn", fold_groups=False)["results"]
    assert {r["datacolumn"] for r in percent} == {"Col99%5"}
