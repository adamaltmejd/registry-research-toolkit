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


def _case_forms(text: str) -> tuple[str, ...]:
    """The case spellings a researcher types: lower, upper, mixed."""
    return (text.lower(), text.upper(), text.capitalize())


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

    underscore = search(conn, "12_", field="varname", fold_groups=False).results
    assert {r.name for r in underscore} == {"Literal 12_5"}

    percent = search(conn, "99%", field="varname", fold_groups=False).results
    assert {r.name for r in percent} == {"Literal 99%5"}


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

    underscore = search(conn, "12_", field="datacolumn", fold_groups=False).results
    assert {r.datacolumn for r in underscore} == {"Col12_5"}

    percent = search(conn, "99%", field="datacolumn", fold_groups=False).results
    assert {r.datacolumn for r in percent} == {"Col99%5"}


# Swedish stored spellings, one per Å/Ä/Ö: mixed, upper and lower case.
_SWEDISH_SPELLINGS = ("Kön", "ÅRSINKOMST", "ägare")


def _assert_every_case_form_matches(
    conn: sqlite3.Connection, *, field: str, attr: str
) -> None:
    """Each `_SWEDISH_SPELLINGS` text is found by every case spelling of itself."""
    for text in _SWEDISH_SPELLINGS:
        for query in _case_forms(text):
            hits = search(conn, query, field=field, fold_groups=False).results
            assert {getattr(r, attr) for r in hits} == {text}, query


def test_varname_matches_swedish_names_in_any_case() -> None:
    conn = build_slugged_db(variable=None)
    for i, name in enumerate(_SWEDISH_SPELLINGS):
        _add_search_var(
            conn,
            var_id=910 + i,
            name=name,
            slug=f"swedish-name-{i}",
            delivery_column_name=f"Col{i}",
        )

    _assert_every_case_form_matches(conn, field="varname", attr="name")


def test_datacolumn_matches_swedish_columns_in_any_case() -> None:
    conn = build_slugged_db(variable=None)
    for i, column in enumerate(_SWEDISH_SPELLINGS):
        _add_search_var(
            conn,
            var_id=920 + i,
            name=f"Variable {i}",
            slug=f"swedish-column-{i}",
            delivery_column_name=column,
        )

    _assert_every_case_form_matches(conn, field="datacolumn", attr="datacolumn")


def test_varname_folds_case_without_a_delivery_alias() -> None:
    """The varname arm stands alone: no `variable_alias` row to fall back on."""
    conn = build_slugged_db(variable=None)
    add_variable(conn, register_id=1, var_id=930, name="Sjöfart", slug="sjofart")

    for query in _case_forms("Sjöfart"):
        hits = search(conn, query, field="varname", fold_groups=False).results
        assert {r.name for r in hits} == {"Sjöfart"}, query


def test_case_folded_query_keeps_literal_wildcards() -> None:
    conn = build_slugged_db(variable=None)
    _add_search_var(
        conn,
        var_id=940,
        name="Kön 12_5",
        slug="kon-underscore",
        delivery_column_name="KÖN12_5",
    )
    _add_search_var(
        conn,
        var_id=941,
        name="Kön 1235",
        slug="kon-plain",
        delivery_column_name="KÖN1235",
    )

    names = search(conn, "KÖN 12_", field="varname", fold_groups=False).results
    assert {r.name for r in names} == {"Kön 12_5"}

    columns = search(conn, "kön12_", field="datacolumn", fold_groups=False).results
    assert {r.datacolumn for r in columns} == {"KÖN12_5"}
