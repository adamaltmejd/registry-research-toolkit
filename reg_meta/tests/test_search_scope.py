"""Search scope contract: which entity surfaces a query reaches, how a
multi-token query combines, and the FQIDs and fields each leaf row carries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta.queries import search
from search_test_support import (
    add_binding,
    add_variable,
    build_slugged_db,
    reader_search_conn,
    rebuild_fts as _rebuild_fts,
)

if TYPE_CHECKING:
    import sqlite3


def test_multi_token_query_requires_every_token(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """`exact` alone also reaches `unrelated` through its delivery column
    `EXACT`; adding `topic` keeps only the variables matching both tokens."""
    conn = reader_search_conn(tmp_path_factory, "search-alias-relevance")

    out = search(conn, "exact topic", field="description", type="variable")

    assert sorted(str(row.fqid) for row in out.results) == [
        "scb/aliases/broad-a",
        "scb/aliases/broad-b",
    ]


def test_punctuation_only_query_matches_no_authored_label(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """The group label `Inkomst -- brutto/netto` contains `--` literally; a
    query with no word character still returns nothing rather than matching it."""
    conn = reader_search_conn(tmp_path_factory, "search-punctuation-label")

    assert search(conn, "Inkomst", field="description").results
    assert search(conn, "--", field="description").results == ()


@pytest.fixture
def db() -> sqlite3.Connection:
    conn = build_slugged_db()
    _rebuild_fts(conn)
    return conn


def _types(results: tuple) -> set[str]:
    return {r.type for r in results}


def test_variable_search_folds_multiple_matched_delivery_columns() -> None:
    conn = build_slugged_db(
        variable=("Orsak till missnöje, formell utbildning", 32183, 1001, "Kol"),
        delivery_column_name="fedunsatreason_1",
        variable_slug="formal-utbildning",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=32183,
        delivery_column_name="fedunsatreason_2",
    )
    _rebuild_fts(conn)

    out = search(conn, "fedunsatreason", field="description", type="variable")

    assert len(out.results) == 1
    assert len(out.results) == 1
    assert str(out.results[0].fqid) == "scb/lisa/formal-utbildning"
    assert out.results[0].delivery_column_names == (
        "fedunsatreason_1",
        "fedunsatreason_2",
    )


def test_variable_search_exact_column_token_shows_only_that_alias() -> None:
    conn = build_slugged_db(
        variable=("Orsak till missnöje, formell utbildning", 32183, 1001, "Kol"),
        delivery_column_name="fedunsatreason_1",
        variable_slug="formal-utbildning",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=32183,
        delivery_column_name="fedunsatreason_2",
    )
    _rebuild_fts(conn)

    out = search(conn, "fedunsatreason_1", field="description", type="variable")

    assert len(out.results) == 1
    assert len(out.results) == 1
    assert out.results[0].delivery_column_names == ("fedunsatreason_1",)


def test_variable_search_multi_alias_query_shows_only_matching_aliases() -> None:
    conn = build_slugged_db(
        variable=("Orsak till missnöje, formell utbildning", 32183, 1001, "Kol"),
        delivery_column_name="fedunsatreason_1",
        variable_slug="formal-utbildning",
    )
    variable_id = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = 'formal-utbildning'"
    ).fetchone()[0]
    conn.executemany(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, 10, ?)",
        [(variable_id, "otheralias"), (variable_id, "zedalias")],
    )
    _rebuild_fts(conn)

    out = search(conn, "fedunsatreason zedalias", field="description", type="variable")

    assert len(out.results) == 1
    assert len(out.results) == 1
    assert out.results[0].delivery_column_names == (
        "fedunsatreason_1",
        "zedalias",
    )


def test_variable_search_preserves_whitespace_delivery_column_name() -> None:
    conn = build_slugged_db(
        variable=("Annual expense", 32183, 1001, "Kol"),
        delivery_column_name="TOTAL COST",
        variable_slug="annual-expense",
    )
    _rebuild_fts(conn)

    out = search(conn, "TOTAL", field="description", type="variable")

    rows = out.results
    assert _types(rows) == {"variable"}
    assert str(rows[0].fqid) == "scb/lisa/annual-expense"
    assert rows[0].delivery_column_names == ("TOTAL COST",)


def test_variable_name_hit_ranks_above_delivery_column_hit() -> None:
    conn = build_slugged_db(
        variable=("Orsak till missnöje, formell utbildning", 32183, 1001, "Kol"),
        delivery_column_name="fedunsatreason_1",
        variable_slug="formal-utbildning",
    )
    add_variable(
        conn,
        register_id=1,
        var_id=42181,
        name="fedunsatreason",
        slug="name-hit",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=42181,
        delivery_column_name="other_column",
    )
    _rebuild_fts(conn)

    out = search(conn, "fedunsatreason", field="description", type="variable")

    assert [str(row.fqid) for row in out.results] == [
        "scb/lisa/name-hit",
        "scb/lisa/formal-utbildning",
    ]


def test_variable_search_carries_operational_definition() -> None:
    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable SET operational_definition = ? WHERE slug = 'kon'",
        ("Registered sex at year end",),
    )
    _rebuild_fts(conn)

    out = search(conn, "Registered", field="description", type="variable")

    rows = out.results
    assert _types(rows) == {"variable"}
    assert rows[0].operational_definition == "Registered sex at year end"


def test_invalid_type_raises(db: sqlite3.Connection) -> None:
    with pytest.raises(RegMetaError) as exc:
        search(db, "x", type="nonsense")
    assert "Invalid search type" in exc.value.message
