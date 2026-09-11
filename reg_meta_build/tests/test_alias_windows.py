from __future__ import annotations

import sqlite3

from reg_meta_build.alias_windows import materialize_multi_alias_windows


def _conn() -> sqlite3.Connection:
    """Just the five tables this pass reads, NOT `db.DDL`: the mixed-shape case
    below seeds two states sharing `(variable_id, register_variant_id,
    valid_from, value_set_version_label)`, which the shipped uniqueness index
    forbids — the pass has to cope with them anyway, since triage resolves that
    collision only after this point. Columns are named in every INSERT, so the
    local order is nobody's business."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE variable_alias_build (
            cvid INTEGER NOT NULL,
            delivery_column_name TEXT NOT NULL
        );
        CREATE TABLE variable_instance (
            cvid INTEGER PRIMARY KEY,
            register_id INTEGER NOT NULL,
            variable_id INTEGER,
            register_variant_id INTEGER NOT NULL,
            regver_id INTEGER NOT NULL,
            value_set_id INTEGER,
            value_set_version_label TEXT
        );
        CREATE TABLE register_version (
            regver_id INTEGER PRIMARY KEY,
            registerversionnamn TEXT
        );
        CREATE TABLE variable_state (
            state_id INTEGER PRIMARY KEY,
            variable_id INTEGER NOT NULL,
            register_variant_id INTEGER NOT NULL,
            valid_from TEXT NOT NULL,
            valid_to TEXT NOT NULL,
            delivery_column_name TEXT,
            value_set_id INTEGER,
            value_set_version_label TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE variable_alias_window (
            variable_id INTEGER NOT NULL,
            register_variant_id INTEGER NOT NULL,
            delivery_column_name TEXT NOT NULL,
            valid_from TEXT NOT NULL,
            valid_to TEXT NOT NULL
        );
        """
    )
    return conn


def _seed(
    conn: sqlite3.Connection,
    *,
    versionname: str,
    aliases: list[str],
    states: list[tuple[int, str, str, str]],
    register_id: int = 1,
) -> None:
    """One multi-alias cvid (10) on variable 100 / variant 200, delivered by a
    single `versionname` edition, plus the `(state_id, valid_from, valid_to,
    delivery_column_name)` states the pass has to choose between."""
    conn.execute(
        "INSERT INTO register_version (regver_id, registerversionnamn) VALUES (1, ?)",
        (versionname,),
    )
    conn.execute(
        "INSERT INTO variable_instance "
        "(cvid, register_id, variable_id, register_variant_id, regver_id, "
        "value_set_id, value_set_version_label) VALUES (10, ?, 100, 200, 1, NULL, '')",
        (register_id,),
    )
    conn.executemany(
        "INSERT INTO variable_alias_build (cvid, delivery_column_name) VALUES (10, ?)",
        [(alias,) for alias in aliases],
    )
    conn.executemany(
        "INSERT INTO variable_state "
        "(state_id, variable_id, register_variant_id, valid_from, valid_to, "
        "delivery_column_name, value_set_id, value_set_version_label) "
        "VALUES (?, 100, 200, ?, ?, ?, NULL, '')",
        states,
    )


def _windows(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    return conn.execute(
        "SELECT delivery_column_name, valid_from, valid_to "
        "FROM variable_alias_window ORDER BY delivery_column_name"
    ).fetchall()


def test_multi_alias_cvid_materializes_state_window_aliases() -> None:
    conn = _conn()
    _seed(
        conn,
        versionname="2018",
        aliases=["A_2018", "A_2017"],
        states=[(1, "2018-01-01", "2018-12-31", "C_2018")],
    )

    counts = materialize_multi_alias_windows(conn)

    assert counts == {"cvids": 1, "windows": 3, "skipped": 0}
    assert _windows(conn) == [
        ("A_2017", "2018-01-01", "2018-12-31"),
        ("A_2018", "2018-01-01", "2018-12-31"),
        ("C_2018", "2018-01-01", "2018-12-31"),
    ]


def test_multi_alias_cvid_skips_mixed_state_shapes() -> None:
    conn = _conn()
    _seed(
        conn,
        versionname="2018",
        aliases=["A_2018", "B_2018"],
        states=[
            (1, "2018-01-01", "2018-06-30", "A_2018"),
            (2, "2018-01-01", "2018-12-31", "B_2018"),
        ],
    )

    counts = materialize_multi_alias_windows(conn)

    assert counts == {"cvids": 1, "windows": 0, "skipped": 1}
    assert _windows(conn) == []


def test_multi_year_version_matches_the_state_it_spans() -> None:
    # Y-113: the pass matches a cvid's edition to its state through the SAME
    # `register_edition_claims` reading the coalescer built the state from. A
    # school-year range spans HT1993..VT2025, so it finds the state covering the
    # tail of that span; read as its first year (1993) alone it would find
    # nothing and the co-delivered columns would go unrecorded.
    conn = _conn()
    _seed(
        conn,
        versionname="Läsåren 1993/1994 - 2024/2025",
        aliases=["Betyg", "BetygNy"],
        states=[(1, "1993-07-01", "2025-06-30", "Betyg")],
    )

    counts = materialize_multi_alias_windows(conn)

    assert counts == {"cvids": 1, "windows": 2, "skipped": 0}
    assert _windows(conn) == [
        ("Betyg", "1993-07-01", "2025-06-30"),
        ("BetygNy", "1993-07-01", "2025-06-30"),
    ]


def test_declared_projection_register_is_read_as_its_vintage_year() -> None:
    # Register 310 is the SCB adapter's one `_PROJECTION_REGISTERS` entry, and
    # this pass goes through the coalescer's own `register_edition_claims`, so
    # both sides agree that `2011-2060` is a 2011 vintage rather than a delivery
    # span. Were the range taken at face value the 2050 state would be matched
    # and windowed.
    conn = _conn()
    _seed(
        conn,
        versionname="2011-2060",
        aliases=["Prognos", "PrognosNy"],
        states=[(1, "2050-01-01", "2050-12-31", "Prognos")],
        register_id=310,
    )

    counts = materialize_multi_alias_windows(conn)

    assert counts == {"cvids": 1, "windows": 0, "skipped": 1}
    assert _windows(conn) == []
