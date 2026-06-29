from __future__ import annotations

import sqlite3

from reg_meta_build.alias_windows import materialize_multi_alias_windows


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE variable_alias_build (
            cvid INTEGER NOT NULL,
            delivery_column_name TEXT NOT NULL
        );
        CREATE TABLE variable_instance (
            cvid INTEGER PRIMARY KEY,
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


def test_multi_alias_cvid_materializes_state_window_aliases() -> None:
    conn = _conn()
    conn.execute("INSERT INTO register_version VALUES (1, '2018')")
    conn.execute("INSERT INTO variable_instance VALUES (10, 100, 200, 1, NULL, '')")
    conn.executemany(
        "INSERT INTO variable_alias_build VALUES (10, ?)",
        [("A_2018",), ("A_2017",)],
    )
    conn.execute(
        "INSERT INTO variable_state VALUES "
        "(1, 100, 200, '2018-01-01', '2018-12-31', 'C_2018', NULL, '')"
    )

    counts = materialize_multi_alias_windows(conn)

    assert counts == {"cvids": 1, "windows": 3, "skipped": 0}
    assert conn.execute(
        "SELECT delivery_column_name, valid_from, valid_to "
        "FROM variable_alias_window ORDER BY delivery_column_name"
    ).fetchall() == [
        ("A_2017", "2018-01-01", "2018-12-31"),
        ("A_2018", "2018-01-01", "2018-12-31"),
        ("C_2018", "2018-01-01", "2018-12-31"),
    ]


def test_multi_alias_cvid_skips_mixed_state_shapes() -> None:
    conn = _conn()
    conn.execute("INSERT INTO register_version VALUES (1, '2018')")
    conn.execute("INSERT INTO variable_instance VALUES (10, 100, 200, 1, NULL, '')")
    conn.executemany(
        "INSERT INTO variable_alias_build VALUES (10, ?)",
        [("A_2018",), ("B_2018",)],
    )
    conn.executemany(
        "INSERT INTO variable_state VALUES (?, 100, 200, ?, ?, ?, NULL, '')",
        [
            (1, "2018-01-01", "2018-06-30", "A_2018"),
            (2, "2018-01-01", "2018-12-31", "B_2018"),
        ],
    )

    counts = materialize_multi_alias_windows(conn)

    assert counts == {"cvids": 1, "windows": 0, "skipped": 1}
    assert conn.execute("SELECT COUNT(*) FROM variable_alias_window").fetchone() == (0,)
