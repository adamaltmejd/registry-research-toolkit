from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta_build.alias_windows import (
    CuratedAliasWindow,
    load_alias_windows,
    materialize_curated_alias_windows,
    materialize_multi_alias_windows,
)

if TYPE_CHECKING:
    from pathlib import Path


def _conn() -> sqlite3.Connection:
    """Just the tables these passes read, NOT `db.DDL`: the mixed-shape case
    below seeds two states sharing `(variable_id, register_variant_id,
    valid_from, value_set_version_label)`, which the shipped uniqueness index
    forbids — the pass has to cope with them anyway, since triage resolves that
    collision only after this point. Columns are named in every INSERT, so the
    local order is nobody's business."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE provider (
            provider_id INTEGER PRIMARY KEY,
            slug TEXT NOT NULL
        );
        CREATE TABLE register (
            register_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            slug TEXT NOT NULL
        );
        CREATE TABLE register_variant (
            register_variant_id INTEGER PRIMARY KEY,
            register_id INTEGER NOT NULL,
            slug TEXT NOT NULL
        );
        CREATE TABLE variable (
            variable_id INTEGER PRIMARY KEY,
            register_id INTEGER NOT NULL,
            slug TEXT NOT NULL
        );
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
            register_variant_id INTEGER NOT NULL,
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
            valid_to TEXT NOT NULL,
            provenance TEXT,
            PRIMARY KEY (
                variable_id, register_variant_id, delivery_column_name, valid_from
            )
        );
        CREATE TABLE variable_alias (
            variable_id INTEGER NOT NULL,
            register_variant_id INTEGER NOT NULL,
            delivery_column_name TEXT NOT NULL,
            PRIMARY KEY (variable_id, register_variant_id, delivery_column_name)
        );
        INSERT INTO provider VALUES (1, 'scb');
        INSERT INTO register VALUES (1, 1, 'testreg');
        INSERT INTO register_variant VALUES (200, 1, 'test-variant');
        INSERT INTO variable VALUES (100, 1, 'test-variable');
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
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn) VALUES (1, 200, ?)",
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
    conn.executemany(
        "INSERT OR IGNORE INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (100, 200, ?)",
        [(alias,) for alias in {*aliases, *(state[3] for state in states)}],
    )


def _windows(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    return conn.execute(
        "SELECT delivery_column_name, valid_from, valid_to "
        "FROM variable_alias_window ORDER BY delivery_column_name, valid_from"
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


def _curated(
    *,
    variable: str = "test-variable",
    variant: str = "test-variant",
    column: str = "AEBUY",
    editions: tuple[str, ...] = ("2018",),
) -> CuratedAliasWindow:
    return CuratedAliasWindow(
        provider="scb",
        register="testreg",
        variable=variable,
        variant=variant,
        column=column,
        source_editions=editions,
        evidence="SWECOV holds this exact header.",
    )


def _seed_curated(
    conn: sqlite3.Connection,
    *,
    editions: tuple[str, ...] = ("2018",),
    aliases: tuple[str, ...] = ("AEBUY", "E_AWBUY"),
    states: tuple[tuple[int, str, str, str], ...] = (
        (1, "2017-01-01", "2018-12-31", "E_AWBUY"),
    ),
    source_column: str = "E_AWBUY",
) -> None:
    conn.executemany(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn) VALUES (?, 200, ?)",
        [(index, edition) for index, edition in enumerate(editions, start=1)],
    )
    conn.executemany(
        "INSERT INTO variable_instance "
        "(cvid, register_id, variable_id, register_variant_id, regver_id, "
        "value_set_id, value_set_version_label) "
        "VALUES (?, 1, 100, 200, ?, NULL, '')",
        [(index, index) for index in range(1, len(editions) + 1)],
    )
    conn.executemany(
        "INSERT INTO variable_alias_build (cvid, delivery_column_name) VALUES (?, ?)",
        [(index, source_column) for index in range(1, len(editions) + 1)],
    )
    conn.executemany(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (100, 200, ?)",
        [(alias,) for alias in aliases],
    )
    conn.executemany(
        "INSERT INTO variable_state "
        "(state_id, variable_id, register_variant_id, valid_from, valid_to, "
        "delivery_column_name, value_set_id, value_set_version_label) "
        "VALUES (?, 100, 200, ?, ?, ?, NULL, '')",
        states,
    )


def test_curated_alias_uses_exact_claim_and_preserves_base_state() -> None:
    conn = _conn()
    _seed_curated(conn)
    original_state = conn.execute("SELECT * FROM variable_state").fetchall()

    counts = materialize_curated_alias_windows(
        conn, (_curated(),), providers=frozenset({"scb"})
    )

    assert counts == {"entries": 1, "windows": 2}
    assert _windows(conn) == [
        ("AEBUY", "2018-01-01", "2018-12-31"),
        ("E_AWBUY", "2017-01-01", "2018-12-31"),
    ]
    assert conn.execute("SELECT * FROM variable_state").fetchall() == original_state
    assert conn.execute(
        "SELECT provenance FROM variable_alias_window "
        "WHERE delivery_column_name = 'AEBUY'"
    ).fetchone()[0] == (
        "errata:scoped-attributions\n"
        '[{"class":"omitted-column-in-version",'
        '"evidence":"SWECOV holds this exact header.",'
        '"source_editions":["2018"]}]'
    )


def test_curated_disjoint_editions_stay_disjoint() -> None:
    conn = _conn()
    _seed_curated(
        conn,
        editions=("2016", "2018"),
        states=((1, "2015-01-01", "2019-12-31", "E_AWBUY"),),
    )

    materialize_curated_alias_windows(
        conn,
        (_curated(editions=("2016", "2018")),),
        providers=frozenset({"scb"}),
    )

    assert _windows(conn) == [
        ("AEBUY", "2016-01-01", "2016-12-31"),
        ("AEBUY", "2018-01-01", "2018-12-31"),
        ("E_AWBUY", "2015-01-01", "2019-12-31"),
    ]


def test_alias_window_loader_rejects_invalid_noted_date(tmp_path: Path) -> None:
    path = tmp_path / "alias_windows.toml"
    path.write_text(
        '[[alias]]\nvariable = "scb/testreg/test-variable"\n'
        'variant = "test-variant"\ncolumn = "AEBUY"\n'
        'source_editions = ["2018"]\nevidence = "held"\nnoted = "soon"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_alias_windows(path)

    assert "YYYY-MM-DD" in exc_info.value.message


def test_curated_alias_rejects_unknown_variable() -> None:
    conn = _conn()
    _seed_curated(conn)

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn,
            (_curated(variable="missing"),),
            providers=frozenset({"scb"}),
        )

    assert exc_info.value.code == "alias_windows_unknown_variable"


def test_curated_alias_rejects_unknown_column() -> None:
    conn = _conn()
    _seed_curated(conn)

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn,
            (_curated(column="MISSING"),),
            providers=frozenset({"scb"}),
        )

    assert exc_info.value.code == "alias_windows_unknown_column"


def test_curated_alias_rejects_cross_variant_column() -> None:
    conn = _conn()
    _seed_curated(conn, aliases=("E_AWBUY",))
    conn.execute(
        "INSERT INTO register_variant VALUES (201, 1, 'other-variant')"
    )
    conn.execute(
        "INSERT INTO variable_alias VALUES (100, 201, 'AEBUY')"
    )

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_cross_variant"


def test_curated_alias_rejects_ambiguous_column_owner() -> None:
    conn = _conn()
    _seed_curated(conn)
    conn.execute("INSERT INTO variable VALUES (101, 1, 'other-variable')")
    conn.execute("INSERT INTO variable_alias VALUES (101, 200, 'AEBUY')")

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_ambiguous_column"


def test_curated_alias_rejects_unsupported_edition() -> None:
    conn = _conn()
    _seed_curated(conn, editions=("2017",))

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_unsupported_edition"


def test_curated_alias_rejects_edition_without_source_instance() -> None:
    conn = _conn()
    _seed_curated(conn)
    conn.execute("DELETE FROM variable_instance")

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_unsupported_edition"
    assert "source instance" in exc_info.value.message


def test_curated_alias_rejects_ambiguous_source_state() -> None:
    conn = _conn()
    _seed_curated(
        conn,
        states=(
            (1, "2017-01-01", "2018-12-31", "E_AWBUY"),
            (2, "2018-01-01", "2019-12-31", "E_AWBUY"),
        ),
    )

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_ambiguous_state"


def test_curated_alias_rejects_source_covered_declaration() -> None:
    conn = _conn()
    _seed_curated(
        conn,
        aliases=("AEBUY",),
        states=((1, "2017-01-01", "2018-12-31", "AEBUY"),),
    )

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_source_covered"
    assert "Retire" in exc_info.value.remediation


def test_curated_alias_rejects_column_documented_in_source_edition() -> None:
    conn = _conn()
    _seed_curated(conn, source_column="AEBUY")

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_source_covered"
    assert "Retire" in exc_info.value.remediation


def test_curated_alias_rejects_existing_window_as_source_covered() -> None:
    conn = _conn()
    _seed_curated(conn)
    conn.execute(
        "INSERT INTO variable_alias_window "
        "(variable_id, register_variant_id, delivery_column_name, valid_from, "
        "valid_to) VALUES (100, 200, 'AEBUY', '2018-01-01', '2018-12-31')"
    )

    with pytest.raises(RegMetaError) as exc_info:
        materialize_curated_alias_windows(
            conn, (_curated(),), providers=frozenset({"scb"})
        )

    assert exc_info.value.code == "alias_windows_source_covered"
    assert "Retire" in exc_info.value.remediation
