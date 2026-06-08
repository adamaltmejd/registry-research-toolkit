"""Shared fixtures for mock_data_wizard tests.

The bundle-runtime fixtures live in ``reg_monabundle/tests/`` because the
modules under test (``classify``, ``sources``, ``spec``, ``extract``, …)
moved there. The ``sys.path.insert`` below bridges
that directory so mdw tests reach them without duplicating — same trick as
``reg_meta/tests/conftest.py``. This module re-exports the stats fixtures
(``MINIMAL_STATS`` / ``SPINE_STATS``); the project_data builders
(``make_project_data`` / ``write_project_data``) are imported directly from
``_project_data_fixtures`` by the tests that need them.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest
from reg_meta.db import SCHEMA_VERSION
from reg_meta_build.db import DDL, _value_set_hash

sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent.parent / "reg_monabundle" / "tests"),
)
from _stats_fixtures import MINIMAL_STATS, SPINE_STATS  # noqa: E402,F401


def mint_value_set(conn: sqlite3.Connection, codes: list[tuple[str, str]]) -> int:
    """Test-only helper. Mint or reuse a value_set for ``codes`` (kod/label
    pairs), inserting any missing value_code rows and populating
    value_set_member. Returns the value_set_id.

    A2.7: callers link the returned id onto `variable_state.value_set_id` (the
    shipped per-era unit); `variable_instance` is dropped before ship. Idempotent
    on the (kod, label) inputs because value_set is content-addressed by
    member_hash.
    """
    code_ids: list[int] = []
    for kod, label in codes:
        row = conn.execute(
            "SELECT code_id FROM value_code WHERE code = ? AND label = ?",
            (kod, label),
        ).fetchone()
        if row is None:
            cur = conn.execute(
                "INSERT INTO value_code (code, label) VALUES (?, ?)",
                (kod, label),
            )
            code_id = int(cur.lastrowid)
        else:
            code_id = int(row[0])
        code_ids.append(code_id)

    member_hash = _value_set_hash(list(codes))
    row = conn.execute(
        "SELECT value_set_id FROM value_set WHERE member_hash = ?", (member_hash,)
    ).fetchone()
    if row is None:
        cur = conn.execute(
            "INSERT INTO value_set (member_hash) VALUES (?)", (member_hash,)
        )
        value_set_id = int(cur.lastrowid)
        conn.executemany(
            "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
            [(value_set_id, c) for c in code_ids],
        )
    else:
        value_set_id = int(row[0])
    return value_set_id


def add_state_with_codes(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    var_id: int,
    codes: list[tuple[str, str]],
    register_variant_id: int = 10,
    value_set_version_label: str = "",
    classification_id: int | None = None,
    valid_from: str = "0001-01-01",
    valid_to: str = "9999-12-31",
    data_type: str = "int",
) -> int:
    """A2.7 test helper: seed a `variable_state` (the shipped per-era unit) for
    an existing `variable`, minting + linking its value_set.

    Replaces the v0.x ``INSERT variable_instance`` + ``assign_value_set(cvid)``
    pattern — `enrich.py` reads `variable_state` now (`variable_instance` is
    dropped before ship). The parent `variable` (register_id, var_id →
    provider_key) must already exist. Returns the value_set_id.
    """
    value_set_id = mint_value_set(conn, codes)
    variable_id = conn.execute(
        "SELECT variable_id FROM variable "
        "WHERE register_id = ? AND provider_key = CAST(? AS TEXT)",
        (register_id, var_id),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, value_set_id, value_set_version_label, classification_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            variable_id,
            register_variant_id,
            valid_from,
            valid_to,
            data_type,
            value_set_id,
            value_set_version_label,
            classification_id,
        ),
    )
    return value_set_id


MULTI_FILE_STATS = {
    "contract_version": "2.0.0",
    "generated_at": "2026-03-15T10:00:00Z",
    "sources": [
        {
            "source_name": "file_a.csv",
            "source_type": "file",
            "source_detail": {"path": "file_a.csv"},
            "row_count": 500,
            "columns": [
                {
                    "column_name": "LopNr",
                    "inferred_type": "id",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 500,
                    "stats": {"id_subtype": "integer"},
                },
                {
                    "column_name": "Value",
                    "inferred_type": "numeric",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 200,
                    "stats": {"min": 0, "max": 100, "mean": 50, "sd": 20},
                },
            ],
        },
        {
            "source_name": "file_b.csv",
            "source_type": "file",
            "source_detail": {"path": "file_b.csv"},
            "row_count": 300,
            "columns": [
                {
                    "column_name": "LopNr",
                    "inferred_type": "id",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 300,
                    "stats": {"id_subtype": "integer"},
                },
                {
                    "column_name": "Status",
                    "inferred_type": "categorical",
                    "nullable": False,
                    "null_count": 0,
                    "null_rate": 0.0,
                    "n_distinct": 3,
                    "stats": {"frequencies": {"A": 100, "B": 100, "C": 100}},
                },
            ],
        },
    ],
    "shared_columns": [
        {
            "column_name": "LopNr",
            "sources": ["file_a.csv", "file_b.csv"],
            "max_n_distinct": 500,
        }
    ],
}


@pytest.fixture
def stats_path(tmp_path: Path) -> Path:
    p = tmp_path / "mock_data_stats.json"
    p.write_text(json.dumps(MINIMAL_STATS), encoding="utf-8")
    return p


@pytest.fixture
def spine_stats_path(tmp_path: Path) -> Path:
    p = tmp_path / "mock_data_stats.json"
    p.write_text(json.dumps(SPINE_STATS), encoding="utf-8")
    return p


@pytest.fixture
def multi_file_stats_path(tmp_path: Path) -> Path:
    p = tmp_path / "mock_data_stats.json"
    p.write_text(json.dumps(MULTI_FILE_STATS), encoding="utf-8")
    return p


@pytest.fixture
def reg_meta_db(tmp_path: Path) -> Path:
    """Build a minimal reg_meta DB with one register, one variable, and value codes."""
    db_path = tmp_path / "reg_meta.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(DDL)
    conn.execute(
        "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
        ("schema_version", SCHEMA_VERSION),
    )
    from reg_meta_build.db import seed_providers

    seed_providers(conn)
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name, purpose) "
        "VALUES (1, 1, 'TESTREG', 'Testing')"
    )
    conn.execute(
        "INSERT INTO register_variant (register_variant_id, register_id, name) "
        "VALUES (10, 1, 'Individer')"
    )
    conn.execute(
        "INSERT INTO register_version (regver_id, register_variant_id, registerversionnamn) "
        "VALUES (100, 10, '2020')"
    )
    variable_id = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, definition, slug) "
        "VALUES (1, '44', 'Kön', 'Kön enligt folkbokföring', 'kon')"
    ).lastrowid
    # A2.7: the shipped DB has no `variable_instance`; `variable_alias` is
    # variable_id-keyed and value sets link via `variable_state.value_set_id`.
    # Two value codes: 1=Man, 2=Kvinna, on a 2020 state carrying the column.
    value_set_id = mint_value_set(conn, [("1", "Man"), ("2", "Kvinna")])
    conn.execute(
        "INSERT INTO variable_alias (variable_id, register_variant_id, delivery_column_name) "
        "VALUES (?, 10, 'Kon')",
        (variable_id,),
    )
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, data_length, delivery_column_name, value_set_id, "
        "value_set_version_label) "
        "VALUES (?, 10, '2020-01-01', '2020-12-31', 'int', '1', 'Kon', ?, 'Kön')",
        (variable_id, value_set_id),
    )
    conn.commit()
    conn.close()
    return db_path
