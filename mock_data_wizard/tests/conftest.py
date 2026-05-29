"""Shared fixtures for mock_data_wizard tests.

The bundle-runtime fixtures (``make_project_data``, ``write_project_data``,
``MINIMAL_STATS``, ``SPINE_STATS``) live in ``reg_monabundle/tests/``
because the modules under test (``classify``, ``sources``, ``spec``,
``extract``, …) moved there in §15 step 5 phase 2c. mdw reaches in via
``sys.path`` rather than duplicating — same trick as
``reg_meta/tests/conftest.py``.
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


def assign_value_set(
    conn: sqlite3.Connection, cvid: int, codes: list[tuple[str, str]]
) -> int:
    """Test-only helper. Mint or reuse a value_set for ``codes`` (kod/label
    pairs), insert any missing value_code rows, populate value_set_member,
    and link ``cvid`` via ``variable_instance.value_set_id``.

    Returns the value_set_id. Idempotent on the (kod, label) inputs because
    value_set is content-addressed by member_hash.
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

    conn.execute(
        "UPDATE variable_instance SET value_set_id = ? WHERE cvid = ?",
        (value_set_id, cvid),
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
    conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, definition) "
        "VALUES (1, '44', 'Kön', 'Kön enligt folkbokföring')"
    )
    conn.execute(
        "INSERT INTO variable_instance (cvid, register_id, register_variant_id, regver_id, var_id, data_type, data_length, value_set_version_label, vardemangdsniva) "
        "VALUES (1001, 1, 10, 100, 44, 'int', '1', 'Kön', '1')"
    )
    conn.execute(
        "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (1001, 'Kon')"
    )
    # Two value codes: 1=Man, 2=Kvinna
    assign_value_set(conn, 1001, [("1", "Man"), ("2", "Kvinna")])
    conn.commit()
    conn.close()
    return db_path
