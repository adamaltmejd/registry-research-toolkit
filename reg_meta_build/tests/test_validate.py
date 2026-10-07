"""Validator and schema guards the damaged-artifact corpus cannot express.

Every damage-then-validate case lives in `cases/validate/` (test_validate_cases.py).
What remains here: a missing artifact (the corpus always copies a fixture) and a
DDL index that refuses the damage itself, so a `damage.sql` would error the loader
rather than reach `validate_built_db`.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


def test_missing_db_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        validate_built_db(tmp_path / "no_such.db")


def test_duplicate_whole_variable_member_rejected_by_index():
    # The COALESCE unique index closes the NULL-distinctness footgun: a second
    # whole-variable (NULL delivery_column) member of `vara` in group 10 — which
    # a bare composite UNIQUE would silently admit — is rejected at insert time.
    from _slugged_db import add_variable, build_slugged_db

    conn = build_slugged_db(classification=None)  # scb/lisa (register 1)
    add_variable(conn, register_id=1, var_id=901, name="A", slug="vara")
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (10, 'variable', 1, 'vara', 'A', 'edge')"
    )
    conn.execute(
        "INSERT INTO concept_group_variable (variable_id, group_id) "
        "SELECT variable_id, 10 FROM variable WHERE slug = 'vara'"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO concept_group_variable (group_id, variable_id, "
            "delivery_column_name) SELECT 10, variable_id, NULL FROM variable "
            "WHERE slug = 'vara'"
        )
