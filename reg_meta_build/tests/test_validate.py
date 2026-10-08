"""Validator and schema guards the damaged-artifact corpus cannot express.

Every damage-then-validate case lives in `cases/validate/` (test_validate_cases.py).
What remains here: a missing artifact (the corpus always copies a fixture) and a
DDL index that refuses the damage itself, so a `damage.sql` would error the loader
rather than reach `validate_built_db`.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _shared_fixtures import connect_built_db
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


def test_missing_db_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        validate_built_db(tmp_path / "no_such.db")


def test_duplicate_whole_variable_member_rejected_by_index(
    fixture_db: Path, tmp_path: Path
):
    # The COALESCE unique index closes the NULL-distinctness footgun: a second
    # whole-variable (NULL delivery_column) member of `kon` in one group — which
    # a bare composite UNIQUE would silently admit — is rejected at insert time.
    db = tmp_path / "reg_meta.db"
    db.write_bytes(fixture_db.read_bytes())
    with closing(connect_built_db(db)) as conn:
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (999, 'variable', 1, 'kon', 'Kön', 'edge')"
        )
        conn.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (1, 999)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO concept_group_variable (group_id, variable_id, "
                "delivery_column_name) VALUES (999, 1, NULL)"
            )
