"""Derived succession tables: terminal successors, classification chains and
families, as built from a readable fixture through the resolved writer."""

from __future__ import annotations

import json
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

from reader_artifacts import cached_reader_artifact
from reg_meta_build.derive import derive

CASE = Path(__file__).parent / "cases/derive/succession-chains"
TABLES = {
    "succession_terminal": "fqid",
    "classification_chain": "anchor_slug, position",
    "classification_family": "family_key, position",
}


def test_chain_tables_follow_active_succession_at_the_policy_year():
    # Fails if the terminal walk picks one branch of a split, ignores the policy
    # year (future-dated edges), or a chain or family loses its walk order.
    expected = json.loads((CASE / "expected.json").read_text(encoding="utf-8"))
    db = cached_reader_artifact(CASE, "catalog")
    with closing(sqlite3.connect(f"file:{db}?immutable=1", uri=True)) as conn:
        actual = {
            table: [
                list(row)
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY {order}")
            ]
            for table, order in TABLES.items()
        }
    assert actual == {table: expected[table] for table in TABLES}


def test_rederiving_the_same_base_is_byte_identical(tmp_path: Path):
    # Fails if derive writes chain rows in an order that depends on the run.
    base = cached_reader_artifact(CASE, "catalog")
    copies = [tmp_path / "first.db", tmp_path / "second.db"]
    for copy in copies:
        shutil.copyfile(base, copy)
        copy.chmod(0o644)
        with closing(sqlite3.connect(copy)) as conn:
            derive(conn)
            conn.commit()
    assert copies[0].read_bytes() == copies[1].read_bytes()
