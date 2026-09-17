"""Explicit reader fixtures, independent of source inference and build policy."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

from reg_meta.db import SCHEMA_VERSION
from reg_meta.fqid import period_token_to_bounds
from reg_meta_build.db import DDL, _populate_fts, _value_set_hash, seed_providers

if TYPE_CHECKING:
    from pathlib import Path


def _build(tmp_path, *, slug, name, years, codes, windows):
    output = tmp_path / "db" / "reg_meta.db"
    output.parent.mkdir()
    with closing(sqlite3.connect(output)) as conn:
        conn.executescript(DDL)
        seed_providers(conn)
        conn.executescript(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (1, 1, 'testreg', 'TESTREG');"
            "INSERT INTO register_variant "
            "(register_variant_id, register_id, slug, name) "
            "VALUES (10, 1, 'individer', 'Individer');"
            "INSERT INTO variable (variable_id, register_id, provider_key, slug, name) "
            "VALUES (2, 1, '2', 'kon', 'Kön');"
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name) "
            "VALUES (2, 10, '2018-01-01', '2018-12-31', 'Kon');"
            "INSERT INTO variable_alias VALUES (2, 10, 'Kon');"
        )
        conn.execute(
            "INSERT INTO import_manifest VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, slug, name) "
            "VALUES (1, 1, '1', ?, ?)",
            (slug, name),
        )
        conn.execute(
            "INSERT INTO value_set (value_set_id, member_hash) VALUES (1, ?)",
            (_value_set_hash(codes),),
        )
        for code_id, (code, label) in enumerate(codes):
            conn.execute(
                "INSERT INTO value_code (code_id, code, label) VALUES (?, ?, ?)",
                (code_id, code, label),
            )
            conn.execute("INSERT INTO value_set_member VALUES (1, ?)", (code_id,))
        conn.executemany(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name, data_type, data_length, "
            "value_set_id) VALUES (1, 10, ?, ?, ?, 'integer', '1', 1)",
            [(f"{year}-01-01", f"{year}-12-31", windows[0][0]) for year in years],
        )
        conn.executemany(
            "INSERT INTO variable_alias VALUES (1, 10, ?)",
            [(column,) for column in dict.fromkeys(w[0] for w in windows)],
        )
        conn.executemany(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) VALUES (1, 10, ?, ?, ?)",
            windows,
        )
        _populate_fts(conn)
        conn.commit()
    return output


def build_alias_representations(tmp_path: Path) -> Path:
    return _build(
        tmp_path,
        slug="loneink-lisa2006",
        name="Kontant bruttolön",
        years=(2018,),
        codes=[("1", "Low"), ("2", "High")],
        windows=[
            (column, "2018-01-01", "2018-12-31")
            for column in ("LoneInk_LISA2006", "LoneInk_LISA2007")
        ],
    )


def build_month_family(tmp_path: Path, *, march_2018: bool = True) -> Path:
    # Reader tests pin the existing period grammar, including its Feb-29 upper
    # bound. Pipeline tests independently require real calendar boundaries.
    return _build(
        tmp_path,
        slug="lonfink",
        name="Lön per månad",
        years=(2018, 2019),
        codes=[("1", "Låg"), ("2", "Hög")],
        windows=[
            (column, *period_token_to_bounds(f"{year}-{month:02d}"))
            for month, column in (
                (1, "LonFinkJan"),
                (2, "LonFinkFeb"),
                (3, "LonFinkMars"),
            )
            for year in (2018, 2019)
            if march_2018 or (month, year) != (3, 2018)
        ],
    )
