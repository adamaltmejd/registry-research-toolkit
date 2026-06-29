"""Resolver coverage for multi-alias delivery-column representations (#945)."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import _var_row
from _shared_fixtures import build_with_rows, vm_rows
from reg_meta.catalog import Catalog, ValueSetMember
from reg_meta.db import open_db

if TYPE_CHECKING:
    from pathlib import Path

_FQID = "scb/testreg/loneink-lisa2006"
_ALIASES = ("LoneInk_LISA2006", "LoneInk_LISA2007")
_CVID = 9100
_VAR_ID = 910
_CODES = [("1", "Low"), ("2", "High")]


def _build_multi_alias_db(tmp_path: Path) -> Path:
    # One concrete SCB cvid can list several delivery headers. They are
    # co-delivered representations of the same state, not search-only aliases.
    ri = [
        _var_row(
            colname=column,
            cvid=_CVID,
            var_id=_VAR_ID,
            varname="Kontant bruttolön",
            year="2018",
            regver_id=910,
            data_type="float",
            data_length="8",
        )
        for column in _ALIASES
    ]
    conn = build_with_rows(tmp_path, ri, vm_rows(_CVID, "LoneInk2018", _CODES))
    conn.close()
    return tmp_path / "db" / "reg_meta.db"


def test_multi_alias_cvid_states_expose_every_delivery_column(tmp_path: Path) -> None:
    db = _build_multi_alias_db(tmp_path)
    conn = open_db(db)
    try:
        cat = Catalog(conn)
        states = cat.states(_FQID)
        assert [s.delivery_column_name for s in states] == list(_ALIASES)
        assert {s.state_id for s in states} == {states[0].state_id}
        assert {(s.valid_from, s.valid_to) for s in states} == {
            ("2018-01-01", "2018-12-31")
        }
        assert all(s.period_token == "2018" for s in states)
        assert all(
            s.value_set
            == (
                ValueSetMember(code="1", label="Low"),
                ValueSetMember(code="2", label="High"),
            )
            for s in states
        )
    finally:
        conn.close()


def test_multi_alias_resolve_at_returns_picker_visible_representations(
    tmp_path: Path,
) -> None:
    db = _build_multi_alias_db(tmp_path)
    conn = open_db(db)
    try:
        cat = Catalog(conn)
        states = cat.resolve_at(_FQID, "2018-06")
        assert [s.delivery_column_name for s in states] == list(_ALIASES)
    finally:
        conn.close()


def test_alias_windows_do_not_hide_overlapping_base_state(tmp_path: Path) -> None:
    db = _build_multi_alias_db(tmp_path)
    write_conn = sqlite3.connect(db)
    try:
        write_conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "SELECT variable_id, register_variant_id, 'LoneInk_BASE' "
            "FROM variable_alias WHERE delivery_column_name = ?",
            (_ALIASES[0],),
        )
        write_conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "data_length, delivery_column_name, value_set_id, "
            "value_set_version_label, classification_id) "
            "SELECT variable_id, register_variant_id, "
            "'2017-01-01', '2019-12-31', data_type, data_length, "
            "'LoneInk_BASE', value_set_id, value_set_version_label, classification_id "
            "FROM variable_state WHERE delivery_column_name = ?",
            (_ALIASES[0],),
        )
        write_conn.commit()
    finally:
        write_conn.close()

    conn = open_db(db)
    try:
        cat = Catalog(conn)
        columns = [s.delivery_column_name for s in cat.states(_FQID)]
        assert "LoneInk_BASE" in columns
        for column in _ALIASES:
            assert column in columns
    finally:
        conn.close()
