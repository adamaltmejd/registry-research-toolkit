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


def test_curated_partial_state_window_preserves_base_and_earlier_alias(
    tmp_path: Path,
) -> None:
    """A curated 2018 alias must not expand to its 2017–2018 state's hull."""
    db = _build_multi_alias_db(tmp_path)
    write_conn = sqlite3.connect(db)
    try:
        variable_id, variant_id, base_column, state_id = write_conn.execute(
            "SELECT vs.variable_id, vs.register_variant_id, "
            "vs.delivery_column_name, vs.state_id FROM variable_state vs "
            "JOIN variable v ON v.variable_id = vs.variable_id "
            "WHERE v.slug = 'loneink-lisa2006'",
        ).fetchone()
        curated_column = next(column for column in _ALIASES if column != base_column)
        write_conn.execute("DELETE FROM variable_alias_window")
        write_conn.execute(
            "UPDATE variable_state SET valid_from = '2017-01-01', "
            "operational_definition = 'Base representation meaning', "
            "provenance = ? "
            "WHERE state_id = ?",
            ("errata:base-state\nBase correction", state_id),
        )
        # Model the same existing alias's earlier source-covered era. The new
        # window family belongs only to the later state, so this row must remain
        # a normal base representation.
        write_conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "data_length, delivery_column_name, source_register_text, "
            "operational_definition, provenance, value_set_id, "
            "value_set_version_label, classification_id) "
            "SELECT variable_id, register_variant_id, '2013-01-01', '2015-12-31', "
            "data_type, data_length, ?, source_register_text, NULL, provenance, "
            "value_set_id, value_set_version_label, classification_id "
            "FROM variable_state WHERE state_id = ?",
            (curated_column, state_id),
        )
        write_conn.execute(
            "INSERT INTO variable_alias_window "
            "(variable_id, register_variant_id, delivery_column_name, valid_from, "
            "valid_to, provenance) VALUES (?, ?, ?, '2017-01-01', "
            "'2018-12-31', NULL)",
            (variable_id, variant_id, base_column),
        )
        write_conn.execute(
            "INSERT INTO variable_alias_window "
            "(variable_id, register_variant_id, delivery_column_name, valid_from, "
            "valid_to, provenance) VALUES (?, ?, ?, '2018-01-01', "
            "'2018-12-31', 'errata:test\nSWECOV holds it')",
            (variable_id, variant_id, curated_column),
        )
        write_conn.commit()
    finally:
        write_conn.close()

    conn = open_db(db)
    try:
        cat = Catalog(conn)
        assert [
            state.delivery_column_name for state in cat.resolve_at(_FQID, "2014")
        ] == [curated_column]
        assert cat.resolve_at(_FQID, "2016") == []

        states_2017 = cat.resolve_at(_FQID, "2017")
        assert [state.delivery_column_name for state in states_2017] == [base_column]
        # Existing alias-window behavior still suppresses the base column's
        # column-specific operational definition on every expanded representation.
        assert states_2017[0].operational_definition is None
        assert states_2017[0].provenance == "errata:base-state\nBase correction"

        states_2018 = cat.resolve_at(_FQID, "2018")
        assert {state.delivery_column_name for state in states_2018} == {
            base_column,
            curated_column,
        }
        base = next(
            state for state in states_2018 if state.delivery_column_name == base_column
        )
        alias = next(
            state
            for state in states_2018
            if state.delivery_column_name == curated_column
        )
        assert base.valid_from == "2017-01-01"
        assert base.valid_to == "2018-12-31"
        assert base.operational_definition is None
        assert alias.valid_from == "2018-01-01"
        assert alias.valid_to == "2018-12-31"
        assert alias.provenance == "errata:test\nSWECOV holds it"
        assert alias.operational_definition is None
        assert (alias.state_id, alias.data_type, alias.data_length, alias.value_set_id) == (
            base.state_id,
            base.data_type,
            base.data_length,
            base.value_set_id,
        )
    finally:
        conn.close()


def test_provenance_column_keeps_monthly_window_selection_unchanged(
    tmp_path: Path,
) -> None:
    db = _build_multi_alias_db(tmp_path)
    write_conn = sqlite3.connect(db)
    try:
        variable_id, variant_id, base_column = write_conn.execute(
            "SELECT vs.variable_id, vs.register_variant_id, vs.delivery_column_name "
            "FROM variable_state vs JOIN variable v "
            "ON v.variable_id = vs.variable_id "
            "WHERE v.slug = 'loneink-lisa2006'",
        ).fetchone()
        february_column = next(column for column in _ALIASES if column != base_column)
        write_conn.execute("DELETE FROM variable_alias_window")
        write_conn.executemany(
            "INSERT INTO variable_alias_window "
            "(variable_id, register_variant_id, delivery_column_name, valid_from, "
            "valid_to, provenance) VALUES (?, ?, ?, ?, ?, NULL)",
            [
                (
                    variable_id,
                    variant_id,
                    base_column,
                    "2018-01-01",
                    "2018-01-31",
                ),
                (
                    variable_id,
                    variant_id,
                    february_column,
                    "2018-02-01",
                    "2018-02-28",
                ),
            ],
        )
        write_conn.commit()
    finally:
        write_conn.close()

    conn = open_db(db)
    try:
        cat = Catalog(conn)
        assert [
            state.delivery_column_name for state in cat.resolve_at(_FQID, "2018-01")
        ] == [base_column]
        assert [
            state.delivery_column_name for state in cat.resolve_at(_FQID, "2018-02")
        ] == [february_column]
    finally:
        conn.close()
