"""Resolver coverage for multi-alias delivery-column representations (#945)."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _representation_fixtures import build_alias_representations
from reg_meta.catalog import Catalog, ValueSetMember
from reg_meta.db import open_db
from reg_meta.queries import (
    get_coded_variables,
    get_schema,
    get_values_by_variable,
    get_varinfo,
)

if TYPE_CHECKING:
    from pathlib import Path

_FQID = "scb/testreg/loneink-lisa2006"
_ALIASES = ("LoneInk_LISA2006", "LoneInk_LISA2007")


def _build_multi_alias_db(tmp_path: Path) -> Path:
    # One concrete SCB cvid can list several delivery headers. They are
    # co-delivered representations of the same state, not search-only aliases.
    return build_alias_representations(tmp_path)


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
        assert states_2017[0].operational_definition == "Base representation meaning"
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
        assert base.operational_definition == "Base representation meaning"
        assert alias.valid_from == "2018-01-01"
        assert alias.valid_to == "2018-12-31"
        assert alias.provenance == "errata:test\nSWECOV holds it"
        assert alias.operational_definition is None
        assert (
            alias.state_id,
            alias.data_type,
            alias.data_length,
            alias.value_set_id,
        ) == (
            base.state_id,
            base.data_type,
            base.data_length,
            base.value_set_id,
        )
    finally:
        conn.close()


def test_curated_window_preserves_partial_family_fallback(tmp_path: Path) -> None:
    """Adding a curated window must not consume an uncovered source-family gap."""
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
            "operational_definition = 'Base representation meaning' "
            "WHERE state_id = ?",
            (state_id,),
        )
        write_conn.execute(
            "INSERT INTO variable_alias_window "
            "(variable_id, register_variant_id, delivery_column_name, valid_from, "
            "valid_to, provenance) VALUES (?, ?, ?, '2017-01-01', "
            "'2017-12-31', NULL)",
            (variable_id, variant_id, base_column),
        )
        write_conn.commit()
    finally:
        write_conn.close()

    before_conn = open_db(db)
    try:
        before_2017 = Catalog(before_conn).resolve_at(_FQID, "2017")
        before_2018 = Catalog(before_conn).resolve_at(_FQID, "2018")
    finally:
        before_conn.close()
    assert len(before_2017) == len(before_2018) == 1
    assert before_2017[0].valid_to == "2017-12-31"
    assert before_2017[0].operational_definition is None
    assert before_2018[0].valid_from == "2017-01-01"
    assert before_2018[0].valid_to == "2018-12-31"
    assert before_2018[0].operational_definition == "Base representation meaning"

    write_conn = sqlite3.connect(db)
    try:
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

    after_conn = open_db(db)
    try:
        cat = Catalog(after_conn)
        assert cat.resolve_at(_FQID, "2017") == before_2017
        after_2018 = cat.resolve_at(_FQID, "2018")
        assert {state.delivery_column_name for state in after_2018} == {
            base_column,
            curated_column,
        }
        base = next(
            state for state in after_2018 if state.delivery_column_name == base_column
        )
        alias = next(
            state
            for state in after_2018
            if state.delivery_column_name == curated_column
        )
        assert base == before_2018[0]
        assert alias.operational_definition is None
        assert alias.provenance == "errata:test\nSWECOV holds it"
        assert (alias.state_id, alias.data_type, alias.data_length) == (
            base.state_id,
            base.data_type,
            base.data_length,
        )
    finally:
        after_conn.close()


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


def test_per_column_coding_preserves_domains_native_labels_and_lazy_loading(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db = _build_multi_alias_db(tmp_path)
    write_conn = sqlite3.connect(db)
    try:
        variable_id, variant_id, state_id, first_set = write_conn.execute(
            "SELECT variable_id, register_variant_id, state_id, value_set_id FROM variable_state WHERE variable_id = 1"
        ).fetchone()
        second_set = write_conn.execute(
            "INSERT INTO value_set (member_hash) VALUES (?) RETURNING value_set_id",
            (b"x" * 32,),
        ).fetchone()[0]
        write_conn.execute(
            "INSERT INTO value_set_member SELECT ?, code_id FROM value_set_member WHERE value_set_id = ? AND code_id = (SELECT MAX(code_id) FROM value_set_member WHERE value_set_id = ?)",
            (second_set, first_set, first_set),
        )
        classification_id = write_conn.execute(
            "INSERT INTO classification (short_name, name, slug) VALUES ('base', 'Base classification', 'base') RETURNING id"
        ).fetchone()[0]
        write_conn.execute(
            "UPDATE variable_state SET classification_id = ?, value_set_id = NULL, value_set_version_label = 'base-label' WHERE state_id = ?",
            (classification_id, state_id),
        )
        write_conn.execute(
            "INSERT INTO classification_conformance VALUES (?, ?, 'kept', 2, 2, 0, 1.0)",
            (state_id, classification_id),
        )
        write_conn.execute("DELETE FROM variable_alias_window")
        write_conn.executemany(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, provenance, coding_metadata, value_set_id, value_set_version_label) VALUES (?, ?, ?, '2018-01-01', '2018-12-31', 'errata:checked-column-coding', 'per_column', ?, 'native-wave')",
            [
                (variable_id, variant_id, column, code_set)
                for column, code_set in zip(
                    _ALIASES, (first_set, second_set), strict=True
                )
            ],
        )
        write_conn.commit()
    finally:
        write_conn.close()
    conn = open_db(db)
    try:
        cat = Catalog(conn)
        states = cat.resolve_at(_FQID, "2018", value_set_version="native-wave")
        assert [s.delivery_column_name for s in states] == list(_ALIASES)
        assert [s.value_set_id for s in states] == [first_set, second_set]
        assert [len(s.value_set) for s in states] == [2, 1]
        assert all(
            s.classification_slug is s.classification_conformance is None
            for s in states
        )
        assert cat.resolve_at(_FQID, "2018", value_set_version="base-label") == []
        assert cat.states(_FQID) == states
        schema = get_schema(conn, register_variant_id=str(variant_id))
        columns = [
            c
            for c in schema["variants"][0]["versions"][0]["columns"]
            if c["variable_id"] == variable_id
        ]
        assert [c["aliases"] for c in columns] == list(_ALIASES)
        assert {c["value_set_version_label"] for c in columns} == {"native-wave"}
        info = get_varinfo(conn, _ALIASES[0])
        assert [i["value_set_count"] for i in info[0]["instances"]] == [2, 1]
        values = get_values_by_variable(conn, _ALIASES[0])
        assert [i["delivery_column_name"] for i in values["instances"]] == list(
            _ALIASES
        )
        assert [len(i["values"]) for i in values["instances"]] == [2, 1]
        assert {i["value_set_version_label"] for i in values["instances"]} == {
            "native-wave"
        }
        assert get_coded_variables(conn)[0]["n_distinct_codes"] == 2

        def unexpected_code_read(*args):
            raise AssertionError("metadata-only resolution must not hydrate codes")

        original_code_reader = cat._value_set_codes
        monkeypatch.setattr(cat, "_value_set_codes", unexpected_code_read)
        metadata = cat.resolve_at(_FQID, "2018", with_codes=False)
        assert [s.value_set_id for s in metadata] == [first_set, second_set]
        assert all(s.value_set is s.value_set_summary is None for s in metadata)
        monkeypatch.setattr(cat, "_value_set_codes", original_code_reader)
        summary = cat.resolve_at(
            _FQID, "2018", with_codes=False, with_code_summary=True
        )
        assert [s.value_set_summary.code_count for s in summary] == [2, 1]
        assert all(s.value_set is s.classification_conformance is None for s in summary)
    finally:
        conn.close()


@pytest.mark.parametrize("mode", ["shared", "per_column"])
def test_column_metadata_preserves_literal_values_and_explicit_nulls(
    tmp_path: Path, mode: str
) -> None:
    db = _build_multi_alias_db(tmp_path)
    write_conn = sqlite3.connect(db)
    try:
        write_conn.execute(
            "UPDATE variable_state SET operational_definition = 'Canonical operation', source_register_text = 'Canonical source' WHERE variable_id = 1"
        )
        if mode == "per_column":
            write_conn.execute(
                "UPDATE variable_alias_window SET column_metadata = 'per_column', data_type = 'integer', data_length = '3', operational_definition = 'Physical operation', source_register_text = 'Physical source' WHERE delivery_column_name = ?",
                (_ALIASES[0],),
            )
            write_conn.execute(
                "UPDATE variable_alias_window SET column_metadata = 'per_column' WHERE delivery_column_name = ?",
                (_ALIASES[1],),
            )
        write_conn.commit()
    finally:
        write_conn.close()
    conn = open_db(db)
    try:
        states = Catalog(conn).resolve_at(_FQID, "2018")
        assert [s.delivery_column_name for s in states] == list(_ALIASES)
        fields = [
            (
                s.data_type,
                s.data_length,
                s.operational_definition,
                s.source_register_text,
            )
            for s in states
        ]
        assert fields == (
            [
                ("integer", "3", "Physical operation", "Physical source"),
                (None, None, None, None),
            ]
            if mode == "per_column"
            else [("integer", "1", None, "Canonical source")] * 2
        )
        assert all(s.value_set_id == 1 and len(s.value_set) == 2 for s in states)
        if mode == "per_column":
            columns = [
                c
                for c in get_schema(conn, register_variant_id="10")["variants"][0][
                    "versions"
                ][0]["columns"]
                if c["variable_id"] == 1
            ]
            assert [
                (c["operational_definition"], c["source_register_text"])
                for c in columns
            ] == [("Physical operation", "Physical source"), (None, None)]
            instances = get_varinfo(conn, _ALIASES[0])[0]["instances"]
            assert [
                (s["operational_definition"], s["source_register_text"])
                for s in instances
            ] == [("Physical operation", "Physical source"), (None, None)]
    finally:
        conn.close()
