"""Catalog equivalence across storage IDs without hiding changed facts or scope."""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from reg_meta.db import register_py_lower
from reg_meta_build.db import DDL, _value_set_hash
from reg_meta_build.semantic_diff import (
    UnsupportedSemanticSurface,
    diff_catalog_semantics,
)

if TYPE_CHECKING:
    from pathlib import Path


def _catalog(path: Path, *, offset: int = 0, split: bool = False) -> None:
    def key(number: int) -> int:
        return offset + number

    with closing(sqlite3.connect(path)) as conn:
        register_py_lower(conn)
        conn.executescript(DDL)
        conn.execute("INSERT INTO provider VALUES (?, 'scb', 'SCB')", (key(1),))
        conn.execute(
            "INSERT INTO register VALUES (?, ?, 'Example', 'Register purpose', 'example')",
            (key(2), key(1)),
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name, slug) "
            "VALUES (?, ?, 'Individuals', 'individuals')",
            (key(3), key(2)),
        )
        conn.execute(
            "INSERT INTO register_version (regver_id, register_variant_id, registerversionnamn) "
            "VALUES (?, ?, '2000')",
            (key(4), key(3)),
        )
        conn.execute(
            "INSERT INTO population VALUES (?, 'Population', 'Definition', 'Comment', 'Period')",
            (key(4),),
        )
        conn.execute(
            "INSERT INTO object_type VALUES (?, 'Person', 'Object definition')",
            (key(4),),
        )
        for number, slug in ((5, "benefit"), (6, "other")):
            conn.execute(
                "INSERT INTO variable (variable_id, register_id, provider_key, slug, name, definition, "
                "description, operational_definition, source_register_id) "
                "VALUES (?, ?, ?, ?, ?, 'Definition', 'Description', 'Operation', ?)",
                (key(number), key(2), str(number), slug, slug, key(2)),
            )
        conn.execute(
            "INSERT INTO classification (id, short_name, name, slug, code_count) "
            "VALUES (?, 'Example coding', 'Classification', 'example-coding', 1)",
            (key(7),),
        )
        conn.execute(
            "INSERT INTO value_code VALUES (?, '01', 'Employment', 1)", (key(8),)
        )
        conn.execute(
            "INSERT INTO value_set VALUES (?, ?)",
            (key(9), _value_set_hash([("01", "Employment")])),
        )
        conn.execute("INSERT INTO value_set_member VALUES (?, ?)", (key(9), key(8)))
        conn.execute(
            "INSERT INTO classification_code VALUES (?, ?, 1, 1)", (key(7), key(8))
        )
        windows = (
            (("2000-01-01", "2000-12-31"), ("2001-01-01", "2001-12-31"))
            if split
            else (("2000-01-01", "2001-12-31"),)
        )
        for index, (start, end) in enumerate(windows):
            conn.execute(
                "INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, "
                "valid_to, data_type, data_length, delivery_column_name, provenance, value_set_id, classification_id) "
                "VALUES (?, ?, ?, ?, ?, NULL, NULL, 'Benefit', 'Reviewed declaration', ?, ?)",
                (key(20 + index), key(5), key(3), start, end, key(9), key(7)),
            )
        conn.execute(
            "INSERT INTO variable_alias VALUES (?, ?, 'Benefit')", (key(5), key(3))
        )
        conn.execute(
            "INSERT INTO variable_alias_window VALUES (?, ?, 'Benefit', '2000-01-01', '2001-12-31', 'Alias reason')",
            (key(5), key(3)),
        )
        conn.execute("INSERT INTO code_variable_map VALUES (?, ?)", (key(8), key(5)))
        conn.execute(
            "INSERT INTO concept_group VALUES (?, 'variable', ?, 'example-group', 'Example group', 'curated')",
            (key(10), key(2)),
        )
        conn.execute(
            "INSERT INTO concept_group_axis VALUES (?, 'part', 0, 'Part')", (key(10),)
        )
        conn.execute(
            "INSERT INTO concept_group_variable VALUES (?, ?, ?, 'Benefit')",
            (key(11), key(10), key(5)),
        )
        conn.execute(
            "INSERT INTO concept_group_variable_facet VALUES (?, 'part', 'one', 'First')",
            (key(11),),
        )
        conn.execute(
            "INSERT INTO tag VALUES (?, 'benefits', 'Benefits', 'Tag description')",
            (key(12),),
        )
        conn.execute(
            "INSERT INTO tag_member VALUES (?, NULL, ?, 1, 0, 'Tag note')",
            (key(12), key(5)),
        )
        conn.execute(
            "INSERT INTO variable_same_as VALUES ('scb', 'example', 'benefit', 'scb', 'example', 'other')"
        )
        conn.execute(
            "INSERT INTO import_manifest VALUES ('source_revision', 'exact-input-revision')"
        )
        conn.commit()


def _change(path: Path, sql: str) -> None:
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(sql)
        conn.commit()


def _linked_facts(path: Path, *, offset: int = 0, split_source: bool = False) -> None:
    def key(number: int) -> int:
        return offset + number

    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            "INSERT INTO value_code VALUES (?, '02', 'Unclassified', 2)", (key(13),)
        )
        conn.execute("UPDATE value_code SET mapping_count = 2")
        conn.execute("INSERT INTO value_set_member VALUES (?, ?)", (key(9), key(13)))
        conn.execute(
            "UPDATE value_set SET member_hash = ?",
            (_value_set_hash([("01", "Employment"), ("02", "Unclassified")]),),
        )
        conn.executemany(
            "INSERT INTO code_variable_map VALUES (?, ?)",
            ((key(8), key(6)), (key(13), key(5)), (key(13), key(6))),
        )
        windows = (
            (("2000-01-01", "2000-12-31"), ("2001-01-01", "2001-12-31"))
            if split_source
            else (("2000-01-01", "2001-12-31"),)
        )
        for index, (start, end) in enumerate(windows):
            conn.execute(
                "INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, "
                "valid_to, delivery_column_name, provenance, value_set_id, classification_id) "
                "VALUES (?, ?, ?, ?, ?, 'SourceBenefit', 'Source evidence', ?, ?)",
                (key(30 + index), key(6), key(3), start, end, key(9), key(7)),
            )
        conn.execute(
            "INSERT INTO classification_conformance "
            "SELECT state_id, ?, 'kept', 2, 1, 1, 0.5 FROM variable_state",
            (key(7),),
        )
        conn.execute(
            "INSERT INTO classification_conformance_code SELECT state_id, ? FROM variable_state",
            (key(13),),
        )
        conn.execute(
            "INSERT INTO variable_state_lineage "
            "SELECT consumer.state_id, source.state_id, "
            "MAX(consumer.valid_from, source.valid_from), MIN(consumer.valid_to, source.valid_to) "
            "FROM variable_state consumer JOIN variable_state source "
            "ON consumer.valid_from <= source.valid_to AND source.valid_from <= consumer.valid_to "
            "WHERE consumer.variable_id = ? AND source.variable_id = ?",
            (key(5), key(6)),
        )
        conn.execute(
            "INSERT INTO variable_state_lineage_warning "
            "SELECT state_id, 'ambiguous_source_variant', 'Review source scope' "
            "FROM variable_state WHERE variable_id = ?",
            (key(5),),
        )
        conn.commit()


def test_storage_id_renumbering_is_equivalent_and_inputs_are_readonly(
    tmp_path: Path,
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right, offset=1000)
    before = [hashlib.sha256(path.read_bytes()).digest() for path in (left, right)]
    result = diff_catalog_semantics(left, right)
    assert result.identical
    assert before == [
        hashlib.sha256(path.read_bytes()).digest() for path in (left, right)
    ]
    assert result.content.db_a == left
    assert result.content.db_b == right


def test_adjacent_states_with_identical_facts_are_equivalent(tmp_path: Path) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right, offset=1000, split=True)
    assert diff_catalog_semantics(left, right).identical


def test_equivalent_alias_window_segmentation_is_neutral(tmp_path: Path) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right)
    _change(right, "UPDATE variable_alias_window SET valid_to = '2000-12-31'")
    _change(
        right,
        "INSERT INTO variable_alias_window VALUES "
        "(5, 3, 'Benefit', '2001-01-01', '2001-12-31', 'Alias reason')",
    )
    assert diff_catalog_semantics(left, right).identical


@pytest.mark.parametrize(
    ("sql", "table"),
    [
        (
            "UPDATE variable_state SET delivery_column_name = 'OtherColumn'",
            "variable_state",
        ),
        ("UPDATE variable_state SET valid_from = '2000-01-02'", "variable_state"),
        ("UPDATE value_code SET code = '1'", "value_code"),
        ("UPDATE value_code SET label = 'Changed meaning'", "value_code"),
        ("DELETE FROM variable_same_as", "variable_same_as"),
        ("UPDATE register SET purpose = 'Other purpose'", "register"),
        ("UPDATE population SET definition = 'Other population'", "population"),
        (
            "UPDATE concept_group_variable_facet SET value = 'two'",
            "concept_group_variable_facet",
        ),
        ("UPDATE tag_member SET note = 'Changed tag reason'", "tag_member"),
        (
            "UPDATE variable_state SET provenance = 'Different evidence'",
            "variable_state",
        ),
        ("UPDATE import_manifest SET value = 'different-input'", "import_manifest"),
    ],
)
def test_changed_facts_have_actionable_table_and_fqid_samples(
    tmp_path: Path, sql: str, table: str
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right, offset=1000)
    _change(right, sql)
    report = diff_catalog_semantics(left, right)
    assert not report.identical
    difference = next(
        result for result in report.content.table_results if result.table == table
    )
    assert not difference.identical
    assert difference.sample_a_not_b or difference.sample_b_not_a
    if table == "variable_state":
        assert "scb/example/benefit" in difference.sample_a_not_b[0].values


def test_state_overlap_multiplicity_is_not_unioned_away(tmp_path: Path) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right, split=True)
    _change(
        right,
        "UPDATE variable_state SET valid_to = '2001-01-01' WHERE valid_from = '2000-01-01'",
    )
    report = diff_catalog_semantics(left, right)
    difference = next(
        result
        for result in report.content.table_results
        if result.table == "variable_state"
    )
    assert not difference.identical
    multiplicity = difference.columns.index("multiplicity")
    assert any(sample.values[multiplicity] == 2 for sample in difference.sample_b_not_a)


def test_state_gap_and_provenance_boundary_prevent_coalescing(tmp_path: Path) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right, split=True)
    _change(
        right,
        "UPDATE variable_state SET valid_from = '2001-01-02', provenance = 'Other evidence' WHERE valid_from = '2001-01-01'",
    )
    assert not diff_catalog_semantics(left, right).identical


def test_schema_differences_are_reported_separately(tmp_path: Path) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right)
    _change(right, "CREATE INDEX changed_index ON register(name)")
    report = diff_catalog_semantics(left, right)
    assert report.schema.schema_differs
    assert report.content.identical
    assert not report.identical


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO classification_candidate VALUES (5, 9, 7)",
        "CREATE TABLE unexpected_surface (id INTEGER)",
    ],
)
def test_unsupported_surfaces_fail_explicitly(tmp_path: Path, sql: str) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right)
    _change(right, sql)
    with pytest.raises(
        UnsupportedSemanticSurface, match="projection|unsupported catalog tables"
    ):
        diff_catalog_semantics(left, right)


def test_invalid_identity_or_reference_cannot_compare_equal(tmp_path: Path) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right)
    _change(right, "UPDATE variable_state SET variable_id = 999999")
    with pytest.raises(UnsupportedSemanticSurface, match="broken foreign-key"):
        diff_catalog_semantics(left, right)


def test_reversed_interval_cannot_cancel_itself_out_of_comparison(
    tmp_path: Path,
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right)
    with closing(sqlite3.connect(right)) as conn:
        conn.execute("PRAGMA ignore_check_constraints=ON")
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name) VALUES "
            "(99, 5, 3, '2001-01-01', '2000-12-31', 'Broken')"
        )
        conn.commit()
    result = diff_catalog_semantics(left, right)
    difference = next(
        row for row in result.content.table_results if row.table == "variable_state"
    )
    assert not difference.identical
    sample = dict(
        zip(difference.columns, difference.sample_b_not_a[0].values, strict=True)
    )
    assert sample["valid_from"] == "2001-01-01"
    assert sample["valid_to"] == "2000-12-31"
    assert sample["interval_representation"] == "opaque"
    for path in (left, right):
        _change(path, "DROP INDEX idx_variable_state_unique")
    with closing(sqlite3.connect(right)) as conn:
        conn.execute("PRAGMA ignore_check_constraints=ON")
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name) VALUES "
            "(100, 5, 3, '2001-01-01', '2000-12-31', 'Broken')"
        )
        conn.commit()
    repeated = next(
        row
        for row in diff_catalog_semantics(left, right).content.table_results
        if row.table == "variable_state"
    )
    assert (difference.count_b, repeated.count_b) == (2, 3)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2000-01-01", "2006-02-29"),
        ("2000-01-01", "2019-02-29"),
        ("2001-01-01", "2000-12-31"),
    ],
)
def test_identical_invalid_alias_bounds_remain_comparable(
    tmp_path: Path, start: str, end: str
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right, offset=1000)
    for path in (left, right):
        with closing(sqlite3.connect(path)) as conn:
            conn.execute("PRAGMA ignore_check_constraints=ON")
            conn.execute(
                "UPDATE variable_alias_window SET valid_from = ?, valid_to = ?",
                (start, end),
            )
            conn.commit()
    assert diff_catalog_semantics(left, right).identical


def test_invalid_alias_bounds_are_not_normalized_to_valid_coverage(
    tmp_path: Path,
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right)
    _change(left, "UPDATE variable_alias_window SET valid_to = '2019-02-29'")
    _change(right, "UPDATE variable_alias_window SET valid_to = '2019-02-28'")
    result = diff_catalog_semantics(left, right)
    difference = next(
        row
        for row in result.content.table_results
        if row.table == "variable_alias_window"
    )
    assert not difference.identical
    old = dict(
        zip(difference.columns, difference.sample_a_not_b[0].values, strict=True)
    )
    new = dict(
        zip(difference.columns, difference.sample_b_not_a[0].values, strict=True)
    )
    assert old["valid_to"] == "2019-02-29"
    assert old["interval_representation"] == "opaque"
    assert new["valid_to"] == "2019-02-28"
    assert new["interval_representation"] == "coverage"


@pytest.mark.parametrize(
    ("consumer_split", "source_split"), [(True, False), (False, True), (True, True)]
)
def test_state_linked_facts_are_equivalent_across_independent_splits(
    tmp_path: Path, consumer_split: bool, source_split: bool
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _linked_facts(left)
    _catalog(right, offset=1000, split=consumer_split)
    _linked_facts(right, offset=1000, split_source=source_split)
    report = diff_catalog_semantics(left, right)
    assert report.identical, [
        r.table for r in report.content.table_results if not r.identical
    ]


@pytest.mark.parametrize(
    ("sql", "table"),
    [
        (
            "UPDATE classification_conformance SET checked_code_count = 3",
            "classification_conformance",
        ),
        (
            "DELETE FROM classification_conformance_code",
            "classification_conformance_code",
        ),
        (
            "UPDATE variable_state_lineage SET source_state_id = consumer_state_id",
            "variable_state_lineage",
        ),
        (
            "UPDATE variable_state_lineage SET consumer_state_id = source_state_id",
            "variable_state_lineage",
        ),
        (
            "UPDATE variable_state SET delivery_column_name = 'OtherSource' WHERE state_id = 1030",
            "variable_state_lineage",
        ),
        (
            "UPDATE variable_state_lineage SET valid_from = '2000-01-02'",
            "variable_state_lineage",
        ),
        (
            "UPDATE variable_state_lineage_warning SET message = 'New warning'",
            "variable_state_lineage_warning",
        ),
    ],
)
def test_changed_state_linked_facts_are_detected(
    tmp_path: Path, sql: str, table: str
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _linked_facts(left)
    _catalog(right, offset=1000)
    _linked_facts(right, offset=1000)
    _change(right, sql)
    result = diff_catalog_semantics(left, right)
    assert not result.identical
    assert not next(
        r for r in result.content.table_results if r.table == table
    ).identical


def test_lineage_outside_endpoint_period_is_compared_exactly(tmp_path: Path) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _linked_facts(left)
    _catalog(right)
    _linked_facts(right)
    _change(right, "UPDATE variable_state_lineage SET valid_to = '2002-01-01'")
    result = diff_catalog_semantics(left, right)
    difference = next(
        row
        for row in result.content.table_results
        if row.table == "variable_state_lineage"
    )
    assert not difference.identical
    sample = dict(
        zip(difference.columns, difference.sample_b_not_a[0].values, strict=True)
    )
    assert sample["valid_to"] == "2002-01-01"


def test_historical_events_preserve_native_tokens_and_duplicate_declarations(
    tmp_path: Path,
) -> None:
    left, right = tmp_path / "left.sqlite", tmp_path / "right.sqlite"
    _catalog(left)
    _catalog(right, offset=1000)
    for path, event_id in ((left, 1), (right, 2001)):
        _change(
            path,
            "INSERT INTO timeseries_event VALUES "
            f"({event_id}, '2000', 'Tidsseriebrott', 'Evidence', 'RegisterVersion', '0007', '', '009')",
        )
    assert diff_catalog_semantics(left, right).identical
    _change(right, "UPDATE timeseries_event SET id1 = '7'")
    assert not diff_catalog_semantics(left, right).identical
    _change(right, "UPDATE timeseries_event SET id1 = '0007'")
    _change(
        right,
        "INSERT INTO timeseries_event VALUES "
        "(2002, '2000', 'Tidsseriebrott', 'Evidence', 'RegisterVersion', '0007', '', '009')",
    )
    result = diff_catalog_semantics(left, right)
    event_diff = next(
        r for r in result.content.table_results if r.table == "timeseries_event"
    )
    assert not event_diff.identical
    assert (event_diff.count_a, event_diff.count_b) == (1, 2)
