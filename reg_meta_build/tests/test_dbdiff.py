"""Tests for the content-level DB diff harness (`reg_meta_build.dbdiff`).

These build tiny on-disk SQLite DBs in ``tmp_path`` and never touch the
330 MB real DBs, so the suite stays fast. They cover the four A4.1
requirements: schema compare, order-independent BLOB-safe content
fingerprint, the minimal ignore list, and actionable mismatch output —
plus the explicit cases the brief calls out (planted schema diff,
row-count diff, single-cell diff, BLOB diff, duplicate-row diff).
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.dbdiff import (
    DEFAULT_IGNORE,
    TableIgnore,
    _row_hash as row_hash,
    diff_db_content,
    format_report,
    main,
)

if TYPE_CHECKING:
    from pathlib import Path

# --------------------------------------------------------------------------
# Fixture builders
# --------------------------------------------------------------------------

# A representative schema: an autoincrement PK + TEXT + BLOB (the member_hash
# analogue), a duplicate-allowing table (no PK/unique), a named index, and the
# KV import_manifest with a schema_version + a nondeterministic import_date.
_DDL = (
    "CREATE TABLE widget ("
    "  id INTEGER PRIMARY KEY,"
    "  name TEXT NOT NULL,"
    "  payload BLOB,"
    "  source_id INTEGER"
    ")",
    "CREATE INDEX idx_widget_name ON widget(name)",
    "CREATE TABLE tag (label TEXT, n INTEGER)",  # no unique → duplicates allowed
    "CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
)

_WIDGETS = [
    (1, "alpha", b"\x00\x01\x02", 100),
    (2, "beta", b"\xff\xfe", None),
    (3, "gamma", None, 300),
]
_TAGS = [("x", 1), ("y", 2), ("x", 1)]  # ("x", 1) appears twice
_MANIFEST = [
    ("schema_version", "5.1.0"),
    ("import_date", "2026-06-01T06:24:08Z"),
    ("input_dir", "/some/path"),
]


def _build(path: Path, *, widgets=_WIDGETS, tags=_TAGS, manifest=_MANIFEST) -> None:
    conn = sqlite3.connect(path)
    try:
        for stmt in _DDL:
            conn.execute(stmt)
        conn.executemany("INSERT INTO widget VALUES (?, ?, ?, ?)", widgets)
        conn.executemany("INSERT INTO tag VALUES (?, ?)", tags)
        conn.executemany("INSERT INTO import_manifest VALUES (?, ?)", manifest)
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def db_a(tmp_path: Path) -> Path:
    p = tmp_path / "a.db"
    _build(p)
    return p


@pytest.fixture
def db_b(tmp_path: Path) -> Path:
    p = tmp_path / "b.db"
    _build(p)
    return p


# --------------------------------------------------------------------------
# Row canonicalization (pure function)
# --------------------------------------------------------------------------


class TestRowHash:
    def test_type_discrimination(self):
        # int 1, text "1", blob b"1", and NULL must all hash differently.
        hashes = {
            row_hash((1,)),
            row_hash(("1",)),
            row_hash((b"1",)),
            row_hash((None,)),
        }
        assert len(hashes) == 4

    def test_length_prefix_prevents_column_bleed(self):
        # ("a", "b") must not collide with ("ab", "") — the length prefix
        # makes column boundaries unambiguous.
        assert row_hash(("a", "b")) != row_hash(("ab", ""))

    def test_blob_is_byte_exact(self):
        assert row_hash((b"\x00\x01",)) != row_hash((b"\x00\x02",))
        assert row_hash((b"\x00\x01",)) == row_hash((b"\x00\x01",))

    def test_deterministic(self):
        row = (3, "gamma", b"\xff", None)
        assert row_hash(row) == row_hash(row)


# --------------------------------------------------------------------------
# Identical DBs
# --------------------------------------------------------------------------


class TestIdentical:
    def test_identical_dbs_match(self, db_a: Path, db_b: Path):
        report = diff_db_content(db_a, db_b)
        assert report.identical
        assert not report.schema_differs
        assert not report.content_differs
        assert "[IDENTICAL]" in format_report(report)

    def test_content_is_order_independent(self, tmp_path: Path):
        # Same rows, different insert order → still identical (the multiset
        # fingerprint is order-independent).
        a = tmp_path / "a.db"
        b = tmp_path / "b.db"
        _build(a, widgets=_WIDGETS)
        _build(b, widgets=list(reversed(_WIDGETS)), tags=list(reversed(_TAGS)))
        assert diff_db_content(a, b).identical

    def test_byte_copy_identical_including_import_date(
        self, db_a: Path, tmp_path: Path
    ):
        # A literal file copy must match even with the ignore list disabled
        # (mirrors the real preserved baseline, which is a byte copy).
        copy = tmp_path / "copy.db"
        copy.write_bytes(db_a.read_bytes())
        assert diff_db_content(db_a, copy, ignore={}).identical


# --------------------------------------------------------------------------
# Content differences (the brief's explicit cases)
# --------------------------------------------------------------------------


class TestContentDiffs:
    def test_single_cell_diff(self, db_a: Path, tmp_path: Path):
        b = tmp_path / "b.db"
        _build(b, widgets=[(1, "ALPHA", b"\x00\x01\x02", 100), *_WIDGETS[1:]])
        report = diff_db_content(db_a, b)
        assert not report.identical
        widget = next(r for r in report.table_results if r.table == "widget")
        assert not widget.identical
        assert widget.count_a == widget.count_b  # same count, content drift
        assert len(widget.sample_a_not_b) == 1
        assert len(widget.sample_b_not_a) == 1
        # The differing row is the one we changed (id=1), shown in both
        # directions with its old/new name.
        a_row = dict(zip(widget.columns, widget.sample_a_not_b[0].values))
        b_row = dict(zip(widget.columns, widget.sample_b_not_a[0].values))
        assert a_row["id"] == 1 and a_row["name"] == "alpha"
        assert b_row["id"] == 1 and b_row["name"] == "ALPHA"
        # Other tables still match.
        tag = next(r for r in report.table_results if r.table == "tag")
        assert tag.identical

    def test_row_count_diff(self, db_a: Path, tmp_path: Path):
        b = tmp_path / "b.db"
        _build(b, widgets=[*_WIDGETS, (4, "delta", None, 400)])
        report = diff_db_content(db_a, b)
        assert not report.identical
        widget = next(r for r in report.table_results if r.table == "widget")
        assert widget.count_a == 3
        assert widget.count_b == 4
        # The extra row is present in B not A.
        assert len(widget.sample_a_not_b) == 0
        assert len(widget.sample_b_not_a) == 1
        extra = dict(zip(widget.columns, widget.sample_b_not_a[0].values))
        assert extra["id"] == 4 and extra["name"] == "delta"
        assert widget.sample_b_not_a[0].net == -1

    def test_blob_diff(self, db_a: Path, tmp_path: Path):
        # Only the BLOB payload differs — proves the fingerprint is BLOB-safe.
        b = tmp_path / "b.db"
        _build(b, widgets=[(1, "alpha", b"\x00\x01\x03", 100), *_WIDGETS[1:]])
        report = diff_db_content(db_a, b)
        widget = next(r for r in report.table_results if r.table == "widget")
        assert not widget.identical
        rendered = format_report(report)
        assert "<blob" in rendered  # BLOB rendered as hex preview, not crash

    def test_null_vs_value_diff(self, db_a: Path, tmp_path: Path):
        # NULL source_id in A vs a value in B for the same row → caught
        # (NULL-aware canonicalization).
        b = tmp_path / "b.db"
        _build(b, widgets=[_WIDGETS[0], (2, "beta", b"\xff\xfe", 999), _WIDGETS[2]])
        report = diff_db_content(db_a, b)
        widget = next(r for r in report.table_results if r.table == "widget")
        assert not widget.identical

    def test_duplicate_row_diff(self, db_a: Path, tmp_path: Path):
        # Same distinct rows, but a different multiplicity: A has ("x",1)
        # twice, B has it once. XOR would miss this; the summed fingerprint
        # catches it.
        b = tmp_path / "b.db"
        _build(b, tags=[("x", 1), ("y", 2)])
        report = diff_db_content(db_a, b)
        tag = next(r for r in report.table_results if r.table == "tag")
        assert not tag.identical
        assert tag.count_a == 3 and tag.count_b == 2
        # The surplus copy in A shows up as net +1 in A-not-B.
        assert len(tag.sample_a_not_b) == 1
        assert tag.sample_a_not_b[0].net == 1
        assert dict(zip(tag.columns, tag.sample_a_not_b[0].values)) == {
            "label": "x",
            "n": 1,
        }


# --------------------------------------------------------------------------
# Schema differences
# --------------------------------------------------------------------------


class TestSchemaDiffs:
    def test_table_only_in_one(self, db_a: Path, tmp_path: Path):
        b = tmp_path / "b.db"
        _build(b)
        conn = sqlite3.connect(b)
        conn.execute("CREATE TABLE extra (x INTEGER)")
        conn.commit()
        conn.close()
        report = diff_db_content(db_a, b)
        assert not report.identical
        assert report.schema_differs
        assert "extra" in report.tables_only_in_b
        assert "extra" not in report.tables_only_in_a

    def test_column_diff_skips_content(self, db_a: Path, tmp_path: Path):
        b = tmp_path / "b.db"
        conn = sqlite3.connect(b)
        for stmt in _DDL:
            conn.execute(stmt)
        conn.execute("ALTER TABLE widget ADD COLUMN note TEXT")
        conn.executemany("INSERT INTO tag VALUES (?, ?)", _TAGS)
        conn.executemany("INSERT INTO import_manifest VALUES (?, ?)", _MANIFEST)
        conn.executemany(
            "INSERT INTO widget (id, name, payload, source_id) VALUES (?, ?, ?, ?)",
            _WIDGETS,
        )
        conn.commit()
        conn.close()
        report = diff_db_content(db_a, b)
        assert report.schema_differs
        cd = next(c for c in report.column_diffs if c.table == "widget")
        assert "note" in cd.only_in_b
        # Content comparison for widget is skipped (a fingerprint over
        # mismatched columns would be meaningless).
        widget = next(r for r in report.table_results if r.table == "widget")
        assert widget.skipped_reason is not None
        assert not widget.identical

    def test_index_diff(self, db_a: Path, tmp_path: Path):
        b = tmp_path / "b.db"
        _build(b)
        conn = sqlite3.connect(b)
        conn.execute("DROP INDEX idx_widget_name")
        conn.commit()
        conn.close()
        report = diff_db_content(db_a, b)
        assert report.schema_differs
        assert "idx_widget_name" in report.indexes_only_in_a


# --------------------------------------------------------------------------
# Ignore list
# --------------------------------------------------------------------------


class TestIgnore:
    def test_import_date_ignored_by_default(self, db_a: Path, tmp_path: Path):
        # Only import_date differs → identical under the default ignore.
        b = tmp_path / "b.db"
        _build(
            b,
            manifest=[
                ("schema_version", "5.1.0"),
                ("import_date", "1999-01-01T00:00:00Z"),
                ("input_dir", "/some/path"),
            ],
        )
        assert "import_manifest" in DEFAULT_IGNORE
        assert diff_db_content(db_a, b).identical

    def test_import_date_caught_without_ignore(self, db_a: Path, tmp_path: Path):
        b = tmp_path / "b.db"
        _build(
            b,
            manifest=[
                ("schema_version", "5.1.0"),
                ("import_date", "1999-01-01T00:00:00Z"),
                ("input_dir", "/some/path"),
            ],
        )
        report = diff_db_content(db_a, b, ignore={})
        assert not report.identical
        manifest = next(r for r in report.table_results if r.table == "import_manifest")
        assert not manifest.identical
        a_row = dict(zip(manifest.columns, manifest.sample_a_not_b[0].values))
        assert a_row["key"] == "import_date"

    def test_schema_version_is_not_ignored(self, db_a: Path, tmp_path: Path):
        # schema_version must always be compared, even under the default ignore.
        b = tmp_path / "b.db"
        _build(
            b,
            manifest=[
                ("schema_version", "9.9.9"),
                ("import_date", "2026-06-01T06:24:08Z"),
                ("input_dir", "/some/path"),
            ],
        )
        report = diff_db_content(db_a, b)  # default ignore
        assert not report.identical
        manifest = next(r for r in report.table_results if r.table == "import_manifest")
        assert not manifest.identical

    def test_drop_columns_ignore(self, db_a: Path, tmp_path: Path):
        # Dropping `name` from the comparison hides a name-only difference.
        b = tmp_path / "b.db"
        _build(b, widgets=[(1, "RENAMED", b"\x00\x01\x02", 100), *_WIDGETS[1:]])
        ignore = {"widget": TableIgnore(drop_columns=frozenset({"name"}))}
        report = diff_db_content(db_a, b, ignore=ignore)
        widget = next(r for r in report.table_results if r.table == "widget")
        assert "name" not in widget.columns
        assert widget.identical


# --------------------------------------------------------------------------
# FTS exclusion + storage-class edge
# --------------------------------------------------------------------------


def _build_with_fts(path: Path, *, populate_fts: bool) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE doc (id INTEGER PRIMARY KEY, body TEXT)")
        conn.execute("CREATE VIRTUAL TABLE doc_fts USING fts5(body, content='doc')")
        conn.executemany(
            "INSERT INTO doc VALUES (?, ?)",
            [(1, "hello world"), (2, "goodbye world")],
        )
        if populate_fts:
            # Rebuild the external-content index. This writes the FTS shadow
            # tables; leaving it empty in the other DB makes their bytes
            # diverge while the base `doc` table stays identical.
            conn.execute("INSERT INTO doc_fts(doc_fts) VALUES ('rebuild')")
        conn.commit()
    finally:
        conn.close()


class TestFtsAndStorageClass:
    def test_fts_tables_excluded_from_content(self, tmp_path: Path):
        a = tmp_path / "a.db"
        b = tmp_path / "b.db"
        _build_with_fts(a, populate_fts=True)
        _build_with_fts(b, populate_fts=False)
        report = diff_db_content(a, b)
        compared = {r.table for r in report.table_results}
        # The base table is compared; the FTS virtual + shadow tables are not.
        assert "doc" in compared
        assert "doc_fts" not in compared
        assert not any(t.startswith("doc_fts_") for t in compared)
        # Divergent FTS index bytes do not break content identity.
        assert report.identical

    def test_storage_class_note(self, tmp_path: Path):
        # A no-affinity column storing INTEGER 1 in A vs REAL 1.0 in B: the
        # type-tagged fingerprint differs, but SQLite's GROUP BY treats them
        # as numerically equal, so the value-level diff is empty → note set.
        a = tmp_path / "a.db"
        b = tmp_path / "b.db"
        for path, value in ((a, 1), (b, 1.0)):
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE nums (v)")  # no affinity
            conn.execute("INSERT INTO nums VALUES (?)", (value,))
            conn.commit()
            conn.close()
        report = diff_db_content(a, b)
        nums = next(r for r in report.table_results if r.table == "nums")
        assert not nums.identical
        assert nums.note is not None
        assert not nums.sample_a_not_b and not nums.sample_b_not_a


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


class TestCli:
    def test_exit_zero_when_identical(self, db_a: Path, db_b: Path, capsys):
        assert main([str(db_a), str(db_b)]) == 0
        assert "[IDENTICAL]" in capsys.readouterr().out

    def test_exit_one_when_different(self, db_a: Path, tmp_path: Path, capsys):
        b = tmp_path / "b.db"
        _build(b, widgets=[(1, "changed", b"\x00\x01\x02", 100), *_WIDGETS[1:]])
        assert main([str(db_a), str(b)]) == 1
        assert "DIFFERENCES FOUND" in capsys.readouterr().out

    def test_exit_two_on_missing_file(self, db_a: Path, tmp_path: Path, capsys):
        assert main([str(db_a), str(tmp_path / "nope.db")]) == 2
        assert "error" in capsys.readouterr().err

    def test_no_default_ignore_flag(self, db_a: Path, tmp_path: Path):
        b = tmp_path / "b.db"
        _build(
            b,
            manifest=[
                ("schema_version", "5.1.0"),
                ("import_date", "1999-01-01T00:00:00Z"),
                ("input_dir", "/some/path"),
            ],
        )
        assert main([str(db_a), str(b)]) == 0  # ignored by default
        assert main(["--no-default-ignore", str(db_a), str(b)]) == 1

    def test_json_output_is_valid(self, db_a: Path, tmp_path: Path, capsys):
        b = tmp_path / "b.db"
        _build(b, widgets=[(1, "changed", b"\x00\x01\x02", 100), *_WIDGETS[1:]])
        main(["--json", str(db_a), str(b)])
        payload = json.loads(capsys.readouterr().out)
        assert payload["identical"] is False
        widget = next(t for t in payload["tables"] if t["table"] == "widget")
        assert widget["identical"] is False
        assert widget["sample_a_not_b"]

    def test_missing_file_raises_in_function(self, db_a: Path, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            diff_db_content(db_a, tmp_path / "nope.db")
