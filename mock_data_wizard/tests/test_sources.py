"""Tests for sources.py.

File-source tests run against a real in-process DuckDB. SQL-source
tests use a fake DB-API connection (the dialect-specific SQL we'd emit
doesn't execute against any local backend, and the iterator's logic is
about table selection and handle shape, not query execution).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mock_data_wizard.sources import (
    FileSource,
    SourceHandle,
    SqlSource,
    _build_pyodbc_connstr,
    _is_archived,
    _resolve_table_aliases,
    _resolve_where,
    _wrap_with_where,
    file_source,
    filter_files,
    iter_file_source,
    iter_source,
    iter_sql_source,
    list_files_in_source,
    needs_discovery,
    sql_source,
)


# -- constructors ---------------------------------------------------------


def test_file_source_validates_path():
    with pytest.raises(ValueError, match="non-empty string"):
        file_source("")


def test_file_source_normalises_collections(tmp_path: Path):
    src = file_source(
        str(tmp_path), include=["a.csv", "b.csv"], exclude=["c.csv"], pattern=r"\.csv$"
    )
    assert isinstance(src, FileSource)
    assert src.include == ("a.csv", "b.csv")
    assert src.exclude == ("c.csv",)
    assert src.pattern == r"\.csv$"
    assert src.type == "file"


def test_sql_source_validates_dsn():
    with pytest.raises(ValueError, match="non-empty string"):
        sql_source("")


def test_sql_source_normalises_pattern_and_schema():
    src = sql_source("P1105", pattern="lisa", schema="dbo")
    assert src.pattern == ("lisa",)
    assert src.schema == ("dbo",)
    assert src.type == "sql"


def test_sql_source_accepts_dict_tables():
    src = sql_source("P1105", tables={"persons": "dbo.persons"})
    assert src.tables == {"persons": "dbo.persons"}


# -- needs_discovery ------------------------------------------------------


def test_needs_discovery_file_no_filters_true(tmp_path: Path):
    assert needs_discovery(file_source(str(tmp_path))) is True


def test_needs_discovery_file_with_include_false(tmp_path: Path):
    assert needs_discovery(file_source(str(tmp_path), include=["a.csv"])) is False


def test_needs_discovery_file_all_opts_out(tmp_path: Path):
    assert needs_discovery(file_source(str(tmp_path), all=True)) is False


def test_needs_discovery_sql_no_filters_true():
    assert needs_discovery(sql_source("P1105")) is True


def test_needs_discovery_sql_with_tables_false():
    assert needs_discovery(sql_source("P1105", tables=["dbo.persons"])) is False


def test_needs_discovery_sql_all_opts_out():
    assert needs_discovery(sql_source("P1105", all=True)) is False


def test_needs_discovery_unknown_raises():
    with pytest.raises(TypeError):
        needs_discovery("not a source")


# -- file listing / filtering --------------------------------------------


def _write_csv(p: Path, header: str = "a,b\n1,2\n3,4\n") -> None:
    p.write_text(header, encoding="utf-8")


def test_list_files_in_source_walks_recursively(tmp_path: Path):
    sub = tmp_path / "sub"
    sub.mkdir()
    _write_csv(tmp_path / "a.csv")
    _write_csv(sub / "b.csv")
    (tmp_path / "ignored.bin").write_bytes(b"\x00\x01")
    found = list_files_in_source(file_source(str(tmp_path)))
    names = sorted(p.name for p in found)
    assert names == ["a.csv", "b.csv"]


def test_list_files_in_source_single_file(tmp_path: Path):
    f = tmp_path / "only.csv"
    _write_csv(f)
    found = list_files_in_source(file_source(str(f)))
    assert found == [f.resolve()]


def test_list_files_in_source_missing_path_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        list_files_in_source(file_source(str(tmp_path / "nope")))


def test_filter_files_include_then_exclude(tmp_path: Path):
    files = [tmp_path / "a.csv", tmp_path / "b.csv", tmp_path / "c.csv"]
    src = file_source(str(tmp_path), include=["a.csv", "b.csv"], exclude=["b.csv"])
    out = filter_files(files, src)
    assert [f.name for f in out] == ["a.csv"]


# -- iter_file_source ----------------------------------------------------


def test_iter_file_source_yields_handles_with_queryable_view(tmp_path: Path):
    _write_csv(tmp_path / "alpha.csv", "x,y\n1,one\n2,two\n3,three\n")
    _write_csv(tmp_path / "beta.csv", "p,q\n10,a\n20,b\n")
    src = file_source(str(tmp_path), include=["alpha.csv", "beta.csv"])
    handles = list(iter_file_source(src))
    assert sorted(h.source_name for h in handles) == ["alpha.csv", "beta.csv"]
    # Names map to their files in source_detail
    for h in handles:
        assert h.source_type == "file"
        assert h.dialect == "duckdb"
        assert h.source_detail["path"].endswith(h.source_name)


def test_iter_file_source_view_is_actually_queryable(tmp_path: Path):
    _write_csv(tmp_path / "rows.csv", "x\n1\n2\n3\n4\n5\n")
    src = file_source(str(tmp_path), include=["rows.csv"])
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {handle.table}")
            (n,) = cur.fetchone()
            assert n == 5
        finally:
            cur.close()


def test_iter_file_source_drops_view_between_handles(tmp_path: Path):
    _write_csv(tmp_path / "a.csv", "x\n1\n")
    _write_csv(tmp_path / "b.csv", "x\n2\n")
    src = file_source(str(tmp_path), include=["a.csv", "b.csv"])
    seen_handles: list[SourceHandle] = []
    iterator = iter_file_source(src)
    first = next(iterator)
    seen_handles.append(first)
    # While we hold first, its view exists
    cur = first.conn.cursor()
    cur.execute(f"SELECT 1 FROM {first.table} LIMIT 1")
    cur.fetchone()
    cur.close()
    # Move to next handle. The previous view should now be dropped.
    second = next(iterator)
    seen_handles.append(second)
    cur = first.conn.cursor()
    with pytest.raises(Exception):
        cur.execute(f"SELECT 1 FROM {first.table}")
    cur.close()
    list(iterator)  # exhaust to trigger cleanup


def test_iter_file_source_duplicate_basenames_raises(tmp_path: Path):
    sub = tmp_path / "sub"
    sub.mkdir()
    _write_csv(tmp_path / "x.csv")
    _write_csv(sub / "x.csv")
    with pytest.raises(ValueError, match="Duplicate file basename"):
        list(iter_file_source(file_source(str(tmp_path))))


# -- SQL helpers ---------------------------------------------------------


def test_resolve_table_aliases_from_list_strips_schema():
    out = _resolve_table_aliases(["dbo.persons", "dbo.events"])
    assert out == {"persons": "dbo.persons", "events": "dbo.events"}


def test_resolve_table_aliases_from_dict_passthrough():
    out = _resolve_table_aliases({"p_dbo": "dbo.persons", "p_p1": "P1105.persons"})
    assert out == {"p_dbo": "dbo.persons", "p_p1": "P1105.persons"}


def test_resolve_table_aliases_conflict_raises():
    with pytest.raises(ValueError, match="Ambiguous table aliases"):
        _resolve_table_aliases(["dbo.persons", "P1105.persons"])


def test_is_archived_recognises_x_prefix():
    assert _is_archived("dbo.x_old_persons") is True
    assert _is_archived("x_old") is True
    assert _is_archived("dbo.persons") is False


def test_build_pyodbc_connstr_includes_dsn_and_trusted():
    src = sql_source("P1105")
    s = _build_pyodbc_connstr(src)
    assert s.startswith("DSN=P1105;")
    assert "Trusted_Connection=yes" in s


def test_build_pyodbc_connstr_with_overrides():
    src = sql_source(
        "P1105", driver="ODBC Driver 17 for SQL Server", database="Individ_2018"
    )
    s = _build_pyodbc_connstr(src)
    assert "Driver={ODBC Driver 17 for SQL Server}" in s
    assert "Database=Individ_2018" in s


# -- iter_sql_source against a fake conn ---------------------------------


class _FakeCursor:
    """Minimal cursor that returns the rows we hand it."""

    def __init__(self, rows):
        self._rows = rows
        self.executed: list[str] = []

    def execute(self, sql: str):
        self.executed.append(sql)
        return self

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class _FakeConn:
    def __init__(self, view_rows):
        self._view_rows = view_rows
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._view_rows)

    def close(self):
        self.closed = True


def test_iter_sql_source_explicit_tables():
    src = sql_source("P1105", tables=["dbo.persons", "dbo.events"])
    handles = list(iter_sql_source(src, conn=_FakeConn(view_rows=[])))
    names = [h.source_name for h in handles]
    assert names == ["persons", "events"]
    assert handles[0].dialect == "mssql"
    assert handles[0].source_type == "sql"
    assert handles[0].table == "[dbo].[persons]"
    assert handles[0].source_detail == {
        "dsn": "P1105",
        "database": None,
        "table": "dbo.persons",
    }


def test_iter_sql_source_pattern_filters_discovered_views():
    discovered = [
        ("dbo", "lisa_2018"),
        ("dbo", "lisa_2019"),
        ("dbo", "rams_2018"),
        ("dbo", "x_archived"),
    ]
    src = sql_source("P1105", pattern="lisa")
    handles = list(iter_sql_source(src, conn=_FakeConn(view_rows=discovered)))
    assert sorted(h.source_name for h in handles) == ["lisa_2018", "lisa_2019"]


def test_iter_sql_source_all_includes_everything_except_archived():
    discovered = [
        ("dbo", "lisa_2018"),
        ("dbo", "rams_2018"),
        ("dbo", "x_old_2010"),
    ]
    src = sql_source("P1105", all=True)
    handles = list(iter_sql_source(src, conn=_FakeConn(view_rows=discovered)))
    assert sorted(h.source_name for h in handles) == ["lisa_2018", "rams_2018"]


def test_iter_sql_source_no_tables_after_filter_raises():
    discovered = [("dbo", "rams_2018")]
    src = sql_source("P1105", pattern="lisa")
    with pytest.raises(ValueError, match="no tables selected"):
        list(iter_sql_source(src, conn=_FakeConn(view_rows=discovered)))


def test_iter_sql_source_no_filter_and_no_all_raises():
    # Construct a SqlSource directly with no tables/pattern/all to bypass the
    # constructor's discovery routing.
    raw = SqlSource(dsn="P1105")
    with pytest.raises(ValueError, match="provide one of"):
        list(iter_sql_source(raw, conn=_FakeConn(view_rows=[])))


# -- iter_source dispatch -------------------------------------------------


def test_iter_source_dispatches_to_file(tmp_path: Path):
    _write_csv(tmp_path / "a.csv")
    src = file_source(str(tmp_path), include=["a.csv"])
    handles = list(iter_source(src))
    assert len(handles) == 1
    assert handles[0].source_type == "file"


def test_iter_source_dispatches_to_sql():
    src = sql_source("P1105", tables=["dbo.persons"])
    handles = list(iter_source(src, conn=_FakeConn(view_rows=[])))
    assert handles[0].source_type == "sql"


def test_iter_source_unknown_type_raises():
    with pytest.raises(TypeError):
        list(iter_source("not a source"))


# -- WHERE clauses --------------------------------------------------------


def test_resolve_where_string_applies_to_any_alias():
    assert _resolve_where("AR > 2015", "any_alias") == "AR > 2015"


def test_resolve_where_dict_per_alias():
    where = {"lisa_2018": "AR > 2018", "lisa_2019": "AR > 2019"}
    assert _resolve_where(where, "lisa_2018") == "AR > 2018"
    assert _resolve_where(where, "lisa_2019") == "AR > 2019"
    # Missing alias gets no filter (don't surprise the user with a typo)
    assert _resolve_where(where, "lisa_2020") is None


def test_resolve_where_none_returns_none():
    assert _resolve_where(None, "x") is None
    assert _resolve_where("", "x") is None


def test_resolve_where_invalid_type_raises():
    with pytest.raises(TypeError, match="must be str"):
        _resolve_where(123, "x")  # type: ignore[arg-type]


def test_wrap_with_where_no_clause_passthrough():
    assert _wrap_with_where('"v"', None) == '"v"'
    assert _wrap_with_where('"v"', "") == '"v"'


def test_wrap_with_where_produces_aliased_derived_table():
    out = _wrap_with_where("[dbo].[t]", "AR > 2015")
    assert out == "(SELECT * FROM [dbo].[t] WHERE AR > 2015) AS __mdw_src"


def test_iter_sql_source_with_string_where_wraps_all_tables():
    src = sql_source(
        "P1105", tables=["dbo.lisa_2018", "dbo.lisa_2019"], where="AR > 2015"
    )
    handles = list(iter_sql_source(src, conn=_FakeConn(view_rows=[])))
    for h in handles:
        assert h.table.startswith("(SELECT * FROM ")
        assert "WHERE AR > 2015" in h.table
        assert h.table.endswith("AS __mdw_src")
        assert h.source_detail["where"] == "AR > 2015"


def test_iter_sql_source_with_dict_where_routes_per_alias():
    src = sql_source(
        "P1105",
        tables=["dbo.lisa_2018", "dbo.lisa_2019"],
        where={"lisa_2018": "AR > 2018"},  # only lisa_2018 gets filtered
    )
    handles = {h.source_name: h for h in iter_sql_source(src, conn=_FakeConn([]))}
    assert "WHERE AR > 2018" in handles["lisa_2018"].table
    assert handles["lisa_2018"].source_detail["where"] == "AR > 2018"
    # lisa_2019 unfiltered — bare quoted name, no derived-table wrapper
    assert "WHERE" not in handles["lisa_2019"].table
    assert "where" not in handles["lisa_2019"].source_detail


def test_iter_file_source_with_where_filters_rows(tmp_path: Path):
    """End-to-end: where clause actually narrows row count via DuckDB."""
    (tmp_path / "events.csv").write_text(
        "ar,event\n2014,a\n2015,b\n2016,c\n2017,d\n2018,e\n",
        encoding="utf-8",
    )
    src = file_source(str(tmp_path), include=["events.csv"], where="ar > 2015")
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {handle.table}")
            (n,) = cur.fetchone()
            assert n == 3  # 2016, 2017, 2018
        finally:
            cur.close()
        assert handle.source_detail["where"] == "ar > 2015"
