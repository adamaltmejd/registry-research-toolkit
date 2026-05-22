"""Tests for sources.py.

File-source tests run against a real in-process DuckDB. SQL-source
tests use a fake DB-API connection (the dialect-specific SQL we'd emit
doesn't execute against any local backend, and the iterator's logic is
about table selection and handle shape, not query execution).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
from _project_data_fixtures import make_project_data
from reg_monabundle.runtime.sources import (
    FileSource,
    SourceHandle,
    SqlSource,
    SqlTable,
    _build_pyodbc_connstr,
    _is_archived,
    _normalize_to_sql_tables,
    _resolve_sql_aliases,
    _semantic_to_duckdb_cast,
    _wrap_with_where,
    file_source,
    filter_files,
    iter_file_source,
    iter_source,
    iter_sql_source,
    list_files_in_source,
    sql_source,
    sql_table,
)
from reg_monabundle.runtime.spec import (
    ColumnTypeOverride,
    LoadedSpec,
    parse_project_data,
)

if TYPE_CHECKING:
    from pathlib import Path


def _spec_with(
    column_types: dict[str, dict[str, ColumnTypeOverride]],
) -> LoadedSpec:
    """Build a LoadedSpec exposing ``column_types`` keyed by SQL header.

    Translates the old MDWConfig-style ``{source: {display_name: override}}``
    shape into a project_data.json payload — each (source, display_name)
    becomes a Source/Column pair carrying the override fields as
    structural column attributes.
    """
    sources = [
        {
            "name": src,
            "columns": [
                {
                    "display_name": col,
                    "type": ov.type,
                    **({"id_subtype": ov.id_subtype} if ov.id_subtype else {}),
                    **(
                        {"numeric_subtype": ov.numeric_subtype}
                        if ov.numeric_subtype
                        else {}
                    ),
                    **({"date_format": ov.date_format} if ov.date_format else {}),
                }
                for col, ov in cols.items()
            ],
        }
        for src, cols in column_types.items()
    ]
    return parse_project_data(make_project_data(sources=sources))


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


def test_iter_file_source_reads_latin1_encoded_csv(tmp_path: Path):
    p = tmp_path / "swedish.csv"
    p.write_bytes("namn,ort\nÅke,Malmö\nÖrjan,Växjö\n".encode("latin-1"))
    src = file_source(str(tmp_path), include=["swedish.csv"], encoding="latin-1")
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT namn, ort FROM {handle.table} ORDER BY namn")
            rows = cur.fetchall()
            assert rows == [("Åke", "Malmö"), ("Örjan", "Växjö")]
        finally:
            cur.close()


def test_iter_file_source_aliases_cp1252_to_latin1(tmp_path: Path):
    """SCB-typical encoding name should round-trip via the alias map."""
    p = tmp_path / "cp.csv"
    p.write_bytes("x\nMalmö\n".encode("cp1252"))
    src = file_source(str(tmp_path), include=["cp.csv"], encoding="cp1252")
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT x FROM {handle.table}")
            assert cur.fetchall() == [("Malmö",)]
        finally:
            cur.close()


def test_iter_file_source_utf8_failure_hint(tmp_path: Path):
    """Non-UTF-8 bytes under the default encoding should surface a hint."""
    p = tmp_path / "bad.csv"
    p.write_bytes("x\nMalm\xf6\n".encode("latin-1"))
    src = file_source(str(tmp_path), include=["bad.csv"])  # default utf-8
    with pytest.raises(RuntimeError, match="encoding='latin-1'"):
        list(iter_file_source(src))


def test_iter_file_source_treats_space_as_null_in_numeric_column(tmp_path: Path):
    """SCB CSVs use ' ' as the missing-value sentinel in numeric columns.

    The auto-detector picks a numeric type from the clean head of the file,
    so rows containing ' ' beyond the sample window must coerce to NULL
    instead of failing the strict-mode BIGINT cast.
    """
    p = tmp_path / "sentinel.csv"
    lines = ["id,Hman\n"]
    # Push the sentinel row past DuckDB's default sample_size (20480) so the
    # column is committed to BIGINT before the parser sees the space.
    lines.extend(f"{i},0\n" for i in range(25_000))
    lines.append("25000, \n")
    p.write_text("".join(lines), encoding="utf-8")
    src = file_source(str(tmp_path), include=["sentinel.csv"])
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT Hman FROM {handle.table} WHERE id = 25000")
            assert cur.fetchone() == (None,)
        finally:
            cur.close()


def test_iter_file_source_no_config_uses_all_varchar(tmp_path: Path):
    """Discover mode (no config) reads every column as VARCHAR. No
    sniffer runs, so there is no rare-row crash path and no
    sample-size cost. Numeric/date detection moves to the extract-time
    opaque auto-promotion step."""
    p = tmp_path / "infer.csv"
    p.write_text("id,score\n1,10\n2,20\n", encoding="utf-8")
    src = file_source(str(tmp_path), include=["infer.csv"])
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
            schema = {r[0]: r[1] for r in cur.fetchall()}
            assert schema == {"id": "VARCHAR", "score": "VARCHAR"}
        finally:
            cur.close()


def test_iter_file_source_no_config_handles_rare_letter_past_window(
    tmp_path: Path,
):
    """The slutbetyg-style file (digits with one literal letter past the
    default sample window) must read cleanly at discover. Previously
    DuckDB's auto-detector with the default 20480-row sample inferred
    BIGINT and crashed during the SELECT; ``sample_size=-1`` fixed the
    crash at full-file sniffer cost; ``all_varchar=true`` sidesteps the
    inference entirely — no scan, no crash."""
    p = tmp_path / "rare_letter.csv"
    lines = ["id,NO_AMNEN\n"]
    lines.extend(f"{i},{i % 10}\n" for i in range(25_000))
    lines.append("25000,C\n")
    p.write_text("".join(lines), encoding="utf-8")
    src = file_source(str(tmp_path), include=["rare_letter.csv"])
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
            schema = {r[0]: r[1] for r in cur.fetchall()}
            assert schema == {"id": "VARCHAR", "NO_AMNEN": "VARCHAR"}
            cur.execute(f"SELECT NO_AMNEN FROM {handle.table} WHERE id = '25000'")
            assert cur.fetchone() == ("C",)
        finally:
            cur.close()


# -- config-driven cast path (issue #40) ---------------------------------


def test_semantic_to_duckdb_cast_only_casts_numeric():
    """id / categorical / opaque / date keep VARCHAR; numeric picks the
    BIGINT vs DOUBLE branch on numeric_subtype."""
    assert _semantic_to_duckdb_cast(ColumnTypeOverride(type="id")) is None
    assert (
        _semantic_to_duckdb_cast(ColumnTypeOverride(type="id", id_subtype="integer"))
        is None
    )
    assert _semantic_to_duckdb_cast(ColumnTypeOverride(type="categorical")) is None
    assert _semantic_to_duckdb_cast(ColumnTypeOverride(type="opaque")) is None
    assert (
        _semantic_to_duckdb_cast(ColumnTypeOverride(type="date", date_format="%Y%m%d"))
        is None
    )
    assert (
        _semantic_to_duckdb_cast(
            ColumnTypeOverride(type="numeric", numeric_subtype="integer")
        )
        == "BIGINT"
    )
    assert (
        _semantic_to_duckdb_cast(
            ColumnTypeOverride(type="numeric", numeric_subtype="double")
        )
        == "DOUBLE"
    )
    # Unspecified subtype defaults to DOUBLE (covers floats and ints up to 2^53).
    assert _semantic_to_duckdb_cast(ColumnTypeOverride(type="numeric")) == "DOUBLE"


def test_iter_file_source_with_config_skips_inference_for_rare_letter(
    tmp_path: Path,
):
    """The slutbetyg-style scenario: a column the user has classified
    as opaque must read cleanly even when DuckDB's auto-inference would
    have picked BIGINT. The cast path uses ``all_varchar=true``, so
    inference doesn't run at all and the rare letter round-trips as a
    string with no per-file ``sample_size=-1`` cost.
    """
    p = tmp_path / "rare_letter.csv"
    lines = ["id,NO_AMNEN\n"]
    lines.extend(f"{i},{i % 10}\n" for i in range(25_000))
    lines.append("25000,C\n")
    p.write_text("".join(lines), encoding="utf-8")
    cfg = _spec_with(
        {
            "rare_letter.csv": {
                "id": ColumnTypeOverride(type="id"),
                "NO_AMNEN": ColumnTypeOverride(type="opaque"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["rare_letter.csv"])
    for handle in iter_file_source(src, spec=cfg):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT NO_AMNEN FROM {handle.table} WHERE id = '25000'")
            assert cur.fetchone() == ("C",)
            cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
            schema = {r[0]: r[1] for r in cur.fetchall()}
            assert schema == {"id": "VARCHAR", "NO_AMNEN": "VARCHAR"}
        finally:
            cur.close()


def test_iter_file_source_with_config_casts_numeric_columns(tmp_path: Path):
    """Columns marked numeric get a BIGINT/DOUBLE cast so MIN/MAX/AVG
    behave numerically. SCB ' ' nulls survive the cast as NULL."""
    p = tmp_path / "nums.csv"
    p.write_text(
        "id,score,price\n1,10, \n2,20,3.5\n3, ,4.5\n4,40,5.5\n",
        encoding="utf-8",
    )
    cfg = _spec_with(
        {
            "nums.csv": {
                "id": ColumnTypeOverride(type="id"),
                "score": ColumnTypeOverride(type="numeric", numeric_subtype="integer"),
                "price": ColumnTypeOverride(type="numeric", numeric_subtype="double"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["nums.csv"])
    for handle in iter_file_source(src, spec=cfg):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
            schema = {r[0]: r[1] for r in cur.fetchall()}
            assert schema == {
                "id": "VARCHAR",
                "score": "BIGINT",
                "price": "DOUBLE",
            }
            cur.execute(
                f"SELECT MIN(score), MAX(score), AVG(price) FROM {handle.table}"
            )
            mn, mx, avg = cur.fetchone()
            assert (mn, mx) == (10, 40)
            assert abs(avg - (3.5 + 4.5 + 5.5) / 3) < 1e-9
        finally:
            cur.close()


def test_iter_file_source_with_config_raises_named_error_on_bad_numeric_cast(
    tmp_path: Path,
):
    """A column marked numeric that contains non-numeric strings raises
    at read time with the column name in the error — that is the
    auditable failure mode the cast path is meant to provide."""
    p = tmp_path / "bad_numeric.csv"
    p.write_text("id,n\n1,10\n2,foo\n3,30\n", encoding="utf-8")
    cfg = _spec_with(
        {
            "bad_numeric.csv": {
                "id": ColumnTypeOverride(type="id"),
                "n": ColumnTypeOverride(type="numeric", numeric_subtype="integer"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["bad_numeric.csv"])
    with pytest.raises(RuntimeError, match=r"\bn\b"):
        list(iter_file_source(src, spec=cfg))


def test_iter_file_source_with_config_passes_through_unknown_columns(
    tmp_path: Path,
):
    """A CSV column the config doesn't mention must still appear in the
    view (as VARCHAR). ``process_handle`` validates the missing override
    later with one error listing every offender; the read shouldn't
    drop the column or fail here."""
    p = tmp_path / "extra.csv"
    p.write_text("id,known,unknown\n1,a,x\n2,b,y\n", encoding="utf-8")
    cfg = _spec_with(
        {
            "extra.csv": {
                "id": ColumnTypeOverride(type="id"),
                "known": ColumnTypeOverride(type="categorical"),
                # `unknown` deliberately omitted from the config
            }
        }
    )
    src = file_source(str(tmp_path), include=["extra.csv"])
    for handle in iter_file_source(src, spec=cfg):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
            cols = [r[0] for r in cur.fetchall()]
            assert cols == ["id", "known", "unknown"]
            cur.execute(f"SELECT unknown FROM {handle.table} ORDER BY id")
            assert [r[0] for r in cur.fetchall()] == ["x", "y"]
        finally:
            cur.close()


def test_iter_file_source_spec_without_entry_for_file_uses_all_varchar(
    tmp_path: Path,
):
    """A config that has *some* sources but not this one reads the absent
    file the same way discover does — all-VARCHAR, no inference. The
    opaque auto-promotion only runs against opaque overrides that
    actually exist for the file."""
    p = tmp_path / "untouched.csv"
    p.write_text("id,n\n1,10\n", encoding="utf-8")
    cfg = _spec_with({"other.csv": {"id": ColumnTypeOverride(type="id")}})
    src = file_source(str(tmp_path), include=["untouched.csv"])
    for handle in iter_file_source(src, spec=cfg):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
            schema = {r[0]: r[1] for r in cur.fetchall()}
            assert schema == {"id": "VARCHAR", "n": "VARCHAR"}
        finally:
            cur.close()


# -- opaque auto-promotion at extract ------------------------------------


def test_opaque_with_all_ints_promotes_to_numeric_integer(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """An opaque column whose non-null values all cleanly TRY_CAST to
    BIGINT is auto-promoted to numeric/integer. The override is mutated
    in place so process_handle dispatches the numeric branch; a WARNING
    is logged so the MONA-side run log records the decision."""
    p = tmp_path / "promote_int.csv"
    p.write_text("id,maybe_num\n1,10\n2,20\n3,30\n", encoding="utf-8")
    cfg = _spec_with(
        {
            "promote_int.csv": {
                "id": ColumnTypeOverride(type="id"),
                "maybe_num": ColumnTypeOverride(type="opaque"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["promote_int.csv"])
    with caplog.at_level(logging.WARNING, logger="mdw.sources"):
        for handle in iter_file_source(src, spec=cfg):
            cur = handle.conn.cursor()
            try:
                cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
                schema = {r[0]: r[1] for r in cur.fetchall()}
                assert schema["maybe_num"] == "BIGINT"
            finally:
                cur.close()
    promoted = cfg.column_types_for_source("promote_int.csv")["maybe_num"]
    assert promoted.type == "numeric"
    assert promoted.numeric_subtype == "integer"
    assert any(
        "maybe_num" in r.message and "numeric/integer" in r.message
        for r in caplog.records
    )


def test_opaque_with_floats_promotes_to_numeric_double(tmp_path: Path):
    """All-DOUBLE-clean (but not all-BIGINT-clean) opaque column promotes
    to numeric/double."""
    p = tmp_path / "promote_float.csv"
    p.write_text("id,p\n1,1.5\n2,2.5\n3,3.0\n", encoding="utf-8")
    cfg = _spec_with(
        {
            "promote_float.csv": {
                "id": ColumnTypeOverride(type="id"),
                "p": ColumnTypeOverride(type="opaque"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["promote_float.csv"])
    for handle in iter_file_source(src, spec=cfg):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
            schema = {r[0]: r[1] for r in cur.fetchall()}
            assert schema["p"] == "DOUBLE"
        finally:
            cur.close()
    promoted = cfg.column_types_for_source("promote_float.csv")["p"]
    assert promoted.type == "numeric"
    assert promoted.numeric_subtype == "double"


def test_opaque_with_iso_dates_promotes_to_date(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """ISO-style dates TRY_CAST cleanly to DATE → promotion. The view
    keeps the column as VARCHAR (date stats use lexicographic MIN/MAX
    plus Python-side parsing); the override flip is what matters."""
    p = tmp_path / "promote_date.csv"
    p.write_text(
        "id,d\n1,2023-01-15\n2,2023-06-30\n3,2024-02-01\n",
        encoding="utf-8",
    )
    cfg = _spec_with(
        {
            "promote_date.csv": {
                "id": ColumnTypeOverride(type="id"),
                "d": ColumnTypeOverride(type="opaque"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["promote_date.csv"])
    with caplog.at_level(logging.WARNING, logger="mdw.sources"):
        for _ in iter_file_source(src, spec=cfg):
            pass
    assert cfg.column_types_for_source("promote_date.csv")["d"].type == "date"
    assert any("'d'" in r.message and "date" in r.message for r in caplog.records)


def test_opaque_with_mixed_values_stays_opaque(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """An opaque column with a non-castable value stays opaque. No
    warning, no schema change."""
    p = tmp_path / "stay_opaque.csv"
    p.write_text("id,mixed\n1,10\n2,foo\n3,30\n", encoding="utf-8")
    cfg = _spec_with(
        {
            "stay_opaque.csv": {
                "id": ColumnTypeOverride(type="id"),
                "mixed": ColumnTypeOverride(type="opaque"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["stay_opaque.csv"])
    with caplog.at_level(logging.WARNING, logger="mdw.sources"):
        for handle in iter_file_source(src, spec=cfg):
            cur = handle.conn.cursor()
            try:
                cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
                schema = {r[0]: r[1] for r in cur.fetchall()}
                assert schema["mixed"] == "VARCHAR"
            finally:
                cur.close()
    assert cfg.column_types_for_source("stay_opaque.csv")["mixed"].type == "opaque"
    assert not any("promoted" in r.message for r in caplog.records)


def test_opaque_yyyymmdd_strings_promote_to_integer_not_date(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    """SCB YYYYMMDD-as-string columns are a known semantic surprise:
    they satisfy ``TRY_CAST(... AS BIGINT)`` first, so the cascade in
    ``_probe_and_promote_opaque`` promotes them to ``numeric/integer``
    rather than ``date``. The DESIGN.md caveat depends on this behaviour
    — locked in here so a future cascade reorder can't silently flip it
    without a test failure (and the WARNING gives the user the signal
    to flip the override to ``date`` in the next config iteration)."""
    p = tmp_path / "yyyymmdd.csv"
    p.write_text(
        "id,d\n1,20230115\n2,20230630\n3,20240201\n",
        encoding="utf-8",
    )
    cfg = _spec_with(
        {
            "yyyymmdd.csv": {
                "id": ColumnTypeOverride(type="id"),
                "d": ColumnTypeOverride(type="opaque"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["yyyymmdd.csv"])
    with caplog.at_level(logging.WARNING, logger="mdw.sources"):
        for handle in iter_file_source(src, spec=cfg):
            cur = handle.conn.cursor()
            try:
                cur.execute(f"DESCRIBE SELECT * FROM {handle.table} LIMIT 0")
                schema = {r[0]: r[1] for r in cur.fetchall()}
                assert schema["d"] == "BIGINT"
            finally:
                cur.close()
    promoted = cfg.column_types_for_source("yyyymmdd.csv")["d"]
    assert promoted.type == "numeric"
    assert promoted.numeric_subtype == "integer"
    assert any(
        "'d'" in r.message and "numeric/integer" in r.message for r in caplog.records
    )


def test_opaque_all_null_stays_opaque(tmp_path: Path):
    """An opaque column with no non-null values can't be probed
    (nothing to TRY_CAST). Stays opaque."""
    p = tmp_path / "all_null.csv"
    p.write_text("id,e\n1,\n2,\n3,\n", encoding="utf-8")
    cfg = _spec_with(
        {
            "all_null.csv": {
                "id": ColumnTypeOverride(type="id"),
                "e": ColumnTypeOverride(type="opaque"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["all_null.csv"])
    for _ in iter_file_source(src, spec=cfg):
        pass
    assert cfg.column_types_for_source("all_null.csv")["e"].type == "opaque"


# -- SQL helpers ---------------------------------------------------------


def test_normalize_strings_become_bare_sql_tables():
    out = _normalize_to_sql_tables(["dbo.persons", "dbo.events"])
    assert out == [
        SqlTable(qualified="dbo.persons"),
        SqlTable(qualified="dbo.events"),
    ]


def test_normalize_passes_through_sql_tables():
    t = sql_table("dbo.lisa_2018", where="AR > 2018")
    assert _normalize_to_sql_tables([t]) == [t]


def test_normalize_mapping_assigns_alias_from_key():
    out = _normalize_to_sql_tables({"p_dbo": "dbo.persons", "p_p1": "P1105.persons"})
    assert out == [
        SqlTable(qualified="dbo.persons", alias="p_dbo"),
        SqlTable(qualified="P1105.persons", alias="p_p1"),
    ]


def test_normalize_mapping_with_sql_table_keeps_existing_alias():
    t = sql_table("dbo.persons", alias="explicit")
    out = _normalize_to_sql_tables({"key_overridden_by_explicit": t})
    assert out == [t]


def test_normalize_mapping_with_sql_table_takes_key_when_no_alias():
    t = sql_table("dbo.persons", where="active = 1")
    out = _normalize_to_sql_tables({"alias_from_key": t})
    assert out[0].qualified == "dbo.persons"
    assert out[0].where == "active = 1"
    assert out[0].alias == "alias_from_key"


def test_normalize_rejects_unknown_entry_types():
    with pytest.raises(TypeError, match="must be str or SqlTable"):
        _normalize_to_sql_tables([42])  # type: ignore[list-item]


def test_resolve_sql_aliases_strips_schema_by_default():
    tables = [SqlTable(qualified="dbo.persons"), SqlTable(qualified="dbo.events")]
    out = _resolve_sql_aliases(tables)
    assert set(out) == {"persons", "events"}
    assert out["persons"].qualified == "dbo.persons"


def test_resolve_sql_aliases_uses_explicit_alias_when_set():
    out = _resolve_sql_aliases([sql_table("dbo.persons", alias="people")])
    assert "people" in out
    assert "persons" not in out


def test_resolve_sql_aliases_conflict_raises():
    with pytest.raises(ValueError, match="Ambiguous table aliases"):
        _resolve_sql_aliases(
            [SqlTable(qualified="dbo.persons"), SqlTable(qualified="P1105.persons")]
        )


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


def test_iter_sql_source_permissive_lists_everything_unfiltered():
    """Discover-mode permissive: no tables/pattern/all -> list everything."""
    discovered = [
        ("dbo", "lisa_2018"),
        ("dbo", "rams_2018"),
        ("dbo", "x_archived"),
    ]
    raw = SqlSource(dsn="P1105")
    handles = list(
        iter_sql_source(raw, conn=_FakeConn(view_rows=discovered), permissive=True)
    )
    # exclude_archived defaults to True
    assert sorted(h.source_name for h in handles) == ["lisa_2018", "rams_2018"]


def test_iter_sql_source_permissive_disambiguates_cross_schema_collisions():
    """Discover must NOT raise on `dbo.persons` + `dim.persons` -- the
    user didn't curate the table list, so we silently key the colliders
    by their qualified names instead of erroring out."""
    discovered = [
        ("dbo", "persons"),
        ("dim", "persons"),
        ("dbo", "events"),
    ]
    raw = SqlSource(dsn="P1105")
    handles = list(
        iter_sql_source(raw, conn=_FakeConn(view_rows=discovered), permissive=True)
    )
    names = sorted(h.source_name for h in handles)
    assert names == ["dbo.persons", "dim.persons", "events"]


def test_iter_sql_source_strict_still_raises_on_collision():
    """Extract mode keeps the strict behavior: explicit `tables=` with
    same-named entries from different schemas must raise."""
    src = sql_source("P1105", tables=["dbo.persons", "dim.persons"])
    with pytest.raises(ValueError, match="Ambiguous table aliases"):
        list(iter_sql_source(src, conn=_FakeConn(view_rows=[])))


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


# -- WHERE clauses (table-level via sql_table; file-source-level) --------


def test_sql_table_validates_qualified():
    with pytest.raises(ValueError, match="non-empty string"):
        sql_table("")


def test_sql_table_returns_frozen_dataclass():
    t = sql_table("dbo.persons", where="active = 1", alias="people")
    assert t.qualified == "dbo.persons"
    assert t.where == "active = 1"
    assert t.alias == "people"


def test_wrap_with_where_no_clause_passthrough():
    assert _wrap_with_where('"v"', None) == '"v"'
    assert _wrap_with_where('"v"', "") == '"v"'


def test_wrap_with_where_produces_aliased_derived_table():
    out = _wrap_with_where("[dbo].[t]", "AR > 2015")
    assert out == "(SELECT * FROM [dbo].[t] WHERE AR > 2015) AS __mdw_src"


def test_iter_sql_source_attaches_per_table_where():
    src = sql_source(
        "P1105",
        tables=(
            sql_table("dbo.lisa_2018", where="AR > 2018"),
            sql_table("dbo.par", where="INDATUM > '2015-01-01'"),
            "dbo.fodelse",  # no filter
        ),
    )
    handles = {h.source_name: h for h in iter_sql_source(src, conn=_FakeConn([]))}
    assert "WHERE AR > 2018" in handles["lisa_2018"].table
    assert handles["lisa_2018"].source_detail["where"] == "AR > 2018"
    assert "INDATUM > '2015-01-01'" in handles["par"].table
    assert handles["par"].source_detail["where"] == "INDATUM > '2015-01-01'"
    # Unfiltered: bare quoted name, no derived-table wrapper
    assert "WHERE" not in handles["fodelse"].table
    assert "where" not in handles["fodelse"].source_detail


def test_iter_sql_source_no_filters_means_no_wrapping():
    src = sql_source("P1105", tables=["dbo.persons"])
    handles = list(iter_sql_source(src, conn=_FakeConn([])))
    assert "WHERE" not in handles[0].table
    assert handles[0].table == "[dbo].[persons]"


def test_iter_sql_source_mapping_alias_with_sql_table():
    src = sql_source(
        "P1105",
        tables={"events": sql_table("dbo.events_v2", where="ts > '2015-01-01'")},
    )
    handles = list(iter_sql_source(src, conn=_FakeConn([])))
    assert handles[0].source_name == "events"
    assert "ts > '2015-01-01'" in handles[0].table
    assert handles[0].source_detail["table"] == "dbo.events_v2"


def test_iter_file_source_with_where_filters_rows(tmp_path: Path):
    """End-to-end: where clause actually narrows row count via DuckDB.

    File reads are all-VARCHAR (no inference), so a WHERE clause that
    needs numeric semantics has to cast (or compare against string
    literals). Year-like values sort identically under lex and numeric
    comparison, so `ar > '2015'` is the natural form for SCB years.
    """
    (tmp_path / "events.csv").write_text(
        "ar,event\n2014,a\n2015,b\n2016,c\n2017,d\n2018,e\n",
        encoding="utf-8",
    )
    src = file_source(str(tmp_path), include=["events.csv"], where="ar > '2015'")
    for handle in iter_file_source(src):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {handle.table}")
            (n,) = cur.fetchone()
            assert n == 3  # 2016, 2017, 2018
        finally:
            cur.close()
        assert handle.source_detail["where"] == "ar > '2015'"


def test_iter_file_source_with_where_numeric_literal_works_in_extract_mode(
    tmp_path: Path,
):
    """In extract mode the cast view exposes the column with its
    configured numeric type, so a WHERE that compares against a bare
    numeric literal (``ar > 2015``) parses cleanly — DESIGN.md's
    "the original form works as-is" claim, locked in."""
    (tmp_path / "events.csv").write_text(
        "ar,event\n2014,a\n2015,b\n2016,c\n2017,d\n2018,e\n",
        encoding="utf-8",
    )
    cfg = _spec_with(
        {
            "events.csv": {
                "ar": ColumnTypeOverride(type="numeric", numeric_subtype="integer"),
                "event": ColumnTypeOverride(type="categorical"),
            }
        }
    )
    src = file_source(str(tmp_path), include=["events.csv"], where="ar > 2015")
    for handle in iter_file_source(src, spec=cfg):
        cur = handle.conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {handle.table}")
            (n,) = cur.fetchone()
            assert n == 3  # 2016, 2017, 2018
        finally:
            cur.close()
