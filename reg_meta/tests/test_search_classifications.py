"""Classification FTS search + FQID enrichment on the FTS leaf rows (#350).

`classification_fts` ships in the DB but the query layer never searched it (see
reg_meta/DESIGN.md → FTS5 configuration); `search(..., type="classification")` now
does. The same change stamps a navigable `fqid` onto every register/variable/
classification leaf row so the discovery surface (`/api/search`) can link results.

FTS5 indexes here are EXTERNAL-CONTENT (content='register' etc.) — base-table
INSERTs don't populate them, so each fixture rebuilds the three indexes from their
content tables before searching.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta.queries import _fts_match_query, search

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "reg_meta_build" / "tests")
)

from _slugged_db import build_slugged_db  # noqa: E402

if TYPE_CHECKING:
    import sqlite3


def _rebuild_fts(conn: sqlite3.Connection) -> None:
    """Repopulate the external-content FTS5 indexes from their content tables."""
    for index in ("register_fts", "variable_fts", "classification_fts"):
        conn.execute(f"INSERT INTO {index}({index}) VALUES('rebuild')")


@pytest.fixture
def db() -> sqlite3.Connection:
    conn = build_slugged_db()
    _rebuild_fts(conn)
    return conn


def _types(results: list[dict]) -> set[str]:
    return {r["type"] for r in results}


def test_classification_fts_match_carries_fqid(db: sqlite3.Connection) -> None:
    # The default classification is SUN2020 "Svensk utbildningsnomenklatur".
    out = search(db, "Svensk", field="description", type="classification")
    rows = out["results"]
    assert _types(rows) == {"classification"}
    row = rows[0]
    assert row["fqid"] == "class/sun2020"
    assert row["short_name"] == "SUN2020"
    assert row["classification_name"] == "Svensk utbildningsnomenklatur"


def test_classification_matches_short_name(db: sqlite3.Connection) -> None:
    out = search(db, "sun2020", field="description", type="classification")
    assert [r["fqid"] for r in out["results"]] == ["class/sun2020"]


def test_register_fts_row_carries_fqid(db: sqlite3.Connection) -> None:
    out = search(db, "LISA", field="description", type="register")
    rows = out["results"]
    assert _types(rows) == {"register"}
    assert rows[0]["fqid"] == "scb/lisa"


def test_variable_fts_row_carries_binding_fqid(db: sqlite3.Connection) -> None:
    out = search(db, "Kön", field="description", type="variable")
    rows = out["results"]
    assert _types(rows) == {"variable"}
    assert rows[0]["fqid"] == "scb/lisa/kon"


def test_type_all_spans_classification(db: sqlite3.Connection) -> None:
    # One query that hits a register/variable AND a classification token.
    reg = search(db, "Svensk", field="description", type="all")
    assert "classification" in _types(reg["results"])


def test_register_type_excludes_classification(db: sqlite3.Connection) -> None:
    out = search(db, "Svensk", field="description", type="register")
    assert "classification" not in _types(out["results"])


def test_variable_type_excludes_classification(db: sqlite3.Connection) -> None:
    out = search(db, "Svensk", field="description", type="variable")
    assert "classification" not in _types(out["results"])


def test_register_scope_excludes_classification(db: sqlite3.Connection) -> None:
    # Classifications are catalog-scoped: a --register scope means "registers
    # only", so the classification index is not searched.
    out = search(
        db, "Svensk", field="description", type="classification", register="lisa"
    )
    assert out["results"] == []


def test_invalid_type_raises(db: sqlite3.Connection) -> None:
    with pytest.raises(RegMetaError) as exc:
        search(db, "x", type="nonsense")
    assert "Invalid search type" in exc.value.message


def test_fts_match_query_quotes_and_prefixes() -> None:
    assert _fts_match_query("inkomst") == '"inkomst"*'
    assert _fts_match_query("lon ink") == '"lon"* "ink"*'


def test_fts_match_query_escapes_quotes() -> None:
    # FTS5 operators inside a token are neutralized by quoting; embedded double
    # quotes are doubled.
    assert _fts_match_query('foo"bar') == '"foo""bar"*'
    assert _fts_match_query("kon*") == '"kon*"*'


def test_fts_match_query_drops_punctuation_only() -> None:
    assert _fts_match_query("") is None
    assert _fts_match_query("   ") is None
    assert _fts_match_query('"" -- ;') is None


def test_fts_special_chars_do_not_raise(db: sqlite3.Connection) -> None:
    # The whole point of the safe builder: stray FTS syntax must not raise.
    for q in ['foo"bar', "AND OR", "kon*", "(a b)", "ssyk:1", "-kon"]:
        search(db, q, field="description")
