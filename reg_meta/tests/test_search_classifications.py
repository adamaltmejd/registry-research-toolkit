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


@pytest.fixture
def db_with_cls_group() -> sqlite3.Connection:
    """Slugged DB with a 2-member classification vintage group (sun2000 +
    sun2020, both named "Svensk utbildningsnomenklatur") so the fold path is
    exercised at the library level."""
    conn = build_slugged_db()  # ships sun2020 (id 1)
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (50, 'SUN2000', 'Svensk utbildningsnomenklatur', 'sun2000')"
    )
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (11, 'classification', NULL, 'sun', "
        "'Svensk utbildningsnomenklatur', 'token')"
    )
    sun2020_id = conn.execute(
        "SELECT id FROM classification WHERE slug = 'sun2020'"
    ).fetchone()[0]
    conn.executemany(
        "INSERT INTO concept_group_classification (classification_id, group_id, "
        "facet_value, facet_label) VALUES (?, 11, ?, ?)",
        [(50, "2000", "2000"), (sun2020_id, "2020", "2020")],
    )
    _rebuild_fts(conn)
    return conn


def test_classification_label_match_folds_and_subsumes_leaves(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # The query matches both classification names (FTS) AND the group label
    # (LIKE) → one group row, member leaves subsumed (no leaf + folded-member
    # duplication, the #350 review bug).
    out = search(
        db_with_cls_group, "Svensk", field="description", type="classification"
    )
    rows = out["results"]
    groups = [r for r in rows if r["type"] == "group"]
    leaves = [r for r in rows if r["type"] == "classification"]
    assert len(groups) == 1
    assert groups[0]["kind"] == "classification"
    assert groups[0]["group_key"] == "sun"
    member_fqids = {m["fqid"] for m in groups[0]["members"]}
    assert {"class/sun2000", "class/sun2020"} <= member_fqids
    assert not ({r["fqid"] for r in leaves} & member_fqids)


def test_classification_member_fold_without_label_match(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # short_name search ("SUN") matches both members' FTS but NOT the group
    # label "Svensk …" — ≥2 member hits still fold the family (symmetric with
    # variables).
    out = search(db_with_cls_group, "SUN", field="description", type="classification")
    groups = [r for r in out["results"] if r["type"] == "group"]
    assert any(r["group_key"] == "sun" for r in groups)


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


def test_cli_display_row_projects_classification() -> None:
    # A classification row carries short_name/classification_name/fqid, none in
    # the mixed-type fallback columns — the CLI projector fills the generic
    # columns so a `--type all` table doesn't render it blank (Codex P2).
    from reg_meta.cli import _search_display_row

    row = _search_display_row(
        {
            "type": "classification",
            "fqid": "class/sun2020",
            "short_name": "SUN2020",
            "classification_name": "Svensk utbildningsnomenklatur",
            "fts_rank": -1.0,
        }
    )
    assert row["register_name"] == "SUN2020"
    assert row["variable_name"] == "Svensk utbildningsnomenklatur"


def test_empty_description_query_folds_nothing(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # No searchable token → the FTS path no-ops AND label folding is gated off,
    # so an empty/punctuation query must NOT return every concept group via the
    # raw `%%` LIKE pattern (Codex P2).
    for q in ("", "   ", '"" -- ;'):
        assert search(db_with_cls_group, q, field="description")["results"] == []


def test_years_excludes_classifications(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # Classifications carry no validity window, so a --years filter excludes both
    # the leaves and the (label-matched) family — no unfilterable false positives
    # (Codex P2). Without --years the same query DOES return the family.
    assert search(
        db_with_cls_group, "Svensk", field="description", type="classification"
    )["results"]
    assert (
        search(
            db_with_cls_group,
            "Svensk",
            field="description",
            type="classification",
            years="2010",
        )["results"]
        == []
    )
