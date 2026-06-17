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

from _slugged_db import add_value_set, build_slugged_db  # noqa: E402

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


def _link_code_to_classification(
    conn: sqlite3.Connection, slug: str, code_id: int, is_valid: int = 1
) -> None:
    cls_id = conn.execute(
        "SELECT id FROM classification WHERE slug = ?", (slug,)
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO classification_code (classification_id, code_id, level, is_valid) "
        "VALUES (?, ?, NULL, ?)",
        (cls_id, code_id, is_valid),
    )


@pytest.fixture
def db_with_class_codes() -> sqlite3.Connection:
    """Slugged DB whose sun2020 classification CONTAINS two code-shaped codes
    ('C12', 'C120') so a code-shaped query surfaces it via code-containment even
    though 'C12' matches no classification NAME.

    A SECOND classification (icd10, name 'C12-titled') deliberately carries 'C12'
    in its NAME so it's a name-FTS hit too — exercising the dedup (it must not be
    double-emitted) and the name-before-code ranking."""
    conn = build_slugged_db()  # ships sun2020 (no codes)
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (60, 'ICD10', 'C12 malignant neoplasm', 'icd10')"
    )
    add_value_set(conn, value_set_id=1, codes=[("C12", "Tongue base"), ("C120", "Sub")])
    code_ids = {
        row["code"]: row["code_id"]
        for row in conn.execute("SELECT code_id, code FROM value_code").fetchall()
    }
    _link_code_to_classification(conn, "sun2020", code_ids["C12"])
    _link_code_to_classification(conn, "sun2020", code_ids["C120"])
    _rebuild_fts(conn)
    return conn


@pytest.fixture
def db_with_class_code_group() -> sqlite3.Connection:
    """`db_with_cls_group` (sun2000 + sun2020 siblings in classification group
    'sun') PLUS a code-shaped code 'V10' linked to BOTH siblings via
    `classification_code`. 'V10' matches NO classification NAME, so each sibling
    surfaces ONLY via code-containment — exercising the fold of ≥2 code-
    containment hits into one `type:"group"` row (the interaction point between
    `_search_classifications_by_code` and `_fold_concept_groups`, keyed on
    `_classification_id`)."""
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
    add_value_set(conn, value_set_id=1, codes=[("V10", "Some code")])
    v10_id = conn.execute(
        "SELECT code_id FROM value_code WHERE code = 'V10'"
    ).fetchone()[0]
    _link_code_to_classification(conn, "sun2000", v10_id)
    _link_code_to_classification(conn, "sun2020", v10_id)
    _rebuild_fts(conn)
    return conn


@pytest.fixture
def db_with_exact_and_prefix_classifications() -> sqlite3.Connection:
    """Two DISTINCT classifications splitting an exact-vs-prefix code match:
    icd10 owns the EXACT query code 'C12'; sun2020 owns only a prefix-extension
    'C120'. Neither carries 'C12' in its NAME, so both surface ONLY via code-
    containment — isolating the `has_exact DESC` ordering ACROSS classifications
    (the shipped `db_with_class_codes` puts C12 + C120 on the SAME classification,
    so cross-classification exact-precedence is never exercised there)."""
    conn = build_slugged_db()  # ships sun2020 (no codes), name has no 'C12'
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (60, 'ICD10', 'Internationell sjukdomsklassifikation', 'icd10')"
    )
    add_value_set(conn, value_set_id=1, codes=[("C12", "Tongue base")])
    add_value_set(conn, value_set_id=2, codes=[("C120", "Sub")])
    code_ids = {
        row["code"]: row["code_id"]
        for row in conn.execute("SELECT code_id, code FROM value_code").fetchall()
    }
    _link_code_to_classification(conn, "icd10", code_ids["C12"])
    _link_code_to_classification(conn, "sun2020", code_ids["C120"])
    _rebuild_fts(conn)
    return conn


@pytest.fixture
def db_with_like_metachar_codes() -> sqlite3.Connection:
    """Two classifications splitting a LIKE-metacharacter query: classification A
    (slug 'underscore-owner') owns a code with a LITERAL underscore ('12_5');
    classification B (slug 'plain-owner') owns plain codes '120' and '125'. Neither
    carries the query in its NAME, so both can only surface via code-containment.
    (Slugs avoid `_` — the FQID grammar forbids it; the literal `_` lives in the
    CODE, which is the surface under test.)

    A query of '12_' must match A literally (its '12_5' code is a literal '12_…'
    prefix) and must NOT match B: unescaped, the `_` in `LIKE '12_%'` wildcards any
    single char and would wrongly surface B's '120'/'125'."""
    conn = build_slugged_db()  # ships sun2020 (no codes), name has no '12'
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (60, 'CLSUNDERSCORE', 'Underscore owner', 'underscore-owner')"
    )
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (61, 'CLSPLAIN', 'Plain owner', 'plain-owner')"
    )
    add_value_set(conn, value_set_id=1, codes=[("12_5", "Literal underscore")])
    add_value_set(conn, value_set_id=2, codes=[("120", "Plain a"), ("125", "Plain b")])
    code_ids = {
        row["code"]: row["code_id"]
        for row in conn.execute("SELECT code_id, code FROM value_code").fetchall()
    }
    _link_code_to_classification(conn, "underscore-owner", code_ids["12_5"])
    _link_code_to_classification(conn, "plain-owner", code_ids["120"])
    _link_code_to_classification(conn, "plain-owner", code_ids["125"])
    _rebuild_fts(conn)
    return conn


def test_like_metacharacter_query_matches_literally(
    db_with_like_metachar_codes: sqlite3.Connection,
) -> None:
    # '12_' is code-shaped (digit + len>=3), so the code-containment arm runs. Its
    # LIKE prefix must be treated LITERALLY: cls_underscore owns '12_5' (a literal
    # '12_…' prefix) and surfaces; cls_plain owns '120'/'125', which an UNESCAPED
    # `_` wildcard would wrongly match — it must NOT surface. Fails before the fix
    # (cls_plain leaks in via the wildcard); passes after (escaped + ESCAPE clause).
    from reg_meta.queries import _is_code_shaped

    assert _is_code_shaped("12_")
    out = search(
        db_with_like_metachar_codes, "12_", field="description", type="classification"
    )
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert "class/underscore-owner" in fqids
    assert "class/plain-owner" not in fqids


def test_code_shaped_query_surfaces_owning_classification(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # 'C12' matches no classification NAME under sun2020, but sun2020 CONTAINS the
    # code → it surfaces via code-containment with a navigable fqid.
    out = search(db_with_class_codes, "C12", field="description", type="classification")
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert "class/sun2020" in fqids


def test_code_containment_dedups_against_name_hit(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # icd10's NAME contains 'C12' (a name-FTS hit) AND it would be a code-
    # containment candidate if it owned the code — but it owns no code here, so the
    # real dedup target is the general invariant: each classification appears once.
    out = search(db_with_class_codes, "C12", field="description", type="classification")
    leaves = [r for r in out["results"] if r["type"] == "classification"]
    fqids = [r["fqid"] for r in leaves]
    assert len(fqids) == len(set(fqids)), f"duplicate classification rows: {fqids}"
    # icd10 is the name hit; sun2020 the code-containment hit.
    assert "class/icd10" in fqids
    assert "class/sun2020" in fqids


def test_name_fts_hits_precede_code_containment_hits(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # icd10 matches by NAME (negative bm25 rank); sun2020 only by code-containment
    # (positive base rank) → the name hit sorts first.
    out = search(db_with_class_codes, "C12", field="description", type="classification")
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert fqids.index("class/icd10") < fqids.index("class/sun2020")


def test_code_containment_excluded_under_register_scope(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # Classifications are catalog-scoped: a --register scope means "registers
    # only", so neither the name arm nor the code-containment arm contributes.
    out = search(
        db_with_class_codes,
        "C12",
        field="description",
        type="classification",
        register="lisa",
    )
    assert out["results"] == []


def test_code_containment_in_type_all(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # type="all" also surfaces the code-containing classification.
    out = search(db_with_class_codes, "C12", field="description", type="all")
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert "class/sun2020" in fqids


def test_non_code_shaped_query_has_no_code_containment(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # A plain word ('Tongue', the C12 label) is NOT code-shaped (no digit), so the
    # code-containment arm never runs — sun2020 isn't surfaced by its code's label.
    out = search(
        db_with_class_codes, "Tongue", field="description", type="classification"
    )
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert "class/sun2020" not in fqids


def test_two_char_code_query_has_no_code_containment(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # A 2-char code ('C1') fails the len>=3 code-shape gate, so no code-containment
    # rows — guards the gate's length floor.
    out = search(db_with_class_codes, "C1", field="description", type="classification")
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert "class/sun2020" not in fqids


def test_code_containment_hits_fold_into_concept_group(
    db_with_class_code_group: sqlite3.Connection,
) -> None:
    # 'V10' matches no classification NAME but is owned by BOTH sun2000 and
    # sun2020 — siblings of the 'sun' classification group. Two code-containment
    # leaf hits (keyed on `_classification_id`) must FOLD into one `type:"group"`
    # row, not stand as two leaves.
    out = search(
        db_with_class_code_group, "V10", field="description", type="classification"
    )
    rows = out["results"]
    groups = [r for r in rows if r["type"] == "group"]
    leaves = [r for r in rows if r["type"] == "classification"]
    assert len(groups) == 1
    assert groups[0]["kind"] == "classification"
    assert groups[0]["group_key"] == "sun"
    member_fqids = {m["fqid"] for m in groups[0]["members"]}
    assert {"class/sun2000", "class/sun2020"} <= member_fqids
    # No leaf row may duplicate a folded member's fqid.
    assert not ({r["fqid"] for r in leaves} & member_fqids)


def test_exact_code_classification_precedes_prefix_across_classifications(
    db_with_exact_and_prefix_classifications: sqlite3.Connection,
) -> None:
    # icd10 owns the EXACT 'C12'; sun2020 owns only the prefix-extension 'C120'.
    # `has_exact DESC` must rank the exact-containing classification first ACROSS
    # the two distinct classifications.
    out = search(
        db_with_exact_and_prefix_classifications,
        "C12",
        field="description",
        type="classification",
    )
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert "class/icd10" in fqids
    assert "class/sun2020" in fqids
    assert fqids.index("class/icd10") < fqids.index("class/sun2020")


@pytest.fixture
def db_with_exact_sorts_after_prefix() -> sqlite3.Connection:
    """Like `db_with_exact_and_prefix_classifications` but with the short_name
    sort order INVERTED relative to ownership: the EXACT-code owner (sun2020,
    short_name 'SUN2020') sorts AFTER the prefix-only owner (icd10, short_name
    'ICD10') under `ORDER BY ..., c.short_name` ('ICD10' < 'SUN2020').

    This is what isolates the case-sensitivity bug: with the exact owner sorting
    LATER, a broken `has_exact` (both 0) lets the prefix owner rank first — wrong.
    Only a correctly-set `has_exact` on the exact owner pulls it back to the top.
    (In the original fixture the exact owner 'ICD10' already sorts first by
    short_name, so a broken `has_exact` happens to produce the right order and
    the bug stays invisible.)"""
    conn = build_slugged_db()  # ships sun2020 (no codes), name has no 'C12'
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (60, 'ICD10', 'Internationell sjukdomsklassifikation', 'icd10')"
    )
    add_value_set(conn, value_set_id=1, codes=[("C12", "Tongue base")])
    add_value_set(conn, value_set_id=2, codes=[("C120", "Sub")])
    code_ids = {
        row["code"]: row["code_id"]
        for row in conn.execute("SELECT code_id, code FROM value_code").fetchall()
    }
    _link_code_to_classification(conn, "sun2020", code_ids["C12"])  # exact owner
    _link_code_to_classification(conn, "icd10", code_ids["C120"])  # prefix owner
    _rebuild_fts(conn)
    return conn


def test_lowercase_code_query_ranks_exact_first(
    db_with_exact_sorts_after_prefix: sqlite3.Connection,
) -> None:
    # Lowercase "c12" admits the stored uppercase "C12"/"C120" via the
    # case-insensitive LIKE, so BOTH classifications surface. The exact owner is
    # sun2020 (short_name 'SUN2020'), the prefix-only owner icd10 (short_name
    # 'ICD10'); 'ICD10' sorts EARLIER, so without a case-insensitive `has_exact`
    # the exact owner scores has_exact=0 and the prefix owner wrongly ranks first.
    # COLLATE NOCASE on the exact test pulls sun2020 (the true exact hit) back to
    # the top. Fails before the fix; passes after.
    out = search(
        db_with_exact_sorts_after_prefix,
        "c12",
        field="description",
        type="classification",
    )
    fqids = [r["fqid"] for r in out["results"] if r["type"] == "classification"]
    assert "class/icd10" in fqids
    assert "class/sun2020" in fqids
    assert fqids.index("class/sun2020") < fqids.index("class/icd10")


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
