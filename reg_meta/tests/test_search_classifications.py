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

from _slugged_db import (  # noqa: E402
    add_binding,
    add_value_set,
    add_variable,
    build_slugged_db,
)


def _seed_classification(
    conn: sqlite3.Connection, *, slug: str, short_name: str, name: str
) -> None:
    conn.execute(
        "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
        (short_name, name, slug),
    )


def _seed_classification_edge(
    conn: sqlite3.Connection,
    *,
    predecessor: str,
    successor: str,
    effective_year: int | None,
) -> None:
    conn.execute(
        "INSERT INTO classification_replaced_by "
        "(predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, 'derived:vintage_chain')",
        (predecessor, successor, effective_year),
    )


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
    # The group's single 'vintage' axis declaration lives in concept_group_axis
    # (#819, replacing the dropped facet_axis column).
    conn.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (11, 'vintage', 0, 'vintage')"
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
    # Single 'vintage' axis declaration in concept_group_axis (#819).
    conn.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (11, 'vintage', 0, 'vintage')"
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
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
    assert "class/underscore-owner" in fqids
    assert "class/plain-owner" not in fqids


def test_code_shaped_query_surfaces_owning_classification(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # 'C12' matches no classification NAME under sun2020, but sun2020 CONTAINS the
    # code → it surfaces via code-containment with a navigable fqid.
    out = search(db_with_class_codes, "C12", field="description", type="classification")
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
    assert "class/sun2020" in fqids


def test_code_containment_dedups_against_name_hit(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # icd10's NAME contains 'C12' (a name-FTS hit) AND it would be a code-
    # containment candidate if it owned the code — but it owns no code here, so the
    # real dedup target is the general invariant: each classification appears once.
    out = search(db_with_class_codes, "C12", field="description", type="classification")
    leaves = [r for r in out.results if r.type == "classification"]
    fqids = [str(r.fqid) for r in leaves]
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
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
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
    assert out.results == ()


def test_code_containment_in_type_all(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # type="all" also surfaces the code-containing classification.
    out = search(db_with_class_codes, "C12", field="description", type="all")
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
    assert "class/sun2020" in fqids


def test_non_code_shaped_query_has_no_code_containment(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # A plain word ('Tongue', the C12 label) is NOT code-shaped (no digit), so the
    # code-containment arm never runs — sun2020 isn't surfaced by its code's label.
    out = search(
        db_with_class_codes, "Tongue", field="description", type="classification"
    )
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
    assert "class/sun2020" not in fqids


def test_two_char_code_query_has_no_code_containment(
    db_with_class_codes: sqlite3.Connection,
) -> None:
    # A 2-char code ('C1') fails the len>=3 code-shape gate, so no code-containment
    # rows — guards the gate's length floor.
    out = search(db_with_class_codes, "C1", field="description", type="classification")
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
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
    rows = out.results
    groups = [r for r in rows if r.type == "group"]
    leaves = [r for r in rows if r.type == "classification"]
    assert len(groups) == 1
    assert groups[0].kind == "classification"
    assert groups[0].group_key == "sun"
    member_fqids = {str(m.fqid) for m in groups[0].members}
    assert {"class/sun2000", "class/sun2020"} <= member_fqids
    # No leaf row may duplicate a folded member's fqid.
    assert not ({str(r.fqid) for r in leaves} & member_fqids)


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
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
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
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
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
    rows = out.results
    groups = [r for r in rows if r.type == "group"]
    leaves = [r for r in rows if r.type == "classification"]
    assert len(groups) == 1
    assert groups[0].kind == "classification"
    assert groups[0].group_key == "sun"
    member_fqids = {str(m.fqid) for m in groups[0].members}
    assert {"class/sun2000", "class/sun2020"} <= member_fqids
    assert not ({str(r.fqid) for r in leaves} & member_fqids)


def test_classification_member_fold_without_label_match(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # short_name search ("SUN") matches both members' FTS but NOT the group
    # label "Svensk …" — ≥2 member hits still fold the family (symmetric with
    # variables).
    out = search(db_with_cls_group, "SUN", field="description", type="classification")
    groups = [r for r in out.results if r.type == "group"]
    assert any(r.group_key == "sun" for r in groups)


def _types(results: tuple) -> set[str]:
    return {r.type for r in results}


def test_classification_fts_match_carries_fqid(db: sqlite3.Connection) -> None:
    # The default classification is SUN2020 "Svensk utbildningsnomenklatur".
    out = search(db, "Svensk", field="description", type="classification")
    rows = out.results
    assert _types(rows) == {"classification"}
    row = rows[0]
    assert str(row.fqid) == "class/sun2020"
    assert row.short_name == "SUN2020"
    assert row.name == "Svensk utbildningsnomenklatur"


def test_classification_matches_short_name(db: sqlite3.Connection) -> None:
    out = search(db, "sun2020", field="description", type="classification")
    assert [str(r.fqid) for r in out.results] == ["class/sun2020"]


def test_register_fts_row_carries_fqid(db: sqlite3.Connection) -> None:
    out = search(db, "LISA", field="description", type="register")
    rows = out.results
    assert _types(rows) == {"register"}
    assert str(rows[0].fqid) == "scb/lisa"


def test_variable_fts_row_carries_binding_fqid(db: sqlite3.Connection) -> None:
    out = search(db, "Kön", field="description", type="variable")
    rows = out.results
    assert _types(rows) == {"variable"}
    assert str(rows[0].fqid) == "scb/lisa/kon"


def test_variable_search_matches_delivery_column_name() -> None:
    conn = build_slugged_db(
        variable=("Orsak till missnöje, formell utbildning", 32183, 1001, "Kol"),
        delivery_column_name="fedunsatreason_1",
        variable_slug="formal-utbildning",
    )
    _rebuild_fts(conn)

    out = search(conn, "fedunsatreason", field="description", type="variable")

    rows = out.results
    assert _types(rows) == {"variable"}
    assert str(rows[0].fqid) == "scb/lisa/formal-utbildning"
    assert rows[0].delivery_column_names == ("fedunsatreason_1",)


def test_variable_search_preserves_whitespace_delivery_column_name() -> None:
    conn = build_slugged_db(
        variable=("Annual expense", 32183, 1001, "Kol"),
        delivery_column_name="TOTAL COST",
        variable_slug="annual-expense",
    )
    _rebuild_fts(conn)

    out = search(conn, "TOTAL", field="description", type="variable")

    rows = out.results
    assert _types(rows) == {"variable"}
    assert str(rows[0].fqid) == "scb/lisa/annual-expense"
    assert rows[0].delivery_column_names == ("TOTAL COST",)


def test_variable_search_reads_alias_table_when_fts_payload_is_legacy_text() -> None:
    conn = build_slugged_db(
        variable=("Orsak till missnöje, formell utbildning", 32183, 1001, "Kol"),
        delivery_column_name="fedunsatreason_1",
        variable_slug="formal-utbildning",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=32183,
        delivery_column_name="zedalias",
    )
    _rebuild_fts(conn)
    variable_id = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = 'formal-utbildning'"
    ).fetchone()[0]
    conn.execute(
        "UPDATE variable_fts SET delivery_column_names = ? WHERE rowid = ?",
        ("fedunsatreason_1 zedalias", variable_id),
    )

    out = search(conn, "fedunsatreason", field="description", type="variable")

    rows = out.results
    assert _types(rows) == {"variable"}
    assert str(rows[0].fqid) == "scb/lisa/formal-utbildning"
    assert rows[0].delivery_column_names == ("fedunsatreason_1", "zedalias")


def test_variable_search_delivery_scope_drops_unheld_alias_hit() -> None:
    conn = build_slugged_db(
        variable=("Plain variable", 32183, 1001, "HeldColumn"),
        delivery_column_name="HeldColumn",
        variable_slug="plain-variable",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=32183,
        delivery_column_name="LeakTermAlias",
    )
    _rebuild_fts(conn)

    out = search(
        conn,
        "LeakTerm",
        field="description",
        type="variable",
        fqids={"scb/lisa/plain-variable"},
        delivery_column_scope={"scb/lisa/plain-variable": {"HeldColumn"}},
    )

    assert out.total_count == 0
    assert out.results == ()


def test_variable_search_delivery_scope_keeps_description_hit() -> None:
    conn = build_slugged_db(
        variable=("Plain variable", 32183, 1001, "HeldColumn"),
        delivery_column_name="HeldColumn",
        variable_slug="plain-variable",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=32183,
        delivery_column_name="LeakTermAlias",
    )
    conn.execute(
        "UPDATE variable SET description = ? WHERE slug = 'plain-variable'",
        ("LeakTerm appears in the public description",),
    )
    _rebuild_fts(conn)

    out = search(
        conn,
        "LeakTerm",
        field="description",
        type="variable",
        fqids={"scb/lisa/plain-variable"},
        delivery_column_scope={"scb/lisa/plain-variable": {"HeldColumn"}},
    )

    assert out.total_count == 1
    assert str(out.results[0].fqid) == "scb/lisa/plain-variable"
    assert out.results[0].delivery_column_names == ("HeldColumn",)


def test_variable_search_delivery_scope_filters_before_group_folding() -> None:
    conn = build_slugged_db(
        variable=("First variable", 32183, 1001, "HeldA"),
        delivery_column_name="HeldA",
        variable_slug="first-variable",
    )
    add_variable(
        conn,
        register_id=1,
        var_id=42181,
        name="Second variable",
        slug="second-variable",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=42181,
        delivery_column_name="HeldB",
    )
    first_id = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = 'first-variable'"
    ).fetchone()[0]
    second_id = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = 'second-variable'"
    ).fetchone()[0]
    conn.executemany(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, 10, ?)",
        [(first_id, "LeakTermA"), (second_id, "LeakTermB")],
    )
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source) VALUES (80, 'variable', 1, 'pair', 'Pair group', 'curated')"
    )
    conn.execute(
        "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
        "VALUES (80, 'member', 0, 'member')"
    )
    conn.executemany(
        "INSERT INTO concept_group_variable "
        "(group_id, variable_id, delivery_column_name) VALUES (80, ?, NULL)",
        [(first_id,), (second_id,)],
    )
    _rebuild_fts(conn)

    out = search(
        conn,
        "LeakTerm",
        field="description",
        type="variable",
        fqids={"scb/lisa/first-variable", "scb/lisa/second-variable"},
        delivery_column_scope={
            "scb/lisa/first-variable": {"HeldA"},
            "scb/lisa/second-variable": {"HeldB"},
        },
    )

    assert out.total_count == 0
    assert out.results == ()


def test_variable_name_hit_ranks_above_delivery_column_hit() -> None:
    conn = build_slugged_db(
        variable=("Orsak till missnöje, formell utbildning", 32183, 1001, "Kol"),
        delivery_column_name="fedunsatreason_1",
        variable_slug="formal-utbildning",
    )
    add_variable(
        conn,
        register_id=1,
        var_id=42181,
        name="fedunsatreason",
        slug="name-hit",
    )
    add_binding(
        conn,
        cvid=1002,
        register_id=1,
        register_variant_id=10,
        regver_id=100,
        var_id=42181,
        delivery_column_name="other_column",
    )
    _rebuild_fts(conn)

    out = search(conn, "fedunsatreason", field="description", type="variable")

    assert [str(row.fqid) for row in out.results] == [
        "scb/lisa/name-hit",
        "scb/lisa/formal-utbildning",
    ]


def test_variable_search_carries_operational_definition() -> None:
    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable SET operational_definition = ? WHERE slug = 'kon'",
        ("Registered sex at year end",),
    )
    _rebuild_fts(conn)

    out = search(conn, "Registered", field="description", type="variable")

    rows = out.results
    assert _types(rows) == {"variable"}
    assert rows[0].operational_definition == "Registered sex at year end"


def test_type_all_spans_classification(db: sqlite3.Connection) -> None:
    # One query that hits a register/variable AND a classification token.
    reg = search(db, "Svensk", field="description", type="all")
    assert "classification" in _types(reg.results)


def test_register_type_excludes_classification(db: sqlite3.Connection) -> None:
    out = search(db, "Svensk", field="description", type="register")
    assert "classification" not in _types(out.results)


def test_variable_type_excludes_classification(db: sqlite3.Connection) -> None:
    out = search(db, "Svensk", field="description", type="variable")
    assert "classification" not in _types(out.results)


def test_register_scope_excludes_classification(db: sqlite3.Connection) -> None:
    # Classifications are catalog-scoped: a --register scope means "registers
    # only", so the classification index is not searched.
    out = search(
        db, "Svensk", field="description", type="classification", register="lisa"
    )
    assert out.results == ()


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

    # `_search_display_row` consumes the typed-model wire shape (#701): a
    # classification row carries `name` (not `classification_name`) and `rank`.
    row = _search_display_row(
        {
            "type": "classification",
            "fqid": "class/sun2020",
            "short_name": "SUN2020",
            "name": "Svensk utbildningsnomenklatur",
            "rank": -1.0,
        }
    )
    assert row["register_name"] == "SUN2020"
    assert row["variable_name"] == "Svensk utbildningsnomenklatur"


def test_cli_display_row_projects_register() -> None:
    # A register row carries `name` (its display name) but no `register` alias
    # key, and `type == "register"` isn't classification/variable — so without a
    # dedicated projection the `register_name` column renders blank (#701
    # typed-return regression: the old dict carried `register_name` directly).
    from reg_meta.cli import _search_display_row

    row = _search_display_row(
        {
            "type": "register",
            "fqid": "scb/lisa",
            "name": "LISA",
            "purpose": "Longitudinal integration database",
            "rank": -1.0,
        }
    )
    assert row["register_name"] == "LISA"


def test_cli_display_row_projects_classification_succession() -> None:
    # A `classification_succession` row (#571) collapses an edition chain; it
    # shares the classification identity columns but adds an `editions` list.
    # The CLI projector fills the generic columns (mirroring classification) and
    # appends a folded-family hint so a `--type all` table reads clearly.
    from reg_meta.cli import _search_display_row

    row = _search_display_row(_succession_row())
    assert row["register_name"] == "SSYK2012"
    assert (
        row["variable_name"] == "Standard för svensk yrkesklassificering (2 editions)"
    )
    # Scalar count for the classification column set; must NOT clobber the raw
    # `editions` list (which feeds --format json).
    assert row["n_editions"] == 2
    assert isinstance(row["editions"], list) and len(row["editions"]) == 2


def _succession_row() -> dict[str, object]:
    # The typed-model wire shape (#701): `name` (not `classification_name`),
    # `matched_count` (not the raw `matched` list), `rank` (not `fts_rank`).
    return {
        "type": "classification_succession",
        "fqid": "class/ssyk2012",
        "short_name": "SSYK2012",
        "name": "Standard för svensk yrkesklassificering",
        "editions": [
            {"slug": "ssyk2012", "name": "SSYK 2012", "effective_year": None},
            {"slug": "ssyk96", "name": "SSYK 96", "effective_year": 1996},
        ],
        "matched_count": 0,
        "rank": -1.0,
    }


def _classification_row() -> dict[str, object]:
    return {
        "type": "classification",
        "fqid": "class/sun2020",
        "short_name": "SUN2020",
        "name": "Svensk utbildningsnomenklatur",
        "rank": -1.0,
    }


def test_write_payload_succession_table_shows_fqid(tmp_path: Path) -> None:
    # Codex P3: a pure-succession result set (types == {"classification_
    # succession"}) must use the classification-native columns so the navigable
    # `fqid` is visible — not the generic columns, which have no fqid.
    from reg_meta.cli import _write_payload

    payload = {"data": {"results": [_succession_row()], "total_count": 1}}
    out = tmp_path / "out.txt"
    _write_payload(("search", None), payload, str(out), fmt="list")
    text = out.read_text(encoding="utf-8")
    assert "class/ssyk2012" in text  # fqid surfaced
    assert "n_editions" in text and "2" in text  # fold count surfaced


def test_write_payload_mixed_classification_succession_shows_fqid(
    tmp_path: Path,
) -> None:
    # A mixed classification + succession set (types <= {classification,
    # classification_succession}) also uses the classification columns.
    from reg_meta.cli import _write_payload

    payload = {
        "data": {
            "results": [_classification_row(), _succession_row()],
            "total_count": 2,
        }
    }
    out = tmp_path / "out.txt"
    _write_payload(("search", None), payload, str(out), fmt="list")
    text = out.read_text(encoding="utf-8")
    assert "class/sun2020" in text and "class/ssyk2012" in text  # both fqids
    assert "n_editions" in text


def test_empty_description_query_folds_nothing(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # No searchable token → the FTS path no-ops AND label folding is gated off,
    # so an empty/punctuation query must NOT return every concept group via the
    # raw `%%` LIKE pattern (Codex P2).
    for q in ("", "   ", '"" -- ;'):
        assert search(db_with_cls_group, q, field="description").results == ()


def test_years_excludes_classifications(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # Classifications carry no validity window, so a --years filter excludes both
    # the leaves and the (label-matched) family — no unfilterable false positives
    # (Codex P2). Without --years the same query DOES return the family.
    assert search(
        db_with_cls_group, "Svensk", field="description", type="classification"
    ).results
    assert (
        search(
            db_with_cls_group,
            "Svensk",
            field="description",
            type="classification",
            years="2010",
        ).results
        == ()
    )


def test_classification_editions_orders_by_bfs_depth() -> None:
    # #588: the search fold `_classification_editions` is TERMINAL-CENTRIC (no
    # queried node — it collapses a whole family onto its terminal), so collect-all-
    # ancestors is correct here; only the ORDERING changes — terminal-first by BFS
    # DEPTH (robust to undated edges), not by descending effective_year. Chain
    # eA→eB(UNDATED)→eC(terminal), plus a MERGE eD→eC: depth 0 = eC, depth 1 = eB+eD,
    # depth 2 = eA. The old year-sort would have sunk the undated eB below dated
    # predecessors; depth order is the walk, date-independent.
    from reg_meta.queries import _classification_editions

    conn = build_slugged_db()
    for slug in ("eA", "eB", "eC", "eD"):
        _seed_classification(conn, slug=slug, short_name=slug.upper(), name=slug)
    _seed_classification_edge(
        conn, predecessor="eA", successor="eB", effective_year=2000
    )
    _seed_classification_edge(
        conn, predecessor="eB", successor="eC", effective_year=None
    )
    _seed_classification_edge(
        conn, predecessor="eD", successor="eC", effective_year=2010
    )
    conn.commit()
    editions = _classification_editions(conn, "eC")
    slugs = [e["slug"] for e in editions]
    # Terminal first (depth 0), then both depth-1 predecessors (slug-sorted: eB, eD),
    # then depth-2 eA. The terminal-centric fold INCLUDES the merge sibling eD
    # (unlike Catalog.classification_chain, which is anchored on a queried node).
    assert slugs == ["eC", "eB", "eD", "eA"]
    by_slug = {e["slug"]: e for e in editions}
    assert by_slug["eC"]["effective_year"] is None  # terminal, no outbound edge
    assert by_slug["eB"]["effective_year"] is None  # undated edge, display-only
    assert by_slug["eA"]["effective_year"] == 2000
