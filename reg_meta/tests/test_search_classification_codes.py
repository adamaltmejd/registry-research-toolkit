"""Classification search: a code-shaped query surfaces the classifications that
own a matching code, deduplicated against name hits, excluded before the bounded
prefix and ranked exact-before-prefix; classification hits fold into concept
groups and succession chains; and classifications stay out of register- and
year-scoped searches.

The fixtures that seed explicit rows rebuild the external-content FTS indexes by
hand (`rebuild_fts`); the readable sources go through the real writer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.queries import search
from search_test_support import (
    add_value_set,
    build_slugged_db,
    reader_search_conn,
    rebuild_fts as _rebuild_fts,
)

if TYPE_CHECKING:
    import sqlite3


def test_code_owner_exclusion_happens_before_the_candidate_limit(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Three classifications own `C12`; the two that sort first by short name are
    excluded. A one-row page must still reach the remaining owner, so the
    exclusion runs before the code arm's bounded prefix, not after it."""
    conn = reader_search_conn(tmp_path_factory, "search-code-exclusion")

    page = search(
        conn,
        "C12",
        field="description",
        type="classification",
        limit=1,
        fold_groups=False,
        exclude_fqids={"class/excluded-a", "class/excluded-b"},
    )

    assert [str(row.fqid) for row in page.results] == ["class/code-owner"]
    assert not page.has_more


def test_succession_editions_are_ordered_terminal_first_by_chain_depth(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Chain `ea -> eb (2000)`, `eb -> ec (undated)`, plus the merge `ed -> ec
    (2010)`. Depth from the terminal orders the editions, never the edge year:
    `ec`, then `eb` and `ed` (depth 1, by slug), then `ea`."""
    conn = reader_search_conn(tmp_path_factory, "classification-editions")

    out = search(conn, "Y10", field="description", type="classification")

    [row] = out.results
    assert row.type == "classification_succession"
    assert str(row.fqid) == "class/ec"
    assert [(e.slug, e.effective_year) for e in row.editions] == [
        ("ec", None),
        ("eb", None),
        ("ed", 2010),
        ("ea", 2000),
    ]


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
    out = search(
        db_with_like_metachar_codes, "12_", field="description", type="classification"
    )
    fqids = [str(r.fqid) for r in out.results if r.type == "classification"]
    assert "class/underscore-owner" in fqids
    assert "class/plain-owner" not in fqids


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


def test_classification_member_fold_without_label_match(
    db_with_cls_group: sqlite3.Connection,
) -> None:
    # short_name search ("SUN") matches both members' FTS but NOT the group
    # label "Svensk …" — ≥2 member hits still fold the family (symmetric with
    # variables).
    out = search(db_with_cls_group, "SUN", field="description", type="classification")
    groups = [r for r in out.results if r.type == "group"]
    assert any(r.group_key == "sun" for r in groups)


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
