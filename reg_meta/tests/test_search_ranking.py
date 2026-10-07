"""Search ranking and cursor contract: the published relevance order, its
stability across cursor pages, and scope/exclusion applied before the bounded
candidate prefix.

The ordering, promotion and cursor properties under test live in the merged
final sort, which every arm (classification name and code containment alike)
feeds.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta.queries import search
from search_test_support import (
    add_register,
    add_state,
    add_variable,
    build_slugged_db,
    reader_search_conn,
    rebuild_fts as _rebuild_fts,
)

if TYPE_CHECKING:
    import sqlite3

    from reg_meta.search import SearchResults


@pytest.fixture(scope="module")
def relevance_conn(tmp_path_factory: pytest.TempPathFactory) -> sqlite3.Connection:
    """`class/exact` (short name `C12`) owns only `C120`, so the code arm ranks
    it after `broad-a`/`broad-b`, which own `C12` itself and whose names merely
    start with it. `item-0..3` own `D45`."""
    return reader_search_conn(tmp_path_factory, "search-relevance")


def _classifications(
    conn: sqlite3.Connection,
    query: str,
    *,
    limit: int,
    cursor: str | None = None,
    exclude_fqids: set[str] | None = None,
) -> SearchResults:
    return search(
        conn,
        query,
        field="description",
        type="classification",
        limit=limit,
        fold_groups=False,
        cursor=cursor,
        exclude_fqids=exclude_fqids,
    )


def _fqids(*pages: SearchResults) -> list[str]:
    return [str(row.fqid) for page in pages for row in page.results]


def test_exact_identity_leads_relevance_order_across_cursor_pages(
    relevance_conn: sqlite3.Connection,
) -> None:
    first = _classifications(relevance_conn, "C12", limit=2)
    assert first.next_cursor is not None
    second = _classifications(relevance_conn, "C12", limit=2, cursor=first.next_cursor)
    whole = _classifications(relevance_conn, "C12", limit=3)

    assert _fqids(first, second) == _fqids(whole)
    assert _fqids(first, second) == ["class/exact", "class/broad-a", "class/broad-b"]
    assert not second.has_more


def test_mixed_type_relevance_order_is_stable_across_cursor_pages(
    relevance_conn: sqlite3.Connection,
) -> None:
    first = search(
        relevance_conn,
        "C12",
        field="description",
        type="all",
        limit=2,
        fold_groups=False,
    )
    assert first.next_cursor is not None
    second = search(
        relevance_conn,
        "C12",
        field="description",
        type="all",
        limit=2,
        fold_groups=False,
        cursor=first.next_cursor,
    )

    assert _fqids(first, second) == ["class/exact", "class/broad-a", "class/broad-b"]
    assert not second.has_more


def test_excluded_identity_is_removed_before_limit_and_binds_the_cursor(
    relevance_conn: sqlite3.Connection,
) -> None:
    excluded = {"class/item-2"}
    first = _classifications(relevance_conn, "D45", limit=2, exclude_fqids=excluded)
    assert first.next_cursor is not None
    second = _classifications(
        relevance_conn,
        "D45",
        limit=2,
        exclude_fqids=excluded,
        cursor=first.next_cursor,
    )

    assert _fqids(first, second) == ["class/item-0", "class/item-1", "class/item-3"]
    with pytest.raises(RegMetaError) as exc:
        _classifications(relevance_conn, "D45", limit=2, cursor=first.next_cursor)
    assert exc.value.code == "invalid_search_cursor"


def test_exact_identity_matches_lead_however_many_share_the_name(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """51 classifications named exactly `C12` (owning only `C120`) and one
    `zz-discriminative` whose description repeats `C12`, which makes it the
    best-ranked name hit without an identity match. More than 50 identity matches
    switch prefix promotion off, but every exact match still leads (#1180)."""
    conn = reader_search_conn(tmp_path_factory, "search-identity-swamp")
    first = search(conn, "C12", field="description", type="classification", limit=25)
    assert first.next_cursor is not None
    second = search(
        conn,
        "C12",
        field="description",
        type="classification",
        limit=50,
        cursor=first.next_cursor,
    )

    combined = _fqids(first, second)
    assert len(combined) == len(set(combined))
    assert set(combined[:51]) == {f"class/generic-{index:02d}" for index in range(51)}
    assert combined[51] == "class/zz-discriminative"


@pytest.fixture(scope="module")
def admission_conn(tmp_path_factory: pytest.TempPathFactory) -> sqlite3.Connection:
    """1,001 `Annual year` fillers (delivery column `Annual year`, definition
    repeating `year` and `bø`) fill every bounded variable prefix ahead of
    `Year` (exact name) and of `Target` and `Place`, exact only through their
    delivery columns `Year` and `Bø`: they outrank them by bm25 and sort before
    them by name and by delivery column."""
    return reader_search_conn(tmp_path_factory, "search-exact-admission")


@pytest.mark.parametrize(
    ("field", "fold_groups", "exact"),
    [
        ("description", True, ["Target", "Year"]),
        ("all", False, ["Target", "Target", "Year", "Year"]),
        ("varname", False, ["Year"]),
        ("datacolumn", False, ["Target"]),
    ],
)
def test_exact_name_is_admitted_and_leads_past_full_candidate_prefixes(
    admission_conn: sqlite3.Connection,
    field: str,
    fold_groups: bool,
    exact: list[str],
) -> None:
    """Every bounded variable arm admits exact name and delivery-column matches
    before its LIMIT, and they lead the order (#1180). Unfolded, one variable
    can lead through several arms."""
    page = search(
        admission_conn,
        "year",
        field=field,
        type="variable",
        limit=len(exact) + 1,
        fold_groups=fold_groups,
    )

    names = [result.name for result in page.results]
    assert sorted(names[:-1]) == exact
    assert names[-1] == "Annual year"


def test_exact_name_without_an_ascii_fold_is_admitted(
    admission_conn: sqlite3.Connection,
) -> None:
    """`ø` has no decomposition, so an ASCII fold of the query deletes it: the
    exact delivery column `Bø` must still admit `Place` past the fillers whose
    definition repeats `bø`."""
    page = search(admission_conn, "Bø", field="description", type="variable", limit=2)

    assert [result.name for result in page.results] == ["Place", "Annual year"]


def test_unheld_exact_alias_does_not_win_admission(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """In holdings scope 1,001 held `Annual year` columns fill the
    delivery-column prefix. `Target` holds the exact column `Year`; `Unheld`
    holds only `Year count` and has an unheld `Year` alias, which must not admit
    it ahead of the fillers."""
    conn = reader_search_conn(tmp_path_factory, "search-exact-holdings", kind="steward")

    page = search(
        conn,
        "year",
        scope="holdings",
        field="datacolumn",
        type="variable",
        limit=3,
    )

    assert [(result.datacolumn, result.name) for result in page.results] == [
        ("Year", "Target"),
        ("Annual year", "Filler"),
        ("Annual year", "Filler"),
    ]


def test_unfolded_identity_swamp_gate_does_not_depend_on_page_size(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """51 classifications reach the query only through the code arm (`C120`)
    and match it by slug prefix (`c12-generic-NN`); `zz-discriminative` is the
    only name hit. Unfolded, the swamp gate must count the whole bounded match
    set, so a small page sees the same order as a large one."""
    conn = reader_search_conn(tmp_path_factory, "search-identity-swamp-code")
    small = search(
        conn,
        "C12",
        field="description",
        type="classification",
        limit=25,
        fold_groups=False,
    )
    large = search(
        conn,
        "C12",
        field="description",
        type="classification",
        limit=60,
        fold_groups=False,
    )

    assert _fqids(small)[0] == "class/zz-discriminative"
    assert _fqids(small) == _fqids(large)[:25]


def test_delivery_alias_identity_participates_in_relevance_order(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """`unrelated` matches `exact` only through its delivery column `EXACT`;
    the two `Exact topic` variables match by name prefix."""
    conn = reader_search_conn(tmp_path_factory, "search-alias-relevance")

    out = search(
        conn, "exact", field="description", type="variable", limit=3, fold_groups=False
    )

    assert _fqids(out) == [
        "scb/aliases/unrelated",
        "scb/aliases/broad-a",
        "scb/aliases/broad-b",
    ]


def test_register_scope_is_applied_before_sql_pagination() -> None:
    conn = build_slugged_db(
        variable=("Needle alpha", 32183, 1001, "Alpha"),
        delivery_column_name="Alpha",
        variable_slug="needle-alpha",
    )
    for index in range(5):
        add_variable(
            conn,
            register_id=1,
            var_id=42180 + index,
            name=f"Needle blocker {index}",
            slug=f"needle-blocker-{index}",
        )
    add_register(conn, register_id=2, slug="regb", name="Register B")
    add_variable(
        conn,
        register_id=2,
        var_id=52180,
        name="Needle target",
        slug="needle-target",
    )
    _rebuild_fts(conn)

    page = search(
        conn,
        "Needle",
        field="description",
        type="variable",
        register="Register B",
        limit=1,
    )

    assert [str(result.fqid) for result in page.results] == ["scb/regb/needle-target"]
    assert not page.has_more


# The candidate prefix every foldable/entity arm takes (the 1,000-position cursor
# horizon plus one look-ahead row), and the number of year-eligible variables
# parked BEHIND a full prefix of ineligible ones. Footgun: this literal mirrors the
# search horizon; if the horizon grows past it, the eligible tail is no longer
# parked behind a FULL prefix and these tests keep passing while testing nothing.
_CANDIDATE_BOUND = 1_001

_YEAR_ELIGIBLE = 4

_ELIGIBLE_NAMES = {
    f"Needle {index:04d}"
    for index in range(_CANDIDATE_BOUND, _CANDIDATE_BOUND + _YEAR_ELIGIBLE)
}


def _year_scoped_conn() -> sqlite3.Connection:
    """A whole candidate prefix of 2010-only variables sharing a searchable name
    prefix, then `_YEAR_ELIGIBLE` more delivered in 2020.

    Both variable arms order these ahead of the eligible tail — the LIKE arm by
    `v.name`, the FTS arm by (tied bm25, `vf.rowid`) — so a year filter applied to
    the bounded prefix instead of inside each candidate query sees only 2010 rows
    and returns nothing at all for `years="2020"`.
    """
    conn = build_slugged_db(variable=None)
    for index in range(_CANDIDATE_BOUND + _YEAR_ELIGIBLE):
        slug = f"needle-{index:04d}"
        add_variable(
            conn,
            register_id=1,
            var_id=20_000 + index,
            name=f"Needle {index:04d}",
            slug=slug,
        )
        eligible = index >= _CANDIDATE_BOUND
        add_state(
            conn,
            register_id=1,
            variable_slug=slug,
            register_variant_id=10,
            valid_from="2020-01-01" if eligible else "2010-01-01",
            valid_to="2020-12-31" if eligible else "2010-12-31",
        )
    conn.commit()
    _rebuild_fts(conn)
    return conn


@pytest.mark.parametrize("field", ["varname", "description"])
@pytest.mark.parametrize("fold_groups", [True, False])
def test_year_scope_is_applied_before_sql_pagination(
    field: str, fold_groups: bool
) -> None:
    conn = _year_scoped_conn()

    page = search(
        conn,
        "Needle",
        field=field,
        type="variable",
        years="2020",
        fold_groups=fold_groups,
    )

    assert {result.name for result in page.results} == _ELIGIBLE_NAMES
    assert not page.has_more


@pytest.mark.parametrize("field", ["varname", "description"])
def test_year_scoped_cursor_traverses_every_eligible_result(field: str) -> None:
    conn = _year_scoped_conn()

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = search(
            conn,
            "Needle",
            field=field,
            type="variable",
            years="2020",
            limit=1,
            cursor=cursor,
        )
        seen.extend(result.name for result in page.results)
        if not page.has_more:
            break
        cursor = page.next_cursor

    assert len(seen) == len(set(seen)) == _YEAR_ELIGIBLE
    assert set(seen) == _ELIGIBLE_NAMES
