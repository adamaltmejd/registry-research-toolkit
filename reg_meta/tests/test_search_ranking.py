"""Search ranking and cursor contract: the published relevance order, its
stability across cursor pages, and scope/exclusion applied before the bounded
candidate prefix.

The readable sources surface classifications through the code-containment arm:
the resolved-catalog writer leaves `classification_fts` unpopulated, so the
classification name arm returns nothing on a pipeline-built artifact. The
ordering, promotion and cursor properties under test live in the merged
final sort, which every arm feeds.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta.queries import search
from search_test_support import reader_search_conn

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


def test_generic_identity_matches_do_not_swamp_the_ranked_order(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """51 classifications named exactly `C12` (owning only `C120`) and one
    `discriminative` owning `C12` itself, which the code arm ranks first. More
    than 50 identity matches switch identity promotion off."""
    conn = reader_search_conn(tmp_path_factory, "search-identity-swamp")
    first = search(conn, "C12", field="description", type="classification", limit=25)
    assert str(first.results[0].fqid) == "class/discriminative"
    assert first.next_cursor is not None
    second = search(
        conn,
        "C12",
        field="description",
        type="classification",
        limit=25,
        cursor=first.next_cursor,
    )

    combined = _fqids(first, second)
    assert len(combined) == len(set(combined))
    assert combined[0] == "class/discriminative"


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
