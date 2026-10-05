"""Search scope contract: which entity surfaces a query reaches, how a
multi-token query combines, and the FQIDs and fields each leaf row carries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from reg_meta.queries import search
from search_test_support import reader_search_conn

if TYPE_CHECKING:
    import pytest


def test_multi_token_query_requires_every_token(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """`exact` alone also reaches `unrelated` through its delivery column
    `EXACT`; adding `topic` keeps only the variables matching both tokens."""
    conn = reader_search_conn(tmp_path_factory, "search-alias-relevance")

    out = search(conn, "exact topic", field="description", type="variable")

    assert sorted(str(row.fqid) for row in out.results) == [
        "scb/aliases/broad-a",
        "scb/aliases/broad-b",
    ]


def test_punctuation_only_query_matches_no_authored_label(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """The group label `Inkomst -- brutto/netto` contains `--` literally; a
    query with no word character still returns nothing rather than matching it."""
    conn = reader_search_conn(tmp_path_factory, "search-punctuation-label")

    assert search(conn, "Inkomst", field="description").results
    assert search(conn, "--", field="description").results == ()
