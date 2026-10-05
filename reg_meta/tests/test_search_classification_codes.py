"""Classification search through code containment: a code-shaped query surfaces
the classifications that own a matching code, deduplicated against name hits,
excluded before the bounded prefix, ranked exact-before-prefix, and folded into
concept groups and succession chains.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from reg_meta.queries import search
from search_test_support import reader_search_conn

if TYPE_CHECKING:
    import pytest


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
