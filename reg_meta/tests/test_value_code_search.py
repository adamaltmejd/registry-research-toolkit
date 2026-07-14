"""Query-layer coverage for #352 code/value search (`search(field="value",
type="value")` → `_search_values_fts`).

Builds a minimal in-memory DB from the shared DDL and seeds exactly the
value_code / code_variable_map / value_code_fts rows each case needs (the SCB
build pipeline can't dial these knobs precisely). value_code_fts is external
content, so it's populated via the FTS5 'rebuild' command after the value_code
INSERTs — the stoplist isn't reproduced here (none of these labels are
stoplisted), which is faithful for the ranking/dedup/scope behaviour under test.
"""

from __future__ import annotations

import json
import sqlite3
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import RegMetaError
from reg_meta.queries import search

if TYPE_CHECKING:
    from collections.abc import Iterator


def _seed_register(conn: sqlite3.Connection, register_id: int, slug: str) -> None:
    conn.execute(
        "INSERT INTO register (register_id, provider_id, slug, name) VALUES (?, 1, ?, ?)",
        (register_id, slug, slug.upper()),
    )


def _seed_variable(
    conn: sqlite3.Connection, register_id: int, provider_key: str, name: str, slug: str
) -> int:
    return conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) VALUES (?, ?, ?, ?)",
        (register_id, provider_key, name, slug),
    ).lastrowid


def _seed_code(conn: sqlite3.Connection, code_id: int, code: str, label: str) -> None:
    conn.execute(
        "INSERT INTO value_code (code_id, code, label) VALUES (?, ?, ?)",
        (code_id, code, label),
    )


def _map(conn: sqlite3.Connection, code_id: int, variable_id: int) -> None:
    conn.execute(
        "INSERT INTO code_variable_map (code_id, variable_id) VALUES (?, ?)",
        (code_id, variable_id),
    )


def _finalize(conn: sqlite3.Connection) -> None:
    """Compute mapping_count + (re)build the external-content FTS index, mirroring
    the build's post-passes over the seeded rows."""
    conn.execute(
        "UPDATE value_code SET mapping_count = ("
        "SELECT COUNT(*) FROM code_variable_map WHERE code_id = value_code.code_id)"
    )
    conn.execute("INSERT INTO value_code_fts(value_code_fts) VALUES('rebuild')")
    conn.commit()


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    from reg_meta_build.db import DDL, seed_providers

    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(DDL)
    seed_providers(c)
    try:
        yield c
    finally:
        c.close()


def test_code_shaped_hit_ranks_above_label_hit(conn: sqlite3.Connection) -> None:
    """A code value that is ALSO a label substring of another pair: querying the
    code text must rank the code-exact hit ABOVE the label-text hit (an exact code
    match is the strongest signal). `fts_rank` is smaller-first."""
    _seed_register(conn, 1, "reg")
    vid = _seed_variable(conn, 1, "10", "Var", "var")
    # code "0180" on one pair; another pair whose LABEL contains "0180".
    _seed_code(conn, 1, "0180", "Stockholms kommun")
    _seed_code(conn, 2, "9999", "Kod 0180 i text")
    _map(conn, 1, vid)
    _map(conn, 2, vid)
    _finalize(conn)

    results = search(conn, "0180", field="value", type="value").results
    by_code = {r.code: r for r in results}
    assert "0180" in by_code, "code-exact hit must be present"
    assert "9999" in by_code, "label-text hit must be present"
    # Smaller fts_rank sorts first; the code-exact hit is seeded below the FTS floor.
    assert by_code["0180"].rank < by_code["9999"].rank
    # And it is actually first in the returned (pre-sorted) order.
    assert results[0].code == "0180"


def test_dedup_code_id_appears_once(conn: sqlite3.Connection) -> None:
    """A code-shaped query whose text matches BOTH a label-FTS hit and the
    code-shape path on the SAME pair returns that code_id ONCE."""
    _seed_register(conn, 1, "reg")
    vid = _seed_variable(conn, 1, "10", "Var", "var")
    # The pair's CODE is "0180" and its LABEL also contains "0180" → both paths fire.
    _seed_code(conn, 1, "0180", "Label 0180 here")
    _map(conn, 1, vid)
    _finalize(conn)

    results = search(conn, "0180", field="value", type="value").results
    codes = [r.code for r in results]
    assert codes.count("0180") == 1, f"expected one dedup'd hit, got {codes}"


def test_code_like_metacharacters_match_literally(conn: sqlite3.Connection) -> None:
    _seed_register(conn, 1, "reg")
    vid = _seed_variable(conn, 1, "10", "Var", "var")
    for code_id, code in (
        (1, "12_5"),
        (2, "120"),
        (3, "12%5"),
        (4, "129"),
    ):
        _seed_code(conn, code_id, code, f"Label {code_id}")
        _map(conn, code_id, vid)
    _finalize(conn)

    def _codes(query: str) -> set[str]:
        return {
            r.code for r in search(conn, query, field="value", type="value").results
        }

    assert _codes("12_") == {"12_5"}
    assert _codes("12%") == {"12%5"}


def test_classification_codes_rank_before_register_local_codes(
    conn: sqlite3.Connection,
) -> None:
    _seed_register(conn, 1, "reg")
    vid = _seed_variable(conn, 1, "10", "Register local code owner", "owner")
    _seed_code(conn, 1, "E22", "Hyperfunktion av hypofysen")
    _seed_code(conn, 2, "E22", "Register local exact")
    _seed_code(conn, 3, "E220", "Register local compact prefix")
    _seed_code(conn, 4, "E22.0", "ICD child")
    _map(conn, 2, vid)
    _map(conn, 3, vid)
    classification_id = conn.execute(
        "INSERT INTO classification (short_name, name) VALUES ('ICD-10-SE', 'ICD')"
    ).lastrowid
    for code_id in (1, 4):
        conn.execute(
            "INSERT INTO classification_code "
            "(classification_id, code_id, level, is_valid) VALUES (?, ?, NULL, 1)",
            (classification_id, code_id),
        )
    _finalize(conn)

    results = search(conn, "E22", field="value", type="value", limit=4).results

    assert [(r.code, r.label) for r in results] == [
        ("E22", "Hyperfunktion av hypofysen"),
        ("E22.0", "ICD child"),
        ("E22", "Register local exact"),
        ("E220", "Register local compact prefix"),
    ]


def test_code_owner_scope_splits_classification_and_register_local_pages(
    conn: sqlite3.Connection,
) -> None:
    _seed_register(conn, 1, "reg")
    vid = _seed_variable(conn, 1, "10", "Register local code owner", "owner")
    _seed_code(conn, 1, "E22", "Hyperfunktion av hypofysen")
    _seed_code(conn, 2, "E22", "Register local exact")
    _seed_code(conn, 3, "E220", "Register local compact prefix")
    _seed_code(conn, 4, "E22.0", "ICD child")
    _map(conn, 2, vid)
    _map(conn, 3, vid)
    classification_id = conn.execute(
        "INSERT INTO classification (short_name, name) VALUES ('ICD-10-SE', 'ICD')"
    ).lastrowid
    for code_id in (1, 4):
        conn.execute(
            "INSERT INTO classification_code "
            "(classification_id, code_id, level, is_valid) VALUES (?, ?, NULL, 1)",
            (classification_id, code_id),
        )
    _finalize(conn)

    classification = search(
        conn,
        "E22",
        field="value",
        type="value",
        limit=2,
        code_owner_scope="classification",
    )
    register_local = search(
        conn,
        "E22",
        field="value",
        type="value",
        limit=2,
        code_owner_scope="register_local",
    )

    assert len(classification.results) == 2
    assert not classification.has_more
    assert [(r.code, r.label) for r in classification.results] == [
        ("E22", "Hyperfunktion av hypofysen"),
        ("E22.0", "ICD child"),
    ]
    assert len(register_local.results) == 2
    assert not register_local.has_more
    assert [(r.code, r.label) for r in register_local.results] == [
        ("E22", "Register local exact"),
        ("E220", "Register local compact prefix"),
    ]


def test_register_scope_drops_out_of_scope_only_owners(
    conn: sqlite3.Connection,
) -> None:
    """`--register` scope: a code whose ONLY owner is in another register yields no
    result (locks the `if reg_ids and not owners...` drop guard)."""
    _seed_register(conn, 1, "rega")
    _seed_register(conn, 2, "regb")
    vid_b = _seed_variable(conn, 2, "20", "BVar", "bvar")
    # The "Singelkod" label's only owner is a variable in register 2 (regb).
    _seed_code(conn, 1, "5", "Singelkod")
    _map(conn, 1, vid_b)
    _finalize(conn)

    # Scoped to register 1 (rega) → no surviving owner → dropped.
    scoped = search(
        conn, "Singelkod", field="value", type="value", register="rega"
    ).results
    assert scoped == (), f"out-of-scope-only code should be dropped, got {scoped}"
    # Scoped to register 2 (regb) → the owner survives → present.
    in_scope = search(
        conn, "Singelkod", field="value", type="value", register="regb"
    ).results
    assert any(r.label == "Singelkod" for r in in_scope)


def test_owner_cap_vs_full_count(conn: sqlite3.Connection) -> None:
    """6 variables owning one code: `variable_count` is the full 6, but the
    `variables` slice is capped at `_CODE_OWNERS_PER_HIT` (5)."""
    from reg_meta.queries import _CODE_OWNERS_PER_HIT

    _seed_register(conn, 1, "reg")
    _seed_code(conn, 1, "7", "Delad kod")
    for i in range(6):
        vid = _seed_variable(conn, 1, str(100 + i), f"Var{i}", f"var{i}")
        _map(conn, 1, vid)
    _finalize(conn)

    results = search(conn, "Delad kod", field="value", type="value").results
    hit = next(r for r in results if r.label == "Delad kod")
    assert hit.variable_count == 6
    assert len(hit.variables) == _CODE_OWNERS_PER_HIT == 5


def test_owner_cap_can_be_disabled_for_variable_owners(
    conn: sqlite3.Connection,
) -> None:
    """The web search page expands code rows into full variable-owner lists."""
    _seed_register(conn, 1, "reg")
    _seed_code(conn, 1, "7", "Delad kod")
    for i in range(6):
        vid = _seed_variable(conn, 1, str(100 + i), f"Var{i}", f"var{i}")
        _map(conn, 1, vid)
    _finalize(conn)

    results = search(
        conn,
        "Delad kod",
        field="value",
        type="value",
        code_variable_owner_limit=None,
    ).results
    hit = next(r for r in results if r.label == "Delad kod")
    assert hit.variable_count == 6
    assert len(hit.variables) == 6


def test_mapping_count_downweight_orders_rarer_first(conn: sqlite3.Connection) -> None:
    """Two pairs with the SAME matched label text but different mapping_count: the
    rarer (lower mapping_count) one ranks first. Pins the downweight DIRECTION."""
    _seed_register(conn, 1, "reg")
    # "common" pair owned by many variables; "rare" pair owned by one.
    _seed_code(conn, 1, "1", "Sjukdom vanlig")
    _seed_code(conn, 2, "2", "Sjukdom vanlig")  # same label text, distinct code/pair
    rare_owner = _seed_variable(conn, 1, "10", "Rare", "rare")
    _map(conn, 2, rare_owner)  # code_id 2 → 1 owner (rare)
    for i in range(8):  # code_id 1 → 8 owners (common)
        vid = _seed_variable(conn, 1, str(200 + i), f"Common{i}", f"common{i}")
        _map(conn, 1, vid)
    _finalize(conn)

    # Sanity: the two pairs really differ in mapping_count as set up.
    counts = dict(conn.execute("SELECT code, mapping_count FROM value_code").fetchall())
    assert counts["1"] == 8 and counts["2"] == 1

    results = search(conn, "Sjukdom vanlig", field="value", type="value").results
    rank = {r.code: r.rank for r in results}
    assert "1" in rank and "2" in rank
    # Rarer pair (code "2", mapping_count 1) sorts before the common one (code "1").
    assert rank["2"] < rank["1"]
    assert results[0].code == "2"


def _seed_n_label_codes(conn: sqlite3.Connection, n: int, label: str) -> None:
    """Seed `n` distinct (code, label) pairs that all share `label` text (so one
    label-FTS query matches all n), each owned by its own variable in register 1."""
    _seed_register(conn, 1, "reg")
    for i in range(n):
        _seed_code(conn, 100 + i, str(100 + i), f"{label} {i:02d}")
        vid = _seed_variable(conn, 1, str(100 + i), f"Var{i}", f"var{i}")
        _map(conn, 100 + i, vid)
    _finalize(conn)


def test_limit_plus_one_reports_more_without_exact_count(
    conn: sqlite3.Connection,
) -> None:
    _seed_n_label_codes(conn, 8, "Diagnos")
    out = search(conn, "Diagnos", field="value", type="value", limit=3)
    assert len(out.results) == 3
    assert out.has_more
    assert out.next_cursor is not None
    assert len(out.results) == 3  # the page is still limit-bounded


def test_cursor_paginates_codes(conn: sqlite3.Connection) -> None:
    _seed_n_label_codes(conn, 8, "Diagnos")
    first = search(conn, "Diagnos", field="value", type="value", limit=3)
    assert first.next_cursor is not None
    page1 = first.results
    page2 = search(
        conn,
        "Diagnos",
        field="value",
        type="value",
        limit=3,
        cursor=first.next_cursor,
    ).results
    assert len(page1) == 3
    assert len(page2) == 3, "the continuation cursor must return the next page"
    # Disjoint pages (same deterministic order across calls).
    assert {r.code for r in page1}.isdisjoint({r.code for r in page2})


def test_register_scope_returns_deep_in_scope_hit(conn: sqlite3.Connection) -> None:
    """Register scope must surface an in-scope hit even when HIGHER-ranked codes are
    all out-of-scope (regression: the arm truncated to `limit` BEFORE the register
    filter, so a deep in-scope hit was fetched, capped off, then lost → [])."""
    _seed_register(conn, 1, "rega")
    _seed_register(conn, 2, "regb")
    # 7 "Diagnos" codes owned only by regB + 1 owned by regA. They all share the
    # label token, so all 8 are label-FTS matches; regA's is not guaranteed to be
    # among the top few by rank.
    for i in range(7):
        _seed_code(conn, 200 + i, str(200 + i), f"Diagnos B{i:02d}")
        vid_b = _seed_variable(conn, 2, str(200 + i), f"BVar{i}", f"bvar{i}")
        _map(conn, 200 + i, vid_b)
    _seed_code(conn, 299, "299", "Diagnos A")
    vid_a = _seed_variable(conn, 1, "299", "AVar", "avar")
    _map(conn, 299, vid_a)
    _finalize(conn)

    # Scoped to regA with a small limit: the single regA-owned code must still come
    # back (the full in-scope set is built before the outer slice).
    scoped = search(
        conn, "Diagnos", field="value", type="value", register="rega", limit=3
    )
    assert len(scoped.results) == 1
    assert [r.label for r in scoped.results] == ["Diagnos A"]


# --------------------------------------------------------------------------- #
# #352 perf: annotate only the shown page (unscoped path).
# --------------------------------------------------------------------------- #


def test_annotate_only_the_page_unscoped(
    conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE regression guard for the perf fix: an unscoped (`reg_ids is None`)
    value query matching MANY codes must annotate AT MOST `limit` (page) code_ids,
    NOT the full match set — owner annotation is deferred to the shown page."""
    from reg_meta import queries

    _seed_n_label_codes(conn, 30, "Diagnos")

    seen_batches: list[list[int]] = []
    real_batch = queries._code_owner_annotations_batch

    def _spy(
        c: sqlite3.Connection,
        code_ids: list[int],
        reg_ids: object,
        *,
        variable_limit: int | None = queries._CODE_OWNERS_PER_HIT,
        variable_counts: object = None,
    ) -> object:
        seen_batches.append(list(code_ids))
        return real_batch(
            c,
            code_ids,
            reg_ids,  # type: ignore[arg-type]
            variable_limit=variable_limit,
            variable_counts=variable_counts,  # type: ignore[arg-type]
        )

    monkeypatch.setattr(queries, "_code_owner_annotations_batch", _spy)

    limit = 5
    out = search(conn, "Diagnos", field="value", type="value", limit=limit)

    # Every batch call (there is exactly one, post-slice) saw at most `limit` codes.
    assert seen_batches, "owner annotation must run for the shown page"
    for batch in seen_batches:
        assert len(batch) <= limit, (
            f"annotated {len(batch)} codes; the page is only {limit} — "
            "the full match set is being annotated (perf regression)"
        )
    # Sanity: more codes matched than were annotated.
    assert out.has_more
    assert len(out.results) == limit


def test_more_flag_with_deferred_annotation(
    conn: sqlite3.Connection,
) -> None:
    """Deferred annotation remains page-bounded while look-ahead reports more."""
    _seed_n_label_codes(conn, 12, "Diagnos")
    out = search(conn, "Diagnos", field="value", type="value", limit=4)
    assert len(out.results) == 4
    assert out.has_more


def test_page_rows_carry_correct_owners(conn: sqlite3.Connection) -> None:
    """Owner-annotation CONTENT of the shown rows is identical to the old
    full-annotate behaviour: a code with known owners is correctly annotated on
    the page, and the internal `_code_id` marker never leaks out."""
    _seed_register(conn, 1, "reg")
    _seed_code(conn, 1, "7", "Delad kod")
    for i in range(3):
        vid = _seed_variable(conn, 1, str(100 + i), f"Var{i}", f"var{i}")
        _map(conn, 1, vid)
    _finalize(conn)

    results = search(conn, "Delad kod", field="value", type="value").results
    hit = next(r for r in results if r.label == "Delad kod")
    assert hit.variable_count == 3
    assert len(hit.variables) == 3
    assert hit.classification_count == 0
    assert hit.classifications == ()
    # The deferred-annotation marker must be stripped before results go public.
    assert not hasattr(hit, "_code_id")


def test_cursor_page_annotated(conn: sqlite3.Connection) -> None:
    """Pagination past offset 0 annotates the RIGHT page: the second page's rows
    carry their own correct owner annotation (not the first page's)."""
    # Two distinctly-owned codes sharing the label token, deterministic order.
    _seed_register(conn, 1, "reg")
    _seed_code(conn, 1, "1", "Diagnos AA")  # 1 owner
    _seed_code(conn, 2, "2", "Diagnos BB")  # 2 owners
    v0 = _seed_variable(conn, 1, "10", "V0", "v0")
    _map(conn, 1, v0)
    v1 = _seed_variable(conn, 1, "11", "V1", "v1")
    v2 = _seed_variable(conn, 1, "12", "V2", "v2")
    _map(conn, 2, v1)
    _map(conn, 2, v2)
    _finalize(conn)

    page1 = search(conn, "Diagnos", field="value", type="value", limit=1)
    assert page1.next_cursor is not None
    page2 = search(
        conn,
        "Diagnos",
        field="value",
        type="value",
        limit=1,
        cursor=page1.next_cursor,
    )
    assert len(page1.results) == 1
    assert len(page2.results) == 1
    # Disjoint pages, each annotated with its OWN code's owner count.
    p1, p2 = page1.results[0], page2.results[0]
    assert p1.code != p2.code
    by_code = {p1.code: p1, p2.code: p2}
    assert by_code["1"].variable_count == 1
    assert by_code["2"].variable_count == 2
    assert not hasattr(p1, "_code_id") and not hasattr(p2, "_code_id")


def test_reg_scope_does_not_use_code_id_marker(conn: sqlite3.Connection) -> None:
    """The reg-scoped (`--register`) arm is byte-identical to before: it annotates
    only the bounded in-scope page and never uses a deferred `_code_id` marker,
    and its rows never carry the `_code_id` marker (annotated up front, not
    deferred)."""
    _seed_register(conn, 1, "rega")
    _seed_register(conn, 2, "regb")
    for i in range(4):
        _seed_code(conn, 200 + i, str(200 + i), f"Diagnos B{i:02d}")
        vid_b = _seed_variable(conn, 2, str(200 + i), f"BVar{i}", f"bvar{i}")
        _map(conn, 200 + i, vid_b)
    _seed_code(conn, 299, "299", "Diagnos A")
    vid_a = _seed_variable(conn, 1, "299", "AVar", "avar")
    _map(conn, 299, vid_a)
    _finalize(conn)

    scoped = search(conn, "Diagnos", field="value", type="value", register="rega")
    # Only the regA-owned code survives the reg-scope drop.
    assert len(scoped.results) == 1
    assert [r.label for r in scoped.results] == ["Diagnos A"]
    assert all(not hasattr(r, "_code_id") for r in scoped.results)
    assert scoped.results[0].variable_count == 1


def test_type_all_only_code_rows_annotated(conn: sqlite3.Connection) -> None:
    """In a mixed `type="all"` page, only `type=="code"` rows get owner annotation;
    other-type rows pass through untouched and no row leaks `_code_id`."""
    _seed_register(conn, 1, "reg")
    # A variable whose NAME matches the query (a varname/all hit), plus a code
    # whose label matches it too.
    _seed_variable(conn, 1, "10", "Cancer var", "cancer-var")
    _seed_code(conn, 1, "1", "Cancer kod")
    vid2 = _seed_variable(conn, 1, "11", "Owner", "owner")
    _map(conn, 1, vid2)
    _finalize(conn)

    out = search(conn, "Cancer", field="all", type="all", fold_groups=False)
    results = out.results
    types = {r.type for r in results}
    assert "code" in types, "the code/value row must be present"
    code_row = next(r for r in results if r.type == "code")
    assert code_row.variable_count == 1
    # No row (code or otherwise) leaks the internal marker.
    assert all(not hasattr(r, "_code_id") for r in results)


def test_code_shaped_drops_ownerless_dangling_code(conn: sqlite3.Connection) -> None:
    # #478: a code-shaped query must not return an ownerless dangling code (no
    # variable owner AND not in classification_code) via the direct value_code
    # lookup. Owned / classification-owned codes are still returned. Mirrors the
    # build-side value_code_fts owner filter for the code-shape bypass path.
    # NOTE: _finalize's FTS 'rebuild' indexes ALL value_code rows (the build-side
    # owner filter is NOT applied in this harness), so searching by CODE string
    # (not label) isolates the QUERY-side code-shape filter under test.
    _seed_register(conn, 1, "reg")
    vid = _seed_variable(conn, 1, "10", "Var", "var")
    _seed_code(conn, 1, "9001", "Owned code label")
    _map(conn, 1, vid)  # owned (mapping_count ≥ 1)
    _seed_code(conn, 2, "9002", "Ownerless dangling")  # no _map, no classification
    _seed_code(conn, 3, "9003", "Classification dangling")  # no _map, but classified
    classification_id = conn.execute(
        "INSERT INTO classification (short_name, name) VALUES ('c', 'C')"
    ).lastrowid
    conn.execute(
        "INSERT INTO classification_code (classification_id, code_id, level, is_valid) "
        "VALUES (?, 3, NULL, 1)",
        (classification_id,),
    )
    _finalize(conn)

    def _codes(query: str) -> list[str]:
        return [
            r.code for r in search(conn, query, field="value", type="value").results
        ]

    owned = _codes("9001")
    assert "9001" in owned, f"owned code must be returned, got {owned}"

    ownerless = _codes("9002")
    assert "9002" not in ownerless, (
        f"ownerless dangling code must be dropped, got {ownerless}"
    )

    classified = _codes("9003")
    assert "9003" in classified, (
        f"classification-owned code must be returned, got {classified}"
    )

    prefix = _codes("900")
    assert "9001" in prefix and "9003" in prefix, (
        f"owned/classified prefix hits must be present, got {prefix}"
    )
    assert "9002" not in prefix, (
        f"ownerless code must be dropped from prefix search too, got {prefix}"
    )


def test_cursor_pages_are_duplicate_free_and_gap_free(conn: sqlite3.Connection) -> None:
    _seed_n_label_codes(conn, 8, "Diagnos")
    expected = search(conn, "Diagnos", field="value", type="value", limit=20)

    seen: list[str] = []
    cursor = None
    while True:
        page = search(
            conn,
            "Diagnos",
            field="value",
            type="value",
            limit=3,
            cursor=cursor,
        )
        seen.extend(result.code for result in page.results)
        if not page.has_more:
            break
        assert page.next_cursor is not None
        cursor = page.next_cursor

    assert seen == [result.code for result in expected.results]
    assert len(seen) == len(set(seen))


def test_cursor_rejects_invalid_and_context_mismatched_tokens(
    conn: sqlite3.Connection,
) -> None:
    _seed_n_label_codes(conn, 4, "Diagnos")
    first = search(conn, "Diagnos", field="value", type="value", limit=1)
    assert first.next_cursor is not None

    for cursor, query, scope in (
        ("not-a-cursor", "Diagnos", "all"),
        (first.next_cursor, "Annan", "all"),
        (first.next_cursor, "Diagnos", "classification"),
    ):
        with pytest.raises(RegMetaError) as exc:
            search(
                conn,
                query,
                field="value",
                type="value",
                limit=1,
                code_owner_scope=scope,
                cursor=cursor,
            )
        assert exc.value.code == "invalid_search_cursor"
        assert "cursor" in exc.value.message.lower()


def test_cursor_binds_catalog_generation(conn: sqlite3.Connection) -> None:
    _seed_n_label_codes(conn, 4, "Diagnos")
    first = search(conn, "Diagnos", field="value", type="value", limit=1)
    assert first.next_cursor is not None
    conn.execute(
        "INSERT OR REPLACE INTO import_manifest (key, value) "
        "VALUES ('import_date', '2099-01-01')"
    )

    with pytest.raises(RegMetaError) as exc:
        search(
            conn,
            "Diagnos",
            field="value",
            type="value",
            limit=1,
            cursor=first.next_cursor,
        )
    assert "catalog generation" in exc.value.message


def test_cursor_rejects_tampered_or_oversized_position(
    conn: sqlite3.Connection,
) -> None:
    _seed_n_label_codes(conn, 4, "Diagnos")
    first = search(conn, "Diagnos", field="value", type="value", limit=1)
    assert first.next_cursor is not None
    raw = urlsafe_b64decode(first.next_cursor + "=" * (-len(first.next_cursor) % 4))
    payload = json.loads(raw)
    payload["offset"] = 1_000_000
    forged = (
        urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        )
        .decode()
        .rstrip("=")
    )

    with pytest.raises(RegMetaError) as exc:
        search(
            conn,
            "Diagnos",
            field="value",
            type="value",
            limit=1,
            cursor=forged,
        )
    assert exc.value.code == "invalid_search_cursor"
