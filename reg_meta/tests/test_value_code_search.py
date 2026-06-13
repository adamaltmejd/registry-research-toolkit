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

import sqlite3
from typing import TYPE_CHECKING

import pytest
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

    results = search(conn, "0180", field="value", type="value")["results"]
    by_code = {r["code"]: r for r in results}
    assert "0180" in by_code, "code-exact hit must be present"
    assert "9999" in by_code, "label-text hit must be present"
    # Smaller fts_rank sorts first; the code-exact hit is seeded below the FTS floor.
    assert by_code["0180"]["fts_rank"] < by_code["9999"]["fts_rank"]
    # And it is actually first in the returned (pre-sorted) order.
    assert results[0]["code"] == "0180"


def test_dedup_code_id_appears_once(conn: sqlite3.Connection) -> None:
    """A code-shaped query whose text matches BOTH a label-FTS hit and the
    code-shape path on the SAME pair returns that code_id ONCE."""
    _seed_register(conn, 1, "reg")
    vid = _seed_variable(conn, 1, "10", "Var", "var")
    # The pair's CODE is "0180" and its LABEL also contains "0180" → both paths fire.
    _seed_code(conn, 1, "0180", "Label 0180 here")
    _map(conn, 1, vid)
    _finalize(conn)

    results = search(conn, "0180", field="value", type="value")["results"]
    codes = [r["code"] for r in results]
    assert codes.count("0180") == 1, f"expected one dedup'd hit, got {codes}"


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
    scoped = search(conn, "Singelkod", field="value", type="value", register="rega")[
        "results"
    ]
    assert scoped == [], f"out-of-scope-only code should be dropped, got {scoped}"
    # Scoped to register 2 (regb) → the owner survives → present.
    in_scope = search(conn, "Singelkod", field="value", type="value", register="regb")[
        "results"
    ]
    assert any(r["label"] == "Singelkod" for r in in_scope)


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

    results = search(conn, "Delad kod", field="value", type="value")["results"]
    hit = next(r for r in results if r["label"] == "Delad kod")
    assert hit["variable_count"] == 6
    assert len(hit["variables"]) == _CODE_OWNERS_PER_HIT == 5


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

    results = search(conn, "Sjukdom vanlig", field="value", type="value")["results"]
    rank = {r["code"]: r["fts_rank"] for r in results}
    assert "1" in rank and "2" in rank
    # Rarer pair (code "2", mapping_count 1) sorts before the common one (code "1").
    assert rank["2"] < rank["1"]
    assert results[0]["code"] == "2"


def _seed_n_label_codes(conn: sqlite3.Connection, n: int, label: str) -> None:
    """Seed `n` distinct (code, label) pairs that all share `label` text (so one
    label-FTS query matches all n), each owned by its own variable in register 1."""
    _seed_register(conn, 1, "reg")
    for i in range(n):
        _seed_code(conn, 100 + i, str(100 + i), f"{label} {i:02d}")
        vid = _seed_variable(conn, 1, str(100 + i), f"Var{i}", f"var{i}")
        _map(conn, 100 + i, vid)
    _finalize(conn)


def test_total_count_reflects_true_match_count(conn: sqlite3.Connection) -> None:
    """total_count is the FULL match count, not saturated at `limit` (regression:
    the value arm used to truncate internally to `limit`, capping total_count)."""
    _seed_n_label_codes(conn, 8, "Diagnos")
    out = search(conn, "Diagnos", field="value", type="value", limit=3)
    assert out["total_count"] == 8, out["total_count"]
    assert len(out["results"]) == 3  # the page is still limit-bounded


def test_offset_paginates_codes(conn: sqlite3.Connection) -> None:
    """offset paginates the value arm (regression: offset>0 used to return [])."""
    _seed_n_label_codes(conn, 8, "Diagnos")
    page1 = search(conn, "Diagnos", field="value", type="value", limit=3, offset=0)[
        "results"
    ]
    page2 = search(conn, "Diagnos", field="value", type="value", limit=3, offset=3)[
        "results"
    ]
    assert len(page1) == 3
    assert len(page2) == 3, "offset=limit must return the NEXT page, not []"
    # Disjoint pages (same deterministic order across calls).
    assert {r["code"] for r in page1}.isdisjoint({r["code"] for r in page2})


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
    assert scoped["total_count"] == 1, scoped["total_count"]
    assert [r["label"] for r in scoped["results"]] == ["Diagnos A"]


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

    def _spy(c: sqlite3.Connection, code_ids: list[int], reg_ids: object) -> object:
        seen_batches.append(list(code_ids))
        return real_batch(c, code_ids, reg_ids)  # type: ignore[arg-type]

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
    assert out["total_count"] == 30
    assert len(out["results"]) == limit


def test_total_count_unchanged_with_deferred_annotation(
    conn: sqlite3.Connection,
) -> None:
    """total_count stays the FULL match count under deferred annotation, and the
    page is limit-bounded (the unscoped arm returns every ranked row, just
    unannotated, so len(all_results) is identical to the old full-annotate path)."""
    _seed_n_label_codes(conn, 12, "Diagnos")
    out = search(conn, "Diagnos", field="value", type="value", limit=4)
    assert out["total_count"] == 12
    assert len(out["results"]) == 4


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

    results = search(conn, "Delad kod", field="value", type="value")["results"]
    hit = next(r for r in results if r["label"] == "Delad kod")
    assert hit["variable_count"] == 3
    assert len(hit["variables"]) == 3
    assert hit["classification_count"] == 0
    assert hit["classifications"] == []
    # The deferred-annotation marker must be stripped before results go public.
    assert "_code_id" not in hit


def test_offset_page_annotated(conn: sqlite3.Connection) -> None:
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

    page1 = search(conn, "Diagnos", field="value", type="value", limit=1, offset=0)
    page2 = search(conn, "Diagnos", field="value", type="value", limit=1, offset=1)
    assert len(page1["results"]) == 1
    assert len(page2["results"]) == 1
    # Disjoint pages, each annotated with its OWN code's owner count.
    p1, p2 = page1["results"][0], page2["results"][0]
    assert p1["code"] != p2["code"]
    by_code = {p1["code"]: p1, p2["code"]: p2}
    assert by_code["1"]["variable_count"] == 1
    assert by_code["2"]["variable_count"] == 2
    assert "_code_id" not in p1 and "_code_id" not in p2


def test_reg_scope_does_not_use_code_id_marker(conn: sqlite3.Connection) -> None:
    """The reg-scoped (`--register`) arm is byte-identical to before: it annotates
    the full set + filters out-of-scope codes + reports the filtered total_count,
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
    assert scoped["total_count"] == 1
    assert [r["label"] for r in scoped["results"]] == ["Diagnos A"]
    assert all("_code_id" not in r for r in scoped["results"])
    assert scoped["results"][0]["variable_count"] == 1


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
    results = out["results"]
    types = {r["type"] for r in results}
    assert "code" in types, "the code/value row must be present"
    code_row = next(r for r in results if r["type"] == "code")
    assert code_row["variable_count"] == 1
    # No row (code or otherwise) leaks the internal marker.
    assert all("_code_id" not in r for r in results)
