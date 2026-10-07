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
from reg_meta.db import register_py_lower
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
    register_py_lower(c)  # `search`'s LIKE arms fold with it; `open_db` registers it
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


def test_owner_cap_keeps_tightest_value_set_owners(
    conn: sqlite3.Connection,
) -> None:
    """The cap prefers variables with fewer distinct codes, then slug order."""
    _seed_register(conn, 1, "reg")
    _seed_code(conn, 1, "TARGET", "Target label")
    owner_specs = (
        ("z-tight", 1),
        ("a-two", 2),
        ("b-three", 3),
        ("c-four", 4),
        ("d-five", 5),
        ("e-six", 6),
    )
    next_code_id = 2
    for owner_index, (slug, code_count) in enumerate(owner_specs):
        variable_id = _seed_variable(conn, 1, str(100 + owner_index), slug, slug)
        _map(conn, 1, variable_id)
        for extra_index in range(code_count - 1):
            _seed_code(
                conn,
                next_code_id,
                f"EXTRA-{owner_index}-{extra_index}",
                f"Unrelated {owner_index} {extra_index}",
            )
            _map(conn, next_code_id, variable_id)
            next_code_id += 1
    _finalize(conn)

    hit = search(conn, "Target label", field="value", type="value").results[0]

    assert hit.variable_count == 6
    assert [owner.name for owner in hit.variables] == [
        "z-tight",
        "a-two",
        "b-three",
        "c-four",
        "d-five",
    ]


def test_owner_cap_breaks_cross_register_slug_ties_deterministically(
    conn: sqlite3.Connection,
) -> None:
    _seed_register(conn, 1, "rega")
    _seed_register(conn, 2, "regb")
    _seed_code(conn, 1, "TARGET", "Target label")
    for owner_index, slug in enumerate(("a", "b", "c", "d")):
        variable_id = _seed_variable(conn, 1, str(100 + owner_index), slug, slug)
        _map(conn, 1, variable_id)
    for register_id, name in ((1, "same-a"), (2, "same-b")):
        variable_id = _seed_variable(conn, register_id, name, name, "same")
        _map(conn, 1, variable_id)
    _finalize(conn)

    hit = search(conn, "Target label", field="value", type="value").results[0]

    assert hit.variable_count == 6
    assert [owner.name for owner in hit.variables] == [
        "a",
        "b",
        "c",
        "d",
        "same-a",
    ]


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


def test_sql_limit_uses_published_mapping_penalized_rank(
    conn: sqlite3.Connection,
) -> None:
    _seed_register(conn, 1, "reg")
    for code_id, code, label, owners in (
        (1, "COMMON", "Needle", 120),
        (2, "MEDIUM", "Needle medium", 60),
        (3, "RARE", "Needle rare suffix", 1),
    ):
        _seed_code(conn, code_id, code, label)
        for owner in range(owners):
            variable_id = _seed_variable(
                conn,
                1,
                f"{code_id}-{owner}",
                f"Owner {code_id}-{owner}",
                f"owner-{code_id}-{owner}",
            )
            _map(conn, code_id, variable_id)
    _finalize(conn)

    expected = search(conn, "Needle", field="value", type="value", limit=20)
    first = search(conn, "Needle", field="value", type="value", limit=1)

    assert first.results[0].code == expected.results[0].code == "RARE"
    seen: list[str] = []
    cursor = None
    while True:
        page = search(
            conn,
            "Needle",
            field="value",
            type="value",
            limit=1,
            cursor=cursor,
        )
        seen.extend(row.code for row in page.results)
        if not page.has_more:
            assert page.next_cursor is None
            break
        assert page.next_cursor is not None
        cursor = page.next_cursor
    assert seen == [row.code for row in expected.results]
    assert len(seen) == len(set(seen))


def _seed_n_label_codes(conn: sqlite3.Connection, n: int, label: str) -> None:
    """Seed `n` distinct (code, label) pairs that all share `label` text (so one
    label-FTS query matches all n), each owned by its own variable in register 1."""
    _seed_register(conn, 1, "reg")
    for i in range(n):
        _seed_code(conn, 100 + i, str(100 + i), f"{label} {i:02d}")
        vid = _seed_variable(conn, 1, str(100 + i), f"Var{i}", f"var{i}")
        _map(conn, 100 + i, vid)
    _finalize(conn)


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
