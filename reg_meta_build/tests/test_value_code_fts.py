"""End-to-end coverage for #352's code/value search additions:

- `value_code_fts` is populated from value_code labels at build time;
- stoplisted junk labels (exact + sentinel-prefix families) are EXCLUDED from the
  index but still PRESENT in the leaf `value_code` table (search-only hiding);
- `value_code.mapping_count` is the precomputed per-(code,label) variable count
  from `code_variable_map`.

Fully synthetic (CLAUDE.md): augments the standard SCB fixture with one variable
whose value set mixes stoplisted and ordinary labels, runs a real `build_db`, and
inspects the built DB. `_no_repo_curation` (autouse in this package's conftest)
keeps the repo TOMLs out of the synthetic build.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import _var_row
from _shared_fixtures import build_with_rows, vm_rows
from reg_meta_build.db import DDL, _populate_fts

if TYPE_CHECKING:
    from pathlib import Path

# A value set mixing labels that must be HIDDEN from search with ordinary,
# searchable labels. Distinct version + codes keep kod != version so the importer's
# drift guard stays quiet.
#   searchable: "Förvärvsarbetande" / "Studerande"; "Småort" (a real legit label
#     that does NOT start with a sentinel stem — proves prefixes don't over-reach
#     into normal concepts, the issue's "don't hide real concepts" guard).
#   exact stoplist: "Nej".
#   sentinel-prefix families (`Okänt%` / `Okänd%` / `Felaktig%`): "Okänt värde",
#     bare "Okänd", and "Felaktigt värde" — the last proves the prefix catches the
#     INFLECTED form ("Felaktigt" ≠ "Felaktig", so only a stem-prefix excludes it).
_MIXED_CODES = [
    ("10", "Förvärvsarbetande"),
    ("20", "Studerande"),
    ("21", "Småort"),
    ("30", "Nej"),
    ("97", "Okänd"),
    ("98", "Felaktigt värde"),
    ("99", "Okänt värde"),
]


def _build(tmp_path: Path):
    ri = [
        _var_row(
            colname="SyssStat",
            cvid=7700,
            var_id=770,
            varname="SyssStatVar",
            year="2020",
            regver_id=770,
            data_length="2",
        )
    ]
    vm = vm_rows(7700, "Sysselsattningsstatus", _MIXED_CODES)
    return build_with_rows(tmp_path, ri, vm)


def _indexed(conn, label: str) -> bool:
    """Whether `label` is actually in the FTS INDEX.

    A `SELECT label FROM value_code_fts` reads back from the CONTENT table
    (external-content FTS5 → value_code), so it lists every label whether indexed
    or not. The only honest probe is a MATCH for the label's exact text and
    confirming the matched rowid is this label's code_id."""
    rows = conn.execute(
        "SELECT vc.label FROM value_code_fts "
        "JOIN value_code vc ON vc.code_id = value_code_fts.rowid "
        "WHERE value_code_fts MATCH ?",
        (f'"{label}"',),
    ).fetchall()
    return any(r[0] == label for r in rows)


def _vc_labels(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT label FROM value_code")}


def test_searchable_labels_indexed(tmp_path: Path) -> None:
    conn = _build(tmp_path)
    try:
        assert _indexed(conn, "Förvärvsarbetande")
        assert _indexed(conn, "Studerande")
        # A legit concept label that does NOT start with a sentinel stem stays
        # indexed — the prefix families don't reach into normal labels (the issue's
        # "don't hide real concepts" guard; Småort is a real, queried label).
        assert _indexed(conn, "Småort")
    finally:
        conn.close()


def test_sentinel_prefix_families_excluded(tmp_path: Path) -> None:
    """#352: the `Okänt%` / `Okänd%` / `Felaktig%` prefix families (the issue spec's
    `Okänt*` / `Okänd*` / `Felaktig*`) are excluded from the FTS index — covering
    the bare sentinel ("Okänd") AND the space-separated ("Okänt värde") AND the
    INFLECTED form ("Felaktigt värde", caught only because the stoplist is a stem
    PREFIX, not an exact/word-boundary match). All stay in the leaf value_code."""
    conn = _build(tmp_path)
    try:
        vc = _vc_labels(conn)
        for hidden in ("Okänd", "Okänt värde", "Felaktigt värde"):
            assert hidden in vc, f"{hidden!r} must remain in value_code"
            assert not _indexed(conn, hidden), (
                f"{hidden!r} must be excluded by its sentinel-prefix family"
            )
    finally:
        conn.close()


def test_stoplisted_labels_hidden_from_index_but_kept_in_leaf(tmp_path: Path) -> None:
    conn = _build(tmp_path)
    try:
        vc = _vc_labels(conn)
        # Exact-stoplist label and sentinel-prefix-family label: in the leaf
        # value_code table (data preserved) but NOT in the search index.
        for hidden in ("Nej", "Okänt värde"):
            assert hidden in vc, f"{hidden!r} must remain in value_code"
            assert not _indexed(conn, hidden), (
                f"{hidden!r} must be excluded from the index"
            )
    finally:
        conn.close()


def test_mapping_count_computed(tmp_path: Path) -> None:
    conn = _build(tmp_path)
    try:
        # An ordinary code mapped to >= 1 variable carries a positive count.
        row = conn.execute(
            "SELECT mapping_count FROM value_code WHERE code = '10' AND label = ?",
            ("Förvärvsarbetande",),
        ).fetchone()
        assert row is not None
        assert row[0] >= 1

        # mapping_count equals the actual code_variable_map fan-out for that code.
        code_id, mc = conn.execute(
            "SELECT code_id, mapping_count FROM value_code "
            "WHERE code = '10' AND label = ?",
            ("Förvärvsarbetande",),
        ).fetchone()
        actual = conn.execute(
            "SELECT COUNT(*) FROM code_variable_map WHERE code_id = ?", (code_id,)
        ).fetchone()[0]
        assert mc == actual

        # No negative counts anywhere; an unmapped code keeps the DEFAULT 0.
        n_neg = conn.execute(
            "SELECT COUNT(*) FROM value_code WHERE mapping_count < 0"
        ).fetchone()[0]
        assert n_neg == 0
    finally:
        conn.close()


def test_owner_filter_drops_ownerless_dangling_codes() -> None:
    """#478: value_code_fts indexes a code ONLY if it has an owner — a variable
    (mapping_count > 0, the code_variable_map count) OR a classification (a
    classification_code row). This mirrors the query-side owner definition in
    reg_meta/queries.py `_code_owner_annotations_batch` (variables ∪
    classifications); the register-scoped value search already drops ownerless
    codes (queries.py:944), but the unscoped path defers owner annotation to the
    shown page and so leaked the ~2,562 ownerless year-projection orphans as
    context-less hits. Mirroring the filter at index-build time removes them
    while keeping classification-owned dangling codes (no value_set_member after
    the year-projection, findable ONLY via value_code_fts) searchable.

    Reproducing a real year-projection orphan via build_with_rows is impractical,
    so this drives `_populate_fts` directly on a minimal DDL-built DB:
      1. reachable code (mapping_count=2)            → indexed;
      2. ownerless dangling code (mapping_count=0,
         no classification_code)                     → NOT indexed, leaf kept;
      3. classification-owned dangling code
         (mapping_count=0, has classification_code)  → STILL indexed.
    """
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(DDL)
        conn.executemany(
            "INSERT INTO value_code (code_id, code, label, mapping_count) "
            "VALUES (?, ?, ?, ?)",
            [
                (1, "10", "Reachable label", 2),
                (2, "20", "Ownerless dangling", 0),
                (3, "30", "Classification dangling", 0),
            ],
        )
        conn.execute(
            "INSERT INTO classification (short_name, name) VALUES (?, ?)",
            ("test-class", "Test classification"),
        )
        class_id = conn.execute(
            "SELECT id FROM classification WHERE short_name = ?", ("test-class",)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO classification_code "
            "(classification_id, code_id, level, is_valid) VALUES (?, ?, ?, ?)",
            (class_id, 3, None, 1),
        )

        _populate_fts(conn, include_value_code=True)

        assert _indexed(conn, "Reachable label")
        assert _indexed(conn, "Classification dangling")
        assert not _indexed(conn, "Ownerless dangling")
        # The ownerless code is hidden from search but kept in the leaf table.
        assert "Ownerless dangling" in _vc_labels(conn)
    finally:
        conn.close()


def test_docsize_reflects_index_not_content(tmp_path: Path) -> None:
    """The FTS5 docsize shadow table is the honest indexed-row count (COUNT(*) on
    an external-content FTS5 table reads the CONTENT table, so it can't see the
    stoplist exclusion). docsize must be < value_code here (two labels stoplisted)."""
    conn = _build(tmp_path)
    try:
        n_vc = conn.execute("SELECT COUNT(*) FROM value_code").fetchone()[0]
        n_idx = conn.execute("SELECT COUNT(*) FROM value_code_fts_docsize").fetchone()[
            0
        ]
        assert 0 < n_idx < n_vc
    finally:
        conn.close()
