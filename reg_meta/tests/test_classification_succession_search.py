"""Classification edition chains on ``reg_meta.queries.search`` (#571, #604).

Edition chains collapse to one row for the terminal edition; a split root is
its own terminal. Editions are hand-seeded onto the slugged fixture DB (the
shared ``_slugged_db`` factory).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import build_slugged_db
from reg_meta.queries import search
from search_test_support import rebuild_fts as _rebuild_fts

if TYPE_CHECKING:
    import sqlite3


def _add_classification(
    conn: sqlite3.Connection, *, cid: int, short_name: str, name: str, slug: str
) -> None:
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) VALUES (?, ?, ?, ?)",
        (cid, short_name, name, slug),
    )


def _add_succession_edge(
    conn: sqlite3.Connection,
    *,
    predecessor: str,
    successor: str,
    effective_year: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO classification_replaced_by "
        "(predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, 'derived:vintage_chain')",
        (predecessor, successor, effective_year),
    )


class TestClassificationSuccessionFold:
    """#571: classification EDITION chains (`classification_replaced_by`) collapse
    to ONE row for the terminal (current) edition in search, carrying the
    non-terminal editions as `editions` history. Runs BEFORE the concept-group
    fold so collapsed terminals then fold into a curated umbrella group (#516)."""

    def test_cursor_completes_succession_before_first_page(self) -> None:
        conn = build_slugged_db()
        name = "Needle classification family"
        _add_classification(
            conn, cid=70, short_name="NEEDLE-A", name=name, slug="needle-old"
        )
        _add_classification(
            conn, cid=71, short_name="NEEDLE-M", name=name, slug="needle-standalone"
        )
        _add_classification(
            conn, cid=72, short_name="NEEDLE-Z", name=name, slug="needle-current"
        )
        _add_succession_edge(conn, predecessor="needle-old", successor="needle-current")
        _rebuild_fts(conn)

        expected = search(conn, "Needle classification", field="description", limit=10)
        cursor = None
        seen = []
        while True:
            page = search(
                conn,
                "Needle classification",
                field="description",
                limit=1,
                cursor=cursor,
            )
            seen.extend(page.results)
            if not page.has_more:
                break
            assert page.next_cursor is not None
            cursor = page.next_cursor

        assert [r.type for r in seen] == [r.type for r in expected.results]
        assert len({r.model_dump_json() for r in seen}) == len(seen)
        succession = next(r for r in seen if r.type == "classification_succession")
        assert succession.matched_count == 2

    def test_future_successor_does_not_fold_until_as_of_year(self) -> None:
        conn = build_slugged_db()
        name = "Svensk sjukdomsklassifikation"
        _add_classification(
            conn, cid=81, short_name="ICD-10-SE", name=name, slug="icd-10-se"
        )
        _add_classification(
            conn, cid=82, short_name="ICD-11-SE", name=name, slug="icd-11-se"
        )
        _add_succession_edge(
            conn,
            predecessor="icd-10-se",
            successor="icd-11-se",
            effective_year=2027,
        )
        conn.commit()
        _rebuild_fts(conn)

        before = search(
            conn,
            "sjukdomsklassifikation",
            field="description",
            classification_as_of_year=2026,
        ).results
        assert [r.type for r in before] == ["classification", "classification"]
        assert {str(r.fqid) for r in before} == {
            "class/icd-10-se",
            "class/icd-11-se",
        }
        assert all(r.terminal_fqid is None for r in before)

        after = search(
            conn,
            "sjukdomsklassifikation",
            field="description",
            classification_as_of_year=2027,
        ).results
        assert [r.type for r in after] == ["classification_succession"]
        assert str(after[0].fqid) == "class/icd-11-se"
        assert [edition.slug for edition in after[0].editions] == [
            "icd-11-se",
            "icd-10-se",
        ]

    def test_collapsed_terminal_then_folds_into_umbrella_group(self) -> None:
        """The interaction: editions collapse to their terminal FIRST, then the
        terminal editions fold into a curated SUN-style umbrella group (#516)."""
        conn = build_slugged_db()  # ships sun2020 (terminal)
        name = "Svensk utbildningsnomenklatur"
        # Two succession chains feeding two terminal editions that are BOTH members
        # of the curated umbrella 'group:sun': sun1996→sun2000 and sunOld→sun2020.
        _add_classification(
            conn, cid=80, short_name="SUN1996", name=name, slug="sun1996"
        )
        _add_classification(
            conn, cid=81, short_name="SUN2000", name=name, slug="sun2000"
        )
        _add_classification(
            conn, cid=82, short_name="SUNOLD", name=name, slug="sun-old"
        )
        _add_succession_edge(
            conn, predecessor="sun1996", successor="sun2000", effective_year=2000
        )
        _add_succession_edge(
            conn, predecessor="sun-old", successor="sun2020", effective_year=2020
        )
        # Curated umbrella over the two TERMINAL editions (sun2000 + sun2020).
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (90, 'classification', NULL, 'sun', "
            "'Svensk utbildningsnomenklatur', 'token')"
        )
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (90, 'vintage', 0, 'vintage')"
        )
        sun2020_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun2020'"
        ).fetchone()[0]
        conn.executemany(
            "INSERT INTO concept_group_classification (classification_id, group_id, "
            "facet_value, facet_label) VALUES (?, 90, ?, ?)",
            [(81, "2000", "2000"), (sun2020_id, "2020", "2020")],
        )
        conn.commit()
        _rebuild_fts(conn)
        results = search(conn, "utbildningsnomenklatur", field="description").results
        # The two collapsed terminals (sun2000, sun2020) then fold into ONE
        # umbrella group row — no stray succession or leaf rows survive.
        groups = [r for r in results if r.type == "group"]
        assert len(groups) == 1
        assert groups[0].group_key == "sun"
        assert not [r for r in results if r.type == "classification_succession"]
        assert not [r for r in results if r.type == "classification"]


class TestClassificationSuccessionSplitRoot:
    """#604: a 1→many split predecessor (sun1996 → {niva,inriktning,grupp}2000) has
    NO single terminal — the chain BRANCHES. `_terminal_classification_slug` must
    stop the walk at the split (the split root is its own terminal), so a `sun1996`
    hit doesn't get folded under one arbitrary branch or annotated with a
    single-branch `terminal_fqid` ("current")."""

    @staticmethod
    def _split_conn() -> sqlite3.Connection:
        """sun1996 splits 3 ways into 2000 editions, each continuing one vintage
        step to its 2020 edition:

            sun1996 ─┬─ sun-niva2000      ── sun-niva2020
                     ├─ sun-inriktning2000 ── sun-inriktning2020
                     └─ sun-grupp2000     ── sun-grupp2020

        All editions share the FTS-searchable name so one query hits them all."""
        conn = build_slugged_db()
        name = "Svensk utbildningsnomenklatur"
        cid = 100
        for stem in ("niva", "inriktning", "grupp"):
            for vintage in ("2000", "2020"):
                slug = f"sun-{stem}{vintage}"
                _add_classification(
                    conn, cid=cid, short_name=slug.upper(), name=name, slug=slug
                )
                cid += 1
            _add_succession_edge(
                conn,
                predecessor=f"sun-{stem}2000",
                successor=f"sun-{stem}2020",
                effective_year=2020,
            )
        _add_classification(
            conn, cid=cid, short_name="SUN1996", name=name, slug="sun1996"
        )
        for stem in ("niva", "inriktning", "grupp"):
            _add_succession_edge(
                conn,
                predecessor="sun1996",
                successor=f"sun-{stem}2000",
                effective_year=2000,
            )
        conn.commit()
        _rebuild_fts(conn)
        return conn

    def test_split_root_hit_stays_leaf_without_terminal_fqid(self) -> None:
        # Only sun1996 matches (the branch editions carry a distinct name) → a lone
        # split-root hit stays a plain leaf and carries NO single-branch terminal.
        conn = build_slugged_db()
        _add_classification(
            conn,
            cid=100,
            short_name="SUN1996",
            name="Gammal utbildningsstandard",
            slug="sun1996",
        )
        for stem in ("niva", "inriktning", "grupp"):
            _add_classification(
                conn,
                cid={"niva": 101, "inriktning": 102, "grupp": 103}[stem],
                short_name=f"SUN-{stem.upper()}2000",
                name="Annan rubrik helt",
                slug=f"sun-{stem}2000",
            )
            _add_succession_edge(
                conn, predecessor="sun1996", successor=f"sun-{stem}2000"
            )
        conn.commit()
        _rebuild_fts(conn)
        results = search(conn, "gammal", field="description").results
        assert [r.type for r in results] == ["classification"]
        assert str(results[0].fqid) == "class/sun1996"
        # The fix: NO misleading "current" pointing at one arbitrary branch.
        assert results[0].terminal_fqid is None

    def test_split_root_does_not_fold_branch_hit(self) -> None:
        # sun1996 AND one branch edition (sun-niva2000) both match. They resolve to
        # DIFFERENT terminals (sun1996 itself vs sun-niva2020), so they do NOT
        # collapse into one succession row — sun1996's branch is not "current".
        conn = self._split_conn()
        results = search(conn, "utbildningsnomenklatur", field="description").results
        # sun1996 is its own terminal with no sibling in that bucket → leaf.
        leaves = [
            r
            for r in results
            if getattr(r, "fqid", None) and str(r.fqid) == "class/sun1996"
        ]
        assert len(leaves) == 1
        assert leaves[0].type == "classification"
        assert leaves[0].terminal_fqid is None

    def test_hits_on_two_branches_stay_separate(self) -> None:
        # Hits on two DIFFERENT branches (a niva edition vs a grupp edition) resolve
        # to different terminals → they stay separate rows, never co-fold.
        conn = build_slugged_db()
        _add_classification(
            conn,
            cid=101,
            short_name="SUN-NIVA2020",
            name="Utbildning niva rubrik",
            slug="sun-niva2020",
        )
        _add_classification(
            conn,
            cid=103,
            short_name="SUN-GRUPP2020",
            name="Utbildning niva rubrik",
            slug="sun-grupp2020",
        )
        _add_classification(
            conn,
            cid=104,
            short_name="SUN1996",
            name="Utbildning niva rubrik",
            slug="sun1996",
        )
        _add_succession_edge(conn, predecessor="sun1996", successor="sun-niva2020")
        _add_succession_edge(conn, predecessor="sun1996", successor="sun-grupp2020")
        conn.commit()
        _rebuild_fts(conn)
        results = search(conn, "niva", field="description").results
        # No co-fold: each terminal (sun-niva2020, sun-grupp2020) is lone in its
        # bucket, and sun1996 (the split root) is lone in its own. All three stay
        # leaves — none is a succession row.
        assert not [r for r in results if r.type == "classification_succession"]
        fqids = {str(r.fqid) for r in results if r.type == "classification"}
        assert fqids == {"class/sun-niva2020", "class/sun-grupp2020", "class/sun1996"}
