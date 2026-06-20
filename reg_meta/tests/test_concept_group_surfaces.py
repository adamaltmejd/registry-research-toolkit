"""Concept groups on the search/CLI surfaces (#322 / #325).

Read-only result-shaping over the 5.3.0 group tables: `search` folds sibling
hits into a group row (and matches group labels), `get groups` lists a
register's families with member facets, and `get schema` annotates member
columns inline. The derivation passes are reg_meta_build territory — groups
here are hand-seeded onto the slugged fixture DB, mirroring
test_catalog_listing's read-surface approach.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _shared_fixtures import _build_stub_doc_db
from _slugged_db import add_state, add_variable, build_slugged_db
from reg_meta.db import SCHEMA_VERSION
from reg_meta.errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from reg_meta.queries import (
    get_classification_concept_groups,
    get_concept_groups,
    get_schema,
    search,
)

if TYPE_CHECKING:
    from pathlib import Path

# (slug, month value, month label) for the curated month family. The labels
# double as searchable variable names ("Lönesumma <month>").
_MONTH_MEMBERS = [
    ("agiinkjan", "01", "januari"),
    ("agiinkfeb", "02", "februari"),
    ("agiinkmar", "03", "mars"),
]


def _seeded_conn() -> sqlite3.Connection:
    """scb/lisa (slugged fixture: variable `kon` under variant 10) plus a
    curated month group, and a classification vintage group over sun2000 +
    sun2020. Group label 'Lönesumma per månad' deliberately shares no token
    with the member names' 'Lönesumma <month>' LIKE matches only via the
    'Lönesumma' stem — tests pick query terms to isolate each match path."""
    conn = build_slugged_db()
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source, facet_axis) VALUES (10, 'variable', 1, 'agiink', "
        "'Lönesumma per månad', 'curated', 'month')"
    )
    for i, (slug, month, month_label) in enumerate(_MONTH_MEMBERS):
        add_variable(
            conn,
            register_id=1,
            var_id=800 + i,
            name=f"Lönesumma {month_label}",
            slug=slug,
        )
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
            (slug,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group_variable "
            "(variable_id, group_id, facet_value, facet_label) VALUES (?, 10, ?, ?)",
            (vid, month, month_label),
        )
    # One member delivers under variant 10 so `get schema` has a grouped column.
    add_state(
        conn,
        register_id=1,
        variable_slug="agiinkjan",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="AgiInkJan",
    )
    # Classification vintage group (catalog-scoped, register_id NULL).
    conn.execute(
        "INSERT INTO classification (id, short_name, name, slug) "
        "VALUES (50, 'SUN2000', 'Svensk utbildningsnomenklatur', 'sun2000')"
    )
    conn.execute(
        "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
        "label, source, facet_axis) VALUES (12, 'classification', NULL, 'sun', "
        "'Svensk utbildningsnomenklatur', 'token', 'vintage')"
    )
    sun2020_id = conn.execute(
        "SELECT id FROM classification WHERE slug = 'sun2020'"
    ).fetchone()[0]
    conn.executemany(
        "INSERT INTO concept_group_classification (classification_id, group_id, "
        "facet_value, facet_label) VALUES (?, 12, ?, ?)",
        [(50, "2000", "2000"), (sun2020_id, "2020", "2020")],
    )
    conn.commit()
    return conn


def _assert_no_internal_keys(results: list[dict]) -> None:
    # Recurses: `matched` can nest two deep (a classification_succession row under
    # a group row's `matched`), so a shallow check would miss the leak (#571).
    for r in results:
        assert "_variable_id" not in r
        assert "_classification_id" not in r
        _assert_no_internal_keys(r.get("matched", []))


def _rebuild_fts(conn: sqlite3.Connection) -> None:
    """Repopulate the external-content FTS5 indexes from their content tables
    (mirrors test_search_classifications) so the classification-leaf search arm
    (`_search_classifications`) returns hits on this in-memory fixture."""
    for index in ("register_fts", "variable_fts", "classification_fts"):
        conn.execute(f"INSERT INTO {index}({index}) VALUES('rebuild')")


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


class TestSearchFolding:
    def test_sibling_hits_fold_into_one_group_row(self) -> None:
        conn = _seeded_conn()
        # 'Lönesumma' hits all three member names AND the group label.
        results = search(conn, "Lönesumma")["results"]
        assert [r["type"] for r in results] == ["group"]
        (group,) = results
        assert group["kind"] == "variable"
        assert group["group_key"] == "agiink"
        assert group["group_label"] == "Lönesumma per månad"
        assert group["group_source"] == "curated"
        assert group["register_id"] == 1
        assert group["register_name"] == "LISA"
        assert group["axes"] == ["month"]
        assert group["member_count"] == 3
        # Members come facet-ordered via the Catalog reuse, with leaf FQIDs.
        assert [m["fqid"] for m in group["members"]] == [
            "scb/lisa/agiinkjan",
            "scb/lisa/agiinkfeb",
            "scb/lisa/agiinkmar",
        ]
        # The original leaf hits ride under `matched` (one varname hit each).
        assert {m["variable_name"] for m in group["matched"]} == {
            "Lönesumma januari",
            "Lönesumma februari",
            "Lönesumma mars",
        }
        _assert_no_internal_keys(results)

    def test_fold_counts_one_result_for_pagination(self) -> None:
        conn = _seeded_conn()
        data = search(conn, "Lönesumma")
        assert data["total_count"] == 1

    def test_lone_member_hit_stays_leaf_with_annotation(self) -> None:
        conn = _seeded_conn()
        # 'januari' hits only one member (and not the group label/key).
        results = search(conn, "januari")["results"]
        assert [r["type"] for r in results] == ["varname"]
        (leaf,) = results
        assert leaf["variable_name"] == "Lönesumma januari"
        assert leaf["concept_group"] == "agiink"
        assert leaf["concept_group_label"] == "Lönesumma per månad"
        _assert_no_internal_keys(results)

    def test_group_label_matches_without_leaf_hits(self) -> None:
        conn = _seeded_conn()
        # 'per månad' appears only in the group LABEL, not in any member name.
        results = search(conn, "per månad")["results"]
        assert [r["type"] for r in results] == ["group"]
        (group,) = results
        assert group["label_matched"] is True
        assert group["matched"] == []
        assert group["member_count"] == 3

    def test_group_label_like_metacharacters_match_literally(self) -> None:
        conn = _seeded_conn()
        groups = (
            (20, 900, "literal-underscore", "Family 12_5"),
            (21, 901, "plain-digits", "Family 120"),
            (22, 902, "literal-percent", "Family 99%5"),
            (23, 903, "plain-percent-candidate", "Family 994"),
        )
        for group_id, var_id, group_key, label in groups:
            conn.execute(
                "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
                "label, source) VALUES (?, 'variable', 1, ?, ?, 'curated')",
                (group_id, group_key, label),
            )
            slug = f"{group_key}-member"
            add_variable(conn, register_id=1, var_id=var_id, name=label, slug=slug)
            variable_id = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
                (slug,),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, ?)",
                (variable_id, group_id),
            )

        underscore = search(conn, "12_", field="description")["results"]
        assert {r["group_key"] for r in underscore if r["type"] == "group"} == {
            "literal-underscore"
        }

        percent = search(conn, "99%", field="description")["results"]
        assert {r["group_key"] for r in percent if r["type"] == "group"} == {
            "literal-percent"
        }

    def test_classification_group_label_matches(self) -> None:
        conn = _seeded_conn()
        results = search(conn, "utbildningsnomenklatur")["results"]
        groups = [r for r in results if r["type"] == "group"]
        assert len(groups) == 1
        (group,) = groups
        assert group["kind"] == "classification"
        assert group["group_key"] == "sun"
        assert group["register_id"] is None
        assert [m["fqid"] for m in group["members"]] == [
            "class/sun2000",
            "class/sun2020",
        ]
        assert [m["facets"][0]["value"] for m in group["members"]] == ["2000", "2020"]

    def test_no_fold_returns_flat_member_rows(self) -> None:
        conn = _seeded_conn()
        results = search(conn, "Lönesumma", fold_groups=False)["results"]
        assert {r["variable_name"] for r in results} == {
            "Lönesumma januari",
            "Lönesumma februari",
            "Lönesumma mars",
        }
        assert all(r["type"] == "varname" for r in results)
        assert all("concept_group" not in r for r in results)
        _assert_no_internal_keys(results)

    def test_type_register_excludes_groups(self) -> None:
        conn = _seeded_conn()
        assert search(conn, "Lönesumma", type="register")["results"] == []

    def test_register_scope_keeps_variable_group_drops_classification(self) -> None:
        conn = _seeded_conn()
        scoped = search(conn, "Lönesumma", register="LISA")["results"]
        assert [r["type"] for r in scoped] == ["group"]
        assert scoped[0]["kind"] == "variable"
        # Classification groups are catalog-scoped → excluded under --register.
        assert search(conn, "utbildningsnomenklatur", register="LISA")["results"] == []

    def test_type_variable_keeps_variable_group_drops_classification(self) -> None:
        conn = _seeded_conn()
        kept = search(conn, "Lönesumma", type="variable")["results"]
        assert [r["type"] for r in kept] == ["group"]
        assert search(conn, "utbildningsnomenklatur", type="variable")["results"] == []

    def test_label_only_match_respects_years_filter(self) -> None:
        # Codex P2 on #331: --years must apply to the label-only path through
        # the group's MEMBER states (the fixture seeds one member state
        # 2018→open-ended; the other members have no states at all).
        conn = _seeded_conn()
        # Out of range: no member state overlaps 1900 → the group is dropped.
        assert search(conn, "per månad", years="1900")["results"] == []
        # In range (open-ended window covers 2020) → the group survives.
        kept = search(conn, "per månad", years="2020")["results"]
        assert [r["type"] for r in kept] == ["group"]
        assert kept[0]["label_matched"] is True


class TestClassificationSuccessionFold:
    """#571: classification EDITION chains (`classification_replaced_by`) collapse
    to ONE row for the terminal (current) edition in search, carrying the
    non-terminal editions as `editions` history. Runs BEFORE the concept-group
    fold so collapsed terminals then fold into a curated umbrella group (#516)."""

    @staticmethod
    def _chain_conn() -> sqlite3.Connection:
        """ssyk1996 → ssyk2001 → ssyk2012, all sharing the FTS-searchable name
        'Standard för svensk yrkesklassificering' so one query hits all three
        editions. No umbrella group — isolates the succession fold."""
        conn = build_slugged_db()
        name = "Standard för svensk yrkesklassificering"
        for cid, slug in ((70, "ssyk1996"), (71, "ssyk2001"), (72, "ssyk2012")):
            _add_classification(
                conn, cid=cid, short_name=slug.upper(), name=name, slug=slug
            )
        _add_succession_edge(
            conn, predecessor="ssyk1996", successor="ssyk2001", effective_year=2001
        )
        _add_succession_edge(
            conn, predecessor="ssyk2001", successor="ssyk2012", effective_year=2012
        )
        conn.commit()
        _rebuild_fts(conn)
        return conn

    def test_chain_collapses_to_terminal_row(self) -> None:
        conn = self._chain_conn()
        results = search(conn, "yrkesklassificering", field="description")["results"]
        succ = [r for r in results if r["type"] == "classification_succession"]
        assert len(succ) == 1
        (row,) = succ
        # The collapsed row IS the terminal (current) edition.
        assert row["fqid"] == "class/ssyk2012"
        assert row["short_name"] == "SSYK2012"
        # All three editions ride under `editions`, terminal-first then descending
        # effective_year.
        assert [e["slug"] for e in row["editions"]] == [
            "ssyk2012",
            "ssyk2001",
            "ssyk1996",
        ]
        assert [e["effective_year"] for e in row["editions"]] == [None, 2012, 2001]
        # The original leaf hits ride under `matched`; no separate edition leaves.
        assert {m["fqid"] for m in row["matched"]} == {
            "class/ssyk1996",
            "class/ssyk2001",
            "class/ssyk2012",
        }
        assert not [r for r in results if r["type"] == "classification"]
        _assert_no_internal_keys(results)

    def test_chain_counts_one_result_for_pagination(self) -> None:
        conn = self._chain_conn()
        data = search(conn, "yrkesklassificering", field="description")
        assert data["total_count"] == 1

    def test_lone_terminal_hit_stays_leaf(self) -> None:
        # Only the TERMINAL edition matches (rename the predecessors out of the
        # FTS name) → a lone terminal hit is an ordinary leaf, not a succession
        # row (no predecessor present to collapse).
        conn = build_slugged_db()
        _add_classification(
            conn, cid=72, short_name="SSYK2012", name="Yrken aktuell", slug="ssyk2012"
        )
        _add_classification(
            conn, cid=71, short_name="SSYK2001", name="Andra namn helt", slug="ssyk2001"
        )
        _add_succession_edge(conn, predecessor="ssyk2001", successor="ssyk2012")
        conn.commit()
        _rebuild_fts(conn)
        results = search(conn, "aktuell", field="description")["results"]
        assert [r["type"] for r in results] == ["classification"]
        assert results[0]["fqid"] == "class/ssyk2012"
        # No succession row, and the terminal itself carries no terminal_fqid (it
        # IS the terminal).
        assert "terminal_fqid" not in results[0]

    def test_lone_old_edition_hit_annotated_with_terminal(self) -> None:
        # Only an OLD (non-terminal) edition matches → stays a leaf, annotated
        # with its terminal so the webapp can link "current".
        conn = build_slugged_db()
        _add_classification(
            conn, cid=72, short_name="SSYK2012", name="Annat helt namn", slug="ssyk2012"
        )
        _add_classification(
            conn,
            cid=71,
            short_name="SSYK2001",
            name="Gammal yrkesstandard",
            slug="ssyk2001",
        )
        _add_succession_edge(conn, predecessor="ssyk2001", successor="ssyk2012")
        conn.commit()
        _rebuild_fts(conn)
        results = search(conn, "gammal", field="description")["results"]
        assert [r["type"] for r in results] == ["classification"]
        assert results[0]["fqid"] == "class/ssyk2001"
        assert results[0]["terminal_fqid"] == "class/ssyk2012"

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
            "label, source, facet_axis) VALUES (90, 'classification', NULL, 'sun', "
            "'Svensk utbildningsnomenklatur', 'token', 'vintage')"
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
        results = search(conn, "utbildningsnomenklatur", field="description")["results"]
        # The two collapsed terminals (sun2000, sun2020) then fold into ONE
        # umbrella group row — no stray succession or leaf rows survive.
        groups = [r for r in results if r["type"] == "group"]
        assert len(groups) == 1
        assert groups[0]["group_key"] == "sun"
        assert not [r for r in results if r["type"] == "classification_succession"]
        assert not [r for r in results if r["type"] == "classification"]
        _assert_no_internal_keys(results)


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

    def test_split_root_is_its_own_terminal(self) -> None:
        from reg_meta.queries import _terminal_classification_slug

        conn = self._split_conn()
        # sun1996 has 3 successors → it is its own terminal (no single current).
        assert _terminal_classification_slug(conn, "sun1996") == "sun1996"
        # Each linear branch still collapses to its real terminal.
        assert _terminal_classification_slug(conn, "sun-niva2000") == "sun-niva2020"
        assert _terminal_classification_slug(conn, "sun-grupp2000") == "sun-grupp2020"

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
        results = search(conn, "gammal", field="description")["results"]
        assert [r["type"] for r in results] == ["classification"]
        assert results[0]["fqid"] == "class/sun1996"
        # The fix: NO misleading "current" pointing at one arbitrary branch.
        assert "terminal_fqid" not in results[0]

    def test_split_root_does_not_fold_branch_hit(self) -> None:
        # sun1996 AND one branch edition (sun-niva2000) both match. They resolve to
        # DIFFERENT terminals (sun1996 itself vs sun-niva2020), so they do NOT
        # collapse into one succession row — sun1996's branch is not "current".
        conn = self._split_conn()
        results = search(conn, "utbildningsnomenklatur", field="description")["results"]
        # sun1996 is its own terminal with no sibling in that bucket → leaf.
        leaves = [r for r in results if r.get("fqid") == "class/sun1996"]
        assert len(leaves) == 1
        assert leaves[0]["type"] == "classification"
        assert "terminal_fqid" not in leaves[0]

    def test_same_branch_linear_pair_still_folds(self) -> None:
        # A linear vintage pair WITHIN one branch (sun-niva2000 → sun-niva2020)
        # still collapses to its terminal — the split-stop only fires at the root.
        conn = build_slugged_db()
        name = "Svensk utbildningsnomenklatur niva"
        _add_classification(
            conn, cid=101, short_name="SUN-NIVA2000", name=name, slug="sun-niva2000"
        )
        _add_classification(
            conn, cid=102, short_name="SUN-NIVA2020", name=name, slug="sun-niva2020"
        )
        _add_succession_edge(
            conn,
            predecessor="sun-niva2000",
            successor="sun-niva2020",
            effective_year=2020,
        )
        conn.commit()
        _rebuild_fts(conn)
        results = search(conn, "niva", field="description")["results"]
        succ = [r for r in results if r["type"] == "classification_succession"]
        assert len(succ) == 1
        assert succ[0]["fqid"] == "class/sun-niva2020"
        assert {e["slug"] for e in succ[0]["editions"]} == {
            "sun-niva2000",
            "sun-niva2020",
        }
        assert not [r for r in results if r["type"] == "classification"]

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
        results = search(conn, "niva", field="description")["results"]
        # No co-fold: each terminal (sun-niva2020, sun-grupp2020) is lone in its
        # bucket, and sun1996 (the split root) is lone in its own. All three stay
        # leaves — none is a succession row.
        assert not [r for r in results if r["type"] == "classification_succession"]
        fqids = {r["fqid"] for r in results if r["type"] == "classification"}
        assert fqids == {"class/sun-niva2020", "class/sun-grupp2020", "class/sun1996"}


class TestGetConceptGroups:
    def test_lists_register_groups_with_members_and_facets(self) -> None:
        conn = _seeded_conn()
        data = get_concept_groups(conn, "LISA")
        (reg,) = data["registers"]
        assert reg["register_id"] == 1
        assert reg["register_name"] == "LISA"
        assert reg["fqid"] == "scb/lisa"
        (group,) = reg["groups"]
        assert group["key"] == "agiink"
        assert group["source"] == "curated"
        assert group["axes"] == ["month"]
        assert group["member_count"] == 3
        jan = group["members"][0]
        assert jan["fqid"] == "scb/lisa/agiinkjan"
        assert jan["name"] == "Lönesumma januari"
        assert jan["facets"] == [{"axis": "month", "value": "01", "label": "januari"}]

    def test_resolves_register_by_numeric_id(self) -> None:
        conn = _seeded_conn()
        data = get_concept_groups(conn, "1")
        assert data["registers"][0]["groups"][0]["key"] == "agiink"

    def test_register_without_groups_is_empty_list(self) -> None:
        conn = build_slugged_db()  # no groups seeded
        data = get_concept_groups(conn, "LISA")
        assert data["registers"][0]["groups"] == []

    def test_unknown_register_raises_not_found(self) -> None:
        conn = _seeded_conn()
        with pytest.raises(RegMetaError) as exc:
            get_concept_groups(conn, "NOPE")
        assert exc.value.exit_code == EXIT_NOT_FOUND

    def test_classification_groups(self) -> None:
        conn = _seeded_conn()
        data = get_classification_concept_groups(conn)
        (group,) = data["groups"]
        assert group["key"] == "sun"
        assert group["axes"] == ["vintage"]
        assert [m["fqid"] for m in group["members"]] == [
            "class/sun2000",
            "class/sun2020",
        ]


class TestSchemaAnnotation:
    def test_member_column_carries_group_key_and_label(self) -> None:
        conn = _seeded_conn()
        data = get_schema(conn, register="LISA")
        cols = {
            c["variable_name"]: c
            for v in data["variants"]
            for ver in v["versions"]
            for c in ver["columns"]
        }
        grouped = cols["Lönesumma januari"]
        assert grouped["concept_group"] == "agiink"
        assert grouped["concept_group_label"] == "Lönesumma per månad"
        ungrouped = cols["Kön"]
        assert ungrouped["concept_group"] is None
        assert ungrouped["concept_group_label"] is None


# ── CLI surface (envelope + flags); doc DB stub required by the query guard ──


@pytest.fixture(scope="module")
def groups_db_dir(tmp_path_factory: pytest.TempPathFactory) -> str:
    conn = _seeded_conn()
    conn.execute(
        "INSERT INTO import_manifest VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()
    db_dir = tmp_path_factory.mktemp("groups_db")
    on_disk = sqlite3.connect(db_dir / "reg_meta.db")
    conn.backup(on_disk)
    on_disk.close()
    conn.close()
    _build_stub_doc_db(db_dir, tmp_path_factory)
    return str(db_dir)


def _run_json(argv: list[str]) -> tuple[dict, int]:
    import io
    import sys

    from reg_meta.cli import run

    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        exit_code = run(["--format", "json", *argv])
    finally:
        sys.stdout = old_stdout
    output = buf.getvalue()
    return (json.loads(output) if output.strip() else {}), exit_code


class TestCliGroups:
    def test_get_groups_register(self, groups_db_dir: str) -> None:
        data, code = _run_json(["--db", groups_db_dir, "get", "groups", "LISA"])
        assert code == 0
        (reg,) = data["registers"]
        assert reg["fqid"] == "scb/lisa"
        assert [g["key"] for g in reg["groups"]] == ["agiink"]
        assert reg["groups"][0]["members"][0]["fqid"] == "scb/lisa/agiinkjan"

    def test_get_groups_classifications(self, groups_db_dir: str) -> None:
        data, code = _run_json(
            ["--db", groups_db_dir, "get", "groups", "--classifications"]
        )
        assert code == 0
        assert [g["key"] for g in data["groups"]] == ["sun"]

    def test_get_groups_requires_exactly_one_target(self, groups_db_dir: str) -> None:
        data, code = _run_json(["--db", groups_db_dir, "get", "groups"])
        assert code == EXIT_USAGE
        assert data["error"]["code"] == "usage_error"
        data, code = _run_json(
            ["--db", groups_db_dir, "get", "groups", "LISA", "--classifications"]
        )
        assert code == EXIT_USAGE

    def test_search_folds_by_default(self, groups_db_dir: str) -> None:
        data, code = _run_json(
            ["--db", groups_db_dir, "search", "--query", "Lönesumma"]
        )
        assert code == 0
        groups = [r for r in data["results"] if r["type"] == "group"]
        assert [g["group_key"] for g in groups] == ["agiink"]

    def test_search_no_fold_flag(self, groups_db_dir: str) -> None:
        data, code = _run_json(
            ["--db", groups_db_dir, "search", "--query", "Lönesumma", "--no-fold"]
        )
        assert code == 0
        assert all(r["type"] != "group" for r in data["results"])
        assert len(data["results"]) == 3


class TestSearchTableDisplay:
    def test_group_rows_render_with_counts(self, tmp_path: Path) -> None:
        from reg_meta.cli import _write_payload

        conn = _seeded_conn()
        payload = {"data": search(conn, "Lönesumma")}
        out = tmp_path / "out.txt"
        _write_payload(("search", None), payload, str(out), fmt="list")
        text = out.read_text(encoding="utf-8")
        # Pure-group results use the dedicated column set: identity + counts.
        assert "agiink" in text
        assert "matched" in text and "members" in text

    def test_group_rows_render_in_mixed_results(self, tmp_path: Path) -> None:
        from reg_meta.cli import _write_payload

        conn = _seeded_conn()
        # 'summa jan' style query that hits both a group and the lone Kön leaf
        # is hard to construct; synthesize a mixed payload instead — the
        # renderer only looks at the result dicts.
        group_row = search(conn, "Lönesumma")["results"][0]
        leaf = search(conn, "Kön", fold_groups=False)["results"][0]
        payload = {"data": {"results": [leaf, group_row], "total_count": 2}}
        out = tmp_path / "out.txt"
        _write_payload(("search", None), payload, str(out), fmt="list")
        text = out.read_text(encoding="utf-8")
        assert "3/3 members matched" in text  # group label projected inline
        assert "Kön" in text

    def test_get_groups_table_one_row_per_group(self, tmp_path: Path) -> None:
        from reg_meta.cli import _write_payload

        conn = _seeded_conn()
        payload = {"data": get_concept_groups(conn, "LISA")}
        out = tmp_path / "out.txt"
        _write_payload(("get", "groups"), payload, str(out), fmt="list")
        text = out.read_text(encoding="utf-8")
        assert "agiink" in text
        assert "Lönesumma per månad" in text
        assert "month" in text
