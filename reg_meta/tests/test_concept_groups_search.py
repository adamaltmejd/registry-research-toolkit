"""Concept-group folding on ``reg_meta.queries.search`` (#322).

`search` folds sibling member hits into one group row, matches group labels,
and applies type/register/year scopes before pagination. Groups are
hand-seeded onto the slugged fixture DB (the shared ``_slugged_db`` factory).
"""

from __future__ import annotations

from _slugged_db import add_state, add_variable
from groups_test_support import seeded_conn as _seeded_conn
from reg_meta.queries import search


class TestSearchFolding:
    def test_folded_prefix_backfills_and_continues_without_gap(self) -> None:
        conn = _seeded_conn()
        add_variable(
            conn,
            register_id=1,
            var_id=999,
            name="Lönesumma fristående",
            slug="lonesumma-fristaende",
        )
        expected = search(conn, "Lönesumma", limit=10)
        assert [result.type for result in expected.results] == ["group", "varname"]

        first = search(conn, "Lönesumma", limit=1)
        assert first.has_more and first.next_cursor is not None
        second = search(conn, "Lönesumma", limit=1, cursor=first.next_cursor)

        assert [result.type for result in (*first.results, *second.results)] == [
            "group",
            "varname",
        ]
        assert not second.has_more

    def test_cursor_completes_group_before_first_page(self) -> None:
        conn = _seeded_conn()
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (30, 'variable', 1, 'needle-family', "
            "'Needle family label', 'curated')"
        )
        for var_id, name, slug in (
            (930, "Needle A", "needle-a"),
            (931, "Needle Z", "needle-z"),
        ):
            add_variable(conn, register_id=1, var_id=var_id, name=name, slug=slug)
            variable_id = conn.execute(
                "SELECT variable_id FROM variable WHERE slug = ?", (slug,)
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, 30)",
                (variable_id,),
            )
        add_variable(
            conn,
            register_id=1,
            var_id=932,
            name="Needle M",
            slug="needle-m",
        )

        expected = search(conn, "Needle", field="varname", type="variable", limit=10)
        first = search(conn, "Needle", field="varname", type="variable", limit=1)
        assert first.results[0].type == "group"
        assert first.results[0].matched_count == 2
        assert first.next_cursor is not None
        second = search(
            conn,
            "Needle",
            field="varname",
            type="variable",
            limit=1,
            cursor=first.next_cursor,
        )

        assert [r.type for r in (*first.results, *second.results)] == [
            r.type for r in expected.results
        ]
        assert (
            len({r.model_dump_json() for r in (*first.results, *second.results)}) == 2
        )
        assert not second.has_more

    def test_lone_member_hit_stays_leaf_with_annotation(self) -> None:
        conn = _seeded_conn()
        # 'januari' hits only one member (and not the group label/key).
        results = search(conn, "januari").results
        assert [r.type for r in results] == ["varname"]
        (leaf,) = results
        assert leaf.name == "Lönesumma januari"
        assert leaf.concept_group == "agiink"
        assert leaf.concept_group_label == "Lönesumma per månad"

    def test_group_label_matches_without_leaf_hits(self) -> None:
        conn = _seeded_conn()
        # 'per månad' appears only in the group LABEL, not in any member name.
        results = search(conn, "per månad").results
        assert [r.type for r in results] == ["group"]
        (group,) = results
        assert group.label_matched is True
        assert group.matched_count == 0
        assert group.member_count == 3

    def test_group_label_matches_in_any_swedish_case(self) -> None:
        conn = _seeded_conn()
        # 'per månad' is label-only text, so this exercises the label arm alone.
        # SQLite's own LIKE case-insensitivity is ASCII-only, which used to let
        # 'per månad' find the family while 'PER MÅNAD' found nothing.
        for query in ("per månad", "PER MÅNAD", "Per Månad"):
            results = search(conn, query, field="varname").results
            assert [r.type for r in results] == ["group"], query
            (group,) = results
            assert group.group_key == "agiink", query
            assert group.label_matched is True, query

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

        underscore = search(conn, "12_", field="description").results
        assert {r.group_key for r in underscore if r.type == "group"} == {
            "literal-underscore"
        }

        percent = search(conn, "99%", field="description").results
        assert {r.group_key for r in percent if r.type == "group"} == {
            "literal-percent"
        }

    def test_no_fold_returns_flat_member_rows(self) -> None:
        conn = _seeded_conn()
        results = search(conn, "Lönesumma", fold_groups=False).results
        assert {r.name for r in results} == {
            "Lönesumma januari",
            "Lönesumma februari",
            "Lönesumma mars",
        }
        assert all(r.type == "varname" for r in results)
        # Unfolded varname rows carry no lone-member annotation.
        assert all(r.concept_group is None for r in results)

    def test_register_scope_keeps_variable_group_drops_classification(self) -> None:
        conn = _seeded_conn()
        scoped = search(conn, "Lönesumma", register="LISA").results
        assert [r.type for r in scoped] == ["group"]
        assert scoped[0].kind == "variable"
        # Classification groups are catalog-scoped → excluded under --register.
        assert search(conn, "utbildningsnomenklatur", register="LISA").results == ()

    def test_type_variable_keeps_variable_group_drops_classification(self) -> None:
        conn = _seeded_conn()
        kept = search(conn, "Lönesumma", type="variable").results
        assert [r.type for r in kept] == ["group"]
        assert search(conn, "utbildningsnomenklatur", type="variable").results == ()

    def test_label_only_match_respects_years_filter(self) -> None:
        # Codex P2 on #331: --years must apply to the label-only path through
        # the group's MEMBER states (the fixture seeds one member state
        # 2018→open-ended; the other members have no states at all).
        conn = _seeded_conn()
        # Out of range: no member state overlaps 1900 → the group is dropped.
        assert search(conn, "per månad", years="1900").results == ()
        # In range (open-ended window covers 2020) → the group survives.
        kept = search(conn, "per månad", years="2020").results
        assert [r.type for r in kept] == ["group"]
        assert kept[0].label_matched is True

    def test_group_label_scope_is_applied_before_limit(self) -> None:
        conn = _seeded_conn()
        for group_id, key in ((40, "a-class"), (41, "b-class")):
            conn.execute(
                "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
                "label, source) VALUES (?, 'classification', NULL, ?, "
                "'Shared scope label', 'curated')",
                (group_id, key),
            )
        conn.execute(
            "UPDATE concept_group SET label = 'Shared scope label' WHERE group_id = 10"
        )

        result = search(
            conn,
            "Shared scope label",
            field="description",
            type="variable",
            limit=1,
        )

        assert [r.group_key for r in result.results if r.type == "group"] == ["agiink"]
        assert not result.has_more

    def test_group_label_year_scope_is_applied_before_limit(self) -> None:
        conn = _seeded_conn()
        for group_id, key, var_id in (
            (50, "a-old", 950),
            (51, "b-old", 951),
            (52, "c-current", 952),
        ):
            conn.execute(
                "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
                "label, source) VALUES (?, 'variable', 1, ?, "
                "'Shared year label', 'curated')",
                (group_id, key),
            )
            slug = f"{key}-member"
            add_variable(conn, register_id=1, var_id=var_id, name=key, slug=slug)
            variable_id = conn.execute(
                "SELECT variable_id FROM variable WHERE slug = ?", (slug,)
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, ?)",
                (variable_id, group_id),
            )
            if key == "c-current":
                add_state(
                    conn,
                    register_id=1,
                    variable_slug=slug,
                    register_variant_id=10,
                    valid_from="2020-01-01",
                    valid_to="2020-12-31",
                )

        result = search(
            conn,
            "Shared year label",
            field="description",
            type="variable",
            years="2020",
            limit=1,
        )

        assert [r.group_key for r in result.results if r.type == "group"] == [
            "c-current"
        ]
        assert not result.has_more
