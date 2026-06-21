"""Tests for the derived concept-group layer (#303; `concept_groups.py`).

Covers the three derivation dimensions against hand-curated slugged DBs
(`_slugged_db`): edge components from the in-build sibling sets (`edge_siblings`,
dimension 0; #591 — not a `variable_related_to` round-trip), with curated
precedence excluding a claimed member; month token folds with their guards
(dimension 1); and curated single-variable families with their single inline
facet axis and the fail-fast resolution errors (dimension 2). The
accept-list (#496) — `[[accept]]` folds of the generated
`concept_groups.auto.toml` by reference — is covered
against synthetic auto families in `TestAcceptList` / `TestAcceptLoader`.
Classification EDITION vintages no longer fold into groups (#571); their
adjacent-chain succession edges (`classification_replaced_by`) are covered in
`TestClassificationSuccession`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.concept_groups import (
    Accept,
    ClassificationGroup,
    ClassificationGroupMember,
    CuratedGroup,
    CuratedMember,
    _insert_group,
    derive_classification_succession,
    load_classification_groups,
    load_concept_group_accepts,
    load_concept_groups,
    materialize_concept_groups,
)

if TYPE_CHECKING:
    import sqlite3

_SCB = frozenset({"scb"})


def _vid(conn: sqlite3.Connection, slug: str) -> int:
    """The `variable_id` PK of the variable with this slug (register-unique in
    these fixtures). The edge fold now consumes `edge_siblings` as variable_id
    pairs (#591), so tests resolve the added variables' real PKs."""
    return conn.execute(
        "SELECT variable_id FROM variable WHERE slug = ?", (slug,)
    ).fetchone()[0]


def _siblings(
    conn: sqlite3.Connection, *pairs: tuple[str, str]
) -> list[tuple[int, int]]:
    """Build an `edge_siblings` list (variable_id pairs) from slug pairs — the
    in-build `same_def` subset the triage hands the concept-group fold (#591)."""
    return [(_vid(conn, a), _vid(conn, b)) for a, b in pairs]


def _add_classification(
    conn: sqlite3.Connection, short_name: str, name: str, slug: str
) -> None:
    conn.execute(
        "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
        (short_name, name, slug),
    )


def _groups(conn: sqlite3.Connection) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for g in conn.execute("SELECT * FROM concept_group"):
        members = [
            r["slug"]
            for r in conn.execute(
                "SELECT v.slug FROM concept_group_variable m "
                "JOIN variable v ON v.variable_id = m.variable_id "
                "WHERE m.group_id = ? ORDER BY v.slug",
                (g["group_id"],),
            )
        ]
        cls_members = [
            (r["slug"], r["facet_value"], r["facet_label"])
            for r in conn.execute(
                "SELECT c.slug, m.facet_value, m.facet_label "
                "FROM concept_group_classification m "
                "JOIN classification c ON c.id = m.classification_id "
                "WHERE m.group_id = ? ORDER BY m.facet_value",
                (g["group_id"],),
            )
        ]
        out[g["group_key"]] = {
            "kind": g["kind"],
            "register_id": g["register_id"],
            "label": g["label"],
            "source": g["source"],
            "facet_axis": g["facet_axis"],
            "members": members,
            "cls_members": cls_members,
        }
    return out


def _facets(conn: sqlite3.Connection, slug: str) -> list[tuple[str, str, str]]:
    """The member's inline single facet as (axis, value, label) — (#585) the
    axis lives on `concept_group`, the value/label inline on
    `concept_group_variable`. Empty when the member carries no facet (edge
    group: NULL facet_value)."""
    return [
        (r["facet_axis"], r["facet_value"], r["facet_label"])
        for r in conn.execute(
            "SELECT g.facet_axis, m.facet_value, m.facet_label "
            "FROM concept_group_variable m "
            "JOIN variable v ON v.variable_id = m.variable_id "
            "JOIN concept_group g ON g.group_id = m.group_id "
            "WHERE v.slug = ? AND m.facet_value IS NOT NULL",
            (slug,),
        )
    ]


class TestEdgeGroups:
    """#591: edge groups fold the IN-BUILD `same_def` split-sibling subset passed
    as `edge_siblings` (variable_id pairs), NOT rows read back from
    `variable_related_to` — those foldable edges are no longer persisted."""

    def test_component_folds_into_one_group(self) -> None:
        conn = build_slugged_db(classification=None)  # scb/lisa with `kon`
        add_variable(conn, register_id=1, var_id=90, name="Utbildning", slug="sun2000")
        add_variable(conn, register_id=1, var_id=90, name="Utbildning", slug="sun2020")
        add_variable(conn, register_id=1, var_id=90, name="Utbildning", slug="sunx")
        # Transitive component: sun2000—sun2020—sunx.
        siblings = _siblings(conn, ("sun2000", "sun2020"), ("sun2020", "sunx"))

        counts = materialize_concept_groups(conn, edge_siblings=siblings)
        assert counts["edge_groups"] == 1
        groups = _groups(conn)
        assert groups["sun2000"] == {
            "kind": "variable",
            "register_id": 1,
            "label": "Utbildning",  # the shared name
            "source": "edge",
            "facet_axis": None,  # edge groups have no axis
            "members": ["sun2000", "sun2020", "sunx"],
            "cls_members": [],
        }
        # Edge members carry no facets — the member list is the presentation.
        assert _facets(conn, "sun2000") == []

    def test_non_same_def_pair_is_simply_absent(self) -> None:
        # A non-foldable split kind (code_vs_label_pair / import_bug_suspect) is
        # never in `edge_siblings` — the build filters by EDGE_RELATION_KIND — so
        # passing no siblings mints no edge group.
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=91, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=91, name="A", slug="varb")
        assert materialize_concept_groups(conn, edge_siblings=[])["edge_groups"] == 0

    def test_cross_register_pair_does_not_group(self) -> None:
        # A cross-register sibling pair must not fold even if passed (the WHERE
        # guard mirrors the A2.2 register-local split).
        conn = build_slugged_db(classification=None)
        add_register(conn, register_id=2, slug="rams", name="RAMS")
        add_variable(conn, register_id=1, var_id=92, name="A", slug="vara")
        add_variable(conn, register_id=2, var_id=93, name="A", slug="varb")
        siblings = _siblings(conn, ("vara", "varb"))
        assert (
            materialize_concept_groups(conn, edge_siblings=siblings)["edge_groups"] == 0
        )

    def test_curated_member_excluded_from_edge_fold(self) -> None:
        # Curated precedence (#591, unblocks #488): a curated `[[variable_group]]`
        # naming a variable that's also in an edge component excludes it from the
        # edge fold. Here the component is {vara, varb}; the curated group claims
        # vara (+ an unrelated varc), so the edge component drops to 1 survivor
        # (varb) → no edge group, and the curated group claims vara.
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=94, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=94, name="A", slug="varb")
        add_variable(conn, register_id=1, var_id=95, name="C", slug="varc")
        siblings = _siblings(conn, ("vara", "varb"))
        curated = CuratedGroup(
            provider="scb",
            register="lisa",
            key="fam",
            label="Familj",
            axis="part",
            members=(
                CuratedMember(variable="vara", value="1", label="A"),
                CuratedMember(variable="varc", value="2", label="C"),
            ),
        )
        counts = materialize_concept_groups(
            conn, (curated,), edge_siblings=siblings, providers=_SCB
        )
        assert counts["edge_groups"] == 0  # component below 2 survivors
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        assert set(groups) == {"fam"}
        assert groups["fam"]["members"] == ["vara", "varc"]

    def test_curated_member_excluded_but_component_survives(self) -> None:
        # Excluding one member of a 3-way component leaves 2 survivors → the edge
        # group still mints (without the curated-claimed member).
        conn = build_slugged_db(classification=None)
        for slug in ("sun2000", "sun2020", "sunx"):
            add_variable(conn, register_id=1, var_id=96, name="U", slug=slug)
        add_variable(conn, register_id=1, var_id=97, name="Other", slug="other")
        siblings = _siblings(conn, ("sun2000", "sun2020"), ("sun2020", "sunx"))
        curated = CuratedGroup(
            provider="scb",
            register="lisa",
            key="fam",
            label="Familj",
            axis="part",
            members=(
                CuratedMember(variable="sunx", value="1", label="x"),
                CuratedMember(variable="other", value="2", label="o"),
            ),
        )
        counts = materialize_concept_groups(
            conn, (curated,), edge_siblings=siblings, providers=_SCB
        )
        assert counts["edge_groups"] == 1
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        # sunx went to the curated group; the edge group keeps the other two.
        assert groups["sun2000"]["members"] == ["sun2000", "sun2020"]
        assert groups["fam"]["members"] == ["other", "sunx"]

    def test_curated_bridge_member_disconnects_survivors(self) -> None:
        # BRIDGE case (#591, unblocks #488): the component is two cliques joined
        # at a single vertex `varx` (a—x and x—b, NO direct a—b). When the curated
        # group claims `varx`, removing it disconnects vara from varb — no
        # surviving same_def path folds them. The fix skips edges touching an
        # excluded endpoint BEFORE the union, so vara and varb land in separate
        # (singleton) components and NO edge group containing both is minted. The
        # old subtract-after-union logic unioned the full a—x—b chain first, then
        # subtracted x, leaving a spurious {vara, varb} edge group — so this test
        # FAILS on the old logic and PASSES on the fix.
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=98, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=98, name="X", slug="varx")
        add_variable(conn, register_id=1, var_id=98, name="B", slug="varb")
        add_variable(conn, register_id=1, var_id=99, name="Other", slug="other")
        siblings = _siblings(conn, ("vara", "varx"), ("varx", "varb"))
        curated = CuratedGroup(
            provider="scb",
            register="lisa",
            key="fam",
            label="Familj",
            axis="part",
            members=(
                CuratedMember(variable="varx", value="1", label="x"),
                CuratedMember(variable="other", value="2", label="o"),
            ),
        )
        counts = materialize_concept_groups(
            conn, (curated,), edge_siblings=siblings, providers=_SCB
        )
        # Both surviving endpoints are now singleton components → no edge group.
        assert counts["edge_groups"] == 0
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        assert set(groups) == {"fam"}
        # Defensive: no minted edge group folds vara and varb together.
        for g in groups.values():
            if g["source"] == "edge":
                assert not ({"vara", "varb"} <= set(g["members"]))


class TestGroupKeyPathSafe:
    """#640: every `concept_group` row passes its `group_key` through the single
    `_insert_group` seam, which rejects a non-URL-path-safe key. This guarantees
    the by-key route contract — `/catalog/group/<provider>/<register>/<key>` —
    holds by construction (a key with `/`, `:`, etc. would be unreachable)."""

    @pytest.mark.parametrize("kind", ["variable", "classification"])
    @pytest.mark.parametrize("bad_key", ["foo/bar", "Foo", "a:b", "x y", ""])
    def test_path_unsafe_key_fails_fast(self, kind: str, bad_key: str) -> None:
        conn = build_slugged_db(classification=None)
        register_id = 1 if kind == "variable" else None
        with pytest.raises(RegMetaError) as exc:
            _insert_group(
                conn,
                kind=kind,
                register_id=register_id,
                group_key=bad_key,
                label="L",
                source="curated",
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_group_key_not_path_safe"

    def test_valid_slug_key_is_accepted(self) -> None:
        # A path-safe slug inserts cleanly — the guard is not over-broad.
        conn = build_slugged_db(classification=None)
        group_id = _insert_group(
            conn,
            kind="variable",
            register_id=1,
            group_key="foo-bar2",
            label="L",
            source="curated",
        )
        assert group_id is not None


class TestMonthGroups:
    @staticmethod
    def _family(
        conn: sqlite3.Connection,
        stem: str,
        tokens: list[str],
        name_fmt: str = "Inkomst i {tok}, totalt",
        register_id: int = 1,
    ) -> None:
        for i, tok in enumerate(tokens):
            add_variable(
                conn,
                register_id=register_id,
                var_id=500 + i,
                name=name_fmt.format(tok=tok),
                slug=f"{stem}{tok}",
            )

    def test_mixed_short_and_full_tokens_fold(self) -> None:
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink", ["jan", "februari", "mars", "okt"])
        counts = materialize_concept_groups(conn)
        assert counts["month_groups"] == 1
        groups = _groups(conn)
        assert groups["ink"]["source"] == "token"
        assert groups["ink"]["label"] == "Inkomst"  # trailing " i" trimmed
        assert groups["ink"]["members"] == [
            "inkfebruari",
            "inkjan",
            "inkmars",
            "inkokt",
        ]
        assert _facets(conn, "inkjan") == [("month", "01", "januari")]
        assert _facets(conn, "inkfebruari") == [("month", "02", "februari")]
        assert _facets(conn, "inkokt") == [("month", "10", "oktober")]

    def test_fewer_than_three_months_do_not_fold(self) -> None:
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink", ["jan", "feb"])
        assert materialize_concept_groups(conn)["month_groups"] == 0

    def test_label_prefix_disagreement_does_not_fold(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=510, name="Aaaa x", slug="xjan")
        add_variable(conn, register_id=1, var_id=511, name="Bbbb y", slug="xfeb")
        add_variable(conn, register_id=1, var_id=512, name="Cccc z", slug="xmars")
        assert materialize_concept_groups(conn)["month_groups"] == 0

    def test_coincidental_month_tail_does_not_fold(self) -> None:
        # "sommar" ends in the 'mar' token but has no sibling months.
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=513, name="Sommarjobb", slug="sommar")
        assert materialize_concept_groups(conn)["month_groups"] == 0

    def test_edge_grouped_members_are_not_reclaimed(self) -> None:
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink", ["jan", "feb", "mars"])
        # Tie one member into an edge component first (priority: edge wins).
        add_variable(conn, register_id=1, var_id=520, name="Annan", slug="annan")
        siblings = _siblings(conn, ("inkjan", "annan"))
        counts = materialize_concept_groups(conn, edge_siblings=siblings)
        assert counts["edge_groups"] == 1
        # inkjan is gone from the candidate pool → only 2 months left → no fold.
        assert counts["month_groups"] == 0


def _succession_edges(conn: sqlite3.Connection) -> list[tuple[str, str, int, str]]:
    """All `classification_replaced_by` rows as
    (predecessor_slug, successor_slug, effective_year, note), ordered."""
    return [
        (r["predecessor_slug"], r["successor_slug"], r["effective_year"], r["note"])
        for r in conn.execute(
            "SELECT predecessor_slug, successor_slug, effective_year, note "
            "FROM classification_replaced_by "
            "ORDER BY predecessor_slug, successor_slug"
        )
    ]


class TestClassificationSuccession:
    """#571: classification EDITION vintages materialize as adjacent-chain
    succession edges in `classification_replaced_by`, NOT concept groups. The
    detection guards (year tail, name agreement) are unchanged from the old
    vintage-group fold; only the OUTPUT differs."""

    def test_vintage_chain_emits_adjacent_edges(self) -> None:
        conn = build_slugged_db(classification=None)
        # Out-of-slug-order insert proves edges sort by YEAR, not slug/insert.
        _add_classification(conn, "LKF2020", "Län och kommuner 2020", "lkf2020")
        _add_classification(conn, "LKF1980", "Län och kommuner 1980", "lkf1980")
        _add_classification(conn, "LKF1998", "Län och kommuner 1998", "lkf1998")
        counts = materialize_concept_groups(conn)
        n_edges = derive_classification_succession(conn)
        # Succession is NOT a concept group — nothing lands in the group tables.
        assert "vintage_groups" not in counts
        assert _groups(conn) == {}
        # [1980<1998<2020] → two adjacent edges, effective_year = successor's.
        assert n_edges == 2
        assert _succession_edges(conn) == [
            ("lkf1980", "lkf1998", 1998, "derived:vintage_chain"),
            ("lkf1998", "lkf2020", 2020, "derived:vintage_chain"),
        ]

    def test_two_edition_chain_single_edge(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "SSYK1996", "Yrken 1996", "ssyk1996")
        _add_classification(conn, "SSYK2012", "Yrken 2012", "ssyk2012")
        assert derive_classification_succession(conn) == 1
        assert _succession_edges(conn) == [
            ("ssyk1996", "ssyk2012", 2012, "derived:vintage_chain"),
        ]

    def test_year_mid_name_still_detected(self) -> None:
        # The label-agreement guard strips the mid-name year; detection is by
        # slug tail, so a year inside the name doesn't block the chain.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "SG2000", "SUN 2000 — Grupper", "sun-grupp2000")
        _add_classification(conn, "SG2020", "SUN 2020 — Grupper", "sun-grupp2020")
        assert derive_classification_succession(conn) == 1
        assert _succession_edges(conn) == [
            ("sun-grupp2000", "sun-grupp2020", 2020, "derived:vintage_chain"),
        ]

    def test_singleton_or_non_year_tails_do_not_chain(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "ISCED2011", "ISCED 2011", "isced2011")  # singleton
        _add_classification(conn, "NG1", "Nivå grov", "niva-grovv1")  # non-year tail
        _add_classification(conn, "NG2", "Nivå old", "niva-oldv1")
        assert derive_classification_succession(conn) == 0
        assert _succession_edges(conn) == []

    def test_name_missing_year_blocks_chain(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "X2000", "Standard X", "x2000")  # no "2000" in name
        _add_classification(conn, "X2020", "Standard X 2020", "x2020")
        assert derive_classification_succession(conn) == 0

    def test_name_disagreement_blocks_chain(self) -> None:
        # Year-stripped names disagree ("Foo" vs "Bar") → not the same
        # classification's editions, so no chain.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "Y2000", "Foo 2000", "y2000")
        _add_classification(conn, "Y2020", "Bar 2020", "y2020")
        assert derive_classification_succession(conn) == 0


def _month_family_db() -> sqlite3.Connection:
    """scb/lisa with two 3-month token families, `agi1ink*` and `agi2ink*`."""
    conn = build_slugged_db(classification=None)
    for rank in (1, 2):
        for i, tok in enumerate(["jan", "feb", "mars"]):
            add_variable(
                conn,
                register_id=1,
                var_id=600 + 10 * rank + i,
                name=f"Inkomst i {tok}, källa {rank}",
                slug=f"agi{rank}ink{tok}",
            )
    return conn


def _curated(members: tuple[CuratedMember, ...], **overrides) -> CuratedGroup:
    kwargs: dict = {
        "provider": "scb",
        "register": "lisa",
        "key": "agiink",
        "label": "Inkomst per månad",
        "axis": "rank",
        "members": members,
    }
    kwargs.update(overrides)
    return CuratedGroup(**kwargs)


class TestCuratedGroups:
    def test_single_variable_members_attach_with_inline_facet(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=700, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=701, name="B", slug="varb")
        curated = _curated(
            (
                CuratedMember(variable="vara", value="1", label="största"),
                CuratedMember(variable="varb", value="2", label="näst"),
            )
        )
        counts = materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert counts["curated_groups"] == 1
        group = _groups(conn)["agiink"]
        assert group["source"] == "curated"
        assert group["members"] == ["vara", "varb"]
        # The group carries its single axis; each member its inline facet.
        assert group["facet_axis"] == "rank"
        assert _facets(conn, "vara") == [("rank", "1", "största")]
        assert _facets(conn, "varb") == [("rank", "2", "näst")]

    def test_inactive_provider_is_skipped(self) -> None:
        conn = _month_family_db()
        add_variable(conn, register_id=1, var_id=730, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=731, name="B", slug="varb")
        curated = _curated(
            (
                CuratedMember(variable="vara", value="1", label="x"),
                CuratedMember(variable="varb", value="2", label="y"),
            ),
        )
        counts = materialize_concept_groups(
            conn, (curated,), providers=frozenset({"sos"})
        )
        assert counts["curated_groups"] == 0
        assert counts["month_groups"] == 2  # token groups untouched

    @pytest.mark.parametrize(
        ("members", "register"),
        [
            # dangling variable reference
            (
                (
                    CuratedMember(variable="nope", value="1", label="x"),
                    CuratedMember(variable="nope2", value="2", label="y"),
                ),
                "lisa",
            ),
            # dangling register
            (
                (
                    CuratedMember(variable="vara", value="1", label="x"),
                    CuratedMember(variable="varb", value="2", label="y"),
                ),
                "no",
            ),
        ],
    )
    def test_dangling_references_fail_fast(
        self, members: tuple[CuratedMember, ...], register: str
    ) -> None:
        conn = _month_family_db()
        add_variable(conn, register_id=1, var_id=740, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=741, name="B", slug="varb")
        curated = _curated(members, register=register)
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_unresolved"

    def test_member_already_in_month_group_fails(self) -> None:
        # A curated member already claimed by the TOKEN (month) pass still fails
        # loud (curated precedence (#591) excludes a member only from the EDGE
        # fold, not the month fold — the month pass runs before curated and
        # claims ungrouped month-suffixed variables first). `inkjan/inkfeb/inkmars`
        # fold into a month group; naming `inkjan` in a curated family collides.
        conn = build_slugged_db(classification=None)
        for i, tok in enumerate(["jan", "feb", "mars"]):
            add_variable(
                conn,
                register_id=1,
                var_id=710 + i,
                name=f"Inkomst i {tok}, totalt",
                slug=f"ink{tok}",
            )
        add_variable(conn, register_id=1, var_id=713, name="C", slug="varc")
        curated = _curated(
            (
                CuratedMember(variable="inkjan", value="1", label="A"),
                CuratedMember(variable="varc", value="2", label="C"),
            ),
            key="fam",
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert exc.value.code == "concept_groups_unresolved"

    def test_under_two_members_fails(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=720, name="A", slug="vara")
        curated = _curated(
            (CuratedMember(variable="vara", value="1", label="A"),),
            key="fam",
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert exc.value.code == "concept_groups_unresolved"

    def test_db_cannot_represent_multi_axis(self) -> None:
        """#585: the multi-axis machinery is gone — `concept_group_variable_facet`
        no longer exists, so a member can carry at most ONE facet (inline on
        `concept_group_variable`). Single-axis is enforced by schema shape."""
        conn = build_slugged_db(classification=None)
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert "concept_group_variable_facet" not in tables
        # The variable member table carries exactly the single inline facet pair.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(concept_group_variable)")}
        assert {"facet_value", "facet_label"} <= cols


def _sun_group(**overrides) -> ClassificationGroup:
    """A SUN-like 3-dimension umbrella over single-axis distinct classifications."""
    kwargs: dict = {
        "key": "sun",
        "label": "Svensk utbildningsnomenklatur (SUN)",
        "axis": "dimension",
        "members": (
            ClassificationGroupMember("sun-niva2020", "niva", "Utbildningsnivå"),
            ClassificationGroupMember(
                "sun-inriktning2020", "inriktning", "Utbildningsinriktning"
            ),
            ClassificationGroupMember("sun-grupp2020", "grupp", "Utbildningsgrupper"),
        ),
    }
    kwargs.update(overrides)
    return ClassificationGroup(**kwargs)


class TestClassificationGroups:
    """#516: curated `kind='classification'` umbrella groups (the SUN umbrella),
    materialized via `_apply_curated_classification_groups`."""

    def _db(self) -> sqlite3.Connection:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "SUN2020-NIVA", "SUN 2020 — Nivå", "sun-niva2020")
        _add_classification(
            conn, "SUN2020-INR", "SUN 2020 — Inriktning", "sun-inriktning2020"
        )
        _add_classification(
            conn, "SUN2020-GRUPP", "SUN 2020 — Grupper", "sun-grupp2020"
        )
        return conn

    def test_resolves_to_one_dimension_group(self) -> None:
        conn = self._db()
        counts = materialize_concept_groups(
            conn, classification_groups=(_sun_group(),), providers=_SCB
        )
        assert counts["classification_curated_groups"] == 1
        assert counts["grouped_classifications"] == 3
        group = _groups(conn)["sun"]
        assert group["kind"] == "classification"
        assert group["register_id"] is None
        assert group["source"] == "curated"
        # facet_value-ordered members carry the dimension facet.
        assert group["cls_members"] == [
            ("sun-grupp2020", "grupp", "Utbildningsgrupper"),
            ("sun-inriktning2020", "inriktning", "Utbildningsinriktning"),
            ("sun-niva2020", "niva", "Utbildningsnivå"),
        ]
        axis = conn.execute(
            "SELECT facet_axis FROM concept_group WHERE group_key = 'sun'"
        ).fetchone()[0]
        assert axis == "dimension"

    def test_not_provider_gated(self) -> None:
        # Classifications are catalog-global — the umbrella materializes even when
        # the build's active providers don't include scb (unlike variable groups).
        conn = self._db()
        counts = materialize_concept_groups(
            conn,
            classification_groups=(_sun_group(),),
            providers=frozenset({"sos"}),
        )
        assert counts["classification_curated_groups"] == 1

    def test_unknown_slug_fails_fast(self) -> None:
        conn = self._db()
        group = _sun_group(
            members=(
                ClassificationGroupMember("sun-niva2020", "niva", "Nivå"),
                ClassificationGroupMember("sun-nope2020", "nope", "Saknas"),
            )
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(
                conn, classification_groups=(group,), providers=_SCB
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_unresolved"

    def test_already_grouped_classification_fails(self) -> None:
        conn = self._db()
        group = _sun_group()
        # First umbrella claims the three slugs; a second umbrella naming one of
        # them must fail (a classification joins at most one group).
        materialize_concept_groups(conn, classification_groups=(group,), providers=_SCB)
        dup = ClassificationGroup(
            key="sun2",
            label="Dup",
            axis="dimension",
            members=(
                ClassificationGroupMember("sun-niva2020", "niva", "Nivå"),
                ClassificationGroupMember("sun-grupp2020", "grupp", "Grupp"),
            ),
        )
        with pytest.raises(RegMetaError) as exc:
            _apply_dup(conn, dup)
        assert exc.value.code == "concept_groups_unresolved"

    def test_under_two_resolving_members_fails(self) -> None:
        # Only one member resolves (the other slug is absent) → < 2 → fail. Use a
        # DB carrying just one of the two referenced slugs.
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "SUN2020-NIVA", "SUN 2020 — Nivå", "sun-niva2020")
        group = ClassificationGroup(
            key="sun",
            label="SUN",
            axis="dimension",
            members=(
                ClassificationGroupMember("sun-niva2020", "niva", "Nivå"),
                ClassificationGroupMember("sun-grupp2020", "grupp", "Grupp"),
            ),
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(
                conn, classification_groups=(group,), providers=_SCB
            )
        assert exc.value.code == "concept_groups_unresolved"


def _apply_dup(conn: sqlite3.Connection, group: ClassificationGroup) -> None:
    """Apply ONE more classification umbrella against an already-materialized DB
    (re-running the full `materialize_concept_groups` would re-derive edge/month
    passes and re-insert the same groups). Calls the curated-classification pass
    directly."""
    from reg_meta_build.concept_groups import _apply_curated_classification_groups

    _apply_curated_classification_groups(conn, (group,))


class TestClassificationGroupLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "concept_groups.toml"
        path.write_text(text, encoding="utf-8")
        return load_classification_groups(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_classification_groups(None) == ()
        assert load_classification_groups(tmp_path / "absent.toml") == ()

    def test_parses_valid_umbrella(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "sun-niva2020"
            value = "niva"
            label = "Nivå"
            [[classification_group.members]]
            classification = "sun-grupp2020"
            value = "grupp"
            label = "Grupp"
            """,
        )
        assert len(groups) == 1
        assert groups[0].key == "sun"
        assert groups[0].axis == "dimension"
        assert [m.classification for m in groups[0].members] == [
            "sun-niva2020",
            "sun-grupp2020",
        ]

    @pytest.mark.parametrize(
        "text",
        [
            # missing axis
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
            # blank key
            """
            [[classification_group]]
            key = ""
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
            # missing members array
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            """,
            # only one member (< 2)
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            """,
            # member missing value
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
            # duplicate member slug
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "a"
            value = "2"
            label = "y"
            """,
        ],
    )
    def test_invalid_shapes_fail(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_keys_fail(self, tmp_path) -> None:
        text = """
        [[classification_group]]
        key = "sun"
        label = "SUN"
        axis = "dimension"
        [[classification_group.members]]
        classification = "a"
        value = "1"
        label = "x"
        [[classification_group.members]]
        classification = "b"
        value = "2"
        label = "y"
        [[classification_group]]
        key = "sun"
        label = "SUN2"
        axis = "dimension"
        [[classification_group.members]]
        classification = "c"
        value = "1"
        label = "x"
        [[classification_group.members]]
        classification = "d"
        value = "2"
        label = "y"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    def test_sibling_kinds_do_not_trip_top_level_guard(self, tmp_path) -> None:
        # A `[[classification_group]]` coexisting with `[[variable_group]]` /
        # `[[accept]]` parses cleanly — they're legal siblings, not typos.
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "vara"
            value = "1"
            label = "x"
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
        )
        assert [g.key for g in groups] == ["sun"]


class TestLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "concept_groups.toml"
        path.write_text(text, encoding="utf-8")
        return load_concept_groups(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_concept_groups(None) == ()
        assert load_concept_groups(tmp_path / "absent.toml") == ()

    def test_parses_valid_family(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axis = "rank"
            [[variable_group.members]]
            variable = "agi1ink"
            value = "1"
            label = "största"
            [[variable_group.members]]
            variable = "extra"
            value = "2"
            label = "annan"
            """,
        )
        assert len(groups) == 1
        assert groups[0].provider == "scb"
        assert groups[0].members[0].variable == "agi1ink"
        assert groups[0].members[1].variable == "extra"

    @pytest.mark.parametrize(
        "text",
        [
            "[[variable_groups]]\n",  # misspelled top-level table
            # member missing the required `variable`
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            value = "1"
            label = "x"
            """,
            # 1-segment register
            """
            [[variable_group]]
            register = "lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "g"
            value = "1"
            label = "x"
            """,
            # missing members
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            """,
            # duplicate member reference
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "g"
            value = "1"
            label = "x"
            [[variable_group.members]]
            variable = "g"
            value = "2"
            label = "y"
            """,
        ],
    )
    def test_invalid_shapes_fail(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_group_keys_fail(self, tmp_path) -> None:
        text = """
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "F"
        axis = "a"
        [[variable_group.members]]
        variable = "g"
        value = "1"
        label = "x"
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "F2"
        axis = "b"
        [[variable_group.members]]
        variable = "h"
        value = "1"
        label = "x"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    def test_accept_sibling_does_not_trip_top_level_guard(self, tmp_path) -> None:
        # A `[[variable_group]]`, an `[[accept]]`, and a `[[classification_group]]`
        # coexist in one file: the variable_group parse must treat the latter two
        # as legal siblings, not unknown-top-level typos. (Case (g).)
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "vara"
            value = "1"
            label = "x"
            [[variable_group.members]]
            variable = "varb"
            value = "2"
            label = "y"
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            """,
        )
        assert len(groups) == 1
        assert groups[0].key == "fam"


def _auto_family(
    members: tuple[CuratedMember, ...],
    *,
    provider: str = "scb",
    register: str = "lisa",
    key: str = "morsak",
    label: str = "Mor",
    axis: str = "ordinal",
) -> CuratedGroup:
    """A synthetic `concept_groups.auto.toml` family (an accept's referent). Its
    members are `variable=` attachments, matching what the candidate generator
    emits and `load_concept_groups` parses for the auto file."""
    return CuratedGroup(
        provider=provider,
        register=register,
        key=key,
        label=label,
        axis=axis,
        members=members,
    )


def _morsak_db() -> sqlite3.Connection:
    """scb/lisa with three ungrouped `morsak{1,2,3}` variables (a digit-suffixed
    family the generator would propose as an `ordinal` candidate)."""
    conn = build_slugged_db(classification=None)
    for i in (1, 2, 3):
        add_variable(
            conn,
            register_id=1,
            var_id=800 + i,
            name=f"Dödsorsak {i}",
            slug=f"morsak{i}",
        )
    return conn


def _morsak_members() -> tuple[CuratedMember, ...]:
    return tuple(
        CuratedMember(variable=f"morsak{i}", value=str(i), label=str(i))
        for i in (1, 2, 3)
    )


class TestAcceptList:
    def test_accept_folds_auto_family(self) -> None:
        # (a) an accept referencing a synthetic auto family folds it with the
        # auto family's members + axis.
        conn = _morsak_db()
        auto = (_auto_family(_morsak_members()),)
        accepts = (Accept("scb", "lisa", "morsak", None, None, ()),)
        counts = materialize_concept_groups(
            conn, auto=auto, accepts=accepts, providers=_SCB
        )
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        assert groups["morsak"]["source"] == "curated"
        assert groups["morsak"]["label"] == "Mor"  # the auto family's label
        assert groups["morsak"]["members"] == ["morsak1", "morsak2", "morsak3"]
        assert _facets(conn, "morsak2") == [("ordinal", "2", "2")]

    def test_accept_label_and_axis_overrides_apply(self) -> None:
        # (b) accept label/axis overrides apply.
        conn = _morsak_db()
        auto = (_auto_family(_morsak_members()),)
        accepts = (Accept("scb", "lisa", "morsak", "Multipel dödsorsak", "rank", ()),)
        materialize_concept_groups(conn, auto=auto, accepts=accepts, providers=_SCB)
        groups = _groups(conn)
        assert groups["morsak"]["label"] == "Multipel dödsorsak"
        assert _facets(conn, "morsak1") == [("rank", "1", "1")]

    def test_accept_exclude_drops_member(self) -> None:
        # (c) accept exclude drops a member.
        conn = _morsak_db()
        auto = (_auto_family(_morsak_members()),)
        accepts = (Accept("scb", "lisa", "morsak", None, None, ("morsak3",)),)
        materialize_concept_groups(conn, auto=auto, accepts=accepts, providers=_SCB)
        groups = _groups(conn)
        assert groups["morsak"]["members"] == ["morsak1", "morsak2"]
        # The excluded member never joins the group, so it carries no facet.
        assert _facets(conn, "morsak3") == []

    def test_accept_of_missing_auto_family_fails(self) -> None:
        # (d) accept of a non-existent auto family fails (drift, EXIT_CONFIG).
        conn = _morsak_db()
        accepts = (Accept("scb", "lisa", "morsak", None, None, ()),)
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, auto=(), accepts=accepts, providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_unresolved"

    def test_accept_stale_exclude_fails(self) -> None:
        # (e) stale exclude (slug not a member) fails.
        conn = _morsak_db()
        auto = (_auto_family(_morsak_members()),)
        accepts = (Accept("scb", "lisa", "morsak", None, None, ("morsak9",)),)
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, auto=auto, accepts=accepts, providers=_SCB)
        assert exc.value.code == "concept_groups_unresolved"

    def test_exclude_below_two_members_fails(self) -> None:
        # exclude that leaves < 2 members is rejected — a group needs >= 2.
        conn = _morsak_db()
        auto = (_auto_family(_morsak_members()),)
        accepts = (Accept("scb", "lisa", "morsak", None, None, ("morsak2", "morsak3")),)
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, auto=auto, accepts=accepts, providers=_SCB)
        assert exc.value.code == "concept_groups_unresolved"

    def test_empty_accepts_with_custom_group_only_materializes_custom(self) -> None:
        # (f) empty accepts + a custom `[[variable_group]]` → only the custom
        # family materializes; the auto family is present but unaccepted, so no
        # auto group appears.
        conn = _morsak_db()
        add_variable(conn, register_id=1, var_id=900, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=901, name="B", slug="varb")
        custom = CuratedGroup(
            provider="scb",
            register="lisa",
            key="fam",
            label="Familj",
            axis="part",
            members=(
                CuratedMember(variable="vara", value="1", label="A"),
                CuratedMember(variable="varb", value="2", label="B"),
            ),
        )
        auto = (_auto_family(_morsak_members()),)
        counts = materialize_concept_groups(
            conn, (custom,), auto=auto, accepts=(), providers=_SCB
        )
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        assert set(groups) == {"fam"}  # the unaccepted morsak family did NOT fold
        assert groups["fam"]["members"] == ["vara", "varb"]

    def test_inactive_provider_accept_is_skipped(self) -> None:
        # An accept for a provider not in this build is gated out, not resolved
        # (so a `--providers=sos` build doesn't fail on an absent scb auto family).
        conn = _morsak_db()
        accepts = (Accept("scb", "lisa", "morsak", None, None, ()),)
        counts = materialize_concept_groups(
            conn, auto=(), accepts=accepts, providers=frozenset({"sos"})
        )
        assert counts["curated_groups"] == 0

    def test_accept_member_claimed_since_generation_fails_with_accept_message(
        self,
    ) -> None:
        # The auto family was generated against an earlier build; here its member
        # is claimed by the TOKEN (month) pass BEFORE the accept resolves (the
        # real footgun: the auto.toml folds against a LATER build whose token pass
        # newly claimed a member). The month pass runs before curated/accept and
        # is NOT subject to curated precedence (#591 excludes only from the EDGE
        # fold), so the collision still fires. The error must point at the
        # `[[accept]]` / auto.toml, NOT at "pick a curated key".
        conn = build_slugged_db(classification=None)
        for i, tok in enumerate(["jan", "feb", "mars"]):
            add_variable(
                conn,
                register_id=1,
                var_id=860 + i,
                name=f"Inkomst i {tok}, totalt",
                slug=f"ink{tok}",
            )
        add_variable(conn, register_id=1, var_id=863, name="Extra", slug="inkextra")
        # The auto family names `inkjan` (claimed by the month fold) + a sibling.
        # Its key `alt` doesn't collide with the month stem `ink`, so the group
        # row inserts and the failure is on the MEMBER `inkjan` (already grouped).
        auto = (
            _auto_family(
                (
                    CuratedMember(variable="inkjan", value="1", label="jan"),
                    CuratedMember(variable="inkextra", value="2", label="extra"),
                ),
                key="alt",
            ),
        )
        accepts = (Accept("scb", "lisa", "alt", None, None, ()),)
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, auto=auto, accepts=accepts, providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_unresolved"
        assert "[[accept]]" in exc.value.message
        assert "concept_groups.auto.toml" in exc.value.remediation
        # The hand-authored remediation must NOT leak into the accept path.
        assert "pick a curated key" not in exc.value.remediation.lower()


class TestAcceptLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "concept_groups.toml"
        path.write_text(text, encoding="utf-8")
        return load_concept_group_accepts(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_concept_group_accepts(None) == ()
        assert load_concept_group_accepts(tmp_path / "absent.toml") == ()

    def test_parses_accept_with_overrides_and_exclude(self, tmp_path) -> None:
        accepts = self._load(
            tmp_path,
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            label = "Multipel dödsorsak"
            axis = "rank"
            exclude = ["morsak9"]
            """,
        )
        assert accepts == (
            Accept("sos", "dors", "morsak", "Multipel dödsorsak", "rank", ("morsak9",)),
        )

    def test_parses_minimal_accept(self, tmp_path) -> None:
        accepts = self._load(
            tmp_path,
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            """,
        )
        assert accepts == (Accept("sos", "dors", "morsak", None, None, ()),)

    def test_variable_group_sibling_does_not_trip_guard(self, tmp_path) -> None:
        # The accept loader must treat `[[variable_group]]` and
        # `[[classification_group]]` as legal siblings.
        accepts = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            variable = "v"
            value = "1"
            label = "x"
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = "dimension"
            [[classification_group.members]]
            classification = "a"
            value = "1"
            label = "x"
            [[classification_group.members]]
            classification = "b"
            value = "2"
            label = "y"
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            """,
        )
        assert accepts == (Accept("sos", "dors", "morsak", None, None, ()),)

    @pytest.mark.parametrize(
        "text",
        [
            # 1-segment register
            """
            [[accept]]
            register = "dors"
            key = "morsak"
            """,
            # 3-segment register (too many)
            """
            [[accept]]
            register = "sos/dors/morsak"
            key = "morsak"
            """,
            # empty key
            """
            [[accept]]
            register = "sos/dors"
            key = ""
            """,
            # blank label (present but empty — drift, not a fallback)
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            label = "  "
            """,
            # exclude with a non-string member
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            exclude = ["ok", 7]
            """,
            # exclude not a list
            """
            [[accept]]
            register = "sos/dors"
            key = "morsak"
            exclude = "morsak9"
            """,
        ],
    )
    def test_invalid_accept_shapes_fail(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_invalid"

    def test_duplicate_accept_fails(self, tmp_path) -> None:
        text = """
        [[accept]]
        register = "sos/dors"
        key = "morsak"
        [[accept]]
        register = "sos/dors"
        key = "morsak"
        axis = "rank"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"
