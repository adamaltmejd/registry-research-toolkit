"""Tests for the derived concept-group layer (#303; `concept_groups.py`).

Covers the three derivation dimensions against hand-curated slugged DBs
(`_slugged_db`): edge components (dimension 0), month/vintage token folds with
their guards (dimension 1), and curated families incl. token-group absorption
and the fail-fast resolution errors (dimension 2). The accept-list (#496) —
`[[accept]]` folds of the generated `concept_groups.auto.toml` by reference —
is covered against synthetic auto families in `TestAcceptList` / `TestAcceptLoader`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.concept_groups import (
    Accept,
    CuratedGroup,
    CuratedMember,
    load_concept_group_accepts,
    load_concept_groups,
    materialize_concept_groups,
)

if TYPE_CHECKING:
    import sqlite3

_SCB = frozenset({"scb"})


def _add_edge(
    conn: sqlite3.Connection,
    register: str,
    a: str,
    b: str,
    *,
    kind: str = "same_definition_different_column",
    b_register: str | None = None,
) -> None:
    """Insert a sibling edge both directions (mirrors the build's writer)."""
    reg_b = b_register or register
    for (ra, va), (rb, vb) in (
        ((register, a), (reg_b, b)),
        ((reg_b, b), (register, a)),
    ):
        conn.execute(
            "INSERT INTO variable_related_to (a_provider, a_register, a_variable, "
            "b_provider, b_register, b_variable, relation_kind, note) "
            "VALUES ('scb', ?, ?, 'scb', ?, ?, ?, 'auto:triage')",
            (ra, va, rb, vb, kind),
        )


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
            "members": members,
            "cls_members": cls_members,
        }
    return out


def _facets(conn: sqlite3.Connection, slug: str) -> list[tuple[str, str, str]]:
    return [
        (r["axis"], r["value"], r["label"])
        for r in conn.execute(
            "SELECT f.axis, f.value, f.label FROM concept_group_variable_facet f "
            "JOIN variable v ON v.variable_id = f.variable_id "
            "WHERE v.slug = ? ORDER BY f.axis",
            (slug,),
        )
    ]


class TestEdgeGroups:
    def test_component_folds_into_one_group(self) -> None:
        conn = build_slugged_db(classification=None)  # scb/lisa with `kon`
        add_variable(conn, register_id=1, var_id=90, name="Utbildning", slug="sun2000")
        add_variable(conn, register_id=1, var_id=90, name="Utbildning", slug="sun2020")
        add_variable(conn, register_id=1, var_id=90, name="Utbildning", slug="sunx")
        _add_edge(conn, "lisa", "sun2000", "sun2020")
        _add_edge(conn, "lisa", "sun2020", "sunx")  # transitive component

        counts = materialize_concept_groups(conn)
        assert counts["edge_groups"] == 1
        groups = _groups(conn)
        assert groups["sun2000"] == {
            "kind": "variable",
            "register_id": 1,
            "label": "Utbildning",  # the shared name
            "source": "edge",
            "members": ["sun2000", "sun2020", "sunx"],
            "cls_members": [],
        }
        # Edge members carry no facets — the member list is the presentation.
        assert _facets(conn, "sun2000") == []

    def test_other_relation_kinds_do_not_group(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=91, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=91, name="A", slug="varb")
        _add_edge(conn, "lisa", "vara", "varb", kind="import_bug_suspect")
        assert materialize_concept_groups(conn)["edge_groups"] == 0

    def test_cross_register_edges_do_not_group(self) -> None:
        conn = build_slugged_db(classification=None)
        add_register(conn, register_id=2, slug="rams", name="RAMS")
        add_variable(conn, register_id=1, var_id=92, name="A", slug="vara")
        add_variable(conn, register_id=2, var_id=93, name="A", slug="varb")
        _add_edge(conn, "lisa", "vara", "varb", b_register="rams")
        assert materialize_concept_groups(conn)["edge_groups"] == 0


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
        _add_edge(conn, "lisa", "inkjan", "annan")
        counts = materialize_concept_groups(conn)
        assert counts["edge_groups"] == 1
        # inkjan is gone from the candidate pool → only 2 months left → no fold.
        assert counts["month_groups"] == 0


class TestClassificationVintageGroups:
    def test_vintage_family_folds_with_year_facets(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "LKF1980", "Län och kommuner 1980", "lkf1980")
        _add_classification(conn, "LKF1998", "Län och kommuner 1998", "lkf1998")
        _add_classification(conn, "LKF2020", "Län och kommuner 2020", "lkf2020")
        counts = materialize_concept_groups(conn)
        assert counts["vintage_groups"] == 1
        groups = _groups(conn)
        assert groups["lkf"]["kind"] == "classification"
        assert groups["lkf"]["register_id"] is None
        assert groups["lkf"]["label"] == "Län och kommuner"
        assert groups["lkf"]["cls_members"] == [
            ("lkf1980", "1980", "1980"),
            ("lkf1998", "1998", "1998"),
            ("lkf2020", "2020", "2020"),
        ]

    def test_year_mid_name_strips_to_agreed_label(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "SG2000", "SUN 2000 — Grupper", "sun-grupp2000")
        _add_classification(conn, "SG2020", "SUN 2020 — Grupper", "sun-grupp2020")
        counts = materialize_concept_groups(conn)
        assert counts["vintage_groups"] == 1
        assert _groups(conn)["sun-grupp"]["label"] == "SUN — Grupper"

    def test_singleton_or_non_year_tails_do_not_fold(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "ISCED2011", "ISCED 2011", "isced2011")  # singleton
        _add_classification(conn, "NG1", "Nivå grov", "niva-grovv1")  # non-year tail
        _add_classification(conn, "NG2", "Nivå old", "niva-oldv1")
        assert materialize_concept_groups(conn)["vintage_groups"] == 0

    def test_name_missing_year_blocks_fold(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(conn, "X2000", "Standard X", "x2000")  # no "2000" in name
        _add_classification(conn, "X2020", "Standard X 2020", "x2020")
        assert materialize_concept_groups(conn)["vintage_groups"] == 0


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
    def test_absorbs_token_groups_into_facet_matrix(self) -> None:
        conn = _month_family_db()
        curated = _curated(
            (
                CuratedMember(
                    group="agi1ink", variable=None, value="1", label="största"
                ),
                CuratedMember(group="agi2ink", variable=None, value="2", label="näst"),
            )
        )
        counts = materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert counts["curated_groups"] == 1
        assert counts["month_groups"] == 0  # both token groups absorbed
        groups = _groups(conn)
        assert groups["agiink"]["source"] == "curated"
        assert len(groups["agiink"]["members"]) == 6
        # Absorbed members keep the month facet and gain the rank facet.
        assert _facets(conn, "agi1inkjan") == [
            ("month", "01", "januari"),
            ("rank", "1", "största"),
        ]
        assert _facets(conn, "agi2inkmars") == [
            ("month", "03", "mars"),
            ("rank", "2", "näst"),
        ]

    def test_single_variable_members_attach(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=700, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=701, name="B", slug="varb")
        curated = _curated(
            (
                CuratedMember(group=None, variable="vara", value="1", label="A"),
                CuratedMember(group=None, variable="varb", value="2", label="B"),
            ),
            key="fam",
            axis="part",
        )
        counts = materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert counts["curated_groups"] == 1
        assert _groups(conn)["fam"]["members"] == ["vara", "varb"]
        assert _facets(conn, "vara") == [("part", "1", "A")]

    def test_inactive_provider_is_skipped(self) -> None:
        conn = _month_family_db()
        curated = _curated(
            (CuratedMember(group="agi1ink", variable=None, value="1", label="x"),),
        )
        counts = materialize_concept_groups(
            conn, (curated,), providers=frozenset({"sos"})
        )
        assert counts["curated_groups"] == 0
        assert counts["month_groups"] == 2  # token groups untouched

    @pytest.mark.parametrize(
        ("members", "register"),
        [
            # dangling token-group reference
            (
                (CuratedMember(group="nope", variable=None, value="1", label="x"),),
                "lisa",
            ),
            # dangling variable reference
            (
                (
                    CuratedMember(group=None, variable="nope", value="1", label="x"),
                    CuratedMember(group=None, variable="nope2", value="2", label="y"),
                ),
                "lisa",
            ),
            # dangling register
            (
                (CuratedMember(group="agi1ink", variable=None, value="1", label="x"),),
                "no",
            ),
        ],
    )
    def test_dangling_references_fail_fast(
        self, members: tuple[CuratedMember, ...], register: str
    ) -> None:
        conn = _month_family_db()
        curated = _curated(members, register=register)
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_unresolved"

    def test_already_grouped_variable_member_fails(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=710, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=711, name="A", slug="varb")
        add_variable(conn, register_id=1, var_id=712, name="C", slug="varc")
        _add_edge(conn, "lisa", "vara", "varb")  # edge pass claims vara/varb
        curated = _curated(
            (
                CuratedMember(group=None, variable="vara", value="1", label="A"),
                CuratedMember(group=None, variable="varc", value="2", label="C"),
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
            (CuratedMember(group=None, variable="vara", value="1", label="A"),),
            key="fam",
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert exc.value.code == "concept_groups_unresolved"


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
            group = "agi1ink"
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
        assert groups[0].members[0].group == "agi1ink"
        assert groups[0].members[1].variable == "extra"

    @pytest.mark.parametrize(
        "text",
        [
            "[[variable_groups]]\n",  # misspelled top-level table
            # both group and variable on one member
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "F"
            axis = "a"
            [[variable_group.members]]
            group = "g"
            variable = "v"
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
            group = "g"
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
            group = "g"
            value = "1"
            label = "x"
            [[variable_group.members]]
            group = "g"
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
        group = "g"
        value = "1"
        label = "x"
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "F2"
        axis = "b"
        [[variable_group.members]]
        group = "h"
        value = "1"
        label = "x"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    def test_accept_sibling_does_not_trip_top_level_guard(self, tmp_path) -> None:
        # A `[[variable_group]]` and an `[[accept]]` coexist in one file: the
        # variable_group parse must treat `[[accept]]` as a legal sibling, not an
        # unknown-top-level typo. (Case (g).)
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
        CuratedMember(group=None, variable=f"morsak{i}", value=str(i), label=str(i))
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
                CuratedMember(group=None, variable="vara", value="1", label="A"),
                CuratedMember(group=None, variable="varb", value="2", label="B"),
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
        # `morsak1` is claimed by an edge group BEFORE the accept resolves (the
        # real footgun: the auto.toml folds against a LATER build whose edge/token
        # pass newly claimed a member). The error must point at the `[[accept]]` /
        # auto.toml, NOT at "pick a curated key".
        conn = _morsak_db()
        add_variable(conn, register_id=1, var_id=850, name="Dödsorsak 1", slug="dxsib")
        _add_edge(conn, "lisa", "morsak1", "dxsib")  # edge pass claims morsak1
        auto = (_auto_family(_morsak_members()),)
        accepts = (Accept("scb", "lisa", "morsak", None, None, ()),)
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
        # The accept loader must treat `[[variable_group]]` as a legal sibling.
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
