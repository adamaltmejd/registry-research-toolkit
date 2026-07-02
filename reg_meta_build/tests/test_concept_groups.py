"""Tests for the derived concept-group layer (#303; `concept_groups.py`).

Covers the three derivation dimensions against hand-curated slugged DBs
(`_slugged_db`): edge components from the in-build sibling sets (`edge_siblings`,
dimension 0 — never a shipped-table round-trip), with curated
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
from _slugged_db import (
    add_register,
    add_state,
    add_value_set,
    add_variable,
    build_slugged_db,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.concept_groups import (
    Accept,
    ClassificationGroup,
    ClassificationGroupMember,
    CodeLabelPair,
    CuratedGroup,
    CuratedMember,
    _insert_group,
    derive_classification_succession,
    load_classification_groups,
    load_code_label_pairs,
    load_concept_group_accepts,
    load_concept_groups,
    materialize_concept_groups,
)
from reg_meta_build.db import _append_code_label_edges

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


def _add_alias(
    conn: sqlite3.Connection,
    slug: str,
    delivery_column_name: str,
    register_variant_id: int = 10,
) -> None:
    """Add a `variable_alias` row for the variable with this slug — so a
    representation member's `delivery_column` resolves at materialize time (#819).
    The default variant id matches `build_slugged_db`'s `_DEFAULT_VARIANT`."""
    variable_id = conn.execute(
        "SELECT variable_id FROM variable WHERE slug = ?", (slug,)
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
        (variable_id, register_variant_id, delivery_column_name),
    )


def _member(variable: str, axis: str, value: str, label: str) -> CuratedMember:
    """A single-axis whole-variable `CuratedMember` (#819): one coord on the
    family's single `axis`, `delivery_column = None`. The coord axis MUST match the
    group's declared axis (the materializer inserts coords verbatim)."""
    return CuratedMember(
        variable=variable, delivery_column=None, coords=((axis, value, label),)
    )


def _axis1(name: str) -> tuple[tuple[str, str], ...]:
    """A single-axis group's `axes` (legacy shape: label == axis name)."""
    return ((name, name),)


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
            "axes": _group_axes(conn, g["group_id"]),
            "members": members,
            "cls_members": cls_members,
        }
    return out


def _group_id(conn: sqlite3.Connection, group_key: str) -> int:
    """The `group_id` of the concept group with this key."""
    return conn.execute(
        "SELECT group_id FROM concept_group WHERE group_key = ?", (group_key,)
    ).fetchone()[0]


def _group_axes(conn: sqlite3.Connection, group_id: int) -> list[tuple[str, str]]:
    """A group's ordered `(axis, label)` declarations from `concept_group_axis`
    (#819). Empty for an axis-less group (edge / classification umbrella)."""
    return [
        (r["axis"], r["label"])
        for r in conn.execute(
            "SELECT axis, label FROM concept_group_axis "
            "WHERE group_id = ? ORDER BY ordinal",
            (group_id,),
        )
    ]


def _facets(conn: sqlite3.Connection, slug: str) -> list[tuple[str, str, str]]:
    """A variable member's per-axis facets as (axis, value, label) (#819): the
    member is keyed on `concept_group_variable`, its coords on
    `concept_group_variable_facet`. Ordered by the axis's ordinal. Empty when the
    member carries no facet (edge group). NOTE: a representation-grained family can
    carry two members on one slug — use `_facets_for` to disambiguate by delivery
    column."""
    return [
        (r["axis"], r["value"], r["label"])
        for r in conn.execute(
            "SELECT f.axis, f.value, f.label "
            "FROM concept_group_variable_facet f "
            "JOIN concept_group_variable m ON m.member_id = f.member_id "
            "JOIN variable v ON v.variable_id = m.variable_id "
            "JOIN concept_group_axis a ON a.group_id = m.group_id AND a.axis = f.axis "
            "WHERE v.slug = ? ORDER BY a.ordinal",
            (slug,),
        )
    ]


def _facets_for(
    conn: sqlite3.Connection, slug: str, delivery_column: str
) -> list[tuple[str, str, str]]:
    """The per-axis facets of the `(slug, delivery_column)` representation member
    (#819), ordered by axis ordinal."""
    return [
        (r["axis"], r["value"], r["label"])
        for r in conn.execute(
            "SELECT f.axis, f.value, f.label "
            "FROM concept_group_variable_facet f "
            "JOIN concept_group_variable m ON m.member_id = f.member_id "
            "JOIN variable v ON v.variable_id = m.variable_id "
            "JOIN concept_group_axis a ON a.group_id = m.group_id AND a.axis = f.axis "
            "WHERE v.slug = ? AND m.delivery_column_name = ? ORDER BY a.ordinal",
            (slug, delivery_column),
        )
    ]


class TestEdgeGroups:
    """Edge groups fold the IN-BUILD split-sibling pairs passed as `edge_siblings`
    (variable_id pairs) the triage minted — never rows read back from a shipped
    table."""

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
            "axes": [],  # edge groups have no axis
            "members": ["sun2000", "sun2020", "sunx"],
            "cls_members": [],
        }
        # Edge members carry no facets — the member list is the presentation.
        assert _facets(conn, "sun2000") == []

    def test_no_siblings_mints_no_group(self) -> None:
        # With no sibling pairs the edge fold mints nothing — the only edge-group
        # input is the triage's in-build sibling set.
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
            axes=_axis1("part"),
            members=(
                _member("vara", "part", "1", "A"),
                _member("varc", "part", "2", "C"),
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
            axes=_axis1("part"),
            members=(
                _member("sunx", "part", "1", "x"),
                _member("other", "part", "2", "o"),
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
            axes=_axis1("part"),
            members=(
                _member("varx", "part", "1", "x"),
                _member("other", "part", "2", "o"),
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

    @pytest.mark.parametrize(
        "good_key",
        # `foo-bar2` is also a slug; the trailing-hyphen keys are NOT slugs but ARE
        # path-safe — they are real `concept_groups.auto.toml` candidates the
        # generator emits, so accepting them is the regression guard for the P2
        # (an `is_slug` guard would over-reject them and break `[[accept]]`).
        ["foo-bar2", "artal-person-", "betyg-i-franska-"],
    )
    def test_path_safe_key_is_accepted(self, good_key: str) -> None:
        # A path-safe key inserts cleanly — the guard is not over-broad and does
        # not narrow path-safety to `is_slug`.
        conn = build_slugged_db(classification=None)
        group_id = _insert_group(
            conn,
            kind="variable",
            register_id=1,
            group_key=good_key,
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

    def test_trailing_hyphen_trimmed_from_month_key(self) -> None:
        # Slugs `inkomst-jan/feb/mars` strip to stem `inkomst-`; the group's URL key
        # is the trailing-hyphen-trimmed `inkomst` (#645), not the dangling form.
        conn = build_slugged_db(classification=None)
        self._family(conn, "inkomst-", ["jan", "feb", "mars"])
        counts = materialize_concept_groups(conn)
        assert counts["month_groups"] == 1
        groups = _groups(conn)
        assert "inkomst" in groups
        assert groups["inkomst"]["source"] == "token"
        assert groups["inkomst"]["members"] == [
            "inkomst-feb",
            "inkomst-jan",
            "inkomst-mars",
        ]

    def test_month_trim_collision_skipped_and_warned(self) -> None:
        # Two distinct month families whose stems TRIM to one key: `ink-jan/feb/mars`
        # (raw stem `ink-`) and `inkjan/feb/mars` (raw stem `ink`), both → key `ink`.
        # Folding both would silently merge unrelated families — skip and WARN, never
        # crash on the unique key index, never merge.
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink-", ["jan", "feb", "mars"])
        self._family(
            conn,
            "ink",
            ["jan", "feb", "mars"],
            name_fmt="Annan {tok}, totalt",
            register_id=1,
        )
        warnings: list[str] = []
        counts = materialize_concept_groups(conn, warn=warnings.append)
        assert counts["month_groups"] == 0
        assert _groups(conn) == {}
        assert any("collapsed two distinct stems" in w for w in warnings)

    def test_noise_singleton_does_not_suppress_month_fold(self) -> None:
        # A valid month family `ink-jan/feb/mars` (raw stem `ink-`) shares the trimmed
        # key `ink` with an unrelated SINGLETON `inkjan` (raw stem `ink`, one month —
        # not itself a fold). The coarse "> 1 raw stem" check would skip the whole
        # bucket and suppress the production fold; the refined check counts only raw
        # stems that independently satisfy the month-sibling floor, so only `ink-`
        # qualifies → the month fold IS produced and the singleton is dropped, with no
        # collision warning (#645).
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink-", ["jan", "feb", "mars"])
        add_variable(
            conn, register_id=1, var_id=540, name="Annan januari", slug="inkjan"
        )
        warnings: list[str] = []
        counts = materialize_concept_groups(conn, warn=warnings.append)
        assert counts["month_groups"] == 1
        groups = _groups(conn)
        assert "ink" in groups
        assert groups["ink"]["members"] == ["ink-feb", "ink-jan", "ink-mars"]
        assert not any("collapsed two distinct stems" in w for w in warnings)

    def test_disagreeing_label_peer_does_not_suppress_month_fold(self) -> None:
        # A valid month family `ink-jan/feb/mars` (raw stem `ink-`, agreeing labels)
        # shares the trimmed key `ink` with a count-qualifying PEER trio
        # `inkjan/inkfeb/inkmars` (raw stem `ink`, 3 distinct months but DISAGREEING
        # labels → would never fold). The buggy collision predicate counted the peer
        # as a competing family on the MONTH FLOOR alone and skip-and-warned, dropping
        # the valid fold. The qualification now mirrors the full fold predicate, so the
        # disagreeing peer doesn't count → the `ink` month fold IS produced, no warning
        # (Codex P2 #646).
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink-", ["jan", "feb", "mars"])
        add_variable(conn, register_id=1, var_id=540, name="Aaaa x", slug="inkjan")
        add_variable(conn, register_id=1, var_id=541, name="Bbbb y", slug="inkfeb")
        add_variable(conn, register_id=1, var_id=542, name="Cccc z", slug="inkmars")
        warnings: list[str] = []
        counts = materialize_concept_groups(conn, warn=warnings.append)
        assert counts["month_groups"] == 1
        groups = _groups(conn)
        assert "ink" in groups
        assert groups["ink"]["members"] == ["ink-feb", "ink-jan", "ink-mars"]
        assert not any("collapsed two distinct stems" in w for w in warnings)

    def test_null_label_peer_does_not_suppress_month_fold(self) -> None:
        # Same shape, but the count-qualifying peer trio `inkjan/inkfeb/inkmars` (raw
        # stem `ink`) has a NULL member name → no labels to agree on, never folds. It
        # must NOT count as a competing family: the valid `ink-jan/feb/mars` fold is
        # still produced, no collision warning (Codex P2 #646).
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink-", ["jan", "feb", "mars"])
        add_variable(conn, register_id=1, var_id=540, name="Lön i jan", slug="inkjan")
        add_variable(conn, register_id=1, var_id=541, name=None, slug="inkfeb")
        add_variable(conn, register_id=1, var_id=542, name="Lön i mars", slug="inkmars")
        warnings: list[str] = []
        counts = materialize_concept_groups(conn, warn=warnings.append)
        assert counts["month_groups"] == 1
        groups = _groups(conn)
        assert "ink" in groups
        assert groups["ink"]["members"] == ["ink-feb", "ink-jan", "ink-mars"]
        assert not any("collapsed two distinct stems" in w for w in warnings)

    def test_hyphen_only_month_stem_not_folded(self) -> None:
        # Slugs whose only non-token prefix is a hyphen (`-jan`/`-feb`/`-mars`) trim
        # to an EMPTY stem → no fold, no empty/invalid group key minted.
        conn = build_slugged_db(classification=None)
        self._family(conn, "-", ["jan", "feb", "mars"])
        counts = materialize_concept_groups(conn)
        assert counts["month_groups"] == 0
        assert _groups(conn) == {}

    def test_month_key_collision_with_pending_curated_key_skipped(self) -> None:
        # Pre-reservation guard (#651): a month family `ink-jan/feb/mars` (raw stem
        # `ink-`) trims to key `ink`, which collides with a PENDING curated
        # `[[variable_group]]` keyed `ink` that `_apply_curated_groups` inserts LATER.
        # The month pass runs first, so without reserving the pending curated keys it
        # would insert `ink` here and then crash the curated insert on
        # `idx_concept_group_key` (the pre-trim key `ink-` would not have collided).
        # The reservation turns it into the existing skip-and-warn instead, so the
        # curated insert on `ink` succeeds.
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink-", ["jan", "feb", "mars"])  # trims to key `ink`
        add_variable(conn, register_id=1, var_id=560, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=561, name="B", slug="varb")
        curated = CuratedGroup(
            provider="scb",
            register="lisa",
            key="ink",  # collides with the month family's trimmed key
            label="Inkomst",
            axes=_axis1("part"),
            members=(
                _member("vara", "part", "1", "A"),
                _member("varb", "part", "2", "B"),
            ),
        )
        warnings: list[str] = []
        counts = materialize_concept_groups(
            conn, (curated,), providers=_SCB, warn=warnings.append
        )
        # The month family is NOT folded (its key is reserved); the curated group on
        # the same key inserts cleanly (no IntegrityError).
        assert counts["month_groups"] == 0
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        assert groups["ink"]["source"] == "curated"
        assert groups["ink"]["members"] == ["vara", "varb"]
        assert any("collides with an existing group key" in w for w in warnings)

    def test_month_key_no_collision_with_unrelated_curated_key_folds(self) -> None:
        # Control: the SAME month family folds normally when the pending curated key
        # does NOT collide with its trimmed key — the reservation guard only suppresses
        # an actual key collision, never a non-colliding month fold (#651).
        conn = build_slugged_db(classification=None)
        self._family(conn, "ink-", ["jan", "feb", "mars"])  # trims to key `ink`
        add_variable(conn, register_id=1, var_id=560, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=561, name="B", slug="varb")
        curated = CuratedGroup(
            provider="scb",
            register="lisa",
            key="other",  # distinct from the month key `ink`
            label="Annan",
            axes=_axis1("part"),
            members=(
                _member("vara", "part", "1", "A"),
                _member("varb", "part", "2", "B"),
            ),
        )
        warnings: list[str] = []
        counts = materialize_concept_groups(
            conn, (curated,), providers=_SCB, warn=warnings.append
        )
        assert counts["month_groups"] == 1
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        assert groups["ink"]["source"] == "token"
        assert groups["ink"]["members"] == ["ink-feb", "ink-jan", "ink-mars"]
        assert "other" in groups
        assert not any("collides with an existing group key" in w for w in warnings)


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

    def test_sni2025_extends_sni_year_tail_chain(self) -> None:
        conn = build_slugged_db(classification=None)
        _add_classification(
            conn,
            "SNI2002",
            "Svensk standard för näringsgrensindelning 2002",
            "sni2002",
        )
        _add_classification(
            conn,
            "SNI2007",
            "Svensk standard för näringsgrensindelning 2007",
            "sni2007",
        )
        _add_classification(
            conn,
            "SNI2025",
            "Svensk standard för näringsgrensindelning 2025",
            "sni2025",
        )
        assert derive_classification_succession(conn) == 2
        assert _succession_edges(conn) == [
            ("sni2002", "sni2007", 2007, "derived:vintage_chain"),
            ("sni2007", "sni2025", 2025, "derived:vintage_chain"),
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
        "axes": _axis1("rank"),
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
                _member("vara", "rank", "1", "största"),
                _member("varb", "rank", "2", "näst"),
            )
        )
        counts = materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert counts["curated_groups"] == 1
        group = _groups(conn)["agiink"]
        assert group["source"] == "curated"
        assert group["members"] == ["vara", "varb"]
        # The group declares its single axis; each member carries one facet on it.
        assert group["axes"] == [("rank", "rank")]
        assert _facets(conn, "vara") == [("rank", "1", "största")]
        assert _facets(conn, "varb") == [("rank", "2", "näst")]

    def test_axisless_variable_umbrella_has_no_facets(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=702, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=703, name="B", slug="varb")
        curated = _curated(
            (
                CuratedMember(variable="vara", delivery_column=None, coords=()),
                CuratedMember(variable="varb", delivery_column=None, coords=()),
            ),
            key="umbrella",
            axes=(),
        )
        counts = materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert counts["curated_groups"] == 1
        group = _groups(conn)["umbrella"]
        assert group["axes"] == []
        assert group["members"] == ["vara", "varb"]
        assert _facets(conn, "vara") == []

    def test_multi_axis_whole_variable_member_has_facets_without_column(self) -> None:
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=704, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=705, name="B", slug="varb")
        curated = _curated(
            (
                CuratedMember(
                    variable="vara",
                    delivery_column=None,
                    coords=(
                        ("source", "a", "A"),
                        ("rank", "1", "största"),
                    ),
                ),
                CuratedMember(
                    variable="varb",
                    delivery_column=None,
                    coords=(
                        ("source", "b", "B"),
                        ("rank", "2", "näst"),
                    ),
                ),
            ),
            key="multi-whole",
            axes=(("source", "Källa"), ("rank", "Förvärvskälla")),
        )
        counts = materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert counts["curated_groups"] == 1
        group = _groups(conn)["multi-whole"]
        assert group["axes"] == [("source", "Källa"), ("rank", "Förvärvskälla")]
        assert _facets(conn, "vara") == [
            ("source", "a", "A"),
            ("rank", "1", "största"),
        ]

    def test_inactive_provider_is_skipped(self) -> None:
        conn = _month_family_db()
        add_variable(conn, register_id=1, var_id=730, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=731, name="B", slug="varb")
        curated = _curated(
            (
                _member("vara", "rank", "1", "x"),
                _member("varb", "rank", "2", "y"),
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
                    _member("nope", "rank", "1", "x"),
                    _member("nope2", "rank", "2", "y"),
                ),
                "lisa",
            ),
            # dangling register
            (
                (
                    _member("vara", "rank", "1", "x"),
                    _member("varb", "rank", "2", "y"),
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
                _member("inkjan", "rank", "1", "A"),
                _member("varc", "rank", "2", "C"),
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
            (_member("vara", "rank", "1", "A"),),
            key="fam",
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (curated,), providers=_SCB)
        assert exc.value.code == "concept_groups_unresolved"


class TestMultiAxisGroups:
    """#819: curated families can declare N named axes over REPRESENTATION members
    (`(variable, delivery_column)`), so one variable can carry two coordinates."""

    @staticmethod
    def _iot_db() -> sqlite3.Connection:
        """scb/lisa with a one-variable-two-representation family + a hushåll
        variable, each delivery column registered in `variable_alias`."""
        conn = build_slugged_db(classification=None)
        add_variable(conn, register_id=1, var_id=900, name="Disp ink", slug="cdisp")
        _add_alias(conn, "cdisp", "CDISP")
        _add_alias(conn, "cdisp", "CDISP5")  # second representation of one variable
        add_variable(conn, register_id=1, var_id=901, name="Disp fam", slug="cdisph")
        _add_alias(conn, "cdisph", "CDISPH")
        return conn

    @staticmethod
    def _group(**overrides) -> CuratedGroup:
        kwargs: dict = {
            "provider": "scb",
            "register": "lisa",
            "key": "disp",
            "label": "Disponibel inkomst",
            "axes": (
                ("enhet", "Enhet"),
                ("hushallsbegrepp", "Hushållsbegrepp"),
                ("kapitalvinst", "Kapitalvinst"),
            ),
            "members": (
                CuratedMember(
                    variable="cdisp",
                    delivery_column="CDISP",
                    coords=(
                        ("enhet", "individ", "Individ"),
                        ("hushallsbegrepp", "na", "—"),
                        ("kapitalvinst", "inkl", "Inkl. kapitalvinst"),
                    ),
                ),
                CuratedMember(
                    variable="cdisp",
                    delivery_column="CDISP5",
                    coords=(
                        ("enhet", "individ", "Individ"),
                        ("hushallsbegrepp", "na", "—"),
                        ("kapitalvinst", "exkl", "Exkl. kapitalvinst"),
                    ),
                ),
                CuratedMember(
                    variable="cdisph",
                    delivery_column="CDISPH",
                    coords=(
                        ("enhet", "hushall", "Hushåll"),
                        ("hushallsbegrepp", "familj", "Familj"),
                        ("kapitalvinst", "inkl", "Inkl. kapitalvinst"),
                    ),
                ),
            ),
        }
        kwargs.update(overrides)
        return CuratedGroup(**kwargs)

    def test_materializes_n_axes_and_n_facets_per_member(self) -> None:
        conn = self._iot_db()
        counts = materialize_concept_groups(conn, (self._group(),), providers=_SCB)
        assert counts["curated_groups"] == 1
        group = _groups(conn)["disp"]
        assert group["axes"] == [
            ("enhet", "Enhet"),
            ("hushallsbegrepp", "Hushållsbegrepp"),
            ("kapitalvinst", "Kapitalvinst"),
        ]
        # Each member carries one facet per axis, ordered by axis ordinal.
        assert _facets_for(conn, "cdisp", "CDISP") == [
            ("enhet", "individ", "Individ"),
            ("hushallsbegrepp", "na", "—"),
            ("kapitalvinst", "inkl", "Inkl. kapitalvinst"),
        ]

    def test_two_representations_of_one_variable_coexist(self) -> None:
        # The load-bearing case: `cdisp` is one variable with two coordinates
        # (CDISP incl., CDISP5 excl.). The surrogate PK + COALESCE unique index let
        # both members coexist under one group.
        conn = self._iot_db()
        materialize_concept_groups(conn, (self._group(),), providers=_SCB)
        cdisp_members = conn.execute(
            "SELECT m.delivery_column_name FROM concept_group_variable m "
            "JOIN variable v ON v.variable_id = m.variable_id "
            "WHERE v.slug = 'cdisp' ORDER BY m.delivery_column_name"
        ).fetchall()
        assert [r[0] for r in cdisp_members] == ["CDISP", "CDISP5"]
        assert _facets_for(conn, "cdisp", "CDISP5") == [
            ("enhet", "individ", "Individ"),
            ("hushallsbegrepp", "na", "—"),
            ("kapitalvinst", "exkl", "Exkl. kapitalvinst"),
        ]

    def test_rejects_mixed_whole_and_representation_grain_per_variable(self) -> None:
        conn = self._iot_db()
        bad = self._group(
            members=(
                CuratedMember(
                    variable="cdisp",
                    delivery_column=None,
                    coords=(
                        ("enhet", "individ", "Individ"),
                        ("hushallsbegrepp", "na", "—"),
                        ("kapitalvinst", "alla", "Alla"),
                    ),
                ),
                CuratedMember(
                    variable="cdisp",
                    delivery_column="CDISP",
                    coords=(
                        ("enhet", "individ", "Individ"),
                        ("hushallsbegrepp", "na", "—"),
                        ("kapitalvinst", "inkl", "Inkl. kapitalvinst"),
                    ),
                ),
            )
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (bad,), providers=_SCB)
        assert exc.value.code == "concept_groups_invalid"

    def test_bad_delivery_column_fails_fast(self) -> None:
        # A delivery column not in `variable_alias` is a curation typo → EXIT_CONFIG.
        conn = self._iot_db()
        bad = self._group(
            members=(
                CuratedMember(
                    variable="cdisp",
                    delivery_column="NOPE",
                    coords=(
                        ("enhet", "individ", "Individ"),
                        ("hushallsbegrepp", "na", "—"),
                        ("kapitalvinst", "inkl", "Inkl. kapitalvinst"),
                    ),
                ),
                CuratedMember(
                    variable="cdisph",
                    delivery_column="CDISPH",
                    coords=(
                        ("enhet", "hushall", "Hushåll"),
                        ("hushallsbegrepp", "familj", "Familj"),
                        ("kapitalvinst", "inkl", "Inkl. kapitalvinst"),
                    ),
                ),
            ),
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (bad,), providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "concept_groups_unresolved"

    def test_representation_member_spanning_two_groups_fails(self) -> None:
        # The one-group-per-variable guard: a variable used as a representation
        # member in one group can't also be claimed by a second group.
        conn = self._iot_db()
        add_variable(conn, register_id=1, var_id=902, name="Other", slug="vother")
        _add_alias(conn, "vother", "VOTHER")
        second = CuratedGroup(
            provider="scb",
            register="lisa",
            key="disp2",
            label="Other",
            axes=_axis1("enhet"),
            members=(
                _member("cdisp", "enhet", "individ", "Individ"),
                _member("vother", "enhet", "hushall", "Hushåll"),
            ),
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_concept_groups(conn, (self._group(), second), providers=_SCB)
        assert exc.value.code == "concept_groups_unresolved"


def _sun_group(**overrides) -> ClassificationGroup:
    """A SUN-like umbrella over distinct classifications. AXIS-LESS by default
    (`axis=None`, mirroring the real curation) — each member still carries its
    own short value/label. Pass `axis="..."` to exercise the still-accepted
    explicit-axis path."""
    kwargs: dict = {
        "key": "sun",
        "label": "Svensk utbildningsnomenklatur (SUN)",
        "axis": None,
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

    def test_resolves_to_axis_less_group(self) -> None:
        # The umbrella is axis-less: facet_axis stores NULL, but every member
        # still carries its own short value/label (the picker label).
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
        # facet_value-ordered members keep their inline value/label.
        assert group["cls_members"] == [
            ("sun-grupp2020", "grupp", "Utbildningsgrupper"),
            ("sun-inriktning2020", "inriktning", "Utbildningsinriktning"),
            ("sun-niva2020", "niva", "Utbildningsnivå"),
        ]
        # Axis-less: zero `concept_group_axis` rows (#819).
        assert _group_axes(conn, _group_id(conn, "sun")) == []

    def test_explicit_axis_still_stored(self) -> None:
        # A provided axis is still accepted and stored as ONE `concept_group_axis`
        # row (#819); members still carry their inline value/label.
        conn = self._db()
        materialize_concept_groups(
            conn,
            classification_groups=(_sun_group(axis="dimension"),),
            providers=_SCB,
        )
        assert _group_axes(conn, _group_id(conn, "sun")) == [("dimension", "dimension")]

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
        # A provided axis is still accepted (the loader does not require it).
        assert groups[0].axis == "dimension"
        assert [m.classification for m in groups[0].members] == [
            "sun-niva2020",
            "sun-grupp2020",
        ]

    def test_axis_is_optional(self, tmp_path) -> None:
        # axis is now optional — an umbrella with no `axis` loads with axis=None
        # (axis-less), members keeping their value/label.
        groups = self._load(
            tmp_path,
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
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
        assert groups[0].axis is None
        assert groups[0].members[0].value == "niva"

    @pytest.mark.parametrize(
        "text",
        [
            # blank axis (present-but-blank is still drift, unlike absent)
            """
            [[classification_group]]
            key = "sun"
            label = "SUN"
            axis = ""
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

    def test_parses_axisless_variable_umbrella(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axes = []
            [[variable_group.members]]
            variable = "vara"
            [[variable_group.members]]
            variable = "varb"
            """,
        )
        assert groups[0].axes == ()
        assert groups[0].members == (
            CuratedMember(variable="vara", delivery_column=None, coords=()),
            CuratedMember(variable="varb", delivery_column=None, coords=()),
        )

    def test_parses_single_explicit_axis_with_flat_member_facets(
        self, tmp_path
    ) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axes = [{ axis = "rank", label = "Förvärvskälla" }]
            [[variable_group.members]]
            variable = "agi1ink"
            value = "1"
            label = "största"
            [[variable_group.members]]
            variable = "agi2ink"
            value = "2"
            label = "näst"
            """,
        )
        assert groups[0].axes == (("rank", "Förvärvskälla"),)
        assert groups[0].members[0].coords == (("rank", "1", "största"),)

    def test_parses_multi_axis_whole_variable_member(self, tmp_path) -> None:
        groups = self._load(
            tmp_path,
            """
            [[variable_group]]
            register = "scb/lisa"
            key = "fam"
            label = "Familj"
            axes = [
              { axis = "source", label = "Källa" },
              { axis = "rank", label = "Förvärvskälla" },
            ]
            [[variable_group.members]]
            variable = "agi1ink"
            coords = [
              { axis = "source", value = "agi", label = "AGI" },
              { axis = "rank", value = "1", label = "största" },
            ]
            [[variable_group.members]]
            variable = "ku1ink"
            coords = [
              { axis = "source", value = "ku", label = "KU" },
              { axis = "rank", value = "1", label = "största" },
            ]
            """,
        )
        assert groups[0].members[0].delivery_column is None
        assert groups[0].members[0].coords == (
            ("source", "agi", "AGI"),
            ("rank", "1", "största"),
        )

    def test_rejects_mixed_whole_and_representation_grain_per_variable(
        self, tmp_path
    ) -> None:
        text = """
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "Familj"
        axes = [
          { axis = "source", label = "Källa" },
          { axis = "rank", label = "Förvärvskälla" },
        ]
        [[variable_group.members]]
        variable = "agi1ink"
        coords = [
          { axis = "source", value = "agi", label = "AGI" },
          { axis = "rank", value = "all", label = "Alla" },
        ]
        [[variable_group.members]]
        variable = "agi1ink"
        delivery_column = "AGI1INK"
        coords = [
          { axis = "source", value = "agi", label = "AGI" },
          { axis = "rank", value = "1", label = "största" },
        ]
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

    @pytest.mark.parametrize(
        "member_fields",
        [
            'value = "1"\nlabel = "x"',
            'coords = [{ axis = "rank", value = "1", label = "x" }]',
            'delivery_column = "COL"',
        ],
    )
    def test_axisless_variable_umbrella_rejects_facet_fields(
        self, tmp_path, member_fields: str
    ) -> None:
        text = f"""
        [[variable_group]]
        register = "scb/lisa"
        key = "fam"
        label = "Familj"
        axes = []
        [[variable_group.members]]
        variable = "vara"
        {member_fields}
        [[variable_group.members]]
        variable = "varb"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.code == "concept_groups_invalid"

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
        axes=_axis1(axis),
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
    return tuple(_member(f"morsak{i}", "ordinal", str(i), str(i)) for i in (1, 2, 3))


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
            axes=_axis1("part"),
            members=(
                _member("vara", "part", "1", "A"),
                _member("varb", "part", "2", "B"),
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
                    _member("inkjan", "ordinal", "1", "jan"),
                    _member("inkextra", "ordinal", "2", "extra"),
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


# ── code↔label pairs (#923) ─────────────────────────────────────────────────


def _code_label_db(
    *,
    code_slug: str = "partikod",
    label_slug: str = "partinamn",
    code_has_value_set: bool = True,
    label_has_value_set: bool = False,
    co_delivered: bool = True,
):
    """Synthetic scb/lisa DB with a coded variable + its label variable, both
    co-delivered in variant 10 (the fixture default). The code variable's state
    carries a `value_set_id`; the label variable's does not. Toggles flip each
    structural-guard precondition so the guard tests can drive a single failure."""
    conn = build_slugged_db(classification=None)
    add_value_set(conn, value_set_id=500, codes=[("01", "A"), ("02", "B")])
    add_variable(conn, register_id=1, var_id=200, name="Parti", slug=code_slug)
    add_variable(conn, register_id=1, var_id=201, name="Partinamn", slug=label_slug)
    add_state(
        conn,
        register_id=1,
        variable_slug=code_slug,
        register_variant_id=10,
        delivery_column_name="Partikod",
        value_set_id=500 if code_has_value_set else None,
    )
    # The label variable's co-delivery is the SHARED register_variant_id with the
    # code variable's state; flip to a different variant to break it.
    add_state(
        conn,
        register_id=1,
        variable_slug=label_slug,
        register_variant_id=10 if co_delivered else 11,
        delivery_column_name="Partinamn",
        value_set_id=500 if label_has_value_set else None,
    )
    conn.commit()
    return conn


def _pair(code_slug: str = "partikod", label_slug: str = "partinamn") -> CodeLabelPair:
    return CodeLabelPair(
        code_provider="scb",
        code_register="lisa",
        code_variable=code_slug,
        label_provider="scb",
        label_register="lisa",
        label_variable=label_slug,
    )


class TestCodeLabelPairs:
    """A curated code↔label pair (#923) feeds the edge `sibling_edges` channel so
    the code variable and its denormalized label column fold into ONE axis-less
    edge concept group."""

    def test_pair_folds_into_one_axisless_group(self) -> None:
        conn = _code_label_db()
        siblings: list[tuple[int, int]] = []
        _append_code_label_edges(conn, (_pair(),), _SCB, siblings)
        # One (code_vid, label_vid) edge appended.
        assert siblings == [(_vid(conn, "partikod"), _vid(conn, "partinamn"))]

        counts = materialize_concept_groups(conn, edge_siblings=siblings)
        assert counts["edge_groups"] == 1
        groups = _groups(conn)
        assert set(groups) == {"partikod"}
        g = groups["partikod"]
        assert g["source"] == "edge"
        assert g["kind"] == "variable"
        assert g["register_id"] == 1
        assert g["members"] == ["partikod", "partinamn"]
        # Axis-less: zero concept_group_axis rows, both members carry no facets and
        # a NULL delivery_column_name.
        assert g["axes"] == []
        assert _facets(conn, "partikod") == []
        assert _facets(conn, "partinamn") == []
        delivery_cols = [
            r[0]
            for r in conn.execute(
                "SELECT delivery_column_name FROM concept_group_variable"
            )
        ]
        assert delivery_cols == [None, None]

    def test_provider_not_in_build_is_skipped(self) -> None:
        # A partial --providers build that excludes the pair's provider can't
        # represent it — skip silently (no edge appended, no raise).
        conn = _code_label_db()
        siblings: list[tuple[int, int]] = []
        _append_code_label_edges(conn, (_pair(),), frozenset({"sos"}), siblings)
        assert siblings == []

    def test_label_provider_not_in_build_is_skipped(self) -> None:
        # The provider gate checks BOTH endpoints (mirroring materialize_same_as):
        # when only the LABEL provider is absent from the active set, the pair is
        # skipped silently — no EXIT_CONFIG raise, no edge appended. (The structural
        # guards never run, so a mismatched label endpoint can't trip them.)
        conn = _code_label_db()
        pair = CodeLabelPair("scb", "lisa", "partikod", "sos", "lisa", "partinamn")
        siblings: list[tuple[int, int]] = []
        _append_code_label_edges(conn, (pair,), _SCB, siblings)
        assert siblings == []

    def test_label_owning_value_set_fails(self) -> None:
        conn = _code_label_db(label_has_value_set=True)
        with pytest.raises(RegMetaError) as exc:
            _append_code_label_edges(conn, (_pair(),), _SCB, [])
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"

    def test_code_owning_no_value_set_fails(self) -> None:
        conn = _code_label_db(code_has_value_set=False)
        with pytest.raises(RegMetaError) as exc:
            _append_code_label_edges(conn, (_pair(),), _SCB, [])
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"

    def test_not_co_delivered_fails(self) -> None:
        conn = _code_label_db(co_delivered=False)
        with pytest.raises(RegMetaError) as exc:
            _append_code_label_edges(conn, (_pair(),), _SCB, [])
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"

    def test_dangling_code_fqid_fails(self) -> None:
        conn = _code_label_db()
        with pytest.raises(RegMetaError) as exc:
            _append_code_label_edges(conn, (_pair(code_slug="nope"),), _SCB, [])
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_unresolved"

    def test_dangling_label_fqid_fails(self) -> None:
        conn = _code_label_db()
        with pytest.raises(RegMetaError) as exc:
            _append_code_label_edges(conn, (_pair(label_slug="nope"),), _SCB, [])
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_unresolved"

    def test_two_pairs_sharing_label_fold_into_one_group(self) -> None:
        # The real shape of the shipped landsting/glyfosat/vaxtskyddsmedel clusters:
        # two coded variables (each owns a value_set) co-delivered with ONE shared
        # denormalized label column. Two pairs share the label endpoint, so the edge
        # component builder unions them into ONE axis-less 3-member group.
        conn = build_slugged_db(classification=None)
        add_value_set(conn, value_set_id=500, codes=[("01", "A"), ("02", "B")])
        add_value_set(conn, value_set_id=501, codes=[("11", "X"), ("12", "Y")])
        add_variable(conn, register_id=1, var_id=200, name="Kod A", slug="koda")
        add_variable(conn, register_id=1, var_id=201, name="Kod B", slug="kodb")
        add_variable(conn, register_id=1, var_id=202, name="Namn", slug="namn")
        # Both codes own a value_set; the shared label owns none; all three
        # co-delivered in variant 10.
        add_state(
            conn,
            register_id=1,
            variable_slug="koda",
            register_variant_id=10,
            delivery_column_name="KodA",
            value_set_id=500,
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kodb",
            register_variant_id=10,
            delivery_column_name="KodB",
            value_set_id=501,
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="namn",
            register_variant_id=10,
            delivery_column_name="Namn",
            value_set_id=None,
        )
        conn.commit()
        pairs = (
            CodeLabelPair("scb", "lisa", "koda", "scb", "lisa", "namn"),
            CodeLabelPair("scb", "lisa", "kodb", "scb", "lisa", "namn"),
        )
        siblings: list[tuple[int, int]] = []
        _append_code_label_edges(conn, pairs, _SCB, siblings)
        assert siblings == [
            (_vid(conn, "koda"), _vid(conn, "namn")),
            (_vid(conn, "kodb"), _vid(conn, "namn")),
        ]

        counts = materialize_concept_groups(conn, edge_siblings=siblings)
        assert counts["edge_groups"] == 1
        groups = _groups(conn)
        assert len(groups) == 1
        (g,) = groups.values()
        assert g["source"] == "edge"
        assert set(g["members"]) == {"koda", "kodb", "namn"}
        # Axis-less: no axis rows, no member facets, NULL delivery columns.
        assert g["axes"] == []
        assert _facets(conn, "koda") == []
        assert _facets(conn, "kodb") == []
        assert _facets(conn, "namn") == []
        delivery_cols = {
            r[0]
            for r in conn.execute(
                "SELECT delivery_column_name FROM concept_group_variable"
            )
        }
        assert delivery_cols == {None}

    def test_curated_group_excludes_pair_endpoint(self) -> None:
        # Curated precedence (#591) over the code-label edge: when a curated
        # `[[variable_group]]` already claims one of a pair's endpoints, the edge
        # fold excludes it (the `exclude_variable_ids` path), so the component drops
        # below 2 survivors and the edge channel mints no overlapping group — the
        # one-group-per-variable invariant holds. Mirrors
        # TestEdgeGroups.test_curated_member_excluded_from_edge_fold, driving the
        # edge via `_append_code_label_edges`.
        conn = _code_label_db()
        add_variable(conn, register_id=1, var_id=202, name="Other", slug="other")
        conn.commit()
        siblings: list[tuple[int, int]] = []
        _append_code_label_edges(conn, (_pair(),), _SCB, siblings)
        assert siblings == [(_vid(conn, "partikod"), _vid(conn, "partinamn"))]
        # The curated group claims the pair's code endpoint (+ an unrelated member),
        # so the {partikod, partinamn} component drops to 1 survivor.
        curated = CuratedGroup(
            provider="scb",
            register="lisa",
            key="fam",
            label="Familj",
            axes=_axis1("part"),
            members=(
                _member("partikod", "part", "1", "A"),
                _member("other", "part", "2", "O"),
            ),
        )
        counts = materialize_concept_groups(
            conn, (curated,), edge_siblings=siblings, providers=_SCB
        )
        assert counts["edge_groups"] == 0  # component below 2 survivors
        assert counts["curated_groups"] == 1
        groups = _groups(conn)
        assert set(groups) == {"fam"}
        assert groups["fam"]["members"] == ["other", "partikod"]
        # No minted group folds partikod and partinamn together.
        for g in groups.values():
            assert not ({"partikod", "partinamn"} <= set(g["members"]))


class TestCodeLabelPairLoader:
    @staticmethod
    def _load(tmp_path, text: str):
        path = tmp_path / "code_label_pairs.toml"
        path.write_text(text, encoding="utf-8")
        return load_code_label_pairs(path)

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert load_code_label_pairs(None) == ()
        assert load_code_label_pairs(tmp_path / "absent.toml") == ()

    def test_parses_pair(self, tmp_path) -> None:
        pairs = self._load(
            tmp_path,
            """
            [[pair]]
            code  = "scb/lisa/partikod"
            label = "scb/lisa/partinamn"
            """,
        )
        assert pairs == (_pair(),)

    @pytest.mark.parametrize(
        "text",
        [
            # missing label
            """
            [[pair]]
            code = "scb/lisa/partikod"
            """,
            # missing code
            """
            [[pair]]
            label = "scb/lisa/partinamn"
            """,
            # 2-segment code FQID
            """
            [[pair]]
            code  = "scb/partikod"
            label = "scb/lisa/partinamn"
            """,
            # 2-segment label FQID
            """
            [[pair]]
            code  = "scb/lisa/partikod"
            label = "lisa/partinamn"
            """,
            # empty segment
            """
            [[pair]]
            code  = "scb//partikod"
            label = "scb/lisa/partinamn"
            """,
        ],
    )
    def test_malformed_entry_fails(self, tmp_path, text: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"

    def test_unknown_top_level_key_fails(self, tmp_path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, '[[pairs]]\ncode = "a/b/c"\nlabel = "a/b/d"\n')
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_pair_fails(self, tmp_path) -> None:
        # Two identical (code, label) entries are drift — fail fast (EXIT_CONFIG,
        # the dedicated duplicate-pair code). The committed TOML is deduplicated, so
        # this guards future drift.
        text = """
        [[pair]]
        code  = "scb/lisa/partikod"
        label = "scb/lisa/partinamn"
        [[pair]]
        code  = "scb/lisa/partikod"
        label = "scb/lisa/partinamn"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"

    def test_self_pair_fails(self, tmp_path) -> None:
        # A pair whose code and label FQID are identical is drift — a variable can't
        # decode itself. Fail fast at load with a clear error, rather than letting
        # the contradictory value_set guards fire at materialize time.
        text = """
        [[pair]]
        code  = "scb/lisa/partikod"
        label = "scb/lisa/partikod"
        """
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, text)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "code_label_pairs_invalid"
