"""Classification edition succession lifts to adjacent variable succession edges only for clean streams."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_register,
    add_state,
    add_variable,
    add_variant,
    build_slugged_db,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.catalog_dependencies import resolve_variable_successions
from reg_meta_build.resolved_catalog import (
    ResolvedClassificationLink,
    ResolvedClassificationSuccession,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
)
from reg_meta_build.resolved_metadata import ResolvedMetadata, ResolvedSuccession

if TYPE_CHECKING:
    import sqlite3


def _add_classification(conn: sqlite3.Connection, short: str, slug: str) -> int:
    """Insert a classification and return its `id`."""
    cur = conn.execute(
        "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
        (short, short, slug),
    )
    return cur.lastrowid


def _cid(conn: sqlite3.Connection, slug: str) -> int:
    """The `classification.id` for a slug (sqlite3.Connection can't carry test
    attrs, so resolve on demand)."""
    return conn.execute(
        "SELECT id FROM classification WHERE slug = ?", (slug,)
    ).fetchone()[0]


def _add_edition_edge(
    conn: sqlite3.Connection, pred: str, succ: str, year: int | None
) -> None:
    """Insert one `classification_replaced_by` edition edge (the #571 chain the
    lift consumes). `year=None` inserts a NULL `effective_year` (a curated/auto
    edition edge may carry no year — the lift passes it through verbatim)."""
    conn.execute(
        "INSERT INTO classification_replaced_by "
        "(predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, 'derived:vintage_chain')",
        (pred, succ, year),
    )


def _lift_rows(conn: sqlite3.Connection) -> list[tuple]:
    """Derived vintage-lift edges, ordered, as (pred_var, succ_var, year)."""
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT predecessor_variable, successor_variable, effective_year "
            "FROM variable_replaced_by "
            "WHERE note = 'derived:classification_vintage_lift' "
            "ORDER BY predecessor_variable, successor_variable"
        )
    ]


def _vintage_db() -> sqlite3.Connection:
    """scb/lisa with a single variant + two chained classification editions
    (sni2002 → sni2007, effective 2007). No variables/states yet — each test
    seeds its own family shape. `classification=None` so the only classifications
    are the two editions (the default fixture's SUN2020 would just be inert, but
    keeping the table minimal makes the chain explicit)."""
    conn = build_slugged_db(
        register=None, variant=None, version=None, variable=None, classification=None
    )
    add_register(conn, register_id=1, slug="lisa", name="LISA")
    add_variant(
        conn, register_variant_id=10, register_id=1, slug="ind", name="Individer"
    )
    _add_classification(conn, "SNI2002", "sni2002")
    _add_classification(conn, "SNI2007", "sni2007")
    _add_edition_edge(conn, "sni2002", "sni2007", 2007)
    conn.commit()
    return conn


def _resolve_vintage_fixture(conn: sqlite3.Connection) -> int:
    """Project the existing relation fixtures into the common resolver.

    Keep their original assertions and table-shaped setup. The fixture's omitted
    column names are irrelevant to edition succession; use the variable slug.
    """
    variables = []
    for row in conn.execute(
        "SELECT v.*, p.slug AS provider, r.slug AS register_slug "
        "FROM variable v JOIN register r USING (register_id) "
        "JOIN provider p USING (provider_id)"
    ):
        states = tuple(
            ResolvedState(
                variant=ResolvedVariant(
                    slug=state["variant_slug"], name=state["variant_name"]
                ),
                valid_from=state["valid_from"],
                valid_to=state["valid_to"],
                delivery_column_name=state["delivery_column_name"] or row["slug"],
                value_set_version_label=state["value_set_version_label"],
                data_type=state["data_type"],
                data_length=None,
                operational_definition=None,
                provenance=None,
                classification_links=tuple(
                    ResolvedClassificationLink(
                        classification=link["slug"], provenance=link["provenance"]
                    )
                    for link in conn.execute(
                        "SELECT c.slug, sc.provenance FROM state_classification sc JOIN classification c ON c.id=sc.classification_id WHERE sc.state_id=? ORDER BY c.slug",
                        (state["state_id"],),
                    )
                ),
            )
            for state in conn.execute(
                "SELECT s.*, rv.slug AS variant_slug, rv.name AS variant_name "
                "FROM variable_state s "
                "JOIN register_variant rv USING (register_variant_id) "
                "WHERE s.variable_id = ?",
                (row["variable_id"],),
            )
        )
        if states:
            variables.append(
                ResolvedVariable(
                    register=ResolvedRegister(
                        provider=row["provider"],
                        slug=row["register_slug"],
                        name=row["register_slug"],
                    ),
                    slug=row["slug"],
                    provider_key=row["provider_key"],
                    name=row["name"],
                    definition=None,
                    description=None,
                    operational_definition=None,
                    measurement_unit=None,
                    is_sensitive=False,
                    is_identifier=False,
                    states=states,
                )
            )
    metadata = ResolvedMetadata(
        successions=tuple(
            ResolvedSuccession(
                predecessor="/".join(row[:3]),
                successor="/".join(row[3:6]),
                effective_year=row[6],
                note=row[7],
                description=row[8],
            )
            for row in conn.execute("SELECT * FROM variable_replaced_by")
        )
    )
    classifications = tuple(
        ResolvedClassificationSuccession(
            predecessor=a, successor=b, effective_year=year
        )
        for a, b, year in conn.execute(
            "SELECT predecessor_slug, successor_slug, effective_year FROM classification_replaced_by"
        )
    )
    result = resolve_variable_successions(metadata, tuple(variables), classifications)
    added = result.successions[len(metadata.successions) :]
    conn.executemany(
        "INSERT INTO variable_replaced_by VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                *e.predecessor.split("/"),
                *e.successor.split("/"),
                e.effective_year,
                e.note,
                e.description,
            )
            for e in added
        ],
    )
    return len(added)


class TestVariableVintageSuccession:
    def test_clean_pair_mints_one_edge(self) -> None:
        # Two DISTINCT variables, same name, one per edition → a bijection.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 1
        assert _lift_rows(conn) == [("sni-2002", "sni-2007", 2007)]

    def test_clean_pair_with_different_streams_mints_nothing(self) -> None:
        # Same-name and 1:1 by edition is not enough after #592: non-vintage
        # slug tokens still define separate streams, so this clean-looking pair
        # would fail the real-corpus stream guard and must be skipped at source.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Kommun", slug="u-ukom")
        add_variable(conn, register_id=1, var_id=2, name="Kommun", slug="u-hkom")
        add_state(
            conn,
            register_id=1,
            variable_slug="u-ukom",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="u-hkom",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_three_edition_chain_mints_adjacent_edges(self) -> None:
        # Adjacent-chain (NOT predecessor→latest): a 3-edition family → 2 edges.
        conn = _vintage_db()
        _add_classification(conn, "SNI2012", "sni2012")
        _add_edition_edge(conn, "sni2007", "sni2012", 2012)
        for vid, slug, cls in (
            (1, "sni-2002", "sni2002"),
            (2, "sni-2007", "sni2007"),
            (3, "sni-2012", "sni2012"),
        ):
            cid = _cid(conn, cls)
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=cid,
            )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 2
        # Adjacent hops only — no 2002→2012 star edge.
        assert _lift_rows(conn) == [
            ("sni-2002", "sni-2007", 2007),
            ("sni-2007", "sni-2012", 2012),
        ]

    def test_entangled_parent_streams_mint_parallel_edges(self) -> None:
        # Two variables BOTH bind sni2002 (and two more bind sni2007): an edition
        # bound by >1 variable in the family. #592 partitions the same-name
        # family by slug stream after stripping the classification vintage token,
        # so fars-* links only to fars-* and mors-* only to mors-*.
        conn = _vintage_db()
        for vid, slug, cls in (
            (1, "fars-sni-2002", "sni2002"),
            (2, "mors-sni-2002", "sni2002"),
            (3, "fars-sni-2007", "sni2007"),
            (4, "mors-sni-2007", "sni2007"),
        ):
            add_variable(
                conn,
                register_id=1,
                var_id=vid,
                name="Föräldrars näringsgren",
                slug=slug,
            )
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("fars-sni-2002", "fars-sni-2007", 2007),
            ("mors-sni-2002", "mors-sni-2007", 2007),
        ]

    def test_entangled_population_streams_do_not_cross_link(self) -> None:
        # Same classification edge, same variable.name, parallel population
        # streams. Stripping only the vintage years leaves the population token in
        # the stream key, so no individ→foretag cross-link is possible.
        conn = _vintage_db()
        for vid, slug, cls in (
            (1, "individ-sni-2002", "sni2002"),
            (2, "foretag-sni-2002", "sni2002"),
            (3, "individ-sni-2007", "sni2007"),
            (4, "foretag-sni-2007", "sni2007"),
        ):
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("foretag-sni-2002", "foretag-sni-2007", 2007),
            ("individ-sni-2002", "individ-sni-2007", 2007),
        ]

    def test_entangled_ambiguous_stream_is_skipped(self) -> None:
        # Two predecessor variables collapse to the same non-vintage stream key.
        # The lift refuses to choose one and skips that stream rather than minting
        # a false cross-product edge.
        conn = _vintage_db()
        for vid, slug, cls in (
            (1, "fars-sni-2002", "sni2002"),
            (2, "fars-sni-2002-2002", "sni2002"),
            (3, "fars-sni-2007", "sni2007"),
        ):
            add_variable(
                conn,
                register_id=1,
                var_id=vid,
                name="Föräldrars näringsgren",
                slug=slug,
            )
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_interval_native_variable_mints_nothing(self) -> None:
        # ONE variable spanning BOTH editions across its own two states (the #271
        # interval-native case) already carries the lineage in one variable_id →
        # no lift. The variable appears under >1 edition, breaking the bijection.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni",
            register_variant_id=10,
            valid_from="2002-01-01",
            valid_to="2006-12-31",
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni",
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="9999-12-31",
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_interval_native_variable_is_not_paired_with_neighbor(self) -> None:
        # A chain-spanning variable is interval-native even when the same-name
        # family has another variable on one edition. It must not be paired with
        # that neighbor; the real corpus has municipality columns in this shape,
        # where a false lift would close a reversed source edge into a cycle.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Kommun", slug="u-ukom")
        add_state(
            conn,
            register_id=1,
            variable_slug="u-ukom",
            register_variant_id=10,
            valid_from="2002-01-01",
            valid_to="2006-12-31",
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="u-ukom",
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="9999-12-31",
            classification_id=_cid(conn, "sni2007"),
        )
        add_variable(conn, register_id=1, var_id=2, name="Kommun", slug="u-hkom")
        add_state(
            conn,
            register_id=1,
            variable_slug="u-hkom",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_existing_curated_edge_wins_no_duplicate(self) -> None:
        # A pre-existing edge on the same PK (curated #375/#440 or auto
        # timeseries_event) WINS — the lift's INSERT OR IGNORE leaves it untouched
        # and mints no derived duplicate. Same clean family as the first test.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        # Pre-seed the SAME PK with a curated row (richer provenance).
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note, beskrivning) "
            "VALUES ('scb','lisa','sni-2002','scb','lisa','sni-2007', "
            "2007, 'curated:slug_toml', 'hand reason')"
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 0  # the derived row collapsed onto the curated PK
        # Exactly ONE row on that PK, and it kept the curated note + beskrivning.
        rows = conn.execute(
            "SELECT note, beskrivning FROM variable_replaced_by "
            "WHERE predecessor_variable = 'sni-2002' "
            "AND successor_variable = 'sni-2007'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "curated:slug_toml"
        assert rows[0][1] == "hand reason"

    def test_reversed_pre_existing_edge_closes_cycle_raises(self) -> None:
        # The lift is the one writer that inserts AFTER curated/auto, so it must
        # re-check the COMBINED graph: a pre-existing REVERSED edge sni-2007 ->
        # sni-2002 plus the lift's chain-direction sni-2002 -> sni-2007 closes a
        # 2-cycle. The earlier passes couldn't see it (the lift edge didn't exist
        # yet), so the lift's own post-insert full-graph cycle check must fail the
        # build loudly. Same clean family as the first test.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        # Pre-seed the REVERSED edge (successor -> predecessor of the lift edge).
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note) "
            "VALUES ('scb','lisa','sni-2007','scb','lisa','sni-2002', "
            "2002, 'curated:slug_toml')"
        )
        conn.commit()
        with pytest.raises(RegMetaError) as exc:
            _resolve_vintage_fixture(conn)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_distinct_levels_do_not_cross_link(self) -> None:
        # Two LEVELS of the same series each bind their own classification lineage
        # (sni2007-grov ≠ sni2007-utokad). The lift over distinct slugs isolates
        # each level's chain — grov never links into utokad — with NO special
        # level handling. Two clean families → two independent edges.
        conn = _vintage_db()  # has sni2002→sni2007 (treat as the "grov" lineage)
        cid_ug_2002 = _add_classification(conn, "SNI2002-UTOKAD", "sni2002-utokad")
        cid_ug_2007 = _add_classification(conn, "SNI2007-UTOKAD", "sni2007-utokad")
        _add_edition_edge(conn, "sni2002-utokad", "sni2007-utokad", 2007)
        # grov family
        add_variable(
            conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-grov-2002"
        )
        add_variable(
            conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-grov-2007"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-grov-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-grov-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        # utokad family — DIFFERENT name so it's a distinct family key (a real
        # level split carries a distinct name/slug; the slug-chain isolation here
        # is what the test asserts: utokad rides its own classification slugs).
        add_variable(
            conn, register_id=1, var_id=3, name="Näringsgren utökad", slug="sni-ut-2002"
        )
        add_variable(
            conn, register_id=1, var_id=4, name="Näringsgren utökad", slug="sni-ut-2007"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-ut-2002",
            register_variant_id=10,
            classification_id=cid_ug_2002,
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-ut-2007",
            register_variant_id=10,
            classification_id=cid_ug_2007,
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("sni-grov-2002", "sni-grov-2007", 2007),
            ("sni-ut-2002", "sni-ut-2007", 2007),
        ]

    def test_same_name_levels_isolate_by_slug_chain(self) -> None:
        # The STRONG isolation case: two levels share the IDENTICAL family key
        # (register=1, name="Näringsgren"), differing ONLY by classification slug
        # — grov rides sni2002/sni2007, utokad rides sni2002-utokad/sni2007-utokad.
        # Each edition still binds exactly ONE variable, so the bijection spans all
        # FOUR editions cleanly and the slug-chain isolation mints grov→grov and
        # utokad→utokad with NO grov↔utokad cross-link, even with no name signal.
        conn = _vintage_db()  # has sni2002→sni2007 (the "grov" lineage)
        cid_ug_2002 = _add_classification(conn, "SNI2002-UTOKAD", "sni2002-utokad")
        cid_ug_2007 = _add_classification(conn, "SNI2007-UTOKAD", "sni2007-utokad")
        _add_edition_edge(conn, "sni2002-utokad", "sni2007-utokad", 2007)
        for vid, slug, cid in (
            (1, "sni-grov-2002", _cid(conn, "sni2002")),
            (2, "sni-grov-2007", _cid(conn, "sni2007")),
            (3, "sni-ut-2002", cid_ug_2002),
            (4, "sni-ut-2007", cid_ug_2007),
        ):
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=cid,
            )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("sni-grov-2002", "sni-grov-2007", 2007),
            ("sni-ut-2002", "sni-ut-2007", 2007),
        ]

    def test_gapped_chain_mints_nothing_across_gap(self) -> None:
        # Documents the no-transitive-link guarantee: a 3-edition chain
        # sni2002→sni2007→sni2012 with variables seeded ONLY for the two ENDS
        # (no variable binds the intermediate sni2007). Each adjacent edge needs
        # BOTH endpoints bound, so the missing middle breaks both hops and no
        # transitive sni2002→sni2012 edge is invented across the gap.
        conn = _vintage_db()
        _add_classification(conn, "SNI2012", "sni2012")
        _add_edition_edge(conn, "sni2007", "sni2012", 2012)
        for vid, slug, cls in (
            (1, "sni-2002", "sni2002"),
            (3, "sni-2012", "sni2012"),
        ):
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_null_effective_year_passes_through(self) -> None:
        # NULL pass-through is intended: a curated/auto edition edge may carry no
        # year, and the lift writes the edge's `effective_year` verbatim — so a
        # NULL-year edition mints a NULL-year variable edge (not a dropped one).
        conn = build_slugged_db(
            register=None,
            variant=None,
            version=None,
            variable=None,
            classification=None,
        )
        add_register(conn, register_id=1, slug="lisa", name="LISA")
        add_variant(
            conn, register_variant_id=10, register_id=1, slug="ind", name="Individer"
        )
        _add_classification(conn, "SNI2002", "sni2002")
        _add_classification(conn, "SNI2007", "sni2007")
        _add_edition_edge(conn, "sni2002", "sni2007", None)  # NULL effective_year
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 1
        row = conn.execute(
            "SELECT predecessor_variable, successor_variable, effective_year "
            "FROM variable_replaced_by "
            "WHERE note = 'derived:classification_vintage_lift'"
        ).fetchone()
        assert (row[0], row[1]) == ("sni-2002", "sni-2007")
        assert row[2] is None
