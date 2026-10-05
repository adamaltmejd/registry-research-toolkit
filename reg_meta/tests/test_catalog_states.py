"""Catalog state history: `states`, `resolve_at` narrowing and the
`with_codes` / `with_code_summary` hydration options."""

from __future__ import annotations

from typing import TYPE_CHECKING

import catalog_test_support
import pytest
from _slugged_db import (
    add_state,
    add_value_set,
    add_variant,
    build_slugged_db,
)
from catalog_test_support import KON as _KON
from reg_meta.catalog import (
    OPEN_ENDED_VALID_TO,
    Catalog,
    ClassificationExtensionMember,
    DenseIntegerRange,
    ResolvedVariable,
    ValueSetMember,
    ValueSetSummary,
)
from reg_meta.errors import RegMetaError

if TYPE_CHECKING:
    import sqlite3

# The shared fixture, bound by assignment: an imported name used only as a
# test parameter reads as an unused import redefined (ruff F401/F811).
slugged_conn = catalog_test_support.slugged_conn


class TestResolveVariableLongitudinal:
    """see DESIGN.md → Catalog API surface: `resolve()` returns the variable's shared metadata + full state
    history, each state tagged with its variant."""

    def test_resolve_returns_resolved_variable(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn).resolve(_KON)
        assert isinstance(r, ResolvedVariable)
        assert r.name == "Kön"
        assert r.provider_key == "44"
        assert r.is_sensitive is False
        assert r.is_identifier is False
        assert len(r.states) >= 1
        assert r.states[0].variant == "individer-15plus"

    def test_identifier_flag_denormalized_onto_states(self) -> None:
        # The variable-grain `is_identifier` is exposed on the ResolvedVariable
        # AND denormalized onto every state via the `_states_in_bounds` JOIN
        # (no-variant branch) — distinct from the variable-meta path — so
        # consumers with no ResolvedVariable in scope still read it.
        conn = build_slugged_db()
        conn.execute("UPDATE variable SET is_identifier = 1 WHERE slug = 'kon'")
        conn.commit()
        r = Catalog(conn).resolve(_KON)
        assert r.is_identifier is True
        assert r.states[0].is_identifier is True

    def test_classification_links_resolved_per_state(self) -> None:
        # Book links belong to their exact state. The unclassified seed keeps
        # an empty tuple; the second state exposes its own SUN2020 link.
        conn = build_slugged_db()
        cls_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun2020'"
        ).fetchone()[0]
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2019-12-31",
            delivery_column_name="Kon",
            classification_id=cls_id,
        )
        conn.commit()
        by_from = {
            s.valid_from: tuple(b.slug for b in s.classifications)
            for s in Catalog(conn).states(_KON)
        }
        assert by_from["2018-01-01"] == ()
        assert by_from["2019-01-01"] == ("sun2020",)

    def test_states_tagged_with_variant(self) -> None:
        # The same variable delivered in two variants → two states, each carrying
        # its own variant coordinate.
        conn = build_slugged_db()
        add_variant(
            conn, register_variant_id=11, register_id=1, slug="foretag", name="Företag"
        )
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=11,
            valid_from="2019-01-01",
            delivery_column_name="Kon",
        )
        r = Catalog(conn).resolve(_KON)
        assert {s.variant for s in r.states} == {"individer-15plus", "foretag"}

    def test_states_chronological_ascending(self) -> None:
        # History reads oldest → newest.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Kon",
        )
        r = Catalog(conn).resolve(_KON)
        froms = [s.valid_from for s in r.states]
        assert froms == sorted(froms)

    def test_states_accessor_equiv_resolve(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        cat = Catalog(slugged_conn)
        assert cat.states(_KON) == list(cat.resolve(_KON).states)

    def test_states_rejects_non_binding_fqid(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # states() must fail like its sibling accessors on a non-binding FQID
        # (a register here) — a structured not_a_binding_fqid usage error, not a
        # raw AttributeError off the polymorphic resolve() (the A2.5 review fix).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).states("scb/lisa")
        assert exc.value.code == "not_a_binding_fqid"

    def test_resolve_states_round_trip_with_resolve_at(self) -> None:
        # see DESIGN.md → Catalog API surface (migration stage A2.5): the full history via resolve(fqid).states
        # equals the union of per-year resolve_at() results on the unambiguous
        # single-variant case.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2019-12-31",
            delivery_column_name="Kon",
        )
        cat = Catalog(conn)
        all_states = set(cat.resolve(_KON).states)
        via_at = {
            s
            for year in (2018, 2019)
            for s in cat.resolve_at(_KON, year, variant="individer-15plus")
        }
        assert via_at == all_states

    def test_value_set_hydrated_on_state(self) -> None:
        # A state carrying a value_set exposes its (code, label) pairs. The 2020
        # state is distinct from the fixture's open-ended 2018 base state, but
        # the base state's open `valid_to` also covers 2020, so query its own
        # year and assert on the value-set-bearing state directly.
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=7, codes=[("1", "Man"), ("2", "Kvinna")])
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Kon",
            value_set_id=7,
        )
        states = Catalog(conn).resolve_at(_KON, 2020, variant="individer-15plus")
        coded = [s for s in states if s.value_set_id == 7]
        assert len(coded) == 1
        assert coded[0].value_set == (
            ValueSetMember(code="1", label="Man"),
            ValueSetMember(code="2", label="Kvinna"),
        )

    def test_state_without_value_set_is_none(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        r = Catalog(slugged_conn).resolve(_KON)
        assert r.states[0].value_set is None

    def test_open_ended_state_has_no_period_token(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # #681: reg_meta now populates `VariableState.period_token`. The fixture's
        # base state is open-ended (2018-01-01..9999-12-31), so its token is None
        # (the open sentinel has no finite token — the SPA renders "since 2018").
        state = Catalog(slugged_conn).resolve(_KON).states[0]
        assert state.valid_to == OPEN_ENDED_VALID_TO
        assert state.period_token is None

    def test_closed_state_carries_coarsest_period_token(self) -> None:
        # #681: a CLOSED window carries the coarsest exact display token —
        # `period_token_for_bounds(valid_from, valid_to)` — populated on the
        # resolved state (was the webapp's `_state_model` job). A full calendar
        # year → the bare year token.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Kon",
        )
        states = Catalog(conn).resolve_at(_KON, 2020, variant="individer-15plus")
        closed = [s for s in states if s.valid_to == "2020-12-31"]
        assert len(closed) == 1
        assert closed[0].period_token == "2020"

    def test_state_carries_variant_family_metadata(self) -> None:
        conn = build_slugged_db()
        add_variant(
            conn,
            register_variant_id=11,
            register_id=1,
            slug="individer-16plus",
            name="Individer 16+",
        )
        conn.execute(
            "UPDATE register_variant SET display_group = ? WHERE register_variant_id = 10",
            ("Individer, 15 år och äldre",),
        )
        conn.execute(
            "UPDATE register_variant SET display_group = ? WHERE register_variant_id = 11",
            ("Individer, 16 år och äldre",),
        )
        conn.execute(
            "INSERT INTO variant_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variant, "
            "successor_provider, successor_register, successor_variant, "
            "effective_year, note) VALUES "
            "('scb', 'lisa', 'individer-16plus', 'scb', 'lisa', "
            "'individer-15plus', 2010, 'curated:slug_toml')"
        )
        conn.commit()

        state = Catalog(conn).resolve(_KON).states[0]
        assert state.variant == "individer-15plus"
        assert state.variant_family == "individer-15plus"
        assert state.variant_family_label == "Individer"


def _traced(conn: sqlite3.Connection) -> list[str]:
    """Collect the SQL statements `conn` executes from now on."""
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    return statements


class TestResolveAt:
    """see DESIGN.md → Catalog API surface: `resolve_at` — period/variant/version-narrowed list of states."""

    @staticmethod
    def _two_state_year_db() -> sqlite3.Connection:
        # One variable, two sub-annual states inside calendar 2020 under one
        # variant: spring (Jan-Jun) and autumn (Jul-Dec). Lets sub-annual period
        # tokens prove the year-only limit is lifted. We drop the fixture's
        # auto-seeded open-ended 2018 base state so only these two remain.
        conn = build_slugged_db()
        conn.execute(
            "DELETE FROM variable_state WHERE variable_id = "
            "(SELECT variable_id FROM variable WHERE register_id=1 AND slug='kon')"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2020-01-01",
            valid_to="2020-06-30",
            delivery_column_name="KonVT",
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2020-07-01",
            valid_to="2020-12-31",
            delivery_column_name="KonHT",
        )
        conn.commit()
        return conn

    @staticmethod
    def _coded_state_db(*, windowed: bool) -> sqlite3.Connection:
        """The `kon` fixture with a value set on its state plus a conformance
        report carrying one nonconforming code — the two per-state CODE LISTS.
        `windowed` adds monthly alias windows so the expansion path is covered.
        """
        conn = build_slugged_db()
        add_value_set(conn, value_set_id=3, codes=[("1", "Man"), ("2", "Kvinna")])
        cls_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun2020'"
        ).fetchone()[0]
        # Replace the fixture's auto-seeded base state with one carrying both
        # code lists' keys, so `kon` still has exactly one state in 2018.
        conn.execute("DELETE FROM variable_state")
        state_id = add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2018-01-01",
            delivery_column_name="Kon",
            value_set_id=3,
            classification_id=cls_id,
        )
        conn.execute(
            "INSERT INTO classification_conformance (state_id, "
            "declared_classification_id, status, checked_code_count, "
            "matched_code_count, nonconforming_code_count, overlap) "
            "VALUES (?, ?, 'extended', 2, 1, 1, 0.5)",
            (state_id, cls_id),
        )
        conn.execute(
            "INSERT INTO classification_conformance_code (state_id, declared_classification_id, code_id, member_kind, scoped_sentinels) "
            "SELECT ?, ?, code_id, 'nonstandard', '[]' FROM value_code WHERE code = '2'",
            (state_id, cls_id),
        )
        if windowed:
            conn.executemany(
                "INSERT INTO variable_alias_window (variable_id, "
                "register_variant_id, delivery_column_name, valid_from, valid_to) "
                "VALUES ((SELECT variable_id FROM variable WHERE slug = 'kon'), "
                "10, ?, ?, ?)",
                [
                    ("Kon", "2018-01-01", "2018-01-31"),
                    ("Kon_feb", "2018-02-01", "2018-02-28"),
                ],
            )
        conn.commit()
        return conn

    @pytest.mark.parametrize("windowed", [False, True])
    def test_with_codes_false_is_metadata_only(self, windowed: bool) -> None:
        # Same states, same identities/windows — only the two code lists drop
        # out, and neither of their queries runs.
        conn = self._coded_state_db(windowed=windowed)
        cat = Catalog(conn)
        full = cat.resolve_at(_KON, 2018, variant="individer-15plus")
        assert len(full) == (2 if windowed else 1)
        assert all(s.value_set is not None for s in full)
        assert all(s.classifications[0].conformance is not None for s in full)

        statements = _traced(conn)
        lean = cat.resolve_at(_KON, 2018, variant="individer-15plus", with_codes=False)
        assert lean == [
            s.model_copy(
                update={
                    "value_set": None,
                    "classifications": tuple(
                        b.model_copy(update={"conformance": None})
                        for b in s.classifications
                    ),
                }
            )
            for s in full
        ]
        assert all(s.value_set_id == 3 for s in lean)
        assert not [
            sql
            for sql in statements
            if "value_set_member" in sql or "classification_conformance_code" in sql
        ]

    def test_int_year(self, slugged_conn: sqlite3.Connection) -> None:
        states = Catalog(slugged_conn).resolve_at(
            _KON, 2018, variant="individer-15plus"
        )
        assert len(states) == 1
        assert states[0].variant == "individer-15plus"

    def test_identifier_flag_on_variant_scoped_state(self) -> None:
        # Resolving with an explicit variant takes the variant-scoped
        # (`register_variant_id IS NOT NULL`) branch of `_states_in_bounds`; the
        # denormalized `is_identifier` must come through there too.
        conn = build_slugged_db()
        conn.execute("UPDATE variable SET is_identifier = 1 WHERE slug = 'kon'")
        conn.commit()
        states = Catalog(conn).resolve_at(_KON, 2018, variant="individer-15plus")
        assert len(states) == 1
        assert states[0].is_identifier is True

    def test_classification_links_on_variant_scoped_state(self) -> None:
        # Mirror of the is_identifier variant-scoped test, for classification: the
        # variant-scoped (`register_variant_id IS NOT NULL`) SELECT branch must
        # also resolve the per-state slug. A pre-2018 window keeps the seed clear
        # of the fixture's open-ended base state so it's the sole match.
        conn = build_slugged_db()
        cls_id = conn.execute(
            "SELECT id FROM classification WHERE slug = 'sun2020'"
        ).fetchone()[0]
        add_state(
            conn,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from="2017-01-01",
            valid_to="2017-12-31",
            delivery_column_name="Kon",
            classification_id=cls_id,
        )
        conn.commit()
        states = Catalog(conn).resolve_at(_KON, 2017, variant="individer-15plus")
        assert len(states) == 1
        assert tuple(b.slug for b in states[0].classifications) == ("sun2020",)

    def test_period_token_month(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(_KON, "2020-08", variant="individer-15plus")
        # Only the autumn state covers August (precise — not year-granular).
        assert [s.delivery_column_name for s in states] == ["KonHT"]

    def test_period_token_quarter(self) -> None:
        conn = self._two_state_year_db()
        # Q1 (Jan-Mar) → spring state only.
        states = Catalog(conn).resolve_at(_KON, "2020-Q1", variant="individer-15plus")
        assert [s.delivery_column_name for s in states] == ["KonVT"]

    def test_period_token_htvt(self) -> None:
        conn = self._two_state_year_db()
        ht = Catalog(conn).resolve_at(_KON, "HT2020", variant="individer-15plus")
        vt = Catalog(conn).resolve_at(_KON, "VT2020", variant="individer-15plus")
        assert [s.delivery_column_name for s in ht] == ["KonHT"]
        assert [s.delivery_column_name for s in vt] == ["KonVT"]

    def test_period_token_iso_date(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(
            _KON, "2020-03-15", variant="individer-15plus"
        )
        assert [s.delivery_column_name for s in states] == ["KonVT"]

    def test_range_period_crosses_states(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(
            _KON, {"from": "2020-01-01", "to": "2020-12-31"}, variant="individer-15plus"
        )
        # The range spans both states; chronological ascending.
        assert [s.delivery_column_name for s in states] == ["KonVT", "KonHT"]

    def test_default_sentinel_returns_all(self) -> None:
        conn = self._two_state_year_db()
        states = Catalog(conn).resolve_at(_KON, "_default")
        # No period filter → every state (both sub-annual ones).
        assert len(states) == 2
        assert [s.delivery_column_name for s in states] == ["KonVT", "KonHT"]

    def test_variant_narrows(self) -> None:
        # Two variants deliver the variable at the same year; omitting `variant`
        # returns both, supplying it returns one.
        conn = build_slugged_db()
        add_variant(
            conn, register_variant_id=11, register_id=1, slug="foretag", name="Företag"
        )
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=11,
            valid_from="2018-01-01",
            delivery_column_name="Kon",
        )
        cat = Catalog(conn)
        assert len(cat.resolve_at(_KON, 2018)) == 2
        assert len(cat.resolve_at(_KON, 2018, variant="foretag")) == 1

    def test_value_set_version_narrows_multivintage(self) -> None:
        # Multi-vintage fold (see reg_meta_build/DESIGN.md → Build-time triage (SCB)): two overlapping states, same variant + year,
        # distinct value_set_version_label (SNI92 + SNI2007 in a crosswalk year).
        # resolve_at returns both; value_set_version narrows to one.
        conn = build_slugged_db()
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="2007-12-31",
            delivery_column_name="Sni",
            value_set_version_label="sni92",
        )
        add_state(
            conn,
            register_id=1,
            var_id=44,
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="2007-12-31",
            delivery_column_name="Sni",
            value_set_version_label="sni2007",
        )
        cat = Catalog(conn)
        both = cat.resolve_at(_KON, 2007, variant="individer-15plus")
        assert len(both) == 2
        narrowed = cat.resolve_at(
            _KON, 2007, variant="individer-15plus", value_set_version="sni2007"
        )
        assert len(narrowed) == 1
        assert narrowed[0].value_set_version_label == "sni2007"

    def test_empty_when_no_state_covers_period(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # No exception — an empty list signals "binding exists, no state here".
        assert Catalog(slugged_conn).resolve_at(_KON, 1850) == []

    def test_empty_when_variant_unknown(self, slugged_conn: sqlite3.Connection) -> None:
        assert Catalog(slugged_conn).resolve_at(_KON, 2018, variant="nope") == []

    def test_unknown_binding_fqid_raises(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        # The binding itself not resolving is the 404 case (distinct from empty).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve_at("scb/lisa/nonexistent", 2018)
        assert exc.value.code == "fqid_not_found"

    def test_invalid_period_raises_usage(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve_at(_KON, "not-a-period")
        assert exc.value.code == "invalid_period"

    @pytest.mark.parametrize("bad", ["2019-02-29", "2018-02-30", "2021-04-31"])
    def test_calendar_invalid_period_raises_usage(
        self, slugged_conn: sqlite3.Connection, bad: str
    ) -> None:
        # #239: a calendar-impossible day is now rejected by the period grammar,
        # so `resolve_at` raises `invalid_period` instead of silently tolerating
        # the string (which previously risked phantom coverage results).
        with pytest.raises(RegMetaError) as exc:
            Catalog(slugged_conn).resolve_at(_KON, bad)
        assert exc.value.code == "invalid_period"


class TestBoundedCodeReads:
    """The narrow reads that replace the embedded per-state code lists (Y-46):
    the per-value-set summary, the full membership behind the bounded route, and
    the per-state classification mismatch list."""

    @staticmethod
    def _shared_coding_db(*, states: int) -> sqlite3.Connection:
        """`kon` with `states` yearly states that all carry ONE value set — the
        shape the ticket is about (290 states, a handful of codings)."""
        conn = build_slugged_db()
        add_value_set(
            conn,
            value_set_id=3,
            codes=[(str(age), f"{age} år") for age in range(20)],
        )
        conn.execute("DELETE FROM variable_state")
        for year in range(2000, 2000 + states):
            add_state(
                conn,
                register_id=1,
                variable_slug="kon",
                register_variant_id=10,
                valid_from=f"{year}-01-01",
                valid_to=f"{year}-12-31",
                delivery_column_name="Kon",
                value_set_id=3,
            )
        conn.commit()
        return conn

    def test_summary_is_the_membership_without_the_members(self) -> None:
        cat = Catalog(self._shared_coding_db(states=1))
        assert cat.value_set_summary(3) == ValueSetSummary(
            code_count=20, integer_range=DenseIntegerRange(min=0, max=19)
        )

    def test_summary_scans_a_shared_coding_once_for_the_whole_history(self) -> None:
        # The acceptance property: 40 states over one coding cost ONE membership
        # scan, so the initial payload's work is independent of how many states
        # reference it (and of how many codes it has, beyond that one read).
        conn = self._shared_coding_db(states=40)
        cat = Catalog(conn)
        statements = _traced(conn)
        states = cat.resolve_binding(_KON, with_codes=False, with_code_summary=True)
        assert len(states.states) == 40
        assert all(s.value_set_summary is not None for s in states.states)
        assert len([sql for sql in statements if "value_set_member" in sql]) == 1

    def test_unknown_value_set_is_not_an_empty_one(self) -> None:
        cat = Catalog(self._shared_coding_db(states=1))
        # Code order, so a bounded page is a stable slice of a stable list.
        assert cat.value_set_codes(3) == tuple(
            ValueSetMember(code=code, label=f"{code} år")
            for code in sorted(str(age) for age in range(20))
        )
        assert cat.value_set_codes(9999) is None

    def test_mismatch_list_is_refused_for_a_state_that_lacks_the_coding(self) -> None:
        conn = TestResolveAt._coded_state_db(windowed=False)
        state_id = conn.execute("SELECT state_id FROM variable_state").fetchone()[0]
        cat = Catalog(conn)
        assert cat.state_nonconforming_codes(
            state_id, 3, classification_slug="sun2020"
        ) == (
            ClassificationExtensionMember(
                code="2", label="Kvinna", member_kind="nonstandard"
            ),
        )
        # The state carries value set 3, not 4 — so 4 cannot be read through it.
        assert (
            cat.state_nonconforming_codes(state_id, 4, classification_slug="sun2020")
            is None
        )
        assert (
            cat.state_nonconforming_codes(999, 3, classification_slug="sun2020") is None
        )


class TestCodeSummaryHydration:
    """`with_code_summary` — the explicit web opt-in that keeps a binding payload
    independent of code cardinality while preserving every state reference."""

    def test_summary_replaces_the_members_and_keeps_the_verdict(self) -> None:
        conn = TestResolveAt._coded_state_db(windowed=False)
        cat = Catalog(conn)
        [state] = cat.resolve_at(
            _KON,
            2018,
            variant="individer-15plus",
            with_codes=False,
            with_code_summary=True,
        )
        assert state.value_set is None
        assert state.value_set_id == 3
        assert state.value_set_summary == ValueSetSummary(
            code_count=2, integer_range=None
        )
        # The stored conformance verdict survives; only its mismatch LIST moves
        # to the on-demand read, and the count is what says there is one.
        conformance = state.classifications[0].conformance
        assert conformance is not None
        assert conformance.status == "extended"
        assert conformance.nonconforming_code_count == 1
        assert conformance.nonconforming_codes == ()

    def test_full_resolution_keeps_its_complete_default_semantics(self) -> None:
        # `Catalog.resolve` / `Catalog.states` are unchanged: members embedded,
        # mismatch list embedded, no summary.
        conn = TestResolveAt._coded_state_db(windowed=False)
        cat = Catalog(conn)
        resolved = cat.resolve(_KON)
        assert isinstance(resolved, ResolvedVariable)
        [state] = resolved.states
        assert state.value_set == (
            ValueSetMember(code="1", label="Man"),
            ValueSetMember(code="2", label="Kvinna"),
        )
        assert state.value_set_summary is None
        conformance = state.classifications[0].conformance
        assert conformance is not None
        assert conformance.nonconforming_codes == (
            ClassificationExtensionMember(
                code="2", label="Kvinna", member_kind="nonstandard"
            ),
        )
        assert cat.states(_KON) == list(resolved.states)
