"""A4.1 SCB adapter / provider-blind materializer tests.

Fast fixture/unit coverage for the adapter→IR→materializer seam introduced by
the A4.1 refactor. These do NOT replace the real-data
dbdiff gate (the orchestrator runs that against the 14GB seed) — they pin the
adapter contract, IR emit order, determinism, and the provenance emit-but-
discard rule on a tiny synthetic fixture.
"""

from __future__ import annotations

import hashlib
import inspect
import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import (
    PIPE,
    REGISTERINFORMATION_ROWS,
    UNIKA_ROWS,
    _var_row,
    write_scb_input,
)
from reg_meta_build.db import DDL, build_db, seed_providers
from reg_meta_build.dbdiff import diff_db_content
from reg_meta_build.ir import (
    IRDeliveryProvenance,
    IRRegister,
    IRValueCode,
    IRValueSet,
    IRVariable,
    IRVariableAlias,
    IRVariableState,
    IRVariant,
    IRWarning,
)
from reg_meta_build.sources.scb import SCBAdapter

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.sources import IRAdapter


def _drained_adapter(tmp_path: Path) -> tuple[sqlite3.Connection, SCBAdapter, list]:
    """Set up a working conn exactly as `build_db` does (DDL + providers +
    staging ATTACH), run `SCBAdapter.emit()` over the standard fixture, and
    return (conn, adapter, emitted_ir_objects)."""
    scb_dir = write_scb_input(tmp_path / "input")
    staging_path = tmp_path / "staging.sqlite"

    conn = sqlite3.connect(tmp_path / "work.db")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.executescript(DDL)
    seed_providers(conn)
    conn.execute("ATTACH DATABASE ? AS staging", (str(staging_path),))

    adapter = SCBAdapter(conn)
    objects = list(adapter.emit(scb_dir))
    return conn, adapter, objects


# ── 1. SCBAdapter conforms to IRAdapter ────────────────────────────────────


class TestAdapterConformance:
    def test_provider_is_scb(self) -> None:
        assert SCBAdapter.provider == "scb"

    def test_emit_is_a_generator(self) -> None:
        assert inspect.isgeneratorfunction(SCBAdapter.emit)

    def test_structural_protocol(self) -> None:
        # `IRAdapter` is a non-runtime-checkable Protocol (ty validates
        # conformance at type-check time); assert the structural surface here.
        conn = sqlite3.connect(":memory:")
        adapter = SCBAdapter(conn)
        assert isinstance(adapter.provider, str)
        assert callable(adapter.emit)
        # The annotation below documents that SCBAdapter satisfies IRAdapter;
        # ty checks it, and this keeps the import live for that contract.
        _typed: IRAdapter = adapter
        assert _typed.provider == "scb"


# ── 2. IR validation + FK-referential emit order ───────────────────────────


class TestEmitOrder:
    def test_emits_wellformed_ir(self, tmp_path: Path) -> None:
        _conn, _adapter, objects = _drained_adapter(tmp_path)
        # Pydantic extra="forbid" already validated each object at construction;
        # assert the stream carries the expected universal types + at least one
        # of each backbone type.
        assert any(isinstance(o, IRRegister) for o in objects)
        assert any(isinstance(o, IRVariant) for o in objects)
        assert any(isinstance(o, IRVariable) for o in objects)
        assert any(isinstance(o, IRVariableState) for o in objects)

    def test_fk_referential_emit_order(self, tmp_path: Path) -> None:
        """Every IRVariableState's variable_id / register_variant_id /
        value_set_id is introduced by a prior object in the stream (parents
        precede children — the materializer can insert in stream order)."""
        _conn, _adapter, objects = _drained_adapter(tmp_path)
        seen_variables: set[int] = set()
        seen_variants: set[int] = set()
        seen_value_sets: set[int] = set()
        for o in objects:
            if isinstance(o, IRRegister):
                pass
            elif isinstance(o, IRVariant):
                seen_variants.add(o.register_variant_id)
            elif isinstance(o, IRVariable):
                seen_variables.add(o.variable_id)
            elif isinstance(o, IRValueSet):
                seen_value_sets.add(o.value_set_id)
            elif isinstance(o, IRVariableState):
                assert o.variable_id in seen_variables
                assert o.register_variant_id in seen_variants
                if o.value_set_id is not None:
                    assert o.value_set_id in seen_value_sets

    def test_value_codes_nested_under_their_set(self, tmp_path: Path) -> None:
        _conn, _adapter, objects = _drained_adapter(tmp_path)
        for o in objects:
            if isinstance(o, IRValueSet):
                for code in o.codes:
                    assert isinstance(code, IRValueCode)
                    assert code.value_set_id == o.value_set_id

    def test_value_set_codes_match_db(self, tmp_path: Path) -> None:
        """The lockstep merge-walk in `_emit_value_sets` must attach EXACTLY the
        DB's members (code + label, in code_id order) to each value_set. Catches
        group/value_set misalignment — `test_fk_referential_emit_order` cannot,
        since the emitted `code.value_set_id` is set from the parent loop var
        regardless of whether the right group was attached."""
        conn, _adapter, objects = _drained_adapter(tmp_path)
        emitted = {
            o.value_set_id: [(c.code, c.label) for c in o.codes]
            for o in objects
            if isinstance(o, IRValueSet)
        }
        assert emitted, "fixture should produce at least one value_set"
        for vsid, codes in emitted.items():
            expected = [
                (row[0], row[1])
                for row in conn.execute(
                    "SELECT vc.code, vc.label FROM value_set_member vsm "
                    "JOIN value_code vc ON vc.code_id = vsm.code_id "
                    "WHERE vsm.value_set_id = ? ORDER BY vsm.code_id",
                    (vsid,),
                )
            ]
            assert codes == expected


# ── 3. Fixture-level dbdiff round-trip (unit mirror of the real-data gate) ──


class TestFixtureRoundTrip:
    def test_two_builds_are_content_identical(self, tmp_path: Path) -> None:
        """Two independent builds of the same fixture through the new
        adapter/materializer must be byte-identical (order-independent content
        diff via `diff_db_content`). Both read ONE input dir so `import_manifest`
        `input_dir` is constant — only the build timestamp (ignored by
        `diff_db_content`) and content determinism are under test."""
        input_dir = tmp_path / "input"
        write_scb_input(input_dir)

        def _build(tag: str) -> Path:
            db_dir = tmp_path / f"db_{tag}"
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                skip_classifications=True,
                skip_slugs=True,
            )
            return db_dir / "reg_meta.db"

        a = _build("a")
        b = _build("b")
        report = diff_db_content(a, b)
        assert report.identical, report


# ── 4. Emit-order determinism (R6) ─────────────────────────────────────────


class TestDeterminism:
    def test_repeated_emit_assigns_identical_ids(self, tmp_path: Path) -> None:
        """emit() over the same fixture twice (fresh conns) yields identical
        id assignment — catches any set/dict nondeterminism in the move."""

        def _ids(sub: str) -> tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]:
            _conn, _adapter, objects = _drained_adapter(tmp_path / sub)
            variables = tuple(
                o.variable_id for o in objects if isinstance(o, IRVariable)
            )
            states = tuple(
                o.state_id for o in objects if isinstance(o, IRVariableState)
            )
            value_sets = tuple(
                o.value_set_id for o in objects if isinstance(o, IRValueSet)
            )
            return variables, states, value_sets

        assert _ids("run1") == _ids("run2")


# ── 5. Provenance emit-but-discard ─────────────────────────────────────────


class TestProvenanceEmitButDiscard:
    def test_emits_provenance_and_warnings(self, tmp_path: Path) -> None:
        """A4.1 obligation: the adapter EMITS >=1 IRDeliveryProvenance (so A4.2
        only has to wire it). Warnings are emitted when the fixture exercises a
        triage-collapse / empty-projection condition; the standard fixture may
        not, so warnings are not asserted >0 here."""
        _conn, _adapter, objects = _drained_adapter(tmp_path)
        provenance = [o for o in objects if isinstance(o, IRDeliveryProvenance)]
        assert len(provenance) >= 1
        # Warnings are a valid (possibly empty) part of the stream; when present
        # each carries a non-empty code. (Don't assert >0 — the standard fixture
        # may not trip a triage-collapse / empty-projection condition.)
        assert all(w.code for w in objects if isinstance(w, IRWarning))

    def test_built_db_populates_provenance(self, tmp_path: Path) -> None:
        """A4.2: the build POPULATES the sibling provenance DB. build_manifest
        carries one row whose universal_db_sha256 matches the live reg_meta.db,
        and the provenance tables hold the expected source/approval rows."""
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir)
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        universal_path = db_dir / "reg_meta.db"
        prov_path = db_dir / "reg_meta.provenance.db"
        assert prov_path.exists()

        live_sha = hashlib.sha256(universal_path.read_bytes()).hexdigest()
        prov = sqlite3.connect(prov_path)
        try:
            rows = prov.execute(
                "SELECT schema_version, universal_db_path, universal_db_sha256 "
                "FROM build_manifest"
            ).fetchall()
            assert len(rows) == 1, "build_manifest must hold exactly one row"
            (_schema, db_path, db_sha) = rows[0]
            assert db_sha == live_sha, "build_manifest sha256 must match live DB"
            assert db_path == str(universal_path)

            # Per-provider source-ID linkage: every register maps to its native
            # Registernamn.
            id_map = dict(
                prov.execute(
                    "SELECT register_id, scb_registernamn FROM scb_register_id_map"
                ).fetchall()
            )
            assert id_map, "scb_register_id_map must be populated"

            # delivery_approval is per (register_variant_id, period_token).
            cols = {
                row[1] for row in prov.execute("PRAGMA table_info(delivery_approval)")
            }
            assert {
                "register_id",
                "register_variant_id",
                "period_token",
                "first_approved_date",
                "last_approved_date",
                # A4.3a WIRE: the 4 delivery-manifest columns from IRDeliveryProvenance.
                "source_file",
                "delivery_version",
                "delivery_date",
                "template_version",
            } <= cols
            # SCB populates source_file (the rest stay None for SCB; SOS fills
            # them at A4.3b). Every delivery_approval row must carry the SCB
            # source_file — pins the WIRE, not just the column presence.
            src_files = {
                row[0]
                for row in prov.execute("SELECT source_file FROM delivery_approval")
            }
            assert src_files <= {"Registerinformation.csv"}, src_files
        finally:
            prov.close()


# ── 5b. Deterministic SCB IDs + per-variant provenance keying (A4.2) ───────


def _multi_variant_ri_rows() -> list[str]:
    """Two variants of register 1 (RegVarID 10, 11) each delivering an edition
    under the SAME Registerversionnamn token ('2020') but with DISTINCT approval
    dates. The A4.1 per-register keying collapsed these into one slot; A4.2 keys
    per register_variant_id so both survive."""
    from _csv_fixtures import _ri_row

    def _row(rvid: str, regverid: str, last_date: str, cvid: str, varid: str) -> str:
        return _ri_row(
            "TESTREG",
            "Testregistret",
            "Testning",
            f"Variant{rvid}",
            f"Variant{rvid}",
            "Beskrivning",
            "Nej",
            "2020",  # shared Registerversionnamn token
            "Version 2020",
            "",
            "Godkänd",
            "2020-01-01",  # first-approval (forsta)
            last_date,  # last-approval (senast) — DISTINCT per variant
            "Hela befolkningen",
            "Alla personer",
            "",
            "2020-12-31",
            "Person",
            "Fysisk person",
            "Kön",
            "Personens kön",
            "Kön enligt folkbokföring",
            "",
            "",
            "",
            "",
            "",
            "",
            "Kon",
            "int",
            "1",
            cvid,
            "1",  # RegisterId
            rvid,  # RegVarID
            regverid,  # RegVerID
            varid,
        )

    return [
        _row("10", "100", "2021-01-15", "2001", "44"),
        _row("11", "101", "2021-06-30", "2002", "45"),
    ]


class TestDeterministicIdsAndKeying:
    def test_source_ids_are_enforced(self, tmp_path: Path) -> None:
        """register.register_id == int(RegisterId) and
        register_variant.register_variant_id == int(RegVarID) round-trip from
        the export (the verifiable core of the SCB deterministic-ID claim)."""
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir)
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = sqlite3.connect(db_dir / "reg_meta.db")
        try:
            # The standard fixture uses RegisterId=1, RegVarID=10.
            assert conn.execute(
                "SELECT register_id FROM register WHERE register_id = 1"
            ).fetchone() == (1,)
            assert conn.execute(
                "SELECT register_variant_id FROM register_variant "
                "WHERE register_variant_id = 10"
            ).fetchone() == (10,)
        finally:
            conn.close()

    def _build_and_read_approvals(self, tmp_path: Path) -> list[tuple]:
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir, registerinformation_rows=_multi_variant_ri_rows())
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
            # No Vardemangder/value-set rows in this fixture; keep it minimal.
        )
        prov = sqlite3.connect(db_dir / "reg_meta.provenance.db")
        try:
            return prov.execute(
                "SELECT register_variant_id, period_token, last_approved_date "
                "FROM delivery_approval ORDER BY register_variant_id, period_token"
            ).fetchall()
        finally:
            prov.close()

    def test_multi_variant_dates_survive_per_variant(self, tmp_path: Path) -> None:
        """Two variants sharing a Registerversionnamn token keep BOTH approval
        dates (no variant collapse — the A4.1 per-register bug)."""
        rows = self._build_and_read_approvals(tmp_path / "a")
        by_variant = {rvid: last for rvid, _token, last in rows}
        assert by_variant.get(10) == "2021-01-15"
        assert by_variant.get(11) == "2021-06-30"

    def test_provenance_keying_is_deterministic(self, tmp_path: Path) -> None:
        """Repeated builds produce byte-identical delivery_approval ordering."""
        rows_a = self._build_and_read_approvals(tmp_path / "a")
        rows_b = self._build_and_read_approvals(tmp_path / "b")
        assert rows_a == rows_b


# ── 6. value_code / code_id parity probe (R1) ──────────────────────────────


class TestCodeIdParity:
    def test_emitted_value_codes_match_db(self, tmp_path: Path) -> None:
        """The adapter wrote `value_code` verbatim (the code_id counter is
        unchanged) and the emitted IRValueSet codes mirror the minted members.
        Assert every emitted (value_set_id, code) pair is present in the DB's
        value_set_member ⨝ value_code — i.e. the emit read-back matches the
        materialized rows, with no code_id drift."""
        conn, _adapter, objects = _drained_adapter(tmp_path)
        db_pairs = {
            (vsid, code)
            for vsid, code in conn.execute(
                "SELECT vsm.value_set_id, vc.code "
                "FROM value_set_member vsm "
                "JOIN value_code vc ON vc.code_id = vsm.code_id"
            )
        }
        emitted_pairs = {
            (vs.value_set_id, code.code)
            for vs in objects
            if isinstance(vs, IRValueSet)
            for code in vs.codes
        }
        assert emitted_pairs == db_pairs
        # max(code_id) is contiguous from the adapter's Vardemangder counter
        # (populate_classifications appends after it under build_db; skipped
        # here). No NULL / gap surfaced as a member orphan.
        orphans = conn.execute(
            "SELECT COUNT(*) FROM value_set_member vsm "
            "LEFT JOIN value_code vc ON vc.code_id = vsm.code_id "
            "WHERE vc.code_id IS NULL"
        ).fetchone()[0]
        assert orphans == 0


# ── 7. A4.3a provider-blindness flip parity gates ──────────────────────────


# The pre-flip `_reparent_variable_alias` projection (the function A4.3a
# deleted) — used to prove the IR-carried IRVariableAlias rows are row-identical.
_OLD_REPARENT_SQL = (
    "SELECT DISTINCT vi.variable_id, vi.register_variant_id, "
    "       vab.delivery_column_name "
    "FROM variable_alias_build vab "
    "JOIN variable_instance vi ON vi.cvid = vab.cvid "
    "WHERE vi.variable_id IS NOT NULL"
)


class TestA43aFlipParity:
    """A4.3a flip: the materializer is the sole writer of the core graph and the
    re-pointed post-passes (variable_alias, code_variable_map) are row-identical
    to the deleted `variable_instance`-scratch derivations."""

    def test_variable_alias_ir_matches_old_reparent(self, tmp_path: Path) -> None:
        """The emitted `IRVariableAlias` rows equal the OLD
        `_reparent_variable_alias` projection (variable_alias_build ⨝
        variable_instance.variable_id) row-for-row — so the materializer writing
        `variable_alias` from IR reproduces the pre-flip table exactly."""
        conn, _adapter, objects = _drained_adapter(tmp_path)
        ir_rows = {
            (a.variable_id, a.register_variant_id, a.delivery_column_name)
            for a in objects
            if isinstance(a, IRVariableAlias)
        }
        old_rows = set(conn.execute(_OLD_REPARENT_SQL).fetchall())
        assert ir_rows == old_rows
        assert ir_rows, "fixture should produce at least one alias"

    def test_code_variable_map_rederivation_is_row_identical(
        self, tmp_path: Path
    ) -> None:
        """#152 grain-parity gate: the A4.3a `code_variable_map` derivation
        (variable_state ⨝ value_set_member) is row-identical to the deleted
        `variable_instance`-based derivation. Both run on the same drained conn
        (the adapter-written variable_state is pre-flip but carries the same
        (variable_id, value_set_id) pairs the materializer re-inserts)."""
        conn, _adapter, _objects = _drained_adapter(tmp_path)
        old = set(
            conn.execute(
                "SELECT DISTINCT vsm.code_id, vi.variable_id "
                "FROM variable_instance vi "
                "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
                "WHERE vi.value_set_id IS NOT NULL AND vi.variable_id IS NOT NULL"
            )
        )
        new = set(
            conn.execute(
                "SELECT DISTINCT vsm.code_id, vs.variable_id "
                "FROM variable_state vs "
                "JOIN value_set_member vsm ON vs.value_set_id = vsm.value_set_id "
                "WHERE vs.value_set_id IS NOT NULL"
            )
        )
        assert new == old

    def test_materializer_is_sole_writer_with_explicit_pks(
        self, tmp_path: Path
    ) -> None:
        """After build_db, the shipped variable / variable_state PKs equal the
        IR-carried IDs the adapter emitted — proving the materializer re-inserted
        the core graph from IR with explicit PKs (no autoincrement drift)."""
        input_dir = tmp_path / "input"
        write_scb_input(input_dir)
        # Capture the IR the adapter emits (its IDs are the contract).
        _conn, _adapter, objects = _drained_adapter(tmp_path / "drain")
        ir_var_ids = {o.variable_id for o in objects if isinstance(o, IRVariable)}
        ir_state_ids = {o.state_id for o in objects if isinstance(o, IRVariableState)}

        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = sqlite3.connect(db_dir / "reg_meta.db")
        shipped_var_ids = {
            r[0] for r in conn.execute("SELECT variable_id FROM variable")
        }
        shipped_state_ids = {
            r[0] for r in conn.execute("SELECT state_id FROM variable_state")
        }
        conn.close()
        assert shipped_var_ids == ir_var_ids
        assert shipped_state_ids == ir_state_ids

    def test_alias_superset_of_state_columns_invariant(self, tmp_path: Path) -> None:
        """STRUCTURAL invariant (validate.py): every variable_state delivery
        column is present in variable_alias under the same (variable_id,
        register_variant_id) key. Holds after the flip writes both from IR."""
        input_dir = tmp_path / "input"
        write_scb_input(input_dir)
        db_dir = tmp_path / "db"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        conn = sqlite3.connect(db_dir / "reg_meta.db")
        missing = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT DISTINCT vs.variable_id, vs.register_variant_id, "
            "    vs.delivery_column_name FROM variable_state vs "
            "  WHERE vs.delivery_column_name IS NOT NULL "
            "  AND NOT EXISTS (SELECT 1 FROM variable_alias va "
            "    WHERE va.variable_id = vs.variable_id "
            "    AND va.register_variant_id = vs.register_variant_id "
            "    AND LOWER(va.delivery_column_name) = LOWER(vs.delivery_column_name)))"
        ).fetchone()[0]
        conn.close()
        assert missing == 0


# ── delivery-column read-boundary hygiene ──────────────────────────────────


def _hygiene_build(
    tmp_path: Path,
    ri_extra: list[str],
    unika_extra: list[str] | None = None,
) -> sqlite3.Connection:
    input_dir = tmp_path / "input"
    write_scb_input(
        input_dir,
        registerinformation_rows=list(REGISTERINFORMATION_ROWS) + ri_extra,
        unika_rows=list(UNIKA_ROWS) + (unika_extra or []),
    )
    db_dir = tmp_path / "db"
    build_db(
        input_dir=input_dir,
        db_dir=db_dir,
        skip_classifications=True,
        skip_slugs=True,
    )
    return sqlite3.connect(db_dir / "reg_meta.db")


class TestDeliveryColumnHygiene:
    """Kolumnnamn is trimmed (and blanks skipped) at the SCB read boundary.

    The real export carries a handful of whitespace-dirty spellings
    ('  Pris', 'Lan ') and ~3.3K blank values. Untrimmed, the dirty
    spellings shard rule-2 connectivity into bogus split-sibling variables
    (corpus: 'Bransle' vs '  Bransle' split var 1721 into
    bransleforbrukning + bransleforbrukning-2); blanks shipped as
    empty-string `variable_alias` rows."""

    def test_whitespace_twin_spellings_reunite(self, tmp_path: Path) -> None:
        # Same var_id delivered as 'Lan ' (2020) then 'Lan' (2021): distinct
        # raw spellings on distinct cvids — pre-trim these never co-occur, so
        # rule 2 puts them in disjoint components and triage splits the
        # variable. Post-trim they are one spelling, one component.
        conn = _hygiene_build(
            tmp_path,
            [
                _var_row(
                    colname="Lan ",
                    cvid=9001,
                    var_id=900,
                    varname="LanVar",
                    year="2020",
                    regver_id=110,
                ),
                _var_row(
                    colname="Lan",
                    cvid=9002,
                    var_id=900,
                    varname="LanVar",
                    year="2021",
                    regver_id=111,
                ),
            ],
        )
        n_vars = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE provider_key = '900'"
        ).fetchone()[0]
        assert n_vars == 1
        aliases = conn.execute(
            "SELECT DISTINCT va.delivery_column_name FROM variable_alias va "
            "JOIN variable v ON v.variable_id = va.variable_id "
            "WHERE v.provider_key = '900'"
        ).fetchall()
        assert aliases == [("Lan",)]
        state_cols = conn.execute(
            "SELECT DISTINCT vs.delivery_column_name FROM variable_state vs "
            "JOIN variable v ON v.variable_id = vs.variable_id "
            "WHERE v.provider_key = '900'"
        ).fetchall()
        assert state_cols == [("Lan",)]
        conn.close()

    def test_unika_flags_match_across_dirty_spelling(self, tmp_path: Path) -> None:
        # Registerinformation ships the clean spelling, unika the padded one.
        # Both sides trim at read, so the sensitivity-flag join
        # (`va.delivery_column_name = us.kolumnnamn`) still matches.
        conn = _hygiene_build(
            tmp_path,
            [
                _var_row(
                    colname="Lan",
                    cvid=9001,
                    var_id=900,
                    varname="LanVar",
                    year="2020",
                    regver_id=110,
                ),
            ],
            unika_extra=[
                PIPE.join(
                    [
                        "TESTREG",
                        "Testregistret",
                        "Individer",
                        "Individer",
                        "LanVar",
                        "Lan ",
                        "2020",
                        "2020",
                        "1",
                        "0",
                        "0",
                    ]
                ),
            ],
        )
        sensitive = conn.execute(
            "SELECT is_sensitive FROM variable WHERE provider_key = '900'"
        ).fetchone()[0]
        assert sensitive == 1
        conn.close()

    def test_blank_kolumnnamn_is_not_an_alias(self, tmp_path: Path) -> None:
        # A blank (or whitespace-only) Kolumnnamn means "no delivery header":
        # the variable still builds, its state carries NULL, and NO
        # variable_alias row ships (empty string is not a header).
        conn = _hygiene_build(
            tmp_path,
            [
                _var_row(
                    colname="",
                    cvid=9101,
                    var_id=910,
                    varname="HeaderlessVar",
                    year="2020",
                    regver_id=110,
                ),
                _var_row(
                    colname="  ",
                    cvid=9102,
                    var_id=911,
                    varname="PaddedBlankVar",
                    year="2020",
                    regver_id=110,
                ),
            ],
        )
        for key, name in (("910", "HeaderlessVar"), ("911", "PaddedBlankVar")):
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM variable WHERE provider_key = ?", (key,)
                ).fetchone()[0]
                == 1
            ), name
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM variable_alias va "
                    "JOIN variable v ON v.variable_id = va.variable_id "
                    "WHERE v.provider_key = ?",
                    (key,),
                ).fetchone()[0]
                == 0
            ), name
            state_cols = conn.execute(
                "SELECT DISTINCT vs.delivery_column_name FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.provider_key = ?",
                (key,),
            ).fetchall()
            assert state_cols == [(None,)], (name, state_cols)
        conn.close()


# ── name-field read-boundary hygiene (#366) ────────────────────────────────


class TestNameFieldHygiene:
    """Variabelnamn / Registernamn / Registervariantnamn are trimmed at the
    SCB read boundary, in lockstep with the join keys they participate in.

    The real export carries ~1,503 padded Variabelnamn rows (plus 9
    Registernamn / 12 Registervariantnamn). Untrimmed they are display noise
    and a latent join fragility: the same value under a dirty spelling would
    silently drop the `unika_join` / sensitivity-flag (`v.name =
    us.variabelnamn`) / coalescer joins the moment one CSV is cleaned but not
    the other."""

    def test_variable_name_trimmed_and_clean_spelling_wins(
        self, tmp_path: Path
    ) -> None:
        # Same var_id delivered as 'LanVar ' (2020, padded) then 'LanVar'
        # (2021, clean): one variable either way (identity is (rid, var_id)),
        # but the first-non-empty fill runs on trimmed values, so the shipped
        # name carries no padding regardless of row order.
        conn = _hygiene_build(
            tmp_path,
            [
                _var_row(
                    colname="Lan",
                    cvid=9201,
                    var_id=920,
                    varname="LanVar ",
                    year="2020",
                    regver_id=110,
                ),
                _var_row(
                    colname="Lan",
                    cvid=9202,
                    var_id=920,
                    varname="LanVar",
                    year="2021",
                    regver_id=111,
                ),
            ],
        )
        names = conn.execute(
            "SELECT name FROM variable WHERE provider_key = '920'"
        ).fetchall()
        # One variable, trimmed name — regardless of which row was read first.
        # (variable_instance is dropped by build's end, so the per-cvid raw
        # name is asserted indirectly: the cross-file join test below would
        # break if `variable_instance.variabelnamn` were left untrimmed.)
        assert names == [("LanVar",)]
        conn.close()

    def test_unika_flags_match_across_dirty_varname(self, tmp_path: Path) -> None:
        # Registerinformation ships the clean Variabelnamn, unika the padded
        # one. Both sides trim at read, so the sensitivity-flag join
        # (`v.name = us.variabelnamn`) still matches.
        conn = _hygiene_build(
            tmp_path,
            [
                _var_row(
                    colname="Lan",
                    cvid=9201,
                    var_id=920,
                    varname="LanVar",
                    year="2020",
                    regver_id=110,
                ),
            ],
            unika_extra=[
                PIPE.join(
                    [
                        "TESTREG",
                        "Testregistret",
                        "Individer",
                        "Individer",
                        "LanVar ",
                        "Lan",
                        "2020",
                        "2020",
                        "1",
                        "0",
                        "0",
                    ]
                ),
            ],
        )
        sensitive = conn.execute(
            "SELECT is_sensitive FROM variable WHERE provider_key = '920'"
        ).fetchone()[0]
        assert sensitive == 1
        conn.close()
