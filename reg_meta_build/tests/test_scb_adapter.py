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

import pytest
from _csv_fixtures import (
    PIPE,
    REGISTERINFORMATION_ROWS,
    UNIKA_ROWS,
    _var_row,
    write_scb_input,
)
from _shared_fixtures import (
    CODING_A,
    CODING_B,
    build_with_rows,
    errata_column,
    errata_delivered,
    errata_version,
    vm_rows,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.db import DDL, build_db, seed_providers
from reg_meta_build.dbdiff import diff_db_content
from reg_meta_build.id import _CANONICAL_SCB_BIT, is_canonical_scb
from reg_meta_build.ir import (
    IRDeliveryProvenance,
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableState,
    IRVariant,
    IRWarning,
)
from reg_meta_build.sources import scb as scb_module
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


# ── 1b. SCB build-scratch performance contracts ────────────────────────────


class TestAdapterScratchIndexes:
    def test_stamped_variable_id_index_exists_for_post_stamp_readers(
        self, tmp_path: Path
    ) -> None:
        conn, _adapter, _objects = _drained_adapter(tmp_path)
        try:
            row = conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'index' "
                "AND name = 'idx_variable_instance_variable_id_cvid'"
            ).fetchone()
            assert row is not None
            assert "variable_instance(variable_id, cvid)" in row[0]

            plan = conn.execute(
                "EXPLAIN QUERY PLAN "
                "SELECT vi.operational_definition FROM variable_instance vi "
                "WHERE vi.variable_id = ? "
                "AND COALESCE(vi.operational_definition, '') <> '' "
                "ORDER BY vi.cvid LIMIT 1",
                (1,),
            ).fetchall()
            assert any(
                "idx_variable_instance_variable_id_cvid" in step[3] for step in plan
            )
        finally:
            conn.close()


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
        # ...and NOTHING outside `materialize()`'s isinstance dispatch (db.py).
        # Notably no value-set object: the value tables are adapter-written, so
        # mirroring the member corpus back into IR produces objects the
        # materializer has no branch for and silently drops.
        consumed = (
            IRDeliveryProvenance,
            IRRegister,
            IRVariable,
            IRVariableAlias,
            IRVariableState,
            IRVariant,
            IRWarning,
        )
        unconsumed = sorted(
            {type(o).__name__ for o in objects if not isinstance(o, consumed)}
        )
        assert not unconsumed, unconsumed

    def test_fk_referential_emit_order(self, tmp_path: Path) -> None:
        """Every IRVariableState's variable_id / register_variant_id is
        introduced by a prior object in the stream (parents precede children —
        the materializer can insert in stream order), and its value_set_id
        resolves against the value_set rows the adapter wrote during emit."""
        conn, _adapter, objects = _drained_adapter(tmp_path)
        value_sets = {
            row[0] for row in conn.execute("SELECT value_set_id FROM value_set")
        }
        seen_variables: set[int] = set()
        seen_variants: set[int] = set()
        for o in objects:
            if isinstance(o, IRVariant):
                seen_variants.add(o.register_variant_id)
            elif isinstance(o, IRVariable):
                seen_variables.add(o.variable_id)
            elif isinstance(o, IRVariableState):
                assert o.variable_id in seen_variables
                assert o.register_variant_id in seen_variants
                if o.value_set_id is not None:
                    assert o.value_set_id in value_sets


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
            conn, _adapter, objects = _drained_adapter(tmp_path / sub)
            variables = tuple(
                o.variable_id for o in objects if isinstance(o, IRVariable)
            )
            states = tuple(
                o.state_id for o in objects if isinstance(o, IRVariableState)
            )
            # value_sets carry no IR object — read the ids the adapter minted
            # straight off the working conn.
            value_sets = tuple(
                row[0]
                for row in conn.execute(
                    "SELECT value_set_id FROM value_set ORDER BY value_set_id"
                )
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


# ── 6. value_code / code_id integrity probe (R1) ───────────────────────────


class TestCodeIdIntegrity:
    def test_value_set_members_resolve_to_codes(self, tmp_path: Path) -> None:
        """Every `value_set_member` the adapter minted resolves to a
        `value_code` row. The working conn runs with `foreign_keys=OFF` and is
        never seen by `validate_built_db`, so this is the only check that the
        adapter linked its own value tables consistently — no code_id drift
        between the Vardemangder counter and the members it wrote."""
        conn, _adapter, _objects = _drained_adapter(tmp_path)
        members, orphans = conn.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE vc.code_id IS NULL) "
            "FROM value_set_member vsm "
            "LEFT JOIN value_code vc ON vc.code_id = vsm.code_id"
        ).fetchone()
        assert members > 0, "fixture should mint at least one value_set member"
        assert orphans == 0


# ── 6b. SCB value prestage cache ───────────────────────────────────────────


class TestValuePrestageCache:
    def test_reuses_valid_cache_without_reimporting_vardemangder(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        input_dir = tmp_path / "input"
        write_scb_input(input_dir)
        cache = tmp_path / "scb-value-prestage.sqlite"

        first_db = tmp_path / "db_first"
        build_db(
            input_dir=input_dir,
            db_dir=first_db,
            skip_classifications=True,
            skip_slugs=True,
            scb_value_prestage_cache=cache,
        )
        assert cache.exists()

        def fail_import(*_args, **_kwargs):
            raise AssertionError("valid prestage cache should skip Vardemangder import")

        monkeypatch.setattr(scb_module, "_import_vardemangder", fail_import)

        second_db = tmp_path / "db_second"
        build_db(
            input_dir=input_dir,
            db_dir=second_db,
            skip_classifications=True,
            skip_slugs=True,
            scb_value_prestage_cache=cache,
        )

        report = diff_db_content(
            first_db / "reg_meta.db",
            second_db / "reg_meta.db",
        )
        assert report.identical, report

    def test_operational_definition_change_does_not_stale_value_prestage(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        input_dir = tmp_path / "input"
        rows = [
            _var_row(
                colname="Kon",
                cvid=1001,
                var_id=101,
                varopdef="Original operational definition",
            )
        ]
        write_scb_input(input_dir, registerinformation_rows=rows)
        cache = tmp_path / "scb-value-prestage.sqlite"
        build_db(
            input_dir=input_dir,
            db_dir=tmp_path / "db_seed",
            skip_classifications=True,
            skip_slugs=True,
            scb_value_prestage_cache=cache,
        )

        rows = [
            _var_row(
                colname="Kon",
                cvid=1001,
                var_id=101,
                varopdef="Updated operational definition",
            )
        ]
        write_scb_input(input_dir, registerinformation_rows=rows)

        def fail_import(*_args, **_kwargs):
            raise AssertionError("op-def-only changes should not rebuild prestage")

        monkeypatch.setattr(scb_module, "_import_vardemangder", fail_import)

        db_dir = tmp_path / "db_replay"
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
            scb_value_prestage_cache=cache,
        )

        conn = sqlite3.connect(db_dir / "reg_meta.db")
        try:
            op_def = conn.execute(
                "SELECT operational_definition FROM variable "
                "WHERE operational_definition IS NOT NULL"
            ).fetchone()[0]
        finally:
            conn.close()
        assert op_def == "Updated operational definition"


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


def _build_from_ri_rows(
    tmp_path: Path,
    ri_extra: list[str],
    unika_extra: list[str] | None = None,
) -> sqlite3.Connection:
    """Build the standard SCB fixture plus `ri_extra` Registerinformation rows,
    and open the shipped DB."""
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
        conn = _build_from_ri_rows(
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
        conn = _build_from_ri_rows(
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
        conn = _build_from_ri_rows(
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


class TestEraOrdering:
    def test_latest_alias_and_type_follow_claimed_year_before_id(
        self, tmp_path: Path
    ) -> None:
        # SCB's edition ids are not chronological: the older delivery has the
        # larger id. The two case-only aliases and text-family types fold into
        # one state, whose displayed shape must still come from 2021.
        conn = _build_from_ri_rows(
            tmp_path,
            [
                _var_row(
                    colname="EraCol",
                    cvid=9900,
                    var_id=990,
                    varname="EraVar",
                    year="2020",
                    regver_id=9901,
                    data_type="char",
                ),
                _var_row(
                    colname="eracol",
                    cvid=9901,
                    var_id=990,
                    varname="EraVar",
                    year="2021",
                    regver_id=9900,
                    data_type="varchar",
                ),
            ],
        )
        try:
            states = conn.execute(
                "SELECT vs.delivery_column_name, vs.data_type "
                "FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '990'"
            ).fetchall()
            assert states == [("eracol", "varchar")]
        finally:
            conn.close()

    def test_disjoint_type_era_claims_preserve_the_middle_years(
        self, tmp_path: Path
    ) -> None:
        # Real TJOMF has four runs: the 2005..2010 int run carries an alternate
        # source text, while both tinyint runs and the 2011..2015 int run carry
        # SLH. Its older edition IDs are deliberately higher than every later
        # ID, and the older alias differs only by case. Claimed-year ordering
        # reunites the spelling as Tjomf; source drift routes the partition to
        # the timeline before residual hull collapse can erase 2011..2015.
        slh_source = "Lönestrukturstatistik, hela ekonomin (SLH)"
        alternate_source = "Lönestruktur, hela ekonomin :: Lönestruktur, hela ekonomin"
        old_tinyint = [
            _var_row(
                colname="TJOMF",
                cvid=40_000 + year,
                var_id=3876,
                varname="TjomfVar",
                year=str(year),
                regver_id=30_000 + year,
                data_type="tinyint",
                data_length="1",
                varsource=slh_source,
                register=("TESTREG", 1, 815),
            )
            for year in range(1995, 2005)
        ]
        alternate_int = [
            _var_row(
                colname="Tjomf",
                cvid=45_000 + year,
                var_id=3876,
                varname="TjomfVar",
                year=str(year),
                regver_id=20_000 + year,
                data_type="int",
                data_length="4",
                varsource=alternate_source,
                register=("TESTREG", 1, 815),
            )
            for year in range(2005, 2011)
        ]
        middle_ids = (4515, 5273, 5898, 7363, 9991)
        middle_int = [
            _var_row(
                colname="Tjomf",
                cvid=50_000 + year,
                var_id=3876,
                varname="TjomfVar",
                year=str(year),
                regver_id=regver_id,
                data_type="int",
                data_length="4",
                varsource=slh_source,
                register=("TESTREG", 1, 815),
            )
            for year, regver_id in zip(range(2011, 2016), middle_ids, strict=True)
        ]
        new_tinyint = [
            _var_row(
                colname="Tjomf",
                cvid=60_000 + year,
                var_id=3876,
                varname="TjomfVar",
                year=str(year),
                regver_id=1000 + year,
                data_type="tinyint",
                data_length="1",
                varsource=slh_source,
                register=("TESTREG", 1, 815),
            )
            for year in range(2016, 2019)
        ]
        conn = _build_from_ri_rows(
            tmp_path, old_tinyint + alternate_int + middle_int + new_tinyint
        )
        try:
            states = conn.execute(
                "SELECT vs.valid_from, vs.valid_to, vs.data_type, "
                "vs.source_register_text "
                "FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '3876' "
                "AND vs.register_variant_id = 815 "
                "ORDER BY vs.valid_from"
            ).fetchall()
            assert states == [
                ("1995-01-01", "2004-12-31", "tinyint", slh_source),
                ("2005-01-01", "2010-12-31", "int", alternate_source),
                ("2011-01-01", "2015-12-31", "int", slh_source),
                ("2016-01-01", "2018-12-31", "tinyint", slh_source),
            ]
        finally:
            conn.close()

    @pytest.mark.parametrize(
        ("var_id", "column", "first_length", "second_length", "second_cvid", "source"),
        [
            (22077, "t7a", "83", "93", 694104, "T7a"),
            (22080, "t7d", "59", "92", 694101, "T7d"),
        ],
    )
    def test_timeline_preserves_documented_second_half_claim(
        self,
        tmp_path: Path,
        var_id: int,
        column: str,
        first_length: str,
        second_length: str,
        second_cvid: int,
        source: str,
    ) -> None:
        # The real course columns change declared length between the two halves
        # of 1996. An older source-text era already makes each partition
        # timeline-owned; the higher-ID first half must neither make PASS1
        # discard regver 14892's disjoint second-half claim nor absorb its
        # interval as code-less drift.
        conn = _build_from_ri_rows(
            tmp_path,
            [
                _var_row(
                    colname=column,
                    cvid=second_cvid + 100,
                    var_id=var_id,
                    varname="Kursämne",
                    year="1995",
                    regver_id=14_000,
                    data_length=first_length,
                    varopdef="T6",
                    register=("TESTREG", 1, 396),
                ),
                _var_row(
                    colname=column,
                    cvid=second_cvid + 1,
                    var_id=var_id,
                    varname="Kursämne",
                    year="1996",
                    versionname="Första halvåret 1996",
                    regver_id=15_000,
                    data_length=first_length,
                    varopdef=source,
                    register=("TESTREG", 1, 396),
                ),
                _var_row(
                    colname=column,
                    cvid=second_cvid,
                    var_id=var_id,
                    varname="Kursämne",
                    year="1996",
                    versionname="Andra halvåret 1996",
                    regver_id=14892,
                    data_length=second_length,
                    varopdef=source,
                    register=("TESTREG", 1, 396),
                ),
            ],
        )
        try:
            states = conn.execute(
                "SELECT vs.valid_from, vs.valid_to, vs.data_length, "
                "vs.source_register_text "
                "FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = CAST(? AS TEXT) "
                "AND vs.register_variant_id = 396 "
                "ORDER BY vs.valid_from",
                (var_id,),
            ).fetchall()
            assert states == [
                ("1995-01-01", "1995-12-31", first_length, "T6"),
                ("1996-01-01", "1996-06-30", first_length, source),
                ("1996-07-01", "1996-12-31", second_length, source),
            ]
        finally:
            conn.close()

    def test_year_bearing_edition_outranks_unknown_year_before_id(
        self, tmp_path: Path
    ) -> None:
        # The yearless edition has the larger id, but it makes no chronology
        # claim. A documented year-bearing edition therefore supplies the
        # displayed alias and type; ids order unknown editions only among
        # themselves.
        conn = _build_from_ri_rows(
            tmp_path,
            [
                _var_row(
                    colname="EraCol",
                    cvid=9902,
                    var_id=991,
                    varname="EraVar",
                    year="2020",
                    versionname="Okänd version",
                    regver_id=9903,
                    data_type="char",
                ),
                _var_row(
                    colname="eracol",
                    cvid=9903,
                    var_id=991,
                    varname="EraVar",
                    year="2019",
                    regver_id=9902,
                    data_type="varchar",
                ),
            ],
        )
        try:
            states = conn.execute(
                "SELECT vs.delivery_column_name, vs.data_type "
                "FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '991'"
            ).fetchall()
            assert states == [("eracol", "varchar")]
        finally:
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
        conn = _build_from_ri_rows(
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
        conn = _build_from_ri_rows(
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

    def test_trimmed_unika_collision_keeps_sensitivity_flag(
        self, tmp_path: Path
    ) -> None:
        # Two unika rows collapse onto one trimmed PK ('LanVar' / 'LanVar '):
        # the non-sensitive ('0') row is listed FIRST, the sensitive ('1') row
        # second. A plain `INSERT OR IGNORE` would keep the first and drop the
        # flag (a PII-scanner false negative); the flag-OR accumulation must
        # preserve is_sensitive=1 regardless of row order.
        conn = _build_from_ri_rows(
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
                        "LanVar",
                        "Lan",
                        "2020",
                        "2020",
                        "0",
                        "0",
                        "0",
                    ]
                ),
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
        # unika_summary is dropped by build's end; the surviving signal is
        # is_sensitive. With `INSERT OR IGNORE` the first ('0') row would win
        # and this would be 0.
        sensitive = conn.execute(
            "SELECT is_sensitive FROM variable WHERE provider_key = '920'"
        ).fetchone()[0]
        assert sensitive == 1
        conn.close()


# ── 8. Multi-year edition names claim their whole span (Y-113) ──────────────


# befolkningsframskrivningar, the one entry in the SCB adapter's
# `_PROJECTION_REGISTERS`: (name, register_id, register_variant_id).
_PROJECTION_REG = ("PROGREG", 310, 3100)


class TestMultiYearEditionWindows:
    """A `registerversionnamn` that names several years is DELIVERED across all
    of them, so `variable_state` must cover the whole span.

    Before Y-113 the claim was the first year only (`extract_year`), narrowed
    inside that year by `edition_bounds` — so flergenerationsregistret collapsed
    to 1961 and every school-year register was a year short at the end of its
    series. These pin the shipped window per name shape; `TestEditionClaims` in
    test_triage.py pins the parse underneath.
    """

    def _window(
        self, conn: sqlite3.Connection, provider_key: str, register_id: int = 1
    ) -> tuple[str, str]:
        rows = conn.execute(
            "SELECT vs.valid_from, vs.valid_to FROM variable_state vs "
            "JOIN variable v ON v.variable_id = vs.variable_id "
            "WHERE v.register_id = ? AND v.provider_key = ?",
            (register_id, provider_key),
        ).fetchall()
        assert len(rows) == 1, f"expected one state for {provider_key}, got {rows}"
        return rows[0][0], rows[0][1]

    def _built(
        self,
        tmp_path: Path,
        versionname: str,
        year: str,
        register: tuple[str, int, int] = ("TESTREG", 1, 10),
    ) -> sqlite3.Connection:
        """One variable delivered by one `versionname` edition, in `register`
        (`(name, id, variant_id)`) — TESTREG unless the case is about the
        declared projection set."""
        return _build_from_ri_rows(
            tmp_path,
            [
                _var_row(
                    colname="SpanCol",
                    cvid=9700,
                    var_id=950,
                    varname="SpanVar",
                    year=year,
                    versionname=versionname,
                    regver_id=9800,
                    register=register,
                )
            ],
        )

    @pytest.mark.parametrize(
        ("versionname", "year", "expected"),
        [
            # innovation-foretag.
            ("2004 - 2006", "2004", ("2004-01-01", "2006-12-31")),
            # hreg doktorander.
            ("1971 - 2024", "1971", ("1971-01-01", "2024-12-31")),
            # flergenreg — every window collapsed to 1961 before Y-113.
            ("1961-01-01 –– 2025-12-31", "1961", ("1961-01-01", "2025-12-31")),
            # grundskola-ak9 / gymnasieskola-betyg / lararreg.
            ("Läsåret 2012/2013", "2012", ("2012-07-01", "2013-06-30")),
            # hreg grundutbildning.
            ("Läsåren 1993/1994 - 2024/2025", "1993", ("1993-07-01", "2025-06-30")),
            # ureg.
            ("Komvux HT 1988 - VT 2024", "1988", ("1988-07-01", "2024-06-30")),
            # utbildningsanalyser.
            (
                "Höstterminen 2020 - Vårterminen 2021",
                "2020",
                ("2020-07-01", "2021-06-30"),
            ),
            # ESF `Programperiod 2021-2027` ends after every other edition in
            # the fixture (the standard rows reach 2020) and still claims all of
            # it: nothing but the declared register set narrows a span.
            ("Programperiod 2021-2027", "2021", ("2021-01-01", "2027-12-31")),
            # Controls: a plain annual and a lone term are claimed as before.
            ("2018", "2018", ("2018-01-01", "2018-12-31")),
            ("Höstterminen 2018", "2018", ("2018-07-01", "2018-12-31")),
        ],
    )
    def test_shipped_window_is_the_full_named_span(
        self,
        tmp_path: Path,
        versionname: str,
        year: str,
        expected: tuple[str, str],
    ) -> None:
        conn = self._built(tmp_path, versionname, year)
        assert self._window(conn, "950") == expected
        conn.close()

    def test_declared_projection_register_keeps_its_vintage_year(
        self, tmp_path: Path
    ) -> None:
        # A version of a register in `_PROJECTION_REGISTERS` (310,
        # befolkningsframskrivningar) names the horizon its forecast reaches, so
        # it ships as the 2011 vintage. Nothing in the NAME says so — the SAME
        # name on any other register claims all 50 years, which is what makes
        # this the register's declared fact rather than the parser's guess.
        conn = self._built(tmp_path, "2011-2060", "2011", _PROJECTION_REG)
        assert self._window(conn, "950", register_id=310) == (
            "2011-01-01",
            "2011-12-31",
        )
        conn.close()
        conn = self._built(tmp_path / "other", "2011-2060", "2011")
        assert self._window(conn, "950") == ("2011-01-01", "2060-12-31")
        conn.close()

    def test_school_year_series_is_not_a_year_short_at_the_end(
        self, tmp_path: Path
    ) -> None:
        # The flagship shape: consecutive `Läsåret A/B` editions on one column.
        # Each claims HT..VT, so the interior calendar years tile exactly and the
        # series fuses into ONE state that runs to the last spring term. Read as
        # first years only it would start in January and stop a year early, at
        # 2012-01-01..2014-12-31.
        conn = _build_from_ri_rows(
            tmp_path,
            [
                _var_row(
                    colname="Betyg",
                    cvid=9700 + i,
                    var_id=950,
                    varname="BetygVar",
                    year=str(year),
                    versionname=f"Läsåret {year}/{year + 1}",
                    regver_id=9800 + i,
                )
                for i, year in enumerate((2012, 2013, 2014))
            ],
        )
        assert self._window(conn, "950") == ("2012-07-01", "2015-06-30")
        conn.close()

    def test_span_years_are_claimed_individually_not_as_a_hull(
        self, tmp_path: Path
    ) -> None:
        # The claim KEY SET carries run/gap structure: a multi-year version
        # contributes each year it spans, so a LATER single-year edition of the
        # same column fuses onto the span instead of leaving a phantom gap, and
        # a genuine gap stays a gap. Two editions, 2004-2006 then 2007.
        conn = _build_from_ri_rows(
            tmp_path,
            [
                _var_row(
                    colname="SpanCol",
                    cvid=9700,
                    var_id=950,
                    varname="SpanVar",
                    year="2004",
                    versionname="2004 - 2006",
                    regver_id=9800,
                ),
                _var_row(
                    colname="SpanCol",
                    cvid=9701,
                    var_id=950,
                    varname="SpanVar",
                    year="2007",
                    versionname="2007",
                    regver_id=9801,
                ),
            ],
        )
        assert self._window(conn, "950") == ("2004-01-01", "2007-12-31")
        conn.close()


# ── 9. SCB export errata (Y-114) ───────────────────────────────────────────


def _built_with_errata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ri_extra: list[str],
    errata_toml: str,
    vm_extra: list[str] | None = None,
) -> sqlite3.Connection:
    """Build the standard fixture plus `ri_extra`, with `errata_toml` standing in
    for the committed `scb_errata.toml`. `build_with_rows` writes the fixture
    slug dir the errata's `scb/testreg` + `individer` slugs resolve against."""
    import reg_meta_build.db as _db

    path = tmp_path / "scb_errata.toml"
    path.write_text(errata_toml, encoding="utf-8")
    monkeypatch.setattr(
        _db,
        "repo_curation_path",
        lambda name: path if name == "scb_errata.toml" else None,
    )
    return build_with_rows(tmp_path, ri_extra, vm_extra or [])


def _state_codes(conn: sqlite3.Connection, provider_key: str) -> list[str]:
    """The codes of the single value set on a variable's states, ascending."""
    return [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT vc.code FROM variable_state vs "
            "JOIN variable v ON v.variable_id = vs.variable_id "
            "JOIN value_set_member vsm ON vsm.value_set_id = vs.value_set_id "
            "JOIN value_code vc ON vc.code_id = vsm.code_id "
            "WHERE v.register_id = 1 AND v.provider_key = ? ORDER BY vc.code",
            (provider_key,),
        )
    ]


def _windows(conn: sqlite3.Connection, provider_key: str) -> list[tuple]:
    return conn.execute(
        "SELECT vs.valid_from, vs.valid_to "
        "FROM variable_state vs JOIN variable v ON v.variable_id = vs.variable_id "
        "WHERE v.register_id = 1 AND v.provider_key = ? ORDER BY vs.valid_from",
        (provider_key,),
    ).fetchall()


def _provenance_windows(conn: sqlite3.Connection, provider_key: str) -> list[tuple]:
    return conn.execute(
        "SELECT vs.valid_from, vs.valid_to, vs.provenance "
        "FROM variable_state vs JOIN variable v ON v.variable_id = vs.variable_id "
        "WHERE v.register_id = 1 AND v.provider_key = ? ORDER BY vs.valid_from",
        (provider_key,),
    ).fetchall()


def _states(conn: sqlite3.Connection, provider_key: str) -> list[tuple]:
    return conn.execute(
        "SELECT vs.delivery_column_name, vs.data_type, vs.data_length, "
        "vs.valid_from, vs.valid_to "
        "FROM variable_state vs JOIN variable v ON v.variable_id = vs.variable_id "
        "WHERE v.register_id = 1 AND v.provider_key = ? "
        "ORDER BY vs.valid_from, vs.delivery_column_name",
        (provider_key,),
    ).fetchall()


class TestScbErrata:
    """A curated errata entry is applied at Registerinformation grain before the
    coalescer, so a cloned or newly named coordinate produces ordinary
    `variable_state` output — windows, gaps, fusing and value sets all fall out
    of the existing passes.

    TESTREG/individer documents versions '2020' (regver 100), '2021' (101) and
    '2022' (102), so an entry can name one without inventing an edition.
    """

    def test_omitted_rows_extend_the_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The seeded LISA shape: SCB documents the column in its latest edition
        # only, the steward holds the two before it.
        ri = [
            _var_row(
                colname="DispCol",
                cvid=9310,
                var_id=931,
                varname="DispVar",
                year="2022",
                regver_id=102,
            )
        ]
        without = _build_from_ri_rows(tmp_path / "plain", ri)
        assert _windows(without, "931") == [("2022-01-01", "2022-12-31")]
        assert _provenance_windows(without, "931") == [
            ("2022-01-01", "2022-12-31", None)
        ]
        without.close()

        conn = _built_with_errata(
            tmp_path, monkeypatch, ri, errata_delivered("DispCol", "2020", "2021")
        )
        try:
            assert _provenance_windows(conn, "931") == [
                (
                    "2020-01-01",
                    "2021-12-31",
                    "errata:omitted-column-in-version\n"
                    "the steward holds DispCol for those years",
                ),
                ("2022-01-01", "2022-12-31", None),
            ]
        finally:
            conn.close()

    def test_blank_target_instance_is_named(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [
                _var_row(
                    colname="",
                    cvid=9311,
                    var_id=931,
                    varname="DispVar",
                    year="2021",
                    regver_id=101,
                ),
                _var_row(
                    colname="DispCol",
                    cvid=9312,
                    var_id=931,
                    varname="DispVar",
                    year="2022",
                    regver_id=102,
                ),
            ],
            errata_delivered("DispCol", "2021"),
        )
        try:
            assert _states(conn, "931") == [
                ("DispCol", "int", "1", "2021-01-01", "2021-12-31"),
                ("DispCol", "int", "1", "2022-01-01", "2022-12-31"),
            ]
        finally:
            conn.close()

    def test_multi_version_entry_names_blank_and_clones_absent_edition(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [
                _var_row(
                    colname="",
                    cvid=9321,
                    var_id=932,
                    varname="DispVar",
                    year="2021",
                    regver_id=101,
                ),
                _var_row(
                    colname="DispCol",
                    cvid=9322,
                    var_id=932,
                    varname="DispVar",
                    year="2022",
                    regver_id=102,
                ),
            ],
            errata_delivered("DispCol", "2020", "2021"),
        )
        try:
            assert _states(conn, "932") == [
                ("DispCol", "int", "1", "2020-01-01", "2021-12-31"),
                ("DispCol", "int", "1", "2022-01-01", "2022-12-31"),
            ]
        finally:
            conn.close()

    def test_named_blank_keeps_target_metadata(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [
                _var_row(
                    colname="",
                    cvid=9331,
                    var_id=933,
                    varname="MetaVar",
                    year="2021",
                    regver_id=101,
                    data_type="int",
                    data_length="2",
                ),
                _var_row(
                    colname="MetaCol",
                    cvid=9332,
                    var_id=933,
                    varname="MetaVar",
                    year="2022",
                    regver_id=102,
                    data_type="varchar",
                    data_length="10",
                ),
            ],
            errata_delivered("MetaCol", "2021"),
        )
        try:
            assert _states(conn, "933") == [
                ("MetaCol", "int", "2", "2021-01-01", "2021-12-31"),
                ("MetaCol", "varchar", "10", "2022-01-01", "2022-12-31"),
            ]
        finally:
            conn.close()

    def test_target_delivered_under_other_column_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(
                tmp_path,
                monkeypatch,
                [
                    _var_row(
                        colname="OtherCol",
                        cvid=9341,
                        var_id=934,
                        varname="DispVar",
                        year="2021",
                        regver_id=101,
                    ),
                    _var_row(
                        colname="DispCol",
                        cvid=9342,
                        var_id=934,
                        varname="DispVar",
                        year="2022",
                        regver_id=102,
                    ),
                ],
                errata_delivered("DispCol", "2021"),
            )
        assert exc.value.code == "scb_errata_delivered_under_other_column"
        assert "OtherCol" in exc.value.message

    def _timeline_rows(self) -> tuple[list[str], list[str]]:
        """One column delivered under two codings: coding A in 2018 and 2022,
        coding B in 2020. Distinct value sets over overlapping spans route the
        (variable, variant) through the coalescer's per-year TIMELINE, where each
        coding's won years RLE into runs — so an added year is visibly a run
        member, not just a widened hull. A 2016 filler edition gives the errata a
        documented version to name below."""
        ri = [
            _var_row(
                colname="TlCol",
                cvid=cvid,
                var_id=940,
                varname="TlVar",
                year=year,
                regver_id=regver,
            )
            for cvid, year, regver in (
                (9400, "2018", 9400),
                (9401, "2020", 9401),
                (9402, "2022", 9402),
            )
        ] + [
            _var_row(
                colname="FillCol",
                cvid=9410,
                var_id=941,
                varname="FillVar",
                year="2016",
                regver_id=9410,
            )
        ]
        vm = (
            vm_rows(9400, "AlphaA", CODING_A)
            + vm_rows(9402, "AlphaA", CODING_A)
            + vm_rows(9401, "BetaB", CODING_B)
        )
        return ri, vm

    def test_adjacent_documented_and_corrected_rows_stay_distinct(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ri, vm = self._timeline_rows()
        plain = tmp_path / "plain"
        plain.mkdir()
        without = build_with_rows(plain, ri, vm)
        assert _windows(without, "940") == [
            ("2018-01-01", "2018-12-31"),
            ("2020-01-01", "2020-12-31"),
            ("2022-01-01", "2022-12-31"),
        ]
        without.close()

        # 2021 is adjacent to coding A's documented 2022 run, but its correction
        # evidence must not label the provider-exported year (or be erased by it).
        conn = _built_with_errata(
            tmp_path, monkeypatch, ri, errata_delivered("TlCol", "2021"), vm_extra=vm
        )
        try:
            assert _windows(conn, "940") == [
                ("2018-01-01", "2018-12-31"),
                ("2020-01-01", "2020-12-31"),
                ("2021-01-01", "2021-12-31"),
                ("2022-01-01", "2022-12-31"),
            ]
        finally:
            conn.close()

    def test_non_adjacent_row_stays_a_separate_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ri, vm = self._timeline_rows()
        # 2016 touches none of coding A's years (2017 is delivered by nobody), so
        # it stays its own window instead of paving the gap.
        conn = _built_with_errata(
            tmp_path, monkeypatch, ri, errata_delivered("TlCol", "2016"), vm_extra=vm
        )
        try:
            assert _windows(conn, "940") == [
                ("2016-01-01", "2016-12-31"),
                ("2018-01-01", "2018-12-31"),
                ("2020-01-01", "2020-12-31"),
                ("2022-01-01", "2022-12-31"),
            ]
        finally:
            conn.close()

    def test_value_set_bearing_column_keeps_its_value_set(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The clone carries the source row's value-set link, so the added year
        # joins the SAME coalescer group (the value set anchors state identity)
        # and the extended window keeps the coding.
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [
                _var_row(
                    colname="CodedCol",
                    cvid=9330,
                    var_id=933,
                    varname="CodedVar",
                    year="2021",
                    regver_id=101,
                )
            ],
            errata_delivered("CodedCol", "2020"),
            vm_extra=vm_rows(9330, "AlphaA", CODING_A),
        )
        try:
            assert _windows(conn, "933") == [
                ("2020-01-01", "2020-12-31"),
                ("2021-01-01", "2021-12-31"),
            ]
            value_set_id = conn.execute(
                "SELECT vs.value_set_id FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = ?",
                ("933",),
            ).fetchone()[0]
            assert value_set_id is not None
            codes = conn.execute(
                "SELECT vc.code FROM value_set_member vsm "
                "JOIN value_code vc ON vc.code_id = vsm.code_id "
                "WHERE vsm.value_set_id = ? ORDER BY vc.code",
                (value_set_id,),
            ).fetchall()
            assert [c[0] for c in codes] == [code for code, _label in CODING_A]
        finally:
            conn.close()

    def test_co_delivered_column_clones_every_row_of_the_edition(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # SCB's 2022 edition carries TwinCol TWICE — two variables, each with its
        # own value set. The nearest edition is cloned whole, so the added 2021
        # coordinate is replayed once per real row. Picking one row would extend
        # one variable, drop the other, and leave which is which to whatever the
        # unordered source scan returned first.
        ri = [
            _var_row(
                colname="TwinCol",
                cvid=9500,
                var_id=950,
                varname="TwinVarA",
                year="2022",
                regver_id=102,
            ),
            _var_row(
                colname="TwinCol",
                cvid=9501,
                var_id=951,
                varname="TwinVarB",
                year="2022",
                regver_id=102,
            ),
        ]
        vm = vm_rows(9500, "AlphaA", CODING_A) + vm_rows(9501, "BetaB", CODING_B)
        conn = _built_with_errata(
            tmp_path, monkeypatch, ri, errata_delivered("TwinCol", "2021"), vm_extra=vm
        )
        try:
            assert _windows(conn, "950") == [
                ("2021-01-01", "2021-12-31"),
                ("2022-01-01", "2022-12-31"),
            ]
            assert _windows(conn, "951") == [
                ("2021-01-01", "2021-12-31"),
                ("2022-01-01", "2022-12-31"),
            ]
            # Each clone carries ITS source row's coding, not one row's dragged
            # onto both.
            assert _state_codes(conn, "950") == [c for c, _l in CODING_A]
            assert _state_codes(conn, "951") == [c for c, _l in CODING_B]
        finally:
            conn.close()

    def test_nearest_edition_is_measured_over_the_years_a_name_claims(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # SpanCol is delivered in a '2010-2012' edition (coding A) and a '2015'
        # one (coding B), and the errata adds the documented 2013 edition. The
        # range CLAIMS 2012, one year from 2013, where 2015 is two — reading only
        # each name's first year would put the range three away and hand 2013 to
        # coding B.
        ri = [
            _var_row(
                colname="SpanCol",
                cvid=9600,
                var_id=960,
                varname="SpanVar",
                year="2010",
                versionname="2010-2012",
                regver_id=9610,
            ),
            _var_row(
                colname="SpanCol",
                cvid=9602,
                var_id=960,
                varname="SpanVar",
                year="2015",
                regver_id=9612,
            ),
            # Gives the errata a documented 2013 edition to name.
            _var_row(
                colname="MarkCol",
                cvid=9603,
                var_id=961,
                varname="MarkVar",
                year="2013",
                regver_id=9613,
            ),
        ]
        vm = vm_rows(9600, "AlphaA", CODING_A) + vm_rows(9602, "BetaB", CODING_B)
        conn = _built_with_errata(
            tmp_path, monkeypatch, ri, errata_delivered("SpanCol", "2013"), vm_extra=vm
        )
        try:
            # 2013 inherits coding A, but remains separate because its evidence
            # must not relabel the documented 2010-2012 window. Cloning the 2015
            # row instead would give the corrected interval coding B.
            assert _windows(conn, "960") == [
                ("2010-01-01", "2012-12-31"),
                ("2013-01-01", "2013-12-31"),
                ("2015-01-01", "2015-12-31"),
            ]
        finally:
            conn.close()

    def test_extended_column_keeps_its_sensitivity_classification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The errata replay runs BEFORE the A1.2 sensitivity lift, so a synthetic
        # row is never a delivery coordinate the PII/identifier classification
        # skipped. TestCol/TestVar is `KansligVariabel = 1` in the fixture's
        # UnikaRegisterOchVariabler and SCB documents it in 2020 only; extending
        # it to 2022 must widen the window AND keep is_sensitive.
        conn = _built_with_errata(
            tmp_path, monkeypatch, [], errata_delivered("TestCol", "2021", "2022")
        )
        try:
            assert _windows(conn, "100") == [
                ("2020-01-01", "2020-12-31"),
                ("2021-01-01", "2022-12-31"),
            ]
            flags = conn.execute(
                "SELECT is_sensitive, is_identifier FROM variable "
                "WHERE register_id = 1 AND provider_key = '100'"
            ).fetchone()
            assert tuple(flags) == (1, 0)
        finally:
            conn.close()

    def test_declared_version_lands_as_a_register_version_and_a_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # SCB documents TESTREG/individer only through 2022; a [[version]] entry
        # declares the 2023 edition the steward holds, and a [[delivered]] entry
        # puts the column into it.
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [
                _var_row(
                    colname="AheadCol",
                    cvid=9340,
                    var_id=934,
                    varname="AheadVar",
                    year="2022",
                    regver_id=102,
                )
            ],
            errata_version("2023") + "\n" + errata_delivered("AheadCol", "2023"),
        )
        try:
            assert _windows(conn, "934") == [
                ("2022-01-01", "2022-12-31"),
                ("2023-01-01", "2023-12-31"),
            ]
            names = conn.execute(
                "SELECT registerversionnamn FROM register_version "
                "WHERE register_variant_id = 10 ORDER BY registerversionnamn"
            ).fetchall()
            assert [n[0] for n in names] == ["2020", "2021", "2022", "2023"]
        finally:
            conn.close()

    def test_declared_same_year_version_stays_accepted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [
                _var_row(
                    colname="SameYearCol",
                    cvid=9342,
                    var_id=935,
                    varname="SameYearVar",
                    year="2022",
                    regver_id=102,
                )
            ],
            errata_version("Höstterminen 2022")
            + "\n"
            + errata_delivered("SameYearCol", "Höstterminen 2022"),
        )
        try:
            # The term-specific edition and the annual provider edition overlap
            # at the catalog's year granularity. Existing availability resolution
            # keeps the documented interval; the term's evidence must not relabel
            # that entire interval as corrected.
            assert _provenance_windows(conn, "935") == [
                ("2022-01-01", "2022-12-31", None)
            ]
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM register_version "
                    "WHERE register_variant_id = 10 "
                    "AND registerversionnamn = 'Höstterminen 2022'"
                ).fetchone()[0]
                == 1
            )
        finally:
            conn.close()

    def test_column_with_no_real_row_is_a_column_entry_not_delivered(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(
                tmp_path, monkeypatch, [], errata_delivered("NeverDelivered", "2020")
            )
        assert exc.value.code == "scb_errata_no_source_row"
        assert exc.value.exit_code == EXIT_CONFIG

    def test_undocumented_version_must_be_declared(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(
                tmp_path, monkeypatch, [], errata_delivered("Kon", "2019")
            )
        assert exc.value.code == "scb_errata_unknown_version"
        assert exc.value.exit_code == EXIT_CONFIG

    def test_declared_older_version_keeps_newer_alias_and_type_latest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # 2019 clones the nearest documented row (2020, EraCol/char). Its minted
        # regver_id is above every source id, but claimed-year ordering keeps the
        # genuinely newer 2022 row (eracol/varchar) as the displayed shape.
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [
                _var_row(
                    colname="EraCol",
                    cvid=9340,
                    var_id=934,
                    varname="EraVar",
                    year="2020",
                    regver_id=100,
                    data_type="char",
                ),
                _var_row(
                    colname="eracol",
                    cvid=9341,
                    var_id=934,
                    varname="EraVar",
                    year="2022",
                    regver_id=102,
                    data_type="varchar",
                ),
            ],
            errata_version("2019") + "\n" + errata_delivered("EraCol", "2019"),
        )
        try:
            assert _windows(conn, "934") == [
                ("2019-01-01", "2019-12-31"),
                ("2020-01-01", "2020-12-31"),
                ("2022-01-01", "2022-12-31"),
            ]
            states = conn.execute(
                "SELECT vs.delivery_column_name, vs.data_type "
                "FROM variable_state vs "
                "JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.register_id = 1 AND v.provider_key = '934' "
                "ORDER BY vs.valid_from"
            ).fetchall()
            assert states == [
                ("EraCol", "char"),
                ("eracol", "varchar"),
                ("eracol", "varchar"),
            ]
        finally:
            conn.close()

    def test_one_fixed_version_retires_only_that_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # SCB has started shipping DispCol's 2022 row while 2020 and 2021 are
        # still missing. The failure must send the maintainer to `versions`, not
        # to the whole entry — deleting it would drop two live omissions.
        ri = [
            _var_row(
                colname="DispCol",
                cvid=9310,
                var_id=931,
                varname="DispVar",
                year="2022",
                regver_id=102,
            )
        ]
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(
                tmp_path,
                monkeypatch,
                ri,
                errata_delivered("DispCol", "2020", "2021", "2022"),
            )
        assert exc.value.code == "scb_errata_now_present"
        assert "'2022'" in exc.value.message
        assert "drop '2022' from that entry's `versions`" in exc.value.remediation
        assert "delete the entry" not in exc.value.remediation


# ── 10. SCB export errata: [[column]] (Y-116) ──────────────────────────────


def _minted(conn: sqlite3.Connection, column: str) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM variable WHERE register_id = 1 AND provider_key = ?",
        (column,),
    ).fetchone()
    conn.row_factory = None
    return row


class TestScbErrataColumn:
    """A `[[column]]` mints the variable SCB documents nowhere, at SOURCE grain:
    the synthetic rows go through the coalescer, so the state's window, slug,
    alias and classification are the ordinary passes' output — not a post-pass's
    hand-written row. Folds in the retired `variable_grafts.py` (`all_versions`)
    and `canonical_attach.py` (`versions`) surfaces.
    """

    def test_mints_variable_state_and_alias_over_every_edition(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _built_with_errata(tmp_path, monkeypatch, [], errata_column("HeldCol"))
        try:
            var = _minted(conn, "HeldCol")
            assert var["name"] == "HeldCol name"
            assert var["description"] == "HeldCol definition"
            assert var["definition"] is None  # SCB's own export field
            assert var["source_label"] == "scb-errata"
            assert is_canonical_scb(var["variable_id"])
            # TESTREG/individer is documented 2020-2022: `all_versions` claims
            # the whole span, and the window is the coalescer's, not a sentinel.
            assert _provenance_windows(conn, "HeldCol") == [
                (
                    "2020-01-01",
                    "2022-12-31",
                    "errata:steward-holdings\nthe steward holds HeldCol",
                )
            ]
            assert conn.execute(
                "SELECT delivery_column_name FROM variable_alias WHERE variable_id = ?",
                (var["variable_id"],),
            ).fetchall() == [("HeldCol",)]
            assert (
                conn.execute(
                    "SELECT slug FROM variable WHERE variable_id = ?",
                    (var["variable_id"],),
                ).fetchone()[0]
                == "heldcol"
            )
        finally:
            conn.close()

    def test_named_versions_window_to_those_editions(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _built_with_errata(
            tmp_path, monkeypatch, [], errata_column("DocCol", "2021", "2022")
        )
        try:
            assert _windows(conn, "DocCol") == [("2021-01-01", "2022-12-31")]
        finally:
            conn.close()

    def test_declared_flags_and_type_survive_the_sensitivity_lift(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The A1.2 lift resolves a variable through its numeric var_id; a minted
        # column has none, so the ENTRY's flags are the published ones.
        conn = _built_with_errata(
            tmp_path,
            monkeypatch,
            [],
            errata_column(
                "PnrCol", data_type="text", is_identifier=True, is_sensitive=True
            ),
        )
        try:
            var = _minted(conn, "PnrCol")
            assert (var["is_identifier"], var["is_sensitive"]) == (1, 1)
            assert (
                conn.execute(
                    "SELECT data_type FROM variable_state WHERE variable_id = ?",
                    (var["variable_id"],),
                ).fetchone()[0]
                == "text"
            )
        finally:
            conn.close()

    def test_absent_type_and_flags_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _built_with_errata(tmp_path, monkeypatch, [], errata_column("BareCol"))
        try:
            var = _minted(conn, "BareCol")
            assert (var["is_identifier"], var["is_sensitive"]) == (0, 0)
            assert (
                conn.execute(
                    "SELECT data_type FROM variable_state WHERE variable_id = ?",
                    (var["variable_id"],),
                ).fetchone()[0]
                is None
            )
        finally:
            conn.close()

    def test_ids_are_deterministic_and_stay_in_the_scb_band(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ids = []
        for run in ("a", "b"):
            (tmp_path / run).mkdir()
            conn = _built_with_errata(
                tmp_path / run, monkeypatch, [], errata_column("StableCol")
            )
            try:
                ids.append(_minted(conn, "StableCol")["variable_id"])
            finally:
                conn.close()
        assert ids[0] == ids[1]
        assert is_canonical_scb(ids[0])

    def test_split_sibling_stays_below_the_minted_sub_band(self) -> None:
        # `variable.variable_id` is AUTOINCREMENT, so a canonical-sub-band row
        # already in the table would drag every later triage split-sibling up
        # with it (`lastrowid` = MAX+1) and break `_check_errata_column_band`.
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(DDL)
            seed_providers(conn)
            conn.execute(
                "INSERT INTO register (register_id, provider_id, name) "
                "VALUES (1, 1, 'r')"
            )
            conn.execute(
                "INSERT INTO variable (variable_id, register_id, provider_key) "
                "VALUES (7, 1, '7')"
            )
            conn.execute(
                "INSERT INTO variable (variable_id, register_id, provider_key) "
                "VALUES (?, 1, 'HeldCol')",
                (_CANONICAL_SCB_BIT + 5,),
            )
            new_vid = scb_module._insert_split_sibling_variable(
                conn,
                register_id=1,
                var_id=7,
                shared=scb_module._inherited_variable_fields(conn, 7),
            )
            assert new_vid == 8
        finally:
            conn.close()

    def test_column_now_in_the_export_fails_the_build(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The mirror of `scb_errata_no_source_row`: once SCB documents the column
        # the entry would duplicate a live variable, so it fails — no silent skip
        # (the retired graft pass's behaviour).
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(tmp_path, monkeypatch, [], errata_column("Kon"))
        assert exc.value.code == "scb_errata_now_present"
        assert exc.value.exit_code == EXIT_CONFIG
        assert "[[delivered]]" in exc.value.remediation

    def test_present_column_check_folds_case_and_diacritics(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(tmp_path, monkeypatch, [], errata_column("kön"))
        assert exc.value.code == "scb_errata_now_present"

    def test_a_delivered_entry_does_not_blind_the_column_check(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Both kinds ask "is this in the export already?", of different things:
        # a [[delivered]] entry of its own versions, a [[column]] of the whole
        # variant. Answering the first must not cost the second its answer —
        # the committed file holds both kinds, so a build that skips the
        # [[column]] guard mints a duplicate of a live variable instead.
        ri = [
            _var_row(
                colname="DispCol",
                cvid=9310,
                var_id=931,
                varname="DispVar",
                year="2022",
                regver_id=102,
            )
        ]
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(
                tmp_path,
                monkeypatch,
                ri,
                errata_delivered("DispCol", "2020") + "\n" + errata_column("Kon"),
            )
        assert exc.value.code == "scb_errata_now_present"
        assert "'Kon'" in exc.value.message

    def test_unknown_version_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            _built_with_errata(
                tmp_path, monkeypatch, [], errata_column("HeldCol", "2019")
            )
        assert exc.value.code == "scb_errata_unknown_version"
        assert "all_versions" in exc.value.remediation
