"""A4.1 SCB adapter / provider-blind materializer tests.

Fast fixture/unit coverage for the adapter→IR→materializer seam introduced by
the A4.1 refactor (plan §8 items 1-6). These do NOT replace the real-data
dbdiff gate (the orchestrator runs that against the 14GB seed) — they pin the
adapter contract, IR emit order, determinism, and the provenance emit-but-
discard rule on a tiny synthetic fixture.
"""

from __future__ import annotations

import inspect
import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import write_scb_input
from reg_meta_build.db import DDL, build_db, seed_providers
from reg_meta_build.dbdiff import diff_db_content
from reg_meta_build.ir import (
    IRDeliveryProvenance,
    IRRegister,
    IRValueCode,
    IRValueSet,
    IRVariable,
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


# ── 5. Provenance emit-but-discard (§5) ────────────────────────────────────


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

    def test_built_db_has_no_provenance_side_effect(self, tmp_path: Path) -> None:
        """The provenance DB scaffolding stays EMPTY in A4.1 — emitting the IR
        provenance objects must not populate it (the materializer discards)."""
        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        write_scb_input(input_dir)
        build_db(
            input_dir=input_dir,
            db_dir=db_dir,
            skip_classifications=True,
            skip_slugs=True,
        )
        prov_path = db_dir / "reg_meta.provenance.db"
        assert prov_path.exists()
        prov = sqlite3.connect(prov_path)
        try:
            # The provenance scaffold ships an empty build_manifest table; A4.2
            # populates it. A4.1 must leave it empty.
            n = prov.execute("SELECT COUNT(*) FROM build_manifest").fetchone()[0]
            assert n == 0
        finally:
            prov.close()


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
