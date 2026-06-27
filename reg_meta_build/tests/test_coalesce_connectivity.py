"""SCB coalescer rule-2 (kolumnnamn) connectivity build behavior.

Covers the AUTOMATIC half of #196 — case/diacritic column twins under separate
cvids fold to one rule-2 node-col with no curation — and its co-delivery guard
(parallel same-edition spellings are NOT folded). It also pins the #846 outcome:
a never-co-occurring era-rename twin that is NOT a case/diacritic variant
(`PNR` → `PersonNr`) forms its OWN component and SPLITS a split-container var into
sibling variables. That split is intentional — the retired `column_merge` surface
used to unify the twins by fiat; the cross-era continuity now rides a
representation-grain `replaced_by` succession edge (`curation/relations.toml`) as
navigation, and the dense `personnr` sibling keeps the pinned slug — so no resolver
is involved (#846).

Fully synthetic (CLAUDE.md): builds from in-memory CSV rows; the surviving
auto-fold connectivity is a pure build mechanic, no curation file involved."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _csv_fixtures import _var_row
from _shared_fixtures import (
    CODING_A,
    CODING_B,
    CODING_C,
    build_with_rows,
    vm_rows,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


def _rename_container(
    *, var_id: int, old_col: str, new_col: str, other_col: str = "Kommun"
) -> tuple[list[str], list[str]]:
    """One var_id that IS a split container (old_col + other_col co-deliver in
    edition 600) and carries an era-rename twin: new_col arrives ALONE in edition
    601 (never co-occurs with old_col). Distinct codings per cvid keep the three
    state groups distinct; the 2020/2021 split keeps the per-column timelines
    conflict-free."""
    ri = [
        _var_row(
            colname=old_col,
            cvid=5001,
            var_id=var_id,
            varname="RenameVar",
            year="2020",
            regver_id=600,
            data_length="3",
        ),
        _var_row(
            colname=other_col,
            cvid=5002,
            var_id=var_id,
            varname="RenameVar",
            year="2020",
            regver_id=600,
            data_length="3",
        ),
        _var_row(
            colname=new_col,
            cvid=5003,
            var_id=var_id,
            varname="RenameVar",
            year="2021",
            regver_id=601,
            data_length="3",
        ),
    ]
    vm = (
        vm_rows(5001, "AlphaA", CODING_A)
        + vm_rows(5002, "BetaB", CODING_B)
        + vm_rows(5003, "GammaC", CODING_C)
    )
    return ri, vm


def _sibling_vids(conn: sqlite3.Connection, var_id: int) -> dict[str, int]:
    """{delivery_column_name: variable_id} for the var's emitted states."""
    return {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT vs.delivery_column_name, vs.variable_id "
            "FROM variable_state vs JOIN variable v ON v.variable_id = vs.variable_id "
            "WHERE v.provider_key = CAST(? AS TEXT) "
            "AND vs.delivery_column_name IS NOT NULL",
            (var_id,),
        )
    }


def _n_vars(conn: sqlite3.Connection, var_id: int) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM variable WHERE register_id = 1 "
        "AND provider_key = CAST(? AS TEXT)",
        (var_id,),
    ).fetchone()[0]


class TestAutoCaseFoldBuild:
    """The automatic half of #196: case twins under SEPARATE cvids fold to one
    rule-2 node with no curation. Pre-#196 the twin column was its own component,
    so a split-container var minted it a fragment sibling of its own."""

    def test_case_twin_joins_its_sibling(self, tmp_path: Path) -> None:
        # HEMKOMMUN (2021, alone) is a case twin of Hemkommun (2020, co-delivered
        # with Skolkommun). The container splits Hemkommun/Skolkommun — and the
        # twin lands on the Hemkommun sibling instead of fragmenting into a third.
        ri, vm = _rename_container(
            var_id=500, old_col="Hemkommun", new_col="HEMKOMMUN", other_col="Skolkommun"
        )
        conn = build_with_rows(tmp_path, ri, vm)
        try:
            vids = _sibling_vids(conn, 500)
            n_vars = _n_vars(conn, 500)
        finally:
            conn.close()
        assert vids["Hemkommun"] == vids["HEMKOMMUN"]
        assert vids["Skolkommun"] != vids["Hemkommun"]
        assert n_vars == 2  # one per concept — NOT a third case-twin fragment

    def test_diacritic_twin_joins_its_sibling(self, tmp_path: Path) -> None:
        # Kön/Kon differ only by diacritic → one node (`kon`), same variable.
        ri, vm = _rename_container(
            var_id=505, old_col="Kon", new_col="Kön", other_col="Alder"
        )
        conn = build_with_rows(tmp_path, ri, vm)
        try:
            vids = _sibling_vids(conn, 505)
            n_vars = _n_vars(conn, 505)
        finally:
            conn.close()
        assert vids["Kon"] == vids["Kön"]
        assert n_vars == 2

    def test_co_delivered_twins_are_not_folded(self, tmp_path: Path) -> None:
        # The guard: `Niva` + `Nivå` ship in the SAME edition carrying two
        # distinct codings (the real HRE shape). Folding them would put both
        # codings on one column and the co-delivery invariant would drop one —
        # instead they keep raw node-cols, the triage folds them by stem into
        # ONE variable, and BOTH codings ship as label-discriminated states.
        ri = [
            _var_row(
                colname="Niva",
                cvid=5201,
                var_id=520,
                varname="NivaVar",
                year="2020",
                regver_id=600,
                data_length="3",
            ),
            _var_row(
                colname="Nivå",
                cvid=5202,
                var_id=520,
                varname="NivaVar",
                year="2020",
                regver_id=600,
                data_length="3",
            ),
        ]
        vm = vm_rows(5201, "Tre grupper", CODING_A) + vm_rows(
            5202, "Två grupper", CODING_B
        )
        conn = build_with_rows(tmp_path, ri, vm)  # must not raise unresolved-codelivery
        try:
            rows = conn.execute(
                "SELECT vs.delivery_column_name, vs.variable_id, vs.value_set_id, "
                "       vs.value_set_version_label "
                "FROM variable_state vs JOIN variable v ON v.variable_id = vs.variable_id "
                "WHERE v.provider_key = '520' AND vs.value_set_id IS NOT NULL"
            ).fetchall()
            n_vars = _n_vars(conn, 520)
        finally:
            conn.close()
        assert n_vars == 1  # stem-folded into one variable, not split, not lost
        assert {r[0] for r in rows} == {"Niva", "Nivå"}  # both columns shipped
        assert len({r[2] for r in rows}) == 2  # both codings survive
        assert len({r[3] for r in rows}) == 2  # discriminated by label


class TestEraRenameTwinSplits:
    """#846: a never-co-occurring era-rename twin that shares no case identity
    (`PNR` → `PersonNr`) gets NO by-fiat unification anymore — the `column_merge`
    surface that did that is retired. This test covers that split: the coalescer
    forms the twin into its own rule-2 component and shards a split-container var
    into sibling variables, with no `column_merge` involved. (Cross-era continuity
    itself is recorded out-of-band as a representation `replaced_by` succession edge
    in `curation/relations.toml`, exercised by the relations tests.)"""

    def test_rename_twin_splits_into_siblings(self, tmp_path: Path) -> None:
        # PNR (2020, co-delivered with Kommun) and PersonNr (2021, alone) never
        # co-occur and are not case twins → three sibling variables, one per
        # disjoint column component. This is the deliberate split #846 bridges.
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        conn = build_with_rows(tmp_path, ri, vm)
        try:
            vids = _sibling_vids(conn, 510)
            n_vars = _n_vars(conn, 510)
        finally:
            conn.close()
        assert vids["PNR"] != vids["PersonNr"]  # NOT unified — split siblings
        assert vids["Kommun"] != vids["PNR"]
        assert n_vars == 3
