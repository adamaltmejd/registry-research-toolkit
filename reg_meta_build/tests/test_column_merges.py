"""Column-merge curation + auto case-fold (#196): the rule-2 connectivity
normalizations in `_coalesce_variable_states` that stop a split-container var
from sharding one concept's columns into sibling fragments.

Layers, mirroring the fold-override tests:
  - `TestLoadColumnMerges` — the loader's good parse (case-folded output) + every
    load-time failure mode (canonical-int ids, ≥2 distinct columns after folding,
    no within-group / cross-group overlap, no fold-to-empty column).
  - `TestColumnMergeKeyIsPerVar` — the key is `(register_id, var_id)`, so a merge
    spanning two var_ids (the #197 cross-var_id shape) is unrepresentable.
  - `TestAutoCaseFoldBuild` — real `build_db`: case twins under separate cvids
    collapse to ONE sibling with NO curation (the automatic half of #196).
  - `TestColumnMergeBuild` — real `build_db`: a curated merge unifies
    never-co-occurring era-rename twins; stale curation FAILS the build; an
    absent-register entry is inert.

Fully synthetic (CLAUDE.md): the repo `column_merges.toml` is keyed on real SCB
register ids, so it never touches these register_id=1 cases; the build tests
monkeypatch `repo_column_merges_path` to inject their own curation.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import (
    PIPE,
    REGISTERINFORMATION_ROWS,
    VARDEMANGDER_ROWS,
    _var_row,
    write_scb_input,
)
from _shared_fixtures import _write_fixture_slug_dir
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.column_merges import load_column_merges
from reg_meta_build.db import build_db

if TYPE_CHECKING:
    from pathlib import Path


# ── loader unit tests ──────────────────────────────────────────────────────


class TestLoadColumnMerges:
    @staticmethod
    def _load(tmp_path: Path, text: str) -> dict:
        path = tmp_path / "column_merges.toml"
        path.write_text(text, encoding="utf-8")
        return load_column_merges(path)

    def test_good_parse_is_case_folded(self, tmp_path: Path) -> None:
        # Columns fold to the rule-2 connectivity key (lowercase, diacritics
        # stripped) — TOML casing is cosmetic.
        m = self._load(
            tmp_path,
            '[[merge]]\nregister_id = 24\nvar_id = 56\ncolumns = ["PNR", "PersonNr"]\n',
        )
        assert m == {(24, 56): [frozenset({"pnr", "personnr"})]}

    def test_two_groups_same_var_are_two_entries(self, tmp_path: Path) -> None:
        m = self._load(
            tmp_path,
            '[[merge]]\nregister_id = 5\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
            '[[merge]]\nregister_id = 5\nvar_id = 9\ncolumns = ["C", "D"]\n',
        )
        assert m == {(5, 9): [frozenset({"a", "b"}), frozenset({"c", "d"})]}

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        assert load_column_merges(None) == {}
        assert load_column_merges(tmp_path / "absent.toml") == {}

    def test_valid_file_with_no_entries_is_empty(self, tmp_path: Path) -> None:
        assert self._load(tmp_path, "# no merge entries yet\n") == {}

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "[[merge]]\nregister_id = = 1\n")
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "column_merge_toml_unreadable"

    def test_single_merge_table_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path, '[merge]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n'
            )
        assert exc.value.code == "column_merge_invalid"

    def test_scalar_merge_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "merge = 5\n")
        assert exc.value.code == "column_merge_invalid"

    def test_non_table_merge_entry_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "merge = [1, 2]\n")
        assert exc.value.code == "column_merge_invalid"

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[merges]]` (typo) would silently disable ALL curation → loud error.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[merges]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "merges" in exc.value.message

    def test_bad_ids_rejected(self, tmp_path: Path) -> None:
        # Canonical-int contract shared with fold_overrides/codelivery
        # (`_curation.canonical_int`): leading-zero string, float, bool, missing.
        for ids in (
            'register_id = 1\nvar_id = "01"',
            "register_id = 1\nvar_id = 1.5",
            "register_id = true\nvar_id = 9",
            "register_id = 1",
        ):
            with pytest.raises(RegMetaError) as exc:
                self._load(tmp_path, f'[[merge]]\n{ids}\ncolumns = ["A", "B"]\n')
            assert exc.value.code == "column_merge_invalid"

    def test_singleton_columns_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path, '[[merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A"]\n'
            )
        assert exc.value.code == "column_merge_invalid"

    def test_non_string_or_empty_column_rejected(self, tmp_path: Path) -> None:
        for cols in ('["A", 5]', '["A", ""]'):
            with pytest.raises(RegMetaError) as exc:
                self._load(
                    tmp_path,
                    f"[[merge]]\nregister_id = 1\nvar_id = 9\ncolumns = {cols}\n",
                )
            assert exc.value.code == "column_merge_invalid"

    def test_case_twin_only_group_rejected(self, tmp_path: Path) -> None:
        # `PNR`/`pnr` fold to ONE column — the auto case-fold already covers it,
        # so a group that survives only on case spelling is a no-op → reject.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["PNR", "pnr"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "case-folding" in exc.value.message

    def test_fold_to_empty_column_rejected(self, tmp_path: Path) -> None:
        # A column with no ASCII content folds to "" and can never match a
        # rule-2 node-col (the coalescer keeps such a column raw).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["Ω", "B"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "empty" in exc.value.message

    def test_overlapping_columns_across_groups_rejected(self, tmp_path: Path) -> None:
        # `B` in two groups of the same (register, var) — compared FOLDED, so the
        # second group's `b` collides with the first group's `B`.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
                '[[merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["b", "C"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "b" in exc.value.message


class TestColumnMergeKeyIsPerVar:
    """The key is `(register_id, var_id)`: the same columns under two different
    vars are two INDEPENDENT entries, and a merge spanning two var_ids — the
    cross-var_id column SHARING of #197 — cannot be expressed."""

    def test_same_columns_different_vars_are_distinct_keys(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "column_merges.toml"
        path.write_text(
            '[[merge]]\nregister_id = 1\nvar_id = 100\ncolumns = ["A", "B"]\n\n'
            '[[merge]]\nregister_id = 1\nvar_id = 200\ncolumns = ["A", "B"]\n',
            encoding="utf-8",
        )
        m = load_column_merges(path)
        assert set(m) == {(1, 100), (1, 200)}
        assert m[(1, 100)] == [frozenset({"a", "b"})]
        assert m[(1, 200)] == [frozenset({"a", "b"})]


# ── build-driven end-to-end ─────────────────────────────────────────────────

# Disjoint codings so each cvid mints its own value set (and state group).
_CODING_A = [("11", "Alpha ett"), ("12", "Alpha två"), ("13", "Alpha tre")]
_CODING_B = [("21", "Beta ett"), ("22", "Beta två"), ("23", "Beta tre")]
_CODING_C = [("31", "Gamma ett"), ("32", "Gamma två"), ("33", "Gamma tre")]


def _vm_rows(cvid: int, version: str, codes: list[tuple[str, str]]) -> list[str]:
    return [PIPE.join([version, "1", kod, ben, str(cvid), ""]) for kod, ben in codes]


def _build(
    tmp_path: Path, ri_extra: list[str], vm_extra: list[str]
) -> sqlite3.Connection:
    input_dir = tmp_path / "input"
    db_dir = tmp_path / "db"
    slug_dir = tmp_path / "slugs"
    for d in (input_dir, db_dir, slug_dir):
        d.mkdir()
    write_scb_input(
        input_dir,
        registerinformation_rows=REGISTERINFORMATION_ROWS + ri_extra,
        vardemangder_rows=VARDEMANGDER_ROWS + vm_extra,
    )
    _write_fixture_slug_dir(slug_dir)
    build_db(
        input_dir=input_dir, db_dir=db_dir, skip_classifications=True, slug_dir=slug_dir
    )
    return sqlite3.connect(db_dir / "reg_meta.db")


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
        _vm_rows(5001, "AlphaA", _CODING_A)
        + _vm_rows(5002, "BetaB", _CODING_B)
        + _vm_rows(5003, "GammaC", _CODING_C)
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
        conn = _build(tmp_path, ri, vm)
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
        conn = _build(tmp_path, ri, vm)
        try:
            vids = _sibling_vids(conn, 505)
            n_vars = _n_vars(conn, 505)
        finally:
            conn.close()
        assert vids["Kon"] == vids["Kön"]
        assert n_vars == 2


class TestColumnMergeBuild:
    """The curated half of #196: an era-rename twin (`PNR` → `PersonNr`) shares
    no case identity, so only the maintainer can assert it — the merge normalizes
    both to one rule-2 node."""

    @staticmethod
    def _patch_merges(tmp_path: Path, monkeypatch, text: str) -> None:
        path = tmp_path / "column_merges.toml"
        path.write_text(text, encoding="utf-8")
        import reg_meta_build.column_merges as _cm

        monkeypatch.setattr(_cm, "repo_column_merges_path", lambda: path)

    def test_merge_unifies_era_rename_twins(self, tmp_path: Path, monkeypatch) -> None:
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        self._patch_merges(
            tmp_path,
            monkeypatch,
            '[[merge]]\nregister_id = 1\nvar_id = 510\ncolumns = ["PNR", "PersonNr"]\n',
        )
        conn = _build(tmp_path, ri, vm)
        try:
            vids = _sibling_vids(conn, 510)
            n_vars = _n_vars(conn, 510)
        finally:
            conn.close()
        assert vids["PNR"] == vids["PersonNr"]  # one identity across the rename
        assert vids["Kommun"] != vids["PNR"]
        assert n_vars == 2

    def test_no_merge_fragments_the_rename_twin(self, tmp_path: Path) -> None:
        # The contrast: without curation the never-co-occurring rename twin is
        # its own component, and the split container shards it into a third
        # sibling — the #196 fragmentation this surface exists to fix.
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        conn = _build(tmp_path, ri, vm)
        try:
            vids = _sibling_vids(conn, 510)
            n_vars = _n_vars(conn, 510)
        finally:
            conn.close()
        assert vids["PNR"] != vids["PersonNr"]
        assert n_vars == 3

    def test_stale_merge_column_fails_build(self, tmp_path: Path, monkeypatch) -> None:
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        self._patch_merges(
            tmp_path,
            monkeypatch,
            '[[merge]]\nregister_id = 1\nvar_id = 510\ncolumns = ["PNR", "Bogus"]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            _build(tmp_path, ri, vm)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "column_merge_unknown_column"
        assert "bogus" in exc.value.message  # folded form
        assert "pnr" in exc.value.message  # observed columns listed (actionable)

    def test_merge_for_absent_register_is_inert(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # An entry whose REGISTER isn't in this build (synthetic / partial /
        # SOS-only build) is silently ignored — the escape that keeps the repo
        # column_merges.toml from failing every register_id=1 fixture build.
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        self._patch_merges(
            tmp_path,
            monkeypatch,
            '[[merge]]\nregister_id = 195\nvar_id = 510\ncolumns = ["X", "Y"]\n',
        )
        conn = _build(tmp_path, ri, vm)
        try:
            n_vars = _n_vars(conn, 510)
        finally:
            conn.close()
        assert n_vars == 3  # untouched — the un-merged fragmentation shape
