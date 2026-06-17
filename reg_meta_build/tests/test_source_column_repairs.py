"""SCB pre-state source-column repairs (#524 consolidation of #196 + #261): the
two sibling sections of `curation/scb/source_column_repairs.toml` — `[[column_merge]]`
(rule-2 connectivity normalizations in `_coalesce_variable_states` that stop a
split-container var from sharding one concept's columns into sibling fragments)
and `[[fold_override]]` (forces DISJOINT-stem contested columns the triage stem
rule would split into one variable).

Layers:
  - `TestLoadColumnMerges` / `TestLoadFoldOverrides` — each loader's good parse
    (case-folded output) + every load-time failure mode (canonical-int ids, ≥2
    distinct columns after folding, no within-group / cross-group overlap, no
    fold-to-empty column). The two share the `_curation.load_column_groups` leaf
    validator, so the empty-fold reject is now unified across both.
  - `TestSiblingSections` — both sections coexist in one file; each loader reads
    only its own section and ignores the sibling's; an unknown third top-level key
    is still rejected.
  - `TestColumnMergeKeyIsPerVar` / `TestFoldOverrideKeyIsPerVar` — the key is
    `(register_id, var_id)`, so a group spanning two var_ids is unrepresentable.
  - `TestAutoCaseFoldBuild` — real `build_db`: case twins under separate cvids
    collapse to ONE sibling with NO curation (the automatic half of #196).
  - `TestColumnMergeBuild` — real `build_db`: a curated merge unifies
    never-co-occurring era-rename twins; stale curation FAILS the build; an
    absent-register entry is inert.
  - `TestFoldOverrideTriage` — `_triage_groups` driven (in-memory, deterministic):
    a curated entry folds disjoint columns into ONE variable; an empty surface is
    byte-identical to today's split; bad curation FAILS the build.
  - `TestFoldOverrideBuild` — a real `build_db` proving the db→adapter→coalesce→
    triage wiring and that the config error propagates through the CLI build.

Fully synthetic (CLAUDE.md): the repo `source_column_repairs.toml` is keyed on
real SCB register ids, so it never touches these register_id=1 cases; the build
tests monkeypatch `repo_source_column_repairs_path` to inject their own curation.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import _var_row
from _shared_fixtures import (
    CODING_A,
    CODING_B,
    CODING_C,
    build_with_rows,
    vm_rows,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.db import DDL
from reg_meta_build.source_column_repairs import (
    load_column_merges,
    load_fold_overrides,
)
from reg_meta_build.sources.scb import _StateGroup, _triage_groups

if TYPE_CHECKING:
    from pathlib import Path

_TOML_NAME = "source_column_repairs.toml"


# ── column-merge loader unit tests ──────────────────────────────────────────


class TestLoadColumnMerges:
    @staticmethod
    def _load(tmp_path: Path, text: str) -> dict:
        path = tmp_path / _TOML_NAME
        path.write_text(text, encoding="utf-8")
        return load_column_merges(path)

    def test_good_parse_is_case_folded(self, tmp_path: Path) -> None:
        # Columns fold to the rule-2 connectivity key (lowercase, diacritics
        # stripped) — TOML casing is cosmetic.
        m = self._load(
            tmp_path,
            "[[column_merge]]\nregister_id = 24\nvar_id = 56\n"
            'columns = ["PNR", "PersonNr"]\n',
        )
        assert m == {(24, 56): [frozenset({"pnr", "personnr"})]}

    def test_two_groups_same_var_are_two_entries(self, tmp_path: Path) -> None:
        m = self._load(
            tmp_path,
            '[[column_merge]]\nregister_id = 5\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
            '[[column_merge]]\nregister_id = 5\nvar_id = 9\ncolumns = ["C", "D"]\n',
        )
        assert m == {(5, 9): [frozenset({"a", "b"}), frozenset({"c", "d"})]}

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        assert load_column_merges(None) == {}
        assert load_column_merges(tmp_path / "absent.toml") == {}

    def test_valid_file_with_no_entries_is_empty(self, tmp_path: Path) -> None:
        assert self._load(tmp_path, "# no merge entries yet\n") == {}

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "[[column_merge]]\nregister_id = = 1\n")
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "column_merge_toml_unreadable"

    def test_single_merge_table_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[column_merge]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "column_merge_invalid"

    def test_scalar_merge_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "column_merge = 5\n")
        assert exc.value.code == "column_merge_invalid"

    def test_non_table_merge_entry_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "column_merge = [1, 2]\n")
        assert exc.value.code == "column_merge_invalid"

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[column_merges]]` (typo) would silently disable ALL curation → loud.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                "[[column_merges]]\nregister_id = 1\nvar_id = 9\n"
                'columns = ["A", "B"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "column_merges" in exc.value.message

    def test_bad_ids_rejected(self, tmp_path: Path) -> None:
        # Canonical-int contract shared with codelivery
        # (`_curation.canonical_int`): leading-zero string, float, bool, missing.
        for ids in (
            'register_id = 1\nvar_id = "01"',
            "register_id = 1\nvar_id = 1.5",
            "register_id = true\nvar_id = 9",
            "register_id = 1",
        ):
            with pytest.raises(RegMetaError) as exc:
                self._load(
                    tmp_path,
                    f'[[column_merge]]\n{ids}\ncolumns = ["A", "B"]\n',
                )
            assert exc.value.code == "column_merge_invalid"

    def test_singleton_columns_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[column_merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A"]\n',
            )
        assert exc.value.code == "column_merge_invalid"

    def test_non_string_or_empty_column_rejected(self, tmp_path: Path) -> None:
        for cols in ('["A", 5]', '["A", ""]'):
            with pytest.raises(RegMetaError) as exc:
                self._load(
                    tmp_path,
                    f"[[column_merge]]\nregister_id = 1\nvar_id = 9\n"
                    f"columns = {cols}\n",
                )
            assert exc.value.code == "column_merge_invalid"

    def test_case_twin_only_group_rejected(self, tmp_path: Path) -> None:
        # `PNR`/`pnr` fold to ONE column — the auto case-fold already covers it,
        # so a group that survives only on case spelling is a no-op → reject.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                "[[column_merge]]\nregister_id = 1\nvar_id = 9\n"
                'columns = ["PNR", "pnr"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "case-folding" in exc.value.message

    def test_fold_to_empty_column_rejected(self, tmp_path: Path) -> None:
        # A column with no ASCII content folds to "" and can never match a
        # rule-2 node-col (the coalescer keeps such a column raw).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[column_merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["Ω", "B"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "empty" in exc.value.message

    def test_overlapping_columns_across_groups_rejected(self, tmp_path: Path) -> None:
        # `B` in two groups of the same (register, var) — compared FOLDED, so the
        # second group's `b` collides with the first group's `B`.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[column_merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
                '[[column_merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["b", "C"]\n',
            )
        assert exc.value.code == "column_merge_invalid"
        assert "b" in exc.value.message


# ── fold-override loader unit tests ─────────────────────────────────────────


class TestLoadFoldOverrides:
    @staticmethod
    def _load(tmp_path: Path, text: str) -> dict:
        path = tmp_path / _TOML_NAME
        path.write_text(text, encoding="utf-8")
        return load_fold_overrides(path)

    def test_good_parse_single_group(self, tmp_path: Path) -> None:
        m = self._load(
            tmp_path,
            "[[fold_override]]\nregister_id = 195\nvar_id = 4027\n"
            'columns = ["Ksjusni", "NG1", "bransch", "sni2"]\n',
        )
        # Columns are case-folded at load to the rule-2 connectivity key (#196).
        assert m == {(195, 4027): [frozenset({"ksjusni", "ng1", "bransch", "sni2"})]}

    def test_two_groups_same_var_are_two_entries(self, tmp_path: Path) -> None:
        m = self._load(
            tmp_path,
            '[[fold_override]]\nregister_id = 5\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
            '[[fold_override]]\nregister_id = 5\nvar_id = 9\ncolumns = ["C", "D"]\n',
        )
        assert m == {(5, 9): [frozenset({"a", "b"}), frozenset({"c", "d"})]}

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        assert load_fold_overrides(None) == {}
        assert load_fold_overrides(tmp_path / "absent.toml") == {}

    def test_valid_file_with_no_entries_is_empty(self, tmp_path: Path) -> None:
        # A present but entry-less file is a no-op, not an error (the byte-identity
        # path the unknown-key / shape guards must not regress).
        assert self._load(tmp_path, "# no fold entries yet\n") == {}

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "[[fold_override]]\nregister_id = = 1\n")
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "fold_override_toml_unreadable"

    def test_single_fold_table_rejected(self, tmp_path: Path) -> None:
        # `[fold_override]` makes it a single table, not the `[[fold_override]]`
        # array → reject (don't let it AttributeError through the generic handler).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[fold_override]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_scalar_fold_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "fold_override = 5\n")
        assert exc.value.code == "fold_override_invalid"

    def test_non_table_fold_entry_rejected(self, tmp_path: Path) -> None:
        # `fold_override` is an array but its entries aren't tables.
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "fold_override = [1, 2]\n")
        assert exc.value.code == "fold_override_invalid"

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[fold_overrides]]` (typo) would silently disable ALL curation → loud.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                "[[fold_overrides]]\nregister_id = 1\nvar_id = 9\n"
                'columns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"
        assert "fold_overrides" in exc.value.message

    def test_leading_zero_id_rejected(self, tmp_path: Path) -> None:
        # A leading-zero string id could alias a canonical int → reject (mirrors
        # fqid_slugs `_parse_canonical_int`).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold_override]]\nregister_id = 1\nvar_id = "01"\n'
                'columns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_non_integer_id_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                "[[fold_override]]\nregister_id = 1\nvar_id = 1.5\n"
                'columns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_boolean_id_rejected(self, tmp_path: Path) -> None:
        # A TOML boolean is a Python `int` subclass (`true == 1`); the loader's
        # explicit bool guard must reject it so `register_id = true` can't alias 1.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                "[[fold_override]]\nregister_id = true\nvar_id = 9\n"
                'columns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_missing_id_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold_override]]\nregister_id = 1\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_singleton_columns_rejected(self, tmp_path: Path) -> None:
        # A one-column fold is a no-op → reject (catches a truncated edit).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold_override]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_non_string_or_empty_column_rejected(self, tmp_path: Path) -> None:
        for cols in ('["A", 5]', '["A", ""]'):
            with pytest.raises(RegMetaError) as exc:
                self._load(
                    tmp_path,
                    f"[[fold_override]]\nregister_id = 1\nvar_id = 9\n"
                    f"columns = {cols}\n",
                )
            assert exc.value.code == "fold_override_invalid"

    def test_fold_to_empty_column_rejected(self, tmp_path: Path) -> None:
        # #524: the empty-fold reject was a column_merge-only check before the
        # consolidation; the shared `load_column_groups` validator now applies it
        # to fold_overrides too (a column of only non-ASCII chars folds to "" and
        # can never match a contested rule-2 node-col).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                "[[fold_override]]\nregister_id = 1\nvar_id = 9\n"
                'columns = ["Ω", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"
        assert "empty" in exc.value.message

    def test_duplicate_column_within_group_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                "[[fold_override]]\nregister_id = 1\nvar_id = 9\n"
                'columns = ["A", "A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_overlapping_columns_across_groups_rejected(self, tmp_path: Path) -> None:
        # `B` in two groups of the same (register, var) → ambiguous → reject.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold_override]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
                '[[fold_override]]\nregister_id = 1\nvar_id = 9\ncolumns = ["B", "C"]\n',
            )
        assert exc.value.code == "fold_override_invalid"
        assert "b" in exc.value.message  # folded form (#196)


# ── sibling-section coexistence (the #524 consolidation) ────────────────────


class TestSiblingSections:
    """Both sections live in ONE file; each loader reads only its own section and
    ignores the sibling's (the `sibling_keys` non-interference contract), and a
    third unknown top-level key is still a loud error."""

    @staticmethod
    def _both(tmp_path: Path) -> Path:
        path = tmp_path / _TOML_NAME
        path.write_text(
            '[[column_merge]]\nregister_id = 2\nvar_id = 57\ncolumns = ["pnr", "personnr"]\n\n'
            "[[fold_override]]\nregister_id = 195\nvar_id = 4027\n"
            'columns = ["bgr98", "bransch"]\n',
            encoding="utf-8",
        )
        return path

    def test_column_merge_loader_ignores_fold_section(self, tmp_path: Path) -> None:
        m = load_column_merges(self._both(tmp_path))
        # Only the [[column_merge]] section is read; the [[fold_override]] sibling
        # is not flagged unknown and not folded in.
        assert m == {(2, 57): [frozenset({"pnr", "personnr"})]}

    def test_fold_override_loader_ignores_merge_section(self, tmp_path: Path) -> None:
        m = load_fold_overrides(self._both(tmp_path))
        assert m == {(195, 4027): [frozenset({"bgr98", "bransch"})]}

    def test_unknown_third_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # A section that is NEITHER known key is still a loud error from BOTH
        # loaders (a typo'd table can't silently disable curation).
        path = tmp_path / _TOML_NAME
        path.write_text(
            '[[column_merge]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
            '[[bogus]]\nregister_id = 1\nvar_id = 9\ncolumns = ["C", "D"]\n',
            encoding="utf-8",
        )
        for loader in (load_column_merges, load_fold_overrides):
            with pytest.raises(RegMetaError) as exc:
                loader(path)
            assert exc.value.code in {"column_merge_invalid", "fold_override_invalid"}
            assert "bogus" in exc.value.message


# ── per-var key shape ───────────────────────────────────────────────────────


class TestColumnMergeKeyIsPerVar:
    """The key is `(register_id, var_id)`: the same columns under two different
    vars are two INDEPENDENT entries, and a merge spanning two var_ids — the
    cross-var_id column SHARING of #197 — cannot be expressed."""

    def test_same_columns_different_vars_are_distinct_keys(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / _TOML_NAME
        path.write_text(
            '[[column_merge]]\nregister_id = 1\nvar_id = 100\ncolumns = ["A", "B"]\n\n'
            '[[column_merge]]\nregister_id = 1\nvar_id = 200\ncolumns = ["A", "B"]\n',
            encoding="utf-8",
        )
        m = load_column_merges(path)
        assert set(m) == {(1, 100), (1, 200)}
        assert m[(1, 100)] == [frozenset({"a", "b"})]
        assert m[(1, 200)] == [frozenset({"a", "b"})]


class TestFoldOverrideKeyIsPerVar:
    """The key is `(register_id, var_id)`, so the SAME columns under two different
    vars are two INDEPENDENT entries — a fold group spanning multiple variables
    cannot be expressed (the lead's 'unrepresentable by construction')."""

    def test_same_columns_different_vars_are_distinct_keys(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / _TOML_NAME
        path.write_text(
            '[[fold_override]]\nregister_id = 1\nvar_id = 100\ncolumns = ["A", "B"]\n\n'
            '[[fold_override]]\nregister_id = 1\nvar_id = 200\ncolumns = ["A", "B"]\n',
            encoding="utf-8",
        )
        m = load_fold_overrides(path)
        assert set(m) == {(1, 100), (1, 200)}
        assert m[(1, 100)] == [frozenset({"a", "b"})]
        assert m[(1, 200)] == [frozenset({"a", "b"})]


# ── column-merge build-driven end-to-end ────────────────────────────────────


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


class TestColumnMergeBuild:
    """The curated half of #196: an era-rename twin (`PNR` → `PersonNr`) shares
    no case identity, so only the maintainer can assert it — the merge normalizes
    both to one rule-2 node."""

    @staticmethod
    def _patch_repairs(tmp_path: Path, monkeypatch, text: str) -> None:
        path = tmp_path / _TOML_NAME
        path.write_text(text, encoding="utf-8")
        import reg_meta_build.source_column_repairs as _scr

        monkeypatch.setattr(_scr, "repo_source_column_repairs_path", lambda: path)

    def test_merge_unifies_era_rename_twins(self, tmp_path: Path, monkeypatch) -> None:
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        self._patch_repairs(
            tmp_path,
            monkeypatch,
            "[[column_merge]]\nregister_id = 1\nvar_id = 510\n"
            'columns = ["PNR", "PersonNr"]\n',
        )
        conn = build_with_rows(tmp_path, ri, vm)
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
        conn = build_with_rows(tmp_path, ri, vm)
        try:
            vids = _sibling_vids(conn, 510)
            n_vars = _n_vars(conn, 510)
        finally:
            conn.close()
        assert vids["PNR"] != vids["PersonNr"]
        assert n_vars == 3

    def test_stale_merge_column_failsbuild_with_rows(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        self._patch_repairs(
            tmp_path,
            monkeypatch,
            "[[column_merge]]\nregister_id = 1\nvar_id = 510\n"
            'columns = ["PNR", "Bogus"]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            build_with_rows(tmp_path, ri, vm)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "column_merge_unknown_column"
        assert "bogus" in exc.value.message  # folded form
        assert "pnr" in exc.value.message  # observed columns listed (actionable)

    def test_merge_for_absent_register_is_inert(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # An entry whose REGISTER isn't in this build (synthetic / partial /
        # SOS-only build) is silently ignored — the escape that keeps the repo
        # source_column_repairs.toml from failing every register_id=1 fixture build.
        ri, vm = _rename_container(var_id=510, old_col="PNR", new_col="PersonNr")
        self._patch_repairs(
            tmp_path,
            monkeypatch,
            '[[column_merge]]\nregister_id = 195\nvar_id = 510\ncolumns = ["X", "Y"]\n',
        )
        conn = build_with_rows(tmp_path, ri, vm)
        try:
            n_vars = _n_vars(conn, 510)
        finally:
            conn.close()
        assert n_vars == 3  # untouched — the un-merged fragmentation shape


# ── fold-override triage-driven tests (in-memory, deterministic) ────────────


def _ddl_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)  # FKs off → no provider/register parent row needed
    return conn


def _insert_var(conn: sqlite3.Connection, *, register_id: int, var_id: int) -> int:
    cur = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name) VALUES (?, ?, ?)",
        (register_id, str(var_id), "Var"),
    )
    assert cur.lastrowid is not None
    return cur.lastrowid


def _container(
    cols_editions: list[tuple[str, int]], *, register_id: int = 1, var_id: int = 920
) -> tuple[dict[tuple, _StateGroup], dict[str, tuple]]:
    """Build a `groups` dict + a `{column: gkey}` index for one split container.
    Each `(column, edition)` is its own group; columns sharing an edition are
    contested (co-delivered)."""
    groups: dict[tuple, _StateGroup] = {}
    gk_by_col: dict[str, tuple] = {}
    for i, (col, edition) in enumerate(cols_editions, start=1):
        gk = (register_id, 10, var_id, "int", "1", i, "", "", col)
        grp = _StateGroup(register_id, 10, var_id, "int", "1", i, "")
        grp.regvers = {edition}
        groups[gk] = grp
        gk_by_col[col] = gk
    return groups, gk_by_col


class TestFoldOverrideTriage:
    def test_override_folds_disjoint_columns_into_one_variable(self) -> None:
        # (a) Ksjusni / NG1 share no stem → the stem rule splits them; the curated
        # override forces them into ONE variable.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container([("Ksjusni", 100), ("NG1", 100)])
        # Maps passed directly to _triage_groups follow the loader contract:
        # case-folded columns (the gate compares on the folded form).
        fold = {(1, 920): [frozenset({"ksjusni", "ng1"})]}
        res = _triage_groups(conn, groups, {(1, 920): orig}, fold)
        assert res.assignments[gk["Ksjusni"]] == res.assignments[gk["NG1"]]
        n_vars = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE register_id=1 AND provider_key='920'"
        ).fetchone()[0]
        assert n_vars == 1  # one folded variable, no minted siblings
        assert res.stats["folds"] == 1
        assert res.labels[gk["Ksjusni"]] != res.labels[gk["NG1"]]  # distinct labels
        conn.close()

    def test_empty_override_splits_like_today(self) -> None:
        # (d) byte-identity guard: with NO override the same two disjoint columns
        # split into two sibling variables — the pre-#261 behavior.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container([("Ksjusni", 100), ("NG1", 100)])
        res = _triage_groups(conn, groups, {(1, 920): orig}, {})
        assert res.assignments[gk["Ksjusni"]] != res.assignments[gk["NG1"]]
        n_vars = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE register_id=1 AND provider_key='920'"
        ).fetchone()[0]
        assert n_vars == 2  # split into two siblings
        assert res.stats["splits"] == 1
        conn.close()

    def test_none_override_matches_empty_map(self) -> None:
        # The default arg path (`fold_overrides=None`) must equal the empty-map
        # path — the existing 3-arg `_triage_groups` callers stay byte-identical.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container([("Ksjusni", 100), ("NG1", 100)])
        res = _triage_groups(conn, groups, {(1, 920): orig})
        assert res.assignments[gk["Ksjusni"]] != res.assignments[gk["NG1"]]
        conn.close()

    def test_non_contested_column_failsbuild_with_rows(self) -> None:
        # (b) the override names `Bogus`, which is not a contested column of the
        # var → fail at the container gate with an actionable EXIT_CONFIG error.
        # (This test feeds _triage_groups a raw map directly — the loader-folded
        # form is exercised by TestFoldOverrideBuild.)
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, _ = _container([("Ksjusni", 100), ("NG1", 100)])
        fold = {(1, 920): [frozenset({"ksjusni", "bogus"})]}
        with pytest.raises(RegMetaError) as exc:
            _triage_groups(conn, groups, {(1, 920): orig}, fold)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "fold_override_unknown_column"
        assert "bogus" in exc.value.message
        conn.close()

    def test_stale_override_for_non_container_failsbuild_with_rows(self) -> None:
        # The var exists but its two columns are in DIFFERENT editions → they never
        # co-occur, so it is not a contested split container. The override goes
        # unconsumed → fail after the loop (stale curation, not a silent no-op).
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, _ = _container([("Ksjusni", 100), ("NG1", 200)])
        # Maps passed directly to _triage_groups follow the loader contract:
        # case-folded columns (the gate compares on the folded form).
        fold = {(1, 920): [frozenset({"ksjusni", "ng1"})]}
        with pytest.raises(RegMetaError) as exc:
            _triage_groups(conn, groups, {(1, 920): orig}, fold)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "fold_override_unused"
        # Actionable: the message names the offending key so a maintainer can find it.
        assert "register_id=1" in exc.value.message
        assert "920" in exc.value.message
        conn.close()

    def test_override_for_absent_register_is_inert(self) -> None:
        # An override whose REGISTER isn't in this build (synthetic / partial /
        # SOS-only build) is silently ignored — the escape that keeps the repo
        # source_column_repairs.toml from failing every register_id=1 fixture build.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container([("Ksjusni", 100), ("NG1", 100)])
        fold = {(195, 4027): [frozenset({"x", "y"})]}  # register 195 not built
        res = _triage_groups(conn, groups, {(1, 920): orig}, fold)
        assert res.assignments[gk["Ksjusni"]] != res.assignments[gk["NG1"]]  # split
        conn.close()

    def test_redundant_override_on_already_folding_set_is_consumed(self) -> None:
        # Ssyk3/Ssyk5 already FOLD by the stem rule (the top-level fold branch). An
        # override naming them is redundant — its columns ARE contested, so it is
        # consumed at the gate (no `fold_override_unused`), even though forced_same
        # never reaches the split branch. Guards the branch-independent placement.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container([("Ssyk3", 100), ("Ssyk5", 100)])
        fold = {(1, 920): [frozenset({"ssyk3", "ssyk5"})]}
        res = _triage_groups(conn, groups, {(1, 920): orig}, fold)  # must not raise
        assert res.assignments[gk["Ssyk3"]] == res.assignments[gk["Ssyk5"]]
        assert res.stats["folds"] == 1
        conn.close()

    def test_two_independent_groups_on_one_var(self) -> None:
        # Two SEPARATE fold groups under one (register, var) key: {Ksjusni, NG1}
        # and {bransch, sni2}, all four disjoint-stem and co-delivered in one
        # edition. Each group folds into its OWN variable (2 variables, not 1, not
        # 4), and BOTH groups are consumed under the single key → no
        # `fold_override_unused`. Pins the per-var list-of-groups semantics.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container(
            [("Ksjusni", 100), ("NG1", 100), ("bransch", 100), ("sni2", 100)]
        )
        fold = {
            (1, 920): [frozenset({"ksjusni", "ng1"}), frozenset({"bransch", "sni2"})]
        }
        res = _triage_groups(conn, groups, {(1, 920): orig}, fold)  # must not raise
        assert res.assignments[gk["Ksjusni"]] == res.assignments[gk["NG1"]]
        assert res.assignments[gk["bransch"]] == res.assignments[gk["sni2"]]
        assert res.assignments[gk["Ksjusni"]] != res.assignments[gk["bransch"]]
        n_vars = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE register_id=1 AND provider_key='920'"
        ).fetchone()[0]
        assert n_vars == 2  # one variable per fold group
        assert res.stats["folds"] == 2  # two fold clusters
        conn.close()

    def test_forced_fold_grows_through_stem_connected_column(self) -> None:
        # Union-find cumulativity: force {Ssyk3, NG1}; the stem rule independently
        # unites Ssyk3–Ssyk5. The forced seed + the stem edge share Ssyk3, so all
        # THREE columns collapse into ONE component → one variable. Pins that
        # forced and stem unions compose (a forced edge can pull a non-named but
        # stem-connected column into the fold), order-independent.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container([("Ssyk3", 100), ("Ssyk5", 100), ("NG1", 100)])
        fold = {(1, 920): [frozenset({"ssyk3", "ng1"})]}
        res = _triage_groups(conn, groups, {(1, 920): orig}, fold)
        assert (
            res.assignments[gk["Ssyk3"]]
            == res.assignments[gk["Ssyk5"]]
            == res.assignments[gk["NG1"]]
        )
        n_vars = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE register_id=1 AND provider_key='920'"
        ).fetchone()[0]
        assert n_vars == 1  # all three folded into the original variable
        assert res.stats["folds"] == 1
        conn.close()


# ── fold-override build-driven end-to-end (db→adapter→coalesce→triage) ──────


def _industry_container(var_id: int = 4027) -> tuple[list[str], list[str]]:
    """Ksjusni + NG1 under one var_id, co-delivered in one edition (regver 600),
    each with disjoint codes — the synthetic näringsgren split container."""
    ri = [
        _var_row(
            colname="Ksjusni",
            cvid=4001,
            var_id=var_id,
            varname="Naringsgren",
            year="2020",
            regver_id=600,
            data_length="3",
        ),
        _var_row(
            colname="NG1",
            cvid=4002,
            var_id=var_id,
            varname="Naringsgren",
            year="2020",
            regver_id=600,
            data_length="3",
        ),
    ]
    vm = vm_rows(4001, "AlphaA", CODING_A) + vm_rows(4002, "BetaB", CODING_B)
    return ri, vm


class TestFoldOverrideBuild:
    @staticmethod
    def _patch_repairs(tmp_path: Path, monkeypatch, text: str) -> None:
        path = tmp_path / _TOML_NAME
        path.write_text(text, encoding="utf-8")
        import reg_meta_build.source_column_repairs as _scr

        monkeypatch.setattr(_scr, "repo_source_column_repairs_path", lambda: path)

    def test_override_folds_into_single_variable_end_to_end(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # (a) end-to-end: with the curated override the two disjoint columns land
        # on ONE variable; without it (test below) they'd be two siblings.
        ri, vm = _industry_container()
        self._patch_repairs(
            tmp_path,
            monkeypatch,
            "[[fold_override]]\nregister_id = 1\nvar_id = 4027\n"
            'columns = ["Ksjusni", "NG1"]\n',
        )
        conn = build_with_rows(tmp_path, ri, vm)
        try:
            vids = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT variable_id FROM variable_state "
                    "WHERE delivery_column_name IN ('Ksjusni', 'NG1')"
                )
            ]
        finally:
            conn.close()
        assert len(vids) == 1  # folded into one variable

    def test_no_override_splits_into_two_variables_end_to_end(
        self, tmp_path: Path
    ) -> None:
        # The byte-identity contrast at the build level: the repo
        # source_column_repairs.toml is keyed on register 195, so it never touches
        # register_id=1 → the same container splits into two sibling variables.
        ri, vm = _industry_container()
        conn = build_with_rows(tmp_path, ri, vm)
        try:
            vids = {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT variable_id FROM variable_state "
                    "WHERE delivery_column_name IN ('Ksjusni', 'NG1')"
                )
            }
        finally:
            conn.close()
        assert len(vids) == 2  # split into two siblings (no override)

    def test_non_contested_override_fails_thebuild_with_rows(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # (b) end-to-end: an override naming a column that isn't contested for the
        # var fails the whole build with a clear EXIT_CONFIG error.
        ri, vm = _industry_container()
        self._patch_repairs(
            tmp_path,
            monkeypatch,
            "[[fold_override]]\nregister_id = 1\nvar_id = 4027\n"
            'columns = ["Ksjusni", "NG1", "Bogus"]\n',
        )
        with pytest.raises(RegMetaError) as exc:
            build_with_rows(tmp_path, ri, vm)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "fold_override_unknown_column"
        assert "bogus" in exc.value.message  # folded form (#196)
