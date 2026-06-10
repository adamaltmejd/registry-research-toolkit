"""Fold-override curation (#261): a maintainer surface that folds DISJOINT-stem
columns the SCB triage stem rule would split (see DESIGN.md → Build-time triage).

Three layers, mirroring the codelivery tests:
  - `TestLoadFoldOverrides` — the loader's good parse + every load-time failure
    mode (canonical-int ids, ≥2 columns, no within-group / cross-group overlap).
  - `TestFoldOverrideKeyIsPerVar` — the key is `(register_id, var_id)`, so a fold
    group spanning two variables is unrepresentable by construction.
  - `TestFoldOverrideTriage` — `_triage_groups` driven (in-memory, deterministic):
    a curated entry folds disjoint columns into ONE variable; an empty surface is
    byte-identical to today's split; bad curation FAILS the build.
  - `TestFoldOverrideBuild` — a real `build_db` proving the db→adapter→coalesce→
    triage wiring and that the config error propagates through the CLI build.

Fully synthetic (CLAUDE.md): the repo `fold_overrides.toml` is keyed on real SCB
register ids, so it never touches these register_id=1 cases; the build tests
monkeypatch `repo_fold_overrides_path` to inject their own curation.
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
from reg_meta_build.db import DDL, build_db
from reg_meta_build.fold_overrides import load_fold_overrides
from reg_meta_build.sources.scb import _StateGroup, _triage_groups

if TYPE_CHECKING:
    from pathlib import Path


# ── loader unit tests ──────────────────────────────────────────────────────


class TestLoadFoldOverrides:
    @staticmethod
    def _load(tmp_path: Path, text: str) -> dict:
        path = tmp_path / "fold_overrides.toml"
        path.write_text(text, encoding="utf-8")
        return load_fold_overrides(path)

    def test_good_parse_single_group(self, tmp_path: Path) -> None:
        m = self._load(
            tmp_path,
            "[[fold]]\nregister_id = 195\nvar_id = 4027\n"
            'columns = ["Ksjusni", "NG1", "bransch", "sni2"]\n',
        )
        # Columns are case-folded at load to the rule-2 connectivity key (#196).
        assert m == {(195, 4027): [frozenset({"ksjusni", "ng1", "bransch", "sni2"})]}

    def test_two_groups_same_var_are_two_entries(self, tmp_path: Path) -> None:
        m = self._load(
            tmp_path,
            '[[fold]]\nregister_id = 5\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
            '[[fold]]\nregister_id = 5\nvar_id = 9\ncolumns = ["C", "D"]\n',
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
            self._load(tmp_path, "[[fold]]\nregister_id = = 1\n")
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "fold_override_toml_unreadable"

    def test_single_fold_table_rejected(self, tmp_path: Path) -> None:
        # `[fold]` makes `fold` a single table, not the `[[fold]]` array → reject
        # (don't let it AttributeError through the generic handler).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path, '[fold]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n'
            )
        assert exc.value.code == "fold_override_invalid"

    def test_scalar_fold_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "fold = 5\n")
        assert exc.value.code == "fold_override_invalid"

    def test_non_table_fold_entry_rejected(self, tmp_path: Path) -> None:
        # `fold` is an array but its entries aren't tables.
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "fold = [1, 2]\n")
        assert exc.value.code == "fold_override_invalid"

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[folds]]` (typo) would silently disable ALL curation → loud error.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[folds]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"
        assert "folds" in exc.value.message

    def test_leading_zero_id_rejected(self, tmp_path: Path) -> None:
        # A leading-zero string id could alias a canonical int → reject (mirrors
        # fqid_slugs `_parse_canonical_int`).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold]]\nregister_id = 1\nvar_id = "01"\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_non_integer_id_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold]]\nregister_id = 1\nvar_id = 1.5\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_boolean_id_rejected(self, tmp_path: Path) -> None:
        # A TOML boolean is a Python `int` subclass (`true == 1`); the loader's
        # explicit bool guard must reject it so `register_id = true` can't alias 1.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold]]\nregister_id = true\nvar_id = 9\ncolumns = ["A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_missing_id_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, '[[fold]]\nregister_id = 1\ncolumns = ["A", "B"]\n')
        assert exc.value.code == "fold_override_invalid"

    def test_singleton_columns_rejected(self, tmp_path: Path) -> None:
        # A one-column fold is a no-op → reject (catches a truncated edit).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path, '[[fold]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A"]\n'
            )
        assert exc.value.code == "fold_override_invalid"

    def test_non_string_or_empty_column_rejected(self, tmp_path: Path) -> None:
        for cols in ('["A", 5]', '["A", ""]'):
            with pytest.raises(RegMetaError) as exc:
                self._load(
                    tmp_path,
                    f"[[fold]]\nregister_id = 1\nvar_id = 9\ncolumns = {cols}\n",
                )
            assert exc.value.code == "fold_override_invalid"

    def test_duplicate_column_within_group_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "A", "B"]\n',
            )
        assert exc.value.code == "fold_override_invalid"

    def test_overlapping_columns_across_groups_rejected(self, tmp_path: Path) -> None:
        # `B` in two groups of the same (register, var) → ambiguous → reject.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[fold]]\nregister_id = 1\nvar_id = 9\ncolumns = ["A", "B"]\n\n'
                '[[fold]]\nregister_id = 1\nvar_id = 9\ncolumns = ["B", "C"]\n',
            )
        assert exc.value.code == "fold_override_invalid"
        assert "b" in exc.value.message  # folded form (#196)


class TestFoldOverrideKeyIsPerVar:
    """The key is `(register_id, var_id)`, so the SAME columns under two different
    vars are two INDEPENDENT entries — a fold group spanning multiple variables
    cannot be expressed (the lead's 'unrepresentable by construction')."""

    def test_same_columns_different_vars_are_distinct_keys(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "fold_overrides.toml"
        path.write_text(
            '[[fold]]\nregister_id = 1\nvar_id = 100\ncolumns = ["A", "B"]\n\n'
            '[[fold]]\nregister_id = 1\nvar_id = 200\ncolumns = ["A", "B"]\n',
            encoding="utf-8",
        )
        m = load_fold_overrides(path)
        assert set(m) == {(1, 100), (1, 200)}
        assert m[(1, 100)] == [frozenset({"a", "b"})]
        assert m[(1, 200)] == [frozenset({"a", "b"})]


# ── triage-driven tests (in-memory, deterministic) ─────────────────────────


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
        fold = {(1, 920): [frozenset({"Ksjusni", "NG1"})]}
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

    def test_non_contested_column_fails_build(self) -> None:
        # (b) the override names `Bogus`, which is not a contested column of the
        # var → fail at the container gate with an actionable EXIT_CONFIG error.
        # (This test feeds _triage_groups a raw map directly — the loader-folded
        # form is exercised by TestFoldOverrideBuild.)
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, _ = _container([("Ksjusni", 100), ("NG1", 100)])
        fold = {(1, 920): [frozenset({"Ksjusni", "Bogus"})]}
        with pytest.raises(RegMetaError) as exc:
            _triage_groups(conn, groups, {(1, 920): orig}, fold)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "fold_override_unknown_column"
        assert "Bogus" in exc.value.message
        conn.close()

    def test_stale_override_for_non_container_fails_build(self) -> None:
        # The var exists but its two columns are in DIFFERENT editions → they never
        # co-occur, so it is not a contested split container. The override goes
        # unconsumed → fail after the loop (stale curation, not a silent no-op).
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, _ = _container([("Ksjusni", 100), ("NG1", 200)])
        fold = {(1, 920): [frozenset({"Ksjusni", "NG1"})]}
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
        # fold_overrides.toml from failing every register_id=1 fixture build.
        conn = _ddl_conn()
        orig = _insert_var(conn, register_id=1, var_id=920)
        groups, gk = _container([("Ksjusni", 100), ("NG1", 100)])
        fold = {(195, 4027): [frozenset({"X", "Y"})]}  # register 195 not built
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
        fold = {(1, 920): [frozenset({"Ssyk3", "Ssyk5"})]}
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
            (1, 920): [frozenset({"Ksjusni", "NG1"}), frozenset({"bransch", "sni2"})]
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
        fold = {(1, 920): [frozenset({"Ssyk3", "NG1"})]}
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


# ── build-driven end-to-end (proves the db→adapter→coalesce→triage wiring) ──

# Two disjoint codings so Ksjusni / NG1 are distinct value sets (and groups).
_CODING_A = [("11", "Alpha ett"), ("12", "Alpha två"), ("13", "Alpha tre")]
_CODING_B = [("21", "Beta ett"), ("22", "Beta två"), ("23", "Beta tre")]


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
    vm = _vm_rows(4001, "AlphaA", _CODING_A) + _vm_rows(4002, "BetaB", _CODING_B)
    return ri, vm


class TestFoldOverrideBuild:
    def test_override_folds_into_single_variable_end_to_end(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # (a) end-to-end: with the curated override the two disjoint columns land
        # on ONE variable; without it (test below) they'd be two siblings.
        ri, vm = _industry_container()
        path = tmp_path / "fold_overrides.toml"
        path.write_text(
            '[[fold]]\nregister_id = 1\nvar_id = 4027\ncolumns = ["Ksjusni", "NG1"]\n',
            encoding="utf-8",
        )
        import reg_meta_build.fold_overrides as _fo

        monkeypatch.setattr(_fo, "repo_fold_overrides_path", lambda: path)
        conn = _build(tmp_path, ri, vm)
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
        # The byte-identity contrast at the build level: the repo fold_overrides.toml
        # is keyed on register 195, so it never touches register_id=1 → the same
        # container splits into two sibling variables.
        ri, vm = _industry_container()
        conn = _build(tmp_path, ri, vm)
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

    def test_non_contested_override_fails_the_build(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # (b) end-to-end: an override naming a column that isn't contested for the
        # var fails the whole build with a clear EXIT_CONFIG error.
        ri, vm = _industry_container()
        path = tmp_path / "fold_overrides.toml"
        path.write_text(
            "[[fold]]\nregister_id = 1\nvar_id = 4027\n"
            'columns = ["Ksjusni", "NG1", "Bogus"]\n',
            encoding="utf-8",
        )
        import reg_meta_build.fold_overrides as _fo

        monkeypatch.setattr(_fo, "repo_fold_overrides_path", lambda: path)
        with pytest.raises(RegMetaError) as exc:
            _build(tmp_path, ri, vm)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "fold_override_unknown_column"
        assert "bogus" in exc.value.message  # folded form (#196)
