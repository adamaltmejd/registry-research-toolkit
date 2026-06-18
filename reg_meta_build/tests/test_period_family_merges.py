"""End-to-end coverage for the period column-family merge (#319).

Fabricates a small monthly family (a stem + jan/feb/mars columns across two
delivery years) in the SCB build fixtures, activates a curated
`curation/period_family_merges.toml` for it, runs a REAL `build_db`, and asserts the
12→1 merge shape: one variable slugged as the stem with ANNUAL states (not
per-month), the per-month alias windows, the resolver expansion, the deleted
siblings, no dangling FK, and the validator closure. Loader-shape tests live
alongside.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import _var_row
from _shared_fixtures import build_with_rows, vm_rows
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.period_family_merges import (
    PeriodFamily,
    load_period_family_merges,
)

if TYPE_CHECKING:
    from pathlib import Path

# The merged family: stem "lonfink", three month columns, two delivery years.
# `derive_variable_slug("LonFinkJan")` → "lonfinkjan" → stem "lonfink" + jan.
_MONTHS = [("jan", 1), ("feb", 2), ("mars", 3)]
_YEARS = [2018, 2019]
_CODES = [("1", "Låg"), ("2", "Hög")]


def _family_ri_vm() -> tuple[list[str], list[str]]:
    """Registerinformation + Vardemangder rows for the 3-month × 2-year family in
    TESTREG (register_id 1, variant 10). Each month is its own variable (distinct
    var_id) delivering the same column name across both years (so it folds to one
    variable with two annual states); all share one value set."""
    ri: list[str] = []
    vm: list[str] = []
    for mi, (token, _month) in enumerate(_MONTHS):
        var_id = 800 + mi
        colname = f"LonFink{token.capitalize()}"
        for yi, year in enumerate(_YEARS):
            cvid = 8000 + mi * 10 + yi
            ri.append(
                _var_row(
                    colname=colname,
                    cvid=cvid,
                    var_id=var_id,
                    varname=f"Inkomst {token}",
                    year=str(year),
                    regver_id=800 + mi * 10 + yi,
                    data_length="1",
                )
            )
            vm.extend(vm_rows(cvid, f"LonFink{year}", _CODES))
    return ri, vm


_LISA_FAMILY = PeriodFamily(
    provider="scb", register="testreg", family_stem="lonfink", label="Lön per månad"
)


def _build_with_family(tmp_path: Path, monkeypatch) -> sqlite3.Connection:
    """Build the fixture + the period family with a curated
    period_family_merges.toml active (the autouse `_no_repo_curation` nulls it;
    re-point it here, after)."""
    import reg_meta_build.db as _db
    import reg_meta_build.period_family_merges as _fm

    toml = tmp_path / "period_family_merges.toml"
    toml.write_text(
        '[[period_family]]\nregister = "scb/testreg"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(_fm, "repo_period_family_merges_path", lambda: toml)
    monkeypatch.setattr(_db, "repo_period_family_merges_path", lambda: toml)
    ri, vm = _family_ri_vm()
    conn = build_with_rows(tmp_path, ri, vm)
    conn.row_factory = sqlite3.Row
    return conn


def _survivor(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute(
        "SELECT variable_id, name, slug FROM variable WHERE slug = 'lonfink'"
    ).fetchone()
    assert row is not None, "merged survivor variable should be slugged 'lonfink'"
    return row


# ── loader shape ──────────────────────────────────────────────────────────────


def test_load_period_family_merges_parses(tmp_path: Path) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(
        '[[period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\n',
        encoding="utf-8",
    )
    families = load_period_family_merges(path)
    assert len(families) == 1
    assert families[0] == PeriodFamily("scb", "lisa", "lonfink", "Lön")


def test_load_period_family_merges_empty_when_no_file() -> None:
    assert load_period_family_merges(None) == ()


@pytest.mark.parametrize(
    "body",
    [
        'register = "lisa"\nfamily_stem = "x"\nlabel = "L"',  # 1-seg register
        'register = "scb/lisa/x"\nfamily_stem = "x"\nlabel = "L"',  # 3-seg register
        'register = "scb/lisa"\nlabel = "L"',  # missing family_stem
        'register = "scb/lisa"\nfamily_stem = "x"',  # missing label
    ],
)
def test_load_period_family_merges_rejects_malformed(tmp_path: Path, body: str) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(f"[[period_family]]\n{body}\n", encoding="utf-8")
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(path)
    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.code == "period_family_merges_invalid"


def test_load_period_family_merges_rejects_duplicate(tmp_path: Path) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(
        '[[period_family]]\nregister = "scb/lisa"\nfamily_stem = "x"\nlabel = "A"\n'
        '[[period_family]]\nregister = "scb/lisa"\nfamily_stem = "x"\nlabel = "B"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(path)
    assert exc.value.code == "period_family_merges_invalid"


# ── merge mechanics (end-to-end build) ────────────────────────────────────────


def test_merge_folds_to_one_variable(tmp_path: Path, monkeypatch) -> None:
    conn = _build_with_family(tmp_path, monkeypatch)
    try:
        survivor = _survivor(conn)
        assert survivor["name"] == "Lön per månad"
        # The 3 month columns are now ONE variable; the other 2 are deleted.
        n_lonfink = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE slug LIKE 'lonfink%'"
        ).fetchone()[0]
        assert n_lonfink == 1
    finally:
        conn.close()


def test_merged_variable_keeps_annual_states(tmp_path: Path, monkeypatch) -> None:
    conn = _build_with_family(tmp_path, monkeypatch)
    try:
        survivor = _survivor(conn)
        states = conn.execute(
            "SELECT valid_from, valid_to FROM variable_state WHERE variable_id = ? "
            "ORDER BY valid_from",
            (survivor["variable_id"],),
        ).fetchall()
        # ONE annual state per delivery year — NOT 12 (3) per year.
        assert len(states) == len(_YEARS), [tuple(s) for s in states]
        for (vf, vt), year in zip(states, _YEARS, strict=True):
            assert vf == f"{year}-01-01"
            assert vt.startswith(str(year))
    finally:
        conn.close()


def test_alias_windows_emitted(tmp_path: Path, monkeypatch) -> None:
    conn = _build_with_family(tmp_path, monkeypatch)
    try:
        survivor = _survivor(conn)
        windows = conn.execute(
            "SELECT delivery_column_name, valid_from, valid_to "
            "FROM variable_alias_window WHERE variable_id = ? "
            "ORDER BY valid_from, delivery_column_name",
            (survivor["variable_id"],),
        ).fetchall()
        # 3 months × 2 years = 6 windows, each a YYYY-MM span.
        assert len(windows) == len(_MONTHS) * len(_YEARS)
        spans = {
            (w["delivery_column_name"], w["valid_from"], w["valid_to"]) for w in windows
        }
        assert ("LonFinkJan", "2018-01-01", "2018-01-31") in spans
        assert ("LonFinkMars", "2019-03-01", "2019-03-31") in spans
    finally:
        conn.close()


def test_all_columns_retained_in_variable_alias(tmp_path: Path, monkeypatch) -> None:
    conn = _build_with_family(tmp_path, monkeypatch)
    try:
        survivor = _survivor(conn)
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT delivery_column_name FROM variable_alias "
                "WHERE variable_id = ?",
                (survivor["variable_id"],),
            )
        }
        assert {"LonFinkJan", "LonFinkFeb", "LonFinkMars"} <= cols
    finally:
        conn.close()


def test_no_dangling_fk_after_merge(tmp_path: Path, monkeypatch) -> None:
    conn = _build_with_family(tmp_path, monkeypatch)
    try:
        # The build's own PRAGMA foreign_key_check must stay clean post-merge.
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        # No state/alias/window row references a deleted sibling.
        for table in ("variable_state", "variable_alias", "variable_alias_window"):
            orphan = conn.execute(
                f"SELECT COUNT(*) FROM {table} t WHERE NOT EXISTS "
                "(SELECT 1 FROM variable v WHERE v.variable_id = t.variable_id)"
            ).fetchone()[0]
            assert orphan == 0, f"{table} has {orphan} orphan rows"
    finally:
        conn.close()


def test_validator_window_closure_passes(tmp_path: Path, monkeypatch) -> None:
    from reg_meta_build.validate import ValidationResult, _check_variable_alias_window

    conn = _build_with_family(tmp_path, monkeypatch)
    try:
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_variable_alias_window(conn, result, tables, corpus=False)
        assert result.passed, result.failures
    finally:
        conn.close()


def test_materialize_dangling_family_fails_loud(tmp_path: Path) -> None:
    """A curated stem that resolves to no coherent period family fails the build
    (the merge runs directly here against a built fixture DB)."""
    from reg_meta_build.period_family_merges import materialize_period_family_merges

    conn = build_with_rows(tmp_path, [], [])  # standard fixture, no month family
    conn.row_factory = sqlite3.Row
    try:
        with pytest.raises(RegMetaError) as exc:
            materialize_period_family_merges(
                conn,
                (PeriodFamily("scb", "testreg", "nosuchstem", "X"),),
                providers=frozenset({"scb"}),
                fold_slug_hints={},
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "period_family_merges_unresolved"
    finally:
        conn.close()


def _divergent_ri_vm() -> tuple[list[str], list[str]]:
    """Like `_family_ri_vm`, but the `mars` column carries a DIFFERENT value set in
    the shared year 2018 (distinct codes → distinct value_set_id), while jan/feb
    share one — an intra-year value-set divergence the merge must reject."""
    ri: list[str] = []
    vm: list[str] = []
    for mi, (token, _month) in enumerate(_MONTHS):
        var_id = 800 + mi
        colname = f"LonFink{token.capitalize()}"
        for yi, year in enumerate(_YEARS):
            cvid = 8000 + mi * 10 + yi
            ri.append(
                _var_row(
                    colname=colname,
                    cvid=cvid,
                    var_id=var_id,
                    varname=f"Inkomst {token}",
                    year=str(year),
                    regver_id=800 + mi * 10 + yi,
                    data_length="1",
                )
            )
            # `mars` in 2018 gets disjoint codes → a distinct value_set_id from the
            # jan/feb columns' shared set, within the same delivery year.
            codes = (
                [("7", "Annan"), ("8", "Extra")]
                if token == "mars" and year == 2018
                else _CODES
            )
            vm.extend(vm_rows(cvid, f"LonFink{year}", codes))
    return ri, vm


def test_intra_year_value_set_divergence_fails_loud(tmp_path: Path) -> None:
    """#319: member columns disagreeing on value_set_id within ONE delivery year
    must LOUD-FAIL — keeping only the survivor's annual claim would silently drop
    the divergent sibling's value domain. Build WITHOUT the merge (curation nulled),
    then run materialize directly so the conflict surfaces."""
    from reg_meta_build.period_family_merges import materialize_period_family_merges

    ri, vm = _divergent_ri_vm()
    conn = build_with_rows(tmp_path, ri, vm)
    conn.row_factory = sqlite3.Row
    try:
        with pytest.raises(RegMetaError) as exc:
            materialize_period_family_merges(
                conn,
                (PeriodFamily("scb", "testreg", "lonfink", "Lön per månad"),),
                providers=frozenset({"scb"}),
                fold_slug_hints={},
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "period_family_merges_value_set_conflict"
        assert "2018" in exc.value.message
    finally:
        conn.close()


def test_shared_value_set_merge_succeeds(tmp_path: Path) -> None:
    """The agreement guard does NOT false-fire when all member columns share the
    value domain (the standard family) — the merge completes."""
    from reg_meta_build.period_family_merges import materialize_period_family_merges

    ri, vm = _family_ri_vm()
    conn = build_with_rows(tmp_path, ri, vm)
    conn.row_factory = sqlite3.Row
    try:
        counts = materialize_period_family_merges(
            conn,
            (PeriodFamily("scb", "testreg", "lonfink", "Lön per månad"),),
            providers=frozenset({"scb"}),
            fold_slug_hints={},
        )
        assert counts["families"] == 1
    finally:
        conn.close()
