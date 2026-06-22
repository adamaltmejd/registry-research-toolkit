"""Resolver-side coverage for the period column-family merge (#319).

A merged family's stored `variable_state` is ONE annual single-claim row per
year; `resolve_at` / `states()` expand it READ-TIME into one `VariableState` per
month-column window (from `variable_alias_window`) overlapping the query:
`resolve_at("YYYY-MM")` → the one month column, `resolve_at("YYYY")` → all months.
Non-merged variables have no window rows → byte-identical 1:1 behaviour (covered
by the rest of the resolver suite).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import _var_row
from _shared_fixtures import build_with_rows, vm_rows
from reg_meta.catalog import Catalog, ValueSetMember
from reg_meta.db import open_db

if TYPE_CHECKING:
    from pathlib import Path

_MONTHS = [("jan", 1), ("feb", 2), ("mars", 3)]
_YEARS = [2018, 2019]
_CODES = [("1", "Låg"), ("2", "Hög")]
_FQID = "scb/testreg/lonfink"


def _build(tmp_path: Path, monkeypatch) -> Path:
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
    # build_with_rows writes the DB under tmp_path/db and returns a connection;
    # we want the path to reopen read-only via open_db.
    conn = build_with_rows(tmp_path, ri, vm)
    conn.close()
    return tmp_path / "db" / "reg_meta.db"


@pytest.fixture
def merged_db(tmp_path: Path, monkeypatch) -> Path:
    return _build(tmp_path, monkeypatch)


def test_resolve_at_month_returns_one_column(merged_db: Path) -> None:
    conn = open_db(merged_db)
    try:
        cat = Catalog(conn)
        states = cat.resolve_at(_FQID, "2018-03")
        assert len(states) == 1
        s = states[0]
        assert s.delivery_column_name == "LonFinkMars"
        assert s.valid_from == "2018-03-01"
        assert s.valid_to == "2018-03-31"
        # value set comes from the annual claim.
        assert s.value_set == (
            ValueSetMember(code="1", label="Låg"),
            ValueSetMember(code="2", label="Hög"),
        )
    finally:
        conn.close()


def test_resolve_at_full_year_returns_all_months(merged_db: Path) -> None:
    conn = open_db(merged_db)
    try:
        cat = Catalog(conn)
        states = cat.resolve_at(_FQID, 2018)
        # One per month column overlapping 2018.
        assert len(states) == len(_MONTHS)
        cols = sorted(s.delivery_column_name for s in states)
        assert cols == ["LonFinkFeb", "LonFinkJan", "LonFinkMars"]
        assert all(s.valid_from.startswith("2018-") for s in states)
    finally:
        conn.close()


def test_resolve_at_other_year_excluded(merged_db: Path) -> None:
    conn = open_db(merged_db)
    try:
        cat = Catalog(conn)
        states = cat.resolve_at(_FQID, "2019-01")
        assert len(states) == 1
        assert states[0].delivery_column_name == "LonFinkJan"
        assert states[0].valid_from == "2019-01-01"
    finally:
        conn.close()


def test_states_returns_all_windows(merged_db: Path) -> None:
    conn = open_db(merged_db)
    try:
        cat = Catalog(conn)
        states = cat.states(_FQID)
        # 3 months × 2 years = 6 windowed states (NOT 2 annual rows).
        assert len(states) == len(_MONTHS) * len(_YEARS)
        windows = {(s.delivery_column_name, s.valid_from) for s in states}
        assert ("LonFinkJan", "2018-01-01") in windows
        assert ("LonFinkMars", "2019-03-01") in windows
        # Each window shares the annual claim's value set.
        assert all(
            s.value_set
            == (
                ValueSetMember(code="1", label="Låg"),
                ValueSetMember(code="2", label="Hög"),
            )
            for s in states
        )
    finally:
        conn.close()


def test_windows_share_annual_state_id(merged_db: Path) -> None:
    """D2: a year's month windows SHARE the annual state's state_id (one claim,
    N representations); the compound (state_id, column, valid_from) is unique."""
    conn = open_db(merged_db)
    try:
        cat = Catalog(conn)
        states = cat.resolve_at(_FQID, 2018)
        assert len({s.state_id for s in states}) == 1  # one annual claim
        compound = {(s.state_id, s.delivery_column_name, s.valid_from) for s in states}
        assert len(compound) == len(states)  # but each window is uniquely keyed
    finally:
        conn.close()


def test_non_merged_variable_unaffected(merged_db: Path) -> None:
    """A non-merged variable (the fixture's `kon`) has no window rows → its
    resolve_at returns the stored annual state 1:1, no expansion."""
    conn = open_db(merged_db)
    try:
        cat = Catalog(conn)
        # The standard fixture's Kön variable in TESTREG.
        slug = conn.execute(
            "SELECT slug FROM variable WHERE register_id = 1 AND name = 'Kön' LIMIT 1"
        ).fetchone()
        if slug is None:
            pytest.skip("fixture has no Kön variable to probe")
        states = cat.states(f"scb/testreg/{slug[0]}")
        # No window rows for it → no expansion (each state maps 1:1).
        n_windows = conn.execute(
            "SELECT COUNT(*) FROM variable_alias_window w "
            "JOIN variable v ON v.variable_id = w.variable_id "
            "WHERE v.slug = ?",
            (slug[0],),
        ).fetchone()[0]
        assert n_windows == 0
        assert all(s.delivery_column_name != "" for s in states)
    finally:
        conn.close()


def _build_gap_year(tmp_path: Path, monkeypatch) -> Path:
    """A family where `mars` delivers ONLY 2019 (not 2018) — so the 2018 annual
    claim has jan/feb windows but NO march window. Exercises the
    `_expand_state_windows` fallback: a query for a month with no window in that
    year keeps the raw annual state (never silently dropped)."""
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

    ri: list[str] = []
    vm: list[str] = []
    for mi, (token, _month) in enumerate(_MONTHS):
        # jan/feb deliver both years; mars only 2019 → 2018 has no march window.
        years = [2019] if token == "mars" else _YEARS
        for year in years:
            cvid = 8000 + mi * 10 + year
            ri.append(
                _var_row(
                    colname=f"LonFink{token.capitalize()}",
                    cvid=cvid,
                    var_id=800 + mi,
                    varname=f"Inkomst {token}",
                    year=str(year),
                    regver_id=8000 + mi * 10 + year,
                    data_length="1",
                )
            )
            vm.extend(vm_rows(cvid, f"LonFink{year}", _CODES))
    conn = build_with_rows(tmp_path, ri, vm)
    conn.close()
    return tmp_path / "db" / "reg_meta.db"


def test_gap_year_month_falls_back_to_annual_state(tmp_path: Path, monkeypatch) -> None:
    db = _build_gap_year(tmp_path, monkeypatch)
    conn = open_db(db)
    try:
        cat = Catalog(conn)
        # 2018 has jan/feb windows but no march → query "2018-03" hits the annual
        # state, no window overlaps → fallback returns the bare annual state (not
        # silently dropped).
        states = cat.resolve_at(_FQID, "2018-03")
        assert len(states) == 1
        s = states[0]
        # The annual claim's own bounds (not a month window).
        assert s.valid_from == "2018-01-01"
        assert s.valid_to == "2018-12-31"
        # Sanity: 2019 DOES have a march window (the gap is 2018-only).
        mar2019 = cat.resolve_at(_FQID, "2019-03")
        assert len(mar2019) == 1
        assert mar2019[0].delivery_column_name == "LonFinkMars"
        assert mar2019[0].valid_from == "2019-03-01"
    finally:
        conn.close()
