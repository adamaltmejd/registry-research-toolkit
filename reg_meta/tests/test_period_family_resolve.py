"""Resolver-side coverage for the period column-family merge (#319).

A merged family's stored `variable_state` is ONE annual single-claim row per
year; `resolve_at` / `states()` expand it READ-TIME into one `VariableState` per
month-column window (from `variable_alias_window`) overlapping the query:
`resolve_at("YYYY-MM")` → the one month column, `resolve_at("YYYY")` → all months.
Non-merged variables have no window rows → byte-identical 1:1 behaviour (covered
by the rest of the resolver suite).
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _representation_fixtures import build_month_family
from reader_artifacts import stamp_catalog_identity
from reg_meta.catalog import Catalog
from reg_meta.db import open_db

if TYPE_CHECKING:
    from pathlib import Path

_FQID = "scb/testreg/lonfink"


def _build(tmp_path: Path, monkeypatch) -> Path:
    path = build_month_family(tmp_path)
    with sqlite3.connect(path) as conn:
        stamp_catalog_identity(conn)
    return path


@pytest.fixture
def merged_db(tmp_path: Path, monkeypatch) -> Path:
    return _build(tmp_path, monkeypatch)


def test_month_window_period_token_is_own_month(merged_db: Path) -> None:
    """Each expanded month window's `period_token` recomputes from that window's
    OWN bounds (e.g. the march column → `"2018-03"`), NOT the base annual token
    (`"2018"`). Regression for `_expand_state_windows` dropping `period_token`
    from its `model_copy(update=…)` — which would leave every window carrying the
    annual token (#681)."""
    conn = open_db(merged_db)
    try:
        cat = Catalog(conn)
        states = cat.resolve_at(_FQID, 2018)
        tokens = {s.delivery_column_name: s.period_token for s in states}
        # The window token is the OWN month, distinct from the annual "2018".
        assert tokens == {
            "LonFinkJan": "2018-01",
            "LonFinkFeb": "2018-02-01..2018-02-28",
            "LonFinkMars": "2018-03",
        }
        assert all(t != "2018" for t in tokens.values())
    finally:
        conn.close()


def _build_gap_year(tmp_path: Path, monkeypatch) -> Path:
    path = build_month_family(tmp_path, march_2018=False)
    with sqlite3.connect(path) as conn:
        stamp_catalog_identity(conn)
    return path


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
