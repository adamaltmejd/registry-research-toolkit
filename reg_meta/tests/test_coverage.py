"""Coverage aggregates over `variable_state` (#351): `Catalog`
`register_variable_coverage` / `provider_register_coverage`.

Query-time aggregates (no materialized columns — see reg_webapp/DESIGN.md →
Coverage aggregates). Covers the open-ended sentinel mapping, a finite window, and
a stateless variable (count 0, bounds None).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from reg_meta.catalog import Catalog, _coverage_bounds

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "reg_meta_build" / "tests")
)

from _slugged_db import add_state, add_variable, build_slugged_db  # noqa: E402

if TYPE_CHECKING:
    import sqlite3


@pytest.fixture
def db() -> sqlite3.Connection:
    """scb/lisa with three variables: `kon` (the default open-ended state),
    `fin` (a finite 2010–2015 window), `nostate` (no states)."""
    conn = build_slugged_db()  # scb/lisa/kon, one state 2018-01-01..9999-12-31
    add_variable(conn, register_id=1, var_id=200, name="Finite", slug="fin")
    add_state(
        conn,
        register_id=1,
        variable_slug="fin",
        register_variant_id=10,
        valid_from="2010-01-01",
        valid_to="2015-12-31",
    )
    add_variable(conn, register_id=1, var_id=201, name="Stateless", slug="nostate")
    return conn


def test_register_variable_coverage(db: sqlite3.Connection) -> None:
    cov = Catalog(db).register_variable_coverage("scb", "lisa")
    assert set(cov) == {"kon", "fin", "nostate"}

    # Default kon state is open-ended → coverage_to None, open_ended True.
    assert cov["kon"].state_count == 1
    assert cov["kon"].coverage_from == "2018-01-01"
    assert cov["kon"].coverage_to is None
    assert cov["kon"].open_ended is True

    # Finite window → coverage_to set, open_ended False.
    assert cov["fin"].state_count == 1
    assert cov["fin"].coverage_from == "2010-01-01"
    assert cov["fin"].coverage_to == "2015-12-31"
    assert cov["fin"].open_ended is False

    # Stateless variable → count 0, both bounds None (distinct from open-ended).
    assert cov["nostate"].state_count == 0
    assert cov["nostate"].coverage_from is None
    assert cov["nostate"].coverage_to is None
    assert cov["nostate"].open_ended is False


def test_provider_register_coverage(db: sqlite3.Connection) -> None:
    cov = Catalog(db).provider_register_coverage("scb")
    assert "lisa" in cov
    lisa = cov["lisa"]
    assert lisa.variable_count == 3  # kon + fin + nostate (all slugged)
    # Span over all states: earliest 2010 (fin), latest open-ended (kon).
    assert lisa.coverage_from == "2010-01-01"
    assert lisa.coverage_to is None
    assert lisa.open_ended is True


def test_coverage_bounds_mapping() -> None:
    # (coverage_from, coverage_to, open_ended).
    assert _coverage_bounds("2010-01-01", "9999-12-31") == ("2010-01-01", None, True)
    assert _coverage_bounds("2010-01-01", "2015-12-31") == (
        "2010-01-01",
        "2015-12-31",
        False,
    )
    assert _coverage_bounds(None, None) == (None, None, False)
