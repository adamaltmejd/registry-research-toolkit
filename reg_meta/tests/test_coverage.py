"""Coverage aggregates over `variable_state` (#351): `Catalog`
`register_variable_coverage` / `register_column_coverage` /
`register_unnamed_column_coverage` / `provider_register_coverage`.

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

from _slugged_db import add_state, add_variable, build_slugged_db

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


def test_multistate_fan_out() -> None:
    """A variable with multiple states + a register with multiple variables: the
    GROUP BY aggregates stay correct under the LEFT JOIN fan-out (MIN/MAX span
    all states, COUNT(DISTINCT) doesn't double-count)."""
    conn = build_slugged_db()  # scb/lisa/kon, one open-ended state
    add_variable(conn, register_id=1, var_id=300, name="Multi", slug="multi")
    add_state(
        conn,
        register_id=1,
        variable_slug="multi",
        register_variant_id=10,
        valid_from="2000-01-01",
        valid_to="2005-12-31",
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="multi",
        register_variant_id=10,
        valid_from="2008-01-01",
        valid_to="2012-12-31",
    )
    cov = Catalog(conn).register_variable_coverage("scb", "lisa")
    assert cov["multi"].state_count == 2
    assert cov["multi"].coverage_from == "2000-01-01"
    assert cov["multi"].coverage_to == "2012-12-31"  # MAX across both windows
    assert cov["multi"].open_ended is False

    reg = Catalog(conn).provider_register_coverage("scb")["lisa"]
    assert reg.variable_count == 2  # kon + multi, NOT fanned out by 3 states


def test_register_column_coverage_distinct_windows() -> None:
    """A variable with two delivery columns (#819) — e.g. CDISP 1968– vs CDISP5
    2020– on one `disponibel-inkomst` variable — gets a DISTINCT per-column window
    from `register_column_coverage`, not the variable's union span. States with a
    NULL `delivery_column_name` are skipped (no per-column key)."""
    conn = build_slugged_db()  # scb/lisa/kon, one state with NULL delivery_column
    add_variable(conn, register_id=1, var_id=400, name="DispInk", slug="disp")
    add_state(
        conn,
        register_id=1,
        variable_slug="disp",
        register_variant_id=10,
        valid_from="1968-01-01",
        valid_to="2024-12-31",
        delivery_column_name="CDISP",
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="disp",
        register_variant_id=10,
        valid_from="2020-01-01",
        valid_to="2024-12-31",
        delivery_column_name="CDISP5",
    )
    # A state with NO delivery column — no per-column key, served by the fallback.
    add_state(
        conn,
        register_id=1,
        variable_slug="disp",
        register_variant_id=10,
        valid_from="1900-01-01",
        valid_to="1967-12-31",
        delivery_column_name=None,
    )

    col_cov = Catalog(conn).register_column_coverage("scb", "lisa")
    # Each representation gets its OWN window — NOT the variable's 1900–2024 union.
    assert col_cov[("disp", "CDISP")].coverage_from == "1968-01-01"
    assert col_cov[("disp", "CDISP")].coverage_to == "2024-12-31"
    assert col_cov[("disp", "CDISP5")].coverage_from == "2020-01-01"
    assert col_cov[("disp", "CDISP5")].coverage_to == "2024-12-31"

    # The NULL-delivery_column state contributes no per-column key for `disp`.
    assert ("disp", None) not in col_cov
    assert all(col is not None for _, col in col_cov)

    unnamed_cov = Catalog(conn).register_unnamed_column_coverage("scb", "lisa")
    assert unnamed_cov["disp"].state_count == 1
    assert unnamed_cov["disp"].coverage_from == "1900-01-01"
    assert unnamed_cov["disp"].coverage_to == "1967-12-31"

    # The variable-level union still spans every representation + the NULL state
    # (the fallback path for whole-variable / column-less members).
    var_cov = Catalog(conn).register_variable_coverage("scb", "lisa")
    assert var_cov["disp"].coverage_from == "1900-01-01"
    assert var_cov["disp"].coverage_to == "2024-12-31"


def test_coverage_bounds_mapping() -> None:
    # (coverage_from, coverage_to, open_ended).
    assert _coverage_bounds("2010-01-01", "9999-12-31") == ("2010-01-01", None, True)
    assert _coverage_bounds("2010-01-01", "2015-12-31") == (
        "2010-01-01",
        "2015-12-31",
        False,
    )
    assert _coverage_bounds(None, None) == (None, None, False)
