"""Inventory ↔ catalog-DB consistency gate (REFACTOR_SPEC.md §12).

Everything here is synthetic: a hand-built catalog DB (`_slugged_db`, the same
builder the Catalog and order tests use) plus fixture inventories parsed through
the real `load_inventory`. No steward's real holdings are committed; the gate
over the COMMITTED swecov inventory lives in
`reg_webapp/backend/tests/test_steward_swecov.py`, which needs a real flavored
DB.

The catalog fixture is one LISA variant delivering two concepts: `kon` (the
plain single-column case, co-delivered under a second alias `Konkod` through the
`variable_alias_window` expansion the resolver applies) and `yrke` (`Ssyk3`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from reg_meta.inventory import load_inventory
from reg_meta.inventory_check import check_inventory, unresolved_message

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    from reg_meta.inventory import DeliveryInventory

CLEAN_INVENTORY = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

[[table.column]]
name = "Ssyk3"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
representation = "Ssyk3"
"""


def _inventory(tmp_path: Path, text: str) -> DeliveryInventory:
    path = tmp_path / "inventory.toml"
    path.write_text(text, encoding="utf-8")
    return load_inventory(path)


@pytest.fixture
def conn() -> sqlite3.Connection:
    """The synthetic catalog: `scb/lisa/individer-15plus` delivering `Kon`
    (plus its co-delivered `Konkod` alias) and `Ssyk3`."""
    db = build_slugged_db()
    add_variable(db, register_id=1, var_id=46, name="Yrke", slug="yrke")
    add_state(
        db,
        register_id=1,
        variable_slug="yrke",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="2020-12-31",
        delivery_column_name="Ssyk3",
    )
    # #945 co-delivered aliases: one state expands into BOTH columns via
    # `variable_alias_window`, so `Konkod` is a real representation that exists
    # nowhere in `variable_state`. The base column participates too — that is
    # what makes `Catalog._expand_state_windows` expand the state at all.
    kon_id = db.execute(
        "SELECT variable_id FROM variable WHERE slug = 'kon'"
    ).fetchone()[0]
    db.executemany(
        "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
        "delivery_column_name, valid_from, valid_to) VALUES (?, 10, ?, ?, ?)",
        [
            (kon_id, "Kon", "2018-01-01", "9999-12-31"),
            (kon_id, "Konkod", "2018-01-01", "9999-12-31"),
        ],
    )
    db.commit()
    return db


def test_a_consistent_inventory_reports_nothing(conn, tmp_path) -> None:
    """The passing baseline: every coordinate resolves, so the gate is silent
    and a steward deployment boots."""
    assert check_inventory(_inventory(tmp_path, CLEAN_INVENTORY), conn) == ()


def test_a_renamed_variant_slug_is_one_finding_over_every_mapping(
    conn, tmp_path
) -> None:
    """Pre-v1 slug churn is legal, so a catalog release CAN rename
    `individer-15plus` out from under the inventory. The repair unit is the
    coordinate, not the mapping: one finding, carrying how many mappings it
    strands and where they sit."""
    (finding,) = check_inventory(
        _inventory(
            tmp_path, CLEAN_INVENTORY.replace("individer-15plus", "individer-16plus")
        ),
        conn,
    )
    assert finding.code == "variant_unresolved"
    assert finding.coordinate == "scb/lisa/individer-16plus"
    assert finding.mapping_count == 2
    assert finding.locations == (
        "table['LISA_Individ_2018.csv'].column['Kon']",
        "table['LISA_Individ_2018.csv'].column['Ssyk3']",
    )


def test_a_renamed_variable_slug_is_reported(conn, tmp_path) -> None:
    (finding,) = check_inventory(
        _inventory(tmp_path, CLEAN_INVENTORY.replace("lisa/yrke", "lisa/yrken")), conn
    )
    assert finding.code == "variable_unresolved"
    assert finding.coordinate == "scb/lisa/yrken"
    assert finding.mapping_count == 1


def test_one_column_serving_several_variants_is_sampled_once(conn, tmp_path) -> None:
    """A combined table's column carries one mapping per variant it serves
    (§12). At the variable grain those are the SAME physical place, so the
    location sample names it once while the count still says two — otherwise
    one column would spend the whole sample."""
    combined = (
        CLEAN_INVENTORY
        + """
[[table.column.mapping]]
register_variant = "scb/lisa/individer-16plus"
variable = "scb/lisa/yrken"
representation = "Ssyk3"
"""
    )
    findings = check_inventory(
        _inventory(tmp_path, combined.replace('lisa/yrke"', 'lisa/yrken"')), conn
    )
    (variable_finding,) = [f for f in findings if f.code == "variable_unresolved"]
    assert variable_finding.mapping_count == 2
    assert variable_finding.locations == (
        "table['LISA_Individ_2018.csv'].column['Ssyk3']",
    )


def test_a_representation_the_catalog_does_not_deliver_is_reported(
    conn, tmp_path
) -> None:
    """The third coordinate: the variant and the variable both resolve, but the
    physical column is not a `delivery_column_name` of that binding at that
    variant — the mapping claims a representation the catalog never had."""
    (finding,) = check_inventory(
        _inventory(tmp_path, CLEAN_INVENTORY.replace('"Ssyk3"', '"Ssyk4"')), conn
    )
    assert finding.code == "representation_unresolved"
    assert finding.coordinate == "scb/lisa/individer-15plus scb/lisa/yrke Ssyk4"
    assert "not a delivery column of scb/lisa/yrke" in finding.message


def test_a_representation_at_the_wrong_variant_is_reported(conn, tmp_path) -> None:
    """`Ssyk3` IS a delivery column — but of `yrke`, not of `kon`. The check is
    per cell, not per column name, so a mapping that crosses bindings is
    caught."""
    (finding,) = check_inventory(
        _inventory(tmp_path, CLEAN_INVENTORY.replace("lisa/yrke", "lisa/kon")), conn
    )
    assert finding.code == "representation_unresolved"
    assert finding.coordinate == "scb/lisa/individer-15plus scb/lisa/kon Ssyk3"


def test_a_window_only_representation_resolves(conn, tmp_path) -> None:
    """`Konkod` exists only in `variable_alias_window` — the resolver expands a
    state into it, so an order can serve it and the gate must not strand it."""
    assert (
        check_inventory(
            _inventory(tmp_path, CLEAN_INVENTORY.replace('"Kon"\n', '"Konkod"\n')),
            conn,
        )
        == ()
    )


def test_a_same_as_aliased_variable_resolves(conn, tmp_path) -> None:
    """A binding FQID that misses the direct slug lookup but resolves through a
    curated `variable_same_as` edge is exactly what the order path resolves, so
    the gate accepts it — a stricter gate would fail a deployment over holdings
    it can actually serve."""
    conn.execute(
        "INSERT INTO variable_same_as "
        "(a_provider, a_register, a_variable, b_provider, b_register, b_variable) "
        "VALUES ('scb','lisa','konkod','scb','lisa','kon')"
    )
    conn.commit()
    assert (
        check_inventory(
            _inventory(tmp_path, CLEAN_INVENTORY.replace("lisa/kon", "lisa/konkod")),
            conn,
        )
        == ()
    )


def test_a_mapping_without_a_representation_takes_no_representation_check(
    conn, tmp_path
) -> None:
    """§12's single-representation arm is request-dependent — only the order
    pass can decide whether the binding resolves to ONE representation across a
    requested period — so an unqualified mapping is checked at the variant and
    variable grains only, physical column name notwithstanding."""
    unqualified = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018

[[table.column]]
name = "SomeColumnTheCatalogNeverNames"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
"""
    assert check_inventory(_inventory(tmp_path, unqualified), conn) == ()


def test_an_unresolved_variant_does_not_cascade_into_its_cells(conn, tmp_path) -> None:
    """One coordinate broken, one finding: reporting each cell under a dead
    variant again would bury the one line the maintainer must act on."""
    findings = check_inventory(
        _inventory(
            tmp_path,
            CLEAN_INVENTORY.replace("scb/lisa/individer-15plus", "scb/lisa/gone"),
        ),
        conn,
    )
    assert [f.code for f in findings] == ["variant_unresolved"]


def test_findings_are_grouped_by_code_and_deterministic(conn, tmp_path) -> None:
    """Grouped variant → variable → representation, sorted by coordinate within
    each group, and byte-identical across runs — a gate whose report reorders
    cannot be diffed against a previous release."""
    broken = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Konx"

[[table.column]]
name = "Ssyk3"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrken"
representation = "Ssyk3"

[[table.column]]
name = "Alder"
[[table.column.mapping]]
register_variant = "scb/lisa/gone"
variable = "scb/lisa/kon"
representation = "Kon"
"""
    inventory = _inventory(tmp_path, broken)
    findings = check_inventory(inventory, conn)
    assert [f.code for f in findings] == [
        "variant_unresolved",
        "variable_unresolved",
        "representation_unresolved",
    ]
    assert check_inventory(inventory, conn) == findings


def test_the_message_names_every_finding_with_its_scale(conn, tmp_path) -> None:
    """The one line the boot failure and the maintainer's pytest both read."""
    findings = check_inventory(
        _inventory(
            tmp_path, CLEAN_INVENTORY.replace("individer-15plus", "individer-16plus")
        ),
        conn,
    )
    message = unresolved_message(findings)
    assert "\n" not in message
    assert "1 unresolved inventory coordinate(s)" in message
    assert "2 mapping(s)" in message
    assert "scb/lisa/individer-16plus" in message
    assert "table['LISA_Individ_2018.csv'].column['Kon']" in message
