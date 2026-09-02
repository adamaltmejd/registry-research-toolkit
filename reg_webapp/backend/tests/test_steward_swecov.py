"""Regression guard for the committed SWECOV delivery inventory.

The inventory (`reg_webapp/stewards/swecov/inventory.toml`) is generated against
a flavored reg_meta DB (see its README). A FULL boot needs that DB and is the
maintainer's real-data validation, not a CI fixture — so this guards the
committed artifact with what CI *can* run: it must load as a steward, parse
through `reg_meta.inventory`'s structural contract (including §12's one-to-one
resolution invariant), and build the boot admission index.

The index build is the load-bearing part: every SWECOV mapping states an
explicit `representation`, which IS reg_meta's canonical
`delivery_column_name`, so the whole 36k-mapping admission set is derived with
**zero** catalog access. That is why this runs against no DB at all — and why
booting the deployment costs a TOML parse rather than thousands of resolutions.

The one DB-BACKED test is REFACTOR_SPEC.md §12's inventory ↔ DB consistency
gate over the same committed inventory. It carries the `release` marker: the
flavored DB is a release asset, so the default gate (`-m "not integration and
not release"`) DESELECTS it rather than skipping it — a deselected test never
enters the JUnit report, while a skipped one reads as missing evidence. The
maintainer runs it when regenerating the inventory or cutting a reg_meta
release:

    REG_META_DB=<extend-db output dir> pytest --run-release -m release

Opting in and then finding no DB is a misconfigured run, not a clean pass, so
the fixture FAILS there instead of skipping. The webapp boot gate is the hard
line; this is the maintainer's early warning.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import reg_meta.db
from reg_meta.inventory import DeliveryInventory, load_inventory
from reg_meta.inventory_check import check_inventory, unresolved_message
from reg_webapp.catalog_index import build_catalog_index

from reg_webapp.stewards import load_delivery_inventory, load_steward

_STEWARDS_DIR = Path(__file__).resolve().parents[2] / "stewards"
_SWECOV = _STEWARDS_DIR / "swecov"


class _NoCatalog:
    """A catalog that fails the test if the index build touches it."""

    def __getattr__(self, name: str):
        raise AssertionError(f"index build touched the catalog: {name}")


@pytest.fixture(scope="module")
def inventory() -> DeliveryInventory:
    # Module-scoped: the committed inventory is ~7 MB, so parse it once.
    return load_inventory(_SWECOV / "inventory.toml")


def test_swecov_steward_loads_with_its_inventory() -> None:
    steward = load_steward("swecov", root=_STEWARDS_DIR)
    assert steward.id == "swecov"
    # The named steward's boot read — absent/mis-stewarded would raise here.
    assert load_delivery_inventory(steward, root=_STEWARDS_DIR) is not None


def test_inventory_shape(inventory: DeliveryInventory) -> None:
    assert inventory.steward == "swecov"
    assert inventory.tables, "inventory must declare at least one table"


def test_every_mapping_pins_its_representation(inventory: DeliveryInventory) -> None:
    """The generator pins the resolved delivery column on every mapping — the
    load-bearing property that makes admission exact without a DB round-trip
    (and keeps the null-representation "concept's single representation" arm
    unused for this steward)."""
    unpinned = [
        f"{table.id}.{column.name}"
        for table in inventory.tables
        for column in table.columns
        for mapping in column.mappings
        if mapping.representation is None
    ]
    assert not unpinned, f"mappings with no representation: {unpinned[:5]}"


def test_index_builds_green_with_no_catalog_access(
    inventory: DeliveryInventory,
) -> None:
    """Observable behavior 3: the deployment's admission set is derived from the
    committed inventory alone — non-empty, drift-free, and DB-free."""
    index = build_catalog_index(inventory, _NoCatalog())

    assert index.drift_warnings == ()
    assert index.admitted_variable_fqids
    assert index.held_provider_slugs
    span = index.catalog_period_span
    assert span is not None and span[0] <= span[1]


def test_known_holdings_are_admitted(inventory: DeliveryInventory) -> None:
    index = build_catalog_index(inventory, _NoCatalog())

    assert index.admits("swecov/population/personnr", "PersonNr")
    assert index.admits("swecov/population/indexpop", "IndexPop")
    assert index.admits("swecov/adress-sarskilt-boende/utdadr2", "UtdAdr2")
    # The stale hreg-sun-groupings register must stay out of the catalog.
    assert not index.admits_register("swecov/hreg-sun-groupings")


@pytest.fixture(scope="module")
def flavored_conn():
    """A read-only connection to the flavored SWECOV catalog DB.

    The deployment's reg_meta asset IS the flavored `extend-db` output, pointed
    at by `REG_META_DB` (REFACTOR_SPEC.md §11), so that env var is the seam
    here too — the inventory binds steward-only providers (`swecov`,
    `region-*`, …) that the plain global release DB does not contain, and
    checking against the wrong DB would report the whole flavor as stranded.

    Nothing here skips. The `release` marker on the only test using this
    fixture is what keeps the flavored DB optional, so reaching this code means
    the runner asked for the gate — an unset `REG_META_DB`, a path naming no
    DB, or a schema-incompatible one (`open_db` raises, exactly as boot would)
    are all misconfigured runs, and a skip would report the gate as clean
    without ever having run it."""
    db_dir = os.environ.get("REG_META_DB")
    if not db_dir:
        pytest.fail(
            "no flavored SWECOV DB: set REG_META_DB to the extend-db output dir "
            "(see stewards/swecov/README.md) to run the §12 consistency gate"
        )
    db_path = reg_meta.db.db_path_from_args(db_dir)
    if not db_path.is_file():
        pytest.fail(f"REG_META_DB names no catalog DB at {db_path}")
    conn = reg_meta.db.open_db(db_path)
    try:
        yield conn
    finally:
        conn.close()


@pytest.mark.release
def test_every_inventory_mapping_resolves_against_the_flavored_db(
    inventory: DeliveryInventory, flavored_conn
) -> None:
    """§12's standing gate over the committed inventory: every mapping's
    `(register_variant, variable FQID, representation)` resolves against the DB
    the SWECOV deployment serves. Zero unresolved mappings is the baseline — a
    catalog release that renames a slug strands mappings silently otherwise,
    and the deployment would refuse to boot.

    `release`-marked because the flavored DB is a release asset: the default
    gate deselects this test, and `--run-release` with `REG_META_DB` set runs
    it."""
    findings = check_inventory(inventory, flavored_conn)
    assert not findings, unresolved_message(findings)
