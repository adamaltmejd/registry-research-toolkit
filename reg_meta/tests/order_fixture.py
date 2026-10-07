from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from reader_artifacts import stamp_catalog_identity as stamp_test_catalog
from reg_meta.catalog import Catalog
from reg_meta.db import SCHEMA_VERSION
from reg_meta.inventory import load_inventory
from reg_meta.order import (
    SUPPORTED_SCHEMA_VERSION,
    requested_intervals,
    resolve_binding,
)
from reg_schema.project_data import Binding, PeriodRange, ProjectData, Source

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    from reg_meta.inventory import DeliveryInventory

_VARIANT = "scb/lisa/individer-15plus"

FIXTURE_INVENTORY = """
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

[[table]]
id = "LISA_Individ_2019-2020.csv"
edition = { from = 2019, to = 2020 }

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

[[table.column]]
name = "DispInk09"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/disponibel-inkomst"
representation = "DispInk09"

[[table.column]]
name = "Ssyk4"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
representation = "Ssyk4"
"""

# Same steward, but `Kon` is delivered for 2018 and 2020 only — an in-availability
# hole at 2019 that the coverage gate must fail the WHOLE order on.
GAPPED_INVENTORY = """
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

[[table]]
id = "LISA_Individ_2020.csv"
edition = 2020
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

"""


PARTITIONED_INVENTORY = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Mikro_2018-2020.csv"
edition = { from = 2018, to = 2020 }
partition = "mikro"
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

[[table]]
id = "LISA_Stora_2018-2020.csv"
edition = { from = 2018, to = 2020 }
partition = "stora"
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

"""


def order_inventory(tmp_path: Path, text: str) -> DeliveryInventory:
    path = tmp_path / "inventory.toml"
    path.write_text(text, encoding="utf-8")
    return load_inventory(path)


@pytest.fixture
def inventory(tmp_path: Path) -> DeliveryInventory:
    return order_inventory(tmp_path, FIXTURE_INVENTORY)


@pytest.fixture
def conn() -> sqlite3.Connection:
    """The synthetic catalog: one variant, three concepts, 2018–2020."""
    db = build_slugged_db()
    # Replace the builder's open-ended seed state with explicit yearly windows,
    # so availability is a real (clippable) interval rather than "since 2018".
    db.execute(
        "DELETE FROM variable_state WHERE variable_id = "
        "(SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'kon')"
    )
    for year in (2018, 2019, 2020):
        add_state(
            db,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from=f"{year}-01-01",
            valid_to=f"{year}-12-31",
            delivery_column_name="Kon",
        )
    add_variable(
        db,
        register_id=1,
        var_id=45,
        name="Disponibel inkomst",
        slug="disponibel-inkomst",
    )
    for year in (2019, 2020):
        add_state(
            db,
            register_id=1,
            variable_slug="disponibel-inkomst",
            register_variant_id=10,
            valid_from=f"{year}-01-01",
            valid_to=f"{year}-12-31",
            delivery_column_name="DispInk09",
        )
    add_variable(db, register_id=1, var_id=46, name="Yrke", slug="yrke")
    add_state(
        db,
        register_id=1,
        variable_slug="yrke",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="2018-12-31",
        delivery_column_name="Ssyk3",
    )
    add_state(
        db,
        register_id=1,
        variable_slug="yrke",
        register_variant_id=10,
        valid_from="2019-01-01",
        valid_to="2020-12-31",
        delivery_column_name="Ssyk4",
    )
    for key, value in (
        ("schema_version", SCHEMA_VERSION),
        ("import_date", "2026-08-01T00:00:00Z"),
    ):
        db.execute("INSERT INTO import_manifest VALUES (?, ?)", (key, value))
    stamp_test_catalog(db)
    db.commit()
    return db


def order_project(
    *variables: str,
    steward: str = "swecov",
    period: object = None,
    representation: str | None = None,
) -> ProjectData:
    return ProjectData(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        steward=steward,
        reg_meta_version="0.39.1",
        name="Synthetic order",
        sources=(
            Source(
                name="lisa",
                register_variant=_VARIANT,
                period=period
                if period is not None
                else PeriodRange(from_=2018, to=2020),
                bindings=tuple(
                    Binding(
                        variable=variable,
                        type="categorical",
                        representation=representation,
                    )
                    for variable in variables
                ),
            ),
        ),
    )


def raw_project(**over) -> dict:
    """`order_project` as the RAW dict the adapter door actually takes
    (`project_from_raw`, and `reg-meta order`'s file). `**over` replaces
    top-level keys — `schema_version` is the one the gate tests vary."""
    return {
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "steward": "global",
        "reg_meta_version": "0.39.1",
        "name": "Synthetic order",
        "sources": [
            {
                "name": "lisa",
                "register_variant": _VARIANT,
                "period": {"from": 2018, "to": 2020},
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            }
        ],
    } | over


def finding_codes(result) -> list[str]:
    return [finding.code for finding in result.findings]


def add_window(
    conn: sqlite3.Connection, *, variable_slug: str, column: str, lo: str, hi: str
) -> None:
    """One raw `variable_alias_window` row (#319) on the fixture's variant — a
    monthly family's month column inside its annual claim."""
    conn.execute(
        "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
        "delivery_column_name, valid_from, valid_to) "
        "SELECT variable_id, 10, ?, ?, ? FROM variable "
        "WHERE register_id = 1 AND slug = ?",
        (column, lo, hi, variable_slug),
    )
    conn.commit()


def resolve_project_binding(
    conn, variable: str, period: object, representation: str | None = None
):
    """Steps 1+2 alone, for the one binding of a single-source project."""
    source = order_project(
        variable, period=period, representation=representation
    ).sources[0]
    return resolve_binding(
        Catalog(conn), source, source.bindings[0], requested_intervals(source.period)
    )


def install_test_holdings(conn, inventory):
    """Compile the legacy readable TOML fixtures through the real compiler."""
    import csv
    import json
    import tempfile
    from pathlib import Path

    import tomlkit
    from reader_artifacts import CASES
    from reg_meta_build.artifact_identity import generation_id
    from reg_meta_build.derive import derive
    from reg_meta_build.holdings_compile import compile_holdings

    stamp_test_catalog(conn)
    with tempfile.TemporaryDirectory() as directory:
        candidate = Path(directory)
        (candidate / "policy").mkdir()
        (candidate / "swecov").mkdir()
        (candidate / "policy/inventory.toml").write_text(
            tomlkit.dumps(
                inventory.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
        )
        for name in (
            "source_policy.toml",
            "inventory_overlay.toml",
            "holdings_policy.toml",
        ):
            (candidate / "policy" / name).write_bytes(
                (CASES / "reader/fixture" / name).read_bytes()
            )
        width = max(len(table.columns) for table in inventory.tables)
        with (candidate / "swecov/SWECOV_variables_full_fixture.csv").open("w") as file:
            writer = csv.writer(file)
            writer.writerow(
                ["Category", "Detail", "Table", *[f"V{n}" for n in range(1, width + 1)]]
            )
            for table in inventory.tables:
                writer.writerow(
                    [
                        "Fixture",
                        "",
                        table.id,
                        *[column.name for column in table.columns],
                        *["" for _ in range(width - len(table.columns))],
                    ]
                )
        census = candidate / "swecov/SWECOV_variables_full_fixture.csv"
        import hashlib

        (candidate / "policy/holdings_policy.toml").write_text(
            f'source_sha256 = "{hashlib.sha256(census.read_bytes()).hexdigest()}"\n'
        )
        # As extend-db does: derive over the edited graph, then compile holdings.
        derive(conn)
        compiled = compile_holdings(conn, candidate, steward=inventory.steward)
    manifest = dict(conn.execute("SELECT key, value FROM import_manifest"))
    manifest.update(
        catalog_artifact_kind="steward",
        steward=inventory.steward,
        base_db_sha256="d" * 64,
        base_generation_id=manifest["generation_id"],
        holdings_input_commit="e" * 40,
        holdings_manifest_sha256="f" * 64,
        holdings_policy_sha256=compiled.accounting.policy_sha256,
        holdings_accounting_sha256=compiled.accounting.sha256,
        holdings_accounting_counts=json.dumps(
            compiled.accounting.counts, sort_keys=True, separators=(",", ":")
        ),
    )
    manifest["generation_id"] = generation_id(manifest)
    conn.executemany(
        "INSERT OR REPLACE INTO import_manifest VALUES (?, ?)", sorted(manifest.items())
    )
    conn.commit()
    return conn
