"""Shared filtered-steward test data and writers."""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

from reg_meta.inventory import DeliveryInventory
from reg_webapp.catalog_index import build_catalog_index

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from reg_meta.catalog import Catalog
    from reg_webapp.catalog_index import CatalogIndex

IFAU_TOML = """\
id = "ifau"
name = "IFAU"
long_name = "Institute for Evaluation of Labour Market and Education Policy"
hostname = "ifau.example.org"
"""

# One admitted holding: (register_variant coordinate, binding FQID,
# representation — the canonical `delivery_column_name`, or None for "the
# concept's single representation" — and the table's edition).
Holding = tuple[str, str, str | None, str]

# The fixture DB resolves scb/lisa/individer-15plus (binding scb/lisa/kon, state
# 2018+, column "Kon") and scb/rams/standard (binding scb/rams/syss, column
# "Syss").
CLEAN_HOLDINGS: list[Holding] = [
    ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2018"),
    ("scb/rams/standard", "scb/rams/syss", "Syss", "2019"),
]

# Y-107: the case-twin holding. The steward states the column as the era its
# inventory was generated over spells it (`Idh`, the alias window's own case — what
# the resolver and the boot gate answer there), while the catalog also delivers that
# column as `IdH` — see `fixture_db.seed_case_twin_column`, which seeds that variable
# and the UNHELD `Taxvarde` rename. Every steward surface must fold the two spellings
# together; the `case_twin_db` fixture pairs with this list.
CASE_TWIN_HOLDINGS: list[Holding] = [
    ("scb/lisa/individer-15plus", "scb/lisa/idve", "Idh", "2013"),
]


def inventory_toml(holdings: Sequence[Holding], *, steward: str = "ifau") -> str:
    """Render a minimal delivery inventory stating ``holdings``.

    One table per holding, so each carries its own edition and the §12
    one-to-one resolution invariant is satisfied by construction as long as the
    holdings name distinct ``(variant, variable, representation)`` coordinates.
    The physical column name is deliberately NOT the representation (it is
    prefixed): a steward's literal delivery spelling and reg_meta's canonical
    ``delivery_column_name`` are different things, and the index must admit the
    latter.
    """
    lines = ["version = 1", f'steward = "{steward}"']
    for idx, (variant, variable, representation, edition) in enumerate(holdings):
        lines += [
            "",
            "[[table]]",
            f'id = "T{idx}_{edition}.csv"',
            f'edition = "{edition}"',
            "",
            "[[table.column]]",
            f'name = "P{idx}_{variable.rsplit("/", 1)[1]}"',
            "[[table.column.mapping]]",
            f'register_variant = "{variant}"',
            f'variable = "{variable}"',
        ]
        if representation is not None:
            lines.append(f'representation = "{representation}"')
    return "\n".join(lines) + "\n"


def write_steward(
    stewards_dir: Path,
    steward_id: str,
    holdings: Sequence[Holding],
    *,
    inventory: bool = True,
) -> None:
    """Write a complete named-steward deployment directory.

    A named steward is configured by ``steward.toml`` (identity) plus
    ``inventory.toml`` (its holdings — both the catalog filter and the order
    topology). ``inventory=False`` writes the INCOMPLETE one — the deployment
    that must fail at boot (see ``test_project_order`` → the named-steward boot
    guard).
    """
    base = stewards_dir / steward_id
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(IFAU_TOML, encoding="utf-8")
    if inventory:
        (base / "inventory.toml").write_text(
            inventory_toml(holdings, steward=steward_id), encoding="utf-8"
        )


def write_global(stewards_dir: Path) -> None:
    base = stewards_dir / "global"
    base.mkdir(parents=True)
    (base / "steward.toml").write_text(
        'id = "global"\nname = "Global"\nlong_name = "Full universe"\n'
        'hostname = "global.example.org"\n',
        encoding="utf-8",
    )


def catalog_index(
    holdings: Sequence[Holding], catalog: Catalog, *, steward: str = "ifau"
) -> CatalogIndex:
    """The boot index a steward stating ``holdings`` would build — the same
    ``model_validate`` ``load_inventory`` runs, without a file on disk."""
    inventory = DeliveryInventory.model_validate(
        tomllib.loads(inventory_toml(holdings, steward=steward))
    )
    return build_catalog_index(inventory, catalog)
