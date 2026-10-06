"""SWECOV generator: the committed inventory overlay's mapping and unmap decisions.

`inventory_overlay.toml` can pin one physical column to a reviewed catalog
representation (`[[mapping]]`) or suppress a column's mappings (`[[unmap]]`). Each
declaration is guarded by the physical coordinates and by full catalog coverage of
the edition; a declaration that no longer holds refuses the run without replacing
the inventory. Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    flavored_db_fixture,  # noqa: F401
    inventory_worklist as _inventory_worklist,
    run_inventory as _run_inventory,
)
from reg_meta.inventory import load_inventory as load_delivery_inventory

if TYPE_CHECKING:
    from pathlib import Path


def _literal_mapping_overlay(**changes: object) -> str:
    fields = {
        "table": "T2019",
        "column": "Physical",
        "edition": 2019,
        "register_variant": "inera/bestallda-prover/_default",
        "variable": "inera/bestallda-prover/t-kolumn",
        "representation": "T_kolumn",
        "reason": "Exact reviewed source coordinate",
        **changes,
    }
    return (
        "[[mapping]]\n"
        + "\n".join(f"{key} = {json.dumps(value)}" for key, value in fields.items())
        + "\n"
    )


def test_inventory_overlay_preserves_physical_column_with_checked_representation(
    tmp_path: Path, flavored_db: Path
) -> None:
    steward = _run_inventory(
        tmp_path,
        flavored_db,
        _literal_mapping_overlay(),
        "T2019",
        ["Physical", "Unknown"],
    )
    table = load_delivery_inventory(steward / "inventory.toml").tables[0]
    assert table.edition == "2019"
    physical = next(c for c in table.columns if c.name == "Physical")
    assert len(physical.mappings) == 1
    assert physical.mappings[0].representation == "T_kolumn"
    assert str(physical.mappings[0].variable) == "inera/bestallda-prover/t-kolumn"
    assert next(c for c in table.columns if c.name == "Unknown").mappings == ()


@pytest.mark.parametrize(
    "reason", [None, '  Unresolved source owner; preserve "FIXBB".  ']
)
def test_inventory_unmap_preserves_exact_reason_without_other_changes(
    tmp_path: Path, flavored_db: Path, reason: str | None
) -> None:
    overlay = '[[unmap]]\ntable = "T2019"\ncolumn = "T_kolumn"\n'
    if reason is not None:
        overlay += f"reason = {json.dumps(reason)}\n"
    steward = _run_inventory(
        tmp_path, flavored_db, overlay, "T2019", ["T_kolumn", "Unknown"]
    )
    inventory = load_delivery_inventory(steward / "inventory.toml")
    assert inventory.tables[0].id == "T2019"
    assert inventory.tables[0].edition == "2019"
    assert [column.name for column in inventory.tables[0].columns] == [
        "T_kolumn",
        "Unknown",
    ]
    assert inventory.tables[0].columns[0].mappings == ()
    assert inventory.tables[0].columns[0].unmapped_reason == reason
    assert inventory.tables[0].columns[1].mappings == ()
    assert inventory.tables[0].columns[1].unmapped_reason is None
    baseline_dir = tmp_path / "without-reason"
    baseline_dir.mkdir()
    baseline_steward = _run_inventory(
        baseline_dir,
        flavored_db,
        '[[unmap]]\ntable = "T2019"\ncolumn = "T_kolumn"\n',
        "T2019",
        ["T_kolumn", "Unknown"],
    )
    expected = load_delivery_inventory(baseline_steward / "inventory.toml").model_dump(
        by_alias=False
    )
    expected["tables"][0]["columns"][0]["unmapped_reason"] = reason
    assert inventory.model_dump(by_alias=False) == expected


def test_inventory_unmap_refuses_blank_reason_before_writing(
    tmp_path: Path, flavored_db: Path
) -> None:
    with pytest.raises(SystemExit, match="unmapped_reason must be nonblank"):
        _run_inventory(
            tmp_path,
            flavored_db,
            '[[unmap]]\ntable = "T2019"\ncolumn = "T_kolumn"\nreason = "  "\n',
            "T2019",
            ["T_kolumn"],
        )
    assert not (tmp_path / "steward/inventory.toml").exists()
    assert not (tmp_path / "derived").exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"edition": 2020},
        {"column": "Missing"},
        {"table": "Missing2019"},
        {"variable": "inera/bestallda-prover/missing"},
        {"representation": "Missing"},
        {"register_variant": "inera/bestallda-prover/unknown"},
    ],
)
def test_inventory_overlay_refuses_drift_without_replacing_inventory(
    tmp_path: Path, flavored_db: Path, changes: dict[str, object]
) -> None:
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(
            tmp_path,
            flavored_db,
            _literal_mapping_overlay(**changes),
            "T2019",
            ["Physical"],
        )
    assert not (tmp_path / "steward/inventory.toml").exists()
    worklist = json.loads((tmp_path / "derived/inventory_worklist.json").read_text())
    assert worklist["mapping_scope_needed"]


def _overlay_db(
    tmp_path: Path, flavored_db: Path, *, valid_to: str, competing_owner: bool
) -> Path:
    """A copy of the flavored DB whose `t-kolumn` state covers 2019-01-01 to
    `valid_to`, optionally beside a second variable, `different-quantity`,
    delivered through the same literal column `T_kolumn` over all of 2019."""
    db = tmp_path / "overlay.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE variable_state SET valid_from='2019-01-01', valid_to=? "
            "WHERE variable_id=904",
            (valid_to,),
        )
        if competing_owner:
            conn.execute(
                "INSERT INTO variable(variable_id,register_id,provider_key,slug,name) "
                "VALUES (910,901,'different-quantity','different-quantity',"
                "'different-quantity')"
            )
            conn.execute(
                "INSERT INTO variable_state(variable_id,register_variant_id,"
                "valid_from,valid_to,delivery_column_name) "
                "VALUES (910,902,'2019-01-01','2019-12-31','T_kolumn')"
            )
    return db


def _refused_mapping(tmp_path: Path, db: Path, overlay: str) -> str:
    """Run `cmd_inventory` over the overlay's physical column; the refusal code."""
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, db, overlay, "T2019", ["Physical"])
    assert not (tmp_path / "steward/inventory.toml").exists()
    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert entry["column"] == "Physical"
    return entry["code"]


def test_inventory_overlay_requires_complete_catalog_coverage(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = _overlay_db(
        tmp_path, flavored_db, valid_to="2019-06-30", competing_owner=False
    )

    assert (
        _refused_mapping(tmp_path, db, _literal_mapping_overlay())
        == "stale_declared_mapping_target"
    )


def test_inventory_overlay_refuses_duplicate_declarations(
    tmp_path: Path, flavored_db: Path
) -> None:
    with pytest.raises(SystemExit, match="duplicate mapping override"):
        _run_inventory(
            tmp_path,
            flavored_db,
            _literal_mapping_overlay() * 2,
            "T2019",
            ["Physical"],
        )
    assert not (tmp_path / "steward/inventory.toml").exists()


def test_inventory_overlay_does_not_hide_ambiguous_catalog_owners(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = _overlay_db(tmp_path, flavored_db, valid_to="2019-12-31", competing_owner=True)

    assert (
        _refused_mapping(tmp_path, db, _literal_mapping_overlay())
        == "stale_declared_mapping_target"
    )


def test_inventory_overlay_preserves_other_positive_owner_for_ambiguity_check(
    tmp_path: Path, flavored_db: Path
) -> None:
    overlay = _literal_mapping_overlay(
        column="T_kolumn",
        variable="inera/bestallda-prover/covid-19-antikroppar",
        representation="Covid_19_antikroppar",
    )
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, flavored_db, overlay, "T2019", ["T_kolumn"])
    worklist = json.loads((tmp_path / "derived/inventory_worklist.json").read_text())
    assert worklist["mapping_scope_needed"][0]["code"] == "ambiguous_delivered_owners"


def test_inventory_overlay_explicit_owner_selection_preserves_physical_column(
    tmp_path: Path, flavored_db: Path
) -> None:
    import shutil

    db = tmp_path / "competing-owner.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE variable_state SET delivery_column_name='T_kolumn' "
            "WHERE variable_id=903"
        )
    overlay = _literal_mapping_overlay(column="T_kolumn", select_owner=True)
    steward = _run_inventory(tmp_path, db, overlay, "T2019", ["T_kolumn"])
    column = load_delivery_inventory(steward / "inventory.toml").tables[0].columns[0]
    assert column.name == "T_kolumn"
    assert [str(m.variable) for m in column.mappings] == [
        "inera/bestallda-prover/t-kolumn"
    ]
    assert column.mappings[0].representation == "T_kolumn"


def test_inventory_overlay_owner_selection_requires_own_complete_coverage(
    tmp_path: Path, flavored_db: Path
) -> None:
    """Selecting an owner narrows the candidates to that owner, whose own
    windows must then cover the edition; another owner's coverage cannot lend
    it any."""
    db = _overlay_db(tmp_path, flavored_db, valid_to="2019-06-30", competing_owner=True)

    assert (
        _refused_mapping(tmp_path, db, _literal_mapping_overlay(select_owner=True))
        == "stale_declared_mapping_target"
    )
