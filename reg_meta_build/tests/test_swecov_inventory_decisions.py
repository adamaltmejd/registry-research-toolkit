"""SWECOV generator: a series' curated column mapping and unmap decisions.

A series `[[column]]` can pin one physical column to a reviewed catalog
representation (`mappings`) or suppress a column's mappings (`unmap`). Each
mapping is guarded by the physical coordinates and by full catalog coverage of
the table's edition; a decision that no longer holds refuses the run without
replacing the inventory. Fixture provenance and the generator's boundary:
`_swecov_fixtures`.
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
from reg_meta_build.errors import RegMetaError
from reg_meta_build.inventory import (
    edition_bounds,
    load_inventory as load_delivery_inventory,
)

if TYPE_CHECKING:
    from pathlib import Path


def _mapping_decision(**changes: object) -> str:
    fields: dict[str, object] = {
        "register_variant": "inera/bestallda-prover/_default",
        "variable": "inera/bestallda-prover/t-kolumn",
        "representation": "T_kolumn",
    }
    column = changes.pop("column", "Physical")
    select_owner = changes.pop("select_owner", False)
    fields.update(changes)
    mapping = ", ".join(f"{key} = {json.dumps(value)}" for key, value in fields.items())
    return (
        f"[[column]]\ncolumn = {json.dumps(column)}\nmappings = [{{ {mapping} }}]\n"
        'reason = "Exact reviewed source coordinate"\n'
        + ("select_owner = true\n" if select_owner else "")
    )


def test_mapping_decision_preserves_physical_column_with_checked_representation(
    tmp_path: Path, flavored_db: Path
) -> None:
    steward = _run_inventory(
        tmp_path,
        flavored_db,
        "T2019",
        ["Physical"],
        decisions=_mapping_decision(),
    )
    table = load_delivery_inventory(steward / "inventory.toml").tables[0]
    assert table.edition == "2019"
    physical = next(c for c in table.columns if c.name == "Physical")
    assert len(physical.mappings) == 1
    assert physical.mappings[0].representation == "T_kolumn"
    assert str(physical.mappings[0].variable) == "inera/bestallda-prover/t-kolumn"


_RANGE = "{ from = 2018, to = 2019 }"


def test_mapping_decision_maps_column_of_range_edition_table(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A multi-year table (its edition a curated `{ from, to }` range) takes a
    reviewed mapping checked against that whole range. Fails if a mapping
    decision accepts only a single year or token edition (#1293)."""
    steward = _run_inventory(
        tmp_path,
        flavored_db,
        "TSERIES",
        ["Physical"],
        edition=_RANGE,
        decisions=_mapping_decision(),
    )
    (table,) = load_delivery_inventory(steward / "inventory.toml").tables
    assert edition_bounds(table.edition) == (("2018-01-01", "2019-12-31"),)
    (physical,) = table.columns
    assert [m.representation for m in physical.mappings] == ["T_kolumn"]


_WARNING = (
    'data_warning = { summary = "Recoded", detail = "Recoded by a later delivery.", '
    "window = { from = 2017, to = 2018 } }"
)


def test_mapping_decision_warning_is_clipped_to_the_table(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A decision's mapping warning (#1684) reaches each of its tables with its
    window clipped to the period the mapping serves there: 2017..2018 on a
    2018..2019 table is 2018, with the authored texts. Fails if the generator
    drops the warning or emits it unclipped (the inventory refuses a warning
    window past what its mapping serves)."""
    decision = _mapping_decision().replace('"T_kolumn" }', f'"T_kolumn", {_WARNING} }}')
    steward = _run_inventory(
        tmp_path,
        flavored_db,
        "TSERIES",
        ["Physical"],
        edition=_RANGE,
        decisions=decision,
    )
    (table,) = load_delivery_inventory(steward / "inventory.toml").tables
    ((mapping,),) = [column.mappings for column in table.columns]
    assert mapping.data_warning is not None
    assert (
        mapping.data_warning.summary,
        mapping.data_warning.detail,
        mapping.data_warning.window,
    ) == ("Recoded", "Recoded by a later delivery.", "2018")


def test_mapping_decision_refuses_range_without_full_coverage(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The coverage guard holds for a range edition too: the catalog state ends
    mid-2019, so a mapping over 2018–2019 is refused. Fails if a range edition
    skips the coverage check that a single-year mapping gets."""
    db = _decision_db(
        tmp_path, flavored_db, valid_to="2019-06-30", competing_owner=False
    )
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(
            tmp_path,
            db,
            "TSERIES",
            ["Physical"],
            edition=_RANGE,
            decisions=_mapping_decision(),
        )
    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert entry["code"] == "stale_declared_mapping_target"


@pytest.mark.parametrize(
    ("window", "code"),
    [
        ("{ from = 2017, to = 2018 }", None),
        ("{ from = 2017, to = 2019 }", "inventory_coverage_warning_invalid"),
    ],
)
def test_mapping_decision_warning_licenses_exactly_the_uncovered_years(
    tmp_path: Path, flavored_db: Path, window: str, code: str | None
) -> None:
    """The catalog state starts in 2019, so a mapping over 2018..2019 leaves 2018
    uncovered (#1684). A warning whose window, clipped to the table, is exactly
    2018 admits the mapping (emitted as 2018); one whose clip is 2018..2019 is
    `inventory_coverage_warning_invalid`, the code extend-db refuses it by.
    Unwarned, the gap stays stale (the case above). Fails if the generator
    compares the unclipped window, accepts a mismatched warning, or reports the
    mismatch as a stale target."""
    db = _decision_db(
        tmp_path, flavored_db, valid_to="2019-12-31", competing_owner=False
    )
    warning = _WARNING.replace("{ from = 2017, to = 2018 }", window)
    decision = _mapping_decision().replace('"T_kolumn" }', f'"T_kolumn", {warning} }}')
    if code is not None:
        with pytest.raises(SystemExit, match="Inventory not replaced"):
            _run_inventory(
                tmp_path,
                db,
                "TSERIES",
                ["Physical"],
                edition=_RANGE,
                decisions=decision,
            )
        (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
        assert entry["code"] == code
        return
    steward = _run_inventory(
        tmp_path, db, "TSERIES", ["Physical"], edition=_RANGE, decisions=decision
    )
    (table,) = load_delivery_inventory(steward / "inventory.toml").tables
    ((mapping,),) = [column.mappings for column in table.columns]
    assert mapping.data_warning is not None
    assert mapping.data_warning.window == "2018"


def test_unmap_preserves_exact_reason_without_other_changes(
    tmp_path: Path, flavored_db: Path
) -> None:
    reason = '  Unresolved source owner; preserve "FIXBB".  '
    steward = _run_inventory(
        tmp_path,
        flavored_db,
        "T2019",
        ["T_kolumn"],
        decisions=f'[[column]]\ncolumn = "T_kolumn"\nunmap = {json.dumps(reason)}\n',
    )
    (table,) = load_delivery_inventory(steward / "inventory.toml").tables
    assert (table.id, table.edition) == ("T2019", "2019")
    assert [
        (column.name, column.mappings, column.unmapped_reason)
        for column in table.columns
    ] == [("T_kolumn", (), reason)]


def test_unmap_refuses_blank_reason_before_writing(
    tmp_path: Path, flavored_db: Path
) -> None:
    with pytest.raises(RegMetaError, match="reasons must be nonblank"):
        _run_inventory(
            tmp_path,
            flavored_db,
            "T2019",
            ["T_kolumn"],
            decisions='[[column]]\ncolumn = "T_kolumn"\nunmap = "  "\n',
        )
    assert not (tmp_path / "steward/inventory.toml").exists()
    assert not (tmp_path / "steward/derived").exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"column": "Missing"},
        {"variable": "inera/bestallda-prover/missing"},
        {"representation": "Missing"},
    ],
)
def test_mapping_decision_refuses_drift_without_replacing_inventory(
    tmp_path: Path, flavored_db: Path, changes: dict[str, object]
) -> None:
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(
            tmp_path,
            flavored_db,
            "T2019",
            ["Physical"],
            decisions=_mapping_decision(**changes),
        )
    assert not (tmp_path / "steward/inventory.toml").exists()
    assert _inventory_worklist(tmp_path)["mapping_scope_needed"]


def _decision_db(
    tmp_path: Path, flavored_db: Path, *, valid_to: str, competing_owner: bool
) -> Path:
    """A copy of the flavored DB whose `t-kolumn` state covers 2019-01-01 to
    `valid_to`, optionally beside a second variable, `different-quantity`,
    delivered through the same literal column `T_kolumn` over all of 2019."""
    db = tmp_path / "decision.db"
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
                "INSERT INTO variable_state(state_id, variable_id,register_variant_id,"
                "valid_from,valid_to,delivery_column_name) "
                "VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), 910,902,'2019-01-01','2019-12-31','T_kolumn')"
            )
    return db


def _refused_mapping(tmp_path: Path, db: Path, decisions: str) -> str:
    """Run the `inventory` emitter over the decision's physical column; the refusal code."""
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, db, "T2019", ["Physical"], decisions=decisions)
    assert not (tmp_path / "steward/inventory.toml").exists()
    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert entry["column"] == "Physical"
    return entry["code"]


def test_mapping_decision_requires_complete_catalog_coverage(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = _decision_db(
        tmp_path, flavored_db, valid_to="2019-06-30", competing_owner=False
    )

    assert (
        _refused_mapping(tmp_path, db, _mapping_decision())
        == "stale_declared_mapping_target"
    )


def test_two_decisions_for_one_column_of_one_table_are_refused(
    tmp_path: Path, flavored_db: Path
) -> None:
    with pytest.raises(RegMetaError, match="second decision for table T2019"):
        _run_inventory(
            tmp_path,
            flavored_db,
            "T2019",
            ["Physical"],
            decisions=_mapping_decision() + "\n" + _mapping_decision(),
        )
    assert not (tmp_path / "steward/inventory.toml").exists()


def test_mapping_decision_does_not_hide_ambiguous_catalog_owners(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = _decision_db(
        tmp_path, flavored_db, valid_to="2019-12-31", competing_owner=True
    )

    assert (
        _refused_mapping(tmp_path, db, _mapping_decision())
        == "stale_declared_mapping_target"
    )


def test_mapping_decision_preserves_other_positive_owner_for_ambiguity_check(
    tmp_path: Path, flavored_db: Path
) -> None:
    decisions = _mapping_decision(
        column="T_kolumn",
        variable="inera/bestallda-prover/covid-19-antikroppar",
        representation="Covid_19_antikroppar",
    )
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(
            tmp_path, flavored_db, "T2019", ["T_kolumn"], decisions=decisions
        )
    assert (
        _inventory_worklist(tmp_path)["mapping_scope_needed"][0]["code"]
        == "ambiguous_delivered_owners"
    )


def test_mapping_decision_explicit_owner_selection_preserves_physical_column(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The competing owner spells the column as a case twin (`T_KOLUMN`), as
    neighbouring CIS waves spell `co11` and `CO11`; the folded lookup reaches
    both. Fails if owner selection drops only the exact spelling, leaving the
    twin's owner to make the column ambiguous (#1571)."""
    db = tmp_path / "competing-owner.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE variable_state SET delivery_column_name='T_KOLUMN' "
            "WHERE variable_id=903"
        )
    decisions = _mapping_decision(column="T_kolumn", select_owner=True)
    steward = _run_inventory(tmp_path, db, "T2019", ["T_kolumn"], decisions=decisions)
    column = load_delivery_inventory(steward / "inventory.toml").tables[0].columns[0]
    assert column.name == "T_kolumn"
    assert [str(m.variable) for m in column.mappings] == [
        "inera/bestallda-prover/t-kolumn"
    ]
    assert column.mappings[0].representation == "T_kolumn"


def test_owner_selection_drops_physical_name_owner_of_renamed_column(
    tmp_path: Path, flavored_db: Path
) -> None:
    """Physical `T_kolumn` is renamed to `Covid_19_antikroppar` and its owner
    selected, while `t-kolumn` delivers the physical name `T_kolumn` on the same
    variant. Over a range edition, whose owners stay unassessed, a surviving
    physical-name owner is written as a second mapping (over a single year it
    makes the column ambiguous instead). Fails if owner selection drops only the
    owners of the representation (#1664)."""
    decisions = _mapping_decision(
        column="T_kolumn",
        variable="inera/bestallda-prover/covid-19-antikroppar",
        representation="Covid_19_antikroppar",
        select_owner=True,
    )
    steward = _run_inventory(
        tmp_path,
        flavored_db,
        "TSERIES",
        ["T_kolumn"],
        edition=_RANGE,
        decisions=decisions,
    )
    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("inera/bestallda-prover/covid-19-antikroppar", "Covid_19_antikroppar")
    ]


def test_mapping_decision_owner_selection_requires_own_complete_coverage(
    tmp_path: Path, flavored_db: Path
) -> None:
    """Selecting an owner narrows the candidates to that owner, whose own
    windows must then cover the edition; another owner's coverage cannot lend
    it any."""
    db = _decision_db(
        tmp_path, flavored_db, valid_to="2019-06-30", competing_owner=True
    )

    assert (
        _refused_mapping(tmp_path, db, _mapping_decision(select_owner=True))
        == "stale_declared_mapping_target"
    )
