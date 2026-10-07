"""SWECOV generator: which delivery owner `cmd_inventory` maps a literal column to.

A column maps to the catalog variable(s) whose resolved delivery windows cover the
table's whole edition; distinct owners covering it together are ambiguous, and a
column no owner covers is refused into `derived/inventory_worklist.json` with the
records it resolved. Which windows a column resolves to is read back from that
refusal. Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    build_catalog,
    flavored_db_fixture,  # noqa: F401
    inventory_worklist as _inventory_worklist,
    run_inventory as _run_inventory,
)
from reg_meta.inventory import load_inventory as load_delivery_inventory

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(("year", "expected"), [(2021, "before-bas"), (2022, "bas")])
def test_inventory_uses_the_declared_owner_covering_its_exact_year(
    tmp_path: Path, flavored_db: Path, year: int, expected: str
) -> None:
    """One literal column, distinct reviewed edition owners."""
    import shutil

    db = tmp_path / "candidate.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        for variable, owner, start, end in (
            (910, "before-bas", "2014-01-01", "2021-12-31"),
            (911, "bas", "2022-01-01", "2023-12-31"),
        ):
            conn.execute(
                "INSERT INTO variable(variable_id,register_id,provider_key,slug,name) "
                "VALUES (?,901,?,?,?)",
                (variable, owner, owner, owner),
            )
            conn.execute(
                "INSERT INTO variable_state(variable_id,register_variant_id,valid_from,"
                "valid_to,delivery_column_name) VALUES (?,902,?,?,'InstKod10')",
                (variable, start, end),
            )
    steward = _run_inventory(tmp_path, db, "", f"T{year}", ["InstKod10"])
    table = load_delivery_inventory(steward / "inventory.toml").tables[0]
    assert str(table.columns[0].mappings[0].variable) == (
        f"inera/bestallda-prover/{expected}"
    )
    assert len(table.columns[0].mappings) == 1


def _owner_db(
    tmp_path: Path, flavored_db: Path, owners: dict[str, list[tuple[str, str] | None]]
) -> Path:
    """A copy of the flavored DB adding one variable per owner slug, delivered
    through the literal column `Column` in each listed window; `None` is a
    year-independent state (no calendar window)."""
    db = tmp_path / "owners.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        for variable_id, (owner, windows) in enumerate(owners.items(), start=910):
            conn.execute(
                "INSERT INTO variable(variable_id,register_id,provider_key,slug,name) "
                "VALUES (?,901,?,?,?)",
                (variable_id, owner, owner, owner),
            )
            for window in windows:
                start, end = window or (None, None)
                conn.execute(
                    "INSERT INTO variable_state(variable_id,register_variant_id,"
                    "period_scope,valid_from,valid_to,delivery_column_name) "
                    "VALUES (?,902,?,?,?,'Column')",
                    (
                        variable_id,
                        "intervals" if window else "year_independent",
                        start,
                        end,
                    ),
                )
    return db


@pytest.mark.parametrize(
    ("owners", "issue"),
    [
        ({"one": [("2019-01-01", "2019-06-30")]}, "no_covering_delivery_owner"),
        (
            {
                "one": [("2019-01-01", "2019-12-31")],
                "two": [("2019-01-01", "2019-12-31")],
            },
            "ambiguous_delivered_owners",
        ),
        ({"one": [None]}, "no_covering_delivery_owner"),
    ],
    ids=["incomplete", "ambiguous", "year-independent"],
)
def test_inventory_refuses_incomplete_ambiguous_or_unknown_owner_scopes(
    tmp_path: Path,
    flavored_db: Path,
    owners: dict[str, list[tuple[str, str] | None]],
    issue: str,
) -> None:
    """An owner whose windows cover only part of the edition, and two owners
    covering it together, each leave the column unmapped: the inventory is not
    replaced and the worklist names why.

    The `year-independent` case pins CURRENT behaviour, not a settled rule: an
    owner whose only state has no calendar window is refused the same way,
    because owner choice reads interval windows only. Whether a year-independent
    state should map regardless of edition awaits the maintainer's ruling."""
    db = _owner_db(tmp_path, flavored_db, owners)

    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, db, "", "T2019", ["Column"])

    assert not (tmp_path / "steward/inventory.toml").exists()
    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert (entry["column"], entry["code"]) == ("Column", issue)


def test_inventory_accepts_abutting_delivery_windows_of_the_same_owner(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = _owner_db(
        tmp_path,
        flavored_db,
        {"one": [("2019-01-01", "2019-06-30"), ("2019-07-01", "2019-12-31")]},
    )

    steward = _run_inventory(tmp_path, db, "", "T2019", ["Column"])

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("inera/bestallda-prover/one", "Column")
    ]
    assert _inventory_worklist(tmp_path)["mapping_scope_needed"] == []


def test_inventory_does_not_choose_an_owner_from_a_pooled_range(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A range edition is record coverage, not per-column availability: every
    owner the range touches is mapped and none is chosen by its window."""
    db = _owner_db(
        tmp_path,
        flavored_db,
        {"one": [("2019-01-01", "2019-12-31")], "two": [("2020-01-01", "2020-12-31")]},
    )

    steward = _run_inventory(
        tmp_path,
        db,
        '[[edition]]\ntable = "T_pooled"\nedition = { from = 2019, to = 2020 }\n',
        "T_pooled",
        ["Column"],
    )

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert {str(m.variable) for m in column.mappings} == {
        "inera/bestallda-prover/one",
        "inera/bestallda-prover/two",
    }
    assert _inventory_worklist(tmp_path)["mapping_scope_needed"] == []


def test_unavailable_inventory_owner_writes_worklist_without_replacing_inventory(
    tmp_path: Path, flavored_db: Path
) -> None:
    import shutil

    db = tmp_path / "candidate.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE variable_state SET valid_to='2018-12-31'")
        conn.execute("UPDATE variable_alias_window SET valid_to='2018-12-31'")
    steward = tmp_path / "steward"
    steward.mkdir()
    (steward / "inventory_overlay.toml").write_text("")
    previous = steward / "inventory.toml"
    previous.write_text("previous inspected inventory")
    csv = tmp_path / "holdings.csv"
    csv.write_text(
        "Category,Detail,Table,V1\nInera/1177,Ordered tests,T2019,T_kolumn\n"
    )
    with pytest.raises(
        SystemExit, match="no_covering|need positive delivery-owner scope"
    ):
        build_catalog.cmd_inventory(argparse.Namespace(csv=csv, db=db, out=steward))
    assert previous.read_text() == "previous inspected inventory"
    worklist = json.loads((tmp_path / "derived/inventory_worklist.json").read_text())
    assert worklist["mapping_scope_needed"][0]["code"] == "no_covering_delivery_owner"
    assert (
        worklist["mapping_scope_needed"][0]["candidate_records"][0]["valid_to"]
        == "2018-12-31"
    )


def _resolved_records(tmp_path: Path, db: Path, columns: list[str]) -> list[dict]:
    """Every resolution record `cmd_inventory` holds for `columns`, read through a
    table of an edition (2017) that no window in these databases covers: each
    column that resolves is refused with its records as `candidate_records`, and
    a column that resolves to nothing is emitted unmapped and refuses nothing."""
    try:
        steward = _run_inventory(tmp_path, db, "", "T2017", columns)
    except SystemExit:
        entries = _inventory_worklist(tmp_path)["mapping_scope_needed"]
        assert {entry["code"] for entry in entries} == {"no_covering_delivery_owner"}
        return [record for entry in entries for record in entry["candidate_records"]]
    table = load_delivery_inventory(steward / "inventory.toml").tables[0]
    assert [column.mappings for column in table.columns] == [()] * len(columns)
    return []


def test_inventory_dated_alias_intersects_states_even_if_literal_was_canonical(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = tmp_path / "alias-intersections.db"
    shutil.copyfile(flavored_db, db)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM variable_state WHERE variable_id=904")
    conn.executemany(
        "INSERT INTO variable_state (variable_id, register_variant_id, "
        "valid_from, valid_to, data_type, delivery_column_name) "
        "VALUES (904, 902, ?, ?, 'varchar', ?)",
        [
            ("2018-01-01", "2018-12-31", "Earlier"),
            ("2019-01-01", "2019-06-30", "Current"),
            ("2019-07-01", "2019-12-31", "Current"),
            ("2020-01-01", "2020-12-31", "Current"),
        ],
    )
    conn.executemany(
        "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
        "delivery_column_name, valid_from, valid_to, column_metadata) "
        "VALUES (904, 902, ?, ?, ?, 'per_column')",
        [
            ("Earlier", "2019-01-01", "2020-12-31"),
            ("Current", "2019-01-01", "2019-12-31"),
        ],
    )
    conn.commit()
    conn.close()

    # No positive current-column backing in 2020: the spanning source alias
    # must not create a temporal hull into that year.
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, db, "", "T2020", ["Earlier"])

    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert entry["code"] == "no_covering_delivery_owner"
    assert [(r["valid_from"], r["valid_to"]) for r in entry["candidate_records"]] == [
        ("2018-01-01", "2018-12-31"),
        ("2019-01-01", "2019-06-30"),
        ("2019-07-01", "2019-12-31"),
    ]


@pytest.mark.parametrize(
    ("provenance", "mode", "start", "end", "expected"),
    [
        (
            "reviewed alias",
            "shared",
            "2019-03-01",
            "2019-08-31",
            [("2019-03-01", "2019-08-31")],
        ),
        (None, "shared", "2019-03-01", "2019-08-31", []),
        ("reviewed alias", "shared", "2018-01-01", "2020-12-31", []),
        (
            "reviewed alias",
            "per_column",
            "2018-01-01",
            "2020-12-31",
            [("2019-01-01", "2019-12-31")],
        ),
    ],
)
def test_inventory_curated_alias_needs_no_base_column_window(
    tmp_path: Path,
    flavored_db: Path,
    provenance: str | None,
    mode: str,
    start: str,
    end: str,
    expected: list[tuple[str, str]],
) -> None:
    db = tmp_path / "additive-alias.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE variable_state SET valid_from='2019-01-01', "
            "valid_to='2019-12-31' WHERE variable_id=904"
        )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to, provenance, column_metadata) "
            "VALUES (904,902,'Historic',?,?,?,?)",
            (start, end, provenance, mode),
        )

    records = _resolved_records(tmp_path, db, ["Historic"])

    assert [(r["valid_from"], r["valid_to"]) for r in records] == expected


@pytest.mark.parametrize("curated_backing", [False, True])
def test_inventory_shared_alias_cannot_use_invalid_or_curated_source_backing(
    tmp_path: Path,
    flavored_db: Path,
    curated_backing: bool,
) -> None:
    db = tmp_path / "source-alias-guards.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE variable_state SET valid_from='2019-01-01', "
            "valid_to='2019-12-31' WHERE variable_id=904"
        )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) "
            "VALUES (904,902,'Historic','2019-03-01','2019-08-31')"
        )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to, provenance) "
            "VALUES (904,902,'T_kolumn',?,?,?)",
            (
                "2019-01-01" if curated_backing else "2018-01-01",
                "2019-12-31" if curated_backing else "2020-12-31",
                "curated base" if curated_backing else None,
            ),
        )

    assert _resolved_records(tmp_path, db, ["Historic"]) == []


@pytest.mark.parametrize(
    "case",
    ["source_replacement", "curated_additive", "source_no_base", "year_independent"],
)
def test_inventory_column_windows_preserve_source_and_curated_scope(
    tmp_path: Path,
    flavored_db: Path,
    case: str,
) -> None:
    db = tmp_path / "public-resolution-parity.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE variable_state SET valid_from='2019-01-01', "
            "valid_to='2019-12-31' WHERE variable_id=904"
        )
        if case == "year_independent":
            conn.execute(
                "UPDATE variable_state SET period_scope='year_independent', "
                "valid_from=NULL,valid_to=NULL WHERE variable_id=904"
            )
        if case in {"source_replacement", "year_independent"}:
            conn.execute(
                "INSERT INTO variable_alias_window (variable_id,register_variant_id, "
                "delivery_column_name,valid_from,valid_to) "
                "VALUES (904,902,'T_kolumn','2019-03-01','2019-08-31')"
            )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id,register_variant_id, "
            "delivery_column_name,valid_from,valid_to,provenance) "
            "VALUES (904,902,'Historic','2019-03-01','2019-08-31',?)",
            ("reviewed alias" if case == "curated_additive" else None,),
        )
    expected = {
        "source_replacement": {
            ("T_kolumn", "2019-03-01", "2019-08-31", "intervals"),
            ("Historic", "2019-03-01", "2019-08-31", "intervals"),
        },
        "curated_additive": {
            ("T_kolumn", "2019-01-01", "2019-12-31", "intervals"),
            ("Historic", "2019-03-01", "2019-08-31", "intervals"),
        },
        "source_no_base": {("T_kolumn", "2019-01-01", "2019-12-31", "intervals")},
        "year_independent": {("T_kolumn", None, None, "year_independent")},
    }[case]

    records = _resolved_records(tmp_path, db, ["Historic", "T_kolumn"])

    actual = {
        (r["col"], r["valid_from"], r["valid_to"], r["period_scope"])
        for r in records
        if r["vslug"] == "t-kolumn"
    }
    assert actual == expected
    if case == "source_replacement":
        assert {r[1:3] for r in actual} == {("2019-03-01", "2019-08-31")}


def _case_twin_db(tmp_path: Path, flavored_db: Path, windows: tuple[str, ...]) -> Path:
    """A copy of the flavored DB where `t-kolumn` (state spelling `T_kolumn`) is
    co-delivered under `windows`, spellings that differ only in case."""
    db = tmp_path / "case-twins.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) "
            "VALUES (904, 902, ?, '0001-01-01', '9999-12-31')",
            [(window,) for window in windows],
        )
    return db


@pytest.mark.parametrize(
    ("physical", "expected"),
    [
        ("T_KOLUMN", "T_KOLUMN"),
        ("P1105_LopNr_T_KOLUMN", "T_KOLUMN"),
        ("t_kolumn", "T_kolumn"),
    ],
    ids=["physical-literal", "lopnr-physical-literal", "representative"],
)
def test_inventory_emits_one_mapping_for_case_only_twin_spellings(
    tmp_path: Path, flavored_db: Path, physical: str, expected: str
) -> None:
    """Case-only twins fold to one canonical triple at holdings compilation, so
    the generator emits one mapping: the physical literal when it is one of the
    twins, otherwise the representative (state) spelling (#1170)."""
    db = _case_twin_db(tmp_path, flavored_db, ("T_kolumn", "T_KOLUMN"))

    steward = _run_inventory(tmp_path, db, "", "T2019", [physical])

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("inera/bestallda-prover/t-kolumn", expected)
    ]


def test_inventory_refuses_case_only_twins_without_physical_or_representative(
    tmp_path: Path, flavored_db: Path
) -> None:
    """With neither the physical literal nor the representative spelling among
    the twins, no spelling is chosen: the column goes to the worklist."""
    db = _case_twin_db(tmp_path, flavored_db, ("T_KOLUMN", "t_KOLUMN"))

    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, db, "", "T2019", ["t_kolumn"])

    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert (entry["column"], entry["code"]) == (
        "t_kolumn",
        "case_twin_without_physical_or_representative_literal",
    )
