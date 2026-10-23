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
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    FLAVORED_TOML,
    REGISTER_ID,
    T_KOLUMN_ID,
    VARIANT_ID,
    build_catalog,
    flavored_db_fixture,  # noqa: F401
    inventory_worklist as _inventory_worklist,
    one_table_series,
    owner_block,
    run_inventory as _run_inventory,
    t_kolumn_window,
    variant_block,
    write_flavored_db,
    write_spec,
)
from reg_meta_build.derive.states import derive_states
from reg_meta_build.inventory import load_inventory as load_delivery_inventory

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(("year", "expected"), [(2021, "before-bas"), (2022, "bas")])
def test_inventory_uses_the_declared_owner_covering_its_exact_year(
    tmp_path: Path, year: int, expected: str
) -> None:
    """One literal column, distinct reviewed edition owners."""
    db = write_flavored_db(
        tmp_path / "candidate",
        FLAVORED_TOML
        + owner_block("before-bas", "InstKod10", ("2014", "2021"))
        + owner_block("bas", "InstKod10", ("2022", "2023")),
    )
    steward = _run_inventory(tmp_path, db, f"T{year}", ["InstKod10"])
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
    with closing(sqlite3.connect(db, autocommit=True)) as conn:
        for variable_id, (owner, windows) in enumerate(owners.items(), start=910):
            conn.execute(
                "INSERT INTO variable(variable_id,register_id,provider_key,slug,name,"
                f"source_name) VALUES (?,{REGISTER_ID},?,?,?,?)",
                (variable_id, owner, owner, owner, owner),
            )
            for window in windows:
                start, end = window or (None, None)
                conn.execute(
                    "INSERT INTO variable_state(state_id, variable_id,register_variant_id,"
                    "period_scope,valid_from,valid_to,delivery_column_name) "
                    f"VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), ?,{VARIANT_ID},?,?,?,'Column')",
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
        _run_inventory(tmp_path, db, "T2019", ["Column"])

    assert not (tmp_path / "steward/inventory.toml").exists()
    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert (entry["column"], entry["code"]) == ("Column", issue)


_CARRIED = (
    '[[column]]\ncolumn = "Column"\n'
    'mappings = [{ register_variant = "inera/bestallda-prover/_default", '
    'variable = "inera/bestallda-prover/one", representation = "Column", '
    "carried = true }]\n"
    'reason = "the row key, carried"\n'
)


def test_inventory_maps_a_carried_key_whose_owner_covers_part_of_the_edition(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A carried key serves no cell, so the owner that would leave a served
    mapping stale (half of 2019 against T2019) still yields the carried mapping.
    Fails if the generator applies served-cell coverage to a carried key, which
    refused Fodelseuppg's person key against RTB's annual states."""
    db = _owner_db(tmp_path, flavored_db, {"one": [("2019-01-01", "2019-06-30")]})

    # T_kolumn is the served cell the carried key rides along with.
    steward = _run_inventory(
        tmp_path, db, "T2019", ["Column", "T_kolumn"], decisions=_CARRIED
    )

    column = load_delivery_inventory(steward / "inventory.toml").tables[0].columns[0]
    assert [(str(m.variable), m.carried) for m in column.mappings] == [
        ("inera/bestallda-prover/one", True)
    ]


def test_inventory_refuses_a_carried_key_whose_owner_misses_the_edition(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The carried twin's refusal: an owner delivering only outside the edition
    (2018 against T2019) is stale. Fails if a carried key admits any record of
    its owner, whatever its period."""
    db = _owner_db(tmp_path, flavored_db, {"one": [("2018-01-01", "2018-12-31")]})

    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(
            tmp_path, db, "T2019", ["Column", "T_kolumn"], decisions=_CARRIED
        )

    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert (entry["column"], entry["code"]) == (
        "Column",
        "stale_declared_mapping_target",
    )


def test_inventory_accepts_abutting_delivery_windows_of_the_same_owner(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = _owner_db(
        tmp_path,
        flavored_db,
        {"one": [("2019-01-01", "2019-06-30"), ("2019-07-01", "2019-12-31")]},
    )

    steward = _run_inventory(tmp_path, db, "T2019", ["Column"])

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
        tmp_path, db, "T_pooled", ["Column"], edition="{ from = 2019, to = 2020 }"
    )

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert {str(m.variable) for m in column.mappings} == {
        "inera/bestallda-prover/one",
        "inera/bestallda-prover/two",
    }
    assert _inventory_worklist(tmp_path)["mapping_scope_needed"] == []


def test_unavailable_inventory_owner_writes_worklist_without_replacing_inventory(
    tmp_path: Path,
) -> None:
    db = write_flavored_db(tmp_path / "candidate", t_kolumn_window(None, "2018-12-31"))
    csv = tmp_path / "holdings.csv"
    csv.write_text(
        "Category,Detail,Table,V1\nInera/1177,Ordered tests,T2019,T_kolumn\n"
    )
    steward = write_spec(tmp_path / "steward", csv, {"s": one_table_series("T2019")})
    previous = steward / "inventory.toml"
    previous.write_text("previous inspected inventory")
    with pytest.raises(
        SystemExit, match="no_covering|need positive delivery-owner scope"
    ):
        build_catalog.write_inventory(
            argparse.Namespace(csv=csv, spec=steward, out=steward), db
        )
    assert previous.read_text() == "previous inspected inventory"
    worklist = json.loads((steward / "derived/inventory_worklist.json").read_text())
    assert worklist["mapping_scope_needed"][0]["code"] == "no_covering_delivery_owner"
    assert (
        worklist["mapping_scope_needed"][0]["candidate_records"][0]["valid_to"]
        == "2018-12-31"
    )


def _resolved_records(tmp_path: Path, db: Path, columns: list[str]) -> list[dict]:
    """Every resolution record `cmd_inventory` holds for `columns`, read through a
    table of an edition (2017) that no window in these databases covers: each
    column that resolves is refused with its records as `candidate_records`, and
    a column that resolves to nothing goes to the unresolved worklist."""
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, db, "T2017", columns)
    worklist = _inventory_worklist(tmp_path)
    entries = worklist["mapping_scope_needed"]
    if not entries:
        assert [entry["column"] for entry in worklist["unresolved"]] == columns
        return []
    assert {entry["code"] for entry in entries} == {"no_covering_delivery_owner"}
    return [record for entry in entries for record in entry["candidate_records"]]


def test_inventory_dated_alias_intersects_states_even_if_literal_was_canonical(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = tmp_path / "alias-intersections.db"
    shutil.copyfile(flavored_db, db)
    conn = sqlite3.connect(db)
    conn.execute(f"DELETE FROM variable_state WHERE variable_id={T_KOLUMN_ID}")
    conn.executemany(
        "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
        "valid_from, valid_to, data_type, delivery_column_name) "
        f"VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), {T_KOLUMN_ID}, {VARIANT_ID}, ?, ?, 'varchar', ?)",
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
        f"VALUES ({T_KOLUMN_ID}, {VARIANT_ID}, ?, ?, ?, 'per_column')",
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
        _run_inventory(tmp_path, db, "T2020", ["Earlier"])

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
    with closing(sqlite3.connect(db, autocommit=True)) as conn:
        conn.execute(
            "UPDATE variable_state SET valid_from='2019-01-01', "
            f"valid_to='2019-12-31' WHERE variable_id={T_KOLUMN_ID}"
        )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to, provenance, column_metadata) "
            f"VALUES ({T_KOLUMN_ID},{VARIANT_ID},'Historic',?,?,?,?)",
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
    with closing(sqlite3.connect(db, autocommit=True)) as conn:
        conn.execute(
            "UPDATE variable_state SET valid_from='2019-01-01', "
            f"valid_to='2019-12-31' WHERE variable_id={T_KOLUMN_ID}"
        )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) "
            f"VALUES ({T_KOLUMN_ID},{VARIANT_ID},'Historic','2019-03-01','2019-08-31')"
        )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to, provenance) "
            f"VALUES ({T_KOLUMN_ID},{VARIANT_ID},'T_kolumn',?,?,?)",
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
    with closing(sqlite3.connect(db, autocommit=True)) as conn:
        conn.execute(
            "UPDATE variable_state SET valid_from='2019-01-01', "
            f"valid_to='2019-12-31' WHERE variable_id={T_KOLUMN_ID}"
        )
        if case == "year_independent":
            conn.execute(
                "UPDATE variable_state SET period_scope='year_independent', "
                f"valid_from=NULL,valid_to=NULL WHERE variable_id={T_KOLUMN_ID}"
            )
        if case in {"source_replacement", "year_independent"}:
            conn.execute(
                "INSERT INTO variable_alias_window (variable_id,register_variant_id, "
                "delivery_column_name,valid_from,valid_to) "
                f"VALUES ({T_KOLUMN_ID},{VARIANT_ID},'T_kolumn','2019-03-01','2019-08-31')"
            )
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id,register_variant_id, "
            "delivery_column_name,valid_from,valid_to,provenance) "
            f"VALUES ({T_KOLUMN_ID},{VARIANT_ID},'Historic','2019-03-01','2019-08-31',?)",
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
    with closing(sqlite3.connect(db, autocommit=True)) as conn:
        conn.executemany(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) "
            f"VALUES ({T_KOLUMN_ID}, {VARIANT_ID}, ?, '0001-01-01', '9999-12-31')",
            [(window,) for window in windows],
        )
        # The generator reads the derived `resolver_column` universe, as the
        # holdings compiler does, so re-derive it over the edited core rows.
        derive_states(conn)
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

    steward = _run_inventory(tmp_path, db, "T2019", [physical])

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("inera/bestallda-prover/t-kolumn", expected)
    ]


def test_inventory_curated_mapping_wins_among_case_only_twins(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A reviewed mapping decision's spelling outranks the physical literal:
    the fold never silently replaces a curated representation."""
    db = _case_twin_db(tmp_path, flavored_db, ("T_kolumn", "T_KOLUMN"))
    decisions = (
        '[[column]]\ncolumn = "T_KOLUMN"\n'
        'mappings = [{ register_variant = "inera/bestallda-prover/_default", '
        'variable = "inera/bestallda-prover/t-kolumn", representation = "T_kolumn" }]\n'
        'reason = "reviewed spelling"\n'
    )

    steward = _run_inventory(tmp_path, db, "T2019", ["T_KOLUMN"], decisions=decisions)

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("inera/bestallda-prover/t-kolumn", "T_kolumn")
    ]


@pytest.mark.parametrize("scope", ["variant", "owner"])
def test_inventory_does_not_fold_case_spellings_across_variants_or_owners(
    tmp_path: Path, scope: str
) -> None:
    """Spellings that differ only in case under different variants, or under
    different owners, are separate coordinates and each keep their mapping."""
    if scope == "variant":
        source = (
            FLAVORED_TOML.replace(
                '  key = "t-kolumn"\n',
                '  key = "t-kolumn"\n  variants = ["_default"]\n',
            )
            + variant_block("andra")
            + owner_block("t-kolumn", "T_KOLUMN", variants=("andra",), name="T_kolumn")
        )
        table = "T2019"
        series = {
            "variants": (
                "inera/bestallda-prover/_default",
                "inera/bestallda-prover/andra",
            )
        }
        expected = [
            ("inera/bestallda-prover/_default", "t-kolumn", "T_kolumn"),
            ("inera/bestallda-prover/andra", "t-kolumn", "T_KOLUMN"),
        ]
    else:
        source = FLAVORED_TOML + owner_block(
            "annan", "T_KOLUMN", ("2020", "2020"), name="Annan"
        )
        table = "T_pooled"
        series = {"edition": "{ from = 2019, to = 2020 }"}
        expected = [
            ("inera/bestallda-prover/_default", "annan", "T_KOLUMN"),
            ("inera/bestallda-prover/_default", "t-kolumn", "T_kolumn"),
        ]
    db = write_flavored_db(tmp_path / "case-scopes", source)

    steward = _run_inventory(tmp_path, db, table, ["T_KOLUMN"], **series)

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert (
        sorted(
            (m.register_variant, m.variable.rsplit("/", 1)[1], m.representation)
            for m in column.mappings
        )
        == expected
    )


def test_inventory_refuses_case_only_twins_without_physical_or_representative(
    tmp_path: Path, flavored_db: Path
) -> None:
    """With neither the physical literal nor the representative spelling among
    the twins, no spelling is chosen: the column goes to the worklist."""
    db = _case_twin_db(tmp_path, flavored_db, ("T_KOLUMN", "t_KOLUMN"))

    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(tmp_path, db, "T2019", ["t_kolumn"])

    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert (entry["column"], entry["code"]) == (
        "t_kolumn",
        "case_twin_without_physical_or_representative_literal",
    )


_THIRD_VARIANT = ("historical", "current", "independent")
# Another variable delivering on `_default` over these dates, in this column.
_COMPETITOR = {
    "competing": ("2018-01-01", "2018-12-31", "Lopnr"),
    "window-outside": ("2018-10-01", "2018-10-31", "Lopnr"),
    "window-inside": ("2018-09-01", "2018-10-31", "Physical"),
    "window-inside-renamed": ("2018-09-01", "2018-10-31", "Lopnr"),
    "plain-beside-window-competing": ("2018-02-01", "2018-03-31", "Lopnr"),
}
_NOV_DEC = {"from": "2018-11", "to": "2018-12"}
# Both variants deliver through 2018; one mapping is plain, the other windowed.
_MIXED = {
    "plain-beside-window": (None, _NOV_DEC),
    "plain-beside-window-competing": (None, _NOV_DEC),
}


@pytest.mark.parametrize(
    ("successor_from", "beside", "refusal"),
    [
        ("2018-10-01", None, None),
        ("2018-10-01", "alias", None),
        ("2018-10-01", "competing", "ambiguous_delivered_owners"),
        ("2018-10-01", "historical", None),
        ("2018-10-01", "independent", None),
        ("2018-10-01", "current", "ambiguous_delivered_owners"),
        ("2018-11-01", None, "union_mapping_gap"),
        ("2018-11-01", "windows", None),
        ("2018-11-01", "window-outside", None),
        ("2018-11-01", "window-inside", "ambiguous_delivered_owners"),
        ("2018-11-01", "window-inside-renamed", "ambiguous_delivered_owners"),
        ("2018-01-01", "plain-beside-window", None),
        (
            "2018-01-01",
            "plain-beside-window-competing",
            "ambiguous_delivered_owners",
        ),
        ("2018-11-01", "wide-window", "stale_declared_mapping_target"),
        ("2018-06-01", None, "union_mapping_overlap"),
    ],
)
def test_inventory_maps_a_column_by_the_union_of_split_variants(
    tmp_path: Path,
    flavored_db: Path,
    successor_from: str,
    beside: str | None,
    refusal: str | None,
) -> None:
    """A column whose 2018 edition straddles a variant split (`_default` to
    2018-09, `efter` from `successor_from`) maps once per variant when the two
    reviewed mappings, each clipped to the edition, tile it (#1555, the FK
    assistansersättning shape). A gap or an overlap between them goes to the scope
    worklist and the run exits. Beside the union on `_default`, a dated `LOPNR`
    spelling of the same owner is dropped (the union still maps), while another
    variable delivering the representation there through 2018 competes with it and
    is refused as ambiguous, even under another physical name (`Physical`, which
    the physical-column lookup misses). On a third admissible variant the decision
    does not map, an owner ending in 2001 or holding only a
    year-independent state is irrelevant (the union maps), while one
    still delivering in 2018 competes and is refused. Fails if a partial mapping
    is still refused alone (the tiled case exits as stale), if the whole-edition
    owner check later drops the union records (a single-year edition is the one
    that check assesses), if the leftover spelling, the historical owner or the
    year-independent one sinks the union, if a competing or current owner is admitted, or if the tiling check
    misses a gap or an overlap (those cases map). With a column `window` on each
    mapping (#1645), the same gap maps: each owner is checked against its own
    window only, the windows are emitted, and October 2018 is stated as not
    delivered; a window reaching October, before its owner starts, is refused.
    Another owner on `_default` competes with its January–September window only
    there: one delivering in October alone is irrelevant, one reaching September
    is refused, whether it delivers the physical name or the representation of a
    renamed column. A plain `_default` mapping beside a windowed `efter` one
    decides its own variant over the edition: with both variants delivering
    through 2018, both map and neither owner competes with the other, while
    another owner of the representation on `_default` in February–March competes
    with the plain mapping of the renamed column and is refused. Fails if a
    windowed mapping is checked against the edition or another mapping's window,
    if its coverage or competitor check is not clipped to its window, if its
    coverage check is skipped, if its window is not emitted, if the plain mapping
    beside a window is judged a competitor or dropped, or if the plain mapping's
    renamed-column competitor check is skipped or clipped to the other mapping's
    window."""
    db = tmp_path / "split.db"
    shutil.copyfile(flavored_db, db)
    with closing(sqlite3.connect(db, autocommit=True)) as conn:
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, "
            f"name, slug) VALUES (905, {REGISTER_ID}, 'Efter', 'efter')"
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, slug, "
            f"name, source_name) VALUES (920, {REGISTER_ID}, 'lopnr', 'lopnr', "
            "'Lopnr', 'Lopnr')"
        )
        for variant, start, end in (
            (
                VARIANT_ID,
                "2002-01-01",
                "9999-12-31" if beside in _MIXED else "2018-09-30",
            ),
            (905, successor_from, "9999-12-31"),
        ):
            conn.execute(
                "INSERT INTO variable_state (state_id, variable_id, "
                "register_variant_id, valid_from, valid_to, data_type, "
                "delivery_column_name) VALUES ((SELECT COALESCE(MAX(state_id), 0) "
                "+ 1 FROM variable_state), 920, ?, ?, ?, 'varchar', 'Lopnr')",
                (variant, start, end),
            )
        if beside == "alias":
            # Co-delivered spellings: each alias window, the state's own included.
            conn.executemany(
                f"INSERT INTO variable_alias VALUES (920, {VARIANT_ID}, ?)",
                [("Lopnr",), ("LOPNR",)],
            )
            conn.executemany(
                "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
                f"delivery_column_name, valid_from, valid_to) VALUES (920, {VARIANT_ID}, ?, ?, ?)",
                [
                    ("Lopnr", "2002-01-01", "2018-09-30"),
                    ("LOPNR", "2018-01-01", "2018-06-30"),
                ],
            )
        if beside in _THIRD_VARIANT:
            conn.execute(
                "INSERT INTO register_variant (register_variant_id, register_id, "
                f"name, slug) VALUES (906, {REGISTER_ID}, 'Tidig', 'tidig')"
            )
            conn.execute(
                "INSERT INTO variable_state (state_id, variable_id, "
                "register_variant_id, valid_from, valid_to, data_type, "
                "delivery_column_name) VALUES ((SELECT COALESCE(MAX(state_id), 0) "
                "+ 1 FROM variable_state), 920, 906, '1990-01-01', ?, 'varchar', "
                "'Lopnr')",
                ("2001-12-31" if beside == "historical" else "9999-12-31",),
            )
        if beside == "independent":
            conn.execute(
                "UPDATE variable_state SET period_scope='year_independent', "
                "valid_from=NULL, valid_to=NULL WHERE register_variant_id=906"
            )
        if beside in _COMPETITOR:
            conn.execute(
                "INSERT INTO variable (variable_id, register_id, provider_key, slug, "
                f"name, source_name) VALUES (921, {REGISTER_ID}, 'annan', 'annan', "
                "'Annan', 'Annan')"
            )
            conn.execute(
                "INSERT INTO variable_state (state_id, variable_id, "
                "register_variant_id, valid_from, valid_to, data_type, "
                "delivery_column_name) VALUES ((SELECT COALESCE(MAX(state_id), 0) "
                f"+ 1 FROM variable_state), 921, {VARIANT_ID}, ?, ?, 'varchar', ?)",
                _COMPETITOR[beside],
            )
    variants = ("inera/bestallda-prover/_default", "inera/bestallda-prover/efter")
    # A third admissible variant the decision does not map.
    admissible = (
        (*variants, "inera/bestallda-prover/tidig")
        if beside in _THIRD_VARIANT
        else variants
    )
    physical = (
        "Physical"
        if beside
        in {
            "competing",
            "window-inside",
            "window-inside-renamed",
            "plain-beside-window-competing",
        }
        else "Lopnr"
    )
    windows = (
        (
            {"from": "2018-01", "to": "2018-09"},
            {
                "from": "2018-10" if beside == "wide-window" else "2018-11",
                "to": "2018-12",
            },
        )
        if beside
        in {
            "windows",
            "wide-window",
            "window-outside",
            "window-inside",
            "window-inside-renamed",
        }
        else _MIXED.get(beside, (None, None))
    )
    decisions = (
        f'[[column]]\ncolumn = "{physical}"\nmappings = ['
        + ", ".join(
            f'{{ register_variant = "{variant}", '
            'variable = "inera/bestallda-prover/lopnr", representation = "Lopnr"'
            + (
                f', window = {{ from = "{window["from"]}", to = "{window["to"]}" }}'
                if window
                else ""
            )
            + " }"
            for variant, window in zip(variants, windows, strict=True)
        )
        + ']\nreason = "one key across the variant split"\n'
    )

    def run() -> Path:
        return _run_inventory(
            tmp_path,
            db,
            "T_split",
            [physical],
            edition="2018",
            variants=admissible,
            decisions=decisions,
        )

    if refusal is not None:
        with pytest.raises(SystemExit, match="Inventory not replaced"):
            run()
        (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
        assert (entry["column"], entry["code"]) == (physical, refusal)
        return
    (column,) = load_delivery_inventory(run() / "inventory.toml").tables[0].columns
    assert sorted(
        (
            m.register_variant,
            str(m.variable),
            m.window and {"from": m.window.from_, "to": m.window.to},
        )
        for m in column.mappings
    ) == [
        (variants[0], "inera/bestallda-prover/lopnr", windows[0]),
        (variants[1], "inera/bestallda-prover/lopnr", windows[1]),
    ]
