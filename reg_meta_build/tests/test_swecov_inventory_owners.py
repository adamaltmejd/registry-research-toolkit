"""SWECOV generator: which delivery owner `cmd_inventory` maps a literal column to.

A column maps to the catalog variable(s) whose resolved delivery windows cover the
table's whole edition; distinct owners covering it together are ambiguous, and a
column no owner covers is refused into `derived/inventory_worklist.json` with the
records it resolved. Which windows a column resolves to is read back from that
refusal. Which spellings and windows a column resolves to:
`test_swecov_inventory_representations`. Fixture provenance and the generator's
boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import json
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    FLAVORED_TOML,
    alias_window,
    build_catalog,
    global_alias,
    global_state,
    global_variable,
    inventory_worklist as _inventory_worklist,
    one_table_series,
    owner_block,
    run_inventory as _run_inventory,
    t_kolumn_window,
    write_flavored_db,
    write_global_db,
    write_spec,
)
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


def _owner_db(tmp_path: Path, owners: dict[str, list[tuple[str, str] | None]]) -> Path:
    """The flavored DB plus one variable per owner slug, delivered through the
    literal column `Column` in each listed window; `[None]` is one
    year-independent state, which makes `_default` a year-independent variant
    (the fixture's other states have no window, so they become year-independent
    too)."""
    source = FLAVORED_TOML + "".join(
        owner_block(owner, "Column", *(w for w in windows if w is not None))
        for owner, windows in owners.items()
    )
    if None in (w for windows in owners.values() for w in windows):
        # A year-independent state carries no co-delivered aliases.
        source = source.replace(
            '  key = "_default"\n',
            '  key = "_default"\n  period_scope = "year_independent"\n',
            1,
        ).replace('    aliases = ["Covid_19_antikroppar"]\n', "", 1)
    return write_flavored_db(tmp_path / "owners", source)


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
    db = _owner_db(tmp_path, owners)

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


def test_inventory_carries_a_key_only_from_an_owner_delivering_inside_the_edition(
    tmp_path: Path,
) -> None:
    """A carried key serves no cell, so an owner covering half of 2019 still
    yields it against T2019; an owner delivering only in 2018 is stale. Fails if
    the generator applies served-cell coverage to a carried key (it refused
    Fodelseuppg's person key against RTB's annual states), or if a carried key
    admits its owner whatever the period. T_kolumn is the served cell."""
    part, miss = tmp_path / "part", tmp_path / "miss"
    part.mkdir()
    miss.mkdir()
    db = _owner_db(part, {"one": [("2019-01-01", "2019-06-30")]})
    steward = _run_inventory(
        part, db, "T2019", ["Column", "T_kolumn"], decisions=_CARRIED
    )
    column = load_delivery_inventory(steward / "inventory.toml").tables[0].columns[0]
    assert [(str(m.variable), m.carried) for m in column.mappings] == [
        ("inera/bestallda-prover/one", True)
    ]

    db = _owner_db(miss, {"one": [("2018-01-01", "2018-12-31")]})
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _run_inventory(miss, db, "T2019", ["Column", "T_kolumn"], decisions=_CARRIED)
    (entry,) = _inventory_worklist(miss)["mapping_scope_needed"]
    assert entry["code"] == "stale_declared_mapping_target"


def test_inventory_accepts_abutting_delivery_windows_of_the_same_owner(
    tmp_path: Path,
) -> None:
    db = _owner_db(
        tmp_path,
        {"one": [("2019-01-01", "2019-06-30"), ("2019-07-01", "2019-12-31")]},
    )

    steward = _run_inventory(tmp_path, db, "T2019", ["Column"])

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("inera/bestallda-prover/one", "Column")
    ]
    assert _inventory_worklist(tmp_path)["mapping_scope_needed"] == []


def test_inventory_does_not_choose_an_owner_from_a_pooled_range(tmp_path: Path) -> None:
    """A range edition is record coverage, not per-column availability: every
    owner the range touches is mapped and none is chosen by its window."""
    db = _owner_db(
        tmp_path,
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
    lopnr = [
        global_state(
            "Lopnr", "2002-01-01", "9999-12-31" if beside in _MIXED else "2018-09-30"
        ),
        global_state("Lopnr", successor_from, variant="efter"),
    ]
    if beside in _THIRD_VARIANT:
        lopnr.append(
            global_state(
                "Lopnr",
                None if beside == "independent" else "1990-01-01",
                "2001-12-31" if beside == "historical" else "9999-12-31",
                variant="tidig",
            )
        )
    # Co-delivered spellings: each alias window, the state's own included.
    aliases = (
        (
            global_alias("Lopnr", alias_window("2002-01-01", "2018-09-30")),
            global_alias("LOPNR", alias_window("2018-01-01", "2018-06-30")),
        )
        if beside == "alias"
        else ()
    )
    others = []
    if beside in _COMPETITOR:
        start, end, column = _COMPETITOR[beside]
        others.append(
            global_variable("annan", global_state(column, start, end), name="Annan")
        )
    db = write_global_db(
        tmp_path / "split",
        global_variable("lopnr", *lopnr, aliases=aliases, name="Lopnr"),
        *others,
    )
    variants = ("scb/bestallda-prover/_default", "scb/bestallda-prover/efter")
    # A third admissible variant the decision does not map.
    admissible = (
        (*variants, "scb/bestallda-prover/tidig")
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
            'variable = "scb/bestallda-prover/lopnr", representation = "Lopnr"'
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
        (variants[0], "scb/bestallda-prover/lopnr", windows[0]),
        (variants[1], "scb/bestallda-prover/lopnr", windows[1]),
    ]
