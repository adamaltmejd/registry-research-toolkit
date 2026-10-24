"""SWECOV generator: which spellings and windows `cmd_inventory` resolves a column to.

A column resolves to the delivery representations of the catalog's states and
alias windows: source windows replace or intersect a state's own window, curated
windows add to it, a window-less alias is search-only, and case-only twin spellings
fold to one mapping. Which windows a column resolves to is read back from the
refusal of an edition no window covers. The alias-window shapes are global-build
ones (`write_global_db`); the case scopes are steward TOML (`write_flavored_db`).
Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    FLAVORED_TOML,
    GLOBAL_VARIANT,
    alias_window,
    global_alias,
    global_state,
    global_variable,
    inventory_worklist,
    owner_block,
    run_inventory,
    variant_block,
    write_flavored_db,
    write_global_db,
)
from reg_meta_build.inventory import load_inventory as load_delivery_inventory

if TYPE_CHECKING:
    from pathlib import Path


def _inventory(
    tmp_path: Path, db: Path, table: str, columns: list[str], **series: str
) -> Path:
    """`run_inventory` over the global `Beställda prover`'s `_default` variant."""
    return run_inventory(
        tmp_path, db, table, columns, variants=(GLOBAL_VARIANT,), **series
    )


def _t_kolumn_db(tmp_path: Path, *states: dict, aliases: tuple[dict, ...]) -> Path:
    """The global `t-kolumn` (named `T_kolumn`) with `states` (by default one
    `T_kolumn` state over 2019) and `aliases`."""
    return write_global_db(
        tmp_path / "global",
        global_variable(
            "t-kolumn",
            *(states or (global_state("T_kolumn", "2019-01-01", "2019-12-31"),)),
            aliases=aliases,
            name="T_kolumn",
        ),
    )


def _resolved_records(tmp_path: Path, db: Path, columns: list[str]) -> list[dict]:
    """Every resolution record `cmd_inventory` holds for `columns`, read through a
    table of an edition (2017) that no window in these databases covers: each
    column that resolves is refused with its records as `candidate_records`, and
    a column that resolves to nothing goes to the unresolved worklist."""
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _inventory(tmp_path, db, "T2017", columns)
    worklist = inventory_worklist(tmp_path)
    entries = worklist["mapping_scope_needed"]
    if not entries:
        assert [entry["column"] for entry in worklist["unresolved"]] == columns
        return []
    assert {entry["code"] for entry in entries} == {"no_covering_delivery_owner"}
    return [record for entry in entries for record in entry["candidate_records"]]


def test_inventory_dated_alias_intersects_states_even_if_literal_was_canonical(
    tmp_path: Path,
) -> None:
    """The per-column source alias `Earlier` reaches into the 2020 `Current`
    state, which no `Current` source window backs. It ends in June: a window
    covering the whole 2020 state would need that backing window to be a valid
    artifact (`validate_built_db`). Fails if the alias is admitted on a state
    whose own column has no participating window (a 2020 candidate record), or
    if it resolves as one hull rather than per intersected state."""
    # The three `Current` states differ in data length, so the writer keeps them
    # apart instead of merging them into one 2019-2020 state.
    db = _t_kolumn_db(
        tmp_path,
        global_state("Earlier", "2018-01-01", "2018-12-31"),
        global_state("Current", "2019-01-01", "2019-06-30", data_length="1"),
        global_state("Current", "2019-07-01", "2019-12-31", data_length="2"),
        global_state("Current", "2020-01-01", "2020-12-31", data_length="3"),
        aliases=(
            global_alias(
                "Earlier",
                alias_window("2019-01-01", "2020-06-30", column_metadata="per_column"),
            ),
            global_alias(
                "Current",
                alias_window("2019-01-01", "2019-12-31", column_metadata="per_column"),
            ),
        ),
    )

    # No positive current-column backing in 2020: the spanning source alias
    # must not create a temporal hull into that year.
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _inventory(tmp_path, db, "T2020", ["Earlier"])

    (entry,) = inventory_worklist(tmp_path)["mapping_scope_needed"]
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
            "2018-07-01",
            "2020-06-30",
            [
                ("2018-07-01", "2018-12-31"),
                ("2019-01-01", "2019-12-31"),
                ("2020-01-01", "2020-06-30"),
            ],
        ),
    ],
)
def test_inventory_curated_alias_needs_no_base_column_window(
    tmp_path: Path,
    provenance: str | None,
    mode: str,
    start: str,
    end: str,
    expected: list[tuple[str, str]],
) -> None:
    """A curated (provenance-bearing) alias window resolves without a base-column
    window: a shared one only inside one state, a per-column one intersected
    with each state it reaches. A source window without its base column does not
    resolve. The per-column window stays inside the annual states, as a valid
    artifact's per-column window must (`validate_built_db`)."""
    # Distinct data lengths keep the three annual states apart.
    db = _t_kolumn_db(
        tmp_path,
        *(
            global_state(
                "T_kolumn", f"{year}-01-01", f"{year}-12-31", data_length=str(year)
            )
            for year in (2018, 2019, 2020)
        ),
        aliases=(
            global_alias(
                "Historic",
                alias_window(start, end, provenance=provenance, column_metadata=mode),
            ),
        ),
    )

    records = _resolved_records(tmp_path, db, ["Historic"])

    assert [(r["valid_from"], r["valid_to"]) for r in records] == expected


@pytest.mark.parametrize("curated_backing", [False, True])
def test_inventory_shared_alias_cannot_use_invalid_or_curated_source_backing(
    tmp_path: Path, curated_backing: bool
) -> None:
    backing = (
        alias_window("2019-01-01", "2019-12-31", provenance="curated base")
        if curated_backing
        else alias_window("2018-01-01", "2020-12-31")
    )
    db = _t_kolumn_db(
        tmp_path,
        aliases=(
            global_alias("Historic", alias_window("2019-03-01", "2019-08-31")),
            global_alias("T_kolumn", backing),
        ),
    )

    assert _resolved_records(tmp_path, db, ["Historic"]) == []


@pytest.mark.parametrize(
    "case",
    ["source_replacement", "curated_additive", "source_no_base"],
)
def test_inventory_column_windows_preserve_source_and_curated_scope(
    tmp_path: Path, case: str
) -> None:
    aliases = [
        global_alias(
            "Historic",
            alias_window(
                "2019-03-01",
                "2019-08-31",
                provenance="reviewed alias" if case == "curated_additive" else None,
            ),
        )
    ]
    if case == "source_replacement":
        aliases.append(
            global_alias("T_kolumn", alias_window("2019-03-01", "2019-08-31"))
        )
    db = _t_kolumn_db(tmp_path, aliases=tuple(aliases))
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


def _case_twin_db(tmp_path: Path, windows: tuple[str, ...]) -> Path:
    """The global `t-kolumn` (state spelling `T_kolumn`, open-ended) co-delivered
    under `windows`, spellings that differ only in case."""
    return _t_kolumn_db(
        tmp_path,
        global_state("T_kolumn"),
        aliases=tuple(
            global_alias(window, alias_window("0001-01-01", "9999-12-31"))
            for window in windows
        ),
    )


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
    tmp_path: Path, physical: str, expected: str
) -> None:
    """Case-only twins fold to one canonical triple at holdings compilation, so
    the generator emits one mapping: the physical literal when it is one of the
    twins, otherwise the representative (state) spelling (#1170)."""
    db = _case_twin_db(tmp_path, ("T_kolumn", "T_KOLUMN"))

    steward = _inventory(tmp_path, db, "T2019", [physical])

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("scb/bestallda-prover/t-kolumn", expected)
    ]


def test_inventory_never_maps_onto_a_search_only_alias(tmp_path: Path) -> None:
    """`T_KOLUMN` is a window-less alias of `t-kolumn`: search-only delivery-column
    history that no order fills. The physical `T_KOLUMN` still maps, by case, onto
    the state spelling `T_kolumn`. Fails if the resolver reads `variable_alias`
    flat: the search-only spelling becomes a case twin and the physical literal
    `T_KOLUMN` is emitted, a mapping no resolver-emitted delivery column backs."""
    db = _t_kolumn_db(
        tmp_path, global_state("T_kolumn"), aliases=(global_alias("T_KOLUMN"),)
    )

    steward = _inventory(tmp_path, db, "T2019", ["T_KOLUMN"])

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("scb/bestallda-prover/t-kolumn", "T_kolumn")
    ]


def test_inventory_curated_mapping_wins_among_case_only_twins(tmp_path: Path) -> None:
    """A reviewed mapping decision's spelling outranks the physical literal:
    the fold never silently replaces a curated representation."""
    db = _case_twin_db(tmp_path, ("T_kolumn", "T_KOLUMN"))
    decisions = (
        '[[column]]\ncolumn = "T_KOLUMN"\n'
        f'mappings = [{{ register_variant = "{GLOBAL_VARIANT}", '
        'variable = "scb/bestallda-prover/t-kolumn", representation = "T_kolumn" }]\n'
        'reason = "reviewed spelling"\n'
    )

    steward = _inventory(tmp_path, db, "T2019", ["T_KOLUMN"], decisions=decisions)

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert [(str(m.variable), m.representation) for m in column.mappings] == [
        ("scb/bestallda-prover/t-kolumn", "T_kolumn")
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

    steward = run_inventory(tmp_path, db, table, ["T_KOLUMN"], **series)

    (column,) = load_delivery_inventory(steward / "inventory.toml").tables[0].columns
    assert (
        sorted(
            (m.register_variant, m.variable.rsplit("/", 1)[1], m.representation)
            for m in column.mappings
        )
        == expected
    )


def test_inventory_refuses_case_only_twins_without_physical_or_representative(
    tmp_path: Path,
) -> None:
    """With neither the physical literal nor the representative spelling among
    the twins, no spelling is chosen: the column goes to the worklist."""
    db = _case_twin_db(tmp_path, ("T_KOLUMN", "t_KOLUMN"))

    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _inventory(tmp_path, db, "T2019", ["t_kolumn"])

    (entry,) = inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert (entry["column"], entry["code"]) == (
        "t_kolumn",
        "case_twin_without_physical_or_representative_literal",
    )
