"""SWECOV generator: the delivery inventory stage (`cmd_inventory`).

Literal delivery columns resolve against a FLAVORED DB into `[[table.column.mapping]]`
entries, a series can re-spell a bare table year as the school year it holds, and
the scope's dispositions decide which census tables are placed at all. Assertions
read the emitted `inventory.toml` and the `derived/inventory_worklist.json` it
leaves. Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import tomllib
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    SPEC,
    build_catalog,
    column as _column,
    flavored_db_fixture,  # noqa: F401
    inventory_worklist as _inventory_worklist,
    one_table_series,
    run_flavor,
    run_inventory as _run_inventory,
    synthetic_enriched as _synthetic_enriched,
    write_spec,
)
from reg_meta_build.errors import RegMetaError
from reg_meta_build.inventory import (
    edition_bounds,
    load_inventory as load_delivery_inventory,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_inventory_maps_every_spelling_of_a_co_delivered_column(
    tmp_path: Path, flavored_db: Path
) -> None:
    """`cmd_inventory` over the synthetic `Ordered tests` holding: each literal
    delivery column gets a `[[table.column.mapping]]` naming its own
    representation, the holding leaves no unresolved residue, and every mapping
    still resolves against the flavored DB it was generated from."""
    columns = sorted(
        c["name"] for c in _synthetic_enriched()["Inera/1177/Ordered tests"]["columns"]
    )
    steward_dir = _run_inventory(tmp_path, flavored_db, "T", columns, edition="2021")

    emitted = tomllib.loads(
        (steward_dir / "inventory.toml").read_text(encoding="utf-8")
    )
    (table,) = emitted["table"]
    assert {
        column["name"]: [
            (m["variable"], m["representation"]) for m in column.get("mapping", ())
        ]
        for column in table["column"]
    } == {
        "Covid-19 antikroppar": [
            ("inera/bestallda-prover/covid-19-antikroppar", "Covid-19 antikroppar")
        ],
        "Covid_19_antikroppar": [
            ("inera/bestallda-prover/covid-19-antikroppar", "Covid_19_antikroppar")
        ],
        "T_kolumn": [("inera/bestallda-prover/t-kolumn", "T_kolumn")],
    }


def test_lone_suffixed_column_beside_its_base_is_a_reasoned_pivot_member(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A lone `T_kolumn1` beside the physical `T_kolumn` is a wide-pivot member of
    `T_kolumn`: unmapped, with the standard pivot reason, while `T_kolumn` keeps
    its direct mapping (the KSju `ersk`/`ersk1` shape, #1302 C5). Fails if the
    pivot count ignores the unsuffixed base column (the lone `T_kolumn1` falls to
    unresolved with no reason), if pivot members are emitted without
    `unmapped_reason`, if the base column is swept into the pivot, or if an
    unresolved column is missing from the worklist."""
    steward = _run_inventory(
        tmp_path,
        flavored_db,
        "T",
        ["T_kolumn", "T_kolumn1", "Okand_kolumn"],
        edition="2021",
    )
    (table,) = load_delivery_inventory(steward / "inventory.toml").tables
    _, base, member = table.columns
    assert [m.representation for m in base.mappings] == ["T_kolumn"]
    assert member.name == "T_kolumn1"
    assert member.mappings == ()
    assert member.unmapped_reason == (
        "Wide-pivot member of T_kolumn: the inventory has no cell shape for N"
        " physical columns serving one variable (#1495)."
    )
    assert _inventory_worklist(tmp_path)["unresolved"] == [
        {
            "table": "T",
            "column": "Okand_kolumn",
            "edition": 2021,
            "register_variants": ["inera/bestallda-prover/_default"],
        }
    ]


def test_curated_pivot_members_map_with_their_index(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A curated decision naming a `member` maps a wide-pivot member with its
    index (#1495): `T_kolumn`, `T_kolumn2` and `Covid_19_antikroppar` as
    positions 1-3 of `T_kolumn` in a 2020-2021 table, and `Emp2021` as the 2021
    member of `Emp`, which the catalog documents only in 2021. The member decision
    is the column's one mapping, so `Covid_19_antikroppar`, a literal the catalog
    documents under another variable, does not keep that auto-matched mapping. A
    year member serves only its year, so `Emp` need not cover 2020. Fails if the
    generator drops `member` (the `T_kolumn` mappings then conflict), keeps the
    auto-matched mapping beside a member (a member column with two mappings), both
    of which `load_inventory` refuses; if it checks a year member's owner against
    the whole edition (`Emp2021` goes to the scope worklist and the run exits); or
    if the spec refuses a curated member."""
    variant = "inera/bestallda-prover/_default"
    db = tmp_path / "candidate.db"
    shutil.copyfile(flavored_db, db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO variable(variable_id,register_id,provider_key,slug,name) "
            "VALUES (920,901,'emp','emp','Emp')"
        )
        conn.execute(
            "INSERT INTO variable_state(state_id,variable_id,register_variant_id,"
            "valid_from,valid_to,delivery_column_name) VALUES ((SELECT "
            "COALESCE(MAX(state_id), 0) + 1 FROM variable_state),920,902,"
            "'2021-01-01','2021-12-31','Emp')"
        )

    def member(column: str, variable: str, representation: str, index: str) -> str:
        return (
            f'[[column]]\ncolumn = "{column}"\n'
            f'mappings = [{{ register_variant = "{variant}", '
            f'variable = "inera/bestallda-prover/{variable}", '
            f'representation = "{representation}", member = {{ {index} }} }}]\n'
            'reason = "reviewed pivot member"\n\n'
        )

    decisions = (
        member("T_kolumn", "t-kolumn", "T_kolumn", "position = 1")
        + member("T_kolumn2", "t-kolumn", "T_kolumn", "position = 2")
        + member("Covid_19_antikroppar", "t-kolumn", "T_kolumn", "position = 3")
        + member("Emp2021", "emp", "Emp", "year = 2021")
    )

    steward = _run_inventory(
        tmp_path,
        db,
        "T",
        ["Covid_19_antikroppar", "Emp2021", "T_kolumn", "T_kolumn2"],
        edition="{ from = 2020, to = 2021 }",
        decisions=decisions,
    )

    (table,) = load_delivery_inventory(steward / "inventory.toml").tables
    assert {
        column.name: [
            (str(m.variable), m.representation, m.member.kind, m.member.index)
            for m in column.mappings
            if m.member is not None
        ]
        + [str(m.variable) for m in column.mappings if m.member is None]
        for column in table.columns
    } == {
        "Covid_19_antikroppar": [
            ("inera/bestallda-prover/t-kolumn", "T_kolumn", "position", 3)
        ],
        "Emp2021": [("inera/bestallda-prover/emp", "Emp", "year", 2021)],
        "T_kolumn": [("inera/bestallda-prover/t-kolumn", "T_kolumn", "position", 1)],
        "T_kolumn2": [("inera/bestallda-prover/t-kolumn", "T_kolumn", "position", 2)],
    }


# --- cmd_inventory: a series' school-year edition spelling --------------------


def _emitted_editions(steward_dir: Path) -> dict[str, object]:
    emitted = tomllib.loads(
        (steward_dir / "inventory.toml").read_text(encoding="utf-8")
    )
    return {table["id"]: table["edition"] for table in emitted["table"]}


@pytest.mark.parametrize(
    ("anchor", "edition", "bounds"),
    [
        (
            "spring",
            "LA2011",
            ("2011-07-01", "2012-06-30"),
        ),
        (
            "autumn",
            "LA2012",
            ("2012-07-01", "2013-06-30"),
        ),
        ("vt", "VT2012", ("2012-01-01", "2012-06-30")),
        ("ht", "HT2012", ("2012-07-01", "2012-12-31")),
    ],
)
def test_a_series_spelling_spells_a_table_year_as_the_school_year_it_holds(
    tmp_path: Path,
    flavored_db: Path,
    anchor: str,
    edition: object,
    bounds: tuple[str, str],
) -> None:
    """The series' `edition_spelling` decides which school year the table
    edition `2012` names — and the emitted spelling expands, through the shared
    period grammar every reader of the inventory uses, to exactly that window."""
    steward_dir = _run_inventory(
        tmp_path,
        flavored_db,
        "T2012",
        ["T_kolumn"],
        header=f'edition_spelling = "{anchor}"',
    )

    assert _emitted_editions(steward_dir) == {"T2012": edition}
    (loaded,) = load_delivery_inventory(steward_dir / "inventory.toml").tables
    assert edition_bounds(loaded.edition) == (bounds,)


def test_an_unknown_edition_spelling_is_refused(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The spelling set is the grammar: a misspelled one is a curation slip, not
    a table year to be re-spelled by guesswork."""
    with pytest.raises(RegMetaError, match="edition_spelling") as refused:
        _run_inventory(
            tmp_path,
            flavored_db,
            "T2012",
            ["T_kolumn"],
            header='edition_spelling = "winter"',
        )
    assert refused.value.code == "steward_spec_invalid"


def test_a_stage_refuses_a_census_table_the_spec_does_not_dispose_of(
    tmp_path: Path, flavored_db: Path
) -> None:
    """Every stage that reads the spec against a census holds it to the
    one-disposition rule, not only `inventory`: `ground` here. Fails if a stage
    loads the spec unchecked, which quietly gives the undisposed table no
    admissible variant. Its twin, a census whose every table is a series
    member, is every passing `run_inventory` case."""
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    csv_path.write_text(
        "Category,Detail,Table\n"
        "Inera/1177,Ordered tests,T2012,T_kolumn\n"
        "Inera/1177,Ordered tests,T2013,T_kolumn\n",
        encoding="utf-8",
    )
    spec = write_spec(tmp_path / "steward", csv_path, {"s": one_table_series("T2012")})
    with pytest.raises(RegMetaError, match="T2013") as refused:
        build_catalog.cmd_ground(
            argparse.Namespace(csv=csv_path, spec=spec, db=flavored_db)
        )
    assert refused.value.code == "steward_table_disposition_invalid"


# --- cmd_inventory: scope dispositions ---------------------------------------


def test_inventory_retains_guarded_undated_tables_separately_from_lookups(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A `[[retain_unknown]]` table never gets a guessed edition: it is listed
    apart in the worklist with its census columns, a `[[lookup]]` table is
    skipped, and only the series member is emitted. Fails if a retained or
    lookup table is emitted, or if the retained table's row guard is not
    checked against the census."""
    from reg_meta_build.source_evidence import canonical_sha256

    source = tmp_path / "holdings.csv"
    source.write_text(
        "Category,Detail,Table,V1,V2\n"
        "Inera/1177,Ordered tests,Known_2020,T_kolumn,\n"
        "Inera/1177,Ordered tests,Undated,T_kolumn,Unknown\n"
        "Inera/1177,Ordered tests,Lookup,Code,Name\n"
    )
    rows = [
        {
            "line": 3,
            "cells": ["Inera/1177", "Ordered tests", "Undated", "T_kolumn", "Unknown"],
        }
    ]
    steward = write_spec(
        tmp_path / "steward",
        source,
        {"s": one_table_series("Known_2020")},
        scope=(
            '[[lookup]]\ntable = "Lookup"\nreason = "Code/name dictionary."\n'
            '[[retain_unknown]]\ntable = "Undated"\nregister = "inera/bestallda-prover"\n'
            f'rows_sha256 = "{canonical_sha256(rows)}"\nreason = "No calendar coverage."\n'
        ),
    )
    build_catalog.write_inventory(
        argparse.Namespace(csv=source, spec=steward, out=steward), flavored_db
    )
    inventory = load_delivery_inventory(steward / "inventory.toml")
    assert {table.id: table.edition for table in inventory.tables} == {
        "Known_2020": "2020"
    }
    (retained,) = _inventory_worklist(tmp_path)["retained_unknown"]
    assert (retained["table"], retained["columns"]) == (
        "Undated",
        ["T_kolumn", "Unknown"],
    )


# --- cmd_inventory: a year-independent table (#1538) --------------------------

_INDEPENDENT_HOLDING = ("RTB", "Födelseuppgifter")
_INDEPENDENT_VARIANT = "swecov/population/fodelseland-grupperat"
(_INDEPENDENT_TABLE,) = next(
    flavor.tables
    for flavor in SPEC.scope.flavors
    if flavor.coordinate == _INDEPENDENT_VARIANT
)
_YEAR_INDEPENDENT = 'period_scope = "year_independent"'
# Three punctuations of one column, co-delivered in the year-independent table:
# a dated variant would group them as one state with aliases. All three kebab to
# `birth-country`, so the flavor keys them in sorted physical-name order (space <
# `-` < `_`): the bare key, then `-2`, then `-3`.
_INDEPENDENT_TWINS = ["Birth country", "Birth-country", "Birth_country"]


@pytest.fixture(scope="module")
def independent_flavored_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The committed policy's year-independent flavor table, through the real
    path: `cmd_flavor` (with three punctuation twins added to that table) then
    `extend_db` over a reader base artifact. Returns the flavored DB."""
    from reader_artifacts import FIXTURE_IMPORT_DATE, build_reader_artifact
    from reg_meta_build.extend_db import extend_db

    tmp_path = tmp_path_factory.mktemp("independent")
    enriched = _synthetic_enriched()
    holding = enriched["/".join(_INDEPENDENT_HOLDING)]
    holding["columns"] += [_column(name) for name in _INDEPENDENT_TWINS]
    holding["table_columns"][_INDEPENDENT_TABLE] += _INDEPENDENT_TWINS
    root = run_flavor(tmp_path / "flavor", enriched)
    base_db = build_reader_artifact(
        tmp_path / "base",
        "reader/classification-editions",
        "catalog",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )
    extend_db(
        base_db=base_db,
        providers_dir=root / "providers",
        db_dir=tmp_path / "flavored",
        steward="swecov",
        slug_dir=root.parents[1] / "fqid_slugs" / "swecov",
        diagnostic=True,
    )
    return tmp_path / "flavored" / "reg_meta.db"


def _independent_inventory(
    tmp_path: Path, db: Path, columns: list[str], **series: str
) -> Path:
    return _run_inventory(
        tmp_path,
        db,
        _INDEPENDENT_TABLE,
        columns,
        holding=_INDEPENDENT_HOLDING,
        variants=(_INDEPENDENT_VARIANT,),
        **series,
    )


def test_year_independent_flavor_table_maps_as_year_independent_holdings(
    tmp_path: Path, independent_flavored_db: Path
) -> None:
    """`cmd_flavor` emits the year-independent variant's states with no calendar
    window, each co-delivered spelling as its own variable, and a series
    member with `period_scope = "year_independent"` places the table as a year-independent holding whose
    every column maps to its own literal and is served by the catalog (no
    coverage miss). Fails if the flavor dates the states
    0001-01-01..9999-12-31 (the `_default` table then has no year-independent
    owner and is refused), if it groups the twins as aliases (extension refuses
    the provider TOML, `curated_toml_invalid`), if a third colliding spelling
    reuses a key or keys out of name order, if `cmd_inventory` cannot place a
    year-independent member or emits it without its `period_scope`, or if extension writes
    the state so the catalog does not serve it."""
    from contextlib import closing

    from reg_meta_build.holdings_compile import canonical_inventory
    from reg_meta_build.inventory_coverage import coverage_misses

    table = _INDEPENDENT_TABLE
    steward = _independent_inventory(
        tmp_path,
        independent_flavored_db,
        [f"{table}_kolumn", *_INDEPENDENT_TWINS],
        edition=_YEAR_INDEPENDENT,
    )

    (emitted,) = tomllib.loads(
        (steward / "inventory.toml").read_text(encoding="utf-8")
    )["table"]
    assert (emitted["edition"], emitted["period_scope"]) == (
        "_default",
        "year_independent",
    )
    assert {
        column["name"]: [
            (m["register_variant"], m["representation"]) for m in column["mapping"]
        ]
        for column in emitted["column"]
    } == {
        name: [("swecov/population/fodelseland-grupperat", name)]
        for name in (f"{table}_kolumn", *_INDEPENDENT_TWINS)
    }
    assert _inventory_worklist(tmp_path)["retained_unknown"] == []
    inventory = load_delivery_inventory(steward / "inventory.toml")
    with closing(sqlite3.connect(independent_flavored_db)) as conn:
        report = coverage_misses(conn, inventory)
        canonical_inventory(conn, inventory)
        # Each spelling maps to its own variable, keyed by its ordinal.
        provider_key = {
            column["name"]: conn.execute(
                "SELECT v.provider_key FROM variable v"
                " JOIN register r USING (register_id)"
                " JOIN provider p USING (provider_id)"
                " WHERE p.slug || '/' || r.slug || '/' || v.slug = ?",
                (mapping["variable"],),
            ).fetchone()[0]
            for column in emitted["column"]
            for mapping in column["mapping"]
            if column["name"] in _INDEPENDENT_TWINS
        }
    assert provider_key == {
        "Birth country": "birth-country",
        "Birth-country": "birth-country-2",
        "Birth_country": "birth-country-3",
    }
    assert (report.pairs, report.misses) == (4, ())


def test_range_edition_never_admits_a_year_independent_state(
    tmp_path: Path, independent_flavored_db: Path
) -> None:
    """A finite range edition over the year-independent table finds no dated
    owner: the column is refused as `no_covering_delivery_owner` and the
    inventory is not replaced. Fails if an unassessed range/list returns its
    records before the period-scope filter, which maps the column to a
    year-independent state that dated coverage then misses."""
    table = _INDEPENDENT_TABLE
    with pytest.raises(SystemExit, match="Inventory not replaced"):
        _independent_inventory(
            tmp_path,
            independent_flavored_db,
            [f"{table}_kolumn"],
            edition="{ from = 2019, to = 2020 }",
        )
    (entry,) = _inventory_worklist(tmp_path)["mapping_scope_needed"]
    assert (entry["column"], entry["code"]) == (
        f"{table}_kolumn",
        "no_covering_delivery_owner",
    )


def test_reviewed_mapping_maps_a_year_independent_member(
    tmp_path: Path, independent_flavored_db: Path
) -> None:
    """A reviewed mapping decision maps a physical column with no catalog
    literal, in a year-independent member, to a year-independent
    representation. Fails if a mapping decision is checked only against finite
    editions, which refuses the column as stale."""
    table = _INDEPENDENT_TABLE
    variable = "swecov/population/swecov-fodelseland-grupperat-kolumn"
    decisions = (
        '[[column]]\ncolumn = "Physical"\n'
        f'mappings = [{{ register_variant = "{_INDEPENDENT_VARIANT}", '
        f'variable = "{variable}", representation = "{table}_kolumn" }}]\n'
        'reason = "Exact reviewed source coordinate"\n'
    )
    steward = _independent_inventory(
        tmp_path,
        independent_flavored_db,
        ["Physical"],
        edition=_YEAR_INDEPENDENT,
        decisions=decisions,
    )
    (emitted,) = load_delivery_inventory(steward / "inventory.toml").tables
    (physical,) = emitted.columns
    assert [(str(m.variable), m.representation) for m in physical.mappings] == [
        (variable, f"{table}_kolumn")
    ]
