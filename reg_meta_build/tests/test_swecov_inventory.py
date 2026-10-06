"""SWECOV generator: the delivery inventory stage (`cmd_inventory`).

Literal delivery columns resolve against a FLAVORED DB into `[[table.column.mapping]]`
entries, a bare table year can be re-spelled as the school year it holds, and routing
decides which holdings are placed at all. Assertions read the emitted
`inventory.toml` and the `derived/inventory_worklist.json` it leaves. Fixture
provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import json
import tomllib
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    build_catalog,
    copied_layout,
    flavored_db_fixture,  # noqa: F401
    run_generator,
    run_inventory as _run_inventory,
    synthetic_enriched as _synthetic_enriched,
)
from reg_meta.inventory import edition_bounds, load_inventory as load_delivery_inventory

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
    steward_dir = _run_inventory(
        tmp_path,
        flavored_db,
        '[[edition]]\ntable = "T"\nedition = 2021\n',
        "T",
        columns,
    )

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


# --- cmd_inventory: per-register school-year editions ------------------------


def _emitted_editions(steward_dir: Path) -> dict[str, object]:
    emitted = tomllib.loads(
        (steward_dir / "inventory.toml").read_text(encoding="utf-8")
    )
    return {table["id"]: table["edition"] for table in emitted["table"]}


def _stale_entries(tmp_path: Path) -> list[str]:
    worklist = json.loads(
        (tmp_path / "derived" / "inventory_worklist.json").read_text(encoding="utf-8")
    )
    return worklist["stale_overlay_entries"]


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
def test_a_school_year_rule_spells_a_table_year_as_the_school_year_it_holds(
    tmp_path: Path,
    flavored_db: Path,
    capsys: pytest.CaptureFixture[str],
    anchor: str,
    edition: object,
    bounds: tuple[str, str],
) -> None:
    """The register's anchor decides which school year the table year `2012`
    names — and the emitted spelling expands, through the shared period
    grammar every reader of the inventory uses, to exactly that window."""
    steward_dir = _run_inventory(
        tmp_path,
        flavored_db,
        f'[[school_year]]\nregister = "inera/bestallda-prover"\nanchor = "{anchor}"\n',
        "T2012",
        ["T_kolumn"],
    )

    assert _emitted_editions(steward_dir) == {"T2012": edition}
    (loaded,) = load_delivery_inventory(steward_dir / "inventory.toml").tables
    assert edition_bounds(loaded.edition) == (bounds,)
    assert (
        f"  school_year inera/bestallda-prover ({anchor}): 1 tables re-spelled"
        in capsys.readouterr().out
    )


def test_a_curated_edition_wins_over_the_school_year_rule(
    tmp_path: Path, flavored_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    steward_dir = _run_inventory(
        tmp_path,
        flavored_db,
        '[[school_year]]\nregister = "inera/bestallda-prover"\nanchor = "spring"\n'
        '[[edition]]\ntable = "T2012"\nedition = 2012\n',
        "T2012",
        ["T_kolumn"],
    )

    assert _emitted_editions(steward_dir) == {"T2012": 2012}
    assert (
        "  school_year inera/bestallda-prover (spring): 0 tables re-spelled"
        in capsys.readouterr().out
    )


def test_a_table_two_school_year_rules_disagree_on_is_refused(
    tmp_path: Path, flavored_db: Path
) -> None:
    """Two registers' rules over one table make its school year ambiguous, so
    no rule applies: the table keeps its calendar year and is named for the
    maintainer, the way a stale overlay entry is."""
    steward_dir = _run_inventory(
        tmp_path,
        flavored_db,
        '[[school_year]]\nregister = "inera/bestallda-prover"\nanchor = "spring"\n'
        '[[school_year]]\nregister = "inera/samtal"\nanchor = "autumn"\n'
        '[[assign]]\ntable = "T2012"\nregister_variant = '
        '["inera/bestallda-prover/_default", "inera/samtal/_default"]\n',
        "T2012",
        ["T_kolumn"],
    )

    assert _emitted_editions(steward_dir) == {"T2012": 2012}
    assert _stale_entries(tmp_path) == [
        "T2012 (school_year rules disagree across inera/bestallda-prover, inera/samtal)"
    ]


def test_a_school_year_rule_no_table_maps_to_is_refused(
    tmp_path: Path, flavored_db: Path
) -> None:
    steward_dir = _run_inventory(
        tmp_path,
        flavored_db,
        '[[school_year]]\nregister = "scb/grundskola-ak9"\nanchor = "spring"\n',
        "T2012",
        ["T_kolumn"],
    )

    assert _emitted_editions(steward_dir) == {"T2012": 2012}
    assert _stale_entries(tmp_path) == [
        "scb/grundskola-ak9 (school_year spring, no emitted table maps to it)"
    ]


def test_an_unknown_school_year_anchor_is_refused(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The anchor set is the grammar: a misspelled one is a curation slip, not
    a table year to be re-spelled by guesswork."""
    with pytest.raises(SystemExit, match="anchor 'winter'"):
        _run_inventory(
            tmp_path,
            flavored_db,
            '[[school_year]]\nregister = "inera/bestallda-prover"\nanchor = "winter"\n',
            "T2012",
            ["T_kolumn"],
        )


# --- cmd_inventory: routing ----------------------------------------------------


def _policy_with_unmapped_route(category: str, detail: str, reason: str) -> str:
    """The committed source policy plus one reviewed `unmapped` route."""
    text = build_catalog.SOURCE_POLICY_PATH.read_text(encoding="utf-8")
    return (
        text.rstrip("\n")
        + "\n\n[[route]]\n"
        + f"category = {json.dumps(category)}\n"
        + f"detail = {json.dumps(detail)}\n"
        + 'status = "unmapped"\n'
        + f"unmapped_reason = {json.dumps(reason)}\n"
    )


@pytest.mark.parametrize("assigned", [False, True])
def test_inventory_unmapped_route_retains_holdings_without_inferred_assignment(
    tmp_path: Path, flavored_db: Path, assigned: bool
) -> None:
    """An `unmapped` route keeps its tables and columns in the inventory, each
    column carrying the route's reason, and never auto-assigns an owner: only an
    explicit `[[assign]]` places the table, after which its columns map."""
    reason = "Separate source variants do not establish this combined delivery's owner."
    generator = copied_layout(
        tmp_path / "layout",
        _policy_with_unmapped_route("Inera/1177", "Ordered tests", reason),
    )
    steward = tmp_path / "steward"
    steward.mkdir()
    (steward / "inventory_overlay.toml").write_text(
        '[[assign]]\ntable = "T2019"\nregister_variant = "inera/bestallda-prover/_default"\n'
        if assigned
        else "",
        encoding="utf-8",
    )
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    csv_path.write_text(
        "Category,Detail,Table\nInera/1177,Ordered tests,T2019,T_kolumn,Unknown\n",
        encoding="utf-8",
    )

    result = run_generator(
        generator,
        "--csv",
        str(csv_path),
        "--db",
        str(flavored_db),
        "inventory",
        "--out",
        str(steward),
    )

    assert result.returncode == 0, result.stderr
    table = load_delivery_inventory(steward / "inventory.toml").tables[0]
    assert table.id == "T2019" and table.edition == "2019"
    assert [column.name for column in table.columns] == ["T_kolumn", "Unknown"]
    worklist = json.loads((tmp_path / "derived/inventory_worklist.json").read_text())
    assert not worklist["assignment_needed"]
    if assigned:
        assert len(table.columns[0].mappings) == 1
    else:
        assert all(
            column.mappings == () and column.unmapped_reason == reason
            for column in table.columns
        )


def test_inventory_retains_guarded_undated_tables_separately_from_lookups(
    tmp_path: Path, flavored_db: Path
) -> None:
    import hashlib

    from reg_meta.inventory import load_inventory
    from reg_meta.source_evidence import canonical_sha256

    steward = tmp_path / "steward"
    steward.mkdir()
    (steward / "inventory_overlay.toml").write_text("")
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
    policy = tmp_path / "holdings_policy.toml"
    policy.write_text(
        f'source_sha256 = "{hashlib.sha256(source.read_bytes()).hexdigest()}"\n'
        '[[exclude]]\ntable = "Lookup"\nreason = "Code/name dictionary."\n'
        '[[retain_unknown]]\ntable = "Undated"\nregister = "inera/bestallda-prover"\n'
        f'rows_sha256 = "{canonical_sha256(rows)}"\nreason = "No calendar coverage."\n'
    )
    build_catalog.cmd_inventory(
        argparse.Namespace(
            csv=source, db=flavored_db, out=steward, holdings_policy=policy
        )
    )
    inventory = load_inventory(steward / "inventory.toml")
    assert [table.id for table in inventory.tables] == ["Known_2020"]
    worklist = json.loads((tmp_path / "derived/inventory_worklist.json").read_text())
    assert worklist["edition_needed"] == []
    assert worklist["retained_unknown"][0]["table"] == "Undated"
    assert worklist["retained_unknown"][0]["columns"] == ["T_kolumn", "Unknown"]
