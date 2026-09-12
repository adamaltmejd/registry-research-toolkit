"""Tests for the SWECOV steward-flavor generator's variable and variant emission.

The generator (`input_data/swecov/build_catalog.py`) is tracked but maintainer-run:
its CSV/workbook inputs are confidential and stay untracked, so these tests feed it
SYNTHETIC columns and a synthetic `holdings_enriched.json` — no real delivery data,
no `derived/` artifact of the maintainer's, no network. Loaded by path (the way
`reg_webapp/backend/tests/conftest.py` reaches `scripts/fixture_db.py`) because
`input_data/` is a seed area, not an importable package.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
import tomllib
from collections import defaultdict
from pathlib import Path

import pytest
from reg_meta.db import open_db
from reg_meta.inventory import edition_bounds, load_inventory as load_delivery_inventory
from reg_meta.inventory_check import check_inventory, unresolved_message
from reg_meta_build.extend_db import load_inventory

_GENERATOR = (
    Path(__file__).resolve().parents[1] / "input_data" / "swecov" / "build_catalog.py"
)
_spec = importlib.util.spec_from_file_location("swecov_build_catalog", _GENERATOR)
assert _spec and _spec.loader
build_catalog = importlib.util.module_from_spec(_spec)
# Registered before exec: the module defines `@dataclass`es, which resolve their
# annotations through `sys.modules[cls.__module__]`.
sys.modules[_spec.name] = build_catalog
_spec.loader.exec_module(build_catalog)


def _column(name: str, **extra: str) -> dict:
    """An enriched-holding column entry (see `cmd_enrich`)."""
    return {"name": name, "normalized": build_catalog.norm_col(name), **extra}


# --- _flavor_variables: vintage-spelling grouping ----------------------------


def test_spelling_variants_become_one_state_carrying_the_other_columns() -> None:
    """The two Covid spellings in Inera's `Ordered tests` delivery.

    They differ only in punctuation, so they are ONE steward variable — and they
    are CO-DELIVERED, so they are ONE state whose `aliases` carry the other
    literal column (extend-db gives each its own `variable_alias` + window row,
    keeping it orderable: steward README → "Near-duplicate physical columns").
    """
    variables = build_catalog._flavor_variables(
        [
            _column("Covid-19 antikroppar", data_type="varchar", description="IgG"),
            _column("Covid_19_antikroppar"),
            _column("P1105_LopNr_PERSONNR"),
            _column("Provdatum"),
        ]
    )

    assert [v["key"] for v in variables] == [
        "covid-19-antikroppar",
        "personnr",
        "provdatum",
    ]
    # No physical column disappears: the pseudonym prefix is an order-template
    # artifact, so `PERSONNR` is the column and the variable is an identifier.
    assert {
        column
        for v in variables
        for s in v["states"]
        for column in (s["column"], *s.get("aliases", ()))
    } == {
        "Covid-19 antikroppar",
        "Covid_19_antikroppar",
        "PERSONNR",
        "Provdatum",
    }
    assert [v["is_identifier"] for v in variables] == [False, True, False]
    covid = variables[0]
    # Key, name, description and data_type come from the first-seen spelling.
    assert covid["name"] == "Covid-19 antikroppar"
    assert covid["description"] == "IgG"
    # ONE state: the spellings are co-delivered, so the other one is an alias —
    # not a second state needing a fake `value_set_version_label` to survive
    # extend-db's (valid_from, value_set_version_label) uniqueness key.
    assert len(covid["states"]) == 1
    assert covid["states"][0]["column"] == "Covid-19 antikroppar"
    assert covid["states"][0]["aliases"] == ["Covid_19_antikroppar"]
    assert "value_set_version_label" not in covid["states"][0]
    assert covid["states"][0]["data_type"] == "varchar"
    # A single-spelling variable stays an undiscriminated single state.
    assert variables[2]["states"] == [
        {
            "column": "Provdatum",
            "data_type": None,
            "valid_from": None,
            "valid_to": None,
        }
    ]


def test_columns_differing_in_letters_stay_separate_variables() -> None:
    """The fold groups punctuation/case/diacritics only — not a plural `S`."""
    variables = build_catalog._flavor_variables(
        [_column("AVERAGE_SPENDING"), _column("AVERAGE_SPENDINGS")]
    )

    assert [v["key"] for v in variables] == ["average-spending", "average-spendings"]
    assert [len(v["states"]) for v in variables] == [1, 1]


def test_kalla_routes_canonical_columns_out_but_keeps_provenance_columns() -> None:
    variables = build_catalog._flavor_variables(
        [
            _column("Konstruerad"),
            _column("Sysselsattning", kalla="LISA"),
            _column("Stodbelopp", kalla="Inrapporterat från Tillväxtverket"),
        ]
    )

    assert [v["name"] for v in variables] == ["Konstruerad", "Stodbelopp"]


# --- cmd_flavor: the disposition's register/variant emission -----------------


def _synthetic_enriched() -> dict[str, dict]:
    """One synthetic holding per `_FLAVOR_DISPOSITION` key.

    Each physical table a table selector names gets one column of its own, so
    every disposition entry draws at least one steward-only variable; keys with
    no selector get a single-column holding.
    """
    tables: dict[str, dict[str, list[str]]] = defaultdict(dict)
    for key, prov_slug, _, reg_key, _, var_key, *_ in build_catalog._FLAVOR_DISPOSITION:
        selector = build_catalog._FLAVOR_VARIANT_TABLES.get(
            (prov_slug, reg_key, var_key)
        )
        for table in sorted(selector or ("T",)):
            tables[key][table] = [f"{table}_kolumn"]

    enriched = {
        key: {
            "columns": [_column(c) for cs in table_columns.values() for c in cs],
            "table_columns": table_columns,
        }
        for key, table_columns in tables.items()
    }
    # The vintage-spelling pair, in the holding it really occurs in.
    enriched["Inera/1177/Ordered tests"]["columns"] += [
        _column("Covid-19 antikroppar"),
        _column("Covid_19_antikroppar"),
    ]
    return enriched


def _run_flavor(tmp_path: Path, enriched: dict[str, dict]) -> Path:
    """Run `cmd_flavor` over synthetic holdings; return its inventory JSON path."""
    derived = tmp_path / "derived"
    derived.mkdir()
    (derived / "holdings_enriched.json").write_text(
        json.dumps(enriched), encoding="utf-8"
    )
    build_catalog.cmd_flavor(
        argparse.Namespace(csv=tmp_path / "SWECOV_variables_2025-12-11.csv")
    )
    return derived / "flavor_inventory.json"


@pytest.fixture(scope="module")
def flavor_inventory_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One generator run for every test that reads its inventory."""
    return _run_flavor(tmp_path_factory.mktemp("flavor"), _synthetic_enriched())


@pytest.fixture(scope="module")
def flavor_inventory(flavor_inventory_path: Path) -> dict:
    return json.loads(flavor_inventory_path.read_text(encoding="utf-8"))


def _variant_names(inventory: dict, provider: str, register: str) -> dict[str, str]:
    reg = next(
        r
        for r in inventory["registers"]
        if r["provider"] == provider and r["key"] == register
    )
    return {v["key"]: v["name"] for v in reg["variants"]}


def test_a_named_variant_carries_its_own_name(flavor_inventory: dict) -> None:
    assert _variant_names(flavor_inventory, "skatteverket", "tillfalligt-anstand") == {
        "ansokt": "Ansökt",
        "beviljat": "Beviljat",
        "upphort": "Upphört",
        "aterkallat": "Återkallat",
    }
    assert _variant_names(flavor_inventory, "tillvaxtverket", "korttidsarbete") == {
        "individer": "Individer",
        "transaktioner": "Transaktioner",
    }


def test_disjoint_deliveries_of_one_service_are_separate_registers(
    flavor_inventory: dict,
) -> None:
    """Inera's two 1177 deliveries are disjoint schemas (see the disposition
    comment), so they are two REGISTERS with a `_default` variant each, under one
    provider named for the service."""
    assert [
        (r["key"], r["name"], [v["key"] for v in r["variants"]])
        for r in flavor_inventory["registers"]
        if r["provider"] == "inera"
    ] == [
        ("bestallda-prover", "Beställda prover", ["_default"]),
        ("samtal", "Samtal 1177", ["_default"]),
    ]
    assert {"slug": "inera", "name": "Inera AB / 1177 Vårdguiden"} in flavor_inventory[
        "providers"
    ]


def test_a_default_variant_keeps_the_register_name(flavor_inventory: dict) -> None:
    defaults = {
        (r["provider"], r["key"]): (r["name"], v["name"])
        for r in flavor_inventory["registers"]
        for v in r["variants"]
        if v["key"] == "_default"
    }
    assert defaults  # the single-variant registers
    assert [c for c, (reg, var) in defaults.items() if reg != var] == []


def test_the_emitted_inventory_loads_under_the_extend_db_contract(
    flavor_inventory_path: Path,
) -> None:
    """A grouped variable's one state must carry the other spelling as an alias
    the `extend-db` contract admits."""
    inventory = load_inventory(flavor_inventory_path)
    register = next(
        r
        for r in inventory.registers
        if (r.provider, r.key) == ("inera", "bestallda-prover")
    )
    variant = next(v for v in register.variants if v.key == "_default")
    covid = next(v for v in variant.variables if v.key == "covid-19-antikroppar")

    assert [(s.column, s.aliases) for s in covid.states] == [
        ("Covid-19 antikroppar", ("Covid_19_antikroppar",))
    ]


def test_two_entries_naming_one_variant_differently_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = build_catalog._FLAVOR_DISPOSITION[0]
    holding = {entry[0]: _synthetic_enriched()[entry[0]]}
    monkeypatch.setattr(
        build_catalog, "_FLAVOR_DISPOSITION", [entry, (*entry[:7], "Annat namn")]
    )

    with pytest.raises(SystemExit, match="declared twice"):
        _run_flavor(tmp_path, holding)


# --- cmd_inventory: alias-only delivery columns ------------------------------


@pytest.fixture(scope="module")
def flavored_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A minimal FLAVORED DB holding `Beställda prover` the way `extend-db`
    writes a grouped variable: ONE `variable_state` on the first spelling, a
    `variable_alias` for every column of that state, and — because these two
    are co-delivered — a `variable_alias_window` for each, the state's own
    included (that write shape is pinned by `test_extend_db.py` →
    `test_co_delivered_aliases_are_one_state_with_alias_windows`; the rows are
    stated directly here because the resolver's input is the DB, and because
    one row below is deliberately one `extend-db` never writes). `T_kolumn` is
    the single-spelling control: one state, its own alias row, no window."""
    from reg_meta_build.db import DDL

    db_path = tmp_path_factory.mktemp("db") / "reg_meta_swecov.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(DDL)
    conn.execute(
        "INSERT INTO provider (provider_id, slug, name) VALUES (900, 'inera', 'Inera AB / 1177 Vårdguiden')"
    )
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name, slug) "
        "VALUES (901, 900, 'Beställda prover', 'bestallda-prover')"
    )
    conn.execute(
        "INSERT INTO register_variant (register_variant_id, register_id, name, slug) "
        "VALUES (902, 901, 'Beställda prover', '_default')"
    )
    for variable_id, slug, columns in (
        (903, "covid-19-antikroppar", ("Covid-19 antikroppar", "Covid_19_antikroppar")),
        (904, "t-kolumn", ("T_kolumn",)),
    ):
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, slug, name) "
            "VALUES (?, 901, ?, ?, ?)",
            (variable_id, slug, slug, columns[0]),
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from,"
            " valid_to, data_type, delivery_column_name) "
            "VALUES (?, 902, '0001-01-01', '9999-12-31', 'varchar', ?)",
            (variable_id, columns[0]),
        )
        alias_rows = [(variable_id, column) for column in columns]
        conn.executemany("INSERT INTO variable_alias VALUES (?, 902, ?)", alias_rows)
        if len(columns) > 1:
            conn.executemany(
                "INSERT INTO variable_alias_window "
                "VALUES (?, 902, ?, '0001-01-01', '9999-12-31')",
                alias_rows,
            )
    # The `HuSSYK1` / `HUSSYK1` shape: a historical spelling carried by
    # `variable_alias` alone. With no window it is a search-only header the
    # order path never delivers, so it must stay out of the resolution index —
    # a mapping onto it would refuse the deployment's boot (§12's gate,
    # `reg_meta.inventory_check`).
    conn.execute("INSERT INTO variable_alias VALUES (904, 902, 'T_KOLUMN')")
    conn.commit()
    conn.close()
    return db_path


def test_an_alias_only_spelling_resolves_beside_its_state_spelling(
    flavored_db: Path,
) -> None:
    """`Covid_19_antikroppar` has no `variable_state` of its own, so before the
    union it resolved to nothing at all — no mapping, hence not orderable."""
    by_regcol, by_provcol, states_vc = build_catalog._steward_load_db(flavored_db)

    coord = "inera/bestallda-prover/_default"
    for spelling in ("Covid-19 antikroppar", "Covid_19_antikroppar"):
        record = {
            "coord": coord,
            "vslug": "covid-19-antikroppar",
            "col": spelling,
            "dtype": "varchar",
            "isid": 0,
        }
        # Both spellings, under the one variable, with the owning state's shape.
        assert by_regcol[("inera/bestallda-prover", spelling.upper())] == [record]
        assert by_provcol[("inera", spelling.upper())] == [record]
        # The alias carries its own window; the state spelling is NOT restated
        # by the alias arm (it has a `variable_state` row of its own).
        assert states_vc[(coord, "covid-19-antikroppar", spelling)] == [
            (None, "0001-01-01", "9999-12-31")
        ]
        # Two spellings of one variable share a value set — never a prune.
        assert not build_catalog._steward_codelivered(
            states_vc, coord, "covid-19-antikroppar", spelling
        )
    # A windowless `variable_alias` spelling is search-only: the column that
    # resolves through it today (UPPER folding onto the state spelling) keeps
    # exactly the record — and so the mapping — it already had.
    assert by_regcol[("inera/bestallda-prover", "T_KOLUMN")] == [
        {
            "coord": coord,
            "vslug": "t-kolumn",
            "col": "T_kolumn",
            "dtype": "varchar",
            "isid": 0,
        }
    ]


def _run_inventory(
    tmp_path: Path, db: Path, overlay: str, table: str, columns: list[str]
) -> Path:
    """`cmd_inventory` over a synthetic one-table CSV; returns the steward dir.

    The table lands under the `Ordered tests` holding, whose disposition entry
    names no table selector — so the table NAME decides only its edition, while
    its columns resolve against the flavored DB (`inera/bestallda-prover`)."""
    steward_dir = tmp_path / "steward"
    steward_dir.mkdir()
    (steward_dir / "inventory_overlay.toml").write_text(overlay, encoding="utf-8")
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    csv_path.write_text(
        "Category,Detail,Table\n"
        + ",".join(("Inera/1177", "Ordered tests", table, *columns))
        + "\n",
        encoding="utf-8",
    )

    build_catalog.cmd_inventory(
        argparse.Namespace(csv=csv_path, db=db, out=steward_dir)
    )
    return steward_dir


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
    # §12's other half: every emitted mapping must resolve against the DB the
    # deployment serves, or `stewards.check_delivery_inventory` refuses boot.
    # `open_db` is the deployment's own read: `sqlite3.Row` and the `py_lower`
    # SQL function the catalog resolves with. `check_schema=False` — the
    # fixture carries no `import_manifest`.
    conn = open_db(flavored_db, check_schema=False)
    try:
        findings = check_inventory(
            load_delivery_inventory(steward_dir / "inventory.toml"), conn
        )
    finally:
        conn.close()
    assert not findings, unresolved_message(findings)


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
            {"from": "2011-07-01", "to": "2012-06-30"},
            ("2011-07-01", "2012-06-30"),
        ),
        (
            "autumn",
            {"from": "2012-07-01", "to": "2013-06-30"},
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


# --- cmd_errata: the scb_errata.toml candidate worklist -----------------------

_VARIABLE_OF = {
    "Covid-19 antikroppar": "covid-19-antikroppar",
    "Covid_19_antikroppar": "covid-19-antikroppar",
    "T_kolumn": "t-kolumn",
}


def _held(steward_dir: Path, holdings: dict[int, tuple[str, ...]]) -> None:
    """A committed-inventory stand-in: one `[[table]]` per edition holding those
    columns of `Beställda prover`, each mapped to its own representation — the
    shape `cmd_inventory` emits."""
    lines = ["version = 1", 'steward = "swecov"']
    for edition, columns in holdings.items():
        lines += ["", "[[table]]", f'id = "T_{edition}"', f"edition = {edition}"]
        for column in columns:
            lines += [
                "",
                "[[table.column]]",
                f'name = "{column}"',
                "",
                "[[table.column.mapping]]",
                'register_variant = "inera/bestallda-prover/_default"',
                f'variable = "inera/bestallda-prover/{_VARIABLE_OF[column]}"',
                f'representation = "{column}"',
            ]
    (steward_dir / "inventory.toml").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _run_errata(tmp_path: Path, db: Path, holdings: dict[int, tuple[str, ...]]) -> dict:
    """Write the holdings, run the subcommand, parse the worklist it leaves under
    `derived/`."""
    steward_dir = tmp_path / "steward"
    steward_dir.mkdir(exist_ok=True)
    _held(steward_dir, holdings)
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    build_catalog.cmd_errata(argparse.Namespace(csv=csv_path, db=db, out=steward_dir))
    return tomllib.loads(
        (csv_path.parent / "derived" / "errata_worklist.toml").read_text(
            encoding="utf-8"
        )
    )


def test_errata_worklist_is_empty_when_every_holding_has_a_window(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The fixture's states are open-ended, so a holding of any edition is
    covered — the worklist carries no candidate entries."""
    assert (
        _run_errata(tmp_path, flavored_db, {2021: ("Covid-19 antikroppar", "T_kolumn")})
        == {}
    )


def test_errata_worklist_splits_version_missing_from_column_missing(
    tmp_path: Path, flavored_db: Path
) -> None:
    """With `T_kolumn` narrowed to 2019 and the variant documented only through
    2020 (the Covid state), a 2020 holding of `T_kolumn` is column-missing and a
    2021 holding of it is version-missing too — so the worklist carries one
    `[[version]]` (2021) and ONE `[[delivered]]` naming both editions (two
    entries for one column would be the duplicate `scb_errata` refuses)."""
    db = tmp_path / "narrowed.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', valid_to = '2019-12-31' "
        "WHERE delivery_column_name = 'T_kolumn'"
    )
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', valid_to = '2020-12-31' "
        "WHERE delivery_column_name = 'Covid-19 antikroppar'"
    )
    conn.commit()
    conn.close()

    worklist = _run_errata(tmp_path, db, {2020: ("T_kolumn",), 2021: ("T_kolumn",)})

    assert worklist["version"] == [
        {"register": "inera/bestallda-prover", "variant": "_default", "name": "2021"}
    ]
    assert worklist["delivered"] == [
        {
            "register": "inera/bestallda-prover",
            "variant": "_default",
            "column": "T_kolumn",
            "versions": ["2020", "2021"],
        }
    ]
