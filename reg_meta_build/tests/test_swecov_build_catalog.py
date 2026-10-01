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
from typing import TYPE_CHECKING

import pytest
from _curation_fixtures import write_lisa_errata
from reg_meta.errors import RegMetaError
from reg_meta.inventory import edition_bounds, load_inventory as load_delivery_inventory
from reg_meta.inventory_check import check_inventory, unresolved_message
from reg_meta_build.db import SCHEMA_VERSION, open_built_db
from reg_meta_build.edition_bounds import edition_claims
from reg_meta_build.ir import (
    IRRegister,
    IRVariable,
    IRVariableAlias,
    IRVariableAliasWindow,
    IRVariableState,
    IRVariant,
)
from reg_meta_build.sources.curated import CuratedAdapter

if TYPE_CHECKING:
    from types import ModuleType

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


def test_default_csv_uses_newest_full_inventory_or_requires_argument(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    generator = tmp_path / "reg_meta_build/input_data/swecov/build_catalog.py"
    generator.parent.mkdir(parents=True)
    generator.write_bytes(_GENERATOR.read_bytes())
    policy = tmp_path / "reg_webapp/stewards/swecov/source_policy.toml"
    policy.parent.mkdir(parents=True)
    policy.write_bytes(build_catalog.SOURCE_POLICY_PATH.read_bytes())
    old_name = generator.parent / "SWECOV_variables_2099-01-01.csv"
    older = generator.parent / "SWECOV_variables_full_2026-08-01.csv"
    newest = generator.parent / "SWECOV_variables_full_2026-09-13.csv"
    for path in (old_name, older, newest):
        path.touch()

    def load(suffix: str) -> ModuleType:
        spec = importlib.util.spec_from_file_location(f"swecov_csv_{suffix}", generator)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        return module

    assert newest == load("matching").DEFAULT_CSV

    older.unlink()
    newest.unlink()
    without_match = load("required")
    assert without_match.DEFAULT_CSV is None
    monkeypatch.setattr(sys, "argv", [str(generator), "normalize"])
    with pytest.raises(SystemExit) as exc_info:
        without_match.main()
    assert exc_info.value.code == 2
    assert "the following arguments are required: --csv" in capsys.readouterr().err


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
    """Run `cmd_flavor` in a synthetic package layout; return its input root."""
    base = tmp_path / "input_data" / "swecov"
    derived = base / "derived"
    derived.mkdir(parents=True)
    (derived / "holdings_enriched.json").write_text(
        json.dumps(enriched), encoding="utf-8"
    )
    build_catalog.cmd_flavor(
        argparse.Namespace(csv=base / "SWECOV_variables_2025-12-11.csv")
    )
    return base


@pytest.fixture(scope="module")
def flavor_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One generator run for every test that reads its provider TOMLs."""
    return _run_flavor(tmp_path_factory.mktemp("flavor"), _synthetic_enriched())


@pytest.fixture(scope="module")
def flavor_providers(flavor_root: Path) -> dict[str, dict]:
    return {
        path.stem: tomllib.loads(path.read_text(encoding="utf-8"))
        for path in sorted((flavor_root / "providers").glob("*.toml"))
    }


def _variant_names(providers: dict, provider: str, register: str) -> dict[str, str]:
    reg = next(r for r in providers[provider]["register"] if r["key"] == register)
    return {v["key"]: v["name"] for v in reg["variant"]}


def test_a_named_variant_carries_its_own_name(flavor_providers: dict) -> None:
    assert _variant_names(flavor_providers, "skatteverket", "tillfalligt-anstand") == {
        "ansokt": "Ansökt",
        "beviljat": "Beviljat",
        "upphort": "Upphört",
        "aterkallat": "Återkallat",
    }
    assert _variant_names(flavor_providers, "tillvaxtverket", "korttidsarbete") == {
        "individer": "Individer",
        "transaktioner": "Transaktioner",
    }


def test_disjoint_deliveries_of_one_service_are_separate_registers(
    flavor_providers: dict,
) -> None:
    """Inera's two 1177 deliveries are disjoint schemas (see the disposition
    comment), so they are two REGISTERS with a `_default` variant each, under one
    provider named for the service."""
    assert [
        (r["key"], r["name"], [v["key"] for v in r["variant"]])
        for r in flavor_providers["inera"]["register"]
    ] == [
        ("bestallda-prover", "Beställda prover", ["_default"]),
        ("samtal", "Samtal 1177", ["_default"]),
    ]
    assert flavor_providers["inera"]["provider"]["name"] == (
        "Inera AB / 1177 Vårdguiden"
    )


def test_a_default_variant_keeps_the_register_name(flavor_providers: dict) -> None:
    defaults = {
        (provider, r["key"]): (r["name"], v["name"])
        for provider, data in flavor_providers.items()
        for r in data["register"]
        for v in r["variant"]
        if v["key"] == "_default"
    }
    assert defaults  # the single-variant registers
    assert [c for c, (reg, var) in defaults.items() if reg != var] == []


def test_emitted_tomls_load_through_curated_adapter(
    flavor_root: Path,
) -> None:
    """A grouped variable's one state must carry the other spelling as an alias
    the `extend-db` contract admits."""
    objs = list(
        CuratedAdapter("inera", steward="swecov").emit(flavor_root / "providers")
    )
    covid = next(
        variable
        for variable in objs
        if isinstance(variable, IRVariable)
        and variable.provider_key == "covid-19-antikroppar"
    )
    states = [
        o
        for o in objs
        if isinstance(o, IRVariableState) and o.variable_id == covid.variable_id
    ]
    aliases = [
        o
        for o in objs
        if isinstance(o, IRVariableAlias) and o.variable_id == covid.variable_id
    ]
    windows = [
        o
        for o in objs
        if isinstance(o, IRVariableAliasWindow) and o.variable_id == covid.variable_id
    ]
    assert [state.delivery_column_name for state in states] == ["Covid-19 antikroppar"]
    assert {alias.delivery_column_name for alias in aliases} == {
        "Covid-19 antikroppar",
        "Covid_19_antikroppar",
    }
    assert {window.delivery_column_name for window in windows} == {
        "Covid-19 antikroppar",
        "Covid_19_antikroppar",
    }


def test_generated_slug_pins_bind_to_emitted_graph(flavor_root: Path) -> None:
    providers_dir = flavor_root / "providers"
    slug_dir = flavor_root.parents[1] / "fqid_slugs" / "swecov"
    for path in sorted(providers_dir.glob("*.toml")):
        objs = list(CuratedAdapter(path.stem, steward="swecov").emit(providers_dir))
        pins = tomllib.loads((slug_dir / path.name).read_text(encoding="utf-8"))
        register_pins = pins.get("register", {})
        variant_pins = pins.get("register_variant", {})
        for obj in objs:
            if isinstance(obj, IRRegister):
                assert str(obj.register_id) in register_pins
            elif isinstance(obj, IRVariant):
                assert f"{obj.register_id}.{obj.register_variant_id}" in variant_pins


def test_repeated_flavor_generation_is_deterministic(tmp_path: Path) -> None:
    root = _run_flavor(tmp_path, _synthetic_enriched())
    providers_dir = root / "providers"
    before = {path.name: path.read_bytes() for path in providers_dir.glob("*.toml")}

    build_catalog.cmd_flavor(
        argparse.Namespace(csv=root / "SWECOV_variables_2025-12-11.csv")
    )

    assert {
        path.name: path.read_bytes() for path in providers_dir.glob("*.toml")
    } == before


def test_removed_provider_refuses_stale_toml_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _run_flavor(tmp_path, _synthetic_enriched())
    providers_dir = root / "providers"
    stale = providers_dir / "swedbank.toml"
    assert stale.is_file()
    control = next(path for path in providers_dir.glob("*.toml") if path != stale)
    control.write_text("must remain untouched\n", encoding="utf-8")
    monkeypatch.setattr(
        build_catalog,
        "_FLAVOR_DISPOSITION",
        [
            entry
            for entry in build_catalog._FLAVOR_DISPOSITION
            if entry[1] != "swedbank"
        ],
    )

    with pytest.raises(SystemExit) as exc:
        build_catalog.cmd_flavor(
            argparse.Namespace(csv=root / "SWECOV_variables_2025-12-11.csv")
        )

    assert "swedbank.toml" in str(exc.value)
    assert "Review and remove obsolete files" in str(exc.value)
    assert stale.is_file()
    assert control.read_text(encoding="utf-8") == "must remain untouched\n"


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
        "INSERT INTO import_manifest VALUES (?, ?)", ("schema_version", SCHEMA_VERSION)
    )
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
                "(variable_id, register_variant_id, delivery_column_name, "
                "valid_from, valid_to) VALUES "
                "(?, 902, ?, '0001-01-01', '9999-12-31')",
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
    by_regcol = build_catalog._steward_load_db(flavored_db)

    coord = "inera/bestallda-prover/_default"
    for spelling in ("Covid-19 antikroppar", "Covid_19_antikroppar"):
        record = {
            "coord": coord,
            "vslug": "covid-19-antikroppar",
            "col": spelling,
            "valid_from": "0001-01-01",
            "valid_to": "9999-12-31",
            "period_scope": "intervals",
        }
        # Both spellings resolve under the one variable.
        assert by_regcol[("inera/bestallda-prover", spelling.upper())] == [record]
    # A windowless `variable_alias` spelling is search-only: the column that
    # resolves through it today (UPPER folding onto the state spelling) keeps
    # exactly the record — and so the mapping — it already had.
    assert by_regcol[("inera/bestallda-prover", "T_KOLUMN")] == [
        {
            "coord": coord,
            "vslug": "t-kolumn",
            "col": "T_kolumn",
            "valid_from": "0001-01-01",
            "valid_to": "9999-12-31",
            "period_scope": "intervals",
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
    # The produced holdings must have complete catalog coordinate coverage.
    conn = open_built_db(flavored_db)
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


# --- cmd_errata: the register-file errata candidate worklist -----------------

_VARIABLE_OF = {
    "Covid-19 antikroppar": "covid-19-antikroppar",
    "Covid_19_antikroppar": "covid-19-antikroppar",
    "T_kolumn": "t-kolumn",
    "Errata_utan_variant": "errata-utan-variant",
}

# The fixture's own variant coordinate — on `inera`, the steward's flavor provider.
_INERA = "inera/bestallda-prover/_default"


def _held(
    steward_dir: Path, holdings: dict[int | str, tuple[str, ...]], variant: str
) -> None:
    """A committed-inventory stand-in: one `[[table]]` per edition holding those
    columns of `Beställda prover`, each mapped to its own representation — the
    shape `cmd_inventory` emits. `variant` is the coordinate they map onto, so a
    holdings statement can name the register under another provider."""
    register = variant.rpartition("/")[0]
    lines = ["version = 1", 'steward = "swecov"']
    for edition, columns in holdings.items():
        edition_value = (
            str(edition)
            if isinstance(edition, int) or edition.startswith(("{", "["))
            else json.dumps(edition)
        )
        lines += [
            "",
            "[[table]]",
            f"id = {json.dumps(f'T_{edition}')}",
            f"edition = {edition_value}",
        ]
        for column in columns:
            lines += [
                "",
                "[[table.column]]",
                f'name = "{column}"',
                "",
                "[[table.column.mapping]]",
                f'register_variant = "{variant}"',
                f'variable = "{register}/{_VARIABLE_OF[column]}"',
                f'representation = "{column}"',
            ]
    (steward_dir / "inventory.toml").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _errata_text(
    tmp_path: Path,
    db: Path,
    holdings: dict[int | str, tuple[str, ...]],
    variant: str = _INERA,
) -> str:
    """Write the holdings, run the subcommand, read back the worklist it leaves
    under `derived/`. `tomllib.loads` of the result is the maintainer's own read:
    comment-only sections carry no entries, so an all-commented worklist is `{}`."""
    steward_dir = tmp_path / "steward"
    steward_dir.mkdir(exist_ok=True)
    _held(steward_dir, holdings, variant)
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    build_catalog.cmd_errata(argparse.Namespace(csv=csv_path, db=db, out=steward_dir))
    return (csv_path.parent / "derived" / "errata_worklist.toml").read_text(
        encoding="utf-8"
    )


def test_errata_worklist_is_empty_when_every_holding_has_a_window(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The fixture's states are open-ended, so a holding of any edition is
    covered — the worklist carries no candidate entries."""
    worklist = _errata_text(
        tmp_path, flavored_db, {2021: ("Covid-19 antikroppar", "T_kolumn")}
    )
    assert tomllib.loads(worklist) == {}


def test_errata_rejects_incompatible_catalog_schema(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = tmp_path / "old.db"
    db.write_bytes(flavored_db.read_bytes())
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE import_manifest SET value = '0.1.0' WHERE key = 'schema_version'"
        )
    with pytest.raises(RegMetaError) as exc_info:
        _errata_text(tmp_path, db, {2021: ("T_kolumn",)})
    assert exc_info.value.code == "schema_incompatible"
    assert not (tmp_path / "derived/errata_worklist.toml").exists()


def test_errata_worklist_splits_version_missing_from_column_missing(
    tmp_path: Path, flavored_db: Path
) -> None:
    """With `T_kolumn` narrowed to 2019 and the variant documenting exactly one
    register version (2020), a 2020 holding of `T_kolumn` is column-missing — the
    omitted row names that version verbatim — and a 2021 holding is version-missing
    too, since the catalog knows no version covering it. So the worklist carries one
    `[[errata.version]]` (2021) and ONE `[[errata.delivered]]` naming both editions (two entries
    for one column would be the duplicate `scb_errata` refuses).

    The register moves to the `scb` provider on the copy, because stanzas are what
    SCB register files accept these tables and no other provider: a miss on the fixture's
    own flavor provider is the curated-window case below, not this one.
    """
    db = tmp_path / "narrowed.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', valid_to = '2019-12-31' "
        "WHERE delivery_column_name = 'T_kolumn'"
    )
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn) "
        "VALUES (905, 902, '2020')"
    )
    conn.commit()
    conn.close()

    worklist = tomllib.loads(
        _errata_text(
            tmp_path,
            db,
            {2020: ("T_kolumn",), 2021: ("T_kolumn",)},
            variant="scb/bestallda-prover/_default",
        )
    )

    # Complete entries, not fragments: every key `load_scb_errata` requires is
    # present, with the curator's own two as TODO placeholders.
    todo = {
        "evidence": "TODO: the evidence that SCB delivered this row",
        "noted": "TODO: YYYY-MM-DD",
    }
    assert worklist["errata"]["version"] == [
        {
            "variant": "_default",
            "name": "2021",
            **todo,
        }
    ]
    assert worklist["errata"]["delivered"] == [
        {
            "variant": "_default",
            "column": "T_kolumn",
            "versions": ["2020", "2021"],
            **todo,
        }
    ]


def test_errata_column_misses_are_inspection_items_not_delivered_candidates(
    tmp_path: Path, flavored_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A `[[column]]`-minted variable has no real SCB row for `[[delivered]]`.

    The finite 2019 state models an explicitly version-limited entry; the second
    variable resolves at register scope but has no state on the target variant,
    so the DB cannot distinguish a missing entry from a routing/materialization
    problem. A real undocumented 2021 edition still earns its independent
    `[[version]]` candidate. The range table proves it contributes to none of the
    candidate or assessed counts.
    """
    db = tmp_path / "errata-columns.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable SET source_label = 'scb-errata' WHERE variable_id = 904"
    )
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', "
        "valid_to = '2019-12-31' WHERE variable_id = 904"
    )
    conn.execute(
        "INSERT INTO variable "
        "(variable_id, register_id, provider_key, slug, name, source_label) "
        "VALUES (906, 901, 'Errata_utan_variant', 'errata-utan-variant', "
        "'Errata utan variant', 'scb-errata')"
    )
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn) "
        "VALUES (905, 902, '2020')"
    )
    conn.commit()
    conn.close()

    text = _errata_text(
        tmp_path,
        db,
        {
            2020: ("T_kolumn", "Errata_utan_variant"),
            2021: ("T_kolumn",),
            "{ from = 2022, to = 2023 }": (
                "T_kolumn",
                "Errata_utan_variant",
            ),
        },
        variant="scb/bestallda-prover/_default",
    )

    worklist = tomllib.loads(text)
    assert [entry["name"] for entry in worklist["errata"]["version"]] == ["2021"]
    assert "delivered" not in worklist
    assert "column-missing: the omitted column rows themselves, 0" in text
    assert (
        "errata-created columns: 2 group(s); no [[errata.delivered]] candidate" in text
    )
    assert "scb/bestallda-prover/_default T_kolumn: held 2020..2021" in text
    assert "scb/bestallda-prover/_default Errata_utan_variant: held 2020" in text
    assert "target-variant [[errata.column]] entry and inventory" in text
    assert "does not reveal `all_versions` versus explicit `versions`" in text
    assert "only when its matching" in text
    assert "uses `all_versions = true`" in text
    assert "routing/materialization mismatch" in text
    assert "1 multi-period table(s) not assessed" in text
    assert 'name = "2022"' not in text
    assert 'name = "2023"' not in text

    stdout = capsys.readouterr().out
    assert "held column × edition pairs: 3 assessed, 3 with no catalog window" in stdout
    assert "1 multi-period table(s) not assessed" in stdout
    assert "version-missing: 1 [[errata.version]] candidate(s)" in stdout
    assert "column-missing: 0 [[errata.delivered]] candidate(s)" in stdout
    assert "errata-column: 2 miss(es) to inspect" in stdout


def test_school_year_version_candidate_round_trips_through_scb_claims(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The holdings token stays `LA2020`, while the suggested SCB version name
    uses the loader's native `2020/2021` spelling and claims those exact bounds.
    A multi-period control remains entirely outside inference.
    """
    db = tmp_path / "school-year.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', "
        "valid_to = '2019-12-31' WHERE variable_id = 904"
    )
    conn.commit()
    conn.close()

    text = _errata_text(
        tmp_path,
        db,
        {"LA2020": ("T_kolumn",), "[2022, 2023]": ("T_kolumn",)},
        variant="scb/bestallda-prover/_default",
    )
    worklist = tomllib.loads(text)

    assert [entry["name"] for entry in worklist["errata"]["version"]] == ["2020/2021"]
    assert worklist["errata"]["delivered"][0]["versions"] == ["2020/2021"]
    claims = edition_claims(worklist["errata"]["version"][0]["name"])
    assert claims == (
        (2020, "2020-07-01", "2020-12-31"),
        (2021, "2021-01-01", "2021-06-30"),
    )
    assert (claims[0][1], claims[-1][2]) == edition_bounds("LA2020")[0]
    assert "held LA2020, catalog windows 2019" in text
    assert "1 multi-period table(s) not assessed" in text
    assert 'name = "2022"' not in text
    assert 'name = "2023"' not in text


def test_errata_worklist_lists_a_non_scb_miss_as_a_curated_window(
    tmp_path: Path, flavored_db: Path
) -> None:
    """`Beställda prover` sits on the steward's own `inera` provider, and
    errata entries belong in the SCB register files alone — so the same narrowed holding
    yields NO stanza at all. It rides in the third section as a comment naming the
    held editions, the catalog's window and the surface that carries it: for a
    flavor provider, the curated-provider TOML `extend-db` overlaid. A
    `[[errata.delivered]]` here would send the maintainer to a file
    whose loader refuses the entry."""
    db = tmp_path / "narrowed.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', valid_to = '2019-12-31' "
        "WHERE delivery_column_name = 'T_kolumn'"
    )
    conn.commit()
    conn.close()

    text = _errata_text(tmp_path, db, {2020: ("T_kolumn",)})

    # Not one entry of either kind — the whole finding is a comment, so no line
    # opens a stanza and the file parses to nothing.
    assert not [ln for ln in text.splitlines() if ln.startswith("[[")]
    assert tomllib.loads(text) == {}
    assert "curated windows: 1 group(s)" in text
    (line,) = [ln for ln in text.splitlines() if "T_kolumn:" in ln]
    assert "held 2020, catalog windows 2019" in line
    assert "not errata (provider `inera`, not `scb`)" in line
    assert "the curated-provider TOML extend-db overlays" in line


@pytest.mark.parametrize("variant", [_INERA, "scb/bestallda-prover/_default"])
def test_errata_worklist_excludes_every_multi_period_suggestion(
    tmp_path: Path, flavored_db: Path, variant: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Partial, absent and disjoint range/list holdings are not availability
    evidence. They produce no version, delivered-column or curated-window
    suggestion regardless of provider, and the worklist/CLI report an honest
    zero assessed denominator plus the skipped-table count."""
    db = tmp_path / "narrowed.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    if variant.startswith("scb/"):
        conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', "
        "valid_to = '2019-12-31' WHERE delivery_column_name = 'T_kolumn'"
    )
    conn.commit()
    conn.close()

    text = _errata_text(
        tmp_path,
        db,
        {
            "{ from = 2018, to = 2020 }": ("T_kolumn",),
            "{ from = 2021, to = 2022 }": ("T_kolumn",),
            "[2016, 2017]": ("T_kolumn",),
        },
        variant=variant,
    )

    assert tomllib.loads(text) == {}
    assert "out of 0 assessed" in text
    assert "3 multi-period table(s) not assessed" in text
    assert "version-missing: 0" in text
    assert "column-missing: the omitted column rows themselves, 0" in text
    assert "errata-created columns: 0" in text
    assert "curated windows: 0" in text
    stdout = capsys.readouterr().out
    assert "held column × edition pairs: 0 assessed, 0 with no catalog window" in stdout
    assert "3 multi-period table(s) not assessed" in stdout
    assert "version-missing: 0 [[errata.version]] candidate(s)" in stdout
    assert "column-missing: 0 [[errata.delivered]] candidate(s)" in stdout
    assert "errata-column: 0 miss(es) to inspect" in stdout
    assert "curated-window: 0 non-scb miss(es)" in stdout


# --- cmd_grafts: the register-file [[errata.column]] candidates ---------------


def _grafts_text(
    tmp_path: Path,
    db: Path,
    gapfill: list[dict],
    enriched: dict[str, dict],
    doc_columns: tuple[str, ...] = (),
) -> str:
    """Run the subcommand over synthetic `globals` output; read back the worklist.

    The CSV is header-only: with no physical holding to consult, a `mapped`
    holding's single coordinate places every column, which is the path under test.
    `doc_columns` seeds the ingested-docs tree the scan cross-references — its root
    is two levels above the CSV, the repo layout `reg_meta_build/docs/<register>/`.
    """
    base = tmp_path / "input_data" / "swecov"
    (base / "derived").mkdir(parents=True)
    csv_path = base / "SWECOV_variables_2025-12-11.csv"
    csv_path.write_text("Category,Detail,Table\n", encoding="utf-8")
    (base / "derived" / "holdings_enriched.json").write_text(
        json.dumps(enriched), encoding="utf-8"
    )
    (base / "derived" / "global_enrichment.json").write_text(
        json.dumps({"gapfill": gapfill}), encoding="utf-8"
    )
    for column in doc_columns:
        page = tmp_path / "docs" / "lisa" / f"{column}.md"
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text(f"# {column}\n", encoding="utf-8")

    build_catalog.cmd_grafts(argparse.Namespace(csv=csv_path, db=db))
    return (base / "derived" / "column_candidates.toml").read_text(encoding="utf-8")


def _gapfill(register: str, column: str, **extra: str) -> dict:
    return {
        "register": register,
        "column": column,
        "description": f"{column} enligt SCB",
        **extra,
    }


def _lisa_holding(*columns: str) -> dict[str, dict]:
    """One `mapped` LISA holding documenting `columns` — the single-coordinate
    mapping every placed candidate below resolves through."""
    return {
        "LISA / Individer": {
            "columns": [{"name": column} for column in columns],
            "mapping": {"status": "mapped", "to": "scb/lisa/individer-15plus"},
        }
    }


def test_a_placed_gapfill_column_is_a_complete_column_entry(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The candidate is a whole `[[column]]`: the variant the mapping placed it on,
    the description as both `name` and `definition`, `all_versions = true` because a
    holdings list dates nothing — and the curator's two fields as TODO placeholders,
    `noted` in the form `load_scb_errata` refuses. No `data_type`: the delivery
    spells `varchar`, which is not one of the four the loader accepts."""
    text = _grafts_text(
        tmp_path,
        flavored_db,
        [_gapfill("scb/lisa", "FastBet", data_type="varchar", kalla="LISA")],
        _lisa_holding("FastBet"),
    )

    assert tomllib.loads(text)["errata"]["column"] == [
        {
            "variant": "individer-15plus",
            "column": "FastBet",
            "name": "FastBet enligt SCB",
            "definition": "FastBet enligt SCB",
            "all_versions": True,
            "source": "steward-holdings",
            "evidence": "TODO: the delivery list holding this column, and that SCB's "
            "export carries no row for it on any version of this variant",
            "noted": "TODO: YYYY-MM-DD",
        }
    ]
    # The scan's own evidence rides as a comment, for the curator to write
    # `evidence` from — not as a key the loader would refuse.
    assert "# scb/lisa/individer-15plus FastBet; källa LISA; delivery type varchar" in (
        text.splitlines()
    )


def test_an_scb_documented_column_is_the_same_entry_under_the_other_source(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A column reg_meta's ingested SCB docs describe is no longer routed away from
    the scan: since Y-116 it is the SAME entry kind, separated only by `source`, so
    both land in one file and the maintainer curates one grammar."""
    text = _grafts_text(
        tmp_path,
        flavored_db,
        [_gapfill("scb/lisa", "Ssyk4_J16"), _gapfill("scb/lisa", "FastBet")],
        _lisa_holding("Ssyk4_J16", "FastBet"),
        doc_columns=("Ssyk4_J16",),
    )

    assert {
        entry["column"]: entry["source"]
        for entry in tomllib.loads(text)["errata"]["column"]
    } == {"Ssyk4_J16": "scb-docs", "FastBet": "steward-holdings"}
    # Sectioned by source, each section saying what still has to be curated in it.
    assert "# ── source = scb-docs: 1 column(s)" in text
    assert "narrow `all_versions` to the years its doc page carries" in text


@pytest.mark.parametrize(
    ("gapfill", "enriched", "section"),
    [
        pytest.param(
            _gapfill("scb/lisa", "Okand"),
            {},
            "no variant:",
            id="unplaced",
        ),
        pytest.param(
            _gapfill("scb/gdb", "Ruta250"),
            {
                "Geografidatabasen / Individer": {
                    "columns": [{"name": "Ruta250"}],
                    "mapping": {"status": "flavor", "graft": "scb/gdb"},
                }
            },
            "steward flavor:",
            id="flavor-register",
        ),
        pytest.param(
            _gapfill("scb/lisa", "SyssStat_klartext"),
            _lisa_holding("SyssStat_klartext"),
            "content-dropped:",
            id="klartext",
        ),
    ],
)
def test_a_column_the_scan_cannot_place_is_a_comment_not_an_entry(
    tmp_path: Path,
    flavored_db: Path,
    gapfill: dict,
    enriched: dict[str, dict],
    section: str,
) -> None:
    """Nothing the scan did not place reaches the grammar. A column with no variant
    has nothing to mint onto, a steward-flavor column is not SCB's export at all,
    and a klartext label column is #373's — each rides as a comment in its own
    section, so the file the maintainer reads parses to no entries."""
    text = _grafts_text(tmp_path, flavored_db, [gapfill], enriched)

    assert tomllib.loads(text) == {}
    assert f"# ── {section} 1 column(s)" in text
    assert any(
        line.startswith("# ") and gapfill["column"] in line
        for line in text.splitlines()
    )


def test_the_emitted_candidates_load_as_scb_errata(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The output IS the repair candidate, so its `[[errata.column]]` table parses,
    and `load_scb_errata` accepts its shape against the repo's curated SCB slugs.

    `noted` is the one field a placeholder cannot satisfy — the loader demands a
    canonical `YYYY-MM-DD`, which is exactly what stops an uncurated paste from
    reaching a build — so the proof is the same one `inventory_coverage`'s worklist
    carries: refused while undated, loads once dated, every other key already what
    the loader wants. Real repo slugs, because that is what the stanzas name.
    """
    from reg_meta_build.scb_errata import load_scb_errata

    from reg_meta_build.fqid_slugs import repo_slug_dir

    text = _grafts_text(
        tmp_path,
        flavored_db,
        [_gapfill("scb/lisa", "Ssyk4_J16"), _gapfill("scb/lisa", "FastBet")],
        _lisa_holding("Ssyk4_J16", "FastBet"),
        doc_columns=("Ssyk4_J16",),
    )
    root = write_lisa_errata(tmp_path / "curation", text)

    with pytest.raises(RegMetaError) as exc:
        load_scb_errata(root, repo_slug_dir())
    assert "`noted`" in exc.value.message

    root = write_lisa_errata(
        root,
        text.replace('noted = "TODO: YYYY-MM-DD"', 'noted = "2026-09-12"'),
    )
    errata = load_scb_errata(root, repo_slug_dir())
    assert {(column.column, column.source) for column in errata.columns} == {
        ("Ssyk4_J16", "scb-docs"),
        ("FastBet", "steward-holdings"),
    }
    # `all_versions` — the generator dates nothing, and neither source does.
    assert all(column.versions is None for column in errata.columns)


@pytest.mark.parametrize(("year", "expected"), [(2021, "before-bas"), (2022, "bas")])
def test_inventory_uses_the_declared_owner_covering_its_exact_year(
    tmp_path: Path, flavored_db: Path, year: int, expected: str
) -> None:
    """The Org_InstKod10 shape: one literal, distinct reviewed edition owners."""
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


def _period_record(owner: str, start: str | None, end: str | None) -> dict:
    return {
        "coord": "inera/bestallda-prover/_default",
        "vslug": owner,
        "col": "Column",
        "valid_from": start,
        "valid_to": end,
        "period_scope": "intervals",
    }


@pytest.mark.parametrize(
    ("records", "issue"),
    [
        (
            [_period_record("one", "2019-01-01", "2019-06-30")],
            "no_covering_delivery_owner",
        ),
        (
            [
                _period_record("one", "2019-01-01", "2019-12-31"),
                _period_record("two", "2019-01-01", "2019-12-31"),
            ],
            "ambiguous_delivered_owners",
        ),
        ([_period_record("one", None, None)], "no_covering_delivery_owner"),
    ],
)
def test_inventory_refuses_incomplete_ambiguous_or_unknown_owner_scopes(
    records: list[dict], issue: str
) -> None:
    admitted, finding = build_catalog._inventory_period_records(records, 2019)
    assert not admitted
    assert finding == issue


def test_inventory_accepts_abutting_delivery_windows_of_the_same_owner() -> None:
    records = [
        _period_record("one", "2019-01-01", "2019-06-30"),
        _period_record("one", "2019-07-01", "2019-12-31"),
    ]
    admitted, issue = build_catalog._inventory_period_records(records, 2019)
    assert {r["vslug"] for r in admitted} == {"one"}
    assert issue is None


def test_inventory_does_not_choose_an_owner_from_a_pooled_range() -> None:
    records = [
        _period_record("one", "2019-01-01", "2019-12-31"),
        _period_record("two", "2020-01-01", "2020-12-31"),
    ]
    admitted, issue = build_catalog._inventory_period_records(
        records, {"from": 2019, "to": 2020}
    )
    assert {r["vslug"] for r in admitted} == {"one", "two"}
    assert issue is None


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


def test_inventory_unmap_refuses_blank_reason_before_writing(tmp_path: Path) -> None:
    overlay = tmp_path / "overlay.toml"
    overlay.write_text('[[unmap]]\ntable = "T2019"\ncolumn = "FIXBB"\nreason = "  "\n')
    with pytest.raises(SystemExit, match="unmapped_reason must be nonblank"):
        build_catalog._load_overlay(overlay)


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


def test_inventory_overlay_requires_complete_catalog_coverage() -> None:
    entry = build_catalog.InventoryMappingOverride.model_validate(
        tomllib.loads(_literal_mapping_overlay())["mapping"][0], strict=True
    )
    records = [
        {
            "coord": entry.register_variant,
            "vslug": entry.variable.variable,
            "col": entry.representation,
            "period_scope": "intervals",
            "valid_from": "2019-01-01",
            "valid_to": "2019-06-30",
        }
    ]
    admitted, issue = build_catalog._inventory_mapping_records(
        entry, records, 2019, {entry.register_variant}
    )
    assert admitted == []
    assert issue == "stale_declared_mapping_target"


def test_inventory_overlay_refuses_duplicate_declarations(
    tmp_path: Path,
) -> None:
    overlay = tmp_path / "overlay.toml"
    overlay.write_text(_literal_mapping_overlay() * 2)
    with pytest.raises(SystemExit, match="duplicate mapping override"):
        build_catalog._load_overlay(overlay)


def test_inventory_overlay_does_not_hide_ambiguous_catalog_owners() -> None:
    entry = build_catalog.InventoryMappingOverride.model_validate(
        tomllib.loads(_literal_mapping_overlay())["mapping"][0], strict=True
    )
    record = {
        "coord": entry.register_variant,
        "vslug": entry.variable.variable,
        "col": entry.representation,
        "period_scope": "intervals",
        "valid_from": "2019-01-01",
        "valid_to": "2019-12-31",
    }
    admitted, issue = build_catalog._inventory_mapping_records(
        entry,
        [record, {**record, "vslug": "different-quantity"}],
        2019,
        {entry.register_variant},
    )
    assert admitted == []
    assert issue == "stale_declared_mapping_target"


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


def test_inventory_overlay_owner_selection_requires_own_complete_coverage() -> None:
    entry = build_catalog.InventoryMappingOverride.model_validate(
        tomllib.loads(_literal_mapping_overlay(select_owner=True))["mapping"][0],
        strict=True,
    )
    record = {
        "coord": entry.register_variant,
        "vslug": entry.variable.variable,
        "col": entry.representation,
        "period_scope": "intervals",
        "valid_from": "2019-01-01",
        "valid_to": "2019-06-30",
    }
    admitted, issue = build_catalog._inventory_mapping_records(
        entry,
        [record, {**record, "vslug": "different-quantity", "valid_to": "2019-12-31"}],
        2019,
        {entry.register_variant},
    )
    assert admitted == []
    assert issue == "stale_declared_mapping_target"


def test_inventory_dated_alias_intersects_states_even_if_literal_was_canonical(
    tmp_path: Path, flavored_db: Path
) -> None:
    import shutil

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
    records = build_catalog._steward_load_db(db)[("inera/bestallda-prover", "EARLIER")]
    assert [(r["valid_from"], r["valid_to"]) for r in records] == [
        ("2018-01-01", "2018-12-31"),
        ("2019-01-01", "2019-06-30"),
        ("2019-07-01", "2019-12-31"),
    ]
    # No positive current-column backing in2020: the spanning source alias
    # must not create a temporal hull into that year.
    admitted, issue = build_catalog._inventory_period_records(records, 2020)
    assert admitted == []
    assert issue == "no_covering_delivery_owner"


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("duplicate_route", "duplicate route"),
        ("duplicate_flavor", "duplicate flavor coordinate"),
        ("unknown_section", "Extra inputs are not permitted"),
        ("unknown_field", "Extra inputs are not permitted"),
        ("unknown_status", "Input should be"),
        ("unknown_selector", "unknown or invalid route selector"),
        ("duplicate_selector", "duplicate route selector"),
        ("duplicate_tables", "flavor tables must be nonblank and unique"),
        ("duplicate_register", "duplicate flavor register"),
        ("unknown_register", "policy registers must be register FQIDs"),
        ("blank_category", "non-catalog categories and reasons must be nonblank"),
        ("unknown_target", "3-part variant coordinate"),
        ("unknown_split_target", "3-part variant coordinate"),
        ("unknown_graft", "policy registers must be register FQIDs"),
        ("unknown_scope_register", "policy registers must be register FQIDs"),
    ],
)
def test_source_policy_rejects_unchecked_or_duplicate_decisions(
    tmp_path: Path, case: str, message: str
) -> None:
    from pydantic import ValidationError

    raw = tomllib.loads(build_catalog.SOURCE_POLICY_PATH.read_text(encoding="utf-8"))
    if case == "duplicate_route":
        raw["route"].append(raw["route"][0])
    elif case == "duplicate_flavor":
        raw["flavor"].append(raw["flavor"][0])
    elif case == "unknown_section":
        raw["unknown"] = []
    elif case == "unknown_field":
        raw["route"][0]["typo"] = "unchecked"
    elif case == "unknown_status":
        raw["route"][0]["status"] = "guess"
    elif case in {"unknown_selector", "duplicate_selector"}:
        entry = next(entry for entry in raw["route"] if entry["status"] == "split")
        if case == "unknown_selector":
            entry["split"][0]["selector"] = "guess:AKU"
        else:
            entry["split"].append(entry["split"][0])
    elif case == "duplicate_tables":
        raw["flavor"][0]["tables"] = ["same", "same"]
    elif case == "duplicate_register":
        raw["flavor_registers"].append(raw["flavor_registers"][0])
    elif case == "unknown_register":
        raw["flavor_registers"] = ["scb"]
    elif case == "unknown_target":
        raw["route"][0]["target"] = "scb/agi"
    elif case == "unknown_split_target":
        entry = next(entry for entry in raw["route"] if entry["status"] == "split")
        entry["split"][0]["target"] = "scb/agi"
    elif case == "unknown_graft":
        raw["route"][0]["graft"] = "scb"
    elif case == "unknown_scope_register":
        raw["register_scope"][0]["register"] = "scb"
    else:
        raw["non_catalog_categories"]["blank"] = " "
    with pytest.raises(ValidationError, match=message):
        build_catalog.SourcePolicy.model_validate(raw)


def test_flavor_rejects_stale_declared_table_before_writing(tmp_path: Path) -> None:
    enriched = _synthetic_enriched()
    entry = next(
        entry
        for entry in build_catalog._FLAVOR_DISPOSITION
        if (entry[1], entry[3], entry[5]) in build_catalog._FLAVOR_VARIANT_TABLES
    )
    table = build_catalog._FLAVOR_VARIANT_TABLES[entry[1], entry[3], entry[5]][0]
    enriched[entry[0]]["table_columns"].pop(table)
    with pytest.raises(SystemExit, match=r"table\(s\) absent from holding"):
        _run_flavor(tmp_path, enriched)
    assert not (tmp_path / "providers").exists()


def test_source_policy_loader_refuses_unknown_sections(tmp_path: Path) -> None:
    path = tmp_path / "policy.toml"
    path.write_text(build_catalog.SOURCE_POLICY_PATH.read_text() + "\n[[unknown]]\n")
    with pytest.raises(SystemExit, match="invalid SWECOV source policy"):
        build_catalog._load_source_policy(path)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("flavor", "provider", "../escape"),
        ("flavor", "provider", "/absolute"),
        ("provider_scope", "provider", "../escape"),
        ("provider_scope", "provider", "/absolute"),
        ("flavor", "register", "../escape"),
        ("flavor", "register", "Not a slug"),
        ("flavor", "variant_slug", "../escape"),
        ("flavor", "variant_slug", "Not a slug"),
    ],
)
def test_source_policy_rejects_unsafe_output_and_catalog_slugs(
    tmp_path: Path, section: str, field: str, value: str
) -> None:
    text = build_catalog.SOURCE_POLICY_PATH.read_text(encoding="utf-8")
    raw = tomllib.loads(text)
    old = raw[section][0][field]
    marker = f"[[{section}]]"
    start = text.index(marker)
    replacement = f"{field} = {json.dumps(value)}"
    text = text[:start] + text[start:].replace(
        f"{field} = {json.dumps(old, ensure_ascii=False)}", replacement, 1
    )
    path = tmp_path / "source_policy.toml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(SystemExit, match="invalid SWECOV source policy"):
        build_catalog._load_source_policy(path)


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
    import shutil

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
    records = build_catalog._steward_load_db(db).get(
        ("inera/bestallda-prover", "HISTORIC"), []
    )
    assert [(r["valid_from"], r["valid_to"]) for r in records] == expected


@pytest.mark.parametrize("curated_backing", [False, True])
def test_inventory_shared_alias_cannot_use_invalid_or_curated_source_backing(
    tmp_path: Path,
    flavored_db: Path,
    curated_backing: bool,
) -> None:
    import shutil

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
    assert ("inera/bestallda-prover", "HISTORIC") not in build_catalog._steward_load_db(
        db
    )


@pytest.mark.parametrize(
    "case",
    ["source_replacement", "curated_additive", "source_no_base", "year_independent"],
)
def test_inventory_column_windows_preserve_source_and_curated_scope(
    tmp_path: Path,
    flavored_db: Path,
    case: str,
) -> None:
    import shutil

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
    records = build_catalog._steward_load_db(db)
    actual = {
        (r["col"], r["valid_from"], r["valid_to"], r["period_scope"])
        for (register, _), rows in records.items()
        if register == "inera/bestallda-prover"
        for r in rows
        if r["vslug"] == "t-kolumn"
    }
    assert actual == expected
    if case == "source_replacement":
        assert {r[1:3] for r in actual} == {("2019-03-01", "2019-08-31")}


@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"unmapped_reason": " "},
        {"unmapped_reason": "Reviewed", "graft": "sos/bu"},
        {"unmapped_reason": "Reviewed", "target": "sos/bu/bu-insats"},
    ],
)
def test_unmapped_source_route_requires_reason_and_no_catalog_target(
    changes: dict,
) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        build_catalog.SourceRoute.model_validate(
            {
                "category": "Socialstyrelsen",
                "detail": "Barn",
                "status": "unmapped",
                **changes,
            }
        )


@pytest.mark.parametrize("assigned", [False, True])
def test_inventory_unmapped_route_retains_holdings_without_inferred_assignment(
    tmp_path: Path, flavored_db: Path, monkeypatch: pytest.MonkeyPatch, assigned: bool
) -> None:
    reason = "Separate source variants do not establish this combined delivery's owner."
    route = build_catalog.SourceRoute.model_validate(
        {
            "category": "Inera/1177",
            "detail": "Ordered tests",
            "status": "unmapped",
            "unmapped_reason": reason,
        }
    )
    policy = build_catalog.SourcePolicy.model_construct(route=[route])
    monkeypatch.setitem(
        build_catalog.MAPPING,
        (route.category, route.detail),
        policy.mapping()[(route.category, route.detail)],
    )
    overlay = (
        '[[assign]]\ntable = "T2019"\nregister_variant = "inera/bestallda-prover/_default"\n'
        if assigned
        else ""
    )
    steward = _run_inventory(
        tmp_path, flavored_db, overlay, "T2019", ["T_kolumn", "Unknown"]
    )
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
        assert build_catalog._steward_scope(
            "Inera/1177/Ordered tests", policy.mapping()[(route.category, route.detail)]
        ) == (set(), set())
