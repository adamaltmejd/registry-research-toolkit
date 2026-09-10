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
import sys
from collections import defaultdict
from pathlib import Path

import pytest
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
    """The two Covid spellings of `inera/vardguiden/bestallda-prover`.

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
    assert _variant_names(flavor_inventory, "inera", "vardguiden") == {
        "samtal": "Samtal 1177",
        "bestallda-prover": "Beställda prover",
    }
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
        r for r in inventory.registers if (r.provider, r.key) == ("inera", "vardguiden")
    )
    variant = next(v for v in register.variants if v.key == "bestallda-prover")
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
