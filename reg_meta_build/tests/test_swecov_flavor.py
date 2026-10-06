"""SWECOV generator: the flavor stage (`cmd_flavor`) and the CLI's default inventory.

`cmd_flavor` projects the enriched holdings named by the committed flavor disposition
into curated-provider TOMLs plus their FQID slug pins; these tests read those emitted
files. Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import json
import tomllib
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    POLICY,
    build_catalog,
    column as _column,
    copied_layout,
    run_generator,
    synthetic_enriched as _synthetic_enriched,
)
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
    from pathlib import Path


def test_default_csv_uses_newest_full_inventory_or_requires_argument(
    tmp_path: Path,
) -> None:
    """Without `--csv` the CLI reads the newest `SWECOV_variables_full_*.csv`
    beside the script, and with none there `--csv` becomes a required argument.
    Each candidate inventory names a category of its own, so the normalized
    holdings say which file was read."""
    generator = copied_layout(
        tmp_path, build_catalog.SOURCE_POLICY_PATH.read_text(encoding="utf-8")
    )
    old_name = generator.parent / "SWECOV_variables_2099-01-01.csv"
    older = generator.parent / "SWECOV_variables_full_2026-08-01.csv"
    newest = generator.parent / "SWECOV_variables_full_2026-09-13.csv"
    for path in (old_name, older, newest):
        path.write_text(
            f"Category,Detail,Table\n{path.stem},Detail,T2020,Kolumn\n",
            encoding="utf-8",
        )

    matching = run_generator(generator, "normalize")
    assert matching.returncode == 0, matching.stderr
    assert list(json.loads(matching.stdout)) == [f"{newest.stem} / Detail"]

    older.unlink()
    newest.unlink()
    required = run_generator(generator, "normalize")
    assert required.returncode == 2
    assert "the following arguments are required: --csv" in required.stderr


# --- cmd_flavor: vintage-spelling grouping ------------------------------------


def _flavor_register(tmp_path: Path, columns: list[dict]) -> dict:
    """`cmd_flavor` with `columns` as the `Ordered tests` holding (a disposition
    entry with no table selector, so the holding's full column list is drawn);
    returns the register the emitted `inera.toml` carries for it."""
    enriched = _synthetic_enriched()
    enriched["Inera/1177/Ordered tests"]["columns"] = columns
    root = _run_flavor(tmp_path, enriched)
    provider = tomllib.loads(
        (root / "providers" / "inera.toml").read_text(encoding="utf-8")
    )
    return next(r for r in provider["register"] if r["key"] == "bestallda-prover")


def test_spelling_variants_become_one_state_carrying_the_other_columns(
    tmp_path: Path,
) -> None:
    """Two spellings of one column in one delivery.

    They differ only in punctuation, so they are ONE steward variable — and they
    are CO-DELIVERED, so they are ONE state whose `aliases` carry the other
    literal column (extend-db gives each its own `variable_alias` + window row,
    keeping it orderable: steward README → "Near-duplicate physical columns").
    """
    variables = _flavor_register(
        tmp_path,
        [
            _column("Covid-19 antikroppar", data_type="varchar", description="IgG"),
            _column("Covid_19_antikroppar"),
            _column("P1105_LopNr_PERSONNR"),
            _column("Provdatum"),
        ],
    )["variable"]

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
        for s in v["state"]
        for column in (s["column"], *s.get("aliases", ()))
    } == {
        "Covid-19 antikroppar",
        "Covid_19_antikroppar",
        "PERSONNR",
        "Provdatum",
    }
    # The emitter writes `is_identifier = true` and omits the key otherwise.
    assert [v.get("is_identifier", False) for v in variables] == [False, True, False]
    covid = variables[0]
    # Key, name, description and data_type come from the first-seen spelling.
    assert covid["name"] == "Covid-19 antikroppar"
    assert covid["description"] == "IgG"
    # ONE state: the spellings are co-delivered, so the other one is an alias —
    # not a second state needing a fake `value_set_version_label` to survive
    # extend-db's (valid_from, value_set_version_label) uniqueness key.
    assert len(covid["state"]) == 1
    assert covid["state"][0]["column"] == "Covid-19 antikroppar"
    assert covid["state"][0]["aliases"] == ["Covid_19_antikroppar"]
    assert "value_set_version_label" not in covid["state"][0]
    assert covid["state"][0]["data_type"] == "varchar"
    # A single-spelling variable stays an undiscriminated single state: no type,
    # no window (the emitter omits unset keys).
    assert variables[2]["state"] == [{"column": "Provdatum"}]


def test_columns_differing_in_letters_stay_separate_variables(tmp_path: Path) -> None:
    """The fold groups punctuation/case/diacritics only — not a plural `S`."""
    variables = _flavor_register(
        tmp_path, [_column("AVERAGE_SPENDING"), _column("AVERAGE_SPENDINGS")]
    )["variable"]

    assert [v["key"] for v in variables] == ["average-spending", "average-spendings"]
    assert [len(v["state"]) for v in variables] == [1, 1]


def test_kalla_routes_canonical_columns_out_but_keeps_provenance_columns(
    tmp_path: Path,
) -> None:
    variables = _flavor_register(
        tmp_path,
        [
            _column("Konstruerad"),
            _column("Sysselsattning", kalla="LISA"),
            _column("Stodbelopp", kalla="Inrapporterat från Tillväxtverket"),
        ],
    )["variable"]

    assert [v["name"] for v in variables] == ["Konstruerad", "Stodbelopp"]


# --- cmd_flavor: the disposition's register/variant emission -----------------


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


def _policy_without_provider(provider: str) -> str:
    """The committed source policy minus every `[[flavor]]` entry of `provider`."""
    text = build_catalog.SOURCE_POLICY_PATH.read_text(encoding="utf-8")
    blocks = text.split("\n[[")
    kept = [
        block
        for block in blocks
        if not (
            block.startswith("flavor]]") and f'\nprovider = "{provider}"\n' in block
        )
    ]
    assert len(kept) < len(blocks)
    return "\n[[".join(kept)


def test_removed_provider_refuses_stale_toml_before_writing(tmp_path: Path) -> None:
    """A provider TOML the current disposition no longer emits blocks the run:
    the CLI, under a policy without that provider, names the file and writes no
    provider output at all."""
    root = _run_flavor(tmp_path, _synthetic_enriched())
    providers_dir = root / "providers"
    stale = providers_dir / "swedbank.toml"
    assert stale.is_file()
    control = next(path for path in providers_dir.glob("*.toml") if path != stale)
    control.write_text("must remain untouched\n", encoding="utf-8")
    generator = copied_layout(tmp_path / "layout", _policy_without_provider("swedbank"))

    result = run_generator(
        generator, "--csv", str(root / "SWECOV_variables_2025-12-11.csv"), "flavor"
    )

    assert result.returncode == 1
    assert "swedbank.toml" in result.stderr
    assert "Review and remove obsolete files" in result.stderr
    assert stale.is_file()
    assert control.read_text(encoding="utf-8") == "must remain untouched\n"


def test_flavor_rejects_stale_declared_table_before_writing(tmp_path: Path) -> None:
    enriched = _synthetic_enriched()
    entry = next(
        entry
        for entry in POLICY.disposition()
        if (entry[1], entry[3], entry[5]) in POLICY.variant_tables()
    )
    table = POLICY.variant_tables()[entry[1], entry[3], entry[5]][0]
    enriched[entry[0]]["table_columns"].pop(table)
    with pytest.raises(SystemExit, match=r"table\(s\) absent from holding"):
        _run_flavor(tmp_path, enriched)
    assert not (tmp_path / "providers").exists()
