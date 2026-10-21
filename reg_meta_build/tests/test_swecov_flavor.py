"""SWECOV generator: the flavor stage (`cmd_flavor`).

`cmd_flavor` projects the enriched holdings named by the committed `[[flavor]]` entries
into curated-provider TOMLs plus their FQID slug pins; these tests read those emitted
files. Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import shutil
import tomllib
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    SPEC,
    SPEC_DIR,
    column as _column,
    flavor_generator,
    flavor_slug_dir,
    run_flavor,
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


# --- cmd_flavor: vintage-spelling grouping ------------------------------------


def _flavor_register(
    tmp_path: Path, columns: list[dict], mapping: dict | None = None
) -> dict:
    """`cmd_flavor` with `columns` (and the route `mapping`, if given) as the
    `Ordered tests` holding (a `[[flavor]]` entry with no `tables`, so the
    holding's full column list is drawn); returns the register the emitted
    `inera.toml` carries for it."""
    enriched = _synthetic_enriched()
    enriched["Inera/1177/Ordered tests"]["columns"] = columns
    if mapping is not None:
        enriched["Inera/1177/Ordered tests"]["mapping"] = mapping
    root = run_flavor(tmp_path, enriched)
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


_KALLA_COLUMNS = [
    _column("Konstruerad"),
    _column("Sysselsattning", kalla="LISA"),
    _column("Stodbelopp", kalla="Inrapporterat från Tillväxtverket"),
]


def test_kalla_routes_canonical_columns_out_but_keeps_provenance_columns(
    tmp_path: Path,
) -> None:
    variables = _flavor_register(
        tmp_path, _KALLA_COLUMNS, {"status": "flavor", "graft": "scb/lisa"}
    )["variable"]

    assert [v["name"] for v in variables] == ["Konstruerad", "Stodbelopp"]


def test_kalla_column_without_a_graft_register_refuses_before_writing(
    tmp_path: Path,
) -> None:
    """The twin above with no graft on the route: `globals` scans only holdings
    with a graft/target register, so the Källa column would land nowhere. Fails
    if the guard is dropped and the column is again silently skipped (#1302)."""
    with pytest.raises(SystemExit) as refusal:
        _flavor_register(tmp_path, _KALLA_COLUMNS, {"status": "flavor"})

    message = str(refusal.value)
    assert "Inera/1177/Ordered tests: Sysselsattning (Källa=LISA)" in message
    assert "Stodbelopp" not in message
    assert not (tmp_path / "out" / "providers").exists()


def test_flavor_column_lists_keep_kalla_columns_and_flag_sensitive(
    flavor_providers: dict,
) -> None:
    """The committed population flavor keeps its Källa=RTB columns as
    steward variables (its route names no graft) and flags FodelseArMan and
    FodelseArManDag `is_sensitive`. Fails if `keep_columns` stops overriding the
    Källa routing or `sensitive_columns` stops reaching the emitted variable
    (#1302)."""
    register = next(
        r for r in flavor_providers["swecov"]["register"] if r["key"] == "population"
    )
    sensitive = {
        v["name"]: v.get("is_sensitive", False)
        for v in register["variable"]
        if v["variants"] == ["_default"]
    }

    assert {"Kon", "AterAnv", "FelPersonNr"} <= sensitive.keys()
    assert [name for name, flag in sensitive.items() if flag] == [
        "FodelseArMan",
        "FodelseArManDag",
    ]


def test_flavor_column_list_naming_an_absent_column_refuses(
    tmp_path: Path,
) -> None:
    """A keep/sensitive name the holding no longer delivers refuses before any
    provider TOML is written, rather than silently flagging nothing."""
    enriched = _synthetic_enriched()
    enriched["RTB/Population"]["columns"] = [
        c for c in enriched["RTB/Population"]["columns"] if c["name"] != "FodelseArMan"
    ]
    with pytest.raises(SystemExit) as refusal:
        run_flavor(tmp_path, enriched)

    assert "absent from holding 'RTB/Population': ['FodelseArMan']" in str(
        refusal.value
    )
    assert not (tmp_path / "out" / "providers").exists()


# --- cmd_flavor: the flavors' register/variant emission ---------------------


@pytest.fixture(scope="module")
def flavor_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One generator run for every test that reads its provider TOMLs."""
    return run_flavor(tmp_path_factory.mktemp("flavor"), _synthetic_enriched())


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
    """Inera's two 1177 deliveries are disjoint schemas (see the scope.toml
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
    slug_dir = flavor_slug_dir(flavor_root)
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
    root = run_flavor(tmp_path, _synthetic_enriched())
    providers_dir = root / "providers"
    before = {path.name: path.read_bytes() for path in providers_dir.glob("*.toml")}

    flavor_generator(tmp_path).cmd_flavor(
        argparse.Namespace(
            csv=tmp_path / "SWECOV_variables_2025-12-11.csv", spec=SPEC_DIR, out=root
        )
    )

    assert {
        path.name: path.read_bytes() for path in providers_dir.glob("*.toml")
    } == before


def _spec_without_provider(root: Path, provider: str) -> Path:
    """A copy of the committed spec whose scope lacks every `[[flavor]]` entry
    of `provider`."""
    spec = root / "spec"
    shutil.copytree(SPEC_DIR, spec)
    text = (spec / "scope.toml").read_text(encoding="utf-8")
    blocks = text.split("\n[[")
    kept = [
        block
        for block in blocks
        if not (
            block.startswith("flavor]]") and f'\nprovider = "{provider}"\n' in block
        )
    ]
    assert len(kept) < len(blocks)
    (spec / "scope.toml").write_text("\n[[".join(kept), encoding="utf-8")
    return spec


def test_removed_provider_refuses_stale_toml_before_writing(tmp_path: Path) -> None:
    """A provider TOML the current spec no longer emits blocks the run:
    the CLI, under a spec without that provider, names the file and writes no
    provider output at all."""
    root = run_flavor(tmp_path, _synthetic_enriched())
    providers_dir = root / "providers"
    stale = providers_dir / "swedbank.toml"
    assert stale.is_file()
    control = next(path for path in providers_dir.glob("*.toml") if path != stale)
    control.write_text("must remain untouched\n", encoding="utf-8")
    spec = _spec_without_provider(tmp_path, "swedbank")

    result = run_generator(
        tmp_path / "reg_webapp" / "stewards" / "swecov" / "build_catalog.py",
        "--csv",
        str(tmp_path / "SWECOV_variables_2025-12-11.csv"),
        "--spec",
        str(spec),
        "flavor",
        "--out",
        str(root),
    )

    assert result.returncode == 1
    assert "swedbank.toml" in result.stderr
    assert "Review and remove obsolete files" in result.stderr
    assert stale.is_file()
    assert control.read_text(encoding="utf-8") == "must remain untouched\n"


def test_flavor_rejects_stale_declared_table_before_writing(tmp_path: Path) -> None:
    enriched = _synthetic_enriched()
    flavor = next(flavor for flavor in SPEC.scope.flavors if flavor.tables)
    enriched[flavor.holding]["table_columns"].pop(flavor.tables[0])
    with pytest.raises(SystemExit, match=r"table\(s\) absent from holding"):
        run_flavor(tmp_path, enriched)
    assert not (tmp_path / "out" / "providers").exists()
