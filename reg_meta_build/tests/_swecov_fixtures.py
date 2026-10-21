"""Shared support for the SWECOV steward-flavor generator tests (`test_swecov_*.py`).

The generator (`reg_webapp/stewards/swecov/build_catalog.py`) is tracked but
maintainer-run: its CSV/workbook inputs are confidential and live outside the repo, so
these tests feed it SYNTHETIC columns and a synthetic `holdings_enriched.json` — no real
delivery data, no `derived/` artifact of the maintainer's, no network. Loaded by path
because the steward directory is not an importable package.

Its public contract is the `cmd_*` entry points, called in-process with an
`argparse.Namespace`, and the argparse CLI itself (`run_generator`). The authored
decisions those entry points read are the steward spec under
`reg_webapp/stewards/swecov/` (`scope.toml` + `series/*.toml`), loaded here
through the public `reg_meta_build.steward_spec` model; a test that needs other
decisions passes its own spec directory (`--spec`) rather than patching the
loaded module.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
import tomllib
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _extend_db_support import BASE_FIXTURE
from reader_artifacts import FIXTURE_IMPORT_DATE, cached_reader_artifact
from reg_meta_build.db import DB_FILENAME
from reg_meta_build.extend_db import extend_db
from reg_meta_build.id import mint
from reg_meta_build.steward_spec import load_steward_spec

if TYPE_CHECKING:
    from types import ModuleType

GENERATOR = (
    Path(__file__).resolve().parents[2]
    / "reg_webapp"
    / "stewards"
    / "swecov"
    / "build_catalog.py"
)


def load_generator(path: Path, name: str) -> ModuleType:
    """The generator at `path`, imported as module `name`."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Registered before exec: the module defines `@dataclass`es, which resolve
    # their annotations through `sys.modules[cls.__module__]`.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_catalog = load_generator(GENERATOR, "swecov_build_catalog")

# The committed steward spec the loaded generator runs under, read through its
# public model.
SPEC_DIR = build_catalog.SPEC_DIR
SPEC = load_steward_spec(SPEC_DIR)

# The steward provider every flavored DB starts from (`write_flavored_db`).
FLAVORED_TOML = (Path(__file__).parent / "cases" / "swecov" / "inera.toml").read_text(
    encoding="utf-8"
)


def column(name: str, **extra: str) -> dict:
    """An enriched-holding column entry (see `cmd_enrich`)."""
    return {"name": name, "normalized": build_catalog.norm_col(name), **extra}


def synthetic_enriched() -> dict[str, dict]:
    """One synthetic holding per `[[flavor]]` holding of the committed scope.

    Each physical table a flavor's `tables` names gets one column of its own, so
    every flavor draws at least one steward-only variable; a flavor with no
    `tables` gets a single-column holding.
    """
    tables: dict[str, dict[str, list[str]]] = defaultdict(dict)
    for flavor in SPEC.scope.flavors:
        for table in sorted(flavor.tables or ("T",)):
            tables[flavor.holding][table] = [f"{table}_kolumn"]

    enriched = {
        key: {
            "columns": [column(c) for cs in table_columns.values() for c in cs],
            "table_columns": table_columns,
        }
        for key, table_columns in tables.items()
    }
    # A vintage-spelling pair (two punctuations of one column), in a holding with
    # no table selector.
    enriched["Inera/1177/Ordered tests"]["columns"] += [
        column("Covid-19 antikroppar"),
        column("Covid_19_antikroppar"),
    ]
    # Every column a flavor's keep/sensitive list names, the kept ones with a
    # register Källa, so the flavor's column lists resolve against the holding.
    for flavor in SPEC.scope.flavors:
        keep, sensitive = set(flavor.keep_columns), set(flavor.sensitive_columns)
        enriched[flavor.holding]["columns"] += [
            column(c, kalla="RTB") for c in sorted(keep)
        ]
        enriched[flavor.holding]["columns"] += [
            column(c) for c in sorted(sensitive - keep)
        ]
    return enriched


def write_flavored_db(
    directory: Path, provider_toml: str = FLAVORED_TOML, *, provider: str = "inera"
) -> Path:
    """A FLAVORED DB: `provider_toml` (by default `cases/swecov/inera.toml`) as
    the steward provider `provider`, overlaid by `extend_db` onto the synthetic
    global base, so every row is one the builder writes. Each register, variant
    and variable is pinned to its own key as its slug, as the committed steward
    slug pins do for `_default`."""
    providers = directory / "providers"
    providers.mkdir(parents=True)
    (providers / f"{provider}.toml").write_text(provider_toml, encoding="utf-8")
    slugs = directory / "slugs"
    slugs.mkdir()
    (slugs / ".snapshot.json").write_text("{}\n", encoding="utf-8")
    pins = []
    for register in tomllib.loads(provider_toml)["register"]:
        register_id = mint("register", provider, register["key"])
        pins.append(f'[register."{register_id}"]\nslug = "{register["key"]}"\n')
        pins += [
            f'[register_variant."{register_id}.'
            f'{mint("variant", provider, register["key"], variant["key"])}"]\n'
            f'slug = "{variant["key"]}"\n'
            for variant in register["variant"]
        ]
        pins += [
            f'[variable."{register_id}.{key}"]\nslug = "{key}"\n'
            for key in dict.fromkeys(v["key"] for v in register["variable"])
        ]
    (slugs / f"{provider}.toml").write_text("".join(pins), encoding="utf-8")
    extend_db(
        base_db=cached_reader_artifact(
            BASE_FIXTURE,
            "catalog",
            identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
        ),
        providers_dir=providers,
        db_dir=directory / "db",
        steward="swecov",
        slug_dir=slugs,
        diagnostic=True,
    )
    return directory / "db" / DB_FILENAME


def on_scb(provider_toml: str = FLAVORED_TOML) -> str:
    """`provider_toml` authored for the `scb` provider (pass `provider="scb"`):
    the base's own provider, whose name a steward TOML must repeat."""
    return provider_toml.replace(
        'name = "Inera AB / 1177 Vårdguiden"', 'name = "Statistiska Centralbyrån"', 1
    )


def t_kolumn_window(
    valid_from: str | None,
    valid_to: str | None = None,
    provider_toml: str = FLAVORED_TOML,
    *,
    variants: tuple[str, ...] | None = None,
) -> str:
    """`provider_toml` with the `T_kolumn` state bounded to `valid_from`..`valid_to`
    (`None` leaves a bound open), delivered on `variants` alone when given."""
    bounds = "".join(
        f'    {key} = "{value}"\n'
        for key, value in (("valid_from", valid_from), ("valid_to", valid_to))
        if value is not None
    )
    provider_toml = provider_toml.replace(
        '    column = "T_kolumn"\n', f'    column = "T_kolumn"\n{bounds}', 1
    )
    if variants is None:
        return provider_toml
    return provider_toml.replace(
        '  key = "t-kolumn"\n',
        '  key = "t-kolumn"\n  variants = ['
        + ", ".join(f'"{v}"' for v in variants)
        + "]\n",
        1,
    )


def variant_block(key: str) -> str:
    """Another `[[register.variant]]` of `Beställda prover`, appended to the TOML."""
    return f'\n  [[register.variant]]\n  key = "{key}"\n  name = "{key}"\n'


def owner_block(
    key: str,
    column: str,
    *windows: tuple[str, str | None],
    variants: tuple[str, ...] | None = None,
    name: str | None = None,
) -> str:
    """Another variable of `Beställda prover` (named `name`, by default its key),
    appended to the TOML: one state delivering `column` per
    `(valid_from, valid_to)` window (`None` leaves it open), or one open state
    without windows; `variants` restricts it to those variant keys."""
    lines = [
        "",
        "  [[register.variable]]",
        f'  key = "{key}"',
        f'  name = "{name or key}"',
    ]
    if variants is not None:
        lines.append("  variants = [" + ", ".join(f'"{v}"' for v in variants) + "]")
    for valid_from, valid_to in windows or ((None, None),):
        lines += ["", "    [[register.variable.state]]", f'    column = "{column}"']
        if valid_from is not None:
            lines.append(f'    valid_from = "{valid_from}"')
        if valid_to is not None:
            lines.append(f'    valid_to = "{valid_to}"')
    return "\n".join(lines) + "\n"


# Row ids of the default flavored DB (`mint`, as `extend_db` keys them), for the
# probes that state a row `extend_db` never writes: alias windows with their own
# provenance or column metadata, a year-independent state on a dated variant.
# Those are global-build shapes; moving them to a global readable source
# (`write_resolved_catalog`) is what remains of #1654.
REGISTER_ID = mint("register", "inera", "bestallda-prover")
VARIANT_ID = mint("variant", "inera", "bestallda-prover", "_default")
T_KOLUMN_ID = mint("variable", "inera", "bestallda-prover", "t-kolumn")


@pytest.fixture(scope="module", name="flavored_db")
def flavored_db_fixture(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One `write_flavored_db` per importing module (import it as
    `flavored_db_fixture`; tests request it as `flavored_db`)."""
    return write_flavored_db(tmp_path_factory.mktemp("flavored"))


def run_flavor(
    tmp_path: Path, enriched: dict[str, dict], spec: Path = SPEC_DIR
) -> Path:
    """Run `cmd_flavor` of a generator copied into a synthetic repo at `tmp_path`
    (`copied_generator`), so its slug pins land in that repo's
    `reg_meta_build/fqid_slugs/swecov/`, not the checkout's; `enriched` is the
    `--out` dir's `derived/holdings_enriched.json`. Returns the `--out` dir
    (`providers/`)."""
    out = tmp_path / "out"
    derived = out / "derived"
    derived.mkdir(parents=True)
    (derived / "holdings_enriched.json").write_text(
        json.dumps(enriched), encoding="utf-8"
    )
    flavor_generator(tmp_path).cmd_flavor(
        argparse.Namespace(
            csv=tmp_path / "SWECOV_variables_2025-12-11.csv", spec=spec, out=out
        )
    )
    return out


def flavor_generator(root: Path) -> ModuleType:
    """The generator copied into the synthetic repo at `root` (once), imported."""
    generator = root / "reg_webapp" / "stewards" / "swecov" / "build_catalog.py"
    if not generator.exists():
        copied_generator(root)
    return load_generator(generator, f"swecov_build_catalog_{abs(hash(root))}")


def flavor_slug_dir(out: Path) -> Path:
    """The slug pins `run_flavor` (whose `--out` is `out`) wrote."""
    return out.parent / "reg_meta_build" / "fqid_slugs" / "swecov"


DEFAULT_VARIANT = "inera/bestallda-prover/_default"


def write_spec(
    spec_dir: Path,
    census: Path,
    series: dict[str, str],
    *,
    scope: str = "",
) -> Path:
    """A steward spec over the synthetic `census`: `scope.toml` (its
    `census_sha256` taken from the census bytes, plus `scope`) and one
    `series/<id>.toml` per `series` entry."""
    (spec_dir / "series").mkdir(parents=True)
    sha = hashlib.sha256(census.read_bytes()).hexdigest()
    (spec_dir / "scope.toml").write_text(
        f'census_sha256 = "{sha}"\n{scope}', encoding="utf-8"
    )
    for series_id, body in series.items():
        (spec_dir / "series" / f"{series_id}.toml").write_text(body, encoding="utf-8")
    return spec_dir


def one_table_series(
    table: str,
    *,
    edition: str | None = None,
    variants: tuple[str, ...] = (DEFAULT_VARIANT,),
    header: str = "",
    decisions: str = "",
    series_id: str = "s",
) -> str:
    """The body of a series whose one member is `table`, admitting `variants`.

    `edition` is the member's TOML edition value (or a `period_scope = ...`
    line); by default the table name's trailing year. `header` adds
    series-level keys (`edition_spelling`); `decisions` appends `[[column]]`
    blocks."""
    if edition is None:
        edition = table[-4:]
    lines = [f'id = "{series_id}"']
    if variants:
        lines.append('register = "{}"'.format("/".join(variants[0].split("/")[:2])))
    lines.append("variants = [" + ", ".join(f'"{v}"' for v in variants) + "]")
    if header:
        lines.append(header.rstrip("\n"))
    lines += ["", "[[table]]", f'table = "{table}"']
    lines.append(
        edition if edition.startswith("period_scope") else f"edition = {edition}"
    )
    return "\n".join(lines) + "\n" + (f"\n{decisions}" if decisions else "")


def run_inventory(
    tmp_path: Path,
    db: Path,
    table: str,
    columns: list[str],
    *,
    holding: tuple[str, str] = ("Inera/1177", "Ordered tests"),
    **series: str | tuple[str, ...],
) -> Path:
    """`write_inventory` (the `inventory` emitter) over a synthetic one-table CSV whose table is the one
    member of series `s` (`one_table_series(table, **series)`); returns the
    steward dir, which is also the spec dir the run reads and writes.

    By default the series admits only `inera/bestallda-prover/_default`, so the
    table's columns resolve against the flavored DB's one register."""
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    csv_path.write_text(
        "Category,Detail,Table\n" + ",".join((*holding, table, *columns)) + "\n",
        encoding="utf-8",
    )
    steward_dir = write_spec(
        tmp_path / "steward", csv_path, {"s": one_table_series(table, **series)}
    )
    build_catalog.write_inventory(
        argparse.Namespace(csv=csv_path, spec=steward_dir, out=steward_dir), db
    )
    return steward_dir


def inventory_worklist(tmp_path: Path) -> dict:
    """The worklist `run_inventory` leaves under its `--out`, written on every run."""
    return json.loads(
        (tmp_path / "steward" / "derived" / "inventory_worklist.json").read_text(
            encoding="utf-8"
        )
    )


def copied_generator(root: Path) -> Path:
    """A copy of the generator in its repo layout under `root`, so the paths it
    derives from its own location (the slug pins under
    `reg_meta_build/fqid_slugs/<steward>/`) stay under `root`; returns the copied
    generator."""
    generator = root / "reg_webapp" / "stewards" / "swecov" / "build_catalog.py"
    generator.parent.mkdir(parents=True)
    shutil.copyfile(GENERATOR, generator)
    return generator


def run_generator(generator: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """The generator's argparse CLI, in a fresh interpreter."""
    return subprocess.run(
        [sys.executable, str(generator), *args],
        capture_output=True,
        text=True,
        check=False,
    )
