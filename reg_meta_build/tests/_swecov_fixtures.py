"""Shared support for the SWECOV steward-flavor generator tests (`test_swecov_*.py`).

The generator (`input_data/swecov/build_catalog.py`) is tracked but maintainer-run:
its CSV/workbook inputs are confidential and stay untracked, so these tests feed it
SYNTHETIC columns and a synthetic `holdings_enriched.json` — no real delivery data,
no `derived/` artifact of the maintainer's, no network. Loaded by path because
`input_data/` is a seed area, not an importable package.

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
import sqlite3
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pytest
from reg_meta_build.db import DDL, SCHEMA_VERSION
from reg_meta_build.steward_spec import load_steward_spec

GENERATOR = (
    Path(__file__).resolve().parents[1] / "input_data" / "swecov" / "build_catalog.py"
)
_spec = importlib.util.spec_from_file_location("swecov_build_catalog", GENERATOR)
assert _spec and _spec.loader
build_catalog = importlib.util.module_from_spec(_spec)
# Registered before exec: the module defines `@dataclass`es, which resolve their
# annotations through `sys.modules[cls.__module__]`.
sys.modules[_spec.name] = build_catalog
_spec.loader.exec_module(build_catalog)

# The committed steward spec the loaded generator runs under, read through its
# public model.
SPEC_DIR = build_catalog.SPEC_DIR
SPEC = load_steward_spec(SPEC_DIR)


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


def write_flavored_db(db_path: Path) -> Path:
    """A minimal FLAVORED DB holding `Beställda prover` the way `extend-db`
    writes a grouped variable: ONE `variable_state` on the first spelling, a
    `variable_alias` for every column of that state, and — because these two
    are co-delivered — a `variable_alias_window` for each, the state's own
    included (that write shape is pinned by `test_extend_db.py` →
    `test_co_delivered_aliases_are_one_state_with_alias_windows`; the rows are
    stated directly here because the resolver's input is the DB, and because
    one row below is deliberately one `extend-db` never writes). `T_kolumn` is
    the single-spelling control: one state, its own alias row, no window.

    Provenance: Python-literal SQL, carried from the pre-split module. It is
    fixture debt that is not mechanical to convert: building it through
    `extend-db` needs a base build and mints hash ids, while tests derive their
    variants by row edits addressing these literal ids, and one row is one
    `extend-db` never writes."""
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
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from,"
            " valid_to, data_type, delivery_column_name) "
            "VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), ?, 902, '0001-01-01', '9999-12-31', 'varchar', ?)",
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
    # A historical case spelling carried by `variable_alias` alone. With no
    # window it is a search-only header the order path never delivers, so it
    # must stay out of the resolution index — a mapping onto it is not
    # supported by resolver-emitted delivery columns.
    conn.execute("INSERT INTO variable_alias VALUES (904, 902, 'T_KOLUMN')")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture(scope="module", name="flavored_db")
def flavored_db_fixture(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One `write_flavored_db` per importing module (import it as
    `flavored_db_fixture`; tests request it as `flavored_db`)."""
    return write_flavored_db(tmp_path_factory.mktemp("db") / "reg_meta_swecov.db")


def run_flavor(
    tmp_path: Path, enriched: dict[str, dict], spec: Path = SPEC_DIR
) -> Path:
    """Run `cmd_flavor` in a synthetic package layout; return its input root
    (`providers/` and, two levels up, `fqid_slugs/swecov/`)."""
    base = tmp_path / "input_data" / "swecov"
    derived = base / "derived"
    derived.mkdir(parents=True)
    (derived / "holdings_enriched.json").write_text(
        json.dumps(enriched), encoding="utf-8"
    )
    build_catalog.cmd_flavor(
        argparse.Namespace(csv=base / "SWECOV_variables_2025-12-11.csv", spec=spec)
    )
    return base


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
    """The worklist `run_inventory` leaves beside its CSV, written on every run."""
    return json.loads(
        (tmp_path / "derived" / "inventory_worklist.json").read_text(encoding="utf-8")
    )


def copied_generator(root: Path) -> Path:
    """A copy of the generator in its repo layout, so its default `--csv` reads
    beside the copy; returns the copied generator."""
    generator = root / "reg_meta_build" / "input_data" / "swecov" / "build_catalog.py"
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
