"""Shared support for the SWECOV steward-flavor generator tests (`test_swecov_*.py`).

The generator (`input_data/swecov/build_catalog.py`) is tracked but maintainer-run:
its CSV/workbook inputs are confidential and stay untracked, so these tests feed it
SYNTHETIC columns and a synthetic `holdings_enriched.json` — no real delivery data,
no `derived/` artifact of the maintainer's, no network. Loaded by path (the way
`reg_webapp/backend/tests/conftest.py` reaches `scripts/fixture_db.py`) because
`input_data/` is a seed area, not an importable package.

Its public contract is the `cmd_*` entry points, called in-process with an
`argparse.Namespace`, and the argparse CLI itself (`run_generator`). The authored
routing those entry points read is `reg_webapp/stewards/swecov/source_policy.toml`,
loaded here through the public `reg_meta_build.swecov_policy` model; a test that
needs another routing runs a copied generator layout beside an edited policy
(`copied_layout`) rather than patching the loaded module.
"""

from __future__ import annotations

import argparse
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
from reg_meta_build.swecov_policy import load_source_policy

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

# The committed routing the loaded generator runs under, read through its public
# model rather than the generator's own module-level projections of it.
POLICY = load_source_policy(build_catalog.SOURCE_POLICY_PATH)


def column(name: str, **extra: str) -> dict:
    """An enriched-holding column entry (see `cmd_enrich`)."""
    return {"name": name, "normalized": build_catalog.norm_col(name), **extra}


def synthetic_enriched() -> dict[str, dict]:
    """One synthetic holding per flavor disposition key.

    Each physical table a table selector names gets one column of its own, so
    every disposition entry draws at least one steward-only variable; keys with
    no selector get a single-column holding.
    """
    tables: dict[str, dict[str, list[str]]] = defaultdict(dict)
    variant_tables = POLICY.variant_tables()
    for key, prov_slug, _, reg_key, _, var_key, *_ in POLICY.disposition():
        selector = variant_tables.get((prov_slug, reg_key, var_key))
        for table in sorted(selector or ("T",)):
            tables[key][table] = [f"{table}_kolumn"]

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


def run_inventory(
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


def inventory_worklist(tmp_path: Path) -> dict:
    """The worklist `run_inventory` leaves beside its CSV, written on every run."""
    return json.loads(
        (tmp_path / "derived" / "inventory_worklist.json").read_text(encoding="utf-8")
    )


def copied_layout(root: Path, policy_text: str) -> Path:
    """A copy of the generator in its repo layout, beside `policy_text` as the
    committed `source_policy.toml` it loads; returns the copied generator."""
    generator = root / "reg_meta_build" / "input_data" / "swecov" / "build_catalog.py"
    generator.parent.mkdir(parents=True)
    shutil.copyfile(GENERATOR, generator)
    policy = root / "reg_webapp" / "stewards" / "swecov" / "source_policy.toml"
    policy.parent.mkdir(parents=True)
    policy.write_text(policy_text, encoding="utf-8")
    return generator


def run_generator(generator: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """The generator's argparse CLI, in a fresh interpreter."""
    return subprocess.run(
        [sys.executable, str(generator), *args],
        capture_output=True,
        text=True,
        check=False,
    )
