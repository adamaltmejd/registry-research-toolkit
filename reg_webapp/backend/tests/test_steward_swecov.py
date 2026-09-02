"""Structural regression guard for the committed SWECOV steward catalog.

The catalog (`reg_webapp/stewards/swecov/`) is generated against a flavored
reg_meta DB (see its README). A FULL boot/admission check needs that DB and is
the maintainer's real-data validation, not a CI fixture — so this guards the
committed artifact WITHOUT a DB: it must load as a steward, pass structural
validation, and be self-consistent (well-formed FQIDs, every binding under its
source's register, a pinned representation, no duplicate bindings). A corrupt or
schema-drifted regenerate fails here.

The one DB-BACKED test is REFACTOR_SPEC.md §12's inventory ↔ DB consistency
gate over the committed `inventory.toml`. It carries the `release` marker: the
flavored DB is a release asset, so the default gate (`-m "not integration and
not release"`) DESELECTS it rather than skipping it — a deselected test never
enters the JUnit report, while a skipped one reads as missing evidence. The
maintainer runs it when regenerating the inventory or cutting a reg_meta
release:

    REG_META_DB=<extend-db output dir> pytest --run-release -m release

Opting in and then finding no DB is a misconfigured run, not a clean pass, so
the fixture FAILS there instead of skipping. The webapp boot gate is the hard
line; this is the maintainer's early warning.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import reg_meta.db
from reg_meta.inventory import load_inventory
from reg_meta.inventory_check import check_inventory, unresolved_message
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural

from reg_webapp.stewards import load_steward

_STEWARDS_DIR = Path(__file__).resolve().parents[2] / "stewards"
_SWECOV = _STEWARDS_DIR / "swecov"


def test_swecov_steward_toml_loads() -> None:
    steward = load_steward("swecov", root=_STEWARDS_DIR)
    assert steward.id == "swecov"
    assert steward.has_catalog_filter  # ships a steward.project_data.json


@pytest.fixture(scope="module")
def project() -> ProjectData:
    raw = (_SWECOV / "steward.project_data.json").read_text(encoding="utf-8")
    data = json.loads(raw)
    result = validate_structural(data)
    errors = [i for i in result.issues if i.level == "error"]
    assert not errors, f"structural errors: {[(i.code, i.path) for i in errors]}"
    return ProjectData.model_validate(data)


def test_catalog_shape(project: ProjectData) -> None:
    assert project.steward == "swecov"
    assert project.sources, "catalog must declare at least one source"
    assert not project.panels, "a steward catalog is sources-only (no panels)"


def test_bindings_are_self_consistent(project: ProjectData) -> None:
    seen: set[tuple[str, str, str | None]] = set()
    for source in project.sources:
        coord = source.register_variant.split("/")
        assert len(coord) == 3, (
            f"register_variant not 3-part: {source.register_variant}"
        )
        reg_prefix = "/".join(coord[:2])
        assert source.bindings, f"source {source.name} has no bindings"
        for binding in source.bindings:
            fqid = binding.variable.split("/")
            assert len(fqid) == 3, f"binding FQID not 3-part: {binding.variable}"
            # A binding lives under its source's register (#206 admission coord).
            assert "/".join(fqid[:2]) == reg_prefix, (
                f"binding {binding.variable} outside register {reg_prefix}"
            )
            # The generator pins the resolved delivery column on every binding —
            # the load-bearing property that keeps `_default` resolution
            # unambiguous (no co-existing-column drift).
            assert binding.representation, (
                f"binding {binding.variable} missing representation"
            )
            key = (source.register_variant, binding.variable, binding.representation)
            assert key not in seen, f"duplicate binding {key}"
            seen.add(key)


def test_stale_hreg_grouping_source_is_removed(
    project: ProjectData,
) -> None:
    bindings = {
        (source.register_variant, binding.variable, binding.representation)
        for source in project.sources
        for binding in source.bindings
    }
    source_names = {source.name for source in project.sources}

    assert "swecov.hreg-sun-groupings._default" not in source_names
    assert not any(
        variant == "swecov/hreg-sun-groupings/_default"
        or variable.startswith("swecov/hreg-sun-groupings/")
        for variant, variable, _representation in bindings
    )
    assert {
        (
            "swecov/adress-sarskilt-boende/_default",
            "swecov/adress-sarskilt-boende/personnr",
            "personnr",
        ),
        (
            "swecov/adress-sarskilt-boende/_default",
            "swecov/adress-sarskilt-boende/utdadr2",
            "UtdAdr2",
        ),
        (
            "swecov/population/_default",
            "swecov/population/personnr",
            "PersonNr",
        ),
        (
            "swecov/population/_default",
            "swecov/population/indexpop",
            "IndexPop",
        ),
    } <= bindings


@pytest.fixture(scope="module")
def flavored_conn():
    """A read-only connection to the flavored SWECOV catalog DB.

    The deployment's reg_meta asset IS the flavored `extend-db` output, pointed
    at by `REG_META_DB` (REFACTOR_SPEC.md §11), so that env var is the seam
    here too — the inventory binds steward-only providers (`swecov`,
    `region-*`, …) that the plain global release DB does not contain, and
    checking against the wrong DB would report the whole flavor as stranded.

    Nothing here skips. The `release` marker on the only test using this
    fixture is what keeps the flavored DB optional, so reaching this code means
    the runner asked for the gate — an unset `REG_META_DB`, a path naming no
    DB, or a schema-incompatible one (`open_db` raises, exactly as boot would)
    are all misconfigured runs, and a skip would report the gate as clean
    without ever having run it."""
    db_dir = os.environ.get("REG_META_DB")
    if not db_dir:
        pytest.fail(
            "no flavored SWECOV DB: set REG_META_DB to the extend-db output dir "
            "(see stewards/swecov/README.md) to run the §12 consistency gate"
        )
    db_path = reg_meta.db.db_path_from_args(db_dir)
    if not db_path.is_file():
        pytest.fail(f"REG_META_DB names no catalog DB at {db_path}")
    conn = reg_meta.db.open_db(db_path)
    try:
        yield conn
    finally:
        conn.close()


@pytest.mark.release
def test_every_inventory_mapping_resolves_against_the_flavored_db(
    flavored_conn,
) -> None:
    """§12's standing gate over the committed inventory: every mapping's
    `(register_variant, variable FQID, representation)` resolves against the DB
    the SWECOV deployment serves. Zero unresolved mappings is the baseline — a
    catalog release that renames a slug strands mappings silently otherwise,
    and the deployment would refuse to boot.

    `release`-marked because the flavored DB is a release asset: the default
    gate deselects this test, and `--run-release` with `REG_META_DB` set runs
    it."""
    findings = check_inventory(
        load_inventory(_SWECOV / "inventory.toml"), flavored_conn
    )
    assert not findings, unresolved_message(findings)
