"""Slug boundary cases: variable source-ID keys at TOML load, seed output read back
through the public slug loader, and name-derived `variable.slug` values in the
populated artifact."""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from reg_meta.errors import RegMetaError
from reg_meta.fqid import validate_slug

from reg_meta_build.fqid_slugs import (
    GLOBAL_FREEZE_STATE_FILE,
    load_provider_toml,
    load_slug_dir,
    populate_variable_slugs,
    seed_all,
)

if TYPE_CHECKING:
    from pathlib import Path


# ---------------------------------------------------------------------------
# Variable source-ID keys (`<RegisterId>.<VarId>[.<discriminator>]`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_id",
    [
        "34.10",  # SCB integer VarId
        "34.10.kon",  # split sibling
        "5028920659479690770.ALDER",  # SOS: minted register id, text VarId
        "123.FOD_DATUMN.heltal",  # text VarId in a split-sibling key
    ],
)
def test_variable_key_shapes_load(tmp_path: Path, source_id: str) -> None:
    path = tmp_path / "scb.toml"
    path.write_text(f'[variable."{source_id}"]\nslug = "kon"\n', encoding="utf-8")
    [entry] = load_provider_toml(path)
    assert (entry.kind, entry.source_id, entry.slug) == ("variable", source_id, "kon")


@pytest.mark.parametrize(
    ("source_id", "message"),
    [
        ("034.10", "RegisterId must be an integer"),
        ("1.010", "numeric VarId must be in canonical"),  # `1.10` / `1.010` alias
        ("1.", "VarId segment is empty"),
        ("1.10.", "split-sibling discriminator is empty"),
        ("1", "expected"),
        ("1.10.kon.extra", "expected"),
    ],
)
def test_malformed_variable_key_rejected_at_load(
    tmp_path: Path, source_id: str, message: str
) -> None:
    path = tmp_path / "scb.toml"
    path.write_text(f'[variable."{source_id}"]\nslug = "kon"\n', encoding="utf-8")
    with pytest.raises(RegMetaError) as exc:
        load_provider_toml(path)
    assert exc.value.code == "slug_toml_invalid"
    assert message in exc.value.message


# ---------------------------------------------------------------------------
# Seed output read back as generated pins
# ---------------------------------------------------------------------------


def _pin_register(out: Path) -> None:
    """Author LISA's register file and pin the scb zone, so `load_slug_dir` reads
    the seeded `lisa.auto.toml` back as LISA's generated pins."""
    (out / "registers" / "scb" / "lisa.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "1"\n',
        encoding="utf-8",
    )
    (out / GLOBAL_FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")


def _variable_pins(out: Path) -> dict[str, str | None]:
    return {e.source_id: e.slug for e in load_slug_dir(out) if e.kind == "variable"}


def test_seeded_pins_load_as_register_variables(tmp_path: Path) -> None:
    out = tmp_path / "curation"
    seed_all(build_slugged_db(), out)
    _pin_register(out)
    assert _variable_pins(out) == {"1.44": "kon"}


def test_reseed_keeps_first_sight_pin(tmp_path: Path) -> None:
    conn = build_slugged_db()
    out = tmp_path / "curation"
    seed_all(conn, out)
    conn.execute("UPDATE variable SET slug = 'changed' WHERE provider_key = '44'")
    seed_all(conn, out)
    body = tomllib.loads(
        (out / "registers" / "scb" / "lisa.auto.toml").read_text(encoding="utf-8")
    )
    assert body == {"variable": [{"native_id": "1.44", "slug": "kon"}]}
    _pin_register(out)
    assert _variable_pins(out) == {"1.44": "kon"}


# ---------------------------------------------------------------------------
# Name-derived slugs (the delivery column folds to nothing, so the name decides)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "slug"),
    [
        # A pure measurement-unit parenthetical is noise and is dropped.
        ("Sockerbetor (areal i hektar)", "sockerbetor"),
        ("Träda (areal i ha)", "trada"),
        ("Övriga växtslag (areal i ha)", "ovriga-vaxtslag"),
        # A distinguishing parenthetical carries signal and is kept.
        ("Utbildningsnivå (3 positioner)", "utbildningsniva-3-positioner"),
        ("Kostnad (landsting)", "kostnad-landsting"),
        ("Resultat (SRU)", "resultat-sru"),
        # A name that is only a unit parenthetical keeps the raw name.
        ("(areal i ha)", "areal-i-ha"),
        # Stripping would leave a reserved word / a period: keep the raw name.
        ("Class (procent)", "class-procent"),
        ("(kr) 2020", "kr-2020"),
        # Truncating to the 60-char cap would leave the reserved `class`: keep
        # the full slug rather than store an unaddressable one.
        ("Class " + "x" * 70, "class-" + "x" * 70),
    ],
)
def test_name_derived_slug(tmp_path: Path, name: str, slug: str) -> None:
    conn = build_slugged_db(variable=(name, 44, 1001, "..."))
    populate_variable_slugs(conn, tmp_path)
    stored = conn.execute(
        "SELECT slug FROM variable WHERE provider_key = '44'"
    ).fetchone()[0]
    assert stored == slug


def test_underivable_text_keyed_variable_gets_folded_v_provider_key(
    tmp_path: Path,
) -> None:
    # A non-SCB provider keys variables by name (TEXT provider_key). When neither
    # the column nor the name yields a slug (both lead with a digit), the
    # last-resort `v<provider_key>` must still be folded into the slug grammar.
    conn = build_slugged_db(variable=None)
    add_variable(conn, register_id=1, var_id=200, name="3D-område")
    add_state(
        conn,
        register_id=1,
        var_id=200,
        register_variant_id=10,
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        delivery_column_name="3DOMR",
    )
    conn.execute("UPDATE variable SET provider_key = 'Födelseår_X'")
    conn.commit()
    populate_variable_slugs(conn, tmp_path)
    [slug] = [row[0] for row in conn.execute("SELECT slug FROM variable")]
    assert slug == "vfodelsear-x"
    validate_slug(slug, "variable")
