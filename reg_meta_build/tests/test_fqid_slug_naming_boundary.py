"""Name-derived `variable.slug` values written by `populate_variable_slugs`, the
extend-db steward overlay's slug engine. Variable source-ID keys at TOML load are
`cases/curation_toml/slugs-provider-variable-key-*`; seed output read back through
the slug loader is `cases/cli/seed-slugs/`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from reg_meta_build.slug_grammar import validate_slug

from reg_meta_build.fqid_slugs import (
    populate_variable_slugs,
)

if TYPE_CHECKING:
    from pathlib import Path


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
        # Exactly at the 60-char cap the slug stays whole; one character over,
        # it is cut back to the last hyphen.
        ("a" * 29 + " " + "b" * 30, "a" * 29 + "-" + "b" * 30),
        ("a" * 30 + " " + "b" * 30, "a" * 30),
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


def test_underivable_text_key_folding_to_a_reserved_token_gets_bare_v(
    tmp_path: Path,
) -> None:
    # `v` + `ARIANTS` folds to the reserved variable token `variants`, so even the
    # last resort is unaddressable and the slug falls back to a bare `v`.
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
    conn.execute("UPDATE variable SET provider_key = 'ARIANTS'")
    conn.commit()
    populate_variable_slugs(conn, tmp_path)
    [slug] = [row[0] for row in conn.execute("SELECT slug FROM variable")]
    assert slug == "v"
    validate_slug(slug, "variable")
