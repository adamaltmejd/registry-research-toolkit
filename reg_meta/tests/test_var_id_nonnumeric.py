"""#466: `var_id` is the SCB legacy numeric variable id (= a pure-digit
`provider_key`). SOS variables carry a Swedish *name* as `provider_key` and
curated thin providers carry a delivery *column* token; both are non-numeric, so
the old `CAST(provider_key AS INTEGER)` yielded a meaningless `var_id: 0`. The
digit-guarded `_VAR_ID_EXPR` now emits the numeric id for a pure-digit
`provider_key` and NULL (→ Python None) otherwise.

Builds a synthetic DB directly (the SCB CSV pipeline can only mint numeric
provider_keys) with one variable per provider flavour, each in its own register
(a register belongs to one provider, so provider_key shape is uniform within a
register — no int/None mixing inside a single query result).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from reg_meta.queries import (
    get_varinfo,
    search,
    search_variables_by_classification,
)

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "reg_meta_build" / "tests")
)

from _slugged_db import (  # noqa: E402
    add_register,
    add_state,
    add_variant,
    build_slugged_db,
)

if TYPE_CHECKING:
    import sqlite3

# provider_key flavours: SCB numeric, SOS name, curated column token, plus a
# MIXED leading-digit value that the OLD `CAST` would have truncated to 44 but
# the digit-guard must reject (→ None).
_SCB_KEY = "44"
_SOS_KEY = "Diagnos"  # a Swedish variable name (SOS provider_key shape)
_CURATED_KEY = "diagnos"  # a delivery-column token (curated provider_key shape)
_MIXED_KEY = "44abc"  # leading-digit-but-not-pure-digit


def _insert_variable(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    provider_key: str,
    name: str,
    slug: str,
) -> int:
    """Insert a `variable` with an explicit (possibly non-numeric) provider_key.

    `_slugged_db.add_variable` types `var_id` as `int`; here provider_key is a
    raw string, so go straight to the INSERT (provider_key is TEXT)."""
    cur = conn.execute(
        "INSERT INTO variable (register_id, provider_key, name, slug) "
        "VALUES (?, ?, ?, ?)",
        (register_id, provider_key, name, slug),
    )
    assert cur.lastrowid is not None
    return cur.lastrowid


@pytest.fixture
def db() -> sqlite3.Connection:
    """One SCB register (numeric key), one SOS register (name key), one curated
    register (column-token key), plus a SCB var with a mixed leading-digit key.
    Each variable gets a variant + an open-range state tagged with the shipped
    SUN2020 classification (id 1) so the classification→variables query returns
    them all."""
    # Drop the default LISA variable so we control every provider_key explicitly;
    # keep the default classification (SUN2020, id 1).
    conn = build_slugged_db(variable=None)

    # SCB register (provider_id 1 = SCB) already exists as register 1 (LISA),
    # variant 10. SOS + curated need their own registers/variants/providers.
    add_register(conn, register_id=2, slug="sosreg", name="SOS Register", provider_id=2)
    add_variant(conn, register_variant_id=20, register_id=2, slug="sosvar", name="SOS")
    add_register(
        conn, register_id=3, slug="curreg", name="Curated Register", provider_id=3
    )
    add_variant(
        conn, register_variant_id=30, register_id=3, slug="curvar", name="Curated"
    )

    cls_id = conn.execute(
        "SELECT id FROM classification WHERE slug = 'sun2020'"
    ).fetchone()[0]

    specs = [
        (1, 10, _SCB_KEY, "Kön", "kon"),
        (2, 20, _SOS_KEY, "Diagnos", "diagnos-sos"),
        (3, 30, _CURATED_KEY, "Diagnos curated", "diagnos-cur"),
        (1, 10, _MIXED_KEY, "Mixed", "mixed"),
    ]
    for register_id, variant_id, provider_key, name, slug in specs:
        _insert_variable(
            conn,
            register_id=register_id,
            provider_key=provider_key,
            name=name,
            slug=slug,
        )
        add_state(
            conn,
            register_id=register_id,
            variable_slug=slug,
            register_variant_id=variant_id,
            delivery_column_name=name,
            classification_id=cls_id,
        )

    conn.commit()
    return conn


def test_classification_to_variables_var_id_guard(db: sqlite3.Connection) -> None:
    rows = search_variables_by_classification(db, "sun2020")
    by_name = {r["variable_name"]: r["var_id"] for r in rows}
    # SCB pure-digit provider_key → the numeric var_id (int 44).
    assert by_name["Kön"] == 44
    assert isinstance(by_name["Kön"], int)
    # SOS (name) + curated (column) provider_keys → None, not 0.
    assert by_name["Diagnos"] is None
    assert by_name["Diagnos curated"] is None
    # Mixed leading-digit value is rejected by the pure-digit guard → None,
    # distinguishing it from the old leading-digit CAST (which would give 44).
    assert by_name["Mixed"] is None


def test_search_varname_var_id_guard(db: sqlite3.Connection) -> None:
    # field="varname" is the LIKE-over-name path (no FTS rebuild needed).
    scb = search(db, "Kön", field="varname")["results"]
    assert scb and scb[0]["var_id"] == 44

    sos = search(db, "Diagnos", field="varname")["results"]
    by_name = {r["variable_name"]: r["var_id"] for r in sos}
    assert by_name["Diagnos"] is None
    assert by_name["Diagnos curated"] is None


def test_varinfo_var_id_guard(db: sqlite3.Connection) -> None:
    # SCB var addressable by numeric var_id, surfaces var_id 44.
    scb = get_varinfo(db, "44", register="lisa")
    assert scb and scb[0]["var_id"] == 44

    # SOS var: addressable by name (it has no numeric var_id), surfaces None.
    # The register is resolved by `register.name`, not slug.
    sos = get_varinfo(db, "Diagnos", register="SOS Register")
    assert sos and sos[0]["var_id"] is None

    # Curated var: addressable by name, surfaces None.
    cur = get_varinfo(db, "Diagnos curated", register="Curated Register")
    assert cur and cur[0]["var_id"] is None


def test_mixed_leading_digit_key_is_none(db: sqlite3.Connection) -> None:
    # The guard is pure-digit, NOT leading-digit: "44abc" → None. The old
    # `CAST('44abc' AS INTEGER)` would have yielded 44 (SQLite leading-digit
    # parse) — this is the behaviour the guard fixes.
    mixed = get_varinfo(db, "Mixed", register="lisa")
    assert mixed and mixed[0]["var_id"] is None


def test_none_var_id_renders_blank_not_none() -> None:
    # A None var_id must render as an empty cell, never the literal "None" or
    # "0" — both the table and list renderers coerce it (#466). >5 rows forces
    # the table path; the list path is checked directly.
    from reg_meta.cli_common import render_list, render_table

    rows = [
        {"var_id": 44, "variable_name": "Kön"},
        {"var_id": None, "variable_name": "Diagnos"},
    ]
    cols = ["var_id", "variable_name"]

    table, _ = render_table(rows + rows + rows, cols)  # 6 rows → table path
    assert "None" not in table
    # The SCB numeric id is still shown; the None row's cell is blank.
    assert "44" in table
    diagnos_line = next(ln for ln in table.splitlines() if "Diagnos" in ln)
    assert not diagnos_line.startswith("None")
    assert "0" not in diagnos_line.split("Diagnos")[0]

    listed = render_list(rows, cols)
    assert "None" not in listed
    assert "44" in listed
