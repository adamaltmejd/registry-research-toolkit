"""#474 (was #466): `var_id` is the SCB legacy numeric variable id (= SCB's
numeric `provider_key`). SOS variables carry a Swedish *name* as `provider_key`
and curated thin providers carry a delivery *column* token; both are non-SCB, so
the old `CAST(provider_key AS INTEGER)` yielded a meaningless `var_id: 0`. The
`_VAR_ID_EXPR` now classifies by the build's minted-id BAND — every SCB
`variable_id` is `< 2^62`, every non-SCB (SOS, curated, FOHM, steward)
`variable_id` is `>= 2^62` (`reg_meta_build/validate.py::_check_minted_id_bands`)
— emitting the numeric id for an SCB-band variable and NULL (→ Python None)
otherwise.

The band guard supersedes #466's pure-digit `provider_key` heuristic: it is
strictly more correct, because a non-SCB `provider_key` that happens to be
digit-only (a curated column literally named `2020`) is still in the high band,
so its `var_id` resolves to None rather than a bogus `2020`.

Builds a synthetic DB directly (the SCB CSV pipeline can only mint numeric
provider_keys) with one variable per provider flavour, each in its own register
(a register belongs to one provider, so provider_key shape is uniform within a
register — no int/None mixing inside a single query result). Each fixture
variable gets a band-correct EXPLICIT `variable_id`: SCB vars low (< 2^62),
non-SCB vars high (>= 2^62) — SQLite's autoincrement would otherwise hand out
low ids (1, 2, 3, …) that wrongly read as SCB-band.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from reg_meta.queries import (
    _SCB_ID_CEILING,
    get_varinfo,
    search,
    search_variables_by_classification,
)

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "reg_meta_build" / "tests")
)

from _slugged_db import (
    add_register,
    add_state,
    add_variant,
    build_slugged_db,
)

if TYPE_CHECKING:
    import sqlite3

# provider_key flavours: SCB numeric, SOS name, curated column token, plus two
# non-SCB values whose `provider_key` would fool a digit heuristic — a MIXED
# leading-digit value and a DIGIT-ONLY value — both of which the BAND guard
# rejects (→ None) purely on their high-band `variable_id`.
_SCB_KEY = "44"
_SOS_KEY = "Diagnos"  # a Swedish variable name (SOS provider_key shape)
_CURATED_KEY = "diagnos"  # a delivery-column token (curated provider_key shape)
_MIXED_KEY = "44abc"  # leading-digit-but-not-pure-digit (non-SCB, high band)
_DIGIT_KEY = "2020"  # PURE-digit but non-SCB (high band) — the band guard's edge
# over the old digit guard (which would have leaked `2020`)

# Minted-id band base for the fixture: the production `_SCB_ID_CEILING`
# (SCB variable_id < it, non-SCB >= it). `test_band_constant_in_sync_with_build`
# guards that this mirrors reg_meta_build.id._MINT_BIT across the build/runtime
# boundary, so the fixture never carries its own divergent literal.


def _insert_variable(
    conn: sqlite3.Connection,
    *,
    variable_id: int,
    register_id: int,
    provider_key: str,
    name: str,
    slug: str,
) -> int:
    """Insert a `variable` with an EXPLICIT band-correct `variable_id` and a
    (possibly non-numeric) `provider_key`.

    `_slugged_db.add_variable` types `var_id` as `int` and lets SQLite assign the
    `variable_id` (low autoincrement) — useless here, since the band guard reads
    `variable_id`, so we must place each id in its provider's band by hand.
    `provider_key` is a raw string, so go straight to the INSERT (it is TEXT)."""
    cur = conn.execute(
        "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
        "VALUES (?, ?, ?, ?, ?)",
        (variable_id, register_id, provider_key, name, slug),
    )
    assert cur.lastrowid is not None
    return cur.lastrowid


@pytest.fixture
def db() -> sqlite3.Connection:
    """One SCB register (numeric key, LOW-band id), one SOS register (name key,
    HIGH-band id), one curated register (column-token key, HIGH-band id), plus two
    non-SCB curated vars whose provider_keys are digit-shaped (`44abc` / `2020`)
    yet HIGH-band — the band guard must still resolve those to None. Each variable
    gets a variant + an open-range state tagged with the shipped SUN2020
    classification (id 1) so the classification→variables query returns them all.

    The `variable_id`s are placed explicitly in their provider's minted-id band
    (SCB < 2^62, non-SCB >= 2^62); SQLite autoincrement would hand out low ids
    that the band guard would misread as SCB."""
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

    # (variable_id, register_id, variant_id, provider_key, name, slug). SCB var
    # gets a LOW id; every non-SCB var gets a HIGH (>= 2^62) id. The digit-shaped
    # non-SCB keys (`44abc`, `2020`) live on the CURATED (non-SCB) register — an
    # SCB register never carries a non-numeric/leading-zero key, so the band guard
    # only has to defend the non-SCB side.
    specs = [
        (44, 1, 10, _SCB_KEY, "Kön", "kon"),
        (_SCB_ID_CEILING + 1, 2, 20, _SOS_KEY, "Diagnos", "diagnos-sos"),
        (_SCB_ID_CEILING + 2, 3, 30, _CURATED_KEY, "Diagnos curated", "diagnos-cur"),
        (_SCB_ID_CEILING + 3, 3, 30, _MIXED_KEY, "Mixed", "mixed"),
        (_SCB_ID_CEILING + 4, 3, 30, _DIGIT_KEY, "DigitOnly", "digit-only"),
    ]
    for variable_id, register_id, variant_id, provider_key, name, slug in specs:
        _insert_variable(
            conn,
            variable_id=variable_id,
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
        # `add_state` seeds only `variable_state`; the datacolumn search reads
        # `variable_alias.delivery_column_name` (the column-alias LIKE path), so
        # seed an alias row too (= the name, mirroring the state) for the
        # field="datacolumn" guard.
        conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, ?, ?)",
            (variable_id, variant_id, name),
        )

    conn.commit()
    return conn


def test_classification_to_variables_var_id_guard(db: sqlite3.Connection) -> None:
    rows = search_variables_by_classification(db, "sun2020")
    by_name = {r["variable_name"]: r["var_id"] for r in rows}
    # SCB var (low-band variable_id) → the numeric var_id (int 44).
    assert by_name["Kön"] == 44
    assert isinstance(by_name["Kön"], int)
    # SOS (name) + curated (column) provider_keys, both HIGH band → None, not 0.
    assert by_name["Diagnos"] is None
    assert by_name["Diagnos curated"] is None
    # The band guard rejects on `variable_id`, not on provider_key shape, so a
    # non-SCB var (high band) resolves to None regardless of its key text:
    #   - "44abc" (leading-digit but not pure-digit), and
    #   - "2020"  (PURE digit) — the band guard's edge over the old digit guard,
    #     which would have leaked 2020 as a var_id.
    assert by_name["Mixed"] is None
    assert by_name["DigitOnly"] is None


def test_search_varname_var_id_guard(db: sqlite3.Connection) -> None:
    # field="varname" is the LIKE-over-name path (no FTS rebuild needed).
    # The typed `varname` row (#701) carries `var_id`/`name` as attributes.
    scb = search(db, "Kön", field="varname").results
    assert scb and scb[0].var_id == 44

    sos = search(db, "Diagnos", field="varname").results
    by_name = {r.name: r.var_id for r in sos}
    assert by_name["Diagnos"] is None
    assert by_name["Diagnos curated"] is None


def test_search_datacolumn_var_id_guard(db: sqlite3.Connection) -> None:
    # field="datacolumn" is the LIKE-over-`delivery_column_name` path; the fixture
    # sets each state's `delivery_column_name` = the variable's `name`, so the same
    # "Kön"/"Diagnos" queries match here (mirror of the varname guard above).
    # The typed `datacolumn` row (#701) carries `var_id`/`name` as attributes.
    scb = search(db, "Kön", field="datacolumn").results
    assert scb and scb[0].var_id == 44
    assert isinstance(scb[0].var_id, int)

    sos = search(db, "Diagnos", field="datacolumn").results
    by_name = {r.name: r.var_id for r in sos}
    assert by_name["Diagnos"] is None
    assert by_name["Diagnos curated"] is None


def test_varinfo_var_id_guard(db: sqlite3.Connection) -> None:
    # SCB var (low-band id) addressable by numeric var_id, surfaces var_id 44.
    scb = get_varinfo(db, "44", register="lisa")
    assert scb and scb[0]["var_id"] == 44

    # SOS var: addressable by name (it has no numeric var_id), surfaces None.
    # The register is resolved by `register.name`, not slug.
    sos = get_varinfo(db, "Diagnos", register="SOS Register")
    assert sos and sos[0]["var_id"] is None

    # Curated var: addressable by name, surfaces None.
    cur = get_varinfo(db, "Diagnos curated", register="Curated Register")
    assert cur and cur[0]["var_id"] is None


def test_digit_shaped_nonscb_key_is_none(db: sqlite3.Connection) -> None:
    # The BAND guard classifies on `variable_id` band, NOT on provider_key shape,
    # so a non-SCB var (high-band id) resolves to None even when its key would
    # have fooled the old digit guard. Both digit-shaped non-SCB vars live on the
    # curated (non-SCB) register:
    #   - "44abc": leading-digit but not pure-digit — the old `CAST(... AS INTEGER)`
    #     would have parsed 44; the digit guard already rejected it, the band guard
    #     keeps rejecting it (for the right reason: high band, not "has letters").
    mixed = get_varinfo(db, "Mixed", register="Curated Register")
    assert mixed and mixed[0]["var_id"] is None
    #   - "2020": PURE digit — the OLD digit guard would have LEAKED 2020 as a
    #     var_id; the band guard correctly returns None because its id is high band.
    digit = get_varinfo(db, "DigitOnly", register="Curated Register")
    assert digit and digit[0]["var_id"] is None


def test_low_band_graft_key_is_none(db: sqlite3.Connection) -> None:
    # Locks the graft regression Codex caught (#474): SCB variable GRAFTS
    # (reg_meta_build/variable_grafts.py) are minted in the SCB band
    # (variable_id < 2^62) but carry a NON-numeric `provider_key` of the form
    # `graft:<column>`. The band-only guard CAST('graft:col' AS INTEGER) = 0,
    # leaking a bogus var_id 0; the digit check makes it resolve to None.
    register_id = 1  # LISA (SCB, provider 1)
    _insert_variable(
        db,
        variable_id=99,  # low band (< 2^62) → reads as SCB
        register_id=register_id,
        provider_key="graft:somecol",  # non-numeric SCB graft key
        name="Grafted",
        slug="grafted",
    )
    add_state(
        db,
        register_id=register_id,
        variable_slug="grafted",
        register_variant_id=10,
        delivery_column_name="Grafted",
    )
    db.commit()

    graft = get_varinfo(db, "Grafted", register="lisa")
    assert graft and graft[0]["var_id"] is None


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


def test_band_constant_in_sync_with_build() -> None:
    # The var_id band boundary is duplicated by design across the build/runtime
    # boundary (reg_meta can't import build-only code at runtime), so only this
    # test ties the two literals together. Importing _MINT_BIT here is fine — a
    # test is dev-time, not the runtime boundary id.py's docstring protects.
    from reg_meta_build.id import _MINT_BIT

    assert _SCB_ID_CEILING == _MINT_BIT, (
        "var_id band constant drift: "
        f"reg_meta/queries.py::_SCB_ID_CEILING ({_SCB_ID_CEILING}) != "
        f"reg_meta_build/id.py::_MINT_BIT ({_MINT_BIT}). They mirror each other "
        "across the build/runtime boundary — update both."
    )
