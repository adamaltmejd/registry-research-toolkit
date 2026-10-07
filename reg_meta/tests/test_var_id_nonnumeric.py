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
    get_varinfo,
    search,
)
from search_test_support import reader_search_conn

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

# Minted-id band base for the fixture (SCB variable_id < it, non-SCB >= it).
# `test_pipeline_built_non_scb_variable_reports_no_var_id` ties the builder's
# minted band to the reader's through a pipeline-built artifact.
_SCB_ID_CEILING = 2**62


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


def test_low_band_column_key_is_none(db: sqlite3.Connection) -> None:
    # Locks the regression Codex caught (#474): a column SCB's own export never
    # documented (a `[[column]]` in reg_meta_build/curation/scb_errata.toml) is minted in
    # the SCB band (variable_id < 2^62) but carries the delivery COLUMN as its
    # `provider_key`. The band-only guard CAST('SomeCol' AS INTEGER) = 0, leaking
    # a bogus var_id 0; the digit check makes it resolve to None.
    register_id = 1  # LISA (SCB, provider 1)
    _insert_variable(
        db,
        variable_id=99,  # low band (< 2^62) → reads as SCB
        register_id=register_id,
        provider_key="SomeCol",  # non-numeric SCB provider key
        name="Minted",
        slug="minted",
    )
    add_state(
        db,
        register_id=register_id,
        variable_slug="minted",
        register_variant_id=10,
        delivery_column_name="SomeCol",
    )
    db.commit()

    minted = get_varinfo(db, "SomeCol", register="lisa")
    assert minted and minted[0]["var_id"] is None


def test_pipeline_built_non_scb_variable_reports_no_var_id(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    # The builder mints every non-SCB id in the high band and the reader's var_id
    # band reads it as non-SCB, so a SOS variable whose provider_key is the
    # digit-only `2020` reports no var_id while the SCB `44` keeps its integer.
    # Either side moving its band boundary alone leaks `2020` as a var_id.
    conn = reader_search_conn(tmp_path_factory, "var-id-bands")

    rows = search(conn, "value", field="varname").results

    assert {row.name: row.var_id for row in rows} == {
        "Scb value": 44,
        "Sos value": None,
    }
