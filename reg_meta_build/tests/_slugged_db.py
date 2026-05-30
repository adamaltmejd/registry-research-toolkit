"""Hand-curated in-memory reg_meta DB for FQID/Catalog tests.

The fixture DB built by `build_db` from synthetic CSVs has slug columns
NULL (those land in step 1c). FQID-aware code paths need a DB with
slugs populated, but the surface area exercised by these tests is
small — a single register/variant/version/variable/classification — so
hand-curated INSERTs beat a full build pipeline.
"""

from __future__ import annotations

import sqlite3

from reg_meta.fqid import derive_variable_slug
from reg_meta_build.db import DDL, seed_providers

# (name, slug, register_id, provider_id)
_DEFAULT_REGISTER = ("LISA", "lisa", 1, 1)
# (name, slug, register_variant_id)
_DEFAULT_VARIANT = ("Individer 15+", "individer-15plus", 10)
# (registerversionnamn, slug, regver_id)
# Bare period name — production SCB names like LISA's are usually just `2018`,
# not `LISA 2018`. A prefix here would trigger seed-slugs' §5.3 residual check
# and flag the row, polluting tests that assume a clean round-trip.
_DEFAULT_VERSION = ("2018", "2018", 100)
# (name, var_id, cvid, delivery_column_name)
_DEFAULT_VARIABLE = ("Kön", 44, 1001, "Kon")


def build_slugged_db(
    *,
    register: tuple[str, str | None, int, int] | None = _DEFAULT_REGISTER,
    variant: tuple[str | None, str | None, int] | None = _DEFAULT_VARIANT,
    version: tuple[str, str | None, int] | None = _DEFAULT_VERSION,
    variable: tuple[str, int, int, str] | None = _DEFAULT_VARIABLE,
    delivery_column_name: str | None = None,
    variable_slug: str | None = None,
    classification: tuple[str, str, str, str] | None = (
        "SUN2020",
        "Svensk utbildningsnomenklatur",
        "2020",
        "sun",
    ),
) -> sqlite3.Connection:
    """Build an in-memory DB. Pass ``None`` for any layer to omit it.

    ``delivery_column_name`` overrides the variable's alias when set (e.g. to test
    diacritic folding).

    A2.1.5: sets the stored ``variable.slug`` (the resolver reads it) and seeds
    one ``variable_state`` era carrying the delivery column (the auto-derive
    source for ``populate_variable_slugs``). ``variable_slug`` defaults to
    ``derive_variable_slug(delivery_column_name)`` so existing tests round-trip
    unchanged; pass an explicit value (e.g. ``"ssyk-3pos"`` with a ``Ssyk``
    delivery column) to prove the stored-slug mechanism.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    seed_providers(conn)

    if register is not None:
        name, slug, register_id, provider_id = register
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, name) "
            "VALUES (?, ?, ?, ?)",
            (register_id, provider_id, slug, name),
        )

    if variant is not None and register is not None:
        var_name, var_slug, register_variant_id = variant
        conn.execute(
            "INSERT INTO register_variant "
            "(register_variant_id, register_id, slug, name) "
            "VALUES (?, ?, ?, ?)",
            (register_variant_id, register[2], var_slug, var_name),
        )

    if version is not None and variant is not None:
        # A2.6: register_version is build-time-only and has no `slug` column
        # anymore (version left the FQID grammar). The version tuple keeps its
        # 3-element shape `(registerversionnamn, _slug, regver_id)` for caller
        # compatibility, but the slug element is ignored on insert.
        ver_name, _ver_slug, regver_id = version
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, registerversionnamn) "
            "VALUES (?, ?, ?)",
            (regver_id, variant[2], ver_name),
        )

    if variable is not None and version is not None and register is not None:
        v_name, var_id, cvid, default_kol = variable
        kol = delivery_column_name or default_kol
        stored_slug = variable_slug or derive_variable_slug(kol)
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name, slug) "
            "VALUES (?, CAST(? AS TEXT), ?, ?)",
            (register[2], var_id, v_name, stored_slug),
        )
        variable_id = cur.lastrowid
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, register_variant_id, regver_id, var_id, data_type) "
            "VALUES (?, ?, ?, ?, ?, 'int')",
            (cvid, register[2], variant[2], version[2], var_id),
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
            (cvid, kol),
        )
        # A2.1.5: populate_variable_slugs auto-derives from
        # variable_state.delivery_column_name; seed one era so the engine has a
        # kolumnnamn to fold. The resolver itself reads variable.slug (above).
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, "
            "data_type, delivery_column_name) "
            "VALUES (?, ?, '2018-01-01', '9999-12-31', 'int', ?)",
            (variable_id, variant[2], kol),
        )

    if classification is not None:
        short, name, version_str, slug = classification
        conn.execute(
            "INSERT INTO classification (short_name, name, version, slug) "
            "VALUES (?, ?, ?, ?)",
            (short, name, version_str, slug),
        )

    conn.commit()
    return conn


def add_register(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    slug: str,
    name: str,
    provider_id: int = 1,
) -> None:
    conn.execute(
        "INSERT INTO register (register_id, provider_id, slug, name) "
        "VALUES (?, ?, ?, ?)",
        (register_id, provider_id, slug, name),
    )


def add_variant(
    conn: sqlite3.Connection,
    *,
    register_variant_id: int,
    register_id: int,
    slug: str,
    name: str,
) -> None:
    conn.execute(
        "INSERT INTO register_variant "
        "(register_variant_id, register_id, slug, name) "
        "VALUES (?, ?, ?, ?)",
        (register_variant_id, register_id, slug, name),
    )


def add_version(
    conn: sqlite3.Connection,
    *,
    regver_id: int,
    register_variant_id: int,
    slug: str,
    name: str,
) -> None:
    # A2.6: register_version has no `slug` column (build-time-only table; version
    # left the FQID grammar). `slug` kwarg is accepted but ignored for caller
    # compatibility; the row exists only for build-side joins (coalescer year
    # fallback, lineage linkers).
    _ = slug
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn) "
        "VALUES (?, ?, ?)",
        (regver_id, register_variant_id, name),
    )


def add_variable(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    var_id: int,
    name: str,
    source_register_id: int | None = None,
    slug: str | None = None,
) -> None:
    # A2.1.5: `slug` sets the stored `variable.slug` the resolver reads. Pass it
    # for variables that must resolve by FQID; leave None for fixture rows that
    # only need to exist (e.g. a source register target).
    conn.execute(
        "INSERT INTO variable "
        "(register_id, provider_key, name, source_register_id, slug) "
        "VALUES (?, CAST(? AS TEXT), ?, ?, ?)",
        (register_id, var_id, name, source_register_id, slug),
    )


def add_state(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    var_id: int | None = None,
    variable_slug: str | None = None,
    register_variant_id: int,
    valid_from: str = "0001-01-01",
    valid_to: str = "9999-12-31",
    data_type: str = "int",
    delivery_column_name: str | None = None,
    value_set_id: int | None = None,
    value_set_version_label: str = "",
) -> int:
    """Insert one `variable_state` for a variable under an explicit variant +
    validity window. Returns the state_id.

    Target the parent variable by `variable_slug` (register-unique, the
    disambiguating key for split siblings sharing a `provider_key`) or by
    `var_id` (the `provider_key` join hint — fine when no split). Exactly one
    must be given. The parent `variable` must already exist.

    A2.5: lets multi-vintage / sub-annual / multi-variant fixtures seed precise
    `variable_state` rows (distinct windows, value_set_version_labels, variants)
    without raw INSERTs."""
    if (var_id is None) == (variable_slug is None):
        raise ValueError("pass exactly one of var_id / variable_slug")
    if variable_slug is not None:
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
            (register_id, variable_slug),
        ).fetchone()[0]
    else:
        vid = conn.execute(
            "SELECT variable_id FROM variable "
            "WHERE register_id = ? AND provider_key = CAST(? AS TEXT)",
            (register_id, var_id),
        ).fetchone()[0]
    cur = conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, delivery_column_name, value_set_id, "
        "value_set_version_label) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            vid,
            register_variant_id,
            valid_from,
            valid_to,
            data_type,
            delivery_column_name,
            value_set_id,
            value_set_version_label,
        ),
    )
    return cur.lastrowid


def add_value_set(
    conn: sqlite3.Connection,
    *,
    value_set_id: int,
    codes: list[tuple[str, str]],
) -> None:
    """Mint a `value_set` + its `value_code` / `value_set_member` rows so a state
    can carry hydratable codes. `codes` is a list of (code, label). The
    member_hash is a throwaway 32-byte blob (content-addressing isn't exercised
    by Catalog reads — it joins by value_set_id)."""
    conn.execute(
        "INSERT INTO value_set (value_set_id, member_hash) VALUES (?, ?)",
        (value_set_id, bytes(32)[:31] + bytes([value_set_id % 256])),
    )
    for code, label in codes:
        cur = conn.execute(
            "INSERT INTO value_code (code, label) VALUES (?, ?)", (code, label)
        )
        conn.execute(
            "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
            (value_set_id, cur.lastrowid),
        )


def add_binding(
    conn: sqlite3.Connection,
    *,
    cvid: int,
    register_id: int,
    register_variant_id: int,
    regver_id: int,
    var_id: int,
    delivery_column_name: str,
    via_source_id: int | None = None,
) -> None:
    """Insert a variable_instance + matching variable_alias + variable_state row.

    Parent rows (register/variant/version/variable) must already exist.
    ``via_source_id`` carries §5.6 consumer-side lineage when set.

    A2.2 resolver flip: binding resolution reads `variable_state` (keyed by
    `variable_id`), so a binding fixture must seed a state too. We resolve the
    variable_id from (register_id, var_id) and write an open-range state
    (`0001-01-01`..`9999-12-31`) so any queried period overlaps it.
    """
    conn.execute(
        "INSERT INTO variable_instance "
        "(cvid, register_id, register_variant_id, regver_id, var_id, data_type, via_source_id) "
        "VALUES (?, ?, ?, ?, ?, 'int', ?)",
        (cvid, register_id, register_variant_id, regver_id, var_id, via_source_id),
    )
    conn.execute(
        "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
        (cvid, delivery_column_name),
    )
    vid_row = conn.execute(
        "SELECT variable_id FROM variable "
        "WHERE register_id = ? AND provider_key = CAST(? AS TEXT)",
        (register_id, var_id),
    ).fetchone()
    if vid_row is not None:
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, ?, '0001-01-01', '9999-12-31', 'int', ?)",
            (vid_row[0], register_variant_id, delivery_column_name),
        )
