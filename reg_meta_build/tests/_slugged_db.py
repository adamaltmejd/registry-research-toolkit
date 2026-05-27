"""Hand-curated in-memory reg_meta DB for FQID/Catalog tests.

The fixture DB built by `build_db` from synthetic CSVs has slug columns
NULL (those land in step 1c). FQID-aware code paths need a DB with
slugs populated, but the surface area exercised by these tests is
small — a single register/variant/version/variable/classification — so
hand-curated INSERTs beat a full build pipeline.
"""

from __future__ import annotations

import sqlite3

from reg_meta_build.db import DDL, seed_providers

# (name, slug, register_id, provider_id)
_DEFAULT_REGISTER = ("LISA", "lisa", 1, 1)
# (name, slug, regvar_id)
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
        var_name, var_slug, regvar_id = variant
        conn.execute(
            "INSERT INTO register_variant "
            "(regvar_id, register_id, slug, name) "
            "VALUES (?, ?, ?, ?)",
            (regvar_id, register[2], var_slug, var_name),
        )

    if version is not None and variant is not None:
        ver_name, ver_slug, regver_id = version
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, slug, registerversionnamn) "
            "VALUES (?, ?, ?, ?)",
            (regver_id, variant[2], ver_slug, ver_name),
        )

    if variable is not None and version is not None and register is not None:
        v_name, var_id, cvid, default_kol = variable
        conn.execute(
            "INSERT INTO variable (register_id, var_id, name) VALUES (?, ?, ?)",
            (register[2], var_id, v_name),
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id, data_type) "
            "VALUES (?, ?, ?, ?, ?, 'int')",
            (cvid, register[2], variant[2], version[2], var_id),
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
            (cvid, delivery_column_name or default_kol),
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
    regvar_id: int,
    register_id: int,
    slug: str,
    name: str,
) -> None:
    conn.execute(
        "INSERT INTO register_variant "
        "(regvar_id, register_id, slug, name) "
        "VALUES (?, ?, ?, ?)",
        (regvar_id, register_id, slug, name),
    )


def add_version(
    conn: sqlite3.Connection,
    *,
    regver_id: int,
    regvar_id: int,
    slug: str,
    name: str,
) -> None:
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, regvar_id, slug, registerversionnamn) "
        "VALUES (?, ?, ?, ?)",
        (regver_id, regvar_id, slug, name),
    )


def add_variable(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    var_id: int,
    name: str,
    source_register_id: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO variable "
        "(register_id, var_id, name, source_register_id) "
        "VALUES (?, ?, ?, ?)",
        (register_id, var_id, name, source_register_id),
    )


def add_binding(
    conn: sqlite3.Connection,
    *,
    cvid: int,
    register_id: int,
    regvar_id: int,
    regver_id: int,
    var_id: int,
    delivery_column_name: str,
    via_source_id: int | None = None,
) -> None:
    """Insert a variable_instance + matching variable_alias row.

    Parent rows (register/variant/version/variable) must already exist.
    ``via_source_id`` carries §5.6 consumer-side lineage when set.
    """
    conn.execute(
        "INSERT INTO variable_instance "
        "(cvid, register_id, regvar_id, regver_id, var_id, data_type, via_source_id) "
        "VALUES (?, ?, ?, ?, ?, 'int', ?)",
        (cvid, register_id, regvar_id, regver_id, var_id, via_source_id),
    )
    conn.execute(
        "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
        (cvid, delivery_column_name),
    )
