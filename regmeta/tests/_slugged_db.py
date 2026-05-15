"""Hand-curated in-memory regmeta DB for FQID/Catalog tests.

The fixture DB built by `build_db` from synthetic CSVs has slug columns
NULL (those land in step 1c). FQID-aware code paths need a DB with
slugs populated, but the surface area exercised by these tests is
small — a single register/variant/version/variable/classification — so
hand-curated INSERTs beat a full build pipeline.
"""

from __future__ import annotations

import sqlite3

from regmeta.db import DDL, seed_providers

# (registernamn, slug, register_id, provider_id)
_DEFAULT_REGISTER = ("LISA", "lisa", 1, 1)
# (registervariantnamn, slug, regvar_id)
_DEFAULT_VARIANT = ("Individer 15+", "individer-15plus", 10)
# (registerversionnamn, regver_id)
_DEFAULT_VERSION = ("LISA 2018", 100)
# (variabelnamn, var_id, cvid, kolumnnamn)
_DEFAULT_VARIABLE = ("Kön", 44, 1001, "Kon")


def build_slugged_db(
    *,
    register: tuple[str, str | None, int, int] | None = _DEFAULT_REGISTER,
    variant: tuple[str | None, str | None, int] | None = _DEFAULT_VARIANT,
    version: tuple[str, int] | None = _DEFAULT_VERSION,
    variable: tuple[str, int, int, str] | None = _DEFAULT_VARIABLE,
    kolumnnamn: str | None = None,
    classification: tuple[str, str, str, str] | None = (
        "SUN2020",
        "Svensk utbildningsnomenklatur",
        "2020",
        "sun",
    ),
) -> sqlite3.Connection:
    """Build an in-memory DB. Pass ``None`` for any layer to omit it.

    ``kolumnnamn`` overrides the variable's alias when set (e.g. to test
    diacritic folding).
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    seed_providers(conn)

    if register is not None:
        name, slug, register_id, provider_id = register
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, registernamn) "
            "VALUES (?, ?, ?, ?)",
            (register_id, provider_id, slug, name),
        )

    if variant is not None and register is not None:
        var_name, var_slug, regvar_id = variant
        conn.execute(
            "INSERT INTO register_variant "
            "(regvar_id, register_id, slug, registervariantnamn) "
            "VALUES (?, ?, ?, ?)",
            (regvar_id, register[2], var_slug, var_name),
        )

    if version is not None and variant is not None:
        ver_name, regver_id = version
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, registerversionnamn) "
            "VALUES (?, ?, ?)",
            (regver_id, variant[2], ver_name),
        )

    if variable is not None and version is not None and register is not None:
        v_name, var_id, cvid, default_kol = variable
        conn.execute(
            "INSERT INTO variable (register_id, var_id, variabelnamn) VALUES (?, ?, ?)",
            (register[2], var_id, v_name),
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id, datatyp) "
            "VALUES (?, ?, ?, ?, ?, 'int')",
            (cvid, register[2], variant[2], version[1], var_id),
        )
        conn.execute(
            "INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (?, ?)",
            (cvid, kolumnnamn or default_kol),
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
