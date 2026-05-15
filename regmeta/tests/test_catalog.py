"""Tests for Catalog.resolve() (REFACTOR_SPEC.md §5.8).

Catalog resolves FQIDs against the regmeta SQLite asset. The fixture DB
shipped by build_db has slug columns NULL (those land with 1c); these
tests build a tiny in-memory DB and populate slugs explicitly so the
resolver path can be exercised end-to-end.
"""

from __future__ import annotations

import sqlite3

import pytest

from regmeta.catalog import (
    Catalog,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedRegisterVariant,
    ResolvedRegisterVersion,
    ResolvedVariableBinding,
)
from regmeta.db import DDL, seed_providers
from regmeta.errors import RegmetaError
from regmeta.fqid import Fqid


@pytest.fixture
def slugged_conn() -> sqlite3.Connection:
    """In-memory DB with the regmeta schema and a hand-curated slug set."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    seed_providers(conn)
    # One register, one variant, one version, one variable, one alias.
    conn.execute(
        "INSERT INTO register (register_id, provider_id, slug, registernamn) "
        "VALUES (1, 1, 'lisa', 'LISA')"
    )
    conn.execute(
        "INSERT INTO register_variant "
        "(regvar_id, register_id, slug, registervariantnamn) "
        "VALUES (10, 1, 'individer-15plus', 'Individer 15+')"
    )
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, regvar_id, registerversionnamn) "
        "VALUES (100, 10, 'LISA 2018')"
    )
    conn.execute(
        "INSERT INTO variable (register_id, var_id, variabelnamn) VALUES (1, 44, 'Kön')"
    )
    conn.execute(
        "INSERT INTO variable_instance "
        "(cvid, register_id, regvar_id, regver_id, var_id, datatyp) "
        "VALUES (1001, 1, 10, 100, 44, 'int')"
    )
    conn.execute("INSERT INTO variable_alias (cvid, kolumnnamn) VALUES (1001, 'Kon')")
    conn.execute(
        "INSERT INTO classification "
        "(short_name, name, version, slug) "
        "VALUES ('SUN2020', 'Svensk utbildningsnomenklatur', '2020', 'sun')"
    )
    conn.commit()
    return conn


class TestResolveProvider:
    def test_resolves_known_provider(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        r = c.resolve("scb")
        assert isinstance(r, ResolvedProvider)
        assert r.slug == "scb"
        assert r.name == "Statistics Sweden"
        assert r.fqid.emit() == "scb"

    def test_unknown_provider_raises_not_found(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        c = Catalog(slugged_conn)
        with pytest.raises(RegmetaError) as exc:
            c.resolve("nope")
        assert exc.value.code == "fqid_not_found"


class TestResolveRegister:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        r = c.resolve("scb/lisa")
        assert isinstance(r, ResolvedRegister)
        assert r.register_id == 1
        assert r.provider_slug == "scb"
        assert r.registernamn == "LISA"

    def test_wrong_provider_misses(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        with pytest.raises(RegmetaError) as exc:
            c.resolve("sos/lisa")
        assert exc.value.code == "fqid_not_found"


class TestResolveVariant:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        r = c.resolve("scb/lisa/individer-15plus")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.regvar_id == 10
        assert r.slug == "individer-15plus"

    def test_default_variant_resolves(self) -> None:
        """`_default` variant is reserved for the variant slot and must
        resolve when synthesized for variant-less registers (§5.1)."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, registernamn) "
            "VALUES (5, 2, 'lss', 'LSS')"
        )
        conn.execute(
            "INSERT INTO register_variant "
            "(regvar_id, register_id, slug, registervariantnamn) "
            "VALUES (50, 5, '_default', NULL)"
        )
        c = Catalog(conn)
        r = c.resolve("sos/lss/_default")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.slug == "_default"


class TestResolveVersion:
    def test_resolves_by_year(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        r = c.resolve("scb/lisa/individer-15plus/2018")
        assert isinstance(r, ResolvedRegisterVersion)
        assert r.regver_id == 100
        assert r.period == "2018"
        assert r.fqid.emit() == "scb/lisa/individer-15plus/2018"

    def test_unknown_period_misses(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        with pytest.raises(RegmetaError) as exc:
            c.resolve("scb/lisa/individer-15plus/2099")
        assert exc.value.code == "fqid_not_found"


class TestResolveBinding:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        # Kolumnnamn "Kon" → variable slug "kon" via derive_variable_slug.
        c = Catalog(slugged_conn)
        r = c.resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.cvid == 1001
        assert r.var_id == 44
        assert r.variable_slug == "kon"
        assert r.kolumnnamn == "Kon"

    def test_swedish_kolumnnamn_resolves_to_ascii_slug(self) -> None:
        # "Kön" decomposes to "kon" via NFKD strip; the binding FQID still
        # uses ASCII because the §5.2 slug grammar bans non-ASCII.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, slug, registernamn) "
            "VALUES (1, 1, 'lisa', 'LISA')"
        )
        conn.execute(
            "INSERT INTO register_variant (regvar_id, register_id, slug) "
            "VALUES (10, 1, 'v')"
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, regvar_id, registerversionnamn) "
            "VALUES (100, 10, 'V 2020')"
        )
        conn.execute(
            "INSERT INTO variable (register_id, var_id, variabelnamn) "
            "VALUES (1, 44, 'Kön')"
        )
        conn.execute(
            "INSERT INTO variable_instance "
            "(cvid, register_id, regvar_id, regver_id, var_id) "
            "VALUES (1, 1, 10, 100, 44)"
        )
        conn.execute("INSERT INTO variable_alias VALUES (1, 'Kön')")
        c = Catalog(conn)
        r = c.resolve("scb/lisa/v/2020/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.kolumnnamn == "Kön"

    def test_unknown_variable_misses(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        with pytest.raises(RegmetaError) as exc:
            c.resolve("scb/lisa/individer-15plus/2018/nonexistent")
        assert exc.value.code == "fqid_not_found"


class TestResolveClassification:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        r = c.resolve("class/sun/2020")
        assert isinstance(r, ResolvedClassification)
        assert r.classification_id is not None
        assert r.slug == "sun"
        assert r.version == "2020"

    def test_unknown_version_misses(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        with pytest.raises(RegmetaError) as exc:
            c.resolve("class/sun/9999")
        assert exc.value.code == "fqid_not_found"


class TestResolveFqidObject:
    def test_accepts_parsed_fqid_object(self, slugged_conn: sqlite3.Connection) -> None:
        c = Catalog(slugged_conn)
        f = Fqid.register_fqid("scb", "lisa")
        r = c.resolve(f)
        assert isinstance(r, ResolvedRegister)


class TestPre1cBootstrap:
    def test_register_with_null_slug_does_not_resolve(self) -> None:
        # Before 1c populates slugs, register rows have slug = NULL. The
        # resolver must miss cleanly rather than match arbitrary NULL rows.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, registernamn) "
            "VALUES (1, 1, 'LISA')"
        )
        c = Catalog(conn)
        with pytest.raises(RegmetaError) as exc:
            c.resolve("scb/lisa")
        assert exc.value.code == "fqid_not_found"
