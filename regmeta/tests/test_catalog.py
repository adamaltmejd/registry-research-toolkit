"""Tests for Catalog.resolve() (REFACTOR_SPEC.md §5.8)."""

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
from regmeta.errors import RegmetaError
from regmeta.fqid import Fqid

from _slugged_db import build_slugged_db


@pytest.fixture
def slugged_conn() -> sqlite3.Connection:
    return build_slugged_db()


class TestResolveProvider:
    def test_resolves_known_provider(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb")
        assert isinstance(r, ResolvedProvider)
        assert r.name == "Statistics Sweden"
        assert r.fqid.provider == "scb"
        assert str(r.fqid) == "scb"

    def test_unknown_provider_raises_not_found(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("nope")
        assert exc.value.code == "fqid_not_found"


class TestResolveRegister:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa")
        assert isinstance(r, ResolvedRegister)
        assert r.register_id == 1
        assert r.fqid.provider == "scb"
        assert r.registernamn == "LISA"

    def test_wrong_provider_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("sos/lisa")
        assert exc.value.code == "fqid_not_found"


class TestResolveVariant:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.regvar_id == 10
        assert r.fqid.variant == "individer-15plus"

    def test_default_variant_resolves(self) -> None:
        # `_default` is the synthesized slug for variant-less registers (§5.1).
        conn = build_slugged_db(
            register=("LSS", "lss", 5, 2),
            variant=("LSS default", "_default", 50),
            version=None,
            variable=None,
        )
        r = Catalog(conn).resolve("sos/lss/_default")
        assert isinstance(r, ResolvedRegisterVariant)
        assert r.fqid.variant == "_default"


class TestResolveVersion:
    def test_resolves_by_year(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018")
        assert isinstance(r, ResolvedRegisterVersion)
        assert r.regver_id == 100
        assert r.fqid.period == "2018"
        assert str(r.fqid) == "scb/lisa/individer-15plus/2018"

    def test_unknown_period_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2099")
        assert exc.value.code == "fqid_not_found"


class TestResolveBinding:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        # Kolumnnamn "Kon" derives to variable slug "kon".
        r = Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.cvid == 1001
        assert r.var_id == 44
        assert r.fqid.variable == "kon"
        assert r.kolumnnamn == "Kon"

    def test_swedish_kolumnnamn_folds_to_ascii_slug(self) -> None:
        # "Kön" → "kon" via NFKD ASCII fold; binding FQIDs are ASCII (§5.2).
        conn = build_slugged_db(kolumnnamn="Kön")
        r = Catalog(conn).resolve("scb/lisa/individer-15plus/2018/kon")
        assert isinstance(r, ResolvedVariableBinding)
        assert r.kolumnnamn == "Kön"

    def test_unknown_variable_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("scb/lisa/individer-15plus/2018/nonexistent")
        assert exc.value.code == "fqid_not_found"


class TestResolveClassification:
    def test_resolves(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve("class/sun/2020")
        assert isinstance(r, ResolvedClassification)
        assert r.classification_id is not None
        assert r.fqid.classification == "sun"
        assert r.fqid.version == "2020"

    def test_unknown_version_misses(self, slugged_conn: sqlite3.Connection) -> None:
        with pytest.raises(RegmetaError) as exc:
            Catalog(slugged_conn).resolve("class/sun/9999")
        assert exc.value.code == "fqid_not_found"


class TestResolveFqidObject:
    def test_accepts_parsed_fqid_object(self, slugged_conn: sqlite3.Connection) -> None:
        r = Catalog(slugged_conn).resolve(Fqid.register_fqid("scb", "lisa"))
        assert isinstance(r, ResolvedRegister)


class TestNullSlugMisses:
    def test_null_register_slug_does_not_resolve(self) -> None:
        # Before 1c populates slugs, register rows have slug = NULL; the
        # resolver must miss rather than match arbitrary NULL rows.
        conn = build_slugged_db(
            register=("LISA", None, 1, 1), variant=None, version=None, variable=None
        )
        with pytest.raises(RegmetaError) as exc:
            Catalog(conn).resolve("scb/lisa")
        assert exc.value.code == "fqid_not_found"
