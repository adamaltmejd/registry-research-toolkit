"""Tests for FQID emission in query results."""

from __future__ import annotations

import sqlite3

import pytest
from _slugged_db import build_slugged_db
from reg_meta.queries import (
    get_classification,
    get_register,
    get_schema,
    get_varinfo,
)
from reg_meta_build.db import DDL, seed_providers


@pytest.fixture
def slugged_conn() -> sqlite3.Connection:
    return build_slugged_db()


class TestGetRegisterFqid:
    def test_register_and_variant_carry_fqids(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        results = get_register(slugged_conn, "LISA")
        assert len(results) == 1
        reg = results[0]
        assert reg["fqid"] == "scb/lisa"
        assert reg["variants"][0]["fqid"] == "scb/lisa/individer-15plus"

    def test_null_slugs_yield_null_fqid(self) -> None:
        # Pre-1c state: slug columns NULL → fqid key present but None;
        # legacy fields unchanged.
        conn = build_slugged_db(
            register=("LISA", None, 1, 1),
            variant=("V", None, 10),
            version=None,
            variable=None,
            classification=None,
        )
        results = get_register(conn, "LISA")
        assert results[0]["fqid"] is None
        assert results[0]["variants"][0]["fqid"] is None


class TestGetSchemaFqid:
    def test_variant_version_binding_fqids(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        result = get_schema(slugged_conn, register_variant_id="10")
        variant = result["variants"][0]
        assert variant["fqid"] == "scb/lisa/individer-15plus"
        version = variant["versions"][0]
        assert version["fqid"] == "scb/lisa/individer-15plus/2018"
        column = version["columns"][0]
        assert column["fqid"] == "scb/lisa/individer-15plus/2018/kon"

    def test_sub_year_period_preserved_in_fqid(self) -> None:
        # An `HT2020` version must not collapse to `.../2020/...`; the FQID
        # carries the most-specific period token so sub-year versions stay
        # distinguishable.
        conn = build_slugged_db(version=("LISA HT2020", "HT2020", 100))
        result = get_schema(conn, register_variant_id="10")
        version = result["variants"][0]["versions"][0]
        assert version["fqid"] == "scb/lisa/individer-15plus/HT2020"
        column = version["columns"][0]
        assert column["fqid"] == "scb/lisa/individer-15plus/HT2020/kon"


class TestGetVarinfoFqid:
    def test_instance_binding_fqid(self, slugged_conn: sqlite3.Connection) -> None:
        results = get_varinfo(slugged_conn, "Kön")
        instance = results[0]["instances"][0]
        assert instance["fqid"] == "scb/lisa/individer-15plus/2018/kon"


class TestGetClassificationFqid:
    def test_classification_fqid(self, slugged_conn: sqlite3.Connection) -> None:
        result = get_classification(slugged_conn, "SUN2020")
        assert result["fqid"] == "class/sun/2020"

    def test_null_slug_omits_fqid(self) -> None:
        # _classification_row drops NULL fields entirely; with slug NULL,
        # the fqid key is absent rather than carrying None.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO classification (short_name, name, version) "
            "VALUES ('SUN2020', 'SUN', '2020')"
        )
        result = get_classification(conn, "SUN2020")
        assert "fqid" not in result
