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
    def test_register_fqid_and_variant_subresource(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        results = get_register(slugged_conn, "LISA")
        assert len(results) == 1
        reg = results[0]
        assert reg["fqid"] == "scb/lisa"
        # A2.6: a variant is a register sub-resource, not a slash-path FQID. It
        # carries the parent register FQID + its browse slug (see DESIGN.md → FQID grammar).
        variant = reg["variants"][0]
        assert variant["register_fqid"] == "scb/lisa"
        assert variant["slug"] == "individer-15plus"
        assert "fqid" not in variant

    def test_null_slugs_yield_null_fqid(self) -> None:
        # Pre-1c state: register slug NULL → fqid key present but None.
        conn = build_slugged_db(
            register=("LISA", None, 1, 1),
            variant=("V", None, 10),
            version=None,
            variable=None,
            classification=None,
        )
        results = get_register(conn, "LISA")
        assert results[0]["fqid"] is None
        # The variant's parent register FQID is None too (register slug NULL).
        assert results[0]["variants"][0]["register_fqid"] is None


class TestGetSchemaFqid:
    def test_edition_and_binding_fqids(self, slugged_conn: sqlite3.Connection) -> None:
        result = get_schema(slugged_conn, register_variant_id="10")
        variant = result["variants"][0]
        # A2.6: variant carries the register FQID + browse slug, not a variant FQID.
        assert variant["register_fqid"] == "scb/lisa"
        assert variant["variant"] == "individer-15plus"
        # Editions are validity windows now (no register_version FQID).
        version = variant["versions"][0]
        assert version["valid_from"] == "2018-01-01"
        assert "fqid" not in version
        # The column carries the 3-seg binding FQID.
        column = version["columns"][0]
        assert column["fqid"] == "scb/lisa/kon"


class TestGetVarinfoFqid:
    def test_instance_binding_fqid(self, slugged_conn: sqlite3.Connection) -> None:
        results = get_varinfo(slugged_conn, "Kön")
        instance = results[0]["instances"][0]
        # A2.6: 3-seg binding FQID; the per-state validity window replaces the
        # register_version coordinate.
        assert instance["fqid"] == "scb/lisa/kon"
        assert instance["valid_from"] == "2018-01-01"


class TestGetClassificationFqid:
    def test_classification_fqid(self, slugged_conn: sqlite3.Connection) -> None:
        # A2.6.1: 2-seg FQID with the vintage baked into the slug.
        result = get_classification(slugged_conn, "SUN2020")
        assert result["fqid"] == "class/sun2020"

    def test_null_slug_omits_fqid(self) -> None:
        # _classification_row drops NULL fields entirely; with slug NULL,
        # the fqid key is absent rather than carrying None.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO classification (short_name, name) VALUES ('SUN2020', 'SUN')"
        )
        result = get_classification(conn, "SUN2020")
        assert "fqid" not in result
