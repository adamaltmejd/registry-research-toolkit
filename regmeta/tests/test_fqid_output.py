"""Tests for FQID emission in query results (REFACTOR_SPEC.md §15 step 1b).

Query commands gain a ``fqid`` key alongside their legacy fields. Where
slug columns are NULL (the state of the fixture DB until 1c populates
them), ``fqid`` is None — existing assertions on legacy fields keep
working. Where slugs are populated (these tests), ``fqid`` carries the
canonical string.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from regmeta.db import DDL, seed_providers
from regmeta.queries import (
    get_classification,
    get_register,
    get_schema,
    get_varinfo,
)


def _build_slugged_db(path: Path) -> sqlite3.Connection:
    """Construct a hand-curated DB exercising all five FQID kinds."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    seed_providers(conn)
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
    conn.execute("INSERT INTO variable_alias VALUES (1001, 'Kon')")
    conn.execute(
        "INSERT INTO classification "
        "(short_name, name, version, slug) "
        "VALUES ('SUN2020', 'Svensk utbildningsnomenklatur', '2020', 'sun')"
    )
    conn.commit()
    return conn


@pytest.fixture
def slugged_conn(tmp_path: Path) -> sqlite3.Connection:
    return _build_slugged_db(tmp_path / "regmeta.db")


class TestGetRegisterFqid:
    def test_register_and_variant_carry_fqids(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        results = get_register(slugged_conn, "LISA")
        assert len(results) == 1
        reg = results[0]
        assert reg["fqid"] == "scb/lisa"
        assert reg["variants"][0]["fqid"] == "scb/lisa/individer-15plus"

    def test_null_slugs_yield_null_fqid(self, tmp_path: Path) -> None:
        # Pre-1c state: slug columns NULL → fqid key is None, legacy fields
        # still present.
        conn = sqlite3.connect(tmp_path / "rm.db")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, registernamn) "
            "VALUES (1, 1, 'LISA')"
        )
        conn.execute(
            "INSERT INTO register_variant (regvar_id, register_id, registervariantnamn) "
            "VALUES (10, 1, 'V')"
        )
        results = get_register(conn, "LISA")
        assert results[0]["fqid"] is None
        assert results[0]["variants"][0]["fqid"] is None


class TestGetSchemaFqid:
    def test_variant_version_binding_fqids(
        self, slugged_conn: sqlite3.Connection
    ) -> None:
        result = get_schema(slugged_conn, regvar_id="10")
        variant = result["variants"][0]
        assert variant["fqid"] == "scb/lisa/individer-15plus"
        version = variant["versions"][0]
        assert version["fqid"] == "scb/lisa/individer-15plus/2018"
        column = version["columns"][0]
        assert column["fqid"] == "scb/lisa/individer-15plus/2018/kon"


class TestGetVarinfoFqid:
    def test_instance_binding_fqid(self, slugged_conn: sqlite3.Connection) -> None:
        results = get_varinfo(slugged_conn, "Kön")
        instance = results[0]["instances"][0]
        assert instance["fqid"] == "scb/lisa/individer-15plus/2018/kon"


class TestGetClassificationFqid:
    def test_classification_fqid(self, slugged_conn: sqlite3.Connection) -> None:
        result = get_classification(slugged_conn, "SUN2020")
        assert result["fqid"] == "class/sun/2020"

    def test_null_slug_omits_fqid(self, tmp_path: Path) -> None:
        # Existing seed in the wider repo may have classifications without
        # slug populated. ``_classification_row`` drops the key entirely
        # rather than carrying a null value — the existing convention for
        # lean JSON output.
        conn = sqlite3.connect(tmp_path / "rm.db")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO classification "
            "(short_name, name, version) "
            "VALUES ('SUN2020', 'SUN', '2020')"
        )
        result = get_classification(conn, "SUN2020")
        assert "fqid" not in result
