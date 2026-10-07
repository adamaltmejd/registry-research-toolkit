"""Tests for FQID emission in query results."""

from __future__ import annotations

from typing import TYPE_CHECKING

import catalog_test_support
from reg_meta.queries import (
    get_classification,
    get_varinfo,
)

if TYPE_CHECKING:
    import sqlite3

# The shared fixture lives in catalog_test_support; bind it here for pytest.
slugged_conn = catalog_test_support.slugged_conn


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
